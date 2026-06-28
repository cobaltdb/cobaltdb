package catalog

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// indexSnapshot holds a single index definition and its tree reference
// captured under Catalog.mu so the buffered INSERT row loop can run lock-free.
type indexSnapshot struct {
	name string
	def  *IndexDef
	tree btree.TreeStore
}

// fkSnapshot holds a referenced table definition and tree for FK validation
// captured under Catalog.mu so the buffered INSERT row loop can run lock-free.
type fkSnapshot struct {
	table *TableDef
	tree  btree.TreeStore
}

// insertSnapshot holds all Catalog metadata needed for the buffered INSERT path.
// It is built under a brief Catalog.mu RLock and then used without the lock.
type insertSnapshot struct {
	table    *TableDef
	tree     btree.TreeStore
	indexes  []indexSnapshot
	triggers []*query.CreateTriggerStmt
	fkRefs   map[string]fkSnapshot
}

// buildInsertSnapshot captures table tree, indexes, triggers, and FK references
// under Catalog.mu so the buffered path can release the lock before the row loop.
func (c *Catalog) buildInsertSnapshot(table *TableDef, stmt *query.InsertStmt, args []interface{}) (*insertSnapshot, error) {
	snap := &insertSnapshot{table: table}

	// Table tree (handles partitioned tables too).
	tree, _, err := c.getInsertTargetTree(table, stmt, args)
	if err != nil {
		return nil, err
	}
	snap.tree = tree

	// Indexes for this table.
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == table.Name {
			snap.indexes = append(snap.indexes, indexSnapshot{
				name: idxName,
				def:  idxDef,
				tree: c.indexTrees[idxName],
			})
		}
	}

	// Triggers.
	snap.triggers = c.getTriggersForTableLocked(table.Name, "INSERT")

	// FK referenced tables.
	if len(table.ForeignKeys) > 0 {
		snap.fkRefs = make(map[string]fkSnapshot, len(table.ForeignKeys))
		for _, fk := range table.ForeignKeys {
			if _, ok := snap.fkRefs[fk.ReferencedTable]; ok {
				continue
			}
			refTable, err := c.getTableLocked(fk.ReferencedTable)
			if err != nil {
				continue
			}
			refTree, exists := c.tableTrees[fk.ReferencedTable]
			if !exists {
				continue
			}
			snap.fkRefs[fk.ReferencedTable] = fkSnapshot{table: refTable, tree: refTree}
		}
	}

	return snap, nil
}

// executeTriggersList is the lock-free variant of executeTriggers that uses a
// pre-snapshot trigger list instead of reading c.triggers.
func (c *Catalog) executeTriggersList(ctx context.Context, triggers []*query.CreateTriggerStmt, event string, timing string, newRow []interface{}, oldRow []interface{}, columns []ColumnDef) error {
	for _, trigger := range triggers {
		if trigger.Time != timing {
			continue
		}
		if len(trigger.Body) == 0 {
			continue
		}
		if trigger.Condition != nil {
			resolvedCond := c.resolveTriggerExpr(trigger.Condition, newRow, oldRow, columns)
			result, err := evaluateExpression(c, nil, nil, resolvedCond, nil)
			if err != nil {
				continue
			}
			if result == nil {
				continue
			}
			if b, ok := result.(bool); ok && !b {
				continue
			}
			if f, ok := toFloat64(result); ok && f == 0 {
				continue
			}
		}
		if err := c.executeTriggerBody(ctx, trigger.Name, trigger.Body, newRow, oldRow, columns); err != nil {
			return fmt.Errorf("trigger %s execution failed: %w", trigger.Name, err)
		}
	}
	return nil
}

// checkUniqueConstraintsSnapshot is the lock-free variant that uses a
// pre-snapshot index list instead of iterating c.indexes/c.indexTrees.
func (c *Catalog) checkUniqueConstraintsSnapshot(tree btree.TreeStore, table *TableDef, stmt *query.InsertStmt, rowValues []interface{}, ts *catalogTxnState, idxSnap []indexSnapshot) (bool, error) {
	for i, col := range table.Columns {
		if !col.Unique || rowValues[i] == nil {
			continue
		}
		var duplicateKey []byte
		colLower := strings.ToLower(col.Name)
		found := false
		for _, idx := range idxSnap {
			if !idx.def.Unique || len(idx.def.Columns) != 1 || strings.ToLower(idx.def.Columns[0]) != colLower {
				continue
			}
			if idx.tree != nil {
				idxKey := typeTaggedKey(rowValues[i])
				if pkData, err := idx.tree.Get([]byte(idxKey)); err == nil {
					duplicateKey = append([]byte(nil), pkData...)
				}
			}
			found = true
			break
		}
		if !found {
			// Read-your-writes: pending writes in this txn supersede the committed
			// tree. The pending value is checked here (skipping soft-deletes), and
			// the committed scan below skips any key overridden by a pending write —
			// so a row deleted earlier in this txn frees its unique value for reuse.
			var pendingMap map[string]PendingWrite
			if ts != nil {
				pendingMap = ts.getPendingWriteMap()[stmt.Table]
			}
			for _, pw := range pendingMap {
				existingRow, live, err := decodeLiveRow(pw.Value, len(table.Columns))
				if err != nil {
					return false, fmt.Errorf("failed to decode pending row for UNIQUE check on table %s: %w", stmt.Table, err)
				}
				if !live {
					continue
				}
				if len(existingRow) > i && compareValues(rowValues[i], existingRow[i]) == 0 {
					if stmt.ConflictAction == query.ConflictIgnore {
						return true, nil
					}
					return false, fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
				}
			}
			iter, err := tree.Scan(nil, nil)
			if err != nil {
				return false, fmt.Errorf("failed to scan table for UNIQUE check: %w", err)
			}
			for iter.HasNext() {
				k, existingData, err := iter.Next()
				if err != nil {
					iter.Close()
					return false, fmt.Errorf("failed to read row during UNIQUE check on table %s: %w", stmt.Table, err)
				}
				if _, overridden := pendingMap[string(k)]; overridden {
					continue // pending value supersedes committed (handled above)
				}
				existingRow, live, err := decodeLiveRow(existingData, len(table.Columns))
				if err != nil {
					iter.Close()
					return false, fmt.Errorf("failed to decode row during UNIQUE check on table %s: %w", stmt.Table, err)
				}
				if !live {
					continue
				}
				if len(existingRow) > i && compareValues(rowValues[i], existingRow[i]) == 0 {
					duplicateKey = k
					break
				}
			}
			iter.Close()
		}
		if duplicateKey != nil {
			if stmt.ConflictAction == query.ConflictIgnore {
				return true, nil
			} else if stmt.ConflictAction == query.ConflictReplace {
				var deletedIndexEntries []deletedIndexEntry
				oldData, getErr := tree.Get(duplicateKey)
				if getErr == nil {
					oldRow, decErr := decodeRow(oldData, len(table.Columns))
					if decErr == nil {
						for _, idx := range idxSnap {
							if idx.def.TableName != stmt.Table || len(idx.def.Columns) == 0 {
								continue
							}
							if idx.tree == nil {
								continue
							}
							deletedEntry, err := deleteIndexEntryForRowTracked(idx.name, table, idx.def, idx.tree, oldRow, duplicateKey)
							if err != nil {
								if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
									return false, fmt.Errorf("failed to delete from index %s: %w; failed to restore deleted index entries: %v", idx.name, err, restoreErr)
								}
								return false, fmt.Errorf("failed to delete from index %s: %w", idx.name, err)
							}
							if deletedEntry != nil {
								deletedIndexEntries = append(deletedIndexEntries, *deletedEntry)
							}
						}
					}
				}
				if err := deleteRowKey(tree, duplicateKey); err != nil {
					if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
						return false, fmt.Errorf("failed to delete duplicate row: %w; failed to restore deleted index entries: %v", err, restoreErr)
					}
					return false, fmt.Errorf("failed to delete duplicate row: %w", err)
				}
				return false, nil
			}
			return false, fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
		}
	}
	return false, nil
}

func deleteRowKey(tree btree.TreeStore, key []byte) error {
	if bt, ok := tree.(*btree.BTree); ok {
		return bt.DeleteString(string(key))
	}
	return tree.Delete(key)
}

type deletedIndexEntry struct {
	indexName string
	tree      btree.TreeStore
	key       []byte
	value     []byte
}

func deleteIndexEntryForRowTracked(indexName string, table *TableDef, idxDef *IndexDef, idxTree btree.TreeStore, row []interface{}, rowKey []byte) (*deletedIndexEntry, error) {
	indexKey, ok := buildCompositeIndexKey(table, idxDef, row)
	if !ok {
		return nil, nil
	}
	var storageKey []byte
	if idxDef.Unique {
		storageKey = []byte(indexKey)
	} else {
		storageKey = []byte(indexKey + "\x00" + string(rowKey))
	}
	if err := idxTree.Delete(storageKey); err != nil {
		return nil, err
	}
	return &deletedIndexEntry{
		indexName: indexName,
		tree:      idxTree,
		key:       append([]byte(nil), storageKey...),
		value:     append([]byte(nil), rowKey...),
	}, nil
}

func restoreDeletedIndexEntries(entries []deletedIndexEntry) error {
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if entry.tree == nil {
			continue
		}
		if err := entry.tree.Put(entry.key, entry.value); err != nil {
			if entry.indexName != "" {
				return fmt.Errorf("%s: %w", entry.indexName, err)
			}
			return err
		}
	}
	return nil
}

// buildBufferedInsertIndexesSnapshot is the lock-free variant that uses a
// pre-snapshot index list instead of iterating c.indexes/c.indexTrees.
func (c *Catalog) buildBufferedInsertIndexesSnapshot(table *TableDef, stmt *query.InsertStmt, key string, rowValues []interface{}, ts *catalogTxnState, idxSnap []indexSnapshot) ([]PendingIndexUpdate, bool, error) {
	var idxUpdates []PendingIndexUpdate
	for _, idx := range idxSnap {
		if idx.def.TableName != stmt.Table || len(idx.def.Columns) == 0 {
			continue
		}
		indexKey, ok := buildCompositeIndexKey(table, idx.def, rowValues)
		if !ok || indexKey == "" {
			continue
		}
		if idx.def.Unique && idx.tree != nil {
			idxVal, err := idx.tree.Get([]byte(indexKey))
			c.recordManagerReadTs(ts, idx.name, indexKey, idxVal)
			// A committed slot counts only if it was not freed by a pending delete
			// in this txn; a pending insert always counts (read-your-writes).
			netState := c.indexKeyPendingState(idx.name, indexKey)
			if (err == nil && netState != -1) || netState == 1 {
				if stmt.ConflictAction == query.ConflictIgnore {
					return nil, true, nil
				}
				return nil, false, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, idx.name)
			}
		}
		var idxStorageKey string
		if idx.def.Unique {
			idxStorageKey = indexKey
		} else {
			idxStorageKey = indexKey + "\x00" + key
		}
		idxUpdates = append(idxUpdates, PendingIndexUpdate{
			IndexName: idx.name,
			Key:       idxStorageKey,
			Value:     []byte(key),
		})
	}
	return idxUpdates, false, nil
}

func foreignKeyValuesForRow(table *TableDef, fk ForeignKeyDef, row []interface{}) ([]interface{}, bool) {
	values := make([]interface{}, len(fk.Columns))
	for i, colName := range fk.Columns {
		colIdx := table.GetColumnIndex(colName)
		if colIdx < 0 || colIdx >= len(row) {
			return nil, true
		}
		values[i] = row[colIdx]
		if values[i] == nil {
			return nil, true
		}
	}
	return values, false
}

func foreignKeyColumnsChanged(table *TableDef, fk ForeignKeyDef, oldRow, newRow []interface{}) bool {
	for _, colName := range fk.Columns {
		colIdx := table.GetColumnIndex(colName)
		if colIdx < 0 || colIdx >= len(newRow) {
			continue
		}
		if colIdx >= len(oldRow) || compareValues(newRow[colIdx], oldRow[colIdx]) != 0 {
			return true
		}
	}
	return false
}

func referencedColumnsForTable(refTable *TableDef, fk ForeignKeyDef) []string {
	if len(fk.ReferencedColumns) > 0 {
		return fk.ReferencedColumns
	}
	if refTable == nil {
		return nil
	}
	return refTable.PrimaryKey
}

func rowMatchesReferencedValues(refTable *TableDef, row []interface{}, refColumns []string, values []interface{}) bool {
	if len(refColumns) != len(values) {
		return false
	}
	for i, refCol := range refColumns {
		refColIdx := refTable.GetColumnIndex(refCol)
		if refColIdx < 0 || refColIdx >= len(row) || compareValues(values[i], row[refColIdx]) != 0 {
			return false
		}
	}
	return true
}

func referencedRowExistsSnapshot(refTable *TableDef, refTree btree.TreeStore, pendingParents map[string]PendingWrite, refColumns []string, values []interface{}) (bool, error) {
	if refTable == nil || refTree == nil {
		return false, nil
	}
	if len(refColumns) == 0 {
		refColumns = refTable.PrimaryKey
	}
	if len(refColumns) != len(values) {
		return false, fmt.Errorf("referenced column count (%d) does not match value count (%d)", len(refColumns), len(values))
	}
	for _, pw := range pendingParents {
		refRow, live, err := decodeLiveRow(pw.Value, len(refTable.Columns))
		if err != nil {
			return false, err
		}
		if !live {
			continue
		}
		if rowMatchesReferencedValues(refTable, refRow, refColumns, values) {
			return true, nil
		}
	}
	refIter, err := refTree.Scan(nil, nil)
	if err != nil {
		return false, err
	}
	defer refIter.Close()
	for refIter.HasNext() {
		refKey, refData, err := refIter.Next()
		if err != nil {
			return false, err
		}
		if _, overridden := pendingParents[string(refKey)]; overridden {
			continue
		}
		refRow, live, err := decodeLiveRow(refData, len(refTable.Columns))
		if err != nil {
			return false, err
		}
		if !live {
			continue
		}
		if rowMatchesReferencedValues(refTable, refRow, refColumns, values) {
			return true, nil
		}
	}
	return false, nil
}

// checkForeignKeyConstraintsSnapshot is the lock-free variant that uses
// pre-snapshot referenced tables instead of reading c.tables/c.tableTrees.
func (c *Catalog) checkForeignKeyConstraintsSnapshot(table *TableDef, rowValues []interface{}, ts *catalogTxnState, fkRefs map[string]fkSnapshot) error {
	for _, fk := range table.ForeignKeys {
		fkValues, skip := foreignKeyValuesForRow(table, fk, rowValues)
		if skip {
			continue
		}
		refSnap, ok := fkRefs[fk.ReferencedTable]
		if !ok || refSnap.tree == nil {
			return fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
		}
		var pendingParents map[string]PendingWrite
		if ts != nil {
			pendingParents = ts.getPendingWriteMap()[fk.ReferencedTable]
		}
		refColumns := referencedColumnsForTable(refSnap.table, fk)
		found, err := referencedRowExistsSnapshot(refSnap.table, refSnap.tree, pendingParents, refColumns, fkValues)
		if err != nil {
			return fmt.Errorf("FOREIGN KEY constraint failed: failed to scan referenced table %s: %w", fk.ReferencedTable, err)
		}
		if !found {
			return fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValues, fk.ReferencedTable)
		}
	}
	return nil
}

// resolvePKConflictSnapshot is the lock-free variant of resolvePKConflict.
// It is identical because it only uses the provided tree and table.
func (c *Catalog) resolvePKConflictSnapshot(tree btree.TreeStore, table *TableDef, stmt *query.InsertStmt, key string) (bool, error) {
	return c.resolvePKConflict(tree, table, stmt, key)
}

// validateInsertRowSnapshot is the lock-free variant of validateInsertRow.
func (c *Catalog) validateInsertRowSnapshot(table *TableDef, tree btree.TreeStore, stmt *query.InsertStmt, rowValues []interface{}, args []interface{}, compositePK bool, key string, ts *catalogTxnState, idxSnap []indexSnapshot, fkRefs map[string]fkSnapshot) (string, bool, error) {
	for i, col := range table.Columns {
		if col.NotNull && !col.AutoIncrement && rowValues[i] == nil {
			return key, false, fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
		}
	}
	if compositePK {
		compositeKey, ok := buildCompositePK(table, rowValues)
		if !ok {
			return key, false, fmt.Errorf("composite PRIMARY KEY columns must all be non-null")
		}
		key = compositeKey
	}
	skipRow, err := c.checkUniqueConstraintsSnapshot(tree, table, stmt, rowValues, ts, idxSnap)
	if err != nil {
		return key, false, err
	}
	if skipRow {
		return key, true, nil
	}
	if err := c.checkInsertConstraints(table, rowValues, nil); err != nil {
		return key, false, err
	}
	if err := c.checkForeignKeyConstraintsSnapshot(table, rowValues, ts, fkRefs); err != nil {
		return key, false, err
	}
	return key, false, nil
}

// insertBufferedLocked executes the buffered INSERT path without holding Catalog.mu.
// All required metadata is passed via snap.
func (c *Catalog) insertBufferedLocked(ctx context.Context, stmt *query.InsertStmt, args []interface{}, snap *insertSnapshot) (int64, int64, error) {
	table := snap.table
	tree := snap.tree
	ts := c.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive

	numInsertCols := len(stmt.Columns)
	if numInsertCols == 0 {
		numInsertCols = len(table.Columns)
	}
	var insertColIndicesBuf [8]int
	var insertColIndices []int
	var insertColumns []string
	if len(stmt.Columns) > 0 {
		insertColumns = stmt.Columns
		n := len(stmt.Columns)
		if n <= 8 {
			insertColIndices = insertColIndicesBuf[:n]
		} else {
			insertColIndices = make([]int, n)
		}
		for i, colName := range stmt.Columns {
			if table.GetColumnIndex(colName) < 0 {
				return 0, 0, fmt.Errorf("column '%s' does not exist in table '%s'", colName, stmt.Table)
			}
			insertColIndices[i] = table.GetColumnIndex(colName)
		}
	}

	rowsAffected := int64(0)
	autoIncValue := int64(0)
	valueRows := stmt.Values
	if stmt.Select != nil {
		var err error
		valueRows, err = c.convertSelectToValueRows(stmt, numInsertCols, args)
		if err != nil {
			return 0, 0, err
		}
	}

	savedAutoIncSeq := atomic.LoadInt64(&table.AutoIncSeq)
	if txnActive {
		c.appendUndoEntry(undoEntry{
			action:        undoAutoIncSeq,
			tableName:     stmt.Table,
			oldAutoIncSeq: savedAutoIncSeq,
		})
	}

	var stmtInserts []stmtInsertEntry
	var insertedRows [][]interface{}
	var insertErr error
	pendingWriteStartPos := 0
	if ts != nil {
		pendingWriteStartPos = len(ts.pendingWrites)
	}
	rollbackInsertErr := func(err error) (int64, int64, error) {
		if ts != nil {
			ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
			rebuildPendingWriteMap(ts)
		}
		if rbErr := c.rollbackStatementInserts(tree, table, stmtInserts, savedAutoIncSeq); rbErr != nil {
			err = fmt.Errorf("%w; rollback failed: %v", err, rbErr)
		}
		if !txnActive {
			return 0, 0, err
		}
		undoToRemove := 1 + len(stmtInserts)
		var undoLog []undoEntry
		if ts != nil {
			undoLog = ts.undoLog
		}
		if len(undoLog) >= undoToRemove {
			c.truncateUndoLog(len(undoLog) - undoToRemove)
		}
		return 0, 0, err
	}

	needsInsertedRows := len(stmt.Returning) > 0 || len(snap.triggers) > 0
	compositePK := len(table.PrimaryKey) > 1

	for _, valueRow := range valueRows {
		if len(valueRow) != numInsertCols {
			// Only `INSERT ... DEFAULT VALUES` may omit values. A short value
			// list without an explicit column list is rejected (as MySQL does):
			// the previous AUTO_INCREMENT relaxation positionally misaligned the
			// row — e.g. `INSERT INTO t VALUES ('alice')` on t(id AUTO_INCREMENT,
			// name) stored 'alice' in id and NULLed name (silent corruption).
			defaultValuesRow := len(valueRow) == 0 && len(stmt.Columns) == 0
			if !defaultValuesRow {
				insertErr = fmt.Errorf("INSERT has %d columns but %d values", numInsertCols, len(valueRow))
				break
			}
		}

		var key string
		hasPrimaryKey := false
		if !compositePK {
			for _, pkColName := range table.PrimaryKey {
				valueIdx := -1
				if insertColIndices != nil {
					for i, tci := range insertColIndices {
						if tci >= 0 && strings.EqualFold(table.Columns[tci].Name, pkColName) {
							valueIdx = i
							break
						}
					}
				} else {
					for i := 0; i < numInsertCols && i < len(table.Columns); i++ {
						if strings.EqualFold(table.Columns[i].Name, pkColName) {
							valueIdx = i
							break
						}
					}
				}
				if valueIdx < 0 || valueIdx >= len(valueRow) {
					continue
				}
				hasPrimaryKey = true
				if numLit, ok := valueRow[valueIdx].(*query.NumberLiteral); ok {
					k, iv, whole := formatFloatKey(numLit.Value)
					key = k
					if whole && iv > atomic.LoadInt64(&table.AutoIncSeq) {
						atomic.StoreInt64(&table.AutoIncSeq, iv)
					}
				} else {
					val, err := evaluateExpression(c, nil, nil, valueRow[valueIdx], args)
					if err == nil && val != nil {
						if strVal, ok := toString(val); ok {
							key = "S:" + strVal
						} else if fVal, ok := toFloat64(val); ok {
							k, iv, whole := formatFloatKey(fVal)
							key = k
							if whole && iv > atomic.LoadInt64(&table.AutoIncSeq) {
								atomic.StoreInt64(&table.AutoIncSeq, iv)
							}
						}
					}
				}
			}
		} else {
			hasPrimaryKey = true
		}

		if !compositePK && (!hasPrimaryKey || key == "") {
			autoIncValue = atomic.AddInt64(&table.AutoIncSeq, 1)
			key = formatKey(autoIncValue)
		}

		var rowValues []interface{}
		if n := len(table.Columns); n <= 8 && ts != nil {
			rowValues = ts.rowBuf[:n]
		} else {
			rowValues = make([]interface{}, n)
		}
		if buildErr := c.buildInsertRow(table, insertColIndices, insertColumns, valueRow, args, autoIncValue, rowValues); buildErr != nil {
			insertErr = buildErr
			break
		}

		if allowed, rlsErr := c.checkRowCheckLocked(ctx, stmt.Table, table.Columns, rowValues, security.PolicyInsert); rlsErr != nil {
			insertErr = fmt.Errorf("RLS policy check failed for INSERT: %w", rlsErr)
			break
		} else if !allowed {
			insertErr = fmt.Errorf("RLS policy denied INSERT on table '%s'", stmt.Table)
			break
		}

		var skipRow bool
		key, skipRow, insertErr = c.validateInsertRowSnapshot(table, tree, stmt, rowValues, args, compositePK, key, ts, snap.indexes, snap.fkRefs)
		if insertErr != nil {
			break
		}
		if skipRow {
			continue
		}

		var valueData []byte
		if ts != nil {
			start := len(ts.valueDataBuf)
			buf, ok := encodeVersionedRowFast(rowValues, time.Now().Unix(), ts.valueDataBuf)
			if ok {
				ts.valueDataBuf = buf
				valueData = ts.valueDataBuf[start:]
			} else {
				ts.valueDataBuf = ts.valueDataBuf[:start]
				data, err := encodeVersionedRow(rowValues, nil)
				if err != nil {
					insertErr = err
					break
				}
				ts.valueDataBuf = append(ts.valueDataBuf, data...)
				valueData = ts.valueDataBuf[start:]
			}
		} else {
			var err error
			valueData, err = encodeVersionedRow(rowValues, nil)
			if err != nil {
				insertErr = err
				break
			}
		}

		if skip, err := c.resolvePKConflictSnapshot(tree, table, stmt, key); err != nil {
			insertErr = err
			break
		} else if skip {
			continue
		}
		if c.keyInPendingWrites(stmt.Table, key) {
			if stmt.ConflictAction == query.ConflictIgnore {
				continue
			}
			insertErr = fmt.Errorf("UNIQUE constraint failed: duplicate primary key value")
			break
		}

		var existingValue []byte
		if bt, ok := tree.(*btree.BTree); ok {
			existingValue, _ = bt.GetString(key)
		} else {
			existingValue, _ = tree.Get([]byte(key))
		}
		c.recordManagerReadTs(ts, stmt.Table, key, existingValue)

		idxUpdates, skipRow, idxErr := c.buildBufferedInsertIndexesSnapshot(table, stmt, key, rowValues, ts, snap.indexes)
		if idxErr != nil {
			insertErr = idxErr
			break
		}
		if skipRow {
			continue
		}

		if trigErr := c.executeTriggersList(ctx, snap.triggers, "INSERT", "BEFORE", rowValues, nil, table.Columns); trigErr != nil {
			insertErr = fmt.Errorf("BEFORE INSERT trigger failed: %w", trigErr)
			break
		}

		c.appendPendingWriteTs(ts, PendingWrite{
			TreeName:     stmt.Table,
			Key:          key,
			Value:        valueData,
			IndexUpdates: idxUpdates,
		})
		if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
			mt.SetWrite(stmt.Table, key, valueData)
		}

		if needsInsertedRows {
			rowCopy := make([]interface{}, len(rowValues))
			copy(rowCopy, rowValues)
			insertedRows = append(insertedRows, rowCopy)
		}
		rowsAffected++
	}

	if insertErr != nil {
		return rollbackInsertErr(insertErr)
	}

	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, insertedRow := range insertedRows {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, insertedRow, table, args)
			if err != nil {
				return rollbackInsertErr(fmt.Errorf("RETURNING clause failed: %w", err))
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	for _, insertedRow := range insertedRows {
		if trigErr := c.executeTriggersList(ctx, snap.triggers, "INSERT", "AFTER", insertedRow, nil, table.Columns); trigErr != nil {
			return rollbackInsertErr(fmt.Errorf("AFTER INSERT trigger failed: %w", trigErr))
		}
	}

	c.invalidateQueryCache(stmt.Table)

	c.setLastReturning(returningRows, returningCols)

	if rowsAffected > 0 {
		c.ensureVacuumMaps()
		c.vacuumMu.Lock()
		c.liveTuples[stmt.Table] += rowsAffected
		c.vacuumMu.Unlock()
	}

	return autoIncValue, rowsAffected, nil
}

// zeroPadding is a lookup table for common zero-padding lengths (0-20).
var zeroPadding = [21]string{
	"",
	"0",
	"00",
	"000",
	"0000",
	"00000",
	"000000",
	"0000000",
	"00000000",
	"000000000",
	"0000000000",
	"00000000000",
	"000000000000",
	"0000000000000",
	"00000000000000",
	"000000000000000",
	"0000000000000000",
	"00000000000000000",
	"000000000000000000",
	"0000000000000000000",
	"00000000000000000000",
}

// formatKeyCache pre-computes zero-padded keys for the most common auto-increment
// values (0..9999). This eliminates the strconv.FormatInt allocation for the
// vast majority of single-row INSERT workloads.
const formatKeyCacheSize = 100000

var formatKeyCache [formatKeyCacheSize]string

func init() {
	for i := 0; i < formatKeyCacheSize; i++ {
		v := int64(i)
		s := strconv.FormatInt(v, 10)
		if n := 20 - len(s); n > 0 {
			formatKeyCache[i] = zeroPadding[n] + s
		} else {
			formatKeyCache[i] = s
		}
	}
}

// formatKey formats int64 as zero-padded string (20 digits) for consistent key ordering.
func formatKey(pkVal int64) string {
	if pkVal >= 0 && pkVal < formatKeyCacheSize {
		return formatKeyCache[pkVal]
	}
	s := strconv.FormatInt(pkVal, 10)
	if n := 20 - len(s); n > 0 {
		return zeroPadding[n] + s
	}
	return s
}

// formatFloatKey builds the B-tree key for a numeric primary key value. A whole
// number uses the integer key (preserving on-disk format and the AUTO_INCREMENT
// interaction); a fractional value uses an "F:"-tagged exact float string so
// distinct floats (e.g. 1.2 and 1.8) do NOT collide on int64(value) — which
// previously truncated them to the same key, causing spurious UNIQUE failures /
// silent overwrites. It must stay consistent with serializePK's float branch.
// Returns (key, intValue, isWholeNumber).
func formatFloatKey(f float64) (string, int64, bool) {
	if f == float64(int64(f)) {
		iv := int64(f)
		return formatKey(iv), iv, true
	}
	return "F:" + strconv.FormatFloat(f, 'g', -1, 64), 0, false
}

// compositeKeySep separates columns in a composite primary key. 0x00 is safe:
// formatKeyComponent outputs only digits or ASCII, never a null byte.
const compositeKeySep = "\x00"

// formatKeyComponent formats a single value as a key component. Must be
// consistent with formatKey for int types so single-column PKs keep their
// existing on-disk representation.
func formatKeyComponent(val interface{}) (string, bool) {
	switch v := val.(type) {
	case int:
		return formatKey(int64(v)), true
	case int64:
		return formatKey(v), true
	case float64:
		return formatKey(int64(v)), true
	case string:
		return "S:" + v, true
	case bool:
		if v {
			return "B:1", true
		}
		return "B:0", true
	case nil:
		return "", false
	default:
		return "X:" + ValueToStringKey(v), true
	}
}

// buildCompositePK builds the btree key for a row given the table's PK columns
// and the already-evaluated rowValues slice (aligned with table.Columns). For
// single-column PKs this produces the same key as the legacy formatKey path,
// preserving backward compatibility. Returns ok=false if any PK column value
// is nil (PK implies NOT NULL).
func buildCompositePK(table *TableDef, rowValues []interface{}) (string, bool) {
	if len(table.PrimaryKey) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(table.PrimaryKey))
	for _, pkCol := range table.PrimaryKey {
		idx := table.GetColumnIndex(pkCol)
		if idx < 0 || idx >= len(rowValues) {
			return "", false
		}
		part, ok := formatKeyComponent(rowValues[idx])
		if !ok {
			return "", false
		}
		parts = append(parts, part)
	}
	if len(parts) == 1 {
		return parts[0], true
	}
	return strings.Join(parts, compositeKeySep), true
}

func (c *Catalog) Insert(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (int64, int64, error) {
	// Fast path: resolve table metadata from schema cache without lock.
	table, ver, cacheHit := c.getCachedTable(stmt.Table)
	if !cacheHit {
		c.mu.RLock()
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil && err != ErrTableNotFound {
			c.mu.RUnlock()
			return 0, 0, err
		}
		// Don't return ErrTableNotFound here — insertLocked may need to check
		// for INSTEAD OF triggers on views.
		if table != nil {
			ver = c.schemaVersion.Load()
			c.putCachedTable(stmt.Table, table)
		}
		c.mu.RUnlock()
	}

	c.mu.RLock()
	// Schema may have changed while we were lock-free; re-resolve if stale.
	if table != nil && c.schemaVersion.Load() != ver {
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil && err != ErrTableNotFound {
			c.mu.RUnlock()
			return 0, 0, err
		}
	}

	// Check INSTEAD OF trigger first (needs lock).
	if trig := c.findInsteadOfTrigger(stmt.Table, "INSERT"); trig != nil {
		c.mu.RUnlock()
		return c.executeInsteadOfTrigger(ctx, trig, stmt, args)
	}

	// Determine buffered mode.  We can check enableBufferedWrites without the
	// lock because it is set once at engine open and never changed afterwards.
	useBuffer := c.isBufferedMode() && table != nil && table.Partition == nil && stmt.ConflictAction != query.ConflictReplace
	if useBuffer && c.hasVectorIndexForTableLocked(stmt.Table) {
		useBuffer = false
	}

	if useBuffer {
		// Snapshot all metadata needed for the buffered row loop, then release
		// Catalog.mu so concurrent writers and DDL do not block us.
		snap, err := c.buildInsertSnapshot(table, stmt, args)
		if err != nil {
			c.mu.RUnlock()
			return 0, 0, err
		}
		c.mu.RUnlock()
		return c.insertBufferedLocked(ctx, stmt, args, snap)
	}

	// Direct mutation path (legacy single-writer or REPLACE).  Keep the lock.
	defer c.mu.RUnlock()
	return c.insertLocked(ctx, stmt, args, table)
}

// buildInsertRow maps provided value expressions to their target columns and
// fills unset columns with defaults (auto-increment, DEFAULT expression, or NULL).
// resolvePKConflict handles an existing primary key during INSERT by checking
// if the existing row is soft-deleted and applying the statement conflict action
// (IGNORE or REPLACE). Returns (true, nil) to skip the row, (false, nil) to
// proceed with insert, or (false, error) on failure.
func (c *Catalog) resolvePKConflict(tree btree.TreeStore, table *TableDef, stmt *query.InsertStmt, key string) (bool, error) {
	// Read-your-writes: a pending write in this txn supersedes the committed tree.
	// If the key was deleted earlier in this same txn, its PK is free for re-insert
	// even though the committed tree still holds the (not-yet-applied) live row.
	if ts := c.getCurrentTxn(); ts != nil && len(ts.pendingWrites) > 0 {
		if m, ok := ts.getPendingWriteMap()[stmt.Table]; ok {
			if pw, exists := m[key]; exists {
				if vrow, decErr := decodeVersionedRow(pw.Value, len(table.Columns)); decErr == nil && vrow.Version.DeletedAt > 0 {
					return false, nil // pending-deleted in this txn -> PK is free
				}
				// A pending live row is a genuine in-txn duplicate; it is rejected by
				// the keyInPendingWrites guard at the call site.
			}
		}
	}

	var existingData []byte
	var err error
	if bt, ok := tree.(*btree.BTree); ok {
		existingData, err = bt.GetString(key)
	} else {
		existingData, err = tree.Get([]byte(key))
	}
	if err != nil {
		return false, nil // Key does not exist, proceed with insert
	}

	vrow, decErr := decodeVersionedRow(existingData, len(table.Columns))
	isDeleted := decErr == nil && vrow.Version.DeletedAt > 0
	if isDeleted {
		return false, nil // Soft-deleted row can be replaced
	}

	if stmt.ConflictAction == query.ConflictIgnore {
		return true, nil // Skip this row
	} else if stmt.ConflictAction == query.ConflictReplace {
		var deletedIndexEntries []deletedIndexEntry
		oldRow, decErr := decodeRow(existingData, len(table.Columns))
		if decErr == nil {
			for idxName, idxTree := range c.indexTrees {
				idxDef := c.indexes[idxName]
				if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
					deletedEntry, err := deleteIndexEntryForRowTracked(idxName, table, idxDef, idxTree, oldRow, []byte(key))
					if err != nil {
						if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
							return false, fmt.Errorf("failed to delete from index %s for REPLACE: %w; failed to restore deleted index entries: %v", idxName, err, restoreErr)
						}
						return false, fmt.Errorf("failed to delete from index %s for REPLACE: %w", idxName, err)
					}
					if deletedEntry != nil {
						deletedIndexEntries = append(deletedIndexEntries, *deletedEntry)
					}
				}
			}
		}
		if err := deleteRowKey(tree, []byte(key)); err != nil {
			if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
				return false, fmt.Errorf("failed to delete row for REPLACE: %w; failed to restore deleted index entries: %v", err, restoreErr)
			}
			return false, fmt.Errorf("failed to delete row for REPLACE: %w", err)
		}
		return false, nil // Proceed with insert after cleanup
	}
	return false, fmt.Errorf("UNIQUE constraint failed: duplicate primary key value")
}

func (c *Catalog) buildInsertRow(table *TableDef, insertColIndices []int, insertColumns []string, valueRow []query.Expression, args []interface{}, autoIncValue int64, rowValues []interface{}) error {
	// Set defaults for all columns first.
	for i, col := range table.Columns {
		if col.AutoIncrement {
			rowValues[i] = float64(autoIncValue)
		} else if col.defaultExpr != nil {
			if defVal, err := EvalExpression(col.defaultExpr, args); err == nil {
				rowValues[i] = defVal
			}
		}
	}

	// Overlay explicit insert values.
	if insertColIndices != nil {
		for colIdx, tableColIdx := range insertColIndices {
			if colIdx < len(valueRow) && tableColIdx >= 0 {
				// The DEFAULT keyword leaves the column default (already set above).
				if _, isDefault := valueRow[colIdx].(*query.DefaultExpr); isDefault {
					continue
				}
				val, err := evaluateExpression(c, nil, nil, valueRow[colIdx], args)
				if err != nil {
					colName := ""
					if insertColumns != nil {
						colName = insertColumns[colIdx]
					} else if colIdx < len(table.Columns) {
						colName = table.Columns[colIdx].Name
					}
					return fmt.Errorf("failed to evaluate value for column '%s': %w", colName, err)
				}
				rowValues[tableColIdx] = val
			}
		}
	} else {
		// Identity mapping: valueRow[i] maps to table.Columns[i].
		for colIdx := 0; colIdx < len(valueRow) && colIdx < len(table.Columns); colIdx++ {
			// The DEFAULT keyword leaves the column default (already set above).
			if _, isDefault := valueRow[colIdx].(*query.DefaultExpr); isDefault {
				continue
			}
			val, err := evaluateExpression(c, nil, nil, valueRow[colIdx], args)
			if err != nil {
				return fmt.Errorf("failed to evaluate value for column '%s': %w", table.Columns[colIdx].Name, err)
			}
			rowValues[colIdx] = val
		}
	}

	// Ensure primary key is set.
	for i, col := range table.Columns {
		if col.PrimaryKey && rowValues[i] == nil && autoIncValue > 0 {
			rowValues[i] = float64(autoIncValue)
		}
	}

	return nil
}

// validateInsertRow checks NOT NULL, composite PK, UNIQUE, CHECK, and FK constraints.
// Returns the (possibly updated) key, whether to skip the row, and any error.
func (c *Catalog) validateInsertRow(table *TableDef, tree btree.TreeStore, stmt *query.InsertStmt, rowValues []interface{}, args []interface{}, compositePK bool, key string, ts *catalogTxnState) (string, bool, error) {
	// Check NOT NULL constraints
	for i, col := range table.Columns {
		if col.NotNull && !col.AutoIncrement && rowValues[i] == nil {
			return key, false, fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
		}
	}

	// For composite primary keys, build the btree key from all PK column values
	if compositePK {
		compositeKey, ok := buildCompositePK(table, rowValues)
		if !ok {
			return key, false, fmt.Errorf("composite PRIMARY KEY columns must all be non-null")
		}
		key = compositeKey
	}

	// Check constraints using the consolidated helper
	if err, skip := c.checkConstraintsForInsert(table, tree, stmt, rowValues, ts); err != nil {
		return key, false, err
	} else if skip {
		return key, true, nil
	}

	return key, false, nil
}

// checkConstraintsForInsert validates UNIQUE, CHECK, and FK constraints for INSERT.
// Uses the lock-based catalog access (table/index lookups require cat.mu).
// Returns (nil, true) when a UNIQUE conflict should cause the row to be skipped.
func (c *Catalog) checkConstraintsForInsert(table *TableDef, tree btree.TreeStore, stmt *query.InsertStmt, rowValues []interface{}, ts *catalogTxnState) (error, bool) {
	// Check UNIQUE constraints
	skipRow, err := c.checkUniqueConstraints(tree, table, stmt, rowValues, ts)
	if err != nil {
		return err, false
	}
	if skipRow {
		return nil, true // conflict: row should be skipped
	}

	// Check NOT NULL + CHECK + FOREIGN KEY via the shared row-validator.
	// The legacy (lock-based) path can use validateRowAgainstConstraints
	// directly because it holds c.mu; the snapshot path (validateInsertRow
	// in catalog_insert.go) cannot — it uses checkForeignKeyConstraintsSnapshot
	// with pre-captured FK refs.
	//
	// validateRowAgainstConstraints also covers NOT NULL, but
	// buildInsertRow + validateInsertRow already enforced NOT NULL before
	// this point, so the additional check is a no-op.
	if err := c.validateRowAgainstConstraints(table, rowValues, nil); err != nil {
		return err, false
	}

	return nil, false
}

func (c *Catalog) insertLocked(ctx context.Context, stmt *query.InsertStmt, args []interface{}, tableArg ...*TableDef) (int64, int64, error) {
	// Check for INSTEAD OF INSERT trigger first (for views)
	if trig := c.findInsteadOfTrigger(stmt.Table, "INSERT"); trig != nil {
		return c.executeInsteadOfTrigger(ctx, trig, stmt, args)
	}

	var table *TableDef
	if len(tableArg) > 0 && tableArg[0] != nil {
		table = tableArg[0]
	} else {
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil {
			return 0, 0, err
		}
	}
	if table.Type == "foreign" {
		return 0, 0, fmt.Errorf("cannot insert into foreign table '%s'", stmt.Table)
	}

	// Get the target tree - may be partitioned
	tree, _, err := c.getInsertTargetTree(table, stmt, args)
	if err != nil {
		return 0, 0, err
	}

	// Determine column mapping.
	// When no columns are specified, all table columns are inserted in order.
	numInsertCols := len(stmt.Columns)
	if numInsertCols == 0 {
		numInsertCols = len(table.Columns)
	}

	// Pre-calculate insert column indices for performance.
	// Use a stack-allocated buffer for small tables to avoid a heap alloc.
	var insertColIndicesBuf [8]int
	var insertColIndices []int
	var insertColumns []string
	if len(stmt.Columns) > 0 {
		insertColumns = stmt.Columns
		n := len(stmt.Columns)
		if n <= 8 {
			insertColIndices = insertColIndicesBuf[:n]
		} else {
			insertColIndices = make([]int, n)
		}
		for i, colName := range stmt.Columns {
			if table.GetColumnIndex(colName) < 0 {
				return 0, 0, fmt.Errorf("column '%s' does not exist in table '%s'", colName, stmt.Table)
			}
			insertColIndices[i] = table.GetColumnIndex(colName)
		}
	}

	// Insert each row
	rowsAffected := int64(0)

	// Handle INSERT...SELECT: execute SELECT and convert to value rows
	valueRows := stmt.Values
	if stmt.Select != nil {
		var err error
		valueRows, err = c.convertSelectToValueRows(stmt, numInsertCols, args)
		if err != nil {
			return 0, 0, err
		}
	}

	// Cache transaction state to avoid repeated goroutine-shard lookups.
	ts := c.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive

	// Save AutoIncSeq before insert loop for rollback
	savedAutoIncSeq := atomic.LoadInt64(&table.AutoIncSeq)
	if txnActive {
		c.appendUndoEntry(undoEntry{
			action:        undoAutoIncSeq,
			tableName:     stmt.Table,
			oldAutoIncSeq: savedAutoIncSeq,
		})
	}

	var stmtInserts []stmtInsertEntry
	var insertedRows [][]interface{} // Track rows for trigger execution
	var insertErr error

	// Track pending-write start position for statement-level rollback in buffered mode.
	pendingWriteStartPos := 0
	if ts != nil {
		pendingWriteStartPos = len(ts.pendingWrites)
	}
	rollbackInsertErr := func(err error) (int64, int64, error) {
		// Discard buffered writes added by this statement.
		if ts != nil {
			ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
			rebuildPendingWriteMap(ts)
		}
		if rbErr := c.rollbackStatementInserts(tree, table, stmtInserts, savedAutoIncSeq); rbErr != nil {
			err = fmt.Errorf("%w; rollback failed: %v", err, rbErr)
		}
		if !txnActive {
			return 0, 0, err
		}
		// Inside explicit transaction - remove undo log entries
		undoToRemove := 1 + len(stmtInserts)
		var undoLog []undoEntry
		if ts != nil {
			undoLog = ts.undoLog
		}
		if len(undoLog) >= undoToRemove {
			c.truncateUndoLog(len(undoLog) - undoToRemove)
		}
		return 0, 0, err
	}

	// Determine if we can use buffered writes for this insert.
	// Buffered mode defers B-tree mutation until commit. It supports tables
	// with secondary indexes as long as we are not doing REPLACE (which
	// requires immediate mutation of committed data).
	useBuffer := c.isBufferedMode() && table.Partition == nil && stmt.ConflictAction != query.ConflictReplace
	if useBuffer && c.hasVectorIndexForTableLocked(stmt.Table) {
		useBuffer = false
	}

	// Skip allocating row copies when no triggers or RETURNING clause need them.
	needsInsertedRows := len(stmt.Returning) > 0 || len(c.getTriggersForTableLocked(stmt.Table, "INSERT")) > 0

	// Track the last generated auto-increment value so we can return it.
	// (Multi-row INSERTs return the last generated id, matching MySQL semantics.)
	var lastAutoIncValue int64

	compositePK := len(table.PrimaryKey) > 1

	for _, valueRow := range valueRows {
		rowValues, key, autoIncValue, skipRow, rowErr := c.prepareInsertRow(
			ctx, table, stmt, args, valueRow, numInsertCols,
			insertColIndices, insertColumns, compositePK, ts, tree,
		)
		if rowErr != nil {
			insertErr = rowErr
			break
		}
		if skipRow {
			continue
		}
		if autoIncValue > 0 {
			lastAutoIncValue = autoIncValue
		}

		// Encode row with temporal versioning.
		// Reuse the per-transaction buffer to avoid a heap alloc per row.
		var valueData []byte
		if ts != nil {
			start := len(ts.valueDataBuf)
			buf, ok := encodeVersionedRowFast(rowValues, time.Now().Unix(), ts.valueDataBuf)
			if ok {
				ts.valueDataBuf = buf
				valueData = ts.valueDataBuf[start:]
			} else {
				ts.valueDataBuf = ts.valueDataBuf[:start]
				data, err := encodeVersionedRow(rowValues, nil)
				if err != nil {
					insertErr = err
					break
				}
				ts.valueDataBuf = append(ts.valueDataBuf, data...)
				valueData = ts.valueDataBuf[start:]
			}
		} else {
			var err error
			valueData, err = encodeVersionedRow(rowValues, nil)
			if err != nil {
				insertErr = err
				break
			}
		}

		if useBuffer {
			// Buffered write path: defer B-tree mutation to commit time.
			// Skip WAL — txn.Manager handles durability at commit.
			bufferedRow, skipRow, bufferedErr := c.applyInsertRowBuffered(
				stmt, table, tree, ts, rowValues, key, valueData, needsInsertedRows,
			)
			if bufferedErr != nil {
				insertErr = bufferedErr
				break
			}
			if skipRow {
				continue
			}
			if bufferedRow != nil {
				insertedRows = append(insertedRows, bufferedRow)
			}
			rowsAffected++
			continue
		}

		// Direct mutation path (legacy single-writer mode).
		insertedRow, stmtInsert, skipRow, directErr := c.applyInsertRowDirect(
			ctx, stmt, table, tree, ts, txnActive, rowValues, key, valueData, needsInsertedRows,
		)
		if directErr != nil {
			insertErr = directErr
			break
		}
		if skipRow {
			continue
		}
		stmtInserts = append(stmtInserts, stmtInsert)
		if insertedRow != nil {
			insertedRows = append(insertedRows, insertedRow)
		}
		rowsAffected++
	}

	// Statement-level atomicity: undo all inserts on error
	if insertErr != nil {
		return rollbackInsertErr(insertErr)
	}

	// Finalize the INSERT: RETURNING → AFTER triggers → cache invalidation →
	// store RETURNING → vacuum bookkeeping. Any error here is reported
	// through the same rollback path so a RETURNING/trigger failure does
	// not leave the B-tree in a partially-applied state.
	if err := c.finalizeInsert(ctx, stmt, table, insertedRows, args, rowsAffected); err != nil {
		return rollbackInsertErr(err)
	}

	return lastAutoIncValue, rowsAffected, nil
}

// prepareInsertRow is the per-row pre-flight for insertLocked. It performs
// the validation, key generation, row build, RLS check, and constraint
// resolution steps, then returns the row, the resolved key, the auto-inc
// value (if generated), a skip flag, and any error. The caller is
// responsible for encoding the row and applying it (buffered or direct
// path). This extraction keeps insertLocked focused on the apply loop
// and gives prepareInsertRow a clear input/output contract that is
// independently testable.
//
// Returns:
//   - rowValues: the built row, suitable for encoding
//   - key:       the resolved B-tree key (after auto-inc if applicable)
//   - autoInc:   the auto-increment value generated for this row (0 if N/A)
//   - skipRow:   true if the row should be skipped (IGNORE/REPLACE behavior)
//   - err:       any validation/RLS/constraint error
func (c *Catalog) prepareInsertRow(
	ctx context.Context,
	table *TableDef,
	stmt *query.InsertStmt,
	args []interface{},
	valueRow []query.Expression,
	numInsertCols int,
	insertColIndices []int,
	insertColumns []string,
	compositePK bool,
	ts *catalogTxnState,
	tree btree.TreeStore,
) (rowValues []interface{}, key string, autoIncValue int64, skipRow bool, err error) {
	// Validate value count matches column count. Only DEFAULT VALUES may omit
	// values; a short value list without an explicit column list is rejected (as
	// MySQL does). The previous AUTO_INCREMENT relaxation positionally misaligned
	// the row (the value landed in the autoinc column, NULLing the real target).
	if len(valueRow) != numInsertCols {
		defaultValuesRow := len(valueRow) == 0 && len(stmt.Columns) == 0
		if !defaultValuesRow {
			return nil, "", 0, false, fmt.Errorf("INSERT has %d columns but %d values", numInsertCols, len(valueRow))
		}
	}

	// Generate unique key (use auto-increment if primary key exists).
	// For composite primary keys we defer key generation until after
	// rowValues have been evaluated (the composite key is built from
	// all PK column values together).
	hasPrimaryKey := false
	if !compositePK {
		for _, pkColName := range table.PrimaryKey {
			// Find which valueRow index corresponds to this PK column.
			valueIdx := -1
			if insertColIndices != nil {
				for i, tci := range insertColIndices {
					if tci >= 0 && strings.EqualFold(table.Columns[tci].Name, pkColName) {
						valueIdx = i
						break
					}
				}
			} else {
				for i := 0; i < numInsertCols && i < len(table.Columns); i++ {
					if strings.EqualFold(table.Columns[i].Name, pkColName) {
						valueIdx = i
						break
					}
				}
			}
			if valueIdx < 0 || valueIdx >= len(valueRow) {
				continue
			}
			hasPrimaryKey = true
			if numLit, ok := valueRow[valueIdx].(*query.NumberLiteral); ok {
				k, iv, whole := formatFloatKey(numLit.Value)
				key = k
				// Keep auto-inc counter ahead of explicit (integer) values.
				if whole && iv > atomic.LoadInt64(&table.AutoIncSeq) {
					atomic.StoreInt64(&table.AutoIncSeq, iv)
				}
			} else {
				// Non-numeric primary key (TEXT, etc.)
				val, evErr := evaluateExpression(c, nil, nil, valueRow[valueIdx], args)
				if evErr == nil && val != nil {
					if strVal, ok := toString(val); ok {
						key = "S:" + strVal // Prefix to distinguish from numeric keys
					} else if fVal, ok := toFloat64(val); ok {
						k, iv, whole := formatFloatKey(fVal)
						key = k
						if whole && iv > atomic.LoadInt64(&table.AutoIncSeq) {
							atomic.StoreInt64(&table.AutoIncSeq, iv)
						}
					}
				}
			}
		}
	} else {
		hasPrimaryKey = true // composite PKs are always present
	}

	if !compositePK && (!hasPrimaryKey || key == "") {
		// Generate auto-increment key (per-table counter)
		autoIncValue = atomic.AddInt64(&table.AutoIncSeq, 1)
		key = formatKey(autoIncValue)
	}

	// Build full row with all columns.
	// Reuse the per-transaction scratch buffer when available to avoid a heap alloc.
	if n := len(table.Columns); n <= 8 && ts != nil {
		rowValues = ts.rowBuf[:n]
	} else {
		rowValues = make([]interface{}, n)
	}
	if buildErr := c.buildInsertRow(table, insertColIndices, insertColumns, valueRow, args, autoIncValue, rowValues); buildErr != nil {
		return nil, "", 0, false, buildErr
	}

	// Apply Row-Level Security check for INSERT
	if allowed, rlsErr := c.checkRowCheckLocked(ctx, stmt.Table, table.Columns, rowValues, security.PolicyInsert); rlsErr != nil {
		return nil, "", 0, false, fmt.Errorf("RLS policy check failed for INSERT: %w", rlsErr)
	} else if !allowed {
		return nil, "", 0, false, fmt.Errorf("RLS policy denied INSERT on table '%s'", stmt.Table)
	}

	// Validate row constraints and resolve key
	key, skipRow, err = c.validateInsertRow(table, tree, stmt, rowValues, args, compositePK, key, ts)
	return rowValues, key, autoIncValue, skipRow, err
}

// finalizeInsert runs the post-apply bookkeeping for a successful INSERT
// statement: evaluate RETURNING, fire AFTER triggers, invalidate the
// query cache, store the RETURNING rows, and update the AutoVacuum live
// tuple counter. Any error returned here is a statement-level error —
// the caller is expected to roll back the entire statement so a
// RETURNING/trigger failure does not leave the B-tree in a partially
// applied state.
//
// Order matters:
//  1. RETURNING runs *before* AFTER triggers so a RETURNING error can
//     abort the statement without leaving trigger side effects behind.
//  2. AFTER triggers run before setLastReturning so the caller's next
//     call (which reads LastReturning) sees a consistent view.
//  3. Cache invalidation + setLastReturning + vacuum counter are
//     bookkeeping that only runs on a clean apply.
func (c *Catalog) finalizeInsert(
	ctx context.Context,
	stmt *query.InsertStmt,
	table *TableDef,
	insertedRows [][]interface{},
	args []interface{},
	rowsAffected int64,
) error {
	// Handle RETURNING clause before AFTER triggers so RETURNING errors can
	// abort the statement without leaving trigger side effects behind.
	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, insertedRow := range insertedRows {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, insertedRow, table, args)
			if err != nil {
				return fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	// Execute AFTER INSERT triggers for each inserted row
	for _, insertedRow := range insertedRows {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "INSERT", "AFTER", insertedRow, nil, table.Columns); trigErr != nil {
			return fmt.Errorf("AFTER INSERT trigger failed: %w", trigErr)
		}
	}

	// Invalidate query cache for the affected table
	c.invalidateQueryCache(stmt.Table)

	// Store returning rows for retrieval
	c.setLastReturning(returningRows, returningCols)

	// Track live tuples for AutoVacuum
	if rowsAffected > 0 {
		c.ensureVacuumMaps()
		c.vacuumMu.Lock()
		c.liveTuples[stmt.Table] += rowsAffected
		c.vacuumMu.Unlock()
	}

	return nil
}

// applyInsertRowBuffered buffers a single INSERT row for commit-time
// application. Returns:
//
//   - insertedRow: a defensive copy of rowValues if needsInsertedRows is
//     true and the row was actually buffered; nil otherwise (skipped,
//     conflict, or no caller wanted a copy).
//   - skipRow:     true if the caller should `continue` to the next row
//     (IGNORE/REPLACE skip, PK conflict, index-skip). The caller must
//     still pass a non-nil skipRow=true back up so finalizeInsert does
//     not see the row.
//   - err:         a statement-level error; the caller must `break` the
//     per-row loop and roll back.
//
// This is the per-row extraction of the buffered INSERT path that was
// previously inline in insertLocked. It defers all B-tree and index
// mutations to commit time; WAL is skipped because the txn.Manager
// handles durability at commit. The PK conflict and pending-write
// checks here use the post-prepare values (committed tree + buffered
// writes) so the row is consistent with the in-txn view.
func (c *Catalog) applyInsertRowBuffered(
	stmt *query.InsertStmt,
	table *TableDef,
	tree btree.TreeStore,
	ts *catalogTxnState,
	rowValues []interface{},
	key string,
	valueData []byte,
	needsInsertedRows bool,
) (insertedRow []interface{}, skipRow bool, err error) {
	// Check PK conflict against committed data AND buffered writes.
	if skip, err := c.resolvePKConflict(tree, table, stmt, key); err != nil {
		return nil, false, err
	} else if skip {
		return nil, true, nil
	}
	if c.keyInPendingWrites(stmt.Table, key) {
		if stmt.ConflictAction == query.ConflictIgnore {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("UNIQUE constraint failed: duplicate primary key value")
	}

	// Record the value we read (nil if absent, soft-deleted row if
	// deleted) so that commit-time validation detects any concurrent
	// change to this key.
	var existingValue []byte
	if bt, ok := tree.(*btree.BTree); ok {
		existingValue, _ = bt.GetString(key)
	} else {
		existingValue, _ = tree.Get([]byte(key))
	}
	c.recordManagerReadTs(ts, stmt.Table, key, existingValue)

	// Build index updates for commit-time application.
	idxUpdates, skipRow, idxErr := c.buildBufferedInsertIndexes(table, stmt, key, rowValues, ts)
	if idxErr != nil {
		return nil, false, idxErr
	}
	if skipRow {
		return nil, true, nil
	}

	// Buffer the write for commit-time application.
	c.appendPendingWriteTs(ts, PendingWrite{
		TreeName:     stmt.Table,
		Key:          key,
		Value:        valueData,
		IndexUpdates: idxUpdates,
	})

	// Also buffer in the Manager transaction's WriteSet for conflict detection.
	if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
		mt.SetWrite(stmt.Table, key, valueData)
	}

	if needsInsertedRows {
		rowCopy := make([]interface{}, len(rowValues))
		copy(rowCopy, rowValues)
		return rowCopy, false, nil
	}
	return nil, false, nil
}

// applyInsertRowDirect is the per-row extraction of the legacy single-writer
// INSERT path. Unlike the buffered path it commits the B-tree mutation
// immediately and records the WAL entry before applying the change. The
// helper encapsulates the full per-row contract — WAL append, PK conflict
// resolution, BEFORE trigger execution, B-tree store, secondary-index
// update with rollback-on-failure, vector index update, undo log entry,
// and statement-level tracking — so insertLocked is left as a thin loop
// that dispatches to either the buffered or direct apply path.
//
// The trickiest concern is the index-update-failure rollback: by the time
// insertRowIndexes reports failure, the row has already been Put into the
// B-tree. We must therefore (a) delete the row, and (b) walk back any
// index entries that were successfully added during the failed
// insertRowIndexes call. Both cleanups tolerate ErrKeyNotFound because
// either tree may have been touched multiple times during a single
// index-update cycle. Cleanup errors are wrapped into the original
// failure so the caller still sees the root cause.
//
// Returns:
//   - insertedRow: a defensive copy of rowValues when the caller needs it
//     for RETURNING or AFTER triggers; nil otherwise.
//   - stmtInsert:  the per-statement tracker entry the caller must append
//     to stmtInserts for statement-level rollback.
//   - skipRow:     true when the row was already taken by a conflicting
//     statement (IGNORE semantics) — caller should not count it.
//   - err:         any unrecoverable error. Caller breaks the loop and
//     rolls back the statement.
func (c *Catalog) applyInsertRowDirect(
	ctx context.Context,
	stmt *query.InsertStmt,
	table *TableDef,
	tree btree.TreeStore,
	ts *catalogTxnState,
	txnActive bool,
	rowValues []interface{},
	key string,
	valueData []byte,
	needsInsertedRows bool,
) (insertedRow []interface{}, stmtInsert stmtInsertEntry, skipRow bool, err error) {
	// Log to WAL before applying change (mirrors the buffered path's
	// skip-WAL rationale in reverse: here durability is per-statement,
	// so a crash mid-loop is recoverable from the WAL).
	if c.wal != nil && txnActive {
		walData, walErr := encodeLogicalWALData(stmt.Table, []byte(key), valueData)
		if walErr != nil {
			return nil, stmtInsertEntry{}, false, walErr
		}
		record := &storage.WALRecord{
			TxnID: ts.txnID,
			Type:  storage.WALInsert,
			Data:  walData,
		}
		if appendErr := c.wal.Append(record); appendErr != nil {
			return nil, stmtInsertEntry{}, false, appendErr
		}
	}

	// Enforce PRIMARY KEY uniqueness - check if key already exists.
	if pkSkip, pkErr := c.resolvePKConflict(tree, table, stmt, key); pkErr != nil {
		return nil, stmtInsertEntry{}, false, pkErr
	} else if pkSkip {
		return nil, stmtInsertEntry{}, true, nil
	}

	if trigErr := c.executeTriggers(ctx, stmt.Table, "INSERT", "BEFORE", rowValues, nil, table.Columns); trigErr != nil {
		return nil, stmtInsertEntry{}, false, fmt.Errorf("BEFORE INSERT trigger failed: %w", trigErr)
	}

	// Store in B+Tree.
	var putErr error
	if bt, ok := tree.(*btree.BTree); ok {
		putErr = bt.PutString(key, valueData)
	} else {
		putErr = tree.Put([]byte(key), valueData)
	}
	if putErr != nil {
		return nil, stmtInsertEntry{}, false, fmt.Errorf("failed to store row: %w", putErr)
	}

	// Update indexes and track changes for undo.
	idxChanges, idxSkip, idxErr := c.insertRowIndexes(tree, table, stmt, key, rowValues, ts)
	if idxErr != nil {
		// Row was stored but index failed - delete the row and roll back
		// any index entries that were successfully inserted in this iteration.
		// Cleanup errors are wrapped into the original failure but do not
		// shadow it, preserving the root cause for diagnostics.
		if rbErr := deleteRowKey(tree, []byte(key)); rbErr != nil && !errors.Is(rbErr, btree.ErrKeyNotFound) {
			idxErr = fmt.Errorf("%w; row cleanup failed: %v", idxErr, rbErr)
		}
		for _, undo := range idxChanges {
			if !undo.wasAdded {
				continue
			}
			idxTree2, ok := c.indexTrees[undo.indexName]
			if !ok {
				continue
			}
			if rbErr := idxTree2.Delete(undo.key); rbErr != nil && !errors.Is(rbErr, btree.ErrKeyNotFound) {
				idxErr = fmt.Errorf("%w; index cleanup failed for %s: %v", idxErr, undo.indexName, rbErr)
			}
		}
		return nil, stmtInsertEntry{}, false, idxErr
	}
	if idxSkip {
		return nil, stmtInsertEntry{}, true, nil
	}

	// Update vector indexes.
	if vErr := c.updateVectorIndexesForInsert(stmt.Table, rowValues, key); vErr != nil {
		return nil, stmtInsertEntry{}, false, vErr
	}

	// Record undo log entry for rollback (after applying change).
	if txnActive {
		keyCopy := []byte(key)
		c.appendUndoEntry(undoEntry{
			action:       undoInsert,
			tableName:    stmt.Table,
			key:          keyCopy,
			indexChanges: idxChanges,
		})
	}

	// Build the per-statement tracker entry. The caller appends this to
	// stmtInserts so rollbackStatementInserts can walk back the keys
	// added by this statement on error.
	si := stmtInsertEntry{key: []byte(key)}
	for _, ic := range idxChanges {
		si.idxKeys = append(si.idxKeys, struct {
			idxName string
			key     []byte
		}{ic.indexName, ic.key})
	}

	var rowCopy []interface{}
	if needsInsertedRows {
		rowCopy = make([]interface{}, len(rowValues))
		copy(rowCopy, rowValues)
	}
	return rowCopy, si, false, nil
}

// convertSelectToValueRows executes the SELECT part of INSERT...SELECT and
// converts the result rows into expression rows that the insert loop can process.
func (c *Catalog) convertSelectToValueRows(stmt *query.InsertStmt, numCols int, args []interface{}) ([][]query.Expression, error) {
	selectCols, selectRows, err := c.selectLocked(stmt.Select, args)
	if err != nil {
		return nil, fmt.Errorf("INSERT...SELECT failed: %w", err)
	}
	if len(selectCols) != numCols {
		return nil, fmt.Errorf("INSERT...SELECT column count mismatch: INSERT has %d columns, SELECT returns %d columns", numCols, len(selectCols))
	}
	valueRows := make([][]query.Expression, len(selectRows))
	for i, row := range selectRows {
		exprRow := make([]query.Expression, len(row))
		for j, val := range row {
			switch v := val.(type) {
			case nil:
				exprRow[j] = &query.NullLiteral{}
			case string:
				exprRow[j] = &query.StringLiteral{Value: v}
			case float64:
				exprRow[j] = &query.NumberLiteral{Value: v}
			case int64:
				exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: strconv.FormatInt(v, 10)}
			case int:
				exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: strconv.Itoa(v)}
			case bool:
				exprRow[j] = &query.BooleanLiteral{Value: v}
			default:
				exprRow[j] = &query.StringLiteral{Value: ValueToStringKey(v)}
			}
		}
		valueRows[i] = exprRow
	}
	return valueRows, nil
}

// checkUniqueConstraints verifies UNIQUE constraints for a single row.
// Returns (skipRow=true) when the row should be silently skipped (ON CONFLICT IGNORE),
// or an error for constraint violations. Handles ON CONFLICT REPLACE by deleting
// the conflicting row and its index entries.
// insertRowIndexes updates all indexes for a single inserted row, handling
// UNIQUE constraint violations with INSERT OR IGNORE/REPLACE conflict resolution.
// Returns index undo entries, whether the row should be skipped, and any error.
func (c *Catalog) insertRowIndexes(tree btree.TreeStore, table *TableDef, stmt *query.InsertStmt, key string, rowValues []interface{}, ts *catalogTxnState) ([]indexUndoEntry, bool, error) {
	var idxChanges []indexUndoEntry
	skipRow := false
	txnActive := ts != nil && ts.txnActive
	for idxName, idxTree := range c.indexTrees {
		idxDef := c.indexes[idxName]
		if idxDef.TableName != stmt.Table || len(idxDef.Columns) == 0 {
			continue
		}
		indexKey, ok := buildCompositeIndexKey(table, idxDef, rowValues)
		if !ok {
			continue
		}
		// Enforce UNIQUE constraint
		if idxDef.Unique {
			if oldPKData, err := idxTree.Get([]byte(indexKey)); err == nil {
				if stmt.ConflictAction == query.ConflictIgnore {
					// Delete the already-stored row from the main table
					if err := deleteRowKey(tree, []byte(key)); err != nil {
						return idxChanges, skipRow, fmt.Errorf("failed to delete ignored row: %w", err)
					}
					// Undo any index entries already added in this loop iteration
					for _, undo := range idxChanges {
						if undo.wasAdded {
							if idxTree2, ok := c.indexTrees[undo.indexName]; ok {
								if err := idxTree2.Delete(undo.key); err != nil {
									return idxChanges, skipRow, fmt.Errorf("failed to rollback index %s for ignored row: %w", undo.indexName, err)
								}
							}
						}
					}
					skipRow = true
					return idxChanges, skipRow, nil
				} else if stmt.ConflictAction == query.ConflictReplace {
					oldPK := string(oldPKData)
					if oldPK != key {
						oldRowData, getErr := tree.Get([]byte(oldPK))
						var evictedIdxUndo []indexUndoEntry
						if getErr == nil {
							oldRow, decErr := decodeRow(oldRowData, len(table.Columns))
							if decErr == nil {
								for otherIdxName, otherIdxTree := range c.indexTrees {
									otherIdxDef := c.indexes[otherIdxName]
									if otherIdxDef.TableName == stmt.Table && len(otherIdxDef.Columns) > 0 {
										del, derr := deleteIndexEntryForRowTracked(otherIdxName, table, otherIdxDef, otherIdxTree, oldRow, []byte(oldPK))
										if derr != nil {
											return idxChanges, skipRow, fmt.Errorf("failed to delete from index %s for REPLACE: %w", otherIdxName, derr)
										}
										if del != nil {
											evictedIdxUndo = append(evictedIdxUndo, indexUndoEntry{
												indexName: del.indexName,
												key:       del.key,
												oldValue:  del.value,
												wasAdded:  false,
											})
										}
									}
								}
							}
						}
						if err := tree.Delete([]byte(oldPK)); err != nil {
							return idxChanges, skipRow, fmt.Errorf("failed to delete old row for index REPLACE: %w", err)
						}
						// Record an undo entry so a transaction ROLLBACK restores the
						// row this REPLACE evicted (via a UNIQUE secondary index) and
						// its index entries. Without it the evicted, previously
						// committed row was permanently lost when the txn aborted.
						if getErr == nil && ts != nil && ts.txnActive {
							c.appendUndoEntry(undoEntry{
								action:       undoDelete,
								tableName:    stmt.Table,
								key:          append([]byte(nil), oldPK...),
								oldValue:     append([]byte(nil), oldRowData...),
								indexChanges: evictedIdxUndo,
							})
						}
					}
				} else {
					return idxChanges, skipRow, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, idxName)
				}
			}
		}
		// For non-unique indexes, use compound key: "indexValue\x00pk"
		var idxStorageKey []byte
		if idxDef.Unique {
			idxStorageKey = []byte(indexKey)
		} else {
			idxStorageKey = []byte(indexKey + "\x00" + key)
		}
		if err := idxTree.Put(idxStorageKey, []byte(key)); err != nil {
			return idxChanges, skipRow, fmt.Errorf("failed to update index %s: %w", idxName, err)
		}
		if txnActive {
			idxChanges = append(idxChanges, indexUndoEntry{
				indexName: idxName,
				key:       idxStorageKey,
				wasAdded:  true,
			})
		}
	}
	return idxChanges, skipRow, nil
}

// buildBufferedInsertIndexes constructs PendingIndexUpdate entries for a buffered
// INSERT without mutating index B-trees. It enforces UNIQUE constraints against
// both committed data and other pending writes in the same transaction.
func (c *Catalog) buildBufferedInsertIndexes(table *TableDef, stmt *query.InsertStmt, key string, rowValues []interface{}, ts *catalogTxnState) ([]PendingIndexUpdate, bool, error) {
	var idxUpdates []PendingIndexUpdate
	for idxName, idxTree := range c.indexTrees {
		idxDef := c.indexes[idxName]
		if idxDef.TableName != stmt.Table || len(idxDef.Columns) == 0 {
			continue
		}
		indexKey, ok := buildCompositeIndexKey(table, idxDef, rowValues)
		if !ok {
			continue
		}
		// Enforce UNIQUE constraint against committed data and buffered writes
		if idxDef.Unique {
			idxVal, err := idxTree.Get([]byte(indexKey))
			c.recordManagerReadTs(ts, idxName, indexKey, idxVal)
			// A committed slot counts only if it was not freed by a pending delete
			// in this txn; a pending insert always counts (read-your-writes).
			netState := c.indexKeyPendingState(idxName, indexKey)
			if (err == nil && netState != -1) || netState == 1 {
				if stmt.ConflictAction == query.ConflictIgnore {
					return nil, true, nil
				}
				return nil, false, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, idxName)
			}
		}
		var idxStorageKey string
		if idxDef.Unique {
			idxStorageKey = indexKey
		} else {
			idxStorageKey = indexKey + "\x00" + key
		}
		idxUpdates = append(idxUpdates, PendingIndexUpdate{
			IndexName: idxName,
			Key:       idxStorageKey,
			Value:     []byte(key),
		})
	}
	return idxUpdates, false, nil
}

func (c *Catalog) checkUniqueConstraints(tree btree.TreeStore, table *TableDef, stmt *query.InsertStmt, rowValues []interface{}, ts *catalogTxnState) (bool, error) {
	for i, col := range table.Columns {
		if !col.Unique || rowValues[i] == nil {
			continue
		}

		var duplicateKey []byte

		// Try index-based lookup first (O(log n) vs full table scan)
		colLower := strings.ToLower(col.Name)
		found := false
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table && idxDef.Unique && len(idxDef.Columns) == 1 && strings.ToLower(idxDef.Columns[0]) == colLower {
				if idxTree, ok := c.indexTrees[idxName]; ok {
					idxKey := typeTaggedKey(rowValues[i])
					if pkData, err := idxTree.Get([]byte(idxKey)); err == nil {
						duplicateKey = append([]byte(nil), pkData...)
					}
				}
				found = true
				break
			}
		}

		// Fallback: full table scan if no unique index found
		if !found {
			// Check pending writes first (buffered mode)
			if ts != nil {
				if m, ok := ts.getPendingWriteMap()[stmt.Table]; ok {
					for _, pw := range m {
						existingRow, live, err := decodeLiveRow(pw.Value, len(table.Columns))
						if err != nil {
							return false, fmt.Errorf("failed to decode pending row for UNIQUE check on table %s: %w", stmt.Table, err)
						}
						if !live {
							continue
						}
						if len(existingRow) > i && compareValues(rowValues[i], existingRow[i]) == 0 {
							if stmt.ConflictAction == query.ConflictIgnore {
								return true, nil
							}
							return false, fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
						}
					}
				}
			}
			iter, err := tree.Scan(nil, nil)
			if err != nil {
				return false, fmt.Errorf("failed to scan table for UNIQUE check: %w", err)
			}
			for iter.HasNext() {
				k, existingData, err := iter.Next()
				if err != nil {
					iter.Close()
					return false, fmt.Errorf("failed to read row during UNIQUE check on table %s: %w", stmt.Table, err)
				}
				existingRow, live, err := decodeLiveRow(existingData, len(table.Columns))
				if err != nil {
					iter.Close()
					return false, fmt.Errorf("failed to decode row during UNIQUE check on table %s: %w", stmt.Table, err)
				}
				if !live {
					continue
				}
				if len(existingRow) > i && compareValues(rowValues[i], existingRow[i]) == 0 {
					duplicateKey = k
					break
				}
			}
			iter.Close()
		}

		if duplicateKey != nil {
			if stmt.ConflictAction == query.ConflictIgnore {
				return true, nil
			} else if stmt.ConflictAction == query.ConflictReplace {
				var deletedIndexEntries []deletedIndexEntry
				oldData, getErr := tree.Get(duplicateKey)
				if getErr == nil {
					oldRow, decErr := decodeRow(oldData, len(table.Columns))
					if decErr == nil {
						for idxName, idxTree := range c.indexTrees {
							idxDef := c.indexes[idxName]
							if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
								deletedEntry, delErr := deleteIndexEntryForRowTracked(idxName, table, idxDef, idxTree, oldRow, duplicateKey)
								if delErr != nil {
									if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
										return false, fmt.Errorf("failed to delete from index %s: %w; failed to restore deleted index entries: %v", idxName, delErr, restoreErr)
									}
									return false, fmt.Errorf("failed to delete from index %s: %w", idxName, delErr)
								}
								if deletedEntry != nil {
									deletedIndexEntries = append(deletedIndexEntries, *deletedEntry)
								}
							}
						}
					}
				}
				if delErr := deleteRowKey(tree, duplicateKey); delErr != nil {
					if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
						return false, fmt.Errorf("failed to delete duplicate row: %w; failed to restore deleted index entries: %v", delErr, restoreErr)
					}
					return false, fmt.Errorf("failed to delete duplicate row: %w", delErr)
				}
			} else {
				return false, fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
			}
		}
	}
	return false, nil
}

// checkInsertConstraints evaluates CHECK constraints for a row being inserted.
// Per SQL standard, NULL (unknown) passes; only explicit false fails.
//
// Deprecated: prefer validateRowAgainstConstraints which covers NOT NULL +
// CHECK + FOREIGN KEY in one call and is the cross-insert/update shared
// helper. Kept for callers that only need CHECK evaluation.
func (c *Catalog) checkInsertConstraints(table *TableDef, rowValues []interface{}, args []interface{}) error {
	return c.checkRowConstraints(table, rowValues, args)
}

// validateRowAgainstConstraints runs the row-level constraint checks
// (NOT NULL, CHECK, FOREIGN KEY) that must hold for any INSERT or UPDATE
// of a row. Returns a wrapped error on the first failure; the caller
// surfaces this to the user.
//
// This is the cross-insert/update shared helper that consolidates the
// NOT NULL/CHECK/FK checks previously inlined at each call site:
//
//   - INSERT (lock-free path): checkRowConstraints + checkForeignKeyConstraintsSnapshot
//   - INSERT (legacy path):     checkRowConstraints + checkForeignKeyConstraints
//   - UPDATE (snapshot path):   NOT NULL loop + checkRowConstraints + FK loop
//
// All four used to be separate code blocks; this helper extracts the
// three that don't depend on the lock state. UNIQUE constraints are
// deliberately NOT here because they require a B-tree handle and the
// pending-write map (callers already have those, and the conflict
// detection semantics are different — INSERT can return skipRow, UPDATE
// can compare against collected entries).
//
// Args: rowValues is the candidate row; args is the bound-args slice
// for evaluating CHECK expressions. Pass nil for the simple case where
// the row has no parameter placeholders.
func (c *Catalog) validateRowAgainstConstraints(table *TableDef, rowValues []interface{}, args []interface{}) error {
	// NOT NULL — column-level.
	for i, col := range table.Columns {
		if col.NotNull && i < len(rowValues) && rowValues[i] == nil {
			return fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
		}
	}
	// CHECK — column-level + table-level (NULL passes, false fails).
	if err := c.checkRowConstraints(table, rowValues, args); err != nil {
		return err
	}
	// FOREIGN KEY — NULL values skip per SQL standard.
	return c.checkForeignKeyConstraints(table, rowValues, nil)
}

// validateRowNonFKConstraints is a focused variant of
// validateRowAgainstConstraints that runs only the NOT NULL and CHECK
// checks. Use this when the caller is going to run a separate, more
// specialized FK check (e.g. the snapshot-aware FK loop in
// checkConstraintsForUpdate, or the FK-enforcer's validateActionRow).
// Avoids duplicate FK work and lets each caller choose the right
// FK-validation strategy for its lock state.
func (c *Catalog) validateRowNonFKConstraints(table *TableDef, rowValues []interface{}, args []interface{}) error {
	// NOT NULL — column-level.
	for i, col := range table.Columns {
		if col.NotNull && i < len(rowValues) && rowValues[i] == nil {
			return fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
		}
	}
	// CHECK — column-level + table-level.
	return c.checkRowConstraints(table, rowValues, args)
}

func (c *Catalog) checkRowConstraints(table *TableDef, rowValues []interface{}, args []interface{}) error {
	for _, col := range table.Columns {
		if col.Check == nil {
			continue
		}
		result, err := evaluateExpression(c, rowValues, table.Columns, col.Check, args)
		if err != nil {
			return fmt.Errorf("CHECK constraint failed: %w", err)
		}
		if result != nil {
			if resultBool, ok := result.(bool); ok && !resultBool {
				return fmt.Errorf("CHECK constraint failed for column: %s", col.Name)
			}
		}
	}
	for _, check := range table.Checks {
		if check.Check == nil {
			continue
		}
		result, err := evaluateExpression(c, rowValues, table.Columns, check.Check, args)
		if err != nil {
			return fmt.Errorf("CHECK constraint failed: %w", err)
		}
		if result != nil {
			if resultBool, ok := result.(bool); ok && !resultBool {
				if check.Name != "" {
					return fmt.Errorf("CHECK constraint failed: %s", check.Name)
				}
				return fmt.Errorf("CHECK constraint failed")
			}
		}
	}
	return nil
}

// checkForeignKeyConstraints validates FOREIGN KEY references for a row.
// NULL values skip FK checking per SQL standard.
func (c *Catalog) checkForeignKeyConstraints(table *TableDef, rowValues []interface{}, ts *catalogTxnState) error {
	fke := NewForeignKeyEnforcer(c)
	for _, fk := range table.ForeignKeys {
		fkValues, skip := foreignKeyValuesForRow(table, fk, rowValues)
		if skip {
			continue
		}
		found, err := fke.referencedRowExists(fk.ReferencedTable, fk.ReferencedColumns, fkValues)
		if err != nil {
			return fmt.Errorf("FOREIGN KEY constraint failed: failed to decode referenced row in table %s: %w", fk.ReferencedTable, err)
		}
		if !found {
			return fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValues, fk.ReferencedTable)
		}
	}
	return nil
}

// stmtInsertEntry is used to track successful row insertions for rollback.
type stmtInsertEntry struct {
	key     []byte
	idxKeys []stmtIndexKey
}

type stmtIndexKey struct {
	idxName string
	key     []byte
}

// rollbackStatementInserts undoes all successfully inserted rows on statement
// failure. Inside an explicit transaction it also removes undo-log entries.
func (c *Catalog) rollbackStatementInserts(tree btree.TreeStore, table *TableDef, stmtInserts []stmtInsertEntry, savedAutoIncSeq int64) error {
	var rollbackErr error
	for i := len(stmtInserts) - 1; i >= 0; i-- {
		si := stmtInserts[i]
		if err := deleteRowKey(tree, si.key); err != nil && !errors.Is(err, btree.ErrKeyNotFound) && rollbackErr == nil {
			rollbackErr = fmt.Errorf("delete inserted row %s: %w", string(si.key), err)
		}
		for _, ik := range si.idxKeys {
			if idxTree, exists := c.indexTrees[ik.idxName]; exists {
				if err := idxTree.Delete(ik.key); err != nil && !errors.Is(err, btree.ErrKeyNotFound) && rollbackErr == nil {
					rollbackErr = fmt.Errorf("delete index %s key: %w", ik.idxName, err)
				}
			}
		}
	}
	atomic.StoreInt64(&table.AutoIncSeq, savedAutoIncSeq)
	return rollbackErr
}

// getInsertTargetTree returns the BTree for inserting a row
// For partitioned tables, determines the correct partition based on partition key value
func (c *Catalog) getInsertTargetTree(table *TableDef, stmt *query.InsertStmt, args []interface{}) (btree.TreeStore, int, error) {
	// If table is not partitioned, use the main table tree
	if table.Partition == nil {
		tree, exists := c.tableTrees[table.Name]
		if !exists {
			return nil, -1, ErrTableNotFound
		}
		return tree, -1, nil
	}

	// Get the partition column index
	partitionColIdx := table.GetColumnIndex(table.Partition.Column)
	if partitionColIdx < 0 {
		return nil, -1, fmt.Errorf("partition column '%s' not found in table '%s'", table.Partition.Column, table.Name)
	}

	// Determine the partition key value from the INSERT statement
	var partitionVal interface{}

	// Check if columns were specified in the INSERT
	if len(stmt.Columns) > 0 {
		// Find the partition column in the insert columns
		for i, colName := range stmt.Columns {
			if strings.EqualFold(colName, table.Partition.Column) {
				// Found it - get the value from the first row
				if len(stmt.Values) > 0 && i < len(stmt.Values[0]) {
					// Evaluate the expression to get the actual value
					val, err := evaluateExpression(c, nil, nil, stmt.Values[0][i], args)
					if err == nil {
						partitionVal = val
					}
				}
				break
			}
		}
	} else {
		// No columns specified - using all table columns in order
		if len(stmt.Values) > 0 && partitionColIdx < len(stmt.Values[0]) {
			val, err := evaluateExpression(c, nil, nil, stmt.Values[0][partitionColIdx], args)
			if err == nil {
				partitionVal = val
			}
		}
	}

	// If partition value is nil, we can't determine the partition
	if partitionVal == nil {
		return nil, -1, fmt.Errorf("partition column '%s' value is NULL, cannot determine partition", table.Partition.Column)
	}

	// Get the partition tree name
	partitionTreeName := table.getPartitionTreeName(partitionVal)
	if partitionTreeName == "" {
		return nil, -1, fmt.Errorf("no matching partition found for value %v", partitionVal)
	}

	// Get or create the partition tree
	tree, exists := c.tableTrees[partitionTreeName]
	if !exists {
		c.partitionTreeMu.Lock()
		// Double-check after acquiring lock
		tree, exists = c.tableTrees[partitionTreeName]
		if !exists {
			// Partition tree doesn't exist yet - create it using the same method as CreateTable
			newTree, err := btree.NewBTree(c.pool)
			if err != nil {
				c.partitionTreeMu.Unlock()
				return nil, -1, fmt.Errorf("failed to create partition tree: %w", err)
			}
			tree = newTree
			c.tableTrees[partitionTreeName] = tree
		}
		c.partitionTreeMu.Unlock()
	}

	return tree, partitionColIdx, nil
}
