package catalog

import (
	"context"
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
		for _, bodyStmt := range trigger.Body {
			resolved := c.resolveTriggerRefs(bodyStmt, newRow, oldRow, columns)
			if err := c.executeTriggerStatement(ctx, resolved); err != nil {
				return fmt.Errorf("trigger %s execution failed: %w", trigger.Name, err)
			}
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
				if _, err := idx.tree.Get([]byte(idxKey)); err == nil {
					duplicateKey = []byte(idxKey)
				}
			}
			found = true
			break
		}
		if !found {
			if ts != nil {
				if m, ok := ts.getPendingWriteMap()[stmt.Table]; ok {
					for _, pw := range m {
						vrow, err := decodeVersionedRow(pw.Value, len(table.Columns))
						if err != nil || vrow.Version.DeletedAt > 0 {
							continue
						}
						existingRow := vrow.Data
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
					break
				}
				vrow, err := decodeVersionedRow(existingData, len(table.Columns))
				if err != nil {
					continue
				}
				if vrow.Version.DeletedAt > 0 {
					continue
				}
				existingRow := vrow.Data
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
				oldData, getErr := tree.Get(duplicateKey)
				if getErr == nil {
					oldRow, decErr := decodeRow(oldData, len(table.Columns))
					if decErr == nil {
						for _, idx := range idxSnap {
							if idx.def.TableName != stmt.Table || len(idx.def.Columns) == 0 {
								continue
							}
							oldIdxKey, ok := buildCompositeIndexKey(table, idx.def, oldRow)
							if ok {
								if idx.def.Unique {
									_ = idx.tree.Delete([]byte(oldIdxKey))
								} else {
									compoundKey := oldIdxKey + "\x00" + string(duplicateKey)
									_ = idx.tree.Delete([]byte(compoundKey))
								}
							}
						}
					}
				}
				if bt, ok := tree.(*btree.BTree); ok {
					_ = bt.DeleteString(string(duplicateKey))
				} else {
					_ = tree.Delete(duplicateKey)
				}
				return false, nil
			}
			return false, fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
		}
	}
	return false, nil
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
			if err == nil {
				if stmt.ConflictAction == query.ConflictIgnore {
					return nil, true, nil
				}
				return nil, false, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, idx.name)
			}
			if c.indexKeyInPendingWrites(idx.name, indexKey) {
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

// checkForeignKeyConstraintsSnapshot is the lock-free variant that uses
// pre-snapshot referenced tables instead of reading c.tables/c.tableTrees.
func (c *Catalog) checkForeignKeyConstraintsSnapshot(table *TableDef, rowValues []interface{}, ts *catalogTxnState, fkRefs map[string]fkSnapshot) error {
	for _, fk := range table.ForeignKeys {
		for i, colName := range fk.Columns {
			colIdx := table.GetColumnIndex(colName)
			if colIdx < 0 || colIdx >= len(rowValues) {
				continue
			}
			fkValue := rowValues[colIdx]
			if fkValue == nil {
				continue
			}
			refSnap, ok := fkRefs[fk.ReferencedTable]
			if !ok || refSnap.tree == nil {
				return fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
			}
			refTable := refSnap.table
			refTree := refSnap.tree
			refColIdx := 0
			if len(fk.ReferencedColumns) > i {
				refColIdx = refTable.GetColumnIndex(fk.ReferencedColumns[i])
			} else if len(refTable.Columns) > 0 {
				refColIdx = 0
			}
			found := false
			refIter, err := refTree.Scan(nil, nil)
			if err != nil {
				return fmt.Errorf("FOREIGN KEY constraint failed: %w", err)
			}
			for refIter.HasNext() {
				_, refData, err := refIter.Next()
				if err != nil {
					break
				}
				vrow, err := decodeVersionedRow(refData, len(refTable.Columns))
				if err != nil {
					continue
				}
				refRow := vrow.Data
				if refColIdx < len(refRow) && compareValues(fkValue, refRow[refColIdx]) == 0 {
					found = true
					break
				}
			}
			refIter.Close()
			if !found && ts != nil {
				if m, ok := ts.getPendingWriteMap()[fk.ReferencedTable]; ok {
					for _, pw := range m {
						vrow, err := decodeVersionedRow(pw.Value, len(refTable.Columns))
						if err != nil || vrow.Version.DeletedAt > 0 {
							continue
						}
						refRow := vrow.Data
						if refColIdx < len(refRow) && compareValues(fkValue, refRow[refColIdx]) == 0 {
							found = true
							break
						}
					}
				}
			}
			if !found {
				return fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValue, fk.ReferencedTable)
			}
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
	if err := c.checkInsertConstraints(table, rowValues, args); err != nil {
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

	needsInsertedRows := len(stmt.Returning) > 0 || len(snap.triggers) > 0
	compositePK := len(table.PrimaryKey) > 1

	for _, valueRow := range valueRows {
		if len(valueRow) != numInsertCols {
			autoIncCount := 0
			for _, col := range table.Columns {
				if col.AutoIncrement {
					autoIncCount++
				}
			}
			if !(autoIncCount > 0 && len(valueRow) == numInsertCols-autoIncCount) {
				return 0, 0, fmt.Errorf("INSERT has %d columns but %d values", numInsertCols, len(valueRow))
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
					pkVal := int64(numLit.Value)
					key = formatKey(pkVal)
					if pkVal > atomic.LoadInt64(&table.AutoIncSeq) {
						atomic.StoreInt64(&table.AutoIncSeq, pkVal)
					}
				} else {
					val, err := evaluateExpression(c, nil, nil, valueRow[valueIdx], args)
					if err == nil && val != nil {
						if strVal, ok := toString(val); ok {
							key = "S:" + strVal
						} else if fVal, ok := toFloat64(val); ok {
							pkVal := int64(fVal)
							key = formatKey(pkVal)
							if pkVal > atomic.LoadInt64(&table.AutoIncSeq) {
								atomic.StoreInt64(&table.AutoIncSeq, pkVal)
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
			return 0, 0, buildErr
		}

		if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, table.Columns, rowValues, security.PolicyInsert); rlsErr != nil {
			return 0, 0, fmt.Errorf("RLS policy check failed for INSERT: %w", rlsErr)
		} else if !allowed {
			return 0, 0, fmt.Errorf("RLS policy denied INSERT on table '%s'", stmt.Table)
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
			return 0, 0, fmt.Errorf("BEFORE INSERT trigger failed: %w", trigErr)
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
		if ts != nil {
			ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
			rebuildPendingWriteMap(ts)
		}
		c.rollbackStatementInserts(tree, table, stmtInserts, savedAutoIncSeq)
		if !txnActive {
			return 0, 0, insertErr
		}
		undoToRemove := 1 + len(stmtInserts)
		var undoLog []undoEntry
		if ts != nil {
			undoLog = ts.undoLog
		}
		if len(undoLog) >= undoToRemove {
			c.truncateUndoLog(len(undoLog) - undoToRemove)
		}
		return 0, 0, insertErr
	}

	for _, insertedRow := range insertedRows {
		if trigErr := c.executeTriggersList(ctx, snap.triggers, "INSERT", "AFTER", insertedRow, nil, table.Columns); trigErr != nil {
			return 0, 0, fmt.Errorf("AFTER INSERT trigger failed: %w", trigErr)
		}
	}

	c.invalidateQueryCache(stmt.Table)

	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, insertedRow := range insertedRows {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, insertedRow, table, args)
			if err != nil {
				return 0, 0, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}
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
		oldRow, decErr := decodeRow(existingData, len(table.Columns))
		if decErr == nil {
			for idxName, idxTree := range c.indexTrees {
				idxDef := c.indexes[idxName]
				if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
					oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
					if ok {
						if idxDef.Unique {
							_ = idxTree.Delete([]byte(oldIdxKey))
						} else {
							compoundKey := oldIdxKey + "\x00" + string(key)
							_ = idxTree.Delete([]byte(compoundKey))
						}
					}
				}
			}
		}
		if bt, ok := tree.(*btree.BTree); ok {
			if err := bt.DeleteString(key); err != nil {
				return false, fmt.Errorf("failed to delete row for REPLACE: %w", err)
			}
		} else {
			if err := tree.Delete([]byte(key)); err != nil {
				return false, fmt.Errorf("failed to delete row for REPLACE: %w", err)
			}
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

	// Check UNIQUE constraints
	skipRow, err := c.checkUniqueConstraints(tree, table, stmt, rowValues, ts)
	if err != nil {
		return key, false, err
	}
	if skipRow {
		return key, true, nil
	}

	// Check CHECK constraints
	if err := c.checkInsertConstraints(table, rowValues, args); err != nil {
		return key, false, err
	}

	// Check FOREIGN KEY constraints
	if err := c.checkForeignKeyConstraints(table, rowValues, ts); err != nil {
		return key, false, err
	}

	return key, false, nil
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
	autoIncValue := int64(0)

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

	// Determine if we can use buffered writes for this insert.
	// Buffered mode defers B-tree mutation until commit. It supports tables
	// with secondary indexes as long as we are not doing REPLACE (which
	// requires immediate mutation of committed data).
	useBuffer := c.isBufferedMode() && table.Partition == nil && stmt.ConflictAction != query.ConflictReplace

	// Skip allocating row copies when no triggers or RETURNING clause need them.
	needsInsertedRows := len(stmt.Returning) > 0 || len(c.getTriggersForTableLocked(stmt.Table, "INSERT")) > 0

	for _, valueRow := range valueRows {
		// Validate value count matches column count
		if len(valueRow) != numInsertCols {
			// Allow one fewer value if there is exactly one AUTO_INCREMENT column
			autoIncCount := 0
			for _, col := range table.Columns {
				if col.AutoIncrement {
					autoIncCount++
				}
			}
			if !(autoIncCount > 0 && len(valueRow) == numInsertCols-autoIncCount) {
				return 0, 0, fmt.Errorf("INSERT has %d columns but %d values", numInsertCols, len(valueRow))
			}
		}

		// Generate unique key (use auto-increment if primary key exists).
		// For composite primary keys we defer key generation until after
		// rowValues have been evaluated (the composite key is built from
		// all PK column values together).
		var key string
		hasPrimaryKey := false
		compositePK := len(table.PrimaryKey) > 1
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
					pkVal := int64(numLit.Value)
					key = formatKey(pkVal)
					// Keep auto-inc counter ahead of explicit values
					if pkVal > atomic.LoadInt64(&table.AutoIncSeq) {
						atomic.StoreInt64(&table.AutoIncSeq, pkVal)
					}
				} else {
					// Non-numeric primary key (TEXT, etc.)
					val, err := evaluateExpression(c, nil, nil, valueRow[valueIdx], args)
					if err == nil && val != nil {
						if strVal, ok := toString(val); ok {
							key = "S:" + strVal // Prefix to distinguish from numeric keys
						} else if fVal, ok := toFloat64(val); ok {
							pkVal := int64(fVal)
							key = formatKey(pkVal)
							if pkVal > atomic.LoadInt64(&table.AutoIncSeq) {
								atomic.StoreInt64(&table.AutoIncSeq, pkVal)
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
		var rowValues []interface{}
		if n := len(table.Columns); n <= 8 && ts != nil {
			rowValues = ts.rowBuf[:n]
		} else {
			rowValues = make([]interface{}, n)
		}
		if buildErr := c.buildInsertRow(table, insertColIndices, insertColumns, valueRow, args, autoIncValue, rowValues); buildErr != nil {
			return 0, 0, buildErr
		}

		// Apply Row-Level Security check for INSERT
		if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, table.Columns, rowValues, security.PolicyInsert); rlsErr != nil {
			return 0, 0, fmt.Errorf("RLS policy check failed for INSERT: %w", rlsErr)
		} else if !allowed {
			return 0, 0, fmt.Errorf("RLS policy denied INSERT on table '%s'", stmt.Table)
		}

		// Validate row constraints and resolve key
		var skipRow bool
		key, skipRow, insertErr = c.validateInsertRow(table, tree, stmt, rowValues, args, compositePK, key, ts)
		if insertErr != nil {
			break
		}
		if skipRow {
			continue
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

			// Check PK conflict against committed data AND buffered writes.
			if skip, err := c.resolvePKConflict(tree, table, stmt, key); err != nil {
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
				insertErr = idxErr
				break
			}
			if skipRow {
				continue
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
				insertedRows = append(insertedRows, rowCopy)
			}
			rowsAffected++
			continue
		}

		// Direct mutation path (legacy single-writer mode).
		// Log to WAL before applying change
		if c.wal != nil && txnActive {
			// For INSERT, we log the key and value
			// Format: key (null-terminated) + value
			walData := append([]byte(key), 0)
			walData = append(walData, valueData...)
			record := &storage.WALRecord{
				TxnID: ts.txnID,
				Type:  storage.WALInsert,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				insertErr = err
				break
			}
		}

		// Enforce PRIMARY KEY uniqueness - check if key already exists
		if skip, err := c.resolvePKConflict(tree, table, stmt, key); err != nil {
			insertErr = err
			break
		} else if skip {
			continue
		}

		if trigErr := c.executeTriggers(ctx, stmt.Table, "INSERT", "BEFORE", rowValues, nil, table.Columns); trigErr != nil {
			insertErr = fmt.Errorf("BEFORE INSERT trigger failed: %w", trigErr)
			break
		}

		// Store in B+Tree
		if bt, ok := tree.(*btree.BTree); ok {
			err = bt.PutString(key, valueData)
		} else {
			err = tree.Put([]byte(key), valueData)
		}
		if err != nil {
			insertErr = fmt.Errorf("failed to store row: %w", err)
			break
		}

		var idxChanges []indexUndoEntry
		// Update indexes and track changes for undo
		idxChanges, skipRow, insertErr = c.insertRowIndexes(tree, table, stmt, key, rowValues, ts)
		if insertErr != nil {
			// Row was stored but index failed - delete the row and roll back
			// any index entries that were successfully inserted in this iteration.
			if bt, ok := tree.(*btree.BTree); ok {
				_ = bt.DeleteString(key)
			} else {
				_ = tree.Delete([]byte(key))
			}
			for _, undo := range idxChanges {
				if undo.wasAdded {
					if idxTree2, ok := c.indexTrees[undo.indexName]; ok {
						_ = idxTree2.Delete(undo.key) // best-effort cleanup
					}
				}
			}
			break
		}

		// Update vector indexes
		c.updateVectorIndexesForInsert(stmt.Table, rowValues, key)

		if skipRow {
			continue
		}

		// Record undo log entry for rollback (after applying change)
		if txnActive {
			keyCopy := []byte(key)
			c.appendUndoEntry(undoEntry{
				action:       undoInsert,
				tableName:    stmt.Table,
				key:          keyCopy,
				indexChanges: idxChanges,
			})
		}

		// Track for statement-level atomicity
		si := stmtInsertEntry{key: []byte(key)}
		for _, ic := range idxChanges {
			si.idxKeys = append(si.idxKeys, struct {
				idxName string
				key     []byte
			}{ic.indexName, ic.key})
		}
		stmtInserts = append(stmtInserts, si)

		if needsInsertedRows {
			rowCopy := make([]interface{}, len(rowValues))
			copy(rowCopy, rowValues)
			insertedRows = append(insertedRows, rowCopy)
		}

		rowsAffected++
	}

	// Statement-level atomicity: undo all inserts on error
	if insertErr != nil {
		// Discard buffered writes added by this statement.
		if ts != nil {
			ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
			rebuildPendingWriteMap(ts)
		}
		c.rollbackStatementInserts(tree, table, stmtInserts, savedAutoIncSeq)
		if !txnActive {
			return 0, 0, insertErr
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
		return 0, 0, insertErr
	}

	// Execute AFTER INSERT triggers for each inserted row
	for _, insertedRow := range insertedRows {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "INSERT", "AFTER", insertedRow, nil, table.Columns); trigErr != nil {
			return 0, 0, fmt.Errorf("AFTER INSERT trigger failed: %w", trigErr)
		}
	}

	// Invalidate query cache for the affected table
	c.invalidateQueryCache(stmt.Table)

	// Handle RETURNING clause
	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, insertedRow := range insertedRows {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, insertedRow, table, args)
			if err != nil {
				return 0, 0, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	// Store returning rows for retrieval
	c.setLastReturning(returningRows, returningCols)

	// Track live tuples for AutoVacuum
	if rowsAffected > 0 {
		c.ensureVacuumMaps()
		c.vacuumMu.Lock()
		c.liveTuples[stmt.Table] += rowsAffected
		c.vacuumMu.Unlock()
	}

	return autoIncValue, rowsAffected, nil
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
					_ = tree.Delete([]byte(key))
					// Undo any index entries already added in this loop iteration
					for _, undo := range idxChanges {
						if undo.wasAdded {
							if idxTree2, ok := c.indexTrees[undo.indexName]; ok {
								_ = idxTree2.Delete(undo.key)
							}
						}
					}
					skipRow = true
					return idxChanges, skipRow, nil
				} else if stmt.ConflictAction == query.ConflictReplace {
					oldPK := string(oldPKData)
					if oldPK != key {
						oldRowData, getErr := tree.Get([]byte(oldPK))
						if getErr == nil {
							oldRow, decErr := decodeRow(oldRowData, len(table.Columns))
							if decErr == nil {
								for otherIdxName, otherIdxTree := range c.indexTrees {
									otherIdxDef := c.indexes[otherIdxName]
									if otherIdxDef.TableName == stmt.Table && len(otherIdxDef.Columns) > 0 {
										oldIdxKey, ok := buildCompositeIndexKey(table, otherIdxDef, oldRow)
										if ok {
											_ = otherIdxTree.Delete([]byte(oldIdxKey))
										}
									}
								}
							}
						}
						if err := tree.Delete([]byte(oldPK)); err != nil {
							return idxChanges, skipRow, fmt.Errorf("failed to delete old row for index REPLACE: %w", err)
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
			if err == nil {
				if stmt.ConflictAction == query.ConflictIgnore {
					return nil, true, nil
				}
				return nil, false, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, idxName)
			}
			if c.indexKeyInPendingWrites(idxName, indexKey) {
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
					if _, err := idxTree.Get([]byte(idxKey)); err == nil {
						duplicateKey = []byte(idxKey)
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
						vrow, err := decodeVersionedRow(pw.Value, len(table.Columns))
						if err != nil || vrow.Version.DeletedAt > 0 {
							continue
						}
						existingRow := vrow.Data
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
					break
				}
				vrow, err := decodeVersionedRow(existingData, len(table.Columns))
				if err != nil {
					continue
				}
				if vrow.Version.DeletedAt > 0 {
					continue
				}
				existingRow := vrow.Data
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
				oldData, getErr := tree.Get(duplicateKey)
				if getErr == nil {
					oldRow, decErr := decodeRow(oldData, len(table.Columns))
					if decErr == nil {
						for idxName, idxTree := range c.indexTrees {
							idxDef := c.indexes[idxName]
							if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
								oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
								if ok {
									var delErr error
									if idxDef.Unique {
										delErr = idxTree.Delete([]byte(oldIdxKey))
									} else {
										compoundKey := oldIdxKey + "\x00" + string(duplicateKey)
										delErr = idxTree.Delete([]byte(compoundKey))
									}
									if delErr != nil {
										return false, fmt.Errorf("failed to delete from index %s: %w", idxName, delErr)
									}
								}
							}
						}
					}
				}
				if delErr := tree.Delete(duplicateKey); delErr != nil {
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
func (c *Catalog) checkInsertConstraints(table *TableDef, rowValues []interface{}, args []interface{}) error {
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
	return nil
}

// checkForeignKeyConstraints validates FOREIGN KEY references for a row.
// NULL values skip FK checking per SQL standard.
func (c *Catalog) checkForeignKeyConstraints(table *TableDef, rowValues []interface{}, ts *catalogTxnState) error {
	for _, fk := range table.ForeignKeys {
		for i, colName := range fk.Columns {
			colIdx := table.GetColumnIndex(colName)
			if colIdx < 0 || colIdx >= len(rowValues) {
				continue
			}
			fkValue := rowValues[colIdx]
			if fkValue == nil {
				continue
			}

			refTable, err := c.getTableLocked(fk.ReferencedTable)
			if err != nil {
				return fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
			}

			refColIdx := 0
			if len(fk.ReferencedColumns) > i {
				refColIdx = refTable.GetColumnIndex(fk.ReferencedColumns[i])
			} else if len(refTable.Columns) > 0 {
				refColIdx = 0
			}

			refTree, exists := c.tableTrees[fk.ReferencedTable]
			if !exists {
				return fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
			}

			found := false
			refIter, err := refTree.Scan(nil, nil)
			if err != nil {
				return fmt.Errorf("FOREIGN KEY constraint failed: %w", err)
			}
			for refIter.HasNext() {
				_, refData, err := refIter.Next()
				if err != nil {
					break
				}
				vrow, err := decodeVersionedRow(refData, len(refTable.Columns))
				if err != nil {
					continue
				}
				refRow := vrow.Data
				if refColIdx < len(refRow) && compareValues(fkValue, refRow[refColIdx]) == 0 {
					found = true
					break
				}
			}
			refIter.Close()

			// Also check pending writes in the current transaction for
			// self-referential or same-statement FK references.
			if !found && ts != nil {
				if m, ok := ts.getPendingWriteMap()[fk.ReferencedTable]; ok {
					for _, pw := range m {
						vrow, err := decodeVersionedRow(pw.Value, len(refTable.Columns))
						if err != nil || vrow.Version.DeletedAt > 0 {
							continue
						}
						refRow := vrow.Data
						if refColIdx < len(refRow) && compareValues(fkValue, refRow[refColIdx]) == 0 {
							found = true
							break
						}
					}
				}
			}

			if !found {
				return fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValue, fk.ReferencedTable)
			}
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
func (c *Catalog) rollbackStatementInserts(tree btree.TreeStore, table *TableDef, stmtInserts []stmtInsertEntry, savedAutoIncSeq int64) {
	for i := len(stmtInserts) - 1; i >= 0; i-- {
		si := stmtInserts[i]
		_ = tree.Delete(si.key)
		for _, ik := range si.idxKeys {
			if idxTree, exists := c.indexTrees[ik.idxName]; exists {
				_ = idxTree.Delete(ik.key)
			}
		}
	}
	atomic.StoreInt64(&table.AutoIncSeq, savedAutoIncSeq)
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
