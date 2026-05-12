package catalog

import (
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// deleteEntry tracks a row to be deleted
type deleteEntry struct {
	key      []byte
	value    []byte
	row      []interface{} // decoded row for RETURNING clause
	treeName string        // which partition tree this entry came from
}

func (c *Catalog) Delete(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
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
		// Don't return ErrTableNotFound here — deleteLocked may need to check
		// for INSTEAD OF triggers on views.
		if table != nil {
			ver = c.schemaVersion.Load()
			c.putCachedTable(stmt.Table, table)
		}
		c.mu.RUnlock()
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if table != nil && c.schemaVersion.Load() != ver {
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil && err != ErrTableNotFound {
			return 0, 0, err
		}
	}
	return c.deleteLocked(ctx, stmt, args, table)
}

func (c *Catalog) deleteLocked(ctx context.Context, stmt *query.DeleteStmt, args []interface{}, tableArg ...*TableDef) (int64, int64, error) {
	// Check for INSTEAD OF DELETE trigger first (for views)
	if trig := c.findInsteadOfTrigger(stmt.Table, "DELETE"); trig != nil {
		return c.executeInsteadOfDeleteTrigger(ctx, trig, stmt, args)
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
		return 0, 0, fmt.Errorf("cannot delete from foreign table '%s'", stmt.Table)
	}

	// Handle DELETE with USING (JOIN)
	if len(stmt.Using) > 0 {
		return c.deleteWithUsingLocked(ctx, stmt, args)
	}

	// Determine if we can use buffered writes for this delete.
	useBuffer := c.isBufferedMode() && table.Partition == nil

	// Get all trees for scanning (handles partitioned tables)
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		return 0, 0, err
	}

	// Collect keys and row data to delete (need row data for index cleanup)
	var entries []deleteEntry
	rowsAffected := int64(0)

	// Scan all partition trees to collect entries to delete
	// Get tree names for later lookup during deletion
	treeNames := []string{stmt.Table}
	if table.Partition != nil {
		treeNames = table.getPartitionTreeNames()
	}

	// Check if we can use an index for the WHERE clause
	var indexedRows map[string]bool
	useIndex := false
	if stmt.Where != nil {
		indexedRows, useIndex = c.useIndexForQueryWithArgs(stmt.Table, stmt.Where, args)
	}

	for i, tree := range trees {
		treeName := treeNames[i]

		// Collect pending writes for this tree to overlay in scan.
		var pendingKeys map[string]PendingWrite
		if ts := c.getCurrentTxn(); ts != nil {
			pendingKeys = ts.getPendingWriteMap()[treeName]
		}

		// If we have indexed rows, only process those specific rows
		if useIndex {
			for pkStr := range indexedRows {
				key := []byte(pkStr)
				valueData, err := tree.Get(key)
				found := err == nil && valueData != nil
				// Overlay pending write
				if pwValue, ok := pendingKeys[pkStr]; ok {
					valueData = pwValue.Value
					found = true
				} else if found && useBuffer {
					c.recordManagerRead(treeName, pkStr, valueData)
				}
				if !found {
					continue
				}
				// Process this row
				if err := c.processDeleteRow(ctx, table, tree, treeName, key, valueData, stmt, args, &entries, &rowsAffected); err != nil {
					return 0, rowsAffected, err
				}
			}
			continue
		}

		// Full table scan path
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to scan table for DELETE: %w", err)
		}
		seenPending := make(map[string]bool)
		for iter.HasNext() {
			k, valueData, err := iter.NextString()
			if err != nil {
				break
			}
			fromPending := false
			if pwValue, ok := pendingKeys[k]; ok {
				valueData = pwValue.Value
				seenPending[k] = true
				fromPending = true
			}

			// Decode row with version info
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			row := vrow.Data

			// Skip already deleted rows
			if vrow.Version.DeletedAt > 0 {
				continue
			}

			// Apply WHERE clause if present
			if stmt.Where != nil {
				matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
				if err != nil {
					iter.Close()
					return 0, rowsAffected, fmt.Errorf("WHERE evaluation error: %w", err)
				}
				if !matched {
					continue // Skip row that doesn't match WHERE condition
				}
			}

			// Apply Row-Level Security check for DELETE
			if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, table.Columns, row, security.PolicyDelete); rlsErr != nil || !allowed {
				continue
			}

			if !fromPending && useBuffer {
				c.recordManagerRead(treeName, k, valueData)
			}

			// Make copies of key and value since iterator may reuse buffers
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			valueCopy := make([]byte, len(valueData))
			copy(valueCopy, valueData)

			entries = append(entries, deleteEntry{key: keyCopy, value: valueCopy, row: row, treeName: treeName})
			rowsAffected++
		}
		iter.Close()

		// Process pending inserts/updates not present in B-tree
		var pendingKeyList []string
		for k := range pendingKeys {
			if !seenPending[k] {
				pendingKeyList = append(pendingKeyList, k)
			}
		}
		sort.Strings(pendingKeyList)
		for _, k := range pendingKeyList {
			key := []byte(k)
			valueData := pendingKeys[k].Value
			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil || vrow.Version.DeletedAt > 0 {
				continue
			}
			row := vrow.Data
			if stmt.Where != nil {
				matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
				if err != nil {
					return 0, rowsAffected, fmt.Errorf("WHERE evaluation error: %w", err)
				}
				if !matched {
					continue
				}
			}
			// Apply Row-Level Security check for DELETE
			if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, table.Columns, row, security.PolicyDelete); rlsErr != nil || !allowed {
				continue
			}
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			valueCopy := make([]byte, len(valueData))
			copy(valueCopy, valueData)
			entries = append(entries, deleteEntry{key: keyCopy, value: valueCopy, row: row, treeName: treeName})
			rowsAffected++
		}
	}

	// Track pending-write start position for statement-level rollback in buffered mode.
	pendingWriteStartPos := 0
	if ts := c.getCurrentTxn(); ts != nil {
		pendingWriteStartPos = len(ts.pendingWrites)
	}

	// Apply soft deletes (FK enforcement, index cleanup, WAL, triggers)
	if useBuffer {
		if err := c.bufferDeleteEntries(ctx, table, stmt, entries); err != nil {
			if ts := c.getCurrentTxn(); ts != nil {
				ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
			}
			return 0, rowsAffected, err
		}
	} else {
		if _, applyErr := c.applyDeleteEntries(ctx, table, stmt, entries); applyErr != nil {
			return 0, rowsAffected, applyErr
		}
	}

	// Invalidate query cache for the affected table
	c.invalidateQueryCache(stmt.Table)

	// Handle RETURNING clause (returns OLD row values)
	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.row, table, args)
			if err != nil {
				return 0, rowsAffected, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	// Store returning rows for retrieval
	c.setLastReturning(returningRows, returningCols)

	return 0, rowsAffected, nil
}

func (c *Catalog) DeleteRow(ctx context.Context, tableName string, pkValue interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteRowLocked(ctx, tableName, pkValue)
}

// processDeleteRow processes a single row deletion from index lookup
func (c *Catalog) processDeleteRow(ctx context.Context, table *TableDef, tree btree.TreeStore, treeName string, key []byte, valueData []byte,
	stmt *query.DeleteStmt, args []interface{}, entries *[]deleteEntry, rowsAffected *int64) error {

	// Decode row with version info
	vrow, err := decodeVersionedRow(valueData, len(table.Columns))
	if err != nil {
		return nil // Skip unparseable rows
	}
	row := vrow.Data

	// Skip already deleted rows
	if vrow.Version.DeletedAt > 0 {
		return nil
	}

	// Apply WHERE clause to filter (in case of partial index match)
	if stmt.Where != nil {
		matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
		if err != nil || !matched {
			return nil
		}
	}

		// Apply Row-Level Security check for DELETE
		if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, table.Columns, row, security.PolicyDelete); rlsErr != nil || !allowed {
			return nil
		}

	// Make copies of key and value
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(valueData))
	copy(valueCopy, valueData)

	*entries = append(*entries, deleteEntry{key: keyCopy, value: valueCopy, row: row, treeName: treeName})
	*rowsAffected++
	return nil
}

// deleteRowLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) deleteRowLocked(ctx context.Context, tableName string, pkValue interface{}) error {
	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Get old value for undo log and index cleanup
	oldData, err := tree.Get(key)
	if err != nil {
		return nil // Key doesn't exist, nothing to delete
	}

	table := c.tables[tableName]

	// Clean up index entries
	var idxChanges []indexUndoEntry
	if table != nil {
		vrow, decErr := decodeVersionedRow(oldData, len(table.Columns))
		if decErr == nil {
			oldRow := vrow.Data
			for idxName, idxTree := range c.indexTrees {
				idxDef := c.indexes[idxName]
				if idxDef != nil && idxDef.TableName == tableName && len(idxDef.Columns) > 0 {
					oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
					if ok {
						if idxDef.Unique {
							if c.isCurrentTxnActive() {
								// Save old index value for undo
								oldIdxVal, _ := idxTree.Get([]byte(oldIdxKey))
								idxChanges = append(idxChanges, indexUndoEntry{
									indexName: idxName,
									key:       []byte(oldIdxKey),
									oldValue:  oldIdxVal,
									wasAdded:  false,
								})
							}
							_ = idxTree.Delete([]byte(oldIdxKey)) // best-effort index cleanup during DELETE
						} else {
							// For non-unique indexes, delete the compound key "indexValue\x00pk"
							compoundKey := oldIdxKey + "\x00" + string(key)
							if c.isCurrentTxnActive() {
								// Save old index value for undo
								oldIdxVal, _ := idxTree.Get([]byte(compoundKey))
								idxChanges = append(idxChanges, indexUndoEntry{
									indexName: idxName,
									key:       []byte(compoundKey),
									oldValue:  oldIdxVal,
									wasAdded:  false,
								})
							}
							_ = idxTree.Delete([]byte(compoundKey)) // best-effort index cleanup during DELETE
						}
					}
				}
			}
		}
	}

	// Cascade FK enforcement before deleting
	if table != nil && len(table.ForeignKeys) > 0 {
		fke := NewForeignKeyEnforcer(c)
		oldRow, decErr := decodeRow(oldData, len(table.Columns))
		if decErr == nil {
			pkValues := make([]interface{}, 0, len(table.PrimaryKey))
			for _, pkCol := range table.PrimaryKey {
				pkColIdx := table.GetColumnIndex(pkCol)
				if pkColIdx >= 0 && pkColIdx < len(oldRow) && oldRow[pkColIdx] != nil {
					pkValues = append(pkValues, oldRow[pkColIdx])
				}
			}
			if len(pkValues) == len(table.PrimaryKey) && len(pkValues) > 0 {
				if fkErr := fke.OnDelete(ctx, tableName, pkValues...); fkErr != nil {
					return fmt.Errorf("cascade delete: %w", fkErr)
				}
			}
		}
	}

	// Decode row with version info
	vrow, err := decodeVersionedRow(oldData, len(table.Columns))
	if err != nil {
		// If we can't decode, fall back to physical delete
		return tree.Delete(key)
	}

	// Record undo log entry for rollback
	if c.isCurrentTxnActive() {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		oldCopy := make([]byte, len(oldData))
		copy(oldCopy, oldData)
		c.appendUndoEntry(undoEntry{
			action:       undoDelete,
			tableName:    tableName,
			key:          keyCopy,
			oldValue:     oldCopy,
			indexChanges: idxChanges,
		})
	}

	// Soft delete: mark row as deleted instead of physically deleting
	vrow.Version.markDeleted(time.Now())

	// Re-encode and store the soft-deleted row
	deletedValueData, err := json.Marshal(vrow)
	if err != nil {
		return fmt.Errorf("failed to encode deleted row: %w", err)
	}

	return tree.Put(key, deletedValueData)
}

// applyDeleteEntries performs soft deletion of collected entries: FK enforcement,
// index cleanup, WAL logging, undo tracking, and AFTER DELETE triggers.
// Returns the number of rows processed and any error encountered.
func (c *Catalog) applyDeleteEntries(ctx context.Context, table *TableDef, stmt *query.DeleteStmt, entries []deleteEntry) (int64, error) {
	// Foreign key enforcer for CASCADE/RESTRICT actions
	fke := NewForeignKeyEnforcer(c)

	var rowsAffected int64

	// Soft delete collected entries (mark as deleted for temporal versioning)
	for _, entry := range entries {
		key := entry.key

		// Decode row with version info
		vrow, err := decodeVersionedRow(entry.value, len(table.Columns))
		if err != nil {
			continue
		}
		row := vrow.Data

		// Enforce foreign key ON DELETE actions (CASCADE, SET NULL, RESTRICT)
		pkValues := make([]interface{}, 0, len(table.PrimaryKey))
		for _, pkCol := range table.PrimaryKey {
			pkColIdx := table.GetColumnIndex(pkCol)
			if pkColIdx >= 0 && pkColIdx < len(row) && row[pkColIdx] != nil {
				pkValues = append(pkValues, row[pkColIdx])
			}
		}
		if len(pkValues) == len(table.PrimaryKey) && len(pkValues) > 0 {
			if fkErr := fke.OnDelete(ctx, stmt.Table, pkValues...); fkErr != nil {
				return rowsAffected, fmt.Errorf("foreign key constraint: %w", fkErr)
			}
		}

		// Remove from indexes first (before soft deleting the row), track for undo
		var idxChanges []indexUndoEntry
		if err == nil {
			for idxName, idxTree := range c.indexTrees {
				idxDef := c.indexes[idxName]
				if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
					indexKey, ok := buildCompositeIndexKey(table, idxDef, row)
					if ok {
						if idxDef.Unique {
							oldIdxVal, getErr := idxTree.Get([]byte(indexKey))
							if delErr := idxTree.Delete([]byte(indexKey)); delErr != nil {
								return rowsAffected, fmt.Errorf("failed to delete from unique index %s: %w", idxName, delErr)
							}
							if c.isCurrentTxnActive() && getErr == nil {
								idxChanges = append(idxChanges, indexUndoEntry{
									indexName: idxName,
									key:       []byte(indexKey),
									oldValue:  oldIdxVal,
									wasAdded:  false, // was deleted
								})
							}
						} else {
							// For non-unique indexes, delete the compound key "indexValue\x00pk"
							compoundKey := indexKey + "\x00" + string(key)
							oldIdxVal, getErr := idxTree.Get([]byte(compoundKey))
							if delErr := idxTree.Delete([]byte(compoundKey)); delErr != nil {
								return rowsAffected, fmt.Errorf("failed to delete from index %s: %w", idxName, delErr)
							}
							if c.isCurrentTxnActive() && getErr == nil {
								idxChanges = append(idxChanges, indexUndoEntry{
									indexName: idxName,
									key:       []byte(compoundKey),
									oldValue:  oldIdxVal,
									wasAdded:  false, // was deleted
								})
							}
						}
					}
				}
			}
		}

		// Update vector indexes - delete the row from vector indexes
		c.updateVectorIndexesForDelete(stmt.Table, string(key))

		// Log to WAL before applying change
		if c.wal != nil && c.isCurrentTxnActive() {
			walData := append([]byte(key), 0)
			record := &storage.WALRecord{
				TxnID: c.getCurrentTxnID(),
				Type:  storage.WALDelete,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return rowsAffected, err
			}
		}

		// Record undo log entry for rollback
		if c.isCurrentTxnActive() {
			keyCopy2 := make([]byte, len(key))
			copy(keyCopy2, key)
			valueCopy2 := make([]byte, len(entry.value))
			copy(valueCopy2, entry.value)
			c.appendUndoEntry(undoEntry{
				action:       undoDelete,
				tableName:    stmt.Table,
				key:          keyCopy2,
				oldValue:     valueCopy2,
				indexChanges: idxChanges,
			})
		}

		// Soft delete: mark row as deleted instead of physically deleting
		deleteTree, exists := c.tableTrees[entry.treeName]
		if !exists {
			return rowsAffected, fmt.Errorf("partition tree %s not found", entry.treeName)
		}

		// Mark as deleted with current timestamp
		vrow.Version.markDeleted(time.Now())

		// Re-encode and store the soft-deleted row
		deletedValueData, err := json.Marshal(vrow)
		if err != nil {
			return rowsAffected, fmt.Errorf("failed to encode deleted row: %w", err)
		}

		if err := deleteTree.Put(key, deletedValueData); err != nil {
			return rowsAffected, fmt.Errorf("failed to soft delete row: %w", err)
		}

		// Execute AFTER DELETE trigger per-row
		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, row, table.Columns); trigErr != nil {
			return rowsAffected, fmt.Errorf("AFTER DELETE trigger failed: %w", trigErr)
		}

		// Track dead tuple for AutoVacuum
		c.ensureVacuumMaps()
		c.vacuumMu.Lock()
		c.deadTuples[entry.treeName]++
		c.vacuumMu.Unlock()

		rowsAffected++
	}
	return rowsAffected, nil
}

// bufferDeleteEntries buffers soft-deleted rows and their index mutations for
// commit-time application in MVCC buffered mode.
func (c *Catalog) bufferDeleteEntries(ctx context.Context, table *TableDef, stmt *query.DeleteStmt, entries []deleteEntry) error {
	fke := NewForeignKeyEnforcer(c)
	for _, entry := range entries {
		key := entry.key
		row := entry.row

		// Enforce foreign key ON DELETE actions (same as direct path).
		pkValues := make([]interface{}, 0, len(table.PrimaryKey))
		for _, pkCol := range table.PrimaryKey {
			pkColIdx := table.GetColumnIndex(pkCol)
			if pkColIdx >= 0 && pkColIdx < len(row) && row[pkColIdx] != nil {
				pkValues = append(pkValues, row[pkColIdx])
			}
		}
		if len(pkValues) == len(table.PrimaryKey) && len(pkValues) > 0 {
			if fkErr := fke.OnDelete(ctx, stmt.Table, pkValues...); fkErr != nil {
				return fmt.Errorf("foreign key constraint: %w", fkErr)
			}
		}

		// Soft-delete encoding.
		vrow, err := decodeVersionedRow(entry.value, len(table.Columns))
		if err != nil {
			continue
		}
		vrow.Version.markDeleted(time.Now())
		deletedValueData, err := json.Marshal(vrow)
		if err != nil {
			return fmt.Errorf("failed to encode deleted row: %w", err)
		}

		var idxUpdates []PendingIndexUpdate
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName != stmt.Table || len(idxDef.Columns) == 0 {
				continue
			}
			indexKey, ok := buildCompositeIndexKey(table, idxDef, entry.row)
			if !ok || indexKey == "" {
				continue
			}
			var idxStorageKey []byte
			if idxDef.Unique {
				idxStorageKey = []byte(indexKey)
			} else {
				idxStorageKey = []byte(indexKey + "\x00" + string(key))
			}
			idxUpdates = append(idxUpdates, PendingIndexUpdate{
				IndexName: idxName,
				Key:       string(idxStorageKey),
				IsDelete:  true,
			})
		}

		c.appendPendingWrite(PendingWrite{
			TreeName:     entry.treeName,
			Key:          string(key),
			Value:        deletedValueData,
			IndexUpdates: idxUpdates,
		})
		if mt, ok := c.getCurrentManagerTxn().(*txn.Transaction); ok && mt != nil {
			mt.SetWrite(entry.treeName, string(key), deletedValueData)
		}

		// Execute AFTER DELETE trigger per-row.
		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, row, table.Columns); trigErr != nil {
			return fmt.Errorf("AFTER DELETE trigger failed: %w", trigErr)
		}
	}
	return nil
}
