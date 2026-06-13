package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/security"
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

// deleteSnapshot holds all Catalog metadata needed for the buffered DELETE scan.
// It is built under a brief Catalog.mu RLock and then used without the lock.
type deleteSnapshot struct {
	table       *TableDef
	trees       []btree.TreeStore
	treeNames   []string
	indexedRows []string
	useIndex    bool
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
	if table != nil && c.schemaVersion.Load() != ver {
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil && err != ErrTableNotFound {
			c.mu.RUnlock()
			return 0, 0, err
		}
	}

	// Check for INSTEAD OF DELETE trigger first (for views)
	if trig := c.findInsteadOfTrigger(stmt.Table, "DELETE"); trig != nil {
		defer c.mu.RUnlock()
		return c.executeInsteadOfDeleteTrigger(ctx, trig, stmt, args)
	}

	if table == nil {
		var err error
		table, err = c.getTableLocked(stmt.Table)
		if err != nil {
			c.mu.RUnlock()
			return 0, 0, err
		}
	}
	if table.Type == "foreign" {
		c.mu.RUnlock()
		return 0, 0, fmt.Errorf("cannot delete from foreign table '%s'", stmt.Table)
	}

	// Handle DELETE with USING/JOIN or target alias. The USING path models
	// the target table as a SELECT source, which lets qualified alias
	// references resolve in WHERE.
	if stmt.Alias != "" || len(stmt.Using) > 0 {
		defer c.mu.RUnlock()
		return c.deleteWithUsingLocked(ctx, stmt, args)
	}

	useBuffer := c.isBufferedMode() && table.Partition == nil
	ts := c.getCurrentTxn()

	if !useBuffer {
		defer c.mu.RUnlock()
		return c.deleteLocked(ctx, stmt, args, table)
	}

	// Buffered path: snapshot and release lock for scan.
	snap, err := c.buildDeleteSnapshot(table, stmt, args)
	if err != nil {
		c.mu.RUnlock()
		return 0, 0, err
	}
	c.mu.RUnlock()

	entries, rowsAffected, err := c.scanDeleteEntries(ctx, stmt, args, snap, ts)
	if err != nil {
		return 0, rowsAffected, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	pendingWriteStartPos := 0
	if ts != nil {
		pendingWriteStartPos = len(ts.pendingWrites)
	}

	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.row, table, args)
			if err != nil {
				if ts != nil {
					ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
					rebuildPendingWriteMap(ts)
				}
				return 0, rowsAffected, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	if err := c.bufferDeleteEntries(ctx, table, stmt, entries, ts); err != nil {
		if ts != nil {
			ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
			rebuildPendingWriteMap(ts)
		}
		return 0, rowsAffected, err
	}

	c.invalidateQueryCache(stmt.Table)

	c.setLastReturning(returningRows, returningCols)

	return 0, rowsAffected, nil
}

func (c *Catalog) buildDeleteSnapshot(table *TableDef, stmt *query.DeleteStmt, args []interface{}) (*deleteSnapshot, error) {
	snap := &deleteSnapshot{table: table}
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		return nil, err
	}
	snap.trees = trees
	snap.treeNames = []string{stmt.Table}
	if table.Partition != nil {
		snap.treeNames = table.getPartitionTreeNames()
	}
	if stmt.Where != nil {
		indexedRows, useIndex, err := c.useIndexForQueryWithArgs(stmt.Table, stmt.Where, args)
		if err != nil {
			return nil, err
		}
		snap.indexedRows, snap.useIndex = indexedRows, useIndex
	}
	return snap, nil
}

func (c *Catalog) scanDeleteEntries(ctx context.Context, stmt *query.DeleteStmt, args []interface{}, snap *deleteSnapshot, ts *catalogTxnState) ([]deleteEntry, int64, error) {
	table := snap.table
	trees := snap.trees
	treeNames := snap.treeNames
	indexedRows := snap.indexedRows
	useIndex := snap.useIndex
	useBuffer := c.isBufferedMode() && table.Partition == nil

	var entries []deleteEntry
	rowsAffected := int64(0)

	for i, tree := range trees {
		treeName := treeNames[i]

		var pendingKeys map[string]PendingWrite
		if ts != nil {
			pendingKeys = ts.getPendingWriteMap()[treeName]
		}

		if useIndex {
			for _, pkStr := range indexedRows {
				key := []byte(pkStr)
				valueData, err := tree.Get(key)
				found := err == nil && valueData != nil
				if pwValue, ok := pendingKeys[pkStr]; ok {
					valueData = pwValue.Value
					found = true
				} else if found && useBuffer {
					c.recordManagerReadTs(ts, treeName, pkStr, valueData)
				}
				if !found {
					continue
				}
				if err := c.processDeleteRow(ctx, table, tree, treeName, key, valueData, stmt, args, &entries, &rowsAffected); err != nil {
					return entries, rowsAffected, err
				}
			}
			continue
		}

		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return entries, 0, fmt.Errorf("failed to scan table for DELETE: %w", err)
		}
		seenPending := make(map[string]bool)
		for iter.HasNext() {
			k, valueData, err := iter.NextString()
			if err != nil {
				iter.Close()
				return entries, rowsAffected, fmt.Errorf("failed to read table for DELETE: %w", err)
			}
			fromPending := false
			if pwValue, ok := pendingKeys[k]; ok {
				valueData = pwValue.Value
				seenPending[k] = true
				fromPending = true
			}

			vrow, err := decodeVersionedRow(valueData, len(table.Columns))
			if err != nil {
				iter.Close()
				return entries, rowsAffected, fmt.Errorf("delete: failed to decode row in table %s: %w", table.Name, err)
			}
			row := vrow.Data

			if vrow.Version.DeletedAt > 0 {
				continue
			}

			if stmt.Where != nil {
				matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
				if err != nil {
					iter.Close()
					return entries, rowsAffected, fmt.Errorf("WHERE evaluation error: %w", err)
				}
				if !matched {
					continue
				}
			}

			if allowed, rlsErr := c.checkRowAccessLocked(ctx, stmt.Table, table.Columns, row, security.PolicyDelete); rlsErr != nil || !allowed {
				continue
			}

			if !fromPending && useBuffer {
				c.recordManagerReadTs(ts, treeName, k, valueData)
			}

			keyCopy := make([]byte, len(k))
			copy(keyCopy, []byte(k))
			valueCopy := make([]byte, len(valueData))
			copy(valueCopy, valueData)

			entries = append(entries, deleteEntry{key: keyCopy, value: valueCopy, row: row, treeName: treeName})
			rowsAffected++
		}
		iter.Close()

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
			if err != nil {
				return entries, rowsAffected, fmt.Errorf("delete: failed to decode pending row in table %s: %w", table.Name, err)
			}
			if vrow.Version.DeletedAt > 0 {
				continue
			}
			row := vrow.Data
			if stmt.Where != nil {
				matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
				if err != nil {
					return entries, rowsAffected, fmt.Errorf("WHERE evaluation error: %w", err)
				}
				if !matched {
					continue
				}
			}
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

	return entries, rowsAffected, nil
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

	// Handle DELETE with USING/JOIN or target alias. The USING path models
	// the target table as a SELECT source, which lets qualified alias
	// references resolve in WHERE.
	if stmt.Alias != "" || len(stmt.Using) > 0 {
		return c.deleteWithUsingLocked(ctx, stmt, args)
	}

	// Determine if we can use buffered writes for this delete.
	useBuffer := c.isBufferedMode() && table.Partition == nil

	// Cache transaction state to avoid repeated goroutine-shard lookups.
	ts := c.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive

	// Get all trees for scanning (handles partitioned tables)
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		return 0, 0, err
	}

	// Scan all partition trees to collect entries to delete
	// Get tree names for later lookup during deletion
	treeNames := []string{stmt.Table}
	if table.Partition != nil {
		treeNames = table.getPartitionTreeNames()
	}

	snap := &deleteSnapshot{
		table:     table,
		trees:     trees,
		treeNames: treeNames,
	}
	if stmt.Where != nil {
		indexedRows, useIndex, err := c.useIndexForQueryWithArgs(stmt.Table, stmt.Where, args)
		if err != nil {
			return 0, 0, err
		}
		snap.indexedRows, snap.useIndex = indexedRows, useIndex
	}
	entries, rowsAffected, err := c.scanDeleteEntries(ctx, stmt, args, snap, ts)
	if err != nil {
		return 0, rowsAffected, err
	}

	// Track pending-write start position for statement-level rollback in buffered mode.
	pendingWriteStartPos := 0
	if ts != nil {
		pendingWriteStartPos = len(ts.pendingWrites)
	}

	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.row, table, args)
			if err != nil {
				if ts != nil {
					ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
					rebuildPendingWriteMap(ts)
				}
				return 0, rowsAffected, fmt.Errorf("RETURNING clause failed: %w", err)
			}
			returningRows = append(returningRows, returningRow)
			if returningCols == nil {
				returningCols = cols
			}
		}
	}

	// Apply soft deletes (FK enforcement, index cleanup, WAL, triggers)
	if useBuffer {
		if err := c.bufferDeleteEntries(ctx, table, stmt, entries, ts); err != nil {
			if ts != nil {
				ts.pendingWrites = ts.pendingWrites[:pendingWriteStartPos]
				rebuildPendingWriteMap(ts)
			}
			return 0, rowsAffected, err
		}
	} else {
		if _, applyErr := c.applyDeleteEntries(ctx, table, stmt, entries, ts, txnActive); applyErr != nil {
			if rbErr := c.rollbackAppliedDeleteEntries(stmt.Table, entries); rbErr != nil {
				return 0, rowsAffected, fmt.Errorf("%w; rollback failed: %v", applyErr, rbErr)
			}
			return 0, rowsAffected, applyErr
		}
	}

	// Invalidate query cache for the affected table
	c.invalidateQueryCache(stmt.Table)

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
		return fmt.Errorf("delete: failed to decode row in table %s: %w", table.Name, err)
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

	// Cache transaction state to avoid repeated goroutine-shard lookups.
	ts := c.getCurrentTxn()
	txnActive := ts != nil && ts.txnActive

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Get old value for undo log and index cleanup
	oldData, err := tree.Get(key)
	if err != nil {
		return nil // Key doesn't exist, nothing to delete
	}

	table := c.tables[tableName]

	var oldRow []interface{}
	if table != nil {
		vrow, decErr := decodeVersionedRow(oldData, len(table.Columns))
		if decErr == nil {
			oldRow = vrow.Data
		}
	}

	// Cascade FK enforcement before deleting indexes or the row.
	if table != nil && oldRow != nil {
		fke := NewForeignKeyEnforcer(c)
		if fkErr := fke.OnDeleteRow(ctx, tableName, oldRow); fkErr != nil {
			return fmt.Errorf("cascade delete: %w", fkErr)
		}
	}

	// Clean up index entries
	var idxChanges []indexUndoEntry
	var deletedIndexEntries []deletedIndexEntry
	if table != nil && oldRow != nil {
		for idxName, idxTree := range c.indexTrees {
			idxDef := c.indexes[idxName]
			if idxDef != nil && idxDef.TableName == tableName && len(idxDef.Columns) > 0 {
				deletedEntry, err := deleteIndexEntryForRowTracked(idxName, table, idxDef, idxTree, oldRow, key)
				if err != nil {
					if rbErr := c.rebuildTableIndexesLocked(tableName); rbErr != nil {
						return fmt.Errorf("failed to delete from index %s: %w; index rebuild failed: %v", idxName, err, rbErr)
					}
					return fmt.Errorf("failed to delete from index %s: %w", idxName, err)
				}
				if deletedEntry != nil {
					deletedIndexEntries = append(deletedIndexEntries, *deletedEntry)
					if txnActive {
						idxChanges = append(idxChanges, indexUndoEntry{
							indexName: idxName,
							key:       append([]byte(nil), deletedEntry.key...),
							oldValue:  append([]byte(nil), deletedEntry.value...),
							wasAdded:  false,
						})
					}
				}
			}
		}
	}

	// Decode row with version info
	vrow, err := decodeVersionedRow(oldData, len(table.Columns))
	if err != nil {
		// If we can't decode, fall back to physical delete
		if err := tree.Delete(key); err != nil {
			if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
				return fmt.Errorf("failed to delete row: %w; failed to restore deleted index entries: %v", err, restoreErr)
			}
			return err
		}
		return nil
	}

	// Soft delete: mark row as deleted instead of physically deleting
	vrow.Version.markDeleted(time.Now())

	// Re-encode and store the soft-deleted row
	deletedValueData, err := json.Marshal(vrow)
	if err != nil {
		if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
			return fmt.Errorf("failed to encode deleted row: %w; failed to restore deleted index entries: %v", err, restoreErr)
		}
		return fmt.Errorf("failed to encode deleted row: %w", err)
	}

	if err := tree.Put(key, deletedValueData); err != nil {
		if restoreErr := restoreDeletedIndexEntries(deletedIndexEntries); restoreErr != nil {
			return fmt.Errorf("failed to soft delete row: %w; failed to restore deleted index entries: %v", err, restoreErr)
		}
		return err
	}

	// Record undo log entry for rollback only after the delete is durable in the tree.
	if txnActive {
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

	return nil
}

// applyDeleteEntries performs soft deletion of collected entries: FK enforcement,
// index cleanup, WAL logging, undo tracking, and AFTER DELETE triggers.
// Returns the number of rows processed and any error encountered.
func (c *Catalog) applyDeleteEntries(ctx context.Context, table *TableDef, stmt *query.DeleteStmt, entries []deleteEntry, ts *catalogTxnState, txnActive bool) (int64, error) {
	// Foreign key enforcer for CASCADE/RESTRICT actions
	fke := NewForeignKeyEnforcer(c)
	undoStart := 0
	if txnActive {
		undoStart = len(c.getCurrentTxnUndoLog())
	}
	rollbackFKActions := func(cause error) error {
		if rbErr := fke.rollbackAppliedDeletes(); rbErr != nil {
			return fmt.Errorf("%w; rollback failed: %v", cause, rbErr)
		}
		if rbErr := fke.rollbackAppliedUpdates(); rbErr != nil {
			return fmt.Errorf("%w; rollback failed: %v", cause, rbErr)
		}
		if txnActive {
			c.truncateUndoLog(undoStart)
		}
		return cause
	}

	var rowsAffected int64
	untrackReferenceRows := fke.trackDeletingReferenceRows(stmt.Table, entries)
	defer untrackReferenceRows()

	// Soft delete collected entries (mark as deleted for temporal versioning)
	for _, entry := range entries {
		key := entry.key

		// Decode row with version info
		vrow, err := decodeVersionedRow(entry.value, len(table.Columns))
		if err != nil {
			return rowsAffected, fmt.Errorf("delete: failed to decode row in table %s: %w", table.Name, err)
		}
		row := vrow.Data

		// Enforce foreign key ON DELETE actions (CASCADE, SET NULL, RESTRICT)
		if fkErr := fke.OnDeleteRow(ctx, stmt.Table, row); fkErr != nil {
			return rowsAffected, rollbackFKActions(fmt.Errorf("foreign key constraint: %w", fkErr))
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
								return rowsAffected, rollbackFKActions(fmt.Errorf("failed to delete from unique index %s: %w", idxName, delErr))
							}
							if txnActive && getErr == nil {
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
								return rowsAffected, rollbackFKActions(fmt.Errorf("failed to delete from index %s: %w", idxName, delErr))
							}
							if txnActive && getErr == nil {
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
		if err := c.updateVectorIndexesForDelete(stmt.Table, string(key)); err != nil {
			return rowsAffected, rollbackFKActions(err)
		}

		// Log to WAL before applying change
		if c.wal != nil && txnActive {
			walData, err := encodeLogicalWALData(stmt.Table, key, nil)
			if err != nil {
				return rowsAffected, rollbackFKActions(err)
			}
			record := &storage.WALRecord{
				TxnID: ts.txnID,
				Type:  storage.WALDelete,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return rowsAffected, rollbackFKActions(err)
			}
		}

		// Record undo log entry for rollback
		if txnActive {
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

		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "BEFORE", nil, row, table.Columns); trigErr != nil {
			return rowsAffected, rollbackFKActions(fmt.Errorf("BEFORE DELETE trigger failed: %w", trigErr))
		}

		// Soft delete: mark row as deleted instead of physically deleting
		deleteTree, exists := c.tableTrees[entry.treeName]
		if !exists {
			return rowsAffected, rollbackFKActions(fmt.Errorf("partition tree %s not found", entry.treeName))
		}

		// Mark as deleted with current timestamp
		vrow.Version.markDeleted(time.Now())

		// Re-encode and store the soft-deleted row
		deletedValueData, err := json.Marshal(vrow)
		if err != nil {
			return rowsAffected, rollbackFKActions(fmt.Errorf("failed to encode deleted row: %w", err))
		}

		if err := deleteTree.Put(key, deletedValueData); err != nil {
			return rowsAffected, rollbackFKActions(fmt.Errorf("failed to soft delete row: %w", err))
		}

		// Execute AFTER DELETE trigger per-row
		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, row, table.Columns); trigErr != nil {
			return rowsAffected, rollbackFKActions(fmt.Errorf("AFTER DELETE trigger failed: %w", trigErr))
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

func (c *Catalog) rollbackAppliedDeleteEntries(tableName string, entries []deleteEntry) error {
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		deleteTree, exists := c.tableTrees[entry.treeName]
		if !exists {
			return fmt.Errorf("partition tree %s not found", entry.treeName)
		}
		if err := deleteTree.Put(entry.key, entry.value); err != nil {
			return fmt.Errorf("restore deleted row: %w", err)
		}
	}
	return c.rebuildTableIndexesLocked(tableName)
}

// bufferDeleteEntries buffers soft-deleted rows and their index mutations for
// commit-time application in MVCC buffered mode.
func (c *Catalog) bufferDeleteEntries(ctx context.Context, table *TableDef, stmt *query.DeleteStmt, entries []deleteEntry, ts *catalogTxnState) error {
	fke := NewForeignKeyEnforcer(c)
	untrackReferenceRows := fke.trackDeletingReferenceRows(stmt.Table, entries)
	defer untrackReferenceRows()
	for _, entry := range entries {
		key := entry.key
		row := entry.row

		// Enforce foreign key ON DELETE actions (same as direct path).
		if fkErr := fke.OnDeleteRow(ctx, stmt.Table, row); fkErr != nil {
			return fmt.Errorf("foreign key constraint: %w", fkErr)
		}

		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "BEFORE", nil, row, table.Columns); trigErr != nil {
			return fmt.Errorf("BEFORE DELETE trigger failed: %w", trigErr)
		}

		if err := c.updateVectorIndexesForDelete(stmt.Table, string(key)); err != nil {
			return err
		}

		// Soft-delete encoding.
		vrow, err := decodeVersionedRow(entry.value, len(table.Columns))
		if err != nil {
			return fmt.Errorf("delete: failed to decode row in table %s: %w", table.Name, err)
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

		c.appendPendingWriteTs(ts, PendingWrite{
			TreeName:     entry.treeName,
			Key:          string(key),
			Value:        deletedValueData,
			IndexUpdates: idxUpdates,
		})
		if mt, ok := ts.managerTxn.(*txn.Transaction); ok && mt != nil {
			mt.SetWrite(entry.treeName, string(key), deletedValueData)
		}

		// Execute AFTER DELETE trigger per-row.
		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, row, table.Columns); trigErr != nil {
			return fmt.Errorf("AFTER DELETE trigger failed: %w", trigErr)
		}
	}
	return nil
}
