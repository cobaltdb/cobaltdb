package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// deleteEntry tracks a row to be deleted
type deleteEntry struct {
	key      []byte
	value    []byte
	row      []interface{} // decoded row for RETURNING clause
	treeName string        // which partition tree this entry came from
}

func (c *Catalog) Delete(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteLocked(ctx, stmt, args)
}

func (c *Catalog) deleteLocked(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	// Check for INSTEAD OF DELETE trigger first (for views)
	if trig := c.findInsteadOfTrigger(stmt.Table, "DELETE"); trig != nil {
		return c.executeInsteadOfDeleteTrigger(ctx, trig, stmt, args)
	}

	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	// Handle DELETE with USING (JOIN)
	if len(stmt.Using) > 0 {
		return c.deleteWithUsingLocked(ctx, stmt, args)
	}

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

		// If we have indexed rows, only process those specific rows
		if useIndex {
			for pkStr := range indexedRows {
				key := []byte(pkStr)
				valueData, err := tree.Get(key)
				if err != nil {
					continue // Row not found
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
		for iter.HasNext() {
			key, valueData, err := iter.Next()
			if err != nil {
				break
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
			if c.enableRLS && c.rlsManager != nil {
				user, _ := ctx.Value("cobaltdb_user").(string)
				roles, _ := ctx.Value("cobaltdb_roles").([]string)
				if user != "" {
					rowMap := make(map[string]interface{})
					for i, col := range table.Columns {
						if i < len(row) {
							rowMap[col.Name] = row[i]
						}
					}
					allowed, rlsErr := c.checkRLSForDeleteInternal(ctx, stmt.Table, rowMap, user, roles)
					if rlsErr != nil {
						continue // Skip rows that fail RLS check
					}
					if !allowed {
						continue // Skip rows the user doesn't have access to
					}
				}
			}

			// Make copies of key and value since iterator may reuse buffers
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			valueCopy := make([]byte, len(valueData))
			copy(valueCopy, valueData)

			entries = append(entries, deleteEntry{key: keyCopy, value: valueCopy, row: row, treeName: treeName})
			rowsAffected++
		}
		iter.Close()
	}

	// Foreign key enforcer for CASCADE/RESTRICT actions
	fke := NewForeignKeyEnforcer(c)

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
		// Extract primary key value from the row for FK lookup
		pkColIdx := -1
		if len(table.PrimaryKey) > 0 {
			pkColIdx = table.GetColumnIndex(table.PrimaryKey[0])
		}
		if pkColIdx >= 0 && pkColIdx < len(row) && row[pkColIdx] != nil {
			if fkErr := fke.OnDelete(ctx, stmt.Table, row[pkColIdx]); fkErr != nil {
				return 0, 0, fmt.Errorf("foreign key constraint: %w", fkErr)
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
								return 0, rowsAffected, fmt.Errorf("failed to delete from unique index %s: %w", idxName, delErr)
							}
							if c.txnActive && getErr == nil {
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
								return 0, rowsAffected, fmt.Errorf("failed to delete from index %s: %w", idxName, delErr)
							}
							if c.txnActive && getErr == nil {
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
		c.updateVectorIndexesForDelete(stmt.Table, key)

		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			walData := append([]byte(key), 0)
			record := &storage.WALRecord{
				TxnID: c.txnID,
				Type:  storage.WALDelete,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return 0, rowsAffected, err
			}
		}

		// Record undo log entry for rollback
		if c.txnActive {
			keyCopy2 := make([]byte, len(key))
			copy(keyCopy2, key)
			valueCopy2 := make([]byte, len(entry.value))
			copy(valueCopy2, entry.value)
			c.undoLog = append(c.undoLog, undoEntry{
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
			return 0, rowsAffected, fmt.Errorf("partition tree %s not found", entry.treeName)
		}

		// Mark as deleted with current timestamp
		vrow.Version.markDeleted(time.Now())

		// Re-encode and store the soft-deleted row
		deletedValueData, err := json.Marshal(vrow)
		if err != nil {
			return 0, rowsAffected, fmt.Errorf("failed to encode deleted row: %w", err)
		}

		if err := deleteTree.Put(key, deletedValueData); err != nil {
			return 0, rowsAffected, fmt.Errorf("failed to soft delete row: %w", err)
		}

		// Execute AFTER DELETE trigger per-row
		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, row, table.Columns); trigErr != nil {
			return 0, rowsAffected, fmt.Errorf("AFTER DELETE trigger failed: %w", trigErr)
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
	c.lastReturningRows = returningRows
	c.lastReturningColumns = returningCols

	return 0, rowsAffected, nil
}

func (c *Catalog) DeleteRow(ctx context.Context, tableName string, pkValue interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteRowLocked(ctx, tableName, pkValue)
}

// processDeleteRow processes a single row deletion from index lookup
func (c *Catalog) processDeleteRow(ctx context.Context, table *TableDef, tree *btree.BTree, treeName string, key []byte, valueData []byte,
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
	if c.enableRLS && c.rlsManager != nil {
		user, _ := ctx.Value("cobaltdb_user").(string)
		roles, _ := ctx.Value("cobaltdb_roles").([]string)
		if user != "" {
			rowMap := make(map[string]interface{})
			for i, col := range table.Columns {
				if i < len(row) {
					rowMap[col.Name] = row[i]
				}
			}
			allowed, rlsErr := c.checkRLSForDeleteInternal(ctx, stmt.Table, rowMap, user, roles)
			if rlsErr != nil {
				return nil // Skip rows that fail RLS check
			}
			if !allowed {
				return nil // Skip rows the user doesn't have access to
			}
		}
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
		if decErr == nil && vrow != nil {
			oldRow := vrow.Data
			for idxName, idxTree := range c.indexTrees {
				idxDef := c.indexes[idxName]
				if idxDef != nil && idxDef.TableName == tableName && len(idxDef.Columns) > 0 {
					oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
					if ok {
						if idxDef.Unique {
							if c.txnActive {
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
							if c.txnActive {
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
		pkColIdx := -1
		if len(table.PrimaryKey) > 0 {
			pkColIdx = table.GetColumnIndex(table.PrimaryKey[0])
		}
		if pkColIdx >= 0 {
			oldRow, decErr := decodeRow(oldData, len(table.Columns))
			if decErr == nil && pkColIdx < len(oldRow) && oldRow[pkColIdx] != nil {
				if fkErr := fke.OnDelete(ctx, tableName, oldRow[pkColIdx]); fkErr != nil {
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
	if c.txnActive {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		oldCopy := make([]byte, len(oldData))
		copy(oldCopy, oldData)
		c.undoLog = append(c.undoLog, undoEntry{
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
