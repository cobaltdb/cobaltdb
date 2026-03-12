package catalog

import (
	"context"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func (c *Catalog) Delete(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteLocked(ctx, stmt, args)
}

func (c *Catalog) deleteLocked(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// Handle DELETE with USING (JOIN)
	if len(stmt.Using) > 0 {
		return c.deleteWithUsingLocked(ctx, stmt, args)
	}

	rowsAffected := int64(0)
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to scan table for DELETE: %w", err)
	}
	defer iter.Close()

	// Collect keys and row data to delete (need row data for index cleanup)
	type deleteEntry struct {
		key   []byte
		value []byte
	}
	var entries []deleteEntry
	for iter.HasNext() {
		key, valueData, err := iter.Next()
		if err != nil {
			break
		}

		// Decode row
		row, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}

		// Apply WHERE clause if present
		if stmt.Where != nil {
			matched, err := evaluateWhere(c, row, table.Columns, stmt.Where, args)
			if err != nil {
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

		entries = append(entries, deleteEntry{key: keyCopy, value: valueCopy})
		rowsAffected++
	}

	// Foreign key enforcer for CASCADE/RESTRICT actions
	fke := NewForeignKeyEnforcer(c)

	// Delete collected entries
	for _, entry := range entries {
		key := entry.key

		// Enforce foreign key ON DELETE actions (CASCADE, SET NULL, RESTRICT)
		// Extract primary key value from the row for FK lookup
		row, err := decodeRow(entry.value, len(table.Columns))
		if err == nil {
			pkColIdx := -1
			if len(table.PrimaryKey) > 0 {
				pkColIdx = table.GetColumnIndex(table.PrimaryKey[0])
			}
			if pkColIdx >= 0 && pkColIdx < len(row) && row[pkColIdx] != nil {
				if fkErr := fke.OnDelete(ctx, stmt.Table, row[pkColIdx]); fkErr != nil {
					return 0, 0, fmt.Errorf("foreign key constraint: %w", fkErr)
				}
			}
		}

		// Remove from indexes first (before deleting the row), track for undo
		var idxChanges []indexUndoEntry
		if row == nil {
			row, err = decodeRow(entry.value, len(table.Columns))
		}
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

		if err := tree.Delete(key); err != nil {
			return 0, rowsAffected, fmt.Errorf("failed to delete row: %w", err)
		}

		// Execute AFTER DELETE trigger per-row
		if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, row, table.Columns); trigErr != nil {
			return 0, rowsAffected, fmt.Errorf("AFTER DELETE trigger failed: %w", trigErr)
		}
	}

	// Invalidate query cache for the affected table
	c.invalidateQueryCache(stmt.Table)

	return 0, rowsAffected, nil
}

func (c *Catalog) DeleteRow(ctx context.Context, tableName string, pkValue interface{}) error {
	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Get old value for undo log and index cleanup
	oldData, err := tree.Get(key)
	if err != nil {
		return tree.Delete(key) // Key might not exist in expected format, try anyway
	}

	table := c.tables[tableName]

	// Clean up index entries
	var idxChanges []indexUndoEntry
	if table != nil {
		oldRow, decErr := decodeRow(oldData, len(table.Columns))
		if decErr == nil {
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

	// Delete from BTree
	return tree.Delete(key)
}