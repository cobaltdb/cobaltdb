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

// updateEntry tracks a row to be updated
type updateEntry struct {
	key      []byte
	oldRow   []interface{}
	newRow   []interface{}
	treeName string // which partition tree this entry came from
}

func (c *Catalog) Update(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updateLocked(ctx, stmt, args)
}

func (c *Catalog) updateLocked(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	// Check for INSTEAD OF UPDATE trigger first (for views)
	if trig := c.findInsteadOfTrigger(stmt.Table, "UPDATE"); trig != nil {
		return c.executeInsteadOfUpdateTrigger(ctx, trig, stmt, args)
	}

	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	// Handle UPDATE with JOIN
	if stmt.From != nil || len(stmt.Joins) > 0 {
		return c.updateWithJoinLocked(ctx, stmt, args)
	}

	// Get all trees for scanning (handles partitioned tables)
	trees, err := c.getTableTreesForScan(table)
	if err != nil {
		return 0, 0, err
	}

	// Get tree names for later lookup during update
	treeNames := []string{stmt.Table}
	if table.Partition != nil {
		treeNames = table.getPartitionTreeNames()
	}

	// Collect entries to update (need old row for index cleanup)
	var entries []updateEntry
	rowsAffected := int64(0)

	// Pre-calculate column indices for SET clauses
	setColumnIndices := make([]int, len(stmt.Set))
	for i, setClause := range stmt.Set {
		setColumnIndices[i] = table.GetColumnIndex(setClause.Column)
		if setColumnIndices[i] < 0 {
			return 0, 0, fmt.Errorf("column '%s' not found in table '%s'", setClause.Column, stmt.Table)
		}
	}

	// Check if we can use an index for the WHERE clause
	var indexedRows map[string]bool
	useIndex := false
	if stmt.Where != nil {
		indexedRows, useIndex = c.useIndexForQueryWithArgs(stmt.Table, stmt.Where, args)
	}

	// Iterate over all partition trees
	for treeIdx, tree := range trees {
		treeName := treeNames[treeIdx]

		// If we have indexed rows, only process those specific rows
		if useIndex {
			for pkStr := range indexedRows {
				key := []byte(pkStr)
				valueData, err := tree.Get(key)
				if err != nil {
					continue // Row not found
				}
				// Process this row
				if err := c.processUpdateRow(ctx, table, tree, treeName, key, valueData, stmt, args, setColumnIndices, &entries, &rowsAffected); err != nil {
					return 0, rowsAffected, err
				}
			}
			continue
		}

		// Full table scan path
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to scan table for UPDATE: %w", err)
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

		// Skip soft-deleted rows
		if vrow.Version.DeletedAt > 0 {
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

		if err := c.processUpdateRowData(ctx, table, tree, treeName, key, row, stmt, args, setColumnIndices, &entries, &rowsAffected); err != nil {
			return 0, rowsAffected, err
		}
	}
	iter.Close()
}

	// Apply updates
	pkColIdx := -1
	if len(table.PrimaryKey) > 0 {
		pkColIdx = table.GetColumnIndex(table.PrimaryKey[0])
	}
	// Foreign key enforcer for CASCADE/RESTRICT/SET NULL actions on PK changes
	fke := NewForeignKeyEnforcer(c)
	for _, entry := range entries {
		oldKey := entry.key

		// Re-encode row with new timestamp
		newValueData, err := encodeVersionedRow(entry.newRow, nil)
		if err != nil {
			return 0, rowsAffected, fmt.Errorf("failed to encode updated row: %w", err)
		}

		// Get the tree for this entry
		updateTree, exists := c.tableTrees[entry.treeName]
		if !exists {
			return 0, rowsAffected, fmt.Errorf("partition tree %s not found", entry.treeName)
		}

		// Check if PRIMARY KEY was changed - need to delete old key and insert new one
		newKey := oldKey
		pkChanged := false
		if pkColIdx >= 0 && pkColIdx < len(entry.newRow) && pkColIdx < len(entry.oldRow) {
			if compareValues(entry.oldRow[pkColIdx], entry.newRow[pkColIdx]) != 0 {
				pkChanged = true
				// Generate new key from the updated PK value
				pkVal := entry.newRow[pkColIdx]
				if strVal, ok := pkVal.(string); ok {
					newKey = []byte("S:" + strVal)
				} else if fVal, ok := toFloat64(pkVal); ok {
					newKey = []byte(fmt.Sprintf("%020d", int64(fVal)))
				}
				// Check if the new PK already exists (duplicate PK violation)
				if existingData, err := updateTree.Get(newKey); err == nil && existingData != nil {
					return 0, 0, fmt.Errorf("PRIMARY KEY constraint failed: duplicate key '%v'", pkVal)
				}
			}
		}

		// Enforce foreign key ON UPDATE actions (CASCADE, SET NULL, RESTRICT)
		if pkChanged && pkColIdx >= 0 {
			if fkErr := fke.OnUpdate(ctx, stmt.Table, entry.oldRow[pkColIdx], entry.newRow[pkColIdx]); fkErr != nil {
				return 0, 0, fmt.Errorf("foreign key constraint: %w", fkErr)
			}
		}

		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			if pkChanged {
				// Log delete of old key
				deleteRecord := &storage.WALRecord{
					TxnID: c.txnID,
					Type:  storage.WALDelete,
					Data:  oldKey,
				}
				if err := c.wal.Append(deleteRecord); err != nil {
					return 0, rowsAffected, err
				}
				// Log insert of new key
				walData := make([]byte, 0, len(newKey)+1+len(newValueData))
				walData = append(walData, newKey...)
				walData = append(walData, 0)
				walData = append(walData, newValueData...)
				insertRecord := &storage.WALRecord{
					TxnID: c.txnID,
					Type:  storage.WALInsert,
					Data:  walData,
				}
				if err := c.wal.Append(insertRecord); err != nil {
					return 0, rowsAffected, err
				}
			} else {
				// For UPDATE without PK change, log the key and new value
				walData := append([]byte(oldKey), 0)
				walData = append(walData, newValueData...)
				record := &storage.WALRecord{
					TxnID: c.txnID,
					Type:  storage.WALUpdate,
					Data:  walData,
				}
				if err := c.wal.Append(record); err != nil {
					return 0, rowsAffected, err
				}
			}
		}

		if pkChanged {
			// Delete old key and insert new key
			if err := updateTree.Delete(oldKey); err != nil {
				return 0, rowsAffected, fmt.Errorf("failed to delete old key during PK update: %w", err)
			}
			if err := updateTree.Put(newKey, newValueData); err != nil {
				return 0, rowsAffected, fmt.Errorf("failed to update row with new key: %w", err)
			}
			// Update auto-increment counter if needed
			if fVal, ok := toFloat64(entry.newRow[pkColIdx]); ok {
				pkVal := int64(fVal)
				if pkVal > table.AutoIncSeq {
					table.AutoIncSeq = pkVal
				}
			}
		} else {
			if err := updateTree.Put(oldKey, newValueData); err != nil {
				return 0, rowsAffected, fmt.Errorf("failed to update row: %w", err)
			}
		}

		// Update indexes: remove old entries and add new ones, track for undo
		var idxChanges []indexUndoEntry
		for idxName, idxTree := range c.indexTrees {
			idxDef := c.indexes[idxName]
			if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
				// Remove old index entry
				oldIndexKey, oldOk := buildCompositeIndexKey(table, idxDef, entry.oldRow)
				if oldOk {
					if idxDef.Unique {
						oldIdxVal, getErr := idxTree.Get([]byte(oldIndexKey))
						_ = idxTree.Delete([]byte(oldIndexKey)) // best-effort index cleanup during UPDATE
						if c.txnActive && getErr == nil {
							idxChanges = append(idxChanges, indexUndoEntry{
								indexName: idxName,
								key:       []byte(oldIndexKey),
								oldValue:  oldIdxVal,
								wasAdded:  false, // was deleted
							})
						}
					} else {
						// For non-unique indexes, delete the compound key "indexValue\x00pk"
						compoundKey := oldIndexKey + "\x00" + string(entry.key)
						oldIdxVal, getErr := idxTree.Get([]byte(compoundKey))
						_ = idxTree.Delete([]byte(compoundKey)) // best-effort index cleanup during UPDATE
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
				// Add new index entry
				newIndexKey, newOk := buildCompositeIndexKey(table, idxDef, entry.newRow)
				if newOk {
					// For non-unique indexes, use compound key: "indexValue\x00pk"
					var idxStorageKey []byte
					if idxDef.Unique {
						idxStorageKey = []byte(newIndexKey)
						// Enforce UNIQUE constraint (skip if value unchanged)
						if newIndexKey != oldIndexKey {
							if _, err := idxTree.Get(idxStorageKey); err == nil {
								return 0, rowsAffected, fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", newIndexKey, idxName)
							}
						}
					} else {
						idxStorageKey = []byte(newIndexKey + "\x00" + string(newKey))
					}
					if err := idxTree.Put(idxStorageKey, newKey); err != nil {
						return 0, rowsAffected, fmt.Errorf("failed to update index %s: %w", idxName, err)
					}
					if c.txnActive {
						idxChanges = append(idxChanges, indexUndoEntry{
							indexName: idxName,
							key:       idxStorageKey,
							wasAdded:  true,
						})
					}
				}
			}
		}

		// Update vector indexes
		c.updateVectorIndexesForUpdate(stmt.Table, entry.newRow, entry.key)

		// Record undo log entry for rollback (after applying change)
		if c.txnActive {
			oldValueData, marshalErr := json.Marshal(entry.oldRow)
			if marshalErr != nil {
				return 0, rowsAffected, fmt.Errorf("failed to encode undo log for row: %w", marshalErr)
			}
			keyCopy := make([]byte, len(oldKey))
			copy(keyCopy, oldKey)
			c.undoLog = append(c.undoLog, undoEntry{
				action:       undoUpdate,
				tableName:    stmt.Table,
				key:          keyCopy,
				oldValue:     oldValueData,
				indexChanges: idxChanges,
			})
		}
	}

	// Execute AFTER UPDATE triggers (per-row)
	for _, entry := range entries {
		if trigErr := c.executeTriggers(ctx, stmt.Table, "UPDATE", "AFTER", entry.newRow, entry.oldRow, table.Columns); trigErr != nil {
			return 0, rowsAffected, fmt.Errorf("AFTER UPDATE trigger failed: %w", trigErr)
		}
	}

	// Invalidate query cache for the affected table
	c.invalidateQueryCache(stmt.Table)

	// Handle RETURNING clause
	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.newRow, table, args)
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

func (c *Catalog) updateWithJoinLocked(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (int64, int64, error) {
	targetTable, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	targetTree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// Build a SELECT statement from the UPDATE to execute the join
	// Select all columns from target table
	var columns []query.Expression
	for _, col := range targetTable.Columns {
		columns = append(columns, &query.QualifiedIdentifier{Table: stmt.Table, Column: col.Name})
	}
	selectStmt := &query.SelectStmt{
		Columns: columns,
		From:    &query.TableRef{Name: stmt.Table},
		Joins:   stmt.Joins,
		Where:   stmt.Where,
	}

	// If FROM clause exists, use it as the main table and add target as first join
	if stmt.From != nil {
		selectStmt.From = stmt.From
		// Add target table as first join with no condition
		selectStmt.Joins = append([]*query.JoinClause{{
			Type:  query.TokenJoin,
			Table: &query.TableRef{Name: stmt.Table},
		}}, stmt.Joins...)
	}

	// Execute the join to find matching rows
	_, resultRows, err := c.selectLocked(selectStmt, args)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to execute UPDATE join: %w", err)
	}

	// Find primary key column index in the target table
	pkColIdx := 0
	if len(targetTable.PrimaryKey) > 0 {
		pkColIdx = targetTable.GetColumnIndex(targetTable.PrimaryKey[0])
		if pkColIdx < 0 {
			pkColIdx = 0
		}
	}

	// Collect keys of rows to update by serializing the PK value
	keysToUpdate := make(map[string]struct{})
	for _, row := range resultRows {
		if pkColIdx < len(row) && row[pkColIdx] != nil {
			key := c.serializePK(row[pkColIdx], targetTree)
			keysToUpdate[string(key)] = struct{}{}
		}
	}

	if len(keysToUpdate) == 0 {
		return 0, 0, nil // No rows to update
	}

	// Now update each row
	rowsAffected := int64(0)
	type updateEntry struct {
		key    []byte
		oldRow []interface{}
		newRow []interface{}
	}
	var entries []updateEntry

	// Pre-calculate column indices for SET clauses
	setColumnIndices := make([]int, len(stmt.Set))
	for i, setClause := range stmt.Set {
		setColumnIndices[i] = targetTable.GetColumnIndex(setClause.Column)
		if setColumnIndices[i] < 0 {
			return 0, 0, fmt.Errorf("column '%s' not found in table '%s'", setClause.Column, stmt.Table)
		}
	}

	// Iterate over keys to update
	for keyStr := range keysToUpdate {
		key := []byte(keyStr)
		valueData, err := targetTree.Get(key)
		if err != nil {
			continue // Row may have been deleted
		}

		row, err := decodeRow(valueData, len(targetTable.Columns))
		if err != nil {
			continue
		}

		// Make a copy of the row to update
		updatedRow := make([]interface{}, len(row))
		copy(updatedRow, row)

		// Apply SET clauses
		for i, setClause := range stmt.Set {
			colIdx := setColumnIndices[i]
			newVal, err := evaluateExpression(c, row, targetTable.Columns, setClause.Value, args)
			if err != nil {
				return 0, rowsAffected, fmt.Errorf("failed to evaluate SET expression: %w", err)
			}
			updatedRow[colIdx] = newVal
		}

		// Check constraints (simplified - full checks in actual implementation)
		for i, col := range targetTable.Columns {
			if col.NotNull && i < len(updatedRow) && updatedRow[i] == nil {
				return 0, rowsAffected, fmt.Errorf("NOT NULL constraint failed: %s", col.Name)
			}
		}

		entries = append(entries, updateEntry{
			key:    key,
			oldRow: row,
			newRow: updatedRow,
		})
		rowsAffected++
	}

	// Apply all updates
	for _, entry := range entries {
		newValue, err := encodeRow(nil, entry.newRow)
		if err != nil {
			return 0, rowsAffected, err
		}
		if err := targetTree.Put(entry.key, newValue); err != nil {
			return 0, rowsAffected, err
		}

		// Update indexes
		for idxName, idxDef := range c.indexes {
			if idxDef.TableName == stmt.Table {
				oldIdxKey, _ := buildCompositeIndexKey(targetTable, idxDef, entry.oldRow)
				newIdxKey, newOk := buildCompositeIndexKey(targetTable, idxDef, entry.newRow)
				if idxTree, exists := c.indexTrees[idxName]; exists {
					if oldIdxKey != "" {
						if err := idxTree.Delete([]byte(oldIdxKey)); err != nil {
							return 0, rowsAffected, fmt.Errorf("failed to delete old index entry: %w", err)
						}
					}
					if newOk && newIdxKey != "" {
						if err := idxTree.Put([]byte(newIdxKey), entry.key); err != nil {
							return 0, rowsAffected, fmt.Errorf("failed to insert new index entry: %w", err)
						}
					}
				}
			}
		}

		// Record undo log for rollback
		if c.txnActive {
			oldValueData, marshalErr := json.Marshal(entry.oldRow)
			if marshalErr == nil {
				keyCopy := make([]byte, len(entry.key))
				copy(keyCopy, entry.key)
				c.undoLog = append(c.undoLog, undoEntry{
					action:    undoUpdate,
					tableName: stmt.Table,
					key:       keyCopy,
					oldValue:  oldValueData,
				})
			}
		}
	}

	// Handle RETURNING clause
	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.newRow, targetTable, args)
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

	return int64(len(entries)), rowsAffected, nil
}

func (c *Catalog) deleteWithUsingLocked(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (int64, int64, error) {
	targetTable, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	targetTree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// Build a SELECT statement to execute the join
	// Select all columns from target table to get the primary key
	var columns []query.Expression
	for _, col := range targetTable.Columns {
		columns = append(columns, &query.QualifiedIdentifier{Table: stmt.Table, Column: col.Name})
	}

	selectStmt := &query.SelectStmt{
		Columns: columns,
		From:    &query.TableRef{Name: stmt.Table},
		Where:   stmt.Where,
	}

	// Add USING tables as joins
	for _, usingTable := range stmt.Using {
		selectStmt.Joins = append(selectStmt.Joins, &query.JoinClause{
			Type:  query.TokenJoin,
			Table: usingTable,
		})
	}

	// Execute the join to find matching rows
	_, resultRows, err := c.selectLocked(selectStmt, args)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to execute DELETE USING: %w", err)
	}

	// Find primary key column index
	pkIdx := 0
	if len(targetTable.PrimaryKey) > 0 {
		pkIdx = targetTable.GetColumnIndex(targetTable.PrimaryKey[0])
		if pkIdx < 0 {
			pkIdx = 0
		}
	}

	// Collect keys of rows to delete by serializing the PK value
	keysToDelete := make(map[string]struct{})
	for _, row := range resultRows {
		if pkIdx < len(row) && row[pkIdx] != nil {
			key := c.serializePK(row[pkIdx], targetTree)
			keysToDelete[string(key)] = struct{}{}
		}
	}

	if len(keysToDelete) == 0 {
		return 0, 0, nil // No rows to delete
	}

	// Now delete each row
	rowsAffected := int64(0)
	type delEntry struct {
		key   []byte
		value []byte
		row   []interface{}
	}
	var entries []delEntry

	// Foreign key enforcer for CASCADE/RESTRICT actions
	fke := NewForeignKeyEnforcer(c)

	// Collect entries to delete
	for keyStr := range keysToDelete {
		key := []byte(keyStr)
		valueData, err := targetTree.Get(key)
		if err != nil {
			continue // Row may have been deleted
		}

		vrow, err := decodeVersionedRow(valueData, len(targetTable.Columns))
		if err != nil {
			continue
		}
		// Skip already deleted rows
		if vrow.Version.DeletedAt > 0 {
			continue
		}
		row := vrow.Data

		// Enforce foreign key ON DELETE actions
		pkColIdx := -1
		if len(targetTable.PrimaryKey) > 0 {
			pkColIdx = targetTable.GetColumnIndex(targetTable.PrimaryKey[0])
		}
		if pkColIdx >= 0 && pkColIdx < len(row) && row[pkColIdx] != nil {
			if fkErr := fke.OnDelete(ctx, stmt.Table, row[pkColIdx]); fkErr != nil {
				return 0, 0, fmt.Errorf("foreign key constraint: %w", fkErr)
			}
		}

		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valueCopy := make([]byte, len(valueData))
		copy(valueCopy, valueData)

		entries = append(entries, delEntry{
			key:   keyCopy,
			value: valueCopy,
			row:   row,
		})
		rowsAffected++
	}

	// Soft delete collected entries
	for _, entry := range entries {
		// Get current value to decode
		currentData, err := targetTree.Get(entry.key)
		if err != nil {
			continue // Row may have been deleted
		}

		vrow, err := decodeVersionedRow(currentData, len(targetTable.Columns))
		if err != nil {
			continue
		}

		// Remove from indexes first
		for idxName, idxTree := range c.indexTrees {
			idxDef := c.indexes[idxName]
			if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
				indexKey, ok := buildCompositeIndexKey(targetTable, idxDef, entry.row)
				if ok {
					if idxDef.Unique {
						if err := idxTree.Delete([]byte(indexKey)); err != nil {
							return 0, rowsAffected, fmt.Errorf("failed to delete index entry: %w", err)
						}
					} else {
						compoundKey := indexKey + "\x00" + string(entry.key)
						if err := idxTree.Delete([]byte(compoundKey)); err != nil {
							return 0, rowsAffected, fmt.Errorf("failed to delete compound index entry: %w", err)
						}
					}
				}
			}
		}

		// Soft delete: mark as deleted
		vrow.Version.markDeleted(time.Now())

		// Re-encode and store
		deletedValueData, err := json.Marshal(vrow)
		if err != nil {
			return 0, rowsAffected, fmt.Errorf("failed to encode deleted row: %w", err)
		}

		if err := targetTree.Put(entry.key, deletedValueData); err != nil {
			return 0, rowsAffected, fmt.Errorf("failed to soft delete row: %w", err)
		}

		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			walData := append([]byte(entry.key), 0)
			record := &storage.WALRecord{
				TxnID: c.txnID,
				Type:  storage.WALDelete,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				return 0, rowsAffected, fmt.Errorf("failed to append WAL record: %w", err)
			}
		}
	}

	// Handle RETURNING clause (returns OLD row values)
	var returningRows [][]interface{}
	var returningCols []string
	if len(stmt.Returning) > 0 && rowsAffected > 0 {
		for _, entry := range entries {
			returningRow, cols, err := c.evaluateReturning(stmt.Returning, entry.row, targetTable, args)
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

	return int64(len(entries)), rowsAffected, nil
}

// processUpdateRow processes a single row update from index lookup (valueData is raw bytes)
func (c *Catalog) processUpdateRow(ctx context.Context, table *TableDef, tree *btree.BTree, treeName string, key []byte, valueData []byte,
	stmt *query.UpdateStmt, args []interface{}, setColumnIndices []int, entries *[]updateEntry, rowsAffected *int64) error {
	vrow, err := decodeVersionedRow(valueData, len(table.Columns))
	if err != nil {
		return nil // Skip unparseable rows
	}
	row := vrow.Data
	if vrow.Version.DeletedAt > 0 {
		return nil // Skip soft-deleted rows
	}
	return c.processUpdateRowData(ctx, table, tree, treeName, key, row, stmt, args, setColumnIndices, entries, rowsAffected)
}

// processUpdateRowData processes a single row update from scan path (row is already decoded)
func (c *Catalog) processUpdateRowData(ctx context.Context, table *TableDef, tree *btree.BTree, treeName string, key []byte, row []interface{},
	stmt *query.UpdateStmt, args []interface{}, setColumnIndices []int, entries *[]updateEntry, rowsAffected *int64) error {

	// Apply Row-Level Security check for UPDATE
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
			allowed, rlsErr := c.checkRLSForUpdateInternal(ctx, stmt.Table, rowMap, user, roles)
			if rlsErr != nil {
				return nil // Skip rows that fail RLS check
			}
			if !allowed {
				return nil // Skip rows the user doesn't have access to
			}
		}
	}

	// Make a copy of the row to update
	updatedRow := make([]interface{}, len(row))
	copy(updatedRow, row)

	// Update fields - use pre-calculated column indices
	for i, setClause := range stmt.Set {
		colIdx := setColumnIndices[i]
		if colIdx >= 0 {
			newVal, err := evaluateExpression(c, row, table.Columns, setClause.Value, args)
			if err != nil {
				return fmt.Errorf("failed to evaluate SET expression for column '%s': %w", setClause.Column, err)
			}
			updatedRow[colIdx] = newVal
		}
	}

	// Make copies since buffers may be reused
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	// Check UNIQUE constraints before updating
	for i, col := range table.Columns {
		if col.Unique && updatedRow[i] != nil {
			// Check if another row (not this one) has the same unique value
			checkIter, scanErr := tree.Scan(nil, nil)
			if scanErr != nil {
				return fmt.Errorf("failed to scan table for UNIQUE check: %w", scanErr)
			}
			duplicate := false
			for checkIter.HasNext() {
				checkKey, existingData, err := checkIter.Next()
				if err != nil {
					break
				}
				// Skip the current row being updated
				if string(checkKey) == string(key) {
					continue
				}
				vrow, err := decodeVersionedRow(existingData, len(table.Columns))
				if err != nil {
					continue
				}
				existingRow := vrow.Data
				if len(existingRow) > i && compareValues(updatedRow[i], existingRow[i]) == 0 {
					duplicate = true
					break
				}
			}
			checkIter.Close()
			if duplicate {
				return fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
			}
		}
	}

	// Check UNIQUE INDEX constraints before updating
	for idxName, idxDef := range c.indexes {
		if idxDef.TableName == stmt.Table && idxDef.Unique && len(idxDef.Columns) > 0 {
			newIdxKey, newOk := buildCompositeIndexKey(table, idxDef, updatedRow)
			if !newOk {
				continue
			}
			oldIdxKey, _ := buildCompositeIndexKey(table, idxDef, row)
			if newIdxKey == oldIdxKey {
				continue // Value unchanged, no conflict possible
			}
			if idxTree, exists := c.indexTrees[idxName]; exists {
				if _, err := idxTree.Get([]byte(newIdxKey)); err == nil {
					return fmt.Errorf("UNIQUE constraint failed: duplicate value in index %s", idxName)
				}
			}
		}
	}

	// Check NOT NULL constraints before updating
	for i, col := range table.Columns {
		if col.NotNull && i < len(updatedRow) && updatedRow[i] == nil {
			return fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
		}
	}

	// Check CHECK constraints before updating
	for _, col := range table.Columns {
		if col.Check != nil {
			result, err := evaluateExpression(c, updatedRow, table.Columns, col.Check, args)
			if err != nil {
				return fmt.Errorf("CHECK constraint failed: %w", err)
			}
			// Per SQL standard, NULL (unknown) passes CHECK constraint; only explicit false fails
			if result != nil {
				if resultBool, ok := result.(bool); ok && !resultBool {
					return fmt.Errorf("CHECK constraint failed for column: %s", col.Name)
				}
			}
		}
	}

	// Check FOREIGN KEY constraints on updated columns
	for _, fk := range table.ForeignKeys {
		for i, colName := range fk.Columns {
			colIdx := table.GetColumnIndex(colName)
			if colIdx < 0 || colIdx >= len(updatedRow) {
				continue
			}
			fkValue := updatedRow[colIdx]
			if fkValue == nil {
				continue // NULL values skip FK check
			}
			// Only check if the FK column value actually changed
			if colIdx < len(row) && compareValues(fkValue, row[colIdx]) == 0 {
				continue // Value didn't change, skip check
			}
			// Check if referenced row exists
			refTable, err := c.getTableLocked(fk.ReferencedTable)
			if err != nil {
				return fmt.Errorf("FOREIGN KEY constraint failed: referenced table '%s' not found", fk.ReferencedTable)
			}
			refColIdx := 0
			if len(fk.ReferencedColumns) > i {
				refColIdx = refTable.GetColumnIndex(fk.ReferencedColumns[i])
			}
			refTree, exists := c.tableTrees[fk.ReferencedTable]
			if !exists {
				return fmt.Errorf("FOREIGN KEY constraint failed: referenced table '%s' not found", fk.ReferencedTable)
			}
			found := false
			refIter, scanErr := refTree.Scan(nil, nil)
			if scanErr != nil {
				return fmt.Errorf("FOREIGN KEY constraint failed: %w", scanErr)
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
			if !found {
				return fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValue, fk.ReferencedTable)
			}
		}
	}

	*entries = append(*entries, updateEntry{
		key:      keyCopy,
		oldRow:   row,
		newRow:   updatedRow,
		treeName: treeName,
	})
	*rowsAffected++
	return nil
}

func (c *Catalog) UpdateRow(tableName string, pkValue interface{}, row map[string]interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updateRowLocked(tableName, pkValue, row)
}

// updateRowLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) updateRowLocked(tableName string, pkValue interface{}, row map[string]interface{}) error {
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Convert row map to values slice
	values := make([]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		if val, exists := row[col.Name]; exists {
			values[i] = val
		} else {
			values[i] = nil
		}
	}

	// Encode the row (Insert uses json.Marshal, so use same format for consistency)
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	// Update in BTree
	return tree.Put(key, data)
}