package catalog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// formatKey formats int64 as zero-padded string (20 digits) for consistent key ordering
func formatKey(pkVal int64) string {
	return fmt.Sprintf("%020d", pkVal)
}

func (c *Catalog) Insert(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.insertLocked(ctx, stmt, args)
}

func (c *Catalog) insertLocked(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (int64, int64, error) {
	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return 0, 0, err
	}

	tree, exists := c.tableTrees[stmt.Table]
	if !exists {
		return 0, 0, ErrTableNotFound
	}

	// Determine column mapping
	// If columns are specified in INSERT, use them; otherwise use all table columns
	var insertColumns []string
	if len(stmt.Columns) > 0 {
		// Validate that all specified column names exist in the table
		for _, colName := range stmt.Columns {
			if table.GetColumnIndex(colName) < 0 {
				return 0, 0, fmt.Errorf("column '%s' does not exist in table '%s'", colName, stmt.Table)
			}
		}
		insertColumns = stmt.Columns
	} else {
		// Use all columns from table definition
		for _, col := range table.Columns {
			insertColumns = append(insertColumns, col.Name)
		}
	}

	// Insert each row
	rowsAffected := int64(0)
	autoIncValue := int64(0)

	// Pre-calculate insert column indices for performance
	insertColIndices := make([]int, len(insertColumns))
	for i, colName := range insertColumns {
		insertColIndices[i] = table.GetColumnIndex(colName)
	}

	// Handle INSERT...SELECT: execute SELECT and convert to value rows
	valueRows := stmt.Values
	if stmt.Select != nil {
		selectCols, selectRows, err := c.selectLocked(stmt.Select, args)
		if err != nil {
			return 0, 0, fmt.Errorf("INSERT...SELECT failed: %w", err)
		}
		// Validate column count matches
		if len(selectCols) != len(insertColumns) {
			return 0, 0, fmt.Errorf("INSERT...SELECT column count mismatch: INSERT has %d columns, SELECT returns %d columns", len(insertColumns), len(selectCols))
		}
		// Convert select results to expression rows
		valueRows = make([][]query.Expression, len(selectRows))
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
					exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: fmt.Sprintf("%d", v)}
				case int:
					exprRow[j] = &query.NumberLiteral{Value: float64(v), Raw: fmt.Sprintf("%d", v)}
				case bool:
					exprRow[j] = &query.BooleanLiteral{Value: v}
				default:
					exprRow[j] = &query.StringLiteral{Value: fmt.Sprintf("%v", v)}
				}
			}
			valueRows[i] = exprRow
		}
	}

	// Save AutoIncSeq before insert loop for rollback
	savedAutoIncSeq := table.AutoIncSeq
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:        undoAutoIncSeq,
			tableName:     stmt.Table,
			oldAutoIncSeq: savedAutoIncSeq,
		})
	}

	// Track insertions for statement-level atomicity (undo on partial failure)
	type stmtInsert struct {
		key     []byte
		idxKeys []struct {
			idxName string
			key     []byte
		}
	}
	var stmtInserts []stmtInsert
	var insertedRows [][]interface{} // Track rows for trigger execution
	var insertErr error

	for _, valueRow := range valueRows {
		// Validate value count matches column count
		if len(valueRow) != len(insertColumns) {
			// Allow one fewer value if there is exactly one AUTO_INCREMENT column
			autoIncCount := 0
			for _, col := range table.Columns {
				if col.AutoIncrement {
					autoIncCount++
				}
			}
			if !(autoIncCount > 0 && len(valueRow) == len(insertColumns)-autoIncCount) {
				return 0, 0, fmt.Errorf("INSERT has %d columns but %d values", len(insertColumns), len(valueRow))
			}
		}

		// Generate unique key (use auto-increment if primary key exists)
		var key string
		hasPrimaryKey := false
		for i, colName := range insertColumns {
			if table.isPrimaryKeyColumn(colName) {
				hasPrimaryKey = true
				// Get primary key value from valueRow if provided
				if i < len(valueRow) {
					if numLit, ok := valueRow[i].(*query.NumberLiteral); ok {
						pkVal := int64(numLit.Value)
						key = formatKey(pkVal)
						// Keep auto-inc counter ahead of explicit values
						if pkVal > table.AutoIncSeq {
							table.AutoIncSeq = pkVal
						}
					} else {
						// Non-numeric primary key (TEXT, etc.)
						val, err := evaluateExpression(c, nil, nil, valueRow[i], args)
						if err == nil && val != nil {
							if strVal, ok := val.(string); ok {
								key = "S:" + strVal // Prefix to distinguish from numeric keys
							} else if fVal, ok := toFloat64(val); ok {
								pkVal := int64(fVal)
								key = formatKey(pkVal)
								if pkVal > table.AutoIncSeq {
									table.AutoIncSeq = pkVal
								}
							}
						}
					}
				}
			}
		}

		if !hasPrimaryKey || key == "" {
			// Generate auto-increment key (per-table counter)
			table.AutoIncSeq++
			autoIncValue = table.AutoIncSeq
			key = fmt.Sprintf("%020d", autoIncValue)
		}

		// Build full row with all columns
		rowValues := make([]interface{}, len(table.Columns))
		colSet := make([]bool, len(table.Columns)) // Track which columns were explicitly set

		// Map provided values to their columns using pre-calculated indices
		for colIdx, tableColIdx := range insertColIndices {
			if colIdx < len(valueRow) && tableColIdx >= 0 {
				val, err := evaluateExpression(c, nil, nil, valueRow[colIdx], args)
				if err != nil {
					return 0, 0, fmt.Errorf("failed to evaluate value for column '%s': %w", insertColumns[colIdx], err)
				} else {
					rowValues[tableColIdx] = val
				}
				colSet[tableColIdx] = true // Mark this column as explicitly set
			}
		}

		// Fill remaining columns with defaults (only for columns not explicitly set)
		for i, col := range table.Columns {
			if !colSet[i] {
				// Auto-increment columns get the generated key value
				if col.AutoIncrement {
					rowValues[i] = float64(autoIncValue)
					continue
				}
				// Try to use DEFAULT expression first
				if col.defaultExpr != nil {
					defVal, err := EvalExpression(col.defaultExpr, args)
					if err == nil {
						rowValues[i] = defVal
						continue
					}
				}
				// SQL standard: omitted columns without DEFAULT get NULL
				rowValues[i] = nil
			}
		}

		// For INTEGER PRIMARY KEY columns, NULL means auto-increment
		for i, col := range table.Columns {
			if col.PrimaryKey && rowValues[i] == nil && autoIncValue > 0 {
				rowValues[i] = float64(autoIncValue)
			}
		}

		// Apply Row-Level Security check for INSERT
		if c.enableRLS && c.rlsManager != nil {
			user, _ := ctx.Value("cobaltdb_user").(string)
			roles, _ := ctx.Value("cobaltdb_roles").([]string)
			if user != "" {
				rowMap := make(map[string]interface{})
				for i, col := range table.Columns {
					if i < len(rowValues) {
						rowMap[col.Name] = rowValues[i]
					}
				}
				allowed, rlsErr := c.checkRLSForInsertInternal(ctx, stmt.Table, rowMap, user, roles)
				if rlsErr != nil {
					return 0, 0, fmt.Errorf("RLS policy check failed for INSERT: %w", rlsErr)
				}
				if !allowed {
					return 0, 0, fmt.Errorf("RLS policy denied INSERT on table '%s'", stmt.Table)
				}
			}
		}

		// Check NOT NULL constraints before inserting
		for i, col := range table.Columns {
			if col.NotNull && !col.AutoIncrement && rowValues[i] == nil {
				insertErr = fmt.Errorf("NOT NULL constraint failed: column '%s' cannot be null", col.Name)
				break
			}
		}
		if insertErr != nil {
			break
		}

		// Check UNIQUE constraints before inserting
		skipRow := false
		for i, col := range table.Columns {
			if col.Unique && rowValues[i] != nil {
				// Check if a row with this unique value already exists
				iter, err := tree.Scan(nil, nil)
				if err != nil {
					return 0, 0, fmt.Errorf("failed to scan table for UNIQUE check: %w", err)
				}
				var duplicateKey []byte
				for iter.HasNext() {
					k, existingData, err := iter.Next()
					if err != nil {
						break
					}
					var existingRow []interface{}
					if err := json.Unmarshal(existingData, &existingRow); err != nil {
						continue
					}
					if len(existingRow) > i && compareValues(rowValues[i], existingRow[i]) == 0 {
						duplicateKey = k
						break
					}
				}
				iter.Close()
				if duplicateKey != nil {
					if stmt.ConflictAction == query.ConflictIgnore {
						skipRow = true
						break
					} else if stmt.ConflictAction == query.ConflictReplace {
						// Clean up index entries for the row being replaced
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
												insertErr = fmt.Errorf("failed to delete from index %s: %w", idxName, delErr)
												break
											}
										}
									}
								}
							}
						}
						if insertErr == nil {
							if delErr := tree.Delete(duplicateKey); delErr != nil {
								insertErr = fmt.Errorf("failed to delete duplicate row: %w", delErr)
							}
						}
					} else {
						insertErr = fmt.Errorf("UNIQUE constraint failed: %s", col.Name)
						break
					}
				}
			}
		}
		if insertErr != nil {
			break
		}
		if skipRow {
			continue
		}

		// Check CHECK constraints before inserting
		for _, col := range table.Columns {
			if col.Check != nil {
				result, err := evaluateExpression(c, rowValues, table.Columns, col.Check, args)
				if err != nil {
					insertErr = fmt.Errorf("CHECK constraint failed: %w", err)
					break
				}
				// Per SQL standard, NULL (unknown) passes CHECK constraint; only explicit false fails
				if result != nil {
					if resultBool, ok := result.(bool); ok && !resultBool {
						insertErr = fmt.Errorf("CHECK constraint failed for column: %s", col.Name)
						break
					}
				}
			}
		}
		if insertErr != nil {
			break
		}

		// Check FOREIGN KEY constraints before inserting
		for _, fk := range table.ForeignKeys {
			// Get the value(s) for the foreign key column(s)
			for i, colName := range fk.Columns {
				colIdx := table.GetColumnIndex(colName)
				if colIdx < 0 || colIdx >= len(rowValues) {
					continue
				}
				fkValue := rowValues[colIdx]
				if fkValue == nil {
					continue // NULL values skip FK check
				}

				// Check if referenced row exists
				refTable, err := c.getTableLocked(fk.ReferencedTable)
				if err != nil {
					insertErr = fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
					break
				}

				refColIdx := 0
				if len(fk.ReferencedColumns) > i {
					refColIdx = refTable.GetColumnIndex(fk.ReferencedColumns[i])
				} else if len(refTable.Columns) > 0 {
					// Default to first column
					refColIdx = 0
				}

				refTree, exists := c.tableTrees[fk.ReferencedTable]
				if !exists {
					insertErr = fmt.Errorf("FOREIGN KEY constraint failed: referenced table not found")
					break
				}

				// Search for matching row
				found := false
				refIter, _ := refTree.Scan(nil, nil)
				for refIter.HasNext() {
					_, refData, err := refIter.Next()
					if err != nil {
						break
					}
					var refRow []interface{}
					if err := json.Unmarshal(refData, &refRow); err != nil {
						continue
					}
					if refColIdx < len(refRow) && compareValues(fkValue, refRow[refColIdx]) == 0 {
						found = true
						break
					}
				}
				refIter.Close()

				if !found {
					insertErr = fmt.Errorf("FOREIGN KEY constraint failed: key %v not found in referenced table %s", fkValue, fk.ReferencedTable)
					break
				}
			}
			if insertErr != nil {
				break
			}
		}
		if insertErr != nil {
			break
		}

		// Encode row
		valueData, err := json.Marshal(rowValues)
		if err != nil {
			insertErr = err
			break
		}

		// Log to WAL before applying change
		if c.wal != nil && c.txnActive {
			// For INSERT, we log the key and value
			// Format: key (null-terminated) + value
			walData := append([]byte(key), 0)
			walData = append(walData, valueData...)
			record := &storage.WALRecord{
				TxnID: c.txnID,
				Type:  storage.WALInsert,
				Data:  walData,
			}
			if err := c.wal.Append(record); err != nil {
				insertErr = err
				break
			}
		}

		// Enforce PRIMARY KEY uniqueness - check if key already exists
		if _, err := tree.Get([]byte(key)); err == nil {
			if stmt.ConflictAction == query.ConflictIgnore {
				continue // Skip this row
			} else if stmt.ConflictAction == query.ConflictReplace {
				// Clean up index entries for the row being replaced (PK conflict)
				oldData, getErr := tree.Get([]byte(key))
				if getErr == nil {
					oldRow, decErr := decodeRow(oldData, len(table.Columns))
					if decErr == nil {
						for idxName, idxTree := range c.indexTrees {
							idxDef := c.indexes[idxName]
							if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
								oldIdxKey, ok := buildCompositeIndexKey(table, idxDef, oldRow)
								if ok {
									if idxDef.Unique {
										idxTree.Delete([]byte(oldIdxKey))
									} else {
										compoundKey := oldIdxKey + "\x00" + key
										idxTree.Delete([]byte(compoundKey))
									}
								}
							}
						}
					}
				}
				// Delete existing row before replacing
				tree.Delete([]byte(key))
			} else {
				insertErr = fmt.Errorf("UNIQUE constraint failed: duplicate primary key value")
				break
			}
		}

		// Store in B+Tree
		if err := tree.Put([]byte(key), valueData); err != nil {
			insertErr = fmt.Errorf("failed to store row: %w", err)
			break
		}

		// Update indexes and track changes for undo
		var idxChanges []indexUndoEntry
		for idxName, idxTree := range c.indexTrees {
			idxDef := c.indexes[idxName]
			if idxDef.TableName == stmt.Table && len(idxDef.Columns) > 0 {
				indexKey, ok := buildCompositeIndexKey(table, idxDef, rowValues)
				if ok {
					// Enforce UNIQUE constraint
					if idxDef.Unique {
						if oldPKData, err := idxTree.Get([]byte(indexKey)); err == nil {
							if stmt.ConflictAction == query.ConflictIgnore {
								// Delete the already-stored row from the main table
								if err := tree.Delete([]byte(key)); err != nil {
									// Continue with conflict handling, but note the error
									_ = err
								}
								// Undo any index entries already added in this loop iteration
								for _, undo := range idxChanges {
									if undo.wasAdded {
										if idxTree2, ok := c.indexTrees[undo.indexName]; ok {
											if err := idxTree2.Delete(undo.key); err != nil {
												_ = err
											}
										}
									}
								}
								skipRow = true
								break
							} else if stmt.ConflictAction == query.ConflictReplace {
								// Delete the old row that conflicts on this unique index
								oldPK := string(oldPKData)
								if oldPK != key { // Only if it's a different row
									oldRowData, getErr := tree.Get([]byte(oldPK))
									if getErr == nil {
										oldRow, decErr := decodeRow(oldRowData, len(table.Columns))
										if decErr == nil {
											// Clean up all index entries for the old row
											for otherIdxName, otherIdxTree := range c.indexTrees {
												otherIdxDef := c.indexes[otherIdxName]
												if otherIdxDef.TableName == stmt.Table && len(otherIdxDef.Columns) > 0 {
													oldIdxKey, ok := buildCompositeIndexKey(table, otherIdxDef, oldRow)
													if ok {
														otherIdxTree.Delete([]byte(oldIdxKey))
													}
												}
											}
										}
									}
									tree.Delete([]byte(oldPK))
								}
							} else {
								insertErr = fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, idxName)
								break
							}
						}
					}
					// For non-unique indexes, use compound key: "indexValue\x00pk" to support multiple rows per value
					var idxStorageKey []byte
					if idxDef.Unique {
						idxStorageKey = []byte(indexKey)
					} else {
						idxStorageKey = []byte(indexKey + "\x00" + key)
					}
					if err := idxTree.Put(idxStorageKey, []byte(key)); err != nil {
						insertErr = fmt.Errorf("failed to update index %s: %w", idxName, err)
						break
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
		if skipRow {
			continue
		}
		if insertErr != nil {
			// Row was stored but index failed - delete the row
			if err := tree.Delete([]byte(key)); err != nil {
				// Best effort cleanup failed, continue with original error
				_ = err
			}
			break
		}

		// Record undo log entry for rollback (after applying change)
		if c.txnActive {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, []byte(key))
			c.undoLog = append(c.undoLog, undoEntry{
				action:       undoInsert,
				tableName:    stmt.Table,
				key:          keyCopy,
				indexChanges: idxChanges,
			})
		}

		// Track for statement-level atomicity
		si := stmtInsert{key: []byte(key)}
		for _, ic := range idxChanges {
			si.idxKeys = append(si.idxKeys, struct {
				idxName string
				key     []byte
			}{ic.indexName, ic.key})
		}
		stmtInserts = append(stmtInserts, si)

		// Save row for trigger execution
		rowCopy := make([]interface{}, len(rowValues))
		copy(rowCopy, rowValues)
		insertedRows = append(insertedRows, rowCopy)

		rowsAffected++
	}

	// Statement-level atomicity: undo all inserts on error (outside explicit transactions)
	if insertErr != nil && !c.txnActive {
		for i := len(stmtInserts) - 1; i >= 0; i-- {
			si := stmtInserts[i]
			if err := tree.Delete(si.key); err != nil {
				// Best effort cleanup failed
				_ = err
			}
			for _, ik := range si.idxKeys {
				if idxTree, exists := c.indexTrees[ik.idxName]; exists {
					if err := idxTree.Delete(ik.key); err != nil {
						// Best effort cleanup failed
						_ = err
					}
				}
			}
		}
		table.AutoIncSeq = savedAutoIncSeq
		return 0, 0, insertErr
	}
	if insertErr != nil {
		// Inside explicit transaction - undo log handles cleanup on ROLLBACK
		// But remove the undo entries for this failed statement's successful rows
		// since the caller will see an error and may want statement-level atomicity
		// Undo the successful rows immediately for statement atomicity
		for i := len(stmtInserts) - 1; i >= 0; i-- {
			si := stmtInserts[i]
			_ = tree.Delete(si.key)
			for _, ik := range si.idxKeys {
				if idxTree, exists := c.indexTrees[ik.idxName]; exists {
					_ = idxTree.Delete(ik.key)
				}
			}
		}
		// Remove the undo log entries we added for this statement
		// (the AutoIncSeq entry + one per successful row)
		undoToRemove := 1 + len(stmtInserts) // 1 for AutoIncSeq + N for rows
		if len(c.undoLog) >= undoToRemove {
			c.undoLog = c.undoLog[:len(c.undoLog)-undoToRemove]
		}
		table.AutoIncSeq = savedAutoIncSeq
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

	return autoIncValue, rowsAffected, nil
}