package catalog

import (
	"encoding/json"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func (c *Catalog) CreateIndex(stmt *query.CreateIndexStmt) error {
	if !stmt.IfNotExists {
		if _, exists := c.indexes[stmt.Index]; exists {
			return ErrIndexExists
		}
	}

	// Verify table exists
	table, err := c.getTableLocked(stmt.Table)
	if err != nil {
		return err
	}

	// Verify all index columns exist in the table
	for _, colName := range stmt.Columns {
		if table.GetColumnIndex(colName) < 0 {
			return fmt.Errorf("column '%s' not found in table '%s'", colName, stmt.Table)
		}
	}

	// Create B+Tree for the index
	indexTree, err := btree.NewBTree(c.pool)
	if err != nil {
		return err
	}

	indexDef := &IndexDef{
		Name:       stmt.Index,
		TableName:  stmt.Table,
		Columns:    stmt.Columns,
		Unique:     stmt.Unique,
		RootPageID: indexTree.RootPageID(),
	}

	c.indexes[stmt.Index] = indexDef
	c.indexTrees[stmt.Index] = indexTree

	// Populate index with existing data from the table
	tree, exists := c.tableTrees[stmt.Table]
	if exists {
		iter, _ := tree.Scan(nil, nil)
		defer iter.Close()
		for iter.HasNext() {
			key, valueData, _ := iter.Next()
			row, err := decodeRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			indexKey, ok := buildCompositeIndexKey(table, indexDef, row)
			if ok {
				// For non-unique indexes, use compound key: "indexValue\x00pk"
				if indexDef.Unique {
					indexTree.Put([]byte(indexKey), key)
				} else {
					compoundKey := indexKey + "\x00" + string(key)
					indexTree.Put([]byte(compoundKey), key)
				}
			}
		}
	}

	// Record DDL undo entry for transaction rollback
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:    undoCreateIndex,
			indexName: stmt.Index,
		})
	}

	return c.storeIndexDef(indexDef)
}

func (c *Catalog) storeIndexDef(index *IndexDef) error {
	key := []byte("idx:" + index.Name)
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}

	if c.tree != nil {
		return c.tree.Put(key, data)
	}
	return nil
}

func (c *Catalog) GetIndex(name string) (*IndexDef, error) {
	index, exists := c.indexes[name]
	if !exists {
		return nil, ErrIndexNotFound
	}
	return index, nil
}

func (c *Catalog) findUsableIndex(tableName string, where query.Expression) (string, string, interface{}) {
	return c.findUsableIndexWithArgs(tableName, where, nil)
}

func (c *Catalog) findUsableIndexWithArgs(tableName string, where query.Expression, args []interface{}) (string, string, interface{}) {
	if where == nil {
		return "", "", nil
	}

	switch expr := where.(type) {
	case *query.BinaryExpr:
		// Recurse into AND conditions to find an indexed column
		if expr.Operator == query.TokenAnd {
			if name, col, val := c.findUsableIndexWithArgs(tableName, expr.Left, args); name != "" {
				return name, col, val
			}
			return c.findUsableIndexWithArgs(tableName, expr.Right, args)
		}

		if expr.Operator == query.TokenEq {
			// Check if left side is a column identifier
			if ident, ok := expr.Left.(*query.Identifier); ok {
				colName := ident.Name
				// Check if there's an index on this column
				for idxName, idxDef := range c.indexes {
					if idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
						// Get the value to search for
						searchVal := c.extractLiteralValue(expr.Right, args)
						if searchVal == nil {
							continue
						}
						return idxName, colName, searchVal
					}
				}
			}
			// Also check right side: value = column
			if ident, ok := expr.Right.(*query.Identifier); ok {
				colName := ident.Name
				for idxName, idxDef := range c.indexes {
					if idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
						searchVal := c.extractLiteralValue(expr.Left, args)
						if searchVal == nil {
							continue
						}
						return idxName, colName, searchVal
					}
				}
			}
		}
	}
	return "", "", nil
}

func (c *Catalog) extractLiteralValue(expr query.Expression, args []interface{}) interface{} {
	switch v := expr.(type) {
	case *query.NumberLiteral:
		return v.Value
	case *query.StringLiteral:
		return v.Value
	case *query.BooleanLiteral:
		return v.Value
	case *query.PlaceholderExpr:
		if args != nil && v.Index < len(args) {
			return args[v.Index]
		}
		return nil
	default:
		return nil
	}
}

func (c *Catalog) useIndexForQuery(tableName string, where query.Expression) (map[string]bool, bool) {
	return c.useIndexForQueryWithArgs(tableName, where, nil)
}

func (c *Catalog) useIndexForQueryWithArgs(tableName string, where query.Expression, args []interface{}) (map[string]bool, bool) {
	// Only use index for exact equality conditions
	// Range scans are more complex and can have edge cases with composite keys
	idxName, _, searchVal := c.findUsableIndexWithArgs(tableName, where, args)
	if idxName != "" && searchVal != nil {
		return c.useIndexForExactMatch(idxName, searchVal)
	}

	return nil, false
}

func (c *Catalog) useIndexForExactMatch(idxName string, searchVal interface{}) (map[string]bool, bool) {
	idxDef, idxExists := c.indexes[idxName]
	if !idxExists {
		return nil, false
	}

	indexTree, exists := c.indexTrees[idxName]
	if !exists {
		return nil, false
	}

	indexKey := fmt.Sprintf("%v", searchVal)
	result := make(map[string]bool)

	if idxDef.Unique {
		// For unique indexes, just do a point lookup
		pkData, err := indexTree.Get([]byte(indexKey))
		if err != nil {
			// No matching rows
			return result, true
		}
		result[string(pkData)] = true
		return result, true
	}

	// For non-unique indexes, we need to scan the range for matching keys
	// Non-unique indexes store: "value\x00pk" -> "pk" to allow multiple rows per value
	startKey := indexKey + "\x00"
	endKey := indexKey + "\x00\xff"

	iter, err := indexTree.Scan([]byte(startKey), []byte(endKey))
	if err != nil {
		return result, true
	}
	defer iter.Close()

	for iter.HasNext() {
		_, pkData, err := iter.Next()
		if err != nil {
			break
		}
		result[string(pkData)] = true
	}

	return result, true
}

func (c *Catalog) findRangeCondition(tableName string, where query.Expression, args []interface{}) *IndexRangeCondition {
	if where == nil {
		return nil
	}

	switch expr := where.(type) {
	case *query.BinaryExpr:
		// Recurse into AND conditions - try to combine both sides
		if expr.Operator == query.TokenAnd {
			leftCond := c.findRangeCondition(tableName, expr.Left, args)
			rightCond := c.findRangeCondition(tableName, expr.Right, args)

			// If both sides have range conditions on the same column and index, combine them
			if leftCond != nil && rightCond != nil &&
				leftCond.IdxName == rightCond.IdxName &&
				leftCond.ColName == rightCond.ColName {
				combined := &IndexRangeCondition{
					IdxName: leftCond.IdxName,
					ColName: leftCond.ColName,
				}

				// Combine lower bounds (take the max)
				if leftCond.LowerBound != nil && rightCond.LowerBound != nil {
					if compareValues(leftCond.LowerBound, rightCond.LowerBound) > 0 {
						combined.LowerBound = leftCond.LowerBound
						combined.LowerInclusive = leftCond.LowerInclusive
					} else {
						combined.LowerBound = rightCond.LowerBound
						combined.LowerInclusive = rightCond.LowerInclusive
					}
				} else if leftCond.LowerBound != nil {
					combined.LowerBound = leftCond.LowerBound
					combined.LowerInclusive = leftCond.LowerInclusive
				} else if rightCond.LowerBound != nil {
					combined.LowerBound = rightCond.LowerBound
					combined.LowerInclusive = rightCond.LowerInclusive
				}

				// Combine upper bounds (take the min)
				if leftCond.UpperBound != nil && rightCond.UpperBound != nil {
					if compareValues(leftCond.UpperBound, rightCond.UpperBound) < 0 {
						combined.UpperBound = leftCond.UpperBound
						combined.UpperInclusive = leftCond.UpperInclusive
					} else {
						combined.UpperBound = rightCond.UpperBound
						combined.UpperInclusive = rightCond.UpperInclusive
					}
				} else if leftCond.UpperBound != nil {
					combined.UpperBound = leftCond.UpperBound
					combined.UpperInclusive = leftCond.UpperInclusive
				} else if rightCond.UpperBound != nil {
					combined.UpperBound = rightCond.UpperBound
					combined.UpperInclusive = rightCond.UpperInclusive
				}

				return combined
			}

			// Return the first valid condition found
			if leftCond != nil {
				return leftCond
			}
			return rightCond
		}

		// Check for comparison operators
		var colName string
		var value interface{}
		var isRangeOp bool

		switch expr.Operator {
		case query.TokenGt, query.TokenGte:
			if ident, ok := expr.Left.(*query.Identifier); ok {
				colName = ident.Name
				value = c.extractLiteralValue(expr.Right, args)
				isRangeOp = true
			}
		case query.TokenLt, query.TokenLte:
			if ident, ok := expr.Left.(*query.Identifier); ok {
				colName = ident.Name
				value = c.extractLiteralValue(expr.Right, args)
				isRangeOp = true
			}
		}

		if isRangeOp && colName != "" && value != nil {
			// Find an index on this column
			for idxName, idxDef := range c.indexes {
				if idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
					cond := &IndexRangeCondition{
						IdxName: idxName,
						ColName: colName,
						IsExact: false,
					}

					switch expr.Operator {
					case query.TokenGt, query.TokenGte:
						cond.LowerBound = value
						cond.LowerInclusive = (expr.Operator == query.TokenGte)
					case query.TokenLt, query.TokenLte:
						cond.UpperBound = value
						cond.UpperInclusive = (expr.Operator == query.TokenLte)
					}

					return cond
				}
			}
		}

	case *query.BetweenExpr:
		if ident, ok := expr.Expr.(*query.Identifier); ok {
			colName := ident.Name
			lowerVal := c.extractLiteralValue(expr.Lower, args)
			upperVal := c.extractLiteralValue(expr.Upper, args)

			if lowerVal != nil && upperVal != nil {
				// Find an index on this column
				for idxName, idxDef := range c.indexes {
					if idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
						return &IndexRangeCondition{
							IdxName:        idxName,
							ColName:        colName,
							LowerBound:     lowerVal,
							UpperBound:     upperVal,
							LowerInclusive: !expr.Not,
							UpperInclusive: !expr.Not,
							IsExact:        false,
						}
					}
				}
			}
		}
	}

	return nil
}

func (c *Catalog) useIndexForRangeScan(cond *IndexRangeCondition) (map[string]bool, bool) {
	idxDef, idxExists := c.indexes[cond.IdxName]
	if !idxExists {
		return nil, false
	}

	indexTree, exists := c.indexTrees[cond.IdxName]
	if !exists {
		return nil, false
	}

	result := make(map[string]bool)

	// Build scan range keys
	var startKey, endKey []byte

	if cond.LowerBound != nil {
		lowerStr := fmt.Sprintf("%v", cond.LowerBound)
		if cond.LowerInclusive {
			startKey = []byte(lowerStr)
		} else {
			// Start just after the lower bound
			startKey = []byte(lowerStr + "\x00")
		}
	}

	if cond.UpperBound != nil {
		upperStr := fmt.Sprintf("%v", cond.UpperBound)
		if cond.UpperInclusive {
			// For inclusive upper bound, we need to scan all keys starting with upperStr
			// Keys are stored as "value\x00pk", so "70\x01" will include "70\x00pk"
			// but exclude "71\x00pk" (since "71" > "70\x01")
			endKey = []byte(upperStr + "\x01")
		} else {
			// For exclusive upper bound, end at the upper bound
			// "70" will exclude "70\x00pk" but include "69\x00pk"
			endKey = []byte(upperStr)
		}
	}

	if idxDef.Unique {
		// For unique indexes, scan the range directly
		iter, err := indexTree.Scan(startKey, endKey)
		if err != nil {
			return result, true
		}
		defer iter.Close()

		for iter.HasNext() {
			_, pkData, err := iter.Next()
			if err != nil {
				break
			}
			result[string(pkData)] = true
		}
	} else {
		// For non-unique indexes, we need to handle the compound key structure
		// Keys are stored as: "value\x00pk" -> "pk"
		iter, err := indexTree.Scan(startKey, endKey)
		if err != nil {
			return result, true
		}
		defer iter.Close()

		for iter.HasNext() {
			_, pkData, err := iter.Next()
			if err != nil {
				break
			}
			result[string(pkData)] = true
		}
	}

	return result, true
}

func (c *Catalog) DropIndex(name string) error {
	idxDef, exists := c.indexes[name]
	if !exists {
		return ErrIndexNotFound
	}

	// Record DDL undo entry for transaction rollback
	if c.txnActive {
		c.undoLog = append(c.undoLog, undoEntry{
			action:    undoDropIndex,
			indexName: name,
			indexDef:  idxDef,
			indexTree: c.indexTrees[name],
		})
	}

	delete(c.indexes, name)
	delete(c.indexTrees, name)

	if c.tree != nil {
		key := []byte("idx:" + name)
		return c.tree.Delete(key)
	}
	return nil
}