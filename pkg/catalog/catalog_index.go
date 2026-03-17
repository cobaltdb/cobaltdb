package catalog

import (
	"encoding/json"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func (c *Catalog) CreateIndex(stmt *query.CreateIndexStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to scan table for index population: %w", err)
		}
		defer iter.Close()
		for iter.HasNext() {
			key, valueData, iterErr := iter.Next()
			if iterErr != nil {
				return fmt.Errorf("failed to read row during index population: %w", iterErr)
			}
			row, err := decodeRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			indexKey, ok := buildCompositeIndexKey(table, indexDef, row)
			if ok {
				// For non-unique indexes, use compound key: "indexValue\x00pk"
				var putErr error
				if indexDef.Unique {
					putErr = indexTree.Put([]byte(indexKey), key)
				} else {
					compoundKey := indexKey + "\x00" + string(key)
					putErr = indexTree.Put([]byte(compoundKey), key)
				}
				if putErr != nil {
					return fmt.Errorf("failed to populate index: %w", putErr)
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	index, exists := c.indexes[name]
	if !exists {
		return nil, ErrIndexNotFound
	}
	return index, nil
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
				// Check if this is the PRIMARY KEY column
				if table, exists := c.tables[tableName]; exists && len(table.PrimaryKey) == 1 && table.PrimaryKey[0] == colName {
					searchVal := c.extractLiteralValue(expr.Right, args)
					if searchVal != nil {
						return "__PK__", colName, searchVal
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
				// Check if this is the PRIMARY KEY column
				if table, exists := c.tables[tableName]; exists && len(table.PrimaryKey) == 1 && table.PrimaryKey[0] == colName {
					searchVal := c.extractLiteralValue(expr.Left, args)
					if searchVal != nil {
						return "__PK__", colName, searchVal
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
	// Special case: PRIMARY KEY lookup
	if idxName == "__PK__" {
		result := make(map[string]bool)
		pkKey := typeTaggedKey(searchVal)
		result[pkKey] = true
		return result, true
	}

	idxDef, idxExists := c.indexes[idxName]
	if !idxExists {
		return nil, false
	}

	indexTree, exists := c.indexTrees[idxName]
	if !exists {
		return nil, false
	}

	indexKey := typeTaggedKey(searchVal)
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

func (c *Catalog) DropIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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