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
	defer c.invalidateSchemaCache()
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
		Status:     IndexBuilding,
	}

	c.indexes[stmt.Index] = indexDef
	c.indexTrees[stmt.Index] = indexTree

	// Record DDL undo entry for transaction rollback
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:    undoCreateIndex,
			indexName: stmt.Index,
		})
	}

	if err := c.storeIndexDef(indexDef); err != nil {
		return err
	}

	// For small tables (≤1000 rows) build synchronously so tests and
	// lightweight DDL scripts get immediate index availability.
	// Large tables get a background build so the main thread returns
	// in <100ms and doesn't block concurrent reads/writes.
	tree := c.tableTrees[stmt.Table]
	if tree != nil && tree.Size() <= 1000 {
		c.populateIndexLocked(indexTree, indexDef, table, tree)
		indexDef.Status = IndexActive
	} else {
		go c.buildIndexInBackground(stmt.Index, stmt.Table, table)
	}

	return nil
}

// populateIndexLocked fills an index tree from a table scan. Must be called
// with Catalog.mu held (or with external guarantees that the table is stable).
func (c *Catalog) populateIndexLocked(indexTree btree.TreeStore, indexDef *IndexDef, table *TableDef, tree btree.TreeStore) {
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return
	}
	defer iter.Close()
	for iter.HasNext() {
		key, valueData, iterErr := iter.Next()
		if iterErr != nil {
			break
		}
		row, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			continue
		}
		indexKey, ok := buildCompositeIndexKey(table, indexDef, row)
		if !ok {
			continue
		}
		if indexDef.Unique {
			_ = indexTree.Put([]byte(indexKey), key)
		} else {
			compoundKey := indexKey + "\x00" + string(key)
			_ = indexTree.Put([]byte(compoundKey), key)
		}
	}
}

// buildIndexInBackground scans the table and populates the index.
// It runs without holding Catalog.mu so reads/writes are not blocked.
func (c *Catalog) buildIndexInBackground(indexName, tableName string, table *TableDef) {
	c.mu.RLock()
	idxDef, ok := c.indexes[indexName]
	idxTree, treeOk := c.indexTrees[indexName]
	tree, tableOk := c.tableTrees[tableName]
	c.mu.RUnlock()

	if !ok || !treeOk || !tableOk || idxDef.Status != IndexBuilding {
		return
	}

	c.populateIndexLocked(idxTree, idxDef, table, tree)

	c.mu.Lock()
	if def, exists := c.indexes[indexName]; exists && def.Status == IndexBuilding {
		def.Status = IndexActive
	}
	c.mu.Unlock()
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
	return cloneIndexDef(index), nil
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
					if idxDef.Status == IndexActive && idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
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
					if idxDef.Status == IndexActive && idxDef.TableName == tableName && len(idxDef.Columns) > 0 && idxDef.Columns[0] == colName {
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

func (c *Catalog) useIndexForQueryWithArgs(tableName string, where query.Expression, args []interface{}) ([]string, bool) {
	// If there are pending buffered writes for this table, the index tree may
	// be stale (index updates are deferred to commit). Fall back to full scan
	// so read-your-writes works correctly.
	if ts := c.getCurrentTxn(); ts != nil && len(ts.pendingWrites) > 0 {
		if _, ok := ts.getPendingWriteMap()[tableName]; ok {
			return nil, false
		}
	}

	// Only use index for exact equality conditions
	// Range scans are more complex and can have edge cases with composite keys
	idxName, _, searchVal := c.findUsableIndexWithArgs(tableName, where, args)
	if idxName != "" && searchVal != nil {
		return c.useIndexForExactMatch(idxName, searchVal)
	}

	return nil, false
}

func (c *Catalog) useIndexForExactMatch(idxName string, searchVal interface{}) ([]string, bool) {
	// Special case: PRIMARY KEY lookup
	if idxName == "__PK__" {
		// Use serializePK format for consistency with table storage
		pkKey, ok := formatKeyComponent(searchVal)
		if !ok {
			pkKey = ValueToStringKey(searchVal)
		}
		return []string{pkKey}, true
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
	var result []string

	if idxDef.Unique {
		// For unique indexes, just do a point lookup
		pkData, err := indexTree.Get([]byte(indexKey))
		if err != nil {
			// No matching rows
			return result, true
		}
		return []string{string(pkData)}, true
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
		result = append(result, string(pkData))
	}

	return result, true
}

// ListIndexesByTable returns all regular indexes grouped by table name.
// Each entry is the list of columns for that index. Primary keys are
// included as an index on the table.
func (c *Catalog) ListIndexesByTable() map[string][][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string][][]string)
	for _, idxDef := range c.indexes {
		result[idxDef.TableName] = append(result[idxDef.TableName], idxDef.Columns)
	}
	// Treat primary keys as existing indexes
	for _, tbl := range c.tables {
		if len(tbl.PrimaryKey) > 0 {
			result[tbl.Name] = append(result[tbl.Name], tbl.PrimaryKey)
		}
	}
	return result
}

func (c *Catalog) DropIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	idxDef, exists := c.indexes[name]
	if !exists {
		return ErrIndexNotFound
	}

	// Record DDL undo entry for transaction rollback
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
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

// rebuildTableIndexesLocked rebuilds all regular B-tree indexes for a single
// table by scanning the table data and repopulating each index.  Must be called
// with c.mu held.
func (c *Catalog) rebuildTableIndexesLocked(tableName string) error {
	table, exists := c.tables[tableName]
	if !exists {
		return nil
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return nil
	}

	for idxName, idxDef := range c.indexes {
		if idxDef.TableName != tableName {
			continue
		}

		// Replace the index tree with a fresh one.
		newTree, err := btree.NewBTree(c.pool)
		if err != nil {
			return fmt.Errorf("failed to create new index tree for %s: %w", idxName, err)
		}

		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to scan table %s for index rebuild: %w", tableName, err)
		}
		for iter.HasNext() {
			key, valueData, iterErr := iter.Next()
			if iterErr != nil {
				iter.Close()
				return fmt.Errorf("failed to read row during index rebuild: %w", iterErr)
			}
			row, err := decodeRow(valueData, len(table.Columns))
			if err != nil {
				continue
			}
			indexKey, ok := buildCompositeIndexKey(table, idxDef, row)
			if ok {
				if idxDef.Unique {
					_ = newTree.Put([]byte(indexKey), key)
				} else {
					compoundKey := indexKey + "\x00" + string(key)
					_ = newTree.Put([]byte(compoundKey), key)
				}
			}
		}
		iter.Close()

		c.indexTrees[idxName] = newTree
		idxDef.RootPageID = newTree.RootPageID()
	}
	return nil
}
