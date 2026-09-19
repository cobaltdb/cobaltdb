package catalog

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

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
		Temporary:  table.Temporary,
	}

	c.indexes[stmt.Index] = indexDef
	c.indexTrees[stmt.Index] = indexTree

	if err := c.storeIndexDef(indexDef); err != nil {
		delete(c.indexes, stmt.Index)
		delete(c.indexTrees, stmt.Index)
		return err
	}

	// Build the index synchronously while holding the exclusive catalog lock.
	// A prior "online" background build (for >1000-row tables) populated the
	// index under only a read lock, concurrently with buffered DML — that raced
	// on the catalog-global c.indexTrees/c.indexes maps (a concurrent map
	// read+write can crash the process) and left the index diverged from the
	// base table (silent wrong results). Building under the exclusive lock keeps
	// DDL and DML mutually exclusive, matching the documented "DDL blocks all
	// DML" model, at the cost of blocking DML for the build duration.
	tree := c.tableTrees[stmt.Table]
	pendingWrites := c.pendingWritesForTable(stmt.Table)
	if tree != nil {
		if err := c.populateIndexVisibleRowsLocked(indexTree, indexDef, table, tree, pendingWrites); err != nil {
			delete(c.indexes, stmt.Index)
			delete(c.indexTrees, stmt.Index)
			if deleteErr := c.deleteCatalogDef("idx:" + stmt.Index); deleteErr != nil {
				return fmt.Errorf("failed to populate index %s: %w; cleanup failed: %v", stmt.Index, err, deleteErr)
			}
			return fmt.Errorf("failed to populate index %s: %w", stmt.Index, err)
		}
	}
	indexDef.Status = IndexActive

	// Record DDL undo only after the index has been created successfully.
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:    undoCreateIndex,
			indexName: stmt.Index,
		})
	}

	return nil
}

func (c *Catalog) pendingWritesForTable(tableName string) map[string]PendingWrite {
	ts := c.getCurrentTxn()
	if ts == nil || len(ts.pendingWrites) == 0 {
		return nil
	}
	return ts.getPendingWriteMap()[tableName]
}

func (c *Catalog) populateIndexVisibleRowsLocked(indexTree btree.TreeStore, indexDef *IndexDef, table *TableDef, tree btree.TreeStore, pendingWrites map[string]PendingWrite) error {
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.HasNext() {
		key, valueData, iterErr := iter.Next()
		if iterErr != nil {
			return iterErr
		}
		if _, shadowed := pendingWrites[string(key)]; shadowed {
			continue
		}
		if err := c.addIndexRowLocked(indexTree, indexDef, table, key, valueData); err != nil {
			return err
		}
	}
	for key, pw := range pendingWrites {
		if pw.Value == nil {
			continue
		}
		if err := c.addIndexRowLocked(indexTree, indexDef, table, []byte(key), pw.Value); err != nil {
			return err
		}
	}
	return nil
}

func (c *Catalog) addIndexRowLocked(indexTree btree.TreeStore, indexDef *IndexDef, table *TableDef, key, valueData []byte) error {
	row, live, err := decodeLiveRow(valueData, len(table.Columns))
	if err != nil {
		return fmt.Errorf("failed to decode row in table %s while populating index %s: %w", table.Name, indexDef.Name, err)
	}
	if !live {
		// Soft-deleted rows remain in the table tree as MVCC tombstones; live
		// indexes never carry tombstone entries (DELETE removes them), so
		// population must skip them too — otherwise a UNIQUE index built
		// after a delete spuriously rejects re-inserting that value.
		return nil
	}
	indexKey, ok := buildCompositeIndexKey(table, indexDef, row)
	if !ok {
		return nil
	}
	if indexDef.Unique {
		if existingKey, err := indexTree.Get([]byte(indexKey)); err == nil && string(existingKey) != string(key) {
			return fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, indexDef.Name)
		}
		return indexTree.Put([]byte(indexKey), key)
	}
	compoundKey := indexKey + "\x00" + string(key)
	return indexTree.Put([]byte(compoundKey), key)
}

// populateIndexLocked fills an index tree from a table scan. Must be called
// with Catalog.mu held (or with external guarantees that the table is stable).
func (c *Catalog) populateIndexLocked(indexTree btree.TreeStore, indexDef *IndexDef, table *TableDef, tree btree.TreeStore) error {
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.HasNext() {
		key, valueData, iterErr := iter.Next()
		if iterErr != nil {
			return iterErr
		}
		row, live, err := decodeLiveRow(valueData, len(table.Columns))
		if err != nil {
			return fmt.Errorf("failed to decode row in table %s while populating index %s: %w", table.Name, indexDef.Name, err)
		}
		if !live {
			// Skip MVCC tombstones: live indexes never carry entries for
			// soft-deleted rows (DELETE removes them).
			continue
		}
		indexKey, ok := buildCompositeIndexKey(table, indexDef, row)
		if !ok {
			continue
		}
		if indexDef.Unique {
			if existingKey, err := indexTree.Get([]byte(indexKey)); err == nil && string(existingKey) != string(key) {
				return fmt.Errorf("UNIQUE constraint failed: duplicate value '%v' in index %s", indexKey, indexDef.Name)
			}
			if err := indexTree.Put([]byte(indexKey), key); err != nil {
				return err
			}
		} else {
			compoundKey := indexKey + "\x00" + string(key)
			if err := indexTree.Put([]byte(compoundKey), key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Catalog) storeIndexDef(index *IndexDef) error {
	if index.Temporary {
		return nil
	}
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
		// Integer literals must keep full int64 precision: going through the
		// float64 Value corrupts values above 2^53, producing a wrong index
		// key and a silent empty result for large integer PK lookups.
		if v.Raw != "" && !strings.ContainsAny(v.Raw, ".eE") {
			if i, err := strconv.ParseInt(v.Raw, 10, 64); err == nil {
				return i
			}
		}
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

func (c *Catalog) useIndexForQueryWithArgs(tableName string, where query.Expression, args []interface{}) ([]string, bool, error) {
	// If there are pending buffered writes for this table, the index tree may
	// be stale (index updates are deferred to commit). Fall back to full scan
	// so read-your-writes works correctly.
	if ts := c.getCurrentTxn(); ts != nil && len(ts.pendingWrites) > 0 {
		if _, ok := ts.getPendingWriteMap()[tableName]; ok {
			return nil, false, nil
		}
	}

	// Only use index for exact equality conditions
	// Range scans are more complex and can have edge cases with composite keys
	idxName, colName, searchVal := c.findUsableIndexWithArgs(tableName, where, args)
	if idxName != "" && searchVal != nil {
		// Coerce the search value to the indexed column's declared type before
		// key formatting. Without this, `WHERE id = '2'` on an INTEGER PK
		// builds a string-typed key that never matches, so the result silently
		// depends on whether an index exists. If coercion fails, fall back to
		// a full scan (which applies the engine's comparison semantics) rather
		// than returning an empty result.
		coerced, ok := c.coerceIndexSearchValue(tableName, colName, searchVal)
		if !ok {
			return nil, false, nil
		}
		return c.useIndexForExactMatch(idxName, coerced)
	}

	return nil, false, nil
}

// coerceIndexSearchValue converts searchVal to the Go type produced by the
// row decoder for the given column's declared SQL type, so that index key
// formatting (formatKeyComponent / typeTaggedKey) matches the stored keys.
// Returns ok=false when the value cannot be represented in the column's type,
// in which case the caller must fall back to a full table scan.
func (c *Catalog) coerceIndexSearchValue(tableName, colName string, searchVal interface{}) (interface{}, bool) {
	table, exists := c.tables[tableName]
	if !exists {
		return searchVal, true
	}
	colIdx := table.GetColumnIndex(colName)
	if colIdx < 0 {
		return searchVal, true
	}
	return coerceValueForColumnType(searchVal, table.Columns[colIdx].Type)
}

// coerceValueForColumnType coerces val to the canonical Go representation for
// the given declared column type (int64 for integer columns, float64 for
// floating-point columns, string for text columns, bool for boolean columns).
// Returns ok=false when the value cannot be losslessly represented.
func coerceValueForColumnType(val interface{}, colType string) (interface{}, bool) {
	t := strings.ToUpper(colType)
	switch {
	case strings.Contains(t, "INT"):
		switch v := val.(type) {
		case int:
			return int64(v), true
		case int32:
			return int64(v), true
		case int64:
			return v, true
		case uint64:
			if v > math.MaxInt64 {
				return nil, false
			}
			return int64(v), true
		case float64:
			if v == float64(int64(v)) && v >= -9.007199254740992e15 && v <= 9.007199254740992e15 {
				return int64(v), true
			}
			return nil, false
		case float32:
			f := float64(v)
			if f == float64(int64(f)) {
				return int64(f), true
			}
			return nil, false
		case string:
			if i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				return i, true
			}
			if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil && f == float64(int64(f)) {
				return int64(f), true
			}
			return nil, false
		default:
			return nil, false
		}
	case strings.Contains(t, "CHAR"), strings.Contains(t, "TEXT"),
		strings.Contains(t, "CLOB"), strings.Contains(t, "STRING"):
		switch v := val.(type) {
		case string:
			return v, true
		case []byte:
			return string(v), true
		default:
			// Number/bool against a text column: comparison semantics are
			// value-dependent, so let the full scan decide.
			return nil, false
		}
	case strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"),
		strings.Contains(t, "DEC"), strings.Contains(t, "NUMERIC"):
		switch v := val.(type) {
		case float64:
			return v, true
		case float32:
			return float64(v), true
		case int:
			return float64(v), true
		case int64:
			return float64(v), true
		case string:
			if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				return f, true
			}
			return nil, false
		default:
			return nil, false
		}
	case strings.Contains(t, "BOOL"):
		if b, ok := val.(bool); ok {
			return b, true
		}
		return nil, false
	default:
		return val, true
	}
}

func (c *Catalog) useIndexForExactMatch(idxName string, searchVal interface{}) ([]string, bool, error) {
	// Special case: PRIMARY KEY lookup
	if idxName == "__PK__" {
		// Use serializePK format for consistency with table storage. Float
		// PKs must use the whole/fractional split of formatFloatKey — the
		// same encoding as the insert path and serializePK. formatKeyComponent
		// truncates fractional floats via int64(v), so rows inserted under
		// the "F:"-tagged key were invisible to equality lookups.
		if fVal, isFloat := searchVal.(float64); isFloat {
			pkKey, _, _ := formatFloatKey(fVal)
			return []string{pkKey}, true, nil
		}
		pkKey, ok := formatKeyComponent(searchVal)
		if !ok {
			pkKey = ValueToStringKey(searchVal)
		}
		return []string{pkKey}, true, nil
	}

	idxDef, idxExists := c.indexes[idxName]
	if !idxExists {
		return nil, false, nil
	}

	indexTree, exists := c.indexTrees[idxName]
	if !exists {
		return nil, false, nil
	}

	indexKey := typeTaggedKey(searchVal)
	var result []string

	if idxDef.Unique {
		// For unique indexes, just do a point lookup
		pkData, err := indexTree.Get([]byte(indexKey))
		if err != nil {
			// No matching rows
			return result, true, nil
		}
		return []string{string(pkData)}, true, nil
	}

	// For non-unique indexes, we need to scan the range for matching keys
	// Non-unique indexes store: "value\x00pk" -> "pk" to allow multiple rows per value
	startKey := indexKey + "\x00"
	endKey := indexKey + "\x00\xff"

	iter, err := indexTree.Scan([]byte(startKey), []byte(endKey))
	if err != nil {
		return nil, false, fmt.Errorf("failed to scan index %s: %w", idxName, err)
	}
	defer iter.Close()

	for iter.HasNext() {
		_, pkData, err := iter.Next()
		if err != nil {
			return nil, false, fmt.Errorf("failed to read index %s: %w", idxName, err)
		}
		result = append(result, string(pkData))
	}

	return result, true, nil
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
	if err := c.deleteCatalogDef("idx:" + name); err != nil {
		return fmt.Errorf("failed to delete index metadata %s: %w", name, err)
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

	return nil
}

func (c *Catalog) DropUniqueConstraint(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	idxDef, exists := c.indexes[name]
	if !exists {
		return ErrIndexNotFound
	}
	if !idxDef.Unique {
		return fmt.Errorf("constraint %s is not a UNIQUE constraint", name)
	}
	if err := c.deleteCatalogDef("idx:" + name); err != nil {
		return fmt.Errorf("failed to delete constraint metadata %s: %w", name, err)
	}

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
			row, live, err := decodeLiveRow(valueData, len(table.Columns))
			if err != nil {
				iter.Close()
				return fmt.Errorf("failed to decode row in table %s during index rebuild: %w", tableName, err)
			}
			if !live {
				// Skip MVCC tombstones: live indexes never carry entries for
				// soft-deleted rows (DELETE removes them).
				continue
			}
			indexKey, ok := buildCompositeIndexKey(table, idxDef, row)
			if ok {
				if idxDef.Unique {
					if err := newTree.Put([]byte(indexKey), key); err != nil {
						iter.Close()
						return fmt.Errorf("failed to write index %s during rebuild: %w", idxName, err)
					}
				} else {
					compoundKey := indexKey + "\x00" + string(key)
					if err := newTree.Put([]byte(compoundKey), key); err != nil {
						iter.Close()
						return fmt.Errorf("failed to write index %s during rebuild: %w", idxName, err)
					}
				}
			}
		}
		iter.Close()

		c.indexTrees[idxName] = newTree
		idxDef.RootPageID = newTree.RootPageID()
	}
	return nil
}
