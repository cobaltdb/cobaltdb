package catalog

import (
	"encoding/json"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tables := make([]string, 0, len(c.tables))
	for name := range c.tables {
		tables = append(tables, name)
	}
	return tables
}

func (c *Catalog) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Save table definitions to catalog tree
	for _, tableDef := range c.tables {
		if err := c.storeTableDef(tableDef); err != nil {
			return fmt.Errorf("failed to save table definition %s: %w", tableDef.Name, err)
		}
	}

	// Flush the catalog B+Tree's in-memory data to its page
	if c.tree != nil {
		if err := c.tree.Flush(); err != nil {
			return fmt.Errorf("failed to flush catalog tree: %w", err)
		}
	}

	// Flush all table B+Trees to their pages
	if err := c.flushTableTreesLocked(); err != nil {
		return fmt.Errorf("failed to flush table trees: %w", err)
	}

	// Flush buffer pool to ensure all pages are written to disk
	if c.pool != nil {
		if err := c.pool.FlushAll(); err != nil {
			return fmt.Errorf("failed to flush buffer pool: %w", err)
		}
	}

	return nil
}

func (c *Catalog) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tree == nil {
		return nil
	}

	// Load table definitions from catalog tree
	// Table data is loaded on-demand via the buffer pool
	iter, err := c.tree.Scan([]byte("tbl:"), []byte("tbl;"))
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Valid() {
		key, value, err := iter.Next()
		if err != nil {
			break
		}

		// Parse key to get table name
		keyStr := string(key)
		if !strings.HasPrefix(keyStr, "tbl:") {
			continue
		}
		tableName := strings.TrimPrefix(keyStr, "tbl:")

		// Unmarshal table definition
		var tableDef TableDef
		if err := json.Unmarshal(value, &tableDef); err != nil {
			continue
		}

		// Restore DEFAULT and CHECK expressions from persisted strings
		for i := range tableDef.Columns {
			if tableDef.Columns[i].Default != "" && tableDef.Columns[i].defaultExpr == nil {
				if parsed, err := query.ParseExpression(tableDef.Columns[i].Default); err == nil {
					tableDef.Columns[i].defaultExpr = parsed
				}
			}
			if tableDef.Columns[i].CheckStr != "" && tableDef.Columns[i].Check == nil {
				if parsed, err := query.ParseExpression(tableDef.Columns[i].CheckStr); err == nil {
					tableDef.Columns[i].Check = parsed
				}
			}
		}

		c.tables[tableName] = &tableDef

		// Create or open B+Tree for the table
		if tableDef.RootPageID != 0 {
			c.tableTrees[tableName] = btree.OpenBTree(c.pool, tableDef.RootPageID)
		} else {
			tree, err := btree.NewBTree(c.pool)
			if err != nil {
				continue
			}
			tableDef.RootPageID = tree.RootPageID()
			c.tableTrees[tableName] = tree
		}

		// Build column index cache
		tableDef.buildColumnIndexCache()
	}

	return nil
}

// SaveData exports all table schemas and row data as JSON files to dir.
// It writes a schema.json containing all table definitions and one <table>.json
// per table containing key/value data.
func (c *Catalog) SaveData(dir string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("save data: cannot create directory %s: %w", dir, err)
	}

	// Export schema
	schema := map[string]interface{}{
		"tables": c.tables,
	}
	schemaData, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("save data: failed to marshal schema: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "schema.json"), schemaData, 0644); err != nil {
		return fmt.Errorf("save data: failed to write schema.json: %w", err)
	}

	// Export each table's data
	for tableName, tree := range c.tableTrees {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("save data: failed to scan table %s: %w", tableName, err)
		}

		var keys [][]byte
		var values [][]byte
		for iter.HasNext() {
			k, v, iterErr := iter.Next()
			if iterErr != nil {
				iter.Close()
				return fmt.Errorf("save data: failed to read from table %s: %w", tableName, iterErr)
			}
			if k == nil {
				break
			}
			keys = append(keys, k)
			values = append(values, v)
		}
		iter.Close()

		tableData := map[string]interface{}{
			"keys":   keys,
			"values": values,
		}
		data, err := json.Marshal(tableData)
		if err != nil {
			return fmt.Errorf("save data: failed to marshal table %s: %w", tableName, err)
		}
		if err := os.WriteFile(filepath.Join(dir, tableName+".json"), data, 0644); err != nil {
			return fmt.Errorf("save data: failed to write %s.json: %w", tableName, err)
		}
	}

	return nil
}

// LoadSchema reads schema.json from dir and recreates the table definitions
// and their B+Trees in the catalog.
func (c *Catalog) LoadSchema(dir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schemaPath := filepath.Join(dir, "schema.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no schema file, nothing to load
		}
		return fmt.Errorf("load schema: failed to read %s: %w", schemaPath, err)
	}

	var schema struct {
		Tables map[string]*TableDef `json:"tables"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("load schema: failed to parse %s: %w", schemaPath, err)
	}

	for name, tableDef := range schema.Tables {
		// Restore DEFAULT and CHECK expressions from persisted strings
		for i := range tableDef.Columns {
			if tableDef.Columns[i].Default != "" && tableDef.Columns[i].defaultExpr == nil {
				if parsed, parseErr := query.ParseExpression(tableDef.Columns[i].Default); parseErr == nil {
					tableDef.Columns[i].defaultExpr = parsed
				}
			}
			if tableDef.Columns[i].CheckStr != "" && tableDef.Columns[i].Check == nil {
				if parsed, parseErr := query.ParseExpression(tableDef.Columns[i].CheckStr); parseErr == nil {
					tableDef.Columns[i].Check = parsed
				}
			}
		}
		tableDef.buildColumnIndexCache()

		c.tables[name] = tableDef

		// Create or open B+Tree for the table
		if tableDef.RootPageID != 0 && c.pool != nil {
			c.tableTrees[name] = btree.OpenBTree(c.pool, tableDef.RootPageID)
		} else {
			var tree *btree.BTree
			if c.pool != nil {
				tree, err = btree.NewBTree(c.pool)
				if err != nil {
					return fmt.Errorf("load schema: failed to create tree for %s: %w", name, err)
				}
				tableDef.RootPageID = tree.RootPageID()
			}
			if tree != nil {
				c.tableTrees[name] = tree
			}
		}

		// Persist to catalog tree
		if c.tree != nil {
			_ = c.storeTableDef(tableDef)
		}
	}

	return nil
}

// LoadData reads <table>.json data files from dir and inserts rows into the tables.
// Each file contains keys/values arrays that are put directly into the table's B+Tree.
func (c *Catalog) LoadData(dir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for tableName := range c.tables {
		dataPath := filepath.Join(dir, tableName+".json")
		data, err := os.ReadFile(dataPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue // no data file for this table
			}
			return fmt.Errorf("load data: failed to read %s: %w", dataPath, err)
		}

		var tableData struct {
			Keys   [][]byte `json:"keys"`
			Values [][]byte `json:"values"`
		}
		if err := json.Unmarshal(data, &tableData); err != nil {
			return fmt.Errorf("load data: failed to parse %s: %w", dataPath, err)
		}

		tree, exists := c.tableTrees[tableName]
		if !exists {
			// Create tree if missing
			if c.pool != nil {
				tree, err = btree.NewBTree(c.pool)
				if err != nil {
					return fmt.Errorf("load data: failed to create tree for %s: %w", tableName, err)
				}
				c.tableTrees[tableName] = tree
			} else {
				continue
			}
		}

		for i, key := range tableData.Keys {
			if i >= len(tableData.Values) {
				break
			}
			if err := tree.Put(key, tableData.Values[i]); err != nil {
				return fmt.Errorf("load data: failed to insert into %s: %w", tableName, err)
			}
		}
	}

	return nil
}

func (c *Catalog) Vacuum() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// In a real implementation, this would:
	// 1. Rebuild all B-trees to eliminate fragmentation
	// 2. Remove deleted entries
	// 3. Compact storage

	// For now, we'll do a simple compaction of table trees using Scan
	for name, tree := range c.tableTrees {
		// Use Scan to get all entries
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("vacuum: failed to scan table %s: %w", name, err)
		}

		// Collect all entries
		type entry struct {
			key   []byte
			value []byte
		}
		var entries []entry
		for iter.HasNext() {
			key, value, iterErr := iter.Next()
			if iterErr != nil {
				iter.Close()
				return fmt.Errorf("vacuum: failed to read entry from table %s: %w", name, iterErr)
			}
			if key == nil {
				break
			}
			entries = append(entries, entry{key: key, value: value})
		}
		iter.Close()

		if len(entries) == 0 {
			continue
		}

		// Create a new tree and copy data
		newTree, err := btree.NewBTree(c.pool)
		if err != nil {
			return fmt.Errorf("vacuum: failed to create new tree for table %s: %w", name, err)
		}
		for _, e := range entries {
			if err := newTree.Put(e.key, e.value); err != nil {
				return fmt.Errorf("vacuum: failed to copy entry in table %s: %w", name, err)
			}
		}

		c.tableTrees[name] = newTree
	}

	// Compact index trees
	for name, tree := range c.indexTrees {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("vacuum: failed to scan index %s: %w", name, err)
		}

		type entry struct {
			key   []byte
			value []byte
		}
		var entries []entry
		for iter.HasNext() {
			key, value, iterErr := iter.Next()
			if iterErr != nil {
				iter.Close()
				return fmt.Errorf("vacuum: failed to read entry from index %s: %w", name, iterErr)
			}
			if key == nil {
				break
			}
			entries = append(entries, entry{key: key, value: value})
		}
		iter.Close()

		if len(entries) == 0 {
			continue
		}

		newTree, err := btree.NewBTree(c.pool)
		if err != nil {
			return fmt.Errorf("vacuum: failed to create new tree for index %s: %w", name, err)
		}
		for _, e := range entries {
			if err := newTree.Put(e.key, e.value); err != nil {
				return fmt.Errorf("vacuum: failed to copy entry in index %s: %w", name, err)
			}
		}

		c.indexTrees[name] = newTree
	}

	return nil
}

func (c *Catalog) Analyze(tableName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Use Scan to iterate over all entries
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	var rowCount int64
	// Analyze each column - first pass: collect all values
	columnValues := make(map[string][]interface{})
	nullCounts := make(map[string]int64)

	for iter.HasNext() {
		_, value, iterErr := iter.Next()
		if iterErr != nil {
			return fmt.Errorf("analyze: failed to read row: %w", iterErr)
		}
		if value == nil {
			break
		}
		rowCount++
		vrow, err := decodeVersionedRow(value, len(table.Columns))
		if err != nil {
			continue
		}
		rowSlice := vrow.Data
		for i, col := range table.Columns {
			if i >= len(rowSlice) || rowSlice[i] == nil {
				nullCounts[col.Name]++
			} else {
				columnValues[col.Name] = append(columnValues[col.Name], rowSlice[i])
			}
		}
	}

	stats := &StatsTableStats{
		TableName:    tableName,
		RowCount:     uint64(rowCount),
		ColumnStats:  make(map[string]*ColumnStats),
		LastAnalyzed: time.Now(),
	}

	// Analyze each column
	for _, col := range table.Columns {
		values := columnValues[col.Name]
		colStats := &ColumnStats{
			ColumnName: col.Name,
		}

		// Count distinct values
		valueSet := make(map[string]bool)
		for _, val := range values {
			valueSet[fmt.Sprintf("%v", val)] = true
		}

		colStats.DistinctCount = uint64(len(valueSet))
		colStats.NullCount = uint64(nullCounts[col.Name])

		// Find min/max
		if len(values) > 0 {
			minVal := values[0]
			maxVal := values[0]
			for _, val := range values[1:] {
				if catalogCompareValues(val, minVal) < 0 {
					minVal = val
				}
				if catalogCompareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}
			colStats.MinValue = minVal
			colStats.MaxValue = maxVal
		}

		stats.ColumnStats[col.Name] = colStats
	}

	c.stats[tableName] = stats
	return nil
}
