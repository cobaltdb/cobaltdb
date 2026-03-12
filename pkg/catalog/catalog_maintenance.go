package catalog

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
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

func (c *Catalog) SaveData(dir string) error {
	return c.Save()
}

func (c *Catalog) LoadSchema(dir string) error {
	return nil
}

func (c *Catalog) LoadData(dir string) error {
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
			continue
		}

		// Collect all entries
		type entry struct {
			key   []byte
			value []byte
		}
		var entries []entry
		for iter.HasNext() {
			key, value, _ := iter.Next()
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
			continue
		}
		for _, e := range entries {
			if err := newTree.Put(e.key, e.value); err != nil {
				// Handle error during Put
				continue
			}
		}

		c.tableTrees[name] = newTree
	}

	// Compact index trees
	for name, tree := range c.indexTrees {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			continue
		}

		type entry struct {
			key   []byte
			value []byte
		}
		var entries []entry
		for iter.HasNext() {
			key, value, _ := iter.Next()
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
			continue
		}
		for _, e := range entries {
			newTree.Put(e.key, e.value)
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
		_, value, _ := iter.Next()
		if value == nil {
			break
		}
		rowCount++
		var rowSlice []interface{}
		if err := json.Unmarshal(value, &rowSlice); err != nil {
			continue
		}
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