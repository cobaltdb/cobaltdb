package catalog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func (c *Catalog) CreateFTSIndex(name, tableName string, columns []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.ftsIndexes[name]; exists {
		return fmt.Errorf("FTS index %s already exists", name)
	}

	// Verify table exists
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	// Verify columns exist
	for _, col := range columns {
		if table.GetColumnIndex(col) == -1 {
			return fmt.Errorf("column %s not found in table %s", col, tableName)
		}
	}

	ftsIndex := &FTSIndexDef{
		Name:      name,
		TableName: tableName,
		Columns:   columns,
		Index:     make(map[string][]int64),
	}

	// Build the index from existing data
	tree, exists := c.tableTrees[tableName]
	if exists {
		iter, err := tree.Scan(nil, nil)
		if err == nil {
			defer iter.Close()
			for iter.HasNext() {
				key, value, iterErr := iter.Next()
				if iterErr != nil {
					break
				}
				if key == nil || len(value) == 0 {
					break
				}
				// CobaltDB stores rows as []interface{} arrays, not maps
				var rowSlice []interface{}
				if err := json.Unmarshal(value, &rowSlice); err != nil {
					continue
				}
				// Convert to map using column definitions
				row := make(map[string]interface{})
				for i, col := range table.Columns {
					if i < len(rowSlice) {
						row[col.Name] = rowSlice[i]
					}
				}
				c.indexRowForFTS(ftsIndex, row, key)
			}
		}
	}

	c.ftsIndexes[name] = ftsIndex
	return nil
}

func (c *Catalog) indexRowForFTS(ftsIndex *FTSIndexDef, row map[string]interface{}, key []byte) {
	// Extract row ID from key - use a simple hash of the key
	rowID := int64(0)
	for _, b := range key {
		rowID = rowID*31 + int64(b)
	}
	if rowID < 0 {
		rowID = -rowID
	}

	// Index each column
	for _, col := range ftsIndex.Columns {
		if val, exists := row[col]; exists && val != nil {
			text := fmt.Sprintf("%v", val)
			words := tokenize(text)
			for _, word := range words {
				word = strings.ToLower(word)
				ftsIndex.Index[word] = append(ftsIndex.Index[word], rowID)
			}
		}
	}
}

func (c *Catalog) DropFTSIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.ftsIndexes[name]; !exists {
		return fmt.Errorf("FTS index %s not found", name)
	}

	delete(c.ftsIndexes, name)
	return nil
}

func (c *Catalog) SearchFTS(indexName string, query string) ([]int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ftsIndex, exists := c.ftsIndexes[indexName]
	if !exists {
		return nil, fmt.Errorf("FTS index %s not found", indexName)
	}

	words := tokenize(query)
	if len(words) == 0 {
		return []int64{}, nil
	}

	// Find rows matching all words (AND logic)
	var result []int64
	first := true

	for _, word := range words {
		word = strings.ToLower(word)
		rows, exists := ftsIndex.Index[word]
		if !exists {
			return []int64{}, nil // Word not found, no matches
		}

		if first {
			result = append([]int64{}, rows...)
			first = false
		} else {
			// Intersection
			result = intersectSorted(result, rows)
			if len(result) == 0 {
				return []int64{}, nil
			}
		}
	}

	return result, nil
}

func (c *Catalog) GetFTSIndex(name string) (*FTSIndexDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ftsIndex, exists := c.ftsIndexes[name]
	if !exists {
		return nil, fmt.Errorf("FTS index %s not found", name)
	}
	return ftsIndex, nil
}

func (c *Catalog) ListFTSIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.ftsIndexes))
	for name := range c.ftsIndexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}