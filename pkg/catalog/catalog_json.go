package catalog

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

func (c *Catalog) CreateJSONIndex(name, tableName, column, path, dataType string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.jsonIndexes[name]; exists {
		return fmt.Errorf("JSON index '%s' already exists", name)
	}

	// Check if table exists
	table, exists := c.tables[tableName]
	if !exists {
		return ErrTableNotFound
	}

	// Check if column exists and is JSON type
	colExists := false
	for _, col := range table.Columns {
		if col.Name == column {
			colExists = true
			if col.Type != "JSON" {
				return fmt.Errorf("column '%s' is not JSON type", column)
			}
			break
		}
	}
	if !colExists {
		return ErrColumnNotFound
	}

	// Create the index
	jsonIndex := &JSONIndexDef{
		Name:      name,
		TableName: tableName,
		Column:    column,
		Path:      path,
		DataType:  dataType,
		Index:     make(map[string][]int64),
		NumIndex:  make(map[string][]int64),
	}

	// Index existing data
	if err := c.buildJSONIndex(jsonIndex); err != nil {
		return fmt.Errorf("failed to build JSON index: %w", err)
	}

	c.jsonIndexes[name] = jsonIndex
	return nil
}

func (c *Catalog) buildJSONIndex(idx *JSONIndexDef) error {
	tree, exists := c.tableTrees[idx.TableName]
	if !exists {
		return nil
	}

	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return fmt.Errorf("failed to scan table for JSON index: %w", err)
	}
	defer iter.Close()
	rowNum := int64(0)

	for iter.HasNext() {
		_, valueData, err := iter.Next()
		if err != nil {
			continue
		}

		var values []interface{}
		if err := json.Unmarshal(valueData, &values); err != nil {
			continue
		}

		// Extract JSON value using path
		jsonVal := c.extractJSONValue(values, idx.Column, idx.Path)
		if jsonVal != nil {
			c.indexJSONValue(idx, jsonVal, rowNum)
		}
		rowNum++
	}

	return nil
}

func (c *Catalog) extractJSONValue(row []interface{}, column, path string) interface{} {
	// Enhanced JSON path resolution supporting nested paths like $.key1.key2.key3
	for _, val := range row {
		if jsonMap, ok := val.(map[string]interface{}); ok {
			// Handle $.key or $.key1.key2.key3 paths
			if len(path) > 2 && path[0] == '$' && path[1] == '.' {
				keys := strings.Split(path[2:], ".")
				var current interface{} = jsonMap
				for i, key := range keys {
					switch curr := current.(type) {
					case map[string]interface{}:
						next, exists := curr[key]
						if !exists {
							current = nil
							break // Key not found
						}
						current = next
					default:
						// Can't traverse further if not a map
						if i < len(keys)-1 {
							current = nil
						}
					}
				}
				if current != nil {
					return current
				}
			}
		}
	}
	return nil
}

// float64Key converts float64 to string key for JSON index
// Uses integer representation for whole numbers to avoid precision issues
func float64Key(f float64) string {
	// For integers that fit in int64, use integer representation
	if f == float64(int64(f)) && f >= float64(math.MinInt64) && f <= float64(math.MaxInt64) {
		return strconv.FormatInt(int64(f), 10)
	}
	// For true floats, use full precision
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func (c *Catalog) indexJSONValue(idx *JSONIndexDef, value interface{}, rowNum int64) {
	switch v := value.(type) {
	case string:
		idx.Index[v] = append(idx.Index[v], rowNum)
	case float64:
		idx.NumIndex[float64Key(v)] = append(idx.NumIndex[float64Key(v)], rowNum)
	case int:
		idx.NumIndex[float64Key(float64(v))] = append(idx.NumIndex[float64Key(float64(v))], rowNum)
	case bool:
		strVal := fmt.Sprintf("%t", v)
		idx.Index[strVal] = append(idx.Index[strVal], rowNum)
	}
}

func (c *Catalog) DropJSONIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.jsonIndexes[name]; !exists {
		return fmt.Errorf("JSON index '%s' not found", name)
	}

	delete(c.jsonIndexes, name)
	return nil
}

func (c *Catalog) QueryJSONIndex(indexName string, value interface{}) ([]int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, exists := c.jsonIndexes[indexName]
	if !exists {
		return nil, fmt.Errorf("JSON index '%s' not found", indexName)
	}

	switch v := value.(type) {
	case string:
		return idx.Index[v], nil
	case float64:
		return idx.NumIndex[float64Key(v)], nil
	case int:
		return idx.NumIndex[float64Key(float64(v))], nil
	default:
		return nil, fmt.Errorf("unsupported value type for JSON index query")
	}
}

func (c *Catalog) GetJSONIndex(name string) (*JSONIndexDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	idx, exists := c.jsonIndexes[name]
	if !exists {
		return nil, fmt.Errorf("JSON index '%s' not found", name)
	}
	return idx, nil
}

func (c *Catalog) ListJSONIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.jsonIndexes))
	for name := range c.jsonIndexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
