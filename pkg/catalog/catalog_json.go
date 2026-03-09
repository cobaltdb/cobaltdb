package catalog

import (
	"encoding/json"
	"fmt"
	"sort"
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
		NumIndex:  make(map[float64][]int64),
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

	iter, _ := tree.Scan(nil, nil)
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

	iter.Close()
	return nil
}

func (c *Catalog) extractJSONValue(row []interface{}, column, path string) interface{} {
	// Simple implementation: find JSON column and extract path
	// TODO: Implement full JSON path resolution
	for _, val := range row {
		if jsonMap, ok := val.(map[string]interface{}); ok {
			// Simple $.key path
			if len(path) > 2 && path[0] == '$' && path[1] == '.' {
				key := path[2:]
				if v, exists := jsonMap[key]; exists {
					return v
				}
			}
		}
	}
	return nil
}

func (c *Catalog) indexJSONValue(idx *JSONIndexDef, value interface{}, rowNum int64) {
	switch v := value.(type) {
	case string:
		idx.Index[v] = append(idx.Index[v], rowNum)
	case float64:
		idx.NumIndex[v] = append(idx.NumIndex[v], rowNum)
	case int:
		idx.NumIndex[float64(v)] = append(idx.NumIndex[float64(v)], rowNum)
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
		return idx.NumIndex[v], nil
	case int:
		return idx.NumIndex[float64(v)], nil
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