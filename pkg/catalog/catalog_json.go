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
	defer c.invalidateSchemaCache()

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
	table, exists := c.tables[idx.TableName]
	if !exists {
		return ErrTableNotFound
	}
	colIdx := table.GetColumnIndex(idx.Column)
	if colIdx < 0 {
		return ErrColumnNotFound
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
			return fmt.Errorf("failed to read row while building JSON index %s: %w", idx.Name, err)
		}

		values, err := decodeRow(valueData, len(table.Columns))
		if err != nil {
			return fmt.Errorf("failed to decode row in table %s while building JSON index %s: %w", idx.TableName, idx.Name, err)
		}

		// Extract JSON value using path
		jsonVal, err := c.extractJSONColumnPath(values, colIdx, idx.Path)
		if err != nil {
			return fmt.Errorf("failed to extract JSON path %s from table %s column %s: %w", idx.Path, idx.TableName, idx.Column, err)
		}
		if jsonVal != nil {
			c.indexJSONValue(idx, jsonVal, rowNum)
		}
		rowNum++
	}

	return nil
}

func (c *Catalog) extractJSONColumnPath(row []interface{}, colIdx int, path string) (interface{}, error) {
	if colIdx < 0 || colIdx >= len(row) {
		return nil, nil
	}
	doc, err := normalizeJSONDocument(row[colIdx])
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}
	return extractJSONPathValue(doc, path), nil
}

func normalizeJSONDocument(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case map[string]interface{}, []interface{}:
		return v, nil
	case string:
		var doc interface{}
		if err := json.Unmarshal([]byte(v), &doc); err != nil {
			return nil, err
		}
		return doc, nil
	case []byte:
		var doc interface{}
		if err := json.Unmarshal(v, &doc); err != nil {
			return nil, err
		}
		return doc, nil
	default:
		return v, nil
	}
}

func extractJSONPathValue(current interface{}, path string) interface{} {
	if path == "$" || path == "" {
		return current
	}
	if !strings.HasPrefix(path, "$") {
		return nil
	}

	rest := path[1:]
	for len(rest) > 0 {
		switch rest[0] {
		case '.':
			rest = rest[1:]
			nextSep := len(rest)
			for i := 0; i < len(rest); i++ {
				if rest[i] == '.' || rest[i] == '[' {
					nextSep = i
					break
				}
			}
			if nextSep == 0 {
				return nil
			}
			key := rest[:nextSep]
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil
			}
			current, ok = obj[key]
			if !ok {
				return nil
			}
			rest = rest[nextSep:]
		case '[':
			end := strings.IndexByte(rest, ']')
			if end <= 1 {
				return nil
			}
			idx, err := strconv.Atoi(rest[1:end])
			if err != nil {
				return nil
			}
			arr, ok := current.([]interface{})
			if !ok || idx < 0 || idx >= len(arr) {
				return nil
			}
			current = arr[idx]
			rest = rest[end+1:]
		default:
			return nil
		}
	}
	return current
}

func (c *Catalog) extractJSONValue(row []interface{}, column, path string) interface{} {
	// Enhanced JSON path resolution supporting nested paths like $.key1.key2.key3
	for _, val := range row {
		doc, err := normalizeJSONDocument(val)
		if err != nil || doc == nil {
			continue
		}
		if current := extractJSONPathValue(doc, path); current != nil {
			return current
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
		var strVal string
		if v {
			strVal = "true"
		} else {
			strVal = "false"
		}
		idx.Index[strVal] = append(idx.Index[strVal], rowNum)
	}
}

func (c *Catalog) DropJSONIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

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
