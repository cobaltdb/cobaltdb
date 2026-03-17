package catalog

import (
	"encoding/json"
	"fmt"
	"sort"
)

// CreateVectorIndex creates a new HNSW vector index on a table column
func (c *Catalog) CreateVectorIndex(name, tableName, columnName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.vectorIndexes[name]; exists {
		return fmt.Errorf("vector index %s already exists", name)
	}

	// Verify table exists
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	// Verify column exists and is VECTOR type
	colIdx := table.GetColumnIndex(columnName)
	if colIdx == -1 {
		return fmt.Errorf("column %s not found in table %s", columnName, tableName)
	}
	col := table.Columns[colIdx]
	if col.Type != "VECTOR" {
		return fmt.Errorf("column %s is not a VECTOR type", columnName)
	}
	if col.Dimensions == 0 {
		return fmt.Errorf("column %s has no dimensions specified", columnName)
	}

	// Create the HNSW index
	hnswIndex := NewHNSWIndex(name, tableName, columnName, col.Dimensions)

	vectorIndex := &VectorIndexDef{
		Name:       name,
		TableName:  tableName,
		ColumnName: columnName,
		Dimensions: col.Dimensions,
		IndexType:  "hnsw",
		HNSW:       hnswIndex,
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
				// CobaltDB stores rows as []interface{} arrays
				var rowSlice []interface{}
				if err := json.Unmarshal(value, &rowSlice); err != nil {
					continue
				}
				c.indexRowForVector(vectorIndex, rowSlice, key, colIdx)
			}
		}
	}

	c.vectorIndexes[name] = vectorIndex
	return nil
}

// indexRowForVector adds a row to the vector index
func (c *Catalog) indexRowForVector(vectorIndex *VectorIndexDef, rowSlice []interface{}, key []byte, colIdx int) {
	// Extract row key as string
	rowKey := string(key)

	// Get the vector value from the row
	if colIdx >= len(rowSlice) {
		return
	}

	vectorVal := rowSlice[colIdx]
	if vectorVal == nil {
		return
	}

	// Convert vector value to []float64
	var vector []float64
	switch v := vectorVal.(type) {
	case []float64:
		vector = v
	case []interface{}:
		vector = make([]float64, len(v))
		for i, val := range v {
			switch fv := val.(type) {
			case float64:
				vector[i] = fv
			case int:
				vector[i] = float64(fv)
			case int64:
				vector[i] = float64(fv)
			case float32:
				vector[i] = float64(fv)
			default:
				return // Invalid type
			}
		}
	default:
		return // Invalid type
	}

	// Validate dimensions
	if len(vector) != vectorIndex.Dimensions {
		return // Dimension mismatch
	}

	// Insert into HNSW index
	if vectorIndex.HNSW != nil {
		vectorIndex.HNSW.Insert(rowKey, vector)
	}
}

// DropVectorIndex removes a vector index
func (c *Catalog) DropVectorIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.vectorIndexes[name]; !exists {
		return fmt.Errorf("vector index %s not found", name)
	}

	delete(c.vectorIndexes, name)
	return nil
}

// SearchVectorKNN performs a K-nearest neighbor search on a vector index
func (c *Catalog) SearchVectorKNN(indexName string, queryVector []float64, k int) ([]string, []float64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vectorIndex, exists := c.vectorIndexes[indexName]
	if !exists {
		return nil, nil, fmt.Errorf("vector index %s not found", indexName)
	}

	if vectorIndex.HNSW == nil {
		return nil, nil, fmt.Errorf("vector index %s has no HNSW structure", indexName)
	}

	return vectorIndex.HNSW.SearchKNN(queryVector, k)
}

// SearchVectorRange performs a range search on a vector index
func (c *Catalog) SearchVectorRange(indexName string, queryVector []float64, radius float64) ([]string, []float64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vectorIndex, exists := c.vectorIndexes[indexName]
	if !exists {
		return nil, nil, fmt.Errorf("vector index %s not found", indexName)
	}

	if vectorIndex.HNSW == nil {
		return nil, nil, fmt.Errorf("vector index %s has no HNSW structure", indexName)
	}

	return vectorIndex.HNSW.SearchRange(queryVector, radius)
}

// GetVectorIndex retrieves a vector index definition
func (c *Catalog) GetVectorIndex(name string) (*VectorIndexDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vectorIndex, exists := c.vectorIndexes[name]
	if !exists {
		return nil, fmt.Errorf("vector index %s not found", name)
	}
	return vectorIndex, nil
}

// ListVectorIndexes returns a sorted list of all vector index names
func (c *Catalog) ListVectorIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.vectorIndexes))
	for name := range c.vectorIndexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// updateVectorIndexesForInsert updates all vector indexes when a row is inserted
func (c *Catalog) updateVectorIndexesForInsert(tableName string, rowSlice []interface{}, key []byte) {
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName != tableName {
			continue
		}

		// Find column index
		table, err := c.getTableLocked(tableName)
		if err != nil {
			continue
		}
		colIdx := table.GetColumnIndex(vectorIndex.ColumnName)
		if colIdx == -1 {
			continue
		}

		c.indexRowForVector(vectorIndex, rowSlice, key, colIdx)
	}
}

// updateVectorIndexesForDelete updates all vector indexes when a row is deleted
func (c *Catalog) updateVectorIndexesForDelete(tableName string, key []byte) {
	rowKey := string(key)
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName != tableName {
			continue
		}
		if vectorIndex.HNSW != nil {
			vectorIndex.HNSW.Delete(rowKey)
		}
	}
}

// updateVectorIndexesForUpdate updates all vector indexes when a row is updated
func (c *Catalog) updateVectorIndexesForUpdate(tableName string, rowSlice []interface{}, key []byte) {
	rowKey := string(key)
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName != tableName {
			continue
		}

		// Delete old entry
		if vectorIndex.HNSW != nil {
			vectorIndex.HNSW.Delete(rowKey)
		}

		// Find column index and re-insert
		table, err := c.getTableLocked(tableName)
		if err != nil {
			continue
		}
		colIdx := table.GetColumnIndex(vectorIndex.ColumnName)
		if colIdx == -1 {
			continue
		}

		c.indexRowForVector(vectorIndex, rowSlice, key, colIdx)
	}
}
