package catalog

import (
	"fmt"
	"sort"
)

// CreateVectorIndex creates a new HNSW vector index on a table column
func (c *Catalog) CreateVectorIndex(name, tableName, columnName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

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
		if err != nil {
			return fmt.Errorf("failed to scan table %s for vector index %s: %w", tableName, name, err)
		}
		defer iter.Close()
		for iter.HasNext() {
			rowKey, value, iterErr := iter.NextString()
			if iterErr != nil {
				return fmt.Errorf("failed to read row for vector index %s: %w", name, iterErr)
			}
			if rowKey == "" || len(value) == 0 {
				break
			}
			// CobaltDB stores rows as VersionedRow (with []interface{} Data)
			vrow, err := decodeVersionedRow(value, len(table.Columns))
			if err != nil {
				continue
			}
			if err := c.indexRowForVector(vectorIndex, vrow.Data, rowKey, colIdx); err != nil {
				return fmt.Errorf("failed to add row %s to vector index %s: %w", rowKey, name, err)
			}
		}
	}

	if err := c.storeVectorIndexDef(vectorIndex); err != nil {
		return fmt.Errorf("failed to persist vector index %s: %w", name, err)
	}

	c.vectorIndexes[name] = vectorIndex
	return nil
}

// indexRowForVector adds a row to the vector index.
func (c *Catalog) indexRowForVector(vectorIndex *VectorIndexDef, rowSlice []interface{}, rowKey string, colIdx int) error {

	// Get the vector value from the row
	if colIdx >= len(rowSlice) {
		return nil
	}

	vectorVal := rowSlice[colIdx]
	if vectorVal == nil {
		return nil
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
				return nil // Invalid type
			}
		}
	default:
		return nil // Invalid type
	}

	// Validate dimensions
	if len(vector) != vectorIndex.Dimensions {
		return nil // Dimension mismatch
	}

	// Insert into HNSW index
	if vectorIndex.HNSW != nil {
		if err := vectorIndex.HNSW.Insert(rowKey, vector); err != nil {
			return err
		}
	}
	return nil
}

// DropVectorIndex removes a vector index
func (c *Catalog) DropVectorIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if _, exists := c.vectorIndexes[name]; !exists {
		return fmt.Errorf("vector index %s not found", name)
	}

	if c.tree != nil {
		if err := c.tree.Delete([]byte("vec:" + name)); err != nil {
			return fmt.Errorf("failed to delete vector index %s metadata: %w", name, err)
		}
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

func (c *Catalog) hasVectorIndexForTableLocked(tableName string) bool {
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName == tableName {
			return true
		}
	}
	return false
}

// updateVectorIndexesForInsert updates all vector indexes when a row is inserted
func (c *Catalog) updateVectorIndexesForInsert(tableName string, rowSlice []interface{}, key string) error {
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

		if err := c.indexRowForVector(vectorIndex, rowSlice, key, colIdx); err != nil {
			return fmt.Errorf("failed to update vector index %s after insert: %w", vectorIndex.Name, err)
		}
		if err := c.storeVectorIndexDef(vectorIndex); err != nil {
			return fmt.Errorf("failed to persist vector index %s after insert: %w", vectorIndex.Name, err)
		}
	}
	return nil
}

// updateVectorIndexesForDelete updates all vector indexes when a row is deleted
func (c *Catalog) updateVectorIndexesForDelete(tableName string, rowKey string) error {
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName != tableName {
			continue
		}
		if vectorIndex.HNSW != nil {
			if err := vectorIndex.HNSW.Delete(rowKey); err != nil {
				return fmt.Errorf("failed to delete row from vector index %s: %w", vectorIndex.Name, err)
			}
		}
		if err := c.storeVectorIndexDef(vectorIndex); err != nil {
			return fmt.Errorf("failed to persist vector index %s after delete: %w", vectorIndex.Name, err)
		}
	}
	return nil
}

// updateVectorIndexesForUpdate updates all vector indexes when a row is updated
func (c *Catalog) updateVectorIndexesForUpdate(tableName string, rowSlice []interface{}, rowKey string) error {
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName != tableName {
			continue
		}

		// Delete old entry
		if vectorIndex.HNSW != nil {
			if err := vectorIndex.HNSW.Delete(rowKey); err != nil {
				return fmt.Errorf("failed to delete row from vector index %s before update: %w", vectorIndex.Name, err)
			}
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

		if err := c.indexRowForVector(vectorIndex, rowSlice, rowKey, colIdx); err != nil {
			return fmt.Errorf("failed to update vector index %s: %w", vectorIndex.Name, err)
		}
		if err := c.storeVectorIndexDef(vectorIndex); err != nil {
			return fmt.Errorf("failed to persist vector index %s after update: %w", vectorIndex.Name, err)
		}
	}
	return nil
}
