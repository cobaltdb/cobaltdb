package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSearchVectorKNN tests the SearchVectorKNN function
func TestSearchVectorKNN(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with VECTOR column
	stmt := &query.CreateTableStmt{
		Table: "vector_items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}

	if err := c.CreateTable(stmt); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert test vectors
	vectors := []struct {
		id     float64
		vector []float64
	}{
		{1, []float64{1.0, 0.0, 0.0}},
		{2, []float64{0.0, 1.0, 0.0}},
		{3, []float64{0.0, 0.0, 1.0}},
		{4, []float64{0.9, 0.1, 0.0}},
		{5, []float64{0.5, 0.5, 0.0}},
	}

	for _, v := range vectors {
		_, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "vector_items",
			Columns: []string{"id", "embedding"},
			Values:  [][]query.Expression{{numReal(v.id), vectorReal(v.vector)}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert vector %v: %v", v.id, err)
		}
	}

	// Create vector index
	if err := c.CreateVectorIndex("idx_knn_test", "vector_items", "embedding"); err != nil {
		t.Fatalf("CreateVectorIndex failed: %v", err)
	}

	// Test SearchVectorKNN
	queryVec := []float64{1.0, 0.0, 0.0}
	keys, dists, err := c.SearchVectorKNN("idx_knn_test", queryVec, 3)
	if err != nil {
		t.Fatalf("SearchVectorKNN failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("Expected 3 results, got %d", len(keys))
	}

	if len(dists) != 3 {
		t.Errorf("Expected 3 distances, got %d", len(dists))
	}

	// Verify distances are non-negative
	for i, d := range dists {
		if d < 0 {
			t.Errorf("Distance %d is negative: %v", i, d)
		}
	}

	// Test error case: non-existent index
	_, _, err = c.SearchVectorKNN("nonexistent_idx", queryVec, 3)
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

// TestSearchVectorRange tests the SearchVectorRange function
func TestSearchVectorRange(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with VECTOR column
	stmt := &query.CreateTableStmt{
		Table: "range_items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "vec", Type: query.TokenVector, Dimensions: 2},
		},
	}

	if err := c.CreateTable(stmt); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert test vectors in a grid pattern
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			id := float64(i*5 + j + 1)
			vec := []float64{float64(i), float64(j)}
			_, _, err := c.Insert(ctx, &query.InsertStmt{
				Table:   "range_items",
				Columns: []string{"id", "vec"},
				Values:  [][]query.Expression{{numReal(id), vectorReal(vec)}},
			}, nil)
			if err != nil {
				t.Fatalf("Failed to insert vector %v: %v", id, err)
			}
		}
	}

	// Create vector index
	if err := c.CreateVectorIndex("idx_range_test", "range_items", "vec"); err != nil {
		t.Fatalf("CreateVectorIndex failed: %v", err)
	}

	// Test SearchVectorRange with small radius (should get few results)
	queryVec := []float64{0.0, 0.0}
	keys, dists, err := c.SearchVectorRange("idx_range_test", queryVec, 1.5)
	if err != nil {
		t.Fatalf("SearchVectorRange failed: %v", err)
	}

	// Should find at least the point at (0,0) and possibly nearby points
	if len(keys) == 0 {
		t.Error("Expected at least one result for range search")
	}

	// All distances should be within radius
	for i, d := range dists {
		if d > 1.5 {
			t.Errorf("Distance %d (%v) exceeds radius 1.5", i, d)
		}
	}

	// Test with larger radius
	keys, dists, err = c.SearchVectorRange("idx_range_test", queryVec, 3.0)
	if err != nil {
		t.Fatalf("SearchVectorRange with larger radius failed: %v", err)
	}

	// Should find more results with larger radius
	t.Logf("Found %d results with radius 3.0", len(keys))

	// Test error case: non-existent index
	_, _, err = c.SearchVectorRange("nonexistent_idx", queryVec, 1.0)
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

// TestIndexRowForVector tests the indexRowForVector function
func TestIndexRowForVector(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	stmt := &query.CreateTableStmt{
		Table: "index_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}

	if err := c.CreateTable(stmt); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create index
	if err := c.CreateVectorIndex("idx_row_test", "index_test", "embedding"); err != nil {
		t.Fatalf("CreateVectorIndex failed: %v", err)
	}

	// Get the index
	vidx, _ := c.GetVectorIndex("idx_row_test")

	// Test with valid vector
	rowData := []interface{}{1, []float64{1.0, 2.0, 3.0}}
	c.indexRowForVector(vidx, rowData, []byte("key1"), 1)

	// Test with nil vector (should not panic)
	rowDataNil := []interface{}{2, nil}
	c.indexRowForVector(vidx, rowDataNil, []byte("key2"), 1)

	// Test with invalid column index (should not panic)
	rowDataValid := []interface{}{3, []float64{4.0, 5.0, 6.0}}
	c.indexRowForVector(vidx, rowDataValid, []byte("key3"), 5) // colIdx out of range

	// Test with wrong type (should not panic)
	rowDataWrong := []interface{}{4, "not a vector"}
	c.indexRowForVector(vidx, rowDataWrong, []byte("key4"), 1)

	// Test with interface slice
	rowDataInterface := []interface{}{5, []interface{}{float64(7.0), float64(8.0), float64(9.0)}}
	c.indexRowForVector(vidx, rowDataInterface, []byte("key5"), 1)

	// Test with wrong dimension vector (should not panic)
	rowDataWrongDim := []interface{}{6, []float64{1.0, 2.0}} // only 2 dimensions
	c.indexRowForVector(vidx, rowDataWrongDim, []byte("key6"), 1)
}

// TestUpdateVectorIndexes tests vector index maintenance during DML operations
func TestUpdateVectorIndexes(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	stmt := &query.CreateTableStmt{
		Table: "dml_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "vec", Type: query.TokenVector, Dimensions: 2},
		},
	}

	if err := c.CreateTable(stmt); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create index
	if err := c.CreateVectorIndex("idx_dml", "dml_test", "vec"); err != nil {
		t.Fatalf("CreateVectorIndex failed: %v", err)
	}

	// Insert rows
	for i := 1; i <= 3; i++ {
		vec := []float64{float64(i), float64(i * 2)}
		_, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "dml_test",
			Columns: []string{"id", "vec"},
			Values:  [][]query.Expression{{numReal(float64(i)), vectorReal(vec)}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Search to verify inserts were indexed
	keys, _, err := c.SearchVectorKNN("idx_dml", []float64{1.0, 2.0}, 3)
	if err != nil {
		t.Fatalf("SearchVectorKNN failed: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("Expected 3 results after insert, got %d", len(keys))
	}

	// Update a row
	_, _, err = c.Update(ctx, &query.UpdateStmt{
		Table: "dml_test",
		Set: []*query.SetClause{
			{Column: "vec", Value: vectorReal([]float64{10.0, 20.0})},
		},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Delete a row
	_, _, err = c.Delete(ctx, &query.DeleteStmt{
		Table: "dml_test",
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "id"},
			Operator: query.TokenEq,
			Right:    numReal(2),
		},
	}, nil)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

// TestHNSWUpdate tests the Update method on HNSWIndex
func TestHNSWUpdate(t *testing.T) {
	index := NewHNSWIndex("test_idx", "test_table", "embedding", 3)

	// Insert initial vector
	if err := index.Insert("key1", []float64{1.0, 0.0, 0.0}); err != nil {
		t.Fatalf("Failed to insert vector: %v", err)
	}

	// Update the vector
	if err := index.Update("key1", []float64{0.0, 1.0, 0.0}); err != nil {
		t.Fatalf("Failed to update vector: %v", err)
	}

	// Search to verify update worked
	keys, _, err := index.SearchKNN([]float64{0.0, 1.0, 0.0}, 1)
	if err != nil {
		t.Fatalf("SearchKNN failed: %v", err)
	}

	if len(keys) != 1 || keys[0] != "key1" {
		t.Errorf("Expected key1 to be nearest to updated vector, got %v", keys)
	}

	// Test update non-existent key (should not error since delete just returns nil if not found)
	if err := index.Update("nonexistent", []float64{0.0, 0.0, 1.0}); err != nil {
		t.Errorf("Update of non-existent key should not error, got: %v", err)
	}
}

// Helper to create vector expression
func vectorReal(v []float64) query.Expression {
	return &query.VectorLiteral{Values: v}
}
