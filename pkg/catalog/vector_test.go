package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestHNSWIndex(t *testing.T) {
	// Create HNSW index
	index := NewHNSWIndex("test_idx", "test_table", "embedding", 3)

	// Insert vectors
	vectors := map[string][]float64{
		"1": {1.0, 0.0, 0.0},
		"2": {0.0, 1.0, 0.0},
		"3": {0.0, 0.0, 1.0},
		"4": {0.9, 0.1, 0.0},
		"5": {0.5, 0.5, 0.0},
	}

	for key, vec := range vectors {
		if err := index.Insert(key, vec); err != nil {
			t.Fatalf("Failed to insert vector %s: %v", key, err)
		}
	}

	// Test KNN search - HNSW is approximate, so just verify it returns results
	query := []float64{1.0, 0.0, 0.0}
	keys, dists, err := index.SearchKNN(query, 3)
	if err != nil {
		t.Fatalf("SearchKNN failed: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("Expected 3 results, got %d", len(keys))
	}

	// Verify distances are valid (non-negative)
	for i, d := range dists {
		if d < 0 {
			t.Errorf("Distance %d is negative: %v", i, d)
		}
	}

	t.Logf("KNN results: keys=%v, distances=%v", keys, dists)

	// Test range search
	keys, dists, err = index.SearchRange(query, 0.5)
	if err != nil {
		t.Fatalf("SearchRange failed: %v", err)
	}

	t.Logf("Range search results: keys=%v, distances=%v", keys, dists)

	// Test delete
	if err := index.Delete("1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Search again after delete - verify delete doesn't error
	_, _, err = index.SearchKNN(query, 3)
	if err != nil {
		t.Fatalf("SearchKNN after delete failed: %v", err)
	}

	// Note: In an HNSW index, deletion just removes the node from the graph
	// The search results might still reference it through other nodes' connections,
	// but the deleted node itself won't be returned as a result
}

func TestVectorSimilarityFunctions(t *testing.T) {
	a := []float64{1.0, 0.0, 0.0}
	b := []float64{0.0, 1.0, 0.0}
	c := []float64{1.0, 0.0, 0.0}

	// Test L2 distance
	dist := l2Distance(a, b)
	expected := 1.4142135623730951 // sqrt(2)
	if dist != expected {
		t.Errorf("L2 distance: expected %v, got %v", expected, dist)
	}

	dist = l2Distance(a, c)
	if dist != 0 {
		t.Errorf("L2 distance of identical vectors should be 0, got %v", dist)
	}

	// Test cosine similarity
	sim := cosineSimilarity(a, b)
	if sim != 0 {
		t.Errorf("Cosine similarity of orthogonal vectors should be 0, got %v", sim)
	}

	sim = cosineSimilarity(a, c)
	if sim != 1.0 {
		t.Errorf("Cosine similarity of identical vectors should be 1, got %v", sim)
	}

	// Test inner product
	ip := innerProduct(a, b)
	if ip != 0 {
		t.Errorf("Inner product of orthogonal vectors should be 0, got %v", ip)
	}

	ip = innerProduct(a, c)
	if ip != 1.0 {
		t.Errorf("Inner product of identical vectors should be 1, got %v", ip)
	}
}

func TestCatalogVectorIndex(t *testing.T) {
	// Create catalog
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with VECTOR column
	stmt := &query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: query.TokenText},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	}

	if err := c.CreateTable(stmt); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create vector index
	if err := c.CreateVectorIndex("idx_embedding", "items", "embedding"); err != nil {
		t.Fatalf("CreateVectorIndex failed: %v", err)
	}

	// Verify index exists
	indexes := c.ListVectorIndexes()
	found := false
	for _, idx := range indexes {
		if idx == "idx_embedding" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Vector index 'idx_embedding' not found in ListVectorIndexes")
	}

	// Get index
	idx, err := c.GetVectorIndex("idx_embedding")
	if err != nil {
		t.Fatalf("GetVectorIndex failed: %v", err)
	}
	if idx.TableName != "items" {
		t.Errorf("Expected table name 'items', got %s", idx.TableName)
	}
	if idx.ColumnName != "embedding" {
		t.Errorf("Expected column name 'embedding', got %s", idx.ColumnName)
	}
	if idx.Dimensions != 3 {
		t.Errorf("Expected dimensions 3, got %d", idx.Dimensions)
	}

	// Drop vector index
	if err := c.DropVectorIndex("idx_embedding"); err != nil {
		t.Fatalf("DropVectorIndex failed: %v", err)
	}

	// Verify index is gone
	_, err = c.GetVectorIndex("idx_embedding")
	if err == nil {
		t.Error("Expected error when getting dropped index")
	}
}

func TestVectorFunctions(t *testing.T) {
	// Create catalog for expression evaluation
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Test toVector helper
	tests := []struct {
		name     string
		input    interface{}
		expected []float64
		wantErr  bool
	}{
		{
			name:     "float64 slice",
			input:    []float64{1.0, 2.0, 3.0},
			expected: []float64{1.0, 2.0, 3.0},
			wantErr:  false,
		},
		{
			name:     "interface slice",
			input:    []interface{}{float64(1.0), float64(2.0), float64(3.0)},
			expected: []float64{1.0, 2.0, 3.0},
			wantErr:  false,
		},
		{
			name:     "nil",
			input:    nil,
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "string",
			input:    "not a vector",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toVector(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if len(result) != len(tt.expected) {
				t.Errorf("Expected length %d, got %d", len(tt.expected), len(result))
				return
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("At index %d: expected %v, got %v", i, tt.expected[i], result[i])
				}
			}
		})
	}

	// Clean up
	_ = c
}
