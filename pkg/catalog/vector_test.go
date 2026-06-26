package catalog

import (
	"fmt"
	"math"
	"sort"
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

func TestHNSWSelectNeighborsByKeyUsesVectorDistance(t *testing.T) {
	index := NewHNSWIndex("test_idx", "test_table", "embedding", 2)
	index.Nodes["near"] = &HNSWNode{Key: "near", Vector: []float64{0.1, 0.1}}
	index.Nodes["mid"] = &HNSWNode{Key: "mid", Vector: []float64{1.0, 1.0}}
	index.Nodes["far"] = &HNSWNode{Key: "far", Vector: []float64{10.0, 10.0}}

	got := index.selectNeighborsByKey([]float64{0, 0}, []string{"far", "missing", "mid", "near"}, 2)
	want := []string{"near", "mid"}
	if len(got) != len(want) {
		t.Fatalf("neighbors = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("neighbors = %v, want %v", got, want)
		}
	}
}

func TestHNSWDeleteHandlesUnevenNeighborLevels(t *testing.T) {
	index := NewHNSWIndex("test_idx", "test_table", "embedding", 3)
	index.Nodes["high"] = &HNSWNode{
		Key:       "high",
		Vector:    []float64{1, 0, 0},
		Level:     2,
		Neighbors: [][]string{{"low"}, {"low"}, {"low"}},
	}
	index.Nodes["low"] = &HNSWNode{
		Key:       "low",
		Vector:    []float64{0, 1, 0},
		Level:     0,
		Neighbors: [][]string{{"high"}},
	}
	index.EntryPoint = index.Nodes["high"]
	index.EntryPointKey = "high"
	index.MaxLevel = 2

	if err := index.Delete("high"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, ok := index.Nodes["high"]; ok {
		t.Fatal("deleted node remains in index")
	}
	if got := index.Nodes["low"].Neighbors[0]; len(got) != 0 {
		t.Fatalf("low-level neighbor list = %v, want empty", got)
	}
	if index.EntryPoint != index.Nodes["low"] {
		t.Fatal("entry point was not reassigned to remaining node")
	}
}

func TestHNSWUpdateRejectsDimensionMismatchWithoutDeletingNode(t *testing.T) {
	index := NewHNSWIndex("test_idx", "test_table", "embedding", 3)
	if err := index.Insert("1", []float64{1, 0, 0}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := index.Update("1", []float64{1, 0}); err == nil {
		t.Fatal("Update with dimension mismatch succeeded")
	}
	if _, ok := index.Nodes["1"]; !ok {
		t.Fatal("dimension mismatch deleted existing node")
	}
}

func TestRandomUnitFloat64Range(t *testing.T) {
	for i := 0; i < 1000; i++ {
		f, err := randomUnitFloat64()
		if err != nil {
			t.Fatalf("randomUnitFloat64 failed: %v", err)
		}
		if math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f >= 1 {
			t.Fatalf("randomUnitFloat64 = %v, want finite value in [0, 1)", f)
		}
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

// TestHNSWSearchKNNRecall verifies that HNSW KNN returns the NEAREST neighbors
// (high recall vs brute force) and in ascending-distance order. The prior bug
// used a min-heap for the result set, so eviction dropped the nearest neighbors
// and the output was reversed — KNN returned the farthest neighbors (recall ~0).
func TestHNSWSearchKNNRecall(t *testing.T) {
	const dim = 8
	const n = 1500
	const k = 10
	index := NewHNSWIndex("recall_idx", "t", "v", dim)

	// Deterministic pseudo-random vectors via a simple LCG (no global rand).
	lcg := uint64(0x9e3779b97f4a7c15)
	nextFloat := func() float64 {
		lcg = lcg*6364136223846793005 + 1442695040888963407
		return float64(lcg>>11) / float64(uint64(1)<<53)
	}

	vectors := make(map[string][]float64, n)
	for i := 0; i < n; i++ {
		v := make([]float64, dim)
		for d := 0; d < dim; d++ {
			v[d] = nextFloat()
		}
		key := fmt.Sprintf("k%d", i)
		vectors[key] = v
		if err := index.Insert(key, v); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
	}

	const queries = 50
	totalRecall := 0.0
	for q := 0; q < queries; q++ {
		query := make([]float64, dim)
		for d := 0; d < dim; d++ {
			query[d] = nextFloat()
		}

		// Brute-force ground truth: the k nearest by L2.
		type kd struct {
			key  string
			dist float64
		}
		all := make([]kd, 0, n)
		for key, v := range vectors {
			all = append(all, kd{key, l2Distance(query, v)})
		}
		sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
		truth := make(map[string]bool, k)
		for i := 0; i < k; i++ {
			truth[all[i].key] = true
		}

		keys, dists, err := index.SearchKNN(query, k)
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		if len(keys) != k {
			t.Fatalf("expected %d results, got %d", k, len(keys))
		}
		// Results must be ascending by distance (closest first).
		for i := 1; i < len(dists); i++ {
			if dists[i] < dists[i-1] {
				t.Fatalf("results not ascending by distance: %v", dists)
			}
		}
		hit := 0
		for _, key := range keys {
			if truth[key] {
				hit++
			}
		}
		totalRecall += float64(hit) / float64(k)
	}

	avgRecall := totalRecall / float64(queries)
	if avgRecall < 0.6 {
		t.Fatalf("HNSW KNN recall too low: %.3f (want >= 0.6); search likely returning farthest neighbors", avgRecall)
	}
}
