package catalog

import (
	"testing"
)

// TestHNSWUpdateWithEmptyIndex tests Update on empty index
func TestHNSWUpdateWithEmptyIndex(t *testing.T) {
	hnsw := NewHNSWIndex("test_idx", "test_table", "vec_col", 3)

	// Update on empty index should work (delete fails silently, then insert)
	vec := []float64{1.0, 2.0, 3.0}
	err := hnsw.Update("key1", vec)
	if err != nil {
		t.Errorf("Update on empty index failed: %v", err)
	}

	// Verify it was added
	if len(hnsw.Nodes) != 1 {
		t.Errorf("Expected 1 node after update, got %d", len(hnsw.Nodes))
	}
}

// TestHNSWUpdateMultipleTimes tests updating same key multiple times
func TestHNSWUpdateMultipleTimes(t *testing.T) {
	hnsw := NewHNSWIndex("test_idx2", "test_table2", "vec_col2", 3)

	// Insert initial vector
	hnsw.Insert("key1", []float64{1.0, 0.0, 0.0})

	// Update multiple times
	for i := 0; i < 5; i++ {
		vec := []float64{float64(i), float64(i + 1), float64(i + 2)}
		err := hnsw.Update("key1", vec)
		if err != nil {
			t.Errorf("Update %d failed: %v", i, err)
		}
	}

	// Should still have 1 node
	if len(hnsw.Nodes) != 1 {
		t.Errorf("Expected 1 node after multiple updates, got %d", len(hnsw.Nodes))
	}
}

// TestHNSWUpdateWithDifferentDimensions tests Update with wrong dimensions
func TestHNSWUpdateWithDifferentDimensions(t *testing.T) {
	hnsw := NewHNSWIndex("test_idx3", "test_table3", "vec_col3", 3)

	// Insert with correct dimensions
	hnsw.Insert("key1", []float64{1.0, 2.0, 3.0})

	// Update with wrong dimensions - should error
	err := hnsw.Update("key1", []float64{1.0, 2.0})
	if err == nil {
		t.Error("Expected error when updating with wrong dimensions")
	}
}

// TestHNSWUpdateThenSearch tests updating and then searching
func TestHNSWUpdateThenSearch(t *testing.T) {
	hnsw := NewHNSWIndex("test_idx4", "test_table4", "vec_col4", 3)

	// Insert vectors
	hnsw.Insert("a", []float64{1.0, 0.0, 0.0})
	hnsw.Insert("b", []float64{0.0, 1.0, 0.0})
	hnsw.Insert("c", []float64{0.0, 0.0, 1.0})

	// Update 'a' to be near 'b'
	hnsw.Update("a", []float64{0.1, 0.9, 0.0})

	// Search for vectors near [0, 1, 0]
	results, _, err := hnsw.SearchKNN([]float64{0.0, 1.0, 0.0}, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) < 2 {
		t.Errorf("Expected at least 2 results, got %d", len(results))
	}

	// 'b' or updated 'a' should be in results
	found := false
	for _, r := range results {
		if r == "a" || r == "b" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected 'a' or 'b' in search results after update")
	}
}
