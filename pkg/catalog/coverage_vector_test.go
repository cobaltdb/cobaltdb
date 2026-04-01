package catalog

import (
	"testing"
)

// TestHNSWIndexUpdate tests the HNSWIndex Update method
func TestHNSWIndexUpdate(t *testing.T) {
	// Create HNSW index
	hnsw := NewHNSWIndex("vec_idx", "vectors", "embedding", 3)

	// Insert initial vectors
	vec1 := []float64{1.0, 2.0, 3.0}
	vec2 := []float64{4.0, 5.0, 6.0}

	if err := hnsw.Insert("vec1", vec1); err != nil {
		t.Fatalf("Failed to insert vec1: %v", err)
	}
	if err := hnsw.Insert("vec2", vec2); err != nil {
		t.Fatalf("Failed to insert vec2: %v", err)
	}

	// Update vec1 with new values - this calls Delete then Insert
	newVec1 := []float64{7.0, 8.0, 9.0}
	if err := hnsw.Update("vec1", newVec1); err != nil {
		t.Errorf("Failed to update vec1: %v", err)
	}

	// Just verify the node count is still 2 (Delete + Insert = same count)
	if len(hnsw.Nodes) != 2 {
		t.Errorf("Expected 2 nodes after update, got %d", len(hnsw.Nodes))
	}

	// Test update of non-existent key (delete fails silently, then insert)
	newVec3 := []float64{10.0, 11.0, 12.0}
	if err := hnsw.Update("vec3", newVec3); err != nil {
		t.Errorf("Failed to update non-existent vec3: %v", err)
	}

	// Now there should be 3 nodes
	if len(hnsw.Nodes) != 3 {
		t.Errorf("Expected 3 nodes after adding vec3, got %d", len(hnsw.Nodes))
	}
}

// TestHNSWIndexUpdateWithSearch verifies update maintains index consistency
func TestHNSWIndexUpdateWithSearch(t *testing.T) {
	hnsw := NewHNSWIndex("vec_idx2", "vectors2", "embedding", 3)

	// Insert vectors
	hnsw.Insert("a", []float64{1.0, 0.0, 0.0})
	hnsw.Insert("b", []float64{0.0, 1.0, 0.0})
	hnsw.Insert("c", []float64{0.0, 0.0, 1.0})

	// Update vector 'a' to be closer to 'b'
	hnsw.Update("a", []float64{0.0, 0.9, 0.1})

	// Search for nearest neighbors to the new position
	results, _, err := hnsw.SearchKNN([]float64{0.0, 1.0, 0.0}, 2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected search results after update")
	}
}
