package engine

import (
	"strings"
	"testing"
)

// TestSearchVectorKNNNegativeKReturnsError pins the boundary contract of the
// public KNN API: a negative k must return an error, not panic. SearchKNN
// sliced candidates[:k] after only checking len(candidates) > k, which is
// true for every negative k — so k = -1 panicked the caller's goroutine with
// a slice-bounds panic (SearchVectorKNN is not wrapped in recover, unlike
// Exec/Query). Every other malformed input on this path (dimension mismatch,
// unknown index) returns an error; negative k now does too.
func TestSearchVectorKNNNegativeKReturnsError(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, `CREATE TABLE vt (id INTEGER PRIMARY KEY, v VECTOR(3))`)
	mustExec(t, db, `INSERT INTO vt VALUES (1, '[0.1,0.2,0.3]'), (2, '[0.9,0.9,0.9]')`)
	mustExec(t, db, `CREATE VECTOR INDEX vidx ON vt(v)`)

	// Sanity: a valid k works end-to-end (index populated, nearest first).
	keys, dists, err := db.SearchVectorKNN("vidx", []float64{0.1, 0.2, 0.3}, 1)
	if err != nil {
		t.Fatalf("valid-k search failed: %v", err)
	}
	if len(keys) != 1 || keys[0] == "" {
		t.Fatalf("valid-k search returned no neighbors: %v", keys)
	}
	if len(dists) != 1 || dists[0] < 0 {
		t.Fatalf("valid-k search returned bad distances: %v", dists)
	}

	// Probe: negative k must error, never panic.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("negative k panicked the public API instead of returning an error: %v", r)
			}
		}()
		_, _, err = db.SearchVectorKNN("vidx", []float64{0.1, 0.2, 0.3}, -1)
		if err == nil {
			t.Fatalf("negative k returned no error (silent success)")
		}
		if !strings.Contains(err.Error(), "k") {
			t.Fatalf("error should mention k, got: %v", err)
		}
	}()

	// Boundary: k = 0 stays empty-and-error-free (pre-existing behavior).
	keys0, dists0, err := db.SearchVectorKNN("vidx", []float64{0.1, 0.2, 0.3}, 0)
	if err != nil {
		t.Fatalf("k=0 should not error, got: %v", err)
	}
	if len(keys0) != 0 || len(dists0) != 0 {
		t.Fatalf("k=0 should return empty results, got %v / %v", keys0, dists0)
	}
}
