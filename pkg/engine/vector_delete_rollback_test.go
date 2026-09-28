package engine

import (
	"context"
	"math"
	"testing"
)

// Regression for the §1.16 rollback/HNSW defect (proven by the option-B
// round's guard): the BUFFERED delete path removed the HNSW node eagerly at
// statement time, and an explicit ROLLBACK discarded the tombstone without
// re-inserting the node — leaving the committed row permanently absent from
// SearchVectorKNN. The fix defers the node deletion to COMMIT (the Phase-2
// tombstone sync); in-transaction visibility is the option-B overlay's job.
func TestVectorDeleteRollbackKeepsRowSearchable(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	exec := func(sql string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	exec(`CREATE TABLE vdel (id INTEGER PRIMARY KEY, tag TEXT, embedding VECTOR(3))`)
	exec(`INSERT INTO vdel VALUES (1, 'a', '[1.0, 0.0, 0.0]')`)
	exec(`INSERT INTO vdel VALUES (2, 'b', '[0.0, 1.0, 0.0]')`)
	exec(`INSERT INTO vdel VALUES (3, 'c', '[-1.0, 0.0, 0.0]')`)
	exec(`CREATE VECTOR INDEX vdel_embedding ON vdel(embedding)`)

	query := []float64{1.0, 0.0, 0.0}
	const key1 = "00000000000000000001"
	const key2 = "00000000000000000002"
	const key3 = "00000000000000000003"

	sqrt2 := math.Sqrt(2)
	assertTop := func(desc string, k int, wantKeys []string, wantDists []float64) {
		t.Helper()
		keys, dists, err := db.SearchVectorKNN("vdel_embedding", query, k)
		if err != nil {
			t.Fatalf("%s: SearchVectorKNN: %v", desc, err)
		}
		if len(keys) != len(wantKeys) {
			t.Fatalf("%s: got keys %v, want %v", desc, keys, wantKeys)
		}
		for i, wk := range wantKeys {
			if keys[i] != wk {
				t.Fatalf("%s: got keys %v, want %v", desc, keys, wantKeys)
			}
			if math.Abs(dists[i]-wantDists[i]) > 1e-9 {
				t.Fatalf("%s: got dists %v, want %v", desc, dists, wantDists)
			}
		}
	}

	// Baseline control (passes both sides).
	assertTop("committed baseline", 3, []string{key1, key2, key3}, []float64{0, sqrt2, 2})

	// PROBE — the defect: a rolled-back buffered DELETE must leave the
	// committed row searchable at its true distance.
	exec(`BEGIN`)
	exec(`DELETE FROM vdel WHERE id = 1`)
	// In-txn own-search guard (option B overlay): the tombstone filters the
	// row from this transaction's own search (passes both sides — pre-fix
	// via the eager node deletion, post-fix via the overlay).
	assertTop("in-txn delete filtered from own search", 2,
		[]string{key2, key3}, []float64{sqrt2, 2})
	exec(`ROLLBACK`)
	// RED pre-fix: key1's HNSW node was deleted eagerly at statement time
	// and ROLLBACK never restored it — the committed row vanished from
	// vector search permanently.
	assertTop("rolled-back delete keeps committed row searchable", 3,
		[]string{key1, key2, key3}, []float64{0, sqrt2, 2})

	// CONTROL — the committed path still removes the node at COMMIT (the
	// Phase-2 tombstone sync), so a COMMITTED delete is honored by search.
	exec(`BEGIN`)
	exec(`DELETE FROM vdel WHERE id = 3`)
	exec(`COMMIT`)
	assertTop("committed delete removes row from search", 2,
		[]string{key1, key2}, []float64{0, sqrt2})
}
