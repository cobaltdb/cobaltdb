package engine

import (
	"context"
	"math"
	"testing"
)

// Regression for refactor.md §1.16 option B applied to SearchVectorRange:
// read-your-own-writes for in-transaction RANGE vector search. HNSW only
// refreshes at COMMIT (option A), so the transaction's own pending
// embeddings were invisible to its own range search until commit. The
// range overlay merges pending embeddings with a radius filter instead of
// the KNN k-cap, and over-fetches the ef frontier by pendingVectorFilter
// Count so tombstone filtering cannot shrink the reachable pool.
func TestVectorRangeSearchReadYourOwnWrites(t *testing.T) {
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
	// Committed distances from query [1,0,0]: key1=0, key2=0.5, key3=3
	// (outside the radius=1.5 filter — SearchRange keeps d <= radius).
	exec(`CREATE TABLE vrange (id INTEGER PRIMARY KEY, tag TEXT, embedding VECTOR(3))`)
	exec(`INSERT INTO vrange VALUES (1, 'a', '[1.0, 0.0, 0.0]')`)
	exec(`INSERT INTO vrange VALUES (2, 'b', '[0.5, 0.0, 0.0]')`)
	exec(`INSERT INTO vrange VALUES (3, 'c', '[-2.0, 0.0, 0.0]')`)
	exec(`CREATE VECTOR INDEX vrange_embedding ON vrange(embedding)`)

	query := []float64{1.0, 0.0, 0.0}
	radius := 1.5
	const key1 = "00000000000000000001"
	const key2 = "00000000000000000002"
	const key3 = "00000000000000000003"
	const key4 = "00000000000000000004"

	assertRange := func(desc string, wantKeys []string, wantDists []float64) {
		t.Helper()
		keys, dists, err := db.SearchVectorRange("vrange_embedding", query, radius)
		if err != nil {
			t.Fatalf("%s: SearchVectorRange: %v", desc, err)
		}
		if len(keys) != len(wantKeys) {
			t.Fatalf("%s: got keys %v (dists %v), want %v", desc, keys, dists, wantKeys)
		}
		for i, wk := range wantKeys {
			if keys[i] != wk {
				t.Fatalf("%s: got keys %v (dists %v), want %v", desc, keys, dists, wantKeys)
			}
			if math.Abs(dists[i]-wantDists[i]) > 1e-9 {
				t.Fatalf("%s: got dists %v, want %v", desc, dists, wantDists)
			}
		}
	}

	// CONTROL (passes both sides): committed rows within radius, boundary
	// inclusive; key2 sits at d=0.5 and key3 outside at d=3.
	assertRange("committed baseline", []string{key1, key2}, []float64{0, 0.5})

	// PROBE 1 — INSERT read-your-own-writes: the transaction's own buffered
	// embedding at d=1.2 must appear in its own range search.
	exec(`BEGIN`)
	exec(`INSERT INTO vrange VALUES (4, 'mine', '[0.4, 0.0, 0.0]')`) // d = 0.6
	assertRange("in-txn insert visible to own range search",
		[]string{key1, key2, key4}, []float64{0, 0.5, 0.6})
	exec(`ROLLBACK`)
	assertRange("rolled-back insert not visible", []string{key1, key2}, []float64{0, 0.5})

	// PROBE 2 — UPDATE read-your-own-writes: a pending embedding that moves
	// a row INSIDE the radius (key3: d=3 → d=0.8) must be found by the
	// transaction's own range search at the pending distance.
	exec(`BEGIN`)
	exec(`UPDATE vrange SET embedding = '[0.2, 0.0, 0.0]' WHERE id = 3`) // d = 0.8
	assertRange("in-txn update recomputes from pending embedding",
		[]string{key1, key2, key3}, []float64{0, 0.5, 0.8})
	exec(`ROLLBACK`)

	// PROBE 3 — DELETE read-your-own-writes: the buffered tombstone must
	// filter the still-indexed row out of the range results.
	exec(`BEGIN`)
	exec(`DELETE FROM vrange WHERE id = 1`)
	assertRange("in-txn delete filtered from own range search",
		[]string{key2}, []float64{0.5})
	exec(`ROLLBACK`)

	// GUARD — post-commit visibility is unchanged.
	exec(`BEGIN`)
	exec(`INSERT INTO vrange VALUES (4, 'kept', '[0.4, 0.0, 0.0]')`)
	exec(`COMMIT`)
	assertRange("committed insert visible after COMMIT",
		[]string{key1, key2, key4}, []float64{0, 0.5, 0.6})
}
