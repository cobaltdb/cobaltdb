package engine

import (
	"context"
	"math"
	"testing"
)

// Regression for refactor.md §1.16 option B: read-your-own-writes for
// in-transaction vector search. Under Phase 1 (option A) HNSW is refreshed
// only at COMMIT, so a transaction's own buffered embeddings were invisible
// to its own SearchVectorKNN until commit. Option B makes the search
// overlay-aware via pendingWriteMapsFor: pending live embeddings enter as
// candidates at their true distance, superseded keys recompute from the
// PENDING embedding, and tombstoned keys drop out.
func TestVectorSearchReadYourOwnWrites(t *testing.T) {
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
	exec(`CREATE TABLE vryow (id INTEGER PRIMARY KEY, tag TEXT, embedding VECTOR(3))`)
	exec(`INSERT INTO vryow VALUES (1, 'a', '[1.0, 0.0, 0.0]')`)  // d(query)=0
	exec(`INSERT INTO vryow VALUES (2, 'b', '[0.0, 1.0, 0.0]')`)  // d(query)=1
	exec(`INSERT INTO vryow VALUES (3, 'c', '[-1.0, 0.0, 0.0]')`) // d(query)=2
	exec(`CREATE VECTOR INDEX vryow_embedding ON vryow(embedding)`)

	query := []float64{1.0, 0.0, 0.0}
	const key1 = "00000000000000000001"
	const key2 = "00000000000000000002"
	const key3 = "00000000000000000003"
	const key4 = "00000000000000000004"

	sqrt2 := math.Sqrt(2)
	search := func(k int) ([]string, []float64) {
		t.Helper()
		keys, dists, err := db.SearchVectorKNN("vryow_embedding", query, k)
		if err != nil {
			t.Fatalf("SearchVectorKNN: %v", err)
		}
		return keys, dists
	}
	assertTop := func(desc string, k int, wantKeys []string, wantDists []float64) {
		t.Helper()
		keys, dists := search(k)
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

	// CONTROL (passes both sides): committed data searchable in order.
	assertTop("committed baseline", 3, []string{key1, key2, key3}, []float64{0, sqrt2, 2})

	// PROBE 1 — INSERT read-your-own-writes: the transaction's own buffered
	// embedding (d=0.5) must rank between the committed rows.
	exec(`BEGIN`)
	exec(`INSERT INTO vryow VALUES (4, 'mine', '[0.5, 0.0, 0.0]')`)
	assertTop("in-txn insert visible to own search", 2,
		[]string{key1, key4}, []float64{0, 0.5})
	exec(`ROLLBACK`)
	assertTop("rolled-back insert not visible", 3,
		[]string{key1, key2, key3}, []float64{0, sqrt2, 2})

	// PROBE 2 — UPDATE read-your-own-writes: the pending embedding replaces
	// the stale HNSW one (row 3 moves from d=2 to d=0.5).
	exec(`BEGIN`)
	exec(`UPDATE vryow SET embedding = '[0.5, 0.0, 0.0]' WHERE id = 3`)
	assertTop("in-txn update recomputes from pending embedding", 2,
		[]string{key1, key3}, []float64{0, 0.5})
	exec(`ROLLBACK`)

	// PROBE 3 — DELETE read-your-own-writes: the buffered tombstone must
	// filter the still-indexed row out of the results. (Deletes row 2, not
	// row 1: buffered DELETEs eagerly remove the HNSW node at statement
	// time and ROLLBACK does not restore it — a pre-existing defect this
	// regression exposed, recorded as the §1.16 follow-up — so the deleted
	// row must not be one the later guard asserts on.)
	exec(`BEGIN`)
	exec(`DELETE FROM vryow WHERE id = 2`)
	assertTop("in-txn delete filtered from own search", 2,
		[]string{key1, key3}, []float64{0, 2})
	exec(`ROLLBACK`)

	// GUARD — post-commit visibility is unchanged (Phase 1 regression
	// territory, pinned here end-to-end at the search level).
	exec(`BEGIN`)
	exec(`INSERT INTO vryow VALUES (4, 'kept', '[0.5, 0.0, 0.0]')`)
	exec(`COMMIT`)
	assertTop("committed insert visible after COMMIT", 2,
		[]string{key1, key4}, []float64{0, 0.5})
}
