package engine

import (
	"context"
	"strconv"
	"testing"
)

// Regression for the applyUndoEntry/undoDelete vector-restoration gap: a
// transaction that mixes DDL with DML routes ROLLBACK through
// replayUndoLog → applyUndoEntry, whose undoDelete case restored only the
// B-tree row. The REPLACE path removes or overwrites the row's HNSW entries
// immediately, so the restored row was left with no (or stale) vector-index
// content — invisible to vector search — while the identical REPLACE+ROLLBACK
// through the DML-only path (applyDMLUndoEntry, fixed in ns-12) re-indexed
// it correctly.
func TestTxnDDLMixedRollbackRestoresVectorIndex(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec(t, db, "CREATE TABLE vitems (id INTEGER PRIMARY KEY, name TEXT, embedding VECTOR(3))")
	mustExec(t, db, "CREATE VECTOR INDEX vitems_emb_idx ON vitems (embedding)")
	mustExec(t, db, "INSERT INTO vitems VALUES (1,'a','[1.0,0.0,0.0]'),(2,'b','[0.0,1.0,0.0]'),(3,'c','[0.0,0.0,1.0]')")

	knn := func(query []float64) map[string]bool {
		t.Helper()
		ids, _, err := db.SearchVectorKNN("vitems_emb_idx", query, 3)
		if err != nil {
			t.Fatalf("SearchVectorKNN: %v", err)
		}
		out := make(map[string]bool, len(ids))
		for _, id := range ids {
			// SearchVectorKNN returns zero-padded uint64 id strings.
			if u, perr := strconv.ParseUint(id, 10, 64); perr == nil {
				id = strconv.FormatUint(u, 10)
			}
			out[id] = true
		}
		return out
	}
	rowPresent := func(id int) bool {
		t.Helper()
		rows, err := db.Query(ctx, "SELECT id FROM vitems WHERE id = ?", id)
		if err != nil {
			t.Fatalf("query row %d: %v", id, err)
		}
		defer rows.Close()
		present := false
		for rows.Next() {
			present = true
		}
		return present
	}

	// Baseline: all three rows searchable via the HNSW index.
	base := knn([]float64{0, 0, 0})
	for _, id := range []string{"1", "2", "3"} {
		if !base[id] {
			t.Fatalf("baseline: row %s not found in the vector index (got %v)", id, base)
		}
	}

	// CONTROL: REPLACE + ROLLBACK with NO DDL in the transaction — the undo
	// replays through applyDMLUndoEntry, whose undoDelete restores the vector
	// index (ns-12). Must pass.
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "REPLACE INTO vitems VALUES (3, 'c3', '[0.0,0.0,1.5]')"); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if !knn([]float64{0, 0, 0})["3"] {
		t.Fatal("control: rolled-back REPLACE lost row 3 from the vector index (applyDMLUndoEntry path)")
	}
	if !rowPresent(3) {
		t.Fatal("control: row 3 missing from the table after rollback")
	}

	// PROBE: the same REPLACE + ROLLBACK, but with a DDL statement in the
	// transaction — the undo log now contains a DDL entry, so ROLLBACK
	// replays through applyUndoEntry, whose undoDelete case (pre-fix)
	// skipped vector restoration.
	tx, err = db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "CREATE TABLE scratch (id INTEGER)"); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "REPLACE INTO vitems VALUES (2, 'b2', '[0.0,1.0,0.5]')"); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// The row must be back in the table ...
	if !rowPresent(2) {
		t.Fatal("row 2 missing from the table after rollback (B-tree restore failed)")
	}
	// ... and searchable via the vector index.
	if !knn([]float64{0, 1, 0})["2"] {
		t.Fatal("vector-layer drift: DDL-mixed rollback restored row 2 to the table but left it unsearchable via the HNSW index (applyUndoEntry undoDelete skipped vector restoration)")
	}
}
