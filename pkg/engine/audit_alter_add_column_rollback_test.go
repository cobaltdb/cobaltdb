package engine

import (
	"context"
	"testing"
)

// TestAlterAddColumnRollbackRestoresRowData is a regression test for a DDL
// transaction-rollback data-corruption bug: ALTER TABLE ADD COLUMN backfills
// existing rows by writing directly to the storage tree, but the undo entry
// only restored table.Columns. A ROLLBACK therefore left the backfilled
// trailing value in storage. A subsequent re-add of the column then observed
// the stale value (its backfill guard skips rows that already carry the extra
// slot), surfacing the wrong data.
func TestAlterAddColumnRollbackRestoresRowData(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatal(err)
	}
	exec := func(sql string) {
		t.Helper()
		if _, e := db.Exec(ctx, sql); e != nil {
			t.Fatalf("exec %q: %v", sql, e)
		}
	}
	scan := func(sql string) interface{} {
		t.Helper()
		var v interface{}
		if e := db.QueryRow(ctx, sql).Scan(&v); e != nil {
			t.Fatalf("query %q: %v", sql, e)
		}
		return v
	}

	exec("CREATE TABLE t (id INTEGER PRIMARY KEY)")
	exec("INSERT INTO t VALUES (1)")

	// Add column inside a transaction (backfills the existing row with 'x'),
	// then roll back. The backfill must be undone.
	exec("BEGIN")
	exec("ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'x'")
	exec("ROLLBACK")

	// Re-add the column with a different default; the existing row must pick up
	// the new default 'y', not the rolled-back 'x'.
	exec("ALTER TABLE t ADD COLUMN c TEXT DEFAULT 'y'")
	if got := scan("SELECT c FROM t WHERE id=1"); got != "y" {
		t.Fatalf("stale backfill after ROLLBACK of ADD COLUMN: got %v, want \"y\"", got)
	}
}
