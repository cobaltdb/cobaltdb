package engine

import (
	"context"
	"testing"
)

// Regression: a failed multi-row INSERT OR REPLACE inside an explicit
// transaction permanently lost the evicted committed row. rollbackInsertErr
// truncated `1 + len(stmtInserts)` undo entries, but a REPLACE row appends
// TWO (the eviction undoDelete from resolvePKConflict plus its own
// undoInsert), so the truncation discarded the eviction restoration record
// while rollbackStatementInserts (which only deletes inserted keys) could
// not restore the evicted row. The statement must roll back atomically:
// the pre-statement row survives with its original values.
func TestFailedMultiRowReplaceKeepsEvictedRow(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT CHECK (name <> 'boom'))`,
		`INSERT INTO t VALUES (1, 'original')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Row 1 REPLACEs the committed row (eviction + two undo entries);
	// row 2 violates the CHECK constraint and fails the statement.
	_, err = tx.Exec(ctx, `INSERT OR REPLACE INTO t VALUES (1, 'replacement'), (2, 'boom')`)
	if err == nil {
		t.Fatal("expected the CHECK violation to fail the statement")
	}

	// Statement-level atomicity: the failed statement's own effects are rolled
	// back immediately (the replacement row is deleted), and the eviction is
	// restored when the transaction rolls back. After ROLLBACK the original
	// committed row must be intact — pre-fix, the eviction's undo entry was
	// truncated away and the row stayed lost.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	rows, qerr := db.Query(ctx, `SELECT id, name FROM t`)
	if qerr != nil {
		t.Fatalf("select: %v", qerr)
	}
	defer rows.Close()
	var gotID int
	var gotName string
	n := 0
	for rows.Next() {
		n++
		if n > 1 {
			continue
		}
		if err := rows.Scan(&gotID, &gotName); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	if n != 1 {
		t.Fatalf("table has %d rows after ROLLBACK, want exactly 1 "+
			"(the evicted committed row was permanently lost)", n)
	}
	if gotID != 1 || gotName != "original" {
		t.Fatalf("surviving row = (%d, %q), want (1, \"original\")", gotID, gotName)
	}
}
