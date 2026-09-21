package engine

import (
	"context"
	"testing"
)

// Regression for the executeTriggersList WHEN-swallow (the buffered insert
// path's trigger executor, catalog_insert.go): an explicit-txn INSERT runs
// its triggers through executeTriggersList, whose WHEN-condition evaluation
// error was silently skipped (`if err != nil { continue }`) — the trigger's
// side effects silently missing while the statement reported success. This
// mirrors the round-4 fix in the locked executeTriggers (catalog_ddl.go),
// which only covered autocommit inserts; evaluation errors must fail the
// statement on BOTH trigger executors.
func TestBufferedPathTriggerWhenErrorFailsStatement(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`CREATE TABLE t (id INTEGER PRIMARY KEY, d INTEGER)`,
		`CREATE TABLE audit (id INTEGER)`,
		`CREATE TRIGGER trg AFTER INSERT ON t WHEN (100 / NEW.d > 10) BEGIN INSERT INTO audit VALUES (NEW.id); END`,
		`INSERT INTO t VALUES (1, 5)`, // control: WHEN true, autocommit (locked path)
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}

	// Control: the satisfiable WHEN fired the trigger on the locked path.
	var cnt int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM audit`).Scan(&cnt); err != nil {
		t.Fatalf("audit count: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("control trigger did not fire: audit rows=%d, want 1", cnt)
	}

	// The buffered path: an explicit-txn INSERT whose WHEN errors (100/0).
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, execErr := tx.Exec(ctx, `INSERT INTO t VALUES (2, 0)`)
	if execErr == nil {
		_ = tx.Rollback()
		t.Fatal("INSERT whose trigger WHEN errors (division by zero) succeeded; " +
			"the buffered-path trigger was silently skipped (executeTriggersList swallow)")
	}
	if rbErr := tx.Rollback(); rbErr != nil {
		t.Fatalf("rollback: %v", rbErr)
	}

	// The erroring statement must not have inserted its row, and the audit
	// trail must be unchanged.
	var tCnt int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM t`).Scan(&tCnt); err != nil {
		t.Fatalf("t count: %v", err)
	}
	if tCnt != 1 {
		t.Fatalf("failed INSERT left %d rows in t, want 1 (statement must roll back)", tCnt)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM audit`).Scan(&cnt); err != nil {
		t.Fatalf("audit count after rollback: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("audit rows=%d after rollback, want 1 (no silent trigger skip)", cnt)
	}
}
