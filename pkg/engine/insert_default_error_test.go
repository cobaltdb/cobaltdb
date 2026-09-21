package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestInsertDefaultEvaluationErrorFailsStatement pins the value-materialization
// contract for column DEFAULTs at insert time: when a DEFAULT expression cannot
// be evaluated, an INSERT that needs it (column omitted from the column list, or
// an explicit DEFAULT keyword) must fail with buildInsertRow's materialization-
// error family instead of silently storing NULL. A column the statement supplies
// is never evaluated, so inserts providing every column keep working on tables
// carrying an un-evaluatable default. Sibling of the ALTER TABLE ADD COLUMN
// backfill fix and the INSTEAD OF trigger fixes.
func TestInsertDefaultEvaluationErrorFailsStatement(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	exec := func(sql string) error {
		_, e := db.Exec(ctx, sql)
		return e
	}
	mustExec := func(sql string) {
		t.Helper()
		if e := exec(sql); e != nil {
			t.Fatalf("exec %q: %v", sql, e)
		}
	}

	// Control: a valid default still materializes on omission.
	mustExec("CREATE TABLE ok (id INTEGER PRIMARY KEY, okc INTEGER DEFAULT 42)")
	mustExec("INSERT INTO ok (id) VALUES (1)")
	var okv interface{}
	if e := db.QueryRow(ctx, "SELECT okc FROM ok WHERE id=1").Scan(&okv); e != nil || fmt.Sprint(okv) != "42" {
		t.Fatalf("valid default materialization broken: v=%v err=%v", okv, e)
	}

	mustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, c INTEGER DEFAULT (1/0))")

	// Omission: the statement needs the default, so the un-evaluatable
	// expression must fail the INSERT, not store NULL.
	err = exec("INSERT INTO t (id, v) VALUES (1, 'a')")
	if err == nil {
		t.Fatal("INSERT omitting column with un-evaluatable DEFAULT (1/0) succeeded; row was silently stored with NULL")
	}
	if !strings.Contains(err.Error(), "failed to evaluate value for column 'c'") {
		t.Fatalf("INSERT error %q does not carry the value-materialization error family", err)
	}

	// Explicit DEFAULT keyword requests the same un-evaluatable default.
	if e := exec("INSERT INTO t VALUES (2, 'b', DEFAULT)"); e == nil {
		t.Fatal("INSERT with explicit DEFAULT keyword for un-evaluatable default succeeded")
	}

	// Supplying the bad-default column explicitly must not evaluate the
	// default at all — the overlay overwrites the slot.
	if e := exec("INSERT INTO t (id, v, c) VALUES (3, 'supplied', 7)"); e != nil {
		t.Fatalf("INSERT supplying the bad-default column must succeed, got %v", e)
	}
	var sv interface{}
	if e := db.QueryRow(ctx, "SELECT c FROM t WHERE id=3").Scan(&sv); e != nil || fmt.Sprint(sv) != "7" {
		t.Fatalf("supplied value lost: c=%v err=%v", sv, e)
	}

	// A failed multi-row INSERT must not partially apply.
	var before int
	if e := db.QueryRow(ctx, "SELECT COUNT(*) FROM t").Scan(&before); e != nil {
		t.Fatalf("count before batch: %v", e)
	}
	if e := exec("INSERT INTO t (id, v) VALUES (10, 'x'), (11, 'y')"); e == nil {
		t.Fatal("multi-row INSERT with un-evaluatable default succeeded")
	}
	var after int
	if e := db.QueryRow(ctx, "SELECT COUNT(*) FROM t").Scan(&after); e != nil || after != before {
		t.Fatalf("failed multi-row INSERT partially applied: before=%d after=%d", before, after)
	}
}
