package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestAlterAddColumnDefaultErrorFailsDDL pins the value-materialization
// contract for ALTER TABLE ADD COLUMN backfill: when the DEFAULT expression
// cannot be evaluated, the ALTER must fail atomically instead of silently
// NULL-backfilling existing rows. Every other failure in AlterTableAddColumn
// (scan/decode/encode/put) aborts the DDL, and the sibling swallows in
// catalog_ddl.go (INSTEAD OF trigger sites) were already fixed to propagate;
// the backfill path was the remaining one in that function.
func TestAlterAddColumnDefaultErrorFailsDDL(t *testing.T) {
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

	mustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec("INSERT INTO t VALUES (1, 'a')")
	mustExec("INSERT INTO t VALUES (2, 'b')")

	// Control: a valid default still backfills existing rows.
	mustExec("ALTER TABLE t ADD COLUMN ok INTEGER DEFAULT 42")
	var okv interface{}
	if e := db.QueryRow(ctx, "SELECT ok FROM t WHERE id=1").Scan(&okv); e != nil || fmt.Sprint(okv) != "42" {
		t.Fatalf("valid default backfill broken: v=%v err=%v", okv, e)
	}

	// Probe: an un-evaluatable default must fail the ALTER, not NULL-fill.
	err = exec("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT (1/0)")
	if err == nil {
		t.Fatal("ALTER TABLE ADD COLUMN with un-evaluatable DEFAULT (1/0) succeeded; existing rows were silently NULL-backfilled")
	}
	if !strings.Contains(err.Error(), "failed to evaluate value for column 'c'") {
		t.Fatalf("ALTER error %q does not carry the value-materialization error family", err)
	}

	// Atomicity: the failed ALTER leaves schema and rows untouched.
	var cv interface{}
	if e := db.QueryRow(ctx, "SELECT c FROM t WHERE id=1").Scan(&cv); e == nil {
		t.Fatalf("column c still queryable after failed ALTER (value %v)", cv)
	}
	var n int
	if e := db.QueryRow(ctx, "SELECT COUNT(*) FROM t").Scan(&n); e != nil || n != 2 {
		t.Fatalf("rows changed after failed ALTER: n=%d err=%v", n, e)
	}

	// Boundary: a zero-row table stays lazy — consistent with CREATE TABLE,
	// which never evaluates defaults.
	mustExec("CREATE TABLE empty_t (id INTEGER PRIMARY KEY)")
	if e := exec("ALTER TABLE empty_t ADD COLUMN c INTEGER DEFAULT (1/0)"); e != nil {
		t.Fatalf("empty-table ALTER should stay lazy: %v", e)
	}
}
