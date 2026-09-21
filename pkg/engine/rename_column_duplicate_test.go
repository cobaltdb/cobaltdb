package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestRenameColumnDuplicateNameFails pins the duplicate-name guard for
// ALTER TABLE ... RENAME COLUMN: renaming onto a name that already exists in
// the table must fail instead of creating two same-named columns (the
// column-index cache is last-wins, so one column would be silently shadowed
// and name-based reads/writes would resolve to the wrong column). Mirrors
// AlterTableAddColumn's duplicate guard and MySQL's "Duplicate column name"
// behavior. A case-variant rename of a column to its own other case stays
// legal (column names are case-insensitive).
func TestRenameColumnDuplicateNameFails(t *testing.T) {
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

	// The renamed column lands AFTER the existing b, so a post-defect
	// SELECT b (last-wins cache) would return the renamed column's data.
	mustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, b TEXT, a INTEGER)")
	mustExec("INSERT INTO t VALUES (1, 'x', 7)")

	// Probe: renaming onto an existing column name must fail.
	err = exec("ALTER TABLE t RENAME COLUMN a TO b")
	if err == nil {
		t.Fatal("RENAME COLUMN onto an existing column name succeeded; schema now has two same-named columns")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("RENAME error %q does not carry the duplicate-column error family", err)
	}

	// Schema intact: both columns still distinct and readable.
	var bv, av interface{}
	if e := db.QueryRow(ctx, "SELECT b, a FROM t WHERE id=1").Scan(&bv, &av); e != nil || fmt.Sprint(bv) != "x" || fmt.Sprint(av) != "7" {
		t.Fatalf("schema not intact after failed rename: b=%v a=%v err=%v", bv, av, e)
	}

	// Control: a legitimate rename still works.
	if e := exec("ALTER TABLE t RENAME COLUMN a TO c"); e != nil {
		t.Fatalf("legitimate rename failed: %v", e)
	}
	var cv interface{}
	if e := db.QueryRow(ctx, "SELECT c FROM t WHERE id=1").Scan(&cv); e != nil || fmt.Sprint(cv) != "7" {
		t.Fatalf("renamed column unreadable: c=%v err=%v", cv, e)
	}

	// Boundary: case-variant self-rename is a legal no-op rename.
	if e := exec("ALTER TABLE t RENAME COLUMN c TO C"); e != nil {
		t.Fatalf("case-variant self-rename should be allowed, got %v", e)
	}
}
