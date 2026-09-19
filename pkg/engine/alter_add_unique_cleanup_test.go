package engine

import (
	"context"
	"strings"
	"testing"
)

// TestAlterAddUniqueCleansUpOnIndexFailure pins cleanup-on-partial-failure
// for ALTER TABLE ADD COLUMN with a named UNIQUE constraint: the column is
// added first, then its UNIQUE constraint index is created. If index creation
// fails (here: a globally colliding index name), the statement must not leave
// the added column behind — mirroring executeCreateTable's cleanupOnError
// convention in the same file.
// Regression for executeAlterTable (pkg/engine/database_ddl.go) ADD branch.
func TestAlterAddUniqueCleansUpOnIndexFailure(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `INSERT INTO t VALUES (1)`)
	mustExec(t, db, `CREATE TABLE other (id INTEGER PRIMARY KEY)`)
	mustExec(t, db, `CREATE INDEX myidx ON other(id)`)

	columns := func() []string {
		t.Helper()
		rows, err := db.Query(ctx, `DESCRIBE t`)
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var field, typ, nullable, key, def, extra string
			if err := rows.Scan(&field, &typ, &nullable, &key, &def, &extra); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, field)
		}
		return out
	}

	hasColumn := func(name string) bool {
		for _, c := range columns() {
			if strings.EqualFold(c, name) {
				return true
			}
		}
		return false
	}

	// Probe: the named UNIQUE constraint index collides with the existing
	// index on `other` — the statement must fail AND leave no column behind.
	_, err = db.Exec(ctx, `ALTER TABLE t ADD COLUMN myidx TEXT CONSTRAINT myidx UNIQUE`)
	if err == nil {
		t.Fatalf("FAIL: colliding UNIQUE index did not fail the ALTER")
	}
	if hasColumn("myidx") {
		t.Fatalf("FAIL: failed ALTER left the added column behind: %v", columns())
	}

	// Recovery: once the colliding index is gone, the same ALTER succeeds.
	mustExec(t, db, `DROP INDEX myidx`)
	mustExec(t, db, `ALTER TABLE t ADD COLUMN myidx TEXT CONSTRAINT myidx UNIQUE`)
	if !hasColumn("myidx") {
		t.Fatalf("FAIL: retried ALTER did not add the column: %v", columns())
	}

	// Control: a plain ADD COLUMN (no UNIQUE index step) works as always.
	mustExec(t, db, `ALTER TABLE t ADD COLUMN plain TEXT`)
	if !hasColumn("plain") {
		t.Fatalf("FAIL: plain ADD COLUMN did not add the column: %v", columns())
	}
}
