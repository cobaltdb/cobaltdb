package engine

import (
	"context"
	"strings"
	"testing"
)

// TestIndexPopulationSkipsTombstones pins the live-row contract for index
// population: building an index must index LIVE rows only. Soft-deleted rows
// stay in the table tree as MVCC tombstones; decodeRow decodes them without
// liveness, so populateIndexVisibleRowsLocked/addIndexRowLocked,
// populateIndexLocked, and rebuildTableIndexesLocked re-created index entries
// for deleted rows — spuriously failing UNIQUE re-inserts and even failing
// CREATE UNIQUE INDEX itself when a tombstone shares a value with a live row.
// DML never leaves tombstone entries in live indexes (DELETE removes them),
// so population violating that invariant is a population-only defect.
func TestIndexPopulationSkipsTombstones(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, u TEXT)`)

	// Probe A: re-inserting a value whose row was deleted BEFORE the unique
	// index was created must succeed.
	mustExec(t, db, `INSERT INTO t VALUES (1, 'x'), (2, 'y')`)
	mustExec(t, db, `DELETE FROM t WHERE id = 1`)
	mustExec(t, db, `CREATE UNIQUE INDEX iu ON t(u)`)
	if _, err := db.Exec(ctx, `INSERT INTO t VALUES (3, 'x')`); err != nil {
		t.Fatalf("FAIL: re-insert of tombstoned value failed after CREATE UNIQUE INDEX: %v", err)
	}

	// Probe B (separate table: t already carries a unique index on u): a
	// unique index whose population crosses a tombstone value with a live
	// value must still be creatable.
	mustExec(t, db, `CREATE TABLE t3 (id INTEGER PRIMARY KEY, u TEXT)`)
	mustExec(t, db, `INSERT INTO t3 VALUES (1, 'z'), (2, 'z')`)
	mustExec(t, db, `DELETE FROM t3 WHERE id = 1`)
	if _, err := db.Exec(ctx, `CREATE UNIQUE INDEX iu2 ON t3(u)`); err != nil {
		t.Fatalf("FAIL: CREATE UNIQUE INDEX failed on a tombstone/live value collision: %v", err)
	}
	// The populated index must serve exactly the live row for equality lookups.
	var n int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM t3 WHERE u = 'z'`).Scan(&n); err != nil {
		t.Fatalf("count t3: %v", err)
	}
	if n != 1 {
		t.Fatalf("FAIL: expected exactly 1 live 'z' row, got %d", n)
	}

	// Control (DML invariant): with the index created BEFORE the delete, DML
	// removes the entry, so the same re-insert must succeed there too.
	mustExec(t, db, `CREATE TABLE t2 (id INTEGER PRIMARY KEY, u TEXT)`)
	mustExec(t, db, `CREATE UNIQUE INDEX iu3 ON t2(u)`)
	mustExec(t, db, `INSERT INTO t2 VALUES (1, 'x')`)
	mustExec(t, db, `DELETE FROM t2 WHERE id = 1`)
	if _, err := db.Exec(ctx, `INSERT INTO t2 VALUES (2, 'x')`); err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			t.Fatalf("FAIL: DML invariant broken (pre-existing index path): %v", err)
		}
		t.Fatalf("control insert failed: %v", err)
	}
}
