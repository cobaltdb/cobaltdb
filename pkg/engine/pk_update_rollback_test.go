package engine

import (
	"context"
	"testing"
)

// countRows returns the primary-key values returned by a query, in order.
func pkRollbackQueryIDs(t *testing.T, db *DB, ctx context.Context, sql string) []int64 {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestPKChangingUpdateRollbackNoOrphan verifies that rolling back a transaction
// that changed a primary key does not leave the row at BOTH the old and new
// keys. Before the fix, the undo log only restored the old key and never
// deleted the row written at the new key, leaving a duplicate orphan (and
// desyncing the base table from secondary indexes).
func TestPKChangingUpdateRollbackNoOrphan(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE t SET id = 2 WHERE id = 1`); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// Full scan must show exactly one row, id=1.
	ids := pkRollbackQueryIDs(t, db, ctx, `SELECT id FROM t`)
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("after rollback, full scan = %v, want [1]", ids)
	}

	// The orphan must not be reachable by primary-key lookup either.
	byNew := pkRollbackQueryIDs(t, db, ctx, `SELECT id FROM t WHERE id = 2`)
	if len(byNew) != 0 {
		t.Fatalf("orphan row id=2 still present via PK lookup: %v", byNew)
	}
}

// TestPKChangingUpdateRollbackToSavepointNoOrphan verifies the same invariant
// for ROLLBACK TO SAVEPOINT, which replays through the same undo handler.
func TestPKChangingUpdateRollbackToSavepointNoOrphan(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO t VALUES (1, 'a')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, `SAVEPOINT s1`); err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE t SET id = 2 WHERE id = 1`); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT s1`); err != nil {
		t.Fatalf("rollback to savepoint: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	ids := pkRollbackQueryIDs(t, db, ctx, `SELECT id FROM t`)
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("after rollback-to-savepoint, full scan = %v, want [1]", ids)
	}
}
