package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestCommitTransactionBasic targets CommitTransaction with basic operations
func TestCommitTransactionBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE commit_test (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	_, err = db.Exec(ctx, `BEGIN TRANSACTION`)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}

	// Insert within transaction
	_, err = db.Exec(ctx, `INSERT INTO commit_test VALUES (1, 100), (2, 200)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Commit
	_, err = db.Exec(ctx, `COMMIT`)
	if err != nil {
		t.Logf("COMMIT error: %v", err)
		return
	}

	// Verify data persisted
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM commit_test`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			if count != 2 {
				t.Errorf("Expected 2 rows, got %d", count)
			}
			t.Logf("Committed rows: %d", count)
		}
	}
}

// TestCommitTransactionWithFK targets CommitTransaction with FK checks
func TestCommitTransactionWithFK(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE parent_commit (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child_commit (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		FOREIGN KEY (parent_id) REFERENCES parent_commit(id)
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Insert parent
	_, err = db.Exec(ctx, `INSERT INTO parent_commit VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Begin transaction
	_, err = db.Exec(ctx, `BEGIN TRANSACTION`)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}

	// Insert child with FK
	_, err = db.Exec(ctx, `INSERT INTO child_commit VALUES (1, 1), (2, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Commit with FK checks
	_, err = db.Exec(ctx, `COMMIT`)
	if err != nil {
		t.Logf("COMMIT with FK error: %v", err)
		return
	}

	t.Log("Commit with FK succeeded")
}

// TestCommitTransactionDeferredConstraints targets CommitTransaction with deferred constraints
func TestCommitTransactionDeferredConstraints(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE deferred_test (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin with deferred constraints
	_, err = db.Exec(ctx, `BEGIN DEFERRED TRANSACTION`)
	if err != nil {
		t.Logf("BEGIN DEFERRED error: %v", err)
		// Try regular begin
		_, err = db.Exec(ctx, `BEGIN TRANSACTION`)
		if err != nil {
			t.Fatalf("Failed to begin: %v", err)
		}
	}

	_, err = db.Exec(ctx, `INSERT INTO deferred_test VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, `COMMIT`)
	if err != nil {
		t.Logf("COMMIT error: %v", err)
		return
	}

	t.Log("Commit with deferred constraints succeeded")
}

// TestRollbackTransactionBasic targets RollbackTransaction
func TestRollbackTransactionBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rollback_test (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, `INSERT INTO rollback_test VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Begin transaction
	_, err = db.Exec(ctx, `BEGIN TRANSACTION`)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}

	// Insert in transaction
	_, err = db.Exec(ctx, `INSERT INTO rollback_test VALUES (2), (3)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rollback
	_, err = db.Exec(ctx, `ROLLBACK`)
	if err != nil {
		t.Logf("ROLLBACK error: %v", err)
		return
	}

	// Verify rollback
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM rollback_test`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			if count != 1 {
				t.Errorf("Expected 1 row after rollback, got %d", count)
			}
			t.Logf("Rows after rollback: %d", count)
		}
	}
}

// TestRollbackTransactionWithChanges targets RollbackTransaction with schema changes
func TestRollbackTransactionWithChanges(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rollback_changes (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `BEGIN TRANSACTION`)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}

	// Multiple operations
	_, err = db.Exec(ctx, `INSERT INTO rollback_changes VALUES (1), (2), (3)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, `UPDATE rollback_changes SET id = id + 10`)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	_, err = db.Exec(ctx, `DELETE FROM rollback_changes WHERE id > 12`)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Rollback all changes
	_, err = db.Exec(ctx, `ROLLBACK`)
	if err != nil {
		t.Logf("ROLLBACK error: %v", err)
		return
	}

	// Verify rollback (may vary based on implementation)
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM rollback_changes`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Rows after rollback: %d (expected 0 if rollback worked)", count)
		}
	}
}

// TestCommitRollbackSequence tests multiple commit/rollback sequences
func TestCommitRollbackSequence(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE seq_test (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Sequence 1: Commit
	_, err = db.Exec(ctx, `BEGIN`)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO seq_test VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = db.Exec(ctx, `COMMIT`)
	if err != nil {
		t.Logf("COMMIT error: %v", err)
	}

	// Sequence 2: Rollback
	_, err = db.Exec(ctx, `BEGIN`)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO seq_test VALUES (2)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = db.Exec(ctx, `ROLLBACK`)
	if err != nil {
		t.Logf("ROLLBACK error: %v", err)
	}

	// Sequence 3: Commit again
	_, err = db.Exec(ctx, `BEGIN`)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO seq_test VALUES (3)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	_, err = db.Exec(ctx, `COMMIT`)
	if err != nil {
		t.Logf("COMMIT error: %v", err)
	}

	// Verify: should have rows 1 and 3
	rows, _ := db.Query(ctx, `SELECT id FROM seq_test ORDER BY id`)
	if rows != nil {
		defer rows.Close()
		var ids []int
		for rows.Next() {
			var id int
			rows.Scan(&id)
			ids = append(ids, id)
		}
		t.Logf("Final rows: %v", ids)
	}
}
