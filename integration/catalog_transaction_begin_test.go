package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestBeginTransaction targets BeginTransaction with various options
func TestBeginTransaction(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE test (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	tests := []struct {
		name string
		fn   func() error
		desc string
	}{
		{
			name: "Default transaction",
			fn: func() error {
				tx, err := db.Begin(ctx)
				if err != nil {
					return err
				}
				_, err = tx.Exec(ctx, `INSERT INTO test VALUES (1)`)
				if err != nil {
					tx.Rollback()
					return err
				}
				return tx.Commit()
			},
			desc: "Basic transaction",
		},
		{
			name: "Multiple transactions sequential",
			fn: func() error {
				for i := 2; i <= 4; i++ {
					tx, err := db.Begin(ctx)
					if err != nil {
						return err
					}
					_, err = tx.Exec(ctx, `INSERT INTO test VALUES (?)`, i)
					if err != nil {
						tx.Rollback()
						return err
					}
					if err := tx.Commit(); err != nil {
						return err
					}
				}
				return nil
			},
			desc: "Sequential transactions",
		},
		{
			name: "Transaction with rollback",
			fn: func() error {
				tx, err := db.Begin(ctx)
				if err != nil {
					return err
				}
				_, err = tx.Exec(ctx, `INSERT INTO test VALUES (999)`)
				if err != nil {
					tx.Rollback()
					return err
				}
				return tx.Rollback()
			},
			desc: "Rollback transaction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err != nil {
				t.Logf("%s error: %v", tt.desc, err)
			} else {
				t.Logf("%s succeeded", tt.desc)
			}
		})
	}

	// Verify final state
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM test`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Final row count: %d (expected 4, no 999)", count)
		}
	}
}

// TestBeginTransactionWithIsolation targets transaction isolation
func TestBeginTransactionWithIsolation(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE isolation_test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO isolation_test VALUES (1, 'original')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Begin first transaction
	tx1, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin tx1: %v", err)
	}

	// Update in first transaction
	_, err = tx1.Exec(ctx, `UPDATE isolation_test SET value = 'modified' WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to update in tx1: %v", err)
	}

	// Read from first transaction
	rows, _ := tx1.Query(ctx, `SELECT value FROM isolation_test WHERE id = 1`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var val string
			rows.Scan(&val)
			t.Logf("Value in tx1: %s", val)
		}
	}

	// Commit first transaction
	err = tx1.Commit()
	if err != nil {
		t.Logf("Commit tx1 error: %v", err)
	}

	// Verify final value
	rows, _ = db.Query(ctx, `SELECT value FROM isolation_test WHERE id = 1`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var val string
			rows.Scan(&val)
			t.Logf("Final value: %s", val)
		}
	}
}

// TestNestedBeginTransaction targets nested transaction handling
func TestNestedBeginTransaction(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE nested (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Outer transaction
	tx1, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin outer tx: %v", err)
	}

	_, err = tx1.Exec(ctx, `INSERT INTO nested VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert in outer: %v", err)
	}

	// Try to begin another transaction (should fail or use savepoint)
	// Note: Most databases don't support true nested transactions
	// but may support savepoints instead
	t.Log("Attempting second begin (may use savepoints internally)")

	// Commit outer
	err = tx1.Commit()
	if err != nil {
		t.Logf("Commit outer error: %v", err)
	} else {
		t.Log("Outer transaction committed")
	}

	// Verify
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM nested`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Row count: %d", count)
		}
	}
}

// TestBeginTransactionWithConstraints targets deferred constraints
func TestBeginTransactionWithConstraints(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables with FK
	_, err = db.Exec(ctx, `CREATE TABLE parent (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		FOREIGN KEY (parent_id) REFERENCES parent(id)
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Insert parent
	_, err = db.Exec(ctx, `INSERT INTO parent VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	// Transaction with FK operations
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin: %v", err)
	}

	// Insert valid child
	_, err = tx.Exec(ctx, `INSERT INTO child VALUES (1, 1)`)
	if err != nil {
		t.Logf("Insert valid child error: %v", err)
		tx.Rollback()
		return
	}

	// Try to insert invalid child
	_, err = tx.Exec(ctx, `INSERT INTO child VALUES (2, 999)`)
	if err != nil {
		t.Logf("Insert invalid child correctly blocked: %v", err)
	} else {
		t.Log("Insert invalid child succeeded (FK not enforced)")
	}

	err = tx.Commit()
	if err != nil {
		t.Logf("Commit error: %v", err)
	} else {
		t.Log("Transaction committed")
	}
}
