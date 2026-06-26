package engine

import (
	"context"
	"testing"
)

// TestAbortConnTransactionRollsBack verifies AbortConnTransaction (used on
// server disconnect) rolls back an open transaction left on the goroutine.
func TestAbortConnTransactionRollsBack(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	// Wire-style explicit transaction (goes through the execute() BEGIN path).
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "INSERT INTO t VALUES (1)")

	// Simulate the connection dropping without COMMIT.
	db.AbortConnTransaction()

	if db.catalog.IsTransactionActive() {
		t.Fatal("transaction still active after AbortConnTransaction")
	}
	var n int64
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 0 {
		t.Fatalf("uncommitted insert survived rollback: COUNT = %d, want 0", n)
	}
	// And a no-op when no transaction is active (must not panic/error).
	db.AbortConnTransaction()
}
