package engine

import (
	"context"
	"testing"
)

// newTestDB opens an in-memory database for testing and registers cleanup.
func newTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024},
	})
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

// mustExecCtx executes a query with context and fails the test on error.
func mustExecCtx(t *testing.T, db *DB, ctx context.Context, query string, args ...interface{}) {
	t.Helper()
	_, err := db.Exec(ctx, query, args...)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}
}

// mustQueryCtx executes a query with context and returns rows, failing the test on error.
func mustQueryCtx(t *testing.T, db *DB, ctx context.Context, query string, args ...interface{}) *Rows {
	t.Helper()
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	return rows
}

// mustQueryValCtx executes a query with context and asserts a single scalar value.
func mustQueryValCtx(t *testing.T, db *DB, ctx context.Context, query string, expected interface{}, args ...interface{}) {
	t.Helper()
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("No rows returned")
	}

	var actual interface{}
	if err := rows.Scan(&actual); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if actual != expected {
		t.Fatalf("Expected %v (%T), got %v (%T)", expected, expected, actual, actual)
	}
}

// mustFailCtx asserts that a query with context fails (any error).
func mustFailCtx(t *testing.T, db *DB, ctx context.Context, query string, args ...interface{}) {
	t.Helper()
	_, err := db.Exec(ctx, query, args...)
	if err == nil {
		t.Fatalf("Expected query to fail, but it succeeded: %s", query)
	}
}
