// Package test provides shared test utilities for CobaltDB
package test

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestDB creates a test database and returns it
// The database is automatically closed when the test ends
func TestDB(t *testing.T) (*engine.DB, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open test database: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	return db, ctx
}

// Exec executes a query and fails the test on error
func Exec(t *testing.T, db *engine.DB, ctx context.Context, query string, args ...interface{}) {
	t.Helper()
	_, err := db.Exec(ctx, query, args...)
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}
}

// Query executes a query and returns rows, fails the test on error
func Query(t *testing.T, db *engine.DB, ctx context.Context, query string, args ...interface{}) *engine.Rows {
	t.Helper()
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	return rows
}

// ExpectRows asserts that a query returns the expected number of rows
func ExpectRows(t *testing.T, db *engine.DB, ctx context.Context, query string, expectedCount int, args ...interface{}) {
	t.Helper()
	rows := Query(t, db, ctx, query, args...)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != expectedCount {
		t.Fatalf("Expected %d rows, got %d", expectedCount, count)
	}
}

// ExpectVal asserts that a query returns a single expected value
func ExpectVal(t *testing.T, db *engine.DB, ctx context.Context, query string, expected interface{}, args ...interface{}) {
	t.Helper()
	rows := Query(t, db, ctx, query, args...)
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

// ExpectError asserts that a query fails with the expected error
func ExpectError(t *testing.T, db *engine.DB, ctx context.Context, query string, expectedErr string) {
	t.Helper()
	_, err := db.Exec(ctx, query)
	if err == nil {
		t.Fatalf("Expected error containing %q, got nil", expectedErr)
	}
	if !strings.Contains(err.Error(), expectedErr) {
		t.Fatalf("Expected error containing %q, got %q", expectedErr, err.Error())
	}
}
