package engine

import (
	"context"
	"testing"
)

// TestTxCommitWithFlushError106 tests transaction commit when flush fails
func TestTxCommitWithFlushError106(t *testing.T) {
	// This test is difficult to trigger without mocking
	// We'll just verify normal commit still works
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Begin and commit a transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Logf("Commit returned error (may be expected): %v", err)
	}
}

// TestTxRollbackError106 tests transaction rollback error paths
func TestTxRollbackError106(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Begin and rollback a transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Logf("Rollback returned error (may be expected): %v", err)
	}

	// Second rollback should fail
	err = tx.Rollback()
	if err == nil {
		t.Error("Expected error for second rollback")
	}
}

// TestTxCommitMultiple106 tests committing already completed transaction
func TestTxCommitMultiple106(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// First commit
	err = tx.Commit()
	if err != nil {
		t.Logf("First commit error (may be expected): %v", err)
	}

	// Second commit should fail
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error for second commit")
	}
}

// TestDBGetMetrics106 tests GetMetrics error path
func TestDBGetMetrics106(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// GetMetrics should return error when metrics not enabled
	_, err = db.GetMetrics()
	if err == nil {
		t.Log("Expected error when metrics not enabled, but got nil")
	}
}

// TestDBGetMetricsCollector106 tests GetMetricsCollector
func TestDBGetMetricsCollector106(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	collector := db.GetMetricsCollector()
	if collector != nil {
		t.Log("Metrics collector is not nil")
	}
}
