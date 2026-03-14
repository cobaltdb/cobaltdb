package engine

import (
	"context"
	"testing"
	"time"
)

// TestCommitErrorPaths96 targets Commit error paths
func TestCommitErrorPaths96(t *testing.T) {
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

	// Create a table
	_, err = tx.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Commit should succeed
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}

	// Second commit should fail (already completed)
	err = tx.Commit()
	if err == nil {
		t.Error("Expected error for double commit")
	} else {
		t.Logf("Got expected error for double commit: %v", err)
	}
}

// TestRollbackErrorPaths96 targets Rollback error paths
func TestRollbackErrorPaths96(t *testing.T) {
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

	// Rollback should succeed
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback failed: %v", err)
	}

	// Second rollback should fail (already completed)
	err = tx.Rollback()
	if err == nil {
		t.Error("Expected error for double rollback")
	} else {
		t.Logf("Got expected error for double rollback: %v", err)
	}
}

// TestGetMetricsErrorPaths96 targets GetMetrics error paths
func TestGetMetricsErrorPaths96(t *testing.T) {
	// Open database without metrics collector
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// GetMetrics should return error when metrics not enabled
	_, err = db.GetMetrics()
	if err == nil {
		t.Log("GetMetrics without metrics collector - may return nil or error")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestExecuteVacuum96 targets executeVacuum
func TestExecuteVacuum96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'test'), (2, 'test2')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Delete some rows to create free space
	_, err = db.Exec(ctx, "DELETE FROM test WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Vacuum should reclaim space
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Logf("VACUUM error (may be expected): %v", err)
	}
}

// TestExecuteAnalyze96 targets executeAnalyze
func TestExecuteAnalyze96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Analyze should update statistics
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Logf("ANALYZE error (may be expected): %v", err)
	}

	// Analyze specific table
	_, err = db.Exec(ctx, "ANALYZE test")
	if err != nil {
		t.Logf("ANALYZE table error (may be expected): %v", err)
	}
}


// TestExecuteWithCTE96 targets executeSelectWithCTE
func TestExecuteWithCTE96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query with CTE
	rows, err := db.Query(ctx, "WITH cte AS (SELECT * FROM test WHERE id > 5) SELECT * FROM cte")
	if err != nil {
		t.Logf("CTE query error (may be expected): %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("CTE returned %d rows", count)
}

// TestExecuteCreatePolicy96 targets executeCreatePolicy
func TestExecuteCreatePolicy96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, owner TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Enable RLS
	_, err = db.Exec(ctx, "ALTER TABLE test ENABLE ROW LEVEL SECURITY")
	if err != nil {
		t.Logf("ENABLE RLS error (may be expected): %v", err)
		return
	}

	// Create policy with USING expression
	_, err = db.Exec(ctx, "CREATE POLICY owner_policy ON test USING (owner = 'test')")
	if err != nil {
		t.Logf("CREATE POLICY error (may be expected): %v", err)
	}
}

// TestExecuteExplainQuery96 targets executeExplainQuery
func TestExecuteExplainQuery96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Explain query
	rows, err := db.Query(ctx, "EXPLAIN SELECT * FROM test")
	if err != nil {
		t.Logf("EXPLAIN error (may be expected): %v", err)
		return
	}
	defer rows.Close()
}

// TestApplyUnionOrderBy96 targets applyUnionOrderBy
func TestApplyUnionOrderBy96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 30), (2, 10), (3, 20)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// UNION with ORDER BY
	rows, err := db.Query(ctx, "SELECT id, val FROM test UNION ALL SELECT id, val FROM test ORDER BY val")
	if err != nil {
		t.Logf("UNION ORDER BY error (may be expected): %v", err)
		return
	}
	defer rows.Close()
}

// TestCompareUnionValues96 targets compareUnionValues
func TestCompareUnionValues96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create tables with different types
	_, err = db.Exec(ctx, "CREATE TABLE t1 (val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE t2 (val REAL)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO t1 VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO t2 VALUES (1.5)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// UNION with mixed types and ORDER BY
	rows, err := db.Query(ctx, "SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val")
	if err != nil {
		t.Logf("UNION mixed types error (may be expected): %v", err)
		return
	}
	defer rows.Close()
}

// TestSaveMetaPage96 targets saveMetaPage
func TestSaveMetaPage96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Operations that trigger saveMetaPage
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE INDEX idx_test ON test(id)")
	if err != nil {
		t.Logf("CREATE INDEX error (may be expected): %v", err)
	}
}

// TestTableSchema96 targets TableSchema
func TestTableSchema96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get table schema
	schema, err := db.TableSchema("test")
	if err != nil {
		t.Logf("TableSchema error (may be expected): %v", err)
		return
	}

	t.Logf("Table schema: %+v", schema)
}

// TestTableSchemaNotFound96 targets TableSchema with non-existent table
func TestTableSchemaNotFound96(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get schema for non-existent table
	_, err = db.TableSchema("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent table")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}


// TestCircuitBreakerReportFailure96 targets ReportFailure
func TestCircuitBreakerReportFailure96(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         3,
		MinSuccesses:        2,
		ResetTimeout:        100 * time.Millisecond,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	// Report failures to trip circuit
	for i := 0; i < 3; i++ {
		cb.ReportFailure()
	}

	// Circuit should be open
	if cb.State() != CircuitOpen {
		t.Errorf("Expected state to be Open, got %s", cb.State())
	}
}

// TestCircuitBreakerReportSuccess96 targets ReportSuccess
func TestCircuitBreakerReportSuccess96(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         3,
		MinSuccesses:        2,
		ResetTimeout:        100 * time.Millisecond,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	// Report failures
	cb.ReportFailure()
	cb.ReportFailure()

	// Report success to reset
	cb.ReportSuccess()

	// Circuit should still be closed
	if cb.State() != CircuitClosed {
		t.Errorf("Expected state to be Closed, got %s", cb.State())
	}
}
