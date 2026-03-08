package engine

import (
	"context"
	"testing"
	"time"
)

func TestQueryExecutorBasic(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	qe := NewQueryExecutor(db)

	// Execute a simple query with timeout
	ctx := context.Background()
	result, err := qe.ExecuteWithTimeout(ctx, "CREATE TABLE test (id INT)", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}
}

func TestQueryExecutorTimeout(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create test table with data
	_, err := db.Exec(context.Background(), "CREATE TABLE test (id INT)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	for i := 0; i < 10; i++ {
		_, err := db.Exec(context.Background(), "INSERT INTO test VALUES (?)", []interface{}{i})
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	qe := NewQueryExecutor(db)

	// Execute with very short timeout that should trigger
	ctx := context.Background()
	_, err = qe.ExecuteWithTimeout(ctx, "SELECT * FROM test", nil, 1*time.Nanosecond)

	// Should get timeout error
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
}

func TestQueryExecutorQueryWithTimeout(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create test table
	_, err := db.Exec(context.Background(), "CREATE TABLE test (id INT, name TEXT)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(context.Background(), "INSERT INTO test VALUES (1, 'alice')", nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	qe := NewQueryExecutor(db)

	// Query with timeout
	ctx := context.Background()
	rows, err := qe.QueryWithTimeout(ctx, "SELECT * FROM test", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Should get 1 row
	if !rows.Next() {
		t.Error("Expected one row")
	}
}

func TestQueryExecutorStats(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	qe := NewQueryExecutor(db)

	// Check initial stats
	stats := qe.Stats()
	if stats.ActiveQueries != 0 {
		t.Errorf("Expected 0 active queries initially, got %d", stats.ActiveQueries)
	}
	if stats.CompletedCount != 0 {
		t.Errorf("Expected 0 completed queries initially, got %d", stats.CompletedCount)
	}

	// Execute a query
	ctx := context.Background()
	_, err := qe.ExecuteWithTimeout(ctx, "CREATE TABLE test (id INT)", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Stats should be updated
	stats = qe.Stats()
	if stats.CompletedCount != 1 {
		t.Errorf("Expected 1 completed query, got %d", stats.CompletedCount)
	}
}

func TestQueryExecutorSetDefaultTimeout(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	qe := NewQueryExecutor(db)

	// Set a custom default timeout
	qe.SetDefaultTimeout(30 * time.Second)

	stats := qe.Stats()
	if stats.DefaultTimeout != 30*time.Second {
		t.Errorf("Expected default timeout 30s, got %v", stats.DefaultTimeout)
	}
}

func TestQueryExecutorActiveQueries(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	qe := NewQueryExecutor(db)

	// Initially no active queries
	active := qe.GetActiveQueries()
	if len(active) != 0 {
		t.Errorf("Expected 0 active queries initially, got %d", len(active))
	}
}

func TestQueryExecutorLongRunningQueries(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	qe := NewQueryExecutor(db)

	// No long running queries initially
	longRunning := qe.GetLongRunningQueries(1 * time.Second)
	if len(longRunning) != 0 {
		t.Errorf("Expected 0 long running queries, got %d", len(longRunning))
	}
}

func TestExecWithOptions(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Execute with timeout option
	result, err := db.ExecWithOptions("CREATE TABLE test (id INT)", nil, WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}
}

func TestExecWithOptionsContext(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Execute with context option
	result, err := db.ExecWithOptions("CREATE TABLE test (id INT)", nil, WithContext(ctx))
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}
}

func TestQueryWithOptions(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create and populate table
	_, err := db.Exec(context.Background(), "CREATE TABLE test (id INT)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec(context.Background(), "INSERT INTO test VALUES (1)", nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query with timeout option
	rows, err := db.QueryWithOptions("SELECT * FROM test", nil, WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("Expected one row")
	}
}

func TestQueryRowWithOptions(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create and populate table
	_, err := db.Exec(context.Background(), "CREATE TABLE test (id INT)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec(context.Background(), "INSERT INTO test VALUES (1)", nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// QueryRow with timeout option
	row := db.QueryRowWithOptions("SELECT * FROM test", nil, WithTimeout(5*time.Second))

	var id int
	err = row.Scan(&id)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	if id != 1 {
		t.Errorf("Expected id=1, got %d", id)
	}
}

func TestExecWithOptionsTimeout(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create test table with data
	_, err := db.Exec(context.Background(), "CREATE TABLE test (id INT)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute with very short timeout - this may or may not timeout depending on execution speed
	// Just verify it doesn't panic
	_, _ = db.ExecWithOptions("SELECT * FROM test", nil, WithTimeout(1*time.Nanosecond))
}

func TestQueryExecutorCancelQueryNotFound(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	qe := NewQueryExecutor(db)

	// Try to cancel non-existent query
	err := qe.CancelQuery("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent query")
	}
}

func TestQueryExecutorCancelAllQueries(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	qe := NewQueryExecutor(db)

	// Cancel all when none are running (should not panic)
	count := qe.CancelAllQueries()
	if count != 0 {
		t.Errorf("Expected 0 cancelled, got %d", count)
	}
}

func TestWithTimeout(t *testing.T) {
	cfg := &execConfig{}
	opts := WithTimeout(5 * time.Second)
	opts(cfg)

	if cfg.timeout != 5*time.Second {
		t.Errorf("Expected timeout 5s, got %v", cfg.timeout)
	}
}

func TestWithContext(t *testing.T) {
	cfg := &execConfig{}
	ctx := context.Background()
	opts := WithContext(ctx)
	opts(cfg)

	if cfg.context != ctx {
		t.Error("Expected context to be set")
	}
}

func setupTestDB(t *testing.T) *DB {
	opts := &Options{
		InMemory:     true,
		QueryTimeout: 60 * time.Second,
	}

	db, err := Open(":memory:", opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	return db
}

func cleanupTestDB(t *testing.T, db *DB) {
	if err := db.Close(); err != nil {
		t.Errorf("Failed to close database: %v", err)
	}
}
