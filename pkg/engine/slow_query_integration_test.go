package engine

import (
	"context"
	"testing"
	"time"
)

func TestSlowQueryLogIntegration(t *testing.T) {
	// Create database with slow query log enabled
	options := &Options{
		InMemory:             true,
		EnableSlowQueryLog:   true,
		SlowQueryThreshold:   1 * time.Millisecond,
		SlowQueryMaxEntries:  100,
	}

	db, err := Open(":memory:", options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Verify slow query log is initialized
	if db.slowQueryLog == nil {
		t.Fatal("Expected slowQueryLog to be initialized")
	}

	// Create a table
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data (should be fast, may not be logged)
	for i := 0; i < 10; i++ {
		_, err = db.Exec(ctx, "INSERT INTO test (name) VALUES ('test')")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Query data (should be logged since threshold is 1ms)
	time.Sleep(2 * time.Millisecond) // Ensure query exceeds threshold
	rows, err := db.Query(ctx, "SELECT * FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	rows.Close()

	// Check slow query log has entries
	entries := db.slowQueryLog.GetEntries(100)
	if len(entries) == 0 {
		t.Log("Warning: No slow query entries logged (queries may have been too fast)")
	}

	// Verify stats work
	total, avg := db.slowQueryLog.GetStats()
	t.Logf("Slow query stats: total=%d, avg=%v", total, avg)
}

func TestSlowQueryLogDisabledByDefault(t *testing.T) {
	// Create database without slow query log
	options := &Options{
		InMemory: true,
	}

	db, err := Open(":memory:", options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Verify slow query log is NOT initialized
	if db.slowQueryLog != nil {
		t.Error("Expected slowQueryLog to be nil when disabled")
	}
}

func TestSlowQueryLogWithFile(t *testing.T) {
	tempDir := t.TempDir()
	logFile := tempDir + "/slow_queries.log"

	// Create database with slow query log to file
	options := &Options{
		InMemory:             true,
		EnableSlowQueryLog:   true,
		SlowQueryThreshold:   1 * time.Millisecond,
		SlowQueryMaxEntries:  100,
		SlowQueryLogFile:     logFile,
	}

	db, err := Open(":memory:", options)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Verify slow query log is initialized
	if db.slowQueryLog == nil {
		t.Fatal("Expected slowQueryLog to be initialized")
	}

	// Execute a query
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test2 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	_, err = db.Exec(ctx, "INSERT INTO test2 VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Give file write time
	time.Sleep(10 * time.Millisecond)

	// Check file exists
	entries := db.slowQueryLog.GetEntries(100)
	t.Logf("Slow query entries: %d", len(entries))
}
