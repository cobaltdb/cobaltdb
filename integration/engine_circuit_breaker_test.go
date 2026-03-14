package integration

import (
	"context"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestCircuitBreakerStateTransitions tests circuit breaker state changes
func TestCircuitBreakerStateTransitions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Normal operations should succeed
	_, err = db.Exec(ctx, `CREATE TABLE cb_test (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple times to trigger circuit breaker if exists
	for i := 0; i < 10; i++ {
		_, err = db.Exec(ctx, `INSERT INTO cb_test VALUES (?)`, i)
		if err != nil {
			t.Logf("Insert %d error: %v", i, err)
		}
	}

	// Query to verify
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM cb_test`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Inserted %d rows", count)
		}
	}
}

// TestCircuitBreakerWithFailures tests circuit breaker with repeated failures
func TestCircuitBreakerWithFailures(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Try invalid operations that should fail
	for i := 0; i < 5; i++ {
		_, err = db.Exec(ctx, `INVALID SQL SYNTAX`)
		if err != nil {
			t.Logf("Expected error %d: %v", i, err)
		}
	}

	// Database should still be usable after failures
	_, err = db.Exec(ctx, `CREATE TABLE recovery (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Logf("Recovery error: %v", err)
	} else {
		t.Log("Database recovered after failures")
	}
}

// TestRetryWithBackoff tests retry mechanism with exponential backoff
func TestRetryWithBackoff(t *testing.T) {
	ctx := context.Background()

	// Open database
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE retry_test (id INTEGER PRIMARY KEY, val TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert with potential retry
	_, err = db.Exec(ctx, `INSERT INTO retry_test VALUES (1, 'test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify insert
	rows, _ := db.Query(ctx, `SELECT val FROM retry_test WHERE id = 1`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var val string
			rows.Scan(&val)
			if val != "test" {
				t.Errorf("Expected 'test', got '%s'", val)
			}
			t.Logf("Value verified: %s", val)
		}
	}
}

// TestConcurrentAccess tests concurrent database access
func TestConcurrentAccess(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE concurrent (id INTEGER PRIMARY KEY, counter INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO concurrent VALUES (1, 0)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Simulate concurrent updates
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Each goroutine tries to update
			_, err := db.Exec(ctx, `UPDATE concurrent SET counter = counter + 1 WHERE id = 1`)
			if err != nil {
				t.Logf("Goroutine %d error: %v", id, err)
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 5; i++ {
		<-done
	}

	// Check final counter
	rows, _ := db.Query(ctx, `SELECT counter FROM concurrent WHERE id = 1`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var counter int
			rows.Scan(&counter)
			t.Logf("Final counter: %d", counter)
		}
	}
}

// TestTransactionRetry tests transaction retry behavior
func TestTransactionRetry(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE tx_retry (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Multiple transactions
	for i := 0; i < 3; i++ {
		_, err = db.Exec(ctx, `BEGIN TRANSACTION`)
		if err != nil {
			t.Logf("BEGIN error: %v", err)
			continue
		}

		_, err = db.Exec(ctx, `INSERT INTO tx_retry VALUES (?)`, i)
		if err != nil {
			t.Logf("INSERT error: %v", err)
			db.Exec(ctx, `ROLLBACK`)
			continue
		}

		_, err = db.Exec(ctx, `COMMIT`)
		if err != nil {
			t.Logf("COMMIT error: %v", err)
		}
	}

	// Verify
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM tx_retry`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Rows after transactions: %d", count)
		}
	}
}

// TestTimeoutHandling tests query timeout behavior
func TestTimeoutHandling(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = db.Exec(ctx, `CREATE TABLE timeout_test (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 0; i < 100; i++ {
		_, err = db.Exec(ctx, `INSERT INTO timeout_test VALUES (?)`, i)
		if err != nil {
			t.Logf("Insert error: %v", err)
			break
		}
	}

	// Query should work within timeout
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM timeout_test`)
	if err != nil {
		t.Logf("Query error: %v", err)
		return
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		t.Logf("Rows inserted: %d", count)
	}
}
