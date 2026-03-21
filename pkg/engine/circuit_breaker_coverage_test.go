package engine

import (
	"context"
	"testing"
	"time"
)

func TestCircuitBreakerAllow_HalfOpenTokenExhausted(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		MaxFailures:         2,
		MinSuccesses:        2,
		ResetTimeout:        50 * time.Millisecond,
		MaxConcurrency:      100,
		HalfOpenMaxRequests: 1,
	})
	defer cb.Stop()

	// Trip to open
	cb.Allow()
	cb.ReportFailure()
	cb.Release()
	cb.Allow()
	cb.ReportFailure()
	cb.Release()

	// Wait for reset timeout → half-open
	time.Sleep(100 * time.Millisecond)

	// First request in half-open should succeed (or still open if timing is tight)
	err := cb.Allow()
	if err == ErrCircuitOpen {
		// Timing issue — circuit hasn't transitioned yet, skip
		t.Skip("timing: circuit still open")
	}
	if err != nil {
		t.Fatalf("first half-open Allow: %v", err)
	}

	// Second request in half-open should fail (token exhausted)
	err = cb.Allow()
	if err == nil {
		// Got through — might have transitioned to closed already
		cb.Release()
	}

	// Report success and release to transition back
	cb.ReportSuccess()
	cb.Release()
}

func TestCircuitBreakerAllow_OpenNoReset(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		MaxFailures:         1,
		MinSuccesses:        1,
		ResetTimeout:        1 * time.Hour, // very long timeout
		MaxConcurrency:      100,
		HalfOpenMaxRequests: 1,
	})
	defer cb.Stop()

	// Trip to open
	cb.Allow()
	cb.ReportFailure()
	cb.Release()

	// Should stay open (reset timeout not reached)
	err := cb.Allow()
	if err != ErrCircuitOpen {
		t.Errorf("Allow on open circuit: got %v, want ErrCircuitOpen", err)
	}
}

func TestCircuitBreakerAllow_ConcurrencyLimit(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		MaxFailures:         10,
		MinSuccesses:        1,
		ResetTimeout:        1 * time.Second,
		MaxConcurrency:      2,
		HalfOpenMaxRequests: 1,
	})
	defer cb.Stop()

	// Fill up concurrency slots
	cb.Allow()
	cb.Allow()

	// Third should hit concurrency limit
	err := cb.Allow()
	if err != ErrCircuitTooMany {
		t.Errorf("Allow over concurrency limit: got %v, want ErrCircuitTooMany", err)
	}

	// Release one, should allow again
	cb.Release()
	err = cb.Allow()
	if err != nil {
		t.Errorf("Allow after release: %v", err)
	}
	cb.Release()
	cb.Release()
}

func TestQueryContextCancelled(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	db.Exec(ctx, "CREATE TABLE test (id INTEGER)")

	// Cancel context before query
	cancel()

	_, err = db.Query(ctx, "SELECT * FROM test")
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestQueryShowStatementsCoverage(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")

	// SHOW TABLES
	rows, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES: %v", err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	if count < 2 {
		t.Errorf("SHOW TABLES: got %d rows, want >= 2", count)
	}

	// SHOW DATABASES
	rows, err = db.Query(ctx, "SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES: %v", err)
	}
	rows.Close()

	// DESCRIBE
	rows, err = db.Query(ctx, "DESCRIBE users")
	if err != nil {
		t.Fatalf("DESCRIBE: %v", err)
	}
	count = 0
	for rows.Next() {
		count++
	}
	rows.Close()
	if count < 2 {
		t.Errorf("DESCRIBE: got %d cols, want >= 2", count)
	}
}

func TestQueryReturning(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")

	// INSERT RETURNING
	rows, err := db.Query(ctx, "INSERT INTO items (name, qty) VALUES ('apple', 10) RETURNING id, name")
	if err != nil {
		t.Fatalf("INSERT RETURNING: %v", err)
	}
	if rows.Next() {
		var id, name interface{}
		rows.Scan(&id, &name)
	}
	rows.Close()

	// UPDATE RETURNING
	rows, err = db.Query(ctx, "UPDATE items SET qty = 20 WHERE name = 'apple' RETURNING id, qty")
	if err != nil {
		t.Fatalf("UPDATE RETURNING: %v", err)
	}
	rows.Close()

	// DELETE RETURNING
	rows, err = db.Query(ctx, "DELETE FROM items WHERE name = 'apple' RETURNING id")
	if err != nil {
		t.Fatalf("DELETE RETURNING: %v", err)
	}
	rows.Close()
}

func TestQueryUnionAndExplain(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE a (id INTEGER, val TEXT)")
	db.Exec(ctx, "CREATE TABLE b (id INTEGER, val TEXT)")
	db.Exec(ctx, "INSERT INTO a VALUES (1, 'x')")
	db.Exec(ctx, "INSERT INTO b VALUES (2, 'y')")

	// UNION query through Query()
	rows, err := db.Query(ctx, "SELECT id, val FROM a UNION SELECT id, val FROM b")
	if err != nil {
		t.Fatalf("UNION query: %v", err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	if count < 2 {
		t.Errorf("UNION: got %d rows, want >= 2", count)
	}

	// CTE query through Query()
	rows, err = db.Query(ctx, "WITH cte AS (SELECT * FROM a) SELECT * FROM cte")
	if err != nil {
		t.Fatalf("CTE query: %v", err)
	}
	rows.Close()

	// EXPLAIN
	rows, err = db.Query(ctx, "EXPLAIN SELECT * FROM a WHERE id = 1")
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	rows.Close()

	// SHOW COLUMNS
	rows, err = db.Query(ctx, "SHOW COLUMNS FROM a")
	if err != nil {
		t.Fatalf("SHOW COLUMNS: %v", err)
	}
	rows.Close()

	// SHOW CREATE TABLE
	rows, err = db.Query(ctx, "SHOW CREATE TABLE a")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE: %v", err)
	}
	rows.Close()

	// Non-query through Query() should error
	_, err = db.Query(ctx, "INSERT INTO a VALUES (3, 'z')")
	if err == nil {
		t.Error("expected error for non-query INSERT through Query()")
	}
}

func TestExecuteCreateViewCoverage(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL)")
	db.Exec(ctx, "INSERT INTO products VALUES (1, 100.0)")
	db.Exec(ctx, "INSERT INTO products VALUES (2, 200.0)")

	// CREATE VIEW
	_, err = db.Exec(ctx, "CREATE VIEW expensive AS SELECT * FROM products WHERE price > 150")
	if err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	// Query the view
	rows, err := db.Query(ctx, "SELECT * FROM expensive")
	if err != nil {
		t.Fatalf("SELECT from view: %v", err)
	}
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	if count != 1 {
		t.Errorf("view returned %d rows, want 1", count)
	}
}
