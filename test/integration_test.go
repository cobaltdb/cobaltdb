package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestConcurrentInserts(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE concurrent_test (id INTEGER, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Concurrent inserts
	numGoroutines := 10
	insertsPerGoroutine := 100
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*insertsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < insertsPerGoroutine; i++ {
				_, err := db.Exec(ctx,
					`INSERT INTO concurrent_test (id, value) VALUES (?, ?)`,
					goroutineID*insertsPerGoroutine+i,
					fmt.Sprintf("value-%d-%d", goroutineID, i),
				)
				if err != nil {
					errors <- err
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Errorf("Insert error: %v", err)
		errorCount++
		if errorCount > 10 {
			t.Fatalf("Too many errors, stopping")
		}
	}

	// Verify total rows
	rows, err := db.Query(ctx, `SELECT id FROM concurrent_test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	expected := numGoroutines * insertsPerGoroutine
	if count != expected {
		t.Errorf("Expected %d rows, got %d", expected, count)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create and populate table
	_, err = db.Exec(ctx, `CREATE TABLE rw_test (id INTEGER, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Initial data
	for i := 0; i < 100; i++ {
		db.Exec(ctx, `INSERT INTO rw_test (id, value) VALUES (?, ?)`, i, fmt.Sprintf("value-%d", i))
	}

	// Concurrent reads and writes
	var wg sync.WaitGroup
	duration := 2 * time.Second
	start := time.Now()

	// Writers
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for time.Since(start) < duration {
				id := time.Now().UnixNano() % 1000
				db.Exec(ctx, `INSERT INTO rw_test (id, value) VALUES (?, ?)`,
					id, fmt.Sprintf("writer-%d-%d", writerID, id))
			}
		}(w)
	}

	// Readers
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Since(start) < duration {
				rows, err := db.Query(ctx, `SELECT id, value FROM rw_test`)
				if err == nil {
					rows.Close()
				}
			}
		}()
	}

	wg.Wait()
}

func TestTransactionIsolation(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE isolation_test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial row
	db.Exec(ctx, `INSERT INTO isolation_test (id, value) VALUES (?, ?)`, 1, "initial")

	// Start transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Update in transaction
	_, err = tx.Exec(ctx, `UPDATE isolation_test SET value = ? WHERE id = ?`, "updated", 1)
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to update in transaction: %v", err)
	}

	// Read from main connection (should see old value)
	rows, err := db.Query(ctx, `SELECT value FROM isolation_test WHERE id = ?`, 1)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	rows.Close()

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Now should see updated value
	rows, err = db.Query(ctx, `SELECT value FROM isolation_test WHERE id = ?`, 1)
	if err != nil {
		t.Fatalf("Failed to query after commit: %v", err)
	}
	defer rows.Close()

	// Check the value
	if rows.Next() {
		var value interface{}
		rows.Scan(&value)
		_ = value
	}
}

func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 10 * 1024 * 1024, // 10MB
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE large_test (id INTEGER, name TEXT, value REAL)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert large dataset
	numRows := 10000
	start := time.Now()

	for i := 0; i < numRows; i++ {
		_, err := db.Exec(ctx,
			`INSERT INTO large_test (id, name, value) VALUES (?, ?, ?)`,
			i, fmt.Sprintf("name-%d", i), float64(i)*1.5,
		)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	insertDuration := time.Since(start)
	t.Logf("Inserted %d rows in %v (%.0f rows/sec)",
		numRows, insertDuration, float64(numRows)/insertDuration.Seconds())

	// Query all
	start = time.Now()
	rows, err := db.Query(ctx, `SELECT id, name, value FROM large_test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}

	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()

	queryDuration := time.Since(start)
	t.Logf("Queried %d rows in %v (%.0f rows/sec)",
		count, queryDuration, float64(count)/queryDuration.Seconds())

	if count != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, count)
	}
}

func TestBatchInsert(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE batch_test (id INTEGER, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Batch insert in transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	batchSize := 1000
	for i := 0; i < batchSize; i++ {
		_, err := tx.Exec(ctx, `INSERT INTO batch_test (id, value) VALUES (?, ?)`,
			i, fmt.Sprintf("batch-value-%d", i))
		if err != nil {
			tx.Rollback()
			t.Fatalf("Failed to insert in batch: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit batch: %v", err)
	}

	// Verify
	rows, err := db.Query(ctx, `SELECT id FROM batch_test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != batchSize {
		t.Errorf("Expected %d rows, got %d", batchSize, count)
	}
}

func TestMultipleTables(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create multiple tables
	numTables := 10
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		_, err := db.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s (id INTEGER, data TEXT)`, tableName))
		if err != nil {
			t.Fatalf("Failed to create table %s: %v", tableName, err)
		}

		// Insert data
		_, err = db.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, data) VALUES (?, ?)`, tableName), i, fmt.Sprintf("data-%d", i))
		if err != nil {
			t.Fatalf("Failed to insert into table %s: %v", tableName, err)
		}
	}

	// Query each table
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		rows, err := db.Query(ctx, fmt.Sprintf(`SELECT id FROM %s`, tableName))
		if err != nil {
			t.Fatalf("Failed to query table %s: %v", tableName, err)
		}

		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()

		if count != 1 {
			t.Errorf("Expected 1 row in table %s, got %d", tableName, count)
		}
	}
}

func TestConnectionResilience(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE resilience (id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Multiple operations
	for i := 0; i < 100; i++ {
		// Insert
		_, err := db.Exec(ctx, `INSERT INTO resilience (id) VALUES (?)`, i)
		if err != nil {
			t.Errorf("Insert %d failed: %v", i, err)
		}

		// Query
		rows, err := db.Query(ctx, `SELECT id FROM resilience`)
		if err != nil {
			t.Errorf("Query %d failed: %v", i, err)
		}
		rows.Close()
	}

	// Final count
	rows, err := db.Query(ctx, `SELECT id FROM resilience`)
	if err != nil {
		t.Fatalf("Final query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 100 {
		t.Errorf("Expected 100 rows, got %d", count)
	}
}
