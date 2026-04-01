package integration

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestChaosConcurrentWrites tests concurrent writes with random delays
func TestChaosConcurrentWrites(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create test table
	_, err = db.Exec(ctx, `CREATE TABLE chaos_test (
		id INTEGER PRIMARY KEY,
		value INTEGER,
		updated_at INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Concurrent writers with random delays
	const numWriters = 50
	const writesPerWriter = 100
	var successCount atomic.Int32
	var errorCount atomic.Int32

	var wg sync.WaitGroup
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				// Random delay to increase contention
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)

				_, err := db.Exec(ctx,
					"INSERT INTO chaos_test VALUES (?, ?, ?)",
					writerID*writesPerWriter+j,
					rand.Intn(1000000),
					time.Now().UnixNano(),
				)
				if err != nil {
					errorCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	success := successCount.Load()
	errors := errorCount.Load()
	total := numWriters * writesPerWriter

	t.Logf("Concurrent writes: %d success, %d errors, %d total", success, errors, total)

	// Verify data integrity
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM chaos_test")
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != int(success) {
			t.Errorf("Row count mismatch: expected %d, got %d", success, count)
		}
	}
}

// TestChaosConcurrentReadWrite tests concurrent reads and writes
func TestChaosConcurrentReadWrite(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create and populate table
	_, err = db.Exec(ctx, `CREATE TABLE rw_test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 0; i < 1000; i++ {
		_, err = db.Exec(ctx, "INSERT INTO rw_test VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	const numReaders = 30
	const numWriters = 10
	const operations = 500

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				select {
				case <-stopCh:
					return
				default:
				}

				rows, err := db.Query(ctx,
					"SELECT * FROM rw_test WHERE id BETWEEN ? AND ? ORDER BY id",
					rand.Intn(500),
					rand.Intn(500)+500,
				)
				if err != nil {
					t.Logf("Reader %d query error: %v", readerID, err)
					continue
				}
				defer rows.Close()

				// Consume rows
				for rows.Next() {
					var id int
					var value string
					rows.Scan(&id, &value)
				}
			}
		}(i)
	}

	// Writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				select {
				case <-stopCh:
					return
				default:
				}

				id := rand.Intn(1000)
				_, err := db.Exec(ctx,
					"UPDATE rw_test SET value = ? WHERE id = ?",
					fmt.Sprintf("updated_%d_%d", writerID, j),
					id,
				)
				if err != nil {
					t.Logf("Writer %d update error: %v", writerID, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(stopCh)

	t.Log("Concurrent read/write test completed")
}

// TestChaosMemoryPressure tests behavior under memory pressure
func TestChaosMemoryPressure(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:   true,
		CacheSize:  64, // Very small cache
		WALEnabled: false,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create large table
	_, err = db.Exec(ctx, `CREATE TABLE pressure_test (
		id INTEGER PRIMARY KEY,
		data TEXT,
		metadata TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert large amount of data
	largeData := make([]byte, 10000) // 10KB per row
	for i := range largeData {
		largeData[i] = byte('a' + i%26)
	}
	dataStr := string(largeData)

	const numRows = 1000
	for i := 0; i < numRows; i++ {
		_, err = db.Exec(ctx,
			"INSERT INTO pressure_test VALUES (?, ?, ?)",
			i,
			dataStr,
			fmt.Sprintf("metadata_%d", i),
		)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Query under pressure
	for i := 0; i < 10; i++ {
		rows, err := db.Query(ctx, "SELECT * FROM pressure_test WHERE id > ? LIMIT 100", rand.Intn(numRows-100))
		if err != nil {
			t.Errorf("Query failed: %v", err)
			continue
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		t.Logf("Query returned %d rows", count)
	}
}

// TestChaosRapidOpenClose tests rapid database open/close cycles
func TestChaosRapidOpenClose(t *testing.T) {
	const cycles = 50

	for i := 0; i < cycles; i++ {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Fatalf("Cycle %d: Failed to open database: %v", i, err)
		}

		ctx := context.Background()
		_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Cycle %d: Failed to create table: %v", i, err)
		}

		_, err = db.Exec(ctx, "INSERT INTO test VALUES (?)", i)
		if err != nil {
			t.Fatalf("Cycle %d: Failed to insert: %v", i, err)
		}

		err = db.Close()
		if err != nil {
			t.Fatalf("Cycle %d: Failed to close: %v", i, err)
		}
	}

	t.Logf("Completed %d open/close cycles", cycles)
}

// TestChaosConnectionExhaustion tests behavior with many concurrent connections
func TestChaosConnectionExhaustion(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:       true,
		MaxConnections: 100,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE conn_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Pre-populate
	for i := 0; i < 100; i++ {
		_, err = db.Exec(ctx, "INSERT INTO conn_test VALUES (?)", i)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	const numWorkers = 200
	const operationsPerWorker = 50

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*operationsPerWorker)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < operationsPerWorker; j++ {
				_, err := db.Query(ctx, "SELECT * FROM conn_test WHERE id = ?", rand.Intn(100))
				if err != nil {
					errors <- fmt.Errorf("worker %d op %d: %v", workerID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for err := range errors {
		t.Logf("Error: %v", err)
		errorCount++
		if errorCount > 10 {
			t.Log("Too many errors, stopping log")
			break
		}
	}

	t.Logf("Completed with %d errors", errorCount)
}

// TestChaosTransactionConflicts tests transaction conflict scenarios
func TestChaosTransactionConflicts(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE txn_test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO txn_test VALUES (1, 100)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Multiple transactions trying to update the same row
	const numTxns = 20
	var wg sync.WaitGroup
	committed := make(chan bool, numTxns)

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(txnID int) {
			defer wg.Done()

			txn, err := db.Begin(ctx)
			if err != nil {
				t.Logf("Txn %d: Begin failed: %v", txnID, err)
				committed <- false
				return
			}

			// Small random delay to increase conflicts
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)

			_, err = txn.Exec(ctx, "UPDATE txn_test SET value = value + 1 WHERE id = 1")
			if err != nil {
				t.Logf("Txn %d: Update failed: %v", txnID, err)
				txn.Rollback()
				committed <- false
				return
			}

			err = txn.Commit()
			if err != nil {
				t.Logf("Txn %d: Commit failed: %v", txnID, err)
				committed <- false
				return
			}

			committed <- true
		}(i)
	}

	wg.Wait()
	close(committed)

	successCount := 0
	for success := range committed {
		if success {
			successCount++
		}
	}

	t.Logf("Transactions: %d committed, %d failed", successCount, numTxns-successCount)
}

// TestChaosRandomQueryPatterns tests random query patterns
func TestChaosRandomQueryPatterns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create schema
	_, err = db.Exec(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			email TEXT,
			age INTEGER,
			created_at INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < 10000; i++ {
		_, err = db.Exec(ctx,
			"INSERT INTO users VALUES (?, ?, ?, ?, ?)",
			i,
			fmt.Sprintf("user_%d", i),
			fmt.Sprintf("user%d@test.com", i),
			rand.Intn(100),
			time.Now().UnixNano(),
		)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Random queries
	queries := []string{
		"SELECT * FROM users WHERE id = ?",
		"SELECT * FROM users WHERE age BETWEEN ? AND ? ORDER BY age",
		"SELECT name, email FROM users WHERE name LIKE ? LIMIT ?",
		"SELECT COUNT(*), AVG(age) FROM users WHERE age > ?",
		"SELECT * FROM users ORDER BY created_at DESC LIMIT ?",
	}

	const numQueries = 1000
	for i := 0; i < numQueries; i++ {
		query := queries[rand.Intn(len(queries))]
		var args []interface{}

		switch query {
		case "SELECT * FROM users WHERE id = ?":
			args = []interface{}{rand.Intn(10000)}
		case "SELECT * FROM users WHERE age BETWEEN ? AND ? ORDER BY age":
			min, max := rand.Intn(50), rand.Intn(50)+50
			args = []interface{}{min, max}
		case "SELECT name, email FROM users WHERE name LIKE ? LIMIT ?":
			args = []interface{}{fmt.Sprintf("%%user_%d%%", rand.Intn(100)), rand.Intn(100)}
		case "SELECT COUNT(*), AVG(age) FROM users WHERE age > ?":
			args = []interface{}{rand.Intn(100)}
		case "SELECT * FROM users ORDER BY created_at DESC LIMIT ?":
			args = []interface{}{rand.Intn(100)}
		}

		rows, err := db.Query(ctx, query, args...)
		if err != nil {
			t.Errorf("Query %d failed: %v", i, err)
			continue
		}
		defer rows.Close()

		// Just consume the rows
		for rows.Next() {
		}
	}

	t.Logf("Completed %d random queries", numQueries)
}

// TestChaosLongRunningTransactions tests long-running transactions
func TestChaosLongRunningTransactions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE long_txn_test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Start long-running transaction
	txn, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert within transaction
	for i := 0; i < 100; i++ {
		_, err = txn.Exec(ctx, "INSERT INTO long_txn_test VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Concurrent operations outside transaction
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, err := db.Exec(ctx,
					"INSERT INTO long_txn_test VALUES (?, ?)",
					10000+workerID*100+j,
					fmt.Sprintf("concurrent_%d_%d", workerID, j),
				)
				if err != nil {
					t.Logf("Worker %d: Insert error: %v", workerID, err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	// Wait a bit then commit long transaction
	time.Sleep(100 * time.Millisecond)
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit long transaction: %v", err)
	}

	wg.Wait()

	// Verify data
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM long_txn_test")
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		rows.Scan(&count)
		t.Logf("Total rows after long transaction: %d", count)
	}
}

// TestChaosIndexContention tests heavy index operations under contention
func TestChaosIndexContention(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `
		CREATE TABLE indexed_table (
			id INTEGER PRIMARY KEY,
			email TEXT UNIQUE,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE INDEX idx_age ON indexed_table(age)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	const numWorkers = 30
	const opsPerWorker = 100

	var wg sync.WaitGroup
	errors := make(chan error, numWorkers*opsPerWorker)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				id := workerID*opsPerWorker + j
				_, err := db.Exec(ctx,
					"INSERT INTO indexed_table VALUES (?, ?, ?)",
					id,
					fmt.Sprintf("user%d@test.com", id),
					rand.Intn(100),
				)
				if err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for err := range errors {
		t.Logf("Index error: %v", err)
		errorCount++
		if errorCount > 10 {
			break
		}
	}

	t.Logf("Index contention test: %d errors", errorCount)
}
