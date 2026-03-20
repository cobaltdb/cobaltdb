package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentAccess tests concurrent access to the database
// This test is designed to be run with -race flag to detect race conditions
// Example: CGO_ENABLED=1 go test -race -run TestConcurrentAccess
func TestConcurrentAccess(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE race_test (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec(ctx, "INSERT INTO race_test VALUES (1, 0)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Concurrent reads and writes
	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	var errCount atomic.Int64

	// Writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				_, err := db.Exec(ctx, "UPDATE race_test SET value = ? WHERE id = 1", id*numOperations+j)
				if err != nil {
					errCount.Add(1)
				}
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				rows, err := db.Query(ctx, "SELECT value FROM race_test WHERE id = 1")
				if err != nil {
					errCount.Add(1)
				} else {
					rows.Close()
				}
			}
		}()
	}

	wg.Wait()
	t.Logf("Errors: %d (expected some under contention)", errCount.Load())
}

// TestConcurrentTransactions tests concurrent transaction operations
// This test is designed to be run with -race flag to detect race conditions
func TestConcurrentTransactions(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE txn_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Concurrent transactions
	var wg sync.WaitGroup
	numGoroutines := 5

	var txnErrs atomic.Int64
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, err := db.Exec(ctx, "INSERT INTO txn_test VALUES (?)", id*10+j)
				if err != nil {
					txnErrs.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify count
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM txn_test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var count int64
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		if count != int64(numGoroutines*10) {
			t.Errorf("Expected %d rows, got %d", numGoroutines*10, count)
		}
	}
}

// TestCatalogConcurrentAccess tests concurrent catalog operations
func TestCatalogConcurrentAccess(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 5

	// Create tables concurrently
	var catErrs atomic.Int64
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sql := "CREATE TABLE catalog_test_" + string(rune('a'+id)) + " (id INTEGER PRIMARY KEY)"
			_, err := db.Exec(ctx, sql)
			if err != nil {
				catErrs.Add(1)
			}
		}(i)
	}

	wg.Wait()
}

// TestQueryCacheRace tests for race conditions in query cache
func TestQueryCacheRace(t *testing.T) {
	db, err := Open(":memory:", &Options{
		EnableQueryCache: true,
		QueryCacheSize:   1000,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE cache_race_test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 0; i < 10; i++ {
		_, err := db.Exec(ctx, "INSERT INTO cache_race_test VALUES (?, ?)", i, "test")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Concurrent queries
	var wg sync.WaitGroup
	numGoroutines := 10
	numQueries := 50

	var cacheErrs atomic.Int64
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numQueries; j++ {
				rows, err := db.Query(ctx, "SELECT * FROM cache_race_test WHERE id = ?", j%10)
				if err != nil {
					cacheErrs.Add(1)
				} else {
					rows.Close()
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestBufferPoolRace tests for race conditions in buffer pool
func TestBufferPoolRace(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CacheSize: 16,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE buffer_test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data to stress buffer pool
	var wg sync.WaitGroup
	numGoroutines := 5

	var bufErrs atomic.Int64
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, err := db.Exec(ctx, "INSERT INTO buffer_test VALUES (?, ?)", id*20+j, "test data")
				if err != nil {
					bufErrs.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
}
