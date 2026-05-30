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
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE chaos_write (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	var wg sync.WaitGroup
	var writes int64

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := db.Exec(ctx, `INSERT INTO chaos_write VALUES (?, ?)`, j, rand.Int())
				if err == nil {
					atomic.AddInt64(&writes, 1)
				}
				time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
			}
		}()
	}

	wg.Wait()
	t.Logf("Concurrent writes completed: %d successful", writes)
}

// TestChaosConcurrentReadWrite tests concurrent reads and writes
func TestChaosConcurrentReadWrite(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE chaos_rw (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO chaos_rw VALUES (1, 0)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	var wg sync.WaitGroup
	var reads, writes int64

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				rows, err := db.Query(ctx, `SELECT * FROM chaos_rw WHERE id = 1`)
				if err == nil {
					atomic.AddInt64(&reads, 1)
					rows.Close()
				}
				time.Sleep(time.Microsecond * time.Duration(rand.Intn(50)))
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, err := db.Exec(ctx, `UPDATE chaos_rw SET val = val + 1 WHERE id = 1`)
				if err == nil {
					atomic.AddInt64(&writes, 1)
				}
				time.Sleep(time.Microsecond * time.Duration(rand.Intn(50)))
			}
		}()
	}

	wg.Wait()
	t.Logf("Concurrent read/write completed: reads=%d, writes=%d", reads, writes)
}

// TestChaosOpenCloseRapidly tests rapid open/close cycles
func TestChaosOpenCloseRapidly(t *testing.T) {
	const cycles = 50

	for i := 0; i < cycles; i++ {
		db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
		if err != nil {
			t.Fatalf("Cycle %d: Failed to open database: %v", i, err)
		}

		ctx := context.Background()
		_, err = db.Exec(ctx, `CREATE TABLE rapid_test (id INTEGER PRIMARY KEY)`)
		if err != nil {
			db.Close()
			t.Fatalf("Cycle %d: Failed to create table: %v", i, err)
		}

		_, err = db.Exec(ctx, `INSERT INTO rapid_test VALUES (1)`)
		if err != nil {
			db.Close()
			t.Fatalf("Cycle %d: Failed to insert: %v", i, err)
		}

		db.Close()
	}

	t.Logf("Rapid open/close cycles completed: %d cycles", cycles)
}

// TestChaosTransactionConflicts tests transaction conflict scenarios
func TestChaosTransactionConflicts(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE conflict_test (id INTEGER PRIMARY KEY, balance REAL)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO conflict_test VALUES (1, 1000.0)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	var wg sync.WaitGroup
	var conflicts int64

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				tx, err := db.Begin(ctx)
				if err != nil {
					continue
				}

				_, err = tx.Exec(ctx, `UPDATE conflict_test SET balance = balance - 10.0 WHERE id = 1`)
				if err != nil {
					tx.Rollback()
					atomic.AddInt64(&conflicts, 1)
					continue
				}

				err = tx.Commit()
				if err != nil {
					atomic.AddInt64(&conflicts, 1)
				}
			}
		}()
	}

	wg.Wait()
	t.Logf("Transaction conflict test completed: conflicts=%d", conflicts)
}

// TestChaosRandomQueryPatterns tests random query patterns
func TestChaosRandomQueryPatterns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE random_query (id INTEGER PRIMARY KEY, val TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO random_query VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	queries := []string{
		`SELECT * FROM random_query`,
		`SELECT * FROM random_query WHERE id = 1`,
		`SELECT * FROM random_query WHERE id > 1`,
		`SELECT COUNT(*) FROM random_query`,
		`SELECT * FROM random_query ORDER BY id DESC`,
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				q := queries[rand.Intn(len(queries))]
				rows, err := db.Query(ctx, q)
				if err == nil {
					rows.Close()
				}
				time.Sleep(time.Microsecond * time.Duration(rand.Intn(50)))
			}
		}()
	}

	wg.Wait()
	t.Log("Random query pattern test completed")
}

// TestChaosLongRunningTransactions tests long-running transactions
func TestChaosLongRunningTransactions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE long_tx (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO long_tx VALUES (1, 0)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Long-running read transaction
	rows, err := tx.Query(ctx, `SELECT * FROM long_tx WHERE id = 1`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	rows.Close()

	// Concurrent write should succeed (different session)
	_, err = db.Exec(ctx, `UPDATE long_tx SET val = 42 WHERE id = 1`)
	if err != nil {
		t.Logf("Concurrent write error (may be expected): %v", err)
	}

	t.Log("Long-running transaction test completed")
}

// TestChaosIndexContention tests heavy index operations under contention
func TestChaosIndexContention(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE index_contention (id INTEGER PRIMARY KEY, email TEXT, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE INDEX idx_email ON index_contention(email)`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				email := fmt.Sprintf("user%d@test.com", rand.Int())
				_, err := db.Exec(ctx, `INSERT INTO index_contention VALUES (?, ?, ?)`, j, email, fmt.Sprintf("User %d", j))
				if err != nil {
					// Duplicate email or other error - ignore
				}
				time.Sleep(time.Microsecond * time.Duration(rand.Intn(30)))
			}
		}(i)
	}

	wg.Wait()
	t.Log("Index contention test completed")
}

// TestChaosMemoryPressure tests behavior under memory pressure
func TestChaosMemoryPressure(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 64, WALEnabled: engine.BoolPtr(false)}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE memory_pressure (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Try to insert a lot of data with small cache
	for i := 0; i < 10000; i++ {
		_, err = db.Exec(ctx, `INSERT INTO memory_pressure VALUES (?, ?)`, i, fmt.Sprintf("data-%d", i))
		if err != nil {
			t.Logf("Insert error at i=%d: %v", i, err)
			break
		}
	}

	t.Log("Memory pressure test completed")
}

// TestChaosConcurrentConnections tests many concurrent connections
func TestChaosConcurrentConnections(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, `CREATE TABLE many_conn (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO many_conn VALUES (1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	var wg sync.WaitGroup
	var success int64

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				rows, err := db.Query(ctx, `SELECT * FROM many_conn`)
				if err == nil {
					atomic.AddInt64(&success, 1)
					rows.Close()
				}
			}
		}()
	}

	wg.Wait()
	t.Logf("Concurrent connections test completed: %d successful queries", success)
}
