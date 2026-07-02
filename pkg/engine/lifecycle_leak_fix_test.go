package engine

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestCloseStopsQueryCacheGoroutine verifies that DB.Close shuts down the
// query cache's cleanup goroutine (previously leaked once per Open/Close
// cycle when EnableQueryCache was set).
func TestCloseStopsQueryCacheGoroutine(t *testing.T) {
	// Warm up any lazily started global goroutines.
	warm, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		QueryCache:  QueryCacheConfig{EnableQueryCache: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	warm.Close()
	time.Sleep(100 * time.Millisecond)

	before := runtime.NumGoroutine()

	const cycles = 20
	for i := 0; i < cycles; i++ {
		db, err := Open(":memory:", &Options{
			CoreStorage: CoreStorage{InMemory: true},
			QueryCache:  QueryCacheConfig{EnableQueryCache: true},
		})
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INTEGER)"); err != nil {
			db.Close()
			t.Fatalf("exec %d: %v", i, err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}

	// Give exiting goroutines a moment to unwind.
	var after int
	for i := 0; i < 50; i++ {
		time.Sleep(50 * time.Millisecond)
		after = runtime.NumGoroutine()
		if after <= before+3 {
			break
		}
	}
	// Each cycle used to leak at least one cleanupLoop goroutine; with 20
	// cycles a leak shows up as ~20 extra goroutines.
	if after > before+cycles/2 {
		t.Fatalf("goroutines leaked across Open/Close cycles: before=%d after=%d", before, after)
	}
}

// TestOpenStartsDeadlockDetectorAndCloseStopsIt verifies the transaction
// manager lifecycle is wired: Open starts the background deadlock detector
// and Close stops it without hanging.
func TestOpenStartsDeadlockDetectorAndCloseStopsIt(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "detector.db"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if db.txnMgr == nil {
		db.Close()
		t.Fatal("expected transaction manager")
	}

	// Exercise the database a little so the detector has run at least once
	// (it ticks every 100ms).
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id INTEGER)"); err != nil {
		db.Close()
		t.Fatalf("exec: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung (deadlock detector not stopping?)")
	}

	// Stop must be idempotent (Close may race Shutdown paths).
	db.txnMgr.Stop()
}
