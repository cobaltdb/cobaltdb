package query

import (
	"testing"
	"time"
)

// TestPreparedCacheCleanup tests the cleanup function for expired statements
func TestPreparedCacheCleanup(t *testing.T) {
	// Create a cache with a very short TTL
	cache := NewPreparedCache(10, 100*time.Millisecond)
	defer cache.Clear()

	// Add a statement
	cache.Put("SELECT * FROM users", &SelectStmt{}, 0)

	// Verify statement is in cache (get the ID first)
	id := cache.GenerateID("SELECT * FROM users")
	if _, found := cache.Get(id); !found {
		t.Fatal("Statement should be in cache")
	}

	// Wait for TTL to expire
	time.Sleep(200 * time.Millisecond)

	// Manually trigger cleanup
	cache.cleanup()

	// Verify statement was removed
	if _, found := cache.Get(id); found {
		t.Error("Statement should have been cleaned up after TTL expired")
	}
}

// TestPreparedCacheCleanupLoop tests the cleanup loop goroutine
func TestPreparedCacheCleanupLoop(t *testing.T) {
	// Create a cache with a short TTL
	cache := NewPreparedCache(10, 100*time.Millisecond)
	defer cache.Clear()

	// Add a statement
	cache.Put("SELECT * FROM orders", &SelectStmt{}, 0)

	// Verify statement is in cache
	id := cache.GenerateID("SELECT * FROM orders")
	if _, found := cache.Get(id); !found {
		t.Fatal("Statement should be in cache")
	}

	// Wait for cleanup loop to run (cleanup runs every TTL/2)
	time.Sleep(300 * time.Millisecond)

	// Verify statement was removed by cleanup loop
	if _, found := cache.Get(id); found {
		t.Error("Statement should have been cleaned up by cleanup loop")
	}
}

// TestPreparedCacheCleanupPartialExpiry tests cleanup with mixed expired/non-expired
func TestPreparedCacheCleanupPartialExpiry(t *testing.T) {
	cache := NewPreparedCache(10, 200*time.Millisecond)
	defer cache.Clear()

	// Manually add first statement with old timestamp (will expire)
	cache.mu.Lock()
	cache.statements["old"] = &PreparedStatement{
		ID:         "old",
		SQL:        "SELECT 1",
		Stmt:       &SelectStmt{},
		ParamCount: 0,
		CreatedAt:  time.Now().Add(-300 * time.Millisecond),
		LastUsedAt: time.Now().Add(-300 * time.Millisecond),
	}
	cache.mu.Unlock()

	// Add second statement (recent, won't expire)
	cache.Put("SELECT 2", &SelectStmt{}, 0)
	newID := cache.GenerateID("SELECT 2")

	// Manually trigger cleanup
	cache.cleanup()

	// Verify old statement was removed
	if _, found := cache.Get("old"); found {
		t.Error("Old statement should have been cleaned up")
	}

	// Verify new statement is still there
	if _, found := cache.Get(newID); !found {
		t.Error("New statement should still be in cache")
	}
}
