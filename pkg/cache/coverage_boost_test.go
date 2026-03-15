package cache

import (
	"sync"
	"testing"
	"time"
)

// TestCacheConcurrency tests concurrent cache operations
func TestCacheConcurrency(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			cache.Set("SELECT * FROM users WHERE id = ?", []interface{}{n},
				[]string{"id", "name"}, [][]interface{}{{n, "user"}}, []string{"users"})
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			cache.Get("SELECT * FROM users WHERE id = ?", []interface{}{n})
		}(i)
	}

	wg.Wait()

	stats := cache.Stats()
	if stats.EntryCount < 10 {
		t.Errorf("Expected at least 10 entries, got %d", stats.EntryCount)
	}
}


// TestCacheMaxEntries tests max entries limit
func TestCacheMaxEntries(t *testing.T) {
	config := &Config{
		MaxSize:         0, // Unlimited size
		MaxEntries:      5,
		TTL:             5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		Enabled:         true,
	}
	cache := New(config)
	defer cache.Close()

	// Add more entries than max
	for i := 0; i < 10; i++ {
		cache.Set("SELECT * FROM users WHERE id = ?", []interface{}{i},
			[]string{"id"}, [][]any{{i}}, []string{"users"})
	}

	// Should have at most 5 entries
	stats := cache.Stats()
	if stats.EntryCount > 5 {
		t.Errorf("Expected at most 5 entries (max), got %d", stats.EntryCount)
	}
}

// TestCacheInvalidateMultipleTables tests invalidating multiple tables
func TestCacheInvalidateMultipleTables(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Add entries with different table dependencies
	cache.Set("SELECT * FROM users", nil,
		[]string{"id"}, [][]interface{}{{1}}, []string{"users"})
	cache.Set("SELECT * FROM orders", nil,
		[]string{"id"}, [][]interface{}{{1}}, []string{"orders"})
	cache.Set("SELECT * FROM users JOIN orders", nil,
		[]string{"id"}, [][]interface{}{{1}}, []string{"users", "orders"})

	// Invalidate users table
	cache.InvalidateTable("users")

	// users query and join query should be gone
	if _, found := cache.Get("SELECT * FROM users", nil); found {
		t.Error("users query should be invalidated")
	}
	if _, found := cache.Get("SELECT * FROM users JOIN orders", nil); found {
		t.Error("join query should be invalidated")
	}

	// orders-only query should still exist
	if _, found := cache.Get("SELECT * FROM orders", nil); !found {
		t.Error("orders query should still exist")
	}
}

// TestCacheInvalidateNonExistentTable tests invalidating non-existent table
func TestCacheInvalidateNonExistentTable(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Add entry
	cache.Set("SELECT * FROM users", nil,
		[]string{"id"}, [][]interface{}{{1}}, []string{"users"})

	// Invalidate non-existent table - should not panic
	cache.InvalidateTable("non_existent")

	// Entry should still exist
	if _, found := cache.Get("SELECT * FROM users", nil); !found {
		t.Error("users query should still exist after invalidating non-existent table")
	}
}

// TestCacheDeleteNonExistent tests deleting non-existent key
func TestCacheDeleteNonExistent(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Should not panic
	cache.Delete("non_existent_key")
}

// TestCacheDisabledGetSet tests operations on disabled cache
func TestCacheDisabledGetSet(t *testing.T) {
	config := &Config{
		MaxSize:         1024 * 1024,
		MaxEntries:      1000,
		TTL:             5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
		Enabled:         false,
	}
	cache := New(config)
	defer cache.Close()

	// Set should not store anything
	cache.Set("SELECT * FROM users", nil,
		[]string{"id"}, [][]interface{}{{1}}, []string{"users"})

	// Get should return not found
	_, found := cache.Get("SELECT * FROM users", nil)
	if found {
		t.Error("Should not find entry in disabled cache")
	}

	stats := cache.Stats()
	if stats.EntryCount != 0 {
		t.Error("Disabled cache should have no entries")
	}
}

// TestCacheCleanupExpired tests background cleanup
func TestCacheCleanupExpired(t *testing.T) {
	config := &Config{
		MaxSize:         1024 * 1024,
		MaxEntries:      100,
		TTL:             100 * time.Millisecond,
		CleanupInterval: 200 * time.Millisecond,
		Enabled:         true,
	}
	cache := New(config)

	// Add entries
	for i := 0; i < 5; i++ {
		cache.Set("query", []interface{}{i},
			[]string{"id"}, [][]interface{}{{i}}, []string{"users"})
	}

	// Wait for entries to expire
	time.Sleep(400 * time.Millisecond)

	// Cleanup should have run
	stats := cache.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("Expected all entries to be cleaned up, got %d", stats.EntryCount)
	}

	cache.Close()
}

// TestCacheUpdateExistingEntry tests updating an existing entry
func TestCacheUpdateExistingEntry(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Set initial entry
	cache.Set("SELECT * FROM users WHERE id = 1", nil,
		[]string{"id"}, [][]interface{}{{1}}, []string{"users"})

	// Update with different data
	cache.Set("SELECT * FROM users WHERE id = 1", nil,
		[]string{"id", "name"}, [][]interface{}{{1, "John"}}, []string{"users"})

	// Should have updated
	entry, found := cache.Get("SELECT * FROM users WHERE id = 1", nil)
	if !found {
		t.Fatal("Should find entry")
	}

	if len(entry.Columns) != 2 {
		t.Errorf("Expected 2 columns after update, got %d", len(entry.Columns))
	}
}

// TestCacheStatsAccuracy tests stats accuracy
func TestCacheStatsAccuracy(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Initial stats
	stats := cache.Stats()
	if stats.Hits != 0 || stats.Misses != 0 {
		t.Error("Initial stats should be zero")
	}

	// Miss
	cache.Get("SELECT * FROM non_existent", nil)
	stats = cache.Stats()
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}

	// Set and hit
	cache.Set("SELECT * FROM users", nil, []string{"id"}, [][]interface{}{{1}}, []string{"users"})
	cache.Get("SELECT * FROM users", nil)
	stats = cache.Stats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}
}

// TestCacheHitRateFromStats tests hit rate calculation via stats
func TestCacheHitRateFromStats(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Add entry and hit multiple times
	cache.Set("SELECT * FROM users", nil, []string{"id"}, [][]any{{1}}, []string{"users"})
	cache.Get("SELECT * FROM users", nil)
	cache.Get("SELECT * FROM users", nil)
	cache.Get("SELECT * FROM users", nil)

	// 3 hits, 0 misses
	stats := cache.Stats()
	// HitRate is calculated - may not be exactly 1.0 due to timing
	if stats.HitRate < 0.99 {
		t.Errorf("Expected hit rate close to 1.0, got %f", stats.HitRate)
	}

	// Add a miss
	cache.Get("SELECT * FROM missing", nil)
	stats = cache.Stats()
	// Should have some hits and misses
	if stats.Hits != 3 {
		t.Errorf("Expected 3 hits, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
}

// TestCacheClearAll tests clearing all entries (InvalidateAll)
func TestCacheClearAll(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Add entries
	for i := 0; i < 10; i++ {
		cache.Set("query", []interface{}{i},
			[]string{"id"}, [][]interface{}{{i}}, []string{"users"})
	}

	// Clear all
	cache.InvalidateAll()

	stats := cache.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("Expected 0 entries after InvalidateAll, got %d", stats.EntryCount)
	}

	// CurrentSize should be 0
	if stats.CurrentSize != 0 {
		t.Errorf("Expected current size 0, got %d", stats.CurrentSize)
	}
}

// TestGenerateKeyConsistency tests key generation consistency
func TestGenerateKeyConsistency(t *testing.T) {
	// Same query and args should generate same key
	key1 := generateKey("SELECT * FROM users WHERE id = ?", []interface{}{1})
	key2 := generateKey("SELECT * FROM users WHERE id = ?", []interface{}{1})

	if key1 != key2 {
		t.Error("Same query and args should generate same key")
	}

	// Different args should generate different keys
	key3 := generateKey("SELECT * FROM users WHERE id = ?", []interface{}{2})
	if key1 == key3 {
		t.Error("Different args should generate different keys")
	}

	// Different queries should generate different keys
	key4 := generateKey("SELECT * FROM orders WHERE id = ?", []interface{}{1})
	if key1 == key4 {
		t.Error("Different queries should generate different keys")
	}
}

// TestEstimateSizeAccuracy tests size estimation
func TestEstimateSizeAccuracy(t *testing.T) {
	// Empty result - still has overhead
	size1 := estimateSize([]string{}, [][]interface{}{})
	// Empty result still has base overhead (256 bytes)
	if size1 < 256 {
		t.Errorf("Expected at least overhead for empty result, got %d", size1)
	}

	// Simple result
	size2 := estimateSize([]string{"id", "name"}, [][]interface{}{{1, "test"}})
	if size2 <= 0 {
		t.Error("Size should be positive for non-empty result")
	}

	// Multiple rows - should be at least the overhead
	size3 := estimateSize([]string{"id"}, [][]interface{}{{1}, {2}, {3}})
	if size3 < 256 {
		t.Error("Size should include base overhead")
	}
}
