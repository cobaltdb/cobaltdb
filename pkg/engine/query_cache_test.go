package engine

import (
	"fmt"
	"testing"
	"time"
)

func TestQueryCacheBasic(t *testing.T) {
	config := DefaultCacheConfig()
	config.MaxEntries = 10
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	// Set cache entry
	err := cache.Set("SELECT * FROM users", []interface{}{}, columns, rows, []string{"users"}, time.Minute)
	if err != nil {
		t.Fatalf("Failed to set cache: %v", err)
	}

	// Get cache entry
	cachedCols, cachedRows, found := cache.Get("SELECT * FROM users", []interface{}{})
	if !found {
		t.Error("Expected to find cached result")
	}

	if len(cachedRows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(cachedRows))
	}

	if len(cachedCols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cachedCols))
	}
}

func TestQueryCacheMiss(t *testing.T) {
	config := DefaultCacheConfig()
	cache := NewQueryCache(config)
	defer cache.Close()

	// Try to get non-existent entry
	_, _, found := cache.Get("SELECT * FROM unknown", []interface{}{})
	if found {
		t.Error("Expected cache miss for unknown query")
	}

	stats := cache.Stats()
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
}

func TestQueryCacheExpiration(t *testing.T) {
	config := DefaultCacheConfig()
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"id"}
	rows := [][]interface{}{{1}}

	// Set with very short TTL
	err := cache.Set("SELECT * FROM test", []interface{}{}, columns, rows, []string{"test"}, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to set cache: %v", err)
	}

	// Should find immediately
	_, _, found := cache.Get("SELECT * FROM test", []interface{}{})
	if !found {
		t.Error("Expected to find cached result immediately")
	}

	// Wait for expiration
	time.Sleep(50 * time.Millisecond)

	// Should not find after expiration
	_, _, found = cache.Get("SELECT * FROM test", []interface{}{})
	if found {
		t.Error("Expected cache entry to be expired")
	}
}

func TestQueryCacheInvalidation(t *testing.T) {
	config := DefaultCacheConfig()
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"id"}
	rows := [][]interface{}{{1}}

	// Set cache entries for different tables
	cache.Set("SELECT * FROM users", []interface{}{}, columns, rows, []string{"users"}, time.Minute)
	cache.Set("SELECT * FROM orders", []interface{}{}, columns, rows, []string{"orders"}, time.Minute)

	// Invalidate users table
	cache.Invalidate("users")

	// Users query should be gone
	_, _, found := cache.Get("SELECT * FROM users", []interface{}{})
	if found {
		t.Error("Expected users cache to be invalidated")
	}

	// Orders query should still exist
	_, _, found = cache.Get("SELECT * FROM orders", []interface{}{})
	if !found {
		t.Error("Expected orders cache to still exist")
	}
}

func TestQueryCacheLRUEviction(t *testing.T) {
	config := DefaultCacheConfig()
	config.MaxEntries = 3
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"id"}
	rows := [][]interface{}{{1}}

	// Add 3 entries
	cache.Set("SELECT 1", []interface{}{}, columns, rows, []string{"t1"}, time.Minute)
	cache.Set("SELECT 2", []interface{}{}, columns, rows, []string{"t2"}, time.Minute)
	cache.Set("SELECT 3", []interface{}{}, columns, rows, []string{"t3"}, time.Minute)

	// Access first entry to make it recently used
	cache.Get("SELECT 1", []interface{}{})

	// Add 4th entry - should evict SELECT 2 (least recently used)
	cache.Set("SELECT 4", []interface{}{}, columns, rows, []string{"t4"}, time.Minute)

	// SELECT 1 should exist (was accessed)
	_, _, found := cache.Get("SELECT 1", []interface{}{})
	if !found {
		t.Error("Expected SELECT 1 to exist (was recently accessed)")
	}

	// SELECT 2 should be evicted
	_, _, found = cache.Get("SELECT 2", []interface{}{})
	if found {
		t.Error("Expected SELECT 2 to be evicted")
	}

	stats := cache.Stats()
	if stats.Evictions != 1 {
		t.Errorf("Expected 1 eviction, got %d", stats.Evictions)
	}
}

func TestQueryCacheStats(t *testing.T) {
	config := DefaultCacheConfig()
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"id"}
	rows := [][]interface{}{{1}}

	// Miss
	cache.Get("SELECT 1", []interface{}{})

	// Set and hit
	cache.Set("SELECT 1", []interface{}{}, columns, rows, []string{"t1"}, time.Minute)
	cache.Get("SELECT 1", []interface{}{})
	cache.Get("SELECT 1", []interface{}{})

	stats := cache.Stats()

	if stats.Hits != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.Hits)
	}

	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}

	if stats.HitRate != 66.66666666666666 {
		t.Errorf("Expected hit rate 66.67%%, got %f", stats.HitRate)
	}
}

func TestQueryCacheDifferentArgs(t *testing.T) {
	config := DefaultCacheConfig()
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"id"}
	rows1 := [][]interface{}{{1}}
	rows2 := [][]interface{}{{2}}

	// Cache same SQL with different args
	cache.Set("SELECT * FROM users WHERE id = ?", []interface{}{1}, columns, rows1, []string{"users"}, time.Minute)
	cache.Set("SELECT * FROM users WHERE id = ?", []interface{}{2}, columns, rows2, []string{"users"}, time.Minute)

	// Get with arg 1
	_, cachedRows, found := cache.Get("SELECT * FROM users WHERE id = ?", []interface{}{1})
	if !found {
		t.Error("Expected to find result for id=1")
	}
	if cachedRows[0][0] != 1 {
		t.Errorf("Expected id=1, got %v", cachedRows[0][0])
	}

	// Get with arg 2
	_, cachedRows, found = cache.Get("SELECT * FROM users WHERE id = ?", []interface{}{2})
	if !found {
		t.Error("Expected to find result for id=2")
	}
	if cachedRows[0][0] != 2 {
		t.Errorf("Expected id=2, got %v", cachedRows[0][0])
	}
}

func TestQueryCacheKeyNormalization(t *testing.T) {
	cache := NewQueryCache(DefaultCacheConfig())
	defer cache.Close()

	columns := []string{"id"}
	rows := [][]interface{}{{1}}

	// Same query with different whitespace
	cache.Set("SELECT   *   FROM   users", []interface{}{}, columns, rows, []string{"users"}, time.Minute)

	// Should match normalized version
	_, _, found := cache.Get("select * from users", []interface{}{})
	if !found {
		t.Error("Expected normalized SQL to match")
	}
}

func TestQueryCacheMaxEntrySize(t *testing.T) {
	config := DefaultCacheConfig()
	config.MaxEntrySize = 100 // Very small
	cache := NewQueryCache(config)
	defer cache.Close()

	// Create a large result (over 100 bytes)
	columns := []string{"data"}
	rows := [][]interface{}{{"this is a very large string that exceeds the limit and is definitely over one hundred bytes in total length here"}}

	err := cache.Set("SELECT * FROM large", []interface{}{}, columns, rows, []string{"large"}, time.Minute)
	if err == nil {
		t.Error("Expected error for oversized entry")
	}
}

func TestQueryCacheInvalidateAll(t *testing.T) {
	config := DefaultCacheConfig()
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"id"}
	rows := [][]interface{}{{1}}

	// Add multiple entries
	cache.Set("SELECT 1", []interface{}{}, columns, rows, []string{"t1"}, time.Minute)
	cache.Set("SELECT 2", []interface{}{}, columns, rows, []string{"t2"}, time.Minute)
	cache.Set("SELECT 3", []interface{}{}, columns, rows, []string{"t3"}, time.Minute)

	// Invalidate all
	cache.InvalidateAll()

	// All should be gone
	for i := 1; i <= 3; i++ {
		_, _, found := cache.Get("SELECT "+string(rune('0'+i)), []interface{}{})
		if found {
			t.Errorf("Expected SELECT %d to be invalidated", i)
		}
	}
}

func TestDefaultCacheConfig(t *testing.T) {
	config := DefaultCacheConfig()

	if !config.Enabled {
		t.Error("Expected cache to be enabled by default")
	}

	if config.MaxSize != 100*1024*1024 {
		t.Errorf("Expected max size 100MB, got %d", config.MaxSize)
	}

	if config.MaxEntries != 10000 {
		t.Errorf("Expected max entries 10000, got %d", config.MaxEntries)
	}
}

func TestQueryCacheDisabled(t *testing.T) {
	config := DefaultCacheConfig()
	config.Enabled = false
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"id"}
	rows := [][]interface{}{{1}}

	// Set should be no-op
	err := cache.Set("SELECT 1", []interface{}{}, columns, rows, []string{"t1"}, time.Minute)
	if err != nil {
		t.Errorf("Expected no error when disabled: %v", err)
	}

	// Get should always miss
	_, _, found := cache.Get("SELECT 1", []interface{}{})
	if found {
		t.Error("Expected cache miss when disabled")
	}
}

func TestEstimateValueSize(t *testing.T) {
	cache := NewQueryCache(DefaultCacheConfig())
	defer cache.Close()

	tests := []struct {
		value interface{}
		min   int64
		max   int64
	}{
		{nil, 1, 1},
		{true, 1, 1},
		{int(42), 8, 8},
		{int64(42), 8, 8},
		{float32(3.14), 4, 4},
		{float64(3.14), 8, 8},
		{"hello", 5, 5},
		{[]byte("hello"), 5, 5},
	}

	for _, tc := range tests {
		size := cache.estimateValueSize(tc.value)
		if size < tc.min || size > tc.max {
			t.Errorf("Value %v: expected size between %d and %d, got %d", tc.value, tc.min, tc.max, size)
		}
	}
}

func TestCacheableQuery(t *testing.T) {
	cache := NewQueryCache(DefaultCacheConfig())
	defer cache.Close()

	tests := []struct {
		sql       string
		cacheable bool
	}{
		{"SELECT * FROM users", true},
		{"select id from orders", true},
		{"INSERT INTO users VALUES (1)", false},
		{"UPDATE users SET x = 1", false},
		{"DELETE FROM users", false},
		{"SELECT * FROM users WHERE created > NOW()", false},
		{"SELECT * FROM users WHERE id = RANDOM()", false},
		{"SELECT * FROM users FOR UPDATE", false},
	}

	for _, tc := range tests {
		result := cache.CacheableQuery(tc.sql)
		if result != tc.cacheable {
			t.Errorf("SQL '%s': expected cacheable=%v, got %v", tc.sql, tc.cacheable, result)
		}
	}
}

func TestExtractTables(t *testing.T) {
	cache := NewQueryCache(DefaultCacheConfig())
	defer cache.Close()

	tests := []struct {
		sql      string
		expected []string
	}{
		{"SELECT * FROM users", []string{"users"}},
		{"SELECT * FROM users, orders", []string{"users"}},
		{"SELECT * FROM users JOIN orders ON users.id = orders.user_id", []string{"users", "orders"}},
	}

	for _, tc := range tests {
		tables := cache.ExtractTables(tc.sql)
		found := false
		for _, exp := range tc.expected {
			for _, tbl := range tables {
				if tbl == exp {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("SQL '%s': expected table %s not found in %v", tc.sql, exp, tables)
			}
		}
	}
}

func TestQueryCacheCopyRows(t *testing.T) {
	cache := NewQueryCache(DefaultCacheConfig())
	defer cache.Close()

	original := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	copy := cache.copyRows(original)

	// Verify deep copy
	if len(copy) != len(original) {
		t.Error("Row count mismatch")
	}

	// Modify copy - should not affect original
	copy[0][0] = 999
	if original[0][0] == 999 {
		t.Error("Expected deep copy - modifying copy affected original")
	}
}

func TestQueryCacheSizeLimit(t *testing.T) {
	config := DefaultCacheConfig()
	config.MaxSize = 1000
	config.MaxEntries = 100
	cache := NewQueryCache(config)
	defer cache.Close()

	columns := []string{"data"}
	rows := [][]interface{}{{"some data"}}

	// Add entries until size limit is reached
	for i := 0; i < 100; i++ {
		sql := fmt.Sprintf("SELECT * FROM table%d", i)
		table := fmt.Sprintf("table%d", i)
		err := cache.Set(sql, []interface{}{}, columns, rows, []string{table}, time.Minute)
		if err != nil {
			t.Fatalf("Failed to set cache entry %d: %v", i, err)
		}
	}

	stats := cache.Stats()
	if stats.Size > config.MaxSize {
		t.Errorf("Cache size %d exceeds max %d", stats.Size, config.MaxSize)
	}
	if stats.Evictions == 0 {
		t.Error("Expected some evictions due to size limit")
	}
}
