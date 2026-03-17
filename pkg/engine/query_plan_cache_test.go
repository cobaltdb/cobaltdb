package engine

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestNewQueryPlanCache(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)
	if cache == nil {
		t.Fatal("NewQueryPlanCache returned nil")
	}
	if cache.maxSize != 1024*1024 {
		t.Errorf("Expected maxSize 1048576, got %d", cache.maxSize)
	}
	if cache.maxEntries != 100 {
		t.Errorf("Expected maxEntries 100, got %d", cache.maxEntries)
	}
}

func TestQueryPlanCache_DefaultValues(t *testing.T) {
	cache := NewQueryPlanCache(0, 0)
	if cache.maxSize != 64*1024*1024 {
		t.Errorf("Expected default maxSize 64MB, got %d", cache.maxSize)
	}
	if cache.maxEntries != 1000 {
		t.Errorf("Expected default maxEntries 1000, got %d", cache.maxEntries)
	}
}

func TestQueryPlanCache_PutAndGet(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	sql := "SELECT * FROM test WHERE id = 1"
	stmt, err := query.Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Put entry
	err = cache.Put(sql, nil, stmt)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// Get entry
	entry, found := cache.Get(sql, nil)
	if !found {
		t.Error("Expected to find cached entry")
	}
	if entry.SQL != sql {
		t.Errorf("Expected SQL %s, got %s", sql, entry.SQL)
	}
	if entry.AccessCount != 2 { // 1 from Put + 1 from Get
		t.Errorf("Expected AccessCount 2, got %d", entry.AccessCount)
	}
}

func TestQueryPlanCache_GetNotFound(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	_, found := cache.Get("SELECT * FROM nonexistent", nil)
	if found {
		t.Error("Expected not to find entry")
	}
}

func TestQueryPlanCache_Statistics(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Initial stats
	stats := cache.GetStats()
	if stats.Hits != 0 {
		t.Errorf("Expected 0 hits, got %d", stats.Hits)
	}
	if stats.Misses != 0 {
		t.Errorf("Expected 0 misses, got %d", stats.Misses)
	}

	// Add and retrieve entry
	sql := "SELECT * FROM test"
	stmt, _ := query.Parse(sql)
	cache.Put(sql, nil, stmt)

	// Miss
	cache.Get("SELECT * FROM other", nil)

	// Hit
	cache.Get(sql, nil)

	stats = cache.GetStats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
	if stats.Size != 1 {
		t.Errorf("Expected size 1, got %d", stats.Size)
	}
}

func TestQueryPlanCache_HitRate(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	sql := "SELECT * FROM test"
	stmt, _ := query.Parse(sql)
	cache.Put(sql, nil, stmt)

	// 3 hits
	cache.Get(sql, nil)
	cache.Get(sql, nil)
	cache.Get(sql, nil)

	// 1 miss
	cache.Get("SELECT * FROM other", nil)

	stats := cache.GetStats()
	expectedHitRate := 75.0 // 3 hits out of 4 total
	if stats.HitRate != expectedHitRate {
		t.Errorf("Expected hit rate %.1f%%, got %.1f%%", expectedHitRate, stats.HitRate)
	}
}

func TestQueryPlanCache_Invalidate(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	sql := "SELECT * FROM test"
	stmt, _ := query.Parse(sql)
	cache.Put(sql, nil, stmt)

	// Verify entry exists
	_, found := cache.Get(sql, nil)
	if !found {
		t.Error("Expected to find entry before invalidation")
	}

	// Invalidate
	cache.Invalidate(sql, nil)

	// Verify entry is gone
	_, found = cache.Get(sql, nil)
	if found {
		t.Error("Expected entry to be invalidated")
	}
}

func TestQueryPlanCache_Clear(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Add multiple entries
	for i := 0; i < 5; i++ {
		sql := "SELECT * FROM test" + string(rune('0'+i))
		stmt, _ := query.Parse(sql)
		cache.Put(sql, nil, stmt)
	}

	if cache.GetStats().Size != 5 {
		t.Errorf("Expected size 5, got %d", cache.GetStats().Size)
	}

	// Clear cache
	cache.Clear()

	if cache.GetStats().Size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", cache.GetStats().Size)
	}
}

func TestQueryPlanCache_TopQueries(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Add entries with different access patterns
	sqls := []string{
		"SELECT * FROM users",
		"SELECT * FROM orders",
		"SELECT * FROM products",
	}

	for i, sql := range sqls {
		stmt, _ := query.Parse(sql)
		cache.Put(sql, nil, stmt)

		// Access i+1 times
		for j := 0; j <= i; j++ {
			cache.Get(sql, nil)
		}
	}

	// Get top 2 queries
	top := cache.GetTopQueries(2)
	if len(top) != 2 {
		t.Errorf("Expected 2 top queries, got %d", len(top))
	}

	// Most accessed should be last one (4 accesses: 1 from Put + 3 from Get)
	if top[0].AccessCount != 4 {
		t.Errorf("Expected top query to have 4 accesses, got %d", top[0].AccessCount)
	}
}

func TestQueryPlanCache_WarmCache(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	queries := []string{
		"SELECT * FROM users",
		"SELECT * FROM orders",
		"SELECT COUNT(*) FROM products",
	}

	err := cache.WarmCache(queries)
	if err != nil {
		t.Errorf("WarmCache failed: %v", err)
	}

	stats := cache.GetStats()
	if stats.Size != 3 {
		t.Errorf("Expected 3 cached queries after warmup, got %d", stats.Size)
	}

	// Verify all queries are cached
	for _, sql := range queries {
		_, found := cache.Get(sql, nil)
		if !found {
			t.Errorf("Expected to find cached query: %s", sql)
		}
	}
}

func TestQueryPlanCache_ResetStats(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	sql := "SELECT * FROM test"
	stmt, _ := query.Parse(sql)
	cache.Put(sql, nil, stmt)
	cache.Get(sql, nil)
	cache.Get("other", nil)

	// Reset stats
	cache.ResetStats()

	stats := cache.GetStats()
	if stats.Hits != 0 {
		t.Errorf("Expected 0 hits after reset, got %d", stats.Hits)
	}
	if stats.Misses != 0 {
		t.Errorf("Expected 0 misses after reset, got %d", stats.Misses)
	}
	// Size should still be preserved
	if stats.Size != 1 {
		t.Errorf("Expected size 1 after reset, got %d", stats.Size)
	}
}

func TestQueryPlanCache_AccessCount(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	sql := "SELECT * FROM test"
	stmt, _ := query.Parse(sql)
	cache.Put(sql, nil, stmt)

	// Access multiple times
	for i := 0; i < 5; i++ {
		cache.Get(sql, nil)
	}

	entry, _ := cache.Get(sql, nil)
	if entry.AccessCount != 7 { // 1 from Put + 6 from Get (Get returns it too)
		t.Errorf("Expected AccessCount 7, got %d", entry.AccessCount)
	}
}
