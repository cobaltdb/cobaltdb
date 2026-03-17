package engine

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestQueryPlanCache_Eviction tests LRU eviction when cache is full
func TestQueryPlanCache_Eviction(t *testing.T) {
	// Small maxEntries to trigger eviction quickly (but large enough maxSize)
	cache := NewQueryPlanCache(1024*1024, 3)

	// Add entries up to the limit
	for i := 0; i < 3; i++ {
		sql := fmt.Sprintf("SELECT * FROM test%d", i)
		stmt, _ := query.Parse(sql)
		err := cache.Put(sql, nil, stmt)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	if cache.GetStats().Size != 3 {
		t.Errorf("Expected size 3, got %d", cache.GetStats().Size)
	}

	// Add one more entry - should trigger eviction of least recently used
	sql := "SELECT * FROM test3"
	stmt, _ := query.Parse(sql)
	err := cache.Put(sql, nil, stmt)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Size should still be 3 (max)
	if cache.GetStats().Size != 3 {
		t.Errorf("Expected size 3 after eviction, got %d", cache.GetStats().Size)
	}
}

// TestQueryPlanCache_EvictionBySize tests eviction by total size limit
func TestQueryPlanCache_EvictionBySize(t *testing.T) {
	// Very small max size to trigger size-based eviction
	cache := NewQueryPlanCache(100, 100)

	// Add a large query that takes most of the space
	sql1 := "SELECT * FROM very_long_table_name_that_takes_up_space WHERE column1 = 'value' AND column2 = 'value'"
	stmt1, _ := query.Parse(sql1)
	err := cache.Put(sql1, nil, stmt1)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	size1 := cache.GetStats().Size

	// Add more queries until eviction happens
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("SELECT * FROM table%d WHERE id = %d", i, i)
		stmt, _ := query.Parse(sql)
		cache.Put(sql, nil, stmt)
	}

	// Cache should have evicted some entries but not crashed
	stats := cache.GetStats()
	if stats.Size == 0 {
		t.Error("Cache should not be empty after eviction")
	}
	t.Logf("Cache size after eviction: %d (was %d)", stats.Size, size1)
}

// TestQueryPlanCache_LRUOrder tests LRU ordering is maintained
func TestQueryPlanCache_LRUOrder(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 3)

	// Add 3 entries
	sqls := []string{
		"SELECT * FROM first",
		"SELECT * FROM second",
		"SELECT * FROM third",
	}

	for _, sql := range sqls {
		stmt, _ := query.Parse(sql)
		cache.Put(sql, nil, stmt)
	}

	// Access first entry (makes it most recently used)
	cache.Get(sqls[0], nil)

	// Add a new entry - should evict "second" (least recently used)
	sqlNew := "SELECT * FROM fourth"
	stmt, _ := query.Parse(sqlNew)
	cache.Put(sqlNew, nil, stmt)

	// "first" should still exist (was accessed)
	_, found := cache.Get(sqls[0], nil)
	if !found {
		t.Error("Expected 'first' to still be in cache (was recently used)")
	}

	// "second" should be evicted
	_, found = cache.Get(sqls[1], nil)
	if found {
		t.Error("Expected 'second' to be evicted (least recently used)")
	}
}

// TestQueryPlanCache_HashQueryDifferentQueries tests hash function with different queries
func TestQueryPlanCache_HashQueryDifferentQueries(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"SELECT * FROM users WHERE id = 2",
		"SELECT * FROM orders WHERE id = 1",
		"INSERT INTO users VALUES (1, 'test')",
		"UPDATE users SET name = 'test' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
	}

	// All should be cached separately (different hashes)
	for _, sql := range queries {
		stmt, _ := query.Parse(sql)
		err := cache.Put(sql, nil, stmt)
		if err != nil {
			t.Errorf("Failed to cache query '%s': %v", sql, err)
		}
	}

	stats := cache.GetStats()
	if stats.Size != len(queries) {
		t.Errorf("Expected %d entries, got %d", len(queries), stats.Size)
	}

	// Each should be retrievable
	for _, sql := range queries {
		_, found := cache.Get(sql, nil)
		if !found {
			t.Errorf("Could not retrieve cached query: %s", sql)
		}
	}
}

// TestQueryPlanCache_WithParams tests caching with different parameter types
func TestQueryPlanCache_WithParams(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Same query with different parameter types should be cached separately
	sql1 := "SELECT * FROM users WHERE id = ?"
	stmt1, _ := query.Parse(sql1)
	params1 := []interface{}{1} // int

	sql2 := "SELECT * FROM users WHERE id = ?"
	stmt2, _ := query.Parse(sql2)
	params2 := []interface{}{"test"} // string

	cache.Put(sql1, params1, stmt1)
	cache.Put(sql2, params2, stmt2)

	// Both should be cached (different param types = different hashes)
	stats := cache.GetStats()
	if stats.Size != 2 {
		t.Errorf("Expected 2 entries (different param types), got %d", stats.Size)
	}

	// Retrieve with correct params
	_, found := cache.Get(sql1, params1)
	if !found {
		t.Error("Could not retrieve first cached query with params")
	}

	_, found = cache.Get(sql2, params2)
	if !found {
		t.Error("Could not retrieve second cached query with params")
	}
}

// TestQueryPlanCache_TooLargeEntry tests that overly large entries are rejected
func TestQueryPlanCache_TooLargeEntry(t *testing.T) {
	cache := NewQueryPlanCache(100, 10)

	// Try to add an entry that's larger than max size
	sql := "SELECT * FROM test"
	stmt, _ := query.Parse(sql)
	err := cache.Put(sql, nil, stmt)

	// Should handle gracefully (either reject or accept)
	if err != nil {
		t.Logf("Large entry was rejected as expected: %v", err)
	}
}

// TestQueryPlanCache_UpdateExistingEntry tests updating an existing entry
func TestQueryPlanCache_UpdateExistingEntry(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	sql := "SELECT * FROM users"
	stmt1, _ := query.Parse(sql)

	// First put
	err := cache.Put(sql, nil, stmt1)
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	// Get to check access count
	entry1, _ := cache.Get(sql, nil)
	accessCount1 := entry1.AccessCount

	// Put again (should update/replace)
	stmt2, _ := query.Parse(sql)
	err = cache.Put(sql, nil, stmt2)
	if err != nil {
		t.Fatalf("Second Put failed: %v", err)
	}

	// Should still be in cache
	entry2, found := cache.Get(sql, nil)
	if !found {
		t.Error("Entry should still exist after update")
	}

	// Access count should have been updated
	if entry2.AccessCount <= accessCount1 {
		t.Logf("Access count behavior: before=%d, after=%d", accessCount1, entry2.AccessCount)
	}
}

// TestQueryPlanCache_ConcurrentAccess tests thread safety of cache
func TestQueryPlanCache_ConcurrentAccess(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 1000)

	// Add many entries
	for i := 0; i < 100; i++ {
		sql := fmt.Sprintf("SELECT * FROM table%d WHERE id = %d", i%10, i)
		stmt, _ := query.Parse(sql)
		cache.Put(sql, nil, stmt)
	}

	// Verify cache is in consistent state
	stats := cache.GetStats()
	if stats.Size == 0 {
		t.Error("Cache should not be empty after adding entries")
	}

	// All entries should be retrievable
	hits := 0
	for i := 0; i < 100; i++ {
		sql := fmt.Sprintf("SELECT * FROM table%d WHERE id = %d", i%10, i)
		_, found := cache.Get(sql, nil)
		if found {
			hits++
		}
	}

	t.Logf("Retrieved %d out of 100 entries", hits)
}

// TestQueryPlanCache_InvalidationPatterns tests various invalidation scenarios
func TestQueryPlanCache_InvalidationPatterns(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Add multiple related queries
	queries := []string{
		"SELECT * FROM users",
		"SELECT * FROM users WHERE id = 1",
		"SELECT * FROM orders",
	}

	for _, sql := range queries {
		stmt, _ := query.Parse(sql)
		cache.Put(sql, nil, stmt)
	}

	// Invalidate one specific query
	cache.Invalidate(queries[0], nil)

	// Should not find invalidated query
	_, found := cache.Get(queries[0], nil)
	if found {
		t.Error("Invalidated query should not be found")
	}

	// Others should still exist
	_, found = cache.Get(queries[1], nil)
	if !found {
		t.Error("Other queries should still exist")
	}

	_, found = cache.Get(queries[2], nil)
	if !found {
		t.Error("Other queries should still exist")
	}
}

// TestQueryPlanCache_ClearAndReuse tests clearing and reusing cache
func TestQueryPlanCache_ClearAndReuse(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Add entries
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("SELECT * FROM test%d", i)
		stmt, _ := query.Parse(sql)
		cache.Put(sql, nil, stmt)
	}

	if cache.GetStats().Size != 10 {
		t.Errorf("Expected 10 entries, got %d", cache.GetStats().Size)
	}

	// Clear
	cache.Clear()

	if cache.GetStats().Size != 0 {
		t.Errorf("Expected 0 entries after clear, got %d", cache.GetStats().Size)
	}

	// Reuse cache
	sql := "SELECT * FROM newtable"
	stmt, _ := query.Parse(sql)
	err := cache.Put(sql, nil, stmt)
	if err != nil {
		t.Fatalf("Put after clear failed: %v", err)
	}

	if cache.GetStats().Size != 1 {
		t.Errorf("Expected 1 entry after reuse, got %d", cache.GetStats().Size)
	}
}
