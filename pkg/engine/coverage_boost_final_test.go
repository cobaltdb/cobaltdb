package engine

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestGetTopQueriesEdgeCases tests GetTopQueries with various edge cases
func TestGetTopQueriesEdgeCases(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Test with empty cache
	results := cache.GetTopQueries(5)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty cache, got %d", len(results))
	}

	// Add some entries with different access counts
	for i := 0; i < 5; i++ {
		stmt, _ := query.Parse("SELECT " + string(rune('a'+i)) + " FROM t")
		cache.Put("query"+string(rune('0'+i)), nil, stmt)
	}

	// Access some queries multiple times to increase their count
	for i := 0; i < 10; i++ {
		cache.Get("query0", nil) // Access query0 10 times
	}
	for i := 0; i < 5; i++ {
		cache.Get("query1", nil) // Access query1 5 times
	}
	for i := 0; i < 3; i++ {
		cache.Get("query2", nil) // Access query2 3 times
	}

	// Test GetTopQueries with n=0 (should default to 10)
	results = cache.GetTopQueries(0)
	if len(results) == 0 {
		t.Error("Expected non-empty results with n=0")
	}

	// Test GetTopQueries with n=2
	results = cache.GetTopQueries(2)
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Test GetTopQueries with n larger than cache size
	results = cache.GetTopQueries(100)
	if len(results) > 5 {
		t.Errorf("Expected at most 5 results, got %d", len(results))
	}
}

// TestGetTopQueriesSorting tests that GetTopQueries returns sorted results
func TestGetTopQueriesSorting(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Add entries
	for i := 0; i < 3; i++ {
		stmt, _ := query.Parse("SELECT " + string(rune('a'+i)) + " FROM t")
		cache.Put("query"+string(rune('0'+i)), nil, stmt)
	}

	// Access in reverse order
	for i := 2; i >= 0; i-- {
		for j := 0; j < i+1; j++ {
			cache.Get("query"+string(rune('0'+i)), nil)
		}
	}

	// Get top queries
	results := cache.GetTopQueries(3)
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Verify sorted by access count (descending)
	for i := 0; i < len(results)-1; i++ {
		if results[i].AccessCount < results[i+1].AccessCount {
			t.Errorf("Results not sorted by access count: %d < %d at position %d",
				results[i].AccessCount, results[i+1].AccessCount, i)
		}
	}
}

// TestWarmCacheEdgeCases tests WarmCache with various edge cases
func TestWarmCacheEdgeCases(t *testing.T) {
	cache := NewQueryPlanCache(1024*1024, 100)

	// Test with empty queries slice
	err := cache.WarmCache([]string{})
	if err != nil {
		t.Errorf("Unexpected error for empty queries: %v", err)
	}

	// Test with invalid queries (should skip them)
	err = cache.WarmCache([]string{
		"NOT A VALID SQL",
		"ALSO INVALID",
	})
	if err != nil {
		t.Errorf("Unexpected error for invalid queries: %v", err)
	}

	// Test with mixed valid and invalid queries
	err = cache.WarmCache([]string{
		"SELECT 1",
		"INVALID SQL HERE",
		"SELECT 2 FROM t",
	})
	if err != nil {
		t.Errorf("Unexpected error for mixed queries: %v", err)
	}

	// Verify at least one valid query was cached
	entry, found := cache.Get("SELECT 1", nil)
	if !found {
		t.Error("Expected 'SELECT 1' to be cached")
	}
	if entry != nil {
		t.Logf("Cached entry SQL: %s", entry.SQL)
	}
}
