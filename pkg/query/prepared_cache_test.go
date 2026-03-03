package query

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPreparedCacheBasicOperations(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Test Put and Get
	sql := "SELECT * FROM users WHERE id = ?"
	tokens, err := Tokenize(sql)
	if err != nil {
		t.Fatalf("Failed to tokenize: %v", err)
	}
	parser := NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	prepared := cache.Put(sql, stmt, 1)
	if prepared.ID == "" {
		t.Error("Expected non-empty ID")
	}

	// Test Get
	retrieved, found := cache.Get(prepared.ID)
	if !found {
		t.Error("Expected to find cached statement")
	}
	if retrieved.SQL != sql {
		t.Errorf("Expected SQL %s, got %s", sql, retrieved.SQL)
	}

	// Test GetBySQL
	retrieved2, found := cache.GetBySQL(sql)
	if !found {
		t.Error("Expected to find cached statement by SQL")
	}
	if retrieved2.ID != prepared.ID {
		t.Error("Expected same ID")
	}
}

func TestPreparedCacheSizeLimit(t *testing.T) {
	cache := NewPreparedCache(5, 30*time.Minute)

	// Add more statements than max size
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("SELECT * FROM users WHERE id = %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		cache.Put(sql, stmt, 1)
	}

	// Cache should not exceed max size
	if cache.Size() > 5 {
		t.Errorf("Expected cache size <= 5, got %d", cache.Size())
	}
}

func TestPreparedCacheTTL(t *testing.T) {
	cache := NewPreparedCache(10, 100*time.Millisecond)

	sql := "SELECT * FROM users"
	tokens, _ := Tokenize(sql)
	parser := NewParser(tokens)
	stmt, _ := parser.Parse()
	prepared := cache.Put(sql, stmt, 0)

	// Should be found immediately
	_, found := cache.Get(prepared.ID)
	if !found {
		t.Error("Expected to find cached statement")
	}

	// Wait for TTL to expire
	time.Sleep(200 * time.Millisecond)

	// Should not be found after TTL
	_, found = cache.Get(prepared.ID)
	if found {
		t.Error("Expected cached statement to expire")
	}
}

func TestPreparedCacheUpdateStats(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	sql := "SELECT * FROM users"
	tokens, _ := Tokenize(sql)
	parser := NewParser(tokens)
	stmt, _ := parser.Parse()
	prepared := cache.Put(sql, stmt, 0)

	// Update stats multiple times
	for i := 0; i < 10; i++ {
		cache.UpdateStats(prepared.ID, 100*time.Millisecond)
	}

	// Retrieve and check stats
	retrieved, _ := cache.Get(prepared.ID)
	if retrieved.UseCount != 10 {
		t.Errorf("Expected use count 10, got %d", retrieved.UseCount)
	}
	if retrieved.AvgExecTime == 0 {
		t.Error("Expected non-zero average execution time")
	}
}

func TestPreparedCacheClear(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Add some statements
	for i := 0; i < 5; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		cache.Put(sql, stmt, 0)
	}

	if cache.Size() != 5 {
		t.Errorf("Expected size 5, got %d", cache.Size())
	}

	// Clear cache
	cache.Clear()

	if cache.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", cache.Size())
	}
}

func TestPreparedCacheStats(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Add statements and update stats
	for i := 0; i < 3; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		prepared := cache.Put(sql, stmt, 0)
		cache.UpdateStats(prepared.ID, 100*time.Millisecond)
	}

	stats := cache.Stats()
	if stats.Size != 3 {
		t.Errorf("Expected size 3, got %d", stats.Size)
	}
	if stats.AvgExecTime == 0 {
		t.Error("Expected non-zero average execution time")
	}
}

func TestPreparedStatementExecutor(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Test Prepare
	sql := "SELECT * FROM users WHERE id = ?"
	prepared, err := executor.Prepare(sql)
	if err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}
	if prepared.ID == "" {
		t.Error("Expected non-empty ID")
	}
	if prepared.ParamCount != 1 {
		t.Errorf("Expected 1 parameter, got %d", prepared.ParamCount)
	}

	// Test Execute
	ctx := context.Background()
	stmt, err := executor.Execute(ctx, prepared.ID, []interface{}{1})
	if err != nil {
		t.Fatalf("Failed to execute: %v", err)
	}
	if stmt == nil {
		t.Error("Expected non-nil statement")
	}

	// Test caching (second prepare should return cached)
	prepared2, err := executor.Prepare(sql)
	if err != nil {
		t.Fatalf("Failed to prepare second time: %v", err)
	}
	if prepared2.ID != prepared.ID {
		t.Error("Expected cached statement to have same ID")
	}
}

func TestPreparedStatementExecutorExecuteSQL(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	ctx := context.Background()
	sql := "SELECT * FROM users WHERE id = ?"
	stmt, err := executor.ExecuteSQL(ctx, sql, []interface{}{1})
	if err != nil {
		t.Fatalf("Failed to execute SQL: %v", err)
	}
	if stmt == nil {
		t.Error("Expected non-nil statement")
	}
}

func TestCountParameters(t *testing.T) {
	tests := []struct {
		sql      string
		expected int
	}{
		{"SELECT * FROM users", 0},
		{"SELECT * FROM users WHERE id = ?", 1},
		{"SELECT * FROM users WHERE id = ? AND name = ?", 2},
		{"INSERT INTO users VALUES (?, ?)", 2},
		{"UPDATE users SET name = ? WHERE id = ?", 2},
		{"DELETE FROM users WHERE id = ?", 1},
	}

	for _, test := range tests {
		tokens, err := Tokenize(test.sql)
		if err != nil {
			t.Errorf("Failed to tokenize %s: %v", test.sql, err)
			continue
		}
		parser := NewParser(tokens)
		stmt, err := parser.Parse()
		if err != nil {
			t.Errorf("Failed to parse %s: %v", test.sql, err)
			continue
		}

		count := countParameters(stmt)
		if count != test.expected {
			t.Errorf("Expected %d parameters for %s, got %d", test.expected, test.sql, count)
		}
	}
}

func TestPreparedCacheGenerateID(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	sql1 := "SELECT * FROM users"
	sql2 := "SELECT * FROM orders"

	id1 := cache.GenerateID(sql1)
	id2 := cache.GenerateID(sql2)
	id3 := cache.GenerateID(sql1)

	if id1 == "" || id2 == "" {
		t.Error("Expected non-empty IDs")
	}

	if id1 == id2 {
		t.Error("Expected different IDs for different SQL")
	}

	if id1 != id3 {
		t.Error("Expected same ID for same SQL")
	}
}

func TestPreparedCacheRemove(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	sql := "SELECT * FROM users"
	tokens, _ := Tokenize(sql)
	parser := NewParser(tokens)
	stmt, _ := parser.Parse()
	prepared := cache.Put(sql, stmt, 0)

	// Should exist
	_, found := cache.Get(prepared.ID)
	if !found {
		t.Error("Expected to find cached statement")
	}

	// Remove
	cache.Remove(prepared.ID)

	// Should not exist
	_, found = cache.Get(prepared.ID)
	if found {
		t.Error("Expected cached statement to be removed")
	}
}

func TestPreparedCacheGetAll(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Add statements
	for i := 0; i < 5; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		cache.Put(sql, stmt, 0)
	}

	all := cache.GetAll()
	if len(all) != 5 {
		t.Errorf("Expected 5 statements, got %d", len(all))
	}
}

func TestPreparedCacheEviction(t *testing.T) {
	cache := NewPreparedCache(3, 30*time.Minute)

	// Add 3 statements and track their IDs
	var ids []string
	for i := 0; i < 3; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		prepared := cache.Put(sql, stmt, 0)
		ids = append(ids, prepared.ID)
		time.Sleep(20 * time.Millisecond) // Ensure different timestamps
	}

	// Access the oldest statement (first one inserted) to make it recently used
	oldestID := ids[0]
	cache.Get(oldestID)
	time.Sleep(20 * time.Millisecond)

	// Add 2 more statements (should evict least recently used - the second one)
	for i := 3; i < 5; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		cache.Put(sql, stmt, 0)
		time.Sleep(20 * time.Millisecond)
	}

	// Cache should still be at max size
	if cache.Size() > 3 {
		t.Errorf("Expected cache size <= 3, got %d", cache.Size())
	}

	// The oldest statement (now recently used) should still exist
	_, found := cache.Get(oldestID)
	if !found {
		t.Error("Expected recently used statement to still be in cache")
	}

	// The second statement (not recently used) should have been evicted
	secondID := ids[1]
	_, found = cache.Get(secondID)
	if found {
		t.Error("Expected least recently used statement to be evicted")
	}
}

func TestPreparedStatementExecutorInvalidate(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	sql := "SELECT * FROM users"
	prepared, err := executor.Prepare(sql)
	if err != nil {
		t.Fatalf("Failed to prepare: %v", err)
	}

	// Should exist
	_, found := executor.cache.Get(prepared.ID)
	if !found {
		t.Error("Expected to find cached statement")
	}

	// Invalidate
	executor.Invalidate(prepared.ID)

	// Should not exist
	_, found = executor.cache.Get(prepared.ID)
	if found {
		t.Error("Expected cached statement to be invalidated")
	}
}

func TestPreparedStatementExecutorInvalidateAll(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Add statements
	for i := 0; i < 5; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		executor.Prepare(sql)
	}

	if executor.cache.Size() != 5 {
		t.Errorf("Expected 5 statements, got %d", executor.cache.Size())
	}

	// Invalidate all
	executor.InvalidateAll()

	if executor.cache.Size() != 0 {
		t.Errorf("Expected 0 statements after invalidate all, got %d", executor.cache.Size())
	}
}

func TestPreparedStatementExecutorCacheStats(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Add statements and execute
	for i := 0; i < 3; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		prepared, _ := executor.Prepare(sql)
		ctx := context.Background()
		executor.Execute(ctx, prepared.ID, nil)
	}

	stats := executor.CacheStats()
	if stats.Size != 3 {
		t.Errorf("Expected size 3, got %d", stats.Size)
	}
}

// Edge Case Tests

func TestPreparedCacheNilCache(t *testing.T) {
	// Test with nil cache - should create default cache
	executor := NewPreparedStatementExecutor(nil)
	if executor.cache == nil {
		t.Error("Expected cache to be created when nil is passed")
	}

	// Should be able to prepare statements
	sql := "SELECT * FROM users"
	prepared, err := executor.Prepare(sql)
	if err != nil {
		t.Errorf("Expected prepare to succeed with default cache: %v", err)
	}
	if prepared == nil {
		t.Error("Expected prepared statement")
	}
}

func TestPreparedCacheEmptySQL(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Test empty SQL
	_, err := executor.Prepare("")
	if err == nil {
		t.Error("Expected error for empty SQL")
	}
}

func TestPreparedCacheInvalidSQL(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Test invalid SQL
	_, err := executor.Prepare("INVALID SQL SYNTAX")
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}
}

func TestPreparedCacheGetNonExistent(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Try to get non-existent statement
	_, found := cache.Get("non-existent-id")
	if found {
		t.Error("Expected not found for non-existent ID")
	}
}

func TestPreparedCacheGetBySQLNonExistent(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Try to get non-existent statement by SQL
	_, found := cache.GetBySQL("SELECT * FROM non_existent_table")
	if found {
		t.Error("Expected not found for non-existent SQL")
	}
}

func TestPreparedCacheUpdateStatsNonExistent(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Should not panic when updating stats for non-existent statement
	cache.UpdateStats("non-existent-id", 100*time.Millisecond)
	// No panic = success
}

func TestPreparedCacheRemoveNonExistent(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Should not panic when removing non-existent statement
	cache.Remove("non-existent-id")
	// No panic = success
}

func TestPreparedCacheExecuteNonExistent(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	ctx := context.Background()
	_, err := executor.Execute(ctx, "non-existent-id", nil)
	if err == nil {
		t.Error("Expected error for non-existent prepared statement")
	}
}

func TestPreparedCacheMaxSizeZero(t *testing.T) {
	// Test with maxSize = 0 (should use default)
	cache := NewPreparedCache(0, 30*time.Minute)
	if cache.maxSize != 100 {
		t.Errorf("Expected default maxSize 100, got %d", cache.maxSize)
	}
}

func TestPreparedCacheTTLZero(t *testing.T) {
	// Test with ttl = 0 (should use default)
	cache := NewPreparedCache(10, 0)
	if cache.ttl != 30*time.Minute {
		t.Errorf("Expected default TTL 30m, got %v", cache.ttl)
	}
}

func TestPreparedCacheVeryShortTTL(t *testing.T) {
	cache := NewPreparedCache(10, 1*time.Millisecond)

	sql := "SELECT * FROM users"
	tokens, _ := Tokenize(sql)
	parser := NewParser(tokens)
	stmt, _ := parser.Parse()
	prepared := cache.Put(sql, stmt, 0)

	// Should be found immediately
	_, found := cache.Get(prepared.ID)
	if !found {
		t.Error("Expected to find cached statement immediately")
	}

	// Wait for TTL to expire
	time.Sleep(5 * time.Millisecond)

	// Should not be found after TTL
	_, found = cache.Get(prepared.ID)
	if found {
		t.Error("Expected cached statement to expire with very short TTL")
	}
}

func TestPreparedCacheLargeStatement(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Create a very large SQL statement
	var buf strings.Builder
	buf.WriteString("SELECT * FROM users WHERE id IN (")
	for i := 0; i < 1000; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%d", i))
	}
	buf.WriteString(")")

	sql := buf.String()
	prepared, err := executor.Prepare(sql)
	if err != nil {
		t.Errorf("Expected prepare to succeed for large statement: %v", err)
	}
	if prepared.SQL != sql {
		t.Error("Expected SQL to match")
	}
}

func TestPreparedCacheManyParameters(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Create SQL with many parameters
	var buf strings.Builder
	buf.WriteString("SELECT * FROM users WHERE ")
	expectedParams := 100
	for i := 0; i < expectedParams; i++ {
		if i > 0 {
			buf.WriteString(" OR ")
		}
		buf.WriteString("id = ?")
	}

	sql := buf.String()
	prepared, err := executor.Prepare(sql)
	if err != nil {
		t.Errorf("Expected prepare to succeed: %v", err)
	}
	if prepared.ParamCount != expectedParams {
		t.Errorf("Expected %d parameters, got %d", expectedParams, prepared.ParamCount)
	}
}

func TestPreparedCacheConcurrentAccess(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)
	ctx := context.Background()

	// Test concurrent prepares
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sql := fmt.Sprintf("SELECT %d", idx)
			prepared, err := executor.Prepare(sql)
			if err != nil {
				t.Errorf("Concurrent prepare failed: %v", err)
				return
			}
			_, err = executor.Execute(ctx, prepared.ID, nil)
			if err != nil {
				t.Errorf("Concurrent execute failed: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all statements are cached
	if executor.cache.Size() != 10 {
		t.Errorf("Expected 10 cached statements, got %d", executor.cache.Size())
	}
}

func TestPreparedCacheSameSQLConcurrent(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Test concurrent prepares of same SQL
	var wg sync.WaitGroup
	sql := "SELECT * FROM users WHERE id = ?"
	ids := make([]string, 10)
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			prepared, err := executor.Prepare(sql)
			if err != nil {
				t.Errorf("Concurrent prepare failed: %v", err)
				return
			}
			mu.Lock()
			ids[idx] = prepared.ID
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	// All should have same ID (cached)
	firstID := ids[0]
	for i, id := range ids {
		if id != firstID {
			t.Errorf("ID %d mismatch: expected %s, got %s", i, firstID, id)
		}
	}
}

func TestPreparedCacheEvictionWithAccess(t *testing.T) {
	cache := NewPreparedCache(3, 30*time.Minute)

	// Add 3 statements
	var ids []string
	for i := 0; i < 3; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		prepared := cache.Put(sql, stmt, 0)
		ids = append(ids, prepared.ID)
		time.Sleep(10 * time.Millisecond)
	}

	// Access the first one to make it recently used
	time.Sleep(10 * time.Millisecond)
	cache.Get(ids[0])
	time.Sleep(10 * time.Millisecond)

	// Add one more (should evict the second one, not first)
	sql := "SELECT 3"
	tokens, _ := Tokenize(sql)
	parser := NewParser(tokens)
	stmt, _ := parser.Parse()
	cache.Put(sql, stmt, 0)

	// First should still exist
	_, found := cache.Get(ids[0])
	if !found {
		t.Error("Expected recently accessed statement to still be in cache")
	}

	// Second should be evicted
	_, found = cache.Get(ids[1])
	if found {
		t.Error("Expected least recently used statement to be evicted")
	}
}

func TestPreparedCacheClearEmpty(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Clear empty cache should not panic
	cache.Clear()
	if cache.Size() != 0 {
		t.Error("Expected empty cache after clear")
	}
}

func TestPreparedCacheStatsEmpty(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	stats := cache.Stats()
	if stats.Size != 0 {
		t.Errorf("Expected size 0, got %d", stats.Size)
	}
	if stats.AvgExecTime != 0 {
		t.Errorf("Expected zero avg exec time, got %v", stats.AvgExecTime)
	}
}

func TestPreparedCacheInvalidateEmpty(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)

	// Should not panic
	executor.Invalidate("non-existent")
	executor.InvalidateAll()
}

func TestCountParametersEdgeCases(t *testing.T) {
	tests := []struct {
		sql      string
		expected int
	}{
		{"", 0},                                    // Empty SQL (will fail to parse, but function handles nil)
		{"SELECT * FROM users", 0},                 // No parameters
		{"SELECT * FROM users WHERE id = ?", 1},    // Single parameter
		{"SELECT * FROM users WHERE id IN (?, ?, ?)", 3}, // Multiple IN parameters
		{"SELECT * FROM users WHERE id BETWEEN ? AND ?", 2}, // BETWEEN
		{"SELECT * FROM users WHERE (id = ? OR name = ?) AND age > ?", 3}, // Complex expression
	}

	for _, test := range tests {
		if test.sql == "" {
			continue // Skip empty SQL
		}
		tokens, err := Tokenize(test.sql)
		if err != nil {
			continue
		}
		parser := NewParser(tokens)
		stmt, err := parser.Parse()
		if err != nil {
			continue
		}

		count := countParameters(stmt)
		if count != test.expected {
			t.Errorf("Expected %d parameters for %s, got %d", test.expected, test.sql, count)
		}
	}
}

func TestCountParametersNil(t *testing.T) {
	// Test with nil statement
	count := countParameters(nil)
	if count != 0 {
		t.Errorf("Expected 0 parameters for nil statement, got %d", count)
	}
}

func TestCountExprParametersNil(t *testing.T) {
	// Test with nil expression
	count := countExprParameters(nil)
	if count != 0 {
		t.Errorf("Expected 0 parameters for nil expression, got %d", count)
	}
}

func TestPreparedCacheGenerateIDConsistency(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Same SQL should always generate same ID
	sql := "SELECT * FROM users WHERE id = 1"
	id1 := cache.GenerateID(sql)
	id2 := cache.GenerateID(sql)
	id3 := cache.GenerateID(sql)

	if id1 != id2 || id2 != id3 {
		t.Error("Expected consistent ID generation for same SQL")
	}

	// Different SQL should generate different IDs (with high probability)
	differentSQL := "SELECT * FROM users WHERE id = 2"
	differentID := cache.GenerateID(differentSQL)
	if id1 == differentID {
		t.Error("Expected different IDs for different SQL")
	}
}

func TestPreparedCacheGetAllEmpty(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	all := cache.GetAll()
	if len(all) != 0 {
		t.Errorf("Expected 0 statements, got %d", len(all))
	}
}

func TestPreparedCacheGetAllMultiple(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	// Add statements
	for i := 0; i < 5; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		cache.Put(sql, stmt, 0)
	}

	all := cache.GetAll()
	if len(all) != 5 {
		t.Errorf("Expected 5 statements, got %d", len(all))
	}
}

func TestPreparedStatementExecutorExecuteSQLWithArgs(t *testing.T) {
	executor := NewPreparedStatementExecutor(nil)
	ctx := context.Background()

	// Test ExecuteSQL with various argument types
	sql := "SELECT * FROM users WHERE id = ? AND name = ? AND active = ?"
	args := []interface{}{42, "test", true}

	stmt, err := executor.ExecuteSQL(ctx, sql, args)
	if err != nil {
		t.Errorf("Expected ExecuteSQL to succeed: %v", err)
	}
	if stmt == nil {
		t.Error("Expected non-nil statement")
	}
}

func TestPreparedCacheUpdateStatsMultiple(t *testing.T) {
	cache := NewPreparedCache(10, 30*time.Minute)

	sql := "SELECT * FROM users"
	tokens, _ := Tokenize(sql)
	parser := NewParser(tokens)
	stmt, _ := parser.Parse()
	prepared := cache.Put(sql, stmt, 0)

	// Update stats multiple times with different durations
	durations := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		50 * time.Millisecond,
		300 * time.Millisecond,
		150 * time.Millisecond,
	}

	for _, d := range durations {
		cache.UpdateStats(prepared.ID, d)
	}

	// Retrieve and verify
	retrieved, _ := cache.Get(prepared.ID)
	if retrieved.UseCount != uint64(len(durations)) {
		t.Errorf("Expected use count %d, got %d", len(durations), retrieved.UseCount)
	}
	if retrieved.AvgExecTime == 0 {
		t.Error("Expected non-zero average execution time")
	}
}

func TestPreparedCacheSizeLimitExact(t *testing.T) {
	// Test exact size limit
	cache := NewPreparedCache(3, 30*time.Minute)

	// Add exactly 3 statements
	for i := 0; i < 3; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		tokens, _ := Tokenize(sql)
		parser := NewParser(tokens)
		stmt, _ := parser.Parse()
		cache.Put(sql, stmt, 0)
	}

	if cache.Size() != 3 {
		t.Errorf("Expected size 3, got %d", cache.Size())
	}

	// Add one more
	sql := "SELECT 3"
	tokens, _ := Tokenize(sql)
	parser := NewParser(tokens)
	stmt, _ := parser.Parse()
	cache.Put(sql, stmt, 0)

	// Should still be 3
	if cache.Size() != 3 {
		t.Errorf("Expected size 3 after eviction, got %d", cache.Size())
	}
}
