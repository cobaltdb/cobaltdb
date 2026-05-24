package cache

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if config.MaxSize != 64*1024*1024 {
		t.Errorf("Expected max size 64MB, got %d", config.MaxSize)
	}
	if config.MaxEntries != 10000 {
		t.Errorf("Expected max entries 10000, got %d", config.MaxEntries)
	}
	if !config.Enabled {
		t.Error("Cache should be enabled by default")
	}
}

func TestCacheCreation(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	if cache == nil {
		t.Fatal("Failed to create cache")
	}

	if cache.config != config {
		t.Error("Config mismatch")
	}
}

func TestCacheCreationNormalizesCleanupInterval(t *testing.T) {
	config := &Config{
		MaxSize:    1024 * 1024,
		MaxEntries: 100,
		TTL:        time.Minute,
		Enabled:    true,
	}

	cache := New(config)
	defer cache.Close()

	if cache.config.CleanupInterval != DefaultConfig().CleanupInterval {
		t.Fatalf("CleanupInterval should be defaulted, got %v", cache.config.CleanupInterval)
	}
	if config.CleanupInterval != 0 {
		t.Fatal("New should not mutate caller config")
	}
}

func TestCacheZeroMaxSizeMeansUnlimited(t *testing.T) {
	cache := New(&Config{
		MaxSize:         0,
		MaxEntries:      10,
		TTL:             time.Minute,
		CleanupInterval: time.Minute,
		Enabled:         true,
	})
	defer cache.Close()

	cache.Set("SELECT * FROM users", nil, []string{"id"}, [][]interface{}{{1}}, []string{"users"})
	if _, found := cache.Get("SELECT * FROM users", nil); !found {
		t.Fatal("MaxSize=0 should not reject cache entries")
	}
}

func TestCacheZeroTTLMeansNoExpiration(t *testing.T) {
	cache := New(&Config{
		MaxSize:         1024 * 1024,
		MaxEntries:      10,
		TTL:             0,
		CleanupInterval: time.Millisecond,
		Enabled:         true,
	})
	defer cache.Close()

	cache.Set("SELECT * FROM users", nil, []string{"id"}, [][]interface{}{{1}}, []string{"users"})
	time.Sleep(5 * time.Millisecond)
	if _, found := cache.Get("SELECT * FROM users", nil); !found {
		t.Fatal("TTL=0 should not expire cache entries")
	}
}

func TestCacheGetSet(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	sql := "SELECT * FROM users WHERE id = ?"
	args := []interface{}{1}
	columns := []string{"id", "name", "email"}
	rows := [][]interface{}{{1, "John", "john@example.com"}}

	// Set cache entry
	cache.Set(sql, args, columns, rows, []string{"users"})

	// Get cache entry
	entry, found := cache.Get(sql, args)
	if !found {
		t.Fatal("Should find cached entry")
	}

	if entry.SQL != sql {
		t.Errorf("SQL mismatch: got %s, want %s", entry.SQL, sql)
	}

	if len(entry.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(entry.Rows))
	}
}

func TestCacheCopiesValuesOnSetAndGet(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	sql := "SELECT payload FROM events WHERE id = ?"
	args := []interface{}{[]byte("arg")}
	columns := []string{"payload"}
	rows := [][]interface{}{{[]byte("cached")}}
	tableDeps := []string{"events"}

	cache.Set(sql, args, columns, rows, tableDeps)
	args[0].([]byte)[0] = 'X'
	columns[0] = "mutated"
	rows[0][0].([]byte)[0] = 'X'
	tableDeps[0] = "mutated"

	entry, found := cache.Get(sql, []interface{}{[]byte("arg")})
	if !found {
		t.Fatal("Should find cached entry")
	}
	if entry.Columns[0] != "payload" {
		t.Fatalf("cached columns should not reflect caller mutation, got %q", entry.Columns[0])
	}
	if got := string(entry.Rows[0][0].([]byte)); got != "cached" {
		t.Fatalf("cached row should not reflect caller mutation, got %q", got)
	}

	entry.Columns[0] = "changed"
	entry.Rows[0][0].([]byte)[0] = 'Y'
	entry.TableDeps[0] = "changed"

	entry, found = cache.Get(sql, []interface{}{[]byte("arg")})
	if !found {
		t.Fatal("Should still find cached entry")
	}
	if entry.Columns[0] != "payload" {
		t.Fatalf("cache entry should not reflect returned column mutation, got %q", entry.Columns[0])
	}
	if got := string(entry.Rows[0][0].([]byte)); got != "cached" {
		t.Fatalf("cache entry should not reflect returned row mutation, got %q", got)
	}
	if entry.TableDeps[0] != "events" {
		t.Fatalf("cache entry should not reflect returned deps mutation, got %q", entry.TableDeps[0])
	}
}

func TestCacheMiss(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Try to get non-existent entry
	_, found := cache.Get("SELECT * FROM nonexistent", nil)
	if found {
		t.Error("Should not find non-existent entry")
	}

	stats := cache.Stats()
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
}

func TestCacheDisabled(t *testing.T) {
	config := DefaultConfig()
	config.Enabled = false
	cache := New(config)

	sql := "SELECT * FROM users"
	cache.Set(sql, nil, []string{"id"}, [][]interface{}{{1}}, []string{"users"})

	_, found := cache.Get(sql, nil)
	if found {
		t.Error("Should not find entry when cache is disabled")
	}
}

func TestCacheExpiration(t *testing.T) {
	config := DefaultConfig()
	config.TTL = 100 * time.Millisecond
	cache := New(config)
	defer cache.Close()

	sql := "SELECT * FROM users"
	cache.Set(sql, nil, []string{"id"}, [][]interface{}{{1}}, []string{"users"})

	// Should find immediately
	_, found := cache.Get(sql, nil)
	if !found {
		t.Error("Should find entry before expiration")
	}

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Should not find after expiration
	_, found = cache.Get(sql, nil)
	if found {
		t.Error("Should not find expired entry")
	}
}

func TestCacheInvalidateTable(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Add entries for different tables
	cache.Set("SELECT * FROM users", nil, []string{"id"}, [][]interface{}{{1}}, []string{"users"})
	cache.Set("SELECT * FROM orders", nil, []string{"id"}, [][]interface{}{{1}}, []string{"orders"})

	// Verify both exist
	_, found := cache.Get("SELECT * FROM users", nil)
	if !found {
		t.Error("Should find users entry")
	}

	// Invalidate users table
	cache.InvalidateTable("users")

	// Users entry should be gone
	_, found = cache.Get("SELECT * FROM users", nil)
	if found {
		t.Error("Users entry should be invalidated")
	}

	// Orders entry should still exist
	_, found = cache.Get("SELECT * FROM orders", nil)
	if !found {
		t.Error("Orders entry should still exist")
	}
}

func TestCacheInvalidateAll(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	// Add multiple entries
	cache.Set("SELECT 1", nil, []string{"c"}, [][]interface{}{{1}}, nil)
	cache.Set("SELECT 2", nil, []string{"c"}, [][]interface{}{{2}}, nil)
	cache.Set("SELECT 3", nil, []string{"c"}, [][]interface{}{{3}}, nil)

	stats := cache.Stats()
	if stats.EntryCount != 3 {
		t.Errorf("Expected 3 entries, got %d", stats.EntryCount)
	}

	// Invalidate all
	cache.InvalidateAll()

	stats = cache.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("Expected 0 entries after invalidate all, got %d", stats.EntryCount)
	}
}

func TestCacheLRUEviction(t *testing.T) {
	config := DefaultConfig()
	config.MaxEntries = 2
	cache := New(config)
	defer cache.Close()

	// Add entries up to limit
	cache.Set("SELECT 1", nil, []string{"c"}, [][]interface{}{{1}}, nil)
	cache.Set("SELECT 2", nil, []string{"c"}, [][]interface{}{{2}}, nil)

	// Access first entry to make it recently used
	cache.Get("SELECT 1", nil)

	// Add third entry (should evict second)
	cache.Set("SELECT 3", nil, []string{"c"}, [][]interface{}{{3}}, nil)

	// First should still exist
	_, found := cache.Get("SELECT 1", nil)
	if !found {
		t.Error("First entry should exist (recently used)")
	}

	// Second should be evicted
	_, found = cache.Get("SELECT 2", nil)
	if found {
		t.Error("Second entry should be evicted (least recently used)")
	}

	// Third should exist
	_, found = cache.Get("SELECT 3", nil)
	if !found {
		t.Error("Third entry should exist")
	}
}

func TestCacheSetExistingDoesNotEvictUnrelatedEntry(t *testing.T) {
	config := DefaultConfig()
	config.MaxEntries = 2
	cache := New(config)
	defer cache.Close()

	cache.Set("SELECT 1", nil, []string{"c"}, [][]interface{}{{1}}, []string{"one"})
	cache.Set("SELECT 2", nil, []string{"c"}, [][]interface{}{{2}}, []string{"two"})
	cache.Set("SELECT 1", nil, []string{"c"}, [][]interface{}{{10}}, []string{"one"})

	entry, found := cache.Get("SELECT 1", nil)
	if !found {
		t.Fatal("updated entry should still exist")
	}
	if got := entry.Rows[0][0]; got != 10 {
		t.Fatalf("expected updated row value 10, got %v", got)
	}

	if _, found := cache.Get("SELECT 2", nil); !found {
		t.Fatal("updating an existing key should not evict unrelated entries")
	}

	stats := cache.Stats()
	if stats.EntryCount != 2 {
		t.Fatalf("expected 2 entries after replacement, got %d", stats.EntryCount)
	}
}

func TestCacheStats(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	sql := "SELECT * FROM users"
	args := []interface{}{1}

	// Miss
	cache.Get(sql, args)

	// Set
	cache.Set(sql, args, []string{"id"}, [][]interface{}{{1}}, nil)

	// Hit
	cache.Get(sql, args)
	cache.Get(sql, args)

	stats := cache.Stats()

	if stats.Hits != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.Hits)
	}

	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}

	if stats.EntryCount != 1 {
		t.Errorf("Expected 1 entry, got %d", stats.EntryCount)
	}

	if stats.HitRate != 66.66666666666666 {
		t.Errorf("Expected hit rate 66.67, got %f", stats.HitRate)
	}
}

func TestGenerateKey(t *testing.T) {
	key1 := generateKey("SELECT * FROM users", []interface{}{1})
	key2 := generateKey("SELECT * FROM users", []interface{}{1})
	key3 := generateKey("SELECT * FROM users", []interface{}{2})
	key4 := generateKey("SELECT * FROM orders", []interface{}{1})

	// Same SQL and args should produce same key
	if key1 != key2 {
		t.Error("Same query should produce same key")
	}

	// Different args should produce different key
	if key1 == key3 {
		t.Error("Different args should produce different key")
	}

	// Different SQL should produce different key
	if key1 == key4 {
		t.Error("Different SQL should produce different key")
	}

	// Key should not be empty
	if len(key1) == 0 {
		t.Error("Key should not be empty")
	}
}

func TestEstimateSize(t *testing.T) {
	columns := []string{"id", "name", "email"}
	rows := [][]interface{}{
		{1, "John Doe", "john@example.com"},
		{2, "Jane Smith", "jane@example.com"},
	}

	size := estimateSize(columns, rows)

	// Should be positive
	if size <= 0 {
		t.Error("Size should be positive")
	}

	// Should account for column names
	minSize := int64(len("id") + len("name") + len("email"))
	if size < minSize {
		t.Errorf("Size should be at least %d, got %d", minSize, size)
	}
}

func TestCacheDelete(t *testing.T) {
	config := DefaultConfig()
	cache := New(config)
	defer cache.Close()

	sql := "SELECT * FROM users"
	cache.Set(sql, nil, []string{"id"}, [][]interface{}{{1}}, nil)

	key := generateKey(sql, nil)
	cache.Delete(key)

	_, found := cache.Get(sql, nil)
	if found {
		t.Error("Deleted entry should not be found")
	}
}
