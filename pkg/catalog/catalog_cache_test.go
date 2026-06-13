package catalog

import (
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/cache"
)

func TestQueryCacheReturnsIsolatedEntries(t *testing.T) {
	cache := NewQueryCache(10, time.Minute)
	columns := []string{"id", "payload"}
	rows := [][]interface{}{
		{int64(1), []byte("alpha"), map[string]interface{}{"tags": []interface{}{"hot"}}},
	}
	tables := []string{"users"}

	cache.Set("key", columns, rows, tables)
	columns[0] = "mutated"
	rows[0][1].([]byte)[0] = 'z'
	rows[0][2].(map[string]interface{})["tags"].([]interface{})[0] = "cold"
	tables[0] = "orders"

	entry, ok := cache.Get("key")
	if !ok {
		t.Fatal("expected cached entry")
	}
	if entry.Columns[0] != "id" {
		t.Fatalf("Set retained caller-owned columns: %v", entry.Columns)
	}
	if string(entry.Rows[0][1].([]byte)) != "alpha" {
		t.Fatalf("Set retained caller-owned row bytes: %q", entry.Rows[0][1])
	}
	if entry.Rows[0][2].(map[string]interface{})["tags"].([]interface{})[0] != "hot" {
		t.Fatalf("Set retained caller-owned nested row value: %v", entry.Rows[0][2])
	}
	if entry.Tables[0] != "users" {
		t.Fatalf("Set retained caller-owned tables: %v", entry.Tables)
	}

	entry.Columns[0] = "external"
	entry.Rows[0][1].([]byte)[0] = 'x'
	entry.Rows[0][2].(map[string]interface{})["tags"].([]interface{})[0] = "warm"
	entry.Tables[0] = "external_table"

	entryAgain, ok := cache.Get("key")
	if !ok {
		t.Fatal("expected cached entry on second get")
	}
	if entryAgain.Columns[0] != "id" {
		t.Fatalf("Get returned mutable columns: %v", entryAgain.Columns)
	}
	if string(entryAgain.Rows[0][1].([]byte)) != "alpha" {
		t.Fatalf("Get returned mutable row bytes: %q", entryAgain.Rows[0][1])
	}
	if entryAgain.Rows[0][2].(map[string]interface{})["tags"].([]interface{})[0] != "hot" {
		t.Fatalf("Get returned mutable nested row value: %v", entryAgain.Rows[0][2])
	}
	if entryAgain.Tables[0] != "users" {
		t.Fatalf("Get returned mutable tables: %v", entryAgain.Tables)
	}
}

func TestQueryCacheZeroTTLDoesNotExpireByAge(t *testing.T) {
	cache := NewQueryCache(10, 0)
	cache.Set("key", []string{"id"}, [][]interface{}{{int64(1)}}, []string{"users"})

	time.Sleep(time.Millisecond)

	if _, ok := cache.Get("key"); !ok {
		t.Fatal("expected zero TTL cache entry to remain valid")
	}
}

func TestCatalogEnableQueryCacheUsesBoundedByteDefault(t *testing.T) {
	c := &Catalog{}
	c.EnableQueryCache(10, time.Minute)
	defer c.DisableQueryCache()

	if c.queryCache == nil {
		t.Fatal("expected query cache to be enabled")
	}
	stats := c.queryCache.Stats()
	if stats.MaxSize != cache.DefaultConfig().MaxSize {
		t.Fatalf("MaxSize = %d, want default %d", stats.MaxSize, cache.DefaultConfig().MaxSize)
	}
}

func TestCatalogEnableQueryCacheWithLimitsUsesByteLimit(t *testing.T) {
	c := &Catalog{}
	c.EnableQueryCacheWithLimits(4096, 7, time.Minute)
	defer c.DisableQueryCache()

	if c.queryCache == nil {
		t.Fatal("expected query cache to be enabled")
	}
	stats := c.queryCache.Stats()
	if stats.MaxSize != 4096 {
		t.Fatalf("MaxSize = %d, want 4096", stats.MaxSize)
	}

	for i := 0; i < 8; i++ {
		c.queryCache.Set(string(rune('a'+i)), nil, []string{"c"}, [][]interface{}{{i}}, nil)
	}
	stats = c.queryCache.Stats()
	if stats.EntryCount > 7 {
		t.Fatalf("EntryCount = %d, want <= 7", stats.EntryCount)
	}
}
