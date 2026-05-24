package catalog

import (
	"testing"
	"time"
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
