package cache

import (
	"container/list"
	"math"
	"testing"
	"time"
)

func newCoverageCache(config *Config) *Cache {
	return New(config)
}

func TestCoverageNewNilAndNilCloneHelpers(t *testing.T) {
	c := New(nil)
	c.Close()

	if cloneEntry(nil) != nil || cloneRows(nil) != nil || cloneStringMap(nil) != nil {
		t.Fatal("nil clone helpers must preserve nil")
	}
	if got := cloneValue([]byte(nil)); got.([]byte) != nil {
		t.Fatalf("nil byte slice clone = %#v, want nil", got)
	}
}

func TestCoverageSetCapacityDefensiveBreaks(t *testing.T) {
	t.Run("size accounting without evictable entry", func(t *testing.T) {
		c := newCoverageCache(&Config{MaxSize: 10_000, MaxEntries: 0, TTL: time.Hour, CleanupInterval: time.Hour, Enabled: true})
		defer c.Close()
		c.currentSize = c.config.MaxSize
		c.Set("q", nil, nil, nil, nil)
		if _, ok := c.Get("q", nil); !ok {
			t.Fatal("Set should still install the entry after defensive eviction break")
		}
	})

	t.Run("entry accounting without evictable entry", func(t *testing.T) {
		c := newCoverageCache(&Config{MaxSize: 0, MaxEntries: 1, TTL: time.Hour, CleanupInterval: time.Hour, Enabled: true})
		defer c.Close()
		c.entries["orphan"] = &Entry{Key: "orphan"}
		c.Set("q", nil, nil, nil, nil)
		if _, ok := c.Get("q", nil); !ok {
			t.Fatal("Set should still install the entry after defensive eviction break")
		}
	})
}

func TestCoverageEvictLRURejectsInvalidState(t *testing.T) {
	c := newCoverageCache(&Config{Enabled: false, CleanupInterval: time.Hour})
	if c.evictLRU() {
		t.Fatal("empty LRU must not evict")
	}
	c.lruList.PushBack("not an entry")
	if c.evictLRU() {
		t.Fatal("invalid LRU payload must not evict")
	}
}

func TestCoverageCleanupExpiredAndLiveEntries(t *testing.T) {
	c := newCoverageCache(&Config{MaxSize: 0, MaxEntries: 0, TTL: time.Second, CleanupInterval: time.Hour, Enabled: true})
	defer c.Close()
	c.Set("expired", nil, nil, nil, nil)
	c.Set("live", nil, nil, nil, nil)
	c.entries[generateKey("expired", nil)].CreatedAt = time.Now().Add(-2 * time.Second)
	c.cleanupExpired()
	if _, ok := c.Get("expired", nil); ok {
		t.Fatal("cleanupExpired retained an expired entry")
	}
	if _, ok := c.Get("live", nil); !ok {
		t.Fatal("cleanupExpired removed a live entry")
	}
}

func TestCoverageKeyEncodingAndSizeEstimation(t *testing.T) {
	sql := "SELECT ?"
	args := []interface{}{float64(1.25), false, uint32(7), math.Inf(1)}
	keys := make(map[string]bool)
	for _, arg := range args {
		key := generateKey(sql, []interface{}{arg})
		if keys[key] {
			t.Fatalf("distinct encoded argument %#v collided", arg)
		}
		keys[key] = true
	}

	if got := estimateSize(nil, [][]interface{}{{nil}}); got != 264 {
		t.Fatalf("estimateSize(nil row) = %d, want 264", got)
	}
	value := []interface{}{
		"abc",
		[]byte("de"),
		[]interface{}{"f", nil},
		[]string{"gh", "i"},
		map[string]interface{}{"j": []byte("kl")},
		map[string]string{"m": "no"},
		nil,
		true,
	}
	var got int64
	for _, item := range value {
		got += estimateValueSize(item)
	}
	// 3 + 2 + (1+8) + 3 + (1+2) + (1+2) + 8 + 16
	if got != 47 {
		t.Fatalf("estimateValueSize aggregate = %d, want 47", got)
	}
}

func TestCoverageCloneRowsNonNil(t *testing.T) {
	rows := [][]interface{}{{[]byte("x")}}
	cloned := cloneRows(rows)
	cloned[0][0].([]byte)[0] = 'y'
	if string(rows[0][0].([]byte)) != "x" {
		t.Fatal("cloneRows aliased nested bytes")
	}

	values := map[string]string(nil)
	if got := cloneValue(values).(map[string]string); got != nil {
		t.Fatalf("cloneValue(nil string map) = %#v", got)
	}
}

func TestCoverageListImportIsUsed(t *testing.T) {
	// Verify the same payload type guard used by evictLRU with a standalone list.
	var l list.List
	l.PushBack(&Entry{Key: "ok"})
	if _, ok := l.Back().Value.(*Entry); !ok {
		t.Fatal("entry payload should retain its dynamic type")
	}
}
