package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestCacheGetConcurrentHits exercises many concurrent Get hits on the same
// entries (plus concurrent Sets and invalidations) to validate the RLock-based
// lookup and out-of-lock deep clone. Run with -race.
func TestCacheGetConcurrentHits(t *testing.T) {
	c := New(&Config{
		MaxSize:         8 << 20,
		MaxEntries:      100,
		TTL:             time.Minute,
		CleanupInterval: 10 * time.Millisecond,
		Enabled:         true,
	})
	defer c.Close()

	rows := [][]interface{}{
		{int64(1), "alpha", []byte("blob-1")},
		{int64(2), "beta", map[string]interface{}{"k": "v"}},
	}
	for i := 0; i < 8; i++ {
		c.Set(fmt.Sprintf("SELECT %d", i), nil, []string{"id", "name", "extra"}, rows, []string{"t"})
	}

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				sql := fmt.Sprintf("SELECT %d", i%8)
				entry, ok := c.Get(sql, nil)
				if !ok {
					continue // may have been invalidated concurrently
				}
				// Returned entries must be private copies: mutating them must
				// never affect other readers.
				entry.Rows[0][1] = fmt.Sprintf("mutated-%d-%d", g, i)
				if len(entry.Rows) != 2 || entry.Columns[0] != "id" {
					t.Errorf("corrupted entry from Get: %+v", entry)
					return
				}
			}
		}(g)
	}
	// Concurrent writers and invalidators.
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			c.Set(fmt.Sprintf("SELECT %d", i%8), nil, []string{"id", "name", "extra"}, rows, []string{"t"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			c.InvalidateTable("t")
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()

	// Verify a surviving entry still round-trips uncorrupted.
	c.Set("final", nil, []string{"id", "name", "extra"}, rows, []string{"t"})
	entry, ok := c.Get("final", nil)
	if !ok {
		t.Fatal("expected final entry")
	}
	if got := entry.Rows[0][1]; got != "alpha" {
		t.Fatalf("cached rows were mutated through a returned copy: %v", got)
	}
}

// TestCacheGetTTLExpiryDeletes verifies the expiry path still deletes stale
// entries via the short write-lock path.
func TestCacheGetTTLExpiryDeletes(t *testing.T) {
	c := New(&Config{
		MaxSize:         1 << 20,
		MaxEntries:      10,
		TTL:             10 * time.Millisecond,
		CleanupInterval: time.Hour, // rely on Get-side expiry only
		Enabled:         true,
	})
	defer c.Close()

	c.Set("q", nil, []string{"a"}, [][]interface{}{{int64(1)}}, nil)
	if _, ok := c.Get("q", nil); !ok {
		t.Fatal("expected hit before TTL")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := c.Get("q", nil); ok {
		t.Fatal("expected TTL-expired miss")
	}
	if got := c.Stats().EntryCount; got != 0 {
		t.Fatalf("expired entry not deleted: %d entries", got)
	}
}
