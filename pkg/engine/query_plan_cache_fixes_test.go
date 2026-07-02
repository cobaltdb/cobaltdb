package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestQueryPlanCachePutExistingKeyRespectsMaxSize verifies that Put on an
// existing key keeps size accounting exact and enforces the size budget
// (previously the replacement path skipped the eviction loop, so currentSize
// could exceed maxSize).
func TestQueryPlanCachePutExistingKeyRespectsMaxSize(t *testing.T) {
	// Each entry costs len(sql)+512 bytes; the budget fits ~2 long entries.
	cache := NewQueryPlanCache(3000, 100)

	longSQL := "SELECT " + strings.Repeat("1,", 400) + "1"
	longStmt, err := query.Parse(longSQL)
	if err != nil {
		t.Fatalf("parse long: %v", err)
	}

	if err := cache.Put(longSQL, nil, longStmt); err != nil {
		t.Fatalf("put long: %v", err)
	}
	// Add another entry so the eviction loop has a tail candidate.
	otherSQL := "SELECT " + strings.Repeat("2,", 300) + "2"
	otherStmt, err := query.Parse(otherSQL)
	if err != nil {
		t.Fatalf("parse other: %v", err)
	}
	if err := cache.Put(otherSQL, nil, otherStmt); err != nil {
		t.Fatalf("put other: %v", err)
	}

	// Re-put the same key several times: accounting must not drift and the
	// budget must hold throughout.
	for i := 0; i < 5; i++ {
		if err := cache.Put(longSQL, nil, longStmt); err != nil {
			t.Fatalf("re-put long %d: %v", i, err)
		}
		stats := cache.GetStats()
		if stats.CurrentBytes > stats.MaxBytes {
			t.Fatalf("iteration %d: currentSize %d exceeds maxSize %d", i, stats.CurrentBytes, stats.MaxBytes)
		}
	}

	stats := cache.GetStats()
	longSize := int64(len(longSQL)) + 512
	if stats.CurrentBytes < longSize {
		t.Fatalf("long entry missing: current %d < %d", stats.CurrentBytes, longSize)
	}
	if stats.CurrentBytes >= 2*longSize {
		t.Fatalf("size accounting drifted on replacement: current %d, entry size %d", stats.CurrentBytes, longSize)
	}
}

// TestQueryPlanCacheMissCounting verifies misses are counted correctly (they
// are now atomic and no longer take the exclusive lock on the miss path).
func TestQueryPlanCacheMissCounting(t *testing.T) {
	cache := NewQueryPlanCache(1<<20, 10)
	for i := 0; i < 7; i++ {
		if _, ok := cache.Get(fmt.Sprintf("SELECT %d", i), nil); ok {
			t.Fatal("unexpected hit")
		}
	}
	stats := cache.GetStats()
	if stats.Misses != 7 {
		t.Fatalf("expected 7 misses, got %d", stats.Misses)
	}
	if stats.Hits != 0 {
		t.Fatalf("expected 0 hits, got %d", stats.Hits)
	}
}
