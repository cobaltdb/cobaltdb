package storage

import (
	"testing"
	"time"
)

func TestBufferPoolStats(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(10, backend)

	// Get initial stats
	stats := bp.Stats()
	if stats.Capacity != 10 {
		t.Errorf("Expected capacity 10, got %d", stats.Capacity)
	}
	if stats.PageCount != 0 {
		t.Errorf("Expected 0 pages, got %d", stats.PageCount)
	}
	if stats.HitCount != 0 {
		t.Errorf("Expected 0 hits, got %d", stats.HitCount)
	}

	// Create some pages
	for i := 0; i < 5; i++ {
		page, err := bp.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to create page: %v", err)
		}
		bp.Unpin(page)
	}

	// Check stats after creation
	stats = bp.Stats()
	if stats.PageCount != 5 {
		t.Errorf("Expected 5 pages, got %d", stats.PageCount)
	}
	if stats.FreeCount != 5 {
		t.Errorf("Expected 5 free, got %d", stats.FreeCount)
	}
	if stats.DirtyCount != 5 {
		t.Errorf("Expected 5 dirty, got %d", stats.DirtyCount)
	}

	// Access pages to generate hits and misses
	for i := uint32(1); i <= 3; i++ {
		_, err := bp.GetPage(i)
		if err != nil {
			t.Fatalf("Failed to get page: %v", err)
		}
	}

	// Try to get non-existent pages to generate misses
	for i := uint32(100); i <= 102; i++ {
		_, err := bp.GetPage(i)
		if err != nil {
			// Expected - these pages don't exist
			continue
		}
	}

	stats = bp.Stats()
	if stats.HitCount == 0 {
		t.Error("Expected some cache hits")
	}
	if stats.MissCount == 0 {
		t.Error("Expected some cache misses")
	}
}

func TestBufferPoolHitRatio(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(10, backend)

	// Create a page
	page, err := bp.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}
	pageID := page.ID()
	bp.Unpin(page)

	// First access - hit (page was created and is in cache)
	_, _ = bp.GetPage(pageID)

	// Second access - hit
	p2, _ := bp.GetPage(pageID)
	bp.Unpin(p2)

	// Third access - hit
	p3, _ := bp.GetPage(pageID)
	bp.Unpin(p3)

	stats := bp.Stats()
	// All 3 accesses are hits because NewPage puts the page in cache
	expectedRatio := float64(3) / float64(3) // 3 hits out of 3 accesses
	if stats.HitRatio != expectedRatio {
		t.Errorf("Expected hit ratio %.2f, got %.2f", expectedRatio, stats.HitRatio)
	}
}

func TestBufferPoolEvictionCount(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(5, backend) // Small capacity to force eviction

	// Create more pages than capacity
	for i := 0; i < 10; i++ {
		page, err := bp.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to create page: %v", err)
		}
		bp.Unpin(page)
	}

	evictions := bp.EvictionCount()
	if evictions == 0 {
		t.Error("Expected some evictions")
	}
	if evictions != 5 {
		t.Errorf("Expected 5 evictions, got %d", evictions)
	}
}

func TestBufferPoolStatsCollector(t *testing.T) {
	sc := newBufferPoolStatsCollector()

	// Record some hits and misses
	sc.recordHit()
	sc.recordHit()
	sc.recordMiss()

	hits := sc.hitCount
	misses := sc.missCount

	if hits != 2 {
		t.Errorf("Expected 2 hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}

	// Check hit ratio
	ratio := sc.getHitRatio()
	expectedRatio := float64(2) / float64(3)
	if ratio != expectedRatio {
		t.Errorf("Expected ratio %.2f, got %.2f", expectedRatio, ratio)
	}
}

func TestBufferPoolReadWriteTimes(t *testing.T) {
	sc := newBufferPoolStatsCollector()

	// Record some read/write times
	sc.recordRead(10 * time.Millisecond)
	sc.recordRead(20 * time.Millisecond)
	sc.recordWrite(30 * time.Millisecond)
	sc.recordWrite(50 * time.Millisecond)

	avgRead := sc.getAvgReadTime()
	avgWrite := sc.getAvgWriteTime()

	expectedAvgRead := float64(10+20) / 2
	if avgRead != expectedAvgRead {
		t.Errorf("Expected avg read time %.2f, got %.2f", expectedAvgRead, avgRead)
	}

	expectedAvgWrite := float64(30+50) / 2
	if avgWrite != expectedAvgWrite {
		t.Errorf("Expected avg write time %.2f, got %.2f", expectedAvgWrite, avgWrite)
	}
}

func TestBufferPoolStatsConcurrent(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(100, backend)

	// Create some pages concurrently
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 10; j++ {
				page, err := bp.NewPage(PageTypeLeaf)
				if err == nil {
					bp.Unpin(page)
				}
			}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	stats := bp.Stats()
	if stats.PageCount != 100 {
		t.Errorf("Expected 100 pages, got %d", stats.PageCount)
	}
}

// --- AllocatedPageCount ---

func TestBufferPoolAllocatedPageCount(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(100, backend)

	// Initially page 0 is reserved for meta page, so nextPageID starts at 1
	if count := bp.AllocatedPageCount(); count != 1 {
		t.Errorf("expected AllocatedPageCount=1 (meta page), got %d", count)
	}

	// Create some pages
	for i := 0; i < 5; i++ {
		page, err := bp.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("NewPage failed: %v", err)
		}
		bp.Unpin(page)
	}

	// Page 0 = meta, pages 1-5 = data, so total should be 6
	if count := bp.AllocatedPageCount(); count != 6 {
		t.Errorf("expected AllocatedPageCount=6 (1 meta + 5 data), got %d", count)
	}
}

// --- FlushErrorCount ---

func TestBufferPoolFlushErrorCount(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(100, backend)

	// Initially 0
	if count := bp.FlushErrorCount(); count != 0 {
		t.Errorf("expected FlushErrorCount=0, got %d", count)
	}
}

// --- PauseBackgroundFlusher / ResumeBackgroundFlusher ---

func TestBufferPoolPauseResumeBackgroundFlusher(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(100, backend)

	// Initial state: not paused
	// Pause sets flushSuspended to true
	bp.PauseBackgroundFlusher()
	if !bp.flushSuspended.Load() {
		t.Error("expected flushSuspended=true after Pause")
	}

	// Pause is idempotent
	bp.PauseBackgroundFlusher()
	if !bp.flushSuspended.Load() {
		t.Error("expected flushSuspended=true after second Pause")
	}

	// Resume sets flushSuspended to false
	bp.ResumeBackgroundFlusher()
	if bp.flushSuspended.Load() {
		t.Error("expected flushSuspended=false after Resume")
	}

	// Resume is idempotent
	bp.ResumeBackgroundFlusher()
	if bp.flushSuspended.Load() {
		t.Error("expected flushSuspended=false after second Resume")
	}
}

// --- DiscardAll ---

func TestBufferPoolDiscardAll(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(10, backend)

	// Create some pages
	var pageIDs []uint32
	for i := 0; i < 3; i++ {
		page, err := bp.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("NewPage failed: %v", err)
		}
		pageIDs = append(pageIDs, page.ID())
		bp.Unpin(page)
	}

	// Verify pages exist
	if bp.PageCount() != 3 {
		t.Errorf("expected 3 pages before DiscardAll, got %d", bp.PageCount())
	}

	// DiscardAll should clear the pool
	bp.DiscardAll()

	if bp.PageCount() != 0 {
		t.Errorf("expected 0 pages after DiscardAll, got %d", bp.PageCount())
	}
	if !bp.closed {
		t.Error("expected closed=true after DiscardAll")
	}

	// Idempotent: second DiscardAll should not panic
	bp.DiscardAll()
	if bp.PageCount() != 0 {
		t.Errorf("expected 0 pages after second DiscardAll, got %d", bp.PageCount())
	}
}

func TestBufferPoolDiscardAllIdempotent(t *testing.T) {
	backend := NewMemory()
	bp := NewBufferPool(10, backend)

	// Create a page
	page, err := bp.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage failed: %v", err)
	}
	bp.Unpin(page)

	// DiscardAll twice
	bp.DiscardAll()
	bp.DiscardAll() // should not panic or error

	if !bp.closed {
		t.Error("expected closed=true after DiscardAll")
	}
}
