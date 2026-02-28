package storage

import (
	"testing"
)

func TestBufferPool(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend) // 10 pages cache
	defer pool.Close()

	if pool == nil {
		t.Fatal("Buffer pool is nil")
	}
}

func TestBufferPoolNewPage(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	// Create a new page
	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}

	if page == nil {
		t.Fatal("Page is nil")
	}

	if page.ID() == 0 {
		t.Error("Expected non-zero page ID")
	}
}

func TestBufferPoolGetPage(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	// Create a page first
	page1, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}
	pageID := page1.ID()
	pool.Unpin(page1)

	// Get the same page
	page2, err := pool.GetPage(pageID)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	if page2.ID() != pageID {
		t.Errorf("Expected PageID %d, got %d", pageID, page2.ID())
	}
}

func TestBufferPoolMultiplePages(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	// Create multiple pages
	for i := 0; i < 10; i++ {
		page, err := pool.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to create page %d: %v", i, err)
		}
		pool.Unpin(page)
	}
}

func TestBufferPoolFlushPage(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	// Create and modify
	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}
	page.SetDirty(true)
	copy(page.Data(), []byte("test data"))

	// Flush
	err = pool.FlushPage(page)
	if err != nil {
		t.Fatalf("Failed to flush page: %v", err)
	}
}

func TestBufferPoolFlushAll(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	// Create multiple pages
	for i := 0; i < 5; i++ {
		page, err := pool.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to create page %d: %v", i, err)
		}
		page.SetDirty(true)
		pool.Unpin(page)
	}

	// Flush all
	err := pool.FlushAll()
	if err != nil {
		t.Fatalf("Failed to flush all: %v", err)
	}
}

func TestBufferPoolEviction(t *testing.T) {
	backend := NewMemory()
	// Small cache to force eviction
	pool := NewBufferPool(2, backend) // Only 2 pages
	defer pool.Close()

	// Create more pages than cache can hold
	for i := 0; i < 5; i++ {
		page, err := pool.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to create page %d: %v", i, err)
		}
		pool.Unpin(page)
	}
}

func TestBufferPoolPageCount(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	if pool.PageCount() != 0 {
		t.Errorf("Expected 0 pages, got %d", pool.PageCount())
	}

	// Create pages
	for i := 0; i < 5; i++ {
		page, _ := pool.NewPage(PageTypeLeaf)
		pool.Unpin(page)
	}

	if pool.PageCount() != 5 {
		t.Errorf("Expected 5 pages, got %d", pool.PageCount())
	}
}

func TestBufferPoolPinUnpin(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}

	// Page starts pinned from NewPage
	if !page.IsPinned() {
		t.Error("Expected page to be pinned after NewPage")
	}

	// Unpin
	page.Unpin()
	if page.IsPinned() {
		t.Error("Expected page to be unpinned after Unpin")
	}

	// Pin again
	page.Pin()
	if !page.IsPinned() {
		t.Error("Expected page to be pinned after Pin")
	}
}

func TestBufferPoolSetWAL(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	// Create temp WAL
	tmpDir := t.TempDir()
	wal, err := OpenWAL(tmpDir + "/test.wal")
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	pool.SetWAL(wal)
}
