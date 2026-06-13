package storage

import (
	"errors"
	"io"
	"strings"
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

	// Page ID can be 0 (meta page is page 0)
	// Just verify we got a valid page
}

func TestBufferPoolOperationsAfterClose(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)

	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to create page: %v", err)
	}
	page.SetDirty(true)

	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}

	if _, err := pool.NewPage(PageTypeLeaf); !errors.Is(err, ErrBufferPoolClosed) {
		t.Fatalf("expected ErrBufferPoolClosed from NewPage after close, got %v", err)
	}
	if _, err := pool.GetPage(page.ID()); !errors.Is(err, ErrBufferPoolClosed) {
		t.Fatalf("expected ErrBufferPoolClosed from GetPage after close, got %v", err)
	}
	if err := pool.FlushAll(); !errors.Is(err, ErrBufferPoolClosed) {
		t.Fatalf("expected ErrBufferPoolClosed from FlushAll after close, got %v", err)
	}
	if err := pool.FlushDirty(); !errors.Is(err, ErrBufferPoolClosed) {
		t.Fatalf("expected ErrBufferPoolClosed from FlushDirty after close, got %v", err)
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

func TestBufferPoolGetPageRejectsMismatchedPageHeaderID(t *testing.T) {
	backend := NewMemory()
	page := NewPage(2, PageTypeLeaf)
	if _, err := backend.WriteAt(page.Data, int64(PageSize)); err != nil {
		t.Fatalf("write corrupt page: %v", err)
	}

	pool, err := NewBufferPoolWithError(10, backend)
	if err != nil {
		t.Fatalf("NewBufferPoolWithError: %v", err)
	}
	defer pool.Close()

	_, err = pool.GetPage(1)
	if !errors.Is(err, ErrPageCorrupted) {
		t.Fatalf("GetPage error = %v, want ErrPageCorrupted", err)
	}
	if !strings.Contains(err.Error(), "does not match requested page") {
		t.Fatalf("expected page ID mismatch error, got %v", err)
	}
	if pool.PageCount() != 0 {
		t.Fatalf("corrupt page should not be cached, cached pages = %d", pool.PageCount())
	}
}

func TestBufferPoolGetPageRejectsInvalidPageHeaderType(t *testing.T) {
	backend := NewMemory()
	page := NewPage(1, PageTypeLeaf)
	page.Data[4] = 0xff
	if _, err := backend.WriteAt(page.Data, int64(PageSize)); err != nil {
		t.Fatalf("write corrupt page: %v", err)
	}

	pool, err := NewBufferPoolWithError(10, backend)
	if err != nil {
		t.Fatalf("NewBufferPoolWithError: %v", err)
	}
	defer pool.Close()

	_, err = pool.GetPage(1)
	if !errors.Is(err, ErrPageCorrupted) {
		t.Fatalf("GetPage error = %v, want ErrPageCorrupted", err)
	}
	if !strings.Contains(err.Error(), "invalid page type") {
		t.Fatalf("expected invalid page type error, got %v", err)
	}
	if pool.PageCount() != 0 {
		t.Fatalf("corrupt page should not be cached, cached pages = %d", pool.PageCount())
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

func TestBufferPoolFlushPageRejectsShortWrite(t *testing.T) {
	backend := &shortWriteBackend{
		Backend: NewMemory(),
		limit:   PageSize - 1,
	}
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage: %v", err)
	}

	if err := pool.FlushPage(page); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("FlushPage short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if !page.IsDirty() {
		t.Fatal("page should remain dirty after failed short write")
	}
}

func TestBufferPoolFlushDirtyPagesDoesNotPanicWhenStopped(t *testing.T) {
	backend := &shortWriteBackend{
		Backend: NewMemory(),
		limit:   PageSize - 1,
	}
	pool := NewBufferPool(10, backend)
	pool.flushErrLimit = 1

	page, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage: %v", err)
	}
	pool.Unpin(page)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("flushDirtyPages panicked after flusher stopped: %v", r)
		}
	}()
	pool.flushDirtyPages()

	if pool.flushRunning {
		t.Fatal("flushDirtyPages should not restart a stopped flusher")
	}
	if pool.flushDone != nil {
		t.Fatal("flushDirtyPages should leave stopped flusher channel nil")
	}
}

func TestBufferPoolGetPageRejectsShortRead(t *testing.T) {
	mem := NewMemory()
	writeTestPage(t, mem, 1, PageTypeLeaf)
	backend := &shortReadBackend{
		Backend: mem,
		limit:   PageSize - 1,
	}
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	_, err := pool.GetPage(1)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("GetPage short read error = %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if pool.PageCount() != 0 {
		t.Fatalf("short-read page should not be cached, cached pages = %d", pool.PageCount())
	}
}

func TestCachedPageUnpinDoesNotUnderflow(t *testing.T) {
	page := &CachedPage{}

	page.Unpin()
	page.Unpin()
	if page.IsPinned() {
		t.Fatal("page should remain unpinned after redundant Unpin calls")
	}

	page.Pin()
	if !page.IsPinned() {
		t.Fatal("page should be pinned after Pin following redundant Unpin calls")
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
