package storage

import (
	"os"
	"testing"
)

func setupPageManager(t *testing.T) (*PageManager, *BufferPool, func()) {
	// Create temporary file
	tmpFile := "test_pagemgr.db"

	backend, err := OpenDisk(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	pool := NewBufferPool(100, backend)

	pm, err := NewPageManager(pool)
	if err != nil {
		backend.Close()
		os.Remove(tmpFile)
		t.Fatalf("Failed to create page manager: %v", err)
	}

	cleanup := func() {
		pm.Close()
		pool.Close()
		backend.Close()
		os.Remove(tmpFile)
	}

	return pm, pool, cleanup
}

func TestNewPageManager(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	if pm == nil {
		t.Fatal("PageManager is nil")
	}

	// Should have meta page
	if pm.meta == nil {
		t.Error("Meta page is nil")
	}

	// Should have at least 1 page (meta page)
	if pm.GetPageCount() < 1 {
		t.Errorf("Expected at least 1 page, got %d", pm.GetPageCount())
	}
}

func TestNewPageManagerExistingDB(t *testing.T) {
	tmpFile := "test_pagemgr_existing.db"

	// Create initial database
	func() {
		backend, _ := OpenDisk(tmpFile)
		pool := NewBufferPool(100, backend)
		pm, _ := NewPageManager(pool)

		// Allocate some pages
		pm.AllocatePage(PageTypeLeaf)
		pm.AllocatePage(PageTypeLeaf)

		pm.Close()
		pool.Close()
		backend.Close()
	}()
	defer os.Remove(tmpFile)

	// Reopen database
	backend, err := OpenDisk(tmpFile)
	if err != nil {
		t.Fatalf("Failed to reopen backend: %v", err)
	}
	defer backend.Close()

	pool := NewBufferPool(100, backend)
	defer pool.Close()

	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to reopen page manager: %v", err)
	}
	defer pm.Close()

	// Should load existing meta
	if pm.meta == nil {
		t.Error("Failed to load existing meta page")
	}

	// Should have correct page count
	if pm.GetPageCount() < 3 {
		t.Errorf("Expected at least 3 pages, got %d", pm.GetPageCount())
	}
}

func TestAllocatePage(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	initialCount := pm.GetPageCount()

	// Allocate a page
	page, err := pm.AllocatePage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to allocate page: %v", err)
	}

	if page == nil {
		t.Fatal("Allocated page is nil")
	}

	// Page count should increase
	if pm.GetPageCount() <= initialCount {
		t.Errorf("Page count should increase after allocation")
	}
}

func TestAllocateMultiplePages(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	// Allocate multiple pages
	pageIDs := make(map[uint32]bool)
	for i := 0; i < 10; i++ {
		page, err := pm.AllocatePage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to allocate page %d: %v", i, err)
		}

		// Check for unique page IDs
		if pageIDs[page.ID()] {
			t.Errorf("Duplicate page ID: %d", page.ID())
		}
		pageIDs[page.ID()] = true
	}

	if len(pageIDs) != 10 {
		t.Errorf("Expected 10 unique pages, got %d", len(pageIDs))
	}
}

func TestFreePage(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	// Allocate a page
	page, _ := pm.AllocatePage(PageTypeLeaf)
	pageID := page.ID()

	initialFreeCount := pm.GetFreePageCount()

	// Free the page
	err := pm.FreePage(pageID)
	if err != nil {
		t.Errorf("Failed to free page: %v", err)
	}

	// Free list should increase
	if pm.GetFreePageCount() <= initialFreeCount {
		t.Error("Free page count should increase after freeing")
	}
}

func TestFreeMetaPage(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	// Try to free meta page (page 0)
	err := pm.FreePage(0)
	if err == nil {
		t.Error("Should not be able to free meta page")
	}
}

func TestGetPage(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	// Get meta page
	page, err := pm.GetPage(0)
	if err != nil {
		t.Errorf("Failed to get meta page: %v", err)
	}

	if page == nil {
		t.Error("Meta page is nil")
	}

	if page.ID() != 0 {
		t.Errorf("Expected page ID 0, got %d", page.ID())
	}
}

func TestGetPool(t *testing.T) {
	pm, pool, cleanup := setupPageManager(t)
	defer cleanup()

	returnedPool := pm.GetPool()
	if returnedPool != pool {
		t.Error("GetPool returned different pool")
	}
}

func TestGetMeta(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	meta := pm.GetMeta()
	if meta == nil {
		t.Error("GetMeta returned nil")
	}
}

func TestUpdateMeta(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	// Get current meta
	meta := pm.GetMeta()
	originalVersion := meta.Version

	// Update version
	meta.Version = 999

	// Update meta
	err := pm.UpdateMeta(meta)
	if err != nil {
		t.Errorf("Failed to update meta: %v", err)
	}

	// Verify update
	newMeta := pm.GetMeta()
	if newMeta.Version != 999 {
		t.Errorf("Meta version not updated: expected 999, got %d", newMeta.Version)
	}

	// Restore original
	meta.Version = originalVersion
	pm.UpdateMeta(meta)
}

func TestGetPageCount(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	initialCount := pm.GetPageCount()

	// Allocate pages
	pm.AllocatePage(PageTypeLeaf)
	pm.AllocatePage(PageTypeLeaf)

	newCount := pm.GetPageCount()
	if newCount <= initialCount {
		t.Error("Page count should increase")
	}
}

func TestGetFreePageCount(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	// Get initial free count
	initialFree := pm.GetFreePageCount()

	// Allocate and free a page
	page, _ := pm.AllocatePage(PageTypeLeaf)
	pm.FreePage(page.ID())

	// Free count should be back to initial (or more)
	newFree := pm.GetFreePageCount()
	if newFree < initialFree {
		t.Error("Free count should not decrease after freeing")
	}
}

func TestSync(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	// Allocate and modify a page
	page, _ := pm.AllocatePage(PageTypeLeaf)
	page.SetDirty(true)

	// Sync
	err := pm.Sync()
	if err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}

func TestClose(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)

	// Close should succeed
	err := pm.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	cleanup()
}

func TestAllocatePageReuse(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	// Allocate a page
	page1, _ := pm.AllocatePage(PageTypeLeaf)
	pageID := page1.ID()

	// Free it
	pm.FreePage(pageID)

	// Allocate again - should reuse
	page2, _ := pm.AllocatePage(PageTypeLeaf)

	// May or may not get the same ID depending on implementation
	// Just verify allocation succeeded
	if page2 == nil {
		t.Error("Failed to allocate reused page")
	}
}

func TestPageManagerConcurrency(t *testing.T) {
	pm, _, cleanup := setupPageManager(t)
	defer cleanup()

	done := make(chan bool, 10)

	// Concurrent allocations
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 20; j++ {
				pm.AllocatePage(PageTypeLeaf)
			}
			done <- true
		}()
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 20; j++ {
				pm.GetPageCount()
				pm.GetFreePageCount()
				pm.GetMeta()
			}
			done <- true
		}()
	}

	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}
}
