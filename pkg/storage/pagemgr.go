package storage

import (
	"fmt"
	"sync"
)

// PageManager handles page allocation, deallocation, and free list management
type PageManager struct {
	pool     *BufferPool
	meta     *MetaPage
	mu       sync.RWMutex
	freeList []uint32 // Cache of free page IDs
	maxPages uint32   // Maximum allocated page ID
}

// NewPageManager creates a new page manager
func NewPageManager(pool *BufferPool) (*PageManager, error) {
	pm := &PageManager{
		pool:     pool,
		freeList: make([]uint32, 0),
		maxPages: 0,
	}

	// Try to load existing meta page
	metaPage, err := pool.GetPage(0)
	if err != nil {
		// Create new database
		return pm.initNewDatabase()
	}
	defer pool.Unpin(metaPage)

	// Deserialize meta page
	meta := &MetaPage{}
	if err := meta.Deserialize(metaPage.Data()); err != nil {
		return nil, fmt.Errorf("failed to deserialize meta page: %w", err)
	}

	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("invalid meta page: %w", err)
	}

	pm.meta = meta
	pm.maxPages = meta.PageCount

	// Load free list
	if err := pm.loadFreeList(); err != nil {
		return nil, fmt.Errorf("failed to load free list: %w", err)
	}

	return pm, nil
}

// initNewDatabase initializes a new database file
func (pm *PageManager) initNewDatabase() (*PageManager, error) {
	// Create meta page
	pm.meta = NewMetaPage()

	// Allocate page 0 using NewPage
	metaPage, err := pm.pool.NewPage(PageTypeMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta page: %w", err)
	}

	pm.meta.Serialize(metaPage.Data())
	metaPage.SetDirty(true)

	// Flush to disk
	if err := pm.pool.FlushPage(metaPage); err != nil {
		return nil, fmt.Errorf("failed to write meta page: %w", err)
	}

	pm.pool.Unpin(metaPage)
	pm.maxPages = 1
	return pm, nil
}

// loadFreeList loads the free list from disk
func (pm *PageManager) loadFreeList() error {
	if pm.meta.FreeListID == 0 {
		// No free pages
		return nil
	}

	// Traverse free list pages
	currentID := pm.meta.FreeListID
	for currentID != 0 {
		page, err := pm.pool.GetPage(currentID)
		if err != nil {
			return err
		}

		data := page.Data()

		// Free list page format: [nextPageID:4][count:4][pageIDs...]
		if len(data) < 8 {
			pm.pool.Unpin(page)
			return fmt.Errorf("invalid free list page %d", currentID)
		}

		nextID := uint32(data[0]) | uint32(data[1])<<8 |
			uint32(data[2])<<16 | uint32(data[3])<<24
		count := uint32(data[4]) | uint32(data[5])<<8 |
			uint32(data[6])<<16 | uint32(data[7])<<24

		// Read page IDs
		offset := 8
		for i := uint32(0); i < count; i++ {
			if offset+4 > len(data) {
				break
			}
			pageID := uint32(data[offset]) | uint32(data[offset+1])<<8 |
				uint32(data[offset+2])<<16 | uint32(data[offset+3])<<24
			pm.freeList = append(pm.freeList, pageID)
			offset += 4
		}

		pm.pool.Unpin(page)
		currentID = nextID
	}

	return nil
}

// saveFreeList saves the free list to disk
func (pm *PageManager) saveFreeList() error {
	// TODO: Implement free list persistence
	// For now, free pages are lost on close (acceptable for basic implementation)
	return nil
}

// AllocatePage allocates a new page
func (pm *PageManager) AllocatePage(pageType PageType) (*CachedPage, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var pageID uint32

	// Try to reuse a free page first
	if len(pm.freeList) > 0 {
		pageID = pm.freeList[len(pm.freeList)-1]
		pm.freeList = pm.freeList[:len(pm.freeList)-1]

		// Get the existing page and reuse it
		page, err := pm.pool.GetPage(pageID)
		if err != nil {
			// Return page ID to free list
			pm.freeList = append(pm.freeList, pageID)
			return nil, err
		}

		// Reset page header - create a fresh page structure
		newPage := NewPage(pageID, pageType)
		copy(page.Data(), newPage.Data)
		page.SetDirty(true)

		return page, nil
	}

	// Allocate new page using buffer pool
	page, err := pm.pool.NewPage(pageType)
	if err != nil {
		return nil, err
	}

	pageID = page.ID()

	// Update max pages if needed
	if pageID >= pm.maxPages {
		pm.maxPages = pageID + 1
		pm.meta.PageCount = pm.maxPages
		if err := pm.writeMetaPage(); err != nil {
			return nil, err
		}
	}

	return page, nil
}

// FreePage marks a page as free
func (pm *PageManager) FreePage(pageID uint32) error {
	if pageID == 0 {
		return fmt.Errorf("cannot free meta page")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Add to free list cache
	pm.freeList = append(pm.freeList, pageID)

	// If free list gets too large, persist some of it
	if len(pm.freeList) > 1000 {
		if err := pm.saveFreeList(); err != nil {
			return err
		}
	}

	return nil
}

// GetPage retrieves a page by ID
func (pm *PageManager) GetPage(pageID uint32) (*CachedPage, error) {
	return pm.pool.GetPage(pageID)
}

// GetPool returns the underlying buffer pool
func (pm *PageManager) GetPool() *BufferPool {
	return pm.pool
}

// GetMeta returns the metadata page
func (pm *PageManager) GetMeta() *MetaPage {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.meta
}

// UpdateMeta updates the metadata page
func (pm *PageManager) UpdateMeta(meta *MetaPage) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.meta = meta
	return pm.writeMetaPage()
}

// writeMetaPage writes the metadata page to disk
func (pm *PageManager) writeMetaPage() error {
	metaPage, err := pm.pool.GetPage(0)
	if err != nil {
		return err
	}
	defer pm.pool.Unpin(metaPage)

	pm.meta.Serialize(metaPage.Data())
	metaPage.SetDirty(true)

	return pm.pool.FlushPage(metaPage)
}

// GetPageCount returns the total number of pages
func (pm *PageManager) GetPageCount() uint32 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.maxPages
}

// GetFreePageCount returns the number of free pages
func (pm *PageManager) GetFreePageCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.freeList)
}

// Sync flushes all dirty pages to disk
func (pm *PageManager) Sync() error {
	return pm.pool.FlushAll()
}

// Close closes the page manager
func (pm *PageManager) Close() error {
	// Save free list
	if err := pm.saveFreeList(); err != nil {
		return err
	}

	// Sync all pages
	return pm.Sync()
}
