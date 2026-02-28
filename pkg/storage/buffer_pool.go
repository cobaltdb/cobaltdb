package storage

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	ErrPageNotFound = errors.New("page not found")
	ErrBufferFull   = errors.New("buffer pool is full")
)

// CachedPage represents a page in the buffer pool
type CachedPage struct {
	id       uint32
	data     []byte // PageSize bytes
	dirty    bool
	pinned   int32 // atomic pin count
	lruElem  *list.Element
}

// ID returns the page ID
func (p *CachedPage) ID() uint32 {
	return p.id
}

// Data returns the page data
func (p *CachedPage) Data() []byte {
	return p.data
}

// SetData sets the page data
func (p *CachedPage) SetData(data []byte) {
	p.data = data
}

// IsDirty returns true if the page has been modified
func (p *CachedPage) IsDirty() bool {
	return p.dirty
}

// SetDirty marks the page as dirty
func (p *CachedPage) SetDirty(dirty bool) {
	p.dirty = dirty
}

// Pin increments the pin count
func (p *CachedPage) Pin() {
	atomic.AddInt32(&p.pinned, 1)
}

// Unpin decrements the pin count
func (p *CachedPage) Unpin() {
	atomic.AddInt32(&p.pinned, -1)
}

// IsPinned returns true if the page is pinned
func (p *CachedPage) IsPinned() bool {
	return atomic.LoadInt32(&p.pinned) > 0
}

// BufferPool manages cached pages in memory
type BufferPool struct {
	capacity int                    // max pages in cache
	pages    map[uint32]*CachedPage // pageID -> cached page
	lru      *list.List             // LRU eviction list
	mu       sync.RWMutex
	backend  Backend
	wal      *WAL
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(capacity int, backend Backend) *BufferPool {
	return &BufferPool{
		capacity: capacity,
		pages:    make(map[uint32]*CachedPage),
		lru:      list.New(),
		backend:  backend,
	}
}

// SetWAL sets the WAL for the buffer pool
func (bp *BufferPool) SetWAL(wal *WAL) {
	bp.wal = wal
}

// GetPage retrieves a page from the cache or loads it from disk
func (bp *BufferPool) GetPage(pageID uint32) (*CachedPage, error) {
	if pageID == 0 {
		return nil, ErrInvalidPageID
	}

	// Fast path: check if page is in cache
	bp.mu.RLock()
	if p, ok := bp.pages[pageID]; ok {
		bp.mu.RUnlock()
		bp.touchLRU(p)
		p.Pin()
		return p, nil
	}
	bp.mu.RUnlock()

	// Slow path: load from disk
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Double-check after acquiring write lock
	if p, ok := bp.pages[pageID]; ok {
		bp.touchLRU(p)
		p.Pin()
		return p, nil
	}

	// Evict if at capacity
	if len(bp.pages) >= bp.capacity {
		if err := bp.evict(); err != nil {
			return nil, err
		}
	}

	// Read page from disk
	data := make([]byte, PageSize)
	offset := int64(pageID) * int64(PageSize)
	_, err := bp.backend.ReadAt(data, offset)
	if err != nil {
		// If page doesn't exist on disk, create a new one
		if errors.Is(err, ErrInvalidOffset) || isEOF(err) {
			page := NewPage(pageID, PageTypeFreeList)
			data = page.Data
		} else {
			return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
		}
	}

	page := &CachedPage{
		id:     pageID,
		data:   data,
		dirty:  false,
		pinned: 1,
	}
	bp.pages[pageID] = page
	page.lruElem = bp.lru.PushFront(page)
	return page, nil
}

// NewPage allocates a new page
func (bp *BufferPool) NewPage(pageType PageType) (*CachedPage, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Find next available page ID
	// Start from 1 (page 0 is meta page)
	pageID := uint32(1)
	for {
		if _, exists := bp.pages[pageID]; !exists {
			break
		}
		pageID++
	}

	// Evict if at capacity
	if len(bp.pages) >= bp.capacity {
		if err := bp.evict(); err != nil {
			return nil, err
		}
	}

	// Create new page
	page := NewPage(pageID, pageType)
	cached := &CachedPage{
		id:     pageID,
		data:   page.Data,
		dirty:  true,
		pinned: 1,
	}
	bp.pages[pageID] = cached
	cached.lruElem = bp.lru.PushFront(cached)
	return cached, nil
}

// FlushPage writes a dirty page to disk
func (bp *BufferPool) FlushPage(page *CachedPage) error {
	if !page.IsDirty() {
		return nil
	}

	offset := int64(page.id) * int64(PageSize)
	if _, err := bp.backend.WriteAt(page.data, offset); err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.id, err)
	}

	page.SetDirty(false)
	return nil
}

// FlushAll writes all dirty pages to disk
func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, page := range bp.pages {
		if err := bp.FlushPage(page); err != nil {
			return err
		}
	}

	return bp.backend.Sync()
}

// Unpin decrements the pin count of a page
func (bp *BufferPool) Unpin(page *CachedPage) {
	page.Unpin()
}

// touchLRU moves a page to the front of the LRU list
func (bp *BufferPool) touchLRU(page *CachedPage) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if page.lruElem != nil {
		bp.lru.MoveToFront(page.lruElem)
	}
}

// evict removes the least recently used unpinned page
func (bp *BufferPool) evict() error {
	elem := bp.lru.Back()
	for elem != nil {
		page := elem.Value.(*CachedPage)
		if !page.IsPinned() {
			// Flush if dirty
			if page.IsDirty() {
				if err := bp.FlushPage(page); err != nil {
					return err
				}
			}
			// Remove from cache
			delete(bp.pages, page.id)
			bp.lru.Remove(elem)
			return nil
		}
		elem = elem.Prev()
	}

	return ErrBufferFull
}

// Close flushes all pages and closes the buffer pool
func (bp *BufferPool) Close() error {
	if err := bp.FlushAll(); err != nil {
		return err
	}
	return nil
}

// PageCount returns the number of pages in the cache
func (bp *BufferPool) PageCount() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return len(bp.pages)
}

// isEOF checks if an error is an EOF-like error
func isEOF(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errors.New("EOF")) || errors.Is(err, errors.New("unexpected EOF"))
}
