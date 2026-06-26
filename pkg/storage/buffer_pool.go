package storage

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

var (
	ErrPageNotFound     = errors.New("page not found")
	ErrBufferFull       = errors.New("buffer pool is full")
	ErrPageIDExhausted  = errors.New("page ID space exhausted")
	ErrBufferPoolClosed = errors.New("buffer pool is closed")
)

// lruTouchThreshold controls how often LRU position is updated on cache hits.
// Only every N-th access acquires the write lock to move the page to the front,
// reducing write lock contention while maintaining approximate LRU behavior.
const lruTouchThreshold = 8

const maxCachedPagePinCount = int32(1<<31 - 1)

// CachedPage represents a page in the buffer pool
// Fields ordered by decreasing alignment to minimize padding
type CachedPage struct {
	data        []byte        // PageSize bytes (24 bytes: ptr+len+cap)
	lruElem     *list.Element // 8 bytes pointer
	mu          sync.RWMutex
	id          uint32 // 4 bytes
	pinned      int32  // 4 bytes, atomic pin count
	accessCount uint32 // 4 bytes, atomic access counter for probabilistic LRU
	dirty       uint32 // 4 bytes, atomic: 1 = dirty, 0 = clean
}

// ID returns the page ID
func (p *CachedPage) ID() uint32 {
	return p.id
}

// Data returns the page data
func (p *CachedPage) Data() []byte {
	return p.data
}

// WithDataWrite provides exclusive access to the page buffer while it is mutated.
func (p *CachedPage) WithDataWrite(fn func([]byte)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(p.data)
}

func (p *CachedPage) dataSnapshot() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()

	snapshot := make([]byte, len(p.data))
	copy(snapshot, p.data)
	return snapshot
}

// SetData copies data into the page-sized buffer, clearing any previous bytes.
func (p *CachedPage) SetData(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cap(p.data) < PageSize {
		p.data = getPageData()
	} else {
		p.data = p.data[:PageSize]
		clear(p.data)
	}
	copy(p.data, data)
}

// IsDirty returns true if the page has been modified
func (p *CachedPage) IsDirty() bool {
	return atomic.LoadUint32(&p.dirty) != 0
}

// SetDirty marks the page as dirty
func (p *CachedPage) SetDirty(dirty bool) {
	if dirty {
		atomic.StoreUint32(&p.dirty, 1)
	} else {
		atomic.StoreUint32(&p.dirty, 0)
	}
}

// Pin increments the pin count
func (p *CachedPage) Pin() {
	for {
		current := atomic.LoadInt32(&p.pinned)
		if current == maxCachedPagePinCount {
			return
		}
		if atomic.CompareAndSwapInt32(&p.pinned, current, current+1) {
			return
		}
	}
}

// Unpin decrements the pin count
func (p *CachedPage) Unpin() {
	for {
		current := atomic.LoadInt32(&p.pinned)
		if current <= 0 {
			return
		}
		if atomic.CompareAndSwapInt32(&p.pinned, current, current-1) {
			return
		}
	}
}

// IsPinned returns true if the page is pinned
func (p *CachedPage) IsPinned() bool {
	return atomic.LoadInt32(&p.pinned) > 0
}

// BufferPool manages cached pages in memory
type BufferPool struct {
	capacity   int                    // max pages in cache
	pages      map[uint32]*CachedPage // pageID -> cached page
	lru        *list.List             // LRU eviction list
	mu         sync.RWMutex
	backend    Backend
	wal        *WAL
	nextPageID uint32 // next available page ID for allocation
	stats      *bufferPoolStatsCollector
	initErr    error
	closed     bool

	// Background flusher
	flushInterval time.Duration
	flushDone     chan struct{}
	flushMu       sync.Mutex
	flushRunning  bool
	flushErrCount int // consecutive flush errors (resets on success)
	flushErrLimit int // max consecutive errors before halting flusher (default 3)
}

// NewBufferPool creates a new buffer pool.
//
// Deprecated for callers that need initialization errors: use
// NewBufferPoolWithError. This compatibility wrapper never panics; if
// initialization fails, subsequent page operations return the stored error.
func NewBufferPool(capacity int, backend Backend) *BufferPool {
	bp, err := NewBufferPoolWithError(capacity, backend)
	if err != nil {
		return &BufferPool{
			capacity: max(1, capacity),
			pages:    make(map[uint32]*CachedPage),
			lru:      list.New(),
			backend:  backend,
			stats:    newBufferPoolStatsCollector(),
			initErr:  err,
		}
	}
	return bp
}

// NewBufferPoolWithError creates a new buffer pool and reports invalid backend
// state instead of deferring the error to the first page operation.
func NewBufferPoolWithError(capacity int, backend Backend) (*BufferPool, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("buffer pool capacity must be positive: %d", capacity)
	}
	if backend == nil {
		return nil, errors.New("buffer pool backend cannot be nil")
	}

	bp := &BufferPool{
		capacity:      capacity,
		pages:         make(map[uint32]*CachedPage, capacity),
		lru:           list.New(),
		backend:       backend,
		stats:         newBufferPoolStatsCollector(),
		flushErrLimit: 3, // halt flusher after 3 consecutive flush errors
	}

	// Initialize nextPageID based on backend size
	// Page 0 is reserved for meta page, so start from 1 if backend is empty
	backendSize := backend.Size()
	if backendSize < 0 {
		return nil, fmt.Errorf("%w: backend size is negative: %d", ErrInvalidSize, backendSize)
	}
	if backendSize == 0 {
		bp.nextPageID = 1 // Reserve page 0 for meta page
	} else {
		pageCount, err := checkedUint32((backendSize+PageSize-1)/PageSize, "backend page count")
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrPageIDExhausted, err)
		}
		bp.nextPageID = pageCount
		if bp.nextPageID == 0 {
			bp.nextPageID = 1
		}
	}

	return bp, nil
}

// SetWAL sets the WAL for the buffer pool
func (bp *BufferPool) SetWAL(wal *WAL) {
	bp.wal = wal
}

// GetPage retrieves a page from the cache or loads it from disk
func (bp *BufferPool) GetPage(pageID uint32) (*CachedPage, error) {
	if bp.initErr != nil {
		return nil, bp.initErr
	}

	// Note: pageID 0 is valid (it's the meta page)

	// Fast path: check if page is in cache
	bp.mu.RLock()
	if bp.closed {
		bp.mu.RUnlock()
		return nil, ErrBufferPoolClosed
	}
	if p, ok := bp.pages[pageID]; ok {
		bp.mu.RUnlock()
		bp.touchLRU(p)
		p.Pin()
		bp.stats.recordHit()
		return p, nil
	}
	bp.mu.RUnlock()

	// Slow path: load from disk
	bp.stats.recordMiss()
	bp.mu.Lock()
	if bp.closed {
		bp.mu.Unlock()
		return nil, ErrBufferPoolClosed
	}

	// Double-check after acquiring write lock
	if p, ok := bp.pages[pageID]; ok {
		bp.touchLRUUnsafe(p)
		p.Pin()
		bp.mu.Unlock()
		bp.stats.recordHit()
		return p, nil
	}

	// Evict if at capacity
	if len(bp.pages) >= bp.capacity {
		if err := bp.evict(); err != nil {
			bp.mu.Unlock()
			return nil, err
		}
	}

	// Read page from disk
	data := getPageData()
	offset := int64(pageID) * int64(PageSize)
	start := time.Now()
	_, err := ReadFullAt(bp.backend, data, offset)
	readTime := time.Since(start)
	if err != nil {
		putPageData(data)
		bp.mu.Unlock()
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}
	if pageID != 0 {
		if err := validatePageHeader(data, pageID); err != nil {
			putPageData(data)
			bp.mu.Unlock()
			return nil, err
		}
	}
	bp.stats.recordRead(readTime)

	page := &CachedPage{
		id:     pageID,
		data:   data,
		dirty:  0, // atomic: 0 = clean
		pinned: 1,
	}
	bp.pages[pageID] = page
	page.lruElem = bp.lru.PushFront(page)
	bp.mu.Unlock()
	return page, nil
}

// NewPage allocates a new page
func (bp *BufferPool) NewPage(pageType PageType) (*CachedPage, error) {
	if bp.initErr != nil {
		return nil, bp.initErr
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.closed {
		return nil, ErrBufferPoolClosed
	}

	// Use next available page ID
	if bp.nextPageID == 0 {
		return nil, ErrPageIDExhausted
	}
	pageID := bp.nextPageID
	bp.nextPageID++

	// Evict if at capacity
	if len(bp.pages) >= bp.capacity {
		if err := bp.evict(); err != nil {
			// Rollback the nextPageID increment on failure
			bp.nextPageID--
			return nil, err
		}
	}

	// Create new page
	page := NewPage(pageID, pageType)
	cached := &CachedPage{
		id:     pageID,
		data:   page.Data,
		dirty:  1, // atomic: 1 = dirty
		pinned: 1,
	}
	bp.pages[pageID] = cached
	cached.lruElem = bp.lru.PushFront(cached)
	return cached, nil
}

// FlushPage writes a dirty page to disk.
//
// The dirty bit is cleared BEFORE the page contents are snapshotted. The
// background flusher does not pin or lock the page across the whole flush, so a
// concurrent writer may mutate the page (WithDataWrite) and SetDirty(true)
// while this flush is in flight. Clearing first means any such concurrent write
// re-sets the dirty bit and the page is re-flushed later; if we cleared the bit
// AFTER snapshotting, that SetDirty(false) would clobber the writer's
// SetDirty(true) and silently drop the newer version (lost update). On write
// failure the bit is restored so the page is retried.
func (bp *BufferPool) FlushPage(page *CachedPage) error {
	if !page.IsDirty() {
		return nil
	}

	offset := int64(page.id) * int64(PageSize)
	page.SetDirty(false)
	data := page.dataSnapshot()
	start := time.Now()
	if _, err := WriteFullAt(bp.backend, data, offset); err != nil {
		page.SetDirty(true) // retry on the next flush cycle
		return fmt.Errorf("failed to write page %d: %w", page.id, err)
	}
	writeTime := time.Since(start)
	bp.stats.recordWrite(writeTime)

	return nil
}

// FlushAll writes all dirty pages to disk
func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.closed {
		return ErrBufferPoolClosed
	}

	for _, page := range bp.pages {
		if err := bp.FlushPage(page); err != nil {
			return err
		}
	}

	return bp.backend.Sync()
}

// FlushDirty writes only dirty pages to disk and syncs the backend.
// Unlike FlushAll, it does not hold bp.mu during I/O, allowing concurrent
// GetPage operations while dirty pages are being flushed. This is safe
// under the checkpoint protocol because flushMu (held by DB.Checkpoint)
// serializes with writers, so no new page modifications can occur during
// the flush+sync window.
func (bp *BufferPool) FlushDirty() error {
	bp.mu.RLock()
	if bp.closed {
		bp.mu.RUnlock()
		return ErrBufferPoolClosed
	}
	dirty := make([]*CachedPage, 0, len(bp.pages))
	for _, page := range bp.pages {
		if page.IsDirty() {
			dirty = append(dirty, page)
		}
	}
	bp.mu.RUnlock()

	for _, page := range dirty {
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

// touchLRU moves a page to the front of the LRU list using probabilistic updates.
// Only every lruTouchThreshold-th access actually acquires the write lock,
// dramatically reducing contention on cache hits while maintaining approximate LRU order.
func (bp *BufferPool) touchLRU(page *CachedPage) {
	count := atomic.AddUint32(&page.accessCount, 1)
	if count%lruTouchThreshold != 0 {
		return
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.touchLRUUnsafe(page)
}

// touchLRUUnsafe moves a page to the front of the LRU list without acquiring lock
// Must be called with bp.mu held
func (bp *BufferPool) touchLRUUnsafe(page *CachedPage) {
	if page.lruElem != nil {
		bp.lru.MoveToFront(page.lruElem)
	}
}

// evict removes the least recently used unpinned page.
// NOTE: This is called while holding bp.mu (write lock). If the victim page is
// dirty, FlushPage performs synchronous I/O under the lock, which can block
// concurrent GetPage/NewPage callers. To reduce this, we first scan the LRU
// tail for a clean unpinned page and only flush a dirty page as a last resort.
func (bp *BufferPool) evict() error {
	// First pass: prefer clean pages to avoid synchronous I/O under the lock.
	// Scan up to 25% of capacity (min 4) looking for a clean, unpinned victim.
	maxScan := bp.capacity / 4
	if maxScan < 4 {
		maxScan = 4
	}
	scanned := 0
	elem := bp.lru.Back()
	for elem != nil && scanned < maxScan {
		page := elem.Value.(*CachedPage)
		if !page.IsPinned() {
			if !page.IsDirty() {
				putPageData(page.data)
				delete(bp.pages, page.id)
				bp.lru.Remove(elem)
				bp.stats.recordEviction()
				return nil
			}
			scanned++
		}
		elem = elem.Prev()
	}

	// Second pass: fall back to flushing a dirty page if necessary.
	elem = bp.lru.Back()
	for elem != nil {
		page := elem.Value.(*CachedPage)
		if !page.IsPinned() {
			if page.IsDirty() {
				if err := bp.FlushPage(page); err != nil {
					return err
				}
			}
			putPageData(page.data)
			delete(bp.pages, page.id)
			bp.lru.Remove(elem)
			bp.stats.recordEviction()
			return nil
		}
		elem = elem.Prev()
	}

	return ErrBufferFull
}

// Close stops the background flusher, flushes all pages, and closes the buffer pool
func (bp *BufferPool) Close() error {
	bp.stopBackgroundFlusher()
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.closed {
		return nil
	}
	for _, page := range bp.pages {
		if err := bp.FlushPage(page); err != nil {
			return err
		}
	}
	if err := bp.backend.Sync(); err != nil {
		return err
	}
	bp.closed = true
	return nil
}

// StartBackgroundFlusher starts a goroutine that periodically flushes dirty pages.
// This reduces eviction latency by proactively writing dirty pages to disk
// instead of flushing them synchronously during eviction under the write lock.
func (bp *BufferPool) StartBackgroundFlusher(interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	bp.flushMu.Lock()
	defer bp.flushMu.Unlock()
	if bp.flushRunning {
		return
	}

	bp.flushInterval = interval
	bp.flushDone = make(chan struct{})
	bp.flushRunning = true
	go bp.backgroundFlushLoop(bp.flushDone, interval)
}

func (bp *BufferPool) stopBackgroundFlusher() {
	bp.flushMu.Lock()
	if !bp.flushRunning {
		bp.flushMu.Unlock()
		return
	}
	done := bp.flushDone
	bp.flushDone = nil
	bp.flushRunning = false
	bp.flushMu.Unlock()

	close(done)
}

func (bp *BufferPool) backgroundFlushLoop(done <-chan struct{}, slowInterval time.Duration) {
	fastInterval := 200 * time.Millisecond
	const dirtyThreshold = 0.25 // 25% dirty ratio triggers fast flushing

	for {
		interval := slowInterval
		if bp.dirtyRatio() > dirtyThreshold {
			interval = fastInterval
		}

		timer := time.NewTimer(interval)
		select {
		case <-done:
			timer.Stop()
			return
		case <-timer.C:
			bp.flushDirtyPages()
		}
	}
}

// dirtyRatio returns the fraction of cached pages that are dirty.
func (bp *BufferPool) dirtyRatio() float64 {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	if len(bp.pages) == 0 {
		return 0
	}
	var dirty int
	for _, page := range bp.pages {
		if page.IsDirty() {
			dirty++
		}
	}
	return float64(dirty) / float64(len(bp.pages))
}

// flushDirtyPages writes dirty unpinned pages to disk without holding the lock during I/O.
// Errors are logged, counted, and used to track consecutive failures. After
// flushErrLimit consecutive errors the flusher is halted to prevent infinite retry.
func (bp *BufferPool) flushDirtyPages() {
	// Collect dirty unpinned pages under read lock
	bp.mu.RLock()
	var dirty []*CachedPage
	for _, page := range bp.pages {
		if page.IsDirty() && !page.IsPinned() {
			dirty = append(dirty, page)
		}
	}
	bp.mu.RUnlock()

	hadError := false
	// Flush each page individually (acquires write lock briefly per page)
	for _, page := range dirty {
		if !page.IsDirty() || page.IsPinned() {
			continue // re-check after lock release
		}
		if err := bp.FlushPage(page); err != nil {
			bp.stats.recordFlushError()
			logger.GetGlobalLogger().Errorf("buffer pool: failed to flush page %d: %v", page.id, err)
			hadError = true
		}
	}

	bp.flushMu.Lock()
	if hadError {
		bp.flushErrCount++
		if bp.flushErrCount >= bp.flushErrLimit && bp.flushRunning {
			logger.GetGlobalLogger().Errorf("buffer pool: halting flusher after %d consecutive flush errors",
				bp.flushErrCount)
			done := bp.flushDone
			bp.flushRunning = false
			bp.flushDone = nil
			if done != nil {
				close(done)
			}
		}
	} else {
		bp.flushErrCount = 0 // reset on success
	}
	bp.flushMu.Unlock()
}

// PageCount returns the number of pages in the cache
func (bp *BufferPool) PageCount() int {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return len(bp.pages)
}

// AllocatedPageCount returns the number of page IDs allocated by this pool.
func (bp *BufferPool) AllocatedPageCount() uint32 {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.nextPageID
}
