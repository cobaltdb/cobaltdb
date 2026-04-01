package btree

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrKeyExists       = errors.New("key already exists")
	ErrTreeFull        = errors.New("tree is full")
	ErrInvalidKey      = errors.New("invalid key")
	ErrInvalidValue    = errors.New("invalid value")
	ErrKeyTooLong      = errors.New("key exceeds maximum length of 65535 bytes")
	ErrMemoryLimit     = errors.New("memory limit exceeded")
	DefaultMemoryLimit = int64(64 * 1024 * 1024) // 64MB default
	MaxKeyLength       = 65535                   // uint16 max - serialization limit
)

// lruEntry tracks memory usage for LRU eviction
type lruEntry struct {
	key  string
	size int64
	elem *list.Element
}

// BTree represents a disk-based B+Tree index using a hybrid approach:
// - Each table has its own BTree instance
// - Data is stored as key-value pairs in pages managed by the buffer pool
// - The BTree maintains an in-memory sorted structure that flushes to disk pages
// - Multi-page overflow: data exceeding one page spills to linked overflow pages

type BTree struct {
	mu            sync.RWMutex
	rootPageID    uint32
	pool          *storage.BufferPool
	order         int
	memStorage    map[string][]byte
	dirty         bool
	overflowPages []uint32 // IDs of overflow pages used by this tree

	// Memory management
	memoryLimit int64                // Maximum memory to use (0 = unlimited)
	memoryUsed  int64                // Current memory usage
	lruMu       sync.Mutex           // Separate lock for LRU updates (avoids write-locking on reads)
	lruList     *list.List           // LRU list for eviction
	lruMap      map[string]*lruEntry // Track entries in LRU
	evictedKeys map[string]bool      // Keys evicted from memory but preserved on disk
}

// usablePageSize is the space available for data in each page (after header)
const usablePageSize = storage.PageSize - storage.PageHeaderSize

// Overflow page format:
// Root page: [totalCount:4][overflowCount:4][overflowIDs:4*N][KV data...]
// Overflow page: [KV data continuation...]
// Root header size = 8 bytes + 4*overflowCount

// NewBTree creates a new B+Tree with default memory limit
func NewBTree(pool *storage.BufferPool) (*BTree, error) {
	return NewBTreeWithLimit(pool, DefaultMemoryLimit)
}

// NewBTreeWithLimit creates a new B+Tree with a specified memory limit
// limit: maximum memory in bytes (0 = unlimited)
func NewBTreeWithLimit(pool *storage.BufferPool, limit int64) (*BTree, error) {
	rootPage, err := pool.NewPage(storage.PageTypeLeaf)
	if err != nil {
		return nil, fmt.Errorf("failed to create root page: %w", err)
	}
	defer pool.Unpin(rootPage)

	return &BTree{
		rootPageID:  rootPage.ID(),
		pool:        pool,
		order:       100,
		memStorage:  make(map[string][]byte),
		dirty:       false,
		memoryLimit: limit,
		memoryUsed:  0,
		lruList:     list.New(),
		lruMap:      make(map[string]*lruEntry),
		evictedKeys: make(map[string]bool),
	}, nil
}

// OpenBTree opens an existing B+Tree with the given root page ID
func OpenBTree(pool *storage.BufferPool, rootPageID uint32) *BTree {
	return OpenBTreeWithLimit(pool, rootPageID, DefaultMemoryLimit)
}

// OpenBTreeWithLimit opens an existing B+Tree with a specified memory limit
func OpenBTreeWithLimit(pool *storage.BufferPool, rootPageID uint32, limit int64) *BTree {
	t := &BTree{
		rootPageID:  rootPageID,
		pool:        pool,
		order:       100,
		memStorage:  make(map[string][]byte),
		dirty:       false,
		memoryLimit: limit,
		memoryUsed:  0,
		lruList:     list.New(),
		lruMap:      make(map[string]*lruEntry),
		evictedKeys: make(map[string]bool),
	}
	if err := t.loadFromPages(); err != nil {
		// Log but continue - tree will appear empty but won't lose data on disk
		fmt.Printf("btree: warning: failed to load pages for root %d: %v\n", rootPageID, err)
	}
	return t
}

// loadFromPages loads serialized key-value pairs from root + overflow pages into memStorage
func (t *BTree) loadFromPages() error {
	root, err := t.pool.GetPage(t.rootPageID)
	if err != nil {
		return fmt.Errorf("failed to load root page %d: %w", t.rootPageID, err)
	}
	defer t.pool.Unpin(root)

	pageData := root.Data()[storage.PageHeaderSize:]
	if len(pageData) < 8 {
		return nil // empty/new page, not an error
	}

	totalCount := binary.LittleEndian.Uint32(pageData[0:4])
	overflowCount := binary.LittleEndian.Uint32(pageData[4:8])

	if totalCount == 0 {
		return nil
	}

	// Read overflow page IDs
	headerSize := 8 + 4*int(overflowCount)
	if headerSize > len(pageData) {
		return fmt.Errorf("corrupt root page %d: header size %d exceeds page data %d", t.rootPageID, headerSize, len(pageData))
	}

	t.overflowPages = make([]uint32, overflowCount)
	for i := uint32(0); i < overflowCount; i++ {
		off := 8 + 4*int(i)
		t.overflowPages[i] = binary.LittleEndian.Uint32(pageData[off : off+4])
	}

	// Collect all data from root page + overflow pages
	var allData []byte
	allData = append(allData, pageData[headerSize:]...)

	for _, pgID := range t.overflowPages {
		pg, err := t.pool.GetPage(pgID)
		if err != nil {
			return fmt.Errorf("failed to load overflow page %d: %w", pgID, err)
		}
		allData = append(allData, pg.Data()[storage.PageHeaderSize:]...)
		t.pool.Unpin(pg)
	}

	// Deserialize KV pairs
	offset := 0
	for i := uint32(0); i < totalCount; i++ {
		if offset+2 > len(allData) {
			break
		}
		keyLen := int(binary.LittleEndian.Uint16(allData[offset : offset+2]))
		offset += 2

		if offset+keyLen > len(allData) {
			break
		}
		key := make([]byte, keyLen)
		copy(key, allData[offset:offset+keyLen])
		offset += keyLen

		if offset+4 > len(allData) {
			break
		}
		valLen := int(binary.LittleEndian.Uint32(allData[offset : offset+4]))
		offset += 4

		if offset+valLen > len(allData) {
			break
		}
		val := make([]byte, valLen)
		copy(val, allData[offset:offset+valLen])
		offset += valLen

		t.memStorage[string(key)] = val
	}
	return nil
}

// readKVFromPages reads all key-value pairs from disk pages without modifying memStorage.
// Used to preserve evicted entries during flush and to serve Get requests for evicted keys.
func (t *BTree) readKVFromPages() map[string][]byte {
	result := make(map[string][]byte)

	root, err := t.pool.GetPage(t.rootPageID)
	if err != nil {
		return result
	}
	defer t.pool.Unpin(root)

	pageData := root.Data()[storage.PageHeaderSize:]
	if len(pageData) < 8 {
		return result
	}

	totalCount := binary.LittleEndian.Uint32(pageData[0:4])
	overflowCount := binary.LittleEndian.Uint32(pageData[4:8])

	if totalCount == 0 {
		return result
	}

	headerSize := 8 + 4*int(overflowCount)
	if headerSize > len(pageData) {
		return result
	}

	overflowIDs := make([]uint32, overflowCount)
	for i := uint32(0); i < overflowCount; i++ {
		off := 8 + 4*int(i)
		overflowIDs[i] = binary.LittleEndian.Uint32(pageData[off : off+4])
	}

	var allData []byte
	allData = append(allData, pageData[headerSize:]...)

	for _, pgID := range overflowIDs {
		pg, err := t.pool.GetPage(pgID)
		if err != nil {
			return result
		}
		allData = append(allData, pg.Data()[storage.PageHeaderSize:]...)
		t.pool.Unpin(pg)
	}

	offset := 0
	for i := uint32(0); i < totalCount; i++ {
		if offset+2 > len(allData) {
			break
		}
		keyLen := int(binary.LittleEndian.Uint16(allData[offset : offset+2]))
		offset += 2

		if offset+keyLen > len(allData) {
			break
		}
		key := string(allData[offset : offset+keyLen])
		offset += keyLen

		if offset+4 > len(allData) {
			break
		}
		valLen := int(binary.LittleEndian.Uint32(allData[offset : offset+4]))
		offset += 4

		if offset+valLen > len(allData) {
			break
		}
		val := make([]byte, valLen)
		copy(val, allData[offset:offset+valLen])
		offset += valLen

		result[key] = val
	}

	return result
}

// RootPageID returns the root page ID of the tree
func (t *BTree) RootPageID() uint32 {
	return t.rootPageID
}

// SetMemoryLimit sets the memory limit for the BTree (0 = unlimited)
func (t *BTree) SetMemoryLimit(limit int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.memoryLimit = limit
}

// MemoryLimit returns the current memory limit
func (t *BTree) MemoryLimit() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.memoryLimit
}

// MemoryUsed returns the current memory usage
func (t *BTree) MemoryUsed() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.memoryUsed
}

// Get retrieves a value by key
func (t *BTree) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrInvalidKey
	}

	t.mu.RLock()
	keyStr := string(key)
	val, ok := t.memStorage[keyStr]
	if ok {
		result := make([]byte, len(val))
		copy(result, val)
		t.mu.RUnlock()

		// Update LRU under its own lock (does not block concurrent readers)
		t.lruMu.Lock()
		if entry, ok := t.lruMap[keyStr]; ok {
			t.lruList.MoveToFront(entry.elem)
		}
		t.lruMu.Unlock()

		return result, nil
	}

	// Check if key was evicted to disk
	if t.evictedKeys[keyStr] {
		diskData := t.readKVFromPages()
		if val, ok := diskData[keyStr]; ok {
			result := make([]byte, len(val))
			copy(result, val)
			t.mu.RUnlock()
			return result, nil
		}
	}

	t.mu.RUnlock()
	return nil, ErrKeyNotFound
}

// Put inserts or updates a key-value pair
func (t *BTree) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(key) > MaxKeyLength {
		return ErrKeyTooLong
	}
	if len(value) == 0 {
		return ErrInvalidValue
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	keyCopy := string(key)
	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	// If overwriting an evicted key, remove from evicted set
	delete(t.evictedKeys, keyCopy)

	// Calculate size change
	oldSize := int64(0)
	if oldVal, exists := t.memStorage[keyCopy]; exists {
		oldSize = int64(len(key) + len(oldVal))
		// Remove old entry from LRU
		t.lruMu.Lock()
		if entry, ok := t.lruMap[keyCopy]; ok {
			t.lruList.Remove(entry.elem)
			delete(t.lruMap, keyCopy)
		}
		t.lruMu.Unlock()
	}
	newSize := int64(len(key) + len(valCopy))
	sizeDelta := newSize - oldSize

	// Check memory limit and evict if necessary
	if t.memoryLimit > 0 && t.memoryUsed+sizeDelta > t.memoryLimit {
		if err := t.evictToMakeSpace(sizeDelta); err != nil {
			return err
		}
	}

	t.memStorage[keyCopy] = valCopy
	t.memoryUsed += sizeDelta
	t.dirty = true

	// Add to LRU front (most recently used)
	t.lruMu.Lock()
	entry := &lruEntry{
		key:  keyCopy,
		size: newSize,
	}
	entry.elem = t.lruList.PushFront(entry)
	t.lruMap[keyCopy] = entry
	t.lruMu.Unlock()

	return nil
}

// evictToMakeSpace evicts entries from LRU until we have enough space
func (t *BTree) evictToMakeSpace(needed int64) error {
	// If we need more space than the limit itself, we can't satisfy
	if needed > t.memoryLimit {
		return ErrMemoryLimit
	}

	// Keep evicting until we have enough space
	for t.memoryUsed+needed > t.memoryLimit {
		// Get least recently used entry
		t.lruMu.Lock()
		elem := t.lruList.Back()
		if elem == nil {
			t.lruMu.Unlock()
			break
		}
		entry := elem.Value.(*lruEntry)
		evictKey := entry.key
		t.lruList.Remove(elem)
		delete(t.lruMap, evictKey)
		t.lruMu.Unlock()

		// Flush to disk before evicting (only if dirty)
		if t.dirty {
			if err := t.flushInternal(); err != nil {
				return fmt.Errorf("failed to flush during eviction: %w", err)
			}
		}

		// Remove from memory but track as evicted (data preserved on disk via flush)
		if val, ok := t.memStorage[evictKey]; ok {
			t.memoryUsed -= int64(len(evictKey) + len(val))
			delete(t.memStorage, evictKey)
			t.evictedKeys[evictKey] = true
		}
	}

	if t.memoryUsed+needed > t.memoryLimit {
		return ErrMemoryLimit
	}

	return nil
}

// flushInternal flushes data to disk pages (must be called with lock held)
func (t *BTree) flushInternal() error {
	if !t.dirty {
		return nil
	}

	// Build the complete data set: memStorage + evicted entries from disk
	toSerialize := make(map[string][]byte, len(t.memStorage)+len(t.evictedKeys))

	// If there are evicted keys, read their values from disk to preserve them
	if len(t.evictedKeys) > 0 {
		diskData := t.readKVFromPages()
		for k, v := range diskData {
			if t.evictedKeys[k] {
				toSerialize[k] = v
			}
		}
	}

	// Overlay with current memStorage (in-memory values are authoritative)
	for k, v := range t.memStorage {
		toSerialize[k] = v
	}

	// Serialize all key-value pairs
	// Format: [keylen:2][key][valuelen:4][value]...
	var kvBuf bytes.Buffer
	count := uint32(len(toSerialize))

	var lenBuf [4]byte
	for k, v := range toSerialize {
		key := []byte(k)
		binary.LittleEndian.PutUint16(lenBuf[:2], uint16(len(key)))
		kvBuf.Write(lenBuf[:2])
		kvBuf.Write(key)
		binary.LittleEndian.PutUint32(lenBuf[:4], uint32(len(v)))
		kvBuf.Write(lenBuf[:4])
		kvBuf.Write(v)
	}

	kvData := kvBuf.Bytes()

	// Calculate how many overflow pages we need
	overflowCount := uint32(0)
	rootHeaderSize := 8

	rootDataSpace := usablePageSize - rootHeaderSize
	if rootDataSpace < 0 {
		rootDataSpace = 0
	}

	if len(kvData) > rootDataSpace {
		remaining := len(kvData) - rootDataSpace
		overflowCount = uint32((remaining + usablePageSize - 1) / usablePageSize)

		for {
			rootHeaderSize = 8 + 4*int(overflowCount)
			rootDataSpace = usablePageSize - rootHeaderSize
			if rootDataSpace < 0 {
				rootDataSpace = 0
			}
			remaining = len(kvData) - rootDataSpace
			if remaining <= 0 {
				overflowCount = 0
				break
			}
			needed := uint32((remaining + usablePageSize - 1) / usablePageSize)
			if needed <= overflowCount {
				overflowCount = needed
				break
			}
			overflowCount = needed
		}
	}

	// Release extra overflow pages
	for len(t.overflowPages) > int(overflowCount) {
		t.overflowPages = t.overflowPages[:len(t.overflowPages)-1]
	}

	// Allocate new overflow pages if needed
	for len(t.overflowPages) < int(overflowCount) {
		newPage, err := t.pool.NewPage(storage.PageTypeLeaf)
		if err != nil {
			return fmt.Errorf("failed to allocate overflow page: %w", err)
		}
		t.overflowPages = append(t.overflowPages, newPage.ID())
		t.pool.Unpin(newPage)
	}

	// Write root page
	root, err := t.pool.GetPage(t.rootPageID)
	if err != nil {
		return err
	}
	defer t.pool.Unpin(root)

	rootBuf := root.Data()[storage.PageHeaderSize:]
	for i := range rootBuf {
		rootBuf[i] = 0
	}

	binary.LittleEndian.PutUint32(rootBuf[0:4], count)
	binary.LittleEndian.PutUint32(rootBuf[4:8], overflowCount)
	for i, pgID := range t.overflowPages {
		off := 8 + 4*i
		if off+4 > len(rootBuf) {
			break
		}
		binary.LittleEndian.PutUint32(rootBuf[off:off+4], pgID)
	}

	rootHeaderSize = 8 + 4*int(overflowCount)
	// Cap header size to prevent overflow - only so many page IDs fit in root
	if rootHeaderSize > usablePageSize {
		rootHeaderSize = usablePageSize
	}
	rootDataSpace = usablePageSize - rootHeaderSize
	dataWritten := 0

	writeLen := rootDataSpace
	if writeLen > len(kvData) {
		writeLen = len(kvData)
	}
	copy(rootBuf[rootHeaderSize:], kvData[:writeLen])
	dataWritten += writeLen
	root.SetDirty(true)

	for _, pgID := range t.overflowPages {
		if dataWritten >= len(kvData) {
			break
		}
		pg, err := t.pool.GetPage(pgID)
		if err != nil {
			return fmt.Errorf("failed to get overflow page %d: %w", pgID, err)
		}
		pgBuf := pg.Data()[storage.PageHeaderSize:]
		for i := range pgBuf {
			pgBuf[i] = 0
		}
		writeLen = usablePageSize
		remaining := len(kvData) - dataWritten
		if writeLen > remaining {
			writeLen = remaining
		}
		copy(pgBuf, kvData[dataWritten:dataWritten+writeLen])
		dataWritten += writeLen
		pg.SetDirty(true)
		t.pool.Unpin(pg)
	}

	t.dirty = false
	return nil
}

// Delete removes a key from the tree
func (t *BTree) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	keyStr := string(key)
	val, ok := t.memStorage[keyStr]
	if ok {
		// Update memory tracking
		t.memoryUsed -= int64(len(keyStr) + len(val))

		// Remove from LRU
		t.lruMu.Lock()
		if entry, ok := t.lruMap[keyStr]; ok {
			t.lruList.Remove(entry.elem)
			delete(t.lruMap, keyStr)
		}
		t.lruMu.Unlock()

		delete(t.memStorage, keyStr)
		t.dirty = true
		return nil
	}

	// Check if key was evicted to disk
	if t.evictedKeys[keyStr] {
		delete(t.evictedKeys, keyStr)
		t.dirty = true
		return nil
	}

	return ErrKeyNotFound
}

// Iterator provides range scan capability
type Iterator struct {
	tree   *BTree
	keys   [][]byte
	values [][]byte
	idx    int
	endKey []byte
	done   bool
}

// Scan returns an iterator for range scanning
func (t *BTree) Scan(startKey, endKey []byte) (*Iterator, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	seen := make(map[string]bool)
	var keys, values [][]byte
	for k, v := range t.memStorage {
		kb := []byte(k)

		if startKey != nil && bytes.Compare(kb, startKey) < 0 {
			continue
		}
		if endKey != nil && bytes.Compare(kb, endKey) > 0 {
			continue
		}

		keyCopy := make([]byte, len(kb))
		copy(keyCopy, kb)
		valCopy := make([]byte, len(v))
		copy(valCopy, v)

		keys = append(keys, keyCopy)
		values = append(values, valCopy)
		seen[k] = true
	}

	// Include evicted entries from disk
	if len(t.evictedKeys) > 0 {
		diskData := t.readKVFromPages()
		for k, v := range diskData {
			if !t.evictedKeys[k] || seen[k] {
				continue
			}
			kb := []byte(k)
			if startKey != nil && bytes.Compare(kb, startKey) < 0 {
				continue
			}
			if endKey != nil && bytes.Compare(kb, endKey) > 0 {
				continue
			}
			keyCopy := make([]byte, len(kb))
			copy(keyCopy, kb)
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			keys = append(keys, keyCopy)
			values = append(values, valCopy)
		}
	}

	// Sort keys using standard library sort (faster than bubble sort)
	sortKeyValues(keys, values)

	return &Iterator{
		tree:   t,
		keys:   keys,
		values: values,
		idx:    0,
		endKey: endKey,
		done:   false,
	}, nil
}

// sortKeyValues sorts keys and values together by key
func sortKeyValues(keys [][]byte, values [][]byte) {
	sort.Sort(&kvSorter{keys: keys, values: values})
}

type kvSorter struct {
	keys   [][]byte
	values [][]byte
}

func (s *kvSorter) Len() int           { return len(s.keys) }
func (s *kvSorter) Less(i, j int) bool { return bytes.Compare(s.keys[i], s.keys[j]) < 0 }
func (s *kvSorter) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.values[i], s.values[j] = s.values[j], s.values[i]
}

// Next advances the iterator
func (it *Iterator) Next() ([]byte, []byte, error) {
	if it.done || it.idx >= len(it.keys) {
		it.done = true
		return nil, nil, nil
	}

	key := it.keys[it.idx]
	value := it.values[it.idx]
	it.idx++

	if it.endKey != nil && bytes.Compare(key, it.endKey) > 0 {
		it.done = true
		return nil, nil, nil
	}

	return key, value, nil
}

// Valid returns true if the iterator has more items
func (it *Iterator) Valid() bool {
	return !it.done && it.idx < len(it.keys)
}

// Close closes the iterator
func (it *Iterator) Close() {
	it.done = true
}

// HasNext returns true if there are more items to iterate
func (it *Iterator) HasNext() bool {
	return it.Valid()
}

// First positions the iterator at the first item
func (it *Iterator) First() bool {
	if len(it.keys) == 0 {
		it.done = true
		return false
	}
	it.idx = 0
	return true
}

// Size returns the number of keys in the tree (including evicted keys on disk)
func (t *BTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.memStorage) + len(t.evictedKeys)
}

// Flush writes all in-memory data to disk pages (with multi-page overflow support)
func (t *BTree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushInternal()
}

// Cell represents a key-value pair in a leaf node (kept for compatibility)
type Cell struct {
	KeySize   uint16
	ValueSize uint32
	Key       []byte
	Value     []byte
}

// InternalCell represents a key and child pointer in an internal node (kept for compatibility)
type InternalCell struct {
	KeySize     uint16
	Key         []byte
	ChildPageID uint32
}
