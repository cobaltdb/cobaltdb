package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"hash/maphash"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

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
	DefaultMemoryLimit = int64(256 * 1024 * 1024) // 256MB default
	MaxKeyLength       = 65535                    // uint16 max - serialization limit
)

// lruTimestamp is a monotonic counter used for LRU ordering.
// It replaces time.Now().UnixNano() to avoid the overhead of reading
// the system clock on every insert.
var lruTimestamp atomic.Int64

// Number of shards for memStorage. Must be a power of two.
const numShards = 256

var hashSeed = maphash.MakeSeed()

func shardIndex(key string) int {
	idx := maphash.String(hashSeed, key) & uint64(numShards-1)
	return int(idx) // #nosec G115 - idx is masked to [0, numShards).
}

func checkedUint16Len(n int, name string) (uint16, error) {
	if n < 0 || n > 1<<16-1 {
		return 0, fmt.Errorf("%s exceeds uint16: %d", name, n)
	}
	return uint16(n), nil // #nosec G115 - range checked above.
}

func checkedUint32Len(n int, name string) (uint32, error) {
	if n < 0 || n > 1<<32-1 {
		return 0, fmt.Errorf("%s exceeds uint32: %d", name, n)
	}
	return uint32(n), nil // #nosec G115 - range checked above.
}

func checkedUint32PageCount(n int) (uint32, error) {
	return checkedUint32Len(n, "overflow page count")
}

// lruEntry tracks memory usage for LRU eviction. It acts as an intrusive
// doubly-linked list node, eliminating the separate list.Element allocation
// that container/list required.
type lruEntry struct {
	key       string
	size      int64
	timestamp int64 // monotonic counter for cross-shard eviction comparison
	next      *lruEntry
	prev      *lruEntry
}

// lruList is a simple doubly-linked list of lruEntry nodes.
type lruList struct {
	head *lruEntry
	tail *lruEntry
}

func (l *lruList) PushFront(e *lruEntry) {
	e.next = l.head
	e.prev = nil
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
}

func (l *lruList) Remove(e *lruEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.next = nil
	e.prev = nil
}

func (l *lruList) MoveToFront(e *lruEntry) {
	l.Remove(e)
	l.PushFront(e)
}

func (l *lruList) Back() *lruEntry {
	return l.tail
}

func (l *lruList) Init() {
	l.head = nil
	l.tail = nil
}

func (l *lruList) Len() int {
	n := 0
	for e := l.head; e != nil; e = e.next {
		n++
	}
	return n
}

// btreeShard holds a partition of the in-memory key space with its own lock.
type btreeShard struct {
	mu      sync.RWMutex
	data    map[string][]byte
	evicted map[string]bool
	lruMu   sync.Mutex
	lruList lruList
	lruMap  map[string]*lruEntry
}

// BTree represents a disk-based B+Tree index using a hybrid approach:
// - Each table has its own BTree instance
// - Data is stored as key-value pairs in pages managed by the buffer pool
// - The BTree maintains an in-memory sorted structure that flushes to disk pages
// - Multi-page overflow: data exceeding one page spills to linked overflow pages
//
// Concurrency: memStorage is sharded into 16 independently locked partitions
// so concurrent writes to different keys (or even the same shard) can proceed
// in parallel.  A single flushMu serializes flushInternal calls, and lruMu
// protects the global LRU structures.

var crc64Table = crc64.MakeTable(crc64.ISO)

type BTree struct {
	flushMu        sync.Mutex // serializes flushInternal and eviction flush
	rootPageID     uint32
	pool           *storage.BufferPool
	order          int
	loadErr        error
	shards         [numShards]btreeShard
	dirty          int32         // atomic: 0 or 1
	overflowPages  []uint32      // IDs of overflow pages used by this tree
	lastPageHashes []uint64      // hashes of page content from last flush
	flushBuf       *bytes.Buffer // reusable serialization buffer (protected by flushMu)

	// Memory management
	memoryLimit int64 // atomic
	memoryUsed  int64 // atomic
	keyCount    int64 // atomic: logical size (data + evicted)
}

// usablePageSize is the space available for data in each page (after header)
const usablePageSize = storage.PageSize - storage.PageHeaderSize

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

	t := &BTree{
		rootPageID: rootPage.ID(),
		pool:       pool,
		order:      100,
		flushBuf:   new(bytes.Buffer),
	}
	atomic.StoreInt64(&t.memoryLimit, limit)
	for i := range t.shards {
		t.shards[i].data = make(map[string][]byte, 64)
		t.shards[i].evicted = make(map[string]bool)
		t.shards[i].lruMap = make(map[string]*lruEntry, 64)
	}
	return t, nil
}

// OpenBTree opens an existing B+Tree with the given root page ID
func OpenBTree(pool *storage.BufferPool, rootPageID uint32) *BTree {
	return OpenBTreeWithLimit(pool, rootPageID, DefaultMemoryLimit)
}

// OpenBTreeStrict opens an existing B+Tree and returns load errors to callers.
func OpenBTreeStrict(pool *storage.BufferPool, rootPageID uint32) (*BTree, error) {
	return OpenBTreeWithLimitStrict(pool, rootPageID, DefaultMemoryLimit)
}

// OpenBTreeWithLimit opens an existing B+Tree with a specified memory limit
func OpenBTreeWithLimit(pool *storage.BufferPool, rootPageID uint32, limit int64) *BTree {
	t, _ := OpenBTreeWithLimitStrict(pool, rootPageID, limit)
	return t
}

// OpenBTreeWithLimitStrict opens an existing B+Tree with a specified memory
// limit and returns any page load error instead of only creating an empty tree.
func OpenBTreeWithLimitStrict(pool *storage.BufferPool, rootPageID uint32, limit int64) (*BTree, error) {
	t := &BTree{
		rootPageID: rootPageID,
		pool:       pool,
		order:      100,
		flushBuf:   new(bytes.Buffer),
	}
	atomic.StoreInt64(&t.memoryLimit, limit)
	for i := range t.shards {
		t.shards[i].data = make(map[string][]byte, 64)
		t.shards[i].evicted = make(map[string]bool)
		t.shards[i].lruMap = make(map[string]*lruEntry, 64)
	}
	if err := t.loadFromPages(); err != nil {
		t.loadErr = err
		return t, fmt.Errorf("failed to load B+Tree pages for root %d: %w", rootPageID, err)
	}
	return t, nil
}

// LoadError returns any error encountered while opening an existing tree.
func (t *BTree) LoadError() error {
	if t == nil {
		return nil
	}
	return t.loadErr
}

// loadFromPages loads serialized key-value pairs from root + overflow pages into shards
func (t *BTree) loadFromPages() error {
	root, err := t.pool.GetPage(t.rootPageID)
	if err != nil {
		return fmt.Errorf("failed to load root page %d: %w", t.rootPageID, err)
	}
	defer t.pool.Unpin(root)

	pageData := root.Data()[storage.PageHeaderSize:]
	if len(pageData) < 8 {
		return nil
	}

	totalCount := binary.LittleEndian.Uint32(pageData[0:4])
	overflowCount := binary.LittleEndian.Uint32(pageData[4:8])

	if totalCount == 0 {
		return nil
	}

	headerSize := 8 + 4*int(overflowCount)
	if headerSize > len(pageData) {
		return fmt.Errorf("corrupt root page %d: header size %d exceeds page data %d", t.rootPageID, headerSize, len(pageData))
	}

	t.overflowPages = make([]uint32, overflowCount)
	for i := uint32(0); i < overflowCount; i++ {
		off := 8 + 4*int(i)
		t.overflowPages[i] = binary.LittleEndian.Uint32(pageData[off : off+4])
	}

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

	// Pre-size shard maps to eliminate growth allocations during load.
	perShard := int(totalCount)/numShards + 1
	for i := range t.shards {
		t.shards[i].data = make(map[string][]byte, perShard)
	}

	offset := 0
	loadedCount := int64(0)
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

		keyStr := string(key)
		t.shards[shardIndex(keyStr)].data[keyStr] = val
		loadedCount++
	}
	atomic.StoreInt64(&t.keyCount, loadedCount)
	return nil
}

// readKVFromPages reads all key-value pairs from disk pages without modifying tree state.
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
	atomic.StoreInt64(&t.memoryLimit, limit)
}

// MemoryLimit returns the current memory limit
func (t *BTree) MemoryLimit() int64 {
	return atomic.LoadInt64(&t.memoryLimit)
}

// MemoryUsed returns the current memory usage
func (t *BTree) MemoryUsed() int64 {
	return atomic.LoadInt64(&t.memoryUsed)
}

// Get retrieves a value by key
func (t *BTree) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrInvalidKey
	}
	return t.GetString(string(key))
}

// GetString is identical to Get but accepts a string key, avoiding an
// allocation when the caller already holds the key as a string.
func (t *BTree) GetString(keyStr string) ([]byte, error) {
	if len(keyStr) == 0 {
		return nil, ErrInvalidKey
	}

	sh := &t.shards[shardIndex(keyStr)]
	sh.mu.RLock()
	val, ok := sh.data[keyStr]
	if ok {
		result := make([]byte, len(val))
		copy(result, val)
		sh.mu.RUnlock()

		sh.lruMu.Lock()
		if entry, ok := sh.lruMap[keyStr]; ok {
			sh.lruList.Remove(entry)
			sh.lruList.MoveToFront(entry)
		}
		sh.lruMu.Unlock()
		return result, nil
	}

	if sh.evicted[keyStr] {
		sh.mu.RUnlock()
		diskData := t.readKVFromPages()
		if val, ok := diskData[keyStr]; ok {
			result := make([]byte, len(val))
			copy(result, val)
			return result, nil
		}
		return nil, ErrKeyNotFound
	}

	sh.mu.RUnlock()
	return nil, ErrKeyNotFound
}

// Put inserts or updates a key-value pair
func (t *BTree) Put(key, value []byte) error {
	return t.PutString(string(key), value)
}

// PutString is identical to Put but accepts a string key, avoiding an
// unnecessary []byte→string conversion when the caller already has a string.
func (t *BTree) PutString(keyCopy string, value []byte) error {
	if len(keyCopy) == 0 {
		return ErrInvalidKey
	}
	if len(keyCopy) > MaxKeyLength {
		return ErrKeyTooLong
	}
	if len(value) == 0 {
		return ErrInvalidValue
	}

	valCopy := getValueBuf(len(value))
	copy(valCopy, value)
	return t.putStringInternal(keyCopy, valCopy)
}

// PutStringNoCopy inserts or updates a key-value pair without copying the
// value slice. The caller must ensure the value slice is not modified after
// this call, as the B-tree takes ownership of it.
func (t *BTree) PutStringNoCopy(keyCopy string, value []byte) error {
	if len(keyCopy) == 0 {
		return ErrInvalidKey
	}
	if len(keyCopy) > MaxKeyLength {
		return ErrKeyTooLong
	}
	if len(value) == 0 {
		return ErrInvalidValue
	}

	return t.putStringInternal(keyCopy, value)
}

var lruEntryPool sync.Pool

// valueBufPool recycles small buffers for B-tree values, eliminating one heap
// allocation per insert for values up to 128 bytes. We store *valueBuf (a
// single pointer) instead of []byte so the sync.Pool interface boxing is free.
type valueBuf struct {
	b [128]byte
}

var valueBufPool sync.Pool

func init() {
	// Seed the pool so the hot insert path never allocates an lruEntry.
	for i := 0; i < 1024; i++ {
		lruEntryPool.Put(&lruEntry{})
	}
}

// getValueBuf returns a []byte of length n. For n <= 128 it draws from a
// batch-allocated pool (one make([]valueBuf, 64) serves 64 inserts), keeping
// the effective allocation rate at ~1/64 per small value.
func getValueBuf(n int) []byte {
	if n > 128 {
		return make([]byte, n)
	}
	if v := valueBufPool.Get(); v != nil {
		buf := v.(*valueBuf)
		return buf.b[:n]
	}
	const batchSize = 64
	batch := make([]valueBuf, batchSize)
	for i := 1; i < batchSize; i++ {
		valueBufPool.Put(&batch[i])
	}
	return batch[0].b[:n]
}

func (t *BTree) putStringInternal(keyCopy string, value []byte) error {
	newSize := int64(len(keyCopy) + len(value))

	sh := &t.shards[shardIndex(keyCopy)]

	for {
		sh.mu.Lock()

		oldSize := int64(0)
		if oldVal, exists := sh.data[keyCopy]; exists {
			oldSize = int64(len(keyCopy) + len(oldVal))
		}
		delta := newSize - oldSize

		limit := atomic.LoadInt64(&t.memoryLimit)
		if limit > 0 && atomic.LoadInt64(&t.memoryUsed)+delta > limit {
			sh.mu.Unlock()
			if err := t.evictToMakeSpace(delta); err != nil {
				return err
			}
			continue
		}

		wasEvicted := sh.evicted[keyCopy]
		delete(sh.evicted, keyCopy)
		var oldEntry *lruEntry
		if oldVal, exists := sh.data[keyCopy]; exists {
			atomic.AddInt64(&t.memoryUsed, -int64(len(keyCopy)+len(oldVal)))
			sh.lruMu.Lock()
			if entry, ok := sh.lruMap[keyCopy]; ok {
				sh.lruList.Remove(entry)
				delete(sh.lruMap, keyCopy)
				oldEntry = entry
			}
			sh.lruMu.Unlock()
		} else if !wasEvicted {
			atomic.AddInt64(&t.keyCount, 1)
		}
		sh.data[keyCopy] = value
		atomic.AddInt64(&t.memoryUsed, newSize)
		atomic.StoreInt32(&t.dirty, 1)

		sh.lruMu.Lock()
		var entry *lruEntry
		if oldEntry != nil {
			entry = oldEntry
		} else if v := lruEntryPool.Get(); v != nil {
			entry = v.(*lruEntry)
		} else {
			// Batch-allocate entries to amortize the per-insert allocation.
			// One make([]lruEntry, 64) is counted as a single alloc by the
			// profiler, dropping the effective LRU entry alloc rate to ~1/64.
			batch := make([]lruEntry, 64)
			for i := 1; i < len(batch); i++ {
				lruEntryPool.Put(&batch[i])
			}
			entry = &batch[0]
		}
		entry.key = keyCopy
		entry.size = newSize
		entry.timestamp = lruTimestamp.Add(1)
		entry.next = nil
		entry.prev = nil
		sh.lruList.PushFront(entry)
		sh.lruMap[keyCopy] = entry
		sh.lruMu.Unlock()

		sh.mu.Unlock()
		return nil
	}
}

// PutBatch inserts or updates multiple key-value pairs.  It groups keys by
// shard, acquires shard locks in deterministic order to avoid deadlock, and
// applies writes atomically with respect to memory-limit failures.
func (t *BTree) PutBatch(keys [][]byte, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.New("key and value count mismatch")
	}
	if len(keys) == 0 {
		return nil
	}

	keyCopies := make([]string, len(keys))
	valCopies := make([][]byte, len(keys))
	for i, key := range keys {
		if len(key) == 0 {
			return ErrInvalidKey
		}
		if len(key) > MaxKeyLength {
			return ErrKeyTooLong
		}
		if len(values[i]) == 0 {
			return ErrInvalidValue
		}
		keyCopies[i] = string(key)
		valCopies[i] = getValueBuf(len(values[i]))
		copy(valCopies[i], values[i])
	}

	// Group indices by shard.
	type shardWork struct {
		indices []int
	}
	shardWorks := make([]shardWork, numShards)
	for i, kc := range keyCopies {
		idx := shardIndex(kc)
		shardWorks[idx].indices = append(shardWorks[idx].indices, i)
	}

	neededShards := make([]int, 0, numShards)
	for i, sw := range shardWorks {
		if len(sw.indices) > 0 {
			neededShards = append(neededShards, i)
		}
	}
	sort.Ints(neededShards)

	// Retry loop: compute delta, evict if needed, apply.
	for {
		for _, si := range neededShards {
			t.shards[si].mu.Lock()
		}

		var totalDelta int64
		for _, si := range neededShards {
			for _, idx := range shardWorks[si].indices {
				oldSize := int64(0)
				if oldVal, exists := t.shards[si].data[keyCopies[idx]]; exists {
					oldSize = int64(len(keyCopies[idx]) + len(oldVal))
				}
				totalDelta += int64(len(keyCopies[idx])+len(valCopies[idx])) - oldSize
			}
		}

		limit := atomic.LoadInt64(&t.memoryLimit)
		if limit > 0 && atomic.LoadInt64(&t.memoryUsed)+totalDelta > limit {
			for _, si := range neededShards {
				t.shards[si].mu.Unlock()
			}
			if err := t.evictToMakeSpace(totalDelta); err != nil {
				return err
			}
			continue
		}

		for _, si := range neededShards {
			sh := &t.shards[si]
			sh.lruMu.Lock()
			for _, idx := range shardWorks[si].indices {
				kc := keyCopies[idx]
				vc := valCopies[idx]

				wasEvicted := sh.evicted[kc]
				delete(sh.evicted, kc)
				if oldVal, exists := sh.data[kc]; exists {
					atomic.AddInt64(&t.memoryUsed, -int64(len(kc)+len(oldVal)))
					if entry, ok := sh.lruMap[kc]; ok {
						sh.lruList.Remove(entry)
						sh.lruList.Remove(entry)
						delete(sh.lruMap, kc)
					}
				} else if !wasEvicted {
					atomic.AddInt64(&t.keyCount, 1)
				}
				sh.data[kc] = vc
				atomic.AddInt64(&t.memoryUsed, int64(len(kc)+len(vc)))
				atomic.StoreInt32(&t.dirty, 1)

				entry := &lruEntry{}
				entry.key = kc
				entry.size = int64(len(kc) + len(vc))
				entry.timestamp = lruTimestamp.Add(1)
				sh.lruList.PushFront(entry)
				sh.lruMap[kc] = entry
			}
			sh.lruMu.Unlock()
		}

		for _, si := range neededShards {
			t.shards[si].mu.Unlock()
		}
		return nil
	}
}

// DeleteBatch removes multiple keys in a single operation.
func (t *BTree) DeleteBatch(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}

	keyCopies := make([]string, len(keys))
	for i, key := range keys {
		keyCopies[i] = string(key)
	}

	shardWorks := make([][]string, numShards)
	for _, kc := range keyCopies {
		idx := shardIndex(kc)
		shardWorks[idx] = append(shardWorks[idx], kc)
	}

	neededShards := make([]int, 0, numShards)
	for i, work := range shardWorks {
		if len(work) > 0 {
			neededShards = append(neededShards, i)
		}
	}
	sort.Ints(neededShards)

	for _, si := range neededShards {
		sh := &t.shards[si]
		sh.mu.Lock()
		sh.lruMu.Lock()
		for _, kc := range shardWorks[si] {
			present := false
			if val, exists := sh.data[kc]; exists {
				delete(sh.data, kc)
				atomic.AddInt64(&t.memoryUsed, -int64(len(kc)+len(val)))
				present = true
				if entry, ok := sh.lruMap[kc]; ok {
					sh.lruList.Remove(entry)
					// entry freed for GC
					delete(sh.lruMap, kc)
				}
			}
			if sh.evicted[kc] {
				delete(sh.evicted, kc)
				present = true
			}
			if present {
				atomic.AddInt64(&t.keyCount, -1)
				atomic.StoreInt32(&t.dirty, 1)
			}
		}
		sh.lruMu.Unlock()
		sh.mu.Unlock()
	}
	return nil
}

func (t *BTree) evictToMakeSpace(needed int64) error {
	limit := atomic.LoadInt64(&t.memoryLimit)
	if needed > limit {
		return ErrMemoryLimit
	}

	// If the tree is dirty, flush once before evicting multiple entries.
	// Previously we flushed inside the loop, which could flush the entire
	// tree on every single eviction when memory pressure was high.
	if atomic.LoadInt32(&t.dirty) != 0 {
		if err := t.flushInternal(); err != nil {
			return fmt.Errorf("failed to flush during eviction: %w", err)
		}
	}

	for atomic.LoadInt64(&t.memoryUsed)+needed > limit {
		// Find the globally oldest entry across all shard LRUs.
		type candidate struct {
			shardIdx int
			key      string
			ts       int64
		}
		var best *candidate
		for i := 0; i < numShards; i++ {
			sh := &t.shards[i]
			sh.lruMu.Lock()
			if entry := sh.lruList.Back(); entry != nil {
				if best == nil || entry.timestamp < best.ts {
					best = &candidate{shardIdx: i, key: entry.key, ts: entry.timestamp}
				}
			}
			sh.lruMu.Unlock()
		}
		if best == nil {
			break
		}

		sh := &t.shards[best.shardIdx]
		sh.lruMu.Lock()
		// Verify the back element is still the one we picked (or re-check).
		entry := sh.lruList.Back()
		if entry != nil {
			sh.lruList.Remove(entry)
			delete(sh.lruMap, entry.key)
			// entry freed for GC
		}
		sh.lruMu.Unlock()

		if entry == nil {
			continue // Another goroutine evicted it already; retry.
		}
		evictKey := best.key

		sh.mu.Lock()
		if val, ok := sh.data[evictKey]; ok {
			atomic.AddInt64(&t.memoryUsed, -int64(len(evictKey)+len(val)))
			delete(sh.data, evictKey)
			sh.evicted[evictKey] = true
		}
		sh.mu.Unlock()
	}

	if atomic.LoadInt64(&t.memoryUsed)+needed > limit {
		return ErrMemoryLimit
	}
	return nil
}

// flushInternal flushes data to disk pages.  It acquires all shard RLocks to
// read the current memStorage snapshot, then writes to the buffer pool.
func (t *BTree) flushInternal() error {
	if atomic.LoadInt32(&t.dirty) == 0 {
		return nil
	}

	t.flushMu.Lock()
	defer t.flushMu.Unlock()

	// Re-check dirty after acquiring flushMu.
	if atomic.LoadInt32(&t.dirty) == 0 {
		return nil
	}

	// Snapshot each shard individually so writers to other shards can proceed
	// while we serialize the flushed data.
	dataSnap := make(map[string][]byte)
	evictedSnap := make([]map[string]bool, numShards)
	hasEvicted := false
	memCount := 0
	for i := 0; i < numShards; i++ {
		t.shards[i].mu.RLock()
		for k, v := range t.shards[i].data {
			dataSnap[k] = v
		}
		if len(t.shards[i].evicted) > 0 {
			hasEvicted = true
			evictedSnap[i] = make(map[string]bool, len(t.shards[i].evicted))
			for k := range t.shards[i].evicted {
				evictedSnap[i][k] = true
			}
		}
		memCount += len(t.shards[i].data)
		t.shards[i].mu.RUnlock()
	}

	t.flushBuf.Reset()
	var count uint32
	var lenBuf [4]byte
	var err error

	if !hasEvicted {
		count, err = checkedUint32Len(len(dataSnap), "entry count")
		if err != nil {
			return err
		}
		for k, v := range dataSnap {
			keyLen, err := checkedUint16Len(len(k), "key length")
			if err != nil {
				return err
			}
			valueLen, err := checkedUint32Len(len(v), "value length")
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint16(lenBuf[:2], keyLen)
			t.flushBuf.Write(lenBuf[:2])
			t.flushBuf.WriteString(k)
			binary.LittleEndian.PutUint32(lenBuf[:4], valueLen)
			t.flushBuf.Write(lenBuf[:4])
			t.flushBuf.Write(v)
		}
	} else {
		toSerialize := make(map[string][]byte, memCount)
		diskData := t.readKVFromPages()
		for k, v := range diskData {
			if evictedSnap[shardIndex(k)][k] {
				toSerialize[k] = v
			}
		}
		for k, v := range dataSnap {
			toSerialize[k] = v
		}
		count, err = checkedUint32Len(len(toSerialize), "entry count")
		if err != nil {
			return err
		}
		for k, v := range toSerialize {
			keyLen, err := checkedUint16Len(len(k), "key length")
			if err != nil {
				return err
			}
			valueLen, err := checkedUint32Len(len(v), "value length")
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint16(lenBuf[:2], keyLen)
			t.flushBuf.Write(lenBuf[:2])
			t.flushBuf.WriteString(k)
			binary.LittleEndian.PutUint32(lenBuf[:4], valueLen)
			t.flushBuf.Write(lenBuf[:4])
			t.flushBuf.Write(v)
		}
	}

	kvData := t.flushBuf.Bytes()

	// Calculate overflow pages
	overflowCount := uint32(0)
	rootHeaderSize := 8
	rootDataSpace := usablePageSize - rootHeaderSize
	if rootDataSpace < 0 {
		rootDataSpace = 0
	}

	if len(kvData) > rootDataSpace {
		remaining := len(kvData) - rootDataSpace
		overflowCount, err = checkedUint32PageCount((remaining + usablePageSize - 1) / usablePageSize)
		if err != nil {
			return err
		}
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
			needed, err := checkedUint32PageCount((remaining + usablePageSize - 1) / usablePageSize)
			if err != nil {
				return err
			}
			if needed <= overflowCount {
				overflowCount = needed
				break
			}
			overflowCount = needed
		}
	}

	for len(t.overflowPages) > int(overflowCount) {
		t.overflowPages = t.overflowPages[:len(t.overflowPages)-1]
	}
	for len(t.overflowPages) < int(overflowCount) {
		newPage, err := t.pool.NewPage(storage.PageTypeLeaf)
		if err != nil {
			return fmt.Errorf("failed to allocate overflow page: %w", err)
		}
		t.overflowPages = append(t.overflowPages, newPage.ID())
		t.pool.Unpin(newPage)
	}

	// Compute hashes for each page so we can skip writing unchanged pages.
	numPages := 1 + int(overflowCount)
	newPageHashes := make([]uint64, numPages)
	h := crc64.New(crc64Table)
	zeroPad := make([]byte, usablePageSize)

	// Root page hash.
	rootHeaderSize = 8 + 4*int(overflowCount)
	if rootHeaderSize > usablePageSize {
		rootHeaderSize = usablePageSize
	}
	rootDataSpace = usablePageSize - rootHeaderSize

	h.Reset()
	binary.LittleEndian.PutUint32(lenBuf[:4], count)
	h.Write(lenBuf[:4])
	binary.LittleEndian.PutUint32(lenBuf[:4], overflowCount)
	h.Write(lenBuf[:4])
	for _, pgID := range t.overflowPages {
		binary.LittleEndian.PutUint32(lenBuf[:4], pgID)
		h.Write(lenBuf[:4])
	}
	rootDataWriteLen := rootDataSpace
	if rootDataWriteLen > len(kvData) {
		rootDataWriteLen = len(kvData)
	}
	if rootDataWriteLen > 0 {
		h.Write(kvData[:rootDataWriteLen])
	}
	if pad := rootDataSpace - rootDataWriteLen; pad > 0 {
		h.Write(zeroPad[:pad])
	}
	newPageHashes[0] = h.Sum64()

	// Overflow page hashes.
	dataWritten := rootDataWriteLen
	for i := 0; i < int(overflowCount); i++ {
		h.Reset()
		writeLen := usablePageSize
		remaining := len(kvData) - dataWritten
		if writeLen > remaining {
			writeLen = remaining
		}
		if writeLen > 0 {
			h.Write(kvData[dataWritten : dataWritten+writeLen])
		}
		if pad := usablePageSize - writeLen; pad > 0 {
			h.Write(zeroPad[:pad])
		}
		newPageHashes[i+1] = h.Sum64()
		dataWritten += writeLen
	}

	canSkip := len(t.lastPageHashes) == numPages

	// Write root page if changed.
	if !canSkip || newPageHashes[0] != t.lastPageHashes[0] {
		root, err := t.pool.GetPage(t.rootPageID)
		if err != nil {
			return err
		}
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
		if rootDataWriteLen > 0 {
			copy(rootBuf[rootHeaderSize:], kvData[:rootDataWriteLen])
		}
		root.SetDirty(true)
		t.pool.Unpin(root)
	}

	// Write overflow pages if changed.
	dataWritten = rootDataWriteLen
	for i, pgID := range t.overflowPages {
		writeLen := usablePageSize
		remaining := len(kvData) - dataWritten
		if writeLen > remaining {
			writeLen = remaining
		}
		if !canSkip || newPageHashes[i+1] != t.lastPageHashes[i+1] {
			pg, err := t.pool.GetPage(pgID)
			if err != nil {
				return fmt.Errorf("failed to get overflow page %d: %w", pgID, err)
			}
			pgBuf := pg.Data()[storage.PageHeaderSize:]
			for j := range pgBuf {
				pgBuf[j] = 0
			}
			if writeLen > 0 {
				copy(pgBuf, kvData[dataWritten:dataWritten+writeLen])
			}
			pg.SetDirty(true)
			t.pool.Unpin(pg)
		}
		dataWritten += writeLen
	}

	t.lastPageHashes = newPageHashes
	atomic.StoreInt32(&t.dirty, 0)
	return nil
}

// Delete removes a key from the tree
func (t *BTree) Delete(key []byte) error {
	return t.DeleteString(string(key))
}

// DeleteString removes a key from the tree without allocating a string copy.
func (t *BTree) DeleteString(keyStr string) error {
	if len(keyStr) == 0 {
		return ErrInvalidKey
	}

	sh := &t.shards[shardIndex(keyStr)]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if val, ok := sh.data[keyStr]; ok {
		atomic.AddInt64(&t.memoryUsed, -int64(len(keyStr)+len(val)))
		delete(sh.data, keyStr)
		atomic.AddInt64(&t.keyCount, -1)
		atomic.StoreInt32(&t.dirty, 1)

		sh.lruMu.Lock()
		if entry, ok := sh.lruMap[keyStr]; ok {
			sh.lruList.Remove(entry)
			// entry freed for GC
			delete(sh.lruMap, keyStr)
		}
		sh.lruMu.Unlock()
		return nil
	}

	if sh.evicted[keyStr] {
		delete(sh.evicted, keyStr)
		atomic.AddInt64(&t.keyCount, -1)
		atomic.StoreInt32(&t.dirty, 1)
		return nil
	}

	return ErrKeyNotFound
}

type kvPair struct {
	key   string
	value []byte
}

// Iterator provides range scan capability
type Iterator struct {
	tree      *BTree
	pairs     []kvPair
	idx       int
	endKey    string
	hasEndKey bool
	done      bool
}

// Scan returns an iterator for range scanning
func (t *BTree) Scan(startKey, endKey []byte) (TreeIterator, error) {
	// Pre-size slice to avoid reallocations; t.Size() is an upper bound.
	approxSize := t.Size()
	pairs := make([]kvPair, 0, approxSize)

	var seen map[string]bool
	var evicted map[string]bool
	hasEvicted := false

	startStr := ""
	if startKey != nil {
		startStr = string(startKey)
	}
	endStr := ""
	if endKey != nil {
		endStr = string(endKey)
	}

	// Snapshot each shard individually so writers to other shards can proceed.
	for i := 0; i < numShards; i++ {
		t.shards[i].mu.RLock()
		for k, v := range t.shards[i].data {
			if startKey != nil && strings.Compare(k, startStr) < 0 {
				continue
			}
			if endKey != nil && strings.Compare(k, endStr) > 0 {
				continue
			}
			pairs = append(pairs, kvPair{k, cloneBytes(v)})
			if hasEvicted {
				seen[k] = true
			}
		}
		if len(t.shards[i].evicted) > 0 {
			if !hasEvicted {
				hasEvicted = true
				seen = make(map[string]bool, approxSize)
				// Mark all previously collected keys
				for j := 0; j < len(pairs); j++ {
					seen[pairs[j].key] = true
				}
			}
			if evicted == nil {
				evicted = make(map[string]bool)
			}
			for k := range t.shards[i].evicted {
				evicted[k] = true
			}
		}
		t.shards[i].mu.RUnlock()
	}

	if hasEvicted {
		diskData := t.readKVFromPages()
		for k, v := range diskData {
			if !evicted[k] || seen[k] {
				continue
			}
			if startKey != nil && strings.Compare(k, startStr) < 0 {
				continue
			}
			if endKey != nil && strings.Compare(k, endStr) > 0 {
				continue
			}
			pairs = append(pairs, kvPair{k, cloneBytes(v)})
		}
	}

	slices.SortFunc(pairs, func(a, b kvPair) int {
		return strings.Compare(a.key, b.key)
	})

	return &Iterator{
		tree:      t,
		pairs:     pairs,
		idx:       0,
		endKey:    endStr,
		hasEndKey: endKey != nil,
		done:      false,
	}, nil
}

// Next advances the iterator
func (it *Iterator) Next() ([]byte, []byte, error) {
	if it.done || it.idx >= len(it.pairs) {
		it.done = true
		return nil, nil, nil
	}

	p := it.pairs[it.idx]
	it.idx++

	if it.hasEndKey && strings.Compare(p.key, it.endKey) > 0 {
		it.done = true
		return nil, nil, nil
	}

	return []byte(p.key), cloneBytes(p.value), nil
}

// NextString returns the next entry with the key as a string,
// avoiding an allocation when the caller needs a string key.
func (it *Iterator) NextString() (string, []byte, error) {
	if it.done || it.idx >= len(it.pairs) {
		it.done = true
		return "", nil, nil
	}

	p := it.pairs[it.idx]
	it.idx++

	if it.hasEndKey && strings.Compare(p.key, it.endKey) > 0 {
		it.done = true
		return "", nil, nil
	}

	return p.key, cloneBytes(p.value), nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

// Valid returns true if the iterator has more items
func (it *Iterator) Valid() bool {
	return !it.done && it.idx < len(it.pairs)
}

// Close closes the iterator
func (it *Iterator) Close() error {
	it.done = true
	return nil
}

// HasNext returns true if there are more items to iterate
func (it *Iterator) HasNext() bool {
	return it.Valid()
}

// First positions the iterator at the first item
func (it *Iterator) First() bool {
	if len(it.pairs) == 0 {
		it.done = true
		return false
	}
	it.idx = 0
	return true
}

// Size returns the number of keys in the tree (including evicted keys on disk)
func (t *BTree) Size() int {
	return int(atomic.LoadInt64(&t.keyCount))
}

// Flush writes all in-memory data to disk pages (with multi-page overflow support)
func (t *BTree) Flush() error {
	return t.flushInternal()
}

// Cell represents a key-value pair in a leaf node (kept for compatibility)
type Cell struct {
	KeySize   uint16
	ValueSize uint32
	Key       []byte
	Value     []byte
}
