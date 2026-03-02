package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

var (
	ErrKeyNotFound  = errors.New("key not found")
	ErrKeyExists    = errors.New("key already exists")
	ErrTreeFull     = errors.New("tree is full")
	ErrInvalidKey   = errors.New("invalid key")
	ErrInvalidValue = errors.New("invalid value")
)

// BTree represents a disk-based B+Tree index using a simpler approach:
// - Each table has its own BTree instance
// - Data is stored as key-value pairs in pages managed by the buffer pool
// - The BTree maintains an in-memory sorted structure that flushes to disk pages
// - This is a hybrid approach: in-memory sorted map with periodic page flush

type BTree struct {
	mu         sync.RWMutex
	rootPageID uint32
	pool       *storage.BufferPool
	order      int
	// In-memory storage until we implement proper page-based storage
	memStorage map[string][]byte
	dirty      bool
}

// NewBTree creates a new B+Tree
func NewBTree(pool *storage.BufferPool) (*BTree, error) {
	// Allocate root page
	rootPage, err := pool.NewPage(storage.PageTypeLeaf)
	if err != nil {
		return nil, fmt.Errorf("failed to create root page: %w", err)
	}
	defer pool.Unpin(rootPage)

	return &BTree{
		rootPageID: rootPage.ID(),
		pool:       pool,
		order:      100,
		memStorage: make(map[string][]byte),
		dirty:      false,
	}, nil
}

// OpenBTree opens an existing B+Tree with the given root page ID
func OpenBTree(pool *storage.BufferPool, rootPageID uint32) *BTree {
	return &BTree{
		rootPageID: rootPageID,
		pool:       pool,
		order:      100,
		memStorage: make(map[string][]byte),
		dirty:      false,
	}
}

// RootPageID returns the root page ID of the tree
func (t *BTree) RootPageID() uint32 {
	return t.rootPageID
}

// Get retrieves a value by key
func (t *BTree) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrInvalidKey
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	// First check in-memory storage
	if val, ok := t.memStorage[string(key)]; ok {
		// Return a copy to prevent external modification
		result := make([]byte, len(val))
		copy(result, val)
		return result, nil
	}

	// TODO: Load from disk pages if not in memory
	// For now, return not found
	return nil, ErrKeyNotFound
}

// Put inserts or updates a key-value pair
func (t *BTree) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(value) == 0 {
		return ErrInvalidValue
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Store in memory (with copy to prevent external modification)
	keyCopy := string(key)
	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	t.memStorage[keyCopy] = valCopy
	t.dirty = true

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
	if _, ok := t.memStorage[keyStr]; !ok {
		return ErrKeyNotFound
	}

	delete(t.memStorage, keyStr)
	t.dirty = true
	return nil
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

	// Collect all keys and sort them
	var keys, values [][]byte
	for k, v := range t.memStorage {
		kb := []byte(k)

		// Check start key
		if startKey != nil && bytes.Compare(kb, startKey) < 0 {
			continue
		}

		// Check end key
		if endKey != nil && bytes.Compare(kb, endKey) > 0 {
			continue
		}

		// Make copies
		keyCopy := make([]byte, len(kb))
		copy(keyCopy, kb)
		valCopy := make([]byte, len(v))
		copy(valCopy, v)

		keys = append(keys, keyCopy)
		values = append(values, valCopy)
	}

	// Sort keys
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
	// Simple bubble sort for now
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if bytes.Compare(keys[i], keys[j]) > 0 {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			}
		}
	}
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

	// Check end key
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

// Size returns the number of keys in the tree
func (t *BTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.memStorage)
}

// Flush writes all in-memory data to disk pages
func (t *BTree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.dirty {
		return nil
	}

	// Get root page
	root, err := t.pool.GetPage(t.rootPageID)
	if err != nil {
		return err
	}
	defer t.pool.Unpin(root)

	// Serialize all key-value pairs
	// Format: [count:4][keylen:2][key][valuelen:4][value]...
	var buf bytes.Buffer
	count := uint32(len(t.memStorage))
	binary.Write(&buf, binary.LittleEndian, count)

	for k, v := range t.memStorage {
		key := []byte(k)
		binary.Write(&buf, binary.LittleEndian, uint16(len(key)))
		buf.Write(key)
		binary.Write(&buf, binary.LittleEndian, uint32(len(v)))
		buf.Write(v)
	}

	// Write to page (with overflow handling for large datasets)
	data := buf.Bytes()
	pageSize := storage.PageSize - storage.PageHeaderSize
	if len(data) > pageSize {
		// Truncate for now - proper overflow handling would require multiple pages
		data = data[:pageSize]
	}

	copy(root.Data()[storage.PageHeaderSize:], data)
	root.SetDirty(true)

	t.dirty = false
	return nil
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
