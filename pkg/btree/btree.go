package btree

import (
	"bytes"
	"errors"
	"sync"
)

var (
	ErrKeyNotFound   = errors.New("key not found")
	ErrKeyExists     = errors.New("key already exists")
	ErrInvalidKey    = errors.New("invalid key")
	ErrInvalidValue  = errors.New("invalid value")
)

// BTree represents an in-memory B+Tree index
type BTree struct {
	mu        sync.RWMutex
	root      *node
	order     int
	size      int
	rootPageID uint32
	pool      interface{} // Keep for compatibility
}

// node represents a B+Tree node
type node struct {
	leaf     bool
	keys     [][]byte
	values   [][]byte    // Only used in leaf nodes
	children []*node     // Only used in internal nodes
}

// NewBTree creates a new B+Tree
func NewBTree(pool interface{}) (*BTree, error) {
	return &BTree{
		root: &node{
			leaf:   true,
			keys:   make([][]byte, 0),
			values: make([][]byte, 0),
		},
		order:     100,
		rootPageID: 1,
		pool:      pool,
	}, nil
}

// OpenBTree opens an existing B+Tree
func OpenBTree(pool interface{}, rootPageID uint32) *BTree {
	tree, _ := NewBTree(pool)
	tree.rootPageID = rootPageID
	return tree
}

// RootPageID returns the root page ID
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

	return t.get(t.root, key)
}

func (t *BTree) get(n *node, key []byte) ([]byte, error) {
	i := t.findKey(n, key)

	if n.leaf {
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			return n.values[i], nil
		}
		return nil, ErrKeyNotFound
	}

	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		i++
	}
	return t.get(n.children[i], key)
}

// Put inserts or updates a key-value pair
func (t *BTree) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	newKey, newChild := t.insert(t.root, key, value)
	if newChild != nil {
		// Split root
		newRoot := &node{
			leaf:     false,
			keys:     [][]byte{newKey},
			children: []*node{t.root, newChild},
		}
		t.root = newRoot
	}

	return nil
}

func (t *BTree) insert(n *node, key, value []byte) ([]byte, *node) {
	i := t.findKey(n, key)

	if n.leaf {
		// Insert into leaf
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			// Update existing - don't increment size
			n.values[i] = value
			return nil, nil
		}

		// Insert new
		n.keys = append(n.keys[:i], append([][]byte{key}, n.keys[i:]...)...)
		n.values = append(n.values[:i], append([][]byte{value}, n.values[i:]...)...)
		t.size++ // Only increment for new keys

		// Split if necessary
		if len(n.keys) > t.order {
			return t.splitLeaf(n)
		}
		return nil, nil
	}

	// Internal node
	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		i++
	}

	newKey, newChild := t.insert(n.children[i], key, value)
	if newChild != nil {
		// Insert new key and child
		n.keys = append(n.keys[:i], append([][]byte{newKey}, n.keys[i:]...)...)
		n.children = append(n.children[:i+1], append([]*node{newChild}, n.children[i+1:]...)...)

		// Split if necessary
		if len(n.keys) > t.order {
			return t.splitInternal(n)
		}
	}
	return nil, nil
}

func (t *BTree) splitLeaf(n *node) ([]byte, *node) {
	mid := len(n.keys) / 2

	newNode := &node{
		leaf:   true,
		keys:   append([][]byte{}, n.keys[mid:]...),
		values: append([][]byte{}, n.values[mid:]...),
	}

	n.keys = n.keys[:mid]
	n.values = n.values[:mid]

	return newNode.keys[0], newNode
}

func (t *BTree) splitInternal(n *node) ([]byte, *node) {
	mid := len(n.keys) / 2

	promotedKey := n.keys[mid]

	newNode := &node{
		leaf:     false,
		keys:     append([][]byte{}, n.keys[mid+1:]...),
		children: append([]*node{}, n.children[mid+1:]...),
	}

	n.keys = n.keys[:mid]
	n.children = n.children[:mid+1]

	return promotedKey, newNode
}

// Delete removes a key from the tree
func (t *BTree) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return t.delete(t.root, key)
}

func (t *BTree) delete(n *node, key []byte) error {
	i := t.findKey(n, key)

	if n.leaf {
		if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
			n.keys = append(n.keys[:i], n.keys[i+1:]...)
			n.values = append(n.values[:i], n.values[i+1:]...)
			t.size--
			return nil
		}
		return ErrKeyNotFound
	}

	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		i++
	}
	return t.delete(n.children[i], key)
}

// findKey finds the position where key should be inserted
func (t *BTree) findKey(n *node, key []byte) int {
	for i := 0; i < len(n.keys); i++ {
		if bytes.Compare(key, n.keys[i]) <= 0 {
			return i
		}
	}
	return len(n.keys)
}

// Iterator for range scans
type Iterator struct {
	keys   [][]byte  // Snapshot of all keys
	values [][]byte  // Snapshot of all values
	idx    int       // Current index
	endKey []byte
	done   bool
}

// Scan returns an iterator for range scanning
func (t *BTree) Scan(startKey, endKey []byte) (*Iterator, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Collect all key-value pairs as a snapshot
	var keys, values [][]byte
	t.collectKVs(t.root, &keys, &values)

	return &Iterator{
		keys:   keys,
		values: values,
		idx:    0,
		endKey: endKey,
		done:   false,
	}, nil
}

// collectKVs collects all key-value pairs from the tree
func (t *BTree) collectKVs(n *node, keys *[][]byte, values *[][]byte) {
	if n.leaf {
		for i := 0; i < len(n.keys); i++ {
			// Copy key and value
			keyCopy := make([]byte, len(n.keys[i]))
			copy(keyCopy, n.keys[i])
			valueCopy := make([]byte, len(n.values[i]))
			copy(valueCopy, n.values[i])

			*keys = append(*keys, keyCopy)
			*values = append(*values, valueCopy)
		}
		return
	}

	for _, child := range n.children {
		t.collectKVs(child, keys, values)
	}
}

func (t *BTree) findLeaf(n *node, key []byte) *node {
	if n.leaf {
		return n
	}

	i := t.findKey(n, key)
	if i < len(n.keys) && bytes.Equal(n.keys[i], key) {
		i++
	}
	return t.findLeaf(n.children[i], key)
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

// Valid returns true if the iterator has more items to read
// Should be called AFTER Next() to check if data was returned
func (it *Iterator) Valid() bool {
	return !it.done
}

// HasNext returns true if there are more items to iterate
// Should be called BEFORE Next() to check if more data exists
func (it *Iterator) HasNext() bool {
	return !it.done && it.idx < len(it.keys)
}

// Close closes the iterator
func (it *Iterator) Close() {
	it.done = true
}

// Size returns the number of keys in the tree
func (t *BTree) Size() int {
	return t.size
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
