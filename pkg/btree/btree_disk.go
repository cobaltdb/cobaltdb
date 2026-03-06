package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// DiskBTree represents a disk-based B+Tree
type DiskBTree struct {
	pm         *storage.PageManager
	rootPageID uint32
	order      int
	mu         sync.RWMutex
}

// NewDiskBTree creates a new disk-based B+Tree
func NewDiskBTree(pm *storage.PageManager) (*DiskBTree, error) {
	// Check if there's an existing root page
	meta := pm.GetMeta()
	if meta.RootPageID != 0 {
		// Existing tree
		return &DiskBTree{
			pm:         pm,
			rootPageID: meta.RootPageID,
			order:      100, // Default order
		}, nil
	}

	// Create new root page (leaf)
	rootPage, err := pm.AllocatePage(storage.PageTypeLeaf)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %w", err)
	}
	pm.GetPool().Unpin(rootPage)

	// Update meta page
	meta.RootPageID = rootPage.ID()
	if err := pm.UpdateMeta(meta); err != nil {
		return nil, fmt.Errorf("failed to update meta: %w", err)
	}

	return &DiskBTree{
		pm:         pm,
		rootPageID: rootPage.ID(),
		order:      100,
	}, nil
}

// Get retrieves a value by key
func (t *DiskBTree) Get(key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.getFromPage(t.rootPageID, key)
}

// getFromPage recursively searches for a key starting from a page
func (t *DiskBTree) getFromPage(pageID uint32, key []byte) ([]byte, error) {
	page, err := t.pm.GetPage(pageID)
	if err != nil {
		return nil, err
	}
	defer t.pm.GetPool().Unpin(page)

	data := page.Data()
	pageType := storage.PageType(data[4])

	if pageType == storage.PageTypeLeaf {
		return t.searchLeafPage(data, key)
	}

	// Internal node - find child page
	childPageID := t.findChildPage(data, key)
	return t.getFromPage(childPageID, key)
}

// searchLeafPage searches for a key in a leaf page
func (t *DiskBTree) searchLeafPage(data []byte, key []byte) ([]byte, error) {
	entries := t.readEntries(data)
	for _, entry := range entries {
		if bytes.Equal(entry.Key, key) {
			return entry.Value, nil
		}
	}
	return nil, ErrKeyNotFound
}

// findChildPage finds the child page ID for a key in an internal node
func (t *DiskBTree) findChildPage(data []byte, key []byte) uint32 {
	entries := t.readEntries(data)

	// Find the first entry with key >= search key
	// The value contains the child page ID
	for i, entry := range entries {
		if bytes.Compare(key, entry.Key) < 0 {
			// Return the left child of this entry
			if i == 0 {
				// First key is greater, use leftmost child (stored in rightPtr)
				rightPtr := binary.LittleEndian.Uint32(data[12:16])
				return rightPtr
			}
			// Get child from previous entry
			prevEntry := entries[i-1]
			if len(prevEntry.Value) == 4 {
				return binary.LittleEndian.Uint32(prevEntry.Value)
			}
		}
	}

	// Key is >= all keys, follow the last entry's child pointer (rightmost child)
	if len(entries) > 0 {
		lastEntry := entries[len(entries)-1]
		if len(lastEntry.Value) == 4 {
			return binary.LittleEndian.Uint32(lastEntry.Value)
		}
	}
	// Fallback to leftmost child pointer (empty internal node)
	return binary.LittleEndian.Uint32(data[12:16])
}

// readEntries reads all entries from a page
func (t *DiskBTree) readEntries(data []byte) []Entry {
	var entries []Entry
	offset := storage.PageHeaderSize

	cellCount := binary.LittleEndian.Uint16(data[6:8])

	for i := 0; i < int(cellCount); i++ {
		if offset+4 > len(data) {
			break
		}

		keyLen := binary.LittleEndian.Uint16(data[offset : offset+2])
		valLen := binary.LittleEndian.Uint16(data[offset+2 : offset+4])

		if offset+4+int(keyLen)+int(valLen) > len(data) {
			break
		}

		key := make([]byte, keyLen)
		val := make([]byte, valLen)
		copy(key, data[offset+4:offset+4+int(keyLen)])
		copy(val, data[offset+4+int(keyLen):offset+4+int(keyLen)+int(valLen)])

		entries = append(entries, Entry{Key: key, Value: val})
		offset += 4 + int(keyLen) + int(valLen)
	}

	return entries
}

// writeEntries writes entries to a page
func (t *DiskBTree) writeEntries(page *storage.CachedPage, entries []Entry) error {
	data := page.Data()
	offset := storage.PageHeaderSize

	for _, entry := range entries {
		if offset+4+len(entry.Key)+len(entry.Value) > len(data) {
			return fmt.Errorf("page overflow")
		}

		keyLen := uint16(len(entry.Key))
		valLen := uint16(len(entry.Value))

		binary.LittleEndian.PutUint16(data[offset:offset+2], keyLen)
		binary.LittleEndian.PutUint16(data[offset+2:offset+4], valLen)

		copy(data[offset+4:], entry.Key)
		copy(data[offset+4+len(entry.Key):], entry.Value)

		offset += 4 + len(entry.Key) + len(entry.Value)
	}

	// Update header
	binary.LittleEndian.PutUint16(data[6:8], uint16(len(entries)))
	binary.LittleEndian.PutUint16(data[8:10], uint16(offset))
	data[5] = data[5] | 0x01 // Set dirty flag

	page.SetDirty(true)

	return nil
}

// Put inserts or updates a key-value pair
func (t *DiskBTree) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalidKey
	}
	if len(value) == 0 {
		return ErrInvalidValue
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if we need to split the root
	rootPage, err := t.pm.GetPage(t.rootPageID)
	if err != nil {
		return err
	}

	entries := t.readEntries(rootPage.Data())
	if len(entries) >= t.order {
		// Split root
		if err := t.splitRoot(); err != nil {
			t.pm.GetPool().Unpin(rootPage)
			return err
		}
	}
	t.pm.GetPool().Unpin(rootPage)

	// Insert into tree
	return t.insertIntoPage(t.rootPageID, key, value)
}

// insertIntoPage inserts a key-value pair into the appropriate page
func (t *DiskBTree) insertIntoPage(pageID uint32, key, value []byte) error {
	page, err := t.pm.GetPage(pageID)
	if err != nil {
		return err
	}
	defer t.pm.GetPool().Unpin(page)

	data := page.Data()
	pageType := storage.PageType(data[4])

	if pageType == storage.PageTypeLeaf {
		return t.insertIntoLeaf(page, key, value)
	}

	// Internal node - find child and insert there
	childPageID := t.findChildPage(data, key)

	// Check if child needs splitting
	childPage, err := t.pm.GetPage(childPageID)
	if err != nil {
		return err
	}

	childEntries := t.readEntries(childPage.Data())
	if len(childEntries) >= t.order {
		// Split child
		newKey, newPageID, err := t.splitChild(page, childPage)
		if err != nil {
			t.pm.GetPool().Unpin(childPage)
			return err
		}

		// Update parent with new key
		if err := t.insertIntoInternal(page, newKey, newPageID); err != nil {
			t.pm.GetPool().Unpin(childPage)
			return err
		}

		// Determine which child to use
		if bytes.Compare(key, newKey) >= 0 {
			childPageID = newPageID
		}
	}
	t.pm.GetPool().Unpin(childPage)

	return t.insertIntoPage(childPageID, key, value)
}

// insertIntoLeaf inserts into a leaf page
func (t *DiskBTree) insertIntoLeaf(page *storage.CachedPage, key, value []byte) error {
	entries := t.readEntries(page.Data())

	// Check if key already exists (update)
	for i, entry := range entries {
		if bytes.Equal(entry.Key, key) {
			// Update existing entry
			entries[i].Value = value
			return t.writeEntries(page, entries)
		}
	}

	// Insert new entry in sorted order
	newEntry := Entry{Key: key, Value: value}
	insertPos := 0
	for i, entry := range entries {
		if bytes.Compare(key, entry.Key) < 0 {
			insertPos = i
			break
		}
		insertPos = i + 1
	}

	// Insert at position
	entries = append(entries[:insertPos], append([]Entry{newEntry}, entries[insertPos:]...)...)

	return t.writeEntries(page, entries)
}

// insertIntoInternal inserts a key into an internal node
func (t *DiskBTree) insertIntoInternal(page *storage.CachedPage, key []byte, childPageID uint32) error {
	entries := t.readEntries(page.Data())

	// Create entry with child page ID as value
	childIDBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(childIDBytes, childPageID)
	newEntry := Entry{Key: key, Value: childIDBytes}

	// Insert in sorted order
	insertPos := 0
	for i, entry := range entries {
		if bytes.Compare(key, entry.Key) < 0 {
			insertPos = i
			break
		}
		insertPos = i + 1
	}

	entries = append(entries[:insertPos], append([]Entry{newEntry}, entries[insertPos:]...)...)

	return t.writeEntries(page, entries)
}

// splitRoot splits the root node
func (t *DiskBTree) splitRoot() error {
	rootPage, err := t.pm.GetPage(t.rootPageID)
	if err != nil {
		return err
	}

	// Move old root data to a new leaf page
	oldRootPage, err := t.pm.AllocatePage(storage.PageTypeLeaf)
	if err != nil {
		t.pm.GetPool().Unpin(rootPage)
		return err
	}

	// Copy old root data to new leaf
	copy(oldRootPage.Data(), rootPage.Data())
	// Preserve the correct page ID in the copied page header
	binary.LittleEndian.PutUint32(oldRootPage.Data()[0:4], oldRootPage.ID())
	// Ensure page type is leaf
	oldRootPage.Data()[4] = byte(storage.PageTypeLeaf)
	oldRootPage.SetDirty(true)

	// Convert root page in-place to an empty internal node
	// The leftmost child pointer (data[12:16]) points to the copy of old data
	rootData := rootPage.Data()
	rootData[4] = byte(storage.PageTypeInternal)                                  // Page type
	binary.LittleEndian.PutUint16(rootData[6:8], 0)                               // Cell count = 0
	binary.LittleEndian.PutUint16(rootData[8:10], uint16(storage.PageHeaderSize)) // Free start
	binary.LittleEndian.PutUint32(rootData[12:16], oldRootPage.ID())              // Leftmost child ptr
	rootPage.SetDirty(true)

	// Flush both pages to disk
	if err := t.pm.GetPool().FlushPage(oldRootPage); err != nil {
		t.pm.GetPool().Unpin(rootPage)
		t.pm.GetPool().Unpin(oldRootPage)
		return err
	}
	if err := t.pm.GetPool().FlushPage(rootPage); err != nil {
		t.pm.GetPool().Unpin(rootPage)
		t.pm.GetPool().Unpin(oldRootPage)
		return err
	}

	t.pm.GetPool().Unpin(oldRootPage)

	return nil
}

// splitChild splits a child node and returns the new key and page ID
func (t *DiskBTree) splitChild(parent, child *storage.CachedPage) ([]byte, uint32, error) {
	childData := child.Data()
	entries := t.readEntries(childData)
	mid := len(entries) / 2

	// Create new page for right half
	childType := storage.PageType(childData[4])
	newPage, err := t.pm.AllocatePage(childType)
	if err != nil {
		return nil, 0, err
	}

	// Split entries
	leftEntries := entries[:mid]
	rightEntries := entries[mid:]
	midKey := rightEntries[0].Key

	// Write left entries to old child
	if err := t.writeEntries(child, leftEntries); err != nil {
		t.pm.GetPool().Unpin(newPage)
		return nil, 0, err
	}

	// Write right entries to new page
	if err := t.writeEntries(newPage, rightEntries); err != nil {
		t.pm.GetPool().Unpin(newPage)
		return nil, 0, err
	}

	// Update linked list for leaf pages
	if childType == storage.PageTypeLeaf {
		// Copy old right ptr to new page
		copy(newPage.Data()[12:16], childData[12:16])
		// Set new page as old page's right ptr
		binary.LittleEndian.PutUint32(childData[12:16], newPage.ID())
		newPage.SetDirty(true)
	}

	// Flush pages
	if err := t.pm.GetPool().FlushPage(child); err != nil {
		t.pm.GetPool().Unpin(newPage)
		return nil, 0, err
	}
	if err := t.pm.GetPool().FlushPage(newPage); err != nil {
		t.pm.GetPool().Unpin(newPage)
		return nil, 0, err
	}

	newPageID := newPage.ID()
	t.pm.GetPool().Unpin(newPage)

	return midKey, newPageID, nil
}

// Delete removes a key from the tree
func (t *DiskBTree) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.deleteFromPage(t.rootPageID, key)
}

// deleteFromPage deletes a key from a page
func (t *DiskBTree) deleteFromPage(pageID uint32, key []byte) error {
	page, err := t.pm.GetPage(pageID)
	if err != nil {
		return err
	}
	defer t.pm.GetPool().Unpin(page)

	data := page.Data()
	pageType := storage.PageType(data[4])

	if pageType == storage.PageTypeLeaf {
		return t.deleteFromLeaf(page, key)
	}

	// Find child and delete from there
	childPageID := t.findChildPage(data, key)
	return t.deleteFromPage(childPageID, key)
}

// deleteFromLeaf deletes a key from a leaf page
func (t *DiskBTree) deleteFromLeaf(page *storage.CachedPage, key []byte) error {
	entries := t.readEntries(page.Data())

	// Find and remove entry
	for i, entry := range entries {
		if bytes.Equal(entry.Key, key) {
			// Remove entry
			entries = append(entries[:i], entries[i+1:]...)
			return t.writeEntries(page, entries)
		}
	}

	return ErrKeyNotFound
}

// Scan returns an iterator for range scanning
func (t *DiskBTree) Scan(startKey, endKey []byte) (*DiskIterator, error) {
	return &DiskIterator{
		tree:     t,
		startKey: startKey,
		endKey:   endKey,
		current:  0,
		entries:  nil,
		idx:      0,
	}, nil
}

// DiskIterator implements range scanning for disk-based BTree
type DiskIterator struct {
	tree     *DiskBTree
	startKey []byte
	endKey   []byte
	current  uint32 // Current page ID
	entries  []Entry
	idx      int
}

// HasNext returns true if there are more entries
func (it *DiskIterator) HasNext() bool {
	if it.idx < len(it.entries) {
		return true
	}

	// Need to load next page
	if it.current == 0 {
		// First call - find starting page
		it.current = it.findStartPage()
	} else {
		// Get next page from linked list
		page, err := it.tree.pm.GetPage(it.current)
		if err != nil {
			return false
		}
		it.current = binary.LittleEndian.Uint32(page.Data()[12:16])
		it.tree.pm.GetPool().Unpin(page)
	}

	if it.current == 0 {
		return false
	}

	// Load entries from current page
	page, err := it.tree.pm.GetPage(it.current)
	if err != nil {
		return false
	}

	it.entries = it.tree.readEntries(page.Data())
	it.idx = 0

	// Skip entries before startKey
	if it.startKey != nil {
		for it.idx < len(it.entries) && bytes.Compare(it.entries[it.idx].Key, it.startKey) < 0 {
			it.idx++
		}
	}

	it.tree.pm.GetPool().Unpin(page)

	return it.idx < len(it.entries)
}

// Next returns the next entry
func (it *DiskIterator) Next() ([]byte, []byte, error) {
	if it.idx >= len(it.entries) {
		return nil, nil, fmt.Errorf("no more entries")
	}

	entry := it.entries[it.idx]
	it.idx++

	// Check if we've passed endKey
	if it.endKey != nil && bytes.Compare(entry.Key, it.endKey) > 0 {
		return nil, nil, fmt.Errorf("past end key")
	}

	return entry.Key, entry.Value, nil
}

// Close closes the iterator
func (it *DiskIterator) Close() error {
	return nil
}

// findStartPage finds the leaf page containing startKey
func (it *DiskIterator) findStartPage() uint32 {
	pageID := it.tree.rootPageID

	for {
		page, err := it.tree.pm.GetPage(pageID)
		if err != nil {
			return 0
		}

		data := page.Data()
		pageType := storage.PageType(data[4])

		if pageType == storage.PageTypeLeaf {
			it.tree.pm.GetPool().Unpin(page)
			return pageID
		}

		childID := it.tree.findChildPage(data, it.startKey)
		it.tree.pm.GetPool().Unpin(page)
		pageID = childID
	}
}

// Entry represents a key-value entry
type Entry struct {
	Key   []byte
	Value []byte
}
