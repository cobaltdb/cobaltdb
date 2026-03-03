package btree

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// setupDiskBTree creates a DiskBTree for testing
func setupDiskBTree(t *testing.T) (*DiskBTree, *storage.PageManager, func()) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)

	pm, err := storage.NewPageManager(pool)
	if err != nil {
		pool.Close()
		t.Fatalf("Failed to create page manager: %v", err)
	}

	tree, err := NewDiskBTree(pm)
	if err != nil {
		pm.Close()
		pool.Close()
		t.Fatalf("Failed to create DiskBTree: %v", err)
	}

	cleanup := func() {
		pm.Close()
		pool.Close()
	}

	return tree, pm, cleanup
}

// TestNewDiskBTree tests creating a new disk-based B+Tree
func TestNewDiskBTree(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	if tree == nil {
		t.Fatal("DiskBTree is nil")
	}

	if tree.rootPageID == 0 {
		t.Error("Root page ID should not be 0")
	}

	if tree.order != 100 {
		t.Errorf("Expected order 100, got %d", tree.order)
	}
}

// TestDiskBTreeGetExisting tests getting an existing key
func TestDiskBTreeGetExisting(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert a key
	key := []byte("testkey")
	value := []byte("testvalue")

	err := tree.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Get the key
	got, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if string(got) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(got))
	}
}

// TestDiskBTreeGetNonExisting tests getting a non-existing key
func TestDiskBTreeGetNonExisting(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Try to get a non-existing key
	_, err := tree.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

// TestDiskBTreePutAndGetMultiple tests putting and getting multiple keys
func TestDiskBTreePutAndGetMultiple(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert multiple keys
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	values := []string{"value1", "value2", "value3", "value4", "value5"}

	for i, key := range keys {
		err := tree.Put([]byte(key), []byte(values[i]))
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// Get all keys
	for i, key := range keys {
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if string(got) != values[i] {
			t.Errorf("Key %s: expected %s, got %s", key, values[i], string(got))
		}
	}
}

// TestDiskBTreeUpdate tests updating an existing key
func TestDiskBTreeUpdate(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	key := []byte("testkey")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// Insert initial value
	err := tree.Put(key, value1)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Update value
	err = tree.Put(key, value2)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Get updated value
	got, err := tree.Get(key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if string(got) != string(value2) {
		t.Errorf("Expected %s, got %s", string(value2), string(got))
	}
}

// TestDiskBTreeDelete tests deleting a key
func TestDiskBTreeDelete(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	key := []byte("testkey")
	value := []byte("testvalue")

	// Insert
	err := tree.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Delete
	err = tree.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Try to get deleted key
	_, err = tree.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound after delete, got %v", err)
	}
}

// TestDiskBTreeDeleteNonExisting tests deleting a non-existing key
func TestDiskBTreeDeleteNonExisting(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Try to delete non-existing key
	err := tree.Delete([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

// TestDiskBTreeScan tests scanning keys
func TestDiskBTreeScan(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		err := tree.Put([]byte(key), []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}

	// Scan all
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		key, _, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		count++
		t.Logf("Scanned key: %s", string(key))
	}

	if count != len(keys) {
		t.Errorf("Expected %d keys, got %d", len(keys), count)
	}
}

// TestDiskBTreeScanRange tests scanning a range of keys
func TestDiskBTreeScanRange(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		err := tree.Put([]byte(key), []byte("value_"+key))
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}

	// Scan range [b, d]
	iter, err := tree.Scan([]byte("b"), []byte("d"))
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		key, _, err := iter.Next()
		if err != nil {
			// Iterator may return error when past end key, which is expected
			break
		}
		count++
		t.Logf("Scanned key in range: %s", string(key))
	}

	// Should get b, c, d (3 keys)
	if count != 3 {
		t.Errorf("Expected 3 keys in range, got %d", count)
	}
}

// TestDiskBTreeLargeData tests with larger number of keys
func TestDiskBTreeLargeData(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert many keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if string(got) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(got))
		}
	}
}

// TestDiskBTreeEmptyKey tests empty key handling
func TestDiskBTreeEmptyKey(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	err := tree.Put([]byte{}, []byte("value"))
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey for empty key, got %v", err)
	}
}

// TestDiskBTreeEmptyValue tests empty value handling
func TestDiskBTreeEmptyValue(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	err := tree.Put([]byte("key"), []byte{})
	if err != ErrInvalidValue {
		t.Errorf("Expected ErrInvalidValue for empty value, got %v", err)
	}
}

// TestDiskBTreeConcurrency tests concurrent access
func TestDiskBTreeConcurrency(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert keys concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("key%d_%d", id, j)
				value := fmt.Sprintf("value%d_%d", id, j)
				tree.Put([]byte(key), []byte(value))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all keys
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key%d_%d", i, j)
			expectedValue := fmt.Sprintf("value%d_%d", i, j)
			got, err := tree.Get([]byte(key))
			if err != nil {
				t.Errorf("Failed to get key %s: %v", key, err)
				continue
			}
			if string(got) != expectedValue {
				t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(got))
			}
		}
	}
}

// TestDiskBTreeIteratorClose tests iterator close
func TestDiskBTreeIteratorClose(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert some keys
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		tree.Put([]byte(key), []byte("value"))
	}

	// Create and close iterator
	iter, _ := tree.Scan(nil, nil)
	iter.Close()

	// Should be safe to close again
	iter.Close()
}

// TestDiskBTreeReopen tests reopening a tree after closing
func TestDiskBTreeReopen(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)

	pm, err := storage.NewPageManager(pool)
	if err != nil {
		pool.Close()
		t.Fatalf("Failed to create page manager: %v", err)
	}

	// Create tree and insert data
	tree1, err := NewDiskBTree(pm)
	if err != nil {
		pm.Close()
		pool.Close()
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert keys
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		tree1.Put([]byte(key), []byte(value))
	}

	// Reopen tree with same page manager
	tree2, err := NewDiskBTree(pm)
	if err != nil {
		pm.Close()
		pool.Close()
		t.Fatalf("Failed to reopen tree: %v", err)
	}

	// Verify all keys
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		got, err := tree2.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s after reopen: %v", key, err)
			continue
		}
		if string(got) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(got))
		}
	}

	pm.Close()
	pool.Close()
}

// TestDiskBTreeManyKeys tests inserting many keys
func TestDiskBTreeManyKeys(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert many keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Verify all keys are retrievable
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if string(got) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(got))
		}
	}
}

// TestDiskBTreeSequentialInsert tests inserting keys in sequential order
func TestDiskBTreeSequentialInsert(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert sequential keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Scan and verify order
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	defer iter.Close()

	count := 0
	prevKey := ""
	for iter.HasNext() {
		key, _, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		if string(key) <= prevKey {
			t.Errorf("Keys out of order: %s <= %s", string(key), prevKey)
		}
		prevKey = string(key)
		count++
	}

	if count != 50 {
		t.Errorf("Expected 50 keys, got %d", count)
	}
}

// TestDiskBTreeReverseOrderInsert tests inserting keys in reverse order
func TestDiskBTreeReverseOrderInsert(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert reverse order keys
	for i := 49; i >= 0; i-- {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Verify all keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if string(got) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(got))
		}
	}
}

// TestDiskBTreeRandomInsert tests inserting keys in random order
func TestDiskBTreeRandomInsert(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Pseudo-random order
	order := []int{50, 25, 75, 12, 37, 62, 87, 5, 18, 31, 43, 56, 68, 81, 93}
	for _, i := range order {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
	}

	// Verify all keys
	for _, i := range order {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if string(got) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(got))
		}
	}
}

// TestDiskBTreeDeleteMany tests deleting many keys
func TestDiskBTreeDeleteMany(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert many keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		tree.Put([]byte(key), []byte(value))
	}

	// Delete some keys
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%04d", i)
		err := tree.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%04d", i)
		_, err := tree.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound for deleted key %s, got %v", key, err)
		}
	}

	// Verify remaining keys exist
	for i := 20; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if string(got) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(got))
		}
	}
}

// TestDiskBTreeUpdateMany tests updating many keys
func TestDiskBTreeUpdateMany(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert many keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%d", i)
		tree.Put([]byte(key), []byte(value))
	}

	// Update all keys
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		newValue := fmt.Sprintf("updated%d", i)
		err := tree.Put([]byte(key), []byte(newValue))
		if err != nil {
			t.Fatalf("Failed to update key %d: %v", i, err)
		}
	}

	// Verify updated values
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		expectedValue := fmt.Sprintf("updated%d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key %d: %v", i, err)
		}
		if string(got) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(got))
		}
	}
}
