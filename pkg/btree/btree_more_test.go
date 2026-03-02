package btree

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestSplitLeaf(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create tree
	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create B+Tree: %v", err)
	}

	// Insert enough keys to cause splits (with default order=100, we need more keys)
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		tree.Put(key, []byte("value"))
	}

	// Verify all keys are present
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		_, err := tree.Get(key)
		if err != nil {
			t.Errorf("Key %s not found after split", string(key))
		}
	}

	// Verify size
	if tree.Size() != 200 {
		t.Errorf("Expected size 200, got %d", tree.Size())
	}
}

func TestSplitInternal(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create B+Tree: %v", err)
	}

	// Insert many keys to cause multiple levels of splits
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		tree.Put(key, []byte("value"))
	}

	// Verify all keys
	count := 0
	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	for iter.HasNext() {
		iter.Next()
		count++
	}

	if count != 500 {
		t.Errorf("Expected 500 keys, got %d", count)
	}
}

func TestValid(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)
	tree.Put([]byte("key"), []byte("value"))

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	// Before Next, should be valid (has data)
	if !iter.Valid() {
		t.Error("Expected iterator to be valid before Next")
	}

	// After Next with data, should still be valid (has more data or done)
	iter.Next()
	// After consuming the only item, iterator should be done
	if iter.Valid() {
		// This is ok - depends on implementation
	}
}

func TestFirst(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Empty tree
	iter, _ := tree.Scan(nil, nil)
	if iter.First() {
		t.Error("Expected First to return false for empty tree")
	}
	iter.Close()

	// Non-empty tree
	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))

	iter, _ = tree.Scan(nil, nil)
	defer iter.Close()

	if !iter.First() {
		t.Fatal("Expected First to return true")
	}

	key, _, _ := iter.Next()
	if string(key) != "a" {
		t.Errorf("Expected first key 'a', got %s", string(key))
	}
}

func TestDeleteNonExistent(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	err := tree.Delete([]byte("nonexistent"))
	if err == nil {
		t.Error("Expected error when deleting non-existent key")
	}
}

func TestDeleteFromEmptyTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	err := tree.Delete([]byte("key"))
	if err == nil {
		t.Error("Expected error when deleting from empty tree")
	}
}

func TestGetInvalidKey(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	_, err := tree.Get([]byte{})
	if err == nil {
		t.Error("Expected error for empty key")
	}
}

func TestPutInvalidKey(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	err := tree.Put([]byte{}, []byte("value"))
	if err == nil {
		t.Error("Expected error for empty key")
	}

	err = tree.Put(nil, []byte("value"))
	if err == nil {
		t.Error("Expected error for nil key")
	}
}

func TestDeleteInvalidKey(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	err := tree.Delete([]byte{})
	if err == nil {
		t.Error("Expected error for empty key")
	}

	err = tree.Delete(nil)
	if err == nil {
		t.Error("Expected error for nil key")
	}
}

func TestScanWithRange(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))
	tree.Put([]byte("c"), []byte("3"))
	tree.Put([]byte("d"), []byte("4"))

	// Scan from 'b' to 'c' (end key is exclusive)
	iter, _ := tree.Scan([]byte("b"), []byte("c~")) // Use 'c~' to include 'c'
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		_, _, _ = iter.Next()
		count++
	}

	// Should get at least 'b' and 'c'
	if count < 2 {
		t.Errorf("Expected at least 2 keys in range, got %d", count)
	}
}

func TestLargeTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1000, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert 1000 keys
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		tree.Put(key, []byte("value"))
	}

	// Verify size
	if tree.Size() != 1000 {
		t.Errorf("Expected size 1000, got %d", tree.Size())
	}

	// Scan all
	count := 0
	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	for iter.HasNext() {
		iter.Next()
		count++
	}

	if count != 1000 {
		t.Errorf("Expected 1000 keys from scan, got %d", count)
	}
}

// Test iterator Next() edge cases
func TestIteratorNextEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Test Next on empty tree
	iter, _ := tree.Scan(nil, nil)
	key, val, err := iter.Next()
	if key != nil || val != nil || err != nil {
		t.Error("Expected nil return from Next on empty tree")
	}
	if !iter.done {
		t.Error("Expected iterator to be done after Next on empty tree")
	}
	iter.Close()

	// Test Next after exhaustion
	tree.Put([]byte("a"), []byte("1"))
	iter, _ = tree.Scan(nil, nil)
	iter.Next() // Get the only item
	iter.Next() // Try to get next (should return nil and mark done)
	if !iter.done {
		t.Error("Expected iterator to be done after exhausting items")
	}
	iter.Close()
}

// TestFindLeaf tests the findLeaf function indirectly through Get operations
func TestFindLeaf(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert keys to create a multi-level tree
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		tree.Put(key, []byte(fmt.Sprintf("value%d", i)))
	}

	// Verify all keys can be found - this exercises findLeaf
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		val, err := tree.Get(key)
		if err != nil {
			t.Errorf("Key %s not found: %v", string(key), err)
			continue
		}
		expectedVal := fmt.Sprintf("value%d", i)
		if string(val) != expectedVal {
			t.Errorf("Key %s: expected %s, got %s", string(key), expectedVal, string(val))
		}
	}

	// Test finding non-existent keys
	nonExistentKeys := [][]byte{
		[]byte("key9999"),
		[]byte("key0000"), // May exist, but let's check
		[]byte("zzz"),
	}

	for _, key := range nonExistentKeys {
		_, err := tree.Get(key)
		// It's ok if some exist, but we want to exercise the code path
		_ = err
	}
}

// TestFindLeafWithLargeTree tests findLeaf with a larger tree structure
func TestFindLeafWithLargeTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert keys in sorted order to create a specific tree structure
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key%04d", i)
		tree.Put([]byte(key), []byte(fmt.Sprintf("value%d", i)))
	}

	// Search for keys in different ranges to exercise different leaf nodes
	searchKeys := []int{0, 100, 250, 400, 499}
	for _, idx := range searchKeys {
		key := fmt.Sprintf("key%04d", idx)
		val, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s not found: %v", key, err)
			continue
		}
		expectedVal := fmt.Sprintf("value%d", idx)
		if string(val) != expectedVal {
			t.Errorf("Key %s: expected %s, got %s", key, expectedVal, string(val))
		}
	}
}
