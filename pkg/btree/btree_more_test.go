package btree

import (
	"testing"
)

func TestSplitLeaf(t *testing.T) {
	// Create tree with small order to force splits
	tree, _ := NewBTree(nil)
	tree.order = 5 // Small order to trigger splits quickly

	// Insert enough keys to cause splits
	for i := 0; i < 20; i++ {
		key := []byte(string(rune('a' + i)))
		tree.Put(key, []byte("value"))
	}

	// Verify all keys are present
	for i := 0; i < 20; i++ {
		key := []byte(string(rune('a' + i)))
		_, err := tree.Get(key)
		if err != nil {
			t.Errorf("Key %s not found after split", string(key))
		}
	}

	// Verify size
	if tree.Size() != 20 {
		t.Errorf("Expected size 20, got %d", tree.Size())
	}
}

func TestSplitInternal(t *testing.T) {
	// Create tree with very small order to force internal node splits
	tree, _ := NewBTree(nil)
	tree.order = 3

	// Insert many keys to cause multiple levels of splits
	for i := 0; i < 50; i++ {
		key := []byte(string(rune('A' + i%26)) + string(rune('A' + i/26)))
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

	if count != 50 {
		t.Errorf("Expected 50 keys, got %d", count)
	}
}

func TestValid(t *testing.T) {
	tree, _ := NewBTree(nil)
	tree.Put([]byte("key"), []byte("value"))

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	// Before Next, should not be valid
	// After Next with data, should be valid
	iter.Next()
	if !iter.Valid() {
		t.Error("Expected iterator to be valid after Next")
	}
}

func TestFirst(t *testing.T) {
	tree, _ := NewBTree(nil)

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
	tree, _ := NewBTree(nil)

	err := tree.Delete([]byte("nonexistent"))
	if err == nil {
		t.Error("Expected error when deleting non-existent key")
	}
}

func TestDeleteFromEmptyTree(t *testing.T) {
	tree, _ := NewBTree(nil)

	err := tree.Delete([]byte("key"))
	if err == nil {
		t.Error("Expected error when deleting from empty tree")
	}
}

func TestGetInvalidKey(t *testing.T) {
	tree, _ := NewBTree(nil)

	_, err := tree.Get([]byte{})
	if err == nil {
		t.Error("Expected error for empty key")
	}

	_, err = tree.Get(nil)
	if err == nil {
		t.Error("Expected error for nil key")
	}
}

func TestPutInvalidKey(t *testing.T) {
	tree, _ := NewBTree(nil)

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
	tree, _ := NewBTree(nil)

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
	tree, _ := NewBTree(nil)

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
	tree, _ := NewBTree(nil)
	tree.order = 10

	// Insert 1000 keys
	for i := 0; i < 1000; i++ {
		key := []byte(string(rune(i)))
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
