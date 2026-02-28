package btree

import (
	"testing"
)

func TestNewBTree(t *testing.T) {
	tree, err := NewBTree(nil)
	if err != nil {
		t.Fatalf("Failed to create B+Tree: %v", err)
	}

	if tree == nil {
		t.Fatal("Tree is nil")
	}

	if tree.RootPageID() == 0 {
		t.Error("Expected non-zero root page ID")
	}
}

func TestOpenBTree(t *testing.T) {
	tree := OpenBTree(nil, 1)
	if tree == nil {
		t.Fatal("Tree is nil")
	}

	if tree.RootPageID() != 1 {
		t.Errorf("Expected root page ID 1, got %d", tree.RootPageID())
	}
}

func TestPutGet(t *testing.T) {
	tree, _ := NewBTree(nil)

	// Put
	err := tree.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Get
	value, err := tree.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if string(value) != "value1" {
		t.Errorf("Expected 'value1', got %q", string(value))
	}
}

func TestGetNonExistent(t *testing.T) {
	tree, _ := NewBTree(nil)

	_, err := tree.Get([]byte("nonexistent"))
	if err == nil {
		t.Error("Expected error when getting non-existent key")
	}
}

func TestDelete(t *testing.T) {
	tree, _ := NewBTree(nil)

	// Put
	tree.Put([]byte("key1"), []byte("value1"))

	// Delete
	err := tree.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify deleted
	_, err = tree.Get([]byte("key1"))
	if err == nil {
		t.Error("Expected error when getting deleted key")
	}
}

func TestMultiplePuts(t *testing.T) {
	tree, _ := NewBTree(nil)

	// Put multiple
	for i := 0; i < 100; i++ {
		key := []byte(string(rune('a' + i%26)) + string(rune('a' + i/26)))
		value := []byte("value")
		tree.Put(key, value)
	}

	// Verify all
	count := 0
	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	for iter.HasNext() {
		iter.Next()
		count++
	}

	if count != 100 {
		t.Errorf("Expected 100 keys, got %d", count)
	}
}

func TestUpdate(t *testing.T) {
	tree, _ := NewBTree(nil)

	// Put
	tree.Put([]byte("key1"), []byte("value1"))

	// Update
	tree.Put([]byte("key1"), []byte("value2"))

	// Verify
	value, _ := tree.Get([]byte("key1"))
	if string(value) != "value2" {
		t.Errorf("Expected 'value2', got %q", string(value))
	}
}

func TestSize(t *testing.T) {
	tree, _ := NewBTree(nil)

	if tree.Size() != 0 {
		t.Errorf("Expected size 0, got %d", tree.Size())
	}

	tree.Put([]byte("key1"), []byte("value1"))
	if tree.Size() != 1 {
		t.Errorf("Expected size 1, got %d", tree.Size())
	}
}

func TestScan(t *testing.T) {
	tree, _ := NewBTree(nil)

	// Insert keys
	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))
	tree.Put([]byte("c"), []byte("3"))

	// Scan
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		key, _, _ := iter.Next()
		if key != nil {
			count++
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 keys, got %d", count)
	}
}
