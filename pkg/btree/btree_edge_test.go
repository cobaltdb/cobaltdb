package btree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestBTreeEmptyKey(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Try to put empty key
	err := tree.Put([]byte{}, []byte("value"))
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey for empty key, got %v", err)
	}

	// Try to get empty key
	_, err = tree.Get([]byte{})
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey for empty key get, got %v", err)
	}

	// Try to delete empty key
	err = tree.Delete([]byte{})
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey for empty key delete, got %v", err)
	}
}

func TestBTreeEmptyValue(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Try to put empty value
	err := tree.Put([]byte("key"), []byte{})
	if err != ErrInvalidValue {
		t.Errorf("Expected ErrInvalidValue for empty value, got %v", err)
	}
}

func TestBTreeLargeKey(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Test with large key (1KB)
	largeKey := make([]byte, 1024)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}

	err := tree.Put(largeKey, []byte("value"))
	if err != nil {
		t.Errorf("Failed to put large key: %v", err)
	}

	val, err := tree.Get(largeKey)
	if err != nil {
		t.Errorf("Failed to get large key: %v", err)
	}
	if string(val) != "value" {
		t.Errorf("Got wrong value for large key")
	}
}

func TestBTreeLargeValue(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Test with large value (10KB)
	largeValue := make([]byte, 10*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err := tree.Put([]byte("key"), largeValue)
	if err != nil {
		t.Errorf("Failed to put large value: %v", err)
	}

	val, err := tree.Get([]byte("key"))
	if err != nil {
		t.Errorf("Failed to get large value: %v", err)
	}
	if !bytes.Equal(val, largeValue) {
		t.Errorf("Got wrong value for large value")
	}
}

func TestBTreeUnicodeKeys(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Test with unicode keys
	keys := []string{
		"日本語",
		"中文",
		"한국어",
		"العربية",
		"עברית",
		"🚀🌟🎉",
		"Café",
		"naïve",
		"résumé",
	}

	for i, key := range keys {
		value := fmt.Sprintf("value%d", i)
		err := tree.Put([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to put unicode key %s: %v", key, err)
		}
	}

	for i, key := range keys {
		expectedValue := fmt.Sprintf("value%d", i)
		val, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get unicode key %s: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("Wrong value for key %s: got %s, want %s", key, string(val), expectedValue)
		}
	}
}

func TestBTreeBinaryKeys(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Test with binary keys containing null bytes and special characters
	keys := [][]byte{
		{0x00, 0x01, 0x02, 0x03},
		{0xFF, 0xFE, 0xFD, 0xFC},
		{0x00, 0x00, 0x00, 0x00},
		{0xFF, 0xFF, 0xFF, 0xFF},
		{0x00, 0x01, 0x00, 0x01}, // null byte in middle
		{0x80, 0x81, 0x82, 0x83}, // high bit set
	}

	for i, key := range keys {
		value := fmt.Sprintf("value%d", i)
		err := tree.Put(key, []byte(value))
		if err != nil {
			t.Errorf("Failed to put binary key %v: %v", key, err)
		}
	}

	for i, key := range keys {
		expectedValue := fmt.Sprintf("value%d", i)
		val, err := tree.Get(key)
		if err != nil {
			t.Errorf("Failed to get binary key %v: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("Wrong value for key %v", key)
		}
	}
}

func TestBTreeOverwrite(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Put initial value
	tree.Put([]byte("key"), []byte("value1"))

	// Overwrite with new value
	tree.Put([]byte("key"), []byte("value2"))

	val, _ := tree.Get([]byte("key"))
	if string(val) != "value2" {
		t.Errorf("Expected value2 after overwrite, got %s", string(val))
	}

	// Overwrite again
	tree.Put([]byte("key"), []byte("value3"))

	val, _ = tree.Get([]byte("key"))
	if string(val) != "value3" {
		t.Errorf("Expected value3 after second overwrite, got %s", string(val))
	}
}

func TestBTreeDeleteNonExistent(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Delete non-existent key should return ErrKeyNotFound
	err := tree.Delete([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for non-existent key, got %v", err)
	}
}

func TestBTreeGetNonExistent(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Get non-existent key
	_, err := tree.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestBTreeDeleteAndReinsert(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert, delete, and reinsert same key
	tree.Put([]byte("key"), []byte("value1"))
	tree.Delete([]byte("key"))
	tree.Put([]byte("key"), []byte("value2"))

	val, err := tree.Get([]byte("key"))
	if err != nil {
		t.Errorf("Failed to get reinserted key: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Expected value2, got %s", string(val))
	}

	if tree.Size() != 1 {
		t.Errorf("Expected size 1, got %d", tree.Size())
	}
}

func TestBTreeSequentialKeys(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert sequential keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%d", i)
		tree.Put([]byte(key), []byte(value))
	}

	// Verify all keys in order
	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		key, _, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		expectedKey := fmt.Sprintf("key%010d", count)
		if string(key) != expectedKey {
			t.Errorf("Expected key %s, got %s", expectedKey, string(key))
		}
		count++
	}

	if count != 1000 {
		t.Errorf("Expected 1000 keys, got %d", count)
	}
}

func TestBTreeReverseOrderKeys(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert keys in reverse order
	for i := 999; i >= 0; i-- {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%d", i)
		tree.Put([]byte(key), []byte(value))
	}

	// Verify all keys are in correct order
	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		key, _, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		expectedKey := fmt.Sprintf("key%010d", count)
		if string(key) != expectedKey {
			t.Errorf("Expected key %s, got %s", expectedKey, string(key))
		}
		count++
	}

	if count != 1000 {
		t.Errorf("Expected 1000 keys, got %d", count)
	}
}

func TestBTreeRandomKeys(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert keys in random order
	keys := []int{5, 2, 8, 1, 9, 3, 7, 4, 6, 0}
	for _, i := range keys {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		tree.Put([]byte(key), []byte(value))
	}

	// Verify all keys exist
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		val, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("Wrong value for key %s", key)
		}
	}
}

func TestBTreeScanRange(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert keys a-z
	for i := 0; i < 26; i++ {
		key := string('a' + byte(i))
		tree.Put([]byte(key), []byte(key))
	}

	// Scan range d-m
	iter, _ := tree.Scan([]byte("d"), []byte("m"))
	defer iter.Close()

	expected := 'd'
	count := 0
	for iter.HasNext() {
		key, _, err := iter.Next()
		if err != nil {
			t.Fatalf("Iterator error: %v", err)
		}
		if key[0] != byte(expected) {
			t.Errorf("Expected key %c, got %s", expected, string(key))
		}
		expected++
		count++
	}

	if count != 10 { // d, e, f, g, h, i, j, k, l, m
		t.Errorf("Expected 10 keys in range, got %d", count)
	}
}

func TestBTreeScanEmptyRange(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert some keys
	tree.Put([]byte("a"), []byte("a"))
	tree.Put([]byte("c"), []byte("c"))
	tree.Put([]byte("e"), []byte("e"))

	// Scan empty range (b-d, only c should match)
	iter, _ := tree.Scan([]byte("b"), []byte("d"))
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		key, _, _ := iter.Next()
		if string(key) != "c" {
			t.Errorf("Expected only key 'c' in range, got %s", string(key))
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 key in range, got %d", count)
	}
}

func TestBTreeSizeAfterOperations(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Empty tree
	if tree.Size() != 0 {
		t.Errorf("Expected size 0 for empty tree, got %d", tree.Size())
	}

	// Add keys
	for i := 0; i < 100; i++ {
		tree.Put([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
		if tree.Size() != i+1 {
			t.Errorf("Expected size %d after %d inserts, got %d", i+1, i+1, tree.Size())
		}
	}

	// Delete half
	for i := 0; i < 50; i++ {
		tree.Delete([]byte(fmt.Sprintf("key%d", i)))
	}
	if tree.Size() != 50 {
		t.Errorf("Expected size 50 after deletes, got %d", tree.Size())
	}

	// Delete rest
	for i := 50; i < 100; i++ {
		tree.Delete([]byte(fmt.Sprintf("key%d", i)))
	}
	if tree.Size() != 0 {
		t.Errorf("Expected size 0 after all deletes, got %d", tree.Size())
	}
}

func TestBTreeIteratorClose(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert some keys
	for i := 0; i < 100; i++ {
		tree.Put([]byte(fmt.Sprintf("key%d", i)), []byte("value"))
	}

	// Create and close iterator without reading all
	iter, _ := tree.Scan(nil, nil)
	iter.Close()

	// Should be safe to close again
	iter.Close()
}

func TestBTreeIteratorEmptyTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	if iter.HasNext() {
		t.Error("Iterator on empty tree should not have next")
	}
}

func TestBTreeConcurrentReads(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)

	// Insert data
	for i := 0; i < 1000; i++ {
		tree.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)))
	}

	// Concurrent reads
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key%d", (id*100+j)%1000)
				tree.Get([]byte(key))
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
