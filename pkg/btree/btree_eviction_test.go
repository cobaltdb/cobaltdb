package btree

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestBTree(t *testing.T, memLimit int64) *BTree {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	t.Cleanup(func() { pool.Close() })
	bt, err := NewBTreeWithLimit(pool, memLimit)
	if err != nil {
		t.Fatal(err)
	}
	return bt
}

// TestEvictionPreservesData verifies that evicted entries are still accessible via Get
func TestEvictionPreservesData(t *testing.T) {
	// Use a very small memory limit to force eviction
	bt := newTestBTree(t, 200)

	// Insert enough data to trigger eviction
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	// Some entries should have been evicted
	if len(bt.evictedKeys) == 0 {
		t.Log("No eviction occurred - memory limit may be too high")
	}

	// Verify ALL entries are still accessible (even evicted ones)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		expectedVal := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		val, err := bt.Get([]byte(key))
		if err != nil {
			t.Errorf("Get %s failed: %v (entry may have been lost during eviction)", key, err)
			continue
		}
		if string(val) != expectedVal {
			t.Errorf("Get %s = %q, want %q", key, val, expectedVal)
		}
	}
}

// TestEvictionScanIncludesEvictedEntries verifies Scan returns evicted entries
func TestEvictionScanIncludesEvictedEntries(t *testing.T) {
	bt := newTestBTree(t, 200)

	// Insert data to trigger eviction
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	// Scan should return ALL entries
	iter, err := bt.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	count := 0
	for {
		k, v, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if k == nil {
			break
		}
		_ = v
		count++
	}

	if count != 10 {
		t.Errorf("Scan returned %d entries, want 10", count)
	}
}

// TestEvictionSizeCountsEvicted verifies Size includes evicted entries
func TestEvictionSizeCountsEvicted(t *testing.T) {
	bt := newTestBTree(t, 200)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	size := bt.Size()
	if size != 10 {
		t.Errorf("Size = %d, want 10", size)
	}
}

// TestEvictionDeleteEvictedKey verifies Delete works for evicted entries
func TestEvictionDeleteEvictedKey(t *testing.T) {
	bt := newTestBTree(t, 200)

	// Insert data to trigger eviction
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	// Find an evicted key
	var evictedKey string
	for k := range bt.evictedKeys {
		evictedKey = k
		break
	}

	if evictedKey == "" {
		t.Skip("No keys were evicted")
	}

	// Delete the evicted key
	err := bt.Delete([]byte(evictedKey))
	if err != nil {
		t.Fatalf("Delete evicted key %s failed: %v", evictedKey, err)
	}

	// Should no longer be accessible
	_, err = bt.Get([]byte(evictedKey))
	if err != ErrKeyNotFound {
		t.Errorf("Get deleted evicted key: expected ErrKeyNotFound, got %v", err)
	}

	// Size should decrease
	if bt.Size() != 9 {
		t.Errorf("Size after delete = %d, want 9", bt.Size())
	}
}

// TestEvictionPutOverwritesEvictedKey verifies Put clears evicted status
func TestEvictionPutOverwritesEvictedKey(t *testing.T) {
	bt := newTestBTree(t, 200)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	var evictedKey string
	for k := range bt.evictedKeys {
		evictedKey = k
		break
	}
	if evictedKey == "" {
		t.Skip("No keys were evicted")
	}

	// Overwrite evicted key — should bring it back to memStorage
	newVal := "updated-value"
	if err := bt.Put([]byte(evictedKey), []byte(newVal)); err != nil {
		t.Fatalf("Put overwrite evicted key failed: %v", err)
	}

	// Should no longer be in evictedKeys
	if bt.evictedKeys[evictedKey] {
		t.Error("Key should not be in evictedKeys after Put overwrite")
	}

	// Should be accessible with new value
	val, err := bt.Get([]byte(evictedKey))
	if err != nil {
		t.Fatalf("Get after overwrite failed: %v", err)
	}
	if string(val) != newVal {
		t.Errorf("Got %q, want %q", val, newVal)
	}
}

// TestEvictionFlushPreservesEvicted verifies flushing doesn't lose evicted data
func TestEvictionFlushPreservesEvicted(t *testing.T) {
	bt := newTestBTree(t, 200)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	// Force a flush
	if err := bt.Flush(); err != nil {
		t.Fatal(err)
	}

	// All entries should still be accessible after flush
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		expectedVal := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		val, err := bt.Get([]byte(key))
		if err != nil {
			t.Errorf("Get %s after flush failed: %v", key, err)
			continue
		}
		if string(val) != expectedVal {
			t.Errorf("Get %s = %q, want %q", key, val, expectedVal)
		}
	}
}

// TestEvictionMultipleFlushCycles verifies data survives multiple flush/modify cycles
func TestEvictionMultipleFlushCycles(t *testing.T) {
	bt := newTestBTree(t, 300)

	// Insert initial data
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	// Flush
	if err := bt.Flush(); err != nil {
		t.Fatal(err)
	}

	// Add more data (may trigger eviction)
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	// Flush again
	if err := bt.Flush(); err != nil {
		t.Fatal(err)
	}

	// Verify all 10 entries are accessible
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		_, err := bt.Get([]byte(key))
		if err != nil {
			t.Errorf("Get %s after multiple flushes failed: %v", key, err)
		}
	}
}

// TestReadKVFromPages verifies the disk read helper
func TestReadKVFromPages(t *testing.T) {
	bt := newTestBTree(t, 0) // unlimited

	// Insert some data and flush
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bt.Flush(); err != nil {
		t.Fatal(err)
	}

	// Read from pages directly
	diskData := bt.readKVFromPages()
	if len(diskData) != 5 {
		t.Errorf("readKVFromPages returned %d entries, want 5", len(diskData))
	}

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%d", i)
		expectedVal := fmt.Sprintf("v%d", i)
		if val, ok := diskData[key]; !ok {
			t.Errorf("missing key %s in disk data", key)
		} else if string(val) != expectedVal {
			t.Errorf("disk data %s = %q, want %q", key, val, expectedVal)
		}
	}
}

// TestReadKVFromPagesEmpty verifies the disk read helper with empty tree
func TestReadKVFromPagesEmpty(t *testing.T) {
	bt := newTestBTree(t, 0)
	diskData := bt.readKVFromPages()
	if len(diskData) != 0 {
		t.Errorf("readKVFromPages on empty tree returned %d entries", len(diskData))
	}
}

// TestEvictionScanWithRange verifies range scan includes evicted entries in range
func TestEvictionScanWithRange(t *testing.T) {
	bt := newTestBTree(t, 200)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%03d", i)
		val := fmt.Sprintf("value%03d-padding-to-make-it-bigger", i)
		if err := bt.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	// Scan a range
	iter, err := bt.Scan([]byte("key003"), []byte("key007"))
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	count := 0
	for {
		k, _, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if k == nil {
			break
		}
		count++
	}

	if count != 5 {
		t.Errorf("Range scan returned %d entries, want 5 (keys 003-007)", count)
	}
}
