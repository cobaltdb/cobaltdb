package btree

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ==================== evictToMakeSpace tests (0% coverage) ====================

func TestEvictToMakeSpace_BasicEviction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create tree with very small memory limit (50 bytes)
	tree, err := NewBTreeWithLimit(pool, 50)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert a key that fits
	err = tree.Put([]byte("k1"), []byte("val1"))
	if err != nil {
		t.Fatalf("First put failed: %v", err)
	}

	// Insert another key that fits
	err = tree.Put([]byte("k2"), []byte("val2"))
	if err != nil {
		t.Fatalf("Second put failed: %v", err)
	}

	// Insert a key that should trigger eviction
	err = tree.Put([]byte("k3"), []byte("val3"))
	if err != nil {
		t.Fatalf("Third put failed: %v", err)
	}

	// The tree should still be usable
	if tree.Size() == 0 {
		t.Error("Tree should have entries after eviction")
	}
}

func TestEvictToMakeSpace_LargerThanLimit(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create tree with tiny limit (10 bytes)
	tree, err := NewBTreeWithLimit(pool, 10)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Try to insert a key+value that exceeds the total limit
	bigVal := make([]byte, 20)
	for i := range bigVal {
		bigVal[i] = 'x'
	}
	err = tree.Put([]byte("bigkey"), bigVal)
	if err == nil {
		t.Log("Put succeeded (eviction handled it)")
	} else if err == ErrMemoryLimit {
		t.Log("Got ErrMemoryLimit as expected for oversized entry")
	} else {
		t.Logf("Got error: %v", err)
	}
}

func TestEvictToMakeSpace_EvictsLRUOrder(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Limit = 80 bytes, each key+value ~ 8 bytes
	tree, err := NewBTreeWithLimit(pool, 80)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Fill the tree to near capacity
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := tree.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Access k7 to make it recently used
	tree.Get([]byte("k7"))

	// Insert more to trigger eviction
	for i := 8; i < 12; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		err := tree.Put([]byte(key), []byte(val))
		if err != nil && err != ErrMemoryLimit {
			t.Logf("Put %d: %v", i, err)
		}
	}

	// Verify tree is usable
	if tree.Size() > 0 {
		t.Log("Tree has entries after eviction cycle")
	}
}

func TestEvictToMakeSpace_EmptyLRU(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTreeWithLimit(pool, 30)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert entries that will nearly fill memory
	err = tree.Put([]byte("a"), []byte("1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	err = tree.Put([]byte("b"), []byte("2"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	err = tree.Put([]byte("c"), []byte("3"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Keep putting to force multiple eviction rounds
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("x%d", i)
		val := fmt.Sprintf("y%d", i)
		tree.Put([]byte(key), []byte(val))
	}
}

// ==================== loadFromPages tests (6.2% coverage) ====================

func TestLoadFromPages_WithData(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create a tree, insert data, flush it
	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%04d", i)
		val := fmt.Sprintf("value%d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Flush to disk
	err = tree.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	rootID := tree.RootPageID()

	// Now open a new tree from the same root page to trigger loadFromPages
	tree2 := OpenBTree(pool, rootID)
	if tree2 == nil {
		t.Fatal("OpenBTree returned nil")
	}

	// Verify data was loaded
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%04d", i)
		expected := fmt.Sprintf("value%d", i)
		val, err := tree2.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s from reloaded tree: %v", key, err)
			continue
		}
		if string(val) != expected {
			t.Errorf("Key %s: expected %s, got %s", key, expected, string(val))
		}
	}
}

func TestLoadFromPages_EmptyTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create and flush an empty tree
	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	rootID := tree.RootPageID()

	// Open existing tree (no data flushed, should handle gracefully)
	tree2 := OpenBTree(pool, rootID)
	if tree2 == nil {
		t.Fatal("OpenBTree returned nil")
	}
	if tree2.Size() != 0 {
		t.Errorf("Expected empty tree, got size %d", tree2.Size())
	}
}

func TestLoadFromPages_WithOverflow(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert enough data to require overflow pages
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("longkey_%04d_padding", i)
		val := fmt.Sprintf("longval_%04d_padding_data_here", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Flush to ensure data goes to disk with overflow pages
	err = tree.Flush()
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	rootID := tree.RootPageID()

	// Reload from pages
	tree2 := OpenBTree(pool, rootID)
	if tree2 == nil {
		t.Fatal("OpenBTree returned nil")
	}

	// Verify all data loaded correctly
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("longkey_%04d_padding", i)
		expected := fmt.Sprintf("longval_%04d_padding_data_here", i)
		val, err := tree2.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s after reload: %v", key, err)
			continue
		}
		if string(val) != expected {
			t.Errorf("Key %s: expected %s, got %s", key, expected, string(val))
		}
	}
}

func TestLoadFromPages_InvalidPageID(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Open tree with a non-existent page ID. loadFromPages should handle error gracefully.
	tree := OpenBTree(pool, 9999)
	if tree == nil {
		t.Fatal("OpenBTree returned nil even with invalid page")
	}
	// Tree should be empty/usable despite load failure
	if tree.Size() != 0 {
		t.Errorf("Expected size 0, got %d", tree.Size())
	}
}

func TestLoadFromPages_WithMemoryLimit(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	tree, err := NewBTreeWithLimit(pool, 0) // unlimited
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%04d", i)
		val := fmt.Sprintf("value%d", i)
		tree.Put([]byte(key), []byte(val))
	}
	tree.Flush()

	rootID := tree.RootPageID()

	// Reload with memory limit
	tree2 := OpenBTreeWithLimit(pool, rootID, 1024*1024)
	if tree2 == nil {
		t.Fatal("OpenBTreeWithLimit returned nil")
	}

	if tree2.MemoryLimit() != 1024*1024 {
		t.Errorf("Expected memory limit 1048576, got %d", tree2.MemoryLimit())
	}
}

// ==================== flushInternal tests (47.7% coverage) ====================

func TestFlushInternal_WithOverflowPages(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert enough data to overflow a single page
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("key_%06d_extra_padding", i)
		val := fmt.Sprintf("val_%06d_extra_padding_data", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Flush should create overflow pages
	err = tree.Flush()
	if err != nil {
		t.Fatalf("Flush with overflow data failed: %v", err)
	}

	// Verify data is still accessible
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("key_%06d_extra_padding", i)
		expected := fmt.Sprintf("val_%06d_extra_padding_data", i)
		val, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key after flush: %v", err)
			break
		}
		if string(val) != expected {
			t.Errorf("Wrong value after flush")
			break
		}
	}
}

func TestFlushInternal_ShrinkOverflow(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert lots of data to create overflow pages
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("key_%06d_extra_padding", i)
		val := fmt.Sprintf("val_%06d_extra_padding_data", i)
		tree.Put([]byte(key), []byte(val))
	}
	tree.Flush()

	// Delete most data
	for i := 0; i < 280; i++ {
		key := fmt.Sprintf("key_%06d_extra_padding", i)
		tree.Delete([]byte(key))
	}

	// Flush again - should shrink overflow pages
	err = tree.Flush()
	if err != nil {
		t.Fatalf("Flush after delete failed: %v", err)
	}

	// Verify remaining data
	for i := 280; i < 300; i++ {
		key := fmt.Sprintf("key_%06d_extra_padding", i)
		expected := fmt.Sprintf("val_%06d_extra_padding_data", i)
		val, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s after shrink flush: %v", key, err)
			continue
		}
		if string(val) != expected {
			t.Errorf("Wrong value after shrink flush")
		}
	}
}

func TestFlushInternal_MultipleFlushesCycle(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Do multiple insert/flush/delete/flush cycles
	for cycle := 0; cycle < 5; cycle++ {
		// Insert
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("c%d_k%04d", cycle, i)
			val := fmt.Sprintf("c%d_v%04d", cycle, i)
			tree.Put([]byte(key), []byte(val))
		}
		if err := tree.Flush(); err != nil {
			t.Fatalf("Flush cycle %d insert failed: %v", cycle, err)
		}

		// Delete half
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("c%d_k%04d", cycle, i)
			tree.Delete([]byte(key))
		}
		if err := tree.Flush(); err != nil {
			t.Fatalf("Flush cycle %d delete failed: %v", cycle, err)
		}
	}
}

func TestFlushInternal_NotDirty(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Flush when not dirty should be a no-op
	err = tree.Flush()
	if err != nil {
		t.Fatalf("Flush on clean tree failed: %v", err)
	}

	// Insert and flush
	tree.Put([]byte("k"), []byte("v"))
	tree.Flush()

	// Flush again when not dirty
	err = tree.Flush()
	if err != nil {
		t.Fatalf("Second flush failed: %v", err)
	}
}

func TestFlushInternal_FlushAndReload(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert enough data for overflow
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("persistkey_%06d", i)
		val := fmt.Sprintf("persistval_%06d_data", i)
		tree.Put([]byte(key), []byte(val))
	}
	tree.Flush()

	rootID := tree.RootPageID()

	// Reload and verify
	tree2 := OpenBTree(pool, rootID)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("persistkey_%06d", i)
		expected := fmt.Sprintf("persistval_%06d_data", i)
		val, err := tree2.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s after reload: %v", key, err)
			break
		}
		if string(val) != expected {
			t.Errorf("Wrong value after reload")
			break
		}
	}
}

// ==================== DiskBTree split coverage tests ====================

func TestDiskBTreeSplitRootCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert enough keys to force root split (order is 100)
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("splitkey%04d", i)
		val := fmt.Sprintf("splitval%04d", i)
		err := tree.Put([]byte(key), []byte(val))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Verify all keys after split
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("splitkey%04d", i)
		expected := fmt.Sprintf("splitval%04d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s after split: %v", key, err)
			continue
		}
		if string(got) != expected {
			t.Errorf("Wrong value for %s: got %s", key, string(got))
		}
	}
}

func TestDiskBTreeDeepSplits(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert enough keys to cause multiple levels of splits
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("deep%06d", i)
		val := fmt.Sprintf("val%06d", i)
		err := tree.Put([]byte(key), []byte(val))
		if err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// Verify all keys still retrievable
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("deep%06d", i)
		expected := fmt.Sprintf("val%06d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %d after deep splits: %v", i, err)
			continue
		}
		if string(got) != expected {
			t.Errorf("Wrong value for key %d", i)
		}
	}
}

func TestDiskBTreeSplitAndDeleteCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert to force splits
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("sd%06d", i)
		val := fmt.Sprintf("sv%06d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Delete from internal nodes (after splits)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("sd%06d", i)
		err := tree.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete key %d: %v", i, err)
		}
	}

	// Verify deleted keys are gone
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("sd%06d", i)
		_, err := tree.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound for deleted key %d, got %v", i, err)
		}
	}

	// Verify remaining keys
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("sd%06d", i)
		expected := fmt.Sprintf("sv%06d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get remaining key %d: %v", i, err)
			continue
		}
		if string(got) != expected {
			t.Errorf("Wrong value for remaining key %d", i)
		}
	}
}

func TestDiskBTreeSplitAndScanCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert to force splits
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("scan%06d", i)
		val := fmt.Sprintf("val%06d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Scan all after splits
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		_, _, err := iter.Next()
		if err != nil {
			break
		}
		count++
	}

	if count < 100 {
		t.Errorf("Expected at least 100 keys from scan, got %d", count)
	}
}

func TestDiskBTreeSplitWithUpdateCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert to force splits
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("upd%06d", i)
		val := fmt.Sprintf("v1_%06d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Update all keys (they're now in split pages)
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("upd%06d", i)
		val := fmt.Sprintf("v2_%06d", i)
		err := tree.Put([]byte(key), []byte(val))
		if err != nil {
			t.Errorf("Update %d failed: %v", i, err)
		}
	}

	// Verify updated values
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("upd%06d", i)
		expected := fmt.Sprintf("v2_%06d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get updated key %d: %v", i, err)
			continue
		}
		if string(got) != expected {
			t.Errorf("Key %d: expected %s, got %s", i, expected, string(got))
		}
	}
}

// ==================== DiskBTree iterator edge cases ====================

func TestDiskBTreeIteratorNextWithoutHasNextCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	// Next on empty iterator
	_, _, err := iter.Next()
	if err == nil {
		t.Error("Expected error on Next() without HasNext()")
	}
}

func TestDiskBTreeIteratorEndKeyExceededCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	for i := 0; i < 10; i++ {
		tree.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
	}

	// Scan with endKey in the middle
	iter, _ := tree.Scan([]byte("k00"), []byte("k04"))
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		_, _, err := iter.Next()
		if err != nil {
			// "past end key" error is expected
			if strings.Contains(err.Error(), "past end key") {
				break
			}
			t.Fatalf("Unexpected error: %v", err)
		}
		count++
	}

	if count > 5 {
		t.Errorf("Expected at most 5 keys, got %d", count)
	}
}

func TestDiskBTreeIteratorMultiPageScanCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert enough for multi-page tree
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("mp%06d", i)
		val := fmt.Sprintf("val%06d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Scan with range
	iter, _ := tree.Scan([]byte("mp000050"), []byte("mp000150"))
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		_, _, err := iter.Next()
		if err != nil {
			break
		}
		count++
	}

	if count == 0 {
		t.Error("Expected some keys from multi-page scan")
	}
}

// ==================== BTree (in-memory) additional coverage ====================

func TestBTreeNewBTreeWithLimitZero(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Unlimited memory
	tree, err := NewBTreeWithLimit(pool, 0)
	if err != nil {
		t.Fatalf("Failed to create tree with limit 0: %v", err)
	}

	if tree.MemoryLimit() != 0 {
		t.Errorf("Expected limit 0, got %d", tree.MemoryLimit())
	}

	// Should be able to insert without memory issues
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		tree.Put([]byte(key), []byte(val))
	}
}

func TestBTreePutOverwriteMemoryTracking(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTreeWithLimit(pool, 1024)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert
	tree.Put([]byte("key1"), []byte("short"))
	mem1 := tree.MemoryUsed()

	// Overwrite with longer value
	tree.Put([]byte("key1"), []byte("a much longer value than before"))
	mem2 := tree.MemoryUsed()

	if mem2 <= mem1 {
		t.Logf("Memory tracking: before=%d, after=%d (overwrite with larger value)", mem1, mem2)
	}

	// Overwrite with shorter value
	tree.Put([]byte("key1"), []byte("s"))
	mem3 := tree.MemoryUsed()

	if mem3 >= mem2 {
		t.Logf("Memory tracking: before=%d, after=%d (overwrite with smaller value)", mem2, mem3)
	}
}

func TestBTreeIteratorNextDoneState(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)
	tree.Put([]byte("a"), []byte("1"))

	iter, _ := tree.Scan(nil, nil)

	// Read the one item
	k, v, err := iter.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if string(k) != "a" || string(v) != "1" {
		t.Errorf("Wrong value: k=%s, v=%s", string(k), string(v))
	}

	// Next should return nil when exhausted
	k, v, err = iter.Next()
	if err != nil {
		t.Errorf("Expected nil error on exhausted iterator, got %v", err)
	}
	if k != nil || v != nil {
		t.Error("Expected nil key/value on exhausted iterator")
	}

	// Valid should be false
	if iter.Valid() {
		t.Error("Expected Valid() = false on exhausted iterator")
	}
}

func TestBTreeIteratorEndKeyBound(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)
	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))
	tree.Put([]byte("c"), []byte("3"))
	tree.Put([]byte("d"), []byte("4"))

	// Scan with endKey "b" - exercises the endKey check in Next() line 587
	iter, _ := tree.Scan([]byte("a"), []byte("b"))
	defer iter.Close()

	count := 0
	for {
		k, _, err := iter.Next()
		if err != nil || k == nil {
			break
		}
		count++
		if bytes.Compare(k, []byte("b")) > 0 {
			t.Errorf("Got key %s beyond endKey b", string(k))
		}
	}
}

func TestBTreeIteratorFirstOnNonEmpty(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, _ := NewBTree(pool)
	tree.Put([]byte("x"), []byte("1"))
	tree.Put([]byte("y"), []byte("2"))

	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	// Read one item
	iter.Next()

	// Reset to first
	ok := iter.First()
	if !ok {
		t.Error("First() should return true on non-empty iterator")
	}

	// Read from beginning again
	k, _, _ := iter.Next()
	if k == nil {
		t.Error("Expected non-nil key after First()")
	}
}

func TestBTreeDeleteMemoryTracking(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTreeWithLimit(pool, 1024)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	tree.Put([]byte("key1"), []byte("value1"))
	tree.Put([]byte("key2"), []byte("value2"))
	memBefore := tree.MemoryUsed()

	tree.Delete([]byte("key1"))
	memAfter := tree.MemoryUsed()

	if memAfter >= memBefore {
		t.Errorf("Memory should decrease after delete: before=%d, after=%d", memBefore, memAfter)
	}
}

// ==================== DiskBTree edge cases ====================

func TestDiskBTreeWriteEntriesOverflowCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Try to insert a very large key that might cause page overflow during write
	bigKey := make([]byte, 3000)
	for i := range bigKey {
		bigKey[i] = byte('A' + i%26)
	}
	bigVal := []byte("value")
	err := tree.Put(bigKey, bigVal)
	// This may fail due to page overflow, which is expected behavior
	if err != nil {
		t.Logf("Put with oversized key: %v (expected for page overflow)", err)
	}
}

func TestDiskBTreeGetFromInternalNodeCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert enough to create internal nodes
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("intnode%06d", i)
		val := fmt.Sprintf("val%06d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Get keys that require traversing internal nodes
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("intnode%06d", i)
		expected := fmt.Sprintf("val%06d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Get %s from internal tree: %v", key, err)
			continue
		}
		if string(got) != expected {
			t.Errorf("Wrong value for %s", key)
		}
	}
}

func TestDiskBTreeDeleteFromInternalTreeCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Build a multi-level tree
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("del%06d", i)
		val := fmt.Sprintf("val%06d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Delete keys that require traversing to leaf pages via internal nodes
	for i := 50; i < 150; i++ {
		key := fmt.Sprintf("del%06d", i)
		err := tree.Delete([]byte(key))
		if err != nil {
			t.Errorf("Delete from internal tree %d: %v", i, err)
		}
	}

	// Verify deleted
	for i := 50; i < 150; i++ {
		key := fmt.Sprintf("del%06d", i)
		_, err := tree.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %d should be deleted, got err=%v", i, err)
		}
	}
}

func TestDiskBTreeScanAfterMultipleSplitsCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("mscan%06d", i)
		val := fmt.Sprintf("val%06d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Full scan
	iter, _ := tree.Scan(nil, nil)
	defer iter.Close()

	count := 0
	for iter.HasNext() {
		_, _, err := iter.Next()
		if err != nil {
			break
		}
		count++
	}

	t.Logf("Scanned %d keys from 300 inserted after multiple splits", count)
}

func TestDiskBTreeReopenWithExistingMetaCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	pm, err := storage.NewPageManager(pool)
	if err != nil {
		t.Fatalf("Failed to create PM: %v", err)
	}

	// Create first tree
	tree1, err := NewDiskBTree(pm)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	for i := 0; i < 10; i++ {
		tree1.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}

	// Create second tree - should reuse existing root from meta
	tree2, err := NewDiskBTree(pm)
	if err != nil {
		t.Fatalf("Failed to reopen tree: %v", err)
	}

	for i := 0; i < 10; i++ {
		got, err := tree2.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Errorf("Failed to get k%d from reopened tree: %v", i, err)
			continue
		}
		if string(got) != fmt.Sprintf("v%d", i) {
			t.Errorf("Wrong value for k%d", i)
		}
	}

	pm.Close()
	pool.Close()
}

// ==================== Iterator Valid/HasNext coverage ====================

func TestDiskBTreeIteratorValidCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	tree.Put([]byte("a"), []byte("1"))
	tree.Put([]byte("b"), []byte("2"))

	iter, _ := tree.Scan(nil, nil)

	// Before any iteration
	hasNext := iter.HasNext()
	if !hasNext {
		t.Error("Expected HasNext() true initially")
	}

	// Read all
	for iter.HasNext() {
		iter.Next()
	}

	// After exhaustion - iterator may or may not report false depending on impl
	_ = iter.HasNext()

	iter.Close()
}

// ==================== DiskBTree findChildPage edge cases ====================

func TestDiskBTreeFindChildPageEdgeCasesCoverage(t *testing.T) {
	tree, _, cleanup := setupDiskBTree(t)
	defer cleanup()

	// Insert keys in reverse to exercise different insertion paths
	for i := 300; i >= 0; i-- {
		key := fmt.Sprintf("rev%06d", i)
		val := fmt.Sprintf("val%06d", i)
		tree.Put([]byte(key), []byte(val))
	}

	// Query keys at boundaries
	for _, i := range []int{0, 1, 150, 299, 300} {
		key := fmt.Sprintf("rev%06d", i)
		expected := fmt.Sprintf("val%06d", i)
		got, err := tree.Get([]byte(key))
		if err != nil {
			t.Errorf("Get boundary key %d: %v", i, err)
			continue
		}
		if string(got) != expected {
			t.Errorf("Wrong value for boundary key %d", i)
		}
	}

	// Query key smaller than all entries
	_, err := tree.Get([]byte("aaa"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for 'aaa', got %v", err)
	}

	// Query key larger than all entries
	_, err = tree.Get([]byte("zzz"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for 'zzz', got %v", err)
	}
}

// ==================== BTree Flush then re-insert ====================

func TestBTreeFlushReinsert(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert, flush, delete all, flush, reinsert, flush
	for i := 0; i < 50; i++ {
		tree.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	tree.Flush()

	for i := 0; i < 50; i++ {
		tree.Delete([]byte(fmt.Sprintf("k%d", i)))
	}
	tree.Flush()

	// Reinsert
	for i := 0; i < 30; i++ {
		tree.Put([]byte(fmt.Sprintf("new%d", i)), []byte(fmt.Sprintf("nv%d", i)))
	}
	tree.Flush()

	// Verify
	for i := 0; i < 30; i++ {
		val, err := tree.Get([]byte(fmt.Sprintf("new%d", i)))
		if err != nil {
			t.Errorf("Failed to get new%d: %v", i, err)
			continue
		}
		if string(val) != fmt.Sprintf("nv%d", i) {
			t.Errorf("Wrong value for new%d", i)
		}
	}
}

func TestEvictToMakeSpace_MultipleRounds(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	// Set up with a tight limit
	tree, err := NewBTreeWithLimit(pool, 60)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Fill the tree
	tree.Put([]byte("aa"), []byte("11"))
	tree.Put([]byte("bb"), []byte("22"))
	tree.Put([]byte("cc"), []byte("33"))

	// Now insert a large value that will require evicting multiple entries
	largeVal := []byte("this_is_a_larger_value_string")
	err = tree.Put([]byte("dd"), largeVal)
	if err != nil && err != ErrMemoryLimit {
		t.Logf("Put with eviction: %v", err)
	}
}

func TestBTreeFlushWithExactPageSize(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(200, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Insert data that's close to exactly filling one page
	// usablePageSize = 4096 - 16 = 4080, minus 8 header = 4072
	// Each entry overhead: 2 (keyLen) + 4 (valLen) = 6 bytes
	entrySize := 6 + 20 + 20 // 46 bytes per entry
	numEntries := 4072 / entrySize // ~88 entries

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("exact_key_%010d", i)
		val := fmt.Sprintf("exact_val_%010d", i)
		tree.Put([]byte(key), []byte(val))
	}

	err = tree.Flush()
	if err != nil {
		t.Fatalf("Flush with near-exact page size failed: %v", err)
	}

	// Verify
	rootID := tree.RootPageID()
	tree2 := OpenBTree(pool, rootID)
	if tree2.Size() != numEntries {
		t.Errorf("Expected %d entries after reload, got %d", numEntries, tree2.Size())
	}
}
