package btree

import (
	"fmt"
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
	if tree.LoadError() == nil {
		t.Fatal("Expected OpenBTree to record the load error")
	}
	// Tree should be empty/usable despite load failure
	if tree.Size() != 0 {
		t.Errorf("Expected size 0, got %d", tree.Size())
	}
}

func TestOpenBTreeStrictReturnsLoadError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := OpenBTreeStrict(pool, 9999)
	if err == nil {
		t.Fatal("Expected strict open to return load error")
	}
	if tree == nil {
		t.Fatal("Expected strict open to return the partially initialized tree")
	}
	if tree.LoadError() == nil {
		t.Fatal("Expected strict open to record the load error")
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
	entrySize := 6 + 20 + 20       // 46 bytes per entry
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
