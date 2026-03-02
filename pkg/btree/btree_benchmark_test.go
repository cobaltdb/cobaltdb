package btree

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// BenchmarkBTreePut benchmarks single Put operations
func BenchmarkBTreePut(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		b.Fatalf("Failed to create B-tree: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%010d", i)
		value := fmt.Sprintf("value_%010d", i)
		tree.Put([]byte(key), []byte(value))
	}
}

// BenchmarkBTreeGet benchmarks single Get operations
func BenchmarkBTreeGet(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		b.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%010d", i)
		value := fmt.Sprintf("value_%010d", i)
		tree.Put([]byte(key), []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%010d", i%10000)
		tree.Get([]byte(key))
	}
}

// BenchmarkBTreePutGet benchmarks mixed Put and Get operations
func BenchmarkBTreePutGet(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		b.Fatalf("Failed to create B-tree: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%010d", i)
		value := fmt.Sprintf("value_%010d", i)
		tree.Put([]byte(key), []byte(value))
		tree.Get([]byte(key))
	}
}

// BenchmarkBTreeDelete benchmarks Delete operations
func BenchmarkBTreeDelete(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		b.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert data
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%010d", i)
		value := fmt.Sprintf("value_%010d", i)
		tree.Put([]byte(key), []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%010d", i)
		tree.Delete([]byte(key))
	}
}

// BenchmarkBTreeScan benchmarks Scan operations
func BenchmarkBTreeScan(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		b.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%010d", i)
		value := fmt.Sprintf("value_%010d", i)
		tree.Put([]byte(key), []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := tree.Scan([]byte("key_0000000000"), []byte("key_0000001000"))
		if iter != nil {
			iter.Close()
		}
	}
}

// BenchmarkBTreeLargeDataset benchmarks operations on large dataset
func BenchmarkBTreeLargeDataset(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Put_%d", size), func(b *testing.B) {
			backend := storage.NewMemory()
			pool := storage.NewBufferPool(1000, backend)
			defer pool.Close()

			tree, _ := NewBTree(pool)

			b.ResetTimer()
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("key_%010d", i)
				value := fmt.Sprintf("value_%010d", i)
				tree.Put([]byte(key), []byte(value))
			}
		})
	}
}

// BenchmarkBTreeRandomAccess benchmarks random access patterns
func BenchmarkBTreeRandomAccess(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		b.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%010d", i)
		value := fmt.Sprintf("value_%010d", i)
		tree.Put([]byte(key), []byte(value))
	}

	r := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := r.Intn(10000)
		key := fmt.Sprintf("key_%010d", idx)
		tree.Get([]byte(key))
	}
}

// BenchmarkBTreeSequentialAccess benchmarks sequential access patterns
func BenchmarkBTreeSequentialAccess(b *testing.B) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		b.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert data
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%010d", i)
		value := fmt.Sprintf("value_%010d", i)
		tree.Put([]byte(key), []byte(value))
	}

	b.ResetTimer()
	iter, _ := tree.Scan(nil, nil)
	if iter != nil {
		defer iter.Close()
		count := 0
		for iter.HasNext() && count < b.N {
			iter.Next()
			count++
		}
	}
}
