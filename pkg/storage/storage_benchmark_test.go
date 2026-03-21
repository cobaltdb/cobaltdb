package storage

import (
	"fmt"
	"testing"
)

// BenchmarkBufferPoolGetPage benchmarks GetPage operations
func BenchmarkBufferPoolGetPage(b *testing.B) {
	const numPages = 500
	backend := NewMemoryWithLimit(64 * 1024 * 1024)
	pool := NewBufferPool(numPages, backend)
	defer pool.Close()

	// Create pages - pool capacity matches page count so no eviction
	for i := 0; i < numPages; i++ {
		page, _ := pool.NewPage(PageTypeLeaf)
		if page != nil {
			pool.Unpin(page)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, _ := pool.GetPage(uint32(i%numPages + 1))
		if page != nil {
			pool.Unpin(page)
		}
	}
}

// BenchmarkBufferPoolNewPage benchmarks NewPage operations.
// Resets pool in batches to prevent unbounded backend growth.
func BenchmarkBufferPoolNewPage(b *testing.B) {
	const batchSize = 5000

	var pool *BufferPool
	reset := func() {
		if pool != nil {
			pool.Close()
		}
		backend := NewMemoryWithLimit(64 * 1024 * 1024)
		pool = NewBufferPool(batchSize, backend)
	}
	reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%batchSize == 0 {
			b.StopTimer()
			reset()
			b.StartTimer()
		}
		page, _ := pool.NewPage(PageTypeLeaf)
		if page != nil {
			pool.Unpin(page)
		}
	}
	b.StopTimer()
	pool.Close()
}

// BenchmarkBufferPoolFlushAll benchmarks FlushAll operations
func BenchmarkBufferPoolFlushAll(b *testing.B) {
	backend := NewMemory()
	pool := NewBufferPool(100, backend)
	defer pool.Close()

	// Create and dirty pages
	for i := 0; i < 100; i++ {
		page, _ := pool.NewPage(PageTypeLeaf)
		if page != nil {
			page.SetDirty(true)
			pool.Unpin(page)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.FlushAll()
	}
}

// BenchmarkWALAppend benchmarks WAL Append operations
func BenchmarkWALAppend(b *testing.B) {
	tempDir := b.TempDir()
	wal, _ := OpenWAL(tempDir + "/test.wal")
	defer wal.Close()

	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := &WALRecord{
			LSN:    uint64(i),
			TxnID:  1,
			Type:   WALInsert,
			PageID: 1,
			Offset: 0,
			Data:   data,
		}
		wal.Append(record)
	}
}

// BenchmarkMemoryWriteAt benchmarks Memory backend WriteAt
func BenchmarkMemoryWriteAt(b *testing.B) {
	mem := NewMemoryWithLimit(64 * 1024 * 1024) // 64MB cap
	data := make([]byte, 4096)
	const maxPages = 10000 // cycle within 40MB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mem.WriteAt(data, int64((i%maxPages)*4096))
	}
}

// BenchmarkMemoryReadAt benchmarks Memory backend ReadAt
func BenchmarkMemoryReadAt(b *testing.B) {
	mem := NewMemory()
	data := make([]byte, 4096)

	// Write data first
	for i := 0; i < 1000; i++ {
		mem.WriteAt(data, int64(i*4096))
	}

	buf := make([]byte, 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mem.ReadAt(buf, int64(i%1000)*4096)
	}
}

// BenchmarkMetaPageSerialize benchmarks MetaPage Serialize
func BenchmarkMetaPageSerialize(b *testing.B) {
	meta := &MetaPage{
		Magic:      [4]byte{'C', 'B', 'D', 'B'},
		Version:    1,
		PageSize:   PageSize,
		PageCount:  100,
		FreeListID: 0,
		RootPageID: 1,
		TxnCounter: 1,
		Checksum:   0,
	}
	data := make([]byte, PageSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meta.Serialize(data)
	}
}

// BenchmarkMetaPageDeserialize benchmarks MetaPage Deserialize
func BenchmarkMetaPageDeserialize(b *testing.B) {
	meta := &MetaPage{
		Magic:      [4]byte{'C', 'B', 'D', 'B'},
		Version:    1,
		PageSize:   PageSize,
		PageCount:  100,
		FreeListID: 0,
		RootPageID: 1,
		TxnCounter: 1,
		Checksum:   0,
	}
	data := make([]byte, PageSize)
	meta.Serialize(data)

	m := &MetaPage{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Deserialize(data)
	}
}

// BenchmarkBufferPoolLargeDataset benchmarks operations on large dataset
func BenchmarkBufferPoolLargeDataset(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Pages_%d", size), func(b *testing.B) {
			backend := NewMemory()
			pool := NewBufferPool(size, backend)
			defer pool.Close()

			// Create pages
			for i := 0; i < size; i++ {
				page, _ := pool.NewPage(PageTypeLeaf)
				if page != nil {
					pool.Unpin(page)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				page, _ := pool.GetPage(uint32(i%size + 1))
				if page != nil {
					pool.Unpin(page)
				}
			}
		})
	}
}
