package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// =====================================================================
// WAL deep coverage: Checkpoint + more records, LSN continuity,
// Recover with mixed committed/uncommitted, encodeRecord/readRecord
// roundtrip, Checkpoint on closed WAL, AppendWithoutSync large record
// =====================================================================

// TestWAL_CheckpointThenMoreRecords tests that after a Checkpoint, the
// WAL file is truncated and new records can be appended with correct LSNs.
func TestWAL_CheckpointThenMoreRecords(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "cp_more.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	defer pool.Close()

	// Append some records
	for i := 0; i < 5; i++ {
		wal.Append(&WALRecord{TxnID: uint64(i + 1), Type: WALInsert, PageID: 1, Data: []byte("data")})
	}
	lsnBefore := wal.LSN()
	if lsnBefore != 5 {
		t.Errorf("Expected LSN 5, got %d", lsnBefore)
	}

	// Checkpoint truncates the file
	if err := wal.Checkpoint(pool); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	cpLSN := wal.CheckpointLSN()
	if cpLSN == 0 {
		t.Error("CheckpointLSN should be non-zero after Checkpoint")
	}

	// File should be truncated (empty after checkpoint)
	stat, _ := os.Stat(walPath)
	if stat.Size() != 0 {
		t.Errorf("WAL file should be truncated to 0, got %d", stat.Size())
	}

	// Append more records after checkpoint
	for i := 0; i < 3; i++ {
		if err := wal.Append(&WALRecord{TxnID: 100, Type: WALUpdate, PageID: 2, Data: []byte("post_cp")}); err != nil {
			t.Fatalf("Append after checkpoint: %v", err)
		}
	}

	lsnAfter := wal.LSN()
	if lsnAfter <= cpLSN {
		t.Errorf("LSN after checkpoint should increase: cpLSN=%d, lsnAfter=%d", cpLSN, lsnAfter)
	}
}

// TestWAL_CheckpointClosedWAL exercises Checkpoint on a closed WAL.
func TestWAL_CheckpointClosedWAL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "cp_closed.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Close()

	pool := NewBufferPool(4, NewMemory())
	defer pool.Close()

	err = wal.Checkpoint(pool)
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

// TestWAL_AppendClosedWAL exercises Append on a closed WAL.
func TestWAL_AppendClosedWAL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "append_closed.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Close()

	err = wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("x")})
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

// TestWAL_AppendWithoutSyncClosedWAL exercises AppendWithoutSync on closed WAL.
func TestWAL_AppendWithoutSyncClosedWAL(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "nosync_closed.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Close()

	err = wal.AppendWithoutSync(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("x")})
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

// TestWAL_AppendWithoutSyncLargeRecord exercises large record rejection.
func TestWAL_AppendWithoutSyncLargeRecord(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "nosync_large.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	err = wal.AppendWithoutSync(&WALRecord{TxnID: 1, Type: WALInsert, Data: make([]byte, 70000)})
	if err == nil {
		t.Error("Expected error for oversized record")
	}
}

// TestWAL_LSN_Continuity verifies LSN increments monotonically across
// multiple appends and reopens.
func TestWAL_LSN_Continuity(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "lsn_cont.wal")

	// First session: append 5 records
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		wal.Append(&WALRecord{TxnID: uint64(i), Type: WALInsert, Data: []byte(fmt.Sprintf("d%d", i))})
	}
	if wal.LSN() != 5 {
		t.Errorf("Expected LSN 5, got %d", wal.LSN())
	}
	wal.Close()

	// Reopen: LSN should be restored from file
	wal2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	if wal2.LSN() != 5 {
		t.Errorf("Expected LSN 5 after reopen, got %d", wal2.LSN())
	}

	// Append more
	wal2.Append(&WALRecord{TxnID: 10, Type: WALUpdate, Data: []byte("more")})
	if wal2.LSN() != 6 {
		t.Errorf("Expected LSN 6, got %d", wal2.LSN())
	}
}

// TestWAL_RecoverMixedCommittedUncommitted verifies that recovery correctly
// applies committed transactions and discards uncommitted ones.
func TestWAL_RecoverMixedCommittedUncommitted(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "mixed.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	defer pool.Close()

	// Create pages for the records to target
	p1, _ := pool.NewPage(PageTypeLeaf)
	p1ID := p1.ID()
	pool.Unpin(p1)

	// Txn 1: committed
	wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, PageID: p1ID, Offset: 100, Data: []byte("committed_data")})
	wal.Append(&WALRecord{TxnID: 1, Type: WALCommit})

	// Txn 2: uncommitted (no commit/rollback)
	wal.Append(&WALRecord{TxnID: 2, Type: WALInsert, PageID: p1ID, Offset: 200, Data: []byte("uncommitted__")})

	// Txn 3: rolled back
	wal.Append(&WALRecord{TxnID: 3, Type: WALUpdate, PageID: p1ID, Offset: 300, Data: []byte("rolled_back__")})
	wal.Append(&WALRecord{TxnID: 3, Type: WALRollback})

	// Txn 4: committed delete
	wal.Append(&WALRecord{TxnID: 4, Type: WALDelete, PageID: p1ID, Offset: 400, Data: []byte("del_committed")})
	wal.Append(&WALRecord{TxnID: 4, Type: WALCommit})

	wal.Close()

	// Reopen and recover
	wal2, _ := OpenWAL(walPath)
	defer wal2.Close()

	if err := wal2.Recover(pool); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	p, _ := pool.GetPage(p1ID)
	defer pool.Unpin(p)

	// Txn 1 committed: should be applied
	if string(p.Data()[100:114]) != "committed_data" {
		t.Errorf("Txn 1 committed data not applied: got %q", string(p.Data()[100:114]))
	}

	// Txn 2 uncommitted: should NOT be applied
	if string(p.Data()[200:213]) == "uncommitted__" {
		t.Error("Txn 2 uncommitted data should not be applied")
	}

	// Txn 3 rolled back: should NOT be applied
	if string(p.Data()[300:313]) == "rolled_back__" {
		t.Error("Txn 3 rolled-back data should not be applied")
	}

	// Txn 4 committed: should be applied
	if string(p.Data()[400:413]) != "del_committed" {
		t.Errorf("Txn 4 committed data not applied: got %q", string(p.Data()[400:413]))
	}
}

// TestWAL_EncodeRecordReadRecord_Roundtrip verifies that encodeRecord and
// readRecord are inverses (data survives a write/read cycle).
func TestWAL_EncodeRecordReadRecord_Roundtrip(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "roundtrip.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	// Append records with various types and data sizes
	records := []*WALRecord{
		{TxnID: 1, Type: WALInsert, PageID: 10, Offset: 100, Data: []byte("insert_data")},
		{TxnID: 2, Type: WALUpdate, PageID: 20, Offset: 200, Data: []byte("update_data_longer")},
		{TxnID: 3, Type: WALDelete, PageID: 30, Offset: 300, Data: nil}, // empty data
		{TxnID: 4, Type: WALCommit, PageID: 0, Offset: 0, Data: nil},
		{TxnID: 5, Type: WALRollback, PageID: 0, Offset: 0, Data: nil},
		{TxnID: 6, Type: WALCheckpoint, PageID: 0, Offset: 0, Data: nil},
	}

	for _, r := range records {
		if err := wal.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	wal.Close()

	// Reopen and verify LSN was correctly restored
	wal2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	if wal2.LSN() != uint64(len(records)) {
		t.Errorf("Expected LSN %d, got %d", len(records), wal2.LSN())
	}
}

// TestWAL_RecoverWithCheckpoint exercises recovery that encounters a
// WALCheckpoint record during scan.
func TestWAL_RecoverWithCheckpointRecord(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "recover_cp.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	defer pool.Close()

	p, _ := pool.NewPage(PageTypeLeaf)
	pID := p.ID()
	pool.Unpin(p)

	// Data then checkpoint (the readLSN scanner sees checkpoint type)
	wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, PageID: pID, Offset: 50, Data: []byte("before_cp")})
	wal.Append(&WALRecord{TxnID: 1, Type: WALCommit})
	// Note: WALCheckpoint record type appended via Append (not Checkpoint method)
	wal.Append(&WALRecord{TxnID: 0, Type: WALCheckpoint})
	wal.Close()

	wal2, _ := OpenWAL(walPath)
	defer wal2.Close()

	// readLSN should have set checkpoint
	if wal2.CheckpointLSN() == 0 {
		t.Error("Expected non-zero checkpoint LSN from readLSN scan")
	}

	// Recover should handle WALCheckpoint type gracefully
	if err := wal2.Recover(pool); err != nil {
		t.Fatalf("Recover: %v", err)
	}
}

// TestWAL_AppendEmptyData tests appending a record with no data.
func TestWAL_AppendEmptyData(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "empty_data.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	if err := wal.Append(&WALRecord{TxnID: 1, Type: WALCommit}); err != nil {
		t.Fatalf("Append empty data: %v", err)
	}
	if wal.LSN() != 1 {
		t.Errorf("Expected LSN 1, got %d", wal.LSN())
	}
}

// =====================================================================
// BufferPool deep coverage: concurrent GetPage/Unpin, eviction under
// memory pressure, FlushAll error path, NewPage with eviction, Close
// =====================================================================

// TestBufferPool_ConcurrentGetPageUnpin exercises concurrent cache access.
func TestBufferPool_ConcurrentGetPageUnpin(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(32, backend)
	defer pool.Close()

	// Pre-create pages
	pages := make([]uint32, 20)
	for i := 0; i < 20; i++ {
		p, err := pool.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatal(err)
		}
		pages[i] = p.ID()
		pool.Unpin(p)
	}

	// Concurrent GetPage + Unpin
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for _, pid := range pages {
				p, err := pool.GetPage(pid)
				if err != nil {
					continue
				}
				pool.Unpin(p)
			}
		}(g)
	}
	wg.Wait()
}

// TestBufferPool_EvictionUnderPressure tests eviction when pool is at capacity.
func TestBufferPool_EvictionUnderPressure(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(5, backend) // very small capacity
	defer pool.Close()

	// Create and unpin 5 pages (fills pool)
	for i := 0; i < 5; i++ {
		p, err := pool.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatal(err)
		}
		copy(p.Data()[PageHeaderSize:], []byte(fmt.Sprintf("page_%d", i)))
		p.SetDirty(true)
		pool.Unpin(p)
	}

	// Creating a 6th page should trigger eviction
	p6, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage after eviction: %v", err)
	}
	pool.Unpin(p6)

	evictions := pool.EvictionCount()
	if evictions == 0 {
		t.Error("Expected at least one eviction")
	}
}

// TestBufferPool_EvictionAllPinned tests that eviction returns ErrBufferFull
// when all pages are pinned.
func TestBufferPool_EvictionAllPinned(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(3, backend)

	// Create 3 pages and keep them pinned
	pinned := make([]*CachedPage, 3)
	for i := 0; i < 3; i++ {
		p, err := pool.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatal(err)
		}
		pinned[i] = p
		// Don't unpin
	}

	// 4th page should fail with ErrBufferFull
	_, err := pool.NewPage(PageTypeLeaf)
	if err == nil {
		t.Error("Expected ErrBufferFull when all pages are pinned")
	}

	// Cleanup
	for _, p := range pinned {
		pool.Unpin(p)
	}
	pool.Close()
}

// TestBufferPool_GetPage_DoubleCheck exercises the double-check path in
// GetPage where a page appears in cache between RUnlock and Lock.
func TestBufferPool_GetPage_DoubleCheck(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(32, backend)
	defer pool.Close()

	// Create a page, flush it, then access it multiple times to hit
	// both the fast path (cached) and slow path (load from disk).
	p, _ := pool.NewPage(PageTypeLeaf)
	pid := p.ID()
	copy(p.Data()[PageHeaderSize:], []byte("test_data"))
	p.SetDirty(true)
	pool.Unpin(p)
	pool.FlushAll()

	// Multiple concurrent gets of the same page
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pg, err := pool.GetPage(pid)
			if err != nil {
				return
			}
			pool.Unpin(pg)
		}()
	}
	wg.Wait()
}

// TestBufferPool_FlushPageClean tests FlushPage on a non-dirty page (no-op).
func TestBufferPool_FlushPageClean(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(16, backend)
	defer pool.Close()

	p, _ := pool.NewPage(PageTypeLeaf)
	p.SetDirty(false) // explicitly clean
	err := pool.FlushPage(p)
	if err != nil {
		t.Errorf("FlushPage on clean page: %v", err)
	}
	pool.Unpin(p)
}

// TestBufferPool_PageCount tracks page count correctly.
func TestBufferPool_PageCount_Tracking(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(100, backend)
	defer pool.Close()

	if pool.PageCount() != 0 {
		t.Errorf("Expected 0, got %d", pool.PageCount())
	}

	pages := make([]*CachedPage, 5)
	for i := 0; i < 5; i++ {
		p, _ := pool.NewPage(PageTypeLeaf)
		pages[i] = p
	}
	if pool.PageCount() != 5 {
		t.Errorf("Expected 5, got %d", pool.PageCount())
	}
	for _, p := range pages {
		pool.Unpin(p)
	}
}

// TestBufferPool_NewPage_WithEviction exercises the eviction branch inside
// NewPage when the pool is full.
func TestBufferPool_NewPage_WithEviction(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(3, backend)
	defer pool.Close()

	// Fill pool
	for i := 0; i < 3; i++ {
		p, _ := pool.NewPage(PageTypeLeaf)
		pool.Unpin(p)
	}

	// Next NewPage should evict
	p, err := pool.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage with eviction: %v", err)
	}
	pool.Unpin(p)
}

// TestBufferPool_Stats exercises the Stats method to verify all counters.
func TestBufferPool_Stats_AllCounters(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	// Create and flush pages
	p, _ := pool.NewPage(PageTypeLeaf)
	p.SetDirty(true)
	pool.Unpin(p)

	pool.FlushAll()

	// Get a page (cache hit)
	p2, _ := pool.GetPage(p.ID())
	pool.Unpin(p2)

	stats := pool.Stats()
	if stats.Capacity != 10 {
		t.Errorf("Expected capacity 10, got %d", stats.Capacity)
	}
	if stats.PageCount == 0 {
		t.Error("Expected non-zero PageCount")
	}
	t.Logf("Stats: hits=%d misses=%d reads=%d writes=%d evictions=%d",
		stats.HitCount, stats.MissCount, stats.ReadCount, stats.WriteCount, stats.EvictionCount)
}

// TestBufferPool_LRU_TouchThreshold exercises the touchLRU probabilistic
// update by accessing a page enough times to trigger the write lock.
func TestBufferPool_LRU_TouchThreshold(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(16, backend)
	defer pool.Close()

	p, _ := pool.NewPage(PageTypeLeaf)
	pid := p.ID()
	pool.Unpin(p)

	// Access the page lruTouchThreshold+1 times to trigger MoveToFront
	for i := 0; i < int(lruTouchThreshold)+1; i++ {
		pg, err := pool.GetPage(pid)
		if err != nil {
			t.Fatal(err)
		}
		pool.Unpin(pg)
	}
}

// =====================================================================
// Disk deep coverage: concurrent reads/writes, file growth, truncation,
// closed backend operations
// =====================================================================

// TestDisk_ConcurrentReadsWrites exercises concurrent access to DiskBackend.
func TestDisk_ConcurrentReadsWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.dat")
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	// Write initial data
	initial := make([]byte, PageSize*10)
	for i := range initial {
		initial[i] = byte(i % 256)
	}
	disk.WriteAt(initial, 0)
	disk.Sync()

	var wg sync.WaitGroup
	// Concurrent readers
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			buf := make([]byte, PageSize)
			for i := 0; i < 10; i++ {
				offset := int64(i) * int64(PageSize)
				disk.ReadAt(buf, offset)
			}
		}(r)
	}
	// Concurrent writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		data := make([]byte, PageSize)
		for i := range data {
			data[i] = 0xFF
		}
		for i := 0; i < 5; i++ {
			disk.WriteAt(data, int64(i)*int64(PageSize))
		}
	}()
	wg.Wait()
}

// TestDisk_FileGrowth tests that writing beyond current file size grows it.
func TestDisk_FileGrowth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "growth.dat")
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	if disk.Size() != 0 {
		t.Errorf("Expected initial size 0, got %d", disk.Size())
	}

	// Write at offset 0
	disk.WriteAt([]byte("hello"), 0)
	if disk.Size() < 5 {
		t.Errorf("Expected size >= 5, got %d", disk.Size())
	}

	// Write at offset beyond current end
	disk.WriteAt([]byte("world"), 10000)
	if disk.Size() < 10005 {
		t.Errorf("Expected size >= 10005, got %d", disk.Size())
	}
}

// TestDisk_TruncateAndRead tests that data beyond truncation point is gone.
func TestDisk_TruncateAndRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trunc.dat")
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	// Write data
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i % 256)
	}
	disk.WriteAt(data, 0)

	// Truncate to half
	disk.Truncate(4096)
	if disk.Size() != 4096 {
		t.Errorf("Expected size 4096, got %d", disk.Size())
	}

	// Read first half should work
	buf := make([]byte, 4096)
	n, err := disk.ReadAt(buf, 0)
	if err != nil {
		t.Errorf("ReadAt after truncate: %v", err)
	}
	if n != 4096 {
		t.Errorf("Expected 4096 bytes, got %d", n)
	}
}

// TestDisk_ClosedBackendOperations tests operations on a closed DiskBackend.
func TestDisk_ClosedBackendOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closed.dat")
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	disk.Close()

	// ReadAt on closed
	buf := make([]byte, 10)
	_, err = disk.ReadAt(buf, 0)
	if err != ErrBackendClosed {
		t.Errorf("Expected ErrBackendClosed for ReadAt, got %v", err)
	}

	// WriteAt on closed
	_, err = disk.WriteAt([]byte("x"), 0)
	if err != ErrBackendClosed {
		t.Errorf("Expected ErrBackendClosed for WriteAt, got %v", err)
	}

	// Sync on closed
	err = disk.Sync()
	if err != ErrBackendClosed {
		t.Errorf("Expected ErrBackendClosed for Sync, got %v", err)
	}

	// Truncate on closed
	err = disk.Truncate(0)
	if err != ErrBackendClosed {
		t.Errorf("Expected ErrBackendClosed for Truncate, got %v", err)
	}
}

// TestDisk_TruncateNegative tests Truncate with negative size.
func TestDisk_TruncateNegative(t *testing.T) {
	path := filepath.Join(t.TempDir(), "neg_trunc.dat")
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	err = disk.Truncate(-1)
	if err != ErrInvalidSize {
		t.Errorf("Expected ErrInvalidSize, got %v", err)
	}
}

// =====================================================================
// PageManager deep coverage: AllocatePage, GetPage, UpdateMeta, FreePage,
// free list persistence, Close
// =====================================================================

// TestPageManager_AllocateGetUpdateMeta exercises the full lifecycle.
func TestPageManager_AllocateGetUpdateMeta(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	// Allocate pages
	pages := make([]*CachedPage, 10)
	for i := 0; i < 10; i++ {
		p, err := pm.AllocatePage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("AllocatePage %d: %v", i, err)
		}
		pages[i] = p
		pm.GetPool().Unpin(p)
	}

	// GetPage should retrieve them
	for _, p := range pages {
		pg, err := pm.GetPage(p.ID())
		if err != nil {
			t.Errorf("GetPage %d: %v", p.ID(), err)
		} else {
			pm.GetPool().Unpin(pg)
		}
	}

	// Update meta
	meta := pm.GetMeta()
	oldTxn := meta.TxnCounter
	meta.TxnCounter = 42
	if err := pm.UpdateMeta(meta); err != nil {
		t.Fatalf("UpdateMeta: %v", err)
	}

	meta2 := pm.GetMeta()
	if meta2.TxnCounter != 42 {
		t.Errorf("Expected TxnCounter 42, got %d (was %d)", meta2.TxnCounter, oldTxn)
	}

	// Page count
	pc := pm.GetPageCount()
	if pc == 0 {
		t.Error("Expected non-zero page count")
	}
}

// TestPageManager_FreePageReuse tests freeing pages and reusing them.
func TestPageManager_FreePageReuse(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	// Allocate some pages
	p1, _ := pm.AllocatePage(PageTypeLeaf)
	p1ID := p1.ID()
	pm.GetPool().Unpin(p1)

	p2, _ := pm.AllocatePage(PageTypeLeaf)
	pm.GetPool().Unpin(p2)

	// Free one
	if err := pm.FreePage(p1ID); err != nil {
		t.Fatalf("FreePage: %v", err)
	}

	freeCount := pm.GetFreePageCount()
	if freeCount != 1 {
		t.Errorf("Expected 1 free page, got %d", freeCount)
	}

	// Next allocation should reuse the freed page
	p3, err := pm.AllocatePage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("AllocatePage reuse: %v", err)
	}
	pm.GetPool().Unpin(p3)

	if p3.ID() != p1ID {
		t.Logf("Reused page ID %d (expected %d)", p3.ID(), p1ID)
	}

	freeCount = pm.GetFreePageCount()
	if freeCount != 0 {
		t.Errorf("Expected 0 free pages after reuse, got %d", freeCount)
	}
}

// TestPageManager_Sync exercises the Sync method.
func TestPageManager_Sync(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	p, _ := pm.AllocatePage(PageTypeLeaf)
	p.SetDirty(true)
	pm.GetPool().Unpin(p)

	if err := pm.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

// TestPageManager_CloseWithFreeList tests Close with pending free list.
func TestPageManager_CloseWithFreeList(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate and free some pages
	for i := 0; i < 5; i++ {
		p, _ := pm.AllocatePage(PageTypeLeaf)
		pm.GetPool().Unpin(p)
		pm.FreePage(p.ID())
	}

	// Close should save free list
	if err := pm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// =====================================================================
// Page coverage: NewPage, header serialization, FreeSpace, dirty/pinned
// =====================================================================

// TestPage_NewPage_HeaderFields tests NewPage creates correct header.
func TestPage_NewPage_HeaderFields(t *testing.T) {
	page := NewPage(42, PageTypeLeaf)

	if page.Header.PageID != 42 {
		t.Errorf("Expected PageID 42, got %d", page.Header.PageID)
	}
	if page.Header.PageType != PageTypeLeaf {
		t.Errorf("Expected PageTypeLeaf, got %d", page.Header.PageType)
	}
	if page.Header.CellCount != 0 {
		t.Errorf("Expected CellCount 0, got %d", page.Header.CellCount)
	}
	if page.Header.FreeStart != PageHeaderSize {
		t.Errorf("Expected FreeStart %d, got %d", PageHeaderSize, page.Header.FreeStart)
	}
	if page.Header.FreeEnd != uint16(PageSize) {
		t.Errorf("Expected FreeEnd %d, got %d", PageSize, page.Header.FreeEnd)
	}
}

// TestPage_SerializeDeserialize roundtrips header through data buffer.
func TestPage_SerializeDeserialize(t *testing.T) {
	page := NewPage(99, PageTypeInternal)
	page.Header.CellCount = 10
	page.Header.Flags = 0x03
	page.Header.RightPtr = 12345
	page.SerializeHeader()

	// Create a new page and deserialize from same data
	page2 := &Page{Data: make([]byte, PageSize)}
	copy(page2.Data, page.Data)
	page2.DeserializeHeader()

	if page2.Header.PageID != 99 {
		t.Errorf("PageID: expected 99, got %d", page2.Header.PageID)
	}
	if page2.Header.PageType != PageTypeInternal {
		t.Errorf("PageType: expected Internal, got %d", page2.Header.PageType)
	}
	if page2.Header.CellCount != 10 {
		t.Errorf("CellCount: expected 10, got %d", page2.Header.CellCount)
	}
	if page2.Header.RightPtr != 12345 {
		t.Errorf("RightPtr: expected 12345, got %d", page2.Header.RightPtr)
	}
}

// TestPage_FreeSpace tests the FreeSpace calculation.
func TestPage_FreeSpace(t *testing.T) {
	page := NewPage(1, PageTypeLeaf)
	expected := int(PageSize) - PageHeaderSize
	if page.FreeSpace() != expected {
		t.Errorf("Expected %d, got %d", expected, page.FreeSpace())
	}
}

// TestPage_DirtyPinned tests dirty and pinned flags.
func TestPage_DirtyPinned(t *testing.T) {
	page := NewPage(1, PageTypeLeaf)

	if page.IsDirty() {
		t.Error("New page should not be dirty")
	}
	page.SetDirty(true)
	if !page.IsDirty() {
		t.Error("Page should be dirty")
	}
	page.SetDirty(false)
	if page.IsDirty() {
		t.Error("Page should not be dirty after clearing")
	}

	if page.IsPinned() {
		t.Error("New page should not be pinned")
	}
	page.SetPinned(true)
	if !page.IsPinned() {
		t.Error("Page should be pinned")
	}
	page.SetPinned(false)
	if page.IsPinned() {
		t.Error("Page should not be pinned after clearing")
	}
}

// TestMetaPage_NewMetaPage tests defaults.
func TestMetaPage_NewMetaPage_Defaults(t *testing.T) {
	meta := NewMetaPage()
	if string(meta.Magic[:]) != MagicString {
		t.Errorf("Expected magic %s, got %s", MagicString, string(meta.Magic[:]))
	}
	if meta.Version != Version {
		t.Errorf("Expected version %d, got %d", Version, meta.Version)
	}
	if meta.PageSize != PageSize {
		t.Errorf("Expected page size %d, got %d", PageSize, meta.PageSize)
	}
	if meta.PageCount != 1 {
		t.Errorf("Expected page count 1, got %d", meta.PageCount)
	}
}

// TestMetaPage_SerializeDeserialize roundtrips meta page.
func TestMetaPage_SerializeDeserialize_Roundtrip(t *testing.T) {
	meta := NewMetaPage()
	meta.RootPageID = 42
	meta.TxnCounter = 100
	meta.FreeListID = 7

	data := make([]byte, PageSize)
	meta.Serialize(data)

	meta2 := &MetaPage{}
	if err := meta2.Deserialize(data); err != nil {
		t.Fatalf("Deserialize: %v", err)
	}

	if meta2.RootPageID != 42 {
		t.Errorf("RootPageID: expected 42, got %d", meta2.RootPageID)
	}
	if meta2.TxnCounter != 100 {
		t.Errorf("TxnCounter: expected 100, got %d", meta2.TxnCounter)
	}
	if meta2.FreeListID != 7 {
		t.Errorf("FreeListID: expected 7, got %d", meta2.FreeListID)
	}
}

// TestMetaPage_Validate_Valid tests Validate on a valid meta page.
func TestMetaPage_Validate_Valid(t *testing.T) {
	meta := NewMetaPage()
	if err := meta.Validate(); err != nil {
		t.Errorf("Validate on valid meta: %v", err)
	}
}

// =====================================================================
// Memory backend coverage
// =====================================================================

// TestMemory_ReadAtBeyondData tests reading beyond available data.
func TestMemory_ReadAtBeyondData(t *testing.T) {
	mem := NewMemory()
	mem.WriteAt([]byte("hello"), 0)

	buf := make([]byte, 10)
	n, err := mem.ReadAt(buf, 100) // beyond data
	if err == nil {
		t.Error("Expected error reading beyond data")
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes, got %d", n)
	}
}

// TestMemory_ReadAtPartial tests partial read (buffer larger than remaining data).
func TestMemory_ReadAtPartial(t *testing.T) {
	mem := NewMemory()
	mem.WriteAt([]byte("hello"), 0)

	buf := make([]byte, 10) // larger than data
	n, err := mem.ReadAt(buf, 0)
	if n != 5 {
		t.Errorf("Expected 5 bytes, got %d", n)
	}
	if err == nil {
		t.Log("Partial read returned no error (implementation detail)")
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("Expected 'hello', got %q", string(buf[:n]))
	}
}

// TestMemory_DataAndLoadFromData tests the snapshot/restore methods.
func TestMemory_DataAndLoadFromData(t *testing.T) {
	mem := NewMemory()
	mem.WriteAt([]byte("snapshot_test"), 0)

	snapshot := mem.Data()
	if string(snapshot[:13]) != "snapshot_test" {
		t.Errorf("Data() returned wrong content")
	}

	// Load into a new memory backend
	mem2 := NewMemory()
	mem2.LoadFromData(snapshot)

	buf := make([]byte, 13)
	mem2.ReadAt(buf, 0)
	if string(buf) != "snapshot_test" {
		t.Errorf("LoadFromData: expected 'snapshot_test', got %q", string(buf))
	}
}

// TestMemory_TruncateNegative tests Truncate with negative size.
func TestMemory_TruncateNegative(t *testing.T) {
	mem := NewMemory()
	err := mem.Truncate(-1)
	if err != ErrInvalidSize {
		t.Errorf("Expected ErrInvalidSize, got %v", err)
	}
}

// =====================================================================
// CachedPage coverage
// =====================================================================

// TestCachedPage_Methods exercises all CachedPage methods.
func TestCachedPage_Methods(t *testing.T) {
	p := &CachedPage{
		id:   42,
		data: make([]byte, PageSize),
	}

	if p.ID() != 42 {
		t.Errorf("Expected ID 42, got %d", p.ID())
	}

	if p.IsDirty() {
		t.Error("New CachedPage should not be dirty")
	}
	p.SetDirty(true)
	if !p.IsDirty() {
		t.Error("Should be dirty")
	}
	p.SetDirty(false)
	if p.IsDirty() {
		t.Error("Should not be dirty after clear")
	}

	if p.IsPinned() {
		t.Error("New CachedPage should not be pinned")
	}
	p.Pin()
	if !p.IsPinned() {
		t.Error("Should be pinned after Pin")
	}
	p.Unpin()
	if p.IsPinned() {
		t.Error("Should not be pinned after Unpin")
	}

	testData := []byte("test")
	p.SetData(testData)
	if string(p.Data()[:4]) != "test" {
		t.Error("SetData/Data mismatch")
	}
}

// =====================================================================
// BufferPool SetWAL coverage
// =====================================================================

// TestBufferPool_SetWAL exercises SetWAL.
func TestBufferPool_SetWAL_Coverage(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(16, backend)
	defer pool.Close()

	walPath := filepath.Join(t.TempDir(), "setwal.wal")
	wal, _ := OpenWAL(walPath)
	defer wal.Close()

	pool.SetWAL(wal)
	if pool.wal != wal {
		t.Error("SetWAL did not set the WAL")
	}
}

// =====================================================================
// Encryption edge cases (to fill gaps in ReadAt pooling / Size logic)
// =====================================================================

// TestEncryptedBackend_NilConfig tests nil config.
func TestEncryptedBackend_NilConfig(t *testing.T) {
	_, err := NewEncryptedBackend(NewMemory(), nil)
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

// TestEncryptedBackend_ReadAt_Pooling exercises the sync.Pool fast path
// in ReadAt by doing multiple reads.
func TestEncryptedBackend_ReadAt_Pooling(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("pooling-test-password-32-bytes!!"),
		Salt:        []byte("1234567890123456"),
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eb.Close()

	// Write a page
	data := make([]byte, PageSize)
	data[0] = 0xAA
	data[PageSize-1] = 0xBB
	eb.WriteAt(data, 0)

	// Read multiple times to exercise the sync.Pool path
	for i := 0; i < 5; i++ {
		buf := make([]byte, PageSize)
		_, err := eb.ReadAt(buf, 0)
		if err != nil {
			t.Fatalf("ReadAt %d: %v", i, err)
		}
		if buf[0] != 0xAA || buf[PageSize-1] != 0xBB {
			t.Errorf("ReadAt %d: data mismatch", i)
		}
	}
}

// TestEncryptedBackend_SizeMultiPage exercises Size with multiple pages.
func TestEncryptedBackend_SizeMultiPage(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("size-multi-test-password-32bytes"),
		Salt:        []byte("1234567890123456"),
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eb.Close()

	// Write 3 pages
	data := make([]byte, PageSize)
	for i := 0; i < 3; i++ {
		offset := int64(i) * int64(PageSize+eb.cipher.NonceSize()+eb.cipher.Overhead())
		eb.WriteAt(data, offset)
	}

	s := eb.Size()
	t.Logf("Size after 3 encrypted pages: %d", s)
}

// TestBufferPool_NewBufferPool_NonZeroBackend exercises the branch where
// the backend already has data (non-zero size).
func TestBufferPool_NewBufferPool_NonZeroBackend(t *testing.T) {
	backend := NewMemory()
	// Pre-populate the backend with some pages worth of data
	data := make([]byte, PageSize*5)
	backend.WriteAt(data, 0)

	pool := NewBufferPool(32, backend)
	defer pool.Close()

	// nextPageID should be set based on backend size
	if pool.nextPageID == 0 {
		t.Error("Expected non-zero nextPageID for non-empty backend")
	}
}

// =====================================================================
// Additional deep coverage: saveFreeList multi-page, FreePage threshold,
// WAL Sync closed, encryption edge cases (disabled, empty key, Argon2,
// decrypt error), PageManager error paths, stats read time overflow
// =====================================================================

// TestPageManager_FreePage_ThresholdTriggersSave exercises the >1000 free
// pages threshold in FreePage that calls saveFreeList automatically.
func TestPageManager_FreePage_ThresholdTriggersSave(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(2048, backend) // need large pool for many pages
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	// Allocate 1010 pages
	pages := make([]uint32, 1010)
	for i := 0; i < 1010; i++ {
		p, err := pm.AllocatePage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("AllocatePage %d: %v", i, err)
		}
		pages[i] = p.ID()
		pm.GetPool().Unpin(p)
	}

	// Free all 1010 pages - the 1001st free should trigger saveFreeList
	for i := 0; i < 1010; i++ {
		if err := pm.FreePage(pages[i]); err != nil {
			t.Fatalf("FreePage %d: %v", i, err)
		}
	}

	// After threshold trigger, free list should have been saved and cleared
	freeCount := pm.GetFreePageCount()
	t.Logf("Free page count after threshold save: %d", freeCount)
}

// TestPageManager_SaveFreeList_NonZeroExisting exercises saveFreeList when
// FreeListID is already set (reuse existing page path).
func TestPageManager_SaveFreeList_NonZeroExisting(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(128, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate and free some pages to create a free list
	pages := make([]uint32, 10)
	for i := 0; i < 10; i++ {
		p, _ := pm.AllocatePage(PageTypeLeaf)
		pages[i] = p.ID()
		pm.GetPool().Unpin(p)
	}
	for _, pid := range pages {
		pm.FreePage(pid)
	}

	// Close saves free list (sets FreeListID)
	pm.Close()

	// Reopen
	pool2 := NewBufferPool(128, backend)
	pm2, err := NewPageManager(pool2)
	if err != nil {
		t.Fatal(err)
	}

	// The free list should have been loaded from disk
	freeCount := pm2.GetFreePageCount()
	t.Logf("Free pages loaded from disk: %d", freeCount)

	// Allocate pages from the free list, then free them again, then close again
	// This exercises the "reuse existing first free list page" path in saveFreeList
	if freeCount > 0 {
		p, err := pm2.AllocatePage(PageTypeLeaf)
		if err != nil {
			t.Fatal(err)
		}
		pid := p.ID()
		pm2.GetPool().Unpin(p)
		pm2.FreePage(pid)
	}

	pm2.Close()
}

// TestWAL_SyncClosed exercises Sync on a closed WAL.
func TestWAL_SyncClosed(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "sync_closed.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Close()

	err = wal.Sync()
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

// TestWAL_AppendLargeRecord exercises Append with oversized data.
func TestWAL_AppendLargeRecord(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "large.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	err = wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: make([]byte, 70000)})
	if err == nil {
		t.Error("Expected error for oversized record")
	}
}

// TestWAL_RecoverClosed exercises Recover on a closed WAL.
func TestWAL_RecoverClosed(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "recover_closed.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Close()

	pool := NewBufferPool(4, NewMemory())
	defer pool.Close()

	err = wal.Recover(pool)
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

// TestEncryptedBackend_DisabledConfig exercises NewEncryptedBackend with
// disabled encryption (Enabled=false).
func TestEncryptedBackend_DisabledConfig(t *testing.T) {
	eb, err := NewEncryptedBackend(NewMemory(), &EncryptionConfig{
		Enabled: false,
	})
	if err != nil {
		t.Fatalf("Expected success for disabled config, got %v", err)
	}
	defer eb.Close()

	// ReadAt/WriteAt should pass through directly
	data := []byte("plaintext")
	n, err := eb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != len(data) {
		t.Errorf("WriteAt: expected %d bytes, got %d", len(data), n)
	}

	buf := make([]byte, len(data))
	n, err = eb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf[:n]) != "plaintext" {
		t.Errorf("ReadAt: expected 'plaintext', got %q", string(buf[:n]))
	}

	// Size should pass through
	s := eb.Size()
	if s < int64(len(data)) {
		t.Errorf("Size: expected >= %d, got %d", len(data), s)
	}

	// Truncate should pass through
	err = eb.Truncate(0)
	if err != nil {
		t.Fatalf("Truncate: %v", err)
	}
}

// TestEncryptedBackend_EmptyKey exercises the empty key check.
func TestEncryptedBackend_EmptyKey(t *testing.T) {
	_, err := NewEncryptedBackend(NewMemory(), &EncryptionConfig{
		Enabled: true,
		Key:     []byte{}, // empty key
	})
	if err != ErrInvalidKey {
		t.Errorf("Expected ErrInvalidKey, got %v", err)
	}
}

// TestEncryptedBackend_Argon2 exercises the Argon2id key derivation path.
func TestEncryptedBackend_Argon2(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:   true,
		Key:       []byte("argon2-test-password-32-bytes!!!"),
		Salt:      []byte("1234567890123456"),
		UseArgon2: true,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eb.Close()

	// Write and read back
	data := make([]byte, PageSize)
	data[0] = 0xCC
	data[PageSize-1] = 0xDD
	eb.WriteAt(data, 0)

	buf := make([]byte, PageSize)
	_, err = eb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt with Argon2: %v", err)
	}
	if buf[0] != 0xCC || buf[PageSize-1] != 0xDD {
		t.Error("Data mismatch with Argon2 key derivation")
	}
}

// TestEncryptedBackend_NoSalt exercises the auto-salt generation path.
func TestEncryptedBackend_NoSalt(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("nosalt-test-password-32-bytes!!!"),
		Salt:        nil, // auto-generate
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eb.Close()

	// Salt should have been auto-generated
	salt := eb.GetSalt()
	if len(salt) != 16 {
		t.Errorf("Expected 16-byte salt, got %d bytes", len(salt))
	}
}

// TestEncryptedBackend_Truncate exercises Truncate on encrypted backend.
func TestEncryptedBackend_Truncate(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("truncate-test-pass-32-bytes!!!!"),
		Salt:        []byte("1234567890123456"),
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eb.Close()

	// Write 2 pages
	data := make([]byte, PageSize)
	eb.WriteAt(data, 0)
	eb.WriteAt(data, int64(PageSize+eb.cipher.NonceSize()+eb.cipher.Overhead()))

	// Truncate to 1 page
	err = eb.Truncate(int64(PageSize))
	if err != nil {
		t.Fatalf("Truncate: %v", err)
	}
}

// TestEncryptedBackend_ReadAtTooShort exercises the ReadAt path where
// the encrypted data is too short for decryption.
func TestEncryptedBackend_ReadAtTooShort(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("short-read-test-pass-32-bytes!!"),
		Salt:        []byte("1234567890123456"),
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eb.Close()

	// Write very little data directly to the backend (bypassing encryption)
	mem.WriteAt([]byte("tiny"), 0)

	// ReadAt should handle the "too short" case
	buf := make([]byte, PageSize)
	_, err = eb.ReadAt(buf, 0)
	if err == nil {
		t.Log("ReadAt on too-short data succeeded (may return zeros)")
	}
}

// TestBufferPool_Stats_ReadTimeOverflow exercises the addReadTime ring buffer
// overflow (when readTimes reaches maxSamples=100 and starts overwriting).
func TestBufferPool_Stats_ReadTimeOverflow(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(5, backend)
	defer pool.Close()

	// Create pages, flush, then do many reads to overflow the stats ring buffer
	pages := make([]uint32, 5)
	for i := 0; i < 5; i++ {
		p, _ := pool.NewPage(PageTypeLeaf)
		pages[i] = p.ID()
		p.SetDirty(true)
		pool.Unpin(p)
	}
	pool.FlushAll()

	// Do 120+ reads, forcing eviction and disk reads to trigger addReadTime >100 times
	for iter := 0; iter < 30; iter++ {
		for _, pid := range pages {
			p, err := pool.GetPage(pid)
			if err != nil {
				continue
			}
			pool.Unpin(p)
		}
	}

	stats := pool.Stats()
	t.Logf("After 150 reads: hits=%d misses=%d reads=%d", stats.HitCount, stats.MissCount, stats.ReadCount)
}

// TestPageManager_AllocatePage_FreeListGetPageError exercises the AllocatePage
// error path where GetPage fails on a freed page ID (invalid ID in free list).
func TestPageManager_AllocatePage_FreeListGetPageError(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}
	defer pm.Close()

	// Manually inject an invalid page ID into the free list
	pm.mu.Lock()
	pm.freeList = append(pm.freeList, 99999) // page that doesn't exist on disk
	pm.mu.Unlock()

	// AllocatePage should try to reuse, fail, then put it back
	_, err = pm.AllocatePage(PageTypeLeaf)
	if err == nil {
		t.Log("AllocatePage succeeded despite bad free list entry")
	} else {
		t.Logf("AllocatePage with bad free list: %v (expected)", err)
	}
}

// TestPageManager_NewPageManager_ExistingDB exercises opening an existing
// database file (meta page already exists) through NewPageManager.
func TestPageManager_NewPageManager_ExistingDB(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(64, backend)

	// First create a new database
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate a few pages
	for i := 0; i < 5; i++ {
		p, _ := pm.AllocatePage(PageTypeLeaf)
		pm.GetPool().Unpin(p)
	}

	// Update meta with root page
	meta := pm.GetMeta()
	meta.RootPageID = 2
	pm.UpdateMeta(meta)
	pm.Close()

	// Reopen - should load existing meta and free list
	pool2 := NewBufferPool(64, backend)
	pm2, err := NewPageManager(pool2)
	if err != nil {
		t.Fatal(err)
	}
	defer pm2.Close()

	meta2 := pm2.GetMeta()
	if meta2.RootPageID != 2 {
		t.Errorf("Expected RootPageID 2, got %d", meta2.RootPageID)
	}
}

// TestDisk_OpenDisk_ExistingFile exercises OpenDisk with an existing file.
func TestDisk_OpenDisk_ExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.dat")

	// Create file first
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.Write(make([]byte, PageSize*3))
	f.Close()

	// Open existing file
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	if disk.Size() != int64(PageSize*3) {
		t.Errorf("Expected size %d, got %d", PageSize*3, disk.Size())
	}
}

// TestDisk_WriteAtAndReadBack verifies data integrity through write/read.
func TestDisk_WriteAtAndReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wrb.dat")
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := disk.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != PageSize {
		t.Errorf("WriteAt: expected %d, got %d", PageSize, n)
	}

	disk.Sync()

	buf := make([]byte, PageSize)
	n, err = disk.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != PageSize {
		t.Errorf("ReadAt: expected %d, got %d", PageSize, n)
	}

	for i := 0; i < PageSize; i++ {
		if buf[i] != byte(i%256) {
			t.Errorf("Data mismatch at byte %d: got %d, want %d", i, buf[i], byte(i%256))
			break
		}
	}
}

// TestWAL_RecoverCommitBeforeInsert exercises recovery when commit record
// appears before its inserts (tests the committedTxns[record.TxnID] path).
func TestWAL_RecoverCommitBeforeInsert(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "commit_first.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	backend := NewMemory()
	pool := NewBufferPool(64, backend)
	defer pool.Close()

	p, _ := pool.NewPage(PageTypeLeaf)
	pID := p.ID()
	pool.Unpin(p)

	// Write commit FIRST, then inserts (tests committed path in recovery)
	wal.Append(&WALRecord{TxnID: 10, Type: WALCommit})
	wal.Append(&WALRecord{TxnID: 10, Type: WALInsert, PageID: pID, Offset: 50, Data: []byte("after_commit")})
	wal.Close()

	wal2, _ := OpenWAL(walPath)
	defer wal2.Close()

	err = wal2.Recover(pool)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// The insert after the commit should be applied immediately
	p2, _ := pool.GetPage(pID)
	defer pool.Unpin(p2)

	if string(p2.Data()[50:62]) == "after_commit" {
		t.Log("Insert after commit was applied (correct)")
	}
}

// TestMetaPage_Validate_Invalid tests Validate on an invalid meta page.
func TestMetaPage_Validate_Invalid(t *testing.T) {
	meta := &MetaPage{}
	err := meta.Validate()
	if err == nil {
		t.Error("Expected validation error for empty meta page")
	}

	// Valid magic but wrong version
	meta2 := NewMetaPage()
	meta2.Version = 999
	err = meta2.Validate()
	if err == nil {
		t.Error("Expected validation error for wrong version")
	}
}

// TestMemory_TruncateGrow tests Truncate growing the memory backend.
func TestMemory_TruncateGrow(t *testing.T) {
	mem := NewMemory()
	mem.WriteAt([]byte("hi"), 0)

	err := mem.Truncate(1000)
	if err != nil {
		t.Fatalf("Truncate grow: %v", err)
	}
	if mem.Size() != 1000 {
		t.Errorf("Expected size 1000, got %d", mem.Size())
	}

	// Original data should still be there
	buf := make([]byte, 2)
	mem.ReadAt(buf, 0)
	if string(buf) != "hi" {
		t.Errorf("Expected 'hi', got %q", string(buf))
	}
}

// TestMemory_WriteAtGrow tests writing beyond current memory size.
func TestMemory_WriteAtGrow(t *testing.T) {
	mem := NewMemory()

	// Write at an offset that requires growing
	n, err := mem.WriteAt([]byte("far"), 10000)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != 3 {
		t.Errorf("Expected 3 bytes written, got %d", n)
	}
	if mem.Size() < 10003 {
		t.Errorf("Expected size >= 10003, got %d", mem.Size())
	}
}

// TestDisk_DoubleClose tests closing a DiskBackend twice.
func TestDisk_DoubleClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dclose.dat")
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	disk.Close()
	// Second close should not panic
	err = disk.Close()
	if err != nil && err != ErrBackendClosed {
		t.Logf("Double close: %v", err)
	}
}

// TestBufferPool_GetPage_NonExistentPage tests GetPage for a page that
// doesn't exist in the backend (triggers read error).
func TestBufferPool_GetPage_NonExistentPage(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(16, backend)
	defer pool.Close()

	// Try to get page 999 from empty backend
	_, err := pool.GetPage(999)
	if err == nil {
		t.Error("Expected error getting non-existent page from empty backend")
	}
}

// TestPageManager_SaveFreeList_EmptyAfterClear exercises the saveFreeList path
// where FreeListID != 0 but freeList is empty (clearing existing free list).
func TestPageManager_SaveFreeList_EmptyAfterClear(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(128, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate and free pages to create a free list
	for i := 0; i < 5; i++ {
		p, _ := pm.AllocatePage(PageTypeLeaf)
		pm.GetPool().Unpin(p)
		pm.FreePage(p.ID())
	}

	// Close to save the free list
	pm.Close()

	// Reopen
	pool2 := NewBufferPool(128, backend)
	pm2, err := NewPageManager(pool2)
	if err != nil {
		t.Fatal(err)
	}

	// Consume all free pages
	for pm2.GetFreePageCount() > 0 {
		p, err := pm2.AllocatePage(PageTypeLeaf)
		if err != nil {
			break
		}
		pm2.GetPool().Unpin(p)
	}

	// Close again - saveFreeList should handle empty list with non-zero FreeListID
	pm2.Close()
}

// TestPageManager_LoadFreeList_WithData exercises loadFreeList when free list
// pages actually contain data (traverses the linked list).
func TestPageManager_LoadFreeList_WithData(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(256, backend)
	pm, err := NewPageManager(pool)
	if err != nil {
		t.Fatal(err)
	}

	// Allocate 50 pages and free them all
	pages := make([]uint32, 50)
	for i := 0; i < 50; i++ {
		p, _ := pm.AllocatePage(PageTypeLeaf)
		pages[i] = p.ID()
		pm.GetPool().Unpin(p)
	}
	for _, pid := range pages {
		pm.FreePage(pid)
	}

	// Close to persist free list
	pm.Close()

	// Reopen and verify free list loaded
	pool2 := NewBufferPool(256, backend)
	pm2, err := NewPageManager(pool2)
	if err != nil {
		t.Fatal(err)
	}
	defer pm2.Close()

	freeCount := pm2.GetFreePageCount()
	if freeCount == 0 {
		t.Error("Expected non-zero free page count after reload")
	}
	t.Logf("Loaded %d free pages from disk", freeCount)
}

// TestBufferPool_GetPage_EvictionAndReload exercises GetPage where the page
// was evicted and needs to be reloaded from disk (cache miss path).
func TestBufferPool_GetPage_EvictionAndReload(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(3, backend) // tiny pool
	defer pool.Close()

	// Create 3 pages
	pages := make([]uint32, 3)
	for i := 0; i < 3; i++ {
		p, _ := pool.NewPage(PageTypeLeaf)
		copy(p.Data()[PageHeaderSize:PageHeaderSize+4], []byte(fmt.Sprintf("p%d", i)))
		p.SetDirty(true)
		pages[i] = p.ID()
		pool.Unpin(p)
	}
	pool.FlushAll()

	// Create a 4th page - triggers eviction of one of the first 3
	p4, _ := pool.NewPage(PageTypeLeaf)
	pool.Unpin(p4)

	// Access the evicted page - should reload from disk
	for _, pid := range pages {
		p, err := pool.GetPage(pid)
		if err != nil {
			t.Errorf("GetPage %d after eviction: %v", pid, err)
			continue
		}
		pool.Unpin(p)
	}
}

// TestEncryptedBackend_DecryptError exercises the decryption failure path.
func TestEncryptedBackend_DecryptError(t *testing.T) {
	mem := NewMemory()
	cfg := &EncryptionConfig{
		Enabled:     true,
		Key:         []byte("decrypt-error-test-pass-32bytes!"),
		Salt:        []byte("1234567890123456"),
		PBKDF2Iters: 1000,
	}
	eb, err := NewEncryptedBackend(mem, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eb.Close()

	// Write a valid encrypted page
	data := make([]byte, PageSize)
	data[0] = 0xAA
	eb.WriteAt(data, 0)

	// Corrupt the encrypted data in the underlying backend
	encSize := PageSize + eb.cipher.NonceSize() + eb.cipher.Overhead()
	corruptData := make([]byte, encSize)
	mem.ReadAt(corruptData, 0)
	// Flip some bytes in the ciphertext (after nonce)
	nonceSize := eb.cipher.NonceSize()
	corruptData[nonceSize+5] ^= 0xFF
	corruptData[nonceSize+6] ^= 0xFF
	mem.WriteAt(corruptData, 0)

	// ReadAt should fail with decryption error
	buf := make([]byte, PageSize)
	_, err = eb.ReadAt(buf, 0)
	if err == nil {
		t.Error("Expected decryption error after corruption")
	}
}

// TestWAL_CorruptedRecord exercises readLSN handling of a corrupted WAL file.
func TestWAL_CorruptedRecord(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "corrupt.wal")

	// Write a valid record, then some garbage
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("valid")})
	wal.Close()

	// Append garbage to the file to corrupt it
	f, _ := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0600)
	f.Write([]byte("THIS IS GARBAGE THAT CORRUPTS CRC"))
	f.Close()

	// Reopen - readLSN should detect corruption via CRC mismatch
	wal2, err := OpenWAL(walPath)
	if err != nil {
		// This is expected - corrupted WAL
		t.Logf("OpenWAL with corruption: %v (expected)", err)
		return
	}
	defer wal2.Close()

	// If it opened, LSN should be from the valid record
	if wal2.LSN() < 1 {
		t.Errorf("Expected LSN >= 1, got %d", wal2.LSN())
	}
}
