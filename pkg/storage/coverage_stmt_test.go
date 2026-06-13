package storage

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func TestBackgroundFlusher(t *testing.T) {
	mem := NewMemory()
	// Pre-populate backend with data so GetPage can read it
	data := make([]byte, PageSize)
	copy(data, []byte("dirty data"))
	mem.WriteAt(data, 0)

	bp := NewBufferPool(100, mem)

	page, err := bp.GetPage(0)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	page.SetData(data)
	page.SetDirty(true)

	if bp.PageCount() != 1 {
		t.Fatalf("expected 1 page, got %d", bp.PageCount())
	}

	// Start background flusher with short interval
	bp.StartBackgroundFlusher(50 * time.Millisecond)

	// Wait for at least one flush cycle
	time.Sleep(200 * time.Millisecond)

	// Stop the flusher
	bp.stopBackgroundFlusher()

	// Give it time to stop
	time.Sleep(50 * time.Millisecond)

	bp.Close()
}

func TestBackgroundFlusherDefaultsNonpositiveInterval(t *testing.T) {
	bp := NewBufferPool(10, NewMemory())
	defer bp.Close()

	bp.StartBackgroundFlusher(-time.Second)
	if got, want := bp.flushInterval, 5*time.Second; got != want {
		t.Fatalf("flush interval = %v, want %v", got, want)
	}
}

func TestBackgroundFlusherLifecycleIsIdempotent(t *testing.T) {
	bp := NewBufferPool(10, NewMemory())

	bp.StartBackgroundFlusher(25 * time.Millisecond)
	bp.StartBackgroundFlusher(10 * time.Millisecond)
	if got, want := bp.flushInterval, 25*time.Millisecond; got != want {
		t.Fatalf("flush interval changed after second start: got %v, want %v", got, want)
	}

	bp.stopBackgroundFlusher()
	bp.stopBackgroundFlusher()

	bp.StartBackgroundFlusher(10 * time.Millisecond)
	if got, want := bp.flushInterval, 10*time.Millisecond; got != want {
		t.Fatalf("flush interval after restart = %v, want %v", got, want)
	}

	if err := bp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFlushDirtyPages(t *testing.T) {
	mem := NewMemory()
	bp := NewBufferPool(100, mem)

	// Write multiple pages to backend first, then load and dirty them
	for i := uint32(0); i < 3; i++ {
		pageType := PageTypeLeaf
		if i == 0 {
			pageType = PageTypeMeta
		}
		writeTestPage(t, mem, i, pageType)
		page, err := bp.GetPage(i)
		if err != nil {
			t.Fatalf("GetPage(%d): %v", i, err)
		}
		page.SetDirty(true)
	}

	// Flush all dirty pages directly
	bp.flushDirtyPages()

	bp.Close()
}

func TestCompressLZ4(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelDefault,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("LZ4 test data for compression"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("LZ4 write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("LZ4 read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("LZ4 round-trip data mismatch")
	}

	cb.Close()
}

func TestCompressLZ4Best(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelBest,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("LZ4 best level test"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("LZ4 best write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("LZ4 best read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("LZ4 best round-trip mismatch")
	}

	cb.Close()
}

func TestCompressZstd(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelDefault,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("Zstd test data for compression"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Zstd write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Zstd read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("Zstd round-trip mismatch")
	}

	cb.Close()
}

func TestCompressZstdBest(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelBest,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("Zstd best compression test"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Zstd best write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Zstd best read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("Zstd best round-trip mismatch")
	}

	cb.Close()
}

func TestCompressLZ4None(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelNone,
		MinRatio:  1.0,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("no compression"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("LZ4 none write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("LZ4 none read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("LZ4 none round-trip mismatch")
	}

	cb.Close()
}

func TestCompressZstdNone(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelNone,
		MinRatio:  1.0,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("no compression"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Zstd none write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Zstd none read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("Zstd none round-trip mismatch")
	}

	cb.Close()
}

func TestCompressZstdFast(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelFast,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("Zstd fast compression test"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Zstd fast write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Zstd fast read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("Zstd fast round-trip mismatch")
	}

	cb.Close()
}

func TestCompressZlibFast(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelFast,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("Zlib fast compression test"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Zlib fast write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Zlib fast read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("Zlib fast round-trip mismatch")
	}

	cb.Close()
}

func TestCompressZlibBest(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelBest,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("Zlib best compression test"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("Zlib best write: %v", err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("Zlib best read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("Zlib best round-trip mismatch")
	}

	cb.Close()
}

// TestCompressPoolReuse writes multiple pages to exercise pool buffer reuse.
func TestCompressPoolReuse(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelDefault,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("pool reuse test data"), 100)
	for i := 0; i < 5; i++ {
		offset := int64(i * PageSize)
		_, err = cb.WriteAt(data, offset)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		buf := make([]byte, len(data))
		n, err := cb.ReadAt(buf, offset)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(buf[:n], data) {
			t.Errorf("round-trip %d mismatch", i)
		}
	}

	cb.Close()
}

func TestCompressIncompressibleData(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelDefault,
		MinRatio:  0.95,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	// Random-ish data that won't compress well
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i * 7 % 256)
	}

	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	buf := make([]byte, PageSize)
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("incompressible round-trip mismatch")
	}

	cb.Close()
}

func TestOpenDisk(t *testing.T) {
	t.Run("CreateNewFile", func(t *testing.T) {
		path := t.TempDir() + "/test.db"
		disk, err := OpenDisk(path)
		if err != nil {
			t.Fatalf("OpenDisk: %v", err)
		}
		disk.Close()
	})

	t.Run("OpenExistingFile", func(t *testing.T) {
		path := t.TempDir() + "/test.db"
		disk, err := OpenDisk(path)
		if err != nil {
			t.Fatal(err)
		}
		disk.Close()

		disk2, err := OpenDisk(path)
		if err != nil {
			t.Fatalf("OpenDisk existing: %v", err)
		}
		disk2.Close()
	})
}

func TestDiskBackendFullRoundTrip(t *testing.T) {
	path := t.TempDir() + "/roundtrip.db"
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()

	data := make([]byte, PageSize)
	copy(data, []byte("page0"))
	n, err := disk.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != PageSize {
		t.Fatalf("wrote %d, expected %d", n, PageSize)
	}

	data2 := make([]byte, PageSize)
	copy(data2, []byte("page1"))
	disk.WriteAt(data2, int64(PageSize))

	if disk.Size() != 2*int64(PageSize) {
		t.Errorf("expected size %d, got %d", 2*PageSize, disk.Size())
	}

	buf := make([]byte, PageSize)
	n, err = disk.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("data mismatch")
	}

	if err := disk.Sync(); err != nil {
		t.Fatal(err)
	}

	if err := disk.Truncate(PageSize); err != nil {
		t.Fatal(err)
	}
	if disk.Size() != int64(PageSize) {
		t.Errorf("expected size %d after truncate, got %d", PageSize, disk.Size())
	}

	_, err = disk.ReadAt(buf, -1)
	if err == nil {
		t.Error("expected error for negative offset")
	}

	_, err = disk.WriteAt(buf, -1)
	if err == nil {
		t.Error("expected error for negative offset write")
	}

	err = disk.Truncate(-1)
	if err == nil {
		t.Error("expected error for negative truncate")
	}
}

func TestDiskBackendClosed(t *testing.T) {
	path := t.TempDir() + "/closed.db"
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatal(err)
	}
	disk.Close()

	buf := make([]byte, PageSize)

	_, err = disk.ReadAt(buf, 0)
	if err == nil {
		t.Error("expected error reading from closed backend")
	}

	_, err = disk.WriteAt(buf, 0)
	if err == nil {
		t.Error("expected error writing to closed backend")
	}

	err = disk.Sync()
	if err == nil {
		t.Error("expected error syncing closed backend")
	}

	err = disk.Truncate(0)
	if err == nil {
		t.Error("expected error truncating closed backend")
	}

	err = disk.Close()
	if err != nil {
		t.Errorf("double close: %v", err)
	}
}

func TestBufferPoolEvictionDirty(t *testing.T) {
	mem := NewMemory()
	bp := NewBufferPool(3, mem)

	for i := 0; i < 5; i++ {
		pageType := PageTypeLeaf
		if i == 0 {
			pageType = PageTypeMeta
		}
		writeTestPage(t, mem, uint32(i), pageType)
	}

	pages := make([]*CachedPage, 3)
	for i := 0; i < 3; i++ {
		p, err := bp.GetPage(uint32(i))
		if err != nil {
			t.Fatalf("GetPage(%d): %v", i, err)
		}
		pages[i] = p
	}

	pages[0].SetData([]byte{0xAA})
	pages[0].SetDirty(true)
	pages[0].Unpin()

	p4, err := bp.GetPage(3)
	if err != nil {
		t.Fatalf("GetPage(3) with eviction: %v", err)
	}
	p4.Unpin()

	for i := 1; i < 3; i++ {
		pages[i].Unpin()
	}

	bp.Close()
}

func TestBufferPoolNewPageEviction(t *testing.T) {
	mem := NewMemory()
	bp := NewBufferPool(2, mem)

	p1, err := bp.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := bp.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatal(err)
	}

	p1.SetDirty(true)
	p1.Unpin()
	p2.Unpin()

	p3, err := bp.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage with eviction: %v", err)
	}
	_ = p3

	bp.Close()
}

func TestBufferPoolFlushDirtyPinned(t *testing.T) {
	mem := NewMemory()
	bp := NewBufferPool(100, mem)

	data := make([]byte, PageSize)
	copy(data, []byte("dirty"))
	mem.WriteAt(data, 0)

	p, err := bp.GetPage(0)
	if err != nil {
		t.Fatal(err)
	}
	p.SetData(data)
	p.SetDirty(true)

	bp.flushDirtyPages()

	if !p.IsDirty() {
		t.Error("expected page to remain dirty when pinned")
	}
	p.Unpin()

	bp.Close()
}

func TestCompressDisabledPassthrough(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{Enabled: false}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("raw data passthrough")
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("passthrough data mismatch")
	}

	cb.Close()
}

func TestCompressedBackendSyncSizeTruncate(t *testing.T) {
	mem := NewMemory()
	config := DefaultCompressionConfig()
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("sync test"), 100)
	cb.WriteAt(data, 0)

	if err := cb.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if cb.Size() <= 0 {
		t.Error("expected positive size")
	}

	if err := cb.Truncate(0); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	cb.Close()
}

func TestCompressedBackendNilConfig(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, nil)
	if err != nil {
		t.Fatal(err)
	}

	data := bytes.Repeat([]byte("default config test"), 100)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(data))
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], data) {
		t.Error("nil config round-trip mismatch")
	}

	cb.Close()
}

func TestCompressedBackendReadEmpty(t *testing.T) {
	mem := NewMemory()
	config := DefaultCompressionConfig()
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, PageSize)
	_, err = cb.ReadAt(buf, 0)
	if err == nil {
		t.Error("expected error reading from empty backend")
	}

	cb.Close()
}
func TestWALCheckpointWithPool(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "checkpoint.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		wal.Append(&WALRecord{
			TxnID:  1,
			Type:   WALInsert,
			PageID: uint32(i),
			Offset: 0,
			Data:   []byte("data"),
		})
	}

	dbPath := filepath.Join(tmpDir, "checkpoint.db")
	disk, err := OpenDisk(dbPath)
	if err != nil {
		wal.Close()
		t.Fatal(err)
	}
	bp := NewBufferPool(100, disk)

	data := make([]byte, PageSize)
	copy(data, []byte("dirty page"))
	disk.WriteAt(data, 0)

	p, _ := bp.GetPage(0)
	p.SetData(data)
	p.SetDirty(true)
	p.Unpin()

	err = wal.Checkpoint(bp)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if wal.CheckpointLSN() == 0 {
		t.Error("expected non-zero checkpoint LSN")
	}

	bp.Close()
	wal.Close()
}

func TestWALRecoverWithRecords(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "recover.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		wal.Append(&WALRecord{
			TxnID:  1,
			Type:   WALUpdate,
			PageID: uint32(i),
			Offset: 0,
			Data:   []byte("updated"),
		})
	}
	wal.Close()

	wal2, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(tmpDir, "recover.db")
	disk, err := OpenDisk(dbPath)
	if err != nil {
		wal2.Close()
		t.Fatal(err)
	}
	bp := NewBufferPool(100, disk)

	err = wal2.Recover(bp)
	if err != nil {
		t.Logf("Recover: %v", err)
	}

	bp.Close()
	wal2.Close()
}

func TestWALSyncMethod(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "sync.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, PageID: 1, Data: []byte("x")})

	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	wal.Close()
}

func TestNewBufferPoolWithExistingData(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "existing.db")

	disk, err := OpenDisk(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	writeTestPage(t, disk, 0, PageTypeMeta)
	writeTestPage(t, disk, 1, PageTypeLeaf)
	writeTestPage(t, disk, 2, PageTypeLeaf)
	disk.Close()

	disk2, err := OpenDisk(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	bp := NewBufferPool(100, disk2)

	p, err := bp.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage(1): %v", err)
	}
	p.Unpin()

	bp.Close()
}

func TestBufferPoolFlushPageClean(t *testing.T) {
	mem := NewMemory()
	bp := NewBufferPool(100, mem)

	data := make([]byte, PageSize)
	mem.WriteAt(data, 0)

	p, _ := bp.GetPage(0)
	p.Unpin()

	if err := bp.FlushPage(p); err != nil {
		t.Fatalf("FlushPage clean: %v", err)
	}

	bp.Close()
}
