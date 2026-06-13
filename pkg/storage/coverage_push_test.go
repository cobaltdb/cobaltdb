package storage

import (
	"testing"
)

func TestFlushDirtyPagesMultiple(t *testing.T) {
	mem := NewMemory()
	bp := NewBufferPool(100, mem)
	defer bp.Close()

	// Pre-populate backend so GetPage succeeds
	for i := uint32(0); i < 5; i++ {
		pageType := PageTypeLeaf
		if i == 0 {
			pageType = PageTypeMeta
		}
		writeTestPage(t, mem, i, pageType)
	}

	// Write multiple pages
	for i := uint32(0); i < 5; i++ {
		page, err := bp.GetPage(i)
		if err != nil {
			t.Fatalf("GetPage %d: %v", i, err)
		}
		data := page.Data()
		data[0] = byte(i)
		page.SetDirty(true)
		bp.Unpin(page)
	}

	// Collect and flush dirty pages
	bp.mu.RLock()
	var dirty []*CachedPage
	for _, page := range bp.pages {
		if page.IsDirty() && !page.IsPinned() {
			dirty = append(dirty, page)
		}
	}
	bp.mu.RUnlock()

	if len(dirty) == 0 {
		t.Fatal("expected dirty pages")
	}

	for _, page := range dirty {
		if err := bp.FlushPage(page); err != nil {
			t.Fatalf("FlushPage failed: %v", err)
		}
	}
}

func TestCompressLZ4Levels(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	for _, level := range []CompressionLevel{CompressionLevelFast, CompressionLevelDefault, CompressionLevelBest, CompressionLevelNone} {
		cb, err := NewCompressedBackend(NewMemory(), &CompressionConfig{
			Enabled:   true,
			Algorithm: CompressionAlgorithmLZ4,
			Level:     level,
		})
		if err != nil {
			t.Fatalf("NewCompressedBackend: %v", err)
		}

		compressed, ratio, err := cb.compressLZ4(data)
		if err != nil {
			t.Errorf("LZ4 level %v: %v", level, err)
			continue
		}
		if level == CompressionLevelNone {
			if len(compressed) != len(data) {
				t.Errorf("LZ4 none: expected same size, got %d", len(compressed))
			}
		} else {
			if ratio > 1.0 {
				t.Errorf("LZ4 level %v: ratio %.2f > 1.0", level, ratio)
			}
		}
	}
}

func TestCompressZstdEncoderPool(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	cb, err := NewCompressedBackend(NewMemory(), &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelFast,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	// First call creates encoder
	_, _, err = cb.compressZstd(data)
	if err != nil {
		t.Fatalf("zstd first call: %v", err)
	}

	// Second call should reuse encoder from pool
	_, _, err = cb.compressZstd(data)
	if err != nil {
		t.Fatalf("zstd second call: %v", err)
	}

	// Test best level
	cb.config.Level = CompressionLevelBest
	_, _, err = cb.compressZstd(data)
	if err != nil {
		t.Fatalf("zstd best level: %v", err)
	}

	// Test none level (passthrough)
	cb.config.Level = CompressionLevelNone
	compressed, _, err := cb.compressZstd(data)
	if err != nil {
		t.Fatalf("zstd none: %v", err)
	}
	if len(compressed) != len(data) {
		t.Errorf("zstd none: expected %d bytes, got %d", len(data), len(compressed))
	}
}

func TestCompressedBackendReadAtWriteAt(t *testing.T) {
	cb, err := NewCompressedBackend(NewMemory(), &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelDefault,
		MinRatio:  0.95,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	// Write highly compressible data
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i % 10)
	}
	n, err := cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n <= 0 {
		t.Errorf("WriteAt: wrote %d, expected >0", n)
	}

	// Read back
	buf := make([]byte, PageSize)
	n, err = cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != PageSize {
		t.Errorf("ReadAt: read %d, expected %d", n, PageSize)
	}

	// Verify round-trip
	for i := range buf {
		if buf[i] != data[i] {
			t.Errorf("data mismatch at byte %d: got %d, want %d", i, buf[i], data[i])
			break
		}
	}
}

func TestOpenDiskAndTruncate(t *testing.T) {
	path := t.TempDir() + "/test.db"
	disk, err := OpenDisk(path)
	if err != nil {
		t.Fatalf("OpenDisk: %v", err)
	}

	// Write some data
	data := make([]byte, PageSize)
	data[0] = 42
	_, err = disk.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	// Truncate
	err = disk.Truncate(0)
	if err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	disk.Close()
}

func TestCompressedBackendDisabledPass(t *testing.T) {
	mem := NewMemory()
	// Write raw data so we can read it back
	raw := make([]byte, PageSize)
	raw[0] = 77
	mem.WriteAt(raw, 0)

	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled: false,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	buf := make([]byte, PageSize)
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt disabled: %v", err)
	}
	if n < 1 || buf[0] != 77 {
		t.Errorf("ReadAt disabled: got n=%d buf[0]=%d", n, buf[0])
	}

	// WriteAt with compression disabled should pass through
	data := make([]byte, PageSize)
	data[0] = 99
	n, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt disabled: %v", err)
	}
	if n != PageSize {
		t.Errorf("WriteAt disabled: wrote %d, expected %d", n, PageSize)
	}
}

func TestCompressedBackendReadAtEmpty(t *testing.T) {
	cb, err := NewCompressedBackend(NewMemory(), &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	buf := make([]byte, PageSize)
	_, err = cb.ReadAt(buf, 0)
	if err == nil {
		t.Error("expected error reading from empty backend")
	}
}

func TestCompressZlibAllLevels(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 10)
	}

	for _, level := range []CompressionLevel{CompressionLevelFast, CompressionLevelDefault, CompressionLevelBest} {
		cb, err := NewCompressedBackend(NewMemory(), &CompressionConfig{
			Enabled:   true,
			Algorithm: CompressionAlgorithmZlib,
			Level:     level,
		})
		if err != nil {
			t.Fatalf("NewCompressedBackend: %v", err)
		}
		_, ratio, err := cb.compressZlib(data)
		if err != nil {
			t.Errorf("zlib level %v: %v", level, err)
		}
		if ratio > 1.0 {
			t.Errorf("zlib level %v: ratio %.2f > 1.0", level, ratio)
		}
		_ = cb
	}
}
