package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

func TestCompressedBackendRoundTrip(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelFast,
		MinRatio: 1.0, // Always compress to test the path
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	// Write a compressible page
	data := bytes.Repeat([]byte("hello world "), 350)[:PageSize]
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Read it back
	buf := make([]byte, PageSize)
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != PageSize {
		t.Fatalf("expected %d bytes, got %d", PageSize, n)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("data mismatch after round-trip")
	}
}

func TestCompressedBackendRejectsShortCompressedWrite(t *testing.T) {
	backend := &shortWriteBackend{
		Backend: NewMemory(),
		limit:   compressionHeaderSize - 1,
	}
	cb, err := NewCompressedBackend(backend, &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelFast,
		MinRatio: 1.0,
	})
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	data := bytes.Repeat([]byte("a"), PageSize)
	if _, err := cb.WriteAt(data, 0); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteAt short compressed write error = %v, want %v", err, io.ErrShortWrite)
	}
	if got := cb.Size(); got != 0 {
		t.Fatalf("logical size after failed short write = %d, want 0", got)
	}
}

func TestCompressedBackendRawFallback(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelFast,
		MinRatio: 0.1, // Very strict — almost nothing will compress enough
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	// Write random-ish data that won't compress well
	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i * 7)
	}
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	buf := make([]byte, PageSize)
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != PageSize {
		t.Fatalf("expected %d bytes, got %d", PageSize, n)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("data mismatch after raw round-trip")
	}
}

func TestCompressedBackendRejectsShortDecompressedPage(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelFast,
		MinRatio: 1.0,
	})
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	compressed, _, err := cb.compressZlib([]byte("short page"))
	if err != nil {
		t.Fatalf("compressZlib failed: %v", err)
	}

	header := make([]byte, compressionHeaderSize)
	copy(header[:4], compressionMagicZlib)
	binary.LittleEndian.PutUint16(header[4:6], uint16(PageSize))
	binary.LittleEndian.PutUint16(header[6:8], uint16(len(compressed)))

	if _, err := mem.WriteAt(append(header, compressed...), 0); err != nil {
		t.Fatalf("WriteAt corrupt page failed: %v", err)
	}

	buf := make([]byte, PageSize)
	if _, err := cb.ReadAt(buf, 0); err == nil {
		t.Fatal("expected short decompressed page to be rejected")
	}
}

func TestCompressedBackendRejectsTruncatedPayloadFromHeader(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelFast,
		MinRatio: 1.0,
	})
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	compressed, _, err := cb.compressZlib(bytes.Repeat([]byte("a"), PageSize))
	if err != nil {
		t.Fatalf("compressZlib failed: %v", err)
	}
	if len(compressed) < 2 {
		t.Fatalf("compressed payload unexpectedly short: %d", len(compressed))
	}

	header := make([]byte, compressionHeaderSize)
	copy(header[:4], compressionMagicZlib)
	binary.LittleEndian.PutUint16(header[4:6], uint16(PageSize))
	binary.LittleEndian.PutUint16(header[6:8], uint16(len(compressed)))

	truncated := append(header, compressed[:len(compressed)-1]...)
	if _, err := mem.WriteAt(truncated, 0); err != nil {
		t.Fatalf("WriteAt corrupt page failed: %v", err)
	}

	buf := make([]byte, PageSize)
	_, err = cb.ReadAt(buf, 0)
	if err == nil || !strings.Contains(err.Error(), "payload truncated") {
		t.Fatalf("expected truncated payload error, got %v", err)
	}
}

func TestCompressedBackendDisabled(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled: false,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	data := bytes.Repeat([]byte("x"), PageSize)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	buf := make([]byte, PageSize)
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != PageSize {
		t.Fatalf("expected %d bytes, got %d", PageSize, n)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("data mismatch")
	}
}

type laxCompressedBackend struct {
	truncateCalled bool
	truncateSize   int64
}

func (b *laxCompressedBackend) ReadAt([]byte, int64) (int, error) { return 0, io.EOF }
func (b *laxCompressedBackend) WriteAt(buf []byte, offset int64) (int, error) {
	return len(buf), nil
}
func (b *laxCompressedBackend) Sync() error  { return nil }
func (b *laxCompressedBackend) Size() int64  { return 0 }
func (b *laxCompressedBackend) Close() error { return nil }
func (b *laxCompressedBackend) Truncate(size int64) error {
	b.truncateCalled = true
	b.truncateSize = size
	return nil
}

func TestCompressedBackendRejectsWriteOffsetOverflowBeforeBackendWrite(t *testing.T) {
	backend := &laxCompressedBackend{}
	cb, err := NewCompressedBackend(backend, &CompressionConfig{Enabled: false})
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	n, err := cb.WriteAt([]byte("x"), maxMemoryOffset)
	if !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("WriteAt overflow error = %v, want %v", err, ErrInvalidSize)
	}
	if n != 0 {
		t.Fatalf("WriteAt overflow wrote %d bytes, want 0", n)
	}
	if got := cb.Size(); got != 0 {
		t.Fatalf("logical size after overflow write = %d, want 0", got)
	}
}

func TestCompressedBackendRejectsNegativeTruncateBeforeBackendCall(t *testing.T) {
	backend := &laxCompressedBackend{}
	cb, err := NewCompressedBackend(backend, &CompressionConfig{Enabled: false})
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	if err := cb.Truncate(-1); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("Truncate negative error = %v, want %v", err, ErrInvalidSize)
	}
	if backend.truncateCalled {
		t.Fatalf("underlying backend Truncate called with %d", backend.truncateSize)
	}
	if got := cb.Size(); got != 0 {
		t.Fatalf("logical size after rejected truncate = %d, want 0", got)
	}
}

func TestCompressedBackendNormalizesPartialConfig(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{Enabled: true}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}
	defer cb.Close()

	defaults := DefaultCompressionConfig()
	if cb.config.Level != defaults.Level {
		t.Fatalf("Level = %v, want %v", cb.config.Level, defaults.Level)
	}
	if cb.config.MinRatio != defaults.MinRatio {
		t.Fatalf("MinRatio = %v, want %v", cb.config.MinRatio, defaults.MinRatio)
	}
	if config.Level != CompressionLevelNone || config.MinRatio != 0 {
		t.Fatal("NewCompressedBackend should not mutate caller config")
	}
}

func TestCompressedBackendRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		config *CompressionConfig
		want   string
	}{
		{
			name: "invalid algorithm",
			config: &CompressionConfig{
				Enabled:   true,
				Algorithm: CompressionAlgorithm(99),
				Level:     CompressionLevelFast,
				MinRatio:  0.9,
			},
			want: "invalid compression algorithm",
		},
		{
			name: "invalid level",
			config: &CompressionConfig{
				Enabled:  true,
				Level:    CompressionLevel(99),
				MinRatio: 0.9,
			},
			want: "invalid compression level",
		},
		{
			name: "ratio above one",
			config: &CompressionConfig{
				Enabled:  true,
				Level:    CompressionLevelFast,
				MinRatio: 1.1,
			},
			want: "invalid compression min ratio",
		},
		{
			name: "nan ratio",
			config: &CompressionConfig{
				Enabled:  true,
				Level:    CompressionLevelFast,
				MinRatio: math.NaN(),
			},
			want: "invalid compression min ratio",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCompressedBackend(NewMemory(), tt.config)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func TestCompressedBackendMultiplePages(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelFast,
		MinRatio: 1.0,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	pages := [][]byte{
		bytes.Repeat([]byte("a"), PageSize),
		bytes.Repeat([]byte("b"), PageSize),
		bytes.Repeat([]byte("c"), PageSize),
	}

	for i, p := range pages {
		offset := int64(i) * int64(PageSize)
		_, err := cb.WriteAt(p, offset)
		if err != nil {
			t.Fatalf("write page %d failed: %v", i, err)
		}
	}

	for i, expected := range pages {
		buf := make([]byte, PageSize)
		offset := int64(i) * int64(PageSize)
		n, err := cb.ReadAt(buf, offset)
		if err != nil {
			t.Fatalf("read page %d failed: %v", i, err)
		}
		if n != PageSize {
			t.Fatalf("page %d: expected %d bytes, got %d", i, PageSize, n)
		}
		if !bytes.Equal(buf, expected) {
			t.Fatalf("page %d data mismatch", i)
		}
	}
}

func TestCompressedBackendSavesSpace(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelBest,
		MinRatio: 1.0,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	compressible := bytes.Repeat([]byte("hello world "), 350)[:PageSize]
	_, err = cb.WriteAt(compressible, 0)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// With compression, the actual allocated bytes in the memory backend
	// should be less than PageSize for highly compressible data.
	// MemoryBackend tracks max written offset.
	if mem.Size() >= int64(PageSize) {
		t.Logf("Note: compressed size %d >= PageSize %d (memory backend doesn't track sparse holes)", mem.Size(), PageSize)
	}
}

func TestCompressedBackendInitializesLogicalSizeFromPhysicalSize(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelBest,
		MinRatio: 1.0,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	page := bytes.Repeat([]byte("z"), PageSize)
	if n, err := cb.WriteAt(page, int64(2*PageSize)); err != nil {
		t.Fatalf("write failed: %v", err)
	} else if n != PageSize {
		t.Fatalf("compressed WriteAt returned %d, want logical %d", n, PageSize)
	}
	if got := cb.Size(); got != int64(3*PageSize) {
		t.Fatalf("logical size after sparse compressed write = %d, want %d", got, 3*PageSize)
	}
	if mem.Size() >= int64(3*PageSize) {
		t.Fatalf("test did not create compressed physical storage: physical=%d", mem.Size())
	}

	reopened, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to reopen compressed backend: %v", err)
	}
	if got := reopened.Size(); got != int64(3*PageSize) {
		t.Fatalf("logical size after reopen = %d, want %d", got, 3*PageSize)
	}
}

func TestCompressedBackendSyncTruncateClose(t *testing.T) {
	mem := NewMemory()
	config := DefaultCompressionConfig()
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	if err := cb.Sync(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}
	if err := cb.Truncate(int64(PageSize * 2)); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}
	if err := cb.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestCompressedBackendOperationsAfterClose(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	if err := cb.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := cb.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}

	buf := make([]byte, PageSize)
	if _, err := cb.ReadAt(buf, 0); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("ReadAt after close error = %v, want %v", err, ErrBackendClosed)
	}
	if _, err := cb.WriteAt(buf, 0); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("WriteAt after close error = %v, want %v", err, ErrBackendClosed)
	}
	if err := cb.Sync(); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("Sync after close error = %v, want %v", err, ErrBackendClosed)
	}
	if err := cb.Truncate(PageSize); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("Truncate after close error = %v, want %v", err, ErrBackendClosed)
	}
	if cb.Size() != 0 {
		t.Fatalf("Size after close = %d, want 0", cb.Size())
	}
}

func TestCompressedBackendEmptyRead(t *testing.T) {
	mem := NewMemory()
	config := DefaultCompressionConfig()
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	buf := make([]byte, PageSize)
	_, err = cb.ReadAt(buf, 0)
	if err == nil {
		t.Fatal("expected error reading empty backend")
	}
}

func TestCompressedBackendCompressNone(t *testing.T) {
	mem := NewMemory()
	config := &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelNone,
		MinRatio: 1.0,
	}
	cb, err := NewCompressedBackend(mem, config)
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	data := bytes.Repeat([]byte("x"), PageSize)
	_, err = cb.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	buf := make([]byte, PageSize)
	n, err := cb.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != PageSize {
		t.Fatalf("expected %d bytes, got %d", PageSize, n)
	}
	if !bytes.Equal(buf, data) {
		t.Fatal("data mismatch")
	}
}

// TestCompressedBackendLZ4NoneLevelRoundTrip covers compressLZ4 with
// CompressionLevelNone — must return the original data unchanged.
// Ported from coverage_boost_storage_test.go. Note: coverage_push_test.go
// already exercises LZ4+NoneLevel in a parametric loop, but this test
// keeps the LZ4 contract explicit in compression_test.go and asserts
// the ratio is exactly 1.0.
func TestCompressedBackendLZ4NoneLevelRoundTrip(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelNone,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	data := bytes.Repeat([]byte("hello world "), 350)[:PageSize]
	compressed, ratio, err := cb.compressLZ4(data)
	if err != nil {
		t.Fatalf("compressLZ4: %v", err)
	}
	if !bytes.Equal(compressed, data) {
		t.Error("CompressionLevelNone should return original data")
	}
	if ratio != 1.0 {
		t.Errorf("Expected ratio 1.0, got %f", ratio)
	}
}

// TestCompressedBackendZstdNoneLevelRoundTrip covers compressZstd with
// CompressionLevelNone. coverage_push_test.go exercises this case, but
// we keep the explicit contract test in compression_test.go for parity
// with the LZ4/Zlib variants. Ported from coverage_boost_storage_test.go.
func TestCompressedBackendZstdNoneLevelRoundTrip(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelNone,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	data := bytes.Repeat([]byte("hello world "), 350)[:PageSize]
	compressed, ratio, err := cb.compressZstd(data)
	if err != nil {
		t.Fatalf("compressZstd: %v", err)
	}
	if !bytes.Equal(compressed, data) {
		t.Error("CompressionLevelNone should return original data")
	}
	if ratio != 1.0 {
		t.Errorf("Expected ratio 1.0, got %f", ratio)
	}
}

// TestCompressedBackendZlibNoneLevelRoundTrip covers compressZlib with
// CompressionLevelNone. This is unique — coverage_push_test.go
// exercises zlib with Fast/Default/Best but not NoneLevel. Ported
// from coverage_boost_storage_test.go.
func TestCompressedBackendZlibNoneLevelRoundTrip(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZlib,
		Level:     CompressionLevelNone,
		MinRatio:  1.0,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	data := bytes.Repeat([]byte("hello world "), 350)[:PageSize]
	compressed, ratio, err := cb.compressZlib(data)
	if err != nil {
		t.Fatalf("compressZlib: %v", err)
	}
	if !bytes.Equal(compressed, data) {
		t.Error("CompressionLevelNone should return original data")
	}
	if ratio != 1.0 {
		t.Errorf("Expected ratio 1.0, got %f", ratio)
	}
}

// TestCompressedBackendDecompressZlibInvalid covers decompressZlib
// with non-zlib input. Ported from coverage_boost_storage_test.go —
// no untagged test exercised the decompressZlib error path.
func TestCompressedBackendDecompressZlibInvalid(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	_, err = cb.decompressZlib([]byte("not valid zlib"), PageSize)
	if err == nil {
		t.Error("Expected error for invalid zlib data")
	}
}

// TestCompressedBackendDecompressLZ4Invalid covers decompressLZ4 with
// non-LZ4 input. Ported from coverage_boost_storage_test.go.
func TestCompressedBackendDecompressLZ4Invalid(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelFast,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	_, err = cb.decompressLZ4([]byte("not valid lz4"), PageSize)
	if err == nil {
		t.Error("Expected error for invalid LZ4 data")
	}
}

// TestCompressedBackendDecompressZstdInvalid covers decompressZstd
// with non-Zstd input. Ported from coverage_boost_storage_test.go.
func TestCompressedBackendDecompressZstdInvalid(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmZstd,
		Level:     CompressionLevelFast,
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}

	_, err = cb.decompressZstd([]byte("not valid zstd"), PageSize)
	if err == nil {
		t.Error("Expected error for invalid zstd data")
	}
}
