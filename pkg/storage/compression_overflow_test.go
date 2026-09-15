package storage

import (
	"bytes"
	"errors"
	"testing"
)

// TestCompressedBackendRejectsOversizedWrite proves that CompressedBackend
// rejects writes larger than one page slot instead of persisting them wrong.
//
// validateCompressedWriteRange caps neither the offset span nor the length,
// and the compressed path stores originalSize as uint16(len(buf)). For a
// compressible payload larger than 65535 bytes (e.g. 70000 zero bytes, which
// zstd shrinks far below PageSize so the compressed path is taken), that
// conversion silently wraps: the persisted header declares a much smaller
// originalSize, the write reports success, and a later ReadAt returns only
// the wrapped byte count with a nil error — a successfully-written page that
// reads back truncated. A write larger than one PageSize slot also spills its
// record across the next page's slot, corrupting the slot-addressed layout.
func TestCompressedBackendRejectsOversizedWrite(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelBest,
		MinRatio: 1.0, // always take the compressed path when it fits
	})
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	// 70000 zero bytes: far above uint16 range, yet compresses to well under
	// PageSize, so the compressed store path (and the uint16 conversion) runs.
	data := make([]byte, 70000)

	_, err = cb.WriteAt(data, 0)
	if err != nil {
		// Fixed behavior: the oversized write is rejected at the door.
		if !errors.Is(err, ErrInvalidSize) {
			t.Fatalf("oversized write rejected with unexpected error: %v", err)
		}
		return
	}

	// Pre-fix path: the oversized write reported success — the round-trip
	// must be faithful. The uint16 originalSize wrap breaks it silently.
	buf := make([]byte, len(data))
	n, readErr := cb.ReadAt(buf, 0)
	if readErr != nil {
		t.Fatalf("FAIL: read after successful oversized write failed: %v", readErr)
	}
	if n != len(data) {
		t.Fatalf("FAIL: wrote %d bytes at offset 0, read back %d bytes with nil error — uint16 originalSize wrap silently truncated the page", len(data), n)
	}
	if !bytes.Equal(buf, data) {
		t.Fatalf("FAIL: data mismatch after oversized write round-trip")
	}
}

// TestCompressedBackendPageSizeWritesStillWork pins the boundary of the
// oversized-write cap: exactly-PageSize writes (the engine's only write size)
// must keep round-tripping through the compressed path.
func TestCompressedBackendPageSizeWritesStillWork(t *testing.T) {
	mem := NewMemory()
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelBest,
		MinRatio: 1.0,
	})
	if err != nil {
		t.Fatalf("failed to create compressed backend: %v", err)
	}

	data := make([]byte, PageSize)
	for i := range data {
		data[i] = byte(i * 31)
	}
	if _, err := cb.WriteAt(data, 0); err != nil {
		t.Fatalf("PageSize write rejected: %v", err)
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
		t.Fatal("data mismatch after PageSize round-trip")
	}
}
