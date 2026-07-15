package storage

import (
	"bytes"
	"testing"
)

// fillIncompressible fills p with a deterministic xorshift pseudo-random stream
// that zlib cannot shrink, so WriteAt takes the raw-storage path.
func fillIncompressible(p []byte, seed uint64) {
	x := seed | 1
	for i := range p {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		p[i] = byte(x)
	}
}

// TestCompressedBackendRawMagicCollisionRefused verifies that a raw
// (incompressible) page whose leading bytes equal a compression magic is refused
// at write time rather than persisted as a silently-unreadable page. Raw pages
// carry no self-describing header, so ReadAt would otherwise misinterpret such a
// page as a compressed record.
func TestCompressedBackendRawMagicCollisionRefused(t *testing.T) {
	cb, err := NewCompressedBackend(NewMemory(), &CompressionConfig{
		Enabled:  true,
		Level:    CompressionLevelFast,
		MinRatio: 0.5, // demand real shrinkage so incompressible data stays raw
	})
	if err != nil {
		t.Fatalf("new compressed backend: %v", err)
	}

	// Incompressible full page whose first 4 bytes collide with the zlib magic.
	page := make([]byte, PageSize)
	fillIncompressible(page, 0xABCDEF)
	copy(page[:4], compressionMagicZlib)

	if _, err := cb.WriteAt(page, 0); err == nil {
		t.Fatal("expected WriteAt to refuse a raw page colliding with a compression magic, got nil error")
	}

	// A normal incompressible page (leading bytes look like a small page id) must
	// still round-trip raw.
	clean := make([]byte, PageSize)
	fillIncompressible(clean, 0x123456)
	clean[0], clean[1], clean[2], clean[3] = 0x01, 0x00, 0x00, 0x00
	if _, err := cb.WriteAt(clean, PageSize); err != nil {
		t.Fatalf("write clean raw page: %v", err)
	}
	buf := make([]byte, PageSize)
	if _, err := cb.ReadAt(buf, PageSize); err != nil {
		t.Fatalf("read clean raw page: %v", err)
	}
	if !bytes.Equal(buf, clean) {
		t.Fatal("raw page did not round-trip")
	}
}
