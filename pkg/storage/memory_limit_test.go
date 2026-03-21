package storage

import (
	"testing"
)

func TestNewMemoryWithLimit(t *testing.T) {
	mem := NewMemoryWithLimit(1024)
	if mem.maxSize != 1024 {
		t.Errorf("maxSize: got %d, want 1024", mem.maxSize)
	}

	// Write within limit
	data := make([]byte, 512)
	n, err := mem.WriteAt(data, 0)
	if err != nil {
		t.Fatalf("WriteAt within limit: %v", err)
	}
	if n != 512 {
		t.Errorf("WriteAt returned %d, want 512", n)
	}

	// Write exceeding limit
	_, err = mem.WriteAt(data, 600)
	if err == nil {
		t.Error("expected error when exceeding max size")
	}

	mem.Close()
}

func TestMemoryWriteAt_CapacityReuse(t *testing.T) {
	mem := NewMemoryWithLimit(4096)

	// Write 1KB — allocates capacity
	data := make([]byte, 1024)
	mem.WriteAt(data, 0)

	// Write within existing capacity — should NOT reallocate
	data2 := make([]byte, 512)
	mem.WriteAt(data2, 1024)

	if mem.Size() != 1536 {
		t.Errorf("Size: got %d, want 1536", mem.Size())
	}
}

func TestMemoryWriteAt_GrowthCap(t *testing.T) {
	// Use a limit larger than maxGrowthIncrement to test capped growth
	limit := int64(256 * 1024 * 1024) // 256MB
	mem := NewMemoryWithLimit(limit)

	// Write small data to trigger geometric growth
	data := make([]byte, 4096)
	for i := 0; i < 100; i++ {
		_, err := mem.WriteAt(data, int64(i*4096))
		if err != nil {
			t.Fatalf("WriteAt at offset %d: %v", i*4096, err)
		}
	}

	if mem.Size() != 100*4096 {
		t.Errorf("Size: got %d, want %d", mem.Size(), 100*4096)
	}
	mem.Close()
}

func TestMemoryTruncate_WithLimit(t *testing.T) {
	mem := NewMemoryWithLimit(1024)

	// Truncate within limit
	err := mem.Truncate(512)
	if err != nil {
		t.Fatalf("Truncate within limit: %v", err)
	}
	if mem.Size() != 512 {
		t.Errorf("Size after truncate: got %d, want 512", mem.Size())
	}

	// Truncate exceeding limit
	err = mem.Truncate(2048)
	if err == nil {
		t.Error("expected error when truncating beyond max size")
	}

	// Truncate within capacity (shrink)
	err = mem.Truncate(256)
	if err != nil {
		t.Fatalf("Truncate shrink: %v", err)
	}
	if mem.Size() != 256 {
		t.Errorf("Size after shrink: got %d, want 256", mem.Size())
	}

	mem.Close()
}

func TestMemoryTruncate_CapacityReuse(t *testing.T) {
	mem := NewMemoryWithLimit(4096)

	// Grow to 2KB
	mem.Truncate(2048)
	// Shrink to 1KB
	mem.Truncate(1024)
	// Grow back to 1.5KB — should reuse capacity without realloc
	mem.Truncate(1536)

	if mem.Size() != 1536 {
		t.Errorf("Size: got %d, want 1536", mem.Size())
	}
	mem.Close()
}

func TestMemoryDefaultLimit(t *testing.T) {
	mem := NewMemory()
	if mem.maxSize != defaultMaxMemorySize {
		t.Errorf("default maxSize: got %d, want %d", mem.maxSize, defaultMaxMemorySize)
	}
}
