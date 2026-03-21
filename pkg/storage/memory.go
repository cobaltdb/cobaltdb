package storage

import (
	"fmt"
	"io"
	"sync"
)

const (
	// maxGrowthIncrement caps geometric growth of MemoryBackend to prevent
	// runaway allocations. Without this cap, doubling a 50GB slice allocates
	// 100GB of contiguous memory, which can lock up the system.
	maxGrowthIncrement = 64 * 1024 * 1024 // 64 MB

	// defaultMaxMemorySize is the default maximum size for MemoryBackend (1GB).
	// Prevents unbounded growth in benchmarks and tests.
	defaultMaxMemorySize = 1024 * 1024 * 1024
)

// MemoryBackend implements the Backend interface using in-memory storage
type MemoryBackend struct {
	data    []byte
	mu      sync.RWMutex
	maxSize int64 // 0 means use defaultMaxMemorySize
}

// NewMemory creates a new in-memory storage backend
func NewMemory() *MemoryBackend {
	return &MemoryBackend{
		data:    make([]byte, 0),
		maxSize: defaultMaxMemorySize,
	}
}

// NewMemoryWithLimit creates a new in-memory storage backend with a custom size limit
func NewMemoryWithLimit(maxSize int64) *MemoryBackend {
	return &MemoryBackend{
		data:    make([]byte, 0),
		maxSize: maxSize,
	}
}

// ReadAt reads data from memory at the specified offset
func (m *MemoryBackend) ReadAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, ErrInvalidOffset
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if offset >= int64(len(m.data)) {
		return 0, io.EOF
	}

	n := copy(buf, m.data[offset:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt writes data to memory at the specified offset
func (m *MemoryBackend) WriteAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, ErrInvalidOffset
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	endOffset := offset + int64(len(buf))

	// Enforce size limit
	maxSz := m.maxSize
	if maxSz <= 0 {
		maxSz = defaultMaxMemorySize
	}
	if endOffset > maxSz {
		return 0, fmt.Errorf("memory backend: write at offset %d (size %d) would exceed max size %d", offset, len(buf), maxSz)
	}

	// Expand data slice if needed
	if endOffset > int64(len(m.data)) {
		if endOffset <= int64(cap(m.data)) {
			// Capacity is sufficient, just extend length
			m.data = m.data[:endOffset]
		} else {
			// Need new allocation with capped geometric growth
			oldCap := int64(cap(m.data))
			newCap := oldCap * 2
			// Cap growth increment to avoid massive over-allocation
			if growth := newCap - oldCap; growth > maxGrowthIncrement {
				newCap = oldCap + maxGrowthIncrement
			}
			if newCap < endOffset {
				newCap = endOffset
			}
			if newCap > maxSz {
				newCap = maxSz
			}
			newData := make([]byte, endOffset, newCap)
			copy(newData, m.data)
			m.data = newData
		}
	}

	copy(m.data[offset:], buf)
	return len(buf), nil
}

// Sync is a no-op for memory backend (data is always "synced")
func (m *MemoryBackend) Sync() error {
	return nil
}

// Size returns the current data size
func (m *MemoryBackend) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.data))
}

// Truncate resizes the memory buffer
func (m *MemoryBackend) Truncate(size int64) error {
	if size < 0 {
		return ErrInvalidSize
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Enforce size limit
	maxSz := m.maxSize
	if maxSz <= 0 {
		maxSz = defaultMaxMemorySize
	}
	if size > maxSz {
		return fmt.Errorf("memory backend: truncate to %d would exceed max size %d", size, maxSz)
	}

	if size > int64(len(m.data)) {
		if size <= int64(cap(m.data)) {
			// Capacity is sufficient, just extend length
			m.data = m.data[:size]
		} else {
			// Expand with capped geometric growth
			oldCap := int64(cap(m.data))
			newCap := oldCap * 2
			if growth := newCap - oldCap; growth > maxGrowthIncrement {
				newCap = oldCap + maxGrowthIncrement
			}
			if newCap < size {
				newCap = size
			}
			if newCap > maxSz {
				newCap = maxSz
			}
			newData := make([]byte, size, newCap)
			copy(newData, m.data)
			m.data = newData
		}
	} else {
		// Shrink
		m.data = m.data[:size]
	}

	return nil
}

// Close clears the memory (optional, for explicit cleanup)
func (m *MemoryBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = nil
	return nil
}

// Data returns a copy of the underlying data (for testing/snapshots)
func (m *MemoryBackend) Data() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]byte, len(m.data))
	copy(result, m.data)
	return result
}

// LoadFromData loads data from a byte slice (for restoring snapshots)
func (m *MemoryBackend) LoadFromData(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = make([]byte, len(data))
	copy(m.data, data)
}
