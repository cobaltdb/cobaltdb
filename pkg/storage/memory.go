package storage

import (
	"fmt"
	"sync"
)

// MemoryBackend implements the Backend interface using in-memory storage
type MemoryBackend struct {
	data []byte
	mu   sync.RWMutex
}

// NewMemory creates a new in-memory storage backend
func NewMemory() *MemoryBackend {
	return &MemoryBackend{
		data: make([]byte, 0),
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
		return 0, fmt.Errorf("offset %d beyond data size %d", offset, len(m.data))
	}

	n := copy(buf, m.data[offset:])
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

	// Expand data slice if needed
	if endOffset > int64(len(m.data)) {
		newData := make([]byte, endOffset)
		copy(newData, m.data)
		m.data = newData
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

	if size > int64(len(m.data)) {
		// Expand
		newData := make([]byte, size)
		copy(newData, m.data)
		m.data = newData
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
