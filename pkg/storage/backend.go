package storage

import (
	"errors"
)

var (
	ErrInvalidOffset = errors.New("invalid offset")
	ErrInvalidSize   = errors.New("invalid size")
	ErrBackendClosed = errors.New("backend is closed")
)

// Backend defines the interface for storage backends (disk or memory)
type Backend interface {
	// ReadAt reads len(buf) bytes from the backend at the given offset
	ReadAt(buf []byte, offset int64) (int, error)

	// WriteAt writes len(buf) bytes to the backend at the given offset
	WriteAt(buf []byte, offset int64) (int, error)

	// Sync ensures all written data is persisted to storage
	Sync() error

	// Size returns the current size of the backend in bytes
	Size() int64

	// Truncate resizes the backend to the specified size
	Truncate(size int64) error

	// Close closes the backend
	Close() error
}
