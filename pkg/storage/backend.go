package storage

import (
	"errors"
	"fmt"
	"io"
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

func WriteFullAt(backend Backend, buf []byte, offset int64) (int, error) {
	n, err := backend.WriteAt(buf, offset)
	if err != nil {
		return n, err
	}
	if n != len(buf) {
		return n, fmt.Errorf("%w: wrote %d of %d bytes at offset %d", io.ErrShortWrite, n, len(buf), offset)
	}
	return n, nil
}

// ReadFullAt reads exactly len(buf) bytes from the backend at the given offset.
// It returns an error if the full buffer could not be read.
func ReadFullAt(backend Backend, buf []byte, offset int64) (int, error) {
	n, err := backend.ReadAt(buf, offset)
	if err != nil {
		return n, err
	}
	if n != len(buf) {
		return n, fmt.Errorf("%w: read %d of %d bytes at offset %d", io.ErrUnexpectedEOF, n, len(buf), offset)
	}
	return n, nil
}
