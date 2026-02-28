package storage

import (
	"fmt"
	"os"
	"sync"
)

// DiskBackend implements the Backend interface using file I/O
type DiskBackend struct {
	file     *os.File
	filePath string
	fileSize int64
	mu       sync.RWMutex
}

// OpenDisk opens or creates a disk-based storage backend
func OpenDisk(path string) (*DiskBackend, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	return &DiskBackend{
		file:     file,
		filePath: path,
		fileSize: stat.Size(),
	}, nil
}

// ReadAt reads data from the file at the specified offset
func (d *DiskBackend) ReadAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, ErrInvalidOffset
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.file == nil {
		return 0, ErrBackendClosed
	}

	return d.file.ReadAt(buf, offset)
}

// WriteAt writes data to the file at the specified offset
func (d *DiskBackend) WriteAt(buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, ErrInvalidOffset
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.file == nil {
		return 0, ErrBackendClosed
	}

	n, err := d.file.WriteAt(buf, offset)
	if err != nil {
		return n, err
	}

	// Update file size if we wrote past the end
	endOffset := offset + int64(n)
	if endOffset > d.fileSize {
		d.fileSize = endOffset
	}

	return n, nil
}

// Sync ensures all data is written to disk
func (d *DiskBackend) Sync() error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.file == nil {
		return ErrBackendClosed
	}

	return d.file.Sync()
}

// Size returns the current file size
func (d *DiskBackend) Size() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.fileSize
}

// Truncate resizes the file
func (d *DiskBackend) Truncate(size int64) error {
	if size < 0 {
		return ErrInvalidSize
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.file == nil {
		return ErrBackendClosed
	}

	if err := d.file.Truncate(size); err != nil {
		return err
	}

	d.fileSize = size
	return nil
}

// Close closes the file
func (d *DiskBackend) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.file == nil {
		return nil
	}

	err := d.file.Close()
	d.file = nil
	return err
}
