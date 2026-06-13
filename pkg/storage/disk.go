package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// DiskBackend implements the Backend interface using file I/O
type DiskBackend struct {
	file     *os.File
	filePath string
	fileSize int64
	mu       sync.RWMutex
}

var diskOpenFile = os.OpenFile

// OpenDisk opens or creates a disk-based storage backend
func OpenDisk(path string) (*DiskBackend, error) {
	cleanPath := filepath.Clean(path)
	if err := rejectStoragePathSymlinkComponents(filepath.Dir(cleanPath), "database directory"); err != nil {
		return nil, err
	}
	info, statErr := os.Lstat(cleanPath)
	preexisting := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("failed to stat file: %w", statErr)
	}
	if preexisting {
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("database file must not be a symlink: %s", cleanPath)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("database file must be a regular file: %s", cleanPath)
		}
	}

	flags := os.O_RDWR
	if !preexisting {
		flags |= os.O_CREATE | os.O_EXCL
	}

	// #nosec G304 -- Path is provided by trusted application configuration and validated before use.
	file, err := diskOpenFile(cleanPath, flags, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	if !stat.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("database file must be a regular file: %s", cleanPath)
	}
	if preexisting && !os.SameFile(info, stat) {
		_ = file.Close()
		return nil, fmt.Errorf("database file changed while opening: %s", cleanPath)
	}
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to set database file permissions: %w", err)
	}
	if !preexisting {
		if err := syncDiskParentDir(cleanPath); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to sync database directory: %w", err)
		}
	}

	return &DiskBackend{
		file:     file,
		filePath: cleanPath,
		fileSize: stat.Size(),
	}, nil
}

func syncDiskParentDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
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

	n, err := writeDiskFullAt(d.file, buf, offset)
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

type diskAtWriter interface {
	WriteAt([]byte, int64) (int, error)
}

func writeDiskFullAt(writer diskAtWriter, buf []byte, offset int64) (int, error) {
	n, err := writer.WriteAt(buf, offset)
	if err != nil {
		return n, err
	}
	if n != len(buf) {
		return n, fmt.Errorf("%w: wrote %d of %d bytes at offset %d", io.ErrShortWrite, n, len(buf), offset)
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

	syncErr := d.file.Sync()
	closeErr := d.file.Close()
	d.file = nil
	return errors.Join(syncErr, closeErr)
}
