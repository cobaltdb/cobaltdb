package storage

import (
	"errors"
	"testing"
)

type oversizedBackend struct{}

func (oversizedBackend) ReadAt([]byte, int64) (int, error)  { return 0, nil }
func (oversizedBackend) WriteAt([]byte, int64) (int, error) { return 0, nil }
func (oversizedBackend) Sync() error                        { return nil }
func (oversizedBackend) Size() int64                        { return int64(PageSize) * (1 << 32) }
func (oversizedBackend) Truncate(int64) error               { return nil }
func (oversizedBackend) Close() error                       { return nil }

type negativeSizeBackend struct{}

func (negativeSizeBackend) ReadAt([]byte, int64) (int, error)  { return 0, nil }
func (negativeSizeBackend) WriteAt([]byte, int64) (int, error) { return 0, nil }
func (negativeSizeBackend) Sync() error                        { return nil }
func (negativeSizeBackend) Size() int64                        { return -1 }
func (negativeSizeBackend) Truncate(int64) error               { return nil }
func (negativeSizeBackend) Close() error                       { return nil }

func TestNewBufferPoolWithErrorRejectsOversizedBackend(t *testing.T) {
	_, err := NewBufferPoolWithError(10, oversizedBackend{})
	if !errors.Is(err, ErrPageIDExhausted) {
		t.Fatalf("expected ErrPageIDExhausted, got %v", err)
	}
}

func TestNewBufferPoolWithErrorRejectsNegativeBackendSize(t *testing.T) {
	_, err := NewBufferPoolWithError(10, negativeSizeBackend{})
	if !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("expected ErrInvalidSize, got %v", err)
	}
}

func TestNewBufferPoolCompatibilityWrapperDoesNotPanic(t *testing.T) {
	bp := NewBufferPool(10, oversizedBackend{})
	if _, err := bp.NewPage(PageTypeLeaf); !errors.Is(err, ErrPageIDExhausted) {
		t.Fatalf("expected ErrPageIDExhausted from deferred init error, got %v", err)
	}
}

func TestNewBufferPoolCompatibilityWrapperDefersNegativeBackendSize(t *testing.T) {
	bp := NewBufferPool(10, negativeSizeBackend{})
	if _, err := bp.NewPage(PageTypeLeaf); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("expected ErrInvalidSize from deferred init error, got %v", err)
	}
}

func TestBufferPoolNewPageRejectsWrappedPageID(t *testing.T) {
	bp, err := NewBufferPoolWithError(10, NewMemory())
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	bp.nextPageID = 0

	if _, err := bp.NewPage(PageTypeLeaf); !errors.Is(err, ErrPageIDExhausted) {
		t.Fatalf("expected ErrPageIDExhausted, got %v", err)
	}
}
