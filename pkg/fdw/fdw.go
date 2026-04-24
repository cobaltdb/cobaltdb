package fdw

import (
	"errors"
	"sync"
)

// ForeignDataWrapper is the interface for external data sources.
type ForeignDataWrapper interface {
	// Name returns the FDW identifier (e.g., "csv", "http", "postgres").
	Name() string

	// Open initializes the connection with options from CREATE FOREIGN TABLE.
	Open(options map[string]string) error

	// Scan reads rows from the foreign table.
	// Columns lists the requested column names; the FDW may return all columns.
	// Each row is a slice of interface{} values in column order.
	Scan(table string, columns []string) ([][]interface{}, error)

	// Close releases any resources.
	Close() error
}

// Registry maintains a collection of named FDW implementations.
type Registry struct {
	mu       sync.RWMutex
	wrappers map[string]ForeignDataWrapper
}

// NewRegistry creates an empty FDW registry.
func NewRegistry() *Registry {
	return &Registry{
		wrappers: make(map[string]ForeignDataWrapper),
	}
}

// Register adds an FDW implementation to the registry.
func (r *Registry) Register(name string, fdw ForeignDataWrapper) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wrappers[name] = fdw
}

// Get looks up an FDW by name.
func (r *Registry) Get(name string) (ForeignDataWrapper, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.wrappers[name]
	return f, ok
}

// List returns all registered FDW names.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.wrappers))
	for n := range r.wrappers {
		names = append(names, n)
	}
	return names
}

// ErrNotFound is returned when an FDW or foreign table is not found.
var ErrNotFound = errors.New("foreign data wrapper not found")

// ErrNotSupported is returned for unsupported operations.
var ErrNotSupported = errors.New("operation not supported by foreign data wrapper")
