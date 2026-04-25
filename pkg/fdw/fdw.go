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
	mu        sync.RWMutex
	factories map[string]func() ForeignDataWrapper
}

// NewRegistry creates an empty FDW registry.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]func() ForeignDataWrapper),
	}
}

// Register adds an FDW factory to the registry.
// The factory should return a new, independent wrapper instance each time it is called.
func (r *Registry) Register(name string, factory func() ForeignDataWrapper) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = factory
}

// Get looks up an FDW by name and returns a fresh instance.
func (r *Registry) Get(name string) (ForeignDataWrapper, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[name]
	if !ok {
		return nil, false
	}
	return f(), true
}

// Has reports whether an FDW with the given name is registered.
func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.factories[name]
	return ok
}

// List returns all registered FDW names.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.factories))
	for n := range r.factories {
		names = append(names, n)
	}
	return names
}

// ErrNotFound is returned when an FDW or foreign table is not found.
var ErrNotFound = errors.New("foreign data wrapper not found")

// ErrNotSupported is returned for unsupported operations.
var ErrNotSupported = errors.New("operation not supported by foreign data wrapper")
