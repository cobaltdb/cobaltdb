package catalog

import (
	"fmt"

	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// CreateForeignTable creates a new foreign table backed by an FDW.
func (c *Catalog) CreateForeignTable(stmt *query.CreateForeignTableStmt) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[stmt.Table]; exists {
		return ErrTableExists
	}
	if _, exists := c.foreignTables[stmt.Table]; exists {
		return ErrTableExists
	}

	// Validate wrapper exists
	if c.fdwRegistry != nil {
		if !c.fdwRegistry.Has(stmt.Wrapper) {
			return fmt.Errorf("foreign data wrapper '%s' not found", stmt.Wrapper)
		}
	}

	cols := make([]ColumnDef, len(stmt.Columns))
	for i, col := range stmt.Columns {
		cols[i] = ColumnDef{
			Name:          col.Name,
			Type:          query.TokenTypeString(col.Type),
			NotNull:       col.NotNull,
			Unique:        col.Unique,
			PrimaryKey:    col.PrimaryKey,
			AutoIncrement: col.AutoIncrement,
		}
	}

	c.foreignTables[stmt.Table] = &ForeignTableDef{
		TableName: stmt.Table,
		Columns:   cols,
		Wrapper:   stmt.Wrapper,
		Options:   stmt.Options,
	}
	return nil
}

// GetForeignTable returns the foreign table definition.
func (c *Catalog) GetForeignTable(name string) (*ForeignTableDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ft, exists := c.foreignTables[name]
	if !exists {
		return nil, ErrTableNotFound
	}
	return ft, nil
}

// DropForeignTable drops a foreign table.
func (c *Catalog) DropForeignTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.foreignTables[name]; !exists {
		return ErrTableNotFound
	}
	delete(c.foreignTables, name)
	return nil
}

// IsForeignTable returns true if the named table is a foreign table.
func (c *Catalog) IsForeignTable(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.foreignTables[name]
	return ok
}

// SetFDWRegistry sets the FDW registry on the catalog.
func (c *Catalog) SetFDWRegistry(registry *fdw.Registry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fdwRegistry = registry
}

// GetFDWRegistry returns the FDW registry.
func (c *Catalog) GetFDWRegistry() *fdw.Registry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fdwRegistry
}
