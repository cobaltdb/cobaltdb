package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestLoadWithNilTree tests Load when tree is nil
func TestLoadWithNilTree(t *testing.T) {
	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		tree:       nil,
		pool:       nil,
	}

	err := c.Load()
	if err != nil {
		t.Errorf("Load with nil tree should return nil: %v", err)
	}
}

// TestLoadWithEmptyTree tests Load with empty catalog tree
func TestLoadWithEmptyTree(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())

	// Create empty catalog
	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create catalog tree: %v", err)
	}

	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		tree:       catalogTree,
		pool:       pool,
	}

	err = c.Load()
	if err != nil {
		t.Errorf("Load with empty tree should succeed: %v", err)
	}

	if len(c.tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(c.tables))
	}
}

// TestLoadWithCorruptTableDef tests Load with corrupted table definition
func TestLoadWithCorruptTableDef(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())

	// Create catalog with corrupted entry
	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create catalog tree: %v", err)
	}

	// Insert corrupted table definition (not valid JSON)
	err = catalogTree.Put([]byte("tbl:corrupt"), []byte("not valid json"))
	if err != nil {
		t.Fatalf("Failed to insert corrupted data: %v", err)
	}

	// Insert a valid non-tbl key (should be skipped)
	err = catalogTree.Put([]byte("idx:someindex"), []byte("index data"))
	if err != nil {
		t.Fatalf("Failed to insert index data: %v", err)
	}

	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		tree:       catalogTree,
		pool:       pool,
	}

	err = c.Load()
	if err != nil {
		t.Errorf("Load should continue on corrupted table def: %v", err)
	}

	// Corrupted table should be skipped
	if _, exists := c.tables["corrupt"]; exists {
		t.Error("Corrupted table should not be loaded")
	}
}

// TestSaveErrorPaths tests Save with error conditions
func TestSaveErrorPaths(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())

	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create catalog tree: %v", err)
	}

	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		tree:       catalogTree,
		pool:       pool,
	}

	// Add a test table
	c.tables["test"] = &TableDef{
		Name:       "test",
		Columns:    []ColumnDef{{Name: "id", Type: "INTEGER"}},
		RootPageID: 0,
	}

	// Save should work
	err = c.Save()
	if err != nil {
		t.Errorf("Save failed: %v", err)
	}
}

// TestSaveWithNilTree tests Save with nil tree
func TestSaveWithNilTree(t *testing.T) {
	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		tree:       nil,
		pool:       nil,
	}

	// Save with nil tree should not crash
	err := c.Save()
	if err != nil {
		t.Errorf("Save with nil tree should not error: %v", err)
	}
}

// TestVacuumErrorPaths tests Vacuum with error conditions
func TestVacuumErrorPaths(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())

	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create catalog tree: %v", err)
	}

	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		tree:       catalogTree,
		pool:       pool,
	}

	// Vacuum with no tables should succeed
	err = c.Vacuum()
	if err != nil {
		t.Errorf("Vacuum with no tables should succeed: %v", err)
	}
}

// TestListTablesLocked tests ListTables while holding the lock
func TestListTablesLocked(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Add some tables directly
	c.tables["users"] = &TableDef{Name: "users"}
	c.tables["orders"] = &TableDef{Name: "orders"}
	c.tables["products"] = &TableDef{Name: "products"}

	tables := c.ListTables()

	if len(tables) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(tables))
	}

	// Verify all tables are listed
	tableMap := make(map[string]bool)
	for _, name := range tables {
		tableMap[name] = true
	}

	if !tableMap["users"] || !tableMap["orders"] || !tableMap["products"] {
		t.Error("Not all tables were listed")
	}
}
