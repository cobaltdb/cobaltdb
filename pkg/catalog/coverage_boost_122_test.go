package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestLoadWithCorruptData tests Load with various corrupted data scenarios
func TestLoadWithCorruptData(t *testing.T) {
	pool := storage.NewBufferPool(1024, storage.NewMemory())

	// Create catalog with corrupted entries
	catalogTree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create catalog tree: %v", err)
	}

	// Insert various malformed entries
	catalogTree.Put([]byte("tbl:"), []byte(""))               // Empty table name
	catalogTree.Put([]byte("tbl:no_json"), []byte("not json")) // Invalid JSON
	catalogTree.Put([]byte("idx:test"), []byte("index data"))  // Non-table key
	catalogTree.Put([]byte("other"), []byte("data"))           // Unknown key

	c := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		tree:       catalogTree,
		pool:       pool,
	}

	err = c.Load()
	if err != nil {
		t.Logf("Load with corrupt data returned: %v", err)
	}
}

// TestSaveWithMultipleTables tests Save with multiple tables
func TestSaveWithMultipleTables(t *testing.T) {
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

	// Add multiple tables
	for i := 1; i <= 10; i++ {
		tableName := "table_" + string(rune('a'+i-1))
		c.tables[tableName] = &TableDef{
			Name:    tableName,
			Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "data", Type: "TEXT"}},
		}
	}

	err = c.Save()
	if err != nil {
		t.Errorf("Save with multiple tables failed: %v", err)
	}

	// Reload and verify
	c2 := &Catalog{
		tables:     make(map[string]*TableDef),
		tableTrees: make(map[string]*btree.BTree),
		tree:       catalogTree,
		pool:       pool,
	}

	err = c2.Load()
	if err != nil {
		t.Errorf("Load after Save failed: %v", err)
	}

	if len(c2.tables) != 10 {
		t.Errorf("Expected 10 tables after reload, got %d", len(c2.tables))
	}
}

// TestCommitTransactionWithChanges tests CommitTransaction with actual changes
func TestCommitTransactionWithChanges(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Begin transaction and make changes
	c.BeginTransaction(1)

	// Create table within transaction
	c.CreateTable(&query.CreateTableStmt{
		Table: "test_txn",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Commit
	err := c.CommitTransaction()
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}
}

// TestRollbackTransactionWithChanges tests RollbackTransaction with actual changes
func TestRollbackTransactionWithChanges(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Begin transaction and make changes
	c.BeginTransaction(1)

	// Create table within transaction
	c.CreateTable(&query.CreateTableStmt{
		Table: "test_rollback",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Rollback
	err := c.RollbackTransaction()
	if err != nil {
		t.Errorf("RollbackTransaction failed: %v", err)
	}
}
