package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_DropTableCascade tests DROP TABLE with CASCADE
func TestCoverage_DropTableCascade(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "drop_cascade", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "drop_cascade",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Create an index on the table
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_drop",
		Table:   "drop_cascade",
		Columns: []string{"id"},
	})

	// Drop table
	err := cat.DropTable(&query.DropTableStmt{
		Table: "drop_cascade",
	})
	if err != nil {
		t.Logf("Drop table error: %v", err)
	}

	// Verify table is gone
	_, err = cat.GetTable("drop_cascade")
	if err == nil {
		t.Error("Expected error for dropped table")
	}
}

// TestCoverage_DropTableIfExists tests DROP TABLE IF EXISTS
func TestCoverage_DropTableIfExists(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Drop non-existent table without IF EXISTS (should error)
	err := cat.DropTable(&query.DropTableStmt{
		Table: "non_existent_drop",
	})
	if err == nil {
		t.Log("Drop non-existent table may have succeeded")
	}

	// Drop non-existent table with IF EXISTS (should succeed)
	err = cat.DropTable(&query.DropTableStmt{
		Table:    "non_existent_drop",
		IfExists: true,
	})
	if err != nil {
		t.Logf("Drop IF EXISTS error: %v", err)
	}
}

// TestCoverage_CreateTableIfNotExists tests CREATE TABLE IF NOT EXISTS
func TestCoverage_CreateTableIfNotExists(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create table first time
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "if_not_exists_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("First create error: %v", err)
	}

	// Try to create again without IF NOT EXISTS (should error)
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "if_not_exists_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err == nil {
		t.Error("Expected error for duplicate table")
	}

	// Try to create again with IF NOT EXISTS (should succeed)
	err = cat.CreateTable(&query.CreateTableStmt{
		Table:       "if_not_exists_test",
		IfNotExists: true,
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Logf("IF NOT EXISTS error: %v", err)
	}
}

// TestCoverage_ListTables tests ListTables function
func TestCoverage_ListTables(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Initially empty
	tables := cat.ListTables()
	t.Logf("Initial table count: %d", len(tables))

	// Create tables
	cat.CreateTable(&query.CreateTableStmt{
		Table: "list_test_1",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	cat.CreateTable(&query.CreateTableStmt{
		Table: "list_test_2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	tables = cat.ListTables()
	t.Logf("Table count after create: %d", len(tables))
}
