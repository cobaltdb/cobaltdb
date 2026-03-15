package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogColumns(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestGetColumnsForTableOrView103 tests getColumnsForTableOrView
func TestGetColumnsForTableOrView103(t *testing.T) {
	c := newTestCatalogColumns(t)

	// Create a table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "age", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test getting columns for existing table
	cols := c.getColumnsForTableOrView("users")
	if len(cols) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(cols))
	}
	if cols[0].Name != "id" || cols[1].Name != "name" || cols[2].Name != "age" {
		t.Errorf("Unexpected column names: %v", cols)
	}

	// Test getting columns for non-existent table/view
	colsNil := c.getColumnsForTableOrView("nonexistent")
	if colsNil != nil {
		t.Error("Expected nil for non-existent table/view")
	}
}
