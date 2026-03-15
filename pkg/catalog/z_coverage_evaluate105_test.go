package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogWhere(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestEvaluateWhere105 tests evaluateWhere function with various types
func TestEvaluateWhere105(t *testing.T) {
	c := newTestCatalogWhere(t)

	columns := []ColumnDef{
		{Name: "id", Type: "INTEGER"},
		{Name: "name", Type: "TEXT"},
	}

	// Test nil where clause
	t.Run("NilWhere", func(t *testing.T) {
		result, err := evaluateWhere(c, nil, columns, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result {
			t.Error("Expected true for nil where")
		}
	})

	// Test boolean result
	t.Run("BooleanResultTrue", func(t *testing.T) {
		where := &query.BooleanLiteral{Value: true}
		result, err := evaluateWhere(c, nil, columns, where, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result {
			t.Error("Expected true for boolean literal true")
		}
	})

	t.Run("BooleanResultFalse", func(t *testing.T) {
		where := &query.BooleanLiteral{Value: false}
		result, err := evaluateWhere(c, nil, columns, where, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result {
			t.Error("Expected false for boolean literal false")
		}
	})

	// Test int result - non-zero
	t.Run("IntResultNonZero", func(t *testing.T) {
		where := &query.NumberLiteral{Value: 42}
		result, err := evaluateWhere(c, nil, columns, where, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result {
			t.Error("Expected true for non-zero int")
		}
	})

	// Test int result - zero
	t.Run("IntResultZero", func(t *testing.T) {
		where := &query.NumberLiteral{Value: 0}
		result, err := evaluateWhere(c, nil, columns, where, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result {
			t.Error("Expected false for zero int")
		}
	})

	// Test float64 result - non-zero
	t.Run("Float64ResultNonZero", func(t *testing.T) {
		where := &query.NumberLiteral{Value: 3.14}
		result, err := evaluateWhere(c, nil, columns, where, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result {
			t.Error("Expected true for non-zero float64")
		}
	})

	// Test float64 result - zero
	t.Run("Float64ResultZero", func(t *testing.T) {
		where := &query.NumberLiteral{Value: 0.0}
		result, err := evaluateWhere(c, nil, columns, where, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result {
			t.Error("Expected false for zero float64")
		}
	})
}

// TestCreateTableWithConstraints105 tests CreateTable with various constraints
func TestCreateTableWithConstraints105(t *testing.T) {
	c := newTestCatalogWhere(t)

	// Create table with multiple constraints
	stmt := &query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: query.TokenText, NotNull: true},
			{Name: "category", Type: query.TokenText, Unique: true},
		},
	}

	err := c.CreateTable(stmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table was created
	table, exists := c.tables["products"]
	if !exists {
		t.Fatal("Table 'products' not found")
	}

	// Check columns
	if len(table.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(table.Columns))
	}

	// Check primary key
	if len(table.PrimaryKey) != 1 || table.PrimaryKey[0] != "id" {
		t.Errorf("Expected primary key on 'id', got %v", table.PrimaryKey)
	}
}

// TestCreateTableErrors105 tests CreateTable error cases
func TestCreateTableErrors105(t *testing.T) {
	c := newTestCatalogWhere(t)

	// Test creating table that already exists
	t.Run("TableExists", func(t *testing.T) {
		stmt := &query.CreateTableStmt{
			Table: "users",
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger},
			},
		}
		err := c.CreateTable(stmt)
		if err != nil {
			t.Fatalf("First create should succeed: %v", err)
		}

		// Try to create again
		err = c.CreateTable(stmt)
		if err == nil {
			t.Error("Expected error for existing table")
		}
	})
}
