package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogRet(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestEvaluateReturningRet tests evaluateReturning function
func TestEvaluateReturningRet(t *testing.T) {
	c := newTestCatalogRet(t)

	// Create a test table
	table := &TableDef{
		Name:    "test",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "TEXT"}},
	}

	// Test with simple column reference
	exprs := []query.Expression{
		&query.ColumnRef{Column: "id"},
		&query.ColumnRef{Column: "name"},
	}
	row := []interface{}{1, "Alice"}

	result, cols, err := c.evaluateReturning(exprs, row, table, nil)
	if err != nil {
		t.Fatalf("evaluateReturning failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result))
	}
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}

	// Test with * (all columns)
	exprs2 := []query.Expression{
		&query.ColumnRef{Column: "*"},
	}
	result2, cols2, err := c.evaluateReturning(exprs2, row, table, nil)
	if err != nil {
		t.Fatalf("evaluateReturning with * failed: %v", err)
	}
	if len(result2) != 2 {
		t.Errorf("Expected 2 results with *, got %d", len(result2))
	}
	if len(cols2) != 2 {
		t.Errorf("Expected 2 columns with *, got %d", len(cols2))
	}
}

// TestEvaluateReturningExprRet tests evaluateReturningExpr function
func TestEvaluateReturningExprRet(t *testing.T) {
	c := newTestCatalogRet(t)

	table := &TableDef{
		Name:    "test",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "TEXT"}},
	}
	row := []interface{}{1, "Alice"}

	// Test ColumnRef
	t.Run("ColumnRef", func(t *testing.T) {
		expr := &query.ColumnRef{Column: "id"}
		vals, cols, err := c.evaluateReturningExpr(expr, row, table, nil)
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}
		if len(vals) != 1 || vals[0] != 1 {
			t.Errorf("Expected [1], got %v", vals)
		}
		if len(cols) != 1 || cols[0] != "id" {
			t.Errorf("Expected ['id'], got %v", cols)
		}
	})

	// Test ColumnRef with *
	t.Run("ColumnRefStar", func(t *testing.T) {
		expr := &query.ColumnRef{Column: "*"}
		vals, cols, err := c.evaluateReturningExpr(expr, row, table, nil)
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}
		if len(vals) != 2 {
			t.Errorf("Expected 2 values, got %d", len(vals))
		}
		if len(cols) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(cols))
		}
		_ = cols
	})

	// Test QualifiedIdentifier
	t.Run("QualifiedIdentifier", func(t *testing.T) {
		expr := &query.QualifiedIdentifier{Table: "test", Column: "name"}
		vals, cols, err := c.evaluateReturningExpr(expr, row, table, nil)
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}
		if len(vals) != 1 || vals[0] != "Alice" {
			t.Errorf("Expected ['Alice'], got %v", vals)
		}
		if len(cols) != 1 || cols[0] != "name" {
			t.Errorf("Expected ['name'], got %v", cols)
		}
	})

	// Test Identifier
	t.Run("Identifier", func(t *testing.T) {
		expr := &query.Identifier{Name: "id"}
		vals, cols, err := c.evaluateReturningExpr(expr, row, table, nil)
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}
		if len(vals) != 1 || vals[0] != 1 {
			t.Errorf("Expected [1], got %v", vals)
		}
		_ = cols
	})

	// Test with column not found
	t.Run("ColumnNotFound", func(t *testing.T) {
		expr := &query.ColumnRef{Column: "nonexistent"}
		_, _, err := c.evaluateReturningExpr(expr, row, table, nil)
		if err == nil {
			t.Error("Expected error for non-existent column")
		}
	})
}

// TestGetReturningColumnsRet tests getReturningColumns function
func TestGetReturningColumnsRet(t *testing.T) {
	c := newTestCatalogRet(t)

	exprs := []query.Expression{
		&query.ColumnRef{Column: "id"},
		&query.ColumnRef{Column: "*"},
		&query.QualifiedIdentifier{Table: "t", Column: "name"},
		&query.Identifier{Name: "value"},
		&query.NumberLiteral{Value: 42},
	}

	cols := c.getReturningColumns(exprs)

	if len(cols) != 5 {
		t.Fatalf("Expected 5 columns, got %d: %v", len(cols), cols)
	}
	if cols[0] != "id" {
		t.Errorf("Expected 'id', got '%s'", cols[0])
	}
	if cols[1] != "*" {
		t.Errorf("Expected '*', got '%s'", cols[1])
	}
	if cols[2] != "name" {
		t.Errorf("Expected 'name', got '%s'", cols[2])
	}
	if cols[3] != "value" {
		t.Errorf("Expected 'value', got '%s'", cols[3])
	}
	if cols[4] != "expr_4" {
		t.Errorf("Expected 'expr_4', got '%s'", cols[4])
	}
}

// TestGetLastReturningRowsRet tests GetLastReturningRows function
func TestGetLastReturningRowsRet(t *testing.T) {
	c := newTestCatalogRet(t)

	// Initially should be nil
	rows := c.GetLastReturningRows()
	if rows != nil {
		t.Error("Expected nil initially")
	}

	// Set some data directly (simulating what INSERT RETURNING would do)
	c.mu.Lock()
	c.lastReturningRows = [][]interface{}{{1, "Alice"}, {2, "Bob"}}
	c.mu.Unlock()

	rows = c.GetLastReturningRows()
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

// TestGetLastReturningColumnsRet tests GetLastReturningColumns function
func TestGetLastReturningColumnsRet(t *testing.T) {
	c := newTestCatalogRet(t)

	// Initially should be nil
	cols := c.GetLastReturningColumns()
	if cols != nil {
		t.Error("Expected nil initially")
	}

	// Set some data
	c.mu.Lock()
	c.lastReturningColumns = []string{"id", "name"}
	c.mu.Unlock()

	cols = c.GetLastReturningColumns()
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}
	if cols[0] != "id" || cols[1] != "name" {
		t.Errorf("Unexpected columns: %v", cols)
	}
}

// TestClearReturningRet tests ClearReturning function
func TestClearReturningRet(t *testing.T) {
	c := newTestCatalogRet(t)

	// Set some data
	c.mu.Lock()
	c.lastReturningRows = [][]interface{}{{1, "Alice"}}
	c.lastReturningColumns = []string{"id", "name"}
	c.mu.Unlock()

	// Clear
	c.ClearReturning()

	// Verify cleared
	if c.GetLastReturningRows() != nil {
		t.Error("Expected rows to be nil after clear")
	}
	if c.GetLastReturningColumns() != nil {
		t.Error("Expected columns to be nil after clear")
	}
}

// TestEvaluateReturningWithExpressionRet tests evaluateReturning with complex expressions
func TestEvaluateReturningWithExpressionRet(t *testing.T) {
	c := newTestCatalogRet(t)

	// Create table with expression evaluation
	table := &TableDef{
		Name:    "test",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "INTEGER"}},
	}
	row := []interface{}{10, 20}

	// Test with literal expression
	expr := &query.NumberLiteral{Value: 42}
	vals, _, err := c.evaluateReturningExpr(expr, row, table, nil)
	if err != nil {
		t.Fatalf("Failed with literal: %v", err)
	}
	if len(vals) != 1 {
		t.Errorf("Expected 1 value, got %d", len(vals))
	}
	// Value may be float64 depending on implementation
	if vals[0] != float64(42) && vals[0] != 42 && vals[0] != int64(42) {
		t.Errorf("Expected 42, got %v (type %T)", vals[0], vals[0])
	}
}
