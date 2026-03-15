package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogInsert(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestInsertLockedWithInsertSelect110 tests INSERT...SELECT via insertLocked
func TestInsertLockedWithInsertSelect110(t *testing.T) {
	c := newTestCatalogInsert(t)
	ctx := context.Background()

	// Create source table with data
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "source_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create source table: %v", err)
	}

	// Insert data to source
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "source_table",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Charlie"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert source data: %v", err)
	}

	// Create target table
	err = c.CreateTable(&query.CreateTableStmt{
		Table: "target_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create target table: %v", err)
	}

	// Test INSERT...SELECT
	insertStmt := &query.InsertStmt{
		Table:   "target_table",
		Columns: []string{"id", "name"},
		Select: &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "id"},
				&query.Identifier{Name: "name"},
			},
			From: &query.TableRef{Name: "source_table"},
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 1},
			},
		},
	}

	_, rowsAffected, err := c.insertLocked(ctx, insertStmt, nil)
	if err != nil {
		t.Fatalf("INSERT...SELECT failed: %v", err)
	}
	if rowsAffected != 2 {
		t.Errorf("Expected 2 rows inserted, got %d", rowsAffected)
	}

	// Verify target table has the data
	_, rows, err := c.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "*"}},
		From:    &query.TableRef{Name: "target_table"},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to select from target: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows in target, got %d", len(rows))
	}
}

// TestInsertLockedColumnValidation110 tests column validation in insertLocked
func TestInsertLockedColumnValidation110(t *testing.T) {
	c := newTestCatalogInsert(t)
	ctx := context.Background()

	// Create table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "test_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test with invalid column
	insertStmt := &query.InsertStmt{
		Table:   "test_table",
		Columns: []string{"id", "nonexistent_column"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "test"}},
		},
	}

	_, _, err = c.insertLocked(ctx, insertStmt, nil)
	if err == nil {
		t.Error("Expected error for non-existent column")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestInsertLockedWithAutoIncrement110 tests auto-increment in insertLocked
func TestInsertLockedWithAutoIncrement110(t *testing.T) {
	c := newTestCatalogInsert(t)
	ctx := context.Background()

	// Create table with auto-increment
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "auto_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test insert without auto-increment value (should auto-generate)
	insertStmt := &query.InsertStmt{
		Table:   "auto_table",
		Columns: []string{"name"},
		Values: [][]query.Expression{
			{&query.StringLiteral{Value: "Alice"}},
			{&query.StringLiteral{Value: "Bob"}},
		},
	}

	lastID, rowsAffected, err := c.insertLocked(ctx, insertStmt, nil)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowsAffected != 2 {
		t.Errorf("Expected 2 rows, got %d", rowsAffected)
	}
	if lastID != 2 {
		t.Errorf("Expected lastID=2, got %d", lastID)
	}

	// Verify IDs were auto-generated
	_, rows, err := c.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "auto_table"},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "id"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(rows))
	}
	if id, ok := rows[0][0].(int64); !ok || id != 1 {
		t.Errorf("Expected first id=1, got %v", rows[0][0])
	}
	if id, ok := rows[1][0].(int64); !ok || id != 2 {
		t.Errorf("Expected second id=2, got %v", rows[1][0])
	}
}

// TestInsertLockedColumnCountMismatch110 tests column/value count mismatch
func TestInsertLockedColumnCountMismatch110(t *testing.T) {
	c := newTestCatalogInsert(t)
	ctx := context.Background()

	// Create table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "test_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "age", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test with wrong number of values
	insertStmt := &query.InsertStmt{
		Table:   "test_table",
		Columns: []string{"id", "name", "age"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "test"}}, // missing age
		},
	}

	_, _, err = c.insertLocked(ctx, insertStmt, nil)
	if err == nil {
		t.Error("Expected error for column/value count mismatch")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestInsertLockedNoColumnsSpecified110 tests INSERT without column list
func TestInsertLockedNoColumnsSpecified110(t *testing.T) {
	c := newTestCatalogInsert(t)
	ctx := context.Background()

	// Create table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "test_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test insert without specifying columns (should use all columns)
	insertStmt := &query.InsertStmt{
		Table: "test_table",
		// No Columns specified
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
		},
	}

	_, rowsAffected, err := c.insertLocked(ctx, insertStmt, nil)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row, got %d", rowsAffected)
	}
}
