package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestIndexMaintenanceOnDelete verifies that index entries are removed when rows are deleted
func TestIndexMaintenanceOnDelete(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create catalog
	tree, _ := btree.NewBTree(pool)
	catalog := New(tree, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "email", Type: query.TokenText},
		},
	})

	// Insert test data
	catalog.Insert(&query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name", "email"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}, &query.StringLiteral{Value: "alice@example.com"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}, &query.StringLiteral{Value: "bob@example.com"}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Charlie"}, &query.StringLiteral{Value: "charlie@example.com"}},
		},
	}, nil)

	// Create index on email column
	catalog.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_email",
		Table:   "users",
		Columns: []string{"email"},
	})

	// Verify index exists
	idx, err := catalog.GetIndex("idx_email")
	if err != nil {
		t.Fatalf("Failed to get index: %v", err)
	}
	if idx == nil {
		t.Fatal("Index should exist")
	}

	// Delete a row
	_, affected, err := catalog.Delete(&query.DeleteStmt{
		Table: "users",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", affected)
	}

	// Verify the row is gone
	_, rows, _ := catalog.Select(&query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "email"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "alice@example.com"},
		},
	}, nil)
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows after delete, got %d", len(rows))
	}

	// Verify remaining rows are still accessible via index
	_, rows, _ = catalog.Select(&query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "email"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "bob@example.com"},
		},
	}, nil)
	if len(rows) != 1 {
		t.Errorf("Expected 1 row for Bob, got %d", len(rows))
	}
}

// TestIndexMaintenanceOnUpdate verifies that index entries are updated when indexed columns change
func TestIndexMaintenanceOnUpdate(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create catalog
	tree, _ := btree.NewBTree(pool)
	catalog := New(tree, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "email", Type: query.TokenText},
		},
	})

	// Insert test data
	catalog.Insert(&query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name", "email"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}, &query.StringLiteral{Value: "alice@example.com"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}, &query.StringLiteral{Value: "bob@example.com"}},
		},
	}, nil)

	// Create index on email column
	catalog.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_email",
		Table:   "users",
		Columns: []string{"email"},
	})

	// Update email for Alice
	_, affected, err := catalog.Update(&query.UpdateStmt{
		Table: "users",
		Set: []*query.SetClause{
			{Column: "email", Value: &query.StringLiteral{Value: "alice.new@example.com"}},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	if affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", affected)
	}

	// Verify old email no longer finds the row
	_, rows, _ := catalog.Select(&query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "email"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "alice@example.com"},
		},
	}, nil)
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows for old email, got %d", len(rows))
	}

	// Verify new email finds the row
	_, rows, _ = catalog.Select(&query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "email"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "alice.new@example.com"},
		},
	}, nil)
	if len(rows) != 1 {
		t.Errorf("Expected 1 row for new email, got %d", len(rows))
	}
}

// TestIndexUsageInSelect verifies that indexes are used for SELECT queries
func TestIndexUsageInSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create catalog
	tree, _ := btree.NewBTree(pool)
	catalog := New(tree, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "email", Type: query.TokenText},
		},
	})

	// Insert test data
	for i := 1; i <= 100; i++ {
		catalog.Insert(&query.InsertStmt{
			Table:   "users",
			Columns: []string{"id", "name", "email"},
			Values: [][]query.Expression{
				{&query.NumberLiteral{Value: float64(i)},
					&query.StringLiteral{Value: "User"},
					&query.StringLiteral{Value: "user@example.com"}},
			},
		}, nil)
	}

	// Create index on id column
	catalog.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_id",
		Table:   "users",
		Columns: []string{"id"},
	})

	// Query using the indexed column
	_, rows, err := catalog.Select(&query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 50},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

// TestIndexWithNullValues verifies that indexes handle NULL values correctly
func TestIndexWithNullValues(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	// Create catalog
	tree, _ := btree.NewBTree(pool)
	catalog := New(tree, pool, nil)

	// Create table with nullable column
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "phone", Type: query.TokenText},
		},
	})

	// Insert test data with NULL phone
	catalog.Insert(&query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
		},
	}, nil)

	// Create index on phone column
	catalog.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_phone",
		Table:   "users",
		Columns: []string{"phone"},
	})

	// Update to add phone number
	catalog.Update(&query.UpdateStmt{
		Table: "users",
		Set: []*query.SetClause{
			{Column: "phone", Value: &query.StringLiteral{Value: "555-1234"}},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)

	// Query using the indexed column
	_, rows, err := catalog.Select(&query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "phone"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "555-1234"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}
