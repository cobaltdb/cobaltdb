package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogInsteadOf2(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestExecuteInsteadOfTriggerInsertSelect115 tests INSTEAD OF INSERT with SELECT
func TestExecuteInsteadOfTriggerInsertSelect115(t *testing.T) {
	c := newTestCatalogInsteadOf2(t)
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

	// Insert data into source
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

	// Create a view on target table
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "target_table"},
	}
	err = c.CreateView("target_view", selectStmt)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Create INSTEAD OF INSERT trigger on view
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_insert_select",
		Table: "target_view",
		Time:  "INSTEAD OF",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "target_table",
				Columns: []string{"id", "name"},
				Values: [][]query.Expression{
					{
						&query.QualifiedIdentifier{Table: "NEW", Column: "id"},
						&query.QualifiedIdentifier{Table: "NEW", Column: "name"},
					},
				},
			},
		},
	}

	// Create INSERT...SELECT statement
	insertSelectStmt := &query.InsertStmt{
		Table: "target_view",
		Select: &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "id"},
				&query.Identifier{Name: "name"},
			},
			From: &query.TableRef{Name: "source_table"},
		},
	}

	// Test executeInsteadOfTrigger with INSERT...SELECT
	_, rowsAffected, err := c.executeInsteadOfTrigger(ctx, trigger, insertSelectStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfTrigger with INSERT...SELECT failed: %v", err)
	}
	if rowsAffected != 3 {
		t.Errorf("Expected 3 rows affected, got %d", rowsAffected)
	}

	// Verify data was inserted into target_table
	_, rows, err := c.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "*"}},
		From:    &query.TableRef{Name: "target_table"},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to select from target_table: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows in target_table, got %d", len(rows))
	}
}

// TestExecuteInsteadOfTriggerInsertSelectFiltered115 tests INSERT...SELECT with WHERE
func TestExecuteInsteadOfTriggerInsertSelectFiltered115(t *testing.T) {
	c := newTestCatalogInsteadOf2(t)
	ctx := context.Background()

	// Create source table with data
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "source_table2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create source table: %v", err)
	}

	// Insert data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "source_table2",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create target table
	err = c.CreateTable(&query.CreateTableStmt{
		Table: "target_table2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create target table: %v", err)
	}

	// Create view
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "target_table2"},
	}
	err = c.CreateView("target_view2", selectStmt)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Create trigger
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_insert_filtered",
		Table: "target_view2",
		Time:  "INSTEAD OF",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "target_table2",
				Columns: []string{"id", "name"},
				Values: [][]query.Expression{
					{
						&query.QualifiedIdentifier{Table: "NEW", Column: "id"},
						&query.QualifiedIdentifier{Table: "NEW", Column: "name"},
					},
				},
			},
		},
	}

	// Create INSERT...SELECT with WHERE clause
	insertSelectStmt := &query.InsertStmt{
		Table: "target_view2",
		Select: &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "id"},
				&query.Identifier{Name: "name"},
			},
			From: &query.TableRef{Name: "source_table2"},
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 1},
			},
		},
	}

	// Test with filtered SELECT
	_, rowsAffected, err := c.executeInsteadOfTrigger(ctx, trigger, insertSelectStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfTrigger failed: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected (filtered), got %d", rowsAffected)
	}
}

// TestExecuteInsteadOfUpdateTriggerTableNotFound115 tests error when table not found
func TestExecuteInsteadOfUpdateTriggerTableNotFound115(t *testing.T) {
	c := newTestCatalogInsteadOf2(t)
	ctx := context.Background()

	// Create trigger for non-existent table
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_update_missing",
		Table: "non_existent_table",
		Time:  "INSTEAD OF",
		Event: "UPDATE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "some_table",
				Set:   []*query.SetClause{{Column: "name", Value: &query.StringLiteral{Value: "test"}}},
			},
		},
	}

	// Create update statement for non-existent table
	updateStmt := &query.UpdateStmt{
		Table: "non_existent_table",
		Set:   []*query.SetClause{{Column: "name", Value: &query.StringLiteral{Value: "newname"}}},
	}

	// Should return error for table not found
	_, _, err := c.executeInsteadOfUpdateTrigger(ctx, trigger, updateStmt, nil)
	if err == nil {
		t.Error("Expected error for non-existent table")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestExecuteInsteadOfDeleteTriggerTableNotFound115 tests error when table not found for DELETE
func TestExecuteInsteadOfDeleteTriggerTableNotFound115(t *testing.T) {
	c := newTestCatalogInsteadOf2(t)
	ctx := context.Background()

	// Create trigger for non-existent table
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_delete_missing",
		Table: "non_existent_table",
		Time:  "INSTEAD OF",
		Event: "DELETE",
		Body: []query.Statement{
			&query.DeleteStmt{
				Table: "some_table",
			},
		},
	}

	// Create delete statement for non-existent table
	deleteStmt := &query.DeleteStmt{
		Table: "non_existent_table",
	}

	// Should return error for table not found
	_, _, err := c.executeInsteadOfDeleteTrigger(ctx, trigger, deleteStmt, nil)
	if err == nil {
		t.Error("Expected error for non-existent table")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestExecuteInsteadOfTriggerEmptyValues115 tests with empty values
func TestExecuteInsteadOfTriggerEmptyValues115(t *testing.T) {
	c := newTestCatalogInsteadOf2(t)
	ctx := context.Background()

	// Create table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "empty_test_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create view
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "empty_test_table"},
	}
	err = c.CreateView("empty_test_view", selectStmt)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Create trigger
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_empty",
		Table: "empty_test_view",
		Time:  "INSTEAD OF",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "empty_test_table",
				Columns: []string{"id", "name"},
				Values: [][]query.Expression{
					{
						&query.QualifiedIdentifier{Table: "NEW", Column: "id"},
						&query.QualifiedIdentifier{Table: "NEW", Column: "name"},
					},
				},
			},
		},
	}

	// Create insert with empty values
	insertStmt := &query.InsertStmt{
		Table:   "empty_test_view",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{}, // Empty values
	}

	// Should handle empty values gracefully
	_, rowsAffected, err := c.executeInsteadOfTrigger(ctx, trigger, insertStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfTrigger failed with empty values: %v", err)
	}
	if rowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", rowsAffected)
	}
}
