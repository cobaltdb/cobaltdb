package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogInsteadOf(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestExecuteInsteadOfTrigger108 tests INSTEAD OF INSERT trigger
func TestExecuteInsteadOfTrigger108(t *testing.T) {
	c := newTestCatalogInsteadOf(t)
	ctx := context.Background()

	// Create base table to insert into via trigger
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "base_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create base table: %v", err)
	}

	// Create a view
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "base_table"},
	}
	err = c.CreateView("test_view", selectStmt)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Create INSTEAD OF INSERT trigger using correct AST structure
	// Note: CreateTriggerStmt uses "Time" field with value "INSTEAD OF"
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_insert",
		Table: "test_view",
		Time:  "INSTEAD OF",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "base_table",
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

	// Test executeInsteadOfTrigger directly
	insertStmt := &query.InsertStmt{
		Table:   "test_view",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
		},
	}

	_, rowsAffected, err := c.executeInsteadOfTrigger(ctx, trigger, insertStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfTrigger failed: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	// Verify the insert happened in base_table
	_, rows, err := c.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "*"}},
		From:    &query.TableRef{Name: "base_table"},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to select from base_table: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row in base_table, got %d", len(rows))
	}
}

// TestExecuteInsteadOfUpdateTrigger108 tests INSTEAD OF UPDATE trigger
func TestExecuteInsteadOfUpdateTrigger108(t *testing.T) {
	c := newTestCatalogInsteadOf(t)
	ctx := context.Background()

	// Create base table with data
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "base_table2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create base table: %v", err)
	}

	// Insert initial data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "base_table2",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create a view
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "base_table2"},
	}
	err = c.CreateView("test_view2", selectStmt)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Create INSTEAD OF UPDATE trigger
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_update",
		Table: "test_view2",
		Time:  "INSTEAD OF",
		Event: "UPDATE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "base_table2",
				Set: []*query.SetClause{
					{Column: "name", Value: &query.QualifiedIdentifier{Table: "NEW", Column: "name"}},
				},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "OLD", Column: "id"},
				},
			},
		},
	}

	// Test executeInsteadOfUpdateTrigger directly
	updateStmt := &query.UpdateStmt{
		Table: "test_view2",
		Set: []*query.SetClause{
			{Column: "name", Value: &query.StringLiteral{Value: "Updated"}},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}

	_, rowsAffected, err := c.executeInsteadOfUpdateTrigger(ctx, trigger, updateStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfUpdateTrigger failed: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	// Verify the update happened
	_, rows, err := c.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "base_table2"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(rows) == 1 {
		if name, ok := rows[0][0].(string); !ok || name != "Updated" {
			t.Errorf("Expected name 'Updated', got %v", rows[0][0])
		}
	}
}

// TestExecuteInsteadOfDeleteTrigger108 tests INSTEAD OF DELETE trigger
func TestExecuteInsteadOfDeleteTrigger108(t *testing.T) {
	c := newTestCatalogInsteadOf(t)
	ctx := context.Background()

	// Create base table with data
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "base_table3",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create base table: %v", err)
	}

	// Insert initial data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "base_table3",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Create a view
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "base_table3"},
	}
	err = c.CreateView("test_view3", selectStmt)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Create INSTEAD OF DELETE trigger
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_delete",
		Table: "test_view3",
		Time:  "INSTEAD OF",
		Event: "DELETE",
		Body: []query.Statement{
			&query.DeleteStmt{
				Table: "base_table3",
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "OLD", Column: "id"},
				},
			},
		},
	}

	// Test executeInsteadOfDeleteTrigger directly
	deleteStmt := &query.DeleteStmt{
		Table: "test_view3",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}

	_, rowsAffected, err := c.executeInsteadOfDeleteTrigger(ctx, trigger, deleteStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfDeleteTrigger failed: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	// Verify the delete happened
	_, rows, err := c.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "*"}},
		From:    &query.TableRef{Name: "base_table3"},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row remaining, got %d", len(rows))
	}
}

// TestExecuteInsteadOfUpdateTriggerTablePath109 tests INSTEAD OF UPDATE via table path
func TestExecuteInsteadOfUpdateTriggerTablePath109(t *testing.T) {
	c := newTestCatalogInsteadOf(t)
	ctx := context.Background()

	// Create table with data (no view)
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "direct_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "direct_table",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create trigger for direct table
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_update_direct",
		Table: "direct_table",
		Time:  "INSTEAD OF",
		Event: "UPDATE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "direct_table",
				Set: []*query.SetClause{
					{Column: "name", Value: &query.StringLiteral{Value: "DirectUpdated"}},
				},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "OLD", Column: "id"},
				},
			},
		},
	}

	// Test via table path (not view)
	updateStmt := &query.UpdateStmt{
		Table: "direct_table",
		Set: []*query.SetClause{
			{Column: "name", Value: &query.StringLiteral{Value: "NewName"}},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}

	_, rowsAffected, err := c.executeInsteadOfUpdateTrigger(ctx, trigger, updateStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfUpdateTrigger failed: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}
}

// TestExecuteInsteadOfDeleteTriggerTablePath109 tests INSTEAD OF DELETE via table path
func TestExecuteInsteadOfDeleteTriggerTablePath109(t *testing.T) {
	c := newTestCatalogInsteadOf(t)
	ctx := context.Background()

	// Create table with data (no view)
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "direct_table2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "direct_table2",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create trigger for direct table
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_delete_direct",
		Table: "direct_table2",
		Time:  "INSTEAD OF",
		Event: "DELETE",
		Body: []query.Statement{
			&query.DeleteStmt{
				Table: "direct_table2",
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "OLD", Column: "id"},
				},
			},
		},
	}

	// Test via table path (not view)
	deleteStmt := &query.DeleteStmt{
		Table: "direct_table2",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}

	_, rowsAffected, err := c.executeInsteadOfDeleteTrigger(ctx, trigger, deleteStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfDeleteTrigger failed: %v", err)
	}
	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}
}

// TestExecuteInsteadOfTriggerNoWhere109 tests INSTEAD OF triggers without WHERE clause
func TestExecuteInsteadOfTriggerNoWhere109(t *testing.T) {
	c := newTestCatalogInsteadOf(t)
	ctx := context.Background()

	// Create table with data
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "no_where_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "no_where_table",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "no_where_table"},
	}
	err = c.CreateView("no_where_view", selectStmt)
	if err != nil {
		t.Fatalf("Failed to create view: %v", err)
	}

	// Test UPDATE without WHERE - should update all rows
	trigger := &query.CreateTriggerStmt{
		Name:  "trg_instead_update_nowhere",
		Table: "no_where_view",
		Time:  "INSTEAD OF",
		Event: "UPDATE",
		Body: []query.Statement{
			&query.UpdateStmt{
				Table: "no_where_table",
				Set: []*query.SetClause{
					{Column: "name", Value: &query.StringLiteral{Value: "AllUpdated"}},
				},
			},
		},
	}

	updateStmt := &query.UpdateStmt{
		Table: "no_where_view",
		Set: []*query.SetClause{
			{Column: "name", Value: &query.StringLiteral{Value: "NewName"}},
		},
		// No WHERE clause
	}

	_, rowsAffected, err := c.executeInsteadOfUpdateTrigger(ctx, trigger, updateStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfUpdateTrigger failed: %v", err)
	}
	if rowsAffected != 2 {
		t.Errorf("Expected 2 rows affected (no WHERE), got %d", rowsAffected)
	}

	// Test DELETE without WHERE - should delete all rows
	triggerDel := &query.CreateTriggerStmt{
		Name:  "trg_instead_delete_nowhere",
		Table: "no_where_view",
		Time:  "INSTEAD OF",
		Event: "DELETE",
		Body: []query.Statement{
			&query.DeleteStmt{
				Table: "no_where_table",
			},
		},
	}

	deleteStmt := &query.DeleteStmt{
		Table: "no_where_view",
		// No WHERE clause
	}

	_, rowsAffected, err = c.executeInsteadOfDeleteTrigger(ctx, triggerDel, deleteStmt, nil)
	if err != nil {
		t.Fatalf("executeInsteadOfDeleteTrigger failed: %v", err)
	}
	if rowsAffected != 2 {
		t.Errorf("Expected 2 rows affected (no WHERE), got %d", rowsAffected)
	}
}
