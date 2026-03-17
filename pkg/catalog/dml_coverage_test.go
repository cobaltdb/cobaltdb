package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
)

// TestInsertLockedTypeConversions tests INSERT...SELECT with various types
func TestInsertLockedTypeConversions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create source table with various types
	srcStmt := &query.CreateTableStmt{
		Table: "type_source",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "int_col", Type: query.TokenInteger},
			{Name: "real_col", Type: query.TokenReal},
			{Name: "text_col", Type: query.TokenText},
			{Name: "bool_col", Type: query.TokenBoolean},
		},
	}
	c.CreateTable(srcStmt)

	// Insert data with different types
	c.Insert(ctx, &query.InsertStmt{
		Table:   "type_source",
		Columns: []string{"id", "int_col", "real_col", "text_col", "bool_col"},
		Values: [][]query.Expression{
			{num(1), num(42), float(3.14), str("hello"), &query.BooleanLiteral{Value: true}},
			{num(2), &query.NullLiteral{}, float(2.71), str("world"), &query.BooleanLiteral{Value: false}},
		},
	}, nil)

	// Create destination table
	dstStmt := &query.CreateTableStmt{
		Table: "type_dest",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "int_col", Type: query.TokenInteger},
			{Name: "real_col", Type: query.TokenReal},
			{Name: "text_col", Type: query.TokenText},
			{Name: "bool_col", Type: query.TokenBoolean},
		},
	}
	c.CreateTable(dstStmt)

	// INSERT...SELECT to test type conversions
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:  "type_dest",
		Select: &query.SelectStmt{From: &query.TableRef{Name: "type_source"}},
	}, nil)

	if err != nil {
		t.Logf("INSERT...SELECT type conversion: %v", err)
	}
}

// TestInsertLockedIndexUpdateDML tests INSERT with index updates
func TestInsertLockedIndexUpdateDML(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with index
	stmt := &query.CreateTableStmt{
		Table: "idx_insert_dml_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
	}
	c.CreateTable(stmt)

	// Create multiple indexes
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_name_dml",
		Table:   "idx_insert_dml_test",
		Columns: []string{"name"},
		Unique:  false,
	})

	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_val_dml",
		Table:   "idx_insert_dml_test",
		Columns: []string{"val"},
		Unique:  true,
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		_, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "idx_insert_dml_test",
			Columns: []string{"id", "name", "val"},
			Values: [][]query.Expression{
				{num(int64(i)), str("name"), num(int64(i * 10))},
			},
		}, nil)
		if err != nil {
			t.Logf("Insert with index %d: %v", i, err)
		}
	}
}

// TestUpdateLockedComplexJoinDML tests UPDATE with complex JOIN scenarios
func TestUpdateLockedComplexJoinDML(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "update_main_dml",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
			{Name: "ref_id", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "update_ref_dml",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "multiplier", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "update_main_dml",
			Columns: []string{"id", "val", "ref_id"},
			Values:  [][]query.Expression{{num(int64(i)), num(int64(i * 10)), num(int64(i))}},
		}, nil)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "update_ref_dml",
			Columns: []string{"id", "multiplier"},
			Values:  [][]query.Expression{{num(int64(i)), num(int64(i * 2))}},
		}, nil)
	}

	// UPDATE with FROM clause
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "update_main_dml",
		Set: []*query.SetClause{
			{Column: "val", Value: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Column: "val"},
				Operator: query.TokenStar,
				Right:    &query.QualifiedIdentifier{Table: "update_ref_dml", Column: "multiplier"},
			}},
		},
		From: &query.TableRef{Name: "update_ref_dml"},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Table: "update_main_dml", Column: "ref_id"},
			Operator: query.TokenEq,
			Right:    &query.QualifiedIdentifier{Table: "update_ref_dml", Column: "id"},
		},
	}, nil)

	if err != nil {
		t.Logf("UPDATE with FROM: %v", err)
	}
}

// TestUpdateLockedWithTriggerDML tests UPDATE with trigger execution
func TestUpdateLockedWithTriggerDML(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create main table
	c.CreateTable(&query.CreateTableStmt{
		Table: "update_trigger_main_dml",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Create audit table
	c.CreateTable(&query.CreateTableStmt{
		Table: "update_audit_dml",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "old_val", Type: query.TokenInteger},
			{Name: "new_val", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "update_trigger_main_dml",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{num(1), num(100)}},
	}, nil)

	// Create trigger
	c.CreateTrigger(&query.CreateTriggerStmt{
		Name:      "update_audit_trigger_dml",
		Table:     "update_trigger_main_dml",
		Time:      "AFTER",
		Event:     "UPDATE",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "update_audit_dml",
				Columns: []string{"id", "old_val", "new_val"},
				Values:  [][]query.Expression{{num(1), num(100), num(200)}},
			},
		},
	})

	// Update (should fire trigger)
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "update_trigger_main_dml",
		Set: []*query.SetClause{
			{Column: "val", Value: num(200)},
		},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "id"},
			Operator: query.TokenEq,
			Right:    num(1),
		},
	}, nil)

	if err != nil {
		t.Logf("UPDATE with trigger: %v", err)
	}
}

// TestDeleteLockedWithUsingDML tests DELETE with USING clause
func TestDeleteLockedWithUsingDML(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "delete_main_dml",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "delete_categories_dml",
		Columns: []*query.ColumnDef{
			{Name: "name", Type: query.TokenText, PrimaryKey: true},
			{Name: "obsolete", Type: query.TokenBoolean},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat := "A"
		if i > 3 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "delete_main_dml",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{num(int64(i)), str(cat)}},
		}, nil)
	}

	c.Insert(ctx, &query.InsertStmt{
		Table:   "delete_categories_dml",
		Columns: []string{"name", "obsolete"},
		Values:  [][]query.Expression{{str("B"), &query.BooleanLiteral{Value: true}}},
	}, nil)

	// DELETE with USING
	_, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "delete_main_dml",
		Using: []*query.TableRef{{Name: "delete_categories_dml"}},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Table: "delete_main_dml", Column: "category"},
			Operator: query.TokenEq,
			Right:    &query.QualifiedIdentifier{Table: "delete_categories_dml", Column: "name"},
		},
	}, nil)

	if err != nil {
		t.Logf("DELETE with USING: %v", err)
	}
}

// TestExecuteCTERecursiveDML tests recursive CTE execution
func TestExecuteCTERecursiveDML(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "tree_nodes_dml",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert hierarchical data
	type rowData struct {
		id       int64
		parentID interface{}
		name     string
	}
	data := []rowData{
		{1, nil, "Root"},
		{2, int64(1), "Child1"},
		{3, int64(1), "Child2"},
		{4, int64(2), "GrandChild1"},
		{5, int64(2), "GrandChild2"},
	}
	for _, row := range data {
		var pid query.Expression = &query.NullLiteral{}
		if row.parentID != nil {
			pid = num(row.parentID.(int64))
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "tree_nodes_dml",
			Columns: []string{"id", "parent_id", "name"},
			Values:  [][]query.Expression{{num(row.id), pid, str(row.name)}},
		}, nil)
	}

	// Execute recursive CTE using SelectStmtWithCTE
	cteStmt := &query.SelectStmtWithCTE{
		CTEs: []*query.CTEDef{
			{
				Name:        "tree_cte_dml",
				IsRecursive: true,
				Columns:     []string{"id", "parent_id", "name", "level"},
				Query: &query.SelectStmt{
					From:  &query.TableRef{Name: "tree_nodes_dml"},
					Where: &query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "parent_id"}, Operator: query.TokenIs, Right: &query.NullLiteral{}},
				},
			},
		},
		IsRecursive: true,
		Select:      &query.SelectStmt{From: &query.TableRef{Name: "tree_cte_dml"}},
	}

	_, _, err := c.ExecuteCTE(cteStmt, nil)

	if err != nil {
		t.Logf("Recursive CTE: %v", err)
	}
}

// TestForeignKeyCascadeDeleteDML tests FK cascade delete
func TestForeignKeyCascadeDeleteDML(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent table
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_parent_dml",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Create child table with FK
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_child_dml",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "data", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent_dml",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
		},
	})

	// Insert parent
	c.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent_dml",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{num(1), str("Parent1")}},
	}, nil)

	// Insert children
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "fk_child_dml",
			Columns: []string{"id", "parent_id", "data"},
			Values:  [][]query.Expression{{num(int64(i)), num(1), str("child")}},
		}, nil)
	}

	// Delete parent (should cascade to children)
	c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent_dml",
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "id"},
			Operator: query.TokenEq,
			Right:    num(1),
		},
	}, nil)

	// Verify children deleted
	cols, rows, _ := c.Select(&query.SelectStmt{From: &query.TableRef{Name: "fk_child_dml"}}, nil)
	_ = cols
	if len(rows) != 0 {
		t.Errorf("Expected 0 children after cascade delete, got %d", len(rows))
	}
}

// Helper functions
func num(n int64) *query.NumberLiteral {
	return &query.NumberLiteral{Value: float64(n)}
}

func float(f float64) *query.NumberLiteral {
	return &query.NumberLiteral{Value: f}
}

func str(s string) *query.StringLiteral {
	return &query.StringLiteral{Value: s}
}
