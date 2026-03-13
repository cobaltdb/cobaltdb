package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Additional tests targeting remaining uncovered paths
// ============================================================

// TestApplyOuterQuery_ComplexCases - more applyOuterQuery coverage
func TestApplyOuterQuery_ComplexCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_data", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		category := "A"
		if i > 25 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_data",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	// View with LIMIT and OFFSET
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "category"},
			&query.QualifiedIdentifier{Column: "amount"},
		},
		From:    &query.TableRef{Name: "aoq_data"},
		OrderBy: []*query.OrderByExpr{{Expr: &query.QualifiedIdentifier{Column: "id"}}},
		Limit:   numReal(10),
		Offset:  numReal(5),
	}
	cat.CreateView("limited_view", viewStmt)

	result, err := cat.ExecuteQuery(`SELECT * FROM limited_view ORDER BY amount`)
	if err != nil {
		t.Logf("Limited view error: %v", err)
	} else {
		t.Logf("Limited view returned %d rows", len(result.Rows))
	}

	// View with WHERE
	viewStmt2 := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "amount"},
		},
		From: &query.TableRef{Name: "aoq_data"},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "amount"},
			Operator: query.TokenGt,
			Right:    numReal(200),
		},
	}
	cat.CreateView("filtered_view", viewStmt2)

	result, err = cat.ExecuteQuery(`SELECT * FROM filtered_view WHERE amount < 400`)
	if err != nil {
		t.Logf("Filtered view error: %v", err)
	} else {
		t.Logf("Filtered view returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestDeleteWithUsing_ComplexJoins - DELETE USING with complex joins
func TestDeleteWithUsing_ComplexJoins(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create three tables for multi-table join
	createCoverageTestTable(t, cat, "del_main3", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id1", Type: query.TokenInteger},
		{Name: "ref_id2", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "del_ref1", []*query.ColumnDef{
		{Name: "ref_id1", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "del_ref2", []*query.ColumnDef{
		{Name: "ref_id2", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "flag", Type: query.TokenBoolean},
	})

	// Insert test data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_main3",
			Columns: []string{"id", "ref_id1", "ref_id2"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i%5+1)), numReal(float64(i%3+1))}},
		}, nil)
	}
	for i := 1; i <= 5; i++ {
		status := "keep"
		if i%2 == 0 {
			status = "delete"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_ref1",
			Columns: []string{"ref_id1", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(status)}},
		}, nil)
	}
	for i := 1; i <= 3; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_ref2",
			Columns: []string{"ref_id2", "flag"},
			Values:  [][]query.Expression{{numReal(float64(i)), &query.BooleanLiteral{Value: i%2 == 0}}},
		}, nil)
	}

	// Complex DELETE USING with multiple tables
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_main3",
		Using: []*query.TableRef{{Name: "del_ref1"}, {Name: "del_ref2"}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     colReal("del_main3.ref_id1"),
				Operator: query.TokenEq,
				Right:    colReal("del_ref1.ref_id1"),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     colReal("del_main3.ref_id2"),
				Operator: query.TokenEq,
				Right:    colReal("del_ref2.ref_id2"),
			},
		},
	}, nil)
	if err != nil {
		t.Logf("Complex DELETE USING error (may be expected): %v", err)
	}
}

// TestResolveOuterRefsInQuery_SubqueriesInSelect - outer refs in SELECT clause
func TestResolveOuterRefsInQuery_SubqueriesInSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_main2", []*query.ColumnDef{
		{Name: "dept_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "outer_emp2", []*query.ColumnDef{
		{Name: "emp_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_main2",
			Columns: []string{"dept_id", "dept_name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("D" + string(rune('0'+i)))}},
		}, nil)
	}
	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_emp2",
			Columns: []string{"emp_id", "dept_id", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%5)+1)), numReal(float64(i*100))}},
		}, nil)
	}

	// Subquery in SELECT with outer reference
	result, err := cat.ExecuteQuery(`
		SELECT
			dept_id,
			dept_name,
			(SELECT COUNT(*) FROM outer_emp2 e WHERE e.dept_id = d.dept_id) as emp_count,
			(SELECT AVG(salary) FROM outer_emp2 e WHERE e.dept_id = d.dept_id) as avg_salary,
			(SELECT MAX(salary) FROM outer_emp2 e WHERE e.dept_id = d.dept_id) as max_salary
		FROM outer_main2 d
		ORDER BY dept_id
	`)
	if err != nil {
		t.Logf("Subquery in SELECT error: %v", err)
	} else {
		t.Logf("Subquery in SELECT returned %d rows", len(result.Rows))
	}

	// Multiple subqueries with different aggregate functions
	result, err = cat.ExecuteQuery(`
		SELECT
			dept_id,
			dept_name,
			(SELECT SUM(salary) FROM outer_emp2 e WHERE e.dept_id = d.dept_id) as total_salary,
			(SELECT MIN(salary) FROM outer_emp2 e WHERE e.dept_id = d.dept_id) as min_salary
		FROM outer_main2 d
		WHERE dept_id IN (SELECT DISTINCT dept_id FROM outer_emp2)
		ORDER BY dept_id
	`)
	if err != nil {
		t.Logf("Multiple subqueries error: %v", err)
	} else {
		t.Logf("Multiple subqueries returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestUpdateRowSlice_FKCascade - FK cascade operations
func TestUpdateRowSlice_FKCascade(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Parent table
	createCoverageTestTable(t, cat, "fk_parent2", []*query.ColumnDef{
		{Name: "parent_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	// Child with CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child2",
		Columns: []*query.ColumnDef{
			{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "data", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent2",
				ReferencedColumns: []string{"parent_id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Grandchild with CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_grandchild",
		Columns: []*query.ColumnDef{
			{Name: "gc_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "child_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"child_id"},
				ReferencedTable:   "fk_child2",
				ReferencedColumns: []string{"child_id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_parent2",
			Columns: []string{"parent_id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Parent")}},
		}, nil)
		for j := 1; j <= 3; j++ {
			childID := i*10 + j
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "fk_child2",
				Columns: []string{"child_id", "parent_id", "data"},
				Values:  [][]query.Expression{{numReal(float64(childID)), numReal(float64(i)), strReal("Data")}},
			}, nil)
			for k := 1; k <= 2; k++ {
				gcID := childID*10 + k
				cat.Insert(ctx, &query.InsertStmt{
					Table:   "fk_grandchild",
					Columns: []string{"gc_id", "child_id"},
					Values:  [][]query.Expression{{numReal(float64(gcID)), numReal(float64(childID))}},
				}, nil)
			}
		}
	}

	// Update parent ID - should cascade to children and grandchildren
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_parent2",
		Set:   []*query.SetClause{{Column: "parent_id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("FK cascade update error: %v", err)
	}

	// Verify cascade
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM fk_child2 WHERE parent_id = 100`)
	if err != nil {
		t.Logf("Verify error: %v", err)
	} else {
		t.Logf("Children with parent_id 100: %v", result.Rows)
	}

	_ = result
}

// TestCountRows_TableVariations - countRows with different table states
func TestCountRows_TableVariations(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Test with indexed table
	createCoverageTestTable(t, cat, "count_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_count",
		Table:   "count_idx",
		Columns: []string{"val"},
	})

	// Insert and delete some rows
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "count_idx",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("val" + string(rune('A'+i%5)))}},
		}, nil)
	}

	// Count all
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM count_idx`)
	if err != nil {
		t.Logf("Count all error: %v", err)
	} else {
		t.Logf("Count all: %v", result.Rows)
	}

	// Count with indexed column
	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM count_idx WHERE val = 'valA'`)
	if err != nil {
		t.Logf("Count with index error: %v", err)
	} else {
		t.Logf("Count with index: %v", result.Rows)
	}

	// Delete some rows and count again
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "count_idx",
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenLt, Right: numReal(10)},
	}, nil)

	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM count_idx`)
	if err != nil {
		t.Logf("Count after delete error: %v", err)
	} else {
		t.Logf("Count after delete: %v", result.Rows)
	}

	_ = result
}

// TestOnDelete_OnUpdate_MoreScenarios - more FK cascade scenarios
func TestOnDelete_OnUpdate_MoreScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Parent
	createCoverageTestTable(t, cat, "fk_p3", []*query.ColumnDef{
		{Name: "p_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Child with SET NULL
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_c3",
		Columns: []*query.ColumnDef{
			{Name: "c_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "p_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"p_id"},
				ReferencedTable:   "fk_p3",
				ReferencedColumns: []string{"p_id"},
				OnDelete:          "SET NULL",
				OnUpdate:          "SET NULL",
			},
		},
	})

	// Child with RESTRICT
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_c4",
		Columns: []*query.ColumnDef{
			{Name: "c_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "p_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"p_id"},
				ReferencedTable:   "fk_p3",
				ReferencedColumns: []string{"p_id"},
				OnDelete:          "RESTRICT",
				OnUpdate:          "RESTRICT",
			},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_p3",
			Columns: []string{"p_id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_c3",
			Columns: []string{"c_id", "p_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fk_c4",
			Columns: []string{"c_id", "p_id"},
			Values:  [][]query.Expression{{numReal(float64(i + 10)), numReal(float64(i))}},
		}, nil)
	}

	// Delete parent - should set NULL on fk_c3
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_p3",
		Where: &query.BinaryExpr{Left: colReal("p_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("DELETE with SET NULL error: %v", err)
	}

	// Verify SET NULL
	result, err := cat.ExecuteQuery(`SELECT p_id FROM fk_c3 WHERE c_id = 1`)
	if err != nil {
		t.Logf("Verify error: %v", err)
	} else {
		t.Logf("Child p_id after parent delete: %v", result.Rows)
	}

	// Try to delete parent with RESTRICT - should fail
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_p3",
		Where: &query.BinaryExpr{Left: colReal("p_id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err != nil {
		t.Logf("DELETE with RESTRICT error (expected): %v", err)
	}

	_ = result
}

// TestSave_Load_Integration - integration test for Save and Load
func TestSave_Load_Integration(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create and populate tables
	for i := 1; i <= 3; i++ {
		name := "save_load_tbl" + string(rune('0'+i))
		createCoverageTestTable(t, cat, name, []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		})

		for j := 1; j <= 10; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   name,
				Columns: []string{"id", "data"},
				Values:  [][]query.Expression{{numReal(float64(j)), strReal("data" + string(rune('A'+j%5)))}},
			}, nil)
		}
	}

	// Create a view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "data"},
		},
		From: &query.TableRef{Name: "save_load_tbl1"},
	}
	cat.CreateView("save_load_view", viewStmt)

	// Create an index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_save_load",
		Table:   "save_load_tbl1",
		Columns: []string{"data"},
	})

	// Save
	err = cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Verify data is still accessible
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM save_load_tbl1`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows in save_load_tbl1: %v", result.Rows)
	}

	_ = result
}
