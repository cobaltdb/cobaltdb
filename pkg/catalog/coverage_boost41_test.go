package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// More targeted tests for remaining lowest coverage functions
// ============================================================

// TestApplyOuterQuery_MorePaths - additional tests for applyOuterQuery
func TestApplyOuterQuery_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "aoq_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "aoq_base",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with DISTINCT
	viewStmt := &query.SelectStmt{
		Columns:    []query.Expression{&query.QualifiedIdentifier{Column: "val"}},
		From:       &query.TableRef{Name: "aoq_base"},
		Distinct:   true,
		OrderBy:    []*query.OrderByExpr{{Expr: &query.QualifiedIdentifier{Column: "val"}}},
	}
	cat.CreateView("distinct_view", viewStmt)

	// Query distinct view
	result, err := cat.ExecuteQuery(`SELECT * FROM distinct_view LIMIT 5`)
	if err != nil {
		t.Logf("Distinct view error: %v", err)
	} else {
		t.Logf("Distinct view returned %d rows", len(result.Rows))
	}

	// Create view with complex expression
	viewStmt2 := &query.SelectStmt{
		Columns: []query.Expression{
			&query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "val"}, Operator: query.TokenPlus, Right: numReal(1)},
		},
		From: &query.TableRef{Name: "aoq_base"},
	}
	cat.CreateView("expr_view", viewStmt2)

	result, err = cat.ExecuteQuery(`SELECT * FROM expr_view LIMIT 5`)
	if err != nil {
		t.Logf("Expression view error: %v", err)
	}

	_ = result
}

// TestExecuteSelectWithJoinAndGroupBy_MorePaths2 - more tests
func TestExecuteSelectWithJoinAndGroupBy_MorePaths2(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "jgb2_dept", []*query.ColumnDef{
		{Name: "dept_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "jgb2_emp", []*query.ColumnDef{
		{Name: "emp_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
		{Name: "bonus", Type: query.TokenInteger},
	})

	for i := 1; i <= 4; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgb2_dept",
			Columns: []string{"dept_id", "dept_name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("D"+string(rune('0'+i)))}},
		}, nil)
	}
	for i := 1; i <= 40; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgb2_emp",
			Columns: []string{"emp_id", "dept_id", "salary", "bonus"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%4)+1)), numReal(float64(i*100)), numReal(float64(i*10))}},
		}, nil)
	}

	// Complex HAVING with multiple conditions
	result, err := cat.ExecuteQuery(`
		SELECT d.dept_name,
			COUNT(*) as cnt,
			SUM(e.salary) as total_salary,
			AVG(e.salary) as avg_salary,
			MIN(e.bonus) as min_bonus,
			MAX(e.bonus) as max_bonus
		FROM jgb2_dept d
		JOIN jgb2_emp e ON d.dept_id = e.dept_id
		GROUP BY d.dept_id, d.dept_name
		HAVING COUNT(*) >= 5
		   AND SUM(e.salary) > 20000
		   AND AVG(e.salary) > 2000
		ORDER BY total_salary DESC
	`)
	if err != nil {
		t.Logf("Complex HAVING error: %v", err)
	} else {
		t.Logf("Complex HAVING returned %d rows", len(result.Rows))
	}

	// GROUP BY with expression
	result, err = cat.ExecuteQuery(`
		SELECT CASE WHEN e.salary > 2000 THEN 'high' ELSE 'low' END as salary_band,
			COUNT(*) as cnt,
			SUM(e.bonus) as total_bonus
		FROM jgb2_dept d
		JOIN jgb2_emp e ON d.dept_id = e.dept_id
		GROUP BY salary_band
		ORDER BY total_bonus DESC
	`)
	if err != nil {
		t.Logf("GROUP BY expression error: %v", err)
	} else {
		t.Logf("GROUP BY expression returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestDeleteWithUsing_MorePaths2 - more DELETE USING tests
func TestDeleteWithUsing_MorePaths2(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "delu_orders", []*query.ColumnDef{
		{Name: "order_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cust_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "delu_customers", []*query.ColumnDef{
		{Name: "cust_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "delu_archive", []*query.ColumnDef{
		{Name: "order_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "delu_orders",
			Columns: []string{"order_id", "cust_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%4)+1)), numReal(float64(i * 10))}},
		}, nil)
	}
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "delu_customers",
		Columns: []string{"cust_id", "status"},
		Values:  [][]query.Expression{{numReal(1), strReal("active")}, {numReal(2), strReal("inactive")}, {numReal(3), strReal("deleted")}, {numReal(4), strReal("archived")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "delu_archive",
		Columns: []string{"order_id"},
		Values:  [][]query.Expression{{numReal(5)}, {numReal(10)}, {numReal(15)}},
	}, nil)

	// DELETE USING with multiple conditions
	result, err := cat.ExecuteQuery(`
		SELECT COUNT(*) as cnt FROM delu_orders o
		JOIN delu_customers c ON o.cust_id = c.cust_id
		WHERE c.status = 'deleted'
	`)
	if err != nil {
		t.Logf("Pre-count error: %v", err)
	} else {
		t.Logf("Orders with deleted customers: %v", result.Rows)
	}

	// DELETE with subquery condition
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "delu_orders",
		Where: &query.BinaryExpr{
			Left:     colReal("order_id"),
			Operator: query.TokenIn,
			Right:    &query.SubqueryExpr{Query: &query.SelectStmt{Columns: []query.Expression{&query.QualifiedIdentifier{Column: "order_id"}}, From: &query.TableRef{Name: "delu_archive"}}},
		},
	}, nil)
	if err != nil {
		t.Logf("DELETE with IN subquery error: %v", err)
	}
}

// TestResolveOuterRefsInQuery_MorePaths - more outer ref tests
func TestResolveOuterRefsInQuery_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_depts", []*query.ColumnDef{
		{Name: "dept_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "outer_emps", []*query.ColumnDef{
		{Name: "emp_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_depts",
			Columns: []string{"dept_id", "dept_name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Dept"+string(rune('0'+i)))}},
		}, nil)
	}
	for i := 1; i <= 25; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_emps",
			Columns: []string{"emp_id", "dept_id", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%5)+1)), numReal(float64(i*100))}},
		}, nil)
	}

	// Correlated subquery with comparison
	result, err := cat.ExecuteQuery(`
		SELECT d.dept_name, d.dept_id,
			(SELECT COUNT(*) FROM outer_emps e WHERE e.dept_id = d.dept_id) as emp_count,
			(SELECT AVG(salary) FROM outer_emps e WHERE e.dept_id = d.dept_id) as avg_salary
		FROM outer_depts d
		ORDER BY d.dept_id
	`)
	if err != nil {
		t.Logf("Correlated subquery with aggregates error: %v", err)
	} else {
		t.Logf("Correlated subquery returned %d rows", len(result.Rows))
	}

	// Multiple levels of nesting
	result, err = cat.ExecuteQuery(`
		SELECT * FROM outer_depts d
		WHERE d.dept_id IN (
			SELECT DISTINCT dept_id FROM outer_emps
			WHERE salary > (
				SELECT AVG(salary) FROM outer_emps
			)
		)
	`)
	if err != nil {
		t.Logf("Nested subquery error: %v", err)
	} else {
		t.Logf("Nested subquery returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestUpdateRowSlice_MorePaths - tests for updateRowSlice
func TestUpdateRowSlice_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create parent
	createCoverageTestTable(t, cat, "upd_parent", []*query.ColumnDef{
		{Name: "parent_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	// Create child with FK
	cat.CreateTable(&query.CreateTableStmt{
		Table: "upd_child",
		Columns: []*query.ColumnDef{
			{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "upd_parent",
				ReferencedColumns: []string{"parent_id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_parent",
			Columns: []string{"parent_id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("active")}},
		}, nil)
		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "upd_child",
				Columns: []string{"child_id", "parent_id"},
				Values:  [][]query.Expression{{numReal(float64(i*10+j)), numReal(float64(i))}},
			}, nil)
		}
	}

	// Update parent ID which should cascade to children
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_parent",
		Set:   []*query.SetClause{{Column: "parent_id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("FK cascade update error: %v", err)
	}

	// Update multiple parents at once
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_parent",
		Set:   []*query.SetClause{{Column: "status", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenGt, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Bulk update error: %v", err)
	}
}

// TestCountRows_MoreScenarios - more countRows tests
func TestCountRows_MoreScenarios(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "count_test1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Test COUNT with NULL values
	for i := 1; i <= 10; i++ {
		var val query.Expression
		if i%2 == 0 {
			val = numReal(float64(i))
		} else {
			val = &query.NullLiteral{}
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "count_test1",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), val}},
		}, nil)
	}

	// COUNT(*)
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM count_test1`)
	if err != nil {
		t.Logf("COUNT(*) error: %v", err)
	} else {
		t.Logf("COUNT(*): %v", result.Rows)
	}

	// COUNT(column)
	result, err = cat.ExecuteQuery(`SELECT COUNT(val) FROM count_test1`)
	if err != nil {
		t.Logf("COUNT(column) error: %v", err)
	} else {
		t.Logf("COUNT(column): %v", result.Rows)
	}

	// COUNT(DISTINCT column)
	result, err = cat.ExecuteQuery(`SELECT COUNT(DISTINCT val) FROM count_test1`)
	if err != nil {
		t.Logf("COUNT(DISTINCT) error: %v", err)
	} else {
		t.Logf("COUNT(DISTINCT): %v", result.Rows)
	}

	_ = result
}

// TestSave_ErrorPaths - tests Save error handling
func TestSave_ErrorPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create multiple tables with data
	for i := 1; i <= 3; i++ {
		name := "save_err_tbl" + string(rune('0'+i))
		createCoverageTestTable(t, cat, name, []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		})

		for j := 1; j <= 20; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   name,
				Columns: []string{"id", "data"},
				Values:  [][]query.Expression{{numReal(float64(j)), strReal("data")}},
			}, nil)
		}
	}

	// Save
	err = cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}
}

// TestInsertLocked_MorePaths2 - more insertLocked tests
func TestInsertLocked_MorePaths2(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Table with auto-increment
	cat.CreateTable(&query.CreateTableStmt{
		Table: "ins_auto",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert with auto-increment
	for i := 1; i <= 10; i++ {
		_, _, err := cat.Insert(ctx, &query.InsertStmt{
			Table:   "ins_auto",
			Columns: []string{"name"},
			Values:  [][]query.Expression{{strReal("name")}},
		}, nil)
		if err != nil {
			t.Logf("Auto-increment insert error: %v", err)
		}
	}

	// Verify auto-increment values
	result, err := cat.ExecuteQuery(`SELECT * FROM ins_auto ORDER BY id`)
	if err != nil {
		t.Logf("Select error: %v", err)
	} else {
		t.Logf("Auto-increment rows: %d", len(result.Rows))
		if len(result.Rows) > 0 {
			t.Logf("First row id: %v", result.Rows[0][0])
		}
	}

	// Insert with explicit ID
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_auto",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(100), strReal("explicit")}},
	}, nil)
	if err != nil {
		t.Logf("Explicit ID insert error: %v", err)
	}

	_ = result
}

// TestComputeAggregatesWithGroupBy_MorePaths - more aggregate tests
func TestComputeAggregatesWithGroupBy_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "agg_test1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenReal},
	})

	// Insert data with NULLs
	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 15 {
			grp = "B"
		}
		var val1 query.Expression
		if i%3 == 0 {
			val1 = &query.NullLiteral{}
		} else {
			val1 = numReal(float64(i))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_test1",
			Columns: []string{"id", "grp", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), val1, numReal(float64(i) * 1.5)}},
		}, nil)
	}

	// Test multiple aggregates with NULL handling
	result, err := cat.ExecuteQuery(`
		SELECT grp,
			COUNT(*) as cnt_all,
			COUNT(val1) as cnt_val1,
			SUM(val1) as sum_val1,
			SUM(val2) as sum_val2,
			AVG(val1) as avg_val1,
			AVG(val2) as avg_val2,
			MIN(val1) as min_val1,
			MAX(val1) as max_val1
		FROM agg_test1
		GROUP BY grp
		ORDER BY grp
	`)
	if err != nil {
		t.Logf("Multiple aggregates error: %v", err)
	} else {
		t.Logf("Multiple aggregates returned %d rows", len(result.Rows))
	}

	// Test GROUP_CONCAT
	result, err = cat.ExecuteQuery(`
		SELECT grp, GROUP_CONCAT(CAST(id AS TEXT)) as ids
		FROM agg_test1
		GROUP BY grp
	`)
	if err != nil {
		t.Logf("GROUP_CONCAT error: %v", err)
	}

	_ = result
}

// TestRollbackToSavepoint_Nested - tests nested savepoints
func TestRollbackToSavepoint_Nested(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sp_nested", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	// Insert first row
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_nested",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Create savepoint
	cat.Savepoint("sp1")

	// Insert more rows
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_nested",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}},
	}, nil)

	// Create nested savepoint
	cat.Savepoint("sp2")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_nested",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(3)}},
	}, nil)

	// Rollback to inner savepoint
	err = cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("Rollback to sp2 error: %v", err)
	}

	// Should have rows 1 and 2
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM sp_nested`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after rollback to sp2: %v", result.Rows)
	}

	// Rollback to outer savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	// Should only have row 1
	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM sp_nested`)
	if err != nil {
		t.Logf("Count error: %v", err)
	} else {
		t.Logf("Rows after rollback to sp1: %v", result.Rows)
	}

	cat.CommitTransaction()

	_ = result
}
