package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Targeted tests for lowest coverage functions
// ============================================================

// TestUpdateLocked_UNIQUEConstraint - tests UNIQUE constraint in UPDATE
func TestUpdateLocked_UNIQUEConstraint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table with UNIQUE column
	createCoverageTestTable(t, cat, "upd_unique", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText, Unique: true},
	})

	// Insert test data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_unique",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("ABC")}, {numReal(2), strReal("DEF")}},
	}, nil)

	// Try to update to a duplicate value - should fail
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_unique",
		Set:   []*query.SetClause{{Column: "code", Value: strReal("ABC")}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err == nil {
		t.Error("Expected error for UNIQUE constraint violation")
	}
}

// TestUpdateLocked_NOTNULLConstraint - tests NOT NULL constraint in UPDATE
func TestUpdateLocked_NOTNULLConstraint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table with NOT NULL column
	createCoverageTestTable(t, cat, "upd_notnull", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "required", Type: query.TokenText, NotNull: true},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_notnull",
		Columns: []string{"id", "required"},
		Values:  [][]query.Expression{{numReal(1), strReal("value")}},
	}, nil)

	// Try to set NULL on NOT NULL column - should fail
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_notnull",
		Set:   []*query.SetClause{{Column: "required", Value: &query.NullLiteral{}}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err == nil {
		t.Error("Expected error for NOT NULL constraint violation")
	}
}

// TestUpdateLocked_CHECKConstraint - tests CHECK constraint in UPDATE
func TestUpdateLocked_CHECKConstraint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table with CHECK constraint
	createCoverageTestTable(t, cat, "upd_check", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "age", Type: query.TokenInteger, Check: &query.BinaryExpr{
			Left:     colReal("age"),
			Operator: query.TokenGte,
			Right:    numReal(0),
		}},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_check",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{numReal(1), numReal(25)}},
	}, nil)

	// Try to set negative age - should fail CHECK constraint
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_check",
		Set:   []*query.SetClause{{Column: "age", Value: numReal(-5)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err == nil {
		t.Error("Expected error for CHECK constraint violation")
	}
}

// TestUpdateLocked_ForeignKeyConstraint - tests FK constraint in UPDATE
func TestUpdateLocked_ForeignKeyConstraint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create parent table
	createCoverageTestTable(t, cat, "fk_parent", []*query.ColumnDef{
		{Name: "parent_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Create child table with FK
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child",
		Columns: []*query.ColumnDef{
			{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent",
				ReferencedColumns: []string{"parent_id"},
				OnDelete:          "RESTRICT",
				OnUpdate:          "RESTRICT",
			},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent",
		Columns: []string{"parent_id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child",
		Columns: []string{"child_id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// Try to update to non-existent parent - should fail
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_child",
		Set:   []*query.SetClause{{Column: "parent_id", Value: numReal(999)}},
		Where: &query.BinaryExpr{Left: colReal("child_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err == nil {
		t.Error("Expected error for FK constraint violation")
	}
}

// TestUpdateLocked_PKChange - tests UPDATE that changes primary key
func TestUpdateLocked_PKChange(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "upd_pkchange", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_pkchange",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("one")}, {numReal(2), strReal("two")}},
	}, nil)

	// Update PK to new value
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_pkchange",
		Set:   []*query.SetClause{{Column: "id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("PK change error (may be expected): %v", err)
	}

	// Try to change PK to existing value - should fail
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_pkchange",
		Set:   []*query.SetClause{{Column: "id", Value: numReal(2)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(100)},
	}, nil)
	if err == nil {
		t.Error("Expected error for duplicate PK")
	}
}

// TestUpdateLocked_UNIQUEIndex - tests UNIQUE index in UPDATE
func TestUpdateLocked_UNIQUEIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "upd_uniq_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText},
	})

	// Create unique index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_email_uniq",
		Table:   "upd_uniq_idx",
		Columns: []string{"email"},
		Unique:  true,
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_uniq_idx",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{numReal(1), strReal("a@test.com")}, {numReal(2), strReal("b@test.com")}},
	}, nil)

	// Try to update to duplicate email - should fail
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_uniq_idx",
		Set:   []*query.SetClause{{Column: "email", Value: strReal("a@test.com")}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err == nil {
		t.Error("Expected error for UNIQUE index violation")
	}
}

// TestExecuteScalarSelect_MoreCases - tests executeScalarSelect paths
func TestExecuteScalarSelect_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Test CASE expression
	result, err := cat.ExecuteQuery(`SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END`)
	if err != nil {
		t.Logf("CASE expression error: %v", err)
	} else {
		t.Logf("CASE expression returned %d rows", len(result.Rows))
	}

	// Test column reference in scalar context (should error)
	_, err = cat.ExecuteQuery(`SELECT unknown_column`)
	if err == nil {
		t.Error("Expected error for unknown column in scalar select")
	}

	// Test BETWEEN expression
	result, err = cat.ExecuteQuery(`SELECT 5 BETWEEN 1 AND 10`)
	if err != nil {
		t.Logf("BETWEEN expression error: %v", err)
	}

	// Test IN expression with list
	result, err = cat.ExecuteQuery(`SELECT 3 IN (1, 2, 3, 4)`)
	if err != nil {
		t.Logf("IN expression error: %v", err)
	}

	// Test NULL comparison
	result, err = cat.ExecuteQuery(`SELECT NULL IS NULL`)
	if err != nil {
		t.Logf("IS NULL expression error: %v", err)
	}

	_ = result
}

// TestSelectLocked_MoreCases - tests selectLocked more paths
func TestSelectLocked_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "sel_distinct", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert duplicate values
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_distinct",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 3))}},
		}, nil)
	}

	// Test DISTINCT
	result, err := cat.ExecuteQuery(`SELECT DISTINCT val FROM sel_distinct ORDER BY val`)
	if err != nil {
		t.Logf("DISTINCT error: %v", err)
	} else {
		t.Logf("DISTINCT returned %d rows", len(result.Rows))
	}

	// Test LIMIT
	result, err = cat.ExecuteQuery(`SELECT * FROM sel_distinct LIMIT 3`)
	if err != nil {
		t.Logf("LIMIT error: %v", err)
	} else if len(result.Rows) != 3 {
		t.Errorf("LIMIT expected 3 rows, got %d", len(result.Rows))
	}

	// Test LIMIT with OFFSET
	result, err = cat.ExecuteQuery(`SELECT * FROM sel_distinct LIMIT 3 OFFSET 5`)
	if err != nil {
		t.Logf("LIMIT OFFSET error: %v", err)
	} else {
		t.Logf("LIMIT OFFSET returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestExecuteSelectWithJoinAndGroupBy_MoreCases - more tests for join+groupby
func TestExecuteSelectWithJoinAndGroupBy_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "jgb_dept", []*query.ColumnDef{
		{Name: "dept_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "jgb_emp", []*query.ColumnDef{
		{Name: "emp_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	// Insert departments
	for i := 1; i <= 3; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_dept",
			Columns: []string{"dept_id", "dept_name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Dept")}},
		}, nil)
	}
	// Insert employees
	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_emp",
			Columns: []string{"emp_id", "dept_id", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%3)+1)), numReal(float64(i * 100))}},
		}, nil)
	}

	// Test multiple JOIN conditions
	result, err := cat.ExecuteQuery(`
		SELECT d.dept_name, COUNT(*) as emp_count, SUM(e.salary) as total_salary,
		       AVG(e.salary) as avg_salary, MIN(e.salary) as min_salary, MAX(e.salary) as max_salary
		FROM jgb_dept d
		JOIN jgb_emp e ON d.dept_id = e.dept_id
		GROUP BY d.dept_id, d.dept_name
		ORDER BY total_salary DESC
	`)
	if err != nil {
		t.Logf("Complex JOIN+GROUP BY error: %v", err)
	} else {
		t.Logf("Complex JOIN+GROUP BY returned %d rows", len(result.Rows))
	}

	// Test CROSS JOIN simulation with GROUP BY
	_, err = cat.ExecuteQuery(`
		SELECT COUNT(*) as cnt FROM jgb_dept d, jgb_emp e
	`)
	if err != nil {
		t.Logf("CROSS JOIN error: %v", err)
	}

	_ = result
}

// TestDeleteWithUsing_MorePaths - additional DELETE USING tests
func TestDeleteWithUsing_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "del_orders", []*query.ColumnDef{
		{Name: "order_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cust_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "del_cust", []*query.ColumnDef{
		{Name: "cust_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_orders",
			Columns: []string{"order_id", "cust_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%3)+1)), numReal(float64(i * 10))}},
		}, nil)
	}
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_cust",
		Columns: []string{"cust_id", "status"},
		Values: [][]query.Expression{
			{numReal(1), strReal("active")},
			{numReal(2), strReal("inactive")},
			{numReal(3), strReal("deleted")},
		},
	}, nil)

	// DELETE USING with JOIN
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_orders",
		Using: []*query.TableRef{{Name: "del_cust"}},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: colReal("del_orders.cust_id"), Operator: query.TokenEq, Right: colReal("del_cust.cust_id")},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: colReal("del_cust.status"), Operator: query.TokenEq, Right: strReal("deleted")},
		},
	}, nil)
	if err != nil {
		t.Logf("DELETE USING error (may be expected): %v", err)
	}
}

// TestResolveOuterRefsInQuery_More - tests resolveOuterRefsInQuery
func TestResolveOuterRefsInQuery_More(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "outer_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, cat, "outer_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_sub",
			Columns: []string{"id", "main_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// Correlated subquery with EXISTS
	result, err := cat.ExecuteQuery(`
		SELECT * FROM outer_main m
		WHERE EXISTS (
			SELECT 1 FROM outer_sub s
			WHERE s.main_id = m.id AND s.id > 2
		)
	`)
	if err != nil {
		t.Logf("Correlated EXISTS error: %v", err)
	} else {
		t.Logf("Correlated EXISTS returned %d rows", len(result.Rows))
	}

	// Correlated subquery with NOT EXISTS
	result, err = cat.ExecuteQuery(`
		SELECT * FROM outer_main m
		WHERE NOT EXISTS (
			SELECT 1 FROM outer_sub s
			WHERE s.main_id = m.id AND s.id > 10
		)
	`)
	if err != nil {
		t.Logf("Correlated NOT EXISTS error: %v", err)
	}

	_ = result
}

// TestApplyOuterQuery_More - tests applyOuterQuery
func TestApplyOuterQuery_More(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "view_agg_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "amt", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "view_agg_base",
			Columns: []string{"id", "grp", "amt"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i))}},
		}, nil)
	}

	// Create view with GROUP BY
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "grp"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Column: "amt"}}},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
		},
		From:       &query.TableRef{Name: "view_agg_base"},
		GroupBy:    []query.Expression{&query.QualifiedIdentifier{Column: "grp"}},
		OrderBy:    []*query.OrderByExpr{{Expr: &query.QualifiedIdentifier{Column: "grp"}}},
	}
	cat.CreateView("agg_view", viewStmt)

	// Query view with HAVING on aggregate
	result, err := cat.ExecuteQuery(`
		SELECT * FROM agg_view
		WHERE 2 > 0
		ORDER BY 2 DESC
	`)
	if err != nil {
		t.Logf("View with WHERE error: %v", err)
	} else {
		t.Logf("View query returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestEvaluateWindowFunctions_More - tests evaluateWindowFunctions
func TestEvaluateWindowFunctions_More(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "win_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "salary", Type: query.TokenInteger},
	})

	data := []struct {
		id     int
		dept   string
		salary int
	}{
		{1, "IT", 100}, {2, "IT", 200}, {3, "IT", 300},
		{4, "HR", 150}, {5, "HR", 250},
	}
	for _, d := range data {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_test",
			Columns: []string{"id", "dept", "salary"},
			Values:  [][]query.Expression{{numReal(float64(d.id)), strReal(d.dept), numReal(float64(d.salary))}},
		}, nil)
	}

	// Window function with PARTITION BY only (no ORDER BY)
	result, err := cat.ExecuteQuery(`
		SELECT dept, salary,
			SUM(salary) OVER (PARTITION BY dept) as dept_total
		FROM win_test
		ORDER BY dept, salary
	`)
	if err != nil {
		t.Logf("Window PARTITION BY only error: %v", err)
	} else {
		t.Logf("Window PARTITION BY returned %d rows", len(result.Rows))
	}

	// Multiple window functions
	result, err = cat.ExecuteQuery(`
		SELECT id, dept, salary,
			ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn,
			SUM(salary) OVER (PARTITION BY dept ORDER BY salary) as running_sum
		FROM win_test
		ORDER BY dept, rn
	`)
	if err != nil {
		t.Logf("Multiple window functions error: %v", err)
	}

	_ = result
}
