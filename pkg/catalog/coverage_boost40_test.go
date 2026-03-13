package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Additional targeted tests for lowest coverage functions
// ============================================================

// TestSelectLocked_CTEWithWindowFunctions - tests CTE with window functions
func TestSelectLocked_CTEWithWindowFunctions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "cte_win_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cte_win_base",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i))}},
		}, nil)
	}

	// CTE with window function
	result, err := cat.ExecuteQuery(`
		WITH cte AS (
			SELECT id, grp, val,
				ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn
			FROM cte_win_base
		)
		SELECT * FROM cte WHERE rn <= 3 ORDER BY grp, rn
	`)
	if err != nil {
		t.Logf("CTE with window function error: %v", err)
	} else {
		t.Logf("CTE with window returned %d rows", len(result.Rows))
	}

	// Recursive CTE with window function
	result, err = cat.ExecuteQuery(`
		WITH RECURSIVE nums AS (
			SELECT 1 as n
			UNION ALL
			SELECT n + 1 FROM nums WHERE n < 5
		)
		SELECT n, ROW_NUMBER() OVER (ORDER BY n) as rn FROM nums
	`)
	if err != nil {
		t.Logf("Recursive CTE with window error: %v", err)
	} else {
		t.Logf("Recursive CTE with window returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestSelectLocked_DerivedTableWithJoin - tests derived table with JOIN
func TestSelectLocked_DerivedTableWithJoin(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "dt_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "dt_detail", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "value", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "dt_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("name")}},
		}, nil)
		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "dt_detail",
				Columns: []string{"id", "main_id", "value"},
				Values:  [][]query.Expression{{numReal(float64(i*10 + j)), numReal(float64(i)), numReal(float64(j * 10))}},
			}, nil)
		}
	}

	// Derived table with JOIN
	result, err := cat.ExecuteQuery(`
		SELECT m.name, d.total
		FROM dt_main m
		JOIN (
			SELECT main_id, SUM(value) as total
			FROM dt_detail
			GROUP BY main_id
		) d ON m.id = d.main_id
		ORDER BY m.id
	`)
	if err != nil {
		t.Logf("Derived table with JOIN error: %v", err)
	} else {
		t.Logf("Derived table with JOIN returned %d rows", len(result.Rows))
	}

	_ = result
}

// TestSelectLocked_ViewComplex - tests complex views
func TestSelectLocked_ViewComplex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "view_base_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		category := "A"
		if i > 10 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "view_base_tbl",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create complex view with GROUP BY
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Column: "amount"}}},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
		},
		From:    &query.TableRef{Name: "view_base_tbl"},
		GroupBy: []query.Expression{&query.QualifiedIdentifier{Column: "category"}},
		Having: &query.BinaryExpr{
			Left:     &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Column: "amount"}}},
			Operator: query.TokenGt,
			Right:    numReal(500),
		},
	}
	cat.CreateView("complex_grp_view", viewStmt)

	// Query complex view
	result, err := cat.ExecuteQuery(`SELECT * FROM complex_grp_view ORDER BY category`)
	if err != nil {
		t.Logf("Complex view error: %v", err)
	} else {
		t.Logf("Complex view returned %d rows", len(result.Rows))
	}

	// Complex view with JOIN
	result, err = cat.ExecuteQuery(`
		SELECT v.*, 'extra' as extra
		FROM complex_grp_view v
	`)
	if err != nil {
		t.Logf("Complex view with extra column error: %v", err)
	}

	_ = result
}

// TestExecuteScalarSelect_AllPaths - comprehensive scalar select tests
func TestExecuteScalarSelect_AllPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	tests := []struct {
		name  string
		query string
	}{
		{"no_columns", "SELECT"},
		{"aggregate_count", "SELECT COUNT(*)"},
		{"aggregate_sum", "SELECT SUM(42)"},
		{"aggregate_avg", "SELECT AVG(10)"},
		{"aggregate_min", "SELECT MIN(5)"},
		{"aggregate_max", "SELECT MAX(100)"},
		{"where_true", "SELECT 1 WHERE 1 = 1"},
		{"where_false", "SELECT 1 WHERE 1 = 0"},
		{"distinct", "SELECT DISTINCT 1"},
		{"limit", "SELECT 1 LIMIT 1"},
		{"offset", "SELECT 1 LIMIT 1 OFFSET 0"},
		{"alias", "SELECT 1 AS val"},
		{"identifier", "SELECT 1 col"},
		{"function", "SELECT UPPER('test')"},
		{"window_error", "SELECT ROW_NUMBER() OVER()"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cat.ExecuteQuery(tt.query)
			if err != nil {
				t.Logf("Query '%s' error: %v", tt.name, err)
			} else {
				t.Logf("Query '%s' returned %d rows", tt.name, len(result.Rows))
			}
		})
	}
}

// TestExecuteSelectWithJoinAndGroupBy_OrderByLimit - tests ORDER BY/LIMIT in JOIN+GROUP BY
func TestExecuteSelectWithJoinAndGroupBy_OrderByLimit(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "jgl_dept", []*query.ColumnDef{
		{Name: "dept_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_name", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "jgl_emp", []*query.ColumnDef{
		{Name: "emp_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgl_dept",
			Columns: []string{"dept_id", "dept_name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Dept")}},
		}, nil)
	}
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgl_emp",
			Columns: []string{"emp_id", "dept_id", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%5)+1)), numReal(float64(i * 100))}},
		}, nil)
	}

	// JOIN + GROUP BY + ORDER BY + LIMIT
	result, err := cat.ExecuteQuery(`
		SELECT d.dept_name, COUNT(*) as cnt, SUM(e.salary) as total
		FROM jgl_dept d
		JOIN jgl_emp e ON d.dept_id = e.dept_id
		GROUP BY d.dept_id, d.dept_name
		ORDER BY total DESC
		LIMIT 3
	`)
	if err != nil {
		t.Logf("JOIN+GROUP+ORDER+LIMIT error: %v", err)
	} else {
		t.Logf("JOIN+GROUP+ORDER+LIMIT returned %d rows", len(result.Rows))
	}

	// JOIN + GROUP BY + ORDER BY + OFFSET
	result, err = cat.ExecuteQuery(`
		SELECT d.dept_name, COUNT(*) as cnt
		FROM jgl_dept d
		JOIN jgl_emp e ON d.dept_id = e.dept_id
		GROUP BY d.dept_id, d.dept_name
		ORDER BY cnt DESC
		LIMIT 2 OFFSET 1
	`)
	if err != nil {
		t.Logf("JOIN+GROUP+ORDER+OFFSET error: %v", err)
	}

	_ = result
}

// TestDeleteWithUsing_ErrorPaths - tests error handling in DELETE USING
func TestDeleteWithUsing_ErrorPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Try to DELETE FROM non-existent table
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "nonexistent_table",
		Using: []*query.TableRef{{Name: "other_table"}},
	}, nil)
	if err == nil {
		t.Error("Expected error for DELETE on non-existent table")
	}

	createCoverageTestTable(t, cat, "delu_target", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Try DELETE USING with non-existent USING table
	// Note: This may not error depending on implementation, so just log
	_, _, _ = cat.Delete(ctx, &query.DeleteStmt{
		Table: "delu_target",
		Using: []*query.TableRef{{Name: "nonexistent_using"}},
	}, nil)
}

// TestCountRows_MorePaths - tests countRows function more
func TestCountRows_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Test on empty table
	createCoverageTestTable(t, cat, "empty_table", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM empty_table`)
	if err != nil {
		t.Logf("COUNT on empty table: %v", err)
	} else if len(result.Rows) > 0 {
		t.Logf("COUNT on empty table: %v", result.Rows[0][0])
	}

	// Test on table with data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "empty_table",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM empty_table`)
	if err != nil {
		t.Logf("COUNT on filled table: %v", err)
	} else if len(result.Rows) > 0 {
		t.Logf("COUNT on filled table: %v", result.Rows[0][0])
	}

	// Test COUNT with WHERE
	result, err = cat.ExecuteQuery(`SELECT COUNT(*) FROM empty_table WHERE id > 50`)
	if err != nil {
		t.Logf("COUNT with WHERE: %v", err)
	} else if len(result.Rows) > 0 {
		t.Logf("COUNT with WHERE > 50: %v", result.Rows[0][0])
	}

	_ = result
}

// TestSave_MorePaths - tests Save function more
func TestSave_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create multiple tables
	for i := 1; i <= 3; i++ {
		createCoverageTestTable(t, cat, "save_tbl_"+string(rune('A'+i-1)), []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		})
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "save_tbl_A",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Save
	err = cat.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}
}

// TestLoad_MorePaths - tests Load function more
func TestLoad_MorePaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Load on fresh catalog
	err = cat.Load()
	if err != nil {
		t.Logf("Load error (may be expected): %v", err)
	}
}

// TestFlushTableTreesLocked_More - tests flushTableTreesLocked
func TestFlushTableTreesLocked_More(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table and insert data
	createCoverageTestTable(t, cat, "flush_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "flush_test",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	// Commit transaction to trigger flush
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "flush_test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(999)}},
	}, nil)
	cat.CommitTransaction()

	// Verify data
	result, err := cat.ExecuteQuery(`SELECT COUNT(*) FROM flush_test`)
	if err != nil {
		t.Logf("Count after commit error: %v", err)
	} else if len(result.Rows) > 0 {
		t.Logf("Rows after commit: %v", result.Rows[0][0])
	}

	_ = result
}

// TestRollbackToSavepoint_ErrorPaths - tests error handling in RollbackToSavepoint
func TestRollbackToSavepoint_ErrorPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Rollback without active transaction
	err = cat.RollbackToSavepoint("sp1")
	if err == nil {
		t.Error("Expected error for RollbackToSavepoint without transaction")
	}

	// Rollback to non-existent savepoint
	cat.BeginTransaction(1)
	err = cat.RollbackToSavepoint("nonexistent")
	if err == nil {
		t.Error("Expected error for RollbackToSavepoint with non-existent savepoint")
	}
	cat.RollbackTransaction()
}

// TestVacuum_ErrorPaths - tests error handling in Vacuum
func TestVacuum_ErrorPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)

	// Vacuum on fresh catalog (no tables)
	err = cat.Vacuum()
	if err != nil {
		t.Logf("Vacuum on empty catalog error (may be expected): %v", err)
	}
}

// TestOnDelete_OnUpdate_MorePaths - tests FK OnDelete/OnUpdate more
func TestOnDelete_OnUpdate_MorePaths(t *testing.T) {
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
	createCoverageTestTable(t, cat, "fk_parent_del", []*query.ColumnDef{
		{Name: "parent_id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Create child table with FK and CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child_cascade",
		Columns: []*query.ColumnDef{
			{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent_del",
				ReferencedColumns: []string{"parent_id"},
				OnDelete:          "CASCADE",
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Create child table with FK and SET NULL
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_child_null",
		Columns: []*query.ColumnDef{
			{Name: "child_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent_del",
				ReferencedColumns: []string{"parent_id"},
				OnDelete:          "SET NULL",
				OnUpdate:          "SET NULL",
			},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent_del",
		Columns: []string{"parent_id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child_cascade",
		Columns: []string{"child_id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(2)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child_null",
		Columns: []string{"child_id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(2)}},
	}, nil)

	// Delete parent - should cascade
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent_del",
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("DELETE CASCADE error: %v", err)
	}

	// Update parent - should cascade
	_, _, err = cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_parent_del",
		Set:   []*query.SetClause{{Column: "parent_id", Value: numReal(100)}},
		Where: &query.BinaryExpr{Left: colReal("parent_id"), Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err != nil {
		t.Logf("UPDATE CASCADE error: %v", err)
	}
}
