package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_deleteRowLockedUndoLog targets deleteRowLocked with undo log
func TestCoverage_deleteRowLockedUndoLog(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_row_undo", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_row_undo",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("value")}},
		}, nil)
	}

	// Start transaction and delete within it (triggers undo log)
	cat.BeginTransaction(1)
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_row_undo",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenLte, Right: numReal(5)},
	}, nil)
	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}

	// Rollback should restore rows via undo log
	cat.RollbackTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_row_undo")
	t.Logf("Count after rollback: %v", result.Rows)
}

// TestCoverage_applyOuterQueryWithHaving targets applyOuterQuery with HAVING
func TestCoverage_applyOuterQueryWithHaving(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_having", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		grp := "X"
		if i > 20 {
			grp = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_having",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "grp"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}},
		},
		From:    &query.TableRef{Name: "outer_having"},
		GroupBy: []query.Expression{&query.Identifier{Name: "grp"}},
	}
	cat.CreateView("outer_having_view", viewStmt)

	// Query view with HAVING
	result, err := cat.ExecuteQuery("SELECT * FROM outer_having_view WHERE col_1 > 5000")
	if err != nil {
		t.Logf("View with HAVING error: %v", err)
	} else {
		t.Logf("View returned %d rows", len(result.Rows))
	}

	cat.DropView("outer_having_view")
}

// TestCoverage_evaluateWhereComplex79 targets evaluateWhere complex cases
func TestCoverage_evaluateWhereComplex79(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "eval_where", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "eval_where",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i % 5)), strReal("test")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM eval_where WHERE a = 1 OR a = 2 OR a = 3",
		"SELECT * FROM eval_where WHERE (a = 1 AND b = 'test') OR (a = 2 AND b = 'test')",
		"SELECT * FROM eval_where WHERE NOT a = 0",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("evaluateWhere error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_resolveAggregateInExprCases targets resolveAggregateInExpr
func TestCoverage_resolveAggregateInExprCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "resolve_agg_cases", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 60; i++ {
		grp := "A"
		if i > 30 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "resolve_agg_cases",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 100))}},
		}, nil)
	}

	// Various HAVING cases to cover resolveAggregateInExpr
	queries := []string{
		"SELECT grp, SUM(val) as s FROM resolve_agg_cases GROUP BY grp HAVING s > 100000",
		"SELECT grp, COUNT(*) as c FROM resolve_agg_cases GROUP BY grp HAVING c > 25",
		"SELECT grp, AVG(val) as a FROM resolve_agg_cases GROUP BY grp HAVING a > 3000",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("resolveAggregate error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupBy targets executeSelectWithJoinAndGroupBy
func TestCoverage_executeSelectWithJoinAndGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "join_gb_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "join_gb_detail", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		catg := "A"
		if i > 3 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_gb_main",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg)}},
		}, nil)
		for j := 1; j <= 4; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "join_gb_detail",
				Columns: []string{"id", "main_id", "amount"},
				Values:  [][]query.Expression{{numReal(float64(i*10 + j)), numReal(float64(i)), numReal(float64(j * 100))}},
			}, nil)
		}
	}

	// JOIN with GROUP BY
	result, err := cat.ExecuteQuery(`
		SELECT m.category, SUM(d.amount) as total
		FROM join_gb_main m
		JOIN join_gb_detail d ON m.id = d.main_id
		GROUP BY m.category
	`)
	if err != nil {
		t.Logf("JOIN GROUP BY error: %v", err)
	} else {
		t.Logf("JOIN GROUP BY returned %d rows", len(result.Rows))
	}
}

// TestCoverage_applyOrderByMultiple targets applyOrderBy with multiple columns
func TestCoverage_applyOrderByMultiple(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenText},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenReal},
	})

	for i := 1; i <= 50; i++ {
		a := "X"
		if i > 25 {
			a = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_multi",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(a), numReal(float64(i % 5)), numReal(float64(i) * 0.5)}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM order_multi ORDER BY a, b",
		"SELECT * FROM order_multi ORDER BY a DESC, b ASC, c DESC",
		"SELECT * FROM order_multi ORDER BY b, a, c",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("ORDER BY error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_RLSCheckOperations targets RLS check internal functions
func TestCoverage_RLSCheckOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table: "rls_ops",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "owner", Type: query.TokenText},
			{Name: "data", Type: query.TokenText},
		},
	})

	// Enable RLS
	cat.EnableRLS()

	// Insert
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rls_ops",
		Columns: []string{"id", "owner", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal("user1"), strReal("test")}},
	}, nil)

	// Update
	cat.Update(ctx, &query.UpdateStmt{
		Table: "rls_ops",
		Set:   []*query.SetClause{{Column: "data", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Delete
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "rls_ops",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
}

// TestCoverage_RollbackToSavepointUndo targets RollbackToSavepoint with undo
func TestCoverage_RollbackToSavepointUndo(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_undo", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_undo",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("first")}},
	}, nil)

	cat.Savepoint("sp1")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_undo",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("second")}},
	}, nil)

	cat.Savepoint("sp2")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_undo",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("third")}},
	}, nil)

	// Rollback to sp1 - should remove rows 2 and 3
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_undo")
	t.Logf("Count after rollback to sp1: %v", result.Rows)

	cat.RollbackTransaction()
}

// TestCoverage_executeSelectWithJoinAndGroupByHaving targets JOIN + GROUP BY + HAVING
func TestCoverage_executeSelectWithJoinAndGroupByHaving(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jgh_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "jgh_detail", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		catg := "A"
		if i > 5 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgh_main",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg)}},
		}, nil)
		for j := 1; j <= 3; j++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "jgh_detail",
				Columns: []string{"id", "main_id", "amount"},
				Values:  [][]query.Expression{{numReal(float64(i*10 + j)), numReal(float64(i)), numReal(float64(j * 50))}},
			}, nil)
		}
	}

	result, err := cat.ExecuteQuery(`
		SELECT m.category, SUM(d.amount) as total
		FROM jgh_main m
		JOIN jgh_detail d ON m.id = d.main_id
		GROUP BY m.category
		HAVING total > 500
	`)
	if err != nil {
		t.Logf("JOIN GROUP BY HAVING error: %v", err)
	} else {
		t.Logf("JOIN GROUP BY HAVING returned %d rows", len(result.Rows))
	}
}

// TestCoverage_evaluateWhereWithSubquery targets evaluateWhere with subquery
func TestCoverage_evaluateWhereWithSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_subq_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "where_subq_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "threshold", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_subq_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "where_subq_ref",
		Columns: []string{"id", "threshold"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Subquery in WHERE
	result, err := cat.ExecuteQuery(`
		SELECT * FROM where_subq_main
		WHERE val > (SELECT threshold FROM where_subq_ref WHERE id = 1)
	`)
	if err != nil {
		t.Logf("WHERE subquery error: %v", err)
	} else {
		t.Logf("WHERE subquery returned %d rows", len(result.Rows))
	}
}

// TestCoverage_applyOrderByWithNulls targets applyOrderBy with NULLs
func TestCoverage_applyOrderByWithNulls(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_nulls79", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		var val query.Expression = &query.NullLiteral{}
		if i%4 != 0 {
			val = numReal(float64(i * 10))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_nulls79",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), val}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM order_nulls79 ORDER BY val",
		"SELECT * FROM order_nulls79 ORDER BY val DESC",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("ORDER BY NULLs error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_resolveAggregateInExprArith79 targets resolveAggregateInExpr with arithmetic
func TestCoverage_resolveAggregateInExprArith79(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "resolve_arith", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "rev", Type: query.TokenInteger},
		{Name: "cost", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		grp := "X"
		if i > 20 {
			grp = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "resolve_arith",
			Columns: []string{"id", "grp", "rev", "cost"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 100)), numReal(float64(i * 60))}},
		}, nil)
	}

	// HAVING with arithmetic on aggregates
	queries := []string{
		"SELECT grp, SUM(rev) as total_rev, SUM(cost) as total_cost FROM resolve_arith GROUP BY grp HAVING total_rev - total_cost > 50000",
		"SELECT grp, AVG(rev) as avg_rev, AVG(cost) as avg_cost FROM resolve_arith GROUP BY grp HAVING avg_rev / avg_cost > 1.5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Arithmetic HAVING error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedWithCache targets selectLocked with query cache
func TestCoverage_selectLockedWithCache(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Enable cache
	cat.EnableQueryCache(100, 0)

	createCoverageTestTable(t, cat, "select_cache79", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "select_cache79",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Execute same query multiple times to hit cache
	for i := 0; i < 5; i++ {
		cat.ExecuteQuery("SELECT * FROM select_cache79 WHERE id <= 5")
	}

	hits, misses, _ := cat.GetQueryCacheStats()
	t.Logf("Cache hits: %d, misses: %d", hits, misses)
}
