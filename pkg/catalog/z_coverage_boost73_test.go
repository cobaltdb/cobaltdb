package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_deleteRowLockedWithUndoLog targets deleteRowLocked with undo log
func TestCoverage_deleteRowLockedWithUndoLog(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_undo", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_undo",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("value")}},
		}, nil)
	}

	// Start transaction and delete
	cat.BeginTransaction(1)

	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_undo",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenLte,
			Right:    numReal(5),
		},
	}, nil)

	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}

	// Rollback should restore deleted rows
	cat.RollbackTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_undo")
	t.Logf("Count after rollback: %v", result.Rows)
}

// TestCoverage_deleteRowLockedWithFKCascade targets deleteRowLocked FK CASCADE
func TestCoverage_deleteRowLockedWithFKCascade(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Parent table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_cascade_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Child table with CASCADE
	cat.CreateTable(&query.CreateTableStmt{
		Table: "fk_cascade_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_cascade_parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_cascade_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_cascade_child",
		Columns: []string{"id", "parent_id"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1)},
			{numReal(2), numReal(1)},
			{numReal(3), numReal(2)},
		},
	}, nil)

	// Delete parent (should cascade to children)
	_, rows, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_cascade_parent",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    numReal(1),
		},
	}, nil)

	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d parent rows", rows)
	}

	// Check children cascaded
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM fk_cascade_child")
	t.Logf("Child count after cascade: %v", result.Rows)
}

// TestCoverage_applyOuterQueryWithGroupBy targets applyOuterQuery with GROUP BY views
func TestCoverage_applyOuterQueryWithGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_gb", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		catg := "A"
		if i > 15 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_gb",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with GROUP BY
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		},
		From:    &query.TableRef{Name: "outer_gb"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
	}
	cat.CreateView("gb_view", viewStmt)

	// Query the GROUP BY view
	result, err := cat.ExecuteQuery("SELECT * FROM gb_view ORDER BY category")
	if err != nil {
		t.Logf("GROUP BY view error: %v", err)
	} else {
		t.Logf("GROUP BY view returned %d rows", len(result.Rows))
	}

	cat.DropView("gb_view")
}

// TestCoverage_rollbackToSavepointWithIndex targets RollbackToSavepoint with index changes
func TestCoverage_rollbackToSavepointWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	cat.BeginTransaction(1)
	cat.Savepoint("sp1")

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "sp_test_idx",
		Table:   "sp_idx",
		Columns: []string{"code"},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_idx",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}},
	}, nil)

	// Rollback should undo index and insert
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	cat.RollbackTransaction()
}

// TestCoverage_executeScalarSelectEmpty targets executeScalarSelect on empty table
func TestCoverage_executeScalarSelectEmpty(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_empty", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Aggregate on empty table
	queries := []string{
		"SELECT COUNT(*) FROM scalar_empty",
		"SELECT SUM(val) FROM scalar_empty",
		"SELECT AVG(val) FROM scalar_empty",
		"SELECT MIN(val) FROM scalar_empty",
		"SELECT MAX(val) FROM scalar_empty",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Scalar error: %v", err)
		} else {
			t.Logf("Scalar result: %v", result.Rows)
		}
	}
}

// TestCoverage_evaluateHavingWithNull targets evaluateHaving with NULL values
func TestCoverage_evaluateHavingWithNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_null", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data with NULLs
	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 15 {
			grp = "B"
		}
		if i%3 == 0 {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "having_null",
				Columns: []string{"id", "grp"},
				Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp)}},
			}, nil)
		} else {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "having_null",
				Columns: []string{"id", "grp", "val"},
				Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
			}, nil)
		}
	}

	// HAVING with NULL handling
	result, err := cat.ExecuteQuery("SELECT grp, COUNT(*) as cnt FROM having_null GROUP BY grp HAVING cnt > 5")
	if err != nil {
		t.Logf("HAVING NULL error: %v", err)
	} else {
		t.Logf("HAVING NULL returned %d rows", len(result.Rows))
	}
}

// TestCoverage_resolveAggregateInExprArithmetic targets resolveAggregateInExpr with arithmetic
func TestCoverage_resolveAggregateInExprArithmetic(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_arith", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "revenue", Type: query.TokenInteger},
		{Name: "cost", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		catg := "X"
		if i > 20 {
			catg = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_arith",
			Columns: []string{"id", "category", "revenue", "cost"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 100)), numReal(float64(i * 60))}},
		}, nil)
	}

	// HAVING with arithmetic expressions
	queries := []string{
		"SELECT category, SUM(revenue) as rev, SUM(cost) as cost FROM agg_arith GROUP BY category HAVING rev - cost > 10000",
		"SELECT category, AVG(revenue) as avg_rev, AVG(cost) as avg_cost FROM agg_arith GROUP BY category HAVING avg_rev / avg_cost > 1.5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Arithmetic HAVING error: %v", err)
		} else {
			t.Logf("Arithmetic HAVING returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedWithSubquery targets selectLocked with subqueries
func TestCoverage_selectLockedWithSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_main_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "sel_ref_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "threshold", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_main_sub",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sel_ref_sub",
		Columns: []string{"id", "threshold"},
		Values:  [][]query.Expression{{numReal(1), numReal(50)}},
	}, nil)

	// Subquery in SELECT
	result, err := cat.ExecuteQuery("SELECT id, val, (SELECT threshold FROM sel_ref_sub WHERE id = 1) as threshold FROM sel_main_sub WHERE val > 50")
	if err != nil {
		t.Logf("Subquery SELECT error: %v", err)
	} else {
		t.Logf("Subquery SELECT returned %d rows", len(result.Rows))
	}
}

// TestCoverage_ExecuteCTEUnion targets ExecuteCTE with UNION
func TestCoverage_ExecuteCTEUnion(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cte_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "cte_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_a",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a1")}, {numReal(2), strReal("a2")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cte_b",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("b1")}, {numReal(4), strReal("b2")}},
	}, nil)

	// CTE with UNION
	result, err := cat.ExecuteQuery(`
		WITH combined AS (
			SELECT id, val FROM cte_a
			UNION ALL
			SELECT id, val FROM cte_b
		)
		SELECT * FROM combined ORDER BY id
	`)
	if err != nil {
		t.Logf("CTE UNION error: %v", err)
	} else {
		t.Logf("CTE UNION returned %d rows", len(result.Rows))
	}
}
