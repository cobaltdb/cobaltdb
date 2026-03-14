package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_UpdateLockedMore tests updateLocked with more scenarios
func TestCoverage_UpdateLockedMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_lock", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_lock",
			Columns: []string{"id", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("test")}},
		}, nil)
	}

	// Update with AND condition
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_lock",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(3),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenLt,
				Right:    numReal(8),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_lock WHERE val = 999")
	t.Logf("Count after AND update: %v", result.Rows)

	// Update with IN condition
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_lock",
		Set:   []*query.SetClause{{Column: "name", Value: strReal("updated")}},
		Where: &query.InExpr{
			Expr: &query.Identifier{Name: "id"},
			List: []query.Expression{numReal(1), numReal(2), numReal(3)},
		},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM upd_lock WHERE name = 'updated'")
	t.Logf("Count after IN update: %v", result.Rows)

	// Update with BETWEEN
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_lock",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(111)}},
		Where: &query.BetweenExpr{
			Expr:  &query.Identifier{Name: "id"},
			Lower: numReal(8),
			Upper: numReal(10),
		},
	}, nil)

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM upd_lock WHERE val = 111")
	t.Logf("Count after BETWEEN update: %v", result.Rows)
}

// TestCoverage_WhereEvaluationMore tests evaluateWhere with complex conditions
func TestCoverage_WhereEvaluationMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_more",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 2)), strReal("value")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_more WHERE a > 5 AND b < 40",
		"SELECT * FROM where_more WHERE a = 1 OR a = 10 OR a = 20",
		"SELECT * FROM where_more WHERE NOT a > 15",
		"SELECT * FROM where_more WHERE (a > 5 AND a < 10) OR (a > 15 AND a < 18)",
		"SELECT * FROM where_more WHERE c = 'value' AND a IN (1, 5, 10, 15)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE eval error: %v", err)
		} else {
			t.Logf("WHERE eval returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ResolveAggregateInExpr tests resolveAggregateInExpr
func TestCoverage_ResolveAggregateInExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_resolve", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 15 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_resolve",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) as total, AVG(val) as avg_val FROM agg_resolve GROUP BY grp HAVING total > AVG(val) * 10",
		"SELECT grp, COUNT(*) as cnt, MIN(val) as min_v, MAX(val) as max_v FROM agg_resolve GROUP BY grp HAVING cnt > 5 AND max_v - min_v > 50",
		"SELECT grp, SUM(val) + COUNT(*) as combined FROM agg_resolve GROUP BY grp HAVING combined > 1000",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate resolve error: %v", err)
		} else {
			t.Logf("Aggregate resolve returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_JoinGroupAggregateExpression tests JOIN+GROUP BY with aggregate expressions
func TestCoverage_JoinGroupAggregateExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jga_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
		{Name: "qty", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jga_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jga_customers",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{numReal(1), strReal("North")}, {numReal(2), strReal("South")}},
	}, nil)

	for i := 1; i <= 20; i++ {
		custID := 1
		if i > 10 {
			custID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jga_orders",
			Columns: []string{"id", "customer_id", "amount", "qty"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(custID)), numReal(float64(i * 100)), numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, SUM(o.amount) as total, AVG(o.amount) as avg_amt, SUM(o.qty) as total_qty FROM jga_orders o JOIN jga_customers c ON o.customer_id = c.id GROUP BY c.region HAVING total > 5000",
		"SELECT c.region, COUNT(*) as cnt, MIN(o.amount) as min_amt, MAX(o.amount) as max_amt FROM jga_orders o JOIN jga_customers c ON o.customer_id = c.id GROUP BY c.region HAVING cnt >= 5",
		"SELECT c.region, SUM(o.amount * o.qty) as weighted_sum FROM jga_orders o JOIN jga_customers c ON o.customer_id = c.id GROUP BY c.region",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN+GROUP+AGG error: %v", err)
		} else {
			t.Logf("JOIN+GROUP+AGG returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_OuterRefResolutionMore tests resolveOuterRefsInQuery more
func TestCoverage_OuterRefResolutionMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_dept", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "outer_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "outer_dept",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales")}, {numReal(2), strReal("Eng")}},
	}, nil)

	for i := 1; i <= 10; i++ {
		deptID := 1
		if i > 5 {
			deptID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_emp",
			Columns: []string{"id", "name", "dept_id", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("emp"), numReal(float64(deptID)), numReal(float64(i * 1000))}},
		}, nil)
	}

	// Query with correlated subquery referencing outer table
	queries := []string{
		"SELECT d.name, (SELECT COUNT(*) FROM outer_emp e WHERE e.dept_id = d.id) as emp_count FROM outer_dept d",
		"SELECT d.name FROM outer_dept d WHERE EXISTS (SELECT 1 FROM outer_emp e WHERE e.dept_id = d.id AND e.salary > 3000)",
		"SELECT e.name, e.salary FROM outer_emp e WHERE e.salary > (SELECT AVG(e2.salary) FROM outer_emp e2 WHERE e2.dept_id = e.dept_id)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Outer ref error: %v", err)
		} else {
			t.Logf("Outer ref returned %d rows", len(result.Rows))
		}
	}
}
