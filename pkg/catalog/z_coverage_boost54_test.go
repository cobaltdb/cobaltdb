package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ApplyOuterQueryWithGroupBy targets applyOuterQuery with GROUP BY
func TestCoverage_ApplyOuterQueryWithGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_gb", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		c := "A"
		if i > 20 {
			c = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_gb",
			Columns: []string{"id", "cat", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(c), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with GROUP BY
	cat.CreateView("view_gb", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "cat"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}},
		},
		From:    &query.TableRef{Name: "outer_gb"},
		GroupBy: []query.Expression{&query.Identifier{Name: "cat"}},
	})

	// Query the view with outer filter
	result, _ := cat.ExecuteQuery("SELECT * FROM view_gb WHERE cat = 'A'")
	t.Logf("View with GROUP BY returned %d rows", len(result.Rows))

	// Query with COUNT
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM view_gb")
	t.Logf("COUNT from view: %v", result.Rows)
}

// TestCoverage_ApplyOuterQueryWithOrderBy targets applyOuterQuery with ORDER BY
func TestCoverage_ApplyOuterQueryWithOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_ob", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_ob",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(51 - i))}},
		}, nil)
	}

	// Create view with ORDER BY
	cat.CreateView("view_ob", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "val"},
		},
		From: &query.TableRef{Name: "outer_ob"},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "val"}, Desc: true},
		},
		Limit: &query.NumberLiteral{Value: 20},
	})

	// Query the view with filter
	result, _ := cat.ExecuteQuery("SELECT * FROM view_ob WHERE val > 10")
	t.Logf("View with ORDER BY returned %d rows", len(result.Rows))
}

// TestCoverage_ApplyOuterQueryWithDistinct targets applyOuterQuery with DISTINCT
func TestCoverage_ApplyOuterQueryWithDistinct(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_distinct", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		c := "A"
		if i > 10 && i <= 20 {
			c = "B"
		} else if i > 20 {
			c = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_distinct",
			Columns: []string{"id", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(c)}},
		}, nil)
	}

	// Create view with DISTINCT
	cat.CreateView("view_distinct", &query.SelectStmt{
		Columns:  []query.Expression{&query.Identifier{Name: "cat"}},
		From:     &query.TableRef{Name: "outer_distinct"},
		Distinct: true,
	})

	// Query the view with filter
	result, _ := cat.ExecuteQuery("SELECT * FROM view_distinct WHERE cat = 'A'")
	t.Logf("View with DISTINCT returned %d rows", len(result.Rows))

	result, _ = cat.ExecuteQuery("SELECT * FROM view_distinct")
	t.Logf("All distinct values: %d rows", len(result.Rows))
}

// TestCoverage_EvaluateWhereComplex targets evaluateWhere with complex expressions
func TestCoverage_EvaluateWhereComplexMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenText},
	})

	for i := 1; i <= 60; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_complex",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i % 10)), strReal("val")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_complex WHERE a > 10 AND b < 5",
		"SELECT * FROM where_complex WHERE a > 40 OR b > 8",
		"SELECT * FROM where_complex WHERE NOT (a > 50)",
		"SELECT * FROM where_complex WHERE a IN (5, 10, 15, 20, 25)",
		"SELECT * FROM where_complex WHERE a BETWEEN 20 AND 30",
		"SELECT * FROM where_complex WHERE c LIKE 'va%'",
		"SELECT * FROM where_complex WHERE a > 5 AND b < 8 AND c = 'val'",
		"SELECT * FROM where_complex WHERE (a > 10 AND a < 20) OR (a > 40 AND a < 50)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE error: %v", err)
		} else {
			t.Logf("WHERE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ResolveAggregateInExprComplex targets resolveAggregateInExpr
func TestCoverage_ResolveAggregateInExprComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_expr", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		grp := "A"
		if i > 25 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_expr",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) as total, COUNT(*) as cnt FROM agg_expr GROUP BY grp HAVING total > 5000 AND cnt > 20",
		"SELECT grp, AVG(val) as avg_val, MAX(val) - MIN(val) as range_val FROM agg_expr GROUP BY grp HAVING avg_val > 200",
		"SELECT grp, SUM(val) / COUNT(*) as calc_avg FROM agg_expr GROUP BY grp",
		"SELECT grp, SUM(val) + AVG(val) as combined FROM agg_expr GROUP BY grp HAVING combined > 5000",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate error: %v", err)
		} else {
			t.Logf("Aggregate returned %d rows", len(result.Rows))
		}
	}
}
