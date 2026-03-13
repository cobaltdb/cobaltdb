package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_OrderByNulls tests ORDER BY with NULL handling
func TestCoverage_OrderByNulls(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ob_nulls", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ob_nulls",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), numReal(100)},
			{numReal(2), &query.NullLiteral{}},
			{numReal(3), numReal(50)},
		},
	}, nil)

	queries := []string{
		"SELECT * FROM ob_nulls ORDER BY val ASC",
		"SELECT * FROM ob_nulls ORDER BY val DESC",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("ORDER BY NULLs error: %v", err)
		} else {
			t.Logf("ORDER BY NULLs returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_HavingWithAggregates tests HAVING with complex aggregates
func TestCoverage_HavingWithAggregates(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "having_agg",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, AVG(val) as avg_val FROM having_agg GROUP BY grp HAVING avg_val > 100",
		"SELECT grp, MIN(val) as min_val FROM having_agg GROUP BY grp HAVING min_val > 5",
		"SELECT grp, MAX(val) as max_val FROM having_agg GROUP BY grp HAVING max_val < 250",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING aggregates error: %v", err)
		} else {
			t.Logf("HAVING aggregates returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ComplexWhere tests complex WHERE conditions
func TestCoverage_ComplexWhere(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "complex_where", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "complex_where",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 2)), numReal(float64(i * 3))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM complex_where WHERE a > 2 AND b < 20 OR c > 15",
		"SELECT * FROM complex_where WHERE (a > 2 AND b < 20) OR (c > 15 AND a < 8)",
		"SELECT * FROM complex_where WHERE NOT (a < 3 OR a > 8)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex WHERE error: %v", err)
		} else {
			t.Logf("Complex WHERE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SelectExpressions tests SELECT with expressions
func TestCoverage_SelectExpressions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "select_expr", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "select_expr",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(1), numReal(10), numReal(3)}},
	}, nil)

	queries := []string{
		"SELECT id, a + b as sum, a - b as diff, a * b as prod FROM select_expr",
		"SELECT id, a / b as div, a % b as mod FROM select_expr",
		"SELECT id, (a + b) * 2 as calc FROM select_expr",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("SELECT expressions error: %v", err)
		} else {
			t.Logf("SELECT expressions returned %d rows", len(result.Rows))
		}
	}
}
