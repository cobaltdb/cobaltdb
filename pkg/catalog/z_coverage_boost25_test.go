package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_WhereEvaluation tests WHERE clause evaluation
func TestCoverage_WhereEvaluation(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_test",
			Columns: []string{"id", "a", "b", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2)), numReal(float64(i * 3)), strReal("item")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_test WHERE a > 5 AND b < 25",
		"SELECT * FROM where_test WHERE a = 2 OR a = 4 OR a = 6",
		"SELECT * FROM where_test WHERE NOT (a < 4)",
		"SELECT * FROM where_test WHERE name = 'item'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE evaluation error: %v", err)
		} else {
			t.Logf("WHERE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_LikeEvaluation tests LIKE pattern evaluation
func TestCoverage_LikeEvaluation(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "like_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	values := []string{"hello", "world", "hello world", "HELLO", "World123", "test"}
	for i, v := range values {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "like_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(v)}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM like_test WHERE val LIKE '%world%'",
		"SELECT * FROM like_test WHERE val LIKE 'hello%'",
		"SELECT * FROM like_test WHERE val LIKE '%123'",
		"SELECT * FROM like_test WHERE val NOT LIKE '%test%'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIKE evaluation error: %v", err)
		} else {
			t.Logf("LIKE query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_HavingEvaluation tests HAVING clause evaluation
func TestCoverage_HavingEvaluation(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_test", []*query.ColumnDef{
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
			Table:   "having_test",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) as total FROM having_test GROUP BY grp HAVING total > 1000",
		"SELECT grp, COUNT(*) as cnt FROM having_test GROUP BY grp HAVING cnt > 5",
		"SELECT grp, AVG(val) as avg_val FROM having_test GROUP BY grp HAVING avg_val BETWEEN 100 AND 200",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING evaluation error: %v", err)
		} else {
			t.Logf("HAVING query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_AggregateResolution tests aggregate resolution
func TestCoverage_AggregateResolution(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_res", []*query.ColumnDef{
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
			Table:   "agg_res",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) + AVG(val) as combined FROM agg_res GROUP BY grp",
		"SELECT grp, MAX(val) - MIN(val) as range_val FROM agg_res GROUP BY grp",
		"SELECT grp, COUNT(*) * 10 as scaled FROM agg_res GROUP BY grp",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate resolution error: %v", err)
		} else {
			t.Logf("Aggregate query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_JoinWithGroupBy tests JOIN with GROUP BY
func TestCoverage_JoinWithGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jg_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jg_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_orders",
		Columns: []string{"id", "customer_id", "amount"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), numReal(100)},
			{numReal(2), numReal(1), numReal(200)},
			{numReal(3), numReal(2), numReal(150)},
			{numReal(4), numReal(2), numReal(250)},
		},
	}, nil)

	queries := []string{
		"SELECT c.name, SUM(o.amount) as total FROM jg_customers c JOIN jg_orders o ON c.id = o.customer_id GROUP BY c.name",
		"SELECT c.name, COUNT(*) as cnt FROM jg_customers c JOIN jg_orders o ON c.id = o.customer_id GROUP BY c.name HAVING cnt >= 2",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN with GROUP BY error: %v", err)
		} else {
			t.Logf("JOIN+GROUP BY query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_OuterReferenceResolution tests outer reference resolution
func TestCoverage_OuterReferenceResolution(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "outer2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer1",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)

		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer2",
			Columns: []string{"id", "ref"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i))}},
		}, nil)
	}

	// Query with correlated subquery
	result, err := cat.ExecuteQuery(`
		SELECT o1.id, o1.val,
		       (SELECT COUNT(*) FROM outer2 o2 WHERE o2.ref = o1.id) as cnt
		FROM outer1 o1
	`)
	if err != nil {
		t.Logf("Correlated subquery error: %v", err)
	} else {
		t.Logf("Correlated subquery returned %d rows", len(result.Rows))
	}
}
