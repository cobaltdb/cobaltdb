package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ScalarSelectMore tests executeScalarSelect more
func TestCoverage_ScalarSelectMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_more",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*) FROM scalar_more",
		"SELECT SUM(val) FROM scalar_more",
		"SELECT AVG(val) FROM scalar_more",
		"SELECT MIN(val) FROM scalar_more",
		"SELECT MAX(val) FROM scalar_more",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Scalar error: %v", err)
		} else {
			t.Logf("Scalar returned %v", result.Rows)
		}
	}
}

// TestCoverage_ApplyOuterQueryMoreComplex tests applyOuterQuery more
func TestCoverage_ApplyOuterQueryMoreComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "outer_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "grp", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "outer_more",
			Columns: []string{"id", "val", "grp"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(grp)}},
		}, nil)
	}

	// Create simple view
	err := cat.CreateView("view_simple2", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "outer_more"},
	})
	if err != nil {
		t.Logf("Create view error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT * FROM view_simple2 WHERE val > 50")
	t.Logf("View query returned %d rows", len(result.Rows))

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM view_simple2")
	t.Logf("View count returned %v", result.Rows)
}

// TestCoverage_ResolveAggregateMore tests resolveAggregateInExpr more
func TestCoverage_ResolveAggregateMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_res_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 40; i++ {
		grp := "A"
		if i > 20 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_res_more",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) as total, AVG(val) as avg_val FROM agg_res_more GROUP BY grp HAVING total > AVG(val) * 20",
		"SELECT grp, COUNT(*) as cnt, MIN(val) as min_v, MAX(val) as max_v FROM agg_res_more GROUP BY grp HAVING cnt > 15 AND max_v - min_v > 100",
		"SELECT grp, SUM(val) + COUNT(*) as combined FROM agg_res_more GROUP BY grp HAVING combined > 2000",
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

// TestCoverage_JoinGroupByComplexMore tests executeSelectWithJoinAndGroupBy more
func TestCoverage_JoinGroupByComplexMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jgb_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
		{Name: "qty", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jgb_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
		{Name: "tier", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jgb_customers",
		Columns: []string{"id", "region", "tier"},
		Values: [][]query.Expression{
			{numReal(1), strReal("North"), strReal("Gold")},
			{numReal(2), strReal("South"), strReal("Silver")},
			{numReal(3), strReal("East"), strReal("Bronze")},
		},
	}, nil)

	for i := 1; i <= 30; i++ {
		custID := ((i - 1) % 3) + 1
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_orders",
			Columns: []string{"id", "customer_id", "amount", "qty"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(custID)), numReal(float64(i * 100)), numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, c.tier, SUM(o.amount) as total, AVG(o.amount) as avg_amt, SUM(o.qty) as total_qty FROM jgb_orders o JOIN jgb_customers c ON o.customer_id = c.id GROUP BY c.region, c.tier",
		"SELECT c.region, AVG(o.amount) as avg_amt, SUM(o.qty) as total_qty FROM jgb_orders o JOIN jgb_customers c ON o.customer_id = c.id GROUP BY c.region HAVING avg_amt > 1000",
		"SELECT c.tier, COUNT(*) as cnt, MIN(o.amount) as min_amt, MAX(o.amount) as max_amt FROM jgb_orders o JOIN jgb_customers c ON o.customer_id = c.id GROUP BY c.tier HAVING cnt > 5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN GROUP BY error: %v", err)
		} else {
			t.Logf("JOIN GROUP BY returned %d rows", len(result.Rows))
		}
	}
}
