package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ExecuteScalarSelect tests executeScalarSelect
func TestCoverage_ExecuteScalarSelect(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "scalar_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "scalar_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*) FROM scalar_test",
		"SELECT SUM(val) FROM scalar_test",
		"SELECT AVG(val) FROM scalar_test",
		"SELECT MIN(val) FROM scalar_test",
		"SELECT MAX(val) FROM scalar_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Scalar select error: %v", err)
		} else {
			t.Logf("Scalar select returned %v", result.Rows)
		}
	}
}

// TestCoverage_SelectLockedComplex tests selectLocked with complex scenarios
func TestCoverage_SelectLockedComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_complex2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
		{Name: "c", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_complex2",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("test"), numReal(float64(i * 100))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM sel_complex2 WHERE a > 100 AND b = 'test' ORDER BY a DESC LIMIT 10 OFFSET 5",
		"SELECT a, b, COUNT(*) as cnt FROM sel_complex2 GROUP BY a, b HAVING cnt >= 1 LIMIT 5",
		"SELECT DISTINCT a FROM sel_complex2 WHERE a > 200 ORDER BY a",
		"SELECT * FROM sel_complex2 WHERE a IN (100, 200, 300, 400, 500)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex select error: %v", err)
		} else {
			t.Logf("Complex select returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_JoinWithGroupByMore tests JOIN with GROUP BY more
func TestCoverage_JoinWithGroupByMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jg_orders2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
		{Name: "qty", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jg_customers2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
		{Name: "tier", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_customers2",
		Columns: []string{"id", "region", "tier"},
		Values: [][]query.Expression{
			{numReal(1), strReal("North"), strReal("Gold")},
			{numReal(2), strReal("South"), strReal("Silver")},
			{numReal(3), strReal("North"), strReal("Gold")},
		},
	}, nil)

	for i := 1; i <= 30; i++ {
		custID := ((i - 1) % 3) + 1
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jg_orders2",
			Columns: []string{"id", "customer_id", "amount", "qty"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(custID)), numReal(float64(i * 100)), numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, c.tier, COUNT(*) as cnt, SUM(o.amount) as total FROM jg_orders2 o JOIN jg_customers2 c ON o.customer_id = c.id GROUP BY c.region, c.tier",
		"SELECT c.region, AVG(o.amount) as avg_amt, SUM(o.qty) as total_qty FROM jg_orders2 o JOIN jg_customers2 c ON o.customer_id = c.id GROUP BY c.region HAVING avg_amt > 1000",
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

// TestCoverage_HavingWithSubquery tests HAVING with subqueries
func TestCoverage_HavingWithSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_sub", []*query.ColumnDef{
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
			Table:   "having_sub",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) as total FROM having_sub GROUP BY grp HAVING total > (SELECT AVG(val) * 20 FROM having_sub)",
		"SELECT grp, COUNT(*) as cnt FROM having_sub GROUP BY grp HAVING cnt > 15",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING subquery error: %v", err)
		} else {
			t.Logf("HAVING subquery returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_InsertLockedWithIndex tests insertLocked with index updates
func TestCoverage_InsertLockedWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ins_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create index
	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "ins_idx",
		Columns: []string{"code"},
	})

	// Insert with index
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ins_idx",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE" + string(rune('A'+i-1))), numReal(float64(i * 10))}},
		}, nil)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM ins_idx")
	t.Logf("Count after indexed inserts: %v", result.Rows)

	result, _ = cat.ExecuteQuery("SELECT * FROM ins_idx WHERE code = 'CODEA'")
	t.Logf("Query by index returned %d rows", len(result.Rows))
}
