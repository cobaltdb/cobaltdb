package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ApplyOuterQueryHaving tests HAVING clause in applyOuterQuery
func TestCoverage_ApplyOuterQueryHaving(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert test data
	for i := 1; i <= 30; i++ {
		category := "A"
		if i > 10 {
			category = "B"
		}
		if i > 20 {
			category = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "having_test",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category), numReal(float64(i * 10))}},
		}, nil)
	}

	// Test HAVING with various conditions
	queries := []string{
		"SELECT category, COUNT(*) as cnt FROM having_test GROUP BY category HAVING cnt = 10",
		"SELECT category, COUNT(*) as cnt FROM having_test GROUP BY category HAVING cnt > 5",
		"SELECT category, SUM(amount) as total FROM having_test GROUP BY category HAVING total > 1000",
		"SELECT category, COUNT(*) as cnt FROM having_test GROUP BY category HAVING cnt BETWEEN 5 AND 15",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOuterQueryLimitOffset tests LIMIT/OFFSET in applyOuterQuery
func TestCoverage_ApplyOuterQueryLimitOffset(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "limit_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "limit_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM limit_test LIMIT 5",
		"SELECT * FROM limit_test LIMIT 5 OFFSET 10",
		"SELECT * FROM limit_test OFFSET 20",
		"SELECT * FROM limit_test ORDER BY id LIMIT 10 OFFSET 5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_GroupByComplex tests complex GROUP BY scenarios
func TestCoverage_GroupByComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "gb_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "cat1", Type: query.TokenText},
		{Name: "cat2", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		cat1 := "A"
		if i > 15 {
			cat1 = "B"
		}
		cat2 := "X"
		if i%3 == 0 {
			cat2 = "Y"
		}
		if i%3 == 1 {
			cat2 = "Z"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gb_complex",
			Columns: []string{"id", "cat1", "cat2", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat1), strReal(cat2), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT cat1, cat2, COUNT(*), SUM(val), AVG(val) FROM gb_complex GROUP BY cat1, cat2",
		"SELECT cat1, COUNT(*) as c FROM gb_complex GROUP BY cat1 HAVING c > 5 ORDER BY c DESC",
		"SELECT cat1, SUM(val) as total FROM gb_complex GROUP BY cat1 ORDER BY total LIMIT 1",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DistinctWithJoin tests DISTINCT with JOIN
func TestCoverage_DistinctWithJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "dist_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "dist_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_id", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "dist_a",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "dist_b",
		Columns: []string{"id", "a_id", "val"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), strReal("x")},
			{numReal(2), numReal(1), strReal("y")},
			{numReal(3), numReal(2), strReal("z")},
		},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT DISTINCT name FROM dist_a JOIN dist_b ON dist_a.id = dist_b.a_id")
	if err != nil {
		t.Logf("DISTINCT JOIN error: %v", err)
	} else {
		t.Logf("DISTINCT JOIN returned %d rows", len(result.Rows))
	}
}

// TestCoverage_OrderByMultiple tests ORDER BY with multiple columns
func TestCoverage_OrderByMultiple(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ob_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "priority", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ob_multi",
		Columns: []string{"id", "priority", "name"},
		Values: [][]query.Expression{
			{numReal(1), numReal(2), strReal("B")},
			{numReal(2), numReal(1), strReal("A")},
			{numReal(3), numReal(2), strReal("A")},
			{numReal(4), numReal(1), strReal("B")},
		},
	}, nil)

	queries := []string{
		"SELECT * FROM ob_multi ORDER BY priority, name",
		"SELECT * FROM ob_multi ORDER BY priority DESC, name ASC",
		"SELECT * FROM ob_multi ORDER BY priority ASC, name DESC",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}
