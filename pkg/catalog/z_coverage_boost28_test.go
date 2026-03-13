package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_JoinWithGroupAggregateExpression tests JOIN with GROUP BY and aggregate expressions
func TestCoverage_JoinWithGroupAggregateExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jge_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jge_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "region", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jge_customers",
		Columns: []string{"id", "name", "region"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), strReal("North")},
			{numReal(2), strReal("Bob"), strReal("South")},
			{numReal(3), strReal("Charlie"), strReal("North")},
		},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jge_orders",
		Columns: []string{"id", "customer_id", "amount"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), numReal(100)},
			{numReal(2), numReal(1), numReal(200)},
			{numReal(3), numReal(2), numReal(150)},
			{numReal(4), numReal(2), numReal(250)},
			{numReal(5), numReal(3), numReal(300)},
		},
	}, nil)

	queries := []string{
		"SELECT c.region, SUM(o.amount) as total, AVG(o.amount) as avg_val FROM jge_customers c JOIN jge_orders o ON c.id = o.customer_id GROUP BY c.region HAVING total > 200",
		"SELECT c.name, COUNT(*) as cnt, MIN(o.amount) as min_amt, MAX(o.amount) as max_amt FROM jge_customers c JOIN jge_orders o ON c.id = o.customer_id GROUP BY c.name",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN+GROUP+aggregate error: %v", err)
		} else {
			t.Logf("JOIN+GROUP+aggregate returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CorrelatedSubqueryOuterRef tests correlated subqueries with outer references
func TestCoverage_CorrelatedSubqueryOuterRef(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cor_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "cor_dept", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cor_dept",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Sales")},
			{numReal(2), strReal("Engineering")},
		},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cor_emp",
		Columns: []string{"id", "name", "dept_id", "salary"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), numReal(1), numReal(5000)},
			{numReal(2), strReal("Bob"), numReal(1), numReal(6000)},
			{numReal(3), strReal("Charlie"), numReal(2), numReal(7000)},
			{numReal(4), strReal("David"), numReal(2), numReal(8000)},
		},
	}, nil)

	// Correlated subquery with EXISTS
	queries := []string{
		"SELECT e.name FROM cor_emp e WHERE EXISTS (SELECT 1 FROM cor_dept d WHERE d.id = e.dept_id AND d.name = 'Sales')",
		"SELECT d.name, (SELECT COUNT(*) FROM cor_emp e WHERE e.dept_id = d.id) as emp_count FROM cor_dept d",
		"SELECT e.name FROM cor_emp e WHERE e.salary > (SELECT AVG(e2.salary) FROM cor_emp e2 WHERE e2.dept_id = e.dept_id)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Correlated subquery error: %v", err)
		} else {
			t.Logf("Correlated subquery returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SelectLockedEdgeCases tests selectLocked edge cases
func TestCoverage_SelectLockedEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sel_edge",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}, {numReal(2), strReal("b")}},
	}, nil)

	queries := []string{
		"SELECT * FROM sel_edge WHERE 1=1",
		"SELECT * FROM sel_edge WHERE id = 1 OR id = 2",
		"SELECT * FROM sel_edge WHERE val LIKE '%'",
		"SELECT DISTINCT * FROM sel_edge",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Select edge case error: %v", err)
		} else {
			t.Logf("Select edge case returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_WhereEdgeCases tests WHERE clause edge cases
func TestCoverage_WhereEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenText},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_edge",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 2)), strReal("test")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_edge WHERE (a = 1 AND b = 2) OR (a = 3 AND b = 6)",
		"SELECT * FROM where_edge WHERE a IN (1, 3, 5) AND b > 2",
		"SELECT * FROM where_edge WHERE c = 'test' OR a > 10",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE edge case error: %v", err)
		} else {
			t.Logf("WHERE edge case returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_UnionWithOrderBy tests UNION with ORDER BY
func TestCoverage_UnionWithOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "union_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "union_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "union_a",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "union_b",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), numReal(20)}, {numReal(3), numReal(30)}},
	}, nil)

	queries := []string{
		"SELECT val FROM union_a UNION SELECT val FROM union_b ORDER BY val",
		"SELECT val FROM union_a UNION ALL SELECT val FROM union_b ORDER BY val DESC",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("UNION ORDER BY error: %v", err)
		} else {
			t.Logf("UNION ORDER BY returned %d rows", len(result.Rows))
		}
	}
}
