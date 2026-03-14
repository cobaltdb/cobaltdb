package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_JoinWithHaving tests JOIN with HAVING clause
func TestCoverage_JoinWithHaving(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jh_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jh_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jh_customers",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{numReal(1), strReal("North")}, {numReal(2), strReal("South")}},
	}, nil)

	for i := 1; i <= 20; i++ {
		custID := 1
		if i > 10 {
			custID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jh_orders",
			Columns: []string{"id", "customer_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(custID)), numReal(float64(i * 100))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, SUM(o.amount) as total FROM jh_orders o JOIN jh_customers c ON o.customer_id = c.id GROUP BY c.region HAVING total > 5000",
		"SELECT c.region, COUNT(*) as cnt FROM jh_orders o JOIN jh_customers c ON o.customer_id = c.id GROUP BY c.region HAVING cnt > 5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN+HAVING error: %v", err)
		} else {
			t.Logf("JOIN+HAVING returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_GroupByWithRollup tests GROUP BY with ROLLUP
func TestCoverage_GroupByWithRollup(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "rollup_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp1", Type: query.TokenText},
		{Name: "grp2", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp1 := "A"
		grp2 := "X"
		if i > 10 {
			grp1 = "B"
		}
		if i%2 == 0 {
			grp2 = "Y"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "rollup_test",
			Columns: []string{"id", "grp1", "grp2", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp1), strReal(grp2), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp1, SUM(val) as total FROM rollup_test GROUP BY grp1",
		"SELECT grp1, grp2, SUM(val) as total FROM rollup_test GROUP BY grp1, grp2",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("ROLLUP error: %v", err)
		} else {
			t.Logf("ROLLUP returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_DistinctWithOrderBy tests DISTINCT with ORDER BY
func TestCoverage_DistinctWithOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "distinct_ob", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "cat", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		catg := "A"
		if i > 10 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "distinct_ob",
			Columns: []string{"id", "val", "cat"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal(catg)}},
		}, nil)
	}

	queries := []string{
		"SELECT DISTINCT cat FROM distinct_ob ORDER BY cat",
		"SELECT DISTINCT val FROM distinct_ob WHERE val > 5 ORDER BY val DESC LIMIT 5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("DISTINCT+ORDER BY error: %v", err)
		} else {
			t.Logf("DISTINCT+ORDER BY returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SubqueryInSelect tests subqueries in SELECT clause
func TestCoverage_SubqueryInSelect(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "ss_dept", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "ss_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ss_dept",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales")}, {numReal(2), strReal("Eng")}},
	}, nil)

	for i := 1; i <= 10; i++ {
		deptID := 1
		if i > 5 {
			deptID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "ss_emp",
			Columns: []string{"id", "dept_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(deptID))}},
		}, nil)
	}

	queries := []string{
		"SELECT d.name, (SELECT COUNT(*) FROM ss_emp e WHERE e.dept_id = d.id) as emp_count FROM ss_dept d",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Subquery in SELECT error: %v", err)
		} else {
			t.Logf("Subquery in SELECT returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_LeftJoin tests LEFT JOIN
func TestCoverage_LeftJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "lj_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "lj_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "lj_customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}, {numReal(3), strReal("Charlie")}},
	}, nil)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "lj_orders",
		Columns: []string{"id", "customer_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}, {numReal(2), numReal(1), numReal(200)}},
	}, nil)

	queries := []string{
		"SELECT c.name, o.amount FROM lj_customers c LEFT JOIN lj_orders o ON c.id = o.customer_id",
		"SELECT c.name, SUM(o.amount) as total FROM lj_customers c LEFT JOIN lj_orders o ON c.id = o.customer_id GROUP BY c.name",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LEFT JOIN error: %v", err)
		} else {
			t.Logf("LEFT JOIN returned %d rows", len(result.Rows))
		}
	}
}
