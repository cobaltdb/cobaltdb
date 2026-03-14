package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_GroupAggregateJoinComplex targets evaluateExprWithGroupAggregatesJoin
func TestCoverage_GroupAggregateJoinComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "gaj_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
		{Name: "qty", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "gaj_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
		{Name: "tier", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "gaj_customers",
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
			Table:   "gaj_orders",
			Columns: []string{"id", "customer_id", "amount", "qty"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(custID)), numReal(float64(i * 100)), numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, SUM(o.amount) as total, AVG(o.amount) as avg_amt, SUM(o.qty) as total_qty FROM gaj_orders o JOIN gaj_customers c ON o.customer_id = c.id GROUP BY c.region HAVING total > 10000",
		"SELECT c.tier, COUNT(*) as cnt, MIN(o.amount) as min_amt, MAX(o.amount) as max_amt FROM gaj_orders o JOIN gaj_customers c ON o.customer_id = c.id GROUP BY c.tier HAVING cnt > 5",
		"SELECT c.region, c.tier, SUM(o.amount * o.qty) as weighted_sum FROM gaj_orders o JOIN gaj_customers c ON o.customer_id = c.id GROUP BY c.region, c.tier",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("GROUP AGG JOIN error: %v", err)
		} else {
			t.Logf("GROUP AGG JOIN returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_OuterRefComplex targets resolveOuterRefsInQuery
func TestCoverage_OuterRefComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "oref_dept", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "budget", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "oref_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
		{Name: "bonus", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "oref_dept",
		Columns: []string{"id", "name", "budget"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales"), numReal(100000)}, {numReal(2), strReal("Eng"), numReal(200000)}},
	}, nil)

	for i := 1; i <= 20; i++ {
		deptID := 1
		if i > 10 {
			deptID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "oref_emp",
			Columns: []string{"id", "dept_id", "salary", "bonus"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(deptID)), numReal(float64(3000 + i*100)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT d.name, (SELECT COUNT(*) FROM oref_emp e WHERE e.dept_id = d.id) as emp_count, (SELECT AVG(e.salary) FROM oref_emp e WHERE e.dept_id = d.id) as avg_sal FROM oref_dept d",
		"SELECT d.name FROM oref_dept d WHERE EXISTS (SELECT 1 FROM oref_emp e WHERE e.dept_id = d.id AND e.salary > 3500)",
		"SELECT e.id, e.salary FROM oref_emp e WHERE e.salary + e.bonus > (SELECT AVG(e2.salary + e2.bonus) FROM oref_emp e2 WHERE e2.dept_id = e.dept_id)",
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

// TestCoverage_SavepointRollbackComplex targets RollbackToSavepoint
func TestCoverage_SavepointRollbackComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_roll", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Test rollback without transaction
	err := cat.RollbackToSavepoint("test")
	if err != nil {
		t.Logf("Rollback without txn error (expected): %v", err)
	}

	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_roll",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	cat.Savepoint("sp1")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_roll",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("sp1")}},
	}, nil)

	cat.Savepoint("sp2")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_roll",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("sp2")}},
	}, nil)

	// Rollback to sp2
	err = cat.RollbackToSavepoint("sp2")
	if err != nil {
		t.Logf("RollbackToSavepoint sp2 error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_roll")
	t.Logf("Count after rollback to sp2: %v", result.Rows)

	// Try to rollback to non-existent savepoint
	err = cat.RollbackToSavepoint("nonexistent")
	if err != nil {
		t.Logf("Rollback to nonexistent error (expected): %v", err)
	}

	cat.RollbackTransaction()

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM sp_roll")
	t.Logf("Count after rollback: %v", result.Rows)
}

// TestCoverage_JoinAggregateExpression tests JOIN with aggregate expressions
func TestCoverage_JoinAggregateExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jae_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jae_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jae_customers",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{numReal(1), strReal("North")}, {numReal(2), strReal("South")}},
	}, nil)

	for i := 1; i <= 20; i++ {
		custID := 1
		if i > 10 {
			custID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jae_orders",
			Columns: []string{"id", "customer_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(custID)), numReal(float64(i * 100))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, SUM(o.amount) as total, AVG(o.amount) as avg_val, total - avg_val as diff FROM jae_orders o JOIN jae_customers c ON o.customer_id = c.id GROUP BY c.region",
		"SELECT c.region, COUNT(*) * 10 as scaled_count, SUM(o.amount) / COUNT(*) as calc_avg FROM jae_orders o JOIN jae_customers c ON o.customer_id = c.id GROUP BY c.region",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN aggregate expr error: %v", err)
		} else {
			t.Logf("JOIN aggregate expr returned %d rows", len(result.Rows))
		}
	}
}
