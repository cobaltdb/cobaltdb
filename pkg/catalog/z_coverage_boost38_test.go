package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_JoinGroupAggregateComplex tests JOIN+GROUP BY with complex aggregates
func TestCoverage_JoinGroupAggregateComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jgac_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
		{Name: "discount", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jgac_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jgac_customers",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{numReal(1), strReal("North")}, {numReal(2), strReal("South")}},
	}, nil)

	for i := 1; i <= 20; i++ {
		custID := 1
		if i > 10 {
			custID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgac_orders",
			Columns: []string{"id", "customer_id", "amount", "discount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(custID)), numReal(float64(i * 100)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, SUM(o.amount) as total, AVG(o.amount) as avg_amt, SUM(o.amount - o.discount) as net FROM jgac_orders o JOIN jgac_customers c ON o.customer_id = c.id GROUP BY c.region",
		"SELECT c.region, COUNT(*) as cnt, MIN(o.amount) as min_amt, MAX(o.amount) as max_amt, max_amt - min_amt as range_val FROM jgac_orders o JOIN jgac_customers c ON o.customer_id = c.id GROUP BY c.region",
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

// TestCoverage_CorrelatedSubqueryComplex tests complex correlated subqueries
func TestCoverage_CorrelatedSubqueryComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cs_dept", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "cs_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cs_dept",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales")}, {numReal(2), strReal("Eng")}},
	}, nil)

	for i := 1; i <= 20; i++ {
		deptID := 1
		if i > 10 {
			deptID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cs_emp",
			Columns: []string{"id", "dept_id", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(deptID)), numReal(float64(3000 + i*100))}},
		}, nil)
	}

	queries := []string{
		"SELECT d.name, (SELECT COUNT(*) FROM cs_emp e WHERE e.dept_id = d.id) as emp_count, (SELECT AVG(e.salary) FROM cs_emp e WHERE e.dept_id = d.id) as avg_salary FROM cs_dept d",
		"SELECT e.id, e.salary FROM cs_emp e WHERE e.salary > (SELECT AVG(e2.salary) FROM cs_emp e2 WHERE e2.dept_id = e.dept_id)",
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

// TestCoverage_SavepointOperations tests savepoint operations extensively
func TestCoverage_SavepointOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_ops", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Test without transaction first
	err := cat.RollbackToSavepoint("test")
	if err != nil {
		t.Logf("Rollback without txn: %v", err)
	}

	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_ops",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	cat.Savepoint("sp1")

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_ops",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("sp1_data")}},
	}, nil)

	// Rollback to sp1
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback error: %v", err)
	}

	cat.CommitTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_ops")
	t.Logf("Count after operations: %v", result.Rows)
}

// TestCoverage_UpdateComplexConditions tests UPDATE with complex conditions
func TestCoverage_UpdateComplexConditions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "upd_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_complex",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("test")}},
		}, nil)
	}

	// Complex AND condition
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_complex",
		Set:   []*query.SetClause{{Column: "a", Value: numReal(999)}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(5),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenLt,
				Right:    numReal(20),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM upd_complex WHERE a = 999")
	t.Logf("Count after complex update: %v", result.Rows)
}

// TestCoverage_DeleteComplexConditions tests DELETE with complex conditions
func TestCoverage_DeleteComplexConditions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_complex",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("test")}},
		}, nil)
	}

	// Complex AND condition
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_complex",
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    numReal(5),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenLt,
				Right:    numReal(20),
			},
		},
	}, nil)

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_complex")
	t.Logf("Count after complex delete: %v", result.Rows)
}
