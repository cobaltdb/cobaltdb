package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_CorrelatedSubqueryWithJoin targets resolveOuterRefsInQuery with JOINs
func TestCoverage_CorrelatedSubqueryWithJoin(t *testing.T) {
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

	createCoverageTestTable(t, cat, "cs_proj", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "emp_id", Type: query.TokenInteger},
		{Name: "hours", Type: query.TokenInteger},
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

	for i := 1; i <= 40; i++ {
		empID := ((i - 1) % 20) + 1
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cs_proj",
			Columns: []string{"id", "emp_id", "hours"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(empID)), numReal(float64(i * 5))}},
		}, nil)
	}

	// Correlated subquery with JOIN - references outer query table in subquery
	queries := []string{
		"SELECT d.name, (SELECT SUM(p.hours) FROM cs_emp e JOIN cs_proj p ON e.id = p.emp_id WHERE e.dept_id = d.id) as total_hours FROM cs_dept d",
		"SELECT d.name FROM cs_dept d WHERE EXISTS (SELECT 1 FROM cs_emp e JOIN cs_proj p ON e.id = p.emp_id WHERE e.dept_id = d.id AND p.hours > 100)",
		"SELECT e.id, e.salary FROM cs_emp e WHERE e.salary > (SELECT AVG(e2.salary) FROM cs_emp e2 WHERE e2.dept_id = e.dept_id)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Correlated with JOIN error: %v", err)
		} else {
			t.Logf("Correlated with JOIN returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CorrelatedSubqueryOrderBy targets ORDER BY with outer refs
func TestCoverage_CorrelatedSubqueryOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cso_dept", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "priority", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "cso_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cso_dept",
		Columns: []string{"id", "name", "priority"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales"), numReal(2)}, {numReal(2), strReal("Eng"), numReal(1)}},
	}, nil)

	for i := 1; i <= 10; i++ {
		deptID := 1
		if i > 5 {
			deptID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cso_emp",
			Columns: []string{"id", "dept_id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(deptID)), strReal("Emp" + string(rune('A'+i)))}},
		}, nil)
	}

	// Query with correlated subquery that has ORDER BY
	result, err := cat.ExecuteQuery("SELECT d.name, (SELECT e.name FROM cso_emp e WHERE e.dept_id = d.id ORDER BY e.name LIMIT 1) as first_emp FROM cso_dept d")
	if err != nil {
		t.Logf("Correlated ORDER BY error: %v", err)
	} else {
		t.Logf("Correlated ORDER BY returned %d rows", len(result.Rows))
	}
}

// TestCoverage_CorrelatedSubqueryGroupBy targets GROUP BY with outer refs
func TestCoverage_CorrelatedSubqueryGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "csg_dept", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "csg_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "role", Type: query.TokenText},
		{Name: "salary", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "csg_dept",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales")}, {numReal(2), strReal("Eng")}},
	}, nil)

	roles := []string{"Lead", "Dev", "Manager"}
	for i := 1; i <= 30; i++ {
		deptID := ((i - 1) % 2) + 1
		role := roles[i%3]
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "csg_emp",
			Columns: []string{"id", "dept_id", "role", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(deptID)), strReal(role), numReal(float64(3000 + i*100))}},
		}, nil)
	}

	// Correlated subquery with GROUP BY - use alias for derived table
	result, err := cat.ExecuteQuery("SELECT d.name, (SELECT MAX(total) FROM (SELECT SUM(salary) as total FROM csg_emp e WHERE e.dept_id = d.id GROUP BY e.role) t) as max_role_total FROM csg_dept d")
	if err != nil {
		t.Logf("Correlated GROUP BY error: %v", err)
	} else {
		t.Logf("Correlated GROUP BY returned %d rows", len(result.Rows))
	}
}
