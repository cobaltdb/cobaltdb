package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_LikePatternsMore tests evaluateLike with more patterns
func TestCoverage_LikePatternsMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "like_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	testValues := []string{
		"hello", "world", "hello world", "HELLO", "World123",
		"test_underscore", "test%percent", "abc", "xyz",
		"prefix_middle_suffix", "start_", "_end", "mid__dle",
	}
	for i, v := range testValues {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "like_more",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(v)}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM like_more WHERE val LIKE '%middle%'",
		"SELECT * FROM like_more WHERE val LIKE 'prefix%'",
		"SELECT * FROM like_more WHERE val LIKE '%suffix'",
		"SELECT * FROM like_more WHERE val LIKE '___'",
		"SELECT * FROM like_more WHERE val LIKE '%__%'",
		"SELECT * FROM like_more WHERE val NOT LIKE '%test%'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIKE pattern error: %v", err)
		} else {
			t.Logf("LIKE pattern returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SelectLockedMore tests selectLocked with complex queries
func TestCoverage_SelectLockedMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_complex",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("value")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM sel_complex WHERE a > 100 ORDER BY a DESC LIMIT 10",
		"SELECT * FROM sel_complex WHERE a BETWEEN 100 AND 300 ORDER BY a",
		"SELECT DISTINCT b FROM sel_complex",
		"SELECT COUNT(*), AVG(a), MAX(a), MIN(a) FROM sel_complex",
		"SELECT * FROM sel_complex WHERE b = 'value' AND a > 200 LIMIT 5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Select error: %v", err)
		} else {
			t.Logf("Select returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_GroupByAggregateExpr tests GROUP BY with complex aggregate expressions
func TestCoverage_GroupByAggregateExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "grp_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	for i := 1; i <= 30; i++ {
		grp := "A"
		if i > 15 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "grp_agg",
			Columns: []string{"id", "grp", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val1) as s1, SUM(val2) as s2, s1 - s2 as diff FROM grp_agg GROUP BY grp",
		"SELECT grp, AVG(val1) as avg1, AVG(val2) as avg2, avg1 + avg2 as total_avg FROM grp_agg GROUP BY grp",
		"SELECT grp, COUNT(*) as cnt, SUM(val1) / COUNT(*) as avg_calc FROM grp_agg GROUP BY grp",
		"SELECT grp, MIN(val1) as min1, MAX(val1) as max1, max1 - min1 as range_val FROM grp_agg GROUP BY grp",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("GROUP BY aggregate error: %v", err)
		} else {
			t.Logf("GROUP BY aggregate returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CorrelatedSubqueryMore tests correlated subqueries more
func TestCoverage_CorrelatedSubqueryMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "corr_dept2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "budget", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "corr_emp2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept_id", Type: query.TokenInteger},
		{Name: "salary", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "corr_dept2",
		Columns: []string{"id", "name", "budget"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales"), numReal(100000)}, {numReal(2), strReal("Eng"), numReal(200000)}},
	}, nil)

	for i := 1; i <= 20; i++ {
		deptID := 1
		if i > 10 {
			deptID = 2
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "corr_emp2",
			Columns: []string{"id", "dept_id", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(deptID)), numReal(float64(3000 + i*100))}},
		}, nil)
	}

	queries := []string{
		"SELECT d.name FROM corr_dept2 d WHERE EXISTS (SELECT 1 FROM corr_emp2 e WHERE e.dept_id = d.id)",
		"SELECT e.id, e.salary FROM corr_emp2 e WHERE e.salary > (SELECT AVG(e2.salary) FROM corr_emp2 e2 WHERE e2.dept_id = e.dept_id)",
		"SELECT d.name, (SELECT SUM(e.salary) FROM corr_emp2 e WHERE e.dept_id = d.id) as total_salary FROM corr_dept2 d",
		"SELECT d.name FROM corr_dept2 d WHERE d.budget > (SELECT SUM(e.salary) FROM corr_emp2 e WHERE e.dept_id = d.id) * 2",
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

// TestCoverage_WindowFunctionsMore tests window functions more
func TestCoverage_WindowFunctionsMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_more", []*query.ColumnDef{
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
			Table:   "win_more",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn FROM win_more",
		"SELECT id, val, RANK() OVER (PARTITION BY grp ORDER BY val DESC) as rk FROM win_more",
		"SELECT id, val, DENSE_RANK() OVER (PARTITION BY grp ORDER BY val) as dr FROM win_more",
		"SELECT id, val, LAG(val, 1, 0) OVER (ORDER BY id) as prev FROM win_more",
		"SELECT id, val, LEAD(val, 1, 0) OVER (ORDER BY id) as next FROM win_more",
		"SELECT id, val, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) as first FROM win_more",
		"SELECT id, val, LAST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) as last FROM win_more",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window function error: %v", err)
		} else {
			t.Logf("Window function returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_TransactionEdgeCases tests transaction edge cases
func TestCoverage_TransactionEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Begin, insert, rollback
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_edge",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("txn1")}},
	}, nil)
	cat.RollbackTransaction()

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_edge")
	t.Logf("Count after rollback: %v", result.Rows)

	// Begin, insert, commit
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_edge",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("txn2")}},
	}, nil)
	cat.CommitTransaction()

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM txn_edge")
	t.Logf("Count after commit: %v", result.Rows)

	// Multiple savepoints
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_edge",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("sp1")}},
	}, nil)
	cat.Savepoint("sp1")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_edge",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(4), strReal("sp2")}},
	}, nil)
	cat.ReleaseSavepoint("sp1")
	cat.CommitTransaction()

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM txn_edge")
	t.Logf("Count after release: %v", result.Rows)
}
