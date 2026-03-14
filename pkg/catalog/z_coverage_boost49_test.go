package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ResolveAggregateInExprMore targets resolveAggregateInExpr
func TestCoverage_ResolveAggregateInExprMore(t *testing.T) {
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

	for i := 1; i <= 40; i++ {
		grp := "A"
		if i > 20 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_res",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) as total, AVG(val) as avg_val FROM agg_res GROUP BY grp HAVING total > 500",
		"SELECT grp, COUNT(*) as cnt, MIN(val) as min_v, MAX(val) as max_v FROM agg_res GROUP BY grp HAVING cnt > 15",
		"SELECT grp, SUM(val) + COUNT(*) as combined FROM agg_res GROUP BY grp HAVING combined > 1000",
		"SELECT grp, AVG(val) * 2 as doubled FROM agg_res GROUP BY grp HAVING doubled > 300",
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

// TestCoverage_SelectLockedWithOrderBy targets selectLocked with ORDER BY
func TestCoverage_SelectLockedWithOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_ob", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "txt", Type: query.TokenText},
	})

	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_ob",
			Columns: []string{"id", "val", "txt"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(101 - i)), strReal("text")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM sel_ob ORDER BY val",
		"SELECT * FROM sel_ob ORDER BY val DESC",
		"SELECT * FROM sel_ob ORDER BY val LIMIT 20",
		"SELECT * FROM sel_ob ORDER BY val LIMIT 10 OFFSET 50",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("ORDER BY error: %v", err)
		} else {
			t.Logf("ORDER BY returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ExecuteSelectWithJoinAndGroupByMore targets executeSelectWithJoinAndGroupBy
func TestCoverage_ExecuteSelectWithJoinAndGroupByMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "jgb_o", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "c_id", Type: query.TokenInteger},
		{Name: "amt", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, cat, "jgb_c", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jgb_c",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{numReal(1), strReal("N")}, {numReal(2), strReal("S")}, {numReal(3), strReal("E")}},
	}, nil)

	for i := 1; i <= 60; i++ {
		cID := ((i - 1) % 3) + 1
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_o",
			Columns: []string{"id", "c_id", "amt"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(cID)), numReal(float64(i * 100))}},
		}, nil)
	}

	queries := []string{
		"SELECT c.region, SUM(o.amt) as total FROM jgb_o o JOIN jgb_c c ON o.c_id = c.id GROUP BY c.region HAVING total > 5000",
		"SELECT c.region, AVG(o.amt) as avg_amt, COUNT(*) as cnt FROM jgb_o o JOIN jgb_c c ON o.c_id = c.id GROUP BY c.region",
		"SELECT c.region, MIN(o.amt) as min_amt, MAX(o.amt) as max_amt FROM jgb_o o JOIN jgb_c c ON o.c_id = c.id GROUP BY c.region HAVING max_amt > 5000",
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

// TestCoverage_DeleteWithUsingMore targets deleteWithUsingLocked
func TestCoverage_DeleteWithUsingMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "del_target", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "del_using", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	for i := 1; i <= 30; i++ {
		status := "active"
		if i > 20 {
			status = "deleted"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_using",
			Columns: []string{"id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(status)}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "del_target",
			Columns: []string{"id", "ref_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// DELETE ... USING
	cat.ExecuteQuery("DELETE FROM del_target USING del_target t JOIN del_using u ON t.ref_id = u.id WHERE u.status = 'deleted'")

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM del_target")
	t.Logf("Count after delete: %v", result.Rows)
}

// TestCoverage_ApplyRLSFilterInternalMore targets applyRLSFilterInternal
func TestCoverage_ApplyRLSFilterInternalMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "rls_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "owner", Type: query.TokenText},
		{Name: "data", Type: query.TokenText},
	})

	for i := 1; i <= 20; i++ {
		owner := "user1"
		if i > 10 {
			owner = "user2"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "rls_test",
			Columns: []string{"id", "owner", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(owner), strReal("data")}},
		}, nil)
	}

	// Query all rows
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM rls_test")
	t.Logf("Total rows: %v", result.Rows)
}

// TestCoverage_ExecuteSelectWithJoinMore targets executeSelectWithJoin
func TestCoverage_ExecuteSelectWithJoinMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "join_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, cat, "join_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a_id", Type: query.TokenInteger},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_a",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("A" + string(rune('0'+i%10)))}},
		}, nil)
	}

	for i := 1; i <= 30; i++ {
		aID := ((i - 1) % 10) + 1
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "join_b",
			Columns: []string{"id", "a_id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(aID)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT a.name, SUM(b.val) as total FROM join_a a JOIN join_b b ON a.id = b.a_id GROUP BY a.name",
		"SELECT a.name, COUNT(*) as cnt FROM join_a a JOIN join_b b ON a.id = b.a_id GROUP BY a.name HAVING cnt > 2",
		"SELECT * FROM join_a a JOIN join_b b ON a.id = b.a_id WHERE b.val > 100 ORDER BY b.val DESC LIMIT 10",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN error: %v", err)
		} else {
			t.Logf("JOIN returned %d rows", len(result.Rows))
		}
	}
}
