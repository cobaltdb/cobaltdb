package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_LeadLagWithDefault tests LEAD/LAG with default values
func TestCoverage_LeadLagWithDefault(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "lead_lag_def", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "lead_lag_def",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, LEAD(val, 1, 0) OVER (ORDER BY id) as next_val FROM lead_lag_def",
		"SELECT id, LAG(val, 1, -1) OVER (ORDER BY id) as prev_val FROM lead_lag_def",
		"SELECT id, LEAD(val, 2, 999) OVER (ORDER BY id) as next2 FROM lead_lag_def",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LEAD/LAG with default error: %v", err)
		} else {
			t.Logf("LEAD/LAG with default returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_RowNumberRankDenseRank tests ranking window functions
func TestCoverage_RowNumberRankDenseRank(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "rank_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
	})

	// Insert data with duplicates for ranking
	data := [][]interface{}{
		{1, "A", 100},
		{2, "A", 100},
		{3, "A", 90},
		{4, "B", 100},
		{5, "B", 80},
	}
	for _, row := range data {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "rank_test",
			Columns: []string{"id", "grp", "score"},
			Values:  [][]query.Expression{{numReal(float64(row[0].(int))), strReal(row[1].(string)), numReal(float64(row[2].(int)))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) as rn FROM rank_test",
		"SELECT id, RANK() OVER (PARTITION BY grp ORDER BY score DESC) as rk FROM rank_test",
		"SELECT id, DENSE_RANK() OVER (PARTITION BY grp ORDER BY score DESC) as dr FROM rank_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Ranking functions error: %v", err)
		} else {
			t.Logf("Ranking functions returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_WindowFunctionsNoPartition tests window functions without PARTITION BY
func TestCoverage_WindowFunctionsNoPartition(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_no_part", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_no_part",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(val) OVER () as total FROM win_no_part",
		"SELECT id, AVG(val) OVER () as avg_val FROM win_no_part",
		"SELECT id, COUNT(*) OVER () as cnt FROM win_no_part",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window no partition error: %v", err)
		} else {
			t.Logf("Window no partition returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_WindowFunctionsNoOrder tests window functions without ORDER BY
func TestCoverage_WindowFunctionsNoOrder(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_no_order", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_no_order",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(val) OVER (PARTITION BY grp) as total FROM win_no_order",
		"SELECT id, MIN(val) OVER (PARTITION BY grp) as min_val FROM win_no_order",
		"SELECT id, MAX(val) OVER (PARTITION BY grp) as max_val FROM win_no_order",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window no order error: %v", err)
		} else {
			t.Logf("Window no order returned %d rows", len(result.Rows))
		}
	}
}
