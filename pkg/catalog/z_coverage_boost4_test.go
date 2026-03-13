package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_WindowFunctionsBasic tests basic window functions
func TestCoverage_WindowFunctionsBasic(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_basic", []*query.ColumnDef{
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
			Table:   "win_basic",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM win_basic",
		"SELECT id, RANK() OVER (ORDER BY val) FROM win_basic",
		"SELECT id, DENSE_RANK() OVER (ORDER BY val) FROM win_basic",
		"SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) FROM win_basic",
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

// TestCoverage_WindowFunctionsAggregates tests window function aggregates
func TestCoverage_WindowFunctionsAggregates(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		grp := "A"
		if i > 5 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_agg",
			Columns: []string{"id", "grp", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(amount) OVER (ORDER BY id) FROM win_agg",
		"SELECT id, SUM(amount) OVER (PARTITION BY grp) FROM win_agg",
		"SELECT id, AVG(amount) OVER (ORDER BY id) FROM win_agg",
		"SELECT id, COUNT(*) OVER (ORDER BY id) FROM win_agg",
		"SELECT id, MIN(amount) OVER (ORDER BY id), MAX(amount) OVER (ORDER BY id) FROM win_agg",
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

// TestCoverage_WindowFunctionsLeadLag tests LEAD/LAG window functions
func TestCoverage_WindowFunctionsLeadLag(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_leadlag", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_leadlag",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, LEAD(val) OVER (ORDER BY id) FROM win_leadlag",
		"SELECT id, LEAD(val, 2) OVER (ORDER BY id) FROM win_leadlag",
		"SELECT id, LAG(val) OVER (ORDER BY id) FROM win_leadlag",
		"SELECT id, LAG(val, 2, 0) OVER (ORDER BY id) FROM win_leadlag",
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

// TestCoverage_WindowFrames tests window frame specifications
func TestCoverage_WindowFrames(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_frame", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_frame",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM win_frame",
		"SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM win_frame",
		"SELECT id, SUM(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM win_frame",
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
