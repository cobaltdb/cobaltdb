package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_WindowPartitionOnly tests window functions with PARTITION BY only
func TestCoverage_WindowPartitionOnly(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_part", []*query.ColumnDef{
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
			Table:   "win_part",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(val) OVER (PARTITION BY grp) as total FROM win_part",
		"SELECT id, AVG(val) OVER (PARTITION BY grp) as avg_val FROM win_part",
		"SELECT id, COUNT(*) OVER (PARTITION BY grp) as cnt FROM win_part",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window partition error: %v", err)
		} else {
			t.Logf("Window partition returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_WindowOrderOnly tests window functions with ORDER BY only
func TestCoverage_WindowOrderOnly(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_order", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_order",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(val) OVER (ORDER BY id) as running_sum FROM win_order",
		"SELECT id, COUNT(*) OVER (ORDER BY id) as running_cnt FROM win_order",
		"SELECT id, ROW_NUMBER() OVER (ORDER BY val) as rn FROM win_order",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window order error: %v", err)
		} else {
			t.Logf("Window order returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_WindowFrameRows tests ROWS frame specification
func TestCoverage_WindowFrameRows(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_rows", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_rows",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(val) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) as sum1 FROM win_rows",
		"SELECT id, SUM(val) OVER (ORDER BY id ROWS 2 PRECEDING) as sum2 FROM win_rows",
		"SELECT id, AVG(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as avg1 FROM win_rows",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window ROWS frame error: %v", err)
		} else {
			t.Logf("Window ROWS frame returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_WindowFrameRange tests RANGE frame specification
func TestCoverage_WindowFrameRange(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_range", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_range",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, SUM(val) OVER (ORDER BY id RANGE UNBOUNDED PRECEDING) as sum1 FROM win_range",
		"SELECT id, AVG(val) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as avg1 FROM win_range",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window RANGE frame error: %v", err)
		} else {
			t.Logf("Window RANGE frame returned %d rows", len(result.Rows))
		}
	}
}
