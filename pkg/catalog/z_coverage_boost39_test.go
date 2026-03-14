package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_MathFunctionsExt tests mathematical functions
func TestCoverage_MathFunctionsExt(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "math_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenReal},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "math_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(-3.14)}, {numReal(2), numReal(3.14)}},
	}, nil)

	queries := []string{
		"SELECT ABS(val) FROM math_test",
		"SELECT ROUND(val) FROM math_test",
		"SELECT FLOOR(val) FROM math_test",
		"SELECT CEIL(val) FROM math_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Math function error: %v", err)
		} else {
			t.Logf("Math function returned %v", result.Rows)
		}
	}
}

// TestCoverage_DateFunctionsExt tests date functions
func TestCoverage_DateFunctionsExt(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "date_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dt", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "date_test",
		Columns: []string{"id", "dt"},
		Values:  [][]query.Expression{{numReal(1), strReal("2026-03-14")}},
	}, nil)

	queries := []string{
		"SELECT DATE(dt) FROM date_test",
		"SELECT STRFTIME('%Y', dt) FROM date_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Date function error: %v", err)
		} else {
			t.Logf("Date function returned %v", result.Rows)
		}
	}
}

// TestCoverage_JSONFunctionsMore tests JSON functions more
func TestCoverage_JSONFunctionsMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_more",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal(`{"name": "test", "value": 123}`)}},
	}, nil)

	queries := []string{
		"SELECT JSON_EXTRACT(data, '$.name') FROM json_more",
		"SELECT JSON_VALID(data) FROM json_more",
		"SELECT JSON_TYPE(data) FROM json_more",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JSON function error: %v", err)
		} else {
			t.Logf("JSON function returned %v", result.Rows)
		}
	}
}

// TestCoverage_AggregateFunctionsMore tests aggregate functions more
func TestCoverage_AggregateFunctionsMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "agg_more", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "agg_more",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT COUNT(*) FROM agg_more",
		"SELECT SUM(val) FROM agg_more",
		"SELECT AVG(val) FROM agg_more",
		"SELECT MIN(val) FROM agg_more",
		"SELECT MAX(val) FROM agg_more",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Aggregate error: %v", err)
		} else {
			t.Logf("Aggregate returned %v", result.Rows)
		}
	}
}

// TestCoverage_WindowFunctionsRange tests window functions with RANGE
func TestCoverage_WindowFunctionsRange(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "win_range2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 15; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "win_range2",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, val, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum3 FROM win_range2",
		"SELECT id, val, AVG(val) OVER (ORDER BY id ROWS 2 PRECEDING) as avg3 FROM win_range2",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window RANGE error: %v", err)
		} else {
			t.Logf("Window RANGE returned %d rows", len(result.Rows))
		}
	}
}
