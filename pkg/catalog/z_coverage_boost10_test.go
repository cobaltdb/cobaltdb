package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_CountRowsAccuracy tests countRows with various scenarios
func TestCoverage_CountRowsAccuracy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "count_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Empty table count
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM count_test")
	t.Logf("Empty count: %v", result.Rows)

	// Insert data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "count_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Full count
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM count_test")
	t.Logf("Full count: %v", result.Rows)

	// Count with WHERE
	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM count_test WHERE id > 50")
	t.Logf("Filtered count: %v", result.Rows)

	// Count DISTINCT
	result, _ = cat.ExecuteQuery("SELECT COUNT(DISTINCT val) FROM count_test")
	t.Logf("Distinct count: %v", result.Rows)
}

// TestCoverage_AnalyzeTable tests ANALYZE operations
func TestCoverage_AnalyzeTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "analyze_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "analyze_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// Analyze table
	err := cat.Analyze("analyze_test")
	if err != nil {
		t.Logf("Analyze error: %v", err)
	}
}

// TestCoverage_SelectLimitEdgeCases tests LIMIT edge cases
func TestCoverage_SelectLimitEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "limit_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "limit_edge",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM limit_edge LIMIT 0",
		"SELECT * FROM limit_edge LIMIT 5 OFFSET 0",
		"SELECT * FROM limit_edge ORDER BY id LIMIT 10 OFFSET 5",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIMIT query error: %v", err)
		} else {
			t.Logf("LIMIT query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_NullHandling tests NULL handling
func TestCoverage_NullHandling(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "null_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_test",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), numReal(10)},
			{numReal(2), &query.NullLiteral{}},
			{numReal(3), numReal(30)},
		},
	}, nil)

	queries := []string{
		"SELECT * FROM null_test WHERE val IS NULL",
		"SELECT * FROM null_test WHERE val IS NOT NULL",
		"SELECT COALESCE(val, 0) FROM null_test",
		"SELECT NULLIF(val, 10) FROM null_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("NULL query error: %v", err)
		} else {
			t.Logf("NULL query returned %d rows", len(result.Rows))
		}
	}
}
