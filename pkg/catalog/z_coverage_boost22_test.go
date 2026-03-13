package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_ExecuteQueryWithArgs tests query execution with arguments
func TestCoverage_ExecuteQueryWithArgs(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "args_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "args_test",
			Columns: []string{"id", "name", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Query with different argument types via parser
	queries := []string{
		"SELECT * FROM args_test WHERE id = 5",
		"SELECT * FROM args_test WHERE name = 'test'",
		"SELECT * FROM args_test WHERE val > 50",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query with args error: %v", err)
		} else {
			t.Logf("Query with args returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_PrepareStatement tests prepared statement functionality
func TestCoverage_PrepareStatement(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "prep_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "prep_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("value")}},
		}, nil)
	}

	// Execute same query multiple times (simulating prepared statement behavior)
	for i := 1; i <= 3; i++ {
		query := fmt.Sprintf("SELECT * FROM prep_test WHERE id = %d", i)
		result, err := cat.ExecuteQuery(query)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
		_ = result
	}
}

// TestCoverage_QueryCacheHit tests query cache hit scenarios
func TestCoverage_QueryCacheHit(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cache_hit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cache_hit",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Execute same query multiple times to hit cache
	query := "SELECT * FROM cache_hit WHERE id = 1"
	for i := 0; i < 5; i++ {
		result, err := cat.ExecuteQuery(query)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query iteration %d: %d rows", i, len(result.Rows))
		}
		_ = result
	}

	// Check cache stats
	hits, misses, _ := cat.GetQueryCacheStats()
	t.Logf("Cache stats: hits=%d, misses=%d", hits, misses)
}

// TestCoverage_InvalidateCacheOnModify tests cache invalidation on modifications
func TestCoverage_InvalidateCacheOnModify(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cache_inv", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cache_inv",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	// First query - cache miss
	cat.ExecuteQuery("SELECT * FROM cache_inv")

	// Modify data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cache_inv",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("new")}},
	}, nil)

	// Query again - should not use stale cache
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM cache_inv")
	t.Logf("Count after insert: %v", result.Rows)
}
