package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_JSONQuote tests JSONQuote function
func TestCoverage_JSONQuote(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_quote_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "json_quote_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("hello")}, {numReal(2), strReal("world")}},
	}, nil)

	queries := []string{
		"SELECT JSONQuote(val) FROM json_quote_test",
		"SELECT JSONQuote('test')",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("JSONQuote error: %v", err)
		} else {
			t.Logf("JSONQuote returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ApplyOrderBy tests applyOrderBy function
func TestCoverage_ApplyOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "order_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		name := "B"
		if i%2 == 0 {
			name = "A"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "order_test",
			Columns: []string{"id", "name", "val"},
			Values:  [][]query.Expression{{numReal(float64(11 - i)), strReal(name), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM order_test ORDER BY name ASC, val DESC",
		"SELECT * FROM order_test ORDER BY id DESC LIMIT 5",
		"SELECT name, SUM(val) as total FROM order_test GROUP BY name ORDER BY total DESC",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("applyOrderBy error: %v", err)
		} else {
			t.Logf("applyOrderBy returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_IsCacheableQuery tests isCacheableQuery function
func TestCoverage_IsCacheableQuery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cache_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cache_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Execute multiple times to test caching
	for i := 0; i < 3; i++ {
		result, err := cat.ExecuteQuery("SELECT * FROM cache_test WHERE id = 1")
		if err != nil {
			t.Logf("Cacheable query error: %v", err)
		} else {
			t.Logf("Cacheable query iteration %d: %d rows", i, len(result.Rows))
		}
	}

	// Test non-cacheable query (with NOW())
	result, err := cat.ExecuteQuery("SELECT * FROM cache_test WHERE val = 'test'")
	if err != nil {
		t.Logf("Query error: %v", err)
	} else {
		t.Logf("Query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_StripQuotes tests stripQuotes function
func TestCoverage_StripQuotes(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create table with quoted identifier
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: `"quoted_table"`,
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Logf("Create table with quotes error: %v", err)
	}

	// Insert and select
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "quoted_table",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	result, err := cat.ExecuteQuery("SELECT * FROM quoted_table")
	if err != nil {
		t.Logf("Select from quoted table error: %v", err)
	} else {
		t.Logf("Select returned %d rows", len(result.Rows))
	}
}
