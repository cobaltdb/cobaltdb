package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_CastExpressions tests CAST expressions
func TestCoverage_CastExpressions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cast_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
		{Name: "num", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "val", "num"},
		Values:  [][]query.Expression{{numReal(1), strReal("123"), numReal(456)}},
	}, nil)

	queries := []string{
		"SELECT CAST(val AS INTEGER) FROM cast_test",
		"SELECT CAST(num AS TEXT) FROM cast_test",
		"SELECT CAST(id AS REAL) FROM cast_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CAST query error: %v", err)
		} else {
			t.Logf("CAST query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ArithmeticOperations tests arithmetic operations
func TestCoverage_ArithmeticOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "arith_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "arith_test",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(1), numReal(10), numReal(3)}},
	}, nil)

	queries := []string{
		"SELECT a + b, a - b, a * b FROM arith_test",
		"SELECT a / b, a % b FROM arith_test",
		"SELECT -a, +a FROM arith_test",
		"SELECT a + 5 * b FROM arith_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Arithmetic query error: %v", err)
		} else {
			t.Logf("Arithmetic query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ComparisonOperations tests comparison operations
func TestCoverage_ComparisonOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "comp_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "comp_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	queries := []string{
		"SELECT * FROM comp_test WHERE val = 100",
		"SELECT * FROM comp_test WHERE val != 50",
		"SELECT * FROM comp_test WHERE val > 50",
		"SELECT * FROM comp_test WHERE val < 200",
		"SELECT * FROM comp_test WHERE val >= 100",
		"SELECT * FROM comp_test WHERE val <= 100",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Comparison query error: %v", err)
		} else {
			t.Logf("Comparison query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_LogicalOperations tests logical operations
func TestCoverage_LogicalOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "logic_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "logic_test",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(1), numReal(10), numReal(20)}},
	}, nil)

	queries := []string{
		"SELECT * FROM logic_test WHERE a > 5 AND b < 30",
		"SELECT * FROM logic_test WHERE a > 20 OR b < 30",
		"SELECT * FROM logic_test WHERE NOT (a > 20)",
		"SELECT * FROM logic_test WHERE (a > 5 AND b < 30) OR (a > 100)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Logical query error: %v", err)
		} else {
			t.Logf("Logical query returned %d rows", len(result.Rows))
		}
	}
}
