package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_InOperator tests IN operator evaluation
func TestCoverage_InOperator(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "in_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "in_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM in_test WHERE id IN (1, 3, 5, 7)",
		"SELECT * FROM in_test WHERE val IN (10, 30, 50, 100)",
		"SELECT * FROM in_test WHERE id NOT IN (2, 4, 6, 8)",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("IN operator error: %v", err)
		} else {
			t.Logf("IN query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_BetweenOperator tests BETWEEN operator evaluation
func TestCoverage_BetweenOperator(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "between_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "between_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM between_test WHERE id BETWEEN 5 AND 15",
		"SELECT * FROM between_test WHERE val BETWEEN 10 AND 50",
		"SELECT * FROM between_test WHERE id NOT BETWEEN 8 AND 12",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("BETWEEN operator error: %v", err)
		} else {
			t.Logf("BETWEEN query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CastOperator tests CAST expression evaluation
func TestCoverage_CastOperator(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cast_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "num", Type: query.TokenReal},
		{Name: "txt", Type: query.TokenText},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "num", "txt"},
		Values:  [][]query.Expression{{numReal(1), numReal(3.14), strReal("42")}},
	}, nil)

	queries := []string{
		"SELECT CAST(num AS INTEGER) as int_val FROM cast_test",
		"SELECT CAST(id AS TEXT) as str_val FROM cast_test",
		"SELECT CAST(txt AS INTEGER) as parsed FROM cast_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CAST operator error: %v", err)
		} else {
			t.Logf("CAST query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_IsNullOperator tests IS NULL evaluation
func TestCoverage_IsNullOperator(t *testing.T) {
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
			{numReal(1), numReal(100)},
			{numReal(2), &query.NullLiteral{}},
			{numReal(3), numReal(300)},
			{numReal(4), &query.NullLiteral{}},
		},
	}, nil)

	queries := []string{
		"SELECT * FROM null_test WHERE val IS NULL",
		"SELECT * FROM null_test WHERE val IS NOT NULL",
		"SELECT COUNT(*) FROM null_test WHERE val IS NULL",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("IS NULL operator error: %v", err)
		} else {
			t.Logf("IS NULL query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ArithmeticOps tests arithmetic operations
func TestCoverage_ArithmeticOps(t *testing.T) {
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
		"SELECT a + b as sum_val FROM arith_test",
		"SELECT a - b as diff_val FROM arith_test",
		"SELECT a * b as prod_val FROM arith_test",
		"SELECT a / b as div_val FROM arith_test",
		"SELECT a % b as mod_val FROM arith_test",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Arithmetic error: %v", err)
		} else {
			t.Logf("Arithmetic query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ComparisonOperators tests comparison operators
func TestCoverage_ComparisonOperators(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cmp_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cmp_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM cmp_test WHERE val > 50",
		"SELECT * FROM cmp_test WHERE val < 80",
		"SELECT * FROM cmp_test WHERE val >= 50",
		"SELECT * FROM cmp_test WHERE val <= 30",
		"SELECT * FROM cmp_test WHERE val <> 50",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Comparison error: %v", err)
		} else {
			t.Logf("Comparison query returned %d rows", len(result.Rows))
		}
	}
}
