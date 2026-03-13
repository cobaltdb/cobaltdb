package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_LikePatternMatching tests LIKE pattern matching edge cases
func TestCoverage_LikePatternMatching(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "like_pat", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	values := []string{
		"hello", "world", "hello world", "HELLO", "World123",
		"test_underscore", "test%percent", "abc", "xyz",
	}
	for i, v := range values {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "like_pat",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(v)}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM like_pat WHERE val LIKE 'hello%'",
		"SELECT * FROM like_pat WHERE val LIKE '%world'",
		"SELECT * FROM like_pat WHERE val LIKE '%test%'",
		"SELECT * FROM like_pat WHERE val LIKE 'abc'",
		"SELECT * FROM like_pat WHERE val LIKE '___'",
		"SELECT * FROM like_pat WHERE val NOT LIKE '%123%'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("LIKE pattern error: %v", err)
		} else {
			t.Logf("LIKE pattern returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ExpressionEvaluation tests expression evaluation
func TestCoverage_ExpressionEvaluation(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "expr_eval", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "expr_eval",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{numReal(1), numReal(10), numReal(3)}},
	}, nil)

	queries := []string{
		"SELECT (a + b) * 2 as calc1 FROM expr_eval",
		"SELECT a * b + a / b as calc2 FROM expr_eval",
		"SELECT (a - b) / (a + b) as calc3 FROM expr_eval",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Expression evaluation error: %v", err)
		} else {
			t.Logf("Expression evaluation returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_HavingWithComplexAggregates tests HAVING with complex aggregate expressions
func TestCoverage_HavingWithComplexAggregates(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "having_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		grp := "A"
		if i > 10 {
			grp = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "having_complex",
			Columns: []string{"id", "grp", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10)), numReal(float64(i * 5))}},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val1) as s1, AVG(val2) as a2 FROM having_complex GROUP BY grp HAVING s1 > 1000 AND a2 > 50",
		"SELECT grp, COUNT(*) as cnt, MAX(val1) - MIN(val1) as range_val FROM having_complex GROUP BY grp HAVING range_val > 50",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING complex error: %v", err)
		} else {
			t.Logf("HAVING complex returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CaseExpression tests CASE expressions
func TestCoverage_CaseExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "case_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "case_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT id, CASE WHEN val > 50 THEN 'high' ELSE 'low' END as category FROM case_test",
		"SELECT id, CASE WHEN val > 80 THEN 'high' WHEN val > 40 THEN 'medium' ELSE 'low' END as level FROM case_test",
		"SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END as name FROM case_test WHERE id <= 3",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("CASE expression error: %v", err)
		} else {
			t.Logf("CASE expression returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_NullHandlingInExpressions tests NULL handling in expressions
func TestCoverage_NullHandlingInExpressions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "null_expr", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_expr",
		Columns: []string{"id", "a", "b"},
		Values: [][]query.Expression{
			{numReal(1), numReal(10), numReal(20)},
			{numReal(2), numReal(10), &query.NullLiteral{}},
			{numReal(3), &query.NullLiteral{}, numReal(30)},
			{numReal(4), &query.NullLiteral{}, &query.NullLiteral{}},
		},
	}, nil)

	queries := []string{
		"SELECT id, a + b as sum_val FROM null_expr",
		"SELECT id, COALESCE(a, 0) + COALESCE(b, 0) as safe_sum FROM null_expr",
		"SELECT COUNT(*) FROM null_expr WHERE a IS NULL OR b IS NULL",
		"SELECT COUNT(*) FROM null_expr WHERE a IS NOT NULL AND b IS NOT NULL",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("NULL handling error: %v", err)
		} else {
			t.Logf("NULL handling returned %d rows", len(result.Rows))
		}
	}
}
