package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Tests for evaluateCastExpr, evaluateBetween, evaluateLike, etc.
// ============================================================

// TestEvaluateCastExpr_MoreCases - covers additional CAST scenarios
func TestEvaluateCastExpr_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "cast_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "str_val", Type: query.TokenText},
		{Name: "real_val", Type: query.TokenReal},
		{Name: "int_val", Type: query.TokenInteger},
	})

	// Insert data with various types
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "str_val", "real_val", "int_val"},
		Values:  [][]query.Expression{{numReal(1), strReal("123"), numReal(45.67), numReal(100)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "str_val", "real_val", "int_val"},
		Values:  [][]query.Expression{{numReal(2), strReal("3.14159"), numReal(2.5), numReal(50)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "str_val", "real_val", "int_val"},
		Values:  [][]query.Expression{{numReal(3), strReal("true"), numReal(1.0), numReal(1)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "str_val", "real_val", "int_val"},
		Values:  [][]query.Expression{{numReal(4), strReal("false"), numReal(0.0), numReal(0)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "str_val", "real_val", "int_val"},
		Values:  [][]query.Expression{{numReal(5), &query.NullLiteral{}, numReal(0.0), numReal(0)}},
	}, nil)

	// Test CAST to INTEGER from various types
	queries := []string{
		`SELECT CAST(str_val AS INTEGER) FROM cast_test WHERE id = 1`,
		`SELECT CAST(real_val AS INTEGER) FROM cast_test WHERE id = 2`,
		`SELECT CAST(int_val AS INTEGER) FROM cast_test WHERE id = 3`,
		`SELECT CAST(str_val AS REAL) FROM cast_test WHERE id = 1`,
		`SELECT CAST(int_val AS REAL) FROM cast_test WHERE id = 2`,
		`SELECT CAST(int_val AS TEXT) FROM cast_test WHERE id = 3`,
		`SELECT CAST(str_val AS BOOLEAN) FROM cast_test WHERE id = 3`,
		`SELECT CAST(str_val AS BOOLEAN) FROM cast_test WHERE id = 4`,
		`SELECT CAST(int_val AS BOOLEAN) FROM cast_test WHERE id = 3`,
		`SELECT CAST(int_val AS BOOLEAN) FROM cast_test WHERE id = 5`,
		`SELECT CAST(real_val AS BOOLEAN) FROM cast_test WHERE id = 3`,
		`CAST(NULL AS INTEGER)`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d result: %v", i, result.Rows)
		}
	}
}

// TestEvaluateBetween_MoreCases - covers BETWEEN edge cases
func TestEvaluateBetween_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "between_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "between_test",
			Columns: []string{"id", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("name")}},
		}, nil)
	}

	// Test BETWEEN with various ranges
	queries := []string{
		`SELECT * FROM between_test WHERE val BETWEEN 20 AND 50`,
		`SELECT * FROM between_test WHERE val NOT BETWEEN 30 AND 60`,
		`SELECT * FROM between_test WHERE name BETWEEN 'a' AND 'z'`,
		`SELECT * FROM between_test WHERE id BETWEEN 3 AND 7`,
		`SELECT * FROM between_test WHERE val BETWEEN NULL AND 50`,
		`SELECT * FROM between_test WHERE val BETWEEN 20 AND NULL`,
		`SELECT * FROM between_test WHERE NULL BETWEEN 20 AND 50`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestEvaluateLike_MoreCases - covers LIKE with escape and patterns
func TestEvaluateLike_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "like_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "pattern", Type: query.TokenText},
	})

	patterns := []string{
		"hello",
		"world",
		"test_123",
		"foo%bar",
		"escape\\_test",
		"a_b_c",
	}
	for i, p := range patterns {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "like_test",
			Columns: []string{"id", "pattern"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(p)}},
		}, nil)
	}

	// Test various LIKE patterns
	queries := []string{
		`SELECT * FROM like_test WHERE pattern LIKE 'hello'`,
		`SELECT * FROM like_test WHERE pattern LIKE 'test_'`,
		`SELECT * FROM like_test WHERE pattern LIKE 'foo%'`,
		`SELECT * FROM like_test WHERE pattern LIKE '%bar'`,
		`SELECT * FROM like_test WHERE pattern LIKE '%world%'`,
		`SELECT * FROM like_test WHERE pattern LIKE 'a_b_c'`,
		`SELECT * FROM like_test WHERE pattern LIKE '%\_%' ESCAPE '\'`,
		`SELECT * FROM like_test WHERE pattern LIKE 'escape\_test' ESCAPE '\'`,
		`SELECT * FROM like_test WHERE pattern LIKE NULL`,
		`SELECT * FROM like_test WHERE pattern LIKE 'test' ESCAPE NULL`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestSubtractMultiplyDivideModulo_MoreCases - arithmetic operations
func TestSubtractMultiplyDivideModulo_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "arith_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "x", Type: query.TokenReal},
		{Name: "y", Type: query.TokenReal},
	})

	// Insert test data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "arith_test",
		Columns: []string{"id", "a", "b", "x", "y"},
		Values:  [][]query.Expression{{numReal(1), numReal(10), numReal(3), numReal(10.5), numReal(2.5)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "arith_test",
		Columns: []string{"id", "a", "b", "x", "y"},
		Values:  [][]query.Expression{{numReal(2), numReal(100), numReal(0), numReal(20.0), numReal(0.0)}},
	}, nil)

	// Test arithmetic operations
	queries := []string{
		`SELECT a - b FROM arith_test WHERE id = 1`,
		`SELECT a * b FROM arith_test WHERE id = 1`,
		`SELECT a / b FROM arith_test WHERE id = 1`,
		`SELECT a % b FROM arith_test WHERE id = 1`,
		`SELECT x - y FROM arith_test WHERE id = 1`,
		`SELECT x * y FROM arith_test WHERE id = 1`,
		`SELECT x / y FROM arith_test WHERE id = 1`,
		`SELECT x / y FROM arith_test WHERE id = 2`,
		`SELECT a % b FROM arith_test WHERE id = 1`,
		`SELECT a % b FROM arith_test WHERE id = 2`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d result: %v", i, result.Rows)
		}
	}
}

// TestEvaluateIsNull_MoreCases - IS NULL and IS NOT NULL
func TestEvaluateIsNull_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "null_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert with and without NULL
	for i := 1; i <= 10; i++ {
		var val query.Expression
		if i%3 == 0 {
			val = &query.NullLiteral{}
		} else {
			val = numReal(float64(i))
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "null_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), val}},
		}, nil)
	}

	queries := []string{
		`SELECT * FROM null_test WHERE val IS NULL`,
		`SELECT * FROM null_test WHERE val IS NOT NULL`,
		`SELECT * FROM null_test WHERE NULL IS NULL`,
		`SELECT * FROM null_test WHERE NULL IS NOT NULL`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestEvaluateIn_MoreCases - IN expression with lists and subqueries
func TestEvaluateIn_MoreCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "in_test_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})
	createCoverageTestTable(t, cat, "in_test_ref", []*query.ColumnDef{
		{Name: "ref_id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		category := "A"
		if i > 3 {
			category = "B"
		}
		if i > 7 {
			category = "C"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "in_test_main",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(category)}},
		}, nil)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "in_test_ref",
			Columns: []string{"ref_id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("code" + string(rune('A'+i%3)))}},
		}, nil)
	}

	queries := []string{
		`SELECT * FROM in_test_main WHERE id IN (1, 3, 5, 7)`,
		`SELECT * FROM in_test_main WHERE id NOT IN (2, 4, 6)`,
		`SELECT * FROM in_test_main WHERE category IN ('A', 'B')`,
		`SELECT * FROM in_test_main WHERE id IN (SELECT ref_id FROM in_test_ref WHERE code = 'codeA')`,
		`SELECT * FROM in_test_main WHERE id IN (NULL, 1, 2)`,
		`SELECT * FROM in_test_main WHERE category IN (NULL)`,
		`SELECT * FROM in_test_main WHERE id IN ()`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestResolveAggregateInExpr_ExtraCases - additional aggregate resolution
func TestResolveAggregateInExpr_ExtraCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	cat := New(tree, pool, nil)
	ctx := context.Background()

	createCoverageTestTable(t, cat, "agg_expr_test", []*query.ColumnDef{
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
			Table:   "agg_expr_test",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		`SELECT grp, COUNT(*), SUM(val), AVG(val) FROM agg_expr_test GROUP BY grp`,
		`SELECT grp, MIN(val), MAX(val) FROM agg_expr_test GROUP BY grp`,
		`SELECT COUNT(DISTINCT grp) FROM agg_expr_test`,
		`SELECT SUM(val) + AVG(val) FROM agg_expr_test`,
		`SELECT grp, COUNT(*) * 10 FROM agg_expr_test GROUP BY grp`,
	}

	for i, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query %d error: %v", i, err)
		} else {
			t.Logf("Query %d returned %d rows", i, len(result.Rows))
		}
	}
}
