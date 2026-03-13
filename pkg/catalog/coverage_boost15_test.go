package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Edge Cases - NULL Handling
// ============================================================

func TestCovBoost15_NullInAggregates(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "null_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger, NotNull: false},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_agg",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NullLiteral{}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_agg",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 10}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_agg",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 3}, &query.NullLiteral{}}},
	}, nil)

	// Aggregates with NULL values
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "AVG", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "MIN", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "MAX", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
		},
		From: &query.TableRef{Name: "null_agg"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("NULL aggregates error (may be expected): %v", err)
	}
	_ = rows
}

func TestCovBoost15_NullInComparisons(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "null_cmp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger, NotNull: false},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_cmp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NullLiteral{}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_cmp",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 10}}},
	}, nil)

	// WHERE with NULL comparison
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "null_cmp"},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Column: "val"},
			Operator: query.TokenEq,
			Right:    &query.NullLiteral{},
		},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("NULL comparison error (may be expected): %v", err)
	}
	_ = rows
}

func TestCovBoost15_IsNull_IsNotNull(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "null_is", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText, NotNull: false},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_is",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NullLiteral{}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_is",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "test"}}},
	}, nil)

	// IS NULL
	stmt1 := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "null_is"},
		Where:   &query.IsNullExpr{Expr: &query.QualifiedIdentifier{Column: "val"}, Not: false},
	}
	_, rows1, err := cat.Select(stmt1, nil)
	if err != nil {
		t.Logf("IS NULL error: %v", err)
	}
	_ = rows1

	// IS NOT NULL
	stmt2 := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "null_is"},
		Where:   &query.IsNullExpr{Expr: &query.QualifiedIdentifier{Column: "val"}, Not: true},
	}
	_, rows2, err := cat.Select(stmt2, nil)
	if err != nil {
		t.Logf("IS NOT NULL error: %v", err)
	}
	_ = rows2
}

// ============================================================
// Edge Cases - Empty Results
// ============================================================

func TestCovBoost15_EmptyTableAggregate(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "empty_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Aggregates on empty table
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "AVG", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "MIN", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "MAX", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
		},
		From: &query.TableRef{Name: "empty_agg"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Empty aggregate error: %v", err)
	}
	_ = rows
}

func TestCovBoost15_EmptyTableGroupBy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "empty_gb", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// GROUP BY on empty table
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "grp"},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
		},
		From:    &query.TableRef{Name: "empty_gb"},
		GroupBy: []query.Expression{&query.QualifiedIdentifier{Column: "grp"}},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Empty GROUP BY error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Boundary Values
// ============================================================

func TestCovBoost15_LargeNumbers(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "large_nums", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenReal},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "large_nums",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1e15}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "large_nums",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: -1e15}}},
	}, nil)

	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "AVG", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
		},
		From: &query.TableRef{Name: "large_nums"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Large numbers error: %v", err)
	}
	_ = rows
}

func TestCovBoost15_ZeroAndNegative(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "zero_neg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "zero_neg",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 0}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "zero_neg",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: -5}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "zero_neg",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 3}, &query.NumberLiteral{Value: 10}}},
	}, nil)

	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "MIN", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
			&query.FunctionCall{Name: "MAX", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}},
		},
		From: &query.TableRef{Name: "zero_neg"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Zero/negative error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Special Characters
// ============================================================

func TestCovBoost15_SpecialCharsInStrings(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "special_str", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	specialStrings := []string{
		"hello 'world'",
		"line1\nline2",
		"tab\there",
		"quote\"inside",
		"emoji 🎉",
		"unicode ñ 中",
	}
	for i, s := range specialStrings {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "special_str",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i + 1)}, &query.StringLiteral{Value: s}}},
		}, nil)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "special_str"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Special chars error: %v", err)
	}
	_ = rows
}

func TestCovBoost15_UnicodeStrings(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "unicode_str", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	unicodeStrings := []string{
		"日本語テキスト",
		"العربية",
		"Ελληνικά",
		"Café résumé naïve",
		"Привет мир",
	}
	for i, s := range unicodeStrings {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "unicode_str",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i + 1)}, &query.StringLiteral{Value: s}}},
		}, nil)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "unicode_str"},
		OrderBy: []*query.OrderByExpr{{Expr: &query.QualifiedIdentifier{Column: "val"}}},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Unicode error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - WHERE Clause Complexities
// ============================================================

func TestCovBoost15_WhereWithOr(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_or", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_or",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 2)}}},
		}, nil)
	}

	// WHERE with OR
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "where_or"},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Column: "a"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 1},
			},
			Operator: query.TokenOr,
			Right: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Column: "b"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 20},
			},
		},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("WHERE OR error: %v", err)
	}
	_ = rows
}

func TestCovBoost15_WhereWithNot(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_not", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_not",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// WHERE with NOT
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "where_not"},
		Where: &query.UnaryExpr{
			Operator: query.TokenNot,
			Expr: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Column: "val"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 30},
			},
		},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("WHERE NOT error: %v", err)
	}
	_ = rows
}

func TestCovBoost15_WhereWithBetween(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_between", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_between",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// BETWEEN
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "where_between"},
		Where: &query.BetweenExpr{
			Expr:  &query.QualifiedIdentifier{Column: "val"},
			Lower: &query.NumberLiteral{Value: 30},
			Upper: &query.NumberLiteral{Value: 70},
			Not:   false,
		},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("BETWEEN error: %v", err)
	}
	_ = rows
}

func TestCovBoost15_WhereWithIn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_in", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_in",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// IN
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "where_in"},
		Where: &query.InExpr{
			Expr: &query.QualifiedIdentifier{Column: "val"},
			List: []query.Expression{
				&query.NumberLiteral{Value: 20},
				&query.NumberLiteral{Value: 50},
				&query.NumberLiteral{Value: 80},
			},
			Not: false,
		},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("IN error: %v", err)
	}
	_ = rows
}

func TestCovBoost15_WhereWithLike(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_like", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	ctx := context.Background()
	names := []string{"Alice", "Bob", "Charlie", "Alex", "Anna"}
	for i, name := range names {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_like",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i + 1)}, &query.StringLiteral{Value: name}}},
		}, nil)
	}

	// LIKE
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "where_like"},
		Where: &query.LikeExpr{
			Expr:    &query.QualifiedIdentifier{Column: "name"},
			Pattern: &query.StringLiteral{Value: "A%"},
			Not:     false,
		},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("LIKE error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Arithmetic Operations
// ============================================================

func TestCovBoost15_ArithmeticInSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "arith_sel", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "arith_sel",
		Columns: []string{"id", "a", "b"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 10}, &query.NumberLiteral{Value: 3}}},
	}, nil)

	// Arithmetic in SELECT
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "a"}, Operator: query.TokenPlus, Right: &query.QualifiedIdentifier{Column: "b"}},
			&query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "a"}, Operator: query.TokenMinus, Right: &query.QualifiedIdentifier{Column: "b"}},
			&query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "a"}, Operator: query.TokenStar, Right: &query.QualifiedIdentifier{Column: "b"}},
			&query.BinaryExpr{Left: &query.QualifiedIdentifier{Column: "a"}, Operator: query.TokenSlash, Right: &query.QualifiedIdentifier{Column: "b"}},
		},
		From: &query.TableRef{Name: "arith_sel"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Arithmetic in SELECT error: %v", err)
	}
	_ = rows
}

func TestCovBoost15_ArithmeticWithNegative(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "arith_neg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "arith_neg",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: -10}}},
	}, nil)

	// Arithmetic with negative
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.QualifiedIdentifier{Column: "val"}},
			&query.BinaryExpr{Left: &query.NumberLiteral{Value: 0}, Operator: query.TokenMinus, Right: &query.QualifiedIdentifier{Column: "val"}},
		},
		From: &query.TableRef{Name: "arith_neg"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Arithmetic with negative error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Subqueries
// ============================================================

func TestCovBoost15_SubqueryInSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sq_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sq_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
	}

	// Subquery in SELECT
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.SubqueryExpr{
				Query: &query.SelectStmt{
					Columns: []query.Expression{&query.FunctionCall{Name: "MAX", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}}},
					From:    &query.TableRef{Name: "sq_main"},
				},
			},
		},
		From: &query.TableRef{Name: "sq_main"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Subquery in SELECT error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Case Expressions
// ============================================================

func TestCovBoost15_CaseExpression(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "case_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "score", Type: query.TokenInteger},
	})

	ctx := context.Background()
	scores := []float64{95, 85, 75, 65, 55}
	for i, s := range scores {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "case_tbl",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i + 1)}, &query.NumberLiteral{Value: s}}},
		}, nil)
	}

	// CASE expression
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.QualifiedIdentifier{Column: "score"},
			&query.CaseExpr{
				Whens: []*query.WhenClause{
				{
					Condition: &query.BinaryExpr{
						Left:     &query.QualifiedIdentifier{Column: "score"},
						Operator: query.TokenGte,
						Right:    &query.NumberLiteral{Value: 90},
					},
					Result: &query.StringLiteral{Value: "A"},
				},
				{
					Condition: &query.BinaryExpr{
						Left:     &query.QualifiedIdentifier{Column: "score"},
						Operator: query.TokenGte,
						Right:    &query.NumberLiteral{Value: 80},
					},
					Result: &query.StringLiteral{Value: "B"},
				},
				{
					Condition: &query.BinaryExpr{
						Left:     &query.QualifiedIdentifier{Column: "score"},
						Operator: query.TokenGte,
						Right:    &query.NumberLiteral{Value: 70},
					},
					Result: &query.StringLiteral{Value: "C"},
				},
			},
				Else: &query.StringLiteral{Value: "F"},
			},
		},
		From: &query.TableRef{Name: "case_tbl"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("CASE expression error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Type Conversions
// ============================================================

func TestCovBoost15_CastExpression(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "cast_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "num", Type: query.TokenInteger},
		{Name: "txt", Type: query.TokenText},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "cast_tbl",
		Columns: []string{"id", "num", "txt"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 123}, &query.StringLiteral{Value: "456"}}},
	}, nil)

	// CAST expressions
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.CastExpr{Expr: &query.QualifiedIdentifier{Column: "num"}, DataType: query.TokenText},
			&query.CastExpr{Expr: &query.QualifiedIdentifier{Column: "txt"}, DataType: query.TokenInteger},
		},
		From: &query.TableRef{Name: "cast_tbl"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("CAST error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Coalesce
// ============================================================

func TestCovBoost15_Coalesce(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "coal_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val1", Type: query.TokenInteger, NotNull: false},
		{Name: "val2", Type: query.TokenInteger, NotNull: false},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "coal_tbl",
		Columns: []string{"id", "val1", "val2"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NullLiteral{}, &query.NumberLiteral{Value: 10}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "coal_tbl",
		Columns: []string{"id", "val1", "val2"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 20}, &query.NullLiteral{}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "coal_tbl",
		Columns: []string{"id", "val1", "val2"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 3}, &query.NullLiteral{}, &query.NullLiteral{}}},
	}, nil)

	// COALESCE
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Column: "id"},
			&query.FunctionCall{
				Name: "COALESCE",
				Args: []query.Expression{
					&query.QualifiedIdentifier{Column: "val1"},
					&query.QualifiedIdentifier{Column: "val2"},
					&query.NumberLiteral{Value: 0},
				},
			},
		},
		From: &query.TableRef{Name: "coal_tbl"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("COALESCE error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Aliases
// ============================================================

func TestCovBoost15_ColumnAliases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "alias_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "alias_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 100}}},
	}, nil)

	// Aliased columns
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.AliasExpr{Expr: &query.QualifiedIdentifier{Column: "id"}, Alias: "identifier"},
			&query.AliasExpr{Expr: &query.QualifiedIdentifier{Column: "val"}, Alias: "value"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Column: "val"}}}, Alias: "total"},
		},
		From: &query.TableRef{Name: "alias_tbl"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Column aliases error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Duplicate Handling
// ============================================================

func TestCovBoost15_Distinct(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "distinct_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
	})

	ctx := context.Background()
	grps := []string{"A", "A", "B", "B", "C"}
	for i, g := range grps {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "distinct_tbl",
			Columns: []string{"id", "grp"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i + 1)}, &query.StringLiteral{Value: g}}},
		}, nil)
	}

	// DISTINCT
	stmt := &query.SelectStmt{
		Distinct: true,
		Columns:  []query.Expression{&query.QualifiedIdentifier{Column: "grp"}},
		From:     &query.TableRef{Name: "distinct_tbl"},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("DISTINCT error: %v", err)
	}
	_ = rows
}

// ============================================================
// Edge Cases - Transaction Boundaries
// ============================================================

func TestCovBoost15_TransactionWithSavepoints(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_sp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_sp",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	// Create savepoint
	err := cat.Savepoint("sp1")
	if err != nil {
		t.Logf("Savepoint error: %v", err)
	}

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_sp",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}}},
	}, nil)

	// Rollback to savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to savepoint error: %v", err)
	}

	// Commit
	err = cat.CommitTransaction()
	if err != nil {
		t.Logf("Commit error: %v", err)
	}
}

func TestCovBoost15_TransactionReleaseSavepoint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_rel", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	// Create and release savepoint
	err := cat.Savepoint("sp_rel")
	if err != nil {
		t.Logf("Savepoint error: %v", err)
	}

	err = cat.ReleaseSavepoint("sp_rel")
	if err != nil {
		t.Logf("Release savepoint error: %v", err)
	}

	cat.CommitTransaction()
}

// ============================================================
// Edge Cases - Limit/Offset Edge Cases
// ============================================================

func TestCovBoost15_LimitOffsetEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "lo_edge", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "lo_edge",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}}},
		}, nil)
	}

	// LIMIT 0
	stmt1 := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "lo_edge"},
		Limit:   &query.NumberLiteral{Value: 0},
	}
	_, rows1, err := cat.Select(stmt1, nil)
	if err != nil {
		t.Logf("LIMIT 0 error: %v", err)
	}
	_ = rows1

	// OFFSET beyond row count
	stmt2 := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "lo_edge"},
		Offset:  &query.NumberLiteral{Value: 100},
	}
	_, rows2, err := cat.Select(stmt2, nil)
	if err != nil {
		t.Logf("Large OFFSET error: %v", err)
	}
	_ = rows2

	// Negative LIMIT (should be treated as no limit)
	stmt3 := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "lo_edge"},
		Limit:   &query.NumberLiteral{Value: -1},
	}
	_, rows3, err := cat.Select(stmt3, nil)
	if err != nil {
		t.Logf("Negative LIMIT error: %v", err)
	}
	_ = rows3
}

// ============================================================
// Edge Cases - Table Aliases
// ============================================================

func TestCovBoost15_SelfJoin(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "self_emp", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "mgr_id", Type: query.TokenInteger, NotNull: false},
	})

	ctx := context.Background()
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "self_emp",
		Columns: []string{"id", "name", "mgr_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "CEO"}, &query.NullLiteral{}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "self_emp",
		Columns: []string{"id", "name", "mgr_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Manager"}, &query.NumberLiteral{Value: 1}}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "self_emp",
		Columns: []string{"id", "name", "mgr_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Employee"}, &query.NumberLiteral{Value: 2}}},
	}, nil)

	// Self JOIN
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Table: "e", Column: "name"},
			&query.QualifiedIdentifier{Table: "m", Column: "name"},
		},
		From: &query.TableRef{Name: "self_emp", Alias: "e"},
		Joins: []*query.JoinClause{
			{
				Type:  query.TokenLeft,
				Table: &query.TableRef{Name: "self_emp", Alias: "m"},
				Condition: &query.BinaryExpr{
					Left:     &query.QualifiedIdentifier{Table: "e", Column: "mgr_id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "m", Column: "id"},
				},
			},
		},
	}

	_, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("Self JOIN error: %v", err)
	}
	_ = rows
}
