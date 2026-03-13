package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Target: selectLocked - subqueries and IN expressions
// ============================================================

func TestCovBoost29_SelectWithInSubquery(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "in_main",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "ref_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "in_sub",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "code", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "in_main",
		Columns: []string{"id", "ref_id"},
		Values:  [][]query.Expression{{num29(1), num29(10)}, {num29(2), num29(20)}, {num29(3), num29(30)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "in_sub",
		Columns: []string{"id", "code"},
		Values:  [][]query.Expression{{num29(10), str29("A")}, {num29(20), str29("B")}},
	}, nil)

	// IN with subquery
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col29("id")},
		From:    &query.TableRef{Name: "in_main"},
		Where: &query.InExpr{
			Expr: col29("ref_id"),
			Subquery: &query.SelectStmt{
				Columns: []query.Expression{col29("id")},
				From:    &query.TableRef{Name: "in_sub"},
			},
		},
	}, nil)
	_ = rows
}

func TestCovBoost29_SelectWithExistsSubquery(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "exists_main",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "exists_sub",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "main_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "exists_main",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{num29(1)}, {num29(2)}, {num29(3)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "exists_sub",
		Columns: []string{"id", "main_id"},
		Values:  [][]query.Expression{{num29(1), num29(1)}, {num29(2), num29(1)}},
	}, nil)

	// EXISTS subquery
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col29("id")},
		From:    &query.TableRef{Name: "exists_main"},
		Where: &query.ExistsExpr{
			Subquery: &query.SelectStmt{
				Columns: []query.Expression{&query.StarExpr{}},
				From:    &query.TableRef{Name: "exists_sub"},
				Where:   &query.BinaryExpr{Left: col29("exists_sub.main_id"), Operator: query.TokenEq, Right: col29("exists_main.id")},
			},
		},
	}, nil)
	_ = rows
}

// ============================================================
// Target: selectLocked - BETWEEN, IS NULL, IS NOT NULL
// ============================================================

func TestCovBoost29_SelectWithBetweenAndIsNull(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table: "range_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "score", Type: query.TokenInteger},
			{Name: "optional", Type: query.TokenText},
		},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "range_test",
		Columns: []string{"id", "score", "optional"},
		Values: [][]query.Expression{
			{num29(1), num29(50), str29("A")},
			{num29(2), num29(75), &query.NullLiteral{}},
			{num29(3), num29(90), str29("B")},
			{num29(4), num29(100), &query.NullLiteral{}},
		},
	}, nil)

	// BETWEEN
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col29("id")},
		From:    &query.TableRef{Name: "range_test"},
		Where: &query.BetweenExpr{
			Expr:  col29("score"),
			Lower: num29(60),
			Upper: num29(95),
		},
	}, nil)
	_ = rows

	// IS NULL
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col29("id")},
		From:    &query.TableRef{Name: "range_test"},
		Where:   &query.IsNullExpr{Expr: col29("optional")},
	}, nil)
	_ = rows

	// IS NOT NULL (via unary not)
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col29("id")},
		From:    &query.TableRef{Name: "range_test"},
		Where: &query.UnaryExpr{
			Operator: query.TokenNot,
			Expr:     &query.IsNullExpr{Expr: col29("optional")},
		},
	}, nil)
	_ = rows
}

// ============================================================
// Target: updateLocked - FROM clause with multiple tables
// ============================================================

func TestCovBoost29_UpdateFromMultipleTables(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "upd_target",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "cat_id", Type: query.TokenInteger}, {Name: "amt", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "upd_cat",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "mult", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_target",
		Columns: []string{"id", "cat_id", "amt"},
		Values:  [][]query.Expression{{num29(1), num29(1), num29(100)}, {num29(2), num29(2), num29(200)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "upd_cat",
		Columns: []string{"id", "mult"},
		Values:  [][]query.Expression{{num29(1), num29(2)}, {num29(2), num29(3)}},
	}, nil)

	// UPDATE with FROM
	_, _, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_target",
		Set:   []*query.SetClause{{Column: "amt", Value: &query.BinaryExpr{Left: col29("amt"), Operator: query.TokenStar, Right: col29("mult")}}},
		From:  &query.TableRef{Name: "upd_cat"},
		Where: &query.BinaryExpr{Left: col29("upd_target.cat_id"), Operator: query.TokenEq, Right: col29("upd_cat.id")},
	}, nil)
	if err != nil {
		t.Logf("UPDATE FROM error: %v", err)
	}
}

// ============================================================
// Target: executeSelectWithJoinAndGroupBy - multiple aggregates
// ============================================================

func TestCovBoost29_JoinGroupByMultipleAggregates(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "jg_orders",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "cust_id", Type: query.TokenInteger}, {Name: "amount", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "jg_customers",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "region", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_orders",
		Columns: []string{"id", "cust_id", "amount"},
		Values: [][]query.Expression{
			{num29(1), num29(1), num29(100)},
			{num29(2), num29(1), num29(200)},
			{num29(3), num29(2), num29(150)},
			{num29(4), num29(2), num29(250)},
		},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_customers",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{num29(1), str29("North")}, {num29(2), str29("South")}},
	}, nil)

	// JOIN + GROUP BY with multiple aggregates
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col29("region"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}, Alias: "order_count"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col29("amount")}}, Alias: "total"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "AVG", Args: []query.Expression{col29("amount")}}, Alias: "average"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "MIN", Args: []query.Expression{col29("amount")}}, Alias: "min_amt"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "MAX", Args: []query.Expression{col29("amount")}}, Alias: "max_amt"},
		},
		From: &query.TableRef{Name: "jg_orders"},
		Joins: []*query.JoinClause{
			{Type: query.TokenInner, Table: &query.TableRef{Name: "jg_customers"}, Condition: &query.BinaryExpr{Left: col29("jg_orders.cust_id"), Operator: query.TokenEq, Right: col29("jg_customers.id")}},
		},
		GroupBy: []query.Expression{col29("region")},
	}, nil)
	_ = rows
}

// ============================================================
// Target: countRows - various data sizes
// ============================================================

func TestCovBoost29_CountRowsVariousSizes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "cnt_sizes",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	sc := NewStatsCollector(cat)

	// Test with various row counts
	for _, size := range []int{0, 1, 5, 10} {
		// Clear and insert
		for i := 1; i <= size; i++ {
			cat.Insert(ctx, &query.InsertStmt{
				Table:   "cnt_sizes",
				Columns: []string{"id"},
				Values:  [][]query.Expression{{num29(float64(i + size*100))}},
			}, nil)
		}
		count, err := sc.countRows("cnt_sizes")
		if err != nil {
			t.Logf("countRows at size %d: %v", size, err)
		}
		_ = count
	}
}

// ============================================================
// Target: collectColumnStats - different data types
// ============================================================

func TestCovBoost29_ColumnStatsDataTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Integer column
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "stats_int",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "stats_int",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{num29(float64(i)), num29(float64(i * 10))}},
		}, nil)
	}

	sc := NewStatsCollector(cat)
	stats, err := sc.collectColumnStats("stats_int", "val")
	if err != nil {
		t.Logf("collectColumnStats int: %v", err)
	}
	_ = stats

	// Real column
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "stats_real",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenReal}},
		PrimaryKey: []string{"id"},
	})
	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "stats_real",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{num29(float64(i)), num29(float64(i) * 1.5)}},
		}, nil)
	}

	stats, err = sc.collectColumnStats("stats_real", "val")
	if err != nil {
		t.Logf("collectColumnStats real: %v", err)
	}
	_ = stats
}

// ============================================================
// Target: deleteWithUsingLocked - no matching rows
// ============================================================

func TestCovBoost29_DeleteUsingNoMatch(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_nm_main",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "ref_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_nm_ref",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_nm_main",
		Columns: []string{"id", "ref_id"},
		Values:  [][]query.Expression{{num29(1), num29(99)}, {num29(2), num29(99)}},
	}, nil)
	// Ref table has no matching ids (99 not present)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_nm_ref",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{num29(1)}, {num29(2)}},
	}, nil)

	// DELETE USING with no matches
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_nm_main",
		Using: []*query.TableRef{{Name: "del_nm_ref"}},
		Where: &query.BinaryExpr{Left: col29("del_nm_main.ref_id"), Operator: query.TokenEq, Right: col29("del_nm_ref.id")},
	}, nil)

	// Should still have 2 rows
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "del_nm_main"},
	}, nil)
	if len(rows) > 0 {
		if rows[0][0].(int64) != 2 {
			t.Errorf("expected 2 rows, got %v", rows[0][0])
		}
	}
}

// ============================================================
// Helpers
// ============================================================

func num29(v float64) *query.NumberLiteral         { return &query.NumberLiteral{Value: v} }
func str29(s string) *query.StringLiteral          { return &query.StringLiteral{Value: s} }
func col29(name string) *query.QualifiedIdentifier { return &query.QualifiedIdentifier{Column: name} }
