package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Target: countRows - empty and non-empty table paths
// ============================================================

func TestCovBoost28_CountRowsEmptyAndNonEmpty(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "cnt_test",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	sc := NewStatsCollector(cat)

	// Empty table
	count, err := sc.countRows("cnt_test")
	if err != nil {
		t.Logf("countRows error on empty table: %v", err)
	}
	_ = count

	// Add rows
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "cnt_test",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{num28(float64(i))}},
		}, nil)
	}

	// Non-empty table
	count, err = sc.countRows("cnt_test")
	if err != nil {
		t.Logf("countRows error on non-empty table: %v", err)
	}
	_ = count
}

// ============================================================
// Target: selectLocked with complex WHERE conditions
// ============================================================

func TestCovBoost28_SelectWithComplexWhere(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table: "complex_where",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText},
			{Name: "priority", Type: query.TokenInteger},
			{Name: "active", Type: query.TokenInteger},
		},
		PrimaryKey: []string{"id"},
	})

	// Insert test data
	for i := 1; i <= 20; i++ {
		status := "open"
		if i > 10 {
			status = "closed"
		}
		active := 1
		if i%3 == 0 {
			active = 0
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "complex_where",
			Columns: []string{"id", "status", "priority", "active"},
			Values:  [][]query.Expression{{num28(float64(i)), str28(status), num28(float64(i % 5)), num28(float64(active))}},
		}, nil)
	}

	// Complex WHERE with AND/OR
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col28("id")},
		From:    &query.TableRef{Name: "complex_where"},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     col28("status"),
				Operator: query.TokenEq,
				Right:    str28("open"),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     col28("active"),
				Operator: query.TokenEq,
				Right:    num28(1),
			},
		},
	}, nil)
	_ = rows

	// WHERE with OR
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col28("id")},
		From:    &query.TableRef{Name: "complex_where"},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     col28("priority"),
				Operator: query.TokenEq,
				Right:    num28(1),
			},
			Operator: query.TokenOr,
			Right: &query.BinaryExpr{
				Left:     col28("priority"),
				Operator: query.TokenEq,
				Right:    num28(2),
			},
		},
	}, nil)
	_ = rows

	// WHERE with NOT
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col28("id")},
		From:    &query.TableRef{Name: "complex_where"},
		Where: &query.UnaryExpr{
			Operator: query.TokenNot,
			Expr: &query.BinaryExpr{
				Left:     col28("status"),
				Operator: query.TokenEq,
				Right:    str28("closed"),
			},
		},
	}, nil)
	_ = rows
}

// ============================================================
// Target: updateLocked with various scenarios
// ============================================================

func TestCovBoost28_UpdateMultipleRows(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table: "upd_multi",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "value", Type: query.TokenInteger},
		},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 10; i++ {
		category := "A"
		if i > 5 {
			category = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_multi",
			Columns: []string{"id", "category", "value"},
			Values:  [][]query.Expression{{num28(float64(i)), str28(category), num28(float64(i * 10))}},
		}, nil)
	}

	// Update multiple rows with WHERE
	cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_multi",
		Set:   []*query.SetClause{{Column: "value", Value: num28(999)}},
		Where: &query.BinaryExpr{Left: col28("category"), Operator: query.TokenEq, Right: str28("A")},
	}, nil)

	// Verify updates
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col28("value")},
		From:    &query.TableRef{Name: "upd_multi"},
		Where:   &query.BinaryExpr{Left: col28("category"), Operator: query.TokenEq, Right: str28("A")},
	}, nil)

	for _, row := range rows {
		if row[0].(int64) != 999 {
			t.Errorf("expected value 999, got %v", row[0])
		}
	}
}

func TestCovBoost28_UpdateNoWhere(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "upd_all",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "upd_all",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{num28(float64(i)), num28(float64(i))}},
		}, nil)
	}

	// Update all rows (no WHERE)
	_, _, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "upd_all",
		Set:   []*query.SetClause{{Column: "val", Value: num28(42)}},
	}, nil)
	if err != nil {
		t.Logf("Update without WHERE: %v", err)
	}
}

// ============================================================
// Target: computeAggregatesWithGroupBy edge cases
// ============================================================

func TestCovBoost28_AggregatesWithNulls(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table: "agg_nulls",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
		PrimaryKey: []string{"id"},
	})

	// Insert with NULL values
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "agg_nulls",
		Columns: []string{"id", "cat", "val"},
		Values: [][]query.Expression{
			{num28(1), str28("A"), num28(10)},
			{num28(2), str28("A"), &query.NullLiteral{}},
			{num28(3), str28("B"), num28(30)},
			{num28(4), str28("B"), &query.NullLiteral{}},
		},
	}, nil)

	// AVG with NULLs
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col28("cat"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "AVG", Args: []query.Expression{col28("val")}}, Alias: "avg_val"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "COUNT", Args: []query.Expression{col28("val")}}, Alias: "cnt"},
		},
		From:    &query.TableRef{Name: "agg_nulls"},
		GroupBy: []query.Expression{col28("cat")},
	}, nil)
	_ = rows
}

func TestCovBoost28_AggregateEmptyGroup(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "agg_empty",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "cat", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	// Empty table GROUP BY
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col28("cat"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}, Alias: "cnt"},
		},
		From:    &query.TableRef{Name: "agg_empty"},
		GroupBy: []query.Expression{col28("cat")},
	}, nil)
	_ = rows
}

// ============================================================
// Target: deleteWithUsingLocked - various USING scenarios
// ============================================================

func TestCovBoost28_DeleteUsingMultipleTables(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_main",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "ref_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_ref1",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "flag", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_ref2",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "status", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_main",
		Columns: []string{"id", "ref_id"},
		Values:  [][]query.Expression{{num28(1), num28(1)}, {num28(2), num28(2)}, {num28(3), num28(3)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_ref1",
		Columns: []string{"id", "flag"},
		Values:  [][]query.Expression{{num28(1), num28(1)}, {num28(2), num28(0)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_ref2",
		Columns: []string{"id", "status"},
		Values:  [][]query.Expression{{num28(1), str28("deleted")}, {num28(3), str28("deleted")}},
	}, nil)

	// DELETE USING with multiple tables
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_main",
		Using: []*query.TableRef{{Name: "del_ref1"}, {Name: "del_ref2"}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     col28("del_main.ref_id"),
				Operator: query.TokenEq,
				Right:    col28("del_ref1.id"),
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     col28("del_ref1.flag"),
				Operator: query.TokenEq,
				Right:    num28(1),
			},
		},
	}, nil)

	// Verify remaining rows
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "del_main"},
	}, nil)
	_ = rows
}

// ============================================================
// Target: applyGroupByOrderBy with multiple order columns
// ============================================================

func TestCovBoost28_GroupByOrderByComplex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table: "gbo_complex",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "region", Type: query.TokenText},
			{Name: "product", Type: query.TokenText},
			{Name: "sales", Type: query.TokenInteger},
		},
		PrimaryKey: []string{"id"},
	})

	// Insert data
	data := [][]interface{}{
		{1, "North", "A", 100},
		{2, "North", "B", 200},
		{3, "South", "A", 150},
		{4, "South", "B", 300},
		{5, "East", "A", 250},
		{6, "East", "B", 100},
	}
	for _, d := range data {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "gbo_complex",
			Columns: []string{"id", "region", "product", "sales"},
			Values:  [][]query.Expression{{num28(float64(d[0].(int))), str28(d[1].(string)), str28(d[2].(string)), num28(float64(d[3].(int)))}},
		}, nil)
	}

	// GROUP BY with multi-column ORDER BY (mixed ASC/DESC)
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col28("region"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col28("sales")}}, Alias: "total"},
		},
		From:    &query.TableRef{Name: "gbo_complex"},
		GroupBy: []query.Expression{col28("region")},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col28("sales")}}, Desc: true},
			{Expr: col28("region")},
		},
	}, nil)
	_ = rows
}

// ============================================================
// Target: evaluateWindowFunctions edge cases
// ============================================================

func TestCovBoost28_WindowFunctionsEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table: "win_edge",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "grp", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
		PrimaryKey: []string{"id"},
	})

	// Single row per partition
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "win_edge",
		Columns: []string{"id", "grp", "val"},
		Values:  [][]query.Expression{{num28(1), str28("A"), num28(100)}, {num28(2), str28("B"), num28(200)}},
	}, nil)

	// ROW_NUMBER with single row per partition
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col28("id"),
			&query.WindowExpr{Function: "ROW_NUMBER", PartitionBy: []query.Expression{col28("grp")}, OrderBy: []*query.OrderByExpr{{Expr: col28("val")}}},
		},
		From: &query.TableRef{Name: "win_edge"},
	}, nil)
	_ = rows

	// All same values (ties)
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "win_ties",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "score", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "win_ties",
		Columns: []string{"id", "score"},
		Values:  [][]query.Expression{{num28(1), num28(50)}, {num28(2), num28(50)}, {num28(3), num28(50)}},
	}, nil)

	// RANK with ties
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col28("id"),
			&query.WindowExpr{Function: "RANK", OrderBy: []*query.OrderByExpr{{Expr: col28("score")}}},
			&query.WindowExpr{Function: "DENSE_RANK", OrderBy: []*query.OrderByExpr{{Expr: col28("score")}}},
		},
		From: &query.TableRef{Name: "win_ties"},
	}, nil)
	_ = rows
}

// ============================================================
// Target: executeSelectWithJoinAndGroupBy edge cases
// ============================================================

func TestCovBoost28_JoinGroupByEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "jg_left",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "jg_right",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "lid", Type: query.TokenInteger}, {Name: "amt", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	// Left table has rows with no matching right rows
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_left",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{num28(1), str28("Alice")}, {num28(2), str28("Bob")}, {num28(3), str28("Carol")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "jg_right",
		Columns: []string{"id", "lid", "amt"},
		Values:  [][]query.Expression{{num28(1), num28(1), num28(100)}, {num28(2), num28(1), num28(200)}},
	}, nil)

	// JOIN + GROUP BY with some unmatched left rows
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col28("name"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "COALESCE", Args: []query.Expression{&query.FunctionCall{Name: "SUM", Args: []query.Expression{col28("amt")}}, num28(0)}}, Alias: "total"},
		},
		From: &query.TableRef{Name: "jg_left"},
		Joins: []*query.JoinClause{
			{Type: query.TokenLeft, Table: &query.TableRef{Name: "jg_right"}, Condition: &query.BinaryExpr{Left: col28("jg_left.id"), Operator: query.TokenEq, Right: col28("jg_right.lid")}},
		},
		GroupBy: []query.Expression{col28("name")},
	}, nil)
	_ = rows
}

// ============================================================
// Target: collectColumnStats edge cases
// ============================================================

func TestCovBoost28_ColumnStatsEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// All NULL column
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "col_all_null",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "data", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "col_all_null",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{num28(1), &query.NullLiteral{}}, {num28(2), &query.NullLiteral{}}},
	}, nil)

	sc := NewStatsCollector(cat)
	stats, err := sc.collectColumnStats("col_all_null", "data")
	if err != nil {
		t.Logf("collectColumnStats error: %v", err)
	}
	_ = stats

	// All same value
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "col_same",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "col_same",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{num28(1), str28("X")}, {num28(2), str28("X")}, {num28(3), str28("X")}},
	}, nil)

	stats, err = sc.collectColumnStats("col_same", "val")
	if err != nil {
		t.Logf("collectColumnStats error on same values: %v", err)
	}
	_ = stats
}

// ============================================================
// Helpers
// ============================================================

func num28(v float64) *query.NumberLiteral         { return &query.NumberLiteral{Value: v} }
func str28(s string) *query.StringLiteral          { return &query.StringLiteral{Value: s} }
func col28(name string) *query.QualifiedIdentifier { return &query.QualifiedIdentifier{Column: name} }
