package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Target: deleteLocked (76.0%)
// ============================================================

func TestCovBoost16_DeleteWithIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "del_idx",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}},
		PrimaryKey:  []string{"id"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_del", Table: "del_idx", Columns: []string{"name"}})

	cat.Insert(ctx, &query.InsertStmt{Table: "del_idx", Columns: []string{"id", "name"}, Values: [][]query.Expression{{num(1), str("a")}, {num(2), str("b")}, {num(3), str("c")}}}, nil)

	// Delete with index condition
	cat.Delete(ctx, &query.DeleteStmt{Table: "del_idx", Where: &query.BinaryExpr{Left: col("name"), Operator: query.TokenEq, Right: str("b")}}, nil)

	// Verify
	_, rows, _ := cat.Select(&query.SelectStmt{Columns: []query.Expression{&query.StarExpr{}}, From: &query.TableRef{Name: "del_idx"}}, nil)
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

func TestCovBoost16_DeleteAllRows(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "del_all",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey:  []string{"id"},
	})
	cat.Insert(ctx, &query.InsertStmt{Table: "del_all", Columns: []string{"id", "val"}, Values: [][]query.Expression{{num(1), num(10)}, {num(2), num(20)}}}, nil)

	// Delete all rows
	cat.Delete(ctx, &query.DeleteStmt{Table: "del_all"}, nil)

	_, rows, _ := cat.Select(&query.SelectStmt{Columns: []query.Expression{&query.StarExpr{}}, From: &query.TableRef{Name: "del_all"}}, nil)
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestCovBoost16_DeleteWithOrCondition(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "del_or",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey:  []string{"id"},
	})
	cat.Insert(ctx, &query.InsertStmt{Table: "del_or", Columns: []string{"id", "val"}, Values: [][]query.Expression{
		{num(1), num(10)}, {num(2), num(20)}, {num(3), num(30)}, {num(4), num(40)},
	}}, nil)

	// Delete with OR condition
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_or",
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: col("val"), Operator: query.TokenEq, Right: num(10)},
			Operator: query.TokenOr,
			Right:    &query.BinaryExpr{Left: col("val"), Operator: query.TokenEq, Right: num(30)},
		},
	}, nil)

	_, rows, _ := cat.Select(&query.SelectStmt{Columns: []query.Expression{&query.StarExpr{}}, From: &query.TableRef{Name: "del_or"}}, nil)
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// Target: JSONQuote (75.0%)
// ============================================================

func TestCovBoost16_JSONQuote(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "jsonq",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "txt", Type: query.TokenText}},
		PrimaryKey:  []string{"id"},
	})
	cat.Insert(ctx, &query.InsertStmt{Table: "jsonq", Columns: []string{"id", "txt"}, Values: [][]query.Expression{
		{num(1), str("hello")}, {num(2), str(`with"quotes`)}, {num(3), str(`with\backslash`)},
	}}, nil)

	// Test JSONQuote function
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "JSON_QUOTE", Args: []query.Expression{col("txt")}}},
		From:    &query.TableRef{Name: "jsonq"},
	}, nil)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

// ============================================================
// Target: useIndexForQueryWithArgs (75.0%)
// ============================================================

func TestCovBoost16_IndexRangeScan(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "idx_range",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey:  []string{"id"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_val", Table: "idx_range", Columns: []string{"val"}})
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{Table: "idx_range", Columns: []string{"id", "val"}, Values: [][]query.Expression{{num(float64(i)), num(float64(i * 10))}}}, nil)
	}

	// Range scan with >
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "idx_range"},
		Where:   &query.BinaryExpr{Left: col("val"), Operator: query.TokenGt, Right: num(500)},
		OrderBy: []*query.OrderByExpr{{Expr: col("val")}},
	}, nil)
	if len(rows) != 50 {
		t.Errorf("expected 50 rows, got %d", len(rows))
	}

	// Range scan with >=
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "idx_range"},
		Where:   &query.BinaryExpr{Left: col("val"), Operator: query.TokenGte, Right: num(500)},
		OrderBy: []*query.OrderByExpr{{Expr: col("val")}},
	}, nil)
	if len(rows) != 51 {
		t.Errorf("expected 51 rows, got %d", len(rows))
	}

	// Range scan with <
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "idx_range"},
		Where:   &query.BinaryExpr{Left: col("val"), Operator: query.TokenLt, Right: num(200)},
		OrderBy: []*query.OrderByExpr{{Expr: col("val")}},
	}, nil)
	if len(rows) != 19 {
		t.Errorf("expected 19 rows, got %d", len(rows))
	}

	// Range scan with <=
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "idx_range"},
		Where:   &query.BinaryExpr{Left: col("val"), Operator: query.TokenLte, Right: num(200)},
		OrderBy: []*query.OrderByExpr{{Expr: col("val")}},
	}, nil)
	if len(rows) != 20 {
		t.Errorf("expected 20 rows, got %d", len(rows))
	}
}

func TestCovBoost16_IndexCompositeWhere(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "idx_comp",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "a", Type: query.TokenInteger}, {Name: "b", Type: query.TokenInteger}},
		PrimaryKey:  []string{"id"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_ab", Table: "idx_comp", Columns: []string{"a"}})
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{Table: "idx_comp", Columns: []string{"id", "a", "b"}, Values: [][]query.Expression{{num(float64(i)), num(float64(i % 5)), num(float64(i * 2))}}}, nil)
	}

	// Complex WHERE with AND
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "idx_comp"},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: col("a"), Operator: query.TokenEq, Right: num(2)},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: col("b"), Operator: query.TokenGt, Right: num(20)},
		},
		OrderBy: []*query.OrderByExpr{{Expr: col("id")}},
	}, nil)
	_ = rows

	// OR condition
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "idx_comp"},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: col("a"), Operator: query.TokenEq, Right: num(1)},
			Operator: query.TokenOr,
			Right:    &query.BinaryExpr{Left: col("a"), Operator: query.TokenEq, Right: num(3)},
		},
		OrderBy: []*query.OrderByExpr{{Expr: col("id")}},
	}, nil)
	if len(rows) != 20 {
		t.Errorf("expected 20 rows, got %d", len(rows))
	}
}

// ============================================================
// Target: applyGroupByOrderBy (60.0%)
// ============================================================

func TestCovBoost16_GroupByOrderByComplex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "gbo",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "cat", Type: query.TokenText}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey:  []string{"id"},
	})
	cat.Insert(ctx, &query.InsertStmt{Table: "gbo", Columns: []string{"id", "cat", "val"}, Values: [][]query.Expression{
		{num(1), str("A"), num(10)}, {num(2), str("A"), num(20)}, {num(3), str("B"), num(5)}, {num(4), str("B"), num(15)}, {num(5), str("C"), num(30)},
	}}, nil)

	// GROUP BY with ORDER BY ASC
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col("cat"), &query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col("val")}}, Alias: "total"}},
		From:    &query.TableRef{Name: "gbo"},
		GroupBy: []query.Expression{col("cat")},
		OrderBy: []*query.OrderByExpr{{Expr: col("total")}},
	}, nil)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}

	// GROUP BY with ORDER BY DESC
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col("cat"), &query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col("val")}}, Alias: "total"}},
		From:    &query.TableRef{Name: "gbo"},
		GroupBy: []query.Expression{col("cat")},
		OrderBy: []*query.OrderByExpr{{Expr: col("total"), Desc: true}},
	}, nil)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}

	// GROUP BY with ORDER BY on multiple columns
	_, rows, _ = cat.Select(&query.SelectStmt{
		Columns: []query.Expression{col("cat"), &query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col("val")}}, Alias: "total"}},
		From:    &query.TableRef{Name: "gbo"},
		GroupBy: []query.Expression{col("cat")},
		OrderBy: []*query.OrderByExpr{{Expr: col("cat")}, {Expr: col("total"), Desc: true}},
	}, nil)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

// ============================================================
// Target: flushTableTreesLocked (75.0%)
// ============================================================

func TestCovBoost16_FlushTableTrees(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "flush1",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey:  []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:       "flush2",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey:  []string{"id"},
	})
	cat.Insert(ctx, &query.InsertStmt{Table: "flush1", Columns: []string{"id", "val"}, Values: [][]query.Expression{{num(1), num(10)}, {num(2), num(20)}}}, nil)
	cat.Insert(ctx, &query.InsertStmt{Table: "flush2", Columns: []string{"id", "val"}, Values: [][]query.Expression{{num(1), num(100)}, {num(2), num(200)}}}, nil)

	// Trigger flush via transaction commit
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{Table: "flush1", Columns: []string{"id", "val"}, Values: [][]query.Expression{{num(3), num(30)}}}, nil)
	cat.Insert(ctx, &query.InsertStmt{Table: "flush2", Columns: []string{"id", "val"}, Values: [][]query.Expression{{num(3), num(300)}}}, nil)
	cat.CommitTransaction()

	// Verify data after commit
	_, rows, _ := cat.Select(&query.SelectStmt{Columns: []query.Expression{&query.StarExpr{}}, From: &query.TableRef{Name: "flush1"}, OrderBy: []*query.OrderByExpr{{Expr: col("id")}}}, nil)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

// ============================================================
// Helpers
// ============================================================

func num(v float64) *query.NumberLiteral { return &query.NumberLiteral{Value: v} }
func str(s string) *query.StringLiteral  { return &query.StringLiteral{Value: s} }
func col(name string) *query.QualifiedIdentifier {
	return &query.QualifiedIdentifier{Column: name}
}
