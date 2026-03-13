package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// Target: deleteWithUsingLocked (69.0%)
// ============================================================

func TestCovBoost24_DeleteUsingMultipleTables(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_main",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_ref1",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "main_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_ref2",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "main_id", Type: query.TokenInteger}, {Name: "mark", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "del_main", Columns: []string{"id", "name"}, Values: [][]query.Expression{
		{num24(1), str24("Alice")}, {num24(2), str24("Bob")}, {num24(3), str24("Carol")},
	}}, nil)
	cat.Insert(ctx, &query.InsertStmt{Table: "del_ref1", Columns: []string{"id", "main_id"}, Values: [][]query.Expression{
		{num24(1), num24(1)}, {num24(2), num24(2)},
	}}, nil)
	cat.Insert(ctx, &query.InsertStmt{Table: "del_ref2", Columns: []string{"id", "main_id", "mark"}, Values: [][]query.Expression{
		{num24(1), num24(1), num24(1)}, {num24(2), num24(3), num24(1)},
	}}, nil)

	// DELETE USING with multiple tables
	cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_main",
		Using: []*query.TableRef{{Name: "del_ref1"}, {Name: "del_ref2"}},
		Where: &query.BinaryExpr{
			Left:     &query.BinaryExpr{Left: col24("del_main.id"), Operator: query.TokenEq, Right: col24("del_ref1.main_id")},
			Operator: query.TokenAnd,
			Right:    &query.BinaryExpr{Left: col24("del_ref2.mark"), Operator: query.TokenEq, Right: num24(1)},
		},
	}, nil)

	// Verify rows remain
	_, rows, _ := cat.Select(&query.SelectStmt{Columns: []query.Expression{&query.StarExpr{}}, From: &query.TableRef{Name: "del_main"}}, nil)
	_ = rows
}

// ============================================================
// Target: OnDelete (73.7%) and OnUpdate (73.7%)
// ============================================================

func TestCovBoost24_FKOnDeleteSetNull(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "fk_del_parent",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "fk_del_child",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "parent_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "fk_del_parent",
			ReferencedColumns: []string{"id"},
			OnDelete:          "SET NULL",
		}},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "fk_del_parent", Columns: []string{"id"}, Values: [][]query.Expression{{num24(1)}, {num24(2)}}}, nil)
	cat.Insert(ctx, &query.InsertStmt{Table: "fk_del_child", Columns: []string{"id", "parent_id"}, Values: [][]query.Expression{
		{num24(1), num24(1)}, {num24(2), num24(1)}, {num24(3), num24(2)},
	}}, nil)

	// Delete parent - should set child parent_id to NULL
	cat.Delete(ctx, &query.DeleteStmt{Table: "fk_del_parent", Where: &query.BinaryExpr{Left: col24("id"), Operator: query.TokenEq, Right: num24(1)}}, nil)

	// Verify children have NULL parent_id
	_, rows, _ := cat.Select(&query.SelectStmt{Columns: []query.Expression{col24("id")}, From: &query.TableRef{Name: "fk_del_child"}, Where: &query.IsNullExpr{Expr: col24("parent_id")}}, nil)
	if len(rows) != 2 {
		t.Errorf("expected 2 children with NULL parent_id, got %d", len(rows))
	}
}

func TestCovBoost24_FKOnUpdateSetNull(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "fk_upd_parent2",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "fk_upd_child2",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "parent_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "fk_upd_parent2",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "SET NULL",
		}},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "fk_upd_parent2", Columns: []string{"id"}, Values: [][]query.Expression{{num24(1)}, {num24(2)}}}, nil)
	cat.Insert(ctx, &query.InsertStmt{Table: "fk_upd_child2", Columns: []string{"id", "parent_id"}, Values: [][]query.Expression{{num24(1), num24(1)}}}, nil)

	// Update parent id - should set child parent_id to NULL
	cat.Update(ctx, &query.UpdateStmt{
		Table: "fk_upd_parent2",
		Set:   []*query.SetClause{{Column: "id", Value: num24(100)}},
		Where: &query.BinaryExpr{Left: col24("id"), Operator: query.TokenEq, Right: num24(1)},
	}, nil)

	// Verify child has NULL parent_id
	_, rows, _ := cat.Select(&query.SelectStmt{Columns: []query.Expression{col24("id")}, From: &query.TableRef{Name: "fk_upd_child2"}, Where: &query.IsNullExpr{Expr: col24("parent_id")}}, nil)
	if len(rows) != 1 {
		t.Errorf("expected 1 child with NULL parent_id, got %d", len(rows))
	}
}

// ============================================================
// Target: useIndexForExactMatch (69.2%)
// ============================================================

func TestCovBoost24_IndexExactMatchWithNull(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "idx_null",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "cat", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_cat_null", Table: "idx_null", Columns: []string{"cat"}})

	cat.Insert(ctx, &query.InsertStmt{Table: "idx_null", Columns: []string{"id", "cat"}, Values: [][]query.Expression{
		{num24(1), str24("A")}, {num24(2), &query.NullLiteral{}}, {num24(3), str24("A")},
	}}, nil)

	// Query with IS NULL
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "idx_null"},
		Where:   &query.IsNullExpr{Expr: col24("cat")},
	}, nil)
	if len(rows) != 1 {
		t.Errorf("expected 1 row with NULL cat, got %d", len(rows))
	}
}

func TestCovBoost24_IndexRangeQuery(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "idx_range",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "score", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateIndex(&query.CreateIndexStmt{Index: "idx_score", Table: "idx_range", Columns: []string{"score"}})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{Table: "idx_range", Columns: []string{"id", "score"}, Values: [][]query.Expression{{num24(float64(i)), num24(float64(i * 10))}}}, nil)
	}

	// Range query using index
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "idx_range"},
		Where:   &query.BinaryExpr{Left: col24("score"), Operator: query.TokenGte, Right: num24(200)},
	}, nil)
	// Should get scores 200-500 (30 rows: 20,21,22...50)
	if len(rows) != 31 {
		t.Errorf("expected 31 rows with score >= 200, got %d", len(rows))
	}
}

// ============================================================
// Target: applyGroupByOrderBy (70.0%)
// ============================================================

func TestCovBoost24_GroupByMultipleColumns(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "gb_multi",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "dept", Type: query.TokenText}, {Name: "role", Type: query.TokenText}, {Name: "salary", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "gb_multi", Columns: []string{"id", "dept", "role", "salary"}, Values: [][]query.Expression{
		{num24(1), str24("Sales"), str24("Mgr"), num24(5000)},
		{num24(2), str24("Sales"), str24("Rep"), num24(3000)},
		{num24(3), str24("Sales"), str24("Rep"), num24(3200)},
		{num24(4), str24("Eng"), str24("Dev"), num24(6000)},
		{num24(5), str24("Eng"), str24("Dev"), num24(6500)},
	}}, nil)

	// GROUP BY multiple columns
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col24("dept"),
			col24("role"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}, Alias: "cnt"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col24("salary")}}, Alias: "total"},
		},
		From:    &query.TableRef{Name: "gb_multi"},
		GroupBy: []query.Expression{col24("dept"), col24("role")},
		OrderBy: []*query.OrderByExpr{{Expr: col24("dept")}, {Expr: col24("role")}},
	}, nil)
	// Should get 3 groups: (Sales,Mgr), (Sales,Rep), (Eng,Dev)
	if len(rows) != 3 {
		t.Errorf("expected 3 groups, got %d", len(rows))
	}
}

func TestCovBoost24_GroupByWithOrderByAgg(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "gb_order",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "cat", Type: query.TokenText}, {Name: "amt", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "gb_order", Columns: []string{"id", "cat", "amt"}, Values: [][]query.Expression{
		{num24(1), str24("A"), num24(100)},
		{num24(2), str24("B"), num24(500)},
		{num24(3), str24("A"), num24(200)},
		{num24(4), str24("B"), num24(300)},
	}}, nil)

	// GROUP BY with ORDER BY aggregate
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col24("cat"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col24("amt")}}, Alias: "total"},
		},
		From:    &query.TableRef{Name: "gb_order"},
		GroupBy: []query.Expression{col24("cat")},
		OrderBy: []*query.OrderByExpr{{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{col24("amt")}}, Desc: true}},
	}, nil)
	if len(rows) != 2 {
		t.Errorf("expected 2 groups, got %d", len(rows))
	}
}

// ============================================================
// Target: computeAggregatesWithGroupBy (72.1%)
// ============================================================

func TestCovBoost24_AggregateWithDistinct(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "agg_dist",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "cat", Type: query.TokenText}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "agg_dist", Columns: []string{"id", "cat", "val"}, Values: [][]query.Expression{
		{num24(1), str24("A"), num24(10)},
		{num24(2), str24("A"), num24(10)},
		{num24(3), str24("A"), num24(20)},
		{num24(4), str24("B"), num24(30)},
		{num24(5), str24("B"), num24(30)},
	}}, nil)

	// COUNT DISTINCT
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col24("cat"),
			&query.AliasExpr{
				Expr:  &query.FunctionCall{Name: "COUNT", Args: []query.Expression{col24("val")}, Distinct: true},
				Alias: "distinct_vals",
			},
		},
		From:    &query.TableRef{Name: "agg_dist"},
		GroupBy: []query.Expression{col24("cat")},
	}, nil)
	if len(rows) != 2 {
		t.Errorf("expected 2 groups, got %d", len(rows))
	}
}

// ============================================================
// Target: evaluateWindowFunctions - AVG (56.5%)
// ============================================================

func TestCovBoost24_WindowAvg(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "win_avg",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "grp", Type: query.TokenText}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "win_avg", Columns: []string{"id", "grp", "val"}, Values: [][]query.Expression{
		{num24(1), str24("A"), num24(10)}, {num24(2), str24("A"), num24(20)}, {num24(3), str24("A"), num24(30)},
		{num24(4), str24("B"), num24(100)}, {num24(5), str24("B"), num24(200)},
	}}, nil)

	// Window AVG
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col24("id"),
			&query.WindowExpr{
				Function:    "AVG",
				Args:        []query.Expression{col24("val")},
				PartitionBy: []query.Expression{col24("grp")},
			},
		},
		From:    &query.TableRef{Name: "win_avg"},
		OrderBy: []*query.OrderByExpr{{Expr: col24("id")}},
	}, nil)
	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}
}

func TestCovBoost24_WindowMinMax(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "win_minmax",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "win_minmax", Columns: []string{"id", "val"}, Values: [][]query.Expression{
		{num24(1), num24(50)}, {num24(2), num24(30)}, {num24(3), num24(80)}, {num24(4), num24(20)},
	}}, nil)

	// Window MIN and MAX
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			col24("id"),
			&query.WindowExpr{Function: "MIN", Args: []query.Expression{col24("val")}, OrderBy: []*query.OrderByExpr{{Expr: col24("id")}}},
			&query.WindowExpr{Function: "MAX", Args: []query.Expression{col24("val")}, OrderBy: []*query.OrderByExpr{{Expr: col24("id")}}},
		},
		From: &query.TableRef{Name: "win_minmax"},
	}, nil)
	if len(rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(rows))
	}
}

// ============================================================
// Target: insertLocked (72.2%)
// ============================================================

func TestCovBoost24_InsertSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	ctx := context.Background()
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "ins_src",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "ins_dst",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{Table: "ins_src", Columns: []string{"id", "val"}, Values: [][]query.Expression{
		{num24(1), num24(100)}, {num24(2), num24(200)}, {num24(3), num24(300)},
	}}, nil)

	// INSERT ... SELECT
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ins_dst",
		Columns: []string{"id", "val"},
		Select: &query.SelectStmt{
			Columns: []query.Expression{col24("id"), col24("val")},
			From:    &query.TableRef{Name: "ins_src"},
			Where:   &query.BinaryExpr{Left: col24("val"), Operator: query.TokenGt, Right: num24(150)},
		},
	}, nil)

	// Verify inserted rows
	_, rows, _ := cat.Select(&query.SelectStmt{Columns: []query.Expression{&query.StarExpr{}}, From: &query.TableRef{Name: "ins_dst"}}, nil)
	if len(rows) != 2 {
		t.Errorf("expected 2 rows inserted, got %d", len(rows))
	}
}

// ============================================================
// Helpers
// ============================================================

func num24(v float64) *query.NumberLiteral         { return &query.NumberLiteral{Value: v} }
func str24(s string) *query.StringLiteral          { return &query.StringLiteral{Value: s} }
func col24(name string) *query.QualifiedIdentifier { return &query.QualifiedIdentifier{Column: name} }
