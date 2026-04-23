package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func setupRC(t *testing.T) (*Catalog, func()) {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)
	return c, func() { pool.Close() }
}

func rcInsert(t *testing.T, c *Catalog, table string, cols []string, vals [][]query.Expression) {
	t.Helper()
	ctx := context.Background()
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{Table: table, Columns: cols, Values: vals}, nil)
	if err != nil {
		t.Fatalf("insert into %s failed: %v", table, err)
	}
}

func numL(v float64) *query.NumberLiteral { return &query.NumberLiteral{Value: v} }
func strL(v string) *query.StringLiteral  { return &query.StringLiteral{Value: v} }

// --- JSONQuote/JSONUnquote ---

func TestRC_JSONQuote(t *testing.T) {
	tests := []string{"hello", "with\"quote", "with\nnewline", "\ttab", "", "unicode: é"}
	for _, s := range tests {
		r := JSONQuote(s)
		if len(r) < 2 || r[0] != '"' {
			t.Errorf("JSONQuote(%q) = %q", s, r)
		}
	}
}

func TestRC_JSONUnquote(t *testing.T) {
	r, _ := JSONUnquote("")
	if r != "" {
		t.Error("empty")
	}
	r, _ = JSONUnquote(`"hello"`)
	if r != "hello" {
		t.Error("hello")
	}
	_, err := JSONUnquote("bad")
	if err == nil {
		t.Error("expected error")
	}
}

func TestRC_IsValidJSON(t *testing.T) {
	if !IsValidJSON(`{"a":1}`) {
		t.Error("valid")
	}
	if IsValidJSON("") {
		t.Error("empty")
	}
	if IsValidJSON("{bad") {
		t.Error("invalid")
	}
}

// --- Save / Load ---

func TestRC_SaveLoad(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "sl_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	rcInsert(t, c, "sl_t", []string{"id", "val"}, [][]query.Expression{{numL(1), strL("a")}})

	if err := c.Save(); err != nil {
		t.Errorf("Save: %v", err)
	}
	if err := c.FlushTableTrees(); err != nil {
		t.Errorf("FlushTableTrees: %v", err)
	}
	if err := c.Load(); err != nil {
		t.Errorf("Load: %v", err)
	}
	_ = c.SaveData("")
	_ = c.LoadSchema("")
	_ = c.LoadData("")
}

// --- Vacuum ---

func TestRC_Vacuum(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "vc_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_vc", Table: "vc_t", Columns: []string{"data"}})

	for i := 0; i < 20; i++ {
		rcInsert(t, c, "vc_t", []string{"id", "data"}, [][]query.Expression{{numL(float64(i)), strL("x")}})
	}
	ctx := context.Background()
	c.Delete(ctx, &query.DeleteStmt{
		Table: "vc_t",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenGt, Right: numL(10)},
	}, nil)

	if err := c.Vacuum(); err != nil {
		t.Errorf("Vacuum: %v", err)
	}
	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "vc_t"},
	}, nil)
	if len(rows) > 0 {
		t.Logf("After vacuum: %v rows", rows[0][0])
	}
}

// --- Rollback Transaction ---

func TestRC_Rollback(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "rb_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	rcInsert(t, c, "rb_t", []string{"id"}, [][]query.Expression{{numL(1)}})

	c.BeginTransaction(1)
	rcInsert(t, c, "rb_t", []string{"id"}, [][]query.Expression{{numL(2)}})

	if err := c.RollbackTransaction(); err != nil {
		t.Errorf("Rollback: %v", err)
	}

	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "rb_t"},
	}, nil)
	if len(rows) > 0 {
		t.Logf("After rollback: %v", rows[0][0])
	}
}

func TestRC_RollbackDDL(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	c.BeginTransaction(2)
	c.CreateTable(&query.CreateTableStmt{
		Table:   "rb_ddl",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.RollbackTransaction()
	_, err := c.getTableLocked("rb_ddl")
	if err == nil {
		t.Error("Table should not exist after DDL rollback")
	}
}

// --- FK CASCADE / SET NULL ---

func TestRC_FKCascade(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	ctx := context.Background()
	c.CreateTable(&query.CreateTableStmt{
		Table:   "fk_p",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_c",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "pid", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns: []string{"pid"}, ReferencedTable: "fk_p", ReferencedColumns: []string{"id"},
			OnDelete: "CASCADE",
		}},
	})
	rcInsert(t, c, "fk_p", []string{"id"}, [][]query.Expression{{numL(1)}})
	rcInsert(t, c, "fk_c", []string{"id", "pid"}, [][]query.Expression{{numL(10), numL(1)}})

	c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_p",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numL(1)},
	}, nil)

	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "fk_c"},
	}, nil)
	if len(rows) > 0 && rows[0][0] != int64(0) {
		t.Errorf("Expected 0 child rows, got %v", rows[0][0])
	}
}

func TestRC_FKSetNull(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	ctx := context.Background()
	c.CreateTable(&query.CreateTableStmt{
		Table:   "sn_p",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "sn_c",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "pid", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns: []string{"pid"}, ReferencedTable: "sn_p", ReferencedColumns: []string{"id"},
			OnDelete: "SET NULL",
		}},
	})
	rcInsert(t, c, "sn_p", []string{"id"}, [][]query.Expression{{numL(1)}})
	rcInsert(t, c, "sn_c", []string{"id", "pid"}, [][]query.Expression{{numL(10), numL(1)}})

	c.Delete(ctx, &query.DeleteStmt{
		Table: "sn_p",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numL(1)},
	}, nil)

	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "pid"}},
		From:    &query.TableRef{Name: "sn_c"},
	}, nil)
	if len(rows) > 0 && rows[0][0] != nil {
		t.Errorf("Expected NULL, got %v", rows[0][0])
	}
}

// --- UPDATE with index ---

func TestRC_UpdateIndex(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	ctx := context.Background()
	c.CreateTable(&query.CreateTableStmt{
		Table: "ui_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_ui", Table: "ui_t", Columns: []string{"name"}})
	rcInsert(t, c, "ui_t", []string{"id", "name"}, [][]query.Expression{{numL(1), strL("old")}})

	c.Update(ctx, &query.UpdateStmt{
		Table: "ui_t",
		Set:   []*query.SetClause{{Column: "name", Value: strL("new")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numL(1)},
	}, nil)

	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "ui_t"},
	}, nil)
	if len(rows) > 0 {
		t.Logf("Updated: %v", rows[0][0])
	}
}

// --- CTE ---

func TestRC_CTEAggr(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "ct_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat", Type: query.TokenText},
			{Name: "amt", Type: query.TokenReal},
		},
	})
	for i := 0; i < 6; i++ {
		cat := "A"
		if i%2 == 0 {
			cat = "B"
		}
		rcInsert(t, c, "ct_t", []string{"id", "cat", "amt"},
			[][]query.Expression{{numL(float64(i)), strL(cat), numL(float64(i * 10))}})
	}

	cols, rows, err := c.ExecuteCTE(&query.SelectStmtWithCTE{
		CTEs: []*query.CTEDef{{
			Name: "s",
			Query: &query.SelectStmt{
				Columns: []query.Expression{
					&query.Identifier{Name: "cat"},
					&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amt"}}}, Alias: "total"},
				},
				From:    &query.TableRef{Name: "ct_t"},
				GroupBy: []query.Expression{&query.Identifier{Name: "cat"}},
			},
		}},
		Select: &query.SelectStmt{
			Columns: []query.Expression{&query.StarExpr{}},
			From:    &query.TableRef{Name: "s"},
		},
	}, nil)
	if err != nil {
		t.Errorf("CTE: %v", err)
	}
	t.Logf("CTE: %d cols, %d rows", len(cols), len(rows))
}

// --- Analyze ---

func TestRC_Analyze(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "an_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "v", Type: query.TokenText},
		},
	})
	for i := 0; i < 30; i++ {
		rcInsert(t, c, "an_t", []string{"id", "v"}, [][]query.Expression{{numL(float64(i)), strL("d")}})
	}
	if err := c.Analyze("an_t"); err != nil {
		t.Errorf("Analyze: %v", err)
	}
	stats, err := c.GetTableStats("an_t")
	if err == nil && stats != nil {
		t.Logf("RowCount: %d", stats.RowCount)
	}
}

// --- Materialized View ---

func TestRC_MatView(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "mv_s",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "g", Type: query.TokenText},
			{Name: "v", Type: query.TokenInteger},
		},
	})
	for i := 0; i < 5; i++ {
		rcInsert(t, c, "mv_s", []string{"id", "g", "v"}, [][]query.Expression{{numL(float64(i)), strL("x"), numL(float64(i))}})
	}

	err := c.CreateMaterializedView("mv_r", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "g"}, &query.AliasExpr{
			Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "v"}}}, Alias: "total"}},
		From:    &query.TableRef{Name: "mv_s"},
		GroupBy: []query.Expression{&query.Identifier{Name: "g"}},
	}, false)
	if err != nil {
		t.Errorf("CreateMV: %v", err)
	}

	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "mv_r"},
	}, nil)
	t.Logf("MV rows: %d", len(rows))

	if err := c.RefreshMaterializedView("mv_r"); err != nil {
		t.Errorf("RefreshMV: %v", err)
	}
}

// --- INSTEAD OF trigger ---

func TestRC_InsteadOfTrigger(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	ctx := context.Background()
	c.CreateTable(&query.CreateTableStmt{
		Table: "io_base",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	c.CreateView("io_view", &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "io_base"},
	})
	c.CreateTrigger(&query.CreateTriggerStmt{
		Name: "trg_io", Table: "io_view", Time: "INSTEAD OF", Event: "INSERT",
		Body: []query.Statement{&query.InsertStmt{
			Table: "io_base", Columns: []string{"id", "val"},
			Values: [][]query.Expression{{
				&query.QualifiedIdentifier{Table: "NEW", Column: "id"},
				&query.QualifiedIdentifier{Table: "NEW", Column: "val"},
			}},
		}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table: "io_view", Columns: []string{"id", "val"},
		Values: [][]query.Expression{{numL(1), strL("triggered")}},
	}, nil)

	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "io_base"},
	}, nil)
	t.Logf("INSTEAD OF rows: %d", len(rows))
}

// --- Scalar SELECT ---

func TestRC_ScalarSelect(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	cols, rows, err := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{
			&query.BinaryExpr{Left: numL(1), Operator: query.TokenPlus, Right: numL(2)},
		},
	}, nil)
	if err != nil {
		t.Errorf("Scalar: %v", err)
	}
	if len(rows) > 0 {
		t.Logf("1+2 = %v (cols: %v)", rows[0][0], cols)
	}
}

// --- DELETE...USING ---

func TestRC_DeleteUsing(t *testing.T) {
	c, cleanup := setupRC(t)
	defer cleanup()

	ctx := context.Background()
	c.CreateTable(&query.CreateTableStmt{
		Table: "du_m",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "rid", Type: query.TokenInteger},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "du_r",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "active", Type: query.TokenInteger},
		},
	})
	rcInsert(t, c, "du_r", []string{"id", "active"}, [][]query.Expression{{numL(1), numL(0)}, {numL(2), numL(1)}})
	rcInsert(t, c, "du_m", []string{"id", "rid"}, [][]query.Expression{{numL(10), numL(1)}, {numL(20), numL(2)}})

	c.Delete(ctx, &query.DeleteStmt{
		Table: "du_m",
		Using: []*query.TableRef{{Name: "du_r"}},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left: &query.QualifiedIdentifier{Table: "du_m", Column: "rid"}, Operator: query.TokenEq,
				Right: &query.QualifiedIdentifier{Table: "du_r", Column: "id"},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left: &query.QualifiedIdentifier{Table: "du_r", Column: "active"}, Operator: query.TokenEq, Right: numL(0),
			},
		},
	}, nil)

	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "du_m"},
	}, nil)
	if len(rows) > 0 {
		t.Logf("After DELETE...USING: %v", rows[0][0])
	}
}
