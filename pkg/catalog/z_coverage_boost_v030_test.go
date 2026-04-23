package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func setup030(t *testing.T) (*Catalog, func()) {
	t.Helper()
	b := storage.NewMemory()
	p := storage.NewBufferPool(4096, b)
	tr, _ := btree.NewBTree(p)
	c := New(tr, p, nil)
	return c, func() { p.Close() }
}

func ins030(t *testing.T, c *Catalog, tbl string, cols []string, vals [][]query.Expression) {
	t.Helper()
	_, _, err := c.insertLocked(context.Background(), &query.InsertStmt{Table: tbl, Columns: cols, Values: vals}, nil)
	if err != nil {
		t.Fatalf("insert %s: %v", tbl, err)
	}
}

func n(v float64) *query.NumberLiteral { return &query.NumberLiteral{Value: v} }
func s(v string) *query.StringLiteral  { return &query.StringLiteral{Value: v} }

// --- Vector distance functions via EvalExpression ---

func TestV030_CosineSimDirect(t *testing.T) {
	sim := cosineSimilarity([]float64{1, 0, 0}, []float64{0, 1, 0})
	t.Logf("CosineSim orthogonal: %v", sim)
	sim = cosineSimilarity([]float64{1, 0, 0}, []float64{1, 0, 0})
	t.Logf("CosineSim identical: %v", sim)
}

func TestV030_L2DistanceDirect(t *testing.T) {
	dist := l2Distance([]float64{1, 0, 0}, []float64{0, 1, 0})
	if dist <= 0 {
		t.Errorf("Expected positive distance, got %v", dist)
	}
	t.Logf("L2 distance: %v", dist)
}

func TestV030_InnerProductDirect(t *testing.T) {
	dot := innerProduct([]float64{1, 2, 3}, []float64{4, 5, 6})
	// 1*4 + 2*5 + 3*6 = 32
	if dot != 32.0 {
		t.Errorf("Expected 32, got %v", dot)
	}
}

func TestV030_CosineSimilarityDirect(t *testing.T) {
	// Same vector = 1.0
	sim := cosineSimilarity([]float64{1, 2, 3}, []float64{1, 2, 3})
	if sim < 0.99 {
		t.Errorf("Expected ~1.0, got %v", sim)
	}
	// Orthogonal = 0.0
	sim = cosineSimilarity([]float64{1, 0}, []float64{0, 1})
	if sim > 0.01 {
		t.Errorf("Expected ~0.0, got %v", sim)
	}
	// Empty vector
	sim = cosineSimilarity([]float64{}, []float64{})
	t.Logf("Empty cosine: %v", sim)
	// Zero vector
	sim = cosineSimilarity([]float64{0, 0, 0}, []float64{1, 2, 3})
	t.Logf("Zero cosine: %v", sim)
}

func TestV030_ToVector(t *testing.T) {
	// float64 slice
	v, err := toVector([]float64{1, 2, 3})
	if err != nil || len(v) != 3 {
		t.Errorf("float64 slice: %v, %v", v, err)
	}

	// interface slice
	v, err = toVector([]interface{}{float64(1), float64(2)})
	if err != nil || len(v) != 2 {
		t.Errorf("interface slice: %v, %v", v, err)
	}

	// int in interface slice
	v, err = toVector([]interface{}{1, 2})
	if err != nil {
		t.Logf("int interface: %v", err)
	}

	// string (invalid)
	_, err = toVector("not a vector")
	if err == nil {
		t.Error("Expected error for string input")
	}

	// nil
	_, err = toVector(nil)
	if err == nil {
		t.Error("Expected error for nil input")
	}
}

// --- ZEROBLOB, QUOTE, GLOB functions ---

func TestV030_ZeroblobQuoteGlob(t *testing.T) {
	c, cleanup := setup030(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "fn_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	ins030(t, c, "fn_t", []string{"id", "val"}, [][]query.Expression{{n(1), s("it's a test")}})

	// ZEROBLOB via SQL
	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "ZEROBLOB", Args: []query.Expression{n(5)}},
		},
		From: &query.TableRef{Name: "fn_t"},
	}, nil)
	if len(rows) > 0 {
		t.Logf("ZEROBLOB(5): len=%d", len(rows[0][0].(string)))
	}

	// QUOTE via SQL
	_, rows, _ = c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "QUOTE", Args: []query.Expression{&query.Identifier{Name: "val"}}},
		},
		From: &query.TableRef{Name: "fn_t"},
	}, nil)
	if len(rows) > 0 {
		t.Logf("QUOTE: %v", rows[0][0])
	}

	// GLOB via SQL
	_, rows, _ = c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "GLOB", Args: []query.Expression{s("*test"), &query.Identifier{Name: "val"}}},
		},
		From: &query.TableRef{Name: "fn_t"},
	}, nil)
	if len(rows) > 0 {
		t.Logf("GLOB: %v", rows[0][0])
	}
}

// --- CTE window functions over CTE result ---

func TestV030_CTEWithWindowFunction(t *testing.T) {
	c, cleanup := setup030(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "wt",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "grp", Type: query.TokenText},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	for i := 0; i < 8; i++ {
		g := "A"
		if i%2 == 0 {
			g = "B"
		}
		ins030(t, c, "wt", []string{"id", "grp", "val"}, [][]query.Expression{
			{n(float64(i)), s(g), n(float64(i * 10))},
		})
	}

	// CTE then window function over CTE result
	cols, rows, err := c.ExecuteCTE(&query.SelectStmtWithCTE{
		CTEs: []*query.CTEDef{{
			Name: "base",
			Query: &query.SelectStmt{
				Columns: []query.Expression{&query.StarExpr{}},
				From:    &query.TableRef{Name: "wt"},
			},
		}},
		Select: &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "id"},
				&query.Identifier{Name: "grp"},
				&query.AliasExpr{
					Expr: &query.WindowExpr{
						Function: "ROW_NUMBER",
						PartitionBy: []query.Expression{
							&query.Identifier{Name: "grp"},
						},
						OrderBy: []*query.OrderByExpr{
							{Expr: &query.Identifier{Name: "val"}, Desc: false},
						},
					},
					Alias: "rn",
				},
			},
			From: &query.TableRef{Name: "base"},
		},
	}, nil)
	if err != nil {
		t.Logf("CTE+Window: %v", err)
	} else {
		t.Logf("CTE+Window: %d cols, %d rows", len(cols), len(rows))
	}
}

// --- Derived table (subquery in FROM) ---

func TestV030_DerivedTableSimple(t *testing.T) {
	c, cleanup := setup030(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "dt",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	for i := 0; i < 5; i++ {
		ins030(t, c, "dt", []string{"id", "val"}, [][]query.Expression{{n(float64(i)), n(float64(i * 3))}})
	}

	// SELECT * FROM (SELECT id, val*2 as doubled FROM dt) sub
	cols, rows, err := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From: &query.TableRef{
			Subquery: &query.SelectStmt{
				Columns: []query.Expression{
					&query.Identifier{Name: "id"},
					&query.AliasExpr{
						Expr: &query.BinaryExpr{
							Left: &query.Identifier{Name: "val"}, Operator: query.TokenStar, Right: n(2),
						},
						Alias: "doubled",
					},
				},
				From: &query.TableRef{Name: "dt"},
			},
			Alias: "sub",
		},
	}, nil)
	if err != nil {
		t.Logf("Derived table: %v", err)
	} else {
		t.Logf("Derived table: %v cols, %d rows", cols, len(rows))
	}
}

// --- MATCH AGAINST (FTS) ---

func TestV030_MatchAgainst(t *testing.T) {
	c, cleanup := setup030(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "fts_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "title", Type: query.TokenText},
			{Name: "body", Type: query.TokenText},
		},
	})
	ins030(t, c, "fts_t", []string{"id", "title", "body"}, [][]query.Expression{
		{n(1), s("Go Programming"), s("Go is a fast compiled language")},
		{n(2), s("Python Tutorial"), s("Python is an interpreted language")},
		{n(3), s("Go Web Server"), s("Building web servers with Go")},
	})

	// Create FTS index
	err := c.CreateFTSIndex("fts_idx", "fts_t", []string{"title", "body"})
	if err != nil {
		t.Fatalf("CreateFTSIndex: %v", err)
	}

	// MATCH ... AGAINST
	_, rows, err := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "title"}},
		From:    &query.TableRef{Name: "fts_t"},
		Where: &query.MatchExpr{
			Columns: []query.Expression{&query.Identifier{Name: "title"}, &query.Identifier{Name: "body"}},
			Pattern: s("Go"),
		},
	}, nil)
	if err != nil {
		t.Logf("MATCH AGAINST: %v", err)
	} else {
		t.Logf("FTS results: %d rows", len(rows))
	}
}

// --- evaluateCast edge cases ---

func TestV030_CastEdgeCases(t *testing.T) {
	// CAST(NULL AS INTEGER)
	result, _ := EvalExpression(&query.CastExpr{
		Expr:     &query.NullLiteral{},
		DataType: query.TokenInteger,
	}, nil)
	if result != nil {
		t.Errorf("Expected nil for CAST(NULL): %v", result)
	}

	// CAST('abc' AS INTEGER)
	result, _ = EvalExpression(&query.CastExpr{
		Expr:     s("abc"),
		DataType: query.TokenInteger,
	}, nil)
	t.Logf("CAST('abc' AS INTEGER): %v", result)

	// CAST(3.14 AS TEXT)
	result, _ = EvalExpression(&query.CastExpr{
		Expr:     n(3.14),
		DataType: query.TokenText,
	}, nil)
	t.Logf("CAST(3.14 AS TEXT): %v", result)

	// CAST(42 AS REAL)
	result, _ = EvalExpression(&query.CastExpr{
		Expr:     n(42),
		DataType: query.TokenReal,
	}, nil)
	t.Logf("CAST(42 AS REAL): %v", result)

	// CAST('1' AS BOOLEAN)
	result, _ = EvalExpression(&query.CastExpr{
		Expr:     s("1"),
		DataType: query.TokenBoolean,
	}, nil)
	t.Logf("CAST('1' AS BOOLEAN): %v", result)
}

// --- BETWEEN, IS NULL, IN edge cases ---

func TestV030_BetweenIsNullIn(t *testing.T) {
	c, cleanup := setup030(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "bi_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	ins030(t, c, "bi_t", []string{"id", "val"}, [][]query.Expression{
		{n(1), n(5)}, {n(2), n(15)}, {n(3), &query.NullLiteral{}},
	})

	// BETWEEN
	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "bi_t"},
		Where: &query.BetweenExpr{
			Expr: &query.Identifier{Name: "val"}, Lower: n(1), Upper: n(10),
		},
	}, nil)
	t.Logf("BETWEEN 1 AND 10: %d rows", len(rows))

	// IS NULL
	_, rows, _ = c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "bi_t"},
		Where:   &query.IsNullExpr{Expr: &query.Identifier{Name: "val"}, Not: false},
	}, nil)
	t.Logf("IS NULL: %d rows", len(rows))

	// IS NOT NULL
	_, rows, _ = c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "bi_t"},
		Where:   &query.IsNullExpr{Expr: &query.Identifier{Name: "val"}, Not: true},
	}, nil)
	t.Logf("IS NOT NULL: %d rows", len(rows))

	// IN
	_, rows, _ = c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "bi_t"},
		Where:   &query.InExpr{Expr: &query.Identifier{Name: "val"}, List: []query.Expression{n(5), n(15)}},
	}, nil)
	t.Logf("IN (5,15): %d rows", len(rows))

	// NOT IN
	_, rows, _ = c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "bi_t"},
		Where:   &query.InExpr{Expr: &query.Identifier{Name: "val"}, List: []query.Expression{n(5)}, Not: true},
	}, nil)
	t.Logf("NOT IN (5): %d rows", len(rows))
}

// --- SELECT with HAVING ---

func TestV030_SelectHaving(t *testing.T) {
	c, cleanup := setup030(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "hv_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat", Type: query.TokenText},
			{Name: "amt", Type: query.TokenReal},
		},
	})
	for i := 0; i < 10; i++ {
		cat := "X"
		if i < 3 {
			cat = "Y"
		}
		ins030(t, c, "hv_t", []string{"id", "cat", "amt"}, [][]query.Expression{
			{n(float64(i)), s(cat), n(float64(i * 5))},
		})
	}

	// GROUP BY ... HAVING COUNT(*) > 3
	_, rows, err := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "cat"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}, Alias: "cnt"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amt"}}}, Alias: "total"},
		},
		From:    &query.TableRef{Name: "hv_t"},
		GroupBy: []query.Expression{&query.Identifier{Name: "cat"}},
		Having: &query.BinaryExpr{
			Left:     &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			Operator: query.TokenGt,
			Right:    n(3),
		},
	}, nil)
	if err != nil {
		t.Logf("HAVING: %v", err)
	} else {
		t.Logf("HAVING result: %d rows", len(rows))
		for _, r := range rows {
			t.Logf("  %v", r)
		}
	}
}

// --- UPDATE...FROM (join update) ---

func TestV030_UpdateFrom(t *testing.T) {
	c, cleanup := setup030(t)
	defer cleanup()

	ctx := context.Background()
	c.CreateTable(&query.CreateTableStmt{
		Table: "uf_target",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "score", Type: query.TokenInteger},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "uf_source",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "bonus", Type: query.TokenInteger},
		},
	})
	ins030(t, c, "uf_target", []string{"id", "score"}, [][]query.Expression{
		{n(1), n(50)}, {n(2), n(60)},
	})
	ins030(t, c, "uf_source", []string{"id", "bonus"}, [][]query.Expression{
		{n(1), n(10)}, {n(2), n(20)},
	})

	// UPDATE...FROM
	c.Update(ctx, &query.UpdateStmt{
		Table: "uf_target",
		Set: []*query.SetClause{{
			Column: "score",
			Value: &query.BinaryExpr{
				Left: &query.Identifier{Name: "score"}, Operator: query.TokenPlus, Right: n(100),
			},
		}},
		From: &query.TableRef{Name: "uf_source"},
		Where: &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Table: "uf_target", Column: "id"},
			Operator: query.TokenEq,
			Right:    &query.QualifiedIdentifier{Table: "uf_source", Column: "id"},
		},
	}, nil)

	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "score"}},
		From:    &query.TableRef{Name: "uf_target"},
		OrderBy: []*query.OrderByExpr{{Expr: &query.Identifier{Name: "id"}}},
	}, nil)
	for _, r := range rows {
		t.Logf("Row: %v", r)
	}
}

// --- Savepoint rollback ---

func TestV030_Savepoint(t *testing.T) {
	c, cleanup := setup030(t)
	defer cleanup()

	c.CreateTable(&query.CreateTableStmt{
		Table: "sp_t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	c.BeginTransaction(1)
	ins030(t, c, "sp_t", []string{"id"}, [][]query.Expression{{n(1)}})

	c.Savepoint("sp1")
	ins030(t, c, "sp_t", []string{"id"}, [][]query.Expression{{n(2)}})

	// Rollback to savepoint
	err := c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Errorf("RollbackToSavepoint: %v", err)
	}

	// Row 2 should be gone, row 1 should remain
	_, rows, _ := c.selectLocked(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "sp_t"},
	}, nil)
	if len(rows) > 0 {
		t.Logf("After savepoint rollback: %v rows", rows[0][0])
	}

	c.CommitTransaction()
}
