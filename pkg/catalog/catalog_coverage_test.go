package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

func createTestTable(t *testing.T, c *Catalog, name string, cols []*query.ColumnDef) {
	t.Helper()
	if err := c.CreateTable(&query.CreateTableStmt{Table: name, Columns: cols}); err != nil {
		t.Fatal(err)
	}
}

func insertTestRow(t *testing.T, c *Catalog, tbl string, v []query.Expression) {
	t.Helper()
	if _, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: tbl, Values: [][]query.Expression{v}}, nil); err != nil {
		t.Fatal(err)
	}
}

func sel(t *testing.T, c *Catalog, s *query.SelectStmt) ([]string, [][]interface{}) {
	t.Helper()
	cols, rows, err := c.Select(s, nil)
	if err != nil {
		t.Fatal(err)
	}
	return cols, rows
}

func nr(v float64) *query.NumberLiteral   { return &query.NumberLiteral{Value: v} }
func sr(v string) *query.StringLiteral    { return &query.StringLiteral{Value: v} }
func nl() *query.NullLiteral             { return &query.NullLiteral{} }
func bl(v bool) *query.BooleanLiteral    { return &query.BooleanLiteral{Value: v} }
func id(n string) *query.Identifier      { return &query.Identifier{Name: n} }
func star() *query.StarExpr              { return &query.StarExpr{} }
func tref(n string) *query.TableRef      { return &query.TableRef{Name: n} }
func qi(t, c string) *query.QualifiedIdentifier {
	return &query.QualifiedIdentifier{Table: t, Column: c}
}
func fn(name string, args ...query.Expression) *query.FunctionCall {
	return &query.FunctionCall{Name: name, Args: args}
}
func binop(l query.Expression, op query.TokenType, r query.Expression) *query.BinaryExpr {
	return &query.BinaryExpr{Left: l, Operator: op, Right: r}
}

// ── SELECT ───────────────────────────────────────────────────────────
func TestCov_SelectBasic(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "items", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "price", Type: query.TokenReal},
	})
	insertTestRow(t, c, "items", []query.Expression{nr(1), sr("apple"), nr(1.5)})
	insertTestRow(t, c, "items", []query.Expression{nr(2), sr("banana"), nr(2.5)})
	cols, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("items")})
	if len(cols) != 3 {
		t.Fatal(len(cols))
	}
	if len(rows) != 2 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectWhere(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "items", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	insertTestRow(t, c, "items", []query.Expression{nr(1), sr("apple")})
	insertTestRow(t, c, "items", []query.Expression{nr(2), sr("banana")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()},
		From:    tref("items"),
		Where:   binop(id("id"), query.TokenEq, nr(1)),
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectOrderLimitOffset(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	insertTestRow(t, c, "t", []query.Expression{nr(3)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		OrderBy: []*query.OrderByExpr{{Expr: id("id")}},
		Limit: nr(2), Offset: nr(1),
	})
	if len(rows) != 2 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectDistinct(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("a")})
	_, rows := sel(t, c, &query.SelectStmt{
		Distinct: true, Columns: []query.Expression{id("val")}, From: tref("t"),
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectGroupByHaving(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "grp", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(3), sr("b")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{id("grp"), fn("COUNT", star())},
		From: tref("t"), GroupBy: []query.Expression{id("grp")},
		Having: binop(fn("COUNT", star()), query.TokenGt, nr(1)),
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectScalar(t *testing.T) {
	c := newTestCatalog(t)
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{binop(nr(1), query.TokenPlus, nr(2))},
	})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_SelectLike(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("apple")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("banana")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		Where: &query.LikeExpr{Expr: id("name"), Pattern: sr("app%")},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectBetween(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(5)})
	insertTestRow(t, c, "t", []query.Expression{nr(10)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		Where: &query.BetweenExpr{Expr: id("id"), Lower: nr(2), Upper: nr(8)},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectIn(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	insertTestRow(t, c, "t", []query.Expression{nr(3)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		Where: &query.InExpr{Expr: id("id"), List: []query.Expression{nr(1), nr(3)}},
	})
	if len(rows) != 2 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectIsNull(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), nl()})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("x")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		Where: &query.IsNullExpr{Expr: id("val")},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectCast(t *testing.T) {
	c := newTestCatalog(t)
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{&query.CastExpr{Expr: sr("42"), DataType: query.TokenInteger}},
	})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_SelectCase(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{&query.CaseExpr{
			Whens: []*query.WhenClause{{Condition: binop(id("id"), query.TokenEq, nr(1)), Result: sr("one")}},
			Else:  sr("other"),
		}},
		From: tref("t"),
	})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_SelectCount(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{fn("COUNT", star())}, From: tref("t"),
	})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_SelectAlias(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	cols, _ := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{&query.AliasExpr{Expr: id("id"), Alias: "myid"}},
		From:    tref("t"),
	})
	if cols[0] != "myid" {
		t.Fatal(cols[0])
	}
}

func TestCov_SelectJoin(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "a", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	createTestTable(t, c, "b", []*query.ColumnDef{{Name: "aid", Type: query.TokenInteger, PrimaryKey: true}, {Name: "info", Type: query.TokenText}})
	insertTestRow(t, c, "a", []query.Expression{nr(1), sr("x")})
	insertTestRow(t, c, "b", []query.Expression{nr(1), sr("y")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{qi("a", "val"), qi("b", "info")},
		From:    tref("a"),
		Joins:   []*query.JoinClause{{Table: tref("b"), Type: query.TokenInner, Condition: binop(qi("a", "id"), query.TokenEq, qi("b", "aid"))}},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectLeftJoin(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "a", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	createTestTable(t, c, "b", []*query.ColumnDef{{Name: "aid", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "a", []query.Expression{nr(1)})
	insertTestRow(t, c, "a", []query.Expression{nr(2)})
	insertTestRow(t, c, "b", []query.Expression{nr(1)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{qi("a", "id"), qi("b", "aid")},
		From:    tref("a"),
		Joins:   []*query.JoinClause{{Table: tref("b"), Type: query.TokenLeft, Condition: binop(qi("a", "id"), query.TokenEq, qi("b", "aid"))}},
	})
	if len(rows) != 2 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectJoinGroupBy(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "o", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "cid", Type: query.TokenInteger}})
	createTestTable(t, c, "c", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}})
	insertTestRow(t, c, "c", []query.Expression{nr(1), sr("alice")})
	insertTestRow(t, c, "o", []query.Expression{nr(1), nr(1)})
	insertTestRow(t, c, "o", []query.Expression{nr(2), nr(1)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{qi("c", "name"), fn("COUNT", star())},
		From:    tref("o"),
		Joins:   []*query.JoinClause{{Table: tref("c"), Type: query.TokenInner, Condition: binop(qi("o", "cid"), query.TokenEq, qi("c", "id"))}},
		GroupBy: []query.Expression{qi("c", "name")},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectSubqueryIn(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t1", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	createTestTable(t, c, "t2", []*query.ColumnDef{{Name: "val", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t1", []query.Expression{nr(1)})
	insertTestRow(t, c, "t1", []query.Expression{nr(2)})
	insertTestRow(t, c, "t2", []query.Expression{nr(1)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t1"),
		Where: &query.InExpr{Expr: id("id"), Subquery: &query.SelectStmt{Columns: []query.Expression{id("val")}, From: tref("t2")}},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectExists(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t1", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	createTestTable(t, c, "t2", []*query.ColumnDef{{Name: "val", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t1", []query.Expression{nr(1)})
	insertTestRow(t, c, "t2", []query.Expression{nr(1)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t1"),
		Where: &query.ExistsExpr{Subquery: &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t2")}},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectWindow(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{id("id"), &query.WindowExpr{Function: "ROW_NUMBER", OrderBy: []*query.OrderByExpr{{Expr: id("id")}}}},
		From:    tref("t"),
	})
	if len(rows) != 2 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectWindowPartition(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "grp", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(3), sr("b")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{id("id"), &query.WindowExpr{Function: "RANK", PartitionBy: []query.Expression{id("grp")}, OrderBy: []*query.OrderByExpr{{Expr: id("id")}}}},
		From:    tref("t"),
	})
	if len(rows) != 3 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectFunctions(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("Hello World")})
	fns := []query.Expression{
		fn("LOWER", id("name")), fn("TRIM", sr("  x  ")),
		fn("SUBSTR", id("name"), nr(1), nr(5)), fn("REPLACE", id("name"), sr("World"), sr("Go")),
		fn("ROUND", nr(3.14159), nr(2)), fn("IFNULL", nl(), sr("def")),
		fn("TYPEOF", id("name")), fn("NULLIF", nr(1), nr(1)),
		fn("INSTR", id("name"), sr("lo")), fn("COALESCE", nl(), sr("ok")),
	}
	for _, f := range fns {
		_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{f}, From: tref("t")})
		if len(rows) != 1 {
			t.Fatal()
		}
	}
}

func TestCov_SelectMultiAgg(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenReal}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), nr(10)})
	insertTestRow(t, c, "t", []query.Expression{nr(2), nr(20)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{fn("MIN", id("val")), fn("MAX", id("val")), fn("SUM", id("val")), fn("AVG", id("val"))},
		From:    tref("t"),
	})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_SelectGroupConcat(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "grp", Type: query.TokenText}, {Name: "name", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a"), sr("x")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("a"), sr("y")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{id("grp"), fn("GROUP_CONCAT", id("name"))},
		From:    tref("t"), GroupBy: []query.Expression{id("grp")},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectCountDistinct(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("a")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{id("val")}, Distinct: true}},
		From:    tref("t"),
	})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_SelectComparisons(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(5)})
	ops := []query.TokenType{query.TokenNeq, query.TokenLt, query.TokenGte, query.TokenLte}
	vals := []float64{3, 10, 5, 5}
	for i, op := range ops {
		_, rows := sel(t, c, &query.SelectStmt{
			Columns: []query.Expression{star()}, From: tref("t"),
			Where: binop(id("id"), op, nr(vals[i])),
		})
		if len(rows) != 1 {
			t.Fatal(op, len(rows))
		}
	}
}

func TestCov_SelectArith(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(10)})
	arithOps := []query.TokenType{query.TokenStar, query.TokenSlash, query.TokenPercent, query.TokenMinus}
	for _, op := range arithOps {
		_, rows := sel(t, c, &query.SelectStmt{
			Columns: []query.Expression{binop(id("id"), op, nr(3))}, From: tref("t"),
		})
		if len(rows) != 1 {
			t.Fatal()
		}
	}
}

func TestCov_SelectFromView(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	c.CreateView("v", &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("v")})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectNotLikeNotBetweenNotIn(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("apple")})
	insertTestRow(t, c, "t", []query.Expression{nr(5), sr("banana")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		Where: &query.LikeExpr{Expr: id("name"), Pattern: sr("app%"), Not: true},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
	_, rows = sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		Where: &query.BetweenExpr{Expr: id("id"), Lower: nr(2), Upper: nr(8), Not: true},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
	_, rows = sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		Where: &query.InExpr{Expr: id("id"), List: []query.Expression{nr(1)}, Not: true},
	})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_SelectConcat(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "a", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("hi")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{binop(id("a"), query.TokenConcat, sr(" world"))}, From: tref("t"),
	})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_SelectMultiOrderBy(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "grp", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("b")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("a")})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		OrderBy: []*query.OrderByExpr{{Expr: id("grp")}, {Expr: id("id")}},
	})
	if len(rows) != 2 {
		t.Fatal()
	}
}

func TestCov_ScalarAggregate(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("SUM", id("id"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

// ── INSERT ───────────────────────────────────────────────────────────
func TestCov_InsertColList(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Columns: []string{"id", "val"}, Values: [][]query.Expression{{nr(1), sr("x")}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_InsertMulti(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Values: [][]query.Expression{{nr(1)}, {nr(2)}, {nr(3)}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_InsertAutoInc(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true}, {Name: "val", Type: query.TokenText}})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Columns: []string{"val"}, Values: [][]query.Expression{{sr("a")}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_InsertNotNull(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText, NotNull: true}})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Values: [][]query.Expression{{nr(1), nl()}}}, nil)
	if err == nil {
		t.Fatal("expected NOT NULL error")
	}
}

func TestCov_InsertNonexistent(t *testing.T) {
	c := newTestCatalog(t)
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "nope", Values: [][]query.Expression{{nr(1)}}}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_InsertPlaceholders(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Values: [][]query.Expression{{&query.PlaceholderExpr{Index: 0}}}}, []interface{}{float64(1)})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_InsertDefault(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText, Default: sr("def")}})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Columns: []string{"id"}, Values: [][]query.Expression{{nr(1)}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

// ── UPDATE ───────────────────────────────────────────────────────────
func TestCov_UpdateBasic(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("old")})
	_, _, err := c.Update(context.Background(), &query.UpdateStmt{Table: "t", Set: []*query.SetClause{{Column: "val", Value: sr("new")}}, Where: binop(id("id"), query.TokenEq, nr(1))}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_UpdateNoWhere(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("b")})
	_, _, err := c.Update(context.Background(), &query.UpdateStmt{Table: "t", Set: []*query.SetClause{{Column: "val", Value: sr("x")}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_UpdateWithIndex(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_val", Table: "t", Columns: []string{"val"}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("old")})
	_, _, err := c.Update(context.Background(), &query.UpdateStmt{Table: "t", Set: []*query.SetClause{{Column: "val", Value: sr("new")}}, Where: binop(id("id"), query.TokenEq, nr(1))}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

// ── DELETE ───────────────────────────────────────────────────────────
func TestCov_DeleteBasic(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	_, n, err := c.Delete(context.Background(), &query.DeleteStmt{Table: "t", Where: binop(id("id"), query.TokenEq, nr(1))}, nil)
	if err != nil || n != 1 {
		t.Fatal(err, n)
	}
}

func TestCov_DeleteAll(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	_, n, err := c.Delete(context.Background(), &query.DeleteStmt{Table: "t"}, nil)
	if err != nil || n != 2 {
		t.Fatal(err, n)
	}
}

func TestCov_DeleteWithIndex(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_del_val", Table: "t", Columns: []string{"val"}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("x")})
	_, _, err := c.Delete(context.Background(), &query.DeleteStmt{Table: "t"}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_DeleteRow(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("x")})
	err := c.DeleteRow(context.Background(), "t", float64(1))
	if err != nil {
		t.Fatal(err)
	}
}

// ── DDL ──────────────────────────────────────────────────────────────
func TestCov_CreateTableIfNotExists(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	err := c.CreateTable(&query.CreateTableStmt{Table: "t", IfNotExists: true, Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_DropTable(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	if err := c.DropTable(&query.DropTableStmt{Table: "t"}); err != nil {
		t.Fatal(err)
	}
}

func TestCov_AlterAddColumn(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	err := c.AlterTableAddColumn(&query.AlterTableStmt{Table: "t", Column: query.ColumnDef{Name: "extra", Type: query.TokenText}})
	if err != nil {
		t.Fatal(err)
	}
	tbl, _ := c.GetTable("t")
	if len(tbl.Columns) != 2 {
		t.Fatal(len(tbl.Columns))
	}
}

func TestCov_AlterDropColumn(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "extra", Type: query.TokenText}})
	err := c.AlterTableDropColumn(&query.AlterTableStmt{Table: "t", NewName: "extra"})
	if err != nil {
		t.Fatal(err)
	}
	tbl, _ := c.GetTable("t")
	if len(tbl.Columns) != 1 {
		t.Fatal(len(tbl.Columns))
	}
}

func TestCov_AlterRename(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	if err := c.AlterTableRename(&query.AlterTableStmt{Table: "t", NewName: "t2"}); err != nil {
		t.Fatal(err)
	}
}

func TestCov_AlterRenameCol(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "old_col", Type: query.TokenText}})
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "t", OldName: "old_col", NewName: "new_col"}); err != nil {
		t.Fatal(err)
	}
}

func TestCov_CreateDropIndex(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	if err := c.CreateIndex(&query.CreateIndexStmt{Index: "idx1", Table: "t", Columns: []string{"val"}}); err != nil {
		t.Fatal(err)
	}
	if err := c.DropIndex("idx1"); err != nil {
		t.Fatal(err)
	}
}

func TestCov_UniqueIndex(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "email", Type: query.TokenText}})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_u", Table: "t", Columns: []string{"email"}, Unique: true})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a@b.com")})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Values: [][]query.Expression{{nr(2), sr("a@b.com")}}}, nil)
	if err == nil {
		t.Fatal("expected unique violation")
	}
}

// ── Transactions ─────────────────────────────────────────────────────
func TestCov_TxnInsertRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.BeginTransaction(1)
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	c.RollbackTransaction()
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if len(rows) != 0 {
		t.Fatal(len(rows))
	}
}

func TestCov_TxnUpdateRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("old")})
	c.BeginTransaction(2)
	c.Update(context.Background(), &query.UpdateStmt{Table: "t", Set: []*query.SetClause{{Column: "val", Value: sr("new")}}, Where: binop(id("id"), query.TokenEq, nr(1))}, nil)
	c.RollbackTransaction()
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{id("val")}, From: tref("t")})
	if rows[0][0] != "old" {
		t.Fatal(rows[0][0])
	}
}

func TestCov_TxnDeleteRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	c.BeginTransaction(3)
	c.Delete(context.Background(), &query.DeleteStmt{Table: "t"}, nil)
	c.RollbackTransaction()
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_TxnCommit(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.BeginTransaction(4)
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	c.CommitTransaction()
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
}

func TestCov_TxnCreateTableRollback(t *testing.T) {
	c := newTestCatalog(t)
	c.BeginTransaction(5)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.RollbackTransaction()
	_, _, err := c.Select(&query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_TxnDropTableRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.BeginTransaction(6)
	c.DropTable(&query.DropTableStmt{Table: "t"})
	c.RollbackTransaction()
	_, _, err := c.Select(&query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")}, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCov_TxnIndexRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("x")})
	c.BeginTransaction(7)
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_r", Table: "t", Columns: []string{"val"}})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("y")})
	c.RollbackTransaction()
}

func TestCov_TxnAlterRenameRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.BeginTransaction(8)
	c.AlterTableRename(&query.AlterTableStmt{Table: "t", NewName: "t2"})
	c.RollbackTransaction()
	if _, err := c.GetTable("t"); err != nil {
		t.Fatal(err)
	}
}

func TestCov_TxnAlterRenameColRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "old_name", Type: query.TokenText}})
	c.BeginTransaction(9)
	c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "t", OldName: "old_name", NewName: "new_name"})
	c.RollbackTransaction()
}

func TestCov_TxnCreateIndexRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	c.BeginTransaction(10)
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_ci", Table: "t", Columns: []string{"val"}})
	c.RollbackTransaction()
}

func TestCov_TxnDropIndexRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_di", Table: "t", Columns: []string{"val"}})
	c.BeginTransaction(11)
	c.DropIndex("idx_di")
	c.RollbackTransaction()
}

// ── Savepoints ───────────────────────────────────────────────────────
func TestCov_Savepoint(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.BeginTransaction(20)
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	c.Savepoint("sp1")
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	c.RollbackToSavepoint("sp1")
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
	c.CommitTransaction()
}

func TestCov_SavepointRelease(t *testing.T) {
	c := newTestCatalog(t)
	c.BeginTransaction(21)
	c.Savepoint("sp1")
	c.ReleaseSavepoint("sp1")
	c.CommitTransaction()
}

func TestCov_SavepointNotInTxn(t *testing.T) {
	c := newTestCatalog(t)
	if err := c.Savepoint("sp1"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_SavepointAlterAddColRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.BeginTransaction(22)
	c.Savepoint("sp1")
	c.AlterTableAddColumn(&query.AlterTableStmt{Table: "t", Column: query.ColumnDef{Name: "extra", Type: query.TokenText}})
	c.RollbackToSavepoint("sp1")
	tbl, _ := c.GetTable("t")
	if len(tbl.Columns) != 1 {
		t.Fatal(len(tbl.Columns))
	}
	c.CommitTransaction()
}

// ── RLS ──────────────────────────────────────────────────────────────
func TestCov_RLSInternalDisabled(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	ctx := context.Background()
	cols, rows, err := c.ApplyRLSFilter(ctx, "t", []string{"id"}, [][]interface{}{{1}}, "user1", nil)
	if err != nil || len(cols) == 0 || len(rows) == 0 {
		t.Fatal(err)
	}
	ok, err := c.CheckRLSForInsert(ctx, "t", map[string]interface{}{"id": 1}, "u", nil)
	if err != nil || !ok {
		t.Fatal()
	}
	ok, _ = c.CheckRLSForUpdate(ctx, "t", map[string]interface{}{"id": 1}, "u", nil)
	if !ok {
		t.Fatal()
	}
	ok, _ = c.CheckRLSForDelete(ctx, "t", map[string]interface{}{"id": 1}, "u", nil)
	if !ok {
		t.Fatal()
	}
}

func TestCov_RLSInternalEnabled(t *testing.T) {
	c := newTestCatalog(t)
	c.EnableRLS()
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	ctx := context.Background()
	cols, rows, err := c.ApplyRLSFilter(ctx, "t", []string{"id"}, [][]interface{}{{1}}, "user1", nil)
	if err != nil || len(cols) == 0 {
		t.Fatal(err)
	}
	_ = rows
	ok, _ := c.CheckRLSForInsert(ctx, "t", map[string]interface{}{"id": 1}, "u", nil)
	_ = ok
	ok, _ = c.CheckRLSForUpdate(ctx, "t", map[string]interface{}{"id": 1}, "u", nil)
	_ = ok
	ok, _ = c.CheckRLSForDelete(ctx, "t", map[string]interface{}{"id": 1}, "u", nil)
	_ = ok
}

func TestCov_SetRLSContext(t *testing.T) {
	c := newTestCatalog(t)
	c.SetRLSContext(context.Background())
}

func TestCov_RLSPolicy(t *testing.T) {
	c := newTestCatalog(t)
	c.EnableRLS()
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.CreateRLSPolicy(&security.Policy{Name: "p1", TableName: "t", Expression: "id > 0", Enabled: true})
	c.DropRLSPolicy("t", "p1")
}

// ── Query Cache ──────────────────────────────────────────────────────
func TestCov_QueryCache(t *testing.T) {
	c := newTestCatalog(t)
	c.EnableQueryCache(10, time.Minute)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	_, _, statsSize := c.GetQueryCacheStats()
	if statsSize < 0 {
		t.Fatal()
	}
	c.DisableQueryCache()
}

// ── Utility functions ────────────────────────────────────────────────
func TestCov_EncodeRow(t *testing.T) {
	data, err := encodeRow([]query.Expression{nr(1), sr("hello"), nl()}, nil)
	if err != nil || len(data) == 0 {
		t.Fatal(err)
	}
}

func TestCov_DecodeRowPad(t *testing.T) {
	data, _ := encodeRow([]query.Expression{nr(1)}, nil)
	row, err := decodeRow(data, 3)
	if err != nil || len(row) != 3 {
		t.Fatal(err, len(row))
	}
}

func TestCov_TypeTaggedKey(t *testing.T) {
	k1 := typeTaggedKey(nil)
	k2 := typeTaggedKey("hello")
	k3 := typeTaggedKey(float64(42))
	k4 := typeTaggedKey(true)
	if k1 == k2 || k2 == k3 || k3 == k4 {
		t.Fatal()
	}
}

func TestCov_TokenTypeToColumnType(t *testing.T) {
	if tokenTypeToColumnType(query.TokenInteger) != "INTEGER" {
		t.Fatal()
	}
	if tokenTypeToColumnType(query.TokenText) != "TEXT" {
		t.Fatal()
	}
}

func TestCov_MatchLike(t *testing.T) {
	if !matchLikeSimple("anything", "%") {
		t.Fatal()
	}
	if !matchLikeSimple("apple", "a%") {
		t.Fatal()
	}
	if matchLikeSimple("banana", "a%") {
		t.Fatal()
	}
	if !matchLikeSimple("apple", "_pple") {
		t.Fatal()
	}
}

func TestCov_EvalExpr(t *testing.T) {
	v, _ := EvalExpression(nr(42), nil)
	if v != float64(42) {
		t.Fatal(v)
	}
	v, _ = EvalExpression(sr("hi"), nil)
	if v != "hi" {
		t.Fatal(v)
	}
	v, _ = EvalExpression(nl(), nil)
	if v != nil {
		t.Fatal(v)
	}
	v, _ = EvalExpression(bl(true), nil)
	if v != true {
		t.Fatal(v)
	}
}

func TestCov_EvalWhereNil(t *testing.T) {
	c := newTestCatalog(t)
	ok, _ := evaluateWhere(c, nil, nil, nil, nil)
	if !ok {
		t.Fatal()
	}
}

func TestCov_BuildCompIdxKey(t *testing.T) {
	table := &TableDef{
		Name:    "t",
		Columns: []ColumnDef{{Name: "a"}, {Name: "b"}},
	}
	table.buildColumnIndexCache()
	idx := &IndexDef{TableName: "t", Columns: []string{"a", "b"}}
	key, ok := buildCompositeIndexKey(table, idx, []interface{}{"x", "y"})
	if !ok || key == "" {
		t.Fatal()
	}
}

func TestCov_ExprToSQL(t *testing.T) {
	s := exprToSQL(nr(42))
	if s == "" {
		t.Fatal()
	}
	s = exprToSQL(id("col"))
	if s == "" {
		t.Fatal()
	}
}

func TestCov_CacheUtils(t *testing.T) {
	if containsSubquery(nil) {
		t.Fatal()
	}
	if !containsSubquery(&query.SubqueryExpr{}) {
		t.Fatal()
	}
	if !isCacheableQuery(&query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")}) {
		t.Fatal()
	}
	if !containsNonDeterministicFunctions(&query.SelectStmt{Columns: []query.Expression{fn("RANDOM")}, From: tref("t")}) {
		t.Fatal()
	}
}

// ── Maintenance ──────────────────────────────────────────────────────
func TestCov_SaveLoad(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	c2 := New(c.tree, c.pool, nil)
	if err := c2.Load(); err != nil {
		t.Fatal(err)
	}
}

func TestCov_Vacuum(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	if err := c.Vacuum(); err != nil {
		t.Fatal(err)
	}
}

func TestCov_Analyze(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	if err := c.Analyze("t"); err != nil {
		t.Fatal(err)
	}
}

func TestCov_ListTables(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t1", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	createTestTable(t, c, "t2", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	if len(c.ListTables()) != 2 {
		t.Fatal()
	}
}

func TestCov_SaveDataLoadSchemaLoadData(t *testing.T) {
	c := newTestCatalog(t)
	c.SaveData(".")
	c.LoadSchema(".")
	c.LoadData(".")
}

func TestCov_HasTableOrView(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	if !c.HasTableOrView("t") {
		t.Fatal()
	}
	c.CreateView("v", &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if !c.HasTableOrView("v") {
		t.Fatal()
	}
	if c.HasTableOrView("nope") {
		t.Fatal()
	}
}

func TestCov_FlushTableTrees(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	if err := c.FlushTableTrees(); err != nil {
		t.Fatal(err)
	}
}

func TestCov_TxnHelpers(t *testing.T) {
	c := newTestCatalog(t)
	if c.IsTransactionActive() {
		t.Fatal()
	}
	c.BeginTransaction(42)
	if !c.IsTransactionActive() {
		t.Fatal()
	}
	if c.TxnID() != 42 {
		t.Fatal()
	}
	c.CommitTransaction()
}

func TestCov_ViewTriggerProcedure(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "vtp", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.CreateView("vtpv", &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("vtp")})
	c.DropView("vtpv")
	c.CreateTrigger(&query.CreateTriggerStmt{Name: "vt1", Table: "vtp", Time: "AFTER", Event: "INSERT", Body: []query.Statement{}})
	if len(c.GetTriggersForTable("vtp", "INSERT")) == 0 {
		t.Fatal()
	}
	c.DropTrigger("vt1")
	c.CreateProcedure(&query.CreateProcedureStmt{Name: "vp1"})
	p, _ := c.GetProcedure("vp1")
	if p == nil {
		t.Fatal()
	}
	c.DropProcedure("vp1")
}

// ── Additional Function Coverage ─────────────────────────────────────
func TestCov_FuncPrintf(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(42)})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("PRINTF", sr("val=%d"), id("id"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
	_, rows = sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("PRINTF", sr("s=%s f=%f"), sr("hi"), nr(3.14))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncConcatWS(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("x")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("CONCAT_WS", sr(","), sr("a"), sr("b"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncReverse(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("hello")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("REVERSE", id("a"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncRepeat(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("ab")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("REPEAT", id("a"), nr(3))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncLeftRight(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("hello")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("LEFT", id("a"), nr(3)), fn("RIGHT", id("a"), nr(3))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncLpadRpad(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("hi")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("LPAD", id("a"), nr(5), sr("0")), fn("RPAD", id("a"), nr(5), sr("0"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncHex(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(255)})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("HEX", id("a"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
	_, rows = sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("HEX", sr("AB"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncIIF(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("IIF", binop(id("a"), query.TokenGt, nr(0)), sr("pos"), sr("neg"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncUnicodeCharZeroblob(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("A")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("UNICODE", id("a")), fn("CHAR", nr(65), nr(66)), fn("ZEROBLOB", nr(4))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncQuoteGlob(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("it's")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("QUOTE", id("a")), fn("QUOTE", nl()), fn("GLOB", sr("hel*"), sr("hello"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncAbsFloorCeil(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenReal, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(-3.7)})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("ABS", id("a")), fn("FLOOR", id("a")), fn("CEIL", id("a"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncConcatDateNowRandom(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("2024-01-01")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{
		fn("CONCAT", sr("a"), sr("b")),
		fn("DATE", id("a")),
		fn("STRFTIME", sr("%Y"), id("a")),
		fn("NOW"),
		fn("RANDOM"),
	}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_FuncLtrimRtrim(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("  hello  ")})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("LTRIM", id("a")), fn("RTRIM", id("a"))}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

// ── Simple CASE Expression ───────────────────────────────────────────
func TestCov_SimpleCaseExpr(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "val", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	caseExpr := &query.CaseExpr{
		Expr: id("val"),
		Whens: []*query.WhenClause{
			{Condition: nr(1), Result: sr("one")},
			{Condition: nr(2), Result: sr("two")},
		},
		Else: sr("other"),
	}
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{caseExpr}, From: tref("t")})
	if len(rows) != 2 {
		t.Fatal()
	}
}

// ── UnaryExpr NOT and negative ───────────────────────────────────────
func TestCov_UnaryExpr(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(5)})
	notExpr := &query.UnaryExpr{Operator: query.TokenNot, Expr: binop(id("a"), query.TokenGt, nr(10))}
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{notExpr}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
	negExpr := &query.UnaryExpr{Operator: query.TokenMinus, Expr: id("a")}
	_, rows = sel(t, c, &query.SelectStmt{Columns: []query.Expression{negExpr}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

// ── GetView ──────────────────────────────────────────────────────────
func TestCov_GetView(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenInteger, PrimaryKey: true}})
	c.CreateView("v1", &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	v, err := c.GetView("v1")
	if err != nil || v == nil {
		t.Fatal(err)
	}
	_, err = c.GetView("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── Vacuum with data and indexes ─────────────────────────────────────
func TestCov_VacuumWithData(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("b")})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_vac", Table: "t", Columns: []string{"name"}})
	if err := c.Vacuum(); err != nil {
		t.Fatal(err)
	}
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if len(rows) != 2 {
		t.Fatal(len(rows))
	}
}

// ── CAST via CastExpr ────────────────────────────────────────────────
func TestCov_CastViaExpr(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("42")})
	castInt := &query.CastExpr{Expr: id("a"), DataType: query.TokenInteger}
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{castInt}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
	castReal := &query.CastExpr{Expr: id("a"), DataType: query.TokenReal}
	_, rows = sel(t, c, &query.SelectStmt{Columns: []query.Expression{castReal}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal()
	}
}

// ── CAST via function call path ──────────────────────────────────────
func TestCov_FuncCast(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenText, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{sr("3.14")})
	for _, typ := range []string{"INTEGER", "REAL", "TEXT", "BOOLEAN"} {
		_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{fn("CAST", id("a"), sr(typ))}, From: tref("t")})
		if len(rows) != 1 {
			t.Fatal(typ)
		}
	}
}

// ── Binary expression with NULL ──────────────────────────────────────
func TestCov_BinaryExprNull(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenInteger, PrimaryKey: true}, {Name: "b", Type: query.TokenInteger}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), nl()})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{binop(id("b"), query.TokenEq, nr(1)), binop(id("b"), query.TokenNeq, nr(1))},
		From:    tref("t"),
	})
	if len(rows) != 1 {
		t.Fatal()
	}
}

func TestCov_BinaryExprNullLogic(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "a", Type: query.TokenInteger, PrimaryKey: true}, {Name: "b", Type: query.TokenInteger}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), nl()})
	// NULL AND false = false, NULL OR true = true, NULL AND true = NULL, NULL OR false = NULL
	for _, op := range []query.TokenType{query.TokenAnd, query.TokenOr} {
		for _, val := range []bool{true, false} {
			e := binop(id("b"), op, bl(val))
			_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{e}, From: tref("t")})
			if len(rows) != 1 {
				t.Fatal()
			}
		}
	}
}

// ── EvalExpression comprehensive ─────────────────────────────────────
func TestCov_EvalExprBinary(t *testing.T) {
	ops := []struct {
		op  query.TokenType
		l   query.Expression
		r   query.Expression
	}{
		{query.TokenPlus, nr(10), nr(5)},
		{query.TokenMinus, nr(10), nr(3)},
		{query.TokenStar, nr(4), nr(3)},
		{query.TokenSlash, nr(10), nr(4)},
		{query.TokenPercent, nr(10), nr(3)},
		{query.TokenConcat, sr("a"), sr("b")},
		{query.TokenEq, nr(5), nr(5)},
		{query.TokenNeq, nr(5), nr(3)},
		{query.TokenLt, nr(3), nr(5)},
		{query.TokenGt, nr(5), nr(3)},
		{query.TokenLte, nr(5), nr(5)},
		{query.TokenGte, nr(5), nr(5)},
		{query.TokenAnd, bl(true), bl(false)},
		{query.TokenOr, bl(true), bl(false)},
	}
	for _, o := range ops {
		_, err := EvalExpression(binop(o.l, o.op, o.r), nil)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCov_EvalExprNullBranches(t *testing.T) {
	v, _ := EvalExpression(binop(nl(), query.TokenEq, nr(1)), nil)
	if v != nil {
		t.Fatal(v)
	}
	v, _ = EvalExpression(binop(nl(), query.TokenNeq, nr(1)), nil)
	if v != nil {
		t.Fatal(v)
	}
}

func TestCov_EvalExprUnary(t *testing.T) {
	v, _ := EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: bl(true)}, nil)
	if v != false {
		t.Fatal(v)
	}
	v, _ = EvalExpression(&query.UnaryExpr{Operator: query.TokenMinus, Expr: nr(5)}, nil)
	if v != int64(-5) {
		t.Fatal(v)
	}
	v, _ = EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: nl()}, nil)
	if v != nil {
		t.Fatal(v)
	}
}

func TestCov_EvalExprFunctions(t *testing.T) {
	fns := []struct {
		expr query.Expression
	}{
		{fn("ABS", nr(-5))},
		{fn("FLOOR", nr(3.7))},
		{fn("CEIL", nr(3.2))},
		{fn("LENGTH", sr("hello"))},
		{fn("UPPER", sr("hi"))},
		{fn("LOWER", sr("HI"))},
	}
	for _, f := range fns {
		_, err := EvalExpression(f.expr, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCov_EvalExprCase(t *testing.T) {
	v, _ := EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{{Condition: bl(true), Result: sr("yes")}},
	}, nil)
	if v != "yes" {
		t.Fatal(v)
	}
	v, _ = EvalExpression(&query.CaseExpr{
		Expr:  nr(1),
		Whens: []*query.WhenClause{{Condition: nr(1), Result: sr("one")}},
	}, nil)
	if v != "one" {
		t.Fatal(v)
	}
	v, _ = EvalExpression(&query.CaseExpr{
		Expr:  nl(),
		Whens: []*query.WhenClause{{Condition: nl(), Result: sr("null")}},
		Else:  sr("else"),
	}, nil)
	if v != "else" {
		t.Fatal(v)
	}
}

func TestCov_EvalExprPlaceholder(t *testing.T) {
	v, _ := EvalExpression(&query.PlaceholderExpr{Index: 0}, []interface{}{"hello"})
	if v != "hello" {
		t.Fatal(v)
	}
	_, err := EvalExpression(&query.PlaceholderExpr{Index: 5}, []interface{}{"hello"})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ── Additional transaction rollback paths ────────────────────────────
func TestCov_TxnAlterAddColumnRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.BeginTransaction(100)
	c.AlterTableAddColumn(&query.AlterTableStmt{Table: "t", Column: query.ColumnDef{Name: "extra", Type: query.TokenText}})
	c.RollbackTransaction()
	tbl, _ := c.GetTable("t")
	if len(tbl.Columns) != 1 {
		t.Fatal("expected 1 column after rollback")
	}
}

func TestCov_TxnAlterDropColumnRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}})
	c.BeginTransaction(101)
	c.AlterTableDropColumn(&query.AlterTableStmt{Table: "t", NewName: "name"})
	c.RollbackTransaction()
	tbl, _ := c.GetTable("t")
	if len(tbl.Columns) != 2 {
		t.Fatal("expected 2 columns after rollback")
	}
}

func TestCov_TxnDeleteWithIndexRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_dwr", Table: "t", Columns: []string{"name"}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("alice")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("bob")})
	c.BeginTransaction(500)
	c.Delete(context.Background(), &query.DeleteStmt{Table: "t", Where: binop(id("id"), query.TokenEq, nr(1))}, nil)
	c.RollbackTransaction()
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if len(rows) != 2 {
		t.Fatal(len(rows))
	}
}

func TestCov_TxnUpdateWithIndexRollback(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}})
	c.CreateIndex(&query.CreateIndexStmt{Index: "idx_uwr", Table: "t", Columns: []string{"name"}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("alice")})
	c.BeginTransaction(501)
	c.Update(context.Background(), &query.UpdateStmt{Table: "t", Set: []*query.SetClause{{Column: "name", Value: sr("bob")}}, Where: binop(id("id"), query.TokenEq, nr(1))}, nil)
	c.RollbackTransaction()
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{id("name")}, From: tref("t")})
	if len(rows) != 1 || rows[0][0] != "alice" {
		t.Fatal(rows)
	}
}

// ── Nested savepoints ────────────────────────────────────────────────
func TestCov_SavepointNested(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	c.BeginTransaction(300)
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	c.Savepoint("sp1")
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	c.Savepoint("sp2")
	insertTestRow(t, c, "t", []query.Expression{nr(3)})
	c.RollbackToSavepoint("sp2")
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if len(rows) != 2 {
		t.Fatal(len(rows))
	}
	c.RollbackToSavepoint("sp1")
	_, rows = sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	if len(rows) != 1 {
		t.Fatal(len(rows))
	}
	c.CommitTransaction()
}

// ── Error paths ──────────────────────────────────────────────────────
func TestCov_SelectNonexistentTable(t *testing.T) {
	c := newTestCatalog(t)
	_, _, err := c.Select(&query.SelectStmt{Columns: []query.Expression{star()}, From: tref("nope")}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_UpdateNonexistentTable(t *testing.T) {
	c := newTestCatalog(t)
	_, _, err := c.Update(context.Background(), &query.UpdateStmt{Table: "nope", Set: []*query.SetClause{{Column: "a", Value: sr("x")}}}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_DeleteNonexistentTable(t *testing.T) {
	c := newTestCatalog(t)
	_, _, err := c.Delete(context.Background(), &query.DeleteStmt{Table: "nope"}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_DropNonexistentTable(t *testing.T) {
	c := newTestCatalog(t)
	err := c.DropTable(&query.DropTableStmt{Table: "nope"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_DropNonexistentIndex(t *testing.T) {
	c := newTestCatalog(t)
	err := c.DropIndex("nope")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_DropViewNonexistent(t *testing.T) {
	c := newTestCatalog(t)
	err := c.DropView("nope")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCov_RollbackToSavepointNotFound(t *testing.T) {
	c := newTestCatalog(t)
	c.BeginTransaction(400)
	c.Savepoint("sp1")
	err := c.RollbackToSavepoint("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	c.CommitTransaction()
}

func TestCov_ReleaseSavepointNotFound(t *testing.T) {
	c := newTestCatalog(t)
	c.BeginTransaction(401)
	c.Savepoint("sp1")
	err := c.ReleaseSavepoint("nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
	c.CommitTransaction()
}

// ── QueryCache ───────────────────────────────────────────────────────
func TestCov_QueryCacheEviction(t *testing.T) {
	qc := NewQueryCache(2, time.Minute)
	qc.Set("k1", nil, nil, []string{"t"})
	qc.Set("k2", nil, nil, []string{"t"})
	qc.Set("k3", nil, nil, []string{"t"})
	_, ok := qc.Get("k1")
	if ok {
		t.Fatal("expected eviction")
	}
	_, ok = qc.Get("k3")
	if !ok {
		t.Fatal("k3 should exist")
	}
	qc.InvalidateAll()
	_, _, statsSize := qc.Stats()
	if statsSize != 0 {
		t.Fatal(statsSize)
	}
}

func TestCov_QueryCacheStale(t *testing.T) {
	qc := NewQueryCache(10, time.Millisecond)
	qc.Set("k1", nil, nil, []string{"t"})
	time.Sleep(5 * time.Millisecond)
	_, ok := qc.Get("k1")
	if ok {
		t.Fatal("expected stale")
	}
}

// ── EvalContext ──────────────────────────────────────────────────────
func TestCov_EvalContext(t *testing.T) {
	c := newTestCatalog(t)
	ctx := NewEvalContext(c, nil, nil, nil)
	if ctx.Catalog != c {
		t.Fatal()
	}
	ctx2 := ctx.WithTable(nil, "test")
	if ctx2.TableName != "test" {
		t.Fatal()
	}
}

// ── Dotted identifier and QualifiedIdentifier fallback ───────────────
func TestCov_DottedIdentifier(t *testing.T) {
	c := newTestCatalog(t)
	cols := []ColumnDef{{Name: "a", sourceTbl: "t"}}
	row := []interface{}{"hello"}
	v, err := evaluateExpression(c, row, cols, &query.Identifier{Name: "t.a"}, nil)
	if err != nil || v != "hello" {
		t.Fatal(v, err)
	}
}

func TestCov_QualifiedIdentFallback(t *testing.T) {
	c := newTestCatalog(t)
	cols := []ColumnDef{{Name: "a"}}
	row := []interface{}{"val"}
	v, err := evaluateExpression(c, row, cols, qi("x", "a"), nil)
	if err != nil || v != "val" {
		t.Fatal(v, err)
	}
}

// ── containsSubquery branches ────────────────────────────────────────
func TestCov_ContainsSubquery(t *testing.T) {
	if containsSubquery(nil) {
		t.Fatal()
	}
	if !containsSubquery(&query.SubqueryExpr{}) {
		t.Fatal()
	}
	if !containsSubquery(&query.ExistsExpr{}) {
		t.Fatal()
	}
	if containsSubquery(nr(1)) {
		t.Fatal()
	}
	if !containsSubquery(binop(&query.SubqueryExpr{}, query.TokenEq, nr(1))) {
		t.Fatal()
	}
	if containsSubquery(&query.InExpr{Subquery: &query.SelectStmt{}}) {
		t.Fatal()
	}
	if containsSubquery(&query.InExpr{List: []query.Expression{nr(1)}}) {
		t.Fatal()
	}
}

func TestCov_HasNonDeterministicFn(t *testing.T) {
	if !hasNonDeterministicFunction(fn("RANDOM")) {
		t.Fatal()
	}
	if !hasNonDeterministicFunction(fn("NOW")) {
		t.Fatal()
	}
	if hasNonDeterministicFunction(fn("UPPER", sr("x"))) {
		t.Fatal()
	}
	if !hasNonDeterministicFunction(binop(fn("RANDOM"), query.TokenPlus, nr(1))) {
		t.Fatal()
	}
}

// ── encodeRow edge cases ─────────────────────────────────────────────
func TestCov_EncodeRowBool(t *testing.T) {
	data, err := encodeRow([]query.Expression{bl(true), nl()}, nil)
	if err != nil || len(data) == 0 {
		t.Fatal(err)
	}
}

// ── exprToSQL more branches ──────────────────────────────────────────
func TestCov_ExprToSQLMore(t *testing.T) {
	s := exprToSQL(nr(42))
	if s == "" {
		t.Fatal()
	}
	s = exprToSQL(sr("hello"))
	if s == "" {
		t.Fatal()
	}
	s = exprToSQL(nl())
	if s == "" {
		t.Fatal()
	}
	s = exprToSQL(bl(true))
	if s == "" {
		t.Fatal()
	}
	s = exprToSQL(binop(id("a"), query.TokenEq, nr(1)))
	if s == "" {
		t.Fatal()
	}
	s = exprToSQL(fn("UPPER", id("a")))
	if s == "" {
		t.Fatal()
	}
}

// ── GetRow, UpdateRow ────────────────────────────────────────────────
func TestCov_GetRow(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("hello")})
	row, err := c.GetRow("t", float64(1))
	if err != nil || row == nil {
		t.Fatal(err)
	}
}

func TestCov_UpdateRow(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("hello")})
	err := c.UpdateRow("t", float64(1), map[string]interface{}{"id": float64(1), "val": "world"})
	if err != nil {
		t.Fatal(err)
	}
}

// ── FTS ──────────────────────────────────────────────────────────────
func TestCov_FTS(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "body", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("the quick brown fox")})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("the lazy dog")})
	if err := c.CreateFTSIndex("fts_body", "t", []string{"body"}); err != nil {
		t.Fatal(err)
	}
	results, err := c.SearchFTS("fts_body", "quick")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected FTS results")
	}
	c.DropFTSIndex("fts_body")
}

// ── Materialized View ────────────────────────────────────────────────
func TestCov_MaterializedView(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	c.CreateMaterializedView("mv1", &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")})
	mv, err := c.GetMaterializedView("mv1")
	if err != nil || mv == nil {
		t.Fatal(err)
	}
	c.RefreshMaterializedView("mv1")
	mvs := c.ListMaterializedViews()
	if len(mvs) != 1 {
		t.Fatal(len(mvs))
	}
	c.DropMaterializedView("mv1")
}

// ── JSON Index ───────────────────────────────────────────────────────
func TestCov_JSONIndex(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "data", Type: query.TokenJSON}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr(`{"name":"alice"}`)})
	err := c.CreateJSONIndex("ji1", "t", "data", "$.name", "TEXT")
	if err != nil {
		t.Fatal(err)
	}
	results, _ := c.QueryJSONIndex("ji1", "alice")
	_ = results
	c.DropJSONIndex("ji1")
}

// ── GetTableStats ────────────────────────────────────────────────────
func TestCov_GetTableStats(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	c.Analyze("t")
	stats, err := c.GetTableStats("t")
	if err != nil {
		t.Fatal(err)
	}
	_ = stats
}

// ── SELECT ORDER BY DESC ─────────────────────────────────────────────
func TestCov_SelectOrderDesc(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	insertTestRow(t, c, "t", []query.Expression{nr(3)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		OrderBy: []*query.OrderByExpr{{Expr: id("id"), Desc: true}},
	})
	if len(rows) != 3 {
		t.Fatal(rows)
	}
}

// ── SELECT with expression alias ─────────────────────────────────────
func TestCov_SelectExprAlias(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	aliasExpr := &query.AliasExpr{Expr: binop(id("id"), query.TokenStar, nr(2)), Alias: "doubled"}
	cols, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{aliasExpr}, From: tref("t")})
	if len(rows) != 1 || cols[0] != "doubled" {
		t.Fatal(cols, rows)
	}
}

// ── SELECT LIMIT=0 ──────────────────────────────────────────────────
func TestCov_SelectLimitZero(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	_, rows := sel(t, c, &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t"), Limit: nr(0)})
	if len(rows) != 0 {
		t.Fatal(len(rows))
	}
}

// ── Foreign Key basic enforcement ────────────────────────────────────
func TestCov_ForeignKeyInsert(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "parent", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "parent", []query.Expression{nr(1)})
	c.CreateTable(&query.CreateTableStmt{
		Table: "child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "pid", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"pid"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"id"},
		}},
	})
	insertTestRow(t, c, "child", []query.Expression{nr(1), nr(1)})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "child", Values: [][]query.Expression{{nr(2), nr(99)}}}, nil)
	if err == nil {
		t.Fatal("expected FK violation")
	}
}

// ── CREATE TABLE with CHECK and UNIQUE ───────────────────────────────
func TestCov_CreateTableCheck(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "age", Type: query.TokenInteger, Check: binop(id("age"), query.TokenGt, nr(0))},
	})
	insertTestRow(t, c, "t", []query.Expression{nr(1), nr(25)})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Values: [][]query.Expression{{nr(2), nr(-1)}}}, nil)
	if err == nil {
		t.Fatal("expected check constraint violation")
	}
}

func TestCov_CreateTableUnique(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText, Unique: true},
	})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a@b.com")})
	_, _, err := c.Insert(context.Background(), &query.InsertStmt{Table: "t", Values: [][]query.Expression{{nr(2), sr("a@b.com")}}}, nil)
	if err == nil {
		t.Fatal("expected unique violation")
	}
}

// ── evaluateWhere ────────────────────────────────────────────────────
func TestCov_EvalWhereWithExprs(t *testing.T) {
	c := newTestCatalog(t)
	cols := []ColumnDef{{Name: "a"}, {Name: "b"}}
	row := []interface{}{float64(5), "hello"}
	matched, err := evaluateWhere(c, row, cols, binop(id("a"), query.TokenGt, nr(3)), nil)
	if err != nil || !matched {
		t.Fatal()
	}
}

// ── DisableQueryCache ────────────────────────────────────────────────
func TestCov_QueryCacheDisable(t *testing.T) {
	c := newTestCatalog(t)
	c.EnableQueryCache(10, time.Minute)
	_, _, statsSize := c.GetQueryCacheStats()
	if statsSize < 0 {
		t.Fatal()
	}
	c.DisableQueryCache()
	_,_,statsSize = c.GetQueryCacheStats()
	if statsSize != 0 {
		t.Fatal("expected nil after disable")
	}
}

// ── matchLikeSimple edge cases ───────────────────────────────────────
func TestCov_MatchLikeEdge(t *testing.T) {
	if !matchLikeSimple("aXXb", "a%b") {
		t.Fatal()
	}
	if matchLikeSimple("aXXc", "a%b") {
		t.Fatal()
	}
	if !matchLikeSimple("aXb", "a_b") {
		t.Fatal()
	}
	if matchLikeSimple("ab", "a_b") {
		t.Fatal()
	}
	if matchLikeSimple("", "_") {
		t.Fatal()
	}
}


// ── Direct evaluateExpression for function edge cases ────────────────
func TestCov_FuncEdgeCases(t *testing.T) {
	c := newTestCatalog(t)
	// Test functions with nil args (NULL handling)
	funcsNil := []string{"LENGTH", "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM", "ABS", "ROUND", "FLOOR", "CEIL", "REVERSE", "REPEAT", "LEFT", "RIGHT", "LPAD", "RPAD", "HEX", "TYPEOF", "UNICODE"}
	for _, name := range funcsNil {
		v, err := evaluateExpression(c, nil, nil, fn(name, nl()), nil)
		_ = v
		_ = err
	}
	// Test functions with too few args
	funcsErr := []string{"LENGTH", "UPPER", "LOWER", "TRIM", "SUBSTR", "ABS", "ROUND", "FLOOR", "CEIL", "NULLIF", "REPLACE", "INSTR", "PRINTF", "STRFTIME", "REVERSE", "REPEAT", "LEFT", "RIGHT", "LPAD", "RPAD", "HEX", "TYPEOF", "UNICODE", "ZEROBLOB", "QUOTE", "GLOB", "IIF", "CONCAT_WS", "CAST"}
	for _, name := range funcsErr {
		v, err := evaluateExpression(c, nil, nil, &query.FunctionCall{Name: name, Args: nil}, nil)
		_ = v
		_ = err
	}
	// COALESCE/IFNULL with all nil
	v, _ := evaluateExpression(c, nil, nil, fn("COALESCE", nl(), nl()), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("IFNULL", nl(), nl()), nil)
	_ = v
	// NULLIF(nil, nil)
	v, _ = evaluateExpression(c, nil, nil, fn("NULLIF", nl(), nl()), nil)
	_ = v
	// REPLACE with nil
	v, _ = evaluateExpression(c, nil, nil, fn("REPLACE", nl(), sr("a"), sr("b")), nil)
	_ = v
	// INSTR with nil
	v, _ = evaluateExpression(c, nil, nil, fn("INSTR", nl(), sr("a")), nil)
	_ = v
	// SUBSTR edge cases
	v, _ = evaluateExpression(c, nil, nil, fn("SUBSTR", nl(), nr(1)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("SUBSTR", sr("hello"), nl()), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("SUBSTR", sr("hello"), nr(1), nl()), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("SUBSTR", sr("hello"), nr(0)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("SUBSTR", sr("hello"), nr(100)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("SUBSTR", sr("hello"), nr(1), nr(-1)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("SUBSTR", sr("hello"), nr(1), nr(100)), nil)
	_ = v
	// ABS with positive (no negate) and non-numeric
	v, _ = evaluateExpression(c, nil, nil, fn("ABS", nr(5)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("ABS", sr("abc")), nil)
	_ = v
	// ROUND edge cases
	v, _ = evaluateExpression(c, nil, nil, fn("ROUND", nr(3.14)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("ROUND", sr("abc")), nil)
	_ = v
	// FLOOR/CEIL with non-numeric
	v, _ = evaluateExpression(c, nil, nil, fn("FLOOR", sr("abc")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("CEIL", sr("abc")), nil)
	_ = v
	// REPEAT(x, 0)
	v, _ = evaluateExpression(c, nil, nil, fn("REPEAT", sr("a"), nr(0)), nil)
	_ = v
	// LEFT/RIGHT edge cases
	v, _ = evaluateExpression(c, nil, nil, fn("LEFT", sr("hi"), nr(0)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("LEFT", sr("hi"), nr(100)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("RIGHT", sr("hi"), nr(0)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("RIGHT", sr("hi"), nr(100)), nil)
	_ = v
	// LPAD/RPAD edge cases
	v, _ = evaluateExpression(c, nil, nil, fn("LPAD", sr("hello"), nr(0), sr("x")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("LPAD", sr("hello"), nr(3), sr("")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("LPAD", sr("hello"), nr(3), sr("x")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("RPAD", sr("hello"), nr(0), sr("x")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("RPAD", sr("hello"), nr(3), sr("")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("RPAD", sr("hello"), nr(3), sr("x")), nil)
	_ = v
	// ZEROBLOB(0)
	v, _ = evaluateExpression(c, nil, nil, fn("ZEROBLOB", nr(0)), nil)
	_ = v
	// IIF with various condition types
	v, _ = evaluateExpression(c, nil, nil, fn("IIF", nr(0), sr("t"), sr("f")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("IIF", sr("truthy"), sr("t"), sr("f")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("IIF", nl(), sr("t"), sr("f")), nil)
	_ = v
	// UNICODE with empty string
	v, _ = evaluateExpression(c, nil, nil, fn("UNICODE", sr("")), nil)
	_ = v
	// GROUP_CONCAT scalar fallback
	v, _ = evaluateExpression(c, nil, nil, fn("GROUP_CONCAT", sr("hello")), nil)
	_ = v
	// TYPEOF with various types
	v, _ = evaluateExpression(c, nil, nil, fn("TYPEOF", nr(3.14)), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("TYPEOF", bl(true)), nil)
	_ = v
	// CONCAT with nil
	v, _ = evaluateExpression(c, nil, nil, fn("CONCAT", sr("a"), nl(), sr("b")), nil)
	_ = v
	// CONCAT_WS with nil separator
	v, _ = evaluateExpression(c, nil, nil, fn("CONCAT_WS", nl(), sr("a")), nil)
	_ = v
	// DATE with no args
	v, _ = evaluateExpression(c, nil, nil, &query.FunctionCall{Name: "DATE", Args: nil}, nil)
	_ = v
	// STRFTIME with nil
	v, _ = evaluateExpression(c, nil, nil, fn("STRFTIME", sr("%Y"), nl()), nil)
	_ = v
	// GLOB with nil
	v, _ = evaluateExpression(c, nil, nil, fn("GLOB", nl(), sr("hello")), nil)
	_ = v
	// CAST with various types
	v, _ = evaluateExpression(c, nil, nil, fn("CAST", bl(true), sr("INTEGER")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("CAST", nr(1), sr("BOOLEAN")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("CAST", sr("hello"), sr("FLOAT")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("CAST", nl(), sr("INTEGER")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("CAST", sr("42"), sr("INTEGER")), nil)
	_ = v
	v, _ = evaluateExpression(c, nil, nil, fn("CAST", sr("true"), sr("BOOLEAN")), nil)
	_ = v
}

// ── RLS internal functions with enabled table ────────────────────────
func TestCov_RLSInternalWithPolicy(t *testing.T) {
	c := newTestCatalog(t)
	c.EnableRLS()
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "owner", Type: query.TokenText}})
	mgr := c.GetRLSManager()
	if mgr != nil {
		mgr.EnableTable("t")
		mgr.CreatePolicy(&security.Policy{Name: "pol1", TableName: "t", Type: security.PolicySelect, Expression: "owner = current_user", Enabled: true})
	}
	ctx := context.Background()
	cols, rows, err := c.applyRLSFilterInternal(ctx, "t", []string{"id", "owner"}, [][]interface{}{{float64(1), "alice"}, {float64(2), "bob"}}, "alice", []string{"user"})
	_ = cols
	_ = rows
	_ = err
	ok, _ := c.checkRLSForInsertInternal(ctx, "t", map[string]interface{}{"id": 1, "owner": "alice"}, "alice", []string{"user"})
	_ = ok
	ok, _ = c.checkRLSForUpdateInternal(ctx, "t", map[string]interface{}{"id": 1, "owner": "alice"}, "alice", []string{"user"})
	_ = ok
	ok, _ = c.checkRLSForDeleteInternal(ctx, "t", map[string]interface{}{"id": 1, "owner": "alice"}, "alice", []string{"user"})
	_ = ok
}

// ── tokenTypeToColumnType more branches ──────────────────────────────
func TestCov_TokenTypeToColumnTypeMore(t *testing.T) {
	types := []query.TokenType{query.TokenReal, query.TokenBlob, query.TokenBoolean, query.TokenJSON}
	for _, tt := range types {
		s := tokenTypeToColumnType(tt)
		if s == "" {
			t.Fatal(tt)
		}
	}
}

// ── evaluateWhere with more expr types ───────────────────────────────
func TestCov_EvalWhereTypes(t *testing.T) {
	c := newTestCatalog(t)
	cols := []ColumnDef{{Name: "a"}, {Name: "b"}}
	row := []interface{}{"hello", float64(5)}
	m, _ := evaluateWhere(c, row, cols, &query.LikeExpr{Expr: id("a"), Pattern: sr("hel%")}, nil)
	if !m { t.Fatal() }
	m, _ = evaluateWhere(c, row, cols, &query.BetweenExpr{Expr: id("b"), Lower: nr(1), Upper: nr(10)}, nil)
	if !m { t.Fatal() }
	m, _ = evaluateWhere(c, row, cols, &query.InExpr{Expr: id("b"), List: []query.Expression{nr(5), nr(10)}}, nil)
	if !m { t.Fatal() }
	m, _ = evaluateWhere(c, row, cols, &query.IsNullExpr{Expr: id("a"), Not: true}, nil)
	if !m { t.Fatal() }
}

// ── HAVING more branches ─────────────────────────────────────────────
func TestCov_HavingWithAgg(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "grp", Type: query.TokenText}, {Name: "val", Type: query.TokenReal}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), sr("a"), nr(10)})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("a"), nr(20)})
	insertTestRow(t, c, "t", []query.Expression{nr(3), sr("b"), nr(5)})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{id("grp"), fn("SUM", id("val"))},
		From: tref("t"), GroupBy: []query.Expression{id("grp")},
		Having: binop(fn("SUM", id("val")), query.TokenGt, nr(10)),
	})
	if len(rows) != 1 { t.Fatal(len(rows)) }
}

// ── ORDER BY with NULLs ─────────────────────────────────────────────
func TestCov_OrderByNull(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}})
	insertTestRow(t, c, "t", []query.Expression{nr(1), nl()})
	insertTestRow(t, c, "t", []query.Expression{nr(2), sr("a")})
	insertTestRow(t, c, "t", []query.Expression{nr(3), nl()})
	_, rows := sel(t, c, &query.SelectStmt{
		Columns: []query.Expression{star()}, From: tref("t"),
		OrderBy: []*query.OrderByExpr{{Expr: id("val")}},
	})
	if len(rows) != 3 { t.Fatal(len(rows)) }
}

// ── CTE basic ────────────────────────────────────────────────────────
func TestCov_CTEBasic(t *testing.T) {
	c := newTestCatalog(t)
	createTestTable(t, c, "t", []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}})
	insertTestRow(t, c, "t", []query.Expression{nr(1)})
	insertTestRow(t, c, "t", []query.Expression{nr(2)})
	_, rows, err := c.ExecuteCTE(
		&query.SelectStmtWithCTE{
			CTEs: []*query.CTEDef{{Name: "cte", Query: &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("t")}}},
			Select: &query.SelectStmt{Columns: []query.Expression{star()}, From: tref("cte")},
		},
		nil,
	)
	if err != nil { t.Fatal(err) }
	if len(rows) != 2 { t.Fatal(len(rows)) }
}

// ── evaluateBinaryExpr IS operator ───────────────────────────────────
func TestCov_BinaryExprIS(t *testing.T) {
	c := newTestCatalog(t)
	v, _ := evaluateExpression(c, nil, nil, binop(nl(), query.TokenIs, bl(true)), nil)
	_ = v
}
