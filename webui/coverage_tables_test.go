package main

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestTableAccumulatorPropagatesNestedSecurityErrors(t *testing.T) {
	bad := &query.CreateTableStmt{Table: "bad"}
	badSelect := &query.SelectStmt{From: &query.TableRef{SubqueryStmt: bad}}
	badExpr := &query.SubqueryExpr{Query: badSelect}
	a := &tableAccumulator{tables: map[string]struct{}{}}
	statements := []query.Statement{
		&query.UpdateStmt{Table: "x", From: &query.TableRef{SubqueryStmt: bad}},
		&query.UpdateStmt{Table: "x", Joins: []*query.JoinClause{{Table: &query.TableRef{SubqueryStmt: bad}}}},
		&query.UpdateStmt{Table: "x", Set: []*query.SetClause{{Value: badExpr}}},
		&query.DeleteStmt{Table: "x", Using: []*query.TableRef{{SubqueryStmt: bad}}},
	}
	for _, stmt := range statements {
		if err := a.walkStatement(stmt, nil); err == nil {
			t.Fatalf("nested error lost for %T", stmt)
		}
	}
	selects := []*query.SelectStmt{
		{From: &query.TableRef{SubqueryStmt: bad}},
		{Joins: []*query.JoinClause{{Table: &query.TableRef{SubqueryStmt: bad}}}},
		{Joins: []*query.JoinClause{{Table: &query.TableRef{Name: "x"}, Condition: badExpr}}},
		{Columns: []query.Expression{badExpr}}, {Where: badExpr}, {GroupBy: []query.Expression{badExpr}}, {Having: badExpr}, {OrderBy: []*query.OrderByExpr{{Expr: badExpr}}},
	}
	for _, s := range selects {
		if err := a.walkSelect(s, nil); err == nil {
			t.Fatal("select nested error lost")
		}
	}
	expressions := []query.Expression{
		&query.InExpr{Expr: badExpr}, &query.InExpr{Expr: &query.Identifier{Name: "x"}, List: []query.Expression{badExpr}},
		&query.BinaryExpr{Left: badExpr}, &query.BinaryExpr{Left: &query.Identifier{Name: "x"}, Right: badExpr},
		&query.FunctionCall{Args: []query.Expression{badExpr}}, &query.FunctionCall{Filter: badExpr},
		&query.BetweenExpr{Expr: badExpr}, &query.BetweenExpr{Expr: &query.Identifier{Name: "x"}, Lower: badExpr}, &query.BetweenExpr{Expr: &query.Identifier{Name: "x"}, Lower: &query.Identifier{Name: "x"}, Upper: badExpr},
		&query.LikeExpr{Expr: badExpr}, &query.LikeExpr{Expr: &query.Identifier{Name: "x"}, Pattern: badExpr}, &query.LikeExpr{Expr: &query.Identifier{Name: "x"}, Pattern: &query.Identifier{Name: "x"}, Escape: badExpr},
		&query.CaseExpr{Expr: badExpr}, &query.CaseExpr{Whens: []*query.WhenClause{{Condition: badExpr}}}, &query.CaseExpr{Whens: []*query.WhenClause{{Condition: &query.Identifier{Name: "x"}, Result: badExpr}}},
		&query.WindowExpr{Args: []query.Expression{badExpr}}, &query.WindowExpr{Filter: badExpr}, &query.WindowExpr{PartitionBy: []query.Expression{badExpr}}, &query.WindowExpr{OrderBy: []*query.OrderByExpr{{Expr: badExpr}}},
		&query.JSONContainsExpr{Column: badExpr}, &query.JSONContainsExpr{Column: &query.Identifier{Name: "x"}, Value: badExpr},
		&query.MatchExpr{Columns: []query.Expression{badExpr}}, &query.MatchExpr{Pattern: badExpr},
		&query.WindowSpec{PartitionBy: []query.Expression{badExpr}}, &query.WindowSpec{OrderBy: []*query.OrderByExpr{{Expr: badExpr}}},
	}
	for _, e := range expressions {
		if err := a.walkExpr(e, nil); err == nil {
			t.Fatalf("expression error lost for %T", e)
		}
	}
}

func TestTableAccumulatorCoversStatementAndExpressionGraph(t *testing.T) {
	leaf := &query.Identifier{Name: "x"}
	sub := &query.SelectStmt{From: &query.TableRef{Name: "sub_table"}, Columns: []query.Expression{leaf}}
	bad := &query.CreateTableStmt{Table: "unsupported"}

	cases := []struct {
		name    string
		stmt    query.Statement
		wantErr bool
	}{
		{"insert-select-values", &query.InsertStmt{Table: "dst", Select: sub, Values: [][]query.Expression{{&query.SubqueryExpr{Query: sub}}}}, false},
		{"update-from-joins-set-where", &query.UpdateStmt{Table: "dst", From: &query.TableRef{Name: "src"}, Joins: []*query.JoinClause{nil, {Table: &query.TableRef{Name: "joined"}}}, Set: []*query.SetClause{nil, {Value: &query.ExistsExpr{Subquery: sub}}}, Where: &query.InExpr{Expr: leaf, Subquery: sub}}, false},
		{"delete-using-where", &query.DeleteStmt{Table: "dst", Using: []*query.TableRef{{Name: "used"}}, Where: &query.SubqueryExpr{Query: sub}}, false},
		{"unsupported", bad, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &tableAccumulator{tables: map[string]struct{}{}}
			err := a.walkStatement(tc.stmt, map[string]struct{}{})
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v", err)
			}
		})
	}

	selectAll := &query.SelectStmt{
		From:    &query.TableRef{Subquery: sub},
		Joins:   []*query.JoinClause{nil, {Table: &query.TableRef{SubqueryStmt: &query.UnionStmt{Left: sub, Right: sub}}, Condition: &query.ExistsExpr{Subquery: sub}}},
		Columns: []query.Expression{&query.SubqueryExpr{Query: sub}},
		Where:   &query.BinaryExpr{Left: &query.SubqueryExpr{Query: sub}, Right: leaf},
		GroupBy: []query.Expression{&query.SubqueryExpr{Query: sub}},
		Having:  &query.SubqueryExpr{Query: sub},
		OrderBy: []*query.OrderByExpr{nil, {Expr: &query.SubqueryExpr{Query: sub}}},
	}
	a := &tableAccumulator{tables: map[string]struct{}{}}
	if err := a.walkSelect(selectAll, map[string]struct{}{}); err != nil {
		t.Fatal(err)
	}
	if err := a.walkSelect(nil, nil); err != nil {
		t.Fatal(err)
	}

	cte := &query.SelectStmtWithCTE{CTEs: []*query.CTEDef{nil, {Name: "c", Query: nil}, {Name: "c", Query: sub}}, Select: &query.SelectStmt{From: &query.TableRef{Name: "c"}}}
	if err := a.walkSelectWithCTE(cte, map[string]struct{}{"outer": {}}); err != nil {
		t.Fatal(err)
	}
	if err := a.walkSelectWithCTE(&query.SelectStmtWithCTE{CTEs: []*query.CTEDef{{Name: "bad", Query: bad}}}, nil); err == nil {
		t.Fatal("nested unsupported CTE accepted")
	}
	if err := a.walkUnion(&query.UnionStmt{Left: bad}, nil); err == nil {
		t.Fatal("unsupported union left accepted")
	}
	if err := a.walkUnion(&query.UnionStmt{Left: sub, Right: sub}, nil); err != nil {
		t.Fatal(err)
	}

	if err := a.walkTableRef(&query.TableRef{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := a.walkTableRef(&query.TableRef{Name: "c"}, map[string]struct{}{"c": {}}); err != nil {
		t.Fatal(err)
	}

	errExprs := []query.Expression{
		&query.InExpr{Expr: &query.SubqueryExpr{Query: nil}, List: []query.Expression{&query.SubqueryExpr{Query: nil}}},
		&query.CaseExpr{Whens: []*query.WhenClause{nil, {Condition: leaf, Result: leaf}}},
		&query.WindowExpr{OrderBy: []*query.OrderByExpr{nil, {Expr: leaf}}},
		&query.WindowSpec{OrderBy: []*query.OrderByExpr{nil, {Expr: leaf}}},
	}
	for _, e := range errExprs {
		if err := a.walkExpr(e, nil); err != nil {
			t.Fatalf("%T: %v", e, err)
		}
	}
	if got := cloneScope(map[string]struct{}{"x": {}}); len(got) != 1 {
		t.Fatal(got)
	}
}
