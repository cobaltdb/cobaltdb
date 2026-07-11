package optimizer

import (
	"reflect"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestOptimizeNoRewritePaths(t *testing.T) {
	o := New(nil, nil)
	got, err := o.Optimize(nil)
	if err != nil || got != nil {
		t.Fatalf("Optimize(nil) = (%v, %v), want (nil, nil)", got, err)
	}

	stmt := &query.SelectStmt{From: &query.TableRef{Name: "users"}}
	got, err = o.Optimize(stmt)
	if err != nil {
		t.Fatalf("Optimize without joins: %v", err)
	}
	if got != stmt {
		t.Fatal("statement without joins should be returned unchanged")
	}

	config := DefaultConfig()
	config.EnableJoinReorder = false
	o = New(config, nil)
	stmt.Joins = []*query.JoinClause{{Type: query.TokenInner, Table: &query.TableRef{Name: "orders"}}}
	got, err = o.Optimize(stmt)
	if err != nil {
		t.Fatalf("Optimize with join reordering disabled: %v", err)
	}
	if got != stmt {
		t.Fatal("disabled join reordering should return the original statement")
	}
}

func TestReorderJoinsHonorsLimitAndSpecialJoins(t *testing.T) {
	config := DefaultConfig()
	config.MaxJoinReorderTables = 1
	o := New(config, nil)
	stmt := &query.SelectStmt{Joins: []*query.JoinClause{
		{Type: query.TokenInner, Table: &query.TableRef{Name: "a"}},
		{Type: query.TokenInner, Table: &query.TableRef{Name: "b"}},
	}}
	if got := o.reorderJoins(stmt); got != stmt || got.Joins[0].Table.Name != "a" {
		t.Fatalf("join limit must preserve input order: %#v", got.Joins)
	}

	config.MaxJoinReorderTables = 6
	o = New(config, nil)
	stmt = &query.SelectStmt{Joins: []*query.JoinClause{
		nil,
		{Type: query.TokenJoin, Table: &query.TableRef{Name: "large"}},
		{Type: query.TokenInner, Natural: true, Table: &query.TableRef{Name: "natural"}},
	}}
	o.UpdateStatistics("large", &TableStatistics{RowCount: 20_000})
	got := o.reorderJoins(stmt)
	if got.Joins[0] != nil || got.Joins[1].Table.Name != "large" || got.Joins[2].Table.Name != "natural" {
		t.Fatalf("nil, bare, and natural joins changed positions: %#v", got.Joins)
	}
}

func TestEstimateJoinSelectivityAllInputs(t *testing.T) {
	o := New(nil, &Statistics{TableStats: map[string]*TableStatistics{
		"cross": {RowCount: 1},
		"right": {RowCount: 1},
	}})
	if got := o.estimateJoinSelectivity(&query.JoinClause{}); got != 0.3 {
		t.Fatalf("nil table selectivity = %v, want 0.3", got)
	}
	if got := o.estimateJoinSelectivity(&query.JoinClause{Type: query.TokenCross, Table: &query.TableRef{Name: "cross"}}); got != 1 {
		t.Fatalf("cross join selectivity = %v, want 1", got)
	}
	if got := o.estimateJoinSelectivity(&query.JoinClause{Type: query.TokenRight, Table: &query.TableRef{Name: "right"}}); got != 0.5 {
		t.Fatalf("right join selectivity = %v, want 0.5", got)
	}
}

func TestSelectBestIndexMissingInputs(t *testing.T) {
	o := New(nil, nil)
	if got := o.SelectBestIndex("missing", &query.Identifier{Name: "id"}); got != "" {
		t.Fatalf("missing table selected index %q", got)
	}
	o.UpdateStatistics("users", &TableStatistics{IndexStats: map[string]*IndexStatistics{
		"idx": {Columns: []string{"id"}},
	}})
	if got := o.SelectBestIndex("users", &query.NumberLiteral{Value: 1}); got != "" {
		t.Fatalf("expression without columns selected index %q", got)
	}
	if got := o.scoreIndex([]string{"id"}, &IndexStatistics{}); got != 0 {
		t.Fatalf("empty index score = %v, want 0", got)
	}
}

func TestExtractColumnReferencesAllExpressionShapes(t *testing.T) {
	o := New(nil, nil)
	id := func(name string) query.Expression { return &query.Identifier{Name: name} }
	subquery := func(name string) *query.SelectStmt {
		return &query.SelectStmt{Where: &query.Identifier{Name: name}}
	}
	expr := &query.CaseExpr{
		Expr: &query.UnaryExpr{Expr: id("unary")},
		Whens: []*query.WhenClause{
			{
				Condition: &query.InExpr{
					Expr:     id("in_expr"),
					List:     []query.Expression{id("in_item")},
					Subquery: subquery("in_where"),
				},
				Result: &query.LikeExpr{Expr: id("like_expr"), Pattern: id("like_pattern")},
			},
			{
				Condition: &query.IsNullExpr{Expr: id("nullable")},
				Result: &query.FunctionCall{Args: []query.Expression{
					&query.BetweenExpr{Expr: id("between_expr"), Lower: id("lower"), Upper: id("upper")},
					&query.CastExpr{Expr: id("cast")},
				}},
			},
			{
				Condition: &query.ExistsExpr{Subquery: subquery("exists_where")},
				Result:    &query.QualifiedIdentifier{Table: "t", Column: "qualified"},
			},
		},
		Else: &query.BinaryExpr{Left: id("else_left"), Right: id("else_right")},
	}
	want := []string{
		"unary", "in_expr", "in_item", "in_where", "like_expr", "like_pattern",
		"nullable", "between_expr", "lower", "upper", "cast", "exists_where",
		"qualified", "else_left", "else_right",
	}
	if got := o.extractColumnReferences(expr); !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}

	// Nil expressions, and subqueries without WHERE clauses, are valid leaves.
	leaves := &query.FunctionCall{Args: []query.Expression{
		nil,
		&query.InExpr{Subquery: &query.SelectStmt{}},
		&query.ExistsExpr{Subquery: &query.SelectStmt{}},
		&query.ExistsExpr{},
	}}
	if got := o.extractColumnReferences(leaves); len(got) != 0 {
		t.Fatalf("column-free leaves produced %v", got)
	}
}

func TestStatisticsNilSafetyAndDeepCopies(t *testing.T) {
	if cloneConfig(nil) != nil || cloneStatistics(nil) != nil || cloneTableStatistics(nil) != nil {
		t.Fatal("nil clone helpers must return nil")
	}

	o := New(nil, nil)
	o.stats.TableStats = nil
	o.UpdateStatistics("nil", nil)
	if got := o.GetTableStatistics("nil"); got != nil {
		t.Fatalf("nil statistics round trip = %#v, want nil", got)
	}

	input := &Statistics{TableStats: map[string]*TableStatistics{
		"nil-table": nil,
		"table": {
			ColumnStats: map[string]*ColumnStatistics{"nil-column": nil},
			IndexStats: map[string]*IndexStatistics{
				"nil-index":  nil,
				"no-columns": {IndexName: "no-columns"},
			},
		},
	}}
	cloned := cloneStatistics(input)
	if cloned.TableStats["nil-table"] != nil {
		t.Fatal("nil table statistics should remain nil")
	}
	table := cloned.TableStats["table"]
	if _, ok := table.ColumnStats["nil-column"]; ok {
		t.Fatal("nil column statistics should be omitted from a clone")
	}
	if _, ok := table.IndexStats["nil-index"]; ok {
		t.Fatal("nil index statistics should be omitted from a clone")
	}
	if table.IndexStats["no-columns"].Columns != nil {
		t.Fatal("nil index columns should remain nil")
	}
}
