package engine

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ---------------------------------------------------------------------------
// schemaStringLiteral
// ---------------------------------------------------------------------------

func TestSchemaStringLiteral(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "'hello'"},
		{"", "''"},
		{"it's", "'it''s'"},
		{"'single quotes'", "'''single quotes'''"},
		{"a'b'c", "'a''b''c'"},
	}
	for _, tc := range tests {
		got := schemaStringLiteral(tc.input)
		if got != tc.want {
			t.Errorf("schemaStringLiteral(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// joinTypeToString
// ---------------------------------------------------------------------------

func TestJoinTypeToString(t *testing.T) {
	tests := []struct {
		tok  query.TokenType
		want string
	}{
		{query.TokenInner, "Inner"},
		{query.TokenLeft, "Left"},
		{query.TokenRight, "Right"},
		{query.TokenOuter, "Full Outer"},
		{query.TokenCross, "Cross"},
		{query.TokenJoin, "Inner"}, // default
		{query.TokenFull, "Inner"}, // default
		{query.TokenNatural, "Inner"},
		{99, "Inner"}, // unknown
	}
	for _, tc := range tests {
		got := joinTypeToString(tc.tok)
		if got != tc.want {
			t.Errorf("joinTypeToString(%d) = %q, want %q", tc.tok, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// expressionsToString
// ---------------------------------------------------------------------------

func TestExpressionsToString(t *testing.T) {
	tests := []struct {
		name  string
		exprs []query.Expression
		want  string
	}{
		{
			name:  "nil slice",
			exprs: nil,
			want:  "",
		},
		{
			name:  "empty slice",
			exprs: []query.Expression{},
			want:  "",
		},
		{
			name:  "single identifier",
			exprs: []query.Expression{&query.Identifier{Name: "x"}},
			want:  "x",
		},
		{
			name:  "multiple identifiers",
			exprs: []query.Expression{&query.Identifier{Name: "a"}, &query.Identifier{Name: "b"}, &query.Identifier{Name: "c"}},
			want:  "a, b, c",
		},
		{
			name: "mixed types",
			exprs: []query.Expression{
				&query.Identifier{Name: "col"},
				&query.NumberLiteral{Value: 42, Raw: "42"},
				&query.StringLiteral{Value: "hello"},
			},
			want: "col, 42, 'hello'",
		},
		{
			name: "qualified identifier",
			exprs: []query.Expression{
				&query.QualifiedIdentifier{Table: "t", Column: "c"},
			},
			want: "t.c",
		},
		{
			name: "null and boolean",
			exprs: []query.Expression{
				&query.NullLiteral{},
				&query.BooleanLiteral{Value: true},
			},
			want: "NULL, TRUE",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := expressionsToString(tc.exprs)
			if got != tc.want {
				t.Errorf("expressionsToString(%s) = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// estimateJoinRows
// ---------------------------------------------------------------------------

func TestEstimateJoinRows(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	tests := []struct {
		name      string
		joinType  query.TokenType
		leftRows  int64
		rightRows int64
		want      int64
	}{
		{"cross join", query.TokenCross, 10, 5, 50},
		{"cross join zero", query.TokenCross, 0, 100, 0},
		{"left join", query.TokenLeft, 100, 50, 100},
		{"right join", query.TokenRight, 50, 100, 100},
		{"right join left larger", query.TokenRight, 200, 30, 200},
		{"full outer", query.TokenOuter, 100, 50, 150},
		{"full outer zero", query.TokenOuter, 0, 0, 0},
		{"inner default", query.TokenInner, 100, 50, 500}, // 100 * 50 * 0.1 = 500
		{"inner default small", query.TokenInner, 10, 10, 10},
		{"inner default zero right", query.TokenInner, 100, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			join := &query.JoinClause{Type: tc.joinType}
			got := db.estimateJoinRows(join, tc.leftRows, tc.rightRows)
			if got != tc.want {
				t.Errorf("estimateJoinRows(%s, %d, %d) = %d, want %d", tc.name, tc.leftRows, tc.rightRows, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isProcedureResultStatement
// ---------------------------------------------------------------------------

func TestIsProcedureResultStatement(t *testing.T) {
	tests := []struct {
		name string
		stmt query.Statement
		want bool
	}{
		{"SelectStmt", &query.SelectStmt{}, true},
		{"UnionStmt", &query.UnionStmt{}, true},
		{"SelectStmtWithCTE", &query.SelectStmtWithCTE{}, true},
		{"ShowTablesStmt", &query.ShowTablesStmt{}, true},
		{"ShowCreateTableStmt", &query.ShowCreateTableStmt{}, true},
		{"ShowColumnsStmt", &query.ShowColumnsStmt{}, true},
		{"ShowDatabasesStmt", &query.ShowDatabasesStmt{}, true},
		{"DescribeStmt", &query.DescribeStmt{}, true},
		{"ExplainStmt", &query.ExplainStmt{}, true},
		{"InsertStmt", &query.InsertStmt{}, false},
		{"UpdateStmt", &query.UpdateStmt{}, false},
		{"DeleteStmt", &query.DeleteStmt{}, false},
		{"CallProcedureStmt", &query.CallProcedureStmt{}, false},
		{"CreateTableStmt", &query.CreateTableStmt{}, false},
		{"SetVarStmt", &query.SetVarStmt{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isProcedureResultStatement(tc.stmt)
			if got != tc.want {
				t.Errorf("isProcedureResultStatement(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isProcedureOutputParam
// ---------------------------------------------------------------------------

func TestIsProcedureOutputParam(t *testing.T) {
	params := map[string]*query.ParamDef{
		"out_param":     {Name: "out_param", Mode: query.TokenOut},
		"inout_param":   {Name: "inout_param", Mode: query.TokenInout},
		"in_param":      {Name: "in_param", Mode: query.TokenIn},
		"default_param": {Name: "default_param"},
	}

	tests := []struct {
		name      string
		paramName string
		want      bool
	}{
		{"out param", "out_param", true},
		{"inout param", "inout_param", true},
		{"in param (not output)", "in_param", false},
		{"default mode param", "default_param", false},
		{"non-existent param", "missing_param", false},
		{"out param with spaces", "  out_param  ", true},
		{"empty string", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isProcedureOutputParam(tc.paramName, params)
			if got != tc.want {
				t.Errorf("isProcedureOutputParam(%q) = %v, want %v", tc.paramName, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// evalProcedureSetValue
// ---------------------------------------------------------------------------

func TestEvalProcedureSetValue(t *testing.T) {
	t.Run("empty value returns error", func(t *testing.T) {
		_, err := evalProcedureSetValue("", nil)
		if err == nil {
			t.Fatal("expected error for empty value")
		}
	})

	t.Run("whitespace-only returns error", func(t *testing.T) {
		_, err := evalProcedureSetValue("  ", nil)
		if err == nil {
			t.Fatal("expected error for whitespace-only value")
		}
	})

	t.Run("simple literal", func(t *testing.T) {
		val, err := evalProcedureSetValue("42", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != float64(42) && val != int64(42) {
			t.Errorf("got %v (%T), want a numeric 42", val, val)
		}
	})

	t.Run("string literal", func(t *testing.T) {
		val, err := evalProcedureSetValue("'hello world'", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "hello world" {
			t.Errorf("got %v, want 'hello world'", val)
		}
	})

	t.Run("with param substitution", func(t *testing.T) {
		paramMap := map[string]interface{}{"x": int64(10)}
		val, err := evalProcedureSetValue("x + 5", paramMap)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != float64(15) && val != int64(15) {
			t.Errorf("got %v (%T), want numeric 15", val, val)
		}
	})

	t.Run("null literal", func(t *testing.T) {
		val, err := evalProcedureSetValue("NULL", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != nil {
			t.Errorf("got %v, want nil", val)
		}
	})

	t.Run("bool literal", func(t *testing.T) {
		val, err := evalProcedureSetValue("TRUE", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != true {
			t.Errorf("got %v, want true", val)
		}
	})

	t.Run("invalid SQL", func(t *testing.T) {
		_, err := evalProcedureSetValue("SELECT * FROM t", nil)
		if err == nil {
			t.Fatal("expected error for invalid value SQL (multiple columns)")
		}
	})
}

// ---------------------------------------------------------------------------
// substituteParamsInSelectStmt
// ---------------------------------------------------------------------------

func TestSubstituteParamsInSelectStmt(t *testing.T) {
	t.Run("nil stmt returns nil", func(t *testing.T) {
		result := substituteParamsInSelectStmt(nil, nil)
		if result != nil {
			t.Fatal("expected nil for nil input")
		}
	})

	t.Run("substitute in columns, where, groupby, having, limit, offset, orderby", func(t *testing.T) {
		paramMap := map[string]interface{}{"p1": int64(42)}
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "p1"}},
			Where:   &query.Identifier{Name: "p1"},
			GroupBy: []query.Expression{&query.Identifier{Name: "p1"}},
			Having:  &query.Identifier{Name: "p1"},
			Limit:   &query.Identifier{Name: "p1"},
			Offset:  &query.Identifier{Name: "p1"},
			OrderBy: []*query.OrderByExpr{
				{Expr: &query.Identifier{Name: "p1"}, Desc: true},
			},
		}
		result := substituteParamsInSelectStmt(stmt, paramMap)
		if result == stmt {
			t.Fatal("expected a new stmt, not the same pointer")
		}

		// Verify columns
		if len(result.Columns) != 1 {
			t.Fatalf("expected 1 column, got %d", len(result.Columns))
		}
		if num, ok := result.Columns[0].(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("column: expected NumberLiteral(42), got %T %v", result.Columns[0], result.Columns[0])
		}

		// Verify where
		if num, ok := result.Where.(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("where: expected NumberLiteral(42), got %T %v", result.Where, result.Where)
		}

		// Verify group by
		if len(result.GroupBy) != 1 {
			t.Fatalf("expected 1 groupby, got %d", len(result.GroupBy))
		}
		if num, ok := result.GroupBy[0].(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("groupby: expected NumberLiteral(42), got %T %v", result.GroupBy[0], result.GroupBy[0])
		}

		// Verify having
		if num, ok := result.Having.(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("having: expected NumberLiteral(42), got %T %v", result.Having, result.Having)
		}

		// Verify limit
		if num, ok := result.Limit.(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("limit: expected NumberLiteral(42), got %T %v", result.Limit, result.Limit)
		}

		// Verify offset
		if num, ok := result.Offset.(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("offset: expected NumberLiteral(42), got %T %v", result.Offset, result.Offset)
		}

		// Verify order by
		if len(result.OrderBy) != 1 {
			t.Fatalf("expected 1 orderby, got %d", len(result.OrderBy))
		}
		if num, ok := result.OrderBy[0].Expr.(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("orderby expr: expected NumberLiteral(42), got %T %v", result.OrderBy[0].Expr, result.OrderBy[0].Expr)
		}
		if !result.OrderBy[0].Desc {
			t.Error("orderby desc should be preserved")
		}
	})

	t.Run("preserves unchanged orderby expr when no param match", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "a"}},
			OrderBy: []*query.OrderByExpr{
				{Expr: &query.Identifier{Name: "a"}, Desc: false},
			},
		}
		// paramMap doesn't have "a", so it stays as Identifier
		result := substituteParamsInSelectStmt(stmt, map[string]interface{}{"x": int64(1)})
		if _, ok := result.Columns[0].(*query.Identifier); !ok {
			t.Error("expected identifier to be preserved when no param match")
		}
		if _, ok := result.OrderBy[0].Expr.(*query.Identifier); !ok {
			t.Error("expected orderby identifier to be preserved")
		}
	})

	t.Run("handles nil orderby entries", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "a"}},
			OrderBy: []*query.OrderByExpr{nil, {Expr: &query.Identifier{Name: "a"}}},
		}
		result := substituteParamsInSelectStmt(stmt, nil)
		if len(result.OrderBy) != 2 {
			t.Fatalf("expected 2 orderby entries, got %d", len(result.OrderBy))
		}
		if result.OrderBy[0] != nil {
			t.Error("expected first orderby to be nil")
		}
	})
}

// ---------------------------------------------------------------------------
// substituteParamsInExprs
// ---------------------------------------------------------------------------

func TestSubstituteParamsInExprs(t *testing.T) {
	t.Run("nil returns nil", func(t *testing.T) {
		result := substituteParamsInExprs(nil, nil)
		if result != nil {
			t.Fatal("expected nil for nil input")
		}
	})

	t.Run("empty slice returns empty", func(t *testing.T) {
		result := substituteParamsInExprs([]query.Expression{}, nil)
		if len(result) != 0 {
			t.Fatal("expected empty slice")
		}
	})

	t.Run("substitutes params in expressions", func(t *testing.T) {
		exprs := []query.Expression{
			&query.Identifier{Name: "a"},
			&query.Identifier{Name: "b"},
			&query.NumberLiteral{Value: 3.14},
		}
		paramMap := map[string]interface{}{
			"a": int64(100),
			"b": "hello",
		}
		result := substituteParamsInExprs(exprs, paramMap)
		if len(result) != 3 {
			t.Fatalf("expected 3 results, got %d", len(result))
		}
		// a becomes NumberLiteral(100)
		if num, ok := result[0].(*query.NumberLiteral); !ok || num.Value != 100 {
			t.Errorf("result[0]: expected NumberLiteral(100), got %T %v", result[0], result[0])
		}
		// b becomes StringLiteral("hello")
		if str, ok := result[1].(*query.StringLiteral); !ok || str.Value != "hello" {
			t.Errorf("result[1]: expected StringLiteral('hello'), got %T %v", result[1], result[1])
		}
		// 3.14 unchanged
		if num, ok := result[2].(*query.NumberLiteral); !ok || num.Value != 3.14 {
			t.Errorf("result[2]: expected NumberLiteral(3.14), got %T %v", result[2], result[2])
		}
	})
}

// ---------------------------------------------------------------------------
// substituteParamsInOrderBy
// ---------------------------------------------------------------------------

func TestSubstituteParamsInOrderBy(t *testing.T) {
	t.Run("empty orderby returns nil", func(t *testing.T) {
		result := substituteParamsInOrderBy(nil, nil)
		if result != nil {
			t.Fatal("expected nil")
		}
		result = substituteParamsInOrderBy([]*query.OrderByExpr{}, nil)
		if result != nil {
			t.Fatal("expected nil for empty slice")
		}
	})

	t.Run("substitutes params in orderby", func(t *testing.T) {
		orderBy := []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "p"}, Desc: true},
			{Expr: &query.Identifier{Name: "q"}, Desc: false},
		}
		paramMap := map[string]interface{}{"p": int64(99), "q": "text"}
		result := substituteParamsInOrderBy(orderBy, paramMap)
		if len(result) != 2 {
			t.Fatalf("expected 2, got %d", len(result))
		}
		if num, ok := result[0].Expr.(*query.NumberLiteral); !ok || num.Value != 99 {
			t.Errorf("result[0]: expected NumberLiteral(99), got %T %v", result[0].Expr, result[0].Expr)
		}
		if !result[0].Desc {
			t.Error("result[0].Desc should be true")
		}
		if str, ok := result[1].Expr.(*query.StringLiteral); !ok || str.Value != "text" {
			t.Errorf("result[1]: expected StringLiteral('text'), got %T %v", result[1].Expr, result[1].Expr)
		}
	})

	t.Run("nil orderby entry", func(t *testing.T) {
		orderBy := []*query.OrderByExpr{nil, {Expr: &query.Identifier{Name: "a"}}}
		result := substituteParamsInOrderBy(orderBy, nil)
		if len(result) != 2 {
			t.Fatalf("expected 2, got %d", len(result))
		}
		if result[0] != nil {
			t.Error("expected first entry nil")
		}
	})
}

// ---------------------------------------------------------------------------
// substituteParamsInSetClauses
// ---------------------------------------------------------------------------

func TestSubstituteParamsInSetClauses(t *testing.T) {
	t.Run("substitutes params in set clauses", func(t *testing.T) {
		set := []*query.SetClause{
			{Column: "col1", Value: &query.Identifier{Name: "p1"}},
			{Column: "col2", Value: &query.Identifier{Name: "p2"}},
		}
		paramMap := map[string]interface{}{"p1": int64(77), "p2": "hello"}
		result := substituteParamsInSetClauses(set, paramMap)
		if len(result) != 2 {
			t.Fatalf("expected 2, got %d", len(result))
		}
		if result[0].Column != "col1" {
			t.Errorf("expected column 'col1', got %q", result[0].Column)
		}
		if num, ok := result[0].Value.(*query.NumberLiteral); !ok || num.Value != 77 {
			t.Errorf("result[0].Value: expected NumberLiteral(77), got %T %v", result[0].Value, result[0].Value)
		}
		if result[1].Column != "col2" {
			t.Errorf("expected column 'col2', got %q", result[1].Column)
		}
		if str, ok := result[1].Value.(*query.StringLiteral); !ok || str.Value != "hello" {
			t.Errorf("result[1].Value: expected StringLiteral('hello'), got %T %v", result[1].Value, result[1].Value)
		}
	})

	t.Run("preserves nil value for unmatched param", func(t *testing.T) {
		set := []*query.SetClause{
			{Column: "col1", Value: &query.Identifier{Name: "not_in_map"}},
		}
		result := substituteParamsInSetClauses(set, map[string]interface{}{})
		if id, ok := result[0].Value.(*query.Identifier); !ok || id.Name != "not_in_map" {
			t.Errorf("expected Identifier preserved, got %T %v", result[0].Value, result[0].Value)
		}
	})
}

// ---------------------------------------------------------------------------
// substituteUpsertValuesOrderBy
// ---------------------------------------------------------------------------

func TestSubstituteUpsertValuesOrderBy(t *testing.T) {
	t.Run("empty returns nil", func(t *testing.T) {
		res, err := substituteUpsertValuesOrderBy(nil, nil, nil)
		if err != nil || res != nil {
			t.Fatalf("expected (nil, nil), got (%v, %v)", res, err)
		}
		res, err = substituteUpsertValuesOrderBy([]*query.OrderByExpr{}, nil, nil)
		if err != nil || res != nil {
			t.Fatalf("expected (nil, nil) for empty, got (%v, %v)", res, err)
		}
	})

	t.Run("substitutes VALUES references", func(t *testing.T) {
		orderBy := []*query.OrderByExpr{
			{Expr: &query.FunctionCall{Name: "VALUES", Args: []query.Expression{&query.Identifier{Name: "col1"}}}},
		}
		colPos := map[string]int{"col1": 0}
		row := []query.Expression{&query.NumberLiteral{Value: 100}}
		result, err := substituteUpsertValuesOrderBy(orderBy, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1, got %d", len(result))
		}
		if num, ok := result[0].Expr.(*query.NumberLiteral); !ok || num.Value != 100 {
			t.Errorf("expected NumberLiteral(100), got %T %v", result[0].Expr, result[0].Expr)
		}
	})

	t.Run("preserves non-values identifiers", func(t *testing.T) {
		orderBy := []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "other_col"}, Desc: true},
		}
		colPos := map[string]int{"col1": 0}
		row := []query.Expression{&query.NumberLiteral{Value: 100}}
		result, err := substituteUpsertValuesOrderBy(orderBy, colPos, row)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id, ok := result[0].Expr.(*query.Identifier); !ok || id.Name != "other_col" {
			t.Errorf("expected Identifier('other_col'), got %T %v", result[0].Expr, result[0].Expr)
		}
		if !result[0].Desc {
			t.Error("expected Desc to be preserved")
		}
	})

	t.Run("handles nil orderby entries", func(t *testing.T) {
		orderBy := []*query.OrderByExpr{nil, {Expr: &query.Identifier{Name: "a"}}}
		result, err := substituteUpsertValuesOrderBy(orderBy, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2, got %d", len(result))
		}
		if result[0] != nil {
			t.Error("expected first entry nil")
		}
	})
}

// ---------------------------------------------------------------------------
// buildJoinPlan
// ---------------------------------------------------------------------------

func TestBuildJoinPlan(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	// Create two tables so we can build a join plan
	_, err := db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE t1: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE t2: %v", err)
	}

	t.Run("inner join with condition", func(t *testing.T) {
		pb := newPlanBuilder(db)
		leftID := pb.addNode(0, "Seq Scan", "t1", 1000, 100)
		join := &query.JoinClause{
			Type:      query.TokenInner,
			Table:     &query.TableRef{Name: "t2"},
			Condition: &query.BinaryExpr{Left: &query.Identifier{Name: "t1.id"}, Operator: query.TokenEq, Right: &query.Identifier{Name: "t2.id"}},
		}
		nodeID := db.buildJoinPlan(join, leftID, pb, 0)
		if nodeID == 0 {
			t.Fatal("expected non-zero node ID")
		}
		node := pb.getNode(nodeID)
		if node == nil {
			t.Fatal("expected node to exist")
		}
		if node.Operation != "Inner Join" {
			t.Errorf("expected 'Inner Join', got %q", node.Operation)
		}
		if node.Detail == "" {
			t.Error("expected non-empty detail (join condition)")
		}
	})

	t.Run("left join with using", func(t *testing.T) {
		pb := newPlanBuilder(db)
		leftID := pb.addNode(0, "Seq Scan", "t1", 1000, 100)
		join := &query.JoinClause{
			Type:  query.TokenLeft,
			Table: &query.TableRef{Name: "t2"},
			Using: []string{"id"},
		}
		nodeID := db.buildJoinPlan(join, leftID, pb, 0)
		node := pb.getNode(nodeID)
		if node.Operation != "Left Join" {
			t.Errorf("expected 'Left Join', got %q", node.Operation)
		}
		if node.Detail != "USING (id)" {
			t.Errorf("expected 'USING (id)', got %q", node.Detail)
		}
	})

	t.Run("natural cross join", func(t *testing.T) {
		pb := newPlanBuilder(db)
		leftID := pb.addNode(0, "Seq Scan", "t1", 1000, 100)
		join := &query.JoinClause{
			Type:    query.TokenCross,
			Table:   &query.TableRef{Name: "t2"},
			Natural: true,
		}
		nodeID := db.buildJoinPlan(join, leftID, pb, 0)
		node := pb.getNode(nodeID)
		if node.Operation != "Natural Cross Join" {
			t.Errorf("expected 'Natural Cross Join', got %q", node.Operation)
		}
	})

	t.Run("right join without condition", func(t *testing.T) {
		pb := newPlanBuilder(db)
		leftID := pb.addNode(0, "Seq Scan", "t1", 1000, 100)
		join := &query.JoinClause{
			Type:  query.TokenRight,
			Table: &query.TableRef{Name: "t2"},
		}
		nodeID := db.buildJoinPlan(join, leftID, pb, 0)
		node := pb.getNode(nodeID)
		if node.Operation != "Right Join" {
			t.Errorf("expected 'Right Join', got %q", node.Operation)
		}
	})

	t.Run("full outer join", func(t *testing.T) {
		pb := newPlanBuilder(db)
		leftID := pb.addNode(0, "Seq Scan", "t1", 1000, 100)
		join := &query.JoinClause{
			Type:  query.TokenOuter,
			Table: &query.TableRef{Name: "t2"},
		}
		nodeID := db.buildJoinPlan(join, leftID, pb, 0)
		node := pb.getNode(nodeID)
		if node.Operation != "Full Outer Join" {
			t.Errorf("expected 'Full Outer Join', got %q", node.Operation)
		}
	})
}

// ---------------------------------------------------------------------------
// queryCallProcedure
// ---------------------------------------------------------------------------

func TestQueryCallProcedure(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	// Create a procedure that returns a SELECT result
	_, err := db.Exec(ctx, `CREATE PROCEDURE test_proc()
		BEGIN
			SELECT 1 AS val;
		END`)
	if err != nil {
		t.Fatalf("CREATE PROCEDURE: %v", err)
	}

	t.Run("query call procedure returns result rows", func(t *testing.T) {
		stmt := &query.CallProcedureStmt{Name: "test_proc"}
		rows, err := db.queryCallProcedure(ctx, stmt, nil)
		if err != nil {
			t.Fatalf("queryCallProcedure: %v", err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("expected at least one row")
		}
		var val int64
		if err := rows.Scan(&val); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if val != 1 {
			t.Errorf("expected 1, got %d", val)
		}
	})

	t.Run("query non-existent procedure returns error", func(t *testing.T) {
		stmt := &query.CallProcedureStmt{Name: "nonexistent_proc"}
		_, err := db.queryCallProcedure(ctx, stmt, nil)
		if err == nil {
			t.Fatal("expected error for non-existent procedure")
		}
	})
}

// ---------------------------------------------------------------------------
// runAutoVacuumJob
// ---------------------------------------------------------------------------

func TestRunAutoVacuumJob(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// runAutoVacuumJob with various thresholds to exercise the code paths
	t.Run("low threshold runs without error", func(t *testing.T) {
		err := db.runAutoVacuumJob(0.0)
		if err != nil {
			t.Fatalf("runAutoVacuumJob with threshold 0: %v", err)
		}
	})

	t.Run("high threshold runs without error", func(t *testing.T) {
		err := db.runAutoVacuumJob(1.0)
		if err != nil {
			t.Fatalf("runAutoVacuumJob with threshold 1: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// runCheckpointJob
// ---------------------------------------------------------------------------

func TestRunCheckpointJob(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	// runCheckpointJob calls db.Checkpoint() which is safe on an in-memory DB
	err := db.runCheckpointJob()
	if err != nil {
		t.Fatalf("runCheckpointJob: %v", err)
	}
}

// ---------------------------------------------------------------------------
// runAutoVacuumJob with explicit error logging path
// ---------------------------------------------------------------------------

func TestRunAutoVacuumJobWithNilLogger(t *testing.T) {
	// Create a DB with a nil logger to hit the nil-guard branch
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{
			InMemory:  true,
			CacheSize: 1024,
			Logger:    nil,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	err = db.runAutoVacuumJob(0.0)
	if err != nil {
		t.Fatalf("runAutoVacuumJob with nil logger: %v", err)
	}
}

// ---------------------------------------------------------------------------
// runCheckpointJob with nil logger
// ---------------------------------------------------------------------------

func TestRunCheckpointJobWithNilLogger(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{
			InMemory:  true,
			CacheSize: 1024,
			Logger:    nil,
		},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	err = db.runCheckpointJob()
	if err != nil {
		t.Fatalf("runCheckpointJob with nil logger: %v", err)
	}
}

// ---------------------------------------------------------------------------
// queryCallProcedure with output parameters
// ---------------------------------------------------------------------------

func TestQueryCallProcedureWithOutputParam(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, `CREATE PROCEDURE out_proc(OUT p1 INTEGER)
		BEGIN
			SET p1 = 42;
		END`)
	if err != nil {
		t.Fatalf("CREATE PROCEDURE: %v", err)
	}

	// queryCallProcedure with OUT param should return the output values.
	// OUT params still require a placeholder argument in the CALL (e.g. NULL).
	stmt := &query.CallProcedureStmt{Name: "out_proc"}
	rows, err := db.queryCallProcedure(ctx, stmt, []interface{}{nil})
	if err != nil {
		t.Fatalf("queryCallProcedure: %v", err)
	}
	defer rows.Close()
	// There should be at least one column (the OUT param)
	if len(rows.Columns()) == 0 {
		t.Fatal("expected at least one column (OUT param)")
	}
}

// ---------------------------------------------------------------------------
// evalProcedureSetValue with bool param map
// ---------------------------------------------------------------------------

func TestEvalProcedureSetValueBoolParam(t *testing.T) {
	val, err := evalProcedureSetValue("flag", map[string]interface{}{"flag": true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != true {
		t.Errorf("expected true, got %v", val)
	}
}

// ---------------------------------------------------------------------------
// evalProcedureSetValue with float64 param
// ---------------------------------------------------------------------------

func TestEvalProcedureSetValueFloatParam(t *testing.T) {
	val, err := evalProcedureSetValue("rate", map[string]interface{}{"rate": 3.5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != float64(3.5) {
		t.Errorf("expected 3.5, got %v (%T)", val, val)
	}
}

// ---------------------------------------------------------------------------
// evalProcedureSetValue error cases
// ---------------------------------------------------------------------------

func TestEvalProcedureSetValueParseError(t *testing.T) {
	// Verify that invalid SQL produces an error
	_, err := evalProcedureSetValue("1 2 3", nil)
	if err == nil {
		t.Fatal("expected error for invalid SQL")
	}
}

func TestEvalProcedureSetValueBinaryExpr(t *testing.T) {
	// Test with binary expression that has Both sides substituted
	val, err := evalProcedureSetValue("a + b", map[string]interface{}{"a": int64(3), "b": int64(4)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != float64(7) && val != int64(7) {
		t.Errorf("expected numeric 7, got %v (%T)", val, val)
	}
}

func TestEvalProcedureSetValueBoolExpression(t *testing.T) {
	val, err := evalProcedureSetValue("a = b", map[string]interface{}{"a": int64(1), "b": int64(1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != true {
		t.Errorf("expected true, got %v (%T)", val, val)
	}
}

// ---------------------------------------------------------------------------
// substituteParamsInExprs with nil param map
// ---------------------------------------------------------------------------

func TestSubstituteParamsInExprsNilParamMap(t *testing.T) {
	exprs := []query.Expression{&query.Identifier{Name: "a"}}
	result := substituteParamsInExprs(exprs, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	// With nil paramMap, identifier should remain unchanged
	if _, ok := result[0].(*query.Identifier); !ok {
		t.Errorf("expected Identifier, got %T", result[0])
	}
}

// ---------------------------------------------------------------------------
// substituteParamsInSetClauses with nil param map
// ---------------------------------------------------------------------------

func TestSubstituteParamsInSetClausesNilParamMap(t *testing.T) {
	set := []*query.SetClause{
		{Column: "c1", Value: &query.Identifier{Name: "p1"}},
	}
	result := substituteParamsInSetClauses(set, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1, got %d", len(result))
	}
	// With nil paramMap, identifier should remain unchanged
	if _, ok := result[0].Value.(*query.Identifier); !ok {
		t.Errorf("expected Identifier, got %T", result[0].Value)
	}
}

// ---------------------------------------------------------------------------
// substituteParamsInExpr: comprehensive param type coverage
// ---------------------------------------------------------------------------

func TestSubstituteParamsInExprAllTypes(t *testing.T) {
	t.Run("string param", func(t *testing.T) {
		result := substituteParamsInExpr(&query.Identifier{Name: "s"}, map[string]interface{}{"s": "text"})
		if str, ok := result.(*query.StringLiteral); !ok || str.Value != "text" {
			t.Errorf("expected StringLiteral('text'), got %T %v", result, result)
		}
	})

	t.Run("int param", func(t *testing.T) {
		result := substituteParamsInExpr(&query.Identifier{Name: "n"}, map[string]interface{}{"n": 42})
		if num, ok := result.(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("expected NumberLiteral(42), got %T %v", result, result)
		}
	})

	t.Run("int64 param", func(t *testing.T) {
		result := substituteParamsInExpr(&query.Identifier{Name: "n"}, map[string]interface{}{"n": int64(42)})
		if num, ok := result.(*query.NumberLiteral); !ok || num.Value != 42 {
			t.Errorf("expected NumberLiteral(42), got %T %v", result, result)
		}
	})

	t.Run("float64 param", func(t *testing.T) {
		result := substituteParamsInExpr(&query.Identifier{Name: "n"}, map[string]interface{}{"n": 3.14})
		if num, ok := result.(*query.NumberLiteral); !ok || num.Value != 3.14 {
			t.Errorf("expected NumberLiteral(3.14), got %T %v", result, result)
		}
	})

	t.Run("bool param", func(t *testing.T) {
		result := substituteParamsInExpr(&query.Identifier{Name: "b"}, map[string]interface{}{"b": true})
		if bl, ok := result.(*query.BooleanLiteral); !ok || bl.Value != true {
			t.Errorf("expected BooleanLiteral(true), got %T %v", result, result)
		}
	})

	t.Run("nil param", func(t *testing.T) {
		result := substituteParamsInExpr(&query.Identifier{Name: "n"}, map[string]interface{}{"n": nil})
		if _, ok := result.(*query.NullLiteral); !ok {
			t.Errorf("expected NullLiteral, got %T %v", result, result)
		}
	})
}

// ---------------------------------------------------------------------------
// isProcedureResultStatement with nil stmt
// ---------------------------------------------------------------------------

func TestIsProcedureResultStatementNil(t *testing.T) {
	if isProcedureResultStatement(nil) {
		t.Error("isProcedureResultStatement(nil) should be false")
	}
}
