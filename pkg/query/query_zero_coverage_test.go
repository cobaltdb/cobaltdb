package query

import (
	"fmt"
	"strings"
	"testing"
)

// =============================================================================
// query_utils.go — GenerateQueryKey
// =============================================================================

func TestGenerateQueryKey(t *testing.T) {
	t.Run("sql only, no args", func(t *testing.T) {
		key := GenerateQueryKey("SELECT 1", nil)
		if key != "SELECT 1" {
			t.Errorf("expected %q, got %q", "SELECT 1", key)
		}
	})

	t.Run("sql with empty args slice", func(t *testing.T) {
		key := GenerateQueryKey("SELECT 1", []interface{}{})
		if key != "SELECT 1" {
			t.Errorf("expected %q, got %q", "SELECT 1", key)
		}
	})

	t.Run("sql with int args", func(t *testing.T) {
		key := GenerateQueryKey("SELECT * FROM t WHERE id = ?", []interface{}{42})
		want := "SELECT * FROM t WHERE id = ?|42"
		if key != want {
			t.Errorf("expected %q, got %q", want, key)
		}
	})

	t.Run("sql with multiple mixed args", func(t *testing.T) {
		key := GenerateQueryKey("SELECT * FROM t WHERE a=? AND b=? AND c=?",
			[]interface{}{"hello", 3.14, true})
		want := "SELECT * FROM t WHERE a=? AND b=? AND c=?|hello|3.14|true"
		if key != want {
			t.Errorf("expected %q, got %q", want, key)
		}
	})

	t.Run("sql with nil arg", func(t *testing.T) {
		key := GenerateQueryKey("INSERT INTO t VALUES (?)", []interface{}{nil})
		if !strings.Contains(key, "|<nil>") {
			t.Errorf("expected nil representation in key, got %q", key)
		}
	})
}

// =============================================================================
// query_utils.go — unionHasNonDeterministic
// =============================================================================

func TestUnionHasNonDeterministic(t *testing.T) {
	t.Run("nil union", func(t *testing.T) {
		if unionHasNonDeterministic(nil) {
			t.Error("expected false for nil union")
		}
	})

	t.Run("left SelectStmt with no non-deterministic functions", func(t *testing.T) {
		u := &UnionStmt{
			Left: &SelectStmt{
				Columns: []Expression{&StarExpr{}},
				From:    &TableRef{Name: "t"},
			},
			Right: &SelectStmt{
				Columns: []Expression{&NumberLiteral{Value: 1}},
			},
		}
		if unionHasNonDeterministic(u) {
			t.Error("expected false for union with no non-det functions")
		}
	})

	t.Run("left SelectStmt with non-deterministic function", func(t *testing.T) {
		u := &UnionStmt{
			Left: &SelectStmt{
				Columns: []Expression{
					&FunctionCall{Name: "RANDOM"},
				},
				From: &TableRef{Name: "t"},
			},
			Right: &SelectStmt{Columns: []Expression{&NumberLiteral{Value: 1}}},
		}
		if !unionHasNonDeterministic(u) {
			t.Error("expected true for union with RANDOM in left")
		}
	})

	t.Run("nested UnionStmt on left", func(t *testing.T) {
		innerUnion := &UnionStmt{
			Left: &SelectStmt{
				Columns: []Expression{&NumberLiteral{Value: 1}},
				From:    &TableRef{Name: "t"},
			},
			Right: &SelectStmt{
				Columns: []Expression{&FunctionCall{Name: "UUID"}},
			},
		}
		u := &UnionStmt{
			Left:  innerUnion,
			Right: &SelectStmt{Columns: []Expression{&NumberLiteral{Value: 2}}},
		}
		if !unionHasNonDeterministic(u) {
			t.Error("expected true for nested union with UUID in right")
		}
	})

	t.Run("non-deterministic in right SelectStmt", func(t *testing.T) {
		u := &UnionStmt{
			Left: &SelectStmt{
				Columns: []Expression{&NumberLiteral{Value: 1}},
				From:    &TableRef{Name: "t"},
			},
			Right: &SelectStmt{
				Columns: []Expression{&FunctionCall{Name: "NOW"}},
			},
		}
		if !unionHasNonDeterministic(u) {
			t.Error("expected true for union with NOW in right")
		}
	})

	t.Run("unknown left type returns true", func(t *testing.T) {
		u := &UnionStmt{
			Left:  &InsertStmt{}, // unknown type to unionHasNonDeterministic
			Right: &SelectStmt{Columns: []Expression{&NumberLiteral{Value: 1}}},
		}
		if !unionHasNonDeterministic(u) {
			t.Error("expected true for unknown left type")
		}
	})
}

// =============================================================================
// query_utils.go — collectTablesFromStatement
// =============================================================================

func TestCollectTablesFromStatement(t *testing.T) {
	t.Run("SelectStmt", func(t *testing.T) {
		tables := make(map[string]bool)
		collectTablesFromStatement(&SelectStmt{
			From: &TableRef{Name: "users"},
			Joins: []*JoinClause{
				{Table: &TableRef{Name: "orders"}},
			},
		}, tables)
		if !tables["users"] {
			t.Error("expected 'users' in tables")
		}
		if !tables["orders"] {
			t.Error("expected 'orders' in tables")
		}
	})

	t.Run("UnionStmt", func(t *testing.T) {
		tables := make(map[string]bool)
		collectTablesFromStatement(&UnionStmt{
			Left: &SelectStmt{
				From: &TableRef{Name: "employees"},
			},
			Right: &SelectStmt{
				From: &TableRef{Name: "contractors"},
			},
		}, tables)
		if !tables["employees"] {
			t.Error("expected 'employees' in tables")
		}
		if !tables["contractors"] {
			t.Error("expected 'contractors' in tables")
		}
	})

	t.Run("SelectStmtWithCTE", func(t *testing.T) {
		tables := make(map[string]bool)
		collectTablesFromStatement(&SelectStmtWithCTE{
			CTEs: []*CTEDef{
				{
					Name: "cte1",
					Query: &SelectStmt{
						From: &TableRef{Name: "source"},
					},
				},
			},
			Select: &SelectStmt{
				From: &TableRef{Name: "cte1"},
			},
		}, tables)
		if !tables["source"] {
			t.Error("expected 'source' in tables from CTE")
		}
		if !tables["cte1"] {
			t.Error("expected 'cte1' in tables from main select")
		}
	})
}

// =============================================================================
// ast.go — Walk, walkChildren, WalkSelectStmt
// =============================================================================

// visitCountVisitor counts how many times each visitor method is called.
type visitCountVisitor struct {
	counts map[string]int
}

func (v *visitCountVisitor) VisitBinaryExpr(_ *BinaryExpr, _ interface{}) interface{} {
	v.counts["BinaryExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitUnaryExpr(_ *UnaryExpr, _ interface{}) interface{} {
	v.counts["UnaryExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitFunctionCall(_ *FunctionCall, _ interface{}) interface{} {
	v.counts["FunctionCall"]++
	return nil
}
func (v *visitCountVisitor) VisitIdentifier(_ *Identifier, _ interface{}) interface{} {
	v.counts["Identifier"]++
	return nil
}
func (v *visitCountVisitor) VisitQualifiedIdentifier(_ *QualifiedIdentifier, _ interface{}) interface{} {
	v.counts["QualifiedIdentifier"]++
	return nil
}
func (v *visitCountVisitor) VisitColumnRef(_ *ColumnRef, _ interface{}) interface{} {
	v.counts["ColumnRef"]++
	return nil
}
func (v *visitCountVisitor) VisitStringLiteral(_ *StringLiteral, _ interface{}) interface{} {
	v.counts["StringLiteral"]++
	return nil
}
func (v *visitCountVisitor) VisitNumberLiteral(_ *NumberLiteral, _ interface{}) interface{} {
	v.counts["NumberLiteral"]++
	return nil
}
func (v *visitCountVisitor) VisitBooleanLiteral(_ *BooleanLiteral, _ interface{}) interface{} {
	v.counts["BooleanLiteral"]++
	return nil
}
func (v *visitCountVisitor) VisitNullLiteral(_ *NullLiteral, _ interface{}) interface{} {
	v.counts["NullLiteral"]++
	return nil
}
func (v *visitCountVisitor) VisitVectorLiteral(_ *VectorLiteral, _ interface{}) interface{} {
	v.counts["VectorLiteral"]++
	return nil
}
func (v *visitCountVisitor) VisitPlaceholder(_ *PlaceholderExpr, _ interface{}) interface{} {
	v.counts["Placeholder"]++
	return nil
}
func (v *visitCountVisitor) VisitInExpr(_ *InExpr, _ interface{}) interface{} {
	v.counts["InExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitBetweenExpr(_ *BetweenExpr, _ interface{}) interface{} {
	v.counts["BetweenExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitLikeExpr(_ *LikeExpr, _ interface{}) interface{} {
	v.counts["LikeExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitIsNullExpr(_ *IsNullExpr, _ interface{}) interface{} {
	v.counts["IsNullExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitCastExpr(_ *CastExpr, _ interface{}) interface{} {
	v.counts["CastExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitCaseExpr(_ *CaseExpr, _ interface{}) interface{} {
	v.counts["CaseExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitSubqueryExpr(_ *SubqueryExpr, _ interface{}) interface{} {
	v.counts["SubqueryExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitExistsExpr(_ *ExistsExpr, _ interface{}) interface{} {
	v.counts["ExistsExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitStarExpr(_ *StarExpr, _ interface{}) interface{} {
	v.counts["StarExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitJSONPathExpr(_ *JSONPathExpr, _ interface{}) interface{} {
	v.counts["JSONPathExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitJSONContainsExpr(_ *JSONContainsExpr, _ interface{}) interface{} {
	v.counts["JSONContainsExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitAliasExpr(_ *AliasExpr, _ interface{}) interface{} {
	v.counts["AliasExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitMatchExpr(_ *MatchExpr, _ interface{}) interface{} {
	v.counts["MatchExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitWindowExpr(_ *WindowExpr, _ interface{}) interface{} {
	v.counts["WindowExpr"]++
	return nil
}
func (v *visitCountVisitor) VisitWindowSpec(_ *WindowSpec, _ interface{}) interface{} {
	v.counts["WindowSpec"]++
	return nil
}

// collectExprVisitor returns the expression as-is so walkChildren recurses.
type collectExprVisitor struct {
	visitCountVisitor
}

func (v *collectExprVisitor) VisitBinaryExpr(e *BinaryExpr, _ interface{}) interface{} {
	v.counts["BinaryExpr"]++
	return e
}
func (v *collectExprVisitor) VisitUnaryExpr(e *UnaryExpr, _ interface{}) interface{} {
	v.counts["UnaryExpr"]++
	return e
}
func (v *collectExprVisitor) VisitFunctionCall(e *FunctionCall, _ interface{}) interface{} {
	v.counts["FunctionCall"]++
	return e
}
func (v *collectExprVisitor) VisitIdentifier(e *Identifier, _ interface{}) interface{} {
	v.counts["Identifier"]++
	return e
}
func (v *collectExprVisitor) VisitQualifiedIdentifier(e *QualifiedIdentifier, _ interface{}) interface{} {
	v.counts["QualifiedIdentifier"]++
	return e
}
func (v *collectExprVisitor) VisitColumnRef(e *ColumnRef, _ interface{}) interface{} {
	v.counts["ColumnRef"]++
	return e
}
func (v *collectExprVisitor) VisitStringLiteral(e *StringLiteral, _ interface{}) interface{} {
	v.counts["StringLiteral"]++
	return e
}
func (v *collectExprVisitor) VisitNumberLiteral(e *NumberLiteral, _ interface{}) interface{} {
	v.counts["NumberLiteral"]++
	return e
}
func (v *collectExprVisitor) VisitBooleanLiteral(e *BooleanLiteral, _ interface{}) interface{} {
	v.counts["BooleanLiteral"]++
	return e
}
func (v *collectExprVisitor) VisitNullLiteral(e *NullLiteral, _ interface{}) interface{} {
	v.counts["NullLiteral"]++
	return e
}
func (v *collectExprVisitor) VisitVectorLiteral(e *VectorLiteral, _ interface{}) interface{} {
	v.counts["VectorLiteral"]++
	return e
}
func (v *collectExprVisitor) VisitPlaceholder(e *PlaceholderExpr, _ interface{}) interface{} {
	v.counts["Placeholder"]++
	return e
}
func (v *collectExprVisitor) VisitInExpr(e *InExpr, _ interface{}) interface{} {
	v.counts["InExpr"]++
	return e
}
func (v *collectExprVisitor) VisitBetweenExpr(e *BetweenExpr, _ interface{}) interface{} {
	v.counts["BetweenExpr"]++
	return e
}
func (v *collectExprVisitor) VisitLikeExpr(e *LikeExpr, _ interface{}) interface{} {
	v.counts["LikeExpr"]++
	return e
}
func (v *collectExprVisitor) VisitIsNullExpr(e *IsNullExpr, _ interface{}) interface{} {
	v.counts["IsNullExpr"]++
	return e
}
func (v *collectExprVisitor) VisitCastExpr(e *CastExpr, _ interface{}) interface{} {
	v.counts["CastExpr"]++
	return e
}
func (v *collectExprVisitor) VisitCaseExpr(e *CaseExpr, _ interface{}) interface{} {
	v.counts["CaseExpr"]++
	return e
}
func (v *collectExprVisitor) VisitSubqueryExpr(e *SubqueryExpr, _ interface{}) interface{} {
	v.counts["SubqueryExpr"]++
	return e
}
func (v *collectExprVisitor) VisitExistsExpr(e *ExistsExpr, _ interface{}) interface{} {
	v.counts["ExistsExpr"]++
	return e
}
func (v *collectExprVisitor) VisitStarExpr(e *StarExpr, _ interface{}) interface{} {
	v.counts["StarExpr"]++
	return e
}
func (v *collectExprVisitor) VisitJSONPathExpr(e *JSONPathExpr, _ interface{}) interface{} {
	v.counts["JSONPathExpr"]++
	return e
}
func (v *collectExprVisitor) VisitJSONContainsExpr(e *JSONContainsExpr, _ interface{}) interface{} {
	v.counts["JSONContainsExpr"]++
	return e
}
func (v *collectExprVisitor) VisitAliasExpr(e *AliasExpr, _ interface{}) interface{} {
	v.counts["AliasExpr"]++
	return e
}
func (v *collectExprVisitor) VisitMatchExpr(e *MatchExpr, _ interface{}) interface{} {
	v.counts["MatchExpr"]++
	return e
}
func (v *collectExprVisitor) VisitWindowExpr(e *WindowExpr, _ interface{}) interface{} {
	v.counts["WindowExpr"]++
	return e
}
func (v *collectExprVisitor) VisitWindowSpec(e *WindowSpec, _ interface{}) interface{} {
	v.counts["WindowSpec"]++
	return e
}

func TestWalk(t *testing.T) {
	t.Run("nil expression does nothing", func(t *testing.T) {
		v := &visitCountVisitor{counts: make(map[string]int)}
		Walk(nil, v, nil)
		if len(v.counts) != 0 {
			t.Errorf("expected no visits for nil, got %v", v.counts)
		}
	})

	t.Run("walks leaf expression", func(t *testing.T) {
		v := &visitCountVisitor{counts: make(map[string]int)}
		Walk(&NumberLiteral{Value: 42}, v, nil)
		if v.counts["NumberLiteral"] != 1 {
			t.Errorf("expected 1 NumberLiteral visit, got %d", v.counts["NumberLiteral"])
		}
	})

	t.Run("walks nested binary expression", func(t *testing.T) {
		v := &collectExprVisitor{visitCountVisitor: visitCountVisitor{counts: make(map[string]int)}}
		expr := &BinaryExpr{
			Left:     &Identifier{Name: "x"},
			Operator: TokenPlus,
			Right:    &NumberLiteral{Value: 1},
		}
		Walk(expr, v, nil)
		if v.counts["BinaryExpr"] != 1 {
			t.Errorf("expected 1 BinaryExpr visit, got %d", v.counts["BinaryExpr"])
		}
		if v.counts["Identifier"] != 1 {
			t.Errorf("expected 1 Identifier visit, got %d", v.counts["Identifier"])
		}
		if v.counts["NumberLiteral"] != 1 {
			t.Errorf("expected 1 NumberLiteral visit, got %d", v.counts["NumberLiteral"])
		}
	})

	t.Run("visitor can skip children by returning nil", func(t *testing.T) {
		// When AcceptVisitor returns nil, Walk skips children.
		v := &visitCountVisitor{counts: make(map[string]int)}
		Walk(&FunctionCall{Name: "test", Args: []Expression{&NumberLiteral{Value: 1}}}, v, nil)
		// FunctionCall returns nil from visitCountVisitor, so children are skipped.
		if v.counts["FunctionCall"] != 1 {
			t.Errorf("expected 1 FunctionCall visit, got %d", v.counts["FunctionCall"])
		}
		// NumberLiteral should NOT be visited because FunctionCall visitor returns nil.
		if v.counts["NumberLiteral"] != 0 {
			t.Errorf("expected 0 NumberLiteral visits (skipped), got %d", v.counts["NumberLiteral"])
		}
	})
}

func TestWalkSelectStmt(t *testing.T) {
	t.Run("nil statement does nothing", func(t *testing.T) {
		v := &visitCountVisitor{counts: make(map[string]int)}
		WalkSelectStmt(nil, v, nil)
		if len(v.counts) != 0 {
			t.Errorf("expected no visits for nil, got %v", v.counts)
		}
	})

	t.Run("walks all expression nodes in a SELECT", func(t *testing.T) {
		v := &collectExprVisitor{visitCountVisitor: visitCountVisitor{counts: make(map[string]int)}}
		stmt := &SelectStmt{
			Columns: []Expression{
				&StarExpr{Table: "t"},
				&AliasExpr{Expr: &Identifier{Name: "x"}, Alias: "y"},
			},
			From: &TableRef{Name: "users"},
			Joins: []*JoinClause{
				{Table: &TableRef{Name: "orders"}, Condition: &BinaryExpr{
					Left:     &ColumnRef{Table: "users", Column: "id"},
					Operator: TokenEq,
					Right:    &ColumnRef{Table: "orders", Column: "user_id"},
				}},
			},
			Where: &BinaryExpr{
				Left:     &Identifier{Name: "status"},
				Operator: TokenEq,
				Right:    &StringLiteral{Value: "active"},
			},
			GroupBy: []Expression{&Identifier{Name: "x"}},
			Having: &BinaryExpr{
				Left:     &FunctionCall{Name: "COUNT", Args: []Expression{&StarExpr{}}},
				Operator: TokenGt,
				Right:    &NumberLiteral{Value: 0},
			},
			OrderBy: []*OrderByExpr{
				{Expr: &Identifier{Name: "x"}, Desc: true},
			},
			Limit:  &NumberLiteral{Value: 10},
			Offset: &NumberLiteral{Value: 0},
		}
		WalkSelectStmt(stmt, v, nil)
		// Verify several key node types were visited
		if v.counts["StarExpr"] == 0 {
			t.Error("expected StarExpr to be visited")
		}
		if v.counts["AliasExpr"] == 0 {
			t.Error("expected AliasExpr to be visited")
		}
		if v.counts["Identifier"] == 0 {
			t.Error("expected Identifier to be visited")
		}
		if v.counts["BinaryExpr"] == 0 {
			t.Error("expected BinaryExpr to be visited")
		}
		if v.counts["ColumnRef"] == 0 {
			t.Error("expected ColumnRef to be visited")
		}
		if v.counts["FunctionCall"] == 0 {
			t.Error("expected FunctionCall to be visited")
		}
		if v.counts["StringLiteral"] == 0 {
			t.Error("expected StringLiteral to be visited")
		}
		if v.counts["NumberLiteral"] == 0 {
			t.Error("expected NumberLiteral to be visited")
		}
	})
}

// =============================================================================
// ast.go — IntervalExpr.Evaluate, intervalMagnitude
// =============================================================================

// stubEvaluator provides a minimal Evaluator for testing Evaluate methods.
type stubEvaluator struct{}

func (s stubEvaluator) EvalBinaryExpr(left, right interface{}, op TokenType) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalBinaryExpr")
}
func (s stubEvaluator) EvalUnaryExpr(val interface{}, op TokenType) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalUnaryExpr")
}
func (s stubEvaluator) EvalIdentifier(name string) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalIdentifier")
}
func (s stubEvaluator) EvalQualifiedIdentifier(table, column string) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalQualifiedIdentifier")
}
func (s stubEvaluator) EvalPlaceholder(index int) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalPlaceholder")
}
func (s stubEvaluator) EvalLike(val, pattern, escape interface{}, not bool) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalLike")
}
func (s stubEvaluator) EvalIn(val interface{}, list []interface{}, not bool) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalIn")
}
func (s stubEvaluator) EvalInSubquery(val interface{}, q *SelectStmt, not bool) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalInSubquery")
}
func (s stubEvaluator) EvalBetween(val, lower, upper interface{}, not bool) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalBetween")
}
func (s stubEvaluator) EvalIsNull(val interface{}, not bool) (bool, error) {
	return false, fmt.Errorf("unexpected EvalIsNull")
}
func (s stubEvaluator) EvalFunctionCall(name string, args []interface{}, distinct bool) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalFunctionCall")
}
func (s stubEvaluator) EvalAlias(inner interface{}) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalAlias")
}
func (s stubEvaluator) EvalCase(expr interface{}, whens [][2]interface{}, elseVal interface{}) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalCase")
}
func (s stubEvaluator) EvalCast(val interface{}, dataType TokenType) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalCast")
}
func (s stubEvaluator) EvalSubquery(q *SelectStmt) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalSubquery")
}
func (s stubEvaluator) EvalExists(q *SelectStmt, not bool) (bool, error) {
	return false, fmt.Errorf("unexpected EvalExists")
}
func (s stubEvaluator) EvalJSONPath(jsonVal interface{}, path string, asText bool) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalJSONPath")
}
func (s stubEvaluator) EvalJSONContains(jsonVal, val interface{}) (bool, error) {
	return false, fmt.Errorf("unexpected EvalJSONContains")
}
func (s stubEvaluator) EvalMatch(expr *MatchExpr, row []interface{}) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalMatch")
}
func (s stubEvaluator) EvalStar(table string) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalStar")
}
func (s stubEvaluator) EvalColumnRef(table, column string) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalColumnRef")
}
func (s stubEvaluator) EvalWindow(w *WindowExpr) (interface{}, error) {
	return nil, fmt.Errorf("unexpected EvalWindow")
}

func TestIntervalExprEvaluate(t *testing.T) {
	t.Run("nil value returns nil,nil", func(t *testing.T) {
		expr := &IntervalExpr{Value: nil, Unit: "DAY"}
		v, err := expr.Evaluate(stubEvaluator{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != nil {
			t.Errorf("expected nil result, got %v", v)
		}
	})

	t.Run("evaluates integer value", func(t *testing.T) {
		expr := &IntervalExpr{
			Value: &NumberLiteral{Value: 5, Raw: "5"},
			Unit:  "DAY",
		}
		v, err := expr.Evaluate(stubEvaluator{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		iv, ok := v.(IntervalValue)
		if !ok {
			t.Fatalf("expected IntervalValue, got %T", v)
		}
		if iv.N != 5 || iv.Unit != "DAY" {
			t.Errorf("expected {5 DAY}, got {%d %s}", iv.N, iv.Unit)
		}
	})

	t.Run("unit is uppercased", func(t *testing.T) {
		expr := &IntervalExpr{
			Value: &NumberLiteral{Value: 1, Raw: "1"},
			Unit:  "month",
		}
		v, err := expr.Evaluate(stubEvaluator{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		iv := v.(IntervalValue)
		if iv.Unit != "MONTH" {
			t.Errorf("expected uppercased 'MONTH', got %q", iv.Unit)
		}
	})

	t.Run("value evaluation error propagates", func(t *testing.T) {
		// Use an expression whose Evaluate returns an error.
		expr := &IntervalExpr{
			Value: &WindowSpec{}, // WindowSpec.Evaluate always returns an error
			Unit:  "DAY",
		}
		_, err := expr.Evaluate(stubEvaluator{})
		if err == nil {
			t.Fatal("expected error from WindowSpec.Evaluate, got nil")
		}
	})

	t.Run("nil evaluated value returns nil,nil", func(t *testing.T) {
		expr := &IntervalExpr{
			Value: &NullLiteral{}, // evaluates to nil
			Unit:  "DAY",
		}
		v, err := expr.Evaluate(stubEvaluator{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != nil {
			t.Errorf("expected nil result, got %v", v)
		}
	})

	t.Run("non-numeric value returns error", func(t *testing.T) {
		expr := &IntervalExpr{
			Value: &StringLiteral{Value: "hello"},
			Unit:  "DAY",
		}
		_, err := expr.Evaluate(stubEvaluator{})
		if err == nil {
			t.Fatal("expected error for non-numeric value, got nil")
		}
	})
}

func TestIntervalMagnitude(t *testing.T) {
	tests := []struct {
		input  interface{}
		wantN  int64
		wantOK bool
	}{
		{int64(42), 42, true},
		{int(42), 42, true},
		{int32(42), 42, true},
		{float64(3.7), 3, true},
		{float32(3.7), 3, true},
		{"42", 42, true},
		{"  99  ", 99, true},
		{"3.14", 3, true},
		{"not a number", 0, false},
		{[]int{1, 2, 3}, 0, false},
		{true, 0, false},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("%T(%v)", tt.input, tt.input)
		t.Run(name, func(t *testing.T) {
			n, ok := intervalMagnitude(tt.input)
			if ok != tt.wantOK {
				t.Errorf("intervalMagnitude(%v) ok=%v, want %v", tt.input, ok, tt.wantOK)
			}
			if n != tt.wantN {
				t.Errorf("intervalMagnitude(%v) n=%d, want %d", tt.input, n, tt.wantN)
			}
		})
	}
}

// =============================================================================
// ast.go — CollectWindowExprs, ExprContainsWindow
// =============================================================================

func TestCollectWindowExprs(t *testing.T) {
	t.Run("nil expression", func(t *testing.T) {
		var out []*WindowExpr
		CollectWindowExprs(nil, &out)
		if len(out) != 0 {
			t.Errorf("expected empty, got %d", len(out))
		}
	})

	t.Run("no window expressions", func(t *testing.T) {
		var out []*WindowExpr
		CollectWindowExprs(&NumberLiteral{Value: 1}, &out)
		if len(out) != 0 {
			t.Errorf("expected empty, got %d", len(out))
		}
	})

	t.Run("direct WindowExpr", func(t *testing.T) {
		var out []*WindowExpr
		we := &WindowExpr{Function: "ROW_NUMBER"}
		CollectWindowExprs(we, &out)
		if len(out) != 1 || out[0] != we {
			t.Errorf("expected 1 window expr, got %d", len(out))
		}
	})

	t.Run("WindowExpr nested inside FunctionCall", func(t *testing.T) {
		var out []*WindowExpr
		we := &WindowExpr{Function: "SUM", Args: []Expression{&Identifier{Name: "x"}}}
		expr := &FunctionCall{
			Name: "fn",
			Args: []Expression{we},
		}
		CollectWindowExprs(expr, &out)
		if len(out) != 1 {
			t.Errorf("expected 1 window expr, got %d", len(out))
		}
	})

	t.Run("nested in various expression types", func(t *testing.T) {
		var out []*WindowExpr
		we1 := &WindowExpr{Function: "ROW_NUMBER"}
		we2 := &WindowExpr{Function: "RANK"}
		expr := &CaseExpr{
			Expr: &AliasExpr{Expr: we1, Alias: "rn"},
			Whens: []*WhenClause{
				{Condition: &BinaryExpr{Left: we2, Operator: TokenEq, Right: &NumberLiteral{Value: 1}}, Result: &StringLiteral{Value: "yes"}},
			},
			Else: &NumberLiteral{Value: 0},
		}
		CollectWindowExprs(expr, &out)
		if len(out) != 2 {
			t.Errorf("expected 2 window exprs, got %d: %+v", len(out), out)
		}
	})

	t.Run("WindowExpr with OrderBy", func(t *testing.T) {
		var out []*WindowExpr
		innerWE := &WindowExpr{Function: "LAG", Args: []Expression{&Identifier{Name: "x"}}}
		we := &WindowExpr{
			Function:    "LEAD",
			Args:        []Expression{innerWE},
			OrderBy:     []*OrderByExpr{{Expr: &Identifier{Name: "y"}}},
			Filter:      &BinaryExpr{Left: &Identifier{Name: "z"}, Operator: TokenGt, Right: &NumberLiteral{Value: 0}},
			PartitionBy: []Expression{&Identifier{Name: "p"}},
		}
		CollectWindowExprs(we, &out)
		if len(out) < 2 {
			t.Errorf("expected at least 2 window exprs (outer + inner), got %d", len(out))
		}
	})
}

func TestExprContainsWindow(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if ExprContainsWindow(nil) {
			t.Error("expected false for nil")
		}
	})

	t.Run("no window expression", func(t *testing.T) {
		if ExprContainsWindow(&NumberLiteral{Value: 1}) {
			t.Error("expected false for plain number")
		}
	})

	t.Run("contains window expression", func(t *testing.T) {
		if !ExprContainsWindow(&WindowExpr{Function: "ROW_NUMBER"}) {
			t.Error("expected true for WindowExpr")
		}
	})

	t.Run("window nested in expression tree", func(t *testing.T) {
		expr := &BinaryExpr{
			Left:     &Identifier{Name: "x"},
			Operator: TokenPlus,
			Right:    &WindowExpr{Function: "SUM", Args: []Expression{&Identifier{Name: "y"}}},
		}
		if !ExprContainsWindow(expr) {
			t.Error("expected true for expression containing WindowExpr")
		}
	})
}

// =============================================================================
// ast.go — DefaultExpr.Evaluate, DefaultExpr.AcceptVisitor
// =============================================================================

func TestDefaultExprEvaluate(t *testing.T) {
	de := &DefaultExpr{}
	v, err := de.Evaluate(stubEvaluator{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != nil {
		t.Errorf("expected nil result, got %v", v)
	}
}

func TestDefaultExprAcceptVisitor(t *testing.T) {
	v := &collectExprVisitor{visitCountVisitor: visitCountVisitor{counts: make(map[string]int)}}
	de := &DefaultExpr{}
	result := de.AcceptVisitor(v, nil)
	if result == nil {
		t.Fatal("expected non-nil result from AcceptVisitor")
	}
	// DefaultExpr should delegate to VisitNullLiteral
	if _, ok := result.(*NullLiteral); !ok {
		t.Errorf("expected NullLiteral from AcceptVisitor, got %T", result)
	}
	// The visitor should have its NullLiteral count incremented
	if v.counts["NullLiteral"] != 1 {
		t.Errorf("expected 1 NullLiteral visit, got %d", v.counts["NullLiteral"])
	}
}

// =============================================================================
// lexer.go — isHexDigit
// =============================================================================

func TestIsHexDigit(t *testing.T) {
	tests := []struct {
		ch   byte
		want bool
	}{
		{'0', true},
		{'5', true},
		{'9', true},
		{'a', true},
		{'f', true},
		{'A', true},
		{'F', true},
		{'g', false},
		{'z', false},
		{'G', false},
		{'Z', false},
		{'/', false},
		{':', false},
		{'@', false},
	}
	for _, tt := range tests {
		t.Run(string(tt.ch), func(t *testing.T) {
			got := isHexDigit(tt.ch)
			if got != tt.want {
				t.Errorf("isHexDigit(%q) = %v, want %v", tt.ch, got, tt.want)
			}
		})
	}
}

// =============================================================================
// parser_expression.go — isIntervalUnit, tryParseIntervalTail
// =============================================================================

func TestIsIntervalUnit(t *testing.T) {
	valid := []string{
		"MICROSECOND", "SECOND", "MINUTE", "HOUR",
		"DAY", "WEEK", "MONTH", "QUARTER", "YEAR",
	}
	invalid := []string{
		"", "SECONDS", "DAYS", "HOURS", "MINUTES",
		"DECADE", "CENTURY", "MILLENNIUM", "YEAR_MONTH",
	}

	for _, u := range valid {
		if !isIntervalUnit(u) {
			t.Errorf("expected %q to be a valid interval unit", u)
		}
	}
	for _, u := range invalid {
		if isIntervalUnit(u) {
			t.Errorf("expected %q to be an invalid interval unit", u)
		}
	}
}

func TestTryParseIntervalTail(t *testing.T) {
	t.Run("INTERVAL 5 DAY", func(t *testing.T) {
		tokens, err := Tokenize("5 DAY")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		expr, ok, err := p.tryParseIntervalTail()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("expected ok=true")
		}
		ie, ok := expr.(*IntervalExpr)
		if !ok {
			t.Fatalf("expected *IntervalExpr, got %T", expr)
		}
		if ie.Unit != "DAY" {
			t.Errorf("expected unit DAY, got %q", ie.Unit)
		}
	})

	t.Run("non-interval token returns ok=false", func(t *testing.T) {
		tokens, err := Tokenize("NOT INTERVAL")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		_, ok, err := p.tryParseIntervalTail()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false for non-interval token")
		}
	})
}

func TestTryParseIntervalTail_EdgeCases(t *testing.T) {
	// Structural keyword after INTERVAL -> ok=false
	t.Run("structural keyword after INTERVAL", func(t *testing.T) {
		tokens, err := Tokenize("SELECT")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		_, ok, err := p.tryParseIntervalTail()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false when next token is structural")
		}
	})

	// Valid number but invalid unit -> ok=false
	t.Run("number with invalid unit", func(t *testing.T) {
		tokens, err := Tokenize("5 DECADE")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		_, ok, err := p.tryParseIntervalTail()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false for invalid unit")
		}
	})
}

// =============================================================================
// parser_expression.go — extractFieldToFunc
// =============================================================================

func TestExtractFieldToFunc(t *testing.T) {
	tests := []struct {
		field string
		want  string
	}{
		{"DOW", "DAYOFWEEK"},
		{"DAYOFWEEK", "DAYOFWEEK"},
		{"DOY", "DAYOFYEAR"},
		{"DAYOFYEAR", "DAYOFYEAR"},
		{"YEAR", "YEAR"},
		{"MONTH", "MONTH"},
		{"DAY", "DAY"},
		{"HOUR", "HOUR"},
		{"MINUTE", "MINUTE"},
		{"SECOND", "SECOND"},
		{"UNKNOWN_FIELD", "UNKNOWN_FIELD"},
	}
	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			got := extractFieldToFunc(tt.field)
			if got != tt.want {
				t.Errorf("extractFieldToFunc(%q) = %q, want %q", tt.field, got, tt.want)
			}
		})
	}
}

// =============================================================================
// parser.go — parseTemporalExpr, parseVectorLiteral, CountPlaceholders
// =============================================================================

func TestParseTemporalExpr(t *testing.T) {
	t.Run("AS OF literal timestamp", func(t *testing.T) {
		tokens, err := Tokenize("AS OF '2024-01-15'")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		p.advance() // consume AS
		p.advance() // consume OF
		te, err := p.parseTemporalExpr()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if te.IsSystem {
			t.Error("expected IsSystem=false for literal timestamp")
		}
		if te.Timestamp == nil {
			t.Fatal("expected non-nil Timestamp")
		}
	})

	t.Run("AS OF SYSTEM TIME", func(t *testing.T) {
		tokens, err := Tokenize("AS OF SYSTEM TIME '-1 hour'")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		p.advance() // consume AS
		p.advance() // consume OF
		te, err := p.parseTemporalExpr()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !te.IsSystem {
			t.Error("expected IsSystem=true for SYSTEM TIME")
		}
	})

	t.Run("AS OF without system time", func(t *testing.T) {
		tokens, err := Tokenize("AS OF 12345")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		p.advance() // consume AS
		p.advance() // consume OF
		te, err := p.parseTemporalExpr()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if te.IsSystem {
			t.Error("expected IsSystem=false")
		}
	})
}

func TestParseVectorLiteral(t *testing.T) {
	t.Run("empty vector []", func(t *testing.T) {
		tokens, err := Tokenize("[]")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		expr, err := p.parseVectorLiteral()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		vl, ok := expr.(*VectorLiteral)
		if !ok {
			t.Fatalf("expected *VectorLiteral, got %T", expr)
		}
		if len(vl.Values) != 0 {
			t.Errorf("expected empty slice, got %v", vl.Values)
		}
	})

	t.Run("vector with values [1, 2, 3]", func(t *testing.T) {
		tokens, err := Tokenize("[1, 2, 3]")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		expr, err := p.parseVectorLiteral()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		vl, ok := expr.(*VectorLiteral)
		if !ok {
			t.Fatalf("expected *VectorLiteral, got %T", expr)
		}
		if len(vl.Values) != 3 {
			t.Errorf("expected 3 values, got %d: %v", len(vl.Values), vl.Values)
		}
	})

	t.Run("vector with missing comma returns error", func(t *testing.T) {
		tokens, err := Tokenize("[1 2]")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		_, err = p.parseVectorLiteral()
		if err == nil {
			t.Fatal("expected error for missing comma in vector literal")
		}
	})

	t.Run("vector with non-number returns error", func(t *testing.T) {
		tokens, err := Tokenize("[abc]")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		_, err = p.parseVectorLiteral()
		if err == nil {
			t.Fatal("expected error for non-number in vector literal")
		}
	})
}

func TestCountPlaceholders(t *testing.T) {
	t.Run("no placeholders", func(t *testing.T) {
		n := CountPlaceholders("SELECT * FROM t WHERE id = 1")
		if n != 0 {
			t.Errorf("expected 0, got %d", n)
		}
	})

	t.Run("one placeholder", func(t *testing.T) {
		n := CountPlaceholders("SELECT * FROM t WHERE id = ?")
		if n != 1 {
			t.Errorf("expected 1, got %d", n)
		}
	})

	t.Run("multiple placeholders", func(t *testing.T) {
		n := CountPlaceholders("INSERT INTO t VALUES (?, ?, ?)")
		if n != 3 {
			t.Errorf("expected 3, got %d", n)
		}
	})

	t.Run("malformed sql returns 0", func(t *testing.T) {
		n := CountPlaceholders("SELECT 'unterminated")
		if n != 0 {
			t.Errorf("expected 0 for malformed SQL, got %d", n)
		}
	})
}

// =============================================================================
// parser_ddl.go — parseCreateVectorIndex
// =============================================================================

func TestParseCreateVectorIndex(t *testing.T) {
	t.Run("basic VECTOR INDEX", func(t *testing.T) {
		stmt, err := Parse("CREATE VECTOR INDEX idx ON t (col)")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		cvi, ok := stmt.(*CreateVectorIndexStmt)
		if !ok {
			t.Fatalf("expected *CreateVectorIndexStmt, got %T", stmt)
		}
		if cvi.Index != "idx" || cvi.Table != "t" || cvi.Column != "col" {
			t.Errorf("got idx=%q table=%q col=%q", cvi.Index, cvi.Table, cvi.Column)
		}
		if cvi.IfNotExists {
			t.Error("expected IfNotExists=false")
		}
	})

	t.Run("VECTOR INDEX IF NOT EXISTS", func(t *testing.T) {
		stmt, err := Parse("CREATE VECTOR INDEX IF NOT EXISTS idx ON t (col)")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		cvi, ok := stmt.(*CreateVectorIndexStmt)
		if !ok {
			t.Fatalf("expected *CreateVectorIndexStmt, got %T", stmt)
		}
		if !cvi.IfNotExists {
			t.Error("expected IfNotExists=true")
		}
	})

	t.Run("VECTOR INDEX missing ON", func(t *testing.T) {
		_, err := Parse("CREATE VECTOR INDEX idx t (col)")
		if err == nil {
			t.Fatal("expected error for missing ON")
		}
	})
}

// =============================================================================
// parser_dml_select.go — parseWindowFrame, parseWindowFrameBound
// =============================================================================

func TestParseWindowFrame(t *testing.T) {
	t.Run("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING", func(t *testing.T) {
		tokens, err := Tokenize("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		frame, err := p.parseWindowFrame()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if frame.Mode != "ROWS" {
			t.Errorf("expected ROWS, got %q", frame.Mode)
		}
		if frame.Start == nil || frame.Start.Type != "PRECEDING" || frame.Start.Offset != 1 {
			t.Errorf("unexpected Start: %+v", frame.Start)
		}
		if frame.End == nil || frame.End.Type != "FOLLOWING" || frame.End.Offset != 1 {
			t.Errorf("unexpected End: %+v", frame.End)
		}
	})

	t.Run("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", func(t *testing.T) {
		tokens, err := Tokenize("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		frame, err := p.parseWindowFrame()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if frame.Mode != "RANGE" {
			t.Errorf("expected RANGE, got %q", frame.Mode)
		}
		if frame.Start == nil || frame.Start.Type != "UNBOUNDED_PRECEDING" {
			t.Errorf("unexpected Start: %+v", frame.Start)
		}
		if frame.End == nil || frame.End.Type != "CURRENT_ROW" {
			t.Errorf("unexpected End: %+v", frame.End)
		}
	})

	t.Run("ROWS UNBOUNDED PRECEDING (single bound)", func(t *testing.T) {
		tokens, err := Tokenize("ROWS UNBOUNDED PRECEDING")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		frame, err := p.parseWindowFrame()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if frame.Mode != "ROWS" {
			t.Errorf("expected ROWS, got %q", frame.Mode)
		}
		if frame.Start == nil || frame.Start.Type != "UNBOUNDED_PRECEDING" {
			t.Errorf("unexpected Start: %+v", frame.Start)
		}
		if frame.End == nil || frame.End.Type != "CURRENT_ROW" {
			t.Errorf("expected End=CURRENT_ROW for single bound, got %+v", frame.End)
		}
	})

	t.Run("invalid window frame bound", func(t *testing.T) {
		tokens, err := Tokenize("ROWS INVALID")
		if err != nil {
			t.Fatalf("Tokenize error: %v", err)
		}
		p := NewParser(tokens)
		_, err = p.parseWindowFrame()
		if err == nil {
			t.Fatal("expected error for invalid frame bound")
		}
	})
}

// =============================================================================
// parser_dml_select.go — parseTruncate
// =============================================================================

func TestParseTruncate(t *testing.T) {
	t.Run("TRUNCATE TABLE", func(t *testing.T) {
		stmt, err := Parse("TRUNCATE TABLE users")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ds, ok := stmt.(*DeleteStmt)
		if !ok {
			t.Fatalf("expected *DeleteStmt, got %T", stmt)
		}
		if ds.Table != "users" {
			t.Errorf("expected table=users, got %q", ds.Table)
		}
	})

	t.Run("TRUNCATE without TABLE keyword", func(t *testing.T) {
		stmt, err := Parse("TRUNCATE users")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		ds, ok := stmt.(*DeleteStmt)
		if !ok {
			t.Fatalf("expected *DeleteStmt, got %T", stmt)
		}
		if ds.Table != "users" {
			t.Errorf("expected table=users, got %q", ds.Table)
		}
	})

	t.Run("TRUNCATE missing table name", func(t *testing.T) {
		_, err := Parse("TRUNCATE TABLE")
		if err == nil {
			t.Fatal("expected error for TRUNCATE without table name")
		}
	})
}

// =============================================================================
// ast.go — remaining Expression Evaluate methods with 0% coverage
// These expression types had 0% Evaluate coverage
// =============================================================================

func TestEvaluateExpressions(t *testing.T) {
	ev := stubEvaluator{}

	t.Run("QualifiedIdentifier.Evaluate calls EvalQualifiedIdentifier", func(t *testing.T) {
		// We can verify it calls through by checking it returns an error from stubEvaluator
		_, err := (&QualifiedIdentifier{Table: "t", Column: "c"}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalQualifiedIdentifier") {
			t.Errorf("expected stub error about EvalQualifiedIdentifier, got %v", err)
		}
	})

	t.Run("ColumnRef.Evaluate calls EvalColumnRef", func(t *testing.T) {
		_, err := (&ColumnRef{Table: "t", Column: "c"}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalColumnRef") {
			t.Errorf("expected stub error about EvalColumnRef, got %v", err)
		}
	})

	t.Run("StringLiteral.Evaluate returns value", func(t *testing.T) {
		v, err := (&StringLiteral{Value: "hello"}).Evaluate(ev)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != "hello" {
			t.Errorf("expected 'hello', got %v", v)
		}
	})

	t.Run("NumberLiteral.Evaluate with integer raw", func(t *testing.T) {
		v, err := (&NumberLiteral{Value: 42, Raw: "42"}).Evaluate(ev)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if i, ok := v.(int64); !ok || i != 42 {
			t.Errorf("expected int64(42), got %T(%v)", v, v)
		}
	})

	t.Run("NumberLiteral.Evaluate with float raw", func(t *testing.T) {
		v, err := (&NumberLiteral{Value: 3.14, Raw: "3.14"}).Evaluate(ev)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if f, ok := v.(float64); !ok || f != 3.14 {
			t.Errorf("expected float64(3.14), got %T(%v)", v, v)
		}
	})

	t.Run("BooleanLiteral.Evaluate returns value", func(t *testing.T) {
		v, err := (&BooleanLiteral{Value: true}).Evaluate(ev)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != true {
			t.Errorf("expected true, got %v", v)
		}
	})

	t.Run("NullLiteral.Evaluate returns nil", func(t *testing.T) {
		v, err := (&NullLiteral{}).Evaluate(ev)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != nil {
			t.Errorf("expected nil, got %v", v)
		}
	})

	t.Run("VectorLiteral.Evaluate returns values", func(t *testing.T) {
		v, err := (&VectorLiteral{Values: []float64{1.0, 2.0}}).Evaluate(ev)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		vals, ok := v.([]float64)
		if !ok || len(vals) != 2 || vals[0] != 1.0 {
			t.Errorf("expected [1, 2], got %v", v)
		}
	})

	t.Run("StarExpr.Evaluate calls EvalStar", func(t *testing.T) {
		_, err := (&StarExpr{Table: "t"}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalStar") {
			t.Errorf("expected stub error about EvalStar, got %v", err)
		}
	})

	t.Run("JSONPathExpr.Evaluate calls EvalJSONPath", func(t *testing.T) {
		_, err := (&JSONPathExpr{Column: &StringLiteral{Value: `{"a":1}`}, Path: "$.a", AsText: true}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalJSONPath") {
			t.Errorf("expected stub error about EvalJSONPath, got %v", err)
		}
	})

	t.Run("JSONContainsExpr.Evaluate calls EvalJSONContains", func(t *testing.T) {
		_, err := (&JSONContainsExpr{
			Column: &StringLiteral{Value: `{"a":1}`},
			Value:  &StringLiteral{Value: `{"a":1}`},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalJSONContains") {
			t.Errorf("expected stub error about EvalJSONContains, got %v", err)
		}
	})

	t.Run("PlaceholderExpr.Evaluate calls EvalPlaceholder", func(t *testing.T) {
		_, err := (&PlaceholderExpr{Index: 1}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalPlaceholder") {
			t.Errorf("expected stub error about EvalPlaceholder, got %v", err)
		}
	})

	t.Run("InExpr.Evaluate with subquery", func(t *testing.T) {
		_, err := (&InExpr{
			Expr:     &NumberLiteral{Value: 1},
			Subquery: &SelectStmt{Columns: []Expression{&StarExpr{}}, From: &TableRef{Name: "t"}},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalInSubquery") {
			t.Errorf("expected stub error about EvalInSubquery, got %v", err)
		}
	})

	t.Run("InExpr.Evaluate without subquery", func(t *testing.T) {
		_, err := (&InExpr{
			Expr: &NumberLiteral{Value: 1},
			List: []Expression{&NumberLiteral{Value: 1}, &NumberLiteral{Value: 2}},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalIn") {
			t.Errorf("expected stub error about EvalIn, got %v", err)
		}
	})

	t.Run("BetweenExpr.Evaluate calls EvalBetween", func(t *testing.T) {
		_, err := (&BetweenExpr{
			Expr:  &NumberLiteral{Value: 5},
			Lower: &NumberLiteral{Value: 1},
			Upper: &NumberLiteral{Value: 10},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalBetween") {
			t.Errorf("expected stub error about EvalBetween, got %v", err)
		}
	})

	t.Run("LikeExpr.Evaluate calls EvalLike", func(t *testing.T) {
		_, err := (&LikeExpr{
			Expr:    &StringLiteral{Value: "hello"},
			Pattern: &StringLiteral{Value: "%ll%"},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalLike") {
			t.Errorf("expected stub error about EvalLike, got %v", err)
		}
	})

	t.Run("LikeExpr.Evaluate with escape", func(t *testing.T) {
		_, err := (&LikeExpr{
			Expr:    &StringLiteral{Value: "100%"},
			Pattern: &StringLiteral{Value: "100!%"},
			Escape:  &StringLiteral{Value: "!"},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalLike") {
			t.Errorf("expected stub error about EvalLike, got %v", err)
		}
	})

	t.Run("IsNullExpr.Evaluate calls EvalIsNull", func(t *testing.T) {
		_, err := (&IsNullExpr{Expr: &NullLiteral{}}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalIsNull") {
			t.Errorf("expected stub error about EvalIsNull, got %v", err)
		}
	})

	t.Run("CastExpr.Evaluate calls EvalCast", func(t *testing.T) {
		_, err := (&CastExpr{Expr: &StringLiteral{Value: "42"}, DataType: TokenInteger}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalCast") {
			t.Errorf("expected stub error about EvalCast, got %v", err)
		}
	})

	t.Run("SubqueryExpr.Evaluate calls EvalSubquery", func(t *testing.T) {
		_, err := (&SubqueryExpr{Query: &SelectStmt{Columns: []Expression{&StarExpr{}}, From: &TableRef{Name: "t"}}}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalSubquery") {
			t.Errorf("expected stub error about EvalSubquery, got %v", err)
		}
	})

	t.Run("ExistsExpr.Evaluate calls EvalExists", func(t *testing.T) {
		_, err := (&ExistsExpr{Subquery: &SelectStmt{Columns: []Expression{&StarExpr{}}, From: &TableRef{Name: "t"}}}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalExists") {
			t.Errorf("expected stub error about EvalExists, got %v", err)
		}
	})

	t.Run("WindowExpr.Evaluate calls EvalWindow", func(t *testing.T) {
		_, err := (&WindowExpr{Function: "ROW_NUMBER"}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalWindow") {
			t.Errorf("expected stub error about EvalWindow, got %v", err)
		}
	})

	t.Run("WindowSpec.Evaluate returns error", func(t *testing.T) {
		_, err := (&WindowSpec{}).Evaluate(ev)
		if err == nil {
			t.Fatal("expected error from WindowSpec.Evaluate")
		}
	})

	t.Run("MatchExpr.Evaluate calls EvalMatch", func(t *testing.T) {
		_, err := (&MatchExpr{
			Columns: []Expression{&Identifier{Name: "title"}},
			Pattern: &StringLiteral{Value: "term"},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalMatch") {
			t.Errorf("expected stub error about EvalMatch, got %v", err)
		}
	})

	t.Run("AliasExpr.Evaluate calls EvalAlias", func(t *testing.T) {
		_, err := (&AliasExpr{Expr: &NumberLiteral{Value: 1}, Alias: "a"}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalAlias") {
			t.Errorf("expected stub error about EvalAlias, got %v", err)
		}
	})

	t.Run("BinaryExpr.Evaluate calls EvalBinaryExpr", func(t *testing.T) {
		_, err := (&BinaryExpr{
			Left:     &NumberLiteral{Value: 1},
			Operator: TokenPlus,
			Right:    &NumberLiteral{Value: 2},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalBinaryExpr") {
			t.Errorf("expected stub error about EvalBinaryExpr, got %v", err)
		}
	})

	t.Run("UnaryExpr.Evaluate calls EvalUnaryExpr", func(t *testing.T) {
		_, err := (&UnaryExpr{
			Operator: TokenMinus,
			Expr:     &NumberLiteral{Value: 5},
		}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalUnaryExpr") {
			t.Errorf("expected stub error about EvalUnaryExpr, got %v", err)
		}
	})
}

// =============================================================================
// ast.go — remaining uncovered Evaluate and AcceptVisitor methods
// =============================================================================

// TestEvaluateIdentifier covers the remaining Identifier.Evaluate method.
func TestEvaluateIdentifier(t *testing.T) {
	ev := stubEvaluator{}

	t.Run("simple identifier calls EvalIdentifier", func(t *testing.T) {
		_, err := (&Identifier{Name: "name"}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalIdentifier") {
			t.Errorf("expected stub error about EvalIdentifier, got %v", err)
		}
	})

	t.Run("dotted identifier calls EvalQualifiedIdentifier", func(t *testing.T) {
		_, err := (&Identifier{Name: "table.column"}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalQualifiedIdentifier") {
			t.Errorf("expected stub error about EvalQualifiedIdentifier, got %v", err)
		}
	})

	t.Run("dotted identifier with trailing dot is simple", func(t *testing.T) {
		_, err := (&Identifier{Name: "table."}).Evaluate(ev)
		if err == nil || !strings.Contains(err.Error(), "unexpected EvalIdentifier") {
			t.Errorf("expected stub error about EvalIdentifier, got %v", err)
		}
	})
}

// TestAllAcceptVisitorMethods exercises every AcceptVisitor method to ensure
// coverage for the remaining uncovered AcceptVisitor implementations.
func TestAllAcceptVisitorMethods(t *testing.T) {
	v := &collectExprVisitor{visitCountVisitor: visitCountVisitor{counts: make(map[string]int)}}
	ctx := struct{}{}

	table := []struct {
		name string
		expr Expression
	}{
		{"UnaryExpr", &UnaryExpr{Operator: TokenMinus, Expr: &NumberLiteral{Value: 1}}},
		{"QualifiedIdentifier", &QualifiedIdentifier{Table: "t", Column: "c"}},
		{"BooleanLiteral", &BooleanLiteral{Value: true}},
		{"NullLiteral", &NullLiteral{}},
		{"VectorLiteral", &VectorLiteral{Values: []float64{1.0}}},
		{"PlaceholderExpr", &PlaceholderExpr{Index: 1}},
		{"InExpr", &InExpr{Expr: &NumberLiteral{Value: 1}, List: []Expression{&NumberLiteral{Value: 1}}}},
		{"BetweenExpr", &BetweenExpr{Expr: &NumberLiteral{Value: 5}, Lower: &NumberLiteral{Value: 1}, Upper: &NumberLiteral{Value: 10}}},
		{"LikeExpr", &LikeExpr{Expr: &StringLiteral{Value: "a"}, Pattern: &StringLiteral{Value: "%"}}},
		{"IsNullExpr", &IsNullExpr{Expr: &NullLiteral{}}},
		{"CastExpr", &CastExpr{Expr: &StringLiteral{Value: "42"}, DataType: TokenInteger}},
		{"CaseExpr", &CaseExpr{Whens: []*WhenClause{{Condition: &BooleanLiteral{Value: true}, Result: &NumberLiteral{Value: 1}}}}},
		{"SubqueryExpr", &SubqueryExpr{Query: &SelectStmt{Columns: []Expression{&StarExpr{}}, From: &TableRef{Name: "t"}}}},
		{"ExistsExpr", &ExistsExpr{Subquery: &SelectStmt{Columns: []Expression{&StarExpr{}}, From: &TableRef{Name: "t"}}}},
		{"JSONPathExpr", &JSONPathExpr{Column: &StringLiteral{Value: `{"a":1}`}, Path: "$.a"}},
		{"JSONContainsExpr", &JSONContainsExpr{Column: &StringLiteral{Value: `{"a":1}`}, Value: &StringLiteral{Value: `{"a":1}`}}},
		{"MatchExpr", &MatchExpr{Columns: []Expression{&Identifier{Name: "title"}}, Pattern: &StringLiteral{Value: "term"}}},
		{"WindowExpr", &WindowExpr{Function: "ROW_NUMBER"}},
		{"WindowSpec", &WindowSpec{}},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			v.counts = make(map[string]int)
			result := tt.expr.AcceptVisitor(v, ctx)
			if result == nil {
				t.Error("AcceptVisitor returned nil, expected non-nil for visitor that returns the original")
			}
		})
	}
}

// TestMissingMarkerMethods covers nodeType/statementNode/expressionNode for
// a few types that TestASTNodeTypes doesn't cover.
func TestMissingMarkerMethods(t *testing.T) {
	// ShowIndexStmt is not in the original TestASTNodeTypes list
	s := &ShowIndexStmt{}
	if n := s.nodeType(); n != "ShowIndexStmt" {
		t.Errorf("nodeType = %q, want ShowIndexStmt", n)
	}
	s.statementNode()

	// DefaultExpr is not in the expression list
	de := &DefaultExpr{}
	if n := de.nodeType(); n != "DefaultExpr" {
		t.Errorf("nodeType = %q, want DefaultExpr", n)
	}
	de.expressionNode()

	// IntervalExpr is not in the expression list
	ie := &IntervalExpr{}
	if n := ie.nodeType(); n != "IntervalExpr" {
		t.Errorf("nodeType = %q, want IntervalExpr", n)
	}
	ie.expressionNode()

	// IntervalExpr.AcceptVisitor
	v := &collectExprVisitor{visitCountVisitor: visitCountVisitor{counts: make(map[string]int)}}
	result := ie.AcceptVisitor(v, nil)
	if result == nil {
		t.Error("IntervalExpr.AcceptVisitor returned nil")
	}
}

// =============================================================================
// CloneStatement nil and edge cases
// =============================================================================

func TestCloneStatementNil(t *testing.T) {
	cloned := CloneStatement(nil)
	if cloned != nil {
		t.Error("expected nil for nil input")
	}
}

func TestCloneExpressionNil(t *testing.T) {
	cloned := CloneExpression(nil)
	if cloned != nil {
		t.Error("expected nil for nil input")
	}
}

// Verify that CloneExpression works for expression types not tested before.
func TestCloneExpressionTypes(t *testing.T) {
	expr := &IntervalExpr{
		Value: &NumberLiteral{Value: 5, Raw: "5"},
		Unit:  "DAY",
	}
	cloned := CloneExpression(expr).(*IntervalExpr)
	if cloned == expr {
		t.Fatal("CloneExpression returned the original pointer")
	}
	if cloned.Value == expr.Value {
		t.Fatal("CloneExpression did not deeply copy Value")
	}
	if cloned.Unit != "DAY" {
		t.Errorf("expected Unit=DAY, got %q", cloned.Unit)
	}
	// Mutate clone and verify original unchanged
	cloned.Unit = "MONTH"
	if expr.Unit != "DAY" {
		t.Errorf("original mutated through clone, got %q", expr.Unit)
	}
}
