package query

// AST-level regression tests: CASE / COALESCE / IFNULL / IIF must evaluate
// lazily so that guarded sub-expressions never run when unreachable.

import (
	"errors"
	"testing"
)

// explodingExpr fails the test's expectation if it is ever evaluated.
type explodingExpr struct{}

func (e *explodingExpr) nodeType() string { return "explodingExpr" }
func (e *explodingExpr) expressionNode()  {}
func (e *explodingExpr) Evaluate(Evaluator) (interface{}, error) {
	return nil, errors.New("guarded expression was evaluated")
}
func (e *explodingExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return nil
}

// lazyMockEvaluator is a minimal Evaluator; only EvalBinaryExpr(=) and
// EvalFunctionCall carry real behavior. It intentionally does NOT implement
// BoolEvaluator so the fallback truthiness path is exercised.
type lazyMockEvaluator struct{}

func (m *lazyMockEvaluator) EvalBinaryExpr(left, right interface{}, op TokenType) (interface{}, error) {
	if op == TokenEq {
		if left == nil || right == nil {
			return nil, nil
		}
		return left == right, nil
	}
	return nil, nil
}
func (m *lazyMockEvaluator) EvalUnaryExpr(val interface{}, op TokenType) (interface{}, error) {
	return val, nil
}
func (m *lazyMockEvaluator) EvalIdentifier(name string) (interface{}, error) { return nil, nil }
func (m *lazyMockEvaluator) EvalQualifiedIdentifier(table, column string) (interface{}, error) {
	return nil, nil
}
func (m *lazyMockEvaluator) EvalPlaceholder(index int) (interface{}, error) { return nil, nil }
func (m *lazyMockEvaluator) EvalLike(val, pattern, escape interface{}, not bool) (interface{}, error) {
	return false, nil
}
func (m *lazyMockEvaluator) EvalIn(val interface{}, list []interface{}, not bool) (interface{}, error) {
	return false, nil
}
func (m *lazyMockEvaluator) EvalInSubquery(val interface{}, q *SelectStmt, not bool) (interface{}, error) {
	return false, nil
}
func (m *lazyMockEvaluator) EvalBetween(val, lower, upper interface{}, not bool) (interface{}, error) {
	return false, nil
}
func (m *lazyMockEvaluator) EvalIsNull(val interface{}, not bool) (bool, error) {
	return val == nil, nil
}
func (m *lazyMockEvaluator) EvalFunctionCall(name string, args []interface{}, distinct bool) (interface{}, error) {
	return nil, nil
}
func (m *lazyMockEvaluator) EvalAlias(inner interface{}) (interface{}, error) { return inner, nil }
func (m *lazyMockEvaluator) EvalCase(expr interface{}, whens [][2]interface{}, elseVal interface{}) (interface{}, error) {
	return elseVal, nil
}
func (m *lazyMockEvaluator) EvalCast(val interface{}, dataType TokenType) (interface{}, error) {
	return val, nil
}
func (m *lazyMockEvaluator) EvalSubquery(q *SelectStmt) (interface{}, error)  { return nil, nil }
func (m *lazyMockEvaluator) EvalExists(q *SelectStmt, not bool) (bool, error) { return false, nil }
func (m *lazyMockEvaluator) EvalJSONPath(v interface{}, p string, t bool) (interface{}, error) {
	return nil, nil
}
func (m *lazyMockEvaluator) EvalJSONContains(jsonVal, val interface{}) (bool, error) {
	return false, nil
}
func (m *lazyMockEvaluator) EvalMatch(expr *MatchExpr, row []interface{}) (interface{}, error) {
	return false, nil
}
func (m *lazyMockEvaluator) EvalStar(table string) (interface{}, error) { return nil, nil }
func (m *lazyMockEvaluator) EvalColumnRef(table, column string) (interface{}, error) {
	return nil, nil
}
func (m *lazyMockEvaluator) EvalWindow(w *WindowExpr) (interface{}, error) { return nil, nil }

func TestCoalesceShortCircuits(t *testing.T) {
	ev := &lazyMockEvaluator{}

	expr := &FunctionCall{Name: "COALESCE", Args: []Expression{
		&StringLiteral{Value: "v"},
		&explodingExpr{},
	}}
	got, err := expr.Evaluate(ev)
	if err != nil {
		t.Fatalf("COALESCE evaluated a guarded argument: %v", err)
	}
	if got != "v" {
		t.Errorf("COALESCE = %v, want v", got)
	}

	// Lowercase spelling works too (manually built ASTs may not uppercase).
	expr = &FunctionCall{Name: "coalesce", Args: []Expression{
		&StringLiteral{Value: "v"},
		&explodingExpr{},
	}}
	if got, err := expr.Evaluate(ev); err != nil || got != "v" {
		t.Errorf("lowercase coalesce = %v, %v; want v", got, err)
	}

	// Falls through NULLs and still evaluates the needed argument.
	expr = &FunctionCall{Name: "IFNULL", Args: []Expression{
		&NullLiteral{},
		&NumberLiteral{Value: 7, Raw: "7"},
	}}
	got, err = expr.Evaluate(ev)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(7) {
		t.Errorf("IFNULL(NULL, 7) = %v (%T), want 7", got, got)
	}

	// All NULL yields NULL.
	expr = &FunctionCall{Name: "COALESCE", Args: []Expression{&NullLiteral{}, &NullLiteral{}}}
	if got, err := expr.Evaluate(ev); err != nil || got != nil {
		t.Errorf("COALESCE(NULL, NULL) = %v, %v; want nil", got, err)
	}
}

func TestSearchedCaseShortCircuits(t *testing.T) {
	ev := &lazyMockEvaluator{}

	// False condition: THEN branch must not run; ELSE is returned.
	expr := &CaseExpr{
		Whens: []*WhenClause{{Condition: &BooleanLiteral{Value: false}, Result: &explodingExpr{}}},
		Else:  &NumberLiteral{Value: 0, Raw: "0"},
	}
	got, err := expr.Evaluate(ev)
	if err != nil {
		t.Fatalf("CASE evaluated an unreachable THEN: %v", err)
	}
	if got != int64(0) {
		t.Errorf("CASE = %v, want 0", got)
	}

	// True condition: ELSE must not run.
	expr = &CaseExpr{
		Whens: []*WhenClause{{Condition: &BooleanLiteral{Value: true}, Result: &StringLiteral{Value: "hit"}}},
		Else:  &explodingExpr{},
	}
	got, err = expr.Evaluate(ev)
	if err != nil {
		t.Fatalf("CASE evaluated an unreachable ELSE: %v", err)
	}
	if got != "hit" {
		t.Errorf("CASE = %v, want hit", got)
	}

	// Numeric condition counts as truthy via the fallback rules.
	expr = &CaseExpr{
		Whens: []*WhenClause{{Condition: &NumberLiteral{Value: 1, Raw: "1"}, Result: &StringLiteral{Value: "yes"}}},
		Else:  &StringLiteral{Value: "no"},
	}
	if got, _ := expr.Evaluate(ev); got != "yes" {
		t.Errorf("CASE WHEN 1 = %v, want yes", got)
	}

	// No match and no ELSE yields NULL.
	expr = &CaseExpr{
		Whens: []*WhenClause{{Condition: &BooleanLiteral{Value: false}, Result: &explodingExpr{}}},
	}
	if got, err := expr.Evaluate(ev); err != nil || got != nil {
		t.Errorf("CASE without match = %v, %v; want nil", got, err)
	}
}

func TestSimpleCaseShortCircuits(t *testing.T) {
	ev := &lazyMockEvaluator{}

	expr := &CaseExpr{
		Expr: &NumberLiteral{Value: 2, Raw: "2"},
		Whens: []*WhenClause{
			{Condition: &NumberLiteral{Value: 1, Raw: "1"}, Result: &explodingExpr{}},
			{Condition: &NumberLiteral{Value: 2, Raw: "2"}, Result: &StringLiteral{Value: "two"}},
		},
		Else: &explodingExpr{},
	}
	got, err := expr.Evaluate(ev)
	if err != nil {
		t.Fatalf("simple CASE evaluated an unreachable branch: %v", err)
	}
	if got != "two" {
		t.Errorf("simple CASE = %v, want two", got)
	}

	// NULL base value matches nothing and falls through to ELSE.
	expr = &CaseExpr{
		Expr: &NullLiteral{},
		Whens: []*WhenClause{
			{Condition: &NullLiteral{}, Result: &explodingExpr{}},
		},
		Else: &StringLiteral{Value: "else"},
	}
	got, err = expr.Evaluate(ev)
	if err != nil {
		t.Fatal(err)
	}
	if got != "else" {
		t.Errorf("CASE NULL WHEN NULL = %v, want else", got)
	}
}

func TestIIFShortCircuits(t *testing.T) {
	ev := &lazyMockEvaluator{}

	expr := &FunctionCall{Name: "IIF", Args: []Expression{
		&BooleanLiteral{Value: true},
		&StringLiteral{Value: "a"},
		&explodingExpr{},
	}}
	got, err := expr.Evaluate(ev)
	if err != nil {
		t.Fatalf("IIF evaluated the unreachable branch: %v", err)
	}
	if got != "a" {
		t.Errorf("IIF(true) = %v, want a", got)
	}

	expr = &FunctionCall{Name: "IF", Args: []Expression{
		&BooleanLiteral{Value: false},
		&explodingExpr{},
		&StringLiteral{Value: "b"},
	}}
	got, err = expr.Evaluate(ev)
	if err != nil {
		t.Fatalf("IF evaluated the unreachable branch: %v", err)
	}
	if got != "b" {
		t.Errorf("IF(false) = %v, want b", got)
	}
}
