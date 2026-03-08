package catalog

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ==============================================================
// EvalExpression comprehensive tests (was at 37%)
// ==============================================================

func TestEvalExpr_Literals(t *testing.T) {
	// StringLiteral
	val, err := EvalExpression(&query.StringLiteral{Value: "hello"}, nil)
	if err != nil || val != "hello" {
		t.Errorf("StringLiteral: got %v, err %v", val, err)
	}

	// NumberLiteral
	val, err = EvalExpression(&query.NumberLiteral{Value: 42}, nil)
	if err != nil || val != float64(42) {
		t.Errorf("NumberLiteral: got %v, err %v", val, err)
	}

	// BooleanLiteral
	val, err = EvalExpression(&query.BooleanLiteral{Value: true}, nil)
	if err != nil || val != true {
		t.Errorf("BooleanLiteral: got %v, err %v", val, err)
	}

	// NullLiteral
	val, err = EvalExpression(&query.NullLiteral{}, nil)
	if err != nil || val != nil {
		t.Errorf("NullLiteral: got %v, err %v", val, err)
	}

	// Identifier
	val, err = EvalExpression(&query.Identifier{Name: "col1"}, nil)
	if err != nil || val != "col1" {
		t.Errorf("Identifier: got %v, err %v", val, err)
	}
}

func TestEvalExpr_Placeholder(t *testing.T) {
	// In-range placeholder
	val, err := EvalExpression(&query.PlaceholderExpr{Index: 0}, []interface{}{"test"})
	if err != nil || val != "test" {
		t.Errorf("Placeholder in range: got %v, err %v", val, err)
	}

	// Out of range placeholder
	_, err = EvalExpression(&query.PlaceholderExpr{Index: 5}, []interface{}{"a"})
	if err == nil {
		t.Error("Expected error for out of range placeholder")
	}
}

func TestEvalExpr_UnaryMinus(t *testing.T) {
	// Negate integer
	val, err := EvalExpression(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.NumberLiteral{Value: 10},
	}, nil)
	if err != nil {
		t.Fatalf("UnaryMinus int: %v", err)
	}
	if val != int64(-10) {
		t.Errorf("Expected -10, got %v (type %T)", val, val)
	}

	// Negate float
	val, err = EvalExpression(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.NumberLiteral{Value: 3.14},
	}, nil)
	if err != nil {
		t.Fatalf("UnaryMinus float: %v", err)
	}
	if val != -3.14 {
		t.Errorf("Expected -3.14, got %v", val)
	}

	// Negate non-numeric (should return as-is)
	val, err = EvalExpression(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.StringLiteral{Value: "text"},
	}, nil)
	if err != nil {
		t.Fatalf("UnaryMinus string: %v", err)
	}
}

func TestEvalExpr_UnaryNot(t *testing.T) {
	// NOT true
	val, err := EvalExpression(&query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil {
		t.Fatalf("NOT true: %v", err)
	}
	if val != false {
		t.Errorf("Expected false, got %v", val)
	}

	// NOT false
	val, err = EvalExpression(&query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil {
		t.Fatalf("NOT false: %v", err)
	}
	if val != true {
		t.Errorf("Expected true, got %v", val)
	}

	// NOT NULL = NULL
	val, err = EvalExpression(&query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("NOT NULL: %v", err)
	}
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}
}

func TestEvalExpr_BinaryArithmetic(t *testing.T) {
	tests := []struct {
		op       query.TokenType
		left     float64
		right    float64
		expected interface{}
	}{
		{query.TokenPlus, 3, 4, int64(7)},
		{query.TokenMinus, 10, 3, int64(7)},
		{query.TokenStar, 5, 6, int64(30)},
		{query.TokenSlash, 10, 3, float64(10) / float64(3)},
		{query.TokenPercent, 10, 3, int64(1)},
	}

	for _, tc := range tests {
		val, err := EvalExpression(&query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: tc.left},
			Operator: tc.op,
			Right:    &query.NumberLiteral{Value: tc.right},
		}, nil)
		if err != nil {
			t.Errorf("Op %v: error %v", tc.op, err)
			continue
		}
		if fmt.Sprintf("%v", val) != fmt.Sprintf("%v", tc.expected) {
			t.Errorf("Op %v: expected %v, got %v", tc.op, tc.expected, val)
		}
	}
}

func TestEvalExpr_BinaryDivisionByZero(t *testing.T) {
	_, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.NumberLiteral{Value: 10},
		Operator: query.TokenSlash,
		Right:    &query.NumberLiteral{Value: 0},
	}, nil)
	if err == nil {
		t.Error("Expected division by zero error")
	}

	_, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NumberLiteral{Value: 10},
		Operator: query.TokenPercent,
		Right:    &query.NumberLiteral{Value: 0},
	}, nil)
	if err == nil {
		t.Error("Expected modulo by zero error")
	}
}

func TestEvalExpr_BinaryComparisons(t *testing.T) {
	tests := []struct {
		op       query.TokenType
		left     interface{}
		right    interface{}
		expected bool
	}{
		{query.TokenEq, float64(5), float64(5), true},
		{query.TokenNeq, float64(5), float64(3), true},
		{query.TokenLt, float64(3), float64(5), true},
		{query.TokenGt, float64(5), float64(3), true},
		{query.TokenLte, float64(5), float64(5), true},
		{query.TokenGte, float64(5), float64(5), true},
	}

	for _, tc := range tests {
		val, err := EvalExpression(&query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: tc.left.(float64)},
			Operator: tc.op,
			Right:    &query.NumberLiteral{Value: tc.right.(float64)},
		}, nil)
		if err != nil {
			t.Errorf("Comparison %v: error %v", tc.op, err)
			continue
		}
		if val != tc.expected {
			t.Errorf("Comparison %v: expected %v, got %v", tc.op, tc.expected, val)
		}
	}
}

func TestEvalExpr_BinaryLogical(t *testing.T) {
	// AND
	val, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil {
		t.Fatalf("AND: %v", err)
	}
	if val != false {
		t.Errorf("true AND false: expected false, got %v", val)
	}

	// OR
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil {
		t.Fatalf("OR: %v", err)
	}
	if val != true {
		t.Errorf("false OR true: expected true, got %v", val)
	}
}

func TestEvalExpr_BinaryNullPropagation(t *testing.T) {
	// NULL + 1 = NULL
	val, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenPlus,
		Right:    &query.NumberLiteral{Value: 1},
	}, nil)
	if err != nil {
		t.Fatalf("NULL + 1: %v", err)
	}
	if val != nil {
		t.Errorf("NULL + 1: expected nil, got %v", val)
	}

	// NULL AND true = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil {
		t.Fatalf("NULL AND true: %v", err)
	}
	if val != nil {
		t.Errorf("NULL AND true: expected nil, got %v", val)
	}

	// NULL AND false = false
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil {
		t.Fatalf("NULL AND false: %v", err)
	}
	if val != false {
		t.Errorf("NULL AND false: expected false, got %v", val)
	}

	// true AND NULL = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("true AND NULL: %v", err)
	}
	if val != nil {
		t.Errorf("true AND NULL: expected nil, got %v", val)
	}

	// false AND NULL = false
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("false AND NULL: %v", err)
	}
	if val != false {
		t.Errorf("false AND NULL: expected false, got %v", val)
	}

	// NULL AND NULL = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("NULL AND NULL: %v", err)
	}
	if val != nil {
		t.Errorf("NULL AND NULL: expected nil, got %v", val)
	}

	// NULL OR true = true
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil {
		t.Fatalf("NULL OR true: %v", err)
	}
	if val != true {
		t.Errorf("NULL OR true: expected true, got %v", val)
	}

	// NULL OR false = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil {
		t.Fatalf("NULL OR false: %v", err)
	}
	if val != nil {
		t.Errorf("NULL OR false: expected nil, got %v", val)
	}

	// true OR NULL = true
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("true OR NULL: %v", err)
	}
	if val != true {
		t.Errorf("true OR NULL: expected true, got %v", val)
	}

	// false OR NULL = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("false OR NULL: %v", err)
	}
	if val != nil {
		t.Errorf("false OR NULL: expected nil, got %v", val)
	}

	// NULL OR NULL = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("NULL OR NULL: %v", err)
	}
	if val != nil {
		t.Errorf("NULL OR NULL: expected nil, got %v", val)
	}

	// NULL || 'text' = NULL (concat)
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenConcat,
		Right:    &query.StringLiteral{Value: "text"},
	}, nil)
	if err != nil {
		t.Fatalf("NULL concat: %v", err)
	}
	if val != nil {
		t.Errorf("NULL || 'text': expected nil, got %v", val)
	}
}

func TestEvalExpr_Concat(t *testing.T) {
	val, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.StringLiteral{Value: "Hello"},
		Operator: query.TokenConcat,
		Right:    &query.StringLiteral{Value: " World"},
	}, nil)
	if err != nil {
		t.Fatalf("Concat: %v", err)
	}
	if val != "Hello World" {
		t.Errorf("Expected 'Hello World', got %v", val)
	}
}

func TestEvalExpr_CaseSimple(t *testing.T) {
	// Simple CASE: CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END
	val, err := EvalExpression(&query.CaseExpr{
		Expr: &query.NumberLiteral{Value: 2},
		Whens: []*query.WhenClause{
			{Condition: &query.NumberLiteral{Value: 1}, Result: &query.StringLiteral{Value: "a"}},
			{Condition: &query.NumberLiteral{Value: 2}, Result: &query.StringLiteral{Value: "b"}},
		},
		Else: &query.StringLiteral{Value: "c"},
	}, nil)
	if err != nil {
		t.Fatalf("Simple CASE: %v", err)
	}
	if val != "b" {
		t.Errorf("Expected 'b', got %v", val)
	}

	// CASE NULL WHEN NULL should not match (SQL standard)
	val, err = EvalExpression(&query.CaseExpr{
		Expr: &query.NullLiteral{},
		Whens: []*query.WhenClause{
			{Condition: &query.NullLiteral{}, Result: &query.StringLiteral{Value: "matched"}},
		},
		Else: &query.StringLiteral{Value: "default"},
	}, nil)
	if err != nil {
		t.Fatalf("CASE NULL WHEN NULL: %v", err)
	}
	if val != "default" {
		t.Errorf("Expected 'default', got %v", val)
	}

	// No match, no else → NULL
	val, err = EvalExpression(&query.CaseExpr{
		Expr: &query.NumberLiteral{Value: 99},
		Whens: []*query.WhenClause{
			{Condition: &query.NumberLiteral{Value: 1}, Result: &query.StringLiteral{Value: "a"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("CASE no match: %v", err)
	}
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}
}

func TestEvalExpr_CaseSearched(t *testing.T) {
	// Searched CASE: CASE WHEN false THEN 'a' WHEN true THEN 'b' END
	val, err := EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{
			{Condition: &query.BooleanLiteral{Value: false}, Result: &query.StringLiteral{Value: "a"}},
			{Condition: &query.BooleanLiteral{Value: true}, Result: &query.StringLiteral{Value: "b"}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Searched CASE: %v", err)
	}
	if val != "b" {
		t.Errorf("Expected 'b', got %v", val)
	}
}

func TestEvalExpr_Cast(t *testing.T) {
	// CAST to INTEGER from float
	val, err := EvalExpression(&query.CastExpr{
		Expr:     &query.NumberLiteral{Value: 3.7},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil {
		t.Fatalf("CAST to INTEGER: %v", err)
	}
	if val != int64(3) {
		t.Errorf("Expected int64(3), got %v (type %T)", val, val)
	}

	// CAST to INTEGER from string
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "42"},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil {
		t.Fatalf("CAST string to INTEGER: %v", err)
	}
	if val != int64(42) {
		t.Errorf("Expected int64(42), got %v (type %T)", val, val)
	}

	// CAST to INTEGER from non-numeric string
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "abc"},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil {
		t.Fatalf("CAST 'abc' to INTEGER: %v", err)
	}
	if val != int64(0) {
		t.Errorf("Expected int64(0), got %v", val)
	}

	// CAST to REAL from string
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "3.14"},
		DataType: query.TokenReal,
	}, nil)
	if err != nil {
		t.Fatalf("CAST to REAL: %v", err)
	}
	if val != float64(3.14) {
		t.Errorf("Expected 3.14, got %v", val)
	}

	// CAST to TEXT
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.NumberLiteral{Value: 42},
		DataType: query.TokenText,
	}, nil)
	if err != nil {
		t.Fatalf("CAST to TEXT: %v", err)
	}
	if val != "42" {
		t.Errorf("Expected '42', got %v", val)
	}

	// CAST NULL
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.NullLiteral{},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil {
		t.Fatalf("CAST NULL: %v", err)
	}
	if val != nil {
		t.Errorf("Expected nil, got %v", val)
	}
}

func TestEvalExpr_Functions(t *testing.T) {
	tests := []struct {
		name     string
		fn       string
		args     []query.Expression
		expected string
	}{
		{"COALESCE nil,val", "COALESCE", []query.Expression{&query.NullLiteral{}, &query.StringLiteral{Value: "b"}}, "b"},
		{"COALESCE all nil", "COALESCE", []query.Expression{&query.NullLiteral{}, &query.NullLiteral{}}, "<nil>"},
		{"NULLIF equal", "NULLIF", []query.Expression{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}, "<nil>"},
		{"NULLIF different", "NULLIF", []query.Expression{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 2}}, "1"},
		{"IIF true", "IIF", []query.Expression{&query.BooleanLiteral{Value: true}, &query.StringLiteral{Value: "yes"}, &query.StringLiteral{Value: "no"}}, "yes"},
		{"IIF false", "IIF", []query.Expression{&query.BooleanLiteral{Value: false}, &query.StringLiteral{Value: "yes"}, &query.StringLiteral{Value: "no"}}, "no"},
		{"ABS negative", "ABS", []query.Expression{&query.NumberLiteral{Value: -5}}, "5"},
		{"ABS positive", "ABS", []query.Expression{&query.NumberLiteral{Value: 3}}, "3"},
		{"UPPER", "UPPER", []query.Expression{&query.StringLiteral{Value: "hello"}}, "HELLO"},
		{"LOWER", "LOWER", []query.Expression{&query.StringLiteral{Value: "HELLO"}}, "hello"},
		{"LENGTH", "LENGTH", []query.Expression{&query.StringLiteral{Value: "abc"}}, "3"},
		{"CONCAT", "CONCAT", []query.Expression{&query.StringLiteral{Value: "a"}, &query.StringLiteral{Value: "b"}}, "ab"},
		{"IFNULL non-nil", "IFNULL", []query.Expression{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 2}}, "1"},
		{"IFNULL nil", "IFNULL", []query.Expression{&query.NullLiteral{}, &query.NumberLiteral{Value: 2}}, "2"},
		{"TRIM", "TRIM", []query.Expression{&query.StringLiteral{Value: "  hi  "}}, "hi"},
		{"LTRIM", "LTRIM", []query.Expression{&query.StringLiteral{Value: "  hi"}}, "hi"},
		{"RTRIM", "RTRIM", []query.Expression{&query.StringLiteral{Value: "hi  "}}, "hi"},
		{"SUBSTR 2-arg", "SUBSTR", []query.Expression{&query.StringLiteral{Value: "hello"}, &query.NumberLiteral{Value: 2}}, "ello"},
		{"SUBSTR 3-arg", "SUBSTR", []query.Expression{&query.StringLiteral{Value: "hello"}, &query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 3}}, "ell"},
		{"REPLACE", "REPLACE", []query.Expression{&query.StringLiteral{Value: "hello"}, &query.StringLiteral{Value: "l"}, &query.StringLiteral{Value: "r"}}, "herro"},
		{"INSTR found", "INSTR", []query.Expression{&query.StringLiteral{Value: "hello"}, &query.StringLiteral{Value: "ll"}}, "3"},
		{"INSTR not found", "INSTR", []query.Expression{&query.StringLiteral{Value: "hello"}, &query.StringLiteral{Value: "xyz"}}, "0"},
		{"ROUND", "ROUND", []query.Expression{&query.NumberLiteral{Value: 3.456}, &query.NumberLiteral{Value: 2}}, "3.46"},
		{"FLOOR", "FLOOR", []query.Expression{&query.NumberLiteral{Value: 3.7}}, "3"},
		{"CEIL", "CEIL", []query.Expression{&query.NumberLiteral{Value: 3.2}}, "4"},
		{"TYPEOF int", "TYPEOF", []query.Expression{&query.NumberLiteral{Value: 5}}, "integer"},
		{"TYPEOF text", "TYPEOF", []query.Expression{&query.StringLiteral{Value: "hi"}}, "text"},
		{"TYPEOF null", "TYPEOF", []query.Expression{&query.NullLiteral{}}, "null"},
		{"TYPEOF bool", "TYPEOF", []query.Expression{&query.BooleanLiteral{Value: true}}, "integer"},
		{"TYPEOF real", "TYPEOF", []query.Expression{&query.NumberLiteral{Value: 3.14}}, "real"},
		{"MIN 2-arg", "MIN", []query.Expression{&query.NumberLiteral{Value: 5}, &query.NumberLiteral{Value: 3}}, "3"},
		{"MAX 2-arg", "MAX", []query.Expression{&query.NumberLiteral{Value: 5}, &query.NumberLiteral{Value: 3}}, "5"},
		{"REVERSE", "REVERSE", []query.Expression{&query.StringLiteral{Value: "abc"}}, "cba"},
		{"REPEAT", "REPEAT", []query.Expression{&query.StringLiteral{Value: "ab"}, &query.NumberLiteral{Value: 3}}, "ababab"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := EvalExpression(&query.FunctionCall{
				Name: tc.fn,
				Args: tc.args,
			}, nil)
			if err != nil {
				t.Fatalf("%s: error %v", tc.name, err)
			}
			got := fmt.Sprintf("%v", val)
			if got != tc.expected {
				t.Errorf("%s: expected %q, got %q", tc.name, tc.expected, got)
			}
		})
	}
}

func TestEvalExpr_FunctionEdgeCases(t *testing.T) {
	// SUBSTR with start beyond string length
	val, err := EvalExpression(&query.FunctionCall{
		Name: "SUBSTR",
		Args: []query.Expression{&query.StringLiteral{Value: "hi"}, &query.NumberLiteral{Value: 100}},
	}, nil)
	if err != nil {
		t.Fatalf("SUBSTR beyond: %v", err)
	}
	if val != "" {
		t.Errorf("Expected empty, got %v", val)
	}

	// SUBSTR with negative length
	val, err = EvalExpression(&query.FunctionCall{
		Name: "SUBSTR",
		Args: []query.Expression{&query.StringLiteral{Value: "hello"}, &query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: -1}},
	}, nil)
	if err != nil {
		t.Fatalf("SUBSTR neg length: %v", err)
	}
	if val != "" {
		t.Errorf("Expected empty, got %v", val)
	}

	// REPLACE with empty search string
	val, err = EvalExpression(&query.FunctionCall{
		Name: "REPLACE",
		Args: []query.Expression{&query.StringLiteral{Value: "hello"}, &query.StringLiteral{Value: ""}, &query.StringLiteral{Value: "x"}},
	}, nil)
	if err != nil {
		t.Fatalf("REPLACE empty: %v", err)
	}
	if val != "hello" {
		t.Errorf("Expected 'hello', got %v", val)
	}

	// REPEAT with 0 count
	val, err = EvalExpression(&query.FunctionCall{
		Name: "REPEAT",
		Args: []query.Expression{&query.StringLiteral{Value: "ab"}, &query.NumberLiteral{Value: 0}},
	}, nil)
	if err != nil {
		t.Fatalf("REPEAT 0: %v", err)
	}
	if val != "" {
		t.Errorf("Expected empty, got %v", val)
	}

	// Unknown function
	_, err = EvalExpression(&query.FunctionCall{
		Name: "UNKNOWN_FUNC",
		Args: []query.Expression{&query.NumberLiteral{Value: 1}},
	}, nil)
	if err == nil {
		t.Error("Expected error for unknown function")
	}
}

func TestEvalExpr_AliasExpr(t *testing.T) {
	val, err := EvalExpression(&query.AliasExpr{
		Expr:  &query.NumberLiteral{Value: 42},
		Alias: "answer",
	}, nil)
	if err != nil {
		t.Fatalf("AliasExpr: %v", err)
	}
	if val != float64(42) {
		t.Errorf("Expected 42, got %v", val)
	}
}

func TestEvalExpr_UnsupportedType(t *testing.T) {
	// StarExpr is not handled by EvalExpression
	_, err := EvalExpression(&query.StarExpr{}, nil)
	if err == nil {
		t.Error("Expected error for unsupported expression type")
	}
}

// ==============================================================
// exprToSQL tests (was at 52.4%, now 57.1%)
// ==============================================================

func TestExprToSQL_AllPaths(t *testing.T) {
	tests := []struct {
		name     string
		expr     query.Expression
		expected string
	}{
		{"nil", nil, ""},
		{"number", &query.NumberLiteral{Value: 42}, "42"},
		{"string", &query.StringLiteral{Value: "it's"}, "'it''s'"},
		{"bool true", &query.BooleanLiteral{Value: true}, "TRUE"},
		{"bool false", &query.BooleanLiteral{Value: false}, "FALSE"},
		{"null", &query.NullLiteral{}, "NULL"},
		{"identifier", &query.Identifier{Name: "col1"}, "col1"},
		{"qualified", &query.QualifiedIdentifier{Table: "t", Column: "c"}, "t.c"},
		{"unary not", &query.UnaryExpr{Operator: query.TokenNot, Expr: &query.Identifier{Name: "x"}}, "NOT x"},
		{"unary minus", &query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.NumberLiteral{Value: 5}}, "-5"},
		{"function", &query.FunctionCall{Name: "UPPER", Args: []query.Expression{&query.Identifier{Name: "name"}}}, "UPPER(name)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := exprToSQL(tc.expr)
			if got != tc.expected {
				t.Errorf("exprToSQL %s: expected %q, got %q", tc.name, tc.expected, got)
			}
		})
	}

	// Binary operators
	binOps := []struct {
		op       query.TokenType
		expected string
	}{
		{query.TokenEq, "="},
		{query.TokenNeq, "!="},
		{query.TokenLt, "<"},
		{query.TokenGt, ">"},
		{query.TokenLte, "<="},
		{query.TokenGte, ">="},
		{query.TokenAnd, "AND"},
		{query.TokenOr, "OR"},
		{query.TokenPlus, "+"},
		{query.TokenMinus, "-"},
		{query.TokenStar, "*"},
		{query.TokenSlash, "/"},
		{query.TokenPercent, "%"},
		{query.TokenConcat, "||"},
	}

	for _, bo := range binOps {
		got := exprToSQL(&query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 1},
			Operator: bo.op,
			Right:    &query.NumberLiteral{Value: 2},
		})
		expected := fmt.Sprintf("(1 %s 2)", bo.expected)
		if got != expected {
			t.Errorf("Binary %s: expected %q, got %q", bo.expected, expected, got)
		}
	}
}

// ==============================================================
// valueToExpr tests (was at 36.4%)
// ==============================================================

func TestValueToExpr_AllTypes(t *testing.T) {
	// nil → NullLiteral
	expr := valueToExpr(nil)
	if _, ok := expr.(*query.NullLiteral); !ok {
		t.Errorf("nil: expected NullLiteral, got %T", expr)
	}

	// string → StringLiteral
	expr = valueToExpr("hello")
	if sl, ok := expr.(*query.StringLiteral); !ok || sl.Value != "hello" {
		t.Errorf("string: expected StringLiteral(hello), got %T %v", expr, expr)
	}

	// float64 → NumberLiteral
	expr = valueToExpr(float64(3.14))
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 3.14 {
		t.Errorf("float64: expected NumberLiteral(3.14), got %T %v", expr, expr)
	}

	// int → NumberLiteral
	expr = valueToExpr(int(42))
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 42 {
		t.Errorf("int: expected NumberLiteral(42), got %T %v", expr, expr)
	}

	// int64 → NumberLiteral
	expr = valueToExpr(int64(99))
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 99 {
		t.Errorf("int64: expected NumberLiteral(99), got %T %v", expr, expr)
	}

	// bool true → NumberLiteral(1)
	expr = valueToExpr(true)
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 1 {
		t.Errorf("true: expected NumberLiteral(1), got %T %v", expr, expr)
	}

	// bool false → NumberLiteral(0)
	expr = valueToExpr(false)
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 0 {
		t.Errorf("false: expected NumberLiteral(0), got %T %v", expr, expr)
	}

	// unknown type → StringLiteral with Sprintf
	expr = valueToExpr([]int{1, 2, 3})
	if _, ok := expr.(*query.StringLiteral); !ok {
		t.Errorf("unknown: expected StringLiteral, got %T", expr)
	}
}

// ==============================================================
// catalogCompareValues tests (was at 0%, now 100%)
// ==============================================================

func TestCatalogCompareValues_Extended(t *testing.T) {
	// String comparison
	if catalogCompareValues("apple", "banana") >= 0 {
		t.Error("apple should be less than banana")
	}
	if catalogCompareValues("banana", "apple") <= 0 {
		t.Error("banana should be greater than apple")
	}
	if catalogCompareValues("same", "same") != 0 {
		t.Error("same should equal same")
	}
}

// ==============================================================
// encodeRow additional paths
// ==============================================================

func TestEncodeRow_AllTypes(t *testing.T) {
	exprs := []query.Expression{
		&query.StringLiteral{Value: "test"},
		&query.NumberLiteral{Value: 42},
		&query.BooleanLiteral{Value: true},
		&query.NullLiteral{},
		&query.Identifier{Name: "col_name"},
	}
	data, err := encodeRow(exprs, nil)
	if err != nil {
		t.Fatalf("encodeRow: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty data")
	}

	// With placeholders
	exprs = []query.Expression{
		&query.PlaceholderExpr{Index: 0},
		&query.PlaceholderExpr{Index: 1},
	}
	data, err = encodeRow(exprs, []interface{}{"a", int64(5)})
	if err != nil {
		t.Fatalf("encodeRow placeholders: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty data")
	}
}
