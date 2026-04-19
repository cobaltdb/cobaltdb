package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestEvalExpressionBranches(t *testing.T) {
	// Placeholder out of range
	_, err := EvalExpression(&query.PlaceholderExpr{Index: 5}, []interface{}{1})
	if err == nil {
		t.Error("Expected placeholder out of range error")
	}

	// Unary minus with float
	val, err := EvalExpression(&query.UnaryExpr{Operator: query.TokenMinus, Expr: &query.NumberLiteral{Value: 3.14}}, nil)
	if err != nil || val != -3.14 {
		t.Errorf("Unary minus float: got %v, %v", val, err)
	}

	// Unary NOT with nil
	val, err = EvalExpression(&query.UnaryExpr{Operator: query.TokenNot, Expr: &query.NullLiteral{}}, nil)
	if err != nil || val != nil {
		t.Errorf("Unary NOT nil: got %v, %v", val, err)
	}

	// Binary addition with floats
	val, err = EvalExpression(&query.BinaryExpr{Operator: query.TokenPlus, Left: &query.NumberLiteral{Value: 1.5}, Right: &query.NumberLiteral{Value: 2.5}}, nil)
	if err != nil || val != 4.0 {
		t.Errorf("Float add: got %v, %v", val, err)
	}

	// Binary subtraction with floats (whole numbers treated as ints)
	val, err = EvalExpression(&query.BinaryExpr{Operator: query.TokenMinus, Left: &query.NumberLiteral{Value: 5.0}, Right: &query.NumberLiteral{Value: 2.0}}, nil)
	if err != nil || val != int64(3) {
		t.Errorf("Float sub: got %v, %v", val, err)
	}

	// Binary multiplication with floats (whole numbers treated as ints)
	val, err = EvalExpression(&query.BinaryExpr{Operator: query.TokenStar, Left: &query.NumberLiteral{Value: 2.0}, Right: &query.NumberLiteral{Value: 3.0}}, nil)
	if err != nil || val != int64(6) {
		t.Errorf("Float mul: got %v, %v", val, err)
	}

	// Unsupported binary operator
	_, err = EvalExpression(&query.BinaryExpr{Operator: query.TokenArrow, Left: &query.NumberLiteral{Value: 1}, Right: &query.NumberLiteral{Value: 2}}, nil)
	if err == nil {
		t.Error("Expected unsupported operator error")
	}

	// Binary expr with error in left
	_, err = EvalExpression(&query.BinaryExpr{Operator: query.TokenPlus, Left: &query.PlaceholderExpr{Index: 99}, Right: &query.NumberLiteral{Value: 1}}, nil)
	if err == nil {
		t.Error("Expected left eval error")
	}

	// Binary expr with error in right
	_, err = EvalExpression(&query.BinaryExpr{Operator: query.TokenPlus, Left: &query.NumberLiteral{Value: 1}, Right: &query.PlaceholderExpr{Index: 99}}, nil)
	if err == nil {
		t.Error("Expected right eval error")
	}

	// CASE with error in WHEN condition
	val, err = EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{{Condition: &query.PlaceholderExpr{Index: 99}, Result: &query.NumberLiteral{Value: 1}}},
	}, nil)
	if err != nil || val != nil {
		t.Errorf("CASE with error: got %v, %v", val, err)
	}

	// CAST string to int
	val, err = EvalExpression(&query.CastExpr{Expr: &query.StringLiteral{Value: "42"}, DataType: query.TokenInteger}, nil)
	if err != nil || val != int64(42) {
		t.Errorf("CAST string to int: got %v, %v", val, err)
	}

	// CAST invalid string to int
	val, err = EvalExpression(&query.CastExpr{Expr: &query.StringLiteral{Value: "abc"}, DataType: query.TokenInteger}, nil)
	if err != nil || val != int64(0) {
		t.Errorf("CAST invalid string to int: got %v, %v", val, err)
	}

	// CAST string to real
	val, err = EvalExpression(&query.CastExpr{Expr: &query.StringLiteral{Value: "3.14"}, DataType: query.TokenReal}, nil)
	if err != nil || val != 3.14 {
		t.Errorf("CAST string to real: got %v, %v", val, err)
	}

	// CAST invalid string to real
	val, err = EvalExpression(&query.CastExpr{Expr: &query.StringLiteral{Value: "abc"}, DataType: query.TokenReal}, nil)
	if err != nil || val != float64(0) {
		t.Errorf("CAST invalid string to real: got %v, %v", val, err)
	}

	// Function arg eval error
	_, err = EvalExpression(&query.FunctionCall{Name: "ABS", Args: []query.Expression{&query.PlaceholderExpr{Index: 99}}}, nil)
	if err == nil {
		t.Error("Expected function arg eval error")
	}

	// NULLIF with less than 2 args
	val, err = EvalExpression(&query.FunctionCall{Name: "NULLIF", Args: []query.Expression{&query.NumberLiteral{Value: 1}}}, nil)
	if err != nil || val != float64(1) {
		t.Errorf("NULLIF 1 arg: got %v, %v", val, err)
	}

	// IIF with less than 3 args
	val, err = EvalExpression(&query.FunctionCall{Name: "IIF", Args: []query.Expression{&query.BooleanLiteral{Value: true}}}, nil)
	if err != nil || val != nil {
		t.Errorf("IIF 1 arg: got %v, %v", val, err)
	}

	// ABS with nil
	val, err = EvalExpression(&query.FunctionCall{Name: "ABS", Args: []query.Expression{&query.NullLiteral{}}}, nil)
	if err != nil || val != nil {
		t.Errorf("ABS nil: got %v, %v", val, err)
	}

	// UPPER with nil
	val, err = EvalExpression(&query.FunctionCall{Name: "UPPER", Args: []query.Expression{&query.NullLiteral{}}}, nil)
	if err != nil || val != nil {
		t.Errorf("UPPER nil: got %v, %v", val, err)
	}

	// LOWER with nil
	val, err = EvalExpression(&query.FunctionCall{Name: "LOWER", Args: []query.Expression{&query.NullLiteral{}}}, nil)
	if err != nil || val != nil {
		t.Errorf("LOWER nil: got %v, %v", val, err)
	}

	// LENGTH with nil
	val, err = EvalExpression(&query.FunctionCall{Name: "LENGTH", Args: []query.Expression{&query.NullLiteral{}}}, nil)
	if err != nil || val != nil {
		t.Errorf("LENGTH nil: got %v, %v", val, err)
	}

	// IFNULL with less than 2 args
	val, err = EvalExpression(&query.FunctionCall{Name: "IFNULL", Args: []query.Expression{&query.NumberLiteral{Value: 1}}}, nil)
	if err != nil || val != nil {
		t.Errorf("IFNULL 1 arg: got %v, %v", val, err)
	}

	// TRIM with nil
	val, err = EvalExpression(&query.FunctionCall{Name: "TRIM", Args: []query.Expression{&query.NullLiteral{}}}, nil)
	if err != nil || val != nil {
		t.Errorf("TRIM nil: got %v, %v", val, err)
	}

	// LTRIM with nil
	val, err = EvalExpression(&query.FunctionCall{Name: "LTRIM", Args: []query.Expression{&query.NullLiteral{}}}, nil)
	if err != nil || val != nil {
		t.Errorf("LTRIM nil: got %v, %v", val, err)
	}

	// RTRIM with nil
	val, err = EvalExpression(&query.FunctionCall{Name: "RTRIM", Args: []query.Expression{&query.NullLiteral{}}}, nil)
	if err != nil || val != nil {
		t.Errorf("RTRIM nil: got %v, %v", val, err)
	}

	// SUBSTR with less than 2 args
	val, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{&query.StringLiteral{Value: "hello"}}}, nil)
	if err != nil || val != nil {
		t.Errorf("SUBSTR 1 arg: got %v, %v", val, err)
	}

	// SUBSTR with nil arg
	val, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{&query.NullLiteral{}, &query.NumberLiteral{Value: 1}}}, nil)
	if err != nil || val != nil {
		t.Errorf("SUBSTR nil arg: got %v, %v", val, err)
	}

	// SUBSTR with start negative (treated as 0)
	val, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{&query.StringLiteral{Value: "hello"}, &query.NumberLiteral{Value: -5}}}, nil)
	if err != nil || val != "hello" {
		t.Errorf("SUBSTR negative start: got %v, %v", val, err)
	}

	// SUBSTR with start beyond length
	val, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{&query.StringLiteral{Value: "hi"}, &query.NumberLiteral{Value: 10}}}, nil)
	if err != nil || val != "" {
		t.Errorf("SUBSTR start beyond: got %v, %v", val, err)
	}

	// SUBSTR with negative length
	val, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{&query.StringLiteral{Value: "hello"}, &query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: -1}}}, nil)
	if err != nil || val != "" {
		t.Errorf("SUBSTR negative length: got %v, %v", val, err)
	}

	// SUBSTR with length truncated
	val, err = EvalExpression(&query.FunctionCall{Name: "SUBSTR", Args: []query.Expression{&query.StringLiteral{Value: "hi"}, &query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 100}}}, nil)
	if err != nil || val != "hi" {
		t.Errorf("SUBSTR length truncated: got %v, %v", val, err)
	}

	// REPLACE max length
	longStr := make([]byte, maxStringResultLen+1)
	for i := range longStr {
		longStr[i] = 'a'
	}
	_, err = EvalExpression(&query.FunctionCall{Name: "REPLACE", Args: []query.Expression{
		&query.StringLiteral{Value: string(longStr)},
		&query.StringLiteral{Value: "a"},
		&query.StringLiteral{Value: "bb"},
	}}, nil)
	if err == nil {
		t.Error("Expected REPLACE max length error")
	}
}
