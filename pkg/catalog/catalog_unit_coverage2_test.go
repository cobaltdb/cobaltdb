package catalog

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newUnitTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

func newUnitTestBTree(t *testing.T) *btree.BTree {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

// ===================== EvalExpression Tests =====================

func TestUnitEvalExpression_Literals(t *testing.T) {
	// String literal
	val, err := EvalExpression(&query.StringLiteral{Value: "hello"}, nil)
	if err != nil || val != "hello" {
		t.Fatalf("string literal: got %v, err %v", val, err)
	}

	// Number literal
	val, err = EvalExpression(&query.NumberLiteral{Value: 42.0}, nil)
	if err != nil || val != 42.0 {
		t.Fatalf("number literal: got %v, err %v", val, err)
	}

	// Boolean literal
	val, err = EvalExpression(&query.BooleanLiteral{Value: true}, nil)
	if err != nil || val != true {
		t.Fatalf("boolean literal: got %v, err %v", val, err)
	}

	// Null literal
	val, err = EvalExpression(&query.NullLiteral{}, nil)
	if err != nil || val != nil {
		t.Fatalf("null literal: got %v, err %v", val, err)
	}

	// Identifier
	val, err = EvalExpression(&query.Identifier{Name: "col1"}, nil)
	if err != nil || val != "col1" {
		t.Fatalf("identifier: got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_Placeholder(t *testing.T) {
	// Valid placeholder
	val, err := EvalExpression(&query.PlaceholderExpr{Index: 0}, []interface{}{"test"})
	if err != nil || val != "test" {
		t.Fatalf("placeholder: got %v, err %v", val, err)
	}

	// Out of range placeholder
	_, err = EvalExpression(&query.PlaceholderExpr{Index: 5}, []interface{}{"test"})
	if err == nil {
		t.Fatal("expected error for out of range placeholder")
	}
}

func TestUnitEvalExpression_UnaryMinus(t *testing.T) {
	// Negate integer
	val, err := EvalExpression(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.NumberLiteral{Value: 5.0},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 5.0 is integer type, so result is -int64(5)
	if val != int64(-5) {
		t.Fatalf("unary minus int: got %v (%T)", val, val)
	}

	// Negate float
	val, err = EvalExpression(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.NumberLiteral{Value: 3.14},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if val != -3.14 {
		t.Fatalf("unary minus float: got %v", val)
	}
}

func TestUnitEvalExpression_UnaryNot(t *testing.T) {
	val, err := EvalExpression(&query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if val != false {
		t.Fatalf("NOT true: got %v", val)
	}

	// NOT NULL = NULL
	val, err = EvalExpression(&query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Fatalf("NOT NULL: got %v", val)
	}
}

func TestUnitEvalExpression_BinaryArith(t *testing.T) {
	tests := []struct {
		name string
		op   query.TokenType
		l, r float64
		want interface{}
	}{
		{"add int", query.TokenPlus, 3, 4, int64(7)},
		{"sub int", query.TokenMinus, 10, 3, int64(7)},
		{"mul int", query.TokenStar, 5, 6, int64(30)},
		{"div", query.TokenSlash, 10, 4, 2.5},
		{"mod", query.TokenPercent, 10, 3, int64(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := EvalExpression(&query.BinaryExpr{
				Left:     &query.NumberLiteral{Value: tc.l},
				Operator: tc.op,
				Right:    &query.NumberLiteral{Value: tc.r},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if val != tc.want {
				t.Fatalf("got %v (%T), want %v (%T)", val, val, tc.want, tc.want)
			}
		})
	}
}

func TestUnitEvalExpression_DivByZero(t *testing.T) {
	_, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.NumberLiteral{Value: 10},
		Operator: query.TokenSlash,
		Right:    &query.NumberLiteral{Value: 0},
	}, nil)
	if err == nil {
		t.Fatal("expected division by zero error")
	}

	_, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NumberLiteral{Value: 10},
		Operator: query.TokenPercent,
		Right:    &query.NumberLiteral{Value: 0},
	}, nil)
	if err == nil {
		t.Fatal("expected modulo by zero error")
	}
}

func TestUnitEvalExpression_BinaryCompare(t *testing.T) {
	tests := []struct {
		name string
		op   query.TokenType
		l, r float64
		want bool
	}{
		{"eq true", query.TokenEq, 5, 5, true},
		{"eq false", query.TokenEq, 5, 6, false},
		{"neq", query.TokenNeq, 5, 6, true},
		{"lt", query.TokenLt, 3, 5, true},
		{"gt", query.TokenGt, 5, 3, true},
		{"lte", query.TokenLte, 5, 5, true},
		{"gte", query.TokenGte, 5, 5, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := EvalExpression(&query.BinaryExpr{
				Left:     &query.NumberLiteral{Value: tc.l},
				Operator: tc.op,
				Right:    &query.NumberLiteral{Value: tc.r},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if val != tc.want {
				t.Fatalf("got %v, want %v", val, tc.want)
			}
		})
	}
}

func TestUnitEvalExpression_BinaryLogic(t *testing.T) {
	val, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || val != false {
		t.Fatalf("AND: got %v, err %v", val, err)
	}

	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil || val != true {
		t.Fatalf("OR: got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_NullPropagation(t *testing.T) {
	// NULL + 5 = NULL
	val, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenPlus,
		Right:    &query.NumberLiteral{Value: 5},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("NULL + 5: got %v, err %v", val, err)
	}

	// NULL AND false = false
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || val != false {
		t.Fatalf("NULL AND false: got %v, err %v", val, err)
	}

	// NULL AND true = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("NULL AND true: got %v, err %v", val, err)
	}

	// NULL OR true = true
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil || val != true {
		t.Fatalf("NULL OR true: got %v, err %v", val, err)
	}

	// NULL OR false = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("NULL OR false: got %v, err %v", val, err)
	}

	// true AND NULL = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("true AND NULL: got %v, err %v", val, err)
	}

	// false AND NULL = false
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil || val != false {
		t.Fatalf("false AND NULL: got %v, err %v", val, err)
	}

	// true OR NULL = true
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil || val != true {
		t.Fatalf("true OR NULL: got %v, err %v", val, err)
	}

	// false OR NULL = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("false OR NULL: got %v, err %v", val, err)
	}

	// NULL AND NULL = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("NULL AND NULL: got %v, err %v", val, err)
	}

	// NULL OR NULL = NULL
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("NULL OR NULL: got %v, err %v", val, err)
	}

	// NULL || 'x' = NULL (concat)
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenConcat,
		Right:    &query.StringLiteral{Value: "x"},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("NULL || 'x': got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_Concat(t *testing.T) {
	val, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.StringLiteral{Value: "hello"},
		Operator: query.TokenConcat,
		Right:    &query.StringLiteral{Value: " world"},
	}, nil)
	if err != nil || val != "hello world" {
		t.Fatalf("concat: got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_CaseSearched(t *testing.T) {
	// CASE WHEN true THEN 'yes' ELSE 'no' END
	val, err := EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{
			{Condition: &query.BooleanLiteral{Value: true}, Result: &query.StringLiteral{Value: "yes"}},
		},
		Else: &query.StringLiteral{Value: "no"},
	}, nil)
	if err != nil || val != "yes" {
		t.Fatalf("searched CASE: got %v, err %v", val, err)
	}

	// Fallthrough to ELSE
	val, err = EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{
			{Condition: &query.BooleanLiteral{Value: false}, Result: &query.StringLiteral{Value: "yes"}},
		},
		Else: &query.StringLiteral{Value: "no"},
	}, nil)
	if err != nil || val != "no" {
		t.Fatalf("searched CASE else: got %v, err %v", val, err)
	}

	// No ELSE, no match -> nil
	val, err = EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{
			{Condition: &query.BooleanLiteral{Value: false}, Result: &query.StringLiteral{Value: "yes"}},
		},
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("searched CASE no else: got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_CaseSimple(t *testing.T) {
	// CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END
	val, err := EvalExpression(&query.CaseExpr{
		Expr: &query.NumberLiteral{Value: 1},
		Whens: []*query.WhenClause{
			{Condition: &query.NumberLiteral{Value: 1}, Result: &query.StringLiteral{Value: "one"}},
			{Condition: &query.NumberLiteral{Value: 2}, Result: &query.StringLiteral{Value: "two"}},
		},
	}, nil)
	if err != nil || val != "one" {
		t.Fatalf("simple CASE: got %v, err %v", val, err)
	}

	// CASE NULL WHEN NULL -> no match (SQL standard: CASE NULL WHEN NULL is UNKNOWN)
	val, err = EvalExpression(&query.CaseExpr{
		Expr: &query.NullLiteral{},
		Whens: []*query.WhenClause{
			{Condition: &query.NullLiteral{}, Result: &query.StringLiteral{Value: "null"}},
		},
		Else: &query.StringLiteral{Value: "else"},
	}, nil)
	if err != nil || val != "else" {
		t.Fatalf("CASE NULL WHEN NULL: got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_Cast(t *testing.T) {
	// CAST('42' AS INTEGER)
	val, err := EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "42"},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil || val != int64(42) {
		t.Fatalf("CAST to int: got %v (%T), err %v", val, val, err)
	}

	// CAST('3.14' AS REAL)
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "3.14"},
		DataType: query.TokenReal,
	}, nil)
	if err != nil || val != 3.14 {
		t.Fatalf("CAST to real: got %v, err %v", val, err)
	}

	// CAST(42 AS TEXT)
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.NumberLiteral{Value: 42},
		DataType: query.TokenText,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if val != "42" {
		t.Fatalf("CAST to text: got %v", val)
	}

	// CAST(NULL AS INTEGER)
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.NullLiteral{},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil || val != nil {
		t.Fatalf("CAST NULL: got %v, err %v", val, err)
	}

	// CAST('abc' AS INTEGER) -> 0
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "abc"},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil || val != int64(0) {
		t.Fatalf("CAST 'abc' to int: got %v, err %v", val, err)
	}

	// CAST('abc' AS REAL) -> 0.0
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "abc"},
		DataType: query.TokenReal,
	}, nil)
	if err != nil || val != float64(0) {
		t.Fatalf("CAST 'abc' to real: got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_Functions(t *testing.T) {
	tests := []struct {
		name string
		fn   string
		args []query.Expression
		want interface{}
	}{
		{"COALESCE nil", "COALESCE", []query.Expression{&query.NullLiteral{}, &query.StringLiteral{Value: "b"}}, "b"},
		{"COALESCE first", "COALESCE", []query.Expression{&query.StringLiteral{Value: "a"}, &query.StringLiteral{Value: "b"}}, "a"},
		{"COALESCE all nil", "COALESCE", []query.Expression{&query.NullLiteral{}, &query.NullLiteral{}}, nil},
		{"NULLIF equal", "NULLIF", []query.Expression{&query.NumberLiteral{Value: 5}, &query.NumberLiteral{Value: 5}}, nil},
		{"NULLIF diff", "NULLIF", []query.Expression{&query.NumberLiteral{Value: 5}, &query.NumberLiteral{Value: 6}}, 5.0},
		{"ABS positive", "ABS", []query.Expression{&query.NumberLiteral{Value: 5}}, 5.0},
		{"ABS negative", "ABS", []query.Expression{&query.NumberLiteral{Value: -5}}, 5.0},
		{"UPPER", "UPPER", []query.Expression{&query.StringLiteral{Value: "hello"}}, "HELLO"},
		{"LOWER", "LOWER", []query.Expression{&query.StringLiteral{Value: "HELLO"}}, "hello"},
		{"LENGTH", "LENGTH", []query.Expression{&query.StringLiteral{Value: "hello"}}, 5},
		{"TRIM", "TRIM", []query.Expression{&query.StringLiteral{Value: "  hi  "}}, "hi"},
		{"LTRIM", "LTRIM", []query.Expression{&query.StringLiteral{Value: "  hi"}}, "hi"},
		{"RTRIM", "RTRIM", []query.Expression{&query.StringLiteral{Value: "hi  "}}, "hi"},
		{"CONCAT", "CONCAT", []query.Expression{&query.StringLiteral{Value: "a"}, &query.StringLiteral{Value: "b"}}, "ab"},
		{"IFNULL non-nil", "IFNULL", []query.Expression{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 2}}, 1.0},
		{"IFNULL nil", "IFNULL", []query.Expression{&query.NullLiteral{}, &query.NumberLiteral{Value: 2}}, 2.0},
		{"REPLACE", "REPLACE", []query.Expression{&query.StringLiteral{Value: "hello"}, &query.StringLiteral{Value: "l"}, &query.StringLiteral{Value: "r"}}, "herro"},
		{"INSTR found", "INSTR", []query.Expression{&query.StringLiteral{Value: "hello"}, &query.StringLiteral{Value: "ll"}}, int64(3)},
		{"INSTR not found", "INSTR", []query.Expression{&query.StringLiteral{Value: "hello"}, &query.StringLiteral{Value: "xyz"}}, int64(0)},
		{"TYPEOF null", "TYPEOF", []query.Expression{&query.NullLiteral{}}, "null"},
		{"TYPEOF text", "TYPEOF", []query.Expression{&query.StringLiteral{Value: "a"}}, "text"},
		{"TYPEOF integer", "TYPEOF", []query.Expression{&query.NumberLiteral{Value: 5}}, "integer"},
		{"REVERSE", "REVERSE", []query.Expression{&query.StringLiteral{Value: "abc"}}, "cba"},
		{"REPEAT", "REPEAT", []query.Expression{&query.StringLiteral{Value: "ab"}, &query.NumberLiteral{Value: 3}}, "ababab"},
		{"FLOOR", "FLOOR", []query.Expression{&query.NumberLiteral{Value: 3.7}}, 3.0},
		{"CEIL", "CEIL", []query.Expression{&query.NumberLiteral{Value: 3.2}}, 4.0},
		{"ROUND", "ROUND", []query.Expression{&query.NumberLiteral{Value: 3.456}, &query.NumberLiteral{Value: 2}}, 3.46},
		{"IIF true", "IIF", []query.Expression{&query.BooleanLiteral{Value: true}, &query.StringLiteral{Value: "y"}, &query.StringLiteral{Value: "n"}}, "y"},
		{"IIF false", "IIF", []query.Expression{&query.BooleanLiteral{Value: false}, &query.StringLiteral{Value: "y"}, &query.StringLiteral{Value: "n"}}, "n"},
		{"MIN two", "MIN", []query.Expression{&query.NumberLiteral{Value: 5}, &query.NumberLiteral{Value: 3}}, 3.0},
		{"MAX two", "MAX", []query.Expression{&query.NumberLiteral{Value: 5}, &query.NumberLiteral{Value: 3}}, 5.0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, err := EvalExpression(&query.FunctionCall{Name: tc.fn, Args: tc.args}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if val != tc.want {
				t.Fatalf("got %v (%T), want %v (%T)", val, val, tc.want, tc.want)
			}
		})
	}
}

func TestUnitEvalExpression_SubstrFunction(t *testing.T) {
	// SUBSTR('hello', 2, 3) -> 'ell'
	val, err := EvalExpression(&query.FunctionCall{
		Name: "SUBSTR",
		Args: []query.Expression{
			&query.StringLiteral{Value: "hello"},
			&query.NumberLiteral{Value: 2},
			&query.NumberLiteral{Value: 3},
		},
	}, nil)
	if err != nil || val != "ell" {
		t.Fatalf("SUBSTR: got %v, err %v", val, err)
	}

	// SUBSTR('hello', 2) -> 'ello'
	val, err = EvalExpression(&query.FunctionCall{
		Name: "SUBSTR",
		Args: []query.Expression{
			&query.StringLiteral{Value: "hello"},
			&query.NumberLiteral{Value: 2},
		},
	}, nil)
	if err != nil || val != "ello" {
		t.Fatalf("SUBSTR no length: got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_AliasExpr(t *testing.T) {
	val, err := EvalExpression(&query.AliasExpr{
		Expr:  &query.NumberLiteral{Value: 42},
		Alias: "x",
	}, nil)
	if err != nil || val != 42.0 {
		t.Fatalf("alias: got %v, err %v", val, err)
	}
}

func TestUnitEvalExpression_UnsupportedFunc(t *testing.T) {
	_, err := EvalExpression(&query.FunctionCall{Name: "NO_SUCH_FUNC", Args: nil}, nil)
	if err == nil {
		t.Fatal("expected error for unsupported function")
	}
}

func TestUnitEvalExpression_UnsupportedExprType(t *testing.T) {
	_, err := EvalExpression(&query.StarExpr{}, nil)
	if err == nil {
		t.Fatal("expected error for unsupported expression type")
	}
}

// ===================== encodeRow Tests =====================

func TestUnitEncodeRow_BasicTypes(t *testing.T) {
	exprs := []query.Expression{
		&query.StringLiteral{Value: "hello"},
		&query.NumberLiteral{Value: 42},
		&query.BooleanLiteral{Value: true},
		&query.NullLiteral{},
	}
	data, err := encodeRow(exprs, nil)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 4 {
		t.Fatalf("expected 4 values, got %d", len(decoded))
	}
	if decoded[0] != "hello" {
		t.Fatalf("first val: got %v", decoded[0])
	}
	if decoded[3] != nil {
		t.Fatalf("null val: got %v", decoded[3])
	}
}

func TestUnitEncodeRow_Placeholder(t *testing.T) {
	exprs := []query.Expression{
		&query.PlaceholderExpr{Index: 0},
		&query.PlaceholderExpr{Index: 1},
	}
	data, err := encodeRow(exprs, []interface{}{"a", 99})
	if err != nil {
		t.Fatal(err)
	}
	var decoded []interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded[0] != "a" {
		t.Fatalf("placeholder 0: got %v", decoded[0])
	}
}

func TestUnitEncodeRow_Identifier(t *testing.T) {
	exprs := []query.Expression{
		&query.Identifier{Name: "myCol"},
	}
	data, err := encodeRow(exprs, nil)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded[0] != "myCol" {
		t.Fatalf("identifier: got %v", decoded[0])
	}
}

func TestUnitEncodeRow_FallbackToEval(t *testing.T) {
	// Binary expression falls through to EvalExpression
	exprs := []query.Expression{
		&query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: 3},
			Operator: query.TokenPlus,
			Right:    &query.NumberLiteral{Value: 4},
		},
	}
	data, err := encodeRow(exprs, nil)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	// 3 + 4 = int64(7), JSON encodes as float64(7)
	if decoded[0] != float64(7) {
		t.Fatalf("eval fallback: got %v", decoded[0])
	}
}

// ===================== containsSubquery Tests =====================

func TestUnitContainsSubquery(t *testing.T) {
	// nil
	if containsSubquery(nil) {
		t.Fatal("nil should not contain subquery")
	}

	// Simple literal
	if containsSubquery(&query.NumberLiteral{Value: 1}) {
		t.Fatal("literal should not contain subquery")
	}

	// SubqueryExpr
	if !containsSubquery(&query.SubqueryExpr{Query: &query.SelectStmt{}}) {
		t.Fatal("SubqueryExpr should be detected")
	}

	// ExistsExpr
	if !containsSubquery(&query.ExistsExpr{Subquery: &query.SelectStmt{}}) {
		t.Fatal("ExistsExpr should be detected")
	}

	// Alias wrapping subquery
	if !containsSubquery(&query.AliasExpr{Expr: &query.SubqueryExpr{Query: &query.SelectStmt{}}}) {
		t.Fatal("AliasExpr wrapping SubqueryExpr should be detected")
	}

	// Binary with subquery on right
	if !containsSubquery(&query.BinaryExpr{
		Left:     &query.NumberLiteral{Value: 1},
		Operator: query.TokenEq,
		Right:    &query.SubqueryExpr{Query: &query.SelectStmt{}},
	}) {
		t.Fatal("BinaryExpr with subquery should be detected")
	}

	// Binary without subquery
	if containsSubquery(&query.BinaryExpr{
		Left:     &query.NumberLiteral{Value: 1},
		Operator: query.TokenEq,
		Right:    &query.NumberLiteral{Value: 2},
	}) {
		t.Fatal("BinaryExpr without subquery should not be detected")
	}

	// Unary with subquery
	if !containsSubquery(&query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.ExistsExpr{Subquery: &query.SelectStmt{}},
	}) {
		t.Fatal("UnaryExpr with subquery should be detected")
	}

	// FunctionCall with subquery arg
	if !containsSubquery(&query.FunctionCall{
		Name: "COALESCE",
		Args: []query.Expression{
			&query.SubqueryExpr{Query: &query.SelectStmt{}},
		},
	}) {
		t.Fatal("FunctionCall with subquery arg should be detected")
	}

	// FunctionCall without subquery
	if containsSubquery(&query.FunctionCall{
		Name: "ABS",
		Args: []query.Expression{&query.NumberLiteral{Value: 5}},
	}) {
		t.Fatal("FunctionCall without subquery should not be detected")
	}
}

// ===================== containsNonDeterministicFunctions Tests =====================

func TestUnitContainsNonDeterministicFunctions(t *testing.T) {
	// No non-deterministic
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.NumberLiteral{Value: 1}},
	}
	if containsNonDeterministicFunctions(stmt) {
		t.Fatal("simple literal should not be non-deterministic")
	}

	// RANDOM() in columns
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "RANDOM"}},
	}
	if !containsNonDeterministicFunctions(stmt) {
		t.Fatal("RANDOM in columns should be detected")
	}

	// NOW() in WHERE
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.NumberLiteral{Value: 1}},
		Where:   &query.FunctionCall{Name: "NOW"},
	}
	if !containsNonDeterministicFunctions(stmt) {
		t.Fatal("NOW in WHERE should be detected")
	}

	// UUID in ORDER BY
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.NumberLiteral{Value: 1}},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.FunctionCall{Name: "UUID"}},
		},
	}
	if !containsNonDeterministicFunctions(stmt) {
		t.Fatal("UUID in ORDER BY should be detected")
	}
}

func TestUnitHasNonDeterministicFunction(t *testing.T) {
	// nil
	if hasNonDeterministicFunction(nil) {
		t.Fatal("nil should not be non-deterministic")
	}

	// Deterministic function
	if hasNonDeterministicFunction(&query.FunctionCall{Name: "ABS", Args: []query.Expression{&query.NumberLiteral{Value: 1}}}) {
		t.Fatal("ABS should not be non-deterministic")
	}

	// Non-deterministic in nested binary
	if !hasNonDeterministicFunction(&query.BinaryExpr{
		Left:     &query.FunctionCall{Name: "RANDOM"},
		Operator: query.TokenPlus,
		Right:    &query.NumberLiteral{Value: 1},
	}) {
		t.Fatal("RANDOM in binary should be detected")
	}

	// Non-deterministic in alias
	if !hasNonDeterministicFunction(&query.AliasExpr{
		Expr: &query.FunctionCall{Name: "RAND"},
	}) {
		t.Fatal("RAND in alias should be detected")
	}

	// Non-deterministic in unary
	if !hasNonDeterministicFunction(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.FunctionCall{Name: "RANDOM"},
	}) {
		t.Fatal("RANDOM in unary should be detected")
	}

	// Non-deterministic nested in function args
	if !hasNonDeterministicFunction(&query.FunctionCall{
		Name: "ABS",
		Args: []query.Expression{&query.FunctionCall{Name: "RANDOM"}},
	}) {
		t.Fatal("RANDOM nested in ABS should be detected")
	}

	// All non-det function names
	for _, name := range []string{"RANDOM", "RAND", "NOW", "CURRENT_TIMESTAMP", "UUID", "NEWID"} {
		if !hasNonDeterministicFunction(&query.FunctionCall{Name: name}) {
			t.Fatalf("%s should be non-deterministic", name)
		}
	}
}

// ===================== tokenTypeToColumnType Tests =====================

func TestUnitTokenTypeToColumnType(t *testing.T) {
	tests := []struct {
		token query.TokenType
		want  string
	}{
		{query.TokenInteger, "INTEGER"},
		{query.TokenText, "TEXT"},
		{query.TokenReal, "REAL"},
		{query.TokenBlob, "BLOB"},
		{query.TokenBoolean, "BOOLEAN"},
		{query.TokenJSON, "JSON"},
		{query.TokenDate, "DATE"},
		{query.TokenTimestamp, "TIMESTAMP"},
		{query.TokenType(9999), "TEXT"}, // unknown defaults to TEXT
	}
	for _, tc := range tests {
		got := tokenTypeToColumnType(tc.token)
		if got != tc.want {
			t.Errorf("tokenTypeToColumnType(%v) = %q, want %q", tc.token, got, tc.want)
		}
	}
}

// ===================== matchLikeSimple Tests =====================

func TestUnitMatchLikeSimple(t *testing.T) {
	tests := []struct {
		s, pattern string
		want       bool
	}{
		{"", "", true},
		{"a", "", false},
		{"", "a", false},
		{"hello", "hello", true},
		{"hello", "HELLO", true}, // case insensitive
		{"hello", "h%", true},
		{"hello", "%o", true},
		{"hello", "%ll%", true},
		{"hello", "h_llo", true},
		{"hello", "h__lo", true},
		{"hello", "_____", true},
		{"hello", "______", false},
		{"hello", "%", true},
		{"", "%", true},
		{"abc", "a%c", true},
		{"ac", "a%c", true},
		{"abc", "a_c", true},
		{"abbc", "a_c", false},
		{"abc", "a%%c", true},
		// Multiple % in a row
		{"hello world", "h%%%d", true},
	}
	for _, tc := range tests {
		got := matchLikeSimple(tc.s, tc.pattern)
		if got != tc.want {
			t.Errorf("matchLikeSimple(%q, %q) = %v, want %v", tc.s, tc.pattern, got, tc.want)
		}
	}
}

func TestUnitMatchLikeSimple_Escape(t *testing.T) {
	// Escape % literal with '\'
	got := matchLikeSimple("50%", "50\\%", '\\')
	if !got {
		t.Fatal("escaped % should match literal %")
	}

	got = matchLikeSimple("50x", "50\\%", '\\')
	if got {
		t.Fatal("escaped % should not match x")
	}

	// Escape _ literal
	got = matchLikeSimple("a_b", "a\\_b", '\\')
	if !got {
		t.Fatal("escaped _ should match literal _")
	}
}

// ===================== typeTaggedKey Tests =====================

func TestUnitTypeTaggedKey(t *testing.T) {
	tests := []struct {
		name string
		val  interface{}
		want string
	}{
		{"nil", nil, "\x01NULL\x01"},
		{"int64", int64(42), "I:42"},
		{"float64 whole", float64(100), "I:100"},
		{"float64 frac", 3.14, "F:3.14"},
		{"bool true", true, "B:1"},
		{"bool false", false, "B:0"},
		{"string", "hello", "S:hello"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := typeTaggedKey(tc.val)
			if got != tc.want {
				t.Fatalf("typeTaggedKey(%v) = %q, want %q", tc.val, got, tc.want)
			}
		})
	}
}

// ===================== valuesEqual Tests (ForeignKeyEnforcer) =====================

func TestUnitValuesEqual(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"nil nil", nil, nil, true},
		{"nil vs val", nil, 1, false},
		{"val vs nil", 1, nil, false},
		{"int int eq", 1, 1, true},
		{"int int neq", 1, 2, false},
		{"int float64 eq", 1, float64(1), true},
		{"int64 float64 eq", int64(5), float64(5), true},
		{"float32 int eq", float32(3), 3, true},
		{"int8 int16 eq", int8(10), int16(10), true},
		{"uint vs int eq", uint(7), 7, true},
		{"uint32 float64", uint32(100), float64(100), true},
		{"uint64 int64", uint64(50), int64(50), true},
		{"string string eq", "abc", "abc", true},
		{"string string neq", "abc", "xyz", false},
		{"string vs int", "abc", 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := fke.valuesEqual(tc.a, tc.b)
			if got != tc.want {
				t.Fatalf("valuesEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// ===================== updateRowSlice Tests =====================

func TestUnitUpdateRowSlice(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	// Create a table and tree
	table := &TableDef{
		Name: "t1",
		Columns: []ColumnDef{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
		},
		PrimaryKey: []string{"id"},
	}
	table.buildColumnIndexCache()
	cat.tables["t1"] = table
	tree := newUnitTestBTree(t)
	cat.tableTrees["t1"] = tree

	// Insert a row
	key := fke.serializeValue(1)
	rowData, _ := json.Marshal([]interface{}{1, "original"})
	if err := tree.Put(key, rowData); err != nil {
		t.Fatal(err)
	}

	// Update the row
	err := fke.updateRowSlice("t1", 1, []interface{}{1, "updated"})
	if err != nil {
		t.Fatal(err)
	}

	// Verify
	val, err := tree.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []interface{}
	if err := json.Unmarshal(val, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded[1] != "updated" {
		t.Fatalf("expected updated, got %v", decoded[1])
	}
}

func TestUnitUpdateRowSlice_TableNotFound(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	err := fke.updateRowSlice("nonexistent", 1, []interface{}{1})
	if err == nil {
		t.Fatal("expected error for non-existent table")
	}
}

func TestUnitUpdateRowSlice_WithIndex(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	table := &TableDef{
		Name: "t2",
		Columns: []ColumnDef{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "val", Type: "TEXT"},
		},
		PrimaryKey: []string{"id"},
	}
	table.buildColumnIndexCache()
	cat.tables["t2"] = table
	tree := newUnitTestBTree(t)
	cat.tableTrees["t2"] = tree

	// Add index
	idxTree := newUnitTestBTree(t)
	cat.indexes["idx_val"] = &IndexDef{Name: "idx_val", TableName: "t2", Columns: []string{"val"}}
	cat.indexTrees["idx_val"] = idxTree

	// Insert row with index entry
	key := fke.serializeValue(1)
	rowData, _ := json.Marshal([]interface{}{1, "old"})
	if err := tree.Put(key, rowData); err != nil {
		t.Fatal(err)
	}
	oldIdxKey := typeTaggedKey("old")
	if err := idxTree.Put([]byte(oldIdxKey), key); err != nil {
		t.Fatal(err)
	}

	// Update should maintain index
	err := fke.updateRowSlice("t2", 1, []interface{}{1, "new"})
	if err != nil {
		t.Fatal(err)
	}

	// Old index entry should be gone
	_, getErr := idxTree.Get([]byte(oldIdxKey))
	if getErr == nil {
		t.Fatal("old index entry should be deleted")
	}

	// New index entry should exist
	newIdxKey := typeTaggedKey("new")
	_, getErr = idxTree.Get([]byte(newIdxKey))
	if getErr != nil {
		t.Fatal("new index entry should exist")
	}
}

// ===================== CheckForeignKeyConstraints Tests =====================

func TestUnitCheckForeignKeyConstraints_NoData(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	// Table without tree -> no data, should be ok
	cat.tables["empty"] = &TableDef{Name: "empty", Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}}}

	err := fke.CheckForeignKeyConstraints(context.Background(), "empty")
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnitCheckForeignKeyConstraints_NoFK(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	table := &TableDef{
		Name:    "t1",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER", PrimaryKey: true}},
	}
	table.buildColumnIndexCache()
	cat.tables["t1"] = table
	tree := newUnitTestBTree(t)
	cat.tableTrees["t1"] = tree

	// Insert a row
	rowData, _ := json.Marshal([]interface{}{1})
	if err := tree.Put([]byte("00000000000000000001"), rowData); err != nil {
		t.Fatal(err)
	}

	err := fke.CheckForeignKeyConstraints(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnitCheckForeignKeyConstraints_TableNotFound(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	// Table has tree but no table def
	cat.tableTrees["ghost"] = newUnitTestBTree(t)

	err := fke.CheckForeignKeyConstraints(context.Background(), "ghost")
	if err == nil {
		t.Fatal("expected error when table def not found")
	}
}

// ===================== deserializeValue Tests =====================

func TestUnitDeserializeValue(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	// String prefix
	val := fke.deserializeValue([]byte("S:hello"))
	if val != "hello" {
		t.Fatalf("string: got %v", val)
	}

	// Integer (zero padded)
	val = fke.deserializeValue([]byte("00000000000000000042"))
	if val != int(42) {
		t.Fatalf("int: got %v (%T)", val, val)
	}

	// Fallback string
	val = fke.deserializeValue([]byte("xy"))
	// "xy" cannot be parsed as int or float, returns as string
	if val != "xy" {
		t.Fatalf("fallback: got %v", val)
	}
}

// ===================== JSONPath.Set Tests =====================

func TestUnitJSONPathSet_ObjectKey(t *testing.T) {
	result, err := JSONSet(`{"a":1}`, "$.a", "2")
	if err != nil {
		t.Fatal(err)
	}
	if result != `{"a":2}` {
		t.Fatalf("JSONSet obj key: got %s", result)
	}
}

func TestUnitJSONPathSet_NewKey(t *testing.T) {
	result, err := JSONSet(`{"a":1}`, "$.b", `"hello"`)
	if err != nil {
		t.Fatal(err)
	}
	// Should have both keys
	var data map[string]interface{}
	json.Unmarshal([]byte(result), &data)
	if data["b"] != "hello" {
		t.Fatalf("JSONSet new key: got %v", data["b"])
	}
}

func TestUnitJSONPathSet_ArrayElement(t *testing.T) {
	result, err := JSONSet(`[1,2,3]`, "$[1]", "99")
	if err != nil {
		t.Fatal(err)
	}
	var data []interface{}
	json.Unmarshal([]byte(result), &data)
	if data[1] != float64(99) {
		t.Fatalf("JSONSet array: got %v", data[1])
	}
}

func TestUnitJSONPathSet_NestedPath(t *testing.T) {
	result, err := JSONSet(`{"a":{"b":1}}`, "$.a.b", "42")
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]interface{}
	json.Unmarshal([]byte(result), &data)
	inner := data["a"].(map[string]interface{})
	if inner["b"] != float64(42) {
		t.Fatalf("JSONSet nested: got %v", inner["b"])
	}
}

func TestUnitJSONPathSet_EmptyPath(t *testing.T) {
	jp := &JSONPath{Segments: []string{}}
	err := jp.Set(&map[string]interface{}{"a": 1}, "new")
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestUnitJSONPathSet_EmptyJSON(t *testing.T) {
	result, err := JSONSet("", "$.a", `"val"`)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]interface{}
	json.Unmarshal([]byte(result), &data)
	if data["a"] != "val" {
		t.Fatalf("JSONSet empty json: got %v", data["a"])
	}
}

func TestUnitJSONPathSet_InvalidJSON(t *testing.T) {
	_, err := JSONSet("not json", "$.a", "1")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestUnitJSONPathSet_InvalidPath(t *testing.T) {
	_, err := JSONSet(`{"a":1}`, "$.", "1")
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestUnitJSONPathSet_NotObject(t *testing.T) {
	jp, _ := ParseJSONPath("$.a")
	var data interface{} = "string"
	err := jp.Set(&data, "val")
	if err == nil {
		t.Fatal("expected error setting key on non-object")
	}
}

func TestUnitJSONPathSet_ArrayOutOfBounds(t *testing.T) {
	jp, _ := ParseJSONPath("$[99]")
	var data interface{} = []interface{}{1, 2, 3}
	err := jp.Set(&data, "val")
	if err == nil {
		t.Fatal("expected error for array index out of bounds")
	}
}

// ===================== JSONPath.Get Tests =====================

func TestUnitJSONPathGet_Nil(t *testing.T) {
	jp := &JSONPath{Segments: []string{"a"}}
	val, err := jp.Get(nil)
	if err != nil || val != nil {
		t.Fatalf("Get nil: got %v, err %v", val, err)
	}
}

func TestUnitJSONPathGet_WildcardFinal(t *testing.T) {
	jp := &JSONPath{Segments: []string{"*"}}
	data := []interface{}{1, 2, 3}
	val, err := jp.Get(data)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := val.([]interface{})
	if !ok || len(arr) != 3 {
		t.Fatalf("wildcard final: got %v", val)
	}
}

func TestUnitJSONPathGet_WildcardEmpty(t *testing.T) {
	jp := &JSONPath{Segments: []string{"*", "a"}}
	data := []interface{}{}
	val, err := jp.Get(data)
	if err != nil || val != nil {
		t.Fatalf("wildcard empty: got %v, err %v", val, err)
	}
}

func TestUnitJSONPathGet_WildcardNotArray(t *testing.T) {
	jp := &JSONPath{Segments: []string{"*", "a"}}
	data := map[string]interface{}{"x": 1}
	val, err := jp.Get(data)
	if err != nil || val != nil {
		t.Fatalf("wildcard non-array: got %v, err %v", val, err)
	}
}

func TestUnitJSONPathGet_ArrayIndexOutOfBounds(t *testing.T) {
	jp := &JSONPath{Segments: []string{"[99]"}}
	data := []interface{}{1, 2}
	val, err := jp.Get(data)
	if err != nil || val != nil {
		t.Fatalf("array OOB: got %v, err %v", val, err)
	}
}

func TestUnitJSONPathGet_NegativeIndex(t *testing.T) {
	jp, _ := ParseJSONPath("$[-1]")
	// ParseJSONPath doesn't parse negative, so we test manually
	jp2 := &JSONPath{Segments: []string{"[-1]"}}
	data := []interface{}{1, 2}
	val, err := jp2.Get(data)
	if err != nil || val != nil {
		t.Fatalf("negative index: got %v, err %v", val, err)
	}
	_ = jp
}

func TestUnitJSONPathGet_NonObject(t *testing.T) {
	jp := &JSONPath{Segments: []string{"key"}}
	data := "string_value"
	_, err := jp.Get(data)
	if err == nil {
		t.Fatal("expected error accessing property on non-object")
	}
}

func TestUnitJSONPathGet_MissingKey(t *testing.T) {
	jp := &JSONPath{Segments: []string{"missing"}}
	data := map[string]interface{}{"a": 1}
	_, err := jp.Get(data)
	if err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestUnitJSONPathGet_ArrayNotArray(t *testing.T) {
	jp := &JSONPath{Segments: []string{"[0]"}}
	data := "not_array"
	val, err := jp.Get(data)
	if err != nil || val != nil {
		t.Fatalf("array on non-array: got %v, err %v", val, err)
	}
}

// ===================== JSON utility functions =====================

func TestUnitJSONRemove_ObjectKey(t *testing.T) {
	result, err := JSONRemove(`{"a":1,"b":2}`, "$.a")
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]interface{}
	json.Unmarshal([]byte(result), &data)
	if _, exists := data["a"]; exists {
		t.Fatal("key 'a' should be removed")
	}
	if data["b"] != float64(2) {
		t.Fatalf("key 'b' should remain, got %v", data["b"])
	}
}

func TestUnitJSONRemove_Empty(t *testing.T) {
	result, err := JSONRemove("", "$.a")
	if err != nil || result != "" {
		t.Fatalf("empty: got %q, err %v", result, err)
	}
}

func TestUnitJSONPretty(t *testing.T) {
	result, err := JSONPretty(`{"a":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if result == `{"a":1}` {
		t.Fatal("pretty should add formatting")
	}

	// Empty
	result, err = JSONPretty("")
	if err != nil || result != "" {
		t.Fatalf("empty pretty: got %q, err %v", result, err)
	}
}

func TestUnitJSONMinify(t *testing.T) {
	result, err := JSONMinify(`{  "a" : 1  }`)
	if err != nil {
		t.Fatal(err)
	}
	if result != `{"a":1}` {
		t.Fatalf("minify: got %q", result)
	}

	// Empty
	result, err = JSONMinify("")
	if err != nil || result != "" {
		t.Fatalf("empty minify: got %q, err %v", result, err)
	}
}

func TestUnitJSONQuote(t *testing.T) {
	result := JSONQuote(`hello "world"`)
	if result != `"hello \"world\""` {
		t.Fatalf("quote: got %q", result)
	}

	result = JSONQuote("")
	if result != `""` {
		t.Fatalf("empty quote: got %q", result)
	}
}

// ===================== validateIdentifier Tests =====================

func TestUnitValidateIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{"valid simple", "users", false},
		{"valid underscore", "my_table", false},
		{"valid digits", "table1", false},
		{"empty", "", true},
		{"too long", "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffggggg", true},
		{"special char", "table-name", true},
		{"space", "my table", true},
		{"SQL keyword SELECT", "SELECT", true},
		{"SQL keyword DROP", "DROP", true},
		{"SQL keyword DELETE", "DELETE", true},
		{"contains INSERT", "INSERT", true},
		{"contains UPDATE", "UPDATE", true},
		{"contains UNION", "UNION", true},
		{"contains comment", "a--b", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateIdentifier(tc.id)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateIdentifier(%q) err=%v, wantErr=%v", tc.id, err, tc.wantErr)
			}
		})
	}
}

// ===================== countRows Tests =====================

func TestUnitCountRows_InvalidIdentifier(t *testing.T) {
	cat := newUnitTestCatalog(t)
	sc := NewStatsCollector(cat)

	// Table name with SQL injection
	_, err := sc.countRows("DROP TABLE")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}
}

func TestUnitCountRows_EmptyResult(t *testing.T) {
	cat := newUnitTestCatalog(t)
	sc := NewStatsCollector(cat)

	// ExecuteQuery returns empty result (stub)
	count, err := sc.countRows("mytable")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected 0, got %d", count)
	}
}

// ===================== collectColumnStats Tests =====================

func TestUnitCollectColumnStats_InvalidIdentifiers(t *testing.T) {
	cat := newUnitTestCatalog(t)
	sc := NewStatsCollector(cat)

	// Invalid table name
	_, err := sc.collectColumnStats("DROP TABLE", "col1")
	if err == nil {
		t.Fatal("expected error for invalid table name")
	}

	// Invalid column name
	_, err = sc.collectColumnStats("mytable", "DROP TABLE")
	if err == nil {
		t.Fatal("expected error for invalid column name")
	}
}

// ===================== valueToLiteral Tests =====================

func TestUnitValueToLiteral(t *testing.T) {
	// nil -> NullLiteral
	expr := valueToLiteral(nil)
	if _, ok := expr.(*query.NullLiteral); !ok {
		t.Fatal("nil should produce NullLiteral")
	}

	// string -> StringLiteral
	expr = valueToLiteral("hello")
	sl, ok := expr.(*query.StringLiteral)
	if !ok || sl.Value != "hello" {
		t.Fatalf("string: got %v", expr)
	}

	// bool -> BooleanLiteral
	expr = valueToLiteral(true)
	bl, ok := expr.(*query.BooleanLiteral)
	if !ok || bl.Value != true {
		t.Fatalf("bool: got %v", expr)
	}

	// number -> NumberLiteral
	expr = valueToLiteral(int64(42))
	nl, ok := expr.(*query.NumberLiteral)
	if !ok || nl.Value != 42.0 {
		t.Fatalf("number: got %v", expr)
	}
}

// ===================== valueToExpr Tests =====================

func TestUnitValueToExpr(t *testing.T) {
	// nil
	expr := valueToExpr(nil)
	if _, ok := expr.(*query.NullLiteral); !ok {
		t.Fatal("nil should be NullLiteral")
	}

	// string
	expr = valueToExpr("test")
	if sl, ok := expr.(*query.StringLiteral); !ok || sl.Value != "test" {
		t.Fatal("string mismatch")
	}

	// float64
	expr = valueToExpr(3.14)
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 3.14 {
		t.Fatal("float64 mismatch")
	}

	// int
	expr = valueToExpr(42)
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 42.0 {
		t.Fatal("int mismatch")
	}

	// int64
	expr = valueToExpr(int64(100))
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 100.0 {
		t.Fatal("int64 mismatch")
	}

	// bool true -> NumberLiteral(1)
	expr = valueToExpr(true)
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 1 {
		t.Fatal("bool true mismatch")
	}

	// bool false -> NumberLiteral(0)
	expr = valueToExpr(false)
	if nl, ok := expr.(*query.NumberLiteral); !ok || nl.Value != 0 {
		t.Fatal("bool false mismatch")
	}

	// Other type -> StringLiteral
	expr = valueToExpr([]byte{1, 2, 3})
	if _, ok := expr.(*query.StringLiteral); !ok {
		t.Fatal("byte slice should become StringLiteral")
	}
}

// ===================== ParseJSONPath edge cases =====================

func TestUnitParseJSONPath_Errors(t *testing.T) {
	// Empty
	_, err := ParseJSONPath("")
	if err == nil {
		t.Fatal("expected error for empty path")
	}

	// Incomplete bracket
	_, err = ParseJSONPath("$[")
	if err == nil {
		t.Fatal("expected error for incomplete bracket")
	}

	// Unclosed string
	_, err = ParseJSONPath(`$["key`)
	if err == nil {
		t.Fatal("expected error for unclosed string")
	}

	// Missing closing bracket
	_, err = ParseJSONPath(`$["key"`)
	if err == nil {
		t.Fatal("expected error for missing ]")
	}

	// Invalid array index
	_, err = ParseJSONPath("$[abc]")
	if err == nil {
		t.Fatal("expected error for invalid array index")
	}

	// Unclosed array bracket
	_, err = ParseJSONPath("$[0")
	if err == nil {
		t.Fatal("expected error for unclosed bracket")
	}
}

func TestUnitParseJSONPath_Wildcard(t *testing.T) {
	jp, err := ParseJSONPath("$[*]")
	if err != nil {
		t.Fatal(err)
	}
	if len(jp.Segments) != 1 || jp.Segments[0] != "*" {
		t.Fatalf("wildcard: got %v", jp.Segments)
	}
}

func TestUnitParseJSONPath_BracketString(t *testing.T) {
	jp, err := ParseJSONPath(`$["key"]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(jp.Segments) != 1 || jp.Segments[0] != "key" {
		t.Fatalf("bracket string: got %v", jp.Segments)
	}

	// Single quotes
	jp, err = ParseJSONPath("$['key']")
	if err != nil {
		t.Fatal(err)
	}
	if len(jp.Segments) != 1 || jp.Segments[0] != "key" {
		t.Fatalf("single quote bracket: got %v", jp.Segments)
	}
}

// ===================== serializeValue / serializeCompositeKey Tests =====================

func TestUnitSerializeValue(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	tests := []struct {
		name string
		val  interface{}
		want string
	}{
		{"string", "hello", "S:hello"},
		{"int", 42, "00000000000000000042"},
		{"int64", int64(99), "00000000000000000099"},
		{"float64", float64(7), "00000000000000000007"},
		{"nil", nil, "NULL"},
		{"bytes", []byte("raw"), "raw"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := string(fke.serializeValue(tc.val))
			if got != tc.want {
				t.Fatalf("serializeValue(%v) = %q, want %q", tc.val, got, tc.want)
			}
		})
	}
}

func TestUnitSerializeCompositeKey(t *testing.T) {
	cat := newUnitTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	key := fke.serializeCompositeKey([]interface{}{1, "hello"})
	// Should be "00000000000000000001" + \x00 + "S:hello"
	expected := "00000000000000000001\x00S:hello"
	if string(key) != expected {
		t.Fatalf("composite key: got %q, want %q", string(key), expected)
	}
}

// ===================== decodeRow Tests =====================

func TestUnitDecodeRow_IntRestoration(t *testing.T) {
	data, _ := json.Marshal([]interface{}{42.0, "hello", nil, 3.14})
	row, err := decodeRow(data, 4)
	if err != nil {
		t.Fatal(err)
	}
	// 42.0 should be restored to int64(42)
	if row[0] != int64(42) {
		t.Fatalf("int restoration: got %v (%T)", row[0], row[0])
	}
	// 3.14 stays float64
	if row[3] != 3.14 {
		t.Fatalf("float preservation: got %v", row[3])
	}
}

func TestUnitDecodeRow_Padding(t *testing.T) {
	data, _ := json.Marshal([]interface{}{1, 2})
	row, err := decodeRow(data, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 4 {
		t.Fatalf("padding: got len %d, want 4", len(row))
	}
	if row[2] != nil || row[3] != nil {
		t.Fatal("padded values should be nil")
	}
}

// ===================== buildCompositeIndexKey Tests =====================

func TestUnitBuildCompositeIndexKey(t *testing.T) {
	table := &TableDef{
		Name: "t1",
		Columns: []ColumnDef{
			{Name: "a", Type: "INTEGER"},
			{Name: "b", Type: "TEXT"},
			{Name: "c", Type: "INTEGER"},
		},
	}
	table.buildColumnIndexCache()

	// Single column index
	idx := &IndexDef{Columns: []string{"a"}}
	key, ok := buildCompositeIndexKey(table, idx, []interface{}{int64(5), "hello", int64(3)})
	if !ok || key != "I:5" {
		t.Fatalf("single col: got %q, ok=%v", key, ok)
	}

	// Composite index
	idx = &IndexDef{Columns: []string{"a", "b"}}
	key, ok = buildCompositeIndexKey(table, idx, []interface{}{int64(5), "hello", int64(3)})
	if !ok {
		t.Fatal("composite: expected ok")
	}
	expected := "I:5\x00S:hello"
	if key != expected {
		t.Fatalf("composite: got %q, want %q", key, expected)
	}

	// Nil value -> not ok
	_, ok = buildCompositeIndexKey(table, idx, []interface{}{nil, "hello", int64(3)})
	if ok {
		t.Fatal("nil col value should return not ok")
	}

	// Empty columns -> not ok
	idx = &IndexDef{Columns: []string{}}
	_, ok = buildCompositeIndexKey(table, idx, []interface{}{int64(5)})
	if ok {
		t.Fatal("empty columns should return not ok")
	}
}
