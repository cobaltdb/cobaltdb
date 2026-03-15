package engine

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestSubstituteParamsInSetClauses100 tests substituteParamsInSetClauses
func TestSubstituteParamsInSetClauses100(t *testing.T) {
	set := []*query.SetClause{
		{Column: "name", Value: &query.Identifier{Name: "param1"}},
		{Column: "age", Value: &query.Identifier{Name: "param2"}},
	}

	paramMap := map[string]interface{}{
		"param1": "Alice",
		"param2": 30,
	}

	result := substituteParamsInSetClauses(set, paramMap)

	if len(result) != 2 {
		t.Fatalf("Expected 2 clauses, got %d", len(result))
	}

	// Check first clause was substituted
	if lit, ok := result[0].Value.(*query.StringLiteral); ok {
		if lit.Value != "Alice" {
			t.Errorf("Expected 'Alice', got '%s'", lit.Value)
		}
	} else {
		t.Errorf("Expected StringLiteral, got %T", result[0].Value)
	}

	// Check second clause was substituted
	if lit, ok := result[1].Value.(*query.NumberLiteral); ok {
		if lit.Value != 30 {
			t.Errorf("Expected 30, got %v", lit.Value)
		}
	} else {
		t.Errorf("Expected NumberLiteral, got %T", result[1].Value)
	}
}

// TestSubstituteParamsInExpr100 tests substituteParamsInExpr with various types
func TestSubstituteParamsInExpr100(t *testing.T) {
	tests := []struct {
		name     string
		expr     query.Expression
		paramMap map[string]interface{}
		wantType string
		wantVal  interface{}
	}{
		{
			name:     "string param",
			expr:     &query.Identifier{Name: "p1"},
			paramMap: map[string]interface{}{"p1": "hello"},
			wantType: "*query.StringLiteral",
			wantVal:  "hello",
		},
		{
			name:     "int param",
			expr:     &query.Identifier{Name: "p1"},
			paramMap: map[string]interface{}{"p1": 42},
			wantType: "*query.NumberLiteral",
			wantVal:  42.0,
		},
		{
			name:     "int64 param",
			expr:     &query.Identifier{Name: "p1"},
			paramMap: map[string]interface{}{"p1": int64(100)},
			wantType: "*query.NumberLiteral",
			wantVal:  100.0,
		},
		{
			name:     "float64 param",
			expr:     &query.Identifier{Name: "p1"},
			paramMap: map[string]interface{}{"p1": 3.14},
			wantType: "*query.NumberLiteral",
			wantVal:  3.14,
		},
		{
			name:     "bool param",
			expr:     &query.Identifier{Name: "p1"},
			paramMap: map[string]interface{}{"p1": true},
			wantType: "*query.BooleanLiteral",
			wantVal:  true,
		},
		{
			name:     "nil param",
			expr:     &query.Identifier{Name: "p1"},
			paramMap: map[string]interface{}{"p1": nil},
			wantType: "*query.NullLiteral",
			wantVal:  nil,
		},
		{
			name:     "unknown type param",
			expr:     &query.Identifier{Name: "p1"},
			paramMap: map[string]interface{}{"p1": []int{1, 2, 3}},
			wantType: "*query.StringLiteral",
			wantVal:  "[1 2 3]",
		},
		{
			name:     "param not found",
			expr:     &query.Identifier{Name: "unknown"},
			paramMap: map[string]interface{}{"p1": "value"},
			wantType: "*query.Identifier",
			wantVal:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := substituteParamsInExpr(tt.expr, tt.paramMap)
			if result == nil {
				t.Fatal("Result is nil")
			}

			gotType := ""
			switch v := result.(type) {
			case *query.StringLiteral:
				gotType = "*query.StringLiteral"
				if tt.wantVal != nil && v.Value != tt.wantVal {
					t.Errorf("Expected %v, got %v", tt.wantVal, v.Value)
				}
			case *query.NumberLiteral:
				gotType = "*query.NumberLiteral"
				if tt.wantVal != nil && v.Value != tt.wantVal {
					t.Errorf("Expected %v, got %v", tt.wantVal, v.Value)
				}
			case *query.BooleanLiteral:
				gotType = "*query.BooleanLiteral"
				if tt.wantVal != nil && v.Value != tt.wantVal {
					t.Errorf("Expected %v, got %v", tt.wantVal, v.Value)
				}
			case *query.NullLiteral:
				gotType = "*query.NullLiteral"
			case *query.Identifier:
				gotType = "*query.Identifier"
			default:
				t.Errorf("Unexpected type: %T", result)
			}

			if gotType != tt.wantType {
				t.Errorf("Expected %s, got %s", tt.wantType, gotType)
			}
		})
	}
}

// TestSubstituteParamsInExprBinary100 tests substituteParamsInExpr with BinaryExpr
func TestSubstituteParamsInExprBinary100(t *testing.T) {
	expr := &query.BinaryExpr{
		Left:     &query.Identifier{Name: "p1"},
		Operator: query.TokenPlus,
		Right:    &query.Identifier{Name: "p2"},
	}

	paramMap := map[string]interface{}{
		"p1": 10,
		"p2": 20,
	}

	result := substituteParamsInExpr(expr, paramMap)

	binExpr, ok := result.(*query.BinaryExpr)
	if !ok {
		t.Fatalf("Expected BinaryExpr, got %T", result)
	}

	if _, ok := binExpr.Left.(*query.NumberLiteral); !ok {
		t.Errorf("Expected NumberLiteral for left, got %T", binExpr.Left)
	}
	if _, ok := binExpr.Right.(*query.NumberLiteral); !ok {
		t.Errorf("Expected NumberLiteral for right, got %T", binExpr.Right)
	}
}

// TestSubstituteParamsInExprUnary100 tests substituteParamsInExpr with UnaryExpr
func TestSubstituteParamsInExprUnary100(t *testing.T) {
	expr := &query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.Identifier{Name: "p1"},
	}

	paramMap := map[string]interface{}{
		"p1": 42,
	}

	result := substituteParamsInExpr(expr, paramMap)

	unaryExpr, ok := result.(*query.UnaryExpr)
	if !ok {
		t.Fatalf("Expected UnaryExpr, got %T", result)
	}

	if _, ok := unaryExpr.Expr.(*query.NumberLiteral); !ok {
		t.Errorf("Expected NumberLiteral, got %T", unaryExpr.Expr)
	}
}

// TestSubstituteParamsInExprFunction100 tests substituteParamsInExpr with FunctionCall
func TestSubstituteParamsInExprFunction100(t *testing.T) {
	expr := &query.FunctionCall{
		Name: "CONCAT",
		Args: []query.Expression{
			&query.Identifier{Name: "p1"},
			&query.StringLiteral{Value: " world"},
		},
	}

	paramMap := map[string]interface{}{
		"p1": "hello",
	}

	result := substituteParamsInExpr(expr, paramMap)

	fnExpr, ok := result.(*query.FunctionCall)
	if !ok {
		t.Fatalf("Expected FunctionCall, got %T", result)
	}

	if len(fnExpr.Args) != 2 {
		t.Fatalf("Expected 2 args, got %d", len(fnExpr.Args))
	}

	if _, ok := fnExpr.Args[0].(*query.StringLiteral); !ok {
		t.Errorf("Expected StringLiteral for first arg, got %T", fnExpr.Args[0])
	}
}

// TestSubstituteParamsInExprNil100 tests substituteParamsInExpr with nil
func TestSubstituteParamsInExprNil100(t *testing.T) {
	result := substituteParamsInExpr(nil, map[string]interface{}{})
	if result != nil {
		t.Errorf("Expected nil, got %v", result)
	}
}

// TestDBGetOptimizer100 tests GetOptimizer
func TestDBGetOptimizer100(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	opt := db.GetOptimizer()
	if opt == nil {
		t.Log("Optimizer is nil (may not be initialized)")
	}
}

// TestDBUpdateTableStatistics100 tests UpdateTableStatistics
func TestDBUpdateTableStatistics100(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Should not panic when optimizer is nil
	db.UpdateTableStatistics("test", nil)

	// If optimizer exists, should update statistics
	if db.optimizer != nil {
		db.UpdateTableStatistics("test", nil)
	}
}

// TestDBGetReplicationManager100 tests GetReplicationManager
func TestDBGetReplicationManager100(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	mgr := db.GetReplicationManager()
	if mgr == nil {
		t.Log("Replication manager is nil (may not be initialized)")
	}
}
