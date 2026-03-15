package engine

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestSubstituteParamsInStatement102 tests substituteParamsInStatement
func TestSubstituteParamsInStatement102(t *testing.T) {
	paramMap := map[string]interface{}{
		"name": "Alice",
		"age":  30,
	}

	// Test InsertStmt
	t.Run("InsertStmt", func(t *testing.T) {
		stmt := &query.InsertStmt{
			Table: "users",
			Values: [][]query.Expression{
				{&query.Identifier{Name: "name"}, &query.Identifier{Name: "age"}},
			},
		}
		result := substituteParamsInStatement(stmt, paramMap)

		insertStmt, ok := result.(*query.InsertStmt)
		if !ok {
			t.Fatalf("Expected *query.InsertStmt, got %T", result)
		}

		// Check values were substituted
		if len(insertStmt.Values) != 1 {
			t.Fatalf("Expected 1 value row, got %d", len(insertStmt.Values))
		}
		if _, ok := insertStmt.Values[0][0].(*query.StringLiteral); !ok {
			t.Errorf("Expected StringLiteral for name, got %T", insertStmt.Values[0][0])
		}
	})

	// Test UpdateStmt
	t.Run("UpdateStmt", func(t *testing.T) {
		stmt := &query.UpdateStmt{
			Table: "users",
			Set: []*query.SetClause{
				{Column: "name", Value: &query.Identifier{Name: "name"}},
			},
			Where: &query.Identifier{Name: "age"},
		}
		result := substituteParamsInStatement(stmt, paramMap)

		updateStmt, ok := result.(*query.UpdateStmt)
		if !ok {
			t.Fatalf("Expected *query.UpdateStmt, got %T", result)
		}

		// Check SET clause was substituted
		if _, ok := updateStmt.Set[0].Value.(*query.StringLiteral); !ok {
			t.Errorf("Expected StringLiteral, got %T", updateStmt.Set[0].Value)
		}

		// Check WHERE clause was substituted
		if _, ok := updateStmt.Where.(*query.NumberLiteral); !ok {
			t.Errorf("Expected NumberLiteral for WHERE, got %T", updateStmt.Where)
		}
	})

	// Test DeleteStmt
	t.Run("DeleteStmt", func(t *testing.T) {
		stmt := &query.DeleteStmt{
			Table: "users",
			Where: &query.Identifier{Name: "age"},
		}
		result := substituteParamsInStatement(stmt, paramMap)

		deleteStmt, ok := result.(*query.DeleteStmt)
		if !ok {
			t.Fatalf("Expected *query.DeleteStmt, got %T", result)
		}

		// Check WHERE clause was substituted
		if _, ok := deleteStmt.Where.(*query.NumberLiteral); !ok {
			t.Errorf("Expected NumberLiteral for WHERE, got %T", deleteStmt.Where)
		}
	})

	// Test unsupported statement type
	t.Run("UnsupportedStmt", func(t *testing.T) {
		stmt := &query.SelectStmt{}
		result := substituteParamsInStatement(stmt, paramMap)

		if result != stmt {
			t.Error("Expected same statement for unsupported type")
		}
	})
}

// TestSubstituteParamsInValues102 tests substituteParamsInValues
func TestSubstituteParamsInValues102(t *testing.T) {
	values := [][]query.Expression{
		{&query.Identifier{Name: "p1"}, &query.Identifier{Name: "p2"}},
		{&query.Identifier{Name: "p3"}},
	}

	paramMap := map[string]interface{}{
		"p1": "value1",
		"p2": 42,
		"p3": true,
	}

	result := substituteParamsInValues(values, paramMap)

	if len(result) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(result))
	}

	// Check first row
	if len(result[0]) != 2 {
		t.Errorf("Expected 2 values in first row, got %d", len(result[0]))
	}
	if _, ok := result[0][0].(*query.StringLiteral); !ok {
		t.Errorf("Expected StringLiteral for p1, got %T", result[0][0])
	}
	if _, ok := result[0][1].(*query.NumberLiteral); !ok {
		t.Errorf("Expected NumberLiteral for p2, got %T", result[0][1])
	}

	// Check second row
	if len(result[1]) != 1 {
		t.Errorf("Expected 1 value in second row, got %d", len(result[1]))
	}
	if _, ok := result[1][0].(*query.BooleanLiteral); !ok {
		t.Errorf("Expected BooleanLiteral for p3, got %T", result[1][0])
	}
}

// TestSubstituteParamsInStatementWithNilWhere102 tests UpdateStmt and DeleteStmt with nil Where
func TestSubstituteParamsInStatementWithNilWhere102(t *testing.T) {
	paramMap := map[string]interface{}{}

	// UpdateStmt with nil Where
	t.Run("UpdateStmtNilWhere", func(t *testing.T) {
		stmt := &query.UpdateStmt{
			Table: "users",
			Set:   []*query.SetClause{{Column: "name", Value: &query.StringLiteral{Value: "test"}}},
			Where: nil,
		}
		result := substituteParamsInStatement(stmt, paramMap)

		updateStmt, ok := result.(*query.UpdateStmt)
		if !ok {
			t.Fatalf("Expected *query.UpdateStmt, got %T", result)
		}
		if updateStmt.Where != nil {
			t.Error("Expected nil Where clause to remain nil")
		}
	})

	// DeleteStmt with nil Where
	t.Run("DeleteStmtNilWhere", func(t *testing.T) {
		stmt := &query.DeleteStmt{
			Table: "users",
			Where: nil,
		}
		result := substituteParamsInStatement(stmt, paramMap)

		deleteStmt, ok := result.(*query.DeleteStmt)
		if !ok {
			t.Fatalf("Expected *query.DeleteStmt, got %T", result)
		}
		if deleteStmt.Where != nil {
			t.Error("Expected nil Where clause to remain nil")
		}
	})
}
