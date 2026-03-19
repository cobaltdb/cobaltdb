package wasm

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestWhereClause tests WHERE clause compilation and execution
func TestWhereClause(t *testing.T) {
	t.Run("compile_where_expression", func(t *testing.T) {
		compiler := NewCompiler()

		// Create WHERE clause: id = 1
		whereExpr := &query.BinaryExpr{
			Left:     &query.QualifiedIdentifier{Table: "test", Column: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		}

		// Compile the expression
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From:  &query.TableRef{Name: "test"},
			Where: whereExpr,
		}

		compiled, err := compiler.CompileQuery("SELECT id FROM test WHERE id = 1", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Verify bytecode was generated
		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled SELECT with WHERE, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("where_arithmetic_expression", func(t *testing.T) {
		compiler := NewCompiler()

		// Create WHERE clause: id + 1 = 2
		whereExpr := &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "test", Column: "id"},
				Operator: query.TokenPlus,
				Right:    &query.NumberLiteral{Value: 1},
			},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 2},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From:  &query.TableRef{Name: "test"},
			Where: whereExpr,
		}

		compiled, err := compiler.CompileQuery("SELECT id FROM test WHERE id + 1 = 2", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		t.Logf("Compiled SELECT with arithmetic WHERE, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("where_comparison_operators", func(t *testing.T) {
		compiler := NewCompiler()

		testCases := []struct {
			name     string
			operator query.TokenType
			sql      string
		}{
			{"less_than", query.TokenLt, "SELECT id FROM test WHERE id < 10"},
			{"greater_than", query.TokenGt, "SELECT id FROM test WHERE id > 5"},
			{"equals", query.TokenEq, "SELECT id FROM test WHERE id = 1"},
			{"not_equals", query.TokenNeq, "SELECT id FROM test WHERE id != 0"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				whereExpr := &query.BinaryExpr{
					Left:     &query.QualifiedIdentifier{Table: "test", Column: "id"},
					Operator: tc.operator,
					Right:    &query.NumberLiteral{Value: 5},
				}

				stmt := &query.SelectStmt{
					Columns: []query.Expression{
						&query.QualifiedIdentifier{Table: "test", Column: "id"},
					},
					From:  &query.TableRef{Name: "test"},
					Where: whereExpr,
				}

				compiled, err := compiler.CompileQuery(tc.sql, stmt, nil)
				if err != nil {
					t.Fatalf("Failed to compile: %v", err)
				}

				if len(compiled.Bytecode) < 8 {
					t.Errorf("Bytecode too short: %d bytes", len(compiled.Bytecode))
				}
			})
		}
	})
}
