package wasm

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestLimitOffset tests SQL LIMIT and OFFSET compilation and execution
func TestLimitOffset(t *testing.T) {
	t.Run("limit_compilation", func(t *testing.T) {
		compiler := NewCompiler()

		// Create SELECT with LIMIT
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From:  &query.TableRef{Name: "test"},
			Limit: &query.NumberLiteral{Value: 2},
		}

		compiled, err := compiler.CompileQuery("SELECT id FROM test LIMIT 2", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled LIMIT, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("limit_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with LIMIT
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From:  &query.TableRef{Name: "test"},
			Limit: &query.NumberLiteral{Value: 2},
		}

		compiled, err := compiler.CompileQuery("SELECT id FROM test LIMIT 2", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema
		compiled.ResultSchema = []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("LIMIT result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have 2 rows (limited from 3)
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	})
}
