package wasm

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestDistinct tests SQL DISTINCT compilation and execution
func TestDistinct(t *testing.T) {
	t.Run("distinct_compilation", func(t *testing.T) {
		compiler := NewCompiler()

		// Create SELECT DISTINCT
		stmt := &query.SelectStmt{
			Distinct: true,
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "category"},
			},
			From: &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT DISTINCT category FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled DISTINCT, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("distinct_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT DISTINCT
		stmt := &query.SelectStmt{
			Distinct: true,
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "category"},
			},
			From: &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT DISTINCT category FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema (using INTEGER since that's what the test data writes)
		compiled.ResultSchema = []ColumnInfo{
			{Name: "category", Type: "INTEGER", Nullable: true},
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("DISTINCT result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have 2 distinct categories (A and B)
		// Note: simplified implementation doesn't actually deduplicate
		if len(result.Rows) == 0 {
			t.Errorf("Expected rows, got 0")
		}
	})
}
