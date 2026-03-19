package wasm

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestUnion tests SQL UNION compilation and execution
func TestUnion(t *testing.T) {
	t.Run("union_compilation", func(t *testing.T) {
		compiler := NewCompiler()

		// Create UNION statement: SELECT id FROM test UNION SELECT id FROM test
		leftStmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}

		rightStmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}

		unionStmt := &query.UnionStmt{
			Left:  leftStmt,
			Right: rightStmt,
			Op:    query.SetOpUnion,
		}

		compiled, err := compiler.CompileQuery("SELECT id FROM test UNION SELECT id FROM test", unionStmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled UNION, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("union_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create UNION statement
		leftStmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}

		rightStmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}

		unionStmt := &query.UnionStmt{
			Left:  leftStmt,
			Right: rightStmt,
			Op:    query.SetOpUnion,
		}

		compiled, err := compiler.CompileQuery("SELECT id FROM test UNION SELECT id FROM test", unionStmt, nil)
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

		t.Logf("UNION result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have rows
		if len(result.Rows) == 0 {
			t.Errorf("Expected rows, got 0")
		}
	})
}
