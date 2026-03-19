package wasm

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestOrderBy tests SQL ORDER BY compilation and execution
func TestOrderBy(t *testing.T) {
	t.Run("order_by_compilation", func(t *testing.T) {
		compiler := NewCompiler()

		// Create SELECT with ORDER BY
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
				&query.QualifiedIdentifier{Table: "test", Column: "name"},
			},
			From: &query.TableRef{Name: "test"},
			OrderBy: []*query.OrderByExpr{
				{
					Expr: &query.QualifiedIdentifier{Table: "test", Column: "id"},
					Desc: false,
				},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT id, name FROM test ORDER BY id", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled ORDER BY, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("order_by_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with ORDER BY
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
			OrderBy: []*query.OrderByExpr{
				{
					Expr: &query.QualifiedIdentifier{Table: "test", Column: "id"},
					Desc: false,
				},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT id FROM test ORDER BY id", stmt, nil)
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

		t.Logf("ORDER BY result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have 3 rows
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
	})
}
