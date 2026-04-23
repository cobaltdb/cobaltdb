package wasm

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestAggregateFunctions tests SQL aggregate function compilation
func TestAggregateFunctions(t *testing.T) {
	t.Run("count_star", func(t *testing.T) {
		compiler := NewCompiler()

		// Create COUNT(*) expression
		fn := &query.FunctionCall{
			Name: "COUNT",
			Args: []query.Expression{
				&query.StarExpr{},
			},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{fn},
			From:    &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT COUNT(*) FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled COUNT(*), bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("sum_column", func(t *testing.T) {
		compiler := NewCompiler()

		// Create SUM(id) expression
		fn := &query.FunctionCall{
			Name: "SUM",
			Args: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{fn},
			From:    &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT SUM(id) FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		t.Logf("Compiled SUM(id), bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("avg_column", func(t *testing.T) {
		compiler := NewCompiler()

		// Create AVG(id) expression
		fn := &query.FunctionCall{
			Name: "AVG",
			Args: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{fn},
			From:    &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT AVG(id) FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		t.Logf("Compiled AVG(id), bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("min_column", func(t *testing.T) {
		compiler := NewCompiler()

		// Create MIN(id) expression
		fn := &query.FunctionCall{
			Name: "MIN",
			Args: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{fn},
			From:    &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT MIN(id) FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		t.Logf("Compiled MIN(id), bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("max_column", func(t *testing.T) {
		compiler := NewCompiler()

		// Create MAX(id) expression
		fn := &query.FunctionCall{
			Name: "MAX",
			Args: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{fn},
			From:    &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT MAX(id) FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		t.Logf("Compiled MAX(id), bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("aggregate_with_group_by_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with GROUP BY
		fn := &query.FunctionCall{
			Name: "COUNT",
			Args: []query.Expression{
				&query.StarExpr{},
			},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "category"},
				fn,
			},
			From: &query.TableRef{Name: "test"},
			GroupBy: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "category"},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT category, COUNT(*) FROM test GROUP BY category", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("GROUP BY result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have 2 groups (A and B)
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 groups, got %d", len(result.Rows))
		}
	})
}
