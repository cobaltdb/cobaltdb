package wasm

import (
	"encoding/binary"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestSubqueries tests SQL subquery compilation and execution
func TestSubqueries(t *testing.T) {
	t.Run("subquery_in_select_compilation", func(t *testing.T) {
		compiler := NewCompiler()

		// Create SELECT with subquery: SELECT (SELECT COUNT(*) FROM test)
		subquery := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{
					Name: "COUNT",
					Args: []query.Expression{
						&query.StarExpr{},
					},
				},
			},
			From: &query.TableRef{Name: "test"},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.SubqueryExpr{Query: subquery},
			},
			From: &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT (SELECT COUNT(*) FROM test) FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled subquery in SELECT, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("subquery_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with subquery
		subquery := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{
					Name: "COUNT",
					Args: []query.Expression{
						&query.StarExpr{},
					},
				},
			},
			From: &query.TableRef{Name: "test"},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.SubqueryExpr{Query: subquery},
			},
			From: &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT (SELECT COUNT(*) FROM test) FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema
		compiled.ResultSchema = []ColumnInfo{
			{Name: "subquery", Type: "INTEGER", Nullable: false},
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("Subquery result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have rows
		if len(result.Rows) == 0 {
			t.Errorf("Expected rows, got 0")
		}
	})

	t.Run("correlated_subquery_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Test correlated subquery directly via host function
		// Write outer row data to memory
		outerRowPtr := int32(1024)
		binary.LittleEndian.PutUint64(rt.Memory[outerRowPtr:], uint64(1)) // outer row id = 1

		outPtr := int32(2048)

		// Call executeCorrelatedSubquery: queryId=0, outerRowPtr=1024, outerRowSize=8, outPtr=2048, maxRows=10
		params := []uint64{0, uint64(outerRowPtr), 8, uint64(outPtr), 10}
		result, err := host.executeCorrelatedSubquery(rt, params)
		if err != nil {
			t.Fatalf("Correlated subquery execution failed: %v", err)
		}

		rowCount := int(result[0])
		t.Logf("Correlated subquery returned %d rows", rowCount)

		// Verify results are in memory
		count := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		t.Logf("Subquery count result: %d", count)
	})

	t.Run("exists_correlated_subquery_compilation", func(t *testing.T) {
		compiler := NewCompiler()

		// Create correlated subquery:
		// SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
		innerSubquery := &query.SelectStmt{
			Columns: []query.Expression{
				&query.NumberLiteral{Value: 1},
			},
			From: &query.TableRef{Name: "orders"},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "u", Column: "id"},
				&query.QualifiedIdentifier{Table: "u", Column: "name"},
			},
			From: &query.TableRef{Name: "users", Alias: "u"},
			Where: &query.ExistsExpr{
				Subquery: innerSubquery,
				Not:      false,
			},
		}

		compiled, err := compiler.CompileQuery(
			"SELECT u.id, u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
			stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile correlated EXISTS subquery: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled correlated EXISTS subquery, bytecode length: %d bytes", len(compiled.Bytecode))
	})
}
