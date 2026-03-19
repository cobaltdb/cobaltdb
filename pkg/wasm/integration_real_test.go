package wasm

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestRealSQLExecution tests actual SQL compilation and execution
func TestRealSQLExecution(t *testing.T) {
	// Test 1: Simple SELECT 1+1
	t.Run("select_literal", func(t *testing.T) {
		compiler := NewCompiler()
		
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.NumberLiteral{Value: 2},
			},
		}
		
		compiled, err := compiler.CompileQuery("SELECT 2", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		
		if compiled == nil {
			t.Fatal("Compiled query is nil")
		}
		
		// Verify bytecode was generated
		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}
		
		// Check WASM magic number
		if compiled.Bytecode[0] != 0x00 || compiled.Bytecode[1] != 0x61 || 
		   compiled.Bytecode[2] != 0x73 || compiled.Bytecode[3] != 0x6d {
			t.Fatal("Invalid WASM magic number")
		}
		
		t.Logf("Generated WASM bytecode: %d bytes", len(compiled.Bytecode))
	})
	
	// Test 2: INSERT VALUES
	t.Run("insert_values", func(t *testing.T) {
		compiler := NewCompiler()
		
		stmt := &query.InsertStmt{
			Table: "users",
			Columns: []string{"id", "name"},
			Values: [][]query.Expression{
				{
					&query.NumberLiteral{Value: 1},
					&query.StringLiteral{Value: "Alice"},
				},
			},
		}
		
		compiled, err := compiler.CompileQuery("INSERT INTO users VALUES (1, 'Alice')", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		
		if compiled == nil {
			t.Fatal("Compiled query is nil")
		}
		
		t.Logf("Generated INSERT WASM: %d bytes", len(compiled.Bytecode))
	})
	
	// Test 3: Runtime execute simple module
	t.Run("runtime_execute", func(t *testing.T) {
		rt := NewRuntime(1)
		
		// Register a mock tableScan import
		rt.RegisterImport("env", "tableScan", func(rt *Runtime, params []uint64) ([]uint64, error) {
			// Mock: return 1 row scanned
			return []uint64{1}, nil
		})
		
		// Create a simple compiled query
		compiler := NewCompiler()
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}
		
		compiled, err := compiler.CompileQuery("SELECT id FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}
		
		// Execute the compiled WASM
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Failed to execute: %v", err)
		}
		
		if result == nil {
			t.Fatal("Result is nil")
		}
		
		t.Logf("Execution result: %+v", result)
	})
}
