package wasm

import (
	"encoding/binary"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestPreparedStatements tests prepared statement compilation and execution
func TestPreparedStatements(t *testing.T) {
	t.Run("prepare_statement", func(t *testing.T) {
		compiler := NewCompiler()

		// Create a SELECT statement
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
				&query.QualifiedIdentifier{Table: "test", Column: "name"},
			},
			From: &query.TableRef{Name: "test"},
		}

		// Prepare statement with 2 parameters
		prepared, err := compiler.Prepare("SELECT id, name FROM test WHERE id = ? AND category = ?", stmt, 2)
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}

		if prepared.ID == "" {
			t.Error("Expected prepared statement to have an ID")
		}

		if prepared.ParamCount != 2 {
			t.Errorf("Expected 2 parameters, got %d", prepared.ParamCount)
		}

		if len(prepared.ParamTypes) != 2 {
			t.Errorf("Expected 2 parameter types, got %d", len(prepared.ParamTypes))
		}

		t.Logf("Prepared statement: ID=%s, ParamCount=%d", prepared.ID, prepared.ParamCount)
	})

	t.Run("get_prepared_statement", func(t *testing.T) {
		compiler := NewCompiler()

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}

		// Prepare statement
		prepared, err := compiler.Prepare("SELECT id FROM test WHERE id = ?", stmt, 1)
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}

		// Retrieve prepared statement
		retrieved, ok := compiler.GetPreparedStatement(prepared.ID)
		if !ok {
			t.Error("Failed to retrieve prepared statement")
		}

		if retrieved.SQL != prepared.SQL {
			t.Error("Retrieved statement SQL mismatch")
		}

		t.Logf("Retrieved prepared statement: ID=%s", retrieved.ID)
	})

	t.Run("execute_prepared_statement", func(t *testing.T) {
		compiler := NewCompiler()

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}

		// Prepare statement
		prepared, err := compiler.Prepare("SELECT id FROM test WHERE id = ?", stmt, 1)
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}

		// Execute with parameters
		compiled, err := compiler.ExecutePrepared(prepared.ID, []interface{}{1})
		if err != nil {
			t.Fatalf("Failed to execute prepared statement: %v", err)
		}

		if compiled == nil {
			t.Error("Expected compiled query, got nil")
		}

		t.Logf("Executed prepared statement, bytecode length: %d", len(compiled.Bytecode))
	})

	t.Run("close_prepared_statement", func(t *testing.T) {
		compiler := NewCompiler()

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}

		// Prepare statement
		prepared, err := compiler.Prepare("SELECT id FROM test", stmt, 0)
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}

		// Close prepared statement
		compiler.ClosePreparedStatement(prepared.ID)

		// Verify it's removed
		_, ok := compiler.GetPreparedStatement(prepared.ID)
		if ok {
			t.Error("Expected prepared statement to be removed after close")
		}
	})

	t.Run("parameter_count_mismatch", func(t *testing.T) {
		compiler := NewCompiler()

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
		}

		// Prepare statement with 2 parameters
		prepared, err := compiler.Prepare("SELECT id FROM test WHERE id = ? AND name = ?", stmt, 2)
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}

		// Execute with wrong number of parameters
		_, err = compiler.ExecutePrepared(prepared.ID, []interface{}{1})
		if err == nil {
			t.Error("Expected error for parameter count mismatch")
		}

		t.Logf("Got expected error: %v", err)
	})

	t.Run("bind_parameter_host_function", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write test parameter value to memory
		valuePtr := int32(2048)
		binary.LittleEndian.PutUint64(rt.Memory[valuePtr:], uint64(42))

		// Call bindParameter: slotIdx=0, valuePtr=2048, valueType=1 (i64)
		params := []uint64{0, uint64(valuePtr), 1}
		result, err := host.bindParameter(rt, params)
		if err != nil {
			t.Fatalf("bindParameter execution failed: %v", err)
		}

		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		t.Logf("bindParameter succeeded")
	})
}
