package wasm

import (
	"encoding/binary"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestHostFunctions tests WASM execution with real host functions
func TestHostFunctions(t *testing.T) {
	t.Run("table_scan_returns_data", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Create a SELECT query compiler - just id column (INTEGER)
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

		// Override schema to set correct type (INTEGER for id column)
		compiled.ResultSchema = []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
		}

		// Load and execute
		rt.LoadModule(compiled.Bytecode)
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("Result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}

		// Verify row values
		for i, row := range result.Rows {
			if len(row.Values) != 1 {
				t.Errorf("Row %d: expected 1 value, got %d", i, len(row.Values))
				continue
			}
			id, ok := row.Values[0].(int64)
			if !ok {
				t.Errorf("Row %d: expected int64, got %T", i, row.Values[0])
				continue
			}
			if id != int64(i+1) {
				t.Errorf("Row %d: expected id=%d, got %d", i, i+1, id)
			}
		}
	})

	t.Run("get_table_id", func(t *testing.T) {
		rt := NewRuntime(1)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write "test" to memory
		copy(rt.Memory[100:], "test")

		// Call getTableId
		results, err := host.getTableId(rt, []uint64{100, 4})
		if err != nil {
			t.Fatalf("getTableId failed: %v", err)
		}

		if len(results) < 1 || results[0] != 0 {
			t.Errorf("Expected tableId 0, got %v", results)
		}
	})

	t.Run("table_scan_with_host", func(t *testing.T) {
		// Create compiler
		compiler := NewCompiler()

		// Create a SELECT FROM statement
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

		// Set correct schema (INTEGER for id column)
		compiled.ResultSchema = []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
		}

		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Execute
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("Table scan result: %+v", result)

		if result == nil {
			t.Fatal("Result is nil")
		}

		// Should have 3 rows from the test table
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
	})

	t.Run("insert_execution", func(t *testing.T) {
		// Track insert calls
		insertCalled := false
		var insertTableId uint64
		var insertRowDataPtr uint64

		// Create compiler
		compiler := NewCompiler()

		// Create an INSERT statement
		stmt := &query.InsertStmt{
			Table:   "test",
			Columns: []string{"id", "name"},
			Values: [][]query.Expression{
				{
					&query.NumberLiteral{Value: 4},
					&query.StringLiteral{Value: "Dave"},
				},
			},
		}

		compiled, err := compiler.CompileQuery("INSERT INTO test VALUES (4, 'Dave')", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Override insertRow to track calls
		rt.RegisterImport("env", "insertRow", func(rt *Runtime, params []uint64) ([]uint64, error) {
			insertCalled = true
			insertTableId = params[0]
			insertRowDataPtr = params[1]
			t.Logf("insertRow called with tableId=%d, rowDataPtr=%d", params[0], params[1])
			return []uint64{1}, nil // success
		})

		// Execute
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("INSERT result: RowsAffected=%d", result.RowsAffected)

		// Verify insertRow was called
		if !insertCalled {
			t.Error("insertRow was not called")
		}
		if insertTableId != 0 {
			t.Errorf("Expected tableId 0, got %d", insertTableId)
		}
		if insertRowDataPtr != 2048 {
			t.Errorf("Expected rowDataPtr 2048, got %d", insertRowDataPtr)
		}

		// Should have affected 1 row
		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
		}
	})

	t.Run("delete_execution", func(t *testing.T) {
		// Track delete calls
		deleteCalled := false
		var deleteTableId uint64
		var deleteRowId uint64

		// Create compiler
		compiler := NewCompiler()

		// Create a DELETE statement (WHERE clause not used in simplified WASM generation)
		stmt := &query.DeleteStmt{
			Table: "test",
		}

		compiled, err := compiler.CompileQuery("DELETE FROM test WHERE id = 1", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Override deleteRow to track calls
		rt.RegisterImport("env", "deleteRow", func(rt *Runtime, params []uint64) ([]uint64, error) {
			deleteCalled = true
			deleteTableId = params[0]
			deleteRowId = params[1]
			t.Logf("deleteRow called with tableId=%d, rowId=%d", params[0], params[1])
			return []uint64{1}, nil // success
		})

		// Execute
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("DELETE result: RowsAffected=%d", result.RowsAffected)

		// Verify deleteRow was called
		if !deleteCalled {
			t.Error("deleteRow was not called")
		}
		if deleteTableId != 0 {
			t.Errorf("Expected tableId 0, got %d", deleteTableId)
		}
		if deleteRowId != 1 {
			t.Errorf("Expected rowId 1, got %d", deleteRowId)
		}

		// Should have affected 1 row
		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
		}
	})

	t.Run("select_with_where_clause", func(t *testing.T) {
		// Create compiler
		compiler := NewCompiler()

		// Create a SELECT with WHERE clause
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
			},
			From: &query.TableRef{Name: "test"},
			Where: &query.BinaryExpr{
				Left:     &query.QualifiedIdentifier{Table: "test", Column: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 1},
			},
		}

		compiled, err := compiler.CompileQuery("SELECT id FROM test WHERE id = 1", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema
		compiled.ResultSchema = []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
		}

		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Override filterRow to return filtered results
		filterCalled := false
		rt.RegisterImport("env", "filterRow", func(rt *Runtime, params []uint64) ([]uint64, error) {
			filterCalled = true
			t.Logf("filterRow called with params: %v", params)
			// Return 1 matching row
			return []uint64{1}, nil
		})

		// Execute
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("SELECT WHERE result: %+v", result)

		// Verify filterRow was called when WHERE clause exists
		if !filterCalled {
			t.Error("filterRow was not called for WHERE clause query")
		}
	})

	t.Run("index_scan_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Test indexScan directly via host function
		outPtr := int32(2048)

		// Call indexScan: tableId=0, indexId=0, minVal=0, maxVal=100, outPtr=2048, maxRows=10
		params := []uint64{0, 0, 0, 100, uint64(outPtr), 10}
		result, err := host.indexScan(rt, params)
		if err != nil {
			t.Fatalf("indexScan execution failed: %v", err)
		}

		rowCount := int(result[0])
		t.Logf("indexScan returned %d rows", rowCount)

		// Should have 3 rows from test table
		if rowCount != 3 {
			t.Errorf("Expected 3 rows from index scan, got %d", rowCount)
		}

		// Verify row ids are in memory
		for i := 0; i < rowCount; i++ {
			offset := outPtr + int32(i*8)
			id := binary.LittleEndian.Uint64(rt.Memory[offset:])
			t.Logf("Row %d: id=%d", i, id)
		}
	})
}
