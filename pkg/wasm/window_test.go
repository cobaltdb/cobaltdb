package wasm

import (
	"encoding/binary"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestWindowFunctions tests SQL window function compilation and execution
func TestWindowFunctions(t *testing.T) {
	t.Run("row_number_compilation", func(t *testing.T) {
		compiler := NewCompiler()

		// Create SELECT with ROW_NUMBER()
		windowExpr := &query.WindowExpr{
			Function: "ROW_NUMBER",
			Args:     []query.Expression{},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
				windowExpr,
			},
			From: &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT id, ROW_NUMBER() OVER () FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		if len(compiled.Bytecode) < 8 {
			t.Fatalf("Bytecode too short: %d bytes", len(compiled.Bytecode))
		}

		t.Logf("Compiled ROW_NUMBER, bytecode length: %d bytes", len(compiled.Bytecode))
	})

	t.Run("row_number_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with ROW_NUMBER()
		windowExpr := &query.WindowExpr{
			Function: "ROW_NUMBER",
			Args:     []query.Expression{},
		}

		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.QualifiedIdentifier{Table: "test", Column: "id"},
				windowExpr,
			},
			From: &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT id, ROW_NUMBER() OVER () FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema
		compiled.ResultSchema = []ColumnInfo{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "row_number", Type: "INTEGER", Nullable: false},
		}

		// Execute the query
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("ROW_NUMBER result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))

		// Should have 3 rows
		if len(result.Rows) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Rows))
		}
	})

	t.Run("lag_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Test LAG function directly via host function
		// Write test data to memory: [1, 2, 3]
		inPtr := int32(2048)
		outPtr := int32(3072)
		binary.LittleEndian.PutUint64(rt.Memory[inPtr:], uint64(1))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+8:], uint64(2))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+16:], uint64(3))

		// Call windowFunction with LAG (funcType=3, offset=1, default=0)
		params := []uint64{uint64(inPtr), 3, 3, uint64(outPtr), 1, 0}
		result, err := host.windowFunction(rt, params)
		if err != nil {
			t.Fatalf("LAG execution failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Verify results: [0, 1, 2] (LAG 1 with default 0)
		val0 := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		val1 := binary.LittleEndian.Uint64(rt.Memory[outPtr+8:])
		val2 := binary.LittleEndian.Uint64(rt.Memory[outPtr+16:])

		if val0 != 0 || val1 != 1 || val2 != 2 {
			t.Errorf("LAG results incorrect: got [%d, %d, %d], expected [0, 1, 2]", val0, val1, val2)
		}

		t.Logf("LAG result: [%d, %d, %d]", val0, val1, val2)
	})

	t.Run("lead_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Test LEAD function directly via host function
		// Write test data to memory: [10, 20, 30]
		inPtr := int32(2048)
		outPtr := int32(3072)
		binary.LittleEndian.PutUint64(rt.Memory[inPtr:], uint64(10))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+8:], uint64(20))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+16:], uint64(30))

		// Call windowFunction with LEAD (funcType=4, offset=1, default=99)
		params := []uint64{uint64(inPtr), 3, 4, uint64(outPtr), 1, 99}
		result, err := host.windowFunction(rt, params)
		if err != nil {
			t.Fatalf("LEAD execution failed: %v", err)
		}
		if result[0] != 1 {
			t.Errorf("Expected success (1), got %d", result[0])
		}

		// Verify results: [20, 30, 99] (LEAD 1 with default 99)
		val0 := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		val1 := binary.LittleEndian.Uint64(rt.Memory[outPtr+8:])
		val2 := binary.LittleEndian.Uint64(rt.Memory[outPtr+16:])

		if val0 != 20 || val1 != 30 || val2 != 99 {
			t.Errorf("LEAD results incorrect: got [%d, %d, %d], expected [20, 30, 99]", val0, val1, val2)
		}

		t.Logf("LEAD result: [%d, %d, %d]", val0, val1, val2)
	})

	t.Run("first_value_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Test FIRST_VALUE function
		inPtr := int32(2048)
		outPtr := int32(3072)
		binary.LittleEndian.PutUint64(rt.Memory[inPtr:], uint64(100))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+8:], uint64(200))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+16:], uint64(300))

		// Call windowFunction with FIRST_VALUE (funcType=5)
		params := []uint64{uint64(inPtr), 3, 5, uint64(outPtr)}
		_, err := host.windowFunction(rt, params)
		if err != nil {
			t.Fatalf("FIRST_VALUE execution failed: %v", err)
		}

		// All rows should have first value (100)
		val0 := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		val1 := binary.LittleEndian.Uint64(rt.Memory[outPtr+8:])
		val2 := binary.LittleEndian.Uint64(rt.Memory[outPtr+16:])

		if val0 != 100 || val1 != 100 || val2 != 100 {
			t.Errorf("FIRST_VALUE results incorrect: got [%d, %d, %d], expected [100, 100, 100]", val0, val1, val2)
		}

		t.Logf("FIRST_VALUE result: [%d, %d, %d]", val0, val1, val2)
	})

	t.Run("running_sum_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Test SUM OVER (running sum)
		inPtr := int32(2048)
		outPtr := int32(3072)
		binary.LittleEndian.PutUint64(rt.Memory[inPtr:], uint64(10))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+8:], uint64(20))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+16:], uint64(30))

		// Call windowFunction with SUM (funcType=10)
		params := []uint64{uint64(inPtr), 3, 10, uint64(outPtr)}
		_, err := host.windowFunction(rt, params)
		if err != nil {
			t.Fatalf("SUM OVER execution failed: %v", err)
		}

		// Verify running sum: [10, 30, 60]
		val0 := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		val1 := binary.LittleEndian.Uint64(rt.Memory[outPtr+8:])
		val2 := binary.LittleEndian.Uint64(rt.Memory[outPtr+16:])

		if val0 != 10 || val1 != 30 || val2 != 60 {
			t.Errorf("SUM OVER results incorrect: got [%d, %d, %d], expected [10, 30, 60]", val0, val1, val2)
		}

		t.Logf("SUM OVER result: [%d, %d, %d]", val0, val1, val2)
	})

	t.Run("running_avg_execution", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Test AVG OVER (running average)
		inPtr := int32(2048)
		outPtr := int32(3072)
		binary.LittleEndian.PutUint64(rt.Memory[inPtr:], uint64(10))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+8:], uint64(20))
		binary.LittleEndian.PutUint64(rt.Memory[inPtr+16:], uint64(30))

		// Call windowFunction with AVG (funcType=11)
		params := []uint64{uint64(inPtr), 3, 11, uint64(outPtr)}
		_, err := host.windowFunction(rt, params)
		if err != nil {
			t.Fatalf("AVG OVER execution failed: %v", err)
		}

		// Verify running avg: [10, 15, 20]
		val0 := binary.LittleEndian.Uint64(rt.Memory[outPtr:])
		val1 := binary.LittleEndian.Uint64(rt.Memory[outPtr+8:])
		val2 := binary.LittleEndian.Uint64(rt.Memory[outPtr+16:])

		if val0 != 10 || val1 != 15 || val2 != 20 {
			t.Errorf("AVG OVER results incorrect: got [%d, %d, %d], expected [10, 15, 20]", val0, val1, val2)
		}

		t.Logf("AVG OVER result: [%d, %d, %d]", val0, val1, val2)
	})
}
