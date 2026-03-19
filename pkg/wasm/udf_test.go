package wasm

import (
	"encoding/binary"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestUserDefinedFunctions tests UDF registration and execution
func TestUserDefinedFunctions(t *testing.T) {
	t.Run("builtin_square_udf", func(t *testing.T) {
		host := NewHostFunctions()

		// Test SQUARE UDF
		udf, ok := host.GetUDF("SQUARE")
		if !ok {
			t.Fatal("SQUARE UDF not found")
		}

		if udf.ParamCount != 1 {
			t.Errorf("Expected 1 parameter, got %d", udf.ParamCount)
		}

		// Execute UDF
		result, err := udf.Fn([]interface{}{int64(5)})
		if err != nil {
			t.Fatalf("UDF execution failed: %v", err)
		}

		if result != int64(25) {
			t.Errorf("Expected 25, got %v", result)
		}

		t.Logf("SQUARE(5) = %v", result)
	})

	t.Run("builtin_cube_udf", func(t *testing.T) {
		host := NewHostFunctions()

		// Test CUBE UDF
		udf, ok := host.GetUDF("CUBE")
		if !ok {
			t.Fatal("CUBE UDF not found")
		}

		result, err := udf.Fn([]interface{}{int64(3)})
		if err != nil {
			t.Fatalf("UDF execution failed: %v", err)
		}

		if result != int64(27) {
			t.Errorf("Expected 27, got %v", result)
		}

		t.Logf("CUBE(3) = %v", result)
	})

	t.Run("builtin_abs_val_udf", func(t *testing.T) {
		host := NewHostFunctions()

		// Test ABS_VAL UDF with positive number
		udf, ok := host.GetUDF("ABS_VAL")
		if !ok {
			t.Fatal("ABS_VAL UDF not found")
		}

		result, err := udf.Fn([]interface{}{int64(-10)})
		if err != nil {
			t.Fatalf("UDF execution failed: %v", err)
		}

		if result != int64(10) {
			t.Errorf("Expected 10, got %v", result)
		}

		// Test with positive number
		result2, _ := udf.Fn([]interface{}{int64(10)})
		if result2 != int64(10) {
			t.Errorf("Expected 10, got %v", result2)
		}

		t.Logf("ABS_VAL(-10) = %v, ABS_VAL(10) = %v", result, result2)
	})

	t.Run("builtin_power_int_udf", func(t *testing.T) {
		host := NewHostFunctions()

		// Test POWER_INT UDF
		udf, ok := host.GetUDF("POWER_INT")
		if !ok {
			t.Fatal("POWER_INT UDF not found")
		}

		if udf.ParamCount != 2 {
			t.Errorf("Expected 2 parameters, got %d", udf.ParamCount)
		}

		result, err := udf.Fn([]interface{}{int64(2), int64(3)})
		if err != nil {
			t.Fatalf("UDF execution failed: %v", err)
		}

		if result != int64(8) {
			t.Errorf("Expected 8, got %v", result)
		}

		t.Logf("POWER_INT(2, 3) = %v", result)
	})

	t.Run("register_custom_udf", func(t *testing.T) {
		host := NewHostFunctions()

		// Register a custom UDF
		customUDF := UserDefinedFunction{
			Name:       "DOUBLE",
			ParamCount: 1,
			Fn: func(args []interface{}) (interface{}, error) {
				if len(args) < 1 {
					return nil, nil
				}
				switch v := args[0].(type) {
				case int64:
					return v * 2, nil
				default:
					return nil, nil
				}
			},
		}

		err := host.RegisterUDF("DOUBLE", customUDF)
		if err != nil {
			t.Fatalf("Failed to register UDF: %v", err)
		}

		// Verify UDF was registered
		udf, ok := host.GetUDF("DOUBLE")
		if !ok {
			t.Fatal("Custom UDF not found after registration")
		}

		// Test execution
		result, _ := udf.Fn([]interface{}{int64(21)})
		if result != int64(42) {
			t.Errorf("Expected 42, got %v", result)
		}

		t.Logf("DOUBLE(21) = %v", result)
	})

	t.Run("execute_udf_host_function", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write function name to memory
		funcNamePtr := int32(1024)
		copy(rt.Memory[funcNamePtr:], "SQUARE")

		// Write argument (5) to memory
		argsPtr := int32(2048)
		binary.LittleEndian.PutUint64(rt.Memory[argsPtr:], uint64(5))

		// Call executeUDF: funcNamePtr=1024, funcNameLen=6, argsPtr=2048, argCount=1
		params := []uint64{uint64(funcNamePtr), 6, uint64(argsPtr), 1}
		result, err := host.executeUDF(rt, params)
		if err != nil {
			t.Fatalf("executeUDF failed: %v", err)
		}

		if result[0] != 25 {
			t.Errorf("Expected 25, got %d", result[0])
		}

		t.Logf("executeUDF('SQUARE', 5) = %d", result[0])
	})

	t.Run("udf_not_found", func(t *testing.T) {
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		// Write non-existent function name to memory
		funcNamePtr := int32(1024)
		copy(rt.Memory[funcNamePtr:], "NONEXISTENT")

		// Call executeUDF
		params := []uint64{uint64(funcNamePtr), 11, 0, 0}
		result, _ := host.executeUDF(rt, params)

		if result[0] != 0 {
			t.Errorf("Expected 0 for non-existent UDF, got %d", result[0])
		}

		t.Log("Non-existent UDF correctly returned 0")
	})

	t.Run("udf_in_select", func(t *testing.T) {
		// Create runtime with host functions
		rt := NewRuntime(10)
		host := NewHostFunctions()
		host.RegisterAll(rt)

		compiler := NewCompiler()

		// Create SELECT with UDF: SELECT SQUARE(id) FROM test
		// For now, we just verify the compilation works
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{
					Name: "SQUARE",
					Args: []query.Expression{
						&query.QualifiedIdentifier{Table: "test", Column: "id"},
					},
				},
			},
			From: &query.TableRef{Name: "test"},
		}

		compiled, err := compiler.CompileQuery("SELECT SQUARE(id) FROM test", stmt, nil)
		if err != nil {
			t.Fatalf("Failed to compile: %v", err)
		}

		// Set correct schema
		compiled.ResultSchema = []ColumnInfo{
			{Name: "square_id", Type: "INTEGER", Nullable: false},
		}

		// Execute
		result, err := rt.Execute(compiled, nil)
		if err != nil {
			t.Fatalf("Execution failed: %v", err)
		}

		t.Logf("UDF in SELECT result: RowsAffected=%d, RowCount=%d", result.RowsAffected, len(result.Rows))
	})
}
