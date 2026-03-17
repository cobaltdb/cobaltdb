package wasm

import (
	"testing"
)

func TestNewRuntime(t *testing.T) {
	r := NewRuntime(1)
	if r == nil {
		t.Fatal("NewRuntime returned nil")
	}
	if len(r.Memory) != 65536 {
		t.Errorf("Expected 64KB initial memory, got %d bytes", len(r.Memory))
	}
	if len(r.Stack) != 0 {
		t.Error("Stack should be empty initially")
	}
}

func TestRuntimeRegisterImport(t *testing.T) {
	r := NewRuntime(1)

	fn := func(rt *Runtime, params []uint64) ([]uint64, error) {
		return []uint64{42}, nil
	}

	r.RegisterImport("env", "test", fn)

	if _, ok := r.Imports["env:test"]; !ok {
		t.Error("RegisterImport didn't register the function")
	}
}

func TestRuntimeWriteReadParams(t *testing.T) {
	r := NewRuntime(1)

	args := []interface{}{int64(42), "hello", nil, 3.14}
	err := r.writeParams(0, args)
	if err != nil {
		t.Errorf("writeParams failed: %v", err)
	}
}

func TestCallFrame(t *testing.T) {
	frame := CallFrame{
		FuncIdx:  0,
		Locals:   make([]uint64, 10),
		ReturnPC: 100,
		SP:       0,
	}

	if frame.FuncIdx != 0 {
		t.Error("CallFrame.FuncIdx incorrect")
	}
	if len(frame.Locals) != 10 {
		t.Error("CallFrame.Locals length incorrect")
	}
}

func TestFunction(t *testing.T) {
	fn := Function{
		TypeIdx: 0,
		Locals:  []ValueType{I32, I64},
		Code:    []byte{0x00, 0x0b},
	}

	if fn.TypeIdx != 0 {
		t.Error("Function.TypeIdx incorrect")
	}
	if len(fn.Locals) != 2 {
		t.Error("Function.Locals length incorrect")
	}
}

func TestQueryResult(t *testing.T) {
	result := &QueryResult{
		RowsAffected: 5,
		Rows: []Row{
			{Values: []interface{}{1, "test"}},
		},
		Columns: []string{"id", "name"},
	}

	if result.RowsAffected != 5 {
		t.Errorf("RowsAffected = %d, want 5", result.RowsAffected)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Rows = %d, want 1", len(result.Rows))
	}
}

func TestRow(t *testing.T) {
	row := Row{
		Values: []interface{}{1, "hello", 3.14},
	}

	if len(row.Values) != 3 {
		t.Errorf("Row.Values length = %d, want 3", len(row.Values))
	}
}

func TestRuntimeValueTypes(t *testing.T) {
	// Test that value types have correct WASM encoding
	if I32 != 0x7f {
		t.Errorf("I32 = 0x%02x, want 0x7f", I32)
	}
	if I64 != 0x7e {
		t.Errorf("I64 = 0x%02x, want 0x7e", I64)
	}
	if F32 != 0x7d {
		t.Errorf("F32 = 0x%02x, want 0x7d", F32)
	}
	if F64 != 0x7c {
		t.Errorf("F64 = 0x%02x, want 0x7c", F64)
	}
}

func TestImportFunc(t *testing.T) {
	var called bool
	fn := ImportFunc(func(rt *Runtime, params []uint64) ([]uint64, error) {
		called = true
		return []uint64{1, 2, 3}, nil
	})

	r := NewRuntime(1)
	result, err := fn(r, []uint64{10, 20})
	if err != nil {
		t.Errorf("ImportFunc returned error: %v", err)
	}
	if !called {
		t.Error("ImportFunc was not called")
	}
	if len(result) != 3 {
		t.Errorf("result length = %d, want 3", len(result))
	}
}

func TestRuntimeMemorySize(t *testing.T) {
	tests := []struct {
		pages    int
		expected int
	}{
		{1, 65536},
		{2, 131072},
		{4, 262144},
	}

	for _, tt := range tests {
		r := NewRuntime(tt.pages)
		if len(r.Memory) != tt.expected {
			t.Errorf("NewRuntime(%d) memory = %d, want %d",
				tt.pages, len(r.Memory), tt.expected)
		}
	}
}

func TestRuntimeCompiledQuery(t *testing.T) {
	query := &CompiledQuery{
		SQL:          "SELECT * FROM test",
		Bytecode:     []byte{0x00, 0x01, 0x02},
		EntryPoint:   0,
		ParamCount:   0,
		ResultSchema: nil,
		MemoryLayout: MemoryLayout{
			ParamBase:  0,
			ResultBase: 1024,
			RowSize:    64,
			MaxRows:    100,
		},
	}

	if query.EntryPoint != 0 {
		t.Errorf("EntryPoint = %d, want 0", query.EntryPoint)
	}
	if query.MemoryLayout.ResultBase != 1024 {
		t.Errorf("ResultBase = %d, want 1024", query.MemoryLayout.ResultBase)
	}
}

func TestRuntimeColumnInfo(t *testing.T) {
	col := ColumnInfo{
		Name:     "id",
		Type:     "INTEGER",
		Nullable: false,
	}

	if col.Name != "id" {
		t.Errorf("Name = %s, want id", col.Name)
	}
	if col.Type != "INTEGER" {
		t.Errorf("Type = %s, want INTEGER", col.Type)
	}
	if col.Nullable {
		t.Error("Nullable should be false")
	}
}

func TestRuntimeMemoryLayout(t *testing.T) {
	layout := MemoryLayout{
		ParamBase:  0,
		ResultBase: 1024,
		RowSize:    64,
		MaxRows:    100,
	}

	if layout.ParamBase != 0 {
		t.Errorf("ParamBase = %d, want 0", layout.ParamBase)
	}
	if layout.ResultBase != 1024 {
		t.Errorf("ResultBase = %d, want 1024", layout.ResultBase)
	}
}
