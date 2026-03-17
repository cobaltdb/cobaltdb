package wasm

import (
	"testing"
)

func TestNewCompiler(t *testing.T) {
	c := NewCompiler()
	if c == nil {
		t.Fatal("NewCompiler returned nil")
	}
	if c.queryCache == nil {
		t.Error("queryCache not initialized")
	}
}

func TestCompilerCompileQuery(t *testing.T) {
	c := NewCompiler()

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name:    "empty_sql",
			sql:     "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// CompileQuery requires a parsed statement, so empty SQL should error
			query, err := c.CompileQuery(tt.sql, nil, nil)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error for empty SQL")
				}
				return
			}
			if err != nil {
				t.Errorf("CompileQuery() error = %v", err)
				return
			}
			if query == nil {
				t.Error("CompileQuery() returned nil query")
			}
		})
	}
}

func TestCompilerValueTypes(t *testing.T) {
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

func TestCompilerMemoryLayout(t *testing.T) {
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

func TestFuncType(t *testing.T) {
	ft := FuncType{
		Params:  []ValueType{I32, I32},
		Results: []ValueType{I32},
	}

	if len(ft.Params) != 2 {
		t.Errorf("Params length = %d, want 2", len(ft.Params))
	}
	if len(ft.Results) != 1 {
		t.Errorf("Results length = %d, want 1", len(ft.Results))
	}
}

func TestCompilerCompiledQuery(t *testing.T) {
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

func TestCompilerColumnInfo(t *testing.T) {
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

func TestValueTypeByte(t *testing.T) {
	// ValueType is defined as byte, test basic operations
	var vt ValueType = I32
	if vt != 0x7f {
		t.Errorf("ValueType I32 = 0x%02x, want 0x7f", vt)
	}

	vt = F64
	if vt != 0x7c {
		t.Errorf("ValueType F64 = 0x%02x, want 0x7c", vt)
	}
}

func TestModule(t *testing.T) {
	m := &Module{}
	if m == nil {
		t.Fatal("Module is nil")
	}
}

func TestMemory(t *testing.T) {
	mem := &Memory{
		MinPages: 1,
		MaxPages: 10,
	}

	if mem.MinPages != 1 {
		t.Errorf("Memory.MinPages = %d, want 1", mem.MinPages)
	}
	if mem.MaxPages != 10 {
		t.Errorf("Memory.MaxPages = %d, want 10", mem.MaxPages)
	}
}

func TestGlobal(t *testing.T) {
	g := Global{
		Type:    I32,
		Init:    []byte{0x41, 0x2a, 0x0b}, // i32.const 42, end
		Mutable: true,
	}

	if g.Type != I32 {
		t.Errorf("Global.Type = 0x%02x, want 0x7f", g.Type)
	}
	if len(g.Init) != 3 {
		t.Errorf("Global.Init length = %d, want 3", len(g.Init))
	}
	if !g.Mutable {
		t.Error("Global.Mutable should be true")
	}
}

func TestExport(t *testing.T) {
	e := Export{
		Name:  "main",
		Kind:  0x00, // function
		Index: 0,
	}

	if e.Name != "main" {
		t.Errorf("Export.Name = %s, want main", e.Name)
	}
	if e.Kind != 0x00 {
		t.Errorf("Export.Kind = 0x%02x, want 0x00", e.Kind)
	}
}

func TestCode(t *testing.T) {
	c := Code{
		Locals: []Local{
			{Count: 1, Type: I32},
			{Count: 1, Type: I64},
		},
		Body: []byte{0x00, 0x0b},
	}

	if len(c.Locals) != 2 {
		t.Errorf("Code.Locals length = %d, want 2", len(c.Locals))
	}
	if len(c.Body) != 2 {
		t.Errorf("Code.Body length = %d, want 2", len(c.Body))
	}
}

func TestDataSegment(t *testing.T) {
	ds := DataSegment{
		Offset: []byte{0x41, 0x64}, // i32.const 100
		Data:   []byte("hello"),
	}

	if len(ds.Offset) != 2 {
		t.Errorf("DataSegment.Offset length = %d, want 2", len(ds.Offset))
	}
	if string(ds.Data) != "hello" {
		t.Errorf("DataSegment.Data = %s, want hello", string(ds.Data))
	}
}

func TestImport(t *testing.T) {
	imp := Import{
		Module: "env",
		Name:   "print",
		Kind:   0x00, // function
		Index:  0,
	}

	if imp.Module != "env" {
		t.Errorf("Import.Module = %s, want env", imp.Module)
	}
	if imp.Name != "print" {
		t.Errorf("Import.Name = %s, want print", imp.Name)
	}
}

func TestLocal(t *testing.T) {
	l := Local{
		Count: 5,
		Type:  I32,
	}

	if l.Count != 5 {
		t.Errorf("Local.Count = %d, want 5", l.Count)
	}
	if l.Type != I32 {
		t.Errorf("Local.Type = 0x%02x, want 0x7f", l.Type)
	}
}
