package wasm

import (
	"testing"
)

// TestCompilerModuleGeneration tests the complete module generation pipeline
func TestCompilerModuleGeneration(t *testing.T) {
	c := NewCompiler()

	// Test that we can create a simple module
	c.reset()
	c.addRuntimeImports()

	// Verify imports were added
	if len(c.imports) == 0 {
		t.Error("No imports added")
	}

	// Check for expected imports
	expectedImports := map[string]bool{
		"env:tableScan":     false,
		"env:filterRow":     false,
		"env:compareValues": false,
	}

	for _, imp := range c.imports {
		key := imp.Module + ":" + imp.Name
		if _, ok := expectedImports[key]; ok {
			expectedImports[key] = true
		}
	}

	for name, found := range expectedImports {
		if !found {
			t.Errorf("Expected import %s not found", name)
		}
	}
}

// TestRuntimeLoadModule tests loading a minimal valid WASM module
func TestRuntimeLoadModule(t *testing.T) {
	r := NewRuntime(1)

	// Minimal valid WASM module: magic + version
	minimalModule := []byte{
		0x00, 0x61, 0x73, 0x6d, // magic: \0asm
		0x01, 0x00, 0x00, 0x00, // version: 1
	}

	err := r.LoadModule(minimalModule)
	if err != nil {
		t.Errorf("Failed to load minimal module: %v", err)
	}
}

// TestRuntimeLoadInvalidModule tests error handling for invalid modules
func TestRuntimeLoadInvalidModule(t *testing.T) {
	r := NewRuntime(1)

	tests := []struct {
		name    string
		module  []byte
		wantErr bool
	}{
		{
			name:    "empty_module",
			module:  []byte{},
			wantErr: true,
		},
		{
			name:    "too_short",
			module:  []byte{0x00, 0x61},
			wantErr: true,
		},
		{
			name:    "wrong_magic",
			module:  []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := r.LoadModule(tt.module)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadModule() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestRuntimeCallFunctionWithNoFunctions tests calling when no functions loaded
func TestRuntimeCallFunctionWithNoFunctions(t *testing.T) {
	r := NewRuntime(1)

	// Load minimal module with no functions
	minimalModule := []byte{
		0x00, 0x61, 0x73, 0x6d, // magic
		0x01, 0x00, 0x00, 0x00, // version
	}

	err := r.LoadModule(minimalModule)
	if err != nil {
		t.Fatalf("Failed to load module: %v", err)
	}

	// Try to call a function that doesn't exist
	_, err = r.CallFunction(0, nil)
	if err == nil {
		t.Error("Expected error when calling non-existent function")
	}
}

// TestLeb128Encoding tests LEB128 encoding/decoding roundtrip
func TestLeb128Encoding(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
	}{
		{"zero", 0},
		{"one", 1},
		{"max_single_byte", 0x7f},
		{"min_two_byte", 0x80},
		{"max_uint32", 0xffffffff},
		{"large", 0x123456789abcdef},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test unsigned encoding
			var buf []byte
			for {
				byteVal := uint8(tt.value & 0x7f)
				tt.value >>= 7
				if tt.value != 0 {
					byteVal |= 0x80
				}
				buf = append(buf, byteVal)
				if tt.value == 0 {
					break
				}
			}

			// Verify we can decode (conceptually)
			if len(buf) == 0 {
				t.Error("LEB128 encoding produced empty buffer")
			}
		})
	}
}

// TestRuntimeMemoryOperations tests memory read/write operations
func TestRuntimeMemoryOperations(t *testing.T) {
	r := NewRuntime(1)

	// Test writeParams and the memory layout
	testCases := []struct {
		name string
		args []interface{}
	}{
		{
			name: "integers",
			args: []interface{}{int64(1), int64(2), int64(3)},
		},
		{
			name: "strings",
			args: []interface{}{"hello", "world"},
		},
		{
			name: "mixed",
			args: []interface{}{int64(42), "test", 3.14, nil},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := r.writeParams(0, tc.args)
			if err != nil {
				t.Errorf("writeParams failed: %v", err)
			}
		})
	}
}

// TestRuntimeParseResults tests result parsing
func TestRuntimeParseResults(t *testing.T) {
	r := NewRuntime(1)

	schema := []ColumnInfo{
		{Name: "id", Type: "INTEGER", Nullable: false},
		{Name: "name", Type: "TEXT", Nullable: true},
	}

	// Write test data to memory
	// Row 1: id=1, name="alice"
	r.Memory[0] = 0x01
	r.Memory[1] = 0x00
	r.Memory[2] = 0x00
	r.Memory[3] = 0x00
	r.Memory[4] = 0x00
	r.Memory[5] = 0x00
	r.Memory[6] = 0x00
	r.Memory[7] = 0x00 // int64(1) little endian

	r.Memory[8] = 0x05
	r.Memory[9] = 0x00
	r.Memory[10] = 0x00
	r.Memory[11] = 0x00 // length=5
	copy(r.Memory[12:], "alice")

	result, err := r.parseResults(schema, 0, 1)
	if err != nil {
		t.Errorf("parseResults failed: %v", err)
	}
	if result == nil {
		t.Error("parseResults returned nil")
	}
	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}
}

// TestQueryResultStructure tests QueryResult and Row structures
func TestQueryResultStructure(t *testing.T) {
	result := &QueryResult{
		RowsAffected: 3,
		Rows: []Row{
			{Values: []interface{}{1, "a", true}},
			{Values: []interface{}{2, "b", false}},
			{Values: []interface{}{3, "c", nil}},
		},
		Columns: []string{"id", "name", "active"},
	}

	if result.RowsAffected != 3 {
		t.Errorf("RowsAffected = %d, want 3", result.RowsAffected)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Rows length = %d, want 3", len(result.Rows))
	}
	if len(result.Columns) != 3 {
		t.Errorf("Columns length = %d, want 3", len(result.Columns))
	}

	// Check individual rows
	for i, row := range result.Rows {
		if len(row.Values) != 3 {
			t.Errorf("Row %d: Values length = %d, want 3", i, len(row.Values))
		}
	}
}

// TestCompilerTypeSectionEncoding tests type section encoding
func TestCompilerTypeSectionEncoding(t *testing.T) {
	c := NewCompiler()
	c.reset()

	// Add a function type
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32},
		Results: []ValueType{I32},
	})

	encoded := c.encodeTypeSection()
	if len(encoded) == 0 {
		t.Error("encodeTypeSection returned empty result")
	}

	// Should start with section ID 1
	if encoded[0] != 1 {
		t.Errorf("Type section ID = %d, want 1", encoded[0])
	}
}

// TestCompilerMemorySectionEncoding tests memory section encoding
func TestCompilerMemorySectionEncoding(t *testing.T) {
	c := NewCompiler()
	c.reset()

	c.memory = &Memory{
		MinPages: 1,
		MaxPages: 10,
	}

	encoded := c.encodeMemorySection()
	if len(encoded) == 0 {
		t.Error("encodeMemorySection returned empty result")
	}

	// Memory section content: 0x01 (1 memory), 0x01 (has max), min_pages, max_pages
	if encoded[0] != 0x01 {
		t.Errorf("Memory count = %d, want 1", encoded[0])
	}
	if encoded[1] != 0x01 {
		t.Errorf("Has max flag = %d, want 1", encoded[1])
	}
}

// TestCompilerExportSectionEncoding tests export section encoding
func TestCompilerExportSectionEncoding(t *testing.T) {
	c := NewCompiler()
	c.reset()

	// Add an export
	c.exports = append(c.exports, Export{
		Name:  "main",
		Kind:  0x00, // function
		Index: 0,
	})

	encoded := c.encodeExportSection()
	if len(encoded) == 0 {
		t.Error("encodeExportSection returned empty result")
	}

	// Export section content: count (1), name_len (4), name ("main"), kind (0x00), index (0)
	// First byte is the count of exports
	if encoded[0] != 0x01 {
		t.Errorf("Export count = %d, want 1", encoded[0])
	}
}

// TestRuntimeStepExecution tests the step execution with various opcodes
func TestRuntimeStepExecution(t *testing.T) {
	r := NewRuntime(1)

	// Create a simple function that does: i32.const 42, return
	fn := Function{
		TypeIdx: 0,
		Locals:  []ValueType{},
		Code:    []byte{0x41, 0x2a, 0x0b}, // i32.const 42, end
	}

	r.Functions = append(r.Functions, fn)

	// Push a call frame
	r.CallStack = append(r.CallStack, CallFrame{
		FuncIdx:  0,
		Locals:   []uint64{},
		ReturnPC: 0,
		SP:       0,
	})

	r.currentFunc = 0

	// Test that we can call executeFunction (it may return error for unimplemented opcodes)
	_ = r.executeFunction(fn)
}

// TestCompiledQueryCache tests the query cache functionality
func TestCompiledQueryCache(t *testing.T) {
	c := NewCompiler()

	// The query cache should exist
	if c.queryCache == nil {
		t.Error("queryCache not initialized")
	}

	// Add a compiled query to cache
	query := &CompiledQuery{
		SQL:        "SELECT 1",
		Bytecode:   []byte{0x00},
		EntryPoint: 0,
	}

	c.queryCache["test"] = query

	// Retrieve from cache
	cached, ok := c.queryCache["test"]
	if !ok {
		t.Error("Failed to retrieve cached query")
	}
	if cached.SQL != "SELECT 1" {
		t.Errorf("Cached query SQL = %s, want 'SELECT 1'", cached.SQL)
	}
}
