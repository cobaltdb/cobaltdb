// Package wasm provides WebAssembly compilation and execution for SQL queries.
// It compiles SQL query plans to WASM bytecode for high-performance execution.
package wasm

import (
	"bytes"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// Compiler compiles SQL queries to WASM bytecode
type Compiler struct {
	// Module holds the WASM module being built
	module *Module
	// Function counter for generating unique function indices
	funcIdx uint32
	// Global counter for generating unique global indices
	globalIdx uint32
	// Local counter for generating unique local indices within functions
	localIdx uint32
	// Type section for function signatures
	types []FuncType
	// Import section
	imports []Import
	// Function section (indices into type section)
	funcs []uint32
	// Memory section
	memory *Memory
	// Global section
	globals []Global
	// Export section
	exports []Export
	// Code section (function bodies)
	codes []Code
	// Data section
	data []DataSegment
	// Query cache for prepared statements
	queryCache map[string]*CompiledQuery
}

// CompiledQuery represents a compiled SQL query ready for execution
type CompiledQuery struct {
	// SQL query text
	SQL string
	// WASM bytecode
	Bytecode []byte
	// Function index for the query entry point
	EntryPoint uint32
	// Parameter count
	ParamCount int
	// Result schema
	ResultSchema []ColumnInfo
	// Memory layout information
	MemoryLayout MemoryLayout
}

// ColumnInfo describes a result column
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// MemoryLayout describes how data is laid out in WASM memory
type MemoryLayout struct {
	// Base address for parameter storage
	ParamBase int32
	// Base address for result storage
	ResultBase int32
	// Size of each result row
	RowSize int32
	// Maximum number of result rows
	MaxRows int32
}

// FuncType represents a WASM function signature
type FuncType struct {
	Params  []ValueType
	Results []ValueType
}

// ValueType represents WASM value types
const (
	I32 ValueType = 0x7f
	I64 ValueType = 0x7e
	F32 ValueType = 0x7d
	F64 ValueType = 0x7c
)

// ValueType is a WASM value type
type ValueType byte

// Import represents a WASM import
type Import struct {
	Module string
	Name   string
	Kind   byte // 0x00 = func, 0x01 = table, 0x02 = mem, 0x03 = global
	Index  uint32
}

// Memory represents WASM memory
type Memory struct {
	MinPages uint32
	MaxPages uint32
}

// Global represents a WASM global
type Global struct {
	Type  ValueType
	Mutable bool
	Init  []byte
}

// Export represents a WASM export
type Export struct {
	Name string
	Kind byte
	Index uint32
}

// Code represents a WASM function body
type Code struct {
	Locals []Local
	Body   []byte
}

// Local represents a local variable declaration
type Local struct {
	Count uint32
	Type  ValueType
}

// DataSegment represents a data segment
type DataSegment struct {
	Offset []byte
	Data   []byte
}

// Module represents a WASM module being built
type Module struct {
	magic   []byte
	version []byte
}

// NewCompiler creates a new WASM compiler
func NewCompiler() *Compiler {
	return &Compiler{
		module: &Module{
			magic:   []byte{0x00, 0x61, 0x73, 0x6d}, // \0asm
			version: []byte{0x01, 0x00, 0x00, 0x00}, // version 1
		},
		types:      make([]FuncType, 0),
		imports:    make([]Import, 0),
		funcs:      make([]uint32, 0),
		globals:    make([]Global, 0),
		exports:    make([]Export, 0),
		codes:      make([]Code, 0),
		data:       make([]DataSegment, 0),
		queryCache: make(map[string]*CompiledQuery),
		memory: &Memory{
			MinPages: 1,  // 64KB minimum
			MaxPages: 10, // 640KB maximum
		},
	}
}

// CompileQuery compiles a SQL query to WASM bytecode
func (c *Compiler) CompileQuery(sql string, stmt query.Statement, args []interface{}) (*CompiledQuery, error) {
	// Check cache
	if cached, ok := c.queryCache[sql]; ok {
		return cached, nil
	}

	// Reset module state for new compilation
	c.reset()

	// Add runtime imports
	c.addRuntimeImports()

	// Compile based on statement type
	var entryPoint uint32
	var err error

	switch s := stmt.(type) {
	case *query.SelectStmt:
		entryPoint, err = c.compileSelect(s)
	case *query.InsertStmt:
		entryPoint, err = c.compileInsert(s)
	case *query.UpdateStmt:
		entryPoint, err = c.compileUpdate(s)
	case *query.DeleteStmt:
		entryPoint, err = c.compileDelete(s)
	default:
		return nil, fmt.Errorf("unsupported statement type for WASM compilation: %T", stmt)
	}

	if err != nil {
		return nil, fmt.Errorf("compilation failed: %w", err)
	}

	// Generate the WASM module
	bytecode := c.generateModule()

	compiled := &CompiledQuery{
		SQL:          sql,
		Bytecode:     bytecode,
		EntryPoint:   entryPoint,
		ParamCount:   len(args),
		ResultSchema: c.inferResultSchema(stmt),
		MemoryLayout: MemoryLayout{
			ParamBase:  0,
			ResultBase: 1024,
			RowSize:    64,
			MaxRows:    1000,
		},
	}

	// Cache compiled query
	c.queryCache[sql] = compiled

	return compiled, nil
}

// reset resets the compiler state for a new compilation
func (c *Compiler) reset() {
	c.funcIdx = 0
	c.globalIdx = 0
	c.localIdx = 0
	c.types = c.types[:0]
	c.imports = c.imports[:0]
	c.funcs = c.funcs[:0]
	c.globals = c.globals[:0]
	c.exports = c.exports[:0]
	c.codes = c.codes[:0]
	c.data = c.data[:0]
}

// addRuntimeImports adds required runtime imports
func (c *Compiler) addRuntimeImports() {
	// Import table scan function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "tableScan",
		Kind:   0x00, // function
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32}, // tableId, outPtr, maxRows
		Results: []ValueType{I32},           // row count
	})

	// Import filter function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "filterRow",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32}, // rowData, predicatePtr
		Results: []ValueType{I32},      // boolean result
	})

	// Import comparison function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "compareValues",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32}, // val1, val2, op
		Results: []ValueType{I32},           // comparison result
	})

	// Update function index to account for imports
	c.funcIdx = uint32(len(c.imports))
}

// compileSelect compiles a SELECT statement
func (c *Compiler) compileSelect(stmt *query.SelectStmt) (uint32, error) {
	// Create function type for query entry point
	funcTypeIdx := uint32(len(c.types))
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32},    // params pointer
		Results: []ValueType{I32, I32}, // result pointer, row count
	})

	// Add to function section
	funcIdx := c.funcIdx
	c.funcs = append(c.funcs, funcTypeIdx)
	c.funcIdx++

	// Generate function body
	body := c.generateSelectBody(stmt)

	// Add code section
	c.codes = append(c.codes, Code{
		Locals: []Local{},
		Body:   body,
	})

	// Export the function
	c.exports = append(c.exports, Export{
		Name:  "query",
		Kind:  0x00, // function
		Index: funcIdx,
	})

	return funcIdx, nil
}

// compileInsert compiles an INSERT statement
func (c *Compiler) compileInsert(stmt *query.InsertStmt) (uint32, error) {
	funcTypeIdx := uint32(len(c.types))
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32}, // params pointer
		Results: []ValueType{I64}, // rows affected
	})

	funcIdx := c.funcIdx
	c.funcs = append(c.funcs, funcTypeIdx)
	c.funcIdx++

	body := c.generateInsertBody(stmt)

	c.codes = append(c.codes, Code{
		Locals: []Local{},
		Body:   body,
	})

	c.exports = append(c.exports, Export{
		Name:  "query",
		Kind:  0x00,
		Index: funcIdx,
	})

	return funcIdx, nil
}

// compileUpdate compiles an UPDATE statement
func (c *Compiler) compileUpdate(stmt *query.UpdateStmt) (uint32, error) {
	funcTypeIdx := uint32(len(c.types))
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32}, // params pointer
		Results: []ValueType{I64}, // rows affected
	})

	funcIdx := c.funcIdx
	c.funcs = append(c.funcs, funcTypeIdx)
	c.funcIdx++

	body := c.generateUpdateBody(stmt)

	c.codes = append(c.codes, Code{
		Locals: []Local{},
		Body:   body,
	})

	c.exports = append(c.exports, Export{
		Name:  "query",
		Kind:  0x00,
		Index: funcIdx,
	})

	return funcIdx, nil
}

// compileDelete compiles a DELETE statement
func (c *Compiler) compileDelete(stmt *query.DeleteStmt) (uint32, error) {
	funcTypeIdx := uint32(len(c.types))
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32}, // params pointer
		Results: []ValueType{I64}, // rows affected
	})

	funcIdx := c.funcIdx
	c.funcs = append(c.funcs, funcTypeIdx)
	c.funcIdx++

	body := c.generateDeleteBody(stmt)

	c.codes = append(c.codes, Code{
		Locals: []Local{},
		Body:   body,
	})

	c.exports = append(c.exports, Export{
		Name:  "query",
		Kind:  0x00,
		Index: funcIdx,
	})

	return funcIdx, nil
}

// generateSelectBody generates WASM bytecode for a SELECT statement
func (c *Compiler) generateSelectBody(stmt *query.SelectStmt) []byte {
	buf := new(bytes.Buffer)

	// Local variables
	// $rowCount = 0
	buf.WriteByte(0x41) // i32.const
	buf.WriteByte(0x00) // 0
	buf.WriteByte(0x21) // local.set
	buf.WriteByte(0x00) // local index 0 (rowCount)

	// $resultPtr = 1024 (result buffer base)
	buf.WriteByte(0x41) // i32.const
	writeLeb128(buf, 1024)
	buf.WriteByte(0x21) // local.set
	buf.WriteByte(0x01) // local index 1 (resultPtr)

	// Call table scan (simplified - just return empty result for now)
	// In real implementation, this would iterate over table rows
	buf.WriteByte(0x41) // i32.const
	buf.WriteByte(0x00) // table id 0
	buf.WriteByte(0x20) // local.get
	buf.WriteByte(0x01) // resultPtr
	buf.WriteByte(0x41) // i32.const
	writeLeb128(buf, 1000) // max rows
	buf.WriteByte(0x10) // call
	writeLeb128(buf, 0) // function index 0 (tableScan import)

	// Store row count
	buf.WriteByte(0x21) // local.set
	buf.WriteByte(0x00) // rowCount

	// Return (resultPtr, rowCount)
	buf.WriteByte(0x20) // local.get
	buf.WriteByte(0x01) // resultPtr
	buf.WriteByte(0x20) // local.get
	buf.WriteByte(0x00) // rowCount

	// End function
	buf.WriteByte(0x0b) // end

	return buf.Bytes()
}

// generateInsertBody generates WASM bytecode for an INSERT statement
func (c *Compiler) generateInsertBody(stmt *query.InsertStmt) []byte {
	buf := new(bytes.Buffer)

	// For now, return 0 rows affected (placeholder)
	// Real implementation would iterate over values and insert rows
	buf.WriteByte(0x42) // i64.const
	buf.WriteByte(0x00) // 0

	buf.WriteByte(0x0b) // end

	return buf.Bytes()
}

// generateUpdateBody generates WASM bytecode for an UPDATE statement
func (c *Compiler) generateUpdateBody(stmt *query.UpdateStmt) []byte {
	buf := new(bytes.Buffer)

	// Placeholder - return 0 rows affected
	buf.WriteByte(0x42) // i64.const
	buf.WriteByte(0x00) // 0

	buf.WriteByte(0x0b) // end

	return buf.Bytes()
}

// generateDeleteBody generates WASM bytecode for a DELETE statement
func (c *Compiler) generateDeleteBody(stmt *query.DeleteStmt) []byte {
	buf := new(bytes.Buffer)

	// Placeholder - return 0 rows affected
	buf.WriteByte(0x42) // i64.const
	buf.WriteByte(0x00) // 0

	buf.WriteByte(0x0b) // end

	return buf.Bytes()
}

// generateModule generates the complete WASM module
func (c *Compiler) generateModule() []byte {
	buf := new(bytes.Buffer)

	// Magic number
	buf.Write(c.module.magic)

	// Version
	buf.Write(c.module.version)

	// Type section (section id = 1)
	if len(c.types) > 0 {
		buf.WriteByte(0x01)
		section := c.encodeTypeSection()
		writeLeb128(buf, uint64(len(section)))
		buf.Write(section)
	}

	// Import section (section id = 2)
	if len(c.imports) > 0 {
		buf.WriteByte(0x02)
		section := c.encodeImportSection()
		writeLeb128(buf, uint64(len(section)))
		buf.Write(section)
	}

	// Function section (section id = 3)
	if len(c.funcs) > 0 {
		buf.WriteByte(0x03)
		section := c.encodeFunctionSection()
		writeLeb128(buf, uint64(len(section)))
		buf.Write(section)
	}

	// Memory section (section id = 5)
	if c.memory != nil {
		buf.WriteByte(0x05)
		section := c.encodeMemorySection()
		writeLeb128(buf, uint64(len(section)))
		buf.Write(section)
	}

	// Global section (section id = 6)
	if len(c.globals) > 0 {
		buf.WriteByte(0x06)
		section := c.encodeGlobalSection()
		writeLeb128(buf, uint64(len(section)))
		buf.Write(section)
	}

	// Export section (section id = 7)
	if len(c.exports) > 0 {
		buf.WriteByte(0x07)
		section := c.encodeExportSection()
		writeLeb128(buf, uint64(len(section)))
		buf.Write(section)
	}

	// Code section (section id = 10)
	if len(c.codes) > 0 {
		buf.WriteByte(0x0a)
		section := c.encodeCodeSection()
		writeLeb128(buf, uint64(len(section)))
		buf.Write(section)
	}

	// Data section (section id = 11)
	if len(c.data) > 0 {
		buf.WriteByte(0x0b)
		section := c.encodeDataSection()
		writeLeb128(buf, uint64(len(section)))
		buf.Write(section)
	}

	return buf.Bytes()
}

// encodeTypeSection encodes the type section
func (c *Compiler) encodeTypeSection() []byte {
	buf := new(bytes.Buffer)
	writeLeb128(buf, uint64(len(c.types)))
	for _, ft := range c.types {
		buf.WriteByte(0x60) // func type
		writeLeb128(buf, uint64(len(ft.Params)))
		for _, p := range ft.Params {
			buf.WriteByte(byte(p))
		}
		writeLeb128(buf, uint64(len(ft.Results)))
		for _, r := range ft.Results {
			buf.WriteByte(byte(r))
		}
	}
	return buf.Bytes()
}

// encodeImportSection encodes the import section
func (c *Compiler) encodeImportSection() []byte {
	buf := new(bytes.Buffer)
	writeLeb128(buf, uint64(len(c.imports)))
	for _, imp := range c.imports {
		writeLeb128(buf, uint64(len(imp.Module)))
		buf.WriteString(imp.Module)
		writeLeb128(buf, uint64(len(imp.Name)))
		buf.WriteString(imp.Name)
		buf.WriteByte(imp.Kind)
		writeLeb128(buf, uint64(imp.Index))
	}
	return buf.Bytes()
}

// encodeFunctionSection encodes the function section
func (c *Compiler) encodeFunctionSection() []byte {
	buf := new(bytes.Buffer)
	writeLeb128(buf, uint64(len(c.funcs)))
	for _, f := range c.funcs {
		writeLeb128(buf, uint64(f))
	}
	return buf.Bytes()
}

// encodeMemorySection encodes the memory section
func (c *Compiler) encodeMemorySection() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(0x01) // 1 memory
	if c.memory.MaxPages > 0 {
		buf.WriteByte(0x01) // has max
	} else {
		buf.WriteByte(0x00) // no max
	}
	writeLeb128(buf, uint64(c.memory.MinPages))
	if c.memory.MaxPages > 0 {
		writeLeb128(buf, uint64(c.memory.MaxPages))
	}
	return buf.Bytes()
}

// encodeGlobalSection encodes the global section
func (c *Compiler) encodeGlobalSection() []byte {
	buf := new(bytes.Buffer)
	writeLeb128(buf, uint64(len(c.globals)))
	for _, g := range c.globals {
		buf.WriteByte(byte(g.Type))
		if g.Mutable {
			buf.WriteByte(0x01)
		} else {
			buf.WriteByte(0x00)
		}
		buf.Write(g.Init)
	}
	return buf.Bytes()
}

// encodeExportSection encodes the export section
func (c *Compiler) encodeExportSection() []byte {
	buf := new(bytes.Buffer)
	writeLeb128(buf, uint64(len(c.exports)))
	for _, e := range c.exports {
		writeLeb128(buf, uint64(len(e.Name)))
		buf.WriteString(e.Name)
		buf.WriteByte(e.Kind)
		writeLeb128(buf, uint64(e.Index))
	}
	return buf.Bytes()
}

// encodeCodeSection encodes the code section
func (c *Compiler) encodeCodeSection() []byte {
	buf := new(bytes.Buffer)
	writeLeb128(buf, uint64(len(c.codes)))
	for _, code := range c.codes {
		funcBuf := new(bytes.Buffer)
		// Local declarations
		writeLeb128(funcBuf, uint64(len(code.Locals)))
		for _, local := range code.Locals {
			writeLeb128(funcBuf, uint64(local.Count))
			funcBuf.WriteByte(byte(local.Type))
		}
		// Function body
		funcBuf.Write(code.Body)

		writeLeb128(buf, uint64(funcBuf.Len()))
		buf.Write(funcBuf.Bytes())
	}
	return buf.Bytes()
}

// encodeDataSection encodes the data section
func (c *Compiler) encodeDataSection() []byte {
	buf := new(bytes.Buffer)
	writeLeb128(buf, uint64(len(c.data)))
	for _, d := range c.data {
		buf.WriteByte(0x00) // memory index 0
		buf.Write(d.Offset)
		writeLeb128(buf, uint64(len(d.Data)))
		buf.Write(d.Data)
	}
	return buf.Bytes()
}

// inferResultSchema infers the result schema from a statement
func (c *Compiler) inferResultSchema(stmt query.Statement) []ColumnInfo {
	switch s := stmt.(type) {
	case *query.SelectStmt:
		schema := make([]ColumnInfo, 0, len(s.Columns))
		for _, col := range s.Columns {
			// Simplified - just use expression string as name
			name := fmt.Sprintf("%v", col)
			schema = append(schema, ColumnInfo{
				Name:     name,
				Type:     "TEXT",
				Nullable: true,
			})
		}
		return schema
	default:
		return nil
	}
}

