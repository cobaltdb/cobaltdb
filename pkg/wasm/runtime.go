package wasm

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Runtime executes compiled WASM queries
type Runtime struct {
	// Memory is the WASM linear memory (64KB pages)
	Memory []byte
	// Globals store global variables
	Globals []uint64
	// Stack for execution
	Stack []uint64
	// Call stack for function calls
	CallStack []CallFrame
	// Functions table
	Functions []Function
	// Types from type section
	Types []FuncType
	// Function type indices from function section
	funcTypeIndices []uint32
	// Imported functions
	Imports map[string]ImportFunc
	// Import names by function index (for lookup in callImport)
	importNames []string
	// Current function index being executed
	currentFunc int
}

// CallFrame represents a function call frame
type CallFrame struct {
	FuncIdx    int
	Locals     []uint64
	ReturnPC   int
	SP         int // Stack pointer
}

// Function represents a WASM function
type Function struct {
	TypeIdx    int
	Locals     []ValueType
	Code       []byte
	IsImport   bool // true if this is an imported function
	ParamCount int  // Number of parameters (for call instruction)
}

// ImportFunc is a host function that can be called from WASM
type ImportFunc func(rt *Runtime, params []uint64) ([]uint64, error)

// NewRuntime creates a new WASM runtime with the given memory size
func NewRuntime(memoryPages int) *Runtime {
	return &Runtime{
		Memory:    make([]byte, memoryPages*64*1024),
		Globals:   make([]uint64, 0),
		Stack:     make([]uint64, 0, 1024),
		CallStack: make([]CallFrame, 0, 64),
		Functions: make([]Function, 0),
		Imports:   make(map[string]ImportFunc),
	}
}

// RegisterImport registers a host function
func (rt *Runtime) RegisterImport(module, name string, fn ImportFunc) {
	rt.Imports[module+":"+name] = fn
}

// Execute executes a compiled query
func (rt *Runtime) Execute(compiled *CompiledQuery, args []interface{}) (*QueryResult, error) {
	// Load the WASM module
	if err := rt.LoadModule(compiled.Bytecode); err != nil {
		return nil, fmt.Errorf("failed to load module: %w", err)
	}

	// Setup parameters in memory
	paramPtr := int32(0)
	if err := rt.writeParams(paramPtr, args); err != nil {
		return nil, fmt.Errorf("failed to write params: %w", err)
	}

	// Call the entry point function
	result, err := rt.CallFunction(int(compiled.EntryPoint), []uint64{uint64(paramPtr)})
	if err != nil {
		return nil, fmt.Errorf("execution failed: %w", err)
	}

	// Parse results
	if compiled.ResultSchema == nil {
		// Non-SELECT query - result is rows affected
		if len(result) > 0 {
			return &QueryResult{
				RowsAffected: int64(result[0]),
			}, nil
		}
		return &QueryResult{}, nil
	}

	// SELECT query - parse result buffer
	if len(result) < 2 {
		return &QueryResult{}, nil
	}
	resultPtr := int32(result[0])
	rowCount := int32(result[1])

	if rowCount < 0 || rowCount > 100000 {
		return nil, fmt.Errorf("invalid row count: %d", rowCount)
	}

	return rt.parseResults(compiled.ResultSchema, resultPtr, rowCount)
}

// ExecuteStreaming executes a compiled query with streaming results
// Returns a StreamingResult that can be used to fetch rows incrementally
func (rt *Runtime) ExecuteStreaming(compiled *CompiledQuery, args []interface{}, chunkSize int) (*StreamingResult, error) {
	// Load the WASM module
	if err := rt.LoadModule(compiled.Bytecode); err != nil {
		return nil, fmt.Errorf("failed to load module: %w", err)
	}

	// Setup parameters in memory
	paramPtr := int32(0)
	if err := rt.writeParams(paramPtr, args); err != nil {
		return nil, fmt.Errorf("failed to write params: %w", err)
	}

	// Call the entry point function
	result, err := rt.CallFunction(int(compiled.EntryPoint), []uint64{uint64(paramPtr)})
	if err != nil {
		return nil, fmt.Errorf("execution failed: %w", err)
	}

	// Parse results
	if compiled.ResultSchema == nil {
		return nil, fmt.Errorf("streaming not supported for non-SELECT queries")
	}

	if len(result) < 2 {
		return &StreamingResult{
			Runtime:   rt,
			Compiled:  compiled,
			ChunkSize: chunkSize,
			TotalRows: 0,
			HasMore:   false,
			Columns:   []string{},
		}, nil
	}

	resultPtr := int32(result[0])
	rowCount := int(result[1])

	if rowCount < 0 || rowCount > 1000000 {
		return nil, fmt.Errorf("invalid row count: %d", rowCount)
	}

	// Extract column names
	columns := make([]string, len(compiled.ResultSchema))
	for i, col := range compiled.ResultSchema {
		columns[i] = col.Name
	}

	return &StreamingResult{
		Runtime:   rt,
		Compiled:  compiled,
		ChunkSize: chunkSize,
		TotalRows: int(rowCount),
		HasMore:   rowCount > 0,
		Columns:   columns,
		resultPtr: resultPtr,
	}, nil
}

// Next fetches the next chunk of results
func (sr *StreamingResult) Next() ([]Row, error) {
	if !sr.HasMore {
		return nil, nil
	}

	// Calculate start and end row for this chunk
	startRow := sr.ChunkIndex * sr.ChunkSize
	endRow := startRow + sr.ChunkSize
	if endRow > sr.TotalRows {
		endRow = sr.TotalRows
	}

	// For simplified implementation, return rows from memory
	schema := sr.Compiled.ResultSchema

	rows := make([]Row, 0, endRow-startRow)
	rowSize := len(schema) * 8 // Simplified: each column is 8 bytes

	for i := startRow; i < endRow; i++ {
		rowOffset := sr.resultPtr + int32(i*rowSize)
		row := Row{Values: make([]interface{}, len(schema))}
		for j := range schema {
			valOffset := rowOffset + int32(j*8)
			if valOffset+8 <= int32(len(sr.Runtime.Memory)) {
				val := binary.LittleEndian.Uint64(sr.Runtime.Memory[valOffset:])
				row.Values[j] = int64(val)
			}
		}
		rows = append(rows, row)
	}

	sr.ChunkIndex++
	sr.HasMore = endRow < sr.TotalRows

	return rows, nil
}

// Close releases streaming result resources
func (sr *StreamingResult) Close() {
	sr.Runtime = nil
	sr.Compiled = nil
	sr.CurrentChunk = nil
}

// QueryResult holds query execution results
type QueryResult struct {
	RowsAffected int64
	Rows         []Row
	Columns      []string
}

// Row represents a result row
type Row struct {
	Values []interface{}
}

// StreamingResult allows iterating over large result sets incrementally
type StreamingResult struct {
	Runtime      *Runtime
	Compiled     *CompiledQuery
	ChunkIndex   int
	ChunkSize    int
	TotalRows    int
	CurrentChunk []Row
	Columns      []string
	Position     int
	HasMore      bool
	resultPtr    int32
}

// writeParams writes query parameters to WASM memory
func (rt *Runtime) writeParams(ptr int32, args []interface{}) error {
	offset := ptr
	for _, arg := range args {
		switch v := arg.(type) {
		case int:
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(v))
			offset += 8
		case int64:
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(v))
			offset += 8
		case float64:
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(v))
			offset += 8
		case string:
			// Write length then data
			binary.LittleEndian.PutUint32(rt.Memory[offset:], uint32(len(v)))
			offset += 4
			copy(rt.Memory[offset:], v)
			offset += int32(len(v))
		case nil:
			// Null marker: 0xFFFFFFFF
			binary.LittleEndian.PutUint32(rt.Memory[offset:], 0xFFFFFFFF)
			offset += 4
		default:
			return fmt.Errorf("unsupported parameter type: %T", arg)
		}
	}
	return nil
}

// parseResults parses query results from WASM memory
func (rt *Runtime) parseResults(schema []ColumnInfo, ptr int32, rowCount int32) (*QueryResult, error) {
	result := &QueryResult{
		Columns: make([]string, len(schema)),
		Rows:    make([]Row, rowCount),
	}

	for i, col := range schema {
		result.Columns[i] = col.Name
	}

	offset := ptr
	for i := int32(0); i < rowCount; i++ {
		row := Row{
			Values: make([]interface{}, len(schema)),
		}

		for j, col := range schema {
			switch col.Type {
			case "INTEGER":
				val := binary.LittleEndian.Uint64(rt.Memory[offset:])
				row.Values[j] = int64(val)
				offset += 8
			case "REAL":
				bits := binary.LittleEndian.Uint64(rt.Memory[offset:])
				row.Values[j] = bits
				offset += 8
			case "TEXT":
				length := binary.LittleEndian.Uint32(rt.Memory[offset:])
				offset += 4
				if length == 0xFFFFFFFF {
					row.Values[j] = nil
				} else {
					row.Values[j] = string(rt.Memory[offset : offset+int32(length)])
					offset += int32(length)
				}
			default:
				length := binary.LittleEndian.Uint32(rt.Memory[offset:])
				offset += 4
				if length == 0xFFFFFFFF {
					row.Values[j] = nil
				} else {
					row.Values[j] = string(rt.Memory[offset : offset+int32(length)])
					offset += int32(length)
				}
			}
		}

		result.Rows[i] = row
	}

	return result, nil
}

// LoadModule loads a WASM module into the runtime
func (rt *Runtime) LoadModule(bytecode []byte) error {
	// Parse WASM module
	if len(bytecode) < 8 {
		return fmt.Errorf("invalid WASM module: too short")
	}

	// Check magic number
	if !bytes.Equal(bytecode[0:4], []byte{0x00, 0x61, 0x73, 0x6d}) {
		return fmt.Errorf("invalid WASM magic number")
	}

	// Check version
	if !bytes.Equal(bytecode[4:8], []byte{0x01, 0x00, 0x00, 0x00}) {
		return fmt.Errorf("unsupported WASM version")
	}

	// Parse sections
	offset := 8
	for offset < len(bytecode) {
		sectionID := bytecode[offset]
		offset++

		sectionSize, n := readLeb128(bytecode, offset)
		offset += n

		sectionEnd := offset + int(sectionSize)
		if sectionEnd > len(bytecode) {
			return fmt.Errorf("section extends past end of module")
		}

		sectionData := bytecode[offset:sectionEnd]

		switch sectionID {
		case 0x01: // Type section
			if err := rt.parseTypeSection(sectionData); err != nil {
				return err
			}
		case 0x02: // Import section
			if err := rt.parseImportSection(sectionData); err != nil {
				return err
			}
		case 0x03: // Function section
			// Parse function section - will be used by code section
			if err := rt.parseFunctionSection(sectionData); err != nil {
				return err
			}
		case 0x07: // Export section
			// Parse export section
		case 0x0a: // Code section
			if err := rt.parseCodeSection(sectionData); err != nil {
				return err
			}
		case 0x05: // Memory section
			if err := rt.parseMemorySection(sectionData); err != nil {
				return err
			}
		}

		offset = sectionEnd
	}

	return nil
}


// callImport calls a registered import function
func (rt *Runtime) callImport(funcIdx int, params []uint64) ([]uint64, error) {
	// Get import name based on function index
	if funcIdx >= len(rt.importNames) {
		return nil, fmt.Errorf("no import name for function index %d", funcIdx)
	}
	importName := rt.importNames[funcIdx]
	fn, ok := rt.Imports[importName]
	if !ok {
		return nil, fmt.Errorf("no import handler registered for %s (function %d)", importName, funcIdx)
	}
	return fn(rt, params)
}

// parseTypeSection parses the type section
func (rt *Runtime) parseTypeSection(data []byte) error {
	count, n := readLeb128(data, 0)
	offset := n

	rt.Types = make([]FuncType, 0, count)

	for i := uint64(0); i < count; i++ {
		if offset >= len(data) {
			return fmt.Errorf("type section truncated")
		}
		// Read form (must be 0x60 for func)
		form := data[offset]
		offset++
		if form != 0x60 {
			return fmt.Errorf("invalid type form: 0x%02x", form)
		}

		// Read param count
		paramCount, n := readLeb128(data, offset)
		offset += n

		// Read params
		params := make([]ValueType, paramCount)
		for j := uint64(0); j < paramCount; j++ {
			if offset >= len(data) {
				return fmt.Errorf("type section truncated reading params")
			}
			params[j] = ValueType(data[offset])
			offset++
		}

		// Read result count
		resultCount, n := readLeb128(data, offset)
		offset += n

		// Read results
		results := make([]ValueType, resultCount)
		for j := uint64(0); j < resultCount; j++ {
			if offset >= len(data) {
				return fmt.Errorf("type section truncated reading results")
			}
			results[j] = ValueType(data[offset])
			offset++
		}

		rt.Types = append(rt.Types, FuncType{
			Params:  params,
			Results: results,
		})
	}

	return nil
}

// parseFunctionSection parses the function section
func (rt *Runtime) parseFunctionSection(data []byte) error {
	count, n := readLeb128(data, 0)
	offset := n

	rt.funcTypeIndices = make([]uint32, 0, count)

	for i := uint64(0); i < count; i++ {
		if offset >= len(data) {
			return fmt.Errorf("function section truncated")
		}
		typeIdx, n := readLeb128(data, offset)
		offset += n
		rt.funcTypeIndices = append(rt.funcTypeIndices, uint32(typeIdx))
	}

	return nil
}

// parseImportSection parses the import section
func (rt *Runtime) parseImportSection(data []byte) error {
	count, n := readLeb128(data, 0)
	_ = count
	offset := n

	for i := uint64(0); i < count; i++ {
		// Read module name
		modLen, n := readLeb128(data, offset)
		offset += n
		module := string(data[offset : offset+int(modLen)])
		offset += int(modLen)

		// Read field name
		fieldLen, n := readLeb128(data, offset)
		offset += n
		field := string(data[offset : offset+int(fieldLen)])
		offset += int(fieldLen)

		// Read import kind
		kind := data[offset]
		offset++

		// Read index
		idx, n := readLeb128(data, offset)
		_ = idx
		offset += n

		// Register import
		importKey := module + ":" + field
		switch kind {
		case 0x00: // Function
			// Get param count from type
			paramCount := 0
			if int(idx) < len(rt.Types) {
				paramCount = len(rt.Types[int(idx)].Params)
			}
			// Track import name by function index
			funcIdx := len(rt.Functions)
			rt.importNames = append(rt.importNames, importKey)
			// Create import function stub (no code - will be handled by callImport)
			rt.Functions = append(rt.Functions, Function{
				TypeIdx:    int(idx),
				Locals:     nil,
				Code:       nil, // Import functions have no code
				IsImport:   true,
				ParamCount: paramCount,
			})
			// Store the import name at the function index
			for len(rt.importNames) <= funcIdx {
				rt.importNames = append(rt.importNames, "")
			}
			rt.importNames[funcIdx] = importKey
		default:
			return fmt.Errorf("unsupported import kind: %d for %s", kind, importKey)
		}
	}

	return nil
}

// parseMemorySection parses the memory section
func (rt *Runtime) parseMemorySection(data []byte) error {
	count, n := readLeb128(data, 0)
	if count != 1 {
		return fmt.Errorf("expected exactly 1 memory, got %d", count)
	}

	offset := n
	flags := data[offset]
	offset++

	min, n := readLeb128(data, offset)
	offset += n

	var max uint64
	if flags&0x01 != 0 {
		max, n = readLeb128(data, offset)
		_ = max
	}

	// Resize memory if needed
	requiredSize := int(min) * 64 * 1024
	if len(rt.Memory) < requiredSize {
		rt.Memory = make([]byte, requiredSize)
	}

	return nil
}

// parseCodeSection parses the code section
func (rt *Runtime) parseCodeSection(data []byte) error {
	count, n := readLeb128(data, 0)
	offset := n

	for i := uint64(0); i < count; i++ {
		funcSize, n := readLeb128(data, offset)
		offset += n

		funcEnd := offset + int(funcSize)
		funcData := data[offset:funcEnd]

		// Parse local declarations
		localCount, n := readLeb128(funcData, 0)
		offset2 := n

		locals := make([]ValueType, 0)
		for j := uint64(0); j < localCount; j++ {
			cnt, n := readLeb128(funcData, offset2)
			offset2 += n
			typ := funcData[offset2]
			offset2++
			for k := uint64(0); k < cnt; k++ {
				locals = append(locals, ValueType(typ))
			}
		}

		// Rest is function body
		body := funcData[offset2:]

		// Get type index from function section
		typeIdx := 0
		if int(i) < len(rt.funcTypeIndices) {
			typeIdx = int(rt.funcTypeIndices[i])
		}

		// Get param count from type
		paramCount := 0
		if typeIdx < len(rt.Types) {
			paramCount = len(rt.Types[typeIdx].Params)
		}

		rt.Functions = append(rt.Functions, Function{
			TypeIdx:    typeIdx,
			Locals:     locals,
			Code:       body,
			ParamCount: paramCount,
		})

		offset = funcEnd
	}

	return nil
}

// CallFunction calls a function by index
func (rt *Runtime) CallFunction(funcIdx int, params []uint64) ([]uint64, error) {
	if funcIdx >= len(rt.Functions) {
		return nil, fmt.Errorf("function index out of range: %d", funcIdx)
	}

	fn := rt.Functions[funcIdx]

	// If this is an import function, call it directly and push results to stack
	if fn.IsImport {
		results, err := rt.callImport(funcIdx, params)
		if err != nil {
			return nil, err
		}
		// Push return values to stack
		for _, r := range results {
			rt.Stack = append(rt.Stack, r)
		}
		return results, nil
	}

	// Create call frame
	frame := CallFrame{
		FuncIdx:  funcIdx,
		Locals:   make([]uint64, len(fn.Locals)+len(params)),
		ReturnPC: -1,
		SP:       len(rt.Stack),
	}

	// Copy parameters to locals
	for i, p := range params {
		frame.Locals[i] = p
	}

	rt.CallStack = append(rt.CallStack, frame)
	rt.currentFunc = funcIdx

	// Execute function
	if err := rt.executeFunction(fn); err != nil {
		return nil, err
	}

	// Pop call frame
	if len(rt.CallStack) > 0 {
		rt.CallStack = rt.CallStack[:len(rt.CallStack)-1]
	}

	// Return values are on stack
	return rt.Stack[frame.SP:], nil
}

// executeFunction executes a function's bytecode
func (rt *Runtime) executeFunction(fn Function) error {
	code := fn.Code
	pc := 0

	for pc < len(code) {
		opcode := code[pc]
		pc++

		switch opcode {
		case 0x00: // unreachable
			return fmt.Errorf("unreachable executed")

		case 0x01: // nop
			// Do nothing

		case 0x02: // block
			// Read block type
			_ = code[pc]
			pc++
			// Simplified - just skip

		case 0x03: // loop
			_ = code[pc]
			pc++

		case 0x04: // if
			_ = code[pc]
			pc++

		case 0x05: // else
			// Simplified

		case 0x0b: // end
			// End of block or function

		case 0x0c: // br
			labelIdx, n := readLeb128(code, pc)
			_ = labelIdx
			pc += n

		case 0x0d: // br_if
			labelIdx, n := readLeb128(code, pc)
			_ = labelIdx
			pc += n
			// Pop condition
			if len(rt.Stack) > 0 {
				rt.Stack = rt.Stack[:len(rt.Stack)-1]
			}

		case 0x0f: // return
			return nil

		case 0x10: // call
			funcIdx, n := readLeb128(code, pc)
			pc += n
			// Get function to know param count
			if int(funcIdx) >= len(rt.Functions) {
				return fmt.Errorf("call to invalid function index: %d", funcIdx)
			}
			fn := rt.Functions[funcIdx]
			paramCount := fn.ParamCount
			// Pop parameters from stack
			var params []uint64
			if paramCount > 0 && len(rt.Stack) >= paramCount {
				params = rt.Stack[len(rt.Stack)-paramCount:]
				rt.Stack = rt.Stack[:len(rt.Stack)-paramCount]
			}
			// Call function
			_, err := rt.CallFunction(int(funcIdx), params)
			if err != nil {
				return err
			}

		case 0x1a: // drop
			if len(rt.Stack) > 0 {
				rt.Stack = rt.Stack[:len(rt.Stack)-1]
			}

		case 0x1b: // select
			if len(rt.Stack) >= 3 {
				c := rt.Stack[len(rt.Stack)-1]
				v2 := rt.Stack[len(rt.Stack)-2]
				v1 := rt.Stack[len(rt.Stack)-3]
				rt.Stack = rt.Stack[:len(rt.Stack)-3]
				if c != 0 {
					rt.Stack = append(rt.Stack, v1)
				} else {
					rt.Stack = append(rt.Stack, v2)
				}
			}

		case 0x20: // local.get
			localIdx, n := readLeb128(code, pc)
			pc += n
			if len(rt.CallStack) > 0 {
				frame := &rt.CallStack[len(rt.CallStack)-1]
				if int(localIdx) < len(frame.Locals) {
					rt.Stack = append(rt.Stack, frame.Locals[localIdx])
				}
			}

		case 0x21: // local.set
			localIdx, n := readLeb128(code, pc)
			pc += n
			if len(rt.Stack) > 0 && len(rt.CallStack) > 0 {
				frame := &rt.CallStack[len(rt.CallStack)-1]
				if int(localIdx) < len(frame.Locals) {
					frame.Locals[localIdx] = rt.Stack[len(rt.Stack)-1]
				}
				rt.Stack = rt.Stack[:len(rt.Stack)-1]
			}

		case 0x41: // i32.const
			val, n := readLeb128Signed(code, pc)
			pc += n
			rt.Stack = append(rt.Stack, uint64(val))

		case 0x42: // i64.const
			val, n := readLeb128Signed(code, pc)
			pc += n
			rt.Stack = append(rt.Stack, uint64(val))

		case 0x44: // f64.const
			// Read 8 bytes
			if pc+8 <= len(code) {
				bits := binary.LittleEndian.Uint64(code[pc:])
				rt.Stack = append(rt.Stack, bits)
				pc += 8
			}

		case 0x6a: // i32.add
			if len(rt.Stack) >= 2 {
				a := uint32(rt.Stack[len(rt.Stack)-2])
				b := uint32(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				rt.Stack = append(rt.Stack, uint64(a+b))
			}

		case 0x6b: // i32.sub
			if len(rt.Stack) >= 2 {
				a := uint32(rt.Stack[len(rt.Stack)-2])
				b := uint32(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				rt.Stack = append(rt.Stack, uint64(a-b))
			}

		case 0x6c: // i32.mul
			if len(rt.Stack) >= 2 {
				a := uint32(rt.Stack[len(rt.Stack)-2])
				b := uint32(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				rt.Stack = append(rt.Stack, uint64(a*b))
			}

		case 0x7c: // i64.add
			if len(rt.Stack) >= 2 {
				a := rt.Stack[len(rt.Stack)-2]
				b := rt.Stack[len(rt.Stack)-1]
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				rt.Stack = append(rt.Stack, a+b)
			}

		case 0x7d: // i64.sub
			if len(rt.Stack) >= 2 {
				a := rt.Stack[len(rt.Stack)-2]
				b := rt.Stack[len(rt.Stack)-1]
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				rt.Stack = append(rt.Stack, a-b)
			}

		case 0x7e: // i64.mul
			if len(rt.Stack) >= 2 {
				a := rt.Stack[len(rt.Stack)-2]
				b := rt.Stack[len(rt.Stack)-1]
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				rt.Stack = append(rt.Stack, a*b)
			}

		case 0x46: // i32.eq
			if len(rt.Stack) >= 2 {
				a := uint32(rt.Stack[len(rt.Stack)-2])
				b := uint32(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				if a == b {
					rt.Stack = append(rt.Stack, 1)
				} else {
					rt.Stack = append(rt.Stack, 0)
				}
			}

		case 0x47: // i32.ne
			if len(rt.Stack) >= 2 {
				a := uint32(rt.Stack[len(rt.Stack)-2])
				b := uint32(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				if a != b {
					rt.Stack = append(rt.Stack, 1)
				} else {
					rt.Stack = append(rt.Stack, 0)
				}
			}

		case 0x48: // i32.lt_s
			if len(rt.Stack) >= 2 {
				a := int32(rt.Stack[len(rt.Stack)-2])
				b := int32(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				if a < b {
					rt.Stack = append(rt.Stack, 1)
				} else {
					rt.Stack = append(rt.Stack, 0)
				}
			}

		case 0x4d: // i32.gt_s
			if len(rt.Stack) >= 2 {
				a := int32(rt.Stack[len(rt.Stack)-2])
				b := int32(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				if a > b {
					rt.Stack = append(rt.Stack, 1)
				} else {
					rt.Stack = append(rt.Stack, 0)
				}
			}

		case 0x51: // i64.eq
			if len(rt.Stack) >= 2 {
				a := rt.Stack[len(rt.Stack)-2]
				b := rt.Stack[len(rt.Stack)-1]
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				if a == b {
					rt.Stack = append(rt.Stack, 1)
				} else {
					rt.Stack = append(rt.Stack, 0)
				}
			}

		case 0x28: // i32.load
			memArgAlign, n := readLeb128(code, pc)
			_ = memArgAlign
			pc += n
			memArgOffset, n := readLeb128(code, pc)
			_ = memArgOffset
			pc += n
			if len(rt.Stack) > 0 {
				addr := int(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-1]
				if addr+4 <= len(rt.Memory) {
					val := binary.LittleEndian.Uint32(rt.Memory[addr:])
					rt.Stack = append(rt.Stack, uint64(val))
				}
			}

		case 0x29: // i64.load
			memArgAlign, n := readLeb128(code, pc)
			_ = memArgAlign
			pc += n
			memArgOffset, n := readLeb128(code, pc)
			_ = memArgOffset
			pc += n
			if len(rt.Stack) > 0 {
				addr := int(rt.Stack[len(rt.Stack)-1])
				rt.Stack = rt.Stack[:len(rt.Stack)-1]
				if addr+8 <= len(rt.Memory) {
					val := binary.LittleEndian.Uint64(rt.Memory[addr:])
					rt.Stack = append(rt.Stack, val)
				}
			}

		case 0x36: // i32.store
			memArgAlign, n := readLeb128(code, pc)
			_ = memArgAlign
			pc += n
			memArgOffset, n := readLeb128(code, pc)
			_ = memArgOffset
			pc += n
			if len(rt.Stack) >= 2 {
				val := uint32(rt.Stack[len(rt.Stack)-1])
				addr := int(rt.Stack[len(rt.Stack)-2])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				if addr+4 <= len(rt.Memory) {
					binary.LittleEndian.PutUint32(rt.Memory[addr:], val)
				}
			}

		case 0x37: // i64.store
			memArgAlign, n := readLeb128(code, pc)
			_ = memArgAlign
			pc += n
			memArgOffset, n := readLeb128(code, pc)
			_ = memArgOffset
			pc += n
			if len(rt.Stack) >= 2 {
				val := rt.Stack[len(rt.Stack)-1]
				addr := int(rt.Stack[len(rt.Stack)-2])
				rt.Stack = rt.Stack[:len(rt.Stack)-2]
				if addr+8 <= len(rt.Memory) {
					binary.LittleEndian.PutUint64(rt.Memory[addr:], val)
				}
			}

		default:
			return fmt.Errorf("unimplemented opcode: 0x%02x", opcode)
		}
	}

	return nil
}

// QueryProfiler provides performance profiling for WASM queries
type QueryProfiler struct {
	// Execution metrics
	TotalExecutions   int64
	TotalDuration     int64 // nanoseconds
	MinDuration       int64
	MaxDuration       int64
	AvgDuration       int64
	LastDuration      int64

	// Memory metrics
	PeakMemoryUsage   int
	TotalMemoryAllocs int64

	// Operation counters
	OpcodesExecuted   int64
	HostCalls         int64
	MemoryAccesses    int64

	// Per-query history (limited size)
	History           []QueryExecutionRecord
	HistorySize       int
}

// QueryExecutionRecord represents a single query execution record
type QueryExecutionRecord struct {
	Timestamp  int64
	Duration   int64
	Rows       int
	MemoryUsed int
}

// NewQueryProfiler creates a new query profiler
func NewQueryProfiler() *QueryProfiler {
	return &QueryProfiler{
		MinDuration:     int64(^uint64(0) >> 1), // Max int64
		HistorySize:     100,
		History:         make([]QueryExecutionRecord, 0, 100),
	}
}

// RecordExecution records a query execution
func (p *QueryProfiler) RecordExecution(duration int64, rows int, memoryUsed int) {
	p.TotalExecutions++
	p.TotalDuration += duration
	p.LastDuration = duration

	if duration < p.MinDuration {
		p.MinDuration = duration
	}
	if duration > p.MaxDuration {
		p.MaxDuration = duration
	}

	p.AvgDuration = p.TotalDuration / p.TotalExecutions

	if memoryUsed > p.PeakMemoryUsage {
		p.PeakMemoryUsage = memoryUsed
	}

	// Add to history
	record := QueryExecutionRecord{
		Timestamp:  timeNow(),
		Duration:   duration,
		Rows:       rows,
		MemoryUsed: memoryUsed,
	}

	if len(p.History) >= p.HistorySize {
		p.History = p.History[1:]
	}
	p.History = append(p.History, record)
}

// GetStats returns profiling statistics
func (p *QueryProfiler) GetStats() ProfileStats {
	return ProfileStats{
		TotalExecutions: p.TotalExecutions,
		TotalDuration:   p.TotalDuration,
		MinDuration:     p.MinDuration,
		MaxDuration:     p.MaxDuration,
		AvgDuration:     p.AvgDuration,
		LastDuration:    p.LastDuration,
		PeakMemoryUsage: p.PeakMemoryUsage,
	}
}

// ProfileStats contains profiling statistics
type ProfileStats struct {
	TotalExecutions int64
	TotalDuration   int64
	MinDuration     int64
	MaxDuration     int64
	AvgDuration     int64
	LastDuration    int64
	PeakMemoryUsage int
}

// timeNow returns current timestamp (simplified)
func timeNow() int64 {
	return 0 // Placeholder - in real impl would use time.Now().UnixNano()
}

// BenchmarkResult represents benchmark results
type BenchmarkResult struct {
	QueryName       string
	Iterations      int
	TotalDuration   int64
	AvgDuration     int64
	MinDuration     int64
	MaxDuration     int64
	RowsPerSecond   float64
	Throughput      float64 // ops/sec
}

// BenchmarkQuery benchmarks a query execution
func BenchmarkQuery(rt *Runtime, compiled *CompiledQuery, iterations int) (*BenchmarkResult, error) {
	if iterations <= 0 {
		iterations = 100
	}

	result := &BenchmarkResult{
		QueryName:  "query",
		Iterations: iterations,
		MinDuration: int64(^uint64(0) >> 1),
	}

	var totalRows int

	for i := 0; i < iterations; i++ {
		start := timeNow()

		queryResult, err := rt.Execute(compiled, nil)
		if err != nil {
			return nil, err
		}

		duration := timeNow() - start
		totalRows += len(queryResult.Rows)

		result.TotalDuration += duration
		if duration < result.MinDuration {
			result.MinDuration = duration
		}
		if duration > result.MaxDuration {
			result.MaxDuration = duration
		}
	}

	result.AvgDuration = result.TotalDuration / int64(iterations)
	result.Throughput = float64(iterations) / (float64(result.TotalDuration) / 1e9)
	result.RowsPerSecond = float64(totalRows) / (float64(result.TotalDuration) / 1e9)

	return result, nil
}