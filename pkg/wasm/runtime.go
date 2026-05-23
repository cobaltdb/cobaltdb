package wasm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

const maxInt32 = math.MaxInt32
const maxUint32 = math.MaxUint32
const maxInt = int(^uint(0) >> 1)

func wasmI32(v uint64) uint32 {
	// #nosec G115 -- WASM i32 values intentionally wrap to 32 bits.
	return uint32(v)
}

func wasmI32Signed(v uint32) int32 {
	// #nosec G115 -- WASM i32 signed operations use two's-complement reinterpretation.
	return int32(v)
}

func wasmI64Bits(v int64) uint64 {
	// #nosec G115 -- WASM stores signed integers as raw two's-complement bits.
	return uint64(v)
}

func wasmI64Signed(v uint64) int64 {
	// #nosec G115 -- WASM i64 signed values use two's-complement reinterpretation.
	return int64(v)
}

func checkedInt(v uint64, name string) (int, error) {
	if v > uint64(maxInt) {
		return 0, fmt.Errorf("%s overflows int: %d", name, v)
	}
	return int(v), nil
}

func checkedInt32(v uint64, name string) (int32, error) {
	if v > math.MaxInt32 {
		return 0, fmt.Errorf("%s overflows int32: %d", name, v)
	}
	return int32(v), nil
}

func checkedInt64(v uint64, name string) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf("%s overflows int64: %d", name, v)
	}
	return int64(v), nil
}

func checkedUint32(v uint64, name string) (uint32, error) {
	if v > maxUint32 {
		return 0, fmt.Errorf("%s overflows uint32: %d", name, v)
	}
	return uint32(v), nil
}

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
	FuncIdx  int
	Locals   []uint64
	ReturnPC int
	SP       int // Stack pointer
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
			rowsAffected, err := checkedInt64(result[0], "rows affected")
			if err != nil {
				return nil, err
			}
			return &QueryResult{
				RowsAffected: rowsAffected,
			}, nil
		}
		return &QueryResult{}, nil
	}

	// SELECT query - parse result buffer
	if len(result) < 2 {
		return &QueryResult{}, nil
	}
	resultPtr, err := checkedInt32(result[0], "result pointer")
	if err != nil {
		return nil, err
	}
	rowCount, err := checkedInt32(result[1], "row count")
	if err != nil {
		return nil, err
	}

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

	resultPtr, err := checkedInt32(result[0], "result pointer")
	if err != nil {
		return nil, err
	}
	rowCount, err := checkedInt(result[1], "row count")
	if err != nil {
		return nil, err
	}

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
				row.Values[j] = wasmI64Signed(val)
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

// memCheck verifies that [offset, offset+size) lies within rt.Memory bounds.
// Returns an error rather than panicking on out-of-range or negative inputs.
func (rt *Runtime) memCheck(offset int32, size int32) error {
	if offset < 0 || size < 0 {
		return fmt.Errorf("wasm: negative offset or size (offset=%d size=%d)", offset, size)
	}
	end := int64(offset) + int64(size)
	if end > int64(len(rt.Memory)) {
		return fmt.Errorf("wasm: memory access out of range (offset=%d size=%d mem_size=%d)", offset, size, len(rt.Memory))
	}
	return nil
}

// writeParams writes query parameters to WASM memory
func (rt *Runtime) writeParams(ptr int32, args []interface{}) error {
	offset := ptr
	for _, arg := range args {
		switch v := arg.(type) {
		case int:
			if err := rt.memCheck(offset, 8); err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(v))
			offset += 8
		case int64:
			if err := rt.memCheck(offset, 8); err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(rt.Memory[offset:], wasmI64Bits(v))
			offset += 8
		case float64:
			if err := rt.memCheck(offset, 8); err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(v))
			offset += 8
		case string:
			// String length must fit in int32 to avoid overflow when added to offset.
			if len(v) > maxInt32 {
				return fmt.Errorf("wasm: string parameter too large (%d bytes)", len(v))
			}
			strLen := int32(len(v))
			if err := rt.memCheck(offset, 4+strLen); err != nil {
				return err
			}
			binary.LittleEndian.PutUint32(rt.Memory[offset:], uint32(strLen))
			offset += 4
			copy(rt.Memory[offset:], v)
			offset += strLen
		case nil:
			if err := rt.memCheck(offset, 4); err != nil {
				return err
			}
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
				if err := rt.memCheck(offset, 8); err != nil {
					return nil, err
				}
				val := binary.LittleEndian.Uint64(rt.Memory[offset:])
				row.Values[j] = wasmI64Signed(val)
				offset += 8
			case "REAL":
				if err := rt.memCheck(offset, 8); err != nil {
					return nil, err
				}
				bits := binary.LittleEndian.Uint64(rt.Memory[offset:])
				row.Values[j] = bits
				offset += 8
			case "TEXT":
				if err := rt.memCheck(offset, 4); err != nil {
					return nil, err
				}
				length := binary.LittleEndian.Uint32(rt.Memory[offset:])
				offset += 4
				if length == 0xFFFFFFFF {
					row.Values[j] = nil
				} else {
					if length > maxInt32 {
						return nil, fmt.Errorf("wasm: TEXT length %d exceeds int32 max", length)
					}
					strLen := int32(length)
					if err := rt.memCheck(offset, strLen); err != nil {
						return nil, err
					}
					row.Values[j] = string(rt.Memory[offset : offset+strLen])
					offset += strLen
				}
			default:
				if err := rt.memCheck(offset, 4); err != nil {
					return nil, err
				}
				length := binary.LittleEndian.Uint32(rt.Memory[offset:])
				offset += 4
				if length == 0xFFFFFFFF {
					row.Values[j] = nil
				} else {
					if length > maxInt32 {
						return nil, fmt.Errorf("wasm: column length %d exceeds int32 max", length)
					}
					strLen := int32(length)
					if err := rt.memCheck(offset, strLen); err != nil {
						return nil, err
					}
					row.Values[j] = string(rt.Memory[offset : offset+strLen])
					offset += strLen
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

		sectionSizeInt, err := checkedInt(sectionSize, "section size")
		if err != nil {
			return err
		}
		sectionEnd := offset + sectionSizeInt
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
		typeIdx32, err := checkedUint32(typeIdx, "function type index")
		if err != nil {
			return err
		}
		rt.funcTypeIndices = append(rt.funcTypeIndices, typeIdx32)
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
		modLenInt, err := checkedInt(modLen, "import module name length")
		if err != nil {
			return err
		}
		if offset+modLenInt > len(data) {
			return fmt.Errorf("import module name truncated")
		}
		module := string(data[offset : offset+modLenInt])
		offset += modLenInt

		// Read field name
		fieldLen, n := readLeb128(data, offset)
		offset += n
		fieldLenInt, err := checkedInt(fieldLen, "import field name length")
		if err != nil {
			return err
		}
		if offset+fieldLenInt > len(data) {
			return fmt.Errorf("import field name truncated")
		}
		field := string(data[offset : offset+fieldLenInt])
		offset += fieldLenInt

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
			typeIdx, err := checkedInt(idx, "import type index")
			if err != nil {
				return err
			}
			if typeIdx < len(rt.Types) {
				paramCount = len(rt.Types[typeIdx].Params)
			}
			// Track import name by function index
			funcIdx := len(rt.Functions)
			rt.importNames = append(rt.importNames, importKey)
			// Create import function stub (no code - will be handled by callImport)
			rt.Functions = append(rt.Functions, Function{
				TypeIdx:    typeIdx,
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
		max, _ = readLeb128(data, offset)
		_ = max
	}

	// Resize memory if needed
	minPages, err := checkedInt(min, "minimum memory pages")
	if err != nil {
		return err
	}
	if minPages > maxInt/(64*1024) {
		return fmt.Errorf("minimum memory pages overflow: %d", min)
	}
	requiredSize := minPages * 64 * 1024
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

		funcSizeInt, err := checkedInt(funcSize, "function body size")
		if err != nil {
			return err
		}
		funcEnd := offset + funcSizeInt
		if funcEnd > len(data) {
			return fmt.Errorf("code section truncated")
		}
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
		rt.Stack = append(rt.Stack, results...)
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
	copy(frame.Locals, params)

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

		switch {
		case opcode == 0x00:
			return fmt.Errorf("unreachable executed")
		case opcode == 0x01: // nop
		case opcode == 0x0b: // end
		case opcode == 0x0f: // return
			return nil
		case opcode == 0x10:
			var err error
			pc, err = rt.execCall(code, pc)
			if err != nil {
				return err
			}
		case opcode == 0x41 || opcode == 0x42:
			pc = rt.execConst(code, pc)
		case opcode == 0x44:
			pc = rt.execF64Const(code, pc)
		case opcode >= 0x46 && opcode <= 0x4d:
			rt.execI32Compare(opcode)
		case opcode == 0x51:
			rt.execI64Eq()
		case opcode == 0x6a || opcode == 0x6b || opcode == 0x6c:
			rt.execI32Arith(opcode)
		case opcode == 0x7c || opcode == 0x7d || opcode == 0x7e:
			rt.execI64Arith(opcode)
		case opcode >= 0x20 && opcode <= 0x21:
			pc = rt.execLocal(opcode, code, pc)
		case opcode >= 0x28 && opcode <= 0x29:
			pc = rt.execLoad(opcode, code, pc)
		case opcode >= 0x36 && opcode <= 0x37:
			pc = rt.execStore(opcode, code, pc)
		case opcode == 0x1a:
			if len(rt.Stack) > 0 {
				rt.Stack = rt.Stack[:len(rt.Stack)-1]
			}
		case opcode == 0x1b:
			rt.execSelect()
		case opcode == 0x02 || opcode == 0x03 || opcode == 0x04:
			pc++
		case opcode == 0x05: // else
		case opcode == 0x0c || opcode == 0x0d:
			_, n := readLeb128(code, pc)
			pc += n
			if opcode == 0x0d && len(rt.Stack) > 0 {
				rt.Stack = rt.Stack[:len(rt.Stack)-1]
			}
		default:
			return fmt.Errorf("unimplemented opcode: 0x%02x", opcode)
		}
	}

	return nil
}

func (rt *Runtime) execCall(code []byte, pc int) (int, error) {
	funcIdx, n := readLeb128(code, pc)
	pc += n
	funcIdxInt, err := checkedInt(funcIdx, "function index")
	if err != nil {
		return pc, err
	}
	if funcIdxInt >= len(rt.Functions) {
		return pc, fmt.Errorf("call to invalid function index: %d", funcIdx)
	}
	fn := rt.Functions[funcIdxInt]
	paramCount := fn.ParamCount
	var params []uint64
	if paramCount > 0 && len(rt.Stack) >= paramCount {
		params = rt.Stack[len(rt.Stack)-paramCount:]
		rt.Stack = rt.Stack[:len(rt.Stack)-paramCount]
	}
	_, err = rt.CallFunction(funcIdxInt, params)
	return pc, err
}

func (rt *Runtime) execConst(code []byte, pc int) int {
	val, n := readLeb128Signed(code, pc)
	pc += n
	rt.Stack = append(rt.Stack, wasmI64Bits(val))
	return pc
}

func (rt *Runtime) execF64Const(code []byte, pc int) int {
	if pc+8 <= len(code) {
		bits := binary.LittleEndian.Uint64(code[pc:])
		rt.Stack = append(rt.Stack, bits)
		pc += 8
	}
	return pc
}

func (rt *Runtime) execI32Compare(opcode byte) {
	if len(rt.Stack) < 2 {
		return
	}
	a := wasmI32(rt.Stack[len(rt.Stack)-2])
	b := wasmI32(rt.Stack[len(rt.Stack)-1])
	rt.Stack = rt.Stack[:len(rt.Stack)-2]
	var result uint64
	switch opcode {
	case 0x46: // i32.eq
		if a == b {
			result = 1
		}
	case 0x47: // i32.ne
		if a != b {
			result = 1
		}
	case 0x48: // i32.lt_s
		if wasmI32Signed(a) < wasmI32Signed(b) {
			result = 1
		}
	case 0x4d: // i32.gt_s
		if wasmI32Signed(a) > wasmI32Signed(b) {
			result = 1
		}
	}
	rt.Stack = append(rt.Stack, result)
}

func (rt *Runtime) execI64Eq() {
	if len(rt.Stack) < 2 {
		return
	}
	a := rt.Stack[len(rt.Stack)-2]
	b := rt.Stack[len(rt.Stack)-1]
	rt.Stack = rt.Stack[:len(rt.Stack)-2]
	if a == b {
		rt.Stack = append(rt.Stack, 1)
	} else {
		rt.Stack = append(rt.Stack, 0)
	}
}

func (rt *Runtime) execI32Arith(opcode byte) {
	if len(rt.Stack) < 2 {
		return
	}
	a := wasmI32(rt.Stack[len(rt.Stack)-2])
	b := wasmI32(rt.Stack[len(rt.Stack)-1])
	rt.Stack = rt.Stack[:len(rt.Stack)-2]
	var result uint32
	switch opcode {
	case 0x6a: // i32.add
		result = a + b
	case 0x6b: // i32.sub
		result = a - b
	case 0x6c: // i32.mul
		result = a * b
	}
	rt.Stack = append(rt.Stack, uint64(result))
}

func (rt *Runtime) execI64Arith(opcode byte) {
	if len(rt.Stack) < 2 {
		return
	}
	a := rt.Stack[len(rt.Stack)-2]
	b := rt.Stack[len(rt.Stack)-1]
	rt.Stack = rt.Stack[:len(rt.Stack)-2]
	var result uint64
	switch opcode {
	case 0x7c: // i64.add
		result = a + b
	case 0x7d: // i64.sub
		result = a - b
	case 0x7e: // i64.mul
		result = a * b
	}
	rt.Stack = append(rt.Stack, result)
}

func (rt *Runtime) execLocal(opcode byte, code []byte, pc int) int {
	localIdx, n := readLeb128(code, pc)
	pc += n
	if len(rt.CallStack) == 0 {
		return pc
	}
	frame := &rt.CallStack[len(rt.CallStack)-1]
	localIdxInt, err := checkedInt(localIdx, "local index")
	if err != nil || localIdxInt >= len(frame.Locals) {
		return pc
	}
	if opcode == 0x20 { // local.get
		rt.Stack = append(rt.Stack, frame.Locals[localIdxInt])
	} else { // local.set
		if len(rt.Stack) > 0 {
			frame.Locals[localIdxInt] = rt.Stack[len(rt.Stack)-1]
			rt.Stack = rt.Stack[:len(rt.Stack)-1]
		}
	}
	return pc
}

func (rt *Runtime) execLoad(opcode byte, code []byte, pc int) int {
	memArgAlign, n := readLeb128(code, pc)
	_ = memArgAlign
	pc += n
	memArgOffset, n := readLeb128(code, pc)
	_ = memArgOffset
	pc += n
	if len(rt.Stack) == 0 {
		return pc
	}
	addr, err := checkedInt(rt.Stack[len(rt.Stack)-1], "load address")
	if err != nil {
		return pc
	}
	rt.Stack = rt.Stack[:len(rt.Stack)-1]
	if addr >= 0 && opcode == 0x28 && addr+4 <= len(rt.Memory) { // i32.load
		val := binary.LittleEndian.Uint32(rt.Memory[addr:])
		rt.Stack = append(rt.Stack, uint64(val))
	} else if addr >= 0 && opcode == 0x29 && addr+8 <= len(rt.Memory) { // i64.load
		val := binary.LittleEndian.Uint64(rt.Memory[addr:])
		rt.Stack = append(rt.Stack, val)
	}
	return pc
}

func (rt *Runtime) execStore(opcode byte, code []byte, pc int) int {
	memArgAlign, n := readLeb128(code, pc)
	_ = memArgAlign
	pc += n
	memArgOffset, n := readLeb128(code, pc)
	_ = memArgOffset
	pc += n
	if len(rt.Stack) < 2 {
		return pc
	}
	val := rt.Stack[len(rt.Stack)-1]
	addr, err := checkedInt(rt.Stack[len(rt.Stack)-2], "store address")
	if err != nil {
		return pc
	}
	rt.Stack = rt.Stack[:len(rt.Stack)-2]
	if addr >= 0 && opcode == 0x36 && addr+4 <= len(rt.Memory) { // i32.store
		binary.LittleEndian.PutUint32(rt.Memory[addr:], wasmI32(val))
	} else if addr >= 0 && opcode == 0x37 && addr+8 <= len(rt.Memory) { // i64.store
		binary.LittleEndian.PutUint64(rt.Memory[addr:], val)
	}
	return pc
}

func (rt *Runtime) execSelect() {
	if len(rt.Stack) < 3 {
		return
	}
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

// QueryProfiler provides performance profiling for WASM queries
type QueryProfiler struct {
	// Execution metrics
	TotalExecutions int64
	TotalDuration   int64 // nanoseconds
	MinDuration     int64
	MaxDuration     int64
	AvgDuration     int64
	LastDuration    int64

	// Memory metrics
	PeakMemoryUsage   int
	TotalMemoryAllocs int64

	// Operation counters
	OpcodesExecuted int64
	HostCalls       int64
	MemoryAccesses  int64

	// Per-query history (limited size)
	History     []QueryExecutionRecord
	HistorySize int
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
		MinDuration: int64(^uint64(0) >> 1), // Max int64
		HistorySize: 100,
		History:     make([]QueryExecutionRecord, 0, 100),
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
	QueryName     string
	Iterations    int
	TotalDuration int64
	AvgDuration   int64
	MinDuration   int64
	MaxDuration   int64
	RowsPerSecond float64
	Throughput    float64 // ops/sec
}

// BenchmarkQuery benchmarks a query execution
func BenchmarkQuery(rt *Runtime, compiled *CompiledQuery, iterations int) (*BenchmarkResult, error) {
	if iterations <= 0 {
		iterations = 100
	}

	result := &BenchmarkResult{
		QueryName:   "query",
		Iterations:  iterations,
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
