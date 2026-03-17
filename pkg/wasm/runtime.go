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
	// Imported functions
	Imports map[string]ImportFunc
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
	TypeIdx int
	Locals  []ValueType
	Code    []byte
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
	resultPtr := int32(result[0])
	rowCount := int32(result[1])

	return rt.parseResults(compiled.ResultSchema, resultPtr, rowCount)
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
			// Parse type section (not needed for simple execution)
		case 0x02: // Import section
			if err := rt.parseImportSection(sectionData); err != nil {
				return err
			}
		case 0x03: // Function section
			// Parse function section
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
			// Create stub function
			rt.Functions = append(rt.Functions, Function{
				TypeIdx: int(idx),
				Locals:  nil,
				Code:    []byte{0x10, byte(len(rt.Functions)), 0x0b}, // call import, end
			})
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

		rt.Functions = append(rt.Functions, Function{
			TypeIdx: int(i),
			Locals:  locals,
			Code:    body,
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
			// Call function
			_, err := rt.CallFunction(int(funcIdx), nil)
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

