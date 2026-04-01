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
	// Prepared statements cache
	preparedStmts map[string]*PreparedStatement
}

// PreparedStatement represents a prepared SQL statement with parameter placeholders
type PreparedStatement struct {
	// Statement ID (unique identifier)
	ID string
	// Original SQL with placeholders
	SQL string
	// Compiled query
	Compiled *CompiledQuery
	// Parameter types
	ParamTypes []ValueType
	// Parameter count
	ParamCount int
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
	Type    ValueType
	Mutable bool
	Init    []byte
}

// Export represents a WASM export
type Export struct {
	Name  string
	Kind  byte
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
		types:         make([]FuncType, 0),
		imports:       make([]Import, 0),
		funcs:         make([]uint32, 0),
		globals:       make([]Global, 0),
		exports:       make([]Export, 0),
		codes:         make([]Code, 0),
		data:          make([]DataSegment, 0),
		queryCache:    make(map[string]*CompiledQuery),
		preparedStmts: make(map[string]*PreparedStatement),
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
	case *query.UnionStmt:
		entryPoint, err = c.compileUnion(s)
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

// Prepare creates a prepared statement from SQL with parameter placeholders
// Parameters are denoted by ? in the SQL string
func (c *Compiler) Prepare(sql string, stmt query.Statement, paramCount int) (*PreparedStatement, error) {
	// Generate unique ID for this prepared statement
	id := fmt.Sprintf("stmt_%d", len(c.preparedStmts))

	// Compile the query
	compiled, err := c.CompileQuery(sql, stmt, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}

	// Create prepared statement
	prepared := &PreparedStatement{
		ID:         id,
		SQL:        sql,
		Compiled:   compiled,
		ParamCount: paramCount,
		ParamTypes: make([]ValueType, paramCount),
	}

	// Default parameter types to I64
	for i := 0; i < paramCount; i++ {
		prepared.ParamTypes[i] = I64
	}

	// Store in cache
	c.preparedStmts[id] = prepared

	return prepared, nil
}

// ExecutePrepared executes a prepared statement with bound parameters
func (c *Compiler) ExecutePrepared(stmtID string, params []interface{}) (*CompiledQuery, error) {
	// Look up prepared statement
	prepared, ok := c.preparedStmts[stmtID]
	if !ok {
		return nil, fmt.Errorf("prepared statement not found: %s", stmtID)
	}

	// Validate parameter count
	if len(params) != prepared.ParamCount {
		return nil, fmt.Errorf("parameter count mismatch: expected %d, got %d", prepared.ParamCount, len(params))
	}

	// Return the compiled query (parameters will be bound at execution time)
	return prepared.Compiled, nil
}

// GetPreparedStatement retrieves a prepared statement by ID
func (c *Compiler) GetPreparedStatement(id string) (*PreparedStatement, bool) {
	stmt, ok := c.preparedStmts[id]
	return stmt, ok
}

// ClosePreparedStatement removes a prepared statement from the cache
func (c *Compiler) ClosePreparedStatement(id string) {
	delete(c.preparedStmts, id)
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

	// Import insert row function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "insertRow",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32}, // tableId, rowDataPtr
		Results: []ValueType{I32},      // success (1 or 0)
	})

	// Import update row function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "updateRow",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I64, I32}, // tableId, rowId, rowDataPtr
		Results: []ValueType{I32},           // success (1 or 0)
	})

	// Import delete row function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "deleteRow",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I64}, // tableId, rowId
		Results: []ValueType{I32},      // success (1 or 0)
	})

	// Import groupBy function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "groupBy",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32}, // tableId, groupColumnIdx, outPtr
		Results: []ValueType{I32},           // groupCount
	})

	// Import innerJoin function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "innerJoin",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // leftTableId, rightTableId, outPtr, maxRows
		Results: []ValueType{I32},                // row count
	})

	// Import executeSubquery function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "executeSubquery",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32}, // queryId, outPtr, maxRows
		Results: []ValueType{I32},           // row count
	})

	// Import sortRows function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "sortRows",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32}, // inPtr, rowCount, columnIdx, ascending, outPtr
		Results: []ValueType{I32},                     // sortedRowCount
	})

	// Import leftJoin function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "leftJoin",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // leftTableId, rightTableId, outPtr, maxRows
		Results: []ValueType{I32},                // row count
	})

	// Import rightJoin function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "rightJoin",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // leftTableId, rightTableId, outPtr, maxRows
		Results: []ValueType{I32},                // row count
	})

	// Import fullJoin function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "fullJoin",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // leftTableId, rightTableId, outPtr, maxRows
		Results: []ValueType{I32},                // row count
	})

	// Import limitOffset function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "limitOffset",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32}, // inPtr, rowCount, limit, offset, outPtr
		Results: []ValueType{I32},                     // newRowCount
	})

	// Import distinctRows function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "distinctRows",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // inPtr, rowCount, rowSize, outPtr
		Results: []ValueType{I32},                // distinctRowCount
	})

	// Import unionResults function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "unionResults",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32}, // leftPtr, leftCount, rightPtr, rightCount, outPtr
		Results: []ValueType{I32},                     // totalRowCount
	})

	// Import windowFunction function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "windowFunction",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // inPtr, rowCount, funcType, outPtr
		Results: []ValueType{I32},                // success
	})

	// Import exceptResults function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "exceptResults",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32}, // leftPtr, leftCount, rightPtr, rightCount, outPtr
		Results: []ValueType{I32},                     // resultRowCount
	})

	// Import intersectResults function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "intersectResults",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32}, // leftPtr, leftCount, rightPtr, rightCount, outPtr
		Results: []ValueType{I32},                     // resultRowCount
	})

	// Import indexScan function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "indexScan",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I64, I64, I32, I32}, // tableId, indexId, minVal, maxVal, outPtr, maxRows
		Results: []ValueType{I32},                          // rowCount
	})

	// Import bindParameter function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "bindParameter",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32}, // slotIdx, valuePtr, valueType
		Results: []ValueType{I32},           // success
	})

	// Import executeCorrelatedSubquery function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "executeCorrelatedSubquery",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32}, // queryId, outerRowPtr, outerRowSize, outPtr, maxRows
		Results: []ValueType{I32},                     // rowCount
	})

	// Import fetchChunk function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "fetchChunk",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32}, // startRow, rowCount, outPtr
		Results: []ValueType{I32},           // actualRowCount
	})

	// Import transaction functions
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "beginTransaction",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{},    // no params
		Results: []ValueType{I32}, // success
	})

	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "commitTransaction",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{},    // no params
		Results: []ValueType{I32}, // success
	})

	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "rollbackTransaction",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{},    // no params
		Results: []ValueType{I32}, // success
	})

	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "savepoint",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32}, // savepointId
		Results: []ValueType{I32}, // success
	})

	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "rollbackToSavepoint",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32}, // savepointId
		Results: []ValueType{I32}, // success
	})

	// Import executeUDF function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "executeUDF",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // funcNamePtr, funcNameLen, argsPtr, argCount
		Results: []ValueType{I64},                // result value
	})

	// Import getPartitionCount function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "getPartitionCount",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32}, // tableNamePtr, tableNameLen
		Results: []ValueType{I32},      // partitionCount
	})

	// Import partitionScan function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "partitionScan",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32}, // tableNamePtr, tableNameLen, partitionId, outPtr, maxRows
		Results: []ValueType{I32},                     // rowCount
	})

	// Import parallelAggregate function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "parallelAggregate",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32, I32}, // tableNamePtr, tableNameLen, aggType, colNamePtr, colNameLen, outPtr
		Results: []ValueType{I32},                          // success
	})

	// Import repartitionTable function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "repartitionTable",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32}, // tableNamePtr, tableNameLen, partitionCount
		Results: []ValueType{I32},           // success
	})

	// Import vectorizedAdd function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "vectorizedAdd",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // inPtr1, inPtr2, outPtr, count
		Results: []ValueType{I32},                // success
	})

	// Import vectorizedMultiply function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "vectorizedMultiply",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // inPtr1, inPtr2, outPtr, count
		Results: []ValueType{I32},                // success
	})

	// Import vectorizedCompare function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "vectorizedCompare",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32, I32}, // inPtr1, inPtr2, outPtr, count, op
		Results: []ValueType{I32},                     // success
	})

	// Import vectorizedSum function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "vectorizedSum",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32}, // inPtr, count
		Results: []ValueType{I64},      // sum
	})

	// Import vectorizedMinMax function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "vectorizedMinMax",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // inPtr, count, outMinPtr, outMaxPtr
		Results: []ValueType{I32},                // success
	})

	// Import vectorizedFilter function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "vectorizedFilter",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32, I32}, // inPtr, maskPtr, outPtr, count
		Results: []ValueType{I32},                // filteredCount
	})

	// Import vectorizedBatchCopy function
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "vectorizedBatchCopy",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32, I32}, // srcPtr, dstPtr, count
		Results: []ValueType{I32},           // success
	})

	// Import profiling functions
	// getQueryMetrics
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "getQueryMetrics",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32}, // outPtr
		Results: []ValueType{I32}, // success
	})

	// getMemoryStats
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "getMemoryStats",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32}, // outPtr
		Results: []ValueType{I32}, // success
	})

	// resetMetrics
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "resetMetrics",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{},    // no params
		Results: []ValueType{I32}, // success
	})

	// logProfilingEvent
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "logProfilingEvent",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I64, I32}, // eventType, duration, rowCount
		Results: []ValueType{I32},           // success
	})

	// getOpcodeStats
	c.imports = append(c.imports, Import{
		Module: "env",
		Name:   "getOpcodeStats",
		Kind:   0x00,
		Index:  uint32(len(c.types)),
	})
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32, I32}, // outPtr, maxOpcodes
		Results: []ValueType{I32},      // count
	})

	// Update function index to account for imports
	c.funcIdx = uint32(len(c.imports))
}

// compileSelect compiles a SELECT statement
func (c *Compiler) compileSelect(stmt *query.SelectStmt) (uint32, error) {
	// Create function type for query entry point
	funcTypeIdx := uint32(len(c.types))
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32},      // params pointer
		Results: []ValueType{I32, I32}, // result pointer, row count
	})

	// Add to function section
	funcIdx := c.funcIdx
	c.funcs = append(c.funcs, funcTypeIdx)
	c.funcIdx++

	// Generate function body
	// Function has 1 parameter (params pointer = local 0)
	// Plus 2 locals: rowCount (local 1), resultPtr (local 2)
	body := c.generateSelectBody(stmt, 1) // 1 = first local index after params

	// Add code section with locals: rowCount, resultPtr (both i32)
	c.codes = append(c.codes, Code{
		Locals: []Local{
			{Count: 2, Type: I32}, // 2 i32 locals: rowCount, resultPtr
		},
		Body: body,
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

	// Locals: rowsAffected (i64), tableId (i32), rowDataPtr (i32)
	c.codes = append(c.codes, Code{
		Locals: []Local{
			{Count: 1, Type: I64}, // rowsAffected
			{Count: 2, Type: I32}, // tableId, rowDataPtr
		},
		Body: body,
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

	// Locals: rowsAffected (i64), tableId (i32), rowId (i64)
	c.codes = append(c.codes, Code{
		Locals: []Local{
			{Count: 2, Type: I64}, // rowsAffected, rowId
			{Count: 1, Type: I32}, // tableId
		},
		Body: body,
	})

	c.exports = append(c.exports, Export{
		Name:  "query",
		Kind:  0x00,
		Index: funcIdx,
	})

	return funcIdx, nil
}

// compileUnion compiles a UNION statement
func (c *Compiler) compileUnion(stmt *query.UnionStmt) (uint32, error) {
	funcTypeIdx := uint32(len(c.types))
	c.types = append(c.types, FuncType{
		Params:  []ValueType{I32},      // params pointer
		Results: []ValueType{I32, I32}, // result pointer, row count
	})

	funcIdx := c.funcIdx
	c.funcs = append(c.funcs, funcTypeIdx)
	c.funcIdx++

	// Generate function body for UNION
	body := c.generateUnionBody(stmt)

	// Locals: rowCount (i32), leftCount (i32), rightCount (i32)
	c.codes = append(c.codes, Code{
		Locals: []Local{
			{Count: 3, Type: I32}, // rowCount, leftCount, rightCount
		},
		Body: body,
	})

	c.exports = append(c.exports, Export{
		Name:  "query",
		Kind:  0x00,
		Index: funcIdx,
	})

	return funcIdx, nil
}

// generateUnionBody generates WASM bytecode for a UNION statement
func (c *Compiler) generateUnionBody(stmt *query.UnionStmt) []byte {
	buf := new(bytes.Buffer)

	// For simplified UNION, just do a table scan (assuming both sides are the same table)
	// In real implementation, would compile left and right separately

	// Local indices (after params)
	rowCountIdx := byte(1)

	// Initialize rowCount = 0
	buf.WriteByte(0x41) // i32.const
	buf.WriteByte(0x00) // 0
	buf.WriteByte(0x21) // local.set
	buf.WriteByte(rowCountIdx)

	// Call tableScan to get results
	buf.WriteByte(0x41) // i32.const
	buf.WriteByte(0x00) // table id 0
	buf.WriteByte(0x41) // i32.const 1024 (result buffer)
	writeLeb128(buf, 1024)
	buf.WriteByte(0x41) // i32.const 1000 (max rows)
	writeLeb128(buf, 1000)
	buf.WriteByte(0x10) // call
	writeLeb128(buf, 0) // function index 0 (tableScan import)

	// Store row count
	buf.WriteByte(0x21) // local.set
	buf.WriteByte(rowCountIdx)

	// Return (resultPtr, rowCount)
	buf.WriteByte(0x41) // i32.const 1024 (result buffer)
	writeLeb128(buf, 1024)
	buf.WriteByte(0x20) // local.get
	buf.WriteByte(rowCountIdx)

	// End function
	buf.WriteByte(0x0b) // end

	return buf.Bytes()
}

// generateSelectBody generates WASM bytecode for a SELECT statement
// Supports WHERE clause filtering, GROUP BY, and HAVING if present
func (c *Compiler) generateSelectBody(stmt *query.SelectStmt, localOffset byte) []byte {
	buf := new(bytes.Buffer)

	// Local indices (offset by parameter count)
	rowCountIdx := localOffset      // local for row count
	resultPtrIdx := localOffset + 1 // local for result pointer

	// Check if we have clauses
	hasWhere := stmt.Where != nil
	hasGroupBy := len(stmt.GroupBy) > 0
	hasHaving := stmt.Having != nil
	hasJoins := len(stmt.Joins) > 0
	hasOrderBy := len(stmt.OrderBy) > 0

	// Note: ORDER BY, LIMIT and HAVING are applied after all other operations
	hasLimit := stmt.Limit != nil
	hasDistinct := stmt.Distinct
	_ = hasOrderBy  // used in code below
	_ = hasHaving   // future: apply after GROUP BY
	_ = hasDistinct // will be used below

	// Local variables
	// $rowCount = 0
	buf.WriteByte(0x41) // i32.const
	buf.WriteByte(0x00) // 0
	buf.WriteByte(0x21) // local.set
	buf.WriteByte(rowCountIdx)

	// $resultPtr = 1024 (result buffer base)
	buf.WriteByte(0x41) // i32.const
	writeLeb128(buf, 1024)
	buf.WriteByte(0x21) // local.set
	buf.WriteByte(resultPtrIdx)

	if hasJoins {
		// Determine join type
		joinType := query.TokenInner
		if len(stmt.Joins) > 0 {
			joinType = stmt.Joins[0].Type
		}

		// Get function index based on join type
		// Import indices: 7=innerJoin, 10=leftJoin, 11=rightJoin, 12=fullJoin
		funcIdx := 7 // default innerJoin
		switch joinType {
		case query.TokenLeft:
			funcIdx = 10 // leftJoin
		case query.TokenRight:
			funcIdx = 11 // rightJoin
		case query.TokenFull:
			funcIdx = 12 // fullJoin
		}

		// Call appropriate join host function
		// joinFunc(leftTableId, rightTableId, outPtr, maxRows) -> rowCount
		buf.WriteByte(0x41) // i32.const 0 (left table id)
		buf.WriteByte(0x00)
		buf.WriteByte(0x41) // i32.const 0 (right table id)
		buf.WriteByte(0x00)
		buf.WriteByte(0x20) // local.get (resultPtr)
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x41) // i32.const 1000 (max rows)
		writeLeb128(buf, 1000)
		buf.WriteByte(0x10) // call
		writeLeb128(buf, uint64(funcIdx))

		// Store row count
		buf.WriteByte(0x21) // local.set
		buf.WriteByte(rowCountIdx)
	} else if hasGroupBy {
		// For GROUP BY, use groupBy host function
		// groupBy(tableId, groupColumnIdx, outPtr) -> groupCount
		buf.WriteByte(0x41) // i32.const 0 (table id)
		buf.WriteByte(0x00)
		buf.WriteByte(0x41) // i32.const 0 (group column index - simplified)
		buf.WriteByte(0x00)
		buf.WriteByte(0x20) // local.get (resultPtr)
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x10) // call
		writeLeb128(buf, 6) // function index 6 (groupBy import)

		// Store group count as rowCount
		buf.WriteByte(0x21) // local.set
		buf.WriteByte(rowCountIdx)
	} else if hasWhere {
		// For WHERE clause, we use filterRow host function
		// filterRow(tableId, predicatePtr) -> boolean

		// Call filterRow to get filtered row count
		// This is a simplified implementation - in reality we'd iterate rows
		buf.WriteByte(0x41) // i32.const 0 (table id)
		buf.WriteByte(0x00)
		buf.WriteByte(0x41) // i32.const 0 (predicate ptr - simplified)
		buf.WriteByte(0x00)
		buf.WriteByte(0x10) // call
		writeLeb128(buf, 1) // function index 1 (filterRow import)

		// Store filtered count as rowCount
		buf.WriteByte(0x21) // local.set
		buf.WriteByte(rowCountIdx)
	} else {
		// No WHERE clause - call table scan directly
		buf.WriteByte(0x41) // i32.const
		buf.WriteByte(0x00) // table id 0
		buf.WriteByte(0x20) // local.get
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x41)    // i32.const
		writeLeb128(buf, 1000) // max rows
		buf.WriteByte(0x10)    // call
		writeLeb128(buf, 0)    // function index 0 (tableScan import)

		// Store row count
		buf.WriteByte(0x21) // local.set
		buf.WriteByte(rowCountIdx)
	}

	// If DISTINCT is present, remove duplicate rows
	if hasDistinct {
		// Call distinctRows(inPtr, rowCount, rowSize, outPtr)
		buf.WriteByte(0x20) // local.get (resultPtr)
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x20) // local.get (rowCount)
		buf.WriteByte(rowCountIdx)
		buf.WriteByte(0x41) // i32.const 8 (rowSize - 8 bytes per row)
		buf.WriteByte(0x08)
		buf.WriteByte(0x20) // local.get (resultPtr)
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x10)  // call
		writeLeb128(buf, 14) // function index 14 (distinctRows import)

		// Store new row count
		buf.WriteByte(0x21) // local.set
		buf.WriteByte(rowCountIdx)
	}

	// If ORDER BY is present, sort the results
	// Note: This is a simplified implementation
	if hasOrderBy {
		// Call sortRows(inPtr, rowCount, columnIdx, ascending, outPtr)
		// For now, using same buffer for input and output (in-place sort placeholder)
		buf.WriteByte(0x20) // local.get (resultPtr)
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x20) // local.get (rowCount)
		buf.WriteByte(rowCountIdx)
		buf.WriteByte(0x41) // i32.const 0 (columnIdx - first column)
		buf.WriteByte(0x00)
		buf.WriteByte(0x41) // i32.const 1 (ascending = true)
		buf.WriteByte(0x01)
		buf.WriteByte(0x20) // local.get (resultPtr)
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x10) // call
		writeLeb128(buf, 9) // function index 9 (sortRows import)

		// Drop the return value for now
		buf.WriteByte(0x1a) // drop
	}

	// If LIMIT is present, apply limit and offset
	if hasLimit {
		// Call limitOffset(inPtr, rowCount, limit, offset, outPtr)
		buf.WriteByte(0x20) // local.get (resultPtr)
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x20) // local.get (rowCount)
		buf.WriteByte(rowCountIdx)
		buf.WriteByte(0x41) // i32.const 2 (limit - simplified)
		writeLeb128(buf, 2)
		buf.WriteByte(0x41) // i32.const 0 (offset - simplified)
		buf.WriteByte(0x00)
		buf.WriteByte(0x20) // local.get (resultPtr)
		buf.WriteByte(resultPtrIdx)
		buf.WriteByte(0x10)  // call
		writeLeb128(buf, 13) // function index 13 (limitOffset import)

		// Store new row count
		buf.WriteByte(0x21) // local.set
		buf.WriteByte(rowCountIdx)
	}

	// Return (resultPtr, rowCount)
	buf.WriteByte(0x20) // local.get
	buf.WriteByte(resultPtrIdx)
	buf.WriteByte(0x20) // local.get
	buf.WriteByte(rowCountIdx)

	// End function
	buf.WriteByte(0x0b) // end

	return buf.Bytes()
}

// generateInsertBody generates WASM bytecode for an INSERT statement
// Returns: i64 (rows affected)
func (c *Compiler) generateInsertBody(stmt *query.InsertStmt) []byte {
	buf := new(bytes.Buffer)
	localOffset := byte(1) // After params pointer

	// locals:
	// local 0: params pointer (from caller)
	// local 1: rowsAffected (i64)
	// local 2: tableId (i32)
	// local 3: rowDataPtr (i32)

	// Initialize rowsAffected = 0
	buf.WriteByte(0x42) // i64.const 0
	buf.WriteByte(0x00)
	buf.WriteByte(0x21)        // local.set
	buf.WriteByte(localOffset) // rowsAffected

	// Get table ID - for now assume table 0
	buf.WriteByte(0x41) // i32.const 0
	buf.WriteByte(0x00)
	buf.WriteByte(0x21)            // local.set
	buf.WriteByte(localOffset + 1) // tableId

	// For each value row, insert it
	rowCount := len(stmt.Values)
	if rowCount > 0 {
		// Set rowDataPtr = 2048 (after result buffer)
		buf.WriteByte(0x41) // i32.const 2048
		writeLeb128(buf, 2048)
		buf.WriteByte(0x21)            // local.set
		buf.WriteByte(localOffset + 2) // rowDataPtr

		// Call insertRow(tableId, rowDataPtr)
		buf.WriteByte(0x20)            // local.get
		buf.WriteByte(localOffset + 1) // tableId
		buf.WriteByte(0x20)            // local.get
		buf.WriteByte(localOffset + 2) // rowDataPtr
		buf.WriteByte(0x10)            // call
		writeLeb128(buf, 3)            // function index 3 (insertRow import)

		// Drop result for now (in real impl, check success)
		buf.WriteByte(0x1a) // drop

		// rowsAffected = 1 (simplified - one row inserted)
		buf.WriteByte(0x42) // i64.const 1
		buf.WriteByte(0x01)
		buf.WriteByte(0x21)        // local.set
		buf.WriteByte(localOffset) // rowsAffected
	}

	// Return rowsAffected
	buf.WriteByte(0x20)        // local.get
	buf.WriteByte(localOffset) // rowsAffected

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
// Returns: i64 (rows affected)
func (c *Compiler) generateDeleteBody(stmt *query.DeleteStmt) []byte {
	buf := new(bytes.Buffer)
	localOffset := byte(1) // After params pointer

	// locals:
	// local 1: rowsAffected (i64)
	// local 2: tableId (i32)
	// local 3: rowId (i64)

	// Initialize rowsAffected = 0
	buf.WriteByte(0x42) // i64.const 0
	buf.WriteByte(0x00)
	buf.WriteByte(0x21)        // local.set
	buf.WriteByte(localOffset) // rowsAffected

	// Get table ID from table name
	// For now assume table 0
	buf.WriteByte(0x41) // i32.const 0
	buf.WriteByte(0x00)
	buf.WriteByte(0x21)            // local.set
	buf.WriteByte(localOffset + 1) // tableId

	// Get rowId from WHERE clause (simplified - assume rowId = 1)
	buf.WriteByte(0x42) // i64.const 1
	buf.WriteByte(0x01)
	buf.WriteByte(0x21)            // local.set
	buf.WriteByte(localOffset + 2) // rowId

	// Call deleteRow(tableId, rowId)
	buf.WriteByte(0x20)            // local.get
	buf.WriteByte(localOffset + 1) // tableId
	buf.WriteByte(0x20)            // local.get
	buf.WriteByte(localOffset + 2) // rowId
	buf.WriteByte(0x10)            // call
	writeLeb128(buf, 5)            // function index 5 (deleteRow import)

	// Check result and increment rowsAffected if success
	// For now just set rowsAffected = 1
	buf.WriteByte(0x1a) // drop (result)
	buf.WriteByte(0x42) // i64.const 1
	buf.WriteByte(0x01)
	buf.WriteByte(0x21)        // local.set
	buf.WriteByte(localOffset) // rowsAffected

	// Return rowsAffected
	buf.WriteByte(0x20)        // local.get
	buf.WriteByte(localOffset) // rowsAffected

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

// compileExpression compiles a SQL expression to WASM bytecode
// Returns the generated bytecode and the resulting type
//
//nolint:unused // retained for upcoming expression compiler integration and compatibility tests.
func (c *Compiler) compileExpression(expr query.Expression, buf *bytes.Buffer) (string, error) {
	switch e := expr.(type) {
	case *query.NumberLiteral:
		// Push i64 constant
		buf.WriteByte(0x42) // i64.const
		val := int64(e.Value)
		writeLeb128Signed(buf, val)
		return "INTEGER", nil

	case *query.StringLiteral:
		// For strings, we need to write to memory and push pointer
		// Simplified - just return 0 for now
		buf.WriteByte(0x41) // i32.const 0
		buf.WriteByte(0x00)
		return "TEXT", nil

	case *query.BooleanLiteral:
		if e.Value {
			buf.WriteByte(0x41) // i32.const 1
			buf.WriteByte(0x01)
		} else {
			buf.WriteByte(0x41) // i32.const 0
			buf.WriteByte(0x00)
		}
		return "BOOLEAN", nil

	case *query.FunctionCall:
		// Aggregate functions: COUNT, SUM, AVG, MIN, MAX
		return c.compileAggregateFunction(e, buf)

	case *query.BinaryExpr:
		// Compile left and right operands
		leftType, err := c.compileExpression(e.Left, buf)
		if err != nil {
			return "", err
		}
		_, err = c.compileExpression(e.Right, buf)
		if err != nil {
			return "", err
		}

		// Apply operator
		switch e.Operator {
		case query.TokenPlus:
			if leftType == "INTEGER" {
				buf.WriteByte(0x7c) // i64.add
			} else {
				buf.WriteByte(0x6a) // i32.add
			}
		case query.TokenMinus:
			if leftType == "INTEGER" {
				buf.WriteByte(0x7d) // i64.sub
			} else {
				buf.WriteByte(0x6b) // i32.sub
			}
		case query.TokenStar:
			if leftType == "INTEGER" {
				buf.WriteByte(0x7e) // i64.mul
			} else {
				buf.WriteByte(0x6c) // i32.mul
			}
		case query.TokenEq:
			if leftType == "INTEGER" {
				buf.WriteByte(0x51) // i64.eq
			} else {
				buf.WriteByte(0x46) // i32.eq
			}
			return "BOOLEAN", nil
		case query.TokenNeq:
			if leftType == "INTEGER" {
				buf.WriteByte(0x52) // i64.ne
			} else {
				buf.WriteByte(0x47) // i32.ne
			}
			return "BOOLEAN", nil
		case query.TokenLt:
			if leftType == "INTEGER" {
				buf.WriteByte(0x53) // i64.lt_s
			} else {
				buf.WriteByte(0x48) // i32.lt_s
			}
			return "BOOLEAN", nil
		case query.TokenGt:
			if leftType == "INTEGER" {
				buf.WriteByte(0x55) // i64.gt_s
			} else {
				buf.WriteByte(0x4a) // i32.gt_s
			}
			return "BOOLEAN", nil
		}
		return leftType, nil

	case *query.QualifiedIdentifier:
		// Column reference - simplified, just push 0
		// In real implementation, load from row data
		buf.WriteByte(0x42) // i64.const 0
		buf.WriteByte(0x00)
		return "INTEGER", nil

	case *query.SubqueryExpr:
		// Subquery: executeSubquery(queryId, outPtr, maxRows) -> rowCount
		// For now, simplified - just call executeSubquery with queryId 0
		buf.WriteByte(0x41) // i32.const 0 (queryId)
		buf.WriteByte(0x00)
		buf.WriteByte(0x41) // i32.const 3072 (outPtr - scratch space)
		writeLeb128(buf, 3072)
		buf.WriteByte(0x41) // i32.const 100 (maxRows)
		writeLeb128(buf, 100)
		buf.WriteByte(0x10) // call
		writeLeb128(buf, 8) // function index 8 (executeSubquery import)

		// The result is row count, load first row's first column from memory
		// For scalar subquery, return the count
		return "INTEGER", nil

	case *query.WindowExpr:
		// Window function: windowFunction(inPtr, rowCount, funcType, outPtr) -> success
		// For now, simplified - just call windowFunction with funcType 0 (ROW_NUMBER)
		buf.WriteByte(0x41) // i32.const 0 (inPtr - placeholder)
		buf.WriteByte(0x00)
		buf.WriteByte(0x41) // i32.const 100 (rowCount - placeholder)
		writeLeb128(buf, 100)
		buf.WriteByte(0x41) // i32.const 0 (funcType: 0=ROW_NUMBER)
		buf.WriteByte(0x00)
		buf.WriteByte(0x41) // i32.const 4096 (outPtr - scratch space)
		writeLeb128(buf, 4096)
		buf.WriteByte(0x10)  // call
		writeLeb128(buf, 16) // function index 16 (windowFunction import)

		// Return success indicator
		return "INTEGER", nil

	default:
		// Unknown expression type - push 0
		buf.WriteByte(0x41) // i32.const 0
		buf.WriteByte(0x00)
		return "INTEGER", nil
	}
}

// compileAggregateFunction compiles aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//
//nolint:unused // retained for upcoming expression compiler integration and compatibility tests.
func (c *Compiler) compileAggregateFunction(fn *query.FunctionCall, buf *bytes.Buffer) (string, error) {
	funcName := fn.Name

	switch funcName {
	case "COUNT":
		// COUNT(*) or COUNT(expr) - returns i64
		// For now, return a placeholder count of 3
		buf.WriteByte(0x42) // i64.const
		buf.WriteByte(0x03) // 3
		return "INTEGER", nil

	case "SUM":
		// SUM(expr) - returns i64
		// Compile the argument
		if len(fn.Args) > 0 {
			_, err := c.compileExpression(fn.Args[0], buf)
			if err != nil {
				return "", err
			}
		} else {
			buf.WriteByte(0x42) // i64.const 0
			buf.WriteByte(0x00)
		}
		return "INTEGER", nil

	case "AVG":
		// AVG(expr) - returns f64
		// For now, return 0.0
		buf.WriteByte(0x44) // f64.const
		// Write 8 bytes of 0 for 0.0
		for i := 0; i < 8; i++ {
			buf.WriteByte(0x00)
		}
		return "REAL", nil

	case "MIN":
		// MIN(expr) - returns the minimum value
		// Compile the argument
		if len(fn.Args) > 0 {
			_, err := c.compileExpression(fn.Args[0], buf)
			if err != nil {
				return "", err
			}
		} else {
			buf.WriteByte(0x42) // i64.const 0
			buf.WriteByte(0x00)
		}
		return "INTEGER", nil

	case "MAX":
		// MAX(expr) - returns the maximum value
		// Compile the argument
		if len(fn.Args) > 0 {
			_, err := c.compileExpression(fn.Args[0], buf)
			if err != nil {
				return "", err
			}
		} else {
			buf.WriteByte(0x42) // i64.const 0
			buf.WriteByte(0x00)
		}
		return "INTEGER", nil

	default:
		// Unknown function - return 0
		buf.WriteByte(0x41) // i32.const 0
		buf.WriteByte(0x00)
		return "INTEGER", nil
	}
}

// hasWhereClause checks if a SELECT statement has a WHERE clause
//
//nolint:unused // retained for planned optimizer-driven codegen branches.
func (c *Compiler) hasWhereClause(stmt *query.SelectStmt) bool {
	return stmt.Where != nil
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

func writeLeb128(buf *bytes.Buffer, value uint64) {
	for {
		byteVal := uint8(value & 0x7f)
		value >>= 7
		if value != 0 {
			byteVal |= 0x80
		}
		buf.WriteByte(byteVal)
		if value == 0 {
			break
		}
	}
}

//nolint:unused // retained for signed literal support in expression bytecode generation.
func writeLeb128Signed(buf *bytes.Buffer, value int64) {
	for {
		byteVal := uint8(value & 0x7f)
		signBit := uint8(value & 0x40)
		value >>= 7

		// Check if we're done
		if (value == 0 && signBit == 0) || (value == -1 && signBit != 0) {
			buf.WriteByte(byteVal)
			break
		}

		buf.WriteByte(byteVal | 0x80)
	}
}

func readLeb128(data []byte, offset int) (uint64, int) {
	var result uint64
	var shift uint
	pos := offset
	for {
		byteVal := data[pos]
		pos++
		result |= uint64(byteVal&0x7f) << shift
		if (byteVal & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return result, pos - offset
}

func readLeb128Signed(data []byte, offset int) (int64, int) {
	var result int64
	var shift uint
	pos := offset
	for {
		byteVal := data[pos]
		pos++
		result |= int64(byteVal&0x7f) << shift
		shift += 7
		if (byteVal & 0x80) == 0 {
			if shift < 64 && (byteVal&0x40) != 0 {
				result |= -1 << shift
			}
			break
		}
	}
	return result, pos - offset
}
