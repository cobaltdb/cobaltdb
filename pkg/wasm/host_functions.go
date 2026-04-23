package wasm

import (
	"encoding/binary"
)

// HostFunctions provides database operations for WASM runtime
// This is a simplified version for testing - full integration with Catalog
// will be implemented in a future update
type HostFunctions struct {
	// Mock data store for testing
	tables map[string][]map[string]interface{}
	// Transaction state
	txActive    bool
	txSavepoint int
	txLog       []TxOperation
	// User-defined functions
	udfs map[string]UserDefinedFunction
	// Partition information for parallel query execution
	partitions map[string][]Partition // table -> partitions
}

// Partition represents a data partition for parallel scanning
type Partition struct {
	ID        int                    // Partition ID (0-based)
	TableName string                 // Table this partition belongs to
	StartRow  int                    // Starting row index (inclusive)
	EndRow    int                    // Ending row index (exclusive)
	Metadata  map[string]interface{} // Additional partition metadata
}

// UserDefinedFunction represents a custom function that can be called from SQL
type UserDefinedFunction struct {
	Name       string
	ParamCount int
	// Function implementation - takes params, returns result
	// In real implementation, this would execute WASM bytecode
	Fn func(args []interface{}) (interface{}, error)
}

// TxOperation represents a transaction operation for rollback
type TxOperation struct {
	Type     string // "insert", "update", "delete"
	Table    string
	Row      map[string]interface{}
	RowIndex int // for update/delete
}

// NewHostFunctions creates a new host function provider
func NewHostFunctions() *HostFunctions {
	hf := &HostFunctions{
		tables:     make(map[string][]map[string]interface{}),
		udfs:       make(map[string]UserDefinedFunction),
		partitions: make(map[string][]Partition),
	}
	// Add some test data
	hf.tables["test"] = []map[string]interface{}{
		{"id": int64(1), "name": "Alice", "category": "A"},
		{"id": int64(2), "name": "Bob", "category": "B"},
		{"id": int64(3), "name": "Charlie", "category": "A"},
	}
	// Create partitions for test table (2 partitions)
	hf.partitions["test"] = []Partition{
		{ID: 0, TableName: "test", StartRow: 0, EndRow: 2, Metadata: map[string]interface{}{"shard": "A"}},
		{ID: 1, TableName: "test", StartRow: 2, EndRow: 3, Metadata: map[string]interface{}{"shard": "B"}},
	}
	// Register built-in UDFs
	hf.registerBuiltinUDFs()
	return hf
}

// registerBuiltinUDFs registers built-in user-defined functions
func (h *HostFunctions) registerBuiltinUDFs() {
	// Square function: returns x^2
	h.udfs["SQUARE"] = UserDefinedFunction{
		Name:       "SQUARE",
		ParamCount: 1,
		Fn: func(args []interface{}) (interface{}, error) {
			if len(args) < 1 {
				return nil, nil
			}
			switch v := args[0].(type) {
			case int64:
				return v * v, nil
			case float64:
				return v * v, nil
			default:
				return nil, nil
			}
		},
	}

	// Cube function: returns x^3
	h.udfs["CUBE"] = UserDefinedFunction{
		Name:       "CUBE",
		ParamCount: 1,
		Fn: func(args []interface{}) (interface{}, error) {
			if len(args) < 1 {
				return nil, nil
			}
			switch v := args[0].(type) {
			case int64:
				return v * v * v, nil
			case float64:
				return v * v * v, nil
			default:
				return nil, nil
			}
		},
	}

	// Absolute value function
	h.udfs["ABS_VAL"] = UserDefinedFunction{
		Name:       "ABS_VAL",
		ParamCount: 1,
		Fn: func(args []interface{}) (interface{}, error) {
			if len(args) < 1 {
				return nil, nil
			}
			switch v := args[0].(type) {
			case int64:
				if v < 0 {
					return -v, nil
				}
				return v, nil
			case float64:
				if v < 0 {
					return -v, nil
				}
				return v, nil
			default:
				return nil, nil
			}
		},
	}

	// Power function: returns x^y
	h.udfs["POWER_INT"] = UserDefinedFunction{
		Name:       "POWER_INT",
		ParamCount: 2,
		Fn: func(args []interface{}) (interface{}, error) {
			if len(args) < 2 {
				return nil, nil
			}
			var base int64
			var exp int64
			switch v := args[0].(type) {
			case int64:
				base = v
			case float64:
				base = int64(v)
			default:
				return nil, nil
			}
			switch v := args[1].(type) {
			case int64:
				exp = v
			case float64:
				exp = int64(v)
			default:
				return nil, nil
			}
			result := int64(1)
			for i := int64(0); i < exp; i++ {
				result *= base
			}
			return result, nil
		},
	}
}

// RegisterUDF registers a user-defined function
func (h *HostFunctions) RegisterUDF(name string, udf UserDefinedFunction) error {
	h.udfs[name] = udf
	return nil
}

// GetUDF retrieves a user-defined function by name
func (h *HostFunctions) GetUDF(name string) (UserDefinedFunction, bool) {
	udf, ok := h.udfs[name]
	return udf, ok
}

// RegisterAll registers all host functions with the runtime
func (h *HostFunctions) RegisterAll(rt *Runtime) {
	rt.RegisterImport("env", "tableScan", h.tableScan)
	rt.RegisterImport("env", "innerJoin", h.innerJoin)
	rt.RegisterImport("env", "leftJoin", h.leftJoin)
	rt.RegisterImport("env", "rightJoin", h.rightJoin)
	rt.RegisterImport("env", "fullJoin", h.fullJoin)
	rt.RegisterImport("env", "executeSubquery", h.executeSubquery)
	rt.RegisterImport("env", "sortRows", h.sortRows)
	rt.RegisterImport("env", "limitOffset", h.limitOffset)
	rt.RegisterImport("env", "distinctRows", h.distinctRows)
	rt.RegisterImport("env", "unionResults", h.unionResults)
	rt.RegisterImport("env", "exceptResults", h.exceptResults)
	rt.RegisterImport("env", "intersectResults", h.intersectResults)
	rt.RegisterImport("env", "windowFunction", h.windowFunction)
	rt.RegisterImport("env", "insertRow", h.insertRow)
	rt.RegisterImport("env", "updateRow", h.updateRow)
	rt.RegisterImport("env", "deleteRow", h.deleteRow)
	rt.RegisterImport("env", "getTableId", h.getTableId)
	rt.RegisterImport("env", "getColumnOffset", h.getColumnOffset)
	rt.RegisterImport("env", "groupBy", h.groupBy)
	rt.RegisterImport("env", "indexScan", h.indexScan)
	rt.RegisterImport("env", "bindParameter", h.bindParameter)
	rt.RegisterImport("env", "executeCorrelatedSubquery", h.executeCorrelatedSubquery)
	rt.RegisterImport("env", "fetchChunk", h.fetchChunk)
	rt.RegisterImport("env", "beginTransaction", h.beginTransaction)
	rt.RegisterImport("env", "commitTransaction", h.commitTransaction)
	rt.RegisterImport("env", "rollbackTransaction", h.rollbackTransaction)
	rt.RegisterImport("env", "savepoint", h.savepoint)
	rt.RegisterImport("env", "rollbackToSavepoint", h.rollbackToSavepoint)
	rt.RegisterImport("env", "executeUDF", h.executeUDF)
	rt.RegisterImport("env", "getPartitionCount", h.getPartitionCount)
	rt.RegisterImport("env", "partitionScan", h.partitionScan)
	rt.RegisterImport("env", "parallelAggregate", h.parallelAggregate)
	rt.RegisterImport("env", "repartitionTable", h.repartitionTable)
	rt.RegisterImport("env", "vectorizedAdd", h.vectorizedAdd)
	rt.RegisterImport("env", "vectorizedMultiply", h.vectorizedMultiply)
	rt.RegisterImport("env", "vectorizedCompare", h.vectorizedCompare)
	rt.RegisterImport("env", "vectorizedSum", h.vectorizedSum)
	rt.RegisterImport("env", "vectorizedMinMax", h.vectorizedMinMax)
	rt.RegisterImport("env", "vectorizedFilter", h.vectorizedFilter)
	rt.RegisterImport("env", "vectorizedBatchCopy", h.vectorizedBatchCopy)
	rt.RegisterImport("env", "getQueryMetrics", h.getQueryMetrics)
	rt.RegisterImport("env", "getMemoryStats", h.getMemoryStats)
	rt.RegisterImport("env", "resetMetrics", h.resetMetrics)
	rt.RegisterImport("env", "logProfilingEvent", h.logProfilingEvent)
	rt.RegisterImport("env", "getOpcodeStats", h.getOpcodeStats)
}

// rightJoin performs a RIGHT OUTER JOIN between two tables
// Params: [leftTableId, rightTableId, outPtr, maxRows] -> Returns: rowCount
func (h *HostFunctions) rightJoin(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	leftTableId := int(params[0])
	rightTableId := int(params[1])
	outPtr := int32(params[2])
	maxRows := int(params[3])

	_ = leftTableId
	_ = rightTableId

	// Simplified: return all right rows with matching left rows
	// For RIGHT JOIN, all right rows appear even if no match
	leftRows := h.tables["test"]
	rightRows := h.tables["test"]

	rowCount := 0
	offset := outPtr

	for _, rightRow := range rightRows {
		matched := false
		for _, leftRow := range leftRows {
			if rowCount >= maxRows {
				break
			}
			// Simple match condition: id == id
			leftId, _ := leftRow["id"].(int64)
			rightId, _ := rightRow["id"].(int64)
			if leftId == rightId {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(leftId))
				offset += 8
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(rightId))
				offset += 8
				rowCount++
				matched = true
				break
			}
		}
		// If no match, still include right row with NULL left (simplified: 0)
		if !matched && rowCount < maxRows {
			binary.LittleEndian.PutUint64(rt.Memory[offset:], 0) // NULL marker
			offset += 8
			if id, ok := rightRow["id"].(int64); ok {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
			}
			offset += 8
			rowCount++
		}
	}

	return []uint64{uint64(rowCount)}, nil
}

// fullJoin performs a FULL OUTER JOIN between two tables
// Params: [leftTableId, rightTableId, outPtr, maxRows] -> Returns: rowCount
func (h *HostFunctions) fullJoin(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	leftTableId := int(params[0])
	rightTableId := int(params[1])
	outPtr := int32(params[2])
	maxRows := int(params[3])

	_ = leftTableId
	_ = rightTableId

	// Simplified: return all left rows and all right rows
	// For FULL JOIN, all rows from both tables appear
	leftRows := h.tables["test"]
	rightRows := h.tables["test"]

	rowCount := 0
	offset := outPtr

	// First, add all matching rows (like INNER JOIN)
	matchedLeft := make(map[int]bool)
	matchedRight := make(map[int]bool)

	for li, leftRow := range leftRows {
		for ri, rightRow := range rightRows {
			if rowCount >= maxRows {
				break
			}
			leftId, _ := leftRow["id"].(int64)
			rightId, _ := rightRow["id"].(int64)
			if leftId == rightId {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(leftId))
				offset += 8
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(rightId))
				offset += 8
				rowCount++
				matchedLeft[li] = true
				matchedRight[ri] = true
			}
		}
	}

	// Add unmatched left rows
	for li, leftRow := range leftRows {
		if rowCount >= maxRows {
			break
		}
		if !matchedLeft[li] {
			if id, ok := leftRow["id"].(int64); ok {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
			}
			offset += 8
			binary.LittleEndian.PutUint64(rt.Memory[offset:], 0) // NULL marker
			offset += 8
			rowCount++
		}
	}

	// Add unmatched right rows
	for ri, rightRow := range rightRows {
		if rowCount >= maxRows {
			break
		}
		if !matchedRight[ri] {
			binary.LittleEndian.PutUint64(rt.Memory[offset:], 0) // NULL marker
			offset += 8
			if id, ok := rightRow["id"].(int64); ok {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
			}
			offset += 8
			rowCount++
		}
	}

	return []uint64{uint64(rowCount)}, nil
}

// leftJoin performs a LEFT OUTER JOIN between two tables
// Params: [leftTableId, rightTableId, outPtr, maxRows] -> Returns: rowCount
func (h *HostFunctions) leftJoin(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	leftTableId := int(params[0])
	rightTableId := int(params[1])
	outPtr := int32(params[2])
	maxRows := int(params[3])

	_ = leftTableId
	_ = rightTableId

	// Simplified: return all left rows with matching right rows
	// For LEFT JOIN, all left rows appear even if no match
	leftRows := h.tables["test"]
	rightRows := h.tables["test"]

	rowCount := 0
	offset := outPtr

	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			if rowCount >= maxRows {
				break
			}
			// Simple match condition: id == id
			leftId, _ := leftRow["id"].(int64)
			rightId, _ := rightRow["id"].(int64)
			if leftId == rightId {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(leftId))
				offset += 8
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(rightId))
				offset += 8
				rowCount++
				matched = true
				break // One match per left row for simplicity
			}
		}
		// If no match, still include left row with NULL right (simplified: 0)
		if !matched && rowCount < maxRows {
			if id, ok := leftRow["id"].(int64); ok {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
			}
			offset += 8
			binary.LittleEndian.PutUint64(rt.Memory[offset:], 0) // NULL marker
			offset += 8
			rowCount++
		}
	}

	return []uint64{uint64(rowCount)}, nil
}

// innerJoin performs an inner join between two tables
// Params: [leftTableId, rightTableId, outPtr, maxRows] -> Returns: rowCount
func (h *HostFunctions) innerJoin(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	leftTableId := int(params[0])
	rightTableId := int(params[1])
	outPtr := int32(params[2])
	maxRows := int(params[3])

	_ = leftTableId
	_ = rightTableId

	// Simplified: return cartesian product of test tables
	leftRows := h.tables["test"]
	rightRows := h.tables["test"]

	rowCount := 0
	offset := outPtr

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			if rowCount >= maxRows {
				break
			}
			// Write left id and right id
			if id, ok := leftRow["id"].(int64); ok {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
			}
			offset += 8
			if id, ok := rightRow["id"].(int64); ok {
				binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
			}
			offset += 8
			rowCount++
		}
	}

	return []uint64{uint64(rowCount)}, nil
}

// tableScan scans a table and writes rows to WASM memory
// Params: [tableId, outPtr, maxRows] -> Returns: rowCount
func (h *HostFunctions) tableScan(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	tableId := int(params[0])
	outPtr := int32(params[1])
	maxRows := int(params[2])

	// Get table by ID (simplified - just use "test" for id 0)
	tableName := "test"
	if tableId != 0 {
		return []uint64{0}, nil
	}

	rows := h.tables[tableName]
	if len(rows) > maxRows {
		rows = rows[:maxRows]
	}

	// Write rows to WASM memory
	// Each row: id (int64) = 8 bytes
	offset := outPtr
	for _, row := range rows {
		// Write id (int64)
		if id, ok := row["id"].(int64); ok {
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
		}
		offset += 8
		// Note: name is not written since schema only has id column
	}

	return []uint64{uint64(len(rows))}, nil
}

// insertRow inserts a row into a table
// Params: [tableId, rowDataPtr] -> Returns: success (1 or 0)
func (h *HostFunctions) insertRow(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 2 {
		return []uint64{0}, nil
	}

	// Simplified - just return success
	return []uint64{1}, nil
}

// updateRow updates a row in a table
// Params: [tableId, rowId, rowDataPtr] -> Returns: success (1 or 0)
func (h *HostFunctions) updateRow(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	// Simplified - just return success
	return []uint64{1}, nil
}

// deleteRow deletes a row from a table
// Params: [tableId, rowId] -> Returns: success (1 or 0)
func (h *HostFunctions) deleteRow(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 2 {
		return []uint64{0}, nil
	}

	// Simplified - just return success
	return []uint64{1}, nil
}

// getTableId gets the ID of a table by name
// Params: [namePtr, nameLen] -> Returns: tableId (-1 if not found)
func (h *HostFunctions) getTableId(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 2 {
		return []uint64{^uint64(0)}, nil // -1 as unsigned
	}

	namePtr := int32(params[0])
	nameLen := int(params[1])

	// Read table name from memory
	if int(namePtr)+nameLen > len(rt.Memory) {
		return []uint64{^uint64(0)}, nil
	}

	tableName := string(rt.Memory[namePtr : namePtr+int32(nameLen)])

	// Return 0 for "test" table
	if tableName == "test" {
		return []uint64{0}, nil
	}

	return []uint64{^uint64(0)}, nil // -1 as unsigned
}

// getColumnOffset gets the byte offset of a column in a row
// Params: [tableId, columnIdx] -> Returns: offset
func (h *HostFunctions) getColumnOffset(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 2 {
		return []uint64{0}, nil
	}

	_ = int(params[0]) // tableId
	columnIdx := int(params[1])

	// Simplified - assume each column is 8 bytes
	offset := columnIdx * 8

	return []uint64{uint64(offset)}, nil
}

// executeSubquery executes a subquery and returns the result
// Params: [queryId, outPtr, maxRows] -> Returns: rowCount
func (h *HostFunctions) executeSubquery(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	_ = int(params[0]) // queryId - identifies which subquery to execute
	outPtr := int32(params[1])
	maxRows := int(params[2])

	// Simplified: return count from test table
	rows := h.tables["test"]
	rowCount := len(rows)
	if rowCount > maxRows {
		rowCount = maxRows
	}

	// Write row count to memory
	binary.LittleEndian.PutUint64(rt.Memory[outPtr:], uint64(rowCount))

	// Write row ids
	offset := outPtr + 8
	for i := 0; i < rowCount; i++ {
		if id, ok := rows[i]["id"].(int64); ok {
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
		}
		offset += 8
	}

	return []uint64{uint64(rowCount)}, nil
}

// sortRows sorts rows by a column
// Params: [inPtr, rowCount, columnIdx, ascending, outPtr] -> Returns: sortedRowCount
func (h *HostFunctions) sortRows(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 5 {
		return []uint64{0}, nil
	}

	inPtr := int32(params[0])
	rowCount := int(params[1])
	_ = int(params[2]) // columnIdx
	_ = params[3] != 0 // ascending
	outPtr := int32(params[4])

	if rowCount <= 0 {
		return []uint64{0}, nil
	}

	// Copy input to output (simplified - no actual sorting for now)
	// In real implementation, would sort by specified column
	for i := 0; i < rowCount; i++ {
		srcOffset := inPtr + int32(i*8)
		dstOffset := outPtr + int32(i*8)

		// Copy row id
		val := binary.LittleEndian.Uint64(rt.Memory[srcOffset:])
		binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], val)
	}

	return []uint64{uint64(rowCount)}, nil
}

// limitOffset applies LIMIT and OFFSET to result set
// Params: [inPtr, rowCount, limit, offset, outPtr] -> Returns: newRowCount
func (h *HostFunctions) limitOffset(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 5 {
		return []uint64{0}, nil
	}

	inPtr := int32(params[0])
	rowCount := int(params[1])
	limit := int(params[2])
	offsetVal := int(params[3])
	outPtr := int32(params[4])

	if rowCount <= 0 || limit <= 0 {
		return []uint64{0}, nil
	}

	// Apply offset
	startIdx := offsetVal
	if startIdx > rowCount {
		startIdx = rowCount
	}

	// Apply limit
	endIdx := startIdx + limit
	if endIdx > rowCount {
		endIdx = rowCount
	}

	// Copy rows
	newRowCount := endIdx - startIdx
	for i := 0; i < newRowCount; i++ {
		srcOffset := inPtr + int32((startIdx+i)*8)
		dstOffset := outPtr + int32(i*8)

		val := binary.LittleEndian.Uint64(rt.Memory[srcOffset:])
		binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], val)
	}

	return []uint64{uint64(newRowCount)}, nil
}

// distinctRows removes duplicate rows from result set
// Params: [inPtr, rowCount, rowSize, outPtr] -> Returns: distinctRowCount
func (h *HostFunctions) distinctRows(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	inPtr := int32(params[0])
	rowCount := int(params[1])
	rowSize := int(params[2])
	outPtr := int32(params[3])

	if rowCount <= 0 || rowSize <= 0 {
		return []uint64{0}, nil
	}

	// Simplified: just copy input to output without deduplication
	// In real implementation, would track seen rows and filter duplicates
	distinctCount := rowCount
	for i := 0; i < rowCount; i++ {
		srcOffset := inPtr + int32(i*rowSize)
		dstOffset := outPtr + int32(i*rowSize)

		// Copy row data
		for j := 0; j < rowSize; j++ {
			rt.Memory[dstOffset+int32(j)] = rt.Memory[srcOffset+int32(j)]
		}
	}

	return []uint64{uint64(distinctCount)}, nil
}

// unionResults combines two result sets (UNION operation)
// Params: [leftPtr, leftCount, rightPtr, rightCount, outPtr] -> Returns: totalRowCount
func (h *HostFunctions) unionResults(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 5 {
		return []uint64{0}, nil
	}

	leftPtr := int32(params[0])
	leftCount := int(params[1])
	rightPtr := int32(params[2])
	rightCount := int(params[3])
	outPtr := int32(params[4])

	// Copy left results
	for i := 0; i < leftCount; i++ {
		srcOffset := leftPtr + int32(i*8)
		dstOffset := outPtr + int32(i*8)
		val := binary.LittleEndian.Uint64(rt.Memory[srcOffset:])
		binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], val)
	}

	// Copy right results
	for i := 0; i < rightCount; i++ {
		srcOffset := rightPtr + int32(i*8)
		dstOffset := outPtr + int32((leftCount+i)*8)
		val := binary.LittleEndian.Uint64(rt.Memory[srcOffset:])
		binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], val)
	}

	totalCount := leftCount + rightCount
	return []uint64{uint64(totalCount)}, nil
}

// exceptResults returns rows in left but not in right (EXCEPT operation)
// Params: [leftPtr, leftCount, rightPtr, rightCount, outPtr] -> Returns: resultRowCount
func (h *HostFunctions) exceptResults(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 5 {
		return []uint64{0}, nil
	}

	leftPtr := int32(params[0])
	leftCount := int(params[1])
	_ = int32(params[2]) // rightPtr
	_ = int(params[3])   // rightCount
	outPtr := int32(params[4])

	// Simplified: just copy left results (no actual EXCEPT logic for now)
	for i := 0; i < leftCount; i++ {
		srcOffset := leftPtr + int32(i*8)
		dstOffset := outPtr + int32(i*8)
		val := binary.LittleEndian.Uint64(rt.Memory[srcOffset:])
		binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], val)
	}

	return []uint64{uint64(leftCount)}, nil
}

// intersectResults returns rows common to both sets (INTERSECT operation)
// Params: [leftPtr, leftCount, rightPtr, rightCount, outPtr] -> Returns: resultRowCount
func (h *HostFunctions) intersectResults(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 5 {
		return []uint64{0}, nil
	}

	leftPtr := int32(params[0])
	leftCount := int(params[1])
	_ = int32(params[2]) // rightPtr
	_ = int(params[3])   // rightCount
	outPtr := int32(params[4])

	// Simplified: just copy left results (no actual INTERSECT logic for now)
	for i := 0; i < leftCount; i++ {
		srcOffset := leftPtr + int32(i*8)
		dstOffset := outPtr + int32(i*8)
		val := binary.LittleEndian.Uint64(rt.Memory[srcOffset:])
		binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], val)
	}

	return []uint64{uint64(leftCount)}, nil
}

// windowFunction computes window functions like ROW_NUMBER, RANK, LAG, LEAD, etc.
// Params: [inPtr, rowCount, funcType, outPtr, arg1, arg2] -> Returns: success
// funcType: 0=ROW_NUMBER, 1=RANK, 2=DENSE_RANK, 3=LAG, 4=LEAD, 5=FIRST_VALUE, 6=LAST_VALUE
//
//	10=SUM, 11=AVG, 12=MIN, 13=MAX, 14=COUNT
//
// arg1: offset for LAG/LEAD (default 1), or input column pointer for aggregates
// arg2: default value for LAG/LEAD (0 if not specified)
func (h *HostFunctions) windowFunction(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	inPtr := int32(params[0])
	rowCount := int(params[1])
	funcType := int(params[2])
	outPtr := int32(params[3])

	if rowCount <= 0 {
		return []uint64{0}, nil
	}

	// Optional parameters with defaults
	arg1 := int64(1) // default offset for LAG/LEAD
	arg2 := int64(0) // default value for LAG/LEAD
	if len(params) >= 5 {
		arg1 = int64(params[4])
	}
	if len(params) >= 6 {
		arg2 = int64(params[5])
	}

	_ = inPtr // Input pointer - used for partition-aware functions in full implementation

	switch funcType {
	case 0: // ROW_NUMBER
		// Assign sequential row numbers starting from 1
		for i := 0; i < rowCount; i++ {
			dstOffset := outPtr + int32(i*8)
			binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(i+1))
		}
	case 1: // RANK
		// Simplified: same as ROW_NUMBER for now
		for i := 0; i < rowCount; i++ {
			dstOffset := outPtr + int32(i*8)
			binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(i+1))
		}
	case 2: // DENSE_RANK
		// Simplified: same as ROW_NUMBER for now
		for i := 0; i < rowCount; i++ {
			dstOffset := outPtr + int32(i*8)
			binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(i+1))
		}
	case 3: // LAG
		// LAG(value, offset, default) - access previous row
		offset := int(arg1)
		for i := 0; i < rowCount; i++ {
			dstOffset := outPtr + int32(i*8)
			srcIdx := i - offset
			if srcIdx >= 0 && srcIdx < rowCount {
				// Read value from source row
				srcOffset := inPtr + int32(srcIdx*8)
				val := binary.LittleEndian.Uint64(rt.Memory[srcOffset:])
				binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], val)
			} else {
				// Use default value
				binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(arg2))
			}
		}
	case 4: // LEAD
		// LEAD(value, offset, default) - access next row
		offset := int(arg1)
		for i := 0; i < rowCount; i++ {
			dstOffset := outPtr + int32(i*8)
			srcIdx := i + offset
			if srcIdx >= 0 && srcIdx < rowCount {
				// Read value from source row
				srcOffset := inPtr + int32(srcIdx*8)
				val := binary.LittleEndian.Uint64(rt.Memory[srcOffset:])
				binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], val)
			} else {
				// Use default value
				binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(arg2))
			}
		}
	case 5: // FIRST_VALUE
		// FIRST_VALUE(value) - value from first row in window
		if rowCount > 0 {
			firstOffset := inPtr
			firstVal := binary.LittleEndian.Uint64(rt.Memory[firstOffset:])
			for i := 0; i < rowCount; i++ {
				dstOffset := outPtr + int32(i*8)
				binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], firstVal)
			}
		}
	case 6: // LAST_VALUE
		// LAST_VALUE(value) - value from last row in window
		if rowCount > 0 {
			lastOffset := inPtr + int32((rowCount-1)*8)
			lastVal := binary.LittleEndian.Uint64(rt.Memory[lastOffset:])
			for i := 0; i < rowCount; i++ {
				dstOffset := outPtr + int32(i*8)
				binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], lastVal)
			}
		}
	case 10: // SUM (running/cumulative)
		// Running sum over the window
		var sum int64 = 0
		for i := 0; i < rowCount; i++ {
			srcOffset := inPtr + int32(i*8)
			val := int64(binary.LittleEndian.Uint64(rt.Memory[srcOffset:]))
			sum += val
			dstOffset := outPtr + int32(i*8)
			binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(sum))
		}
	case 11: // AVG (running/cumulative)
		// Running average over the window
		var sum int64 = 0
		for i := 0; i < rowCount; i++ {
			srcOffset := inPtr + int32(i*8)
			val := int64(binary.LittleEndian.Uint64(rt.Memory[srcOffset:]))
			sum += val
			avg := sum / int64(i+1)
			dstOffset := outPtr + int32(i*8)
			binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(avg))
		}
	case 12: // MIN (running)
		// Running minimum over the window
		if rowCount > 0 {
			srcOffset := inPtr
			minVal := int64(binary.LittleEndian.Uint64(rt.Memory[srcOffset:]))
			binary.LittleEndian.PutUint64(rt.Memory[outPtr:], uint64(minVal))
			for i := 1; i < rowCount; i++ {
				srcOffset := inPtr + int32(i*8)
				val := int64(binary.LittleEndian.Uint64(rt.Memory[srcOffset:]))
				if val < minVal {
					minVal = val
				}
				dstOffset := outPtr + int32(i*8)
				binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(minVal))
			}
		}
	case 13: // MAX (running)
		// Running maximum over the window
		if rowCount > 0 {
			srcOffset := inPtr
			maxVal := int64(binary.LittleEndian.Uint64(rt.Memory[srcOffset:]))
			binary.LittleEndian.PutUint64(rt.Memory[outPtr:], uint64(maxVal))
			for i := 1; i < rowCount; i++ {
				srcOffset := inPtr + int32(i*8)
				val := int64(binary.LittleEndian.Uint64(rt.Memory[srcOffset:]))
				if val > maxVal {
					maxVal = val
				}
				dstOffset := outPtr + int32(i*8)
				binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(maxVal))
			}
		}
	case 14: // COUNT (running)
		// Running count over the window
		for i := 0; i < rowCount; i++ {
			dstOffset := outPtr + int32(i*8)
			binary.LittleEndian.PutUint64(rt.Memory[dstOffset:], uint64(i+1))
		}
	}

	return []uint64{1}, nil
}

// executeCorrelatedSubquery executes a subquery with access to outer query row
// Params: [queryId, outerRowPtr, outerRowSize, outPtr, maxRows] -> Returns: rowCount
// The outerRowPtr points to the current outer query row data that the subquery can reference
func (h *HostFunctions) executeCorrelatedSubquery(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 5 {
		return []uint64{0}, nil
	}

	_ = int(params[0])   // queryId - identifies which subquery to execute
	_ = int32(params[1]) // outerRowPtr - pointer to outer query row data
	_ = int(params[2])   // outerRowSize - size of outer row in bytes
	outPtr := int32(params[3])
	maxRows := int(params[4])

	// Simplified: return count from test table (correlated logic would use outerRow data)
	rows := h.tables["test"]
	rowCount := len(rows)
	if rowCount > maxRows {
		rowCount = maxRows
	}

	// Write row count to memory
	binary.LittleEndian.PutUint64(rt.Memory[outPtr:], uint64(rowCount))

	// Write row ids
	offset := outPtr + 8
	for i := 0; i < rowCount; i++ {
		if id, ok := rows[i]["id"].(int64); ok {
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
		}
		offset += 8
	}

	return []uint64{uint64(rowCount)}, nil
}

// groupBy groups rows by a column value and returns group count
// Params: [tableId, groupColumnIdx, outPtr] -> Returns: groupCount
func (h *HostFunctions) groupBy(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	tableId := int(params[0])
	groupColumnIdx := int(params[1])
	outPtr := int32(params[2])

	// Get table by ID
	tableName := "test"
	if tableId != 0 {
		return []uint64{0}, nil
	}

	rows := h.tables[tableName]

	// Group by category (simplified - hardcoded for test data)
	// In real implementation, would use groupColumnIdx to determine grouping
	_ = groupColumnIdx

	// Count unique groups (A and B = 2 groups)
	groups := make(map[string]int)
	for _, row := range rows {
		if cat, ok := row["category"].(string); ok {
			groups[cat]++
		}
	}

	// Write group info to memory: [groupCount, group1Count, group2Count, ...]
	groupCount := len(groups)
	binary.LittleEndian.PutUint64(rt.Memory[outPtr:], uint64(groupCount))

	// Write group counts
	offset := outPtr + 8
	for _, count := range groups {
		binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(count))
		offset += 8
	}

	return []uint64{uint64(groupCount)}, nil
}

// fetchChunk fetches a chunk of rows for streaming results
// Params: [startRow, rowCount, outPtr] -> Returns: actualRowCount
func (h *HostFunctions) fetchChunk(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	_ = int(params[0])   // startRow
	_ = int(params[1])   // rowCount
	_ = int32(params[2]) // outPtr

	// Simplified: return rows from test table
	// In full implementation, would fetch specific chunk from storage
	rows := h.tables["test"]
	return []uint64{uint64(len(rows))}, nil
}

// indexScan scans a table using an index for faster lookups
// Params: [tableId, indexId, minVal, maxVal, outPtr, maxRows] -> Returns: rowCount
// For equality lookups, set minVal = maxVal = target value
// For range scans, set minVal and maxVal accordingly
// Use minVal = -infinity and maxVal = +infinity for full index scan
func (h *HostFunctions) indexScan(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 6 {
		return []uint64{0}, nil
	}

	tableId := int(params[0])
	_ = int(params[1])   // indexId - which index to use
	_ = int64(params[2]) // minVal - minimum value for range
	_ = int64(params[3]) // maxVal - maximum value for range
	outPtr := int32(params[4])
	maxRows := int(params[5])

	// Get table by ID (simplified - just use "test" for id 0)
	tableName := "test"
	if tableId != 0 {
		return []uint64{0}, nil
	}

	rows := h.tables[tableName]
	if len(rows) > maxRows {
		rows = rows[:maxRows]
	}

	// Write rows to WASM memory
	// Each row: id (int64) = 8 bytes
	offset := outPtr
	for _, row := range rows {
		// Write id (int64)
		if id, ok := row["id"].(int64); ok {
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
		}
		offset += 8
	}

	return []uint64{uint64(len(rows))}, nil
}

// bindParameter binds a parameter value to a prepared statement slot
// Params: [slotIdx, valuePtr, valueType] -> Returns: success (1 or 0)
// valueType: 0=i32, 1=i64, 2=f32, 3=f64
func (h *HostFunctions) bindParameter(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	slotIdx := int(params[0])
	valuePtr := int32(params[1])
	valueType := int(params[2])

	// Read value from memory based on type
	var value interface{}
	switch valueType {
	case 0: // i32
		value = int32(binary.LittleEndian.Uint32(rt.Memory[valuePtr:]))
	case 1: // i64
		value = int64(binary.LittleEndian.Uint64(rt.Memory[valuePtr:]))
	case 2: // f32
		value = float32(binary.LittleEndian.Uint32(rt.Memory[valuePtr:]))
	case 3: // f64
		bits := binary.LittleEndian.Uint64(rt.Memory[valuePtr:])
		value = float64(bits)
	}

	// Store in parameter slots (simplified - would use actual prepared statement context)
	_ = slotIdx
	_ = value

	return []uint64{1}, nil
}

// beginTransaction starts a new transaction
// Params: [] -> Returns: success (1 or 0)
func (h *HostFunctions) beginTransaction(rt *Runtime, params []uint64) ([]uint64, error) {
	if h.txActive {
		return []uint64{0}, nil // Already in transaction
	}
	h.txActive = true
	h.txSavepoint = 0
	h.txLog = make([]TxOperation, 0)
	return []uint64{1}, nil
}

// commitTransaction commits the current transaction
// Params: [] -> Returns: success (1 or 0)
func (h *HostFunctions) commitTransaction(rt *Runtime, params []uint64) ([]uint64, error) {
	if !h.txActive {
		return []uint64{0}, nil // No active transaction
	}
	h.txActive = false
	h.txSavepoint = 0
	h.txLog = nil // Clear log
	return []uint64{1}, nil
}

// rollbackTransaction rolls back the current transaction
// Params: [] -> Returns: success (1 or 0)
func (h *HostFunctions) rollbackTransaction(rt *Runtime, params []uint64) ([]uint64, error) {
	if !h.txActive {
		return []uint64{0}, nil // No active transaction
	}

	// Rollback operations in reverse order
	for i := len(h.txLog) - 1; i >= 0; i-- {
		op := h.txLog[i]
		switch op.Type {
		case "insert":
			// Remove inserted row
			if rows, ok := h.tables[op.Table]; ok && len(rows) > 0 {
				h.tables[op.Table] = rows[:len(rows)-1]
			}
		case "delete":
			// Restore deleted row
			h.tables[op.Table] = append(h.tables[op.Table], op.Row)
		case "update":
			// Restore original row
			if rows, ok := h.tables[op.Table]; ok && op.RowIndex < len(rows) {
				rows[op.RowIndex] = op.Row
			}
		}
	}

	h.txActive = false
	h.txSavepoint = 0
	h.txLog = nil
	return []uint64{1}, nil
}

// savepoint creates a savepoint within the current transaction
// Params: [savepointId] -> Returns: success (1 or 0)
func (h *HostFunctions) savepoint(rt *Runtime, params []uint64) ([]uint64, error) {
	if !h.txActive {
		return []uint64{0}, nil // No active transaction
	}
	if len(params) < 1 {
		return []uint64{0}, nil
	}
	h.txSavepoint = int(params[0])
	return []uint64{1}, nil
}

// rollbackToSavepoint rolls back to a specific savepoint
// Params: [savepointId] -> Returns: success (1 or 0)
func (h *HostFunctions) rollbackToSavepoint(rt *Runtime, params []uint64) ([]uint64, error) {
	if !h.txActive {
		return []uint64{0}, nil // No active transaction
	}
	if len(params) < 1 {
		return []uint64{0}, nil
	}

	targetSavepoint := int(params[0])

	// In a real implementation, each operation would track savepoint metadata.
	// For now we only keep the value to preserve behavior without stale loop code.
	_ = targetSavepoint

	return []uint64{1}, nil
}

// executeUDF executes a user-defined function
// Params: [funcNamePtr, funcNameLen, argsPtr, argCount] -> Returns: result value
func (h *HostFunctions) executeUDF(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	funcNamePtr := int32(params[0])
	funcNameLen := int(params[1])
	argsPtr := int32(params[2])
	argCount := int(params[3])

	// Read function name from memory
	if int(funcNamePtr)+funcNameLen > len(rt.Memory) {
		return []uint64{0}, nil
	}

	// Look up UDF
	udf, ok := h.udfs[string(rt.Memory[funcNamePtr:funcNamePtr+int32(funcNameLen)])]
	if !ok {
		return []uint64{0}, nil // Function not found
	}

	// Read arguments from memory (simplified - assumes int64 args)
	args := make([]interface{}, argCount)
	for i := 0; i < argCount && i < udf.ParamCount; i++ {
		argOffset := argsPtr + int32(i*8)
		if argOffset+8 <= int32(len(rt.Memory)) {
			val := binary.LittleEndian.Uint64(rt.Memory[argOffset:])
			args[i] = int64(val)
		}
	}

	// Execute UDF
	result, err := udf.Fn(args)
	if err != nil {
		return []uint64{0}, nil
	}

	// Return result (simplified - assumes int64 result)
	switch v := result.(type) {
	case int64:
		return []uint64{uint64(v)}, nil
	case int:
		return []uint64{uint64(v)}, nil
	default:
		return []uint64{0}, nil
	}
}

// getPartitionCount returns the number of partitions for a table
// Params: [tableNamePtr, tableNameLen] -> Returns: partitionCount
func (h *HostFunctions) getPartitionCount(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 2 {
		return []uint64{0}, nil
	}

	tableNamePtr := int32(params[0])
	tableNameLen := int(params[1])

	// Read table name from memory
	if int(tableNamePtr)+tableNameLen > len(rt.Memory) {
		return []uint64{0}, nil
	}

	// Get partition count
	partitions, ok := h.partitions[string(rt.Memory[tableNamePtr:tableNamePtr+int32(tableNameLen)])]
	if !ok {
		// Table not partitioned - return 1 (single implicit partition)
		return []uint64{1}, nil
	}

	return []uint64{uint64(len(partitions))}, nil
}

// partitionScan scans a specific partition of a table
// Params: [tableNamePtr, tableNameLen, partitionId, outPtr, maxRows] -> Returns: rowCount
func (h *HostFunctions) partitionScan(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 5 {
		return []uint64{0}, nil
	}

	tableNamePtr := int32(params[0])
	tableNameLen := int(params[1])
	partitionId := int(params[2])
	outPtr := int32(params[3])
	maxRows := int(params[4])

	// Read table name from memory
	if int(tableNamePtr)+tableNameLen > len(rt.Memory) {
		return []uint64{0}, nil
	}
	tableName := string(rt.Memory[tableNamePtr : tableNamePtr+int32(tableNameLen)])

	// Get partition info
	partitions, ok := h.partitions[tableName]
	if !ok {
		// Table not partitioned - scan entire table as one partition
		return h.scanTableRows(rt, tableName, 0, len(h.tables[tableName]), outPtr, maxRows)
	}

	if partitionId < 0 || partitionId >= len(partitions) {
		return []uint64{0}, nil // Invalid partition ID
	}

	partition := partitions[partitionId]
	return h.scanTableRows(rt, tableName, partition.StartRow, partition.EndRow, outPtr, maxRows)
}

// scanTableRows is a helper to scan rows from a table
func (h *HostFunctions) scanTableRows(rt *Runtime, tableName string, startRow, endRow int, outPtr int32, maxRows int) ([]uint64, error) {
	rows, ok := h.tables[tableName]
	if !ok {
		return []uint64{0}, nil
	}

	// Clamp endRow to actual table size
	if endRow > len(rows) {
		endRow = len(rows)
	}
	if startRow > len(rows) {
		startRow = len(rows)
	}

	rowCount := 0
	offset := outPtr

	for i := startRow; i < endRow && rowCount < maxRows; i++ {
		row := rows[i]

		// Write row id
		if id, ok := row["id"].(int64); ok {
			binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(id))
		} else {
			binary.LittleEndian.PutUint64(rt.Memory[offset:], 0)
		}
		offset += 8

		rowCount++
	}

	return []uint64{uint64(rowCount)}, nil
}

// parallelAggregate performs aggregation across all partitions in parallel
// Params: [tableNamePtr, tableNameLen, aggType, columnNamePtr, columnNameLen, outPtr] -> Returns: success (1 or 0)
// aggType: 0=COUNT, 1=SUM, 2=AVG, 3=MIN, 4=MAX
func (h *HostFunctions) parallelAggregate(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 6 {
		return []uint64{0}, nil
	}

	tableNamePtr := int32(params[0])
	tableNameLen := int(params[1])
	aggType := int(params[2])
	columnNamePtr := int32(params[3])
	columnNameLen := int(params[4])
	outPtr := int32(params[5])

	// Read table name
	if int(tableNamePtr)+tableNameLen > len(rt.Memory) {
		return []uint64{0}, nil
	}
	tableName := string(rt.Memory[tableNamePtr : tableNamePtr+int32(tableNameLen)])

	// Read column name
	if int(columnNamePtr)+columnNameLen > len(rt.Memory) {
		return []uint64{0}, nil
	}
	columnName := string(rt.Memory[columnNamePtr : columnNamePtr+int32(columnNameLen)])

	// Get rows (across all partitions)
	rows, ok := h.tables[tableName]
	if !ok {
		return []uint64{0}, nil
	}

	// Perform aggregation
	var result int64
	switch aggType {
	case 0: // COUNT
		result = int64(len(rows))
	case 1: // SUM
		for _, row := range rows {
			if val, ok := row[columnName].(int64); ok {
				result += val
			}
		}
	case 2: // AVG
		var sum int64
		var count int64
		for _, row := range rows {
			if val, ok := row[columnName].(int64); ok {
				sum += val
				count++
			}
		}
		if count > 0 {
			result = sum / count
		}
	case 3: // MIN
		if len(rows) > 0 {
			result = int64(1<<63 - 1)
			for _, row := range rows {
				if val, ok := row[columnName].(int64); ok {
					if val < result {
						result = val
					}
				}
			}
		}
	case 4: // MAX
		if len(rows) > 0 {
			result = int64(-1 << 63)
			for _, row := range rows {
				if val, ok := row[columnName].(int64); ok {
					if val > result {
						result = val
					}
				}
			}
		}
	}

	// Write result to memory
	binary.LittleEndian.PutUint64(rt.Memory[outPtr:], uint64(result))

	return []uint64{1}, nil
}

// repartitionTable redistributes table data across partitions
// Params: [tableNamePtr, tableNameLen, partitionCount] -> Returns: success (1 or 0)
func (h *HostFunctions) repartitionTable(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	tableNamePtr := int32(params[0])
	tableNameLen := int(params[1])
	partitionCount := int(params[2])

	if partitionCount < 1 || partitionCount > 100 {
		return []uint64{0}, nil // Invalid partition count
	}

	// Read table name
	if int(tableNamePtr)+tableNameLen > len(rt.Memory) {
		return []uint64{0}, nil
	}
	tableName := string(rt.Memory[tableNamePtr : tableNamePtr+int32(tableNameLen)])

	// Get table rows
	rows, ok := h.tables[tableName]
	if !ok {
		return []uint64{0}, nil
	}

	// Create new partitions
	newPartitions := make([]Partition, partitionCount)
	rowsPerPartition := len(rows) / partitionCount
	extraRows := len(rows) % partitionCount

	startRow := 0
	for i := 0; i < partitionCount; i++ {
		partitionSize := rowsPerPartition
		if i < extraRows {
			partitionSize++ // Distribute extra rows
		}

		newPartitions[i] = Partition{
			ID:        i,
			TableName: tableName,
			StartRow:  startRow,
			EndRow:    startRow + partitionSize,
			Metadata:  map[string]interface{}{"created": "repartitioned"},
		}

		startRow += partitionSize
	}

	h.partitions[tableName] = newPartitions

	return []uint64{1}, nil
}

// vectorizedAdd performs SIMD-style batch addition
// Params: [inPtr1, inPtr2, outPtr, count] -> Returns: success (1 or 0)
// Each element is 8 bytes (int64), processes 'count' elements in bulk
func (h *HostFunctions) vectorizedAdd(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	inPtr1 := int32(params[0])
	inPtr2 := int32(params[1])
	outPtr := int32(params[2])
	count := int(params[3])

	if count <= 0 || count > 10000 {
		return []uint64{0}, nil
	}

	// Check bounds
	maxOffset := int32(count * 8)
	if int(inPtr1)+int(maxOffset) > len(rt.Memory) ||
		int(inPtr2)+int(maxOffset) > len(rt.Memory) ||
		int(outPtr)+int(maxOffset) > len(rt.Memory) {
		return []uint64{0}, nil
	}

	// Vectorized addition
	for i := 0; i < count; i++ {
		offset := int32(i * 8)
		val1 := int64(binary.LittleEndian.Uint64(rt.Memory[inPtr1+offset:]))
		val2 := int64(binary.LittleEndian.Uint64(rt.Memory[inPtr2+offset:]))
		result := val1 + val2
		binary.LittleEndian.PutUint64(rt.Memory[outPtr+offset:], uint64(result))
	}

	return []uint64{1}, nil
}

// vectorizedMultiply performs SIMD-style batch multiplication
// Params: [inPtr1, inPtr2, outPtr, count] -> Returns: success (1 or 0)
func (h *HostFunctions) vectorizedMultiply(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	inPtr1 := int32(params[0])
	inPtr2 := int32(params[1])
	outPtr := int32(params[2])
	count := int(params[3])

	if count <= 0 || count > 10000 {
		return []uint64{0}, nil
	}

	maxOffset := int32(count * 8)
	if int(inPtr1)+int(maxOffset) > len(rt.Memory) ||
		int(inPtr2)+int(maxOffset) > len(rt.Memory) ||
		int(outPtr)+int(maxOffset) > len(rt.Memory) {
		return []uint64{0}, nil
	}

	for i := 0; i < count; i++ {
		offset := int32(i * 8)
		val1 := int64(binary.LittleEndian.Uint64(rt.Memory[inPtr1+offset:]))
		val2 := int64(binary.LittleEndian.Uint64(rt.Memory[inPtr2+offset:]))
		result := val1 * val2
		binary.LittleEndian.PutUint64(rt.Memory[outPtr+offset:], uint64(result))
	}

	return []uint64{1}, nil
}

// vectorizedCompare performs SIMD-style batch comparison
// Params: [inPtr1, inPtr2, outPtr, count, op] -> Returns: success (1 or 0)
// op: 0=eq, 1=ne, 2=lt, 3=le, 4=gt, 5=ge
// Output: 1 for true, 0 for false (int64 per element)
func (h *HostFunctions) vectorizedCompare(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 5 {
		return []uint64{0}, nil
	}

	inPtr1 := int32(params[0])
	inPtr2 := int32(params[1])
	outPtr := int32(params[2])
	count := int(params[3])
	op := int(params[4])

	if count <= 0 || count > 10000 {
		return []uint64{0}, nil
	}

	maxOffset := int32(count * 8)
	if int(inPtr1)+int(maxOffset) > len(rt.Memory) ||
		int(inPtr2)+int(maxOffset) > len(rt.Memory) ||
		int(outPtr)+int(maxOffset) > len(rt.Memory) {
		return []uint64{0}, nil
	}

	for i := 0; i < count; i++ {
		offset := int32(i * 8)
		val1 := int64(binary.LittleEndian.Uint64(rt.Memory[inPtr1+offset:]))
		val2 := int64(binary.LittleEndian.Uint64(rt.Memory[inPtr2+offset:]))

		var result int64
		switch op {
		case 0: // eq
			if val1 == val2 {
				result = 1
			}
		case 1: // ne
			if val1 != val2 {
				result = 1
			}
		case 2: // lt
			if val1 < val2 {
				result = 1
			}
		case 3: // le
			if val1 <= val2 {
				result = 1
			}
		case 4: // gt
			if val1 > val2 {
				result = 1
			}
		case 5: // ge
			if val1 >= val2 {
				result = 1
			}
		}

		binary.LittleEndian.PutUint64(rt.Memory[outPtr+offset:], uint64(result))
	}

	return []uint64{1}, nil
}

// vectorizedSum computes sum of all elements (reduction operation)
// Params: [inPtr, count] -> Returns: sum value
func (h *HostFunctions) vectorizedSum(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 2 {
		return []uint64{0}, nil
	}

	inPtr := int32(params[0])
	count := int(params[1])

	if count <= 0 || count > 10000 {
		return []uint64{0}, nil
	}

	if int(inPtr)+int(count*8) > len(rt.Memory) {
		return []uint64{0}, nil
	}

	var sum int64
	for i := 0; i < count; i++ {
		offset := int32(i * 8)
		val := int64(binary.LittleEndian.Uint64(rt.Memory[inPtr+offset:]))
		sum += val
	}

	return []uint64{uint64(sum)}, nil
}

// vectorizedMinMax finds min and max values (reduction operation)
// Params: [inPtr, count, outMinPtr, outMaxPtr] -> Returns: success (1 or 0)
func (h *HostFunctions) vectorizedMinMax(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	inPtr := int32(params[0])
	count := int(params[1])
	outMinPtr := int32(params[2])
	outMaxPtr := int32(params[3])

	if count <= 0 || count > 10000 {
		return []uint64{0}, nil
	}

	if int(inPtr)+int(count*8) > len(rt.Memory) ||
		int(outMinPtr)+8 > len(rt.Memory) ||
		int(outMaxPtr)+8 > len(rt.Memory) {
		return []uint64{0}, nil
	}

	minVal := int64(^uint64(0) >> 1) // Max int64
	maxVal := int64(-1 << 63)        // Min int64

	for i := 0; i < count; i++ {
		offset := int32(i * 8)
		val := int64(binary.LittleEndian.Uint64(rt.Memory[inPtr+offset:]))
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
	}

	binary.LittleEndian.PutUint64(rt.Memory[outMinPtr:], uint64(minVal))
	binary.LittleEndian.PutUint64(rt.Memory[outMaxPtr:], uint64(maxVal))

	return []uint64{1}, nil
}

// vectorizedFilter filters elements based on a predicate mask
// Params: [inPtr, maskPtr, outPtr, count] -> Returns: filtered count
// mask: 1 = include, 0 = exclude
func (h *HostFunctions) vectorizedFilter(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 4 {
		return []uint64{0}, nil
	}

	inPtr := int32(params[0])
	maskPtr := int32(params[1])
	outPtr := int32(params[2])
	count := int(params[3])

	if count <= 0 || count > 10000 {
		return []uint64{0}, nil
	}

	if int(inPtr)+int(count*8) > len(rt.Memory) ||
		int(maskPtr)+int(count*8) > len(rt.Memory) ||
		int(outPtr)+int(count*8) > len(rt.Memory) {
		return []uint64{0}, nil
	}

	filteredCount := 0
	for i := 0; i < count; i++ {
		offset := int32(i * 8)
		mask := binary.LittleEndian.Uint64(rt.Memory[maskPtr+offset:])
		if mask != 0 {
			// Include this element
			val := binary.LittleEndian.Uint64(rt.Memory[inPtr+offset:])
			outOffset := int32(filteredCount * 8)
			binary.LittleEndian.PutUint64(rt.Memory[outPtr+outOffset:], val)
			filteredCount++
		}
	}

	return []uint64{uint64(filteredCount)}, nil
}

// vectorizedBatchCopy copies a batch of elements
// Params: [srcPtr, dstPtr, count] -> Returns: success (1 or 0)
func (h *HostFunctions) vectorizedBatchCopy(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	srcPtr := int32(params[0])
	dstPtr := int32(params[1])
	count := int(params[2])

	if count <= 0 || count > 10000 {
		return []uint64{0}, nil
	}

	byteCount := count * 8
	if int(srcPtr)+byteCount > len(rt.Memory) ||
		int(dstPtr)+byteCount > len(rt.Memory) {
		return []uint64{0}, nil
	}

	copy(rt.Memory[dstPtr:dstPtr+int32(byteCount)], rt.Memory[srcPtr:srcPtr+int32(byteCount)])

	return []uint64{1}, nil
}

// getQueryMetrics returns query execution metrics
// Params: [outPtr] -> Returns: success (1 or 0)
// Writes metrics to memory as: [totalExecs, totalTime, minTime, maxTime, avgTime] (all int64)
func (h *HostFunctions) getQueryMetrics(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 1 {
		return []uint64{0}, nil
	}

	outPtr := int32(params[0])

	// Check bounds
	if int(outPtr)+40 > len(rt.Memory) {
		return []uint64{0}, nil
	}

	// Write sample metrics (in real implementation, these would come from profiler)
	metrics := []int64{100, 5000000, 10000, 200000, 50000} // execs, total, min, max, avg (ns)
	offset := outPtr
	for _, m := range metrics {
		binary.LittleEndian.PutUint64(rt.Memory[offset:], uint64(m))
		offset += 8
	}

	return []uint64{1}, nil
}

// getMemoryStats returns memory usage statistics
// Params: [outPtr] -> Returns: success (1 or 0)
// Writes: [totalMemory, usedMemory, peakMemory, allocationCount]
func (h *HostFunctions) getMemoryStats(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 1 {
		return []uint64{0}, nil
	}

	outPtr := int32(params[0])

	if int(outPtr)+32 > len(rt.Memory) {
		return []uint64{0}, nil
	}

	totalMemory := len(rt.Memory)
	usedMemory := 0 // Would track actual usage in real implementation
	peakMemory := totalMemory / 2
	allocCount := int64(42)

	binary.LittleEndian.PutUint64(rt.Memory[outPtr:], uint64(totalMemory))
	binary.LittleEndian.PutUint64(rt.Memory[outPtr+8:], uint64(usedMemory))
	binary.LittleEndian.PutUint64(rt.Memory[outPtr+16:], uint64(peakMemory))
	binary.LittleEndian.PutUint64(rt.Memory[outPtr+24:], uint64(allocCount))

	return []uint64{1}, nil
}

// resetMetrics resets all performance metrics
// Params: [] -> Returns: success (1 or 0)
func (h *HostFunctions) resetMetrics(rt *Runtime, params []uint64) ([]uint64, error) {
	// Reset all counters (simplified - in real impl would clear profiler state)
	return []uint64{1}, nil
}

// logProfilingEvent logs a profiling event
// Params: [eventType, duration, rowCount] -> Returns: success (1 or 0)
// eventType: 0=query_start, 1=query_end, 2=host_call, 3=memory_alloc
func (h *HostFunctions) logProfilingEvent(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 3 {
		return []uint64{0}, nil
	}

	_ = int(params[0])   // eventType
	_ = int64(params[1]) // duration
	_ = int(params[2])   // rowCount

	// In real implementation, would log to profiler
	return []uint64{1}, nil
}

// getOpcodeStats returns opcode execution statistics
// Params: [outPtr, maxOpcodes] -> Returns: count of opcodes reported
func (h *HostFunctions) getOpcodeStats(rt *Runtime, params []uint64) ([]uint64, error) {
	if len(params) < 2 {
		return []uint64{0}, nil
	}

	outPtr := int32(params[0])
	maxOpcodes := int(params[1])

	if maxOpcodes <= 0 || maxOpcodes > 256 {
		return []uint64{0}, nil
	}

	if int(outPtr)+maxOpcodes*16 > len(rt.Memory) {
		return []uint64{0}, nil
	}

	// Write sample opcode stats (opcode byte + count)
	// Format: [opcode:1 byte, padding:7 bytes, count:8 bytes]
	for i := 0; i < maxOpcodes && i < 10; i++ {
		offset := outPtr + int32(i*16)
		rt.Memory[offset] = byte(0x20 + i) // Sample opcodes
		binary.LittleEndian.PutUint64(rt.Memory[offset+8:], uint64(1000*(i+1)))
	}

	return []uint64{uint64(min(maxOpcodes, 10))}, nil
}

// min helper
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
