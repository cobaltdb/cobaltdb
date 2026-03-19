# CobaltDB WebAssembly (WASM) System

## Overview

The WASM system provides a complete WebAssembly compilation and execution environment for SQL queries. It compiles SQL statements to WASM bytecode and executes them in a stack-based virtual machine with host function integration.

## Architecture

```
SQL Query → Parser → AST → Compiler → WASM Bytecode → Runtime → Execution
                                              ↓
                                       Host Functions
                                       (tableScan, insertRow, etc.)
```

## Components

### 1. Compiler (`compiler.go`)

Compiles SQL AST to WASM bytecode.

**Supported Statements:**
- `SELECT` - Query data with table scanning
- `SELECT DISTINCT` - Remove duplicate rows
- `SELECT ... WHERE` - Filtered queries with WHERE clause
- `SELECT ... GROUP BY` - Grouped aggregation with COUNT, SUM, AVG, MIN, MAX
- `SELECT ... HAVING` - Post-aggregation filtering
- `SELECT ... ORDER BY` - Sort results
- `SELECT ... LIMIT/OFFSET` - Limit result set size
- `SELECT ... INNER JOIN` - Inner join between tables
- `SELECT ... LEFT JOIN` - Left outer join
- `SELECT ... RIGHT JOIN` - Right outer join
- `SELECT ... FULL JOIN` - Full outer join
- `SELECT ... UNION` - Combine results from multiple queries
- `SELECT ... EXCEPT` - Rows in first query but not in second
- `SELECT ... INTERSECT` - Rows common to both queries
- `SELECT ... ROW_NUMBER()` - Window functions for analytics
- `SELECT ... LAG()/LEAD()` - Access previous/next row values
- `SELECT ... FIRST_VALUE()/LAST_VALUE()` - Window value functions
- `SELECT ... SUM() OVER ()` - Running aggregate functions
- `SELECT ... AVG() OVER ()` - Running average
- `SELECT ... MIN() OVER ()` - Running minimum
- `SELECT ... MAX() OVER ()` - Running maximum
- `SELECT ... COUNT() OVER ()` - Running count
- `SELECT ... (subquery)` - Scalar subqueries in SELECT list
- `SELECT ... WHERE EXISTS (subquery)` - Correlated subqueries with outer query references
- `INSERT` - Insert rows into tables
- `UPDATE` - Update existing rows
- `DELETE` - Delete rows from tables
- `PREPARE` - Prepared statements with parameter binding
- Partitioned Queries - Parallel scan and aggregation across table partitions

**WHERE Clause Support:**
- Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
- Arithmetic in predicates: `WHERE id + 1 = 2`
- Multiple conditions: `AND`, `OR` (partial)

**Aggregate Functions:**
- `COUNT(*)` - Count rows
- `SUM(expr)` - Sum values
- `AVG(expr)` - Average value
- `MIN(expr)` - Minimum value
- `MAX(expr)` - Maximum value

**Expression Compilation:**
- Literals: Numbers, Strings, Booleans
- Arithmetic: `+`, `-`, `*`
- Comparisons: `=`, `!=`, `<`, `>`
- Column references (simplified)
- Function calls: Aggregate functions

**Generated WASM Sections:**
- Type Section - Function signatures
- Import Section - Host function imports
- Function Section - Function declarations
- Memory Section - Linear memory (64KB pages)
- Export Section - Exported entry points
- Code Section - Function bodies

### 2. Runtime (`runtime.go`)

Stack-based WASM interpreter.

**Features:**
- Complete WASM module parsing
- Type section parsing
- Function section parsing
- Import section with name tracking
- Memory management (64KB pages)
- Call stack management
- Operand stack operations
- Import function dispatch

**Supported Opcodes:**
- Control: `unreachable`, `nop`, `block`, `loop`, `if`, `else`, `end`, `br`, `br_if`, `return`, `call`
- Parametric: `drop`, `select`
- Variable: `local.get`, `local.set`
- Memory: `i32.load`, `i64.load`, `i32.store`, `i64.store`
- Constants: `i32.const`, `i64.const`, `f64.const`
- Arithmetic: `i32.add`, `i32.sub`, `i32.mul`, `i64.add`, `i64.sub`, `i64.mul`
- Comparison: `i32.eq`, `i32.ne`, `i32.lt_s`, `i32.gt_s`, `i64.eq`, `i64.ne`, `i64.lt_s`, `i64.gt_s`

### 3. Host Functions (`host_functions.go`)

Bridge between WASM runtime and database operations.

**Available Host Functions:**

| Function | Signature | Description |
|----------|-----------|-------------|
| `tableScan` | `(i32, i32, i32) -> i32` | Scan table, write rows to memory |
| `filterRow` | `(i32, i32) -> i32` | Filter row by predicate (WHERE clause) |
| `insertRow` | `(i32, i32) -> i32` | Insert a row into table |
| `updateRow` | `(i32, i64, i32) -> i32` | Update a row by ID |
| `deleteRow` | `(i32, i64) -> i32` | Delete a row by ID |
| `getTableId` | `(i32, i32) -> i32` | Get table ID by name |
| `getColumnOffset` | `(i32, i32) -> i32` | Get column byte offset |
| `groupBy` | `(i32, i32, i32) -> i32` | Group rows by column, return group count |
| `innerJoin` | `(i32, i32, i32, i32) -> i32` | Inner join between two tables |
| `leftJoin` | `(i32, i32, i32, i32) -> i32` | Left outer join between two tables |
| `rightJoin` | `(i32, i32, i32, i32) -> i32` | Right outer join between two tables |
| `fullJoin` | `(i32, i32, i32, i32) -> i32` | Full outer join between two tables |
| `executeSubquery` | `(i32, i32, i32) -> i32` | Execute a subquery, return row count |
| `executeCorrelatedSubquery` | `(i32, i32, i32, i32, i32) -> i32` | Execute subquery with outer row access |
| `fetchChunk` | `(i32, i32, i32) -> i32` | Fetch a chunk of rows for streaming |
| `sortRows` | `(i32, i32, i32, i32, i32) -> i32` | Sort rows by column |
| `limitOffset` | `(i32, i32, i32, i32, i32) -> i32` | Apply LIMIT and OFFSET to results |
| `distinctRows` | `(i32, i32, i32, i32) -> i32` | Remove duplicate rows from results |
| `unionResults` | `(i32, i32, i32, i32, i32) -> i32` | Combine two result sets (UNION) |
| `exceptResults` | `(i32, i32, i32, i32, i32) -> i32` | Rows in first but not second (EXCEPT) |
| `intersectResults` | `(i32, i32, i32, i32, i32) -> i32` | Rows common to both (INTERSECT) |
| `windowFunction` | `(i32, i32, i32, i32, i64, i64) -> i32` | Compute ROW_NUMBER, LAG, LEAD, SUM OVER, etc. |
| `indexScan` | `(i32, i32, i64, i64, i32, i32) -> i32` | Index-based table scan for fast lookups |
| `bindParameter` | `(i32, i32, i32) -> i32` | Bind parameter value for prepared statements |
| `beginTransaction` | `() -> i32` | Start a new transaction |
| `commitTransaction` | `() -> i32` | Commit the current transaction |
| `rollbackTransaction` | `() -> i32` | Rollback the current transaction |
| `savepoint` | `(i32) -> i32` | Create a savepoint within transaction |
| `rollbackToSavepoint` | `(i32) -> i32` | Rollback to a specific savepoint |
| `executeUDF` | `(i32, i32, i32, i32) -> i64` | Execute a user-defined function |
| `getPartitionCount` | `(i32, i32) -> i32` | Get number of partitions for a table |
| `partitionScan` | `(i32, i32, i32, i32, i32) -> i32` | Scan a specific partition of a table |
| `parallelAggregate` | `(i32, i32, i32, i32, i32, i32) -> i32` | Perform aggregation across all partitions |
| `repartitionTable` | `(i32, i32, i32) -> i32` | Redistribute table data across partitions |
| `vectorizedAdd` | `(i32, i32, i32, i32) -> i32` | SIMD-style element-wise addition |
| `vectorizedMultiply` | `(i32, i32, i32, i32) -> i32` | SIMD-style element-wise multiplication |
| `vectorizedCompare` | `(i32, i32, i32, i32, i32) -> i32` | SIMD-style batch comparison |
| `vectorizedSum` | `(i32, i32) -> i64` | Sum all elements (reduction) |
| `vectorizedMinMax` | `(i32, i32, i32, i32) -> i32` | Find min and max values |
| `vectorizedFilter` | `(i32, i32, i32, i32) -> i32` | Filter elements by predicate mask |
| `vectorizedBatchCopy` | `(i32, i32, i32) -> i32` | Fast bulk memory copy |
| `getQueryMetrics` | `(i32) -> i32` | Get query execution metrics |
| `getMemoryStats` | `(i32) -> i32` | Get memory usage statistics |
| `resetMetrics` | `() -> i32` | Reset all performance metrics |
| `logProfilingEvent` | `(i32, i64, i32) -> i32` | Log a profiling event |
| `getOpcodeStats` | `(i32, i32) -> i32` | Get opcode execution statistics |

### 4. Data Types (`types.go`)

WASM type definitions and helpers.

## Prepared Statements

The WASM compiler supports prepared statements for improved performance and SQL injection prevention.

```go
// Create compiler
compiler := wasm.NewCompiler()

// Prepare statement with parameters
stmt := &query.SelectStmt{
    Columns: []query.Expression{
        &query.QualifiedIdentifier{Table: "users", Column: "id"},
        &query.QualifiedIdentifier{Table: "users", Column: "name"},
    },
    From: &query.TableRef{Name: "users"},
}

prepared, err := compiler.Prepare("SELECT id, name FROM users WHERE id = ?", stmt, 1)
if err != nil {
    log.Fatal(err)
}

// Execute with parameters
compiled, err := compiler.ExecutePrepared(prepared.ID, []interface{}{42})
if err != nil {
    log.Fatal(err)
}

// Execute runtime
rt := wasm.NewRuntime(10)
host := wasm.NewHostFunctions()
host.RegisterAll(rt)

result, err := rt.Execute(compiled, nil)
if err != nil {
    log.Fatal(err)
}

// Clean up when done
compiler.ClosePreparedStatement(prepared.ID)
```

## Usage

### Basic SELECT Query

```go
import (
    "github.com/cobaltdb/cobaltdb/pkg/query"
    "github.com/cobaltdb/cobaltdb/pkg/wasm"
)

// Create compiler
compiler := wasm.NewCompiler()

// Define SELECT statement
stmt := &query.SelectStmt{
    Columns: []query.Expression{
        &query.QualifiedIdentifier{Table: "users", Column: "id"},
        &query.QualifiedIdentifier{Table: "users", Column: "name"},
    },
    From: &query.TableRef{Name: "users"},
}

// Compile to WASM
compiled, err := compiler.CompileQuery("SELECT id, name FROM users", stmt, nil)
if err != nil {
    log.Fatal(err)
}

// Create runtime with host functions
rt := wasm.NewRuntime(10) // 10 pages = 640KB
host := wasm.NewHostFunctions()
host.RegisterAll(rt)

// Execute
result, err := rt.Execute(compiled, nil)
if err != nil {
    log.Fatal(err)
}

// Process results
fmt.Printf("Rows: %d\n", len(result.Rows))
for _, row := range result.Rows {
    fmt.Printf("id=%v, name=%v\n", row.Values[0], row.Values[1])
}
```

### SELECT with WHERE Clause

```go
// Define SELECT with WHERE clause
stmt := &query.SelectStmt{
    Columns: []query.Expression{
        &query.QualifiedIdentifier{Table: "users", Column: "id"},
        &query.QualifiedIdentifier{Table: "users", Column: "name"},
    },
    From: &query.TableRef{Name: "users"},
    Where: &query.BinaryExpr{
        Left:     &query.QualifiedIdentifier{Table: "users", Column: "id"},
        Operator: query.TokenEq,
        Right:    &query.NumberLiteral{Value: 1},
    },
}

compiled, _ := compiler.CompileQuery("SELECT id, name FROM users WHERE id = 1", stmt, nil)
result, _ := rt.Execute(compiled, nil)
fmt.Printf("Filtered rows: %d\n", len(result.Rows))
```

### INSERT Query

```go
stmt := &query.InsertStmt{
    Table:   "users",
    Columns: []string{"id", "name"},
    Values: [][]query.Expression{
        {
            &query.NumberLiteral{Value: 1},
            &query.StringLiteral{Value: "Alice"},
        },
    },
}

compiled, _ := compiler.CompileQuery("INSERT INTO users VALUES (1, 'Alice')", stmt, nil)
result, _ := rt.Execute(compiled, nil)
fmt.Printf("Rows affected: %d\n", result.RowsAffected)
```

### Custom Host Function

```go
rt := wasm.NewRuntime(10)

// Register custom import
rt.RegisterImport("env", "customFunc", func(rt *wasm.Runtime, params []uint64) ([]uint64, error) {
    // Access WASM memory
    data := rt.Memory[params[0]:params[0]+params[1]]

    // Process data...

    // Return results
    return []uint64{42}, nil
})
```

## Memory Layout

```
WASM Linear Memory (64KB pages):
┌─────────────────┐ 0x0000
│   Parameters    │
│   (params ptr)  │
├─────────────────┤ 0x0400 (1024)
│  Result Buffer  │
│  (row data)     │
├─────────────────┤ 0x0800 (2048)
│   Row Data Ptr  │
│  (for inserts)  │
├─────────────────┤ 0x0C00 (3072)
│   Scratch Space │
│                 │
└─────────────────┘ 0xFFFF (64KB per page)
```

## Testing

Run WASM tests:

```bash
# All WASM tests
go test ./pkg/wasm/... -v

# Specific test categories
go test ./pkg/wasm/... -run TestHostFunctions -v
go test ./pkg/wasm/... -run TestCompiler -v
go test ./pkg/wasm/... -run TestRuntime -v
```

## Implementation Details

### Stack Machine Execution

The runtime uses a stack-based execution model:

1. **Operand Stack** - Holds intermediate values during execution
2. **Call Stack** - Tracks function call frames with locals
3. **Linear Memory** - Byte-addressable memory for data storage

### Import Resolution

Import functions are resolved by index at parse time and stored in `importNames` slice. When `call` instruction executes an import:

1. Get function index
2. Lookup import name in `importNames`
3. Find handler in `Imports` map
4. Call handler with runtime and params
5. Push return values to stack

### LEB128 Encoding

WASM uses LEB128 encoding for integers:
- Unsigned LEB128 for section sizes, counts, indices
- Signed LEB128 for constant values

### Expression Compilation

The compiler can compile SQL expressions to WASM bytecode:

```go
expr := &query.BinaryExpr{
    Left:     &query.NumberLiteral{Value: 10},
    Operator: query.TokenPlus,
    Right:    &query.NumberLiteral{Value: 20},
}

buf := new(bytes.Buffer)
typ, _ := compiler.compileExpression(expr, buf)
// Generates: i64.const 10, i64.const 20, i64.add
```

**Supported Operations:**
- Arithmetic: `+`, `-`, `*`
- Comparisons: `=`, `!=`, `<`, `>`
- Literals: numbers, booleans, strings

## Performance Considerations

- **Memory Pages**: Start with minimal pages, grow as needed
- **Stack Size**: Pre-allocated stack with 1024 capacity
- **Call Stack**: Pre-allocated with 64 frame capacity
- **Query Cache**: Compiled queries are cached by SQL text

## Streaming Results

For large result sets, use streaming execution to process rows incrementally:

```go
// Execute query with streaming
streaming, err := rt.ExecuteStreaming(compiled, nil, 1000) // chunk size 1000
if err != nil {
    log.Fatal(err)
}
defer streaming.Close()

// Process chunks
for streaming.HasMore {
    rows, err := streaming.Next()
    if err != nil {
        log.Fatal(err)
    }
    if rows == nil {
        break
    }

    // Process this chunk
    for _, row := range rows {
        fmt.Printf("id=%v\n", row.Values[0])
    }
}
```

## Transaction Support

The WASM runtime supports ACID transactions with savepoints:

```go
// Begin transaction
host.BeginTransaction()

// Perform operations
compiled1, _ := compiler.CompileQuery("INSERT INTO accounts VALUES (1, 100)", stmt1, nil)
rt.Execute(compiled1, nil)

compiled2, _ := compiler.CompileQuery("UPDATE accounts SET balance = balance - 10 WHERE id = 1", stmt2, nil)
rt.Execute(compiled2, nil)

// Create savepoint
host.Savepoint(1)

// More operations...

// Rollback to savepoint if needed
host.RollbackToSavepoint(1)

// Or commit the transaction
host.CommitTransaction()
```

**Transaction Operations:**
- `BeginTransaction()` - Start a new transaction
- `CommitTransaction()` - Commit all changes
- `RollbackTransaction()` - Rollback all changes
- `Savepoint(id)` - Create a named savepoint
- `RollbackToSavepoint(id)` - Rollback to a specific savepoint

## User-Defined Functions (UDF)

The WASM runtime supports custom functions that can be called from SQL:

```go
// Register a custom UDF
host.RegisterUDF("DOUBLE", UserDefinedFunction{
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
})

// Use UDF in SQL
stmt := &query.SelectStmt{
    Columns: []query.Expression{
        &query.FunctionCall{
            Name: "DOUBLE",
            Args: []query.Expression{
                &query.QualifiedIdentifier{Table: "test", Column: "id"},
            },
        },
    },
    From: &query.TableRef{Name: "test"},
}
```

**Built-in UDFs:**
- `SQUARE(x)` - Returns x²
- `CUBE(x)` - Returns x³
- `ABS_VAL(x)` - Returns absolute value
- `POWER_INT(x, y)` - Returns x^y

## Partitioned Queries

The WASM runtime supports partitioned tables for parallel query execution. Large tables can be split into multiple partitions that can be scanned in parallel.

```go
// Create runtime with host functions
rt := wasm.NewRuntime(10)
host := wasm.NewHostFunctions()
host.RegisterAll(rt)

// Get number of partitions for a table
tableName := "sales"
partitionCount, _ := host.GetPartitionCount(tableName)

// Scan each partition in parallel
for i := 0; i < partitionCount; i++ {
    rows, _ := host.PartitionScan(tableName, i)
    // Process partition results concurrently...
}

// Parallel aggregation across all partitions
result, _ := host.ParallelAggregate("sales", "SUM", "amount")
```

**Partition Operations:**
- `GetPartitionCount(tableName)` - Get number of partitions
- `PartitionScan(tableName, partitionId)` - Scan specific partition
- `ParallelAggregate(tableName, aggType, column)` - Aggregate across partitions
- `RepartitionTable(tableName, count)` - Redistribute data across partitions

## Vectorized Execution (SIMD)

The WASM runtime supports vectorized (SIMD-style) operations for bulk data processing. These operations process arrays of data in batches for improved performance.

```go
// Create runtime with host functions
rt := wasm.NewRuntime(20)
host := NewHostFunctions()
host.RegisterAll(rt)

// Setup arrays
prices := []int64{10, 20, 30, 40, 50}
quantities := []int64{2, 3, 4, 5, 6}

// Write to WASM memory
pricePtr := int32(1024)
qtyPtr := int32(2048)
valuePtr := int32(3072)

// Vectorized multiply: prices * quantities
host.VectorizedMultiply(pricePtr, qtyPtr, valuePtr, 5)

// Vectorized sum: total of all values
total := host.VectorizedSum(valuePtr, 5)
```

**Vectorized Operations:**
- `vectorizedAdd` - Element-wise addition of two arrays
- `vectorizedMultiply` - Element-wise multiplication
- `vectorizedCompare` - Batch comparison (eq, ne, lt, le, gt, ge)
- `vectorizedSum` - Sum all elements (reduction)
- `vectorizedMinMax` - Find min and max values
- `vectorizedFilter` - Filter elements by mask
- `vectorizedBatchCopy` - Fast bulk memory copy

## Performance Profiling & Benchmarking

The WASM runtime includes comprehensive performance profiling and benchmarking tools for query optimization.

```go
// Create profiler
profiler := wasm.NewQueryProfiler()

// Record query executions
profiler.RecordExecution(duration, rowCount, memoryUsed)

// Get statistics
stats := profiler.GetStats()
fmt.Printf("Avg: %dns, Min: %dns, Max: %dns\n",
    stats.AvgDuration, stats.MinDuration, stats.MaxDuration)

// Benchmark a query
result, err := wasm.BenchmarkQuery(rt, compiled, 100)
fmt.Printf("Throughput: %.2f queries/sec\n", result.Throughput)
```

**Profiling Features:**
- `QueryProfiler` - Track query execution metrics over time
- `BenchmarkQuery()` - Run standardized benchmarks
- Host functions for runtime metrics:
  - `getQueryMetrics` - Execution statistics
  - `getMemoryStats` - Memory usage
  - `getOpcodeStats` - Opcode frequency analysis
  - `logProfilingEvent` - Custom event logging
  - `resetMetrics` - Reset counters

**Metrics Tracked:**
- Total executions, duration (total/min/max/avg)
- Memory usage (peak, allocations)
- Per-query execution history
- Throughput (queries/sec, rows/sec)

## Future Enhancements

1. ~~**Partitioned Queries**~~ - ✅ Completed
2. ~~**Vectorized Execution**~~ - ✅ Completed

## File Structure

```
pkg/wasm/
├── compiler.go           # WASM compiler with expression compilation
├── compiler_test.go      # Compiler tests
├── runtime.go            # WASM interpreter
├── runtime_test.go       # Runtime tests
├── host_functions.go     # Database host functions
├── integration_host_test.go  # Integration tests
├── expression_test.go    # Expression compilation tests
├── where_test.go         # WHERE clause tests
├── aggregate_test.go     # Aggregate function tests
├── join_test.go          # JOIN operation tests
├── subquery_test.go      # Subquery tests
├── orderby_test.go       # ORDER BY tests
├── limit_test.go         # LIMIT/OFFSET tests
├── distinct_test.go      # DISTINCT tests
├── union_test.go         # UNION tests
├── window_test.go        # Window function tests
├── prepared_test.go      # Prepared statement tests
├── streaming_test.go     # Streaming results tests
├── transaction_test.go   # Transaction support tests
├── udf_test.go           # User-defined function tests
├── partition_test.go     # Partitioned query tests
├── vectorized_test.go    # Vectorized/SIMD execution tests
├── profiling_test.go     # Performance profiling tests
├── integration_real_test.go  # Real SQL execution tests
├── types.go              # Type definitions
└── README.md             # This file
```
