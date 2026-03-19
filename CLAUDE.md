# CobaltDB Developer Guide

## Project Overview
CobaltDB is a production-ready SQL database engine written in Go. It supports standard SQL with extensions for JSON, full-text search, window functions, CTEs, and row-level security.

## Architecture

### Core Packages
- `pkg/catalog` - SQL execution engine (SELECT, INSERT, UPDATE, DELETE, DDL)
- `pkg/query` - SQL parser and AST definitions
- `pkg/btree` - B-tree storage engine (in-memory and disk-based)
- `pkg/storage` - Storage layer (buffer pool, WAL, encryption)
- `pkg/engine` - Database orchestration with circuit breaker and retry logic
- `pkg/wasm` - WebAssembly compiler and runtime for SQL execution
- `pkg/server` - MySQL protocol server implementation
- `pkg/security` - Row-level security (RLS) policies
- `pkg/auth` - Authentication and authorization
- `pkg/audit` - Audit logging
- `pkg/metrics` - Metrics collection and monitoring

### Key Components

#### Catalog
The Catalog is the central execution engine. It manages tables, indexes, and executes queries.

**Thread Safety:**
- Uses `sync.RWMutex` for concurrency control
- Public methods acquire locks, internal `*Locked` methods are lock-free
- The mutex is NOT reentrant - never call locked methods from locked methods

**Main Execution Paths:**
- `selectLocked` - Simple SELECT execution
- `executeSelectWithJoin` - JOIN support
- `executeSelectWithJoinAndGroupBy` - GROUP BY with aggregates

#### Query Cache
Simple LRU cache for query results. Enabled via `SetQueryCache(true, maxEntries)`.

#### Storage
- `DiskBackend` - Persistent storage with page manager
- `BufferPool` - LRU page cache with pin/unpin protocol
- `WAL` - Write-ahead logging for durability
- `Encryption` - AES-GCM encryption at rest

## Testing

### Running Tests
```bash
# All package tests
go test ./pkg/...

# Integration tests
go test ./integration/...

# Coverage
go test ./pkg/... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

### Test Statistics
- 5,965+ test functions
- 499 test files
- Target: %90+ coverage per package

## Code Guidelines

### Adding New Features
1. Add tests first (TDD preferred)
2. Maintain %90+ test coverage
3. Use existing error types (ErrTableExists, ErrTableNotFound, etc.)
4. Update this document for significant changes

### Common Pitfalls
- **CRLF line endings** - Use Go scripts for text replacements, Edit tool may fail
- **JSON encoding** - Write paths use `json.Marshal`, read paths use `decodeRow()`
- **Reserved words** - `rank`, `key` are reserved SQL keywords
- **Float precision** - Use `fmt.Sprintf("%.1f", val)` for comparisons
- **NULL sorting** - NULLs sort last ASC / first DESC

## Performance Considerations

### Catalog.mu Lock Contention
The main mutex can become a bottleneck under high concurrency. Consider:
- Minimizing work inside critical sections
- Using read locks when possible
- Avoiding nested lock acquisitions

### Row Encoding
- Write: `json.Marshal` for row data
- Read: `decodeRow()` handles both JSON and binary formats
- Do NOT convert write paths to `encodeRowFast` (known to cause failures)

## Known Limitations
- UPDATE...FROM SET can only reference target table columns
- Composite multi-column PRIMARY KEY not supported
- Parser doesn't support: NATURAL JOIN, INSTEAD OF trigger, NO ACTION FK, ->> operator

## Security Features
- TLS support for connections
- Row-Level Security (RLS) with policies
- Audit logging for compliance
- SQL injection protection via prepared statements
- Encryption at rest

## WebAssembly (WASM) System (2026-03-18)

Fully functional WASM compiler and runtime for SQL query execution.

**Location:** `pkg/wasm/`

**Features:**
- Compiles SQL (SELECT, INSERT, UPDATE, DELETE) to WASM bytecode
- Stack-based WASM interpreter with full opcode support
- Host functions for database operations (tableScan, insertRow, updateRow, deleteRow)
- Linear memory management (64KB pages)
- Query result parsing from WASM memory
- JOIN operations (INNER, LEFT, RIGHT, FULL)
- Window functions (ROW_NUMBER, LAG, LEAD, aggregates OVER)
- Set operations (UNION, EXCEPT, INTERSECT)
- Subqueries (scalar and correlated)
- Prepared statements with parameter binding
- Streaming/chunked result processing
- ACID transactions with savepoints
- User-defined functions (UDFs)
- Partitioned queries for parallel execution
- Vectorized/SIMD operations for bulk processing
- Performance profiling and benchmarking tools

**Usage:**
```go
compiler := wasm.NewCompiler()
compiled, _ := compiler.CompileQuery(sql, stmt, args)

rt := wasm.NewRuntime(10) // 10 pages = 640KB
host := wasm.NewHostFunctions()
host.RegisterAll(rt)

result, _ := rt.Execute(compiled, args)
```

**Files:**
- `compiler.go` - SQL to WASM bytecode compiler
- `runtime.go` - WASM interpreter/executor
- `host_functions.go` - Database host function implementations
- `README.md` - Complete WASM documentation

## Dead Code Removed (2026-03-18)
The following features were removed as they were never integrated:
- Connection pooling
- Query plan cache
- Group commit, Read replicas, Index advisor
- Query timeout, Slow query log, Table partitioning
- Deadlock detection, PITR/Backup, Parallel query
- FDW, AlertManager, AutoVacuum, JobScheduler, Compression
