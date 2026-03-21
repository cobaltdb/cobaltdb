# CobaltDB Developer Guide

## Project Overview
CobaltDB is a production-ready SQL database engine written in Go. It supports standard SQL with extensions for JSON, full-text search, window functions, CTEs, and row-level security.

## Architecture

### Core Packages
- `pkg/catalog` - SQL execution engine (SELECT, INSERT, UPDATE, DELETE, DDL)
- `pkg/query` - SQL parser, AST definitions, and query optimizer
- `pkg/btree` - B-tree storage engine (in-memory and disk-based)
- `pkg/storage` - Storage layer (buffer pool, WAL, encryption)
- `pkg/engine` - Database orchestration with circuit breaker, retry, and query plan cache
- `pkg/wasm` - WebAssembly compiler and runtime for SQL execution
- `pkg/server` - MySQL protocol server implementation
- `pkg/security` - Row-level security (RLS) policies
- `pkg/auth` - Authentication and authorization
- `pkg/audit` - Audit logging
- `pkg/metrics` - Metrics collection, monitoring, and slow query logging
- `pkg/optimizer` - Cost-based query optimizer with join reordering and index selection
- `pkg/cache` - Query result cache with TTL support
- `pkg/pool` - Connection pooling with health checks and dynamic sizing
- `pkg/replication` - Master-slave replication (async, sync, full_sync modes)
- `pkg/backup` - Backup and restore with compression support

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
- 10,400+ test functions
- 500+ test files
- 20 packages, all passing
- Target: %90+ coverage per package (19/20 packages above 90%, only catalog at 85%)

### Running Benchmarks Safely
```bash
# Full suite — bounded memory, safe CacheSize
go test ./test/ -run=^$ -bench=BenchmarkFullSuite -benchtime=500ms -benchmem

# Package-level
go test ./pkg/btree/ -bench=. -benchmem
go test ./pkg/storage/ -bench=. -benchmem
go test ./pkg/query/ -bench=. -benchmem
```

## Code Guidelines

### Adding New Features
1. Add tests first (TDD preferred)
2. Maintain %90+ test coverage
3. Use existing error types (ErrTableExists, ErrTableNotFound, etc.)
4. Update this document for significant changes

### Common Pitfalls
- **CRLF line endings** - Use Go scripts for text replacements, Edit tool may fail
- **JSON encoding** - Write paths use `json.Marshal`, read paths use `decodeVersionedRow()`
- **Reserved words** - `rank`, `key` are reserved SQL keywords
- **Float precision** - Use `fmt.Sprintf("%.1f", val)` for comparisons
- **NULL sorting** - NULLs sort last ASC / first DESC
- **CacheSize is PAGE COUNT not bytes** - `CacheSize: 1024` = 1024 pages = 4MB. Do NOT use `1024*1024` (that's 4GB!)
- **MemoryBackend has 1GB default limit** - Use `NewMemoryWithLimit()` for custom limits
- **fmt.Sprintf in hot paths** - Use `strconv.FormatInt/FormatFloat` instead (see `hashJoinKey`, `valueToString`)

## Performance Considerations

### Row Decoding (v0.3.1)
- `decodeVersionedRowFast()` is the primary decoder — zero-reflection byte scanning (204ns/row)
- Falls back to `json.Unmarshal` for edge cases (1051ns/row)
- Integers parsed directly as `int64` (no float64→int64 roundtrip)
- Do NOT convert write paths to `encodeRowFast` (known to cause failures)

### Fast Paths
- **COUNT(*)** - `tryCountStarFastPath()`: skips row decode, uses `bytesContainDeletedAt()` byte scan
- **SUM/AVG** - `trySimpleAggregateFastPath()`: uses `extractColumnFloat64()` byte-level extraction when no WHERE clause. Falls back to full decode per-row on failure.
- **LIMIT/OFFSET** - Early termination in iterate loop when no ORDER BY/DISTINCT/window functions
- **Hash JOIN** - `hashJoinKey()` uses `strconv` instead of `fmt.Sprintf` for key generation
- **JSONPath** - `getCachedJSONPath()` caches parsed paths in `sync.Map`

### Catalog.mu Lock Contention
The main mutex can become a bottleneck under high concurrency. Consider:
- Minimizing work inside critical sections
- Using read locks when possible
- Avoiding nested lock acquisitions

### Storage Safety
- `MemoryBackend` caps geometric growth at 64MB increments (prevents 50GB→100GB doubling)
- Default max size: 1GB. Use `NewMemoryWithLimit()` for benchmarks/tests
- `BufferPool` uses `sync.Pool` for page buffer recycling (`pageDataPool`)
- `BufferPool.evict()` recycles page data via `putPageData()`

## Known Limitations
- UPDATE...FROM SET can only reference target table columns
- Composite multi-column PRIMARY KEY not supported
- Parser doesn't support: ->> JSON operator, RESTRICTIVE RLS policy

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

## Integrated Features (v2.2.0+)
The following features are fully implemented and integrated in the engine:
- **Query Plan Cache** (`pkg/engine/query_plan_cache.go`) - LRU cache for parsed statements
- **Query Result Cache** (`pkg/cache/`) - TTL-based query result caching
- **Query Optimizer** (`pkg/optimizer/`) - Cost-based optimizer with join reordering
- **Replication** (`pkg/replication/`) - Master-slave with async/sync/full_sync modes
- **Backup/Restore** (`pkg/backup/`) - Full, incremental, differential backups with compression
- **Connection Pooling** (`pkg/pool/`) - Health checks and dynamic sizing
- **Slow Query Log** (`pkg/metrics/slow_query.go`) - Threshold-based slow query tracking
- **Query Timeout** - Configurable per-database timeout enforcement
- **Table Partitioning** - Partition definitions in DDL

## Features Not Implemented
The following features do not exist in the codebase:
- FDW (Foreign Data Wrappers)
- AlertManager
- AutoVacuum
- JobScheduler
- Compression (storage-level)
- Deadlock detection
- Group commit
- Index advisor
- Parallel query execution
