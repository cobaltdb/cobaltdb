# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
CobaltDB is a production-ready pure-Go SQL database engine (zero CGO at runtime). It ships in two shapes from the same codebase:
- **Embedded library** — `github.com/cobaltdb/cobaltdb/pkg/engine` opened via `engine.Open(path, *engine.Options)` for in-process use (`:memory:` or disk-backed).
- **Standalone server** — `cmd/cobaltdb-server` speaks the MySQL wire protocol (default port 4200/3307) so any MySQL client or ORM works unchanged.

Supports standard SQL plus JSON, full-text search, window functions, CTEs, row-level security, temporal `AS OF` queries, HNSW vector search, replication, and AES-256-GCM encryption at rest.

**Note:** `AGENTS.md` contains an older near-duplicate of this guide. When updating guidance, prefer editing `CLAUDE.md` — keep `AGENTS.md` aligned if the change affects agent-facing instructions.

## Build & Verify

Use the Makefile — it pins the right flags. Outputs go to `bin/`.

```bash
make build              # builds bin/cobaltdb-server and bin/cobaltdb-cli
make verify             # go build + go vet + go test ./... (core gate)
make verify-security    # verify + race + vuln + gosec + lint (needs CGO for -race)
make race               # CGO_ENABLED=1 go test -race ./...
make lint               # golangci-lint (errcheck + govet) on security-critical pkgs only
make test-coverage      # writes coverage.out + coverage.html
make release            # cross-compiles server+CLI for linux/darwin/windows × amd64/arm64 into dist/
```

The `lint` and `gosec` targets intentionally scope to `SECURITY_PKGS` (defined at the top of `Makefile`) — cmd/cobaltdb-server, pkg/server, pkg/protocol, pkg/storage, pkg/auth, sdk/go, pkg/logger, pkg/query. Broader linting is not wired up.

Runtime: `make run-server` / `make run-cli` use `go run`. Binaries already built at repo root (`cobaltdb-server`, `cobaltdb-cli`) are artifacts from prior builds, not canonical — always rebuild via `make build` into `bin/`.

## Architecture

### Core Packages
- `pkg/catalog` - SQL execution engine (SELECT, INSERT, UPDATE, DELETE, DDL)
- `pkg/query` - SQL parser, AST definitions, and query optimizer
- `pkg/btree` - B-tree storage engine (in-memory and disk-based)
- `pkg/storage` - Storage layer (buffer pool, WAL, encryption)
- `pkg/engine` - Database orchestration with circuit breaker, retry, and query plan cache (the public embedded entrypoint)
- `pkg/txn` - Transaction manager, lock manager, MVCC, deadlock detection
- `pkg/wasm` - WebAssembly compiler and runtime for SQL execution
- `pkg/server` - MySQL protocol server implementation
- `pkg/protocol` / `pkg/wire` - MySQL wire protocol codec primitives
- `pkg/security` - Row-level security (RLS) policies
- `pkg/auth` - Authentication and authorization (Argon2id)
- `pkg/audit` - Audit logging (encrypted)
- `pkg/logger` - Structured logging used across packages
- `pkg/metrics` - Metrics collection, monitoring, slow query logging, AlertManager
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

# Integration tests (lives in ./integration, not ./pkg — not covered by ./pkg/...)
go test ./integration/...

# Single package
go test ./pkg/catalog/

# Single test by name (regex)
go test ./pkg/catalog/ -run TestCatalog_SelectWithJoin -v

# Single integration test
go test ./integration/ -run TestMySQLE2E -v

# Coverage
go test ./pkg/... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

Three test trees coexist — `./pkg/...` for unit tests, `./integration/...` for cross-package integration, `./test/...` for benchmarks and large end-to-end suites. `go test ./...` runs all three.

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

### Deadlock Detection
- Automatic deadlock detection runs every 100ms in background
- Uses wait-for graph with DFS cycle detection
- Automatically aborts youngest transaction (highest StartTS) in cycle
- Metrics tracked at `/transaction-metrics` endpoint
- Lock wait timeout: 5s default (configurable via `Options.LockWaitTimeout`)
- Transaction timeout: Optional (configurable via `Options.Timeout`)

## Known Limitations
(No engine-level limitations currently tracked.)

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
- **Deadlock Detection** (`pkg/txn/manager.go`) - Wait-for graph with automatic cycle detection
- **Transaction Timeout** - Per-transaction and lock wait timeouts
- **Transaction Metrics** - Real-time monitoring via HTTP endpoint
- **AutoVacuum** (`pkg/catalog/catalog_maintenance.go`, `pkg/engine/database.go`) - Automatic dead tuple cleanup with configurable interval and threshold
- **Group Commit** (`pkg/storage/wal.go`) - WAL-level batching of fsyncs; `SyncMode` controls behavior (SyncFull=immediate, SyncNormal=1ms batch, SyncOff=async)
- **JobScheduler** (`pkg/scheduler/`) - Background job scheduler with worker pool, retry logic, and panic recovery. Runs AutoVacuum and AutoAnalyze by default. Supports custom user jobs via `DB.GetScheduler().Register()`.
- **Page-level Storage Compression** (`pkg/storage/compression.go`) - Optional zlib-based per-page compression. Stores compressed pages with inline header at logical `pageID * PageSize` offsets, creating sparse-file holes on supported filesystems. Falls back to raw storage when compression doesn't meet the configured `MinRatio` threshold. Configurable via `Options.CompressionConfig`.
- **Index Advisor** (`pkg/advisor/`) - AST-based query analyzer that tracks column usage in WHERE, JOIN, ORDER BY, and GROUP BY clauses. Recommends single-column and composite indexes, suppressing suggestions already covered by existing indexes or primary keys. Accessible via `DB.GetIndexRecommendations()`.
- **Parallel Query Execution** (`pkg/parallel/`, `pkg/catalog/catalog_core.go`, `pkg/catalog/catalog_aggregate.go`) - Chunk-based parallel processing for simple SELECT scans and GROUP BY queries. Splits materialized row data across worker goroutines for CPU-bound work (row decoding, WHERE evaluation, projection, grouping). Enabled by default with `runtime.NumCPU()` workers and a threshold of 1000 rows. Configurable via `Options.ParallelWorkers` and `Options.ParallelThreshold`.
- **Foreign Data Wrappers (FDW)** (`pkg/fdw/`, `pkg/catalog/catalog_fdw.go`, `pkg/engine/database.go`) - Lightweight FDW framework for querying external data sources via SQL. Supports `CREATE FOREIGN TABLE ... WRAPPER ... OPTIONS (...)`. Built-in CSV wrapper reads local CSV files. Foreign tables are materialized into temporary B-trees at scan time, enabling full query engine features (JOIN, GROUP BY, ORDER BY) without changes to scan loops. Foreign tables are read-only in this version.

## Features Not Implemented
The following features do not exist in the codebase:
*(none — all planned features are implemented)*

**Note:** Deadlock detection is now fully implemented in `pkg/txn/manager.go`.
Alert system is implemented in `pkg/metrics/alerting.go` (AlertManager with rules, handlers, severity levels).
