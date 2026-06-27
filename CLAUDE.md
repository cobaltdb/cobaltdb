# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
CobaltDB is a production-oriented pure-Go SQL database engine (zero CGO at runtime). It ships in two shapes from the same codebase:
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
make lint               # golangci-lint runs globally across all packages (6 linters)
make test-coverage      # writes coverage.out + coverage.html
make release            # cross-compiles server+CLI for linux/darwin/windows × amd64/arm64 into dist/
```

The `lint` and `gosec` targets run golangci-lint and gosec globally across all packages — there is no per-package security restriction. The `SECURITY_PKGS` note in older docs is stale.

Runtime: `make run-server` / `make run-cli` use `go run`. Binaries already built at repo root (`cobaltdb-server`, `cobaltdb-cli`) are artifacts from prior builds, not canonical — always rebuild via `make build` into `bin/`.

## Architecture

### Core Packages
- `pkg/catalog` - SQL execution engine (SELECT, INSERT, UPDATE, DELETE, DDL)
- `pkg/query` - SQL parser, AST definitions, and query optimizer
- `pkg/btree` - B-tree storage engine (in-memory and disk-based)
- `pkg/storage` - Storage layer (buffer pool, WAL, encryption)
- `pkg/engine` - Database orchestration (the public embedded entrypoint); `circuit_breaker.go` and `retry.go` are available in this package but are not yet wired into the Exec/Query path (tracked in `refactor.md`)
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

**File Organization:**
- `catalog_core.go` - Catalog struct, `selectLocked` dispatch, `scanTableRows`, table utilities
- `catalog_select.go` - JOIN execution, outer-query projection, view aggregate processing
- `catalog_select_helpers.go` - Column resolution, CTE handling, post-processing
- `catalog_eval.go` - Expression evaluation (`evaluateExpression`, `evaluateWhere`, `evaluateLike`, `evaluateIn`, `evaluateBetween`, function dispatch)
- `catalog_eval_json.go` - JSON function evaluation
- `catalog_eval_string.go` - String function evaluation (UPPER, LOWER, TRIM, SUBSTR, etc.)
- `catalog_aggregate.go` - GROUP BY, aggregates, HAVING, hidden column management
- `catalog_window.go` - Window functions (ROW_NUMBER, RANK, LAG, LEAD, aggregates OVER)
- `catalog_insert.go` - INSERT logic with constraint validation
- `catalog_update.go` - UPDATE/DELETE with JOIN support
- `catalog_delete.go` - DELETE with soft-delete and FK enforcement
- `catalog_ddl.go` - DDL operations (CREATE TABLE, indexes, triggers, policies)
- `catalog_fastpath.go` - COUNT(*) and SUM/AVG streaming fast paths
- `catalog_rls.go` - Row-level security helpers
- `catalog_txn.go` - Transaction management, rollback, undo replay
- `catalog_maintenance.go` - Save/Load, vacuum, analyze
- `catalog_cte.go` - CTE execution (recursive and non-recursive)
- `catalog_view.go` - Materialized view management
- `catalog_returning.go` - RETURNING clause evaluation

**Thread Safety:**
- Uses `sync.RWMutex` for concurrency control
- Public methods acquire locks, internal `*Locked` methods are lock-free
- The mutex is NOT reentrant - never call locked methods from locked methods

**Main Execution Paths:**
- `selectLocked` - Simple SELECT execution (dispatches to helpers)
- `scanTableRows` - Row scanning (index, MV, or full table scan)
- `executeSelectWithJoin` - JOIN support
- `executeSelectWithJoinAndGroupBy` - GROUP BY with aggregates
- `evaluateFunctionCall` - Function evaluation with dispatch helpers (math, string, vector, CAST)

#### Query Cache
Query results are cached in `pkg/cache` (`cache.Cache`). The catalog exposes `EnableQueryCacheWithLimits(maxBytes, maxEntries, ttl)` and `GetQueryCache()`; `InvalidateAll()` is called on catalog reload. Disabled by default; enable via `Options.QueryCache.EnableQueryCache`.

#### Storage
- `DiskBackend` - Persistent storage with page manager
- `BufferPool` - LRU page cache with pin/unpin protocol
- `WAL` - Write-ahead logging for durability
- `Encryption` - AES-GCM encryption at rest

#### Interfaces
Core interfaces are defined for testability and modularity:
- `btree.KVStore` - Key-value storage operations (`pkg/btree/interfaces.go`)
- `storage.WriteAheadLog` - WAL operations (`pkg/storage/interfaces.go`)
- `storage.BufferPoolManager` - Buffer pool page management
- `txn.LockManager` - Lock acquisition/release with shared and exclusive modes
- `txn.TransactionManager` - Transaction lifecycle
- `catalog.QueryCacheStore` - Query result caching

#### MVCC Version Store
`pkg/txn/version_store.go` provides per-key version chains for snapshot isolation:
- `Commit(key, value, commitTS)` adds a new version
- `GetAtSnapshot(key, snapshotTS)` walks the chain for visible version
- `Prune(minActiveTS)` garbage-collects old versions

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

Previously ~194 "coverage-boost" test files were gated behind a `coverage_padding` build tag and excluded from the default suite. They were verified to provide zero unique coverage of the untagged test suite, so the build tag and all 194 files have been removed (commit history, see `refactor.md`). The default `go test ./...` is now the only suite.

```bash
go test ./...                  # default suite — all tests
make test                     # same as above
make test-coverage            # coverage profile (no special tag needed)
```

Shared test helpers used across the boundary live in untagged `*_test.go` files (e.g. `pkg/catalog/shared_literals_test.go`, `pkg/replication/shared_mocks_test.go`). The experimental WASM package is separately gated behind `wasm_experimental` (see the WASM section).

### Test Statistics
- ~4,200 test functions across 276 test files (the `coverage_padding` build tag and its ~194 gated files were removed in a prior session; `go test ./...` is the single suite)
- All `pkg` packages passing
- Per-package `go test -cover` reports coverage; overall aggregate varies by package
- Direction: lift coverage through focused table-driven tests in production-critical packages; target 90%+ per package

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
2. Keep coverage moving toward the 90%+ package target
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

- **Single-writer model** — Only one write transaction at a time; long-running SELECTs block writes.
- **Coarse-grained locking** — Catalog uses a single `sync.RWMutex`; DDL blocks all DML.
- **HA / clustering** — No built-in sharding, Raft/Paxos, or automatic failover.
- **WASM streaming** — Streaming results are only supported for SELECT queries.
- **RLS evaluates post-projection** — Row-level security policies now filter rows by the
  per-query user (the query context is propagated to the catalog via
  `Catalog.SelectWithContext`, which holds the exclusive lock so the shared RLS context
  is per-query-safe under concurrency). However, policies are evaluated against the
  *projected* columns, so a column referenced by a policy must appear in the `SELECT`
  list; if it does not, the policy cannot see it and the row is excluded (fail-closed —
  safe but over-restrictive). Full-row policy evaluation (applying RLS before projection)
  is scoped follow-up work.
- **Crash recovery** — Committed writes to a disk database survive an unclean shutdown and
  are replayed from the WAL on reopen (verified end-to-end, including brand-new databases).
  The catalog schema is flushed to disk after each DDL (`DB.persistSchema`, called from
  `execute`), so a database that performs DDL+DML and then crashes before its first
  checkpoint is still recoverable; WAL logical replay is idempotent against already-flushed
  data pages (no double-apply). Schema-flush is skipped for `:memory:` databases.

## Security Features
- TLS support for connections
- Row-Level Security (RLS) with policies
- Audit logging for compliance
- SQL injection protection via prepared statements
- Encryption at rest
- MySQL prepared statement protocol support (COM_STMT_PREPARE/EXECUTE/CLOSE/RESET)

## WebAssembly (WASM) System (2026-03-18)

WASM compiler and runtime for SQL query execution.

> **Build-gated / experimental (2026-05-29):** `pkg/wasm` is **not** part of the
> default build, test run, or coverage. It has no production callers — the engine
> does not route any query through it — so it is isolated behind the
> `wasm_experimental` build tag to remove it from the maintenance/coverage
> surface while preserving the code. Build or test it explicitly with
> `go build -tags wasm_experimental ./pkg/wasm/` /
> `go test -tags wasm_experimental ./pkg/wasm/`. It is a parallel, unintegrated
> reimplementation of query execution; treat it as a research prototype, not a
> supported feature. (See `refactor.md` §8.)

**Location:** `pkg/wasm/` (build tag: `wasm_experimental`)

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

## Features Not Implemented / Not Production-Grade

- **Automatic HA failover / clustering** — No built-in sharding, Raft/Paxos, leader election, or split-brain-safe failover.
- **Broad production certification** — Crash-recovery fault injection, package-level coverage gates, and long-running soak tests remain active hardening work.
- **Audit log external trust root** — Audit logs are encrypted, hash-chained, and offline-verifiable, but external signing/HSM-backed anchoring is not yet implemented.
- **Encryption key rotation workflow** — Storage encryption exists, but operational key rotation is not yet exposed as a production workflow.
- **WASM as primary execution engine** — WASM execution is functional for selected paths but should be treated as experimental.

**Note:** Deadlock detection is now fully implemented in `pkg/txn/manager.go`.
Alert system is implemented in `pkg/metrics/alerting.go` (AlertManager with rules, handlers, severity levels).
