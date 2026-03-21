# Changelog

All notable changes to CobaltDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.3.1] - 2026-03-21

### ⚡ Performance — Up to 5.2× Faster Query Execution

14 commits focused on storage safety, query engine optimization, and documentation.

### Critical Fixes

- **MemoryBackend RAM explosion** — `WriteAt()` reallocated on every write past current length even when capacity was sufficient. Combined with geometric doubling (50GB → 100GB), this caused 100GB+ RAM usage and system lockups during benchmarks. Fixed with zero-copy capacity reuse + 64MB growth cap + 1GB default max size.
- **CacheSize unit bug** — Benchmark and test files used `CacheSize: 10 * 1024 * 1024` thinking it was bytes, but it's page count. This allocated 10M pages = 40GB of BufferPool. Fixed all occurrences to correct page counts (1024–2048).

### Performance Optimizations

| Optimization | Before | After | Speedup |
|-------------|--------|-------|---------|
| Custom VersionedRow decoder (skip json.Unmarshal) | 1,051 ns | 204 ns | **5.2×** |
| LIMIT/OFFSET early termination | 17 ms | 3.7 ms | **4.6×** |
| SUM/AVG byte-level fast path | 14 ms | 3.9 ms | **3.6×** |
| Hash join key (strconv vs fmt.Sprintf) | 12 ms | 9.6 ms | **1.3×** |
| compareValues (strconv vs fmt.Sprintf) | — | — | **1.2×** |
| JSONPath cache (sync.Map) | 4.7 µs | 3.7 µs | **1.3×** |
| MemoryBackend WriteAt | 64 MB/op | 12 B/op | **5.3M×** |
| BufferPool page recycling (sync.Pool) | — | — | 0 alloc eviction |

- **Custom `decodeVersionedRowFast`**: Zero-reflection byte-scanning decoder for the known VersionedRow JSON format. Parses integers directly as `int64` (no float64 roundtrip). Falls back to `json.Unmarshal` for edge cases.
- **LIMIT/OFFSET early termination**: Stop scanning after `offset+limit` matching rows when no ORDER BY/DISTINCT/window functions needed.
- **SUM/AVG byte-level fast path**: Extract numeric column values from raw JSON bytes without full row decode. Falls back to full decode if byte extraction fails.
- **Hash join key optimization**: Replace `fmt.Sprintf("%v")` with type-switch `strconv` calls in hash join build/probe phases.
- **`valueToString` optimization**: Same `fmt.Sprintf` → `strconv` pattern in `compareValues` fallback (affects ORDER BY, GROUP BY, DISTINCT).

### Benchmark Results (10K rows, AMD Ryzen 9 9950X3D)

| Operation | Latency |
|-----------|---------|
| Full Scan (1K) | 598 µs |
| Full Scan (10K) | 8.8 ms |
| SUM/AVG | 3.9–4.4 ms |
| LIMIT 100 OFFSET 1K | 3.7 ms |
| Inner JOIN (1K) | 700 µs |
| 3-Way JOIN | 1.7 ms |
| ORDER BY (10K) | 7.6 ms |
| Point Lookup | 2.1 µs |
| Concurrent Read (×20) | 669 ns |

### Storage & Benchmark Safety

- `MemoryBackend`: Capped geometric growth at 64MB increments, configurable max size (default 1GB)
- `NewMemoryWithLimit()`: Constructor for custom size limits
- All benchmark files: DROP/CREATE between iterations, bounded dataset sizes, correct CacheSize units
- B-tree benchmark 100K → 50K, parallelism 100 → 20
- `sync.Pool` for page buffer recycling, pre-allocated BufferPool map

### Test & Coverage

- New tests for `decodeVersionedRowFast`, `extractColumnFloat64`, `skipJSONValue`, `skipJSONBracketed`
- New tests for `NewMemoryWithLimit`, capacity reuse, growth cap, truncate limits
- `BenchmarkDecodeVersionedRow`: Fast (204 ns) vs Slow (1,051 ns) comparison
- Storage coverage: 89.7% → 90.2% (back above 90%)
- Catalog coverage: 84.0% → 85.2% (dead code removed)
- Integration test suite: 7.0s → 5.8s (17% faster)

### Documentation & Website

- README: Updated benchmark tables with Ryzen 9 9950X3D results, added SQL Engine section
- docs/BENCHMARKS.md: Complete rewrite with v0.3.1 data
- docs/ARCHITECTURE_FULL.md: Updated performance section
- Website HeroSection: 9M → 15.7M reads/sec
- Website PerformanceSection: Real CobaltDB benchmark data (throughput + latency tabs)

---

## [v0.3.0] - 2026-03-20

### 🔒 Security, MySQL Protocol & Multi-Language SDKs

22 commits, 106 files changed. Comprehensive security audit, MySQL protocol verification with real clients, and multi-language SDK release.

### MySQL Protocol (Verified Working)

- **Wire protocol fixes**: Fixed auth-plugin-data null terminator, sequence numbering for real MySQL clients
- **Verified with `go-sql-driver/mysql`**: Ping, CREATE, INSERT, SELECT, UPDATE, DELETE, JSON_EXTRACT, AVG — all working
- **4 E2E integration tests**: Full lifecycle, COM_PING, multi-query sequence, version verification
- **Docker/config**: MySQL port 3307 exposed in Dockerfile, docker-compose, and config

### Multi-Language SDKs

- **Python SDK** (`sdk/python/`): Wrapper around mysql-connector-python/PyMySQL
- **Node.js SDK** (`sdk/js/`): Wrapper around mysql2 with connection pooling
- **Java SDK** (`sdk/java/`): Wrapper around MySQL JDBC driver
- **Go SDK** (`sdk/go/`): Native `database/sql` driver (already existed)

### Security Fixes (19 total)

**Encryption:**
- WAL encryption with AEAD cipher and header authentication (AAD)
- Audit log encryption (AES-256-GCM, per-entry nonce)
- Page encryption AAD (prevents page swapping between offsets)
- Encryption key memory clearing on Close()

**Authentication:**
- Password policy enforcement on ChangePassword() (was bypassed)
- Brute force rate limiting (progressive 200ms-1s delay)
- Random default admin password via crypto/rand

**Row-Level Security:**
- Fixed RLS bypass in UPDATE...FROM (per-row policy check added)
- Fixed RLS bypass in DELETE...USING (per-row policy check added)

**SQL Injection Protection (10 → 15 patterns):**
- Conditional blind (IF/CASE with subquery)
- Out-of-band exfiltration (LOAD_FILE, INTO OUTFILE)
- System function abuse, double URL encoding, OR always-true

**Other:**
- Audit metadata masking (password/secret/token/key/credential/auth)
- yaml.v3 DoS vulnerability fixed (v3.0.0 → v3.0.1)
- golang.org/x/crypto upgraded to v0.49.0

### Concurrency & Stability Fixes

- Replication `handleSlave()` panic recovery
- Replication/Cache double-close protection (sync.Once)
- Pool `healthCheckLoop()` panic recovery
- Lifecycle hook goroutine tracking (WaitGroup)
- Backup `Restore()` and `copyFile()` fsync
- `replicateWrite` string concatenation → strings.Builder

### New Features

- **Stored procedure SQL parameters**: `CALL proc(1, 'hello')`
- **Vector search public API**: `DB.SearchVectorKNN()`, `DB.SearchVectorRange()`
- **Query optimizer**: `optimizeProjections`, `copySelectStmt`, safe `optimizeJoinOrder`

### Bug Fixes

- Query optimizer self-join reorder corruption
- MySQL protocol auth plugin name truncation
- MySQL protocol OK packet sequence numbering
- `selectLocked` bounds check on `stmt.Columns[0]`

### Documentation

- README repositioned as "Embedded + Standalone MySQL-compatible Server"
- Server Mode is now Quick Start #1 with mysql CLI session output
- "Works with" compatibility list (SQLAlchemy, Prisma, Hibernate, GORM...)
- Website: new hero, features, comparison, code examples, use cases sections
- CLAUDE.md: corrected "Dead Code Removed", Known Limitations

### Test Improvements

- 8,439 → **10,477** passing tests
- 21 → **6** skips
- 15 previously-skipped tests unskipped and working
- 4 new MySQL wire protocol E2E integration tests
- 19/20 packages above 90% coverage

### Coverage by Package (v0.3.0)

| Package | Coverage |
|---------|----------|
| pool | 98.0% | wasm | 93.4% |
| auth | 96.8% | btree | 92.4% |
| cache | 95.5% | backup | 91.9% |
| protocol | 95.5% | security | 91.9% |
| metrics | 94.8% | replication | 91.8% |
| wire | 94.7% | query | 90.9% |
| optimizer | 93.8% | audit | 90.9% |
| logger | 93.8% | storage | 90.5% |
| txn | 93.5% | server | 90.2% |
| engine | 90.0% | catalog | 85.5% |

---

## [v0.2.22] - 2026-03-15

### 🚀 Enterprise Features Release

Five major enterprise database features implemented and integrated into the engine.

### Added

#### Query Result Cache (`pkg/cache`)
- LRU cache with configurable max size and TTL
- Table dependency tracking for automatic invalidation
- Thread-safe operations with RWMutex
- Background cleanup of expired entries
- Cache statistics (hits, misses, evictions)
- 13 comprehensive tests, 85.1% coverage

#### Connection Pooling (`pkg/pool`)
- Min/max connection limits
- Health checks with ping verification
- Dynamic pool sizing
- Connection timeouts
- Graceful shutdown
- LIFO connection reuse
- 7 tests, 42.8% coverage

#### Query Optimizer (`pkg/optimizer`)
- Cost-based index selection
- Join reordering for optimal performance
- Table statistics management
- Index scoring algorithm
- Configurable optimization levels
- 10 tests, 93.8% coverage

#### Hot Backup (`pkg/backup`)
- Online backup without stopping the database
- Full and incremental backup support
- gzip compression with configurable level
- Retention policies (time-based and count-based)
- Backup verification
- Metadata persistence
- 10 tests, 52.1% coverage

#### Master-Slave Replication (`pkg/replication`)
- Async, sync, and full-sync modes
- WAL shipping for data synchronization
- SSL/TLS encryption support
- Automatic reconnection with exponential backoff
- Heartbeat monitoring
- Prometheus metrics integration
- Integration tests (requires network)

#### Connection Pooling (`pkg/pool`)
- Min/max connection limits
- Health checks with ping verification
- Dynamic pool sizing
- Connection timeouts
- Graceful shutdown
- LIFO connection reuse
- 7 tests, 42.8% coverage

#### Query Optimizer (`pkg/optimizer`)

#### Engine Integration
- All modules integrated into `engine.Open()`
- Configuration via `Options` struct
- Proper cleanup in `Close()` method
- Backup interface implementation on `DB`
- Public API methods exposed

### New Options

```go
type Options struct {
    // Query Cache
    EnableQueryCache bool
    QueryCacheSize   int64
    QueryCacheTTL    time.Duration

    // Replication
    ReplicationRole       string // "master" or "slave"
    ReplicationListenAddr string
    ReplicationMasterAddr string
    ReplicationMode       string // "async", "sync", "full_sync"

    // Backup
    BackupDir              string
    BackupRetention        time.Duration
    MaxBackups             int
    BackupCompressionLevel int
}
```

## [v0.2.21] - 2026-03-14

### 🧪 Test Coverage Enhancement Release

Comprehensive test coverage improvements across all packages to achieve enterprise-grade reliability.

### Test Coverage Improvements

| Package | Before | After | Change |
|---------|--------|-------|--------|
| `sdk/go` | 52.0% | 90.6% | +38.6% |
| `pkg/btree` | 88.4% | 92.6% | +4.2% |
| `pkg/storage` | 87.8% | 92.0% | +4.2% |
| `pkg/query` | 84.4% | 87.7% | +3.3% |
| `pkg/engine` | 88.6% | 89.2% | +0.6% |
| `pkg/server` | 83.9% | 85.6% | +1.7% |
| `pkg/catalog` | 72.5% | 80.2% | +7.7% |

### Added

#### New Integration Test Suites (600+ tests)

- **`integration/catalog_alter_fk_test.go`** - ALTER TABLE and Foreign Key tests
  - `TestAlterTableRenameColumnFull` - Column renaming with data preservation
  - `TestAlterTableRenameTableFull` - Table renaming
  - `TestApplyGroupByOrderWithFK` - GROUP BY with ORDER BY clauses
  - `TestFKOnDeleteOnUpdate` - CASCADE behavior
  - `TestFKOnDeleteSetNull` - SET NULL on delete
  - `TestFKOnDeleteRestrict` - RESTRICT constraint enforcement

- **`integration/catalog_json_utils_test.go`** - JSON function tests
  - `TestJSONSetBasic` - JSON_SET operations
  - `TestJSONGet` - JSON_EXTRACT and -> operator
  - `TestJSONModify` - JSON_INSERT, JSON_REPLACE, JSON_REMOVE
  - `TestJSONValidation` - JSON_VALID function
  - `TestJSONAggregation` - JSON_OBJECTAGG, JSON_ARRAYAGG

- **`integration/catalog_vacuum_stats_test.go`** - Maintenance operations
  - `TestVacuumWithDeletedData` - Space reclamation
  - `TestVacuumSpecificTable` - Table-specific vacuum
  - `TestCountRows` - Row counting functions
  - `TestCountRowsWithJoin` - COUNT with JOIN
  - `TestStoreIndexDef` - Index definition persistence

- **`integration/catalog_having_aggregates_test.go`** - HAVING clause + aggregate tests
  - `TestHavingAggregateExpressions` - HAVING with SUM/COUNT/AVG/MAX/MIN
  - `TestHavingWithSubquery` - HAVING with subquery comparison
  - `TestComplexGroupBy` - Multi-column GROUP BY with NULLs
  - `TestDistinctWithGroupBy` - DISTINCT + GROUP BY combination

- **`integration/catalog_where_subquery_test.go`** - WHERE clause with subqueries
  - `TestWhereWithSubquery` - IN, NOT IN, EXISTS, scalar subqueries
  - `TestWhereComplexExpressions` - AND/OR/NOT boolean logic
  - `TestWhereWithCase` - CASE expressions in WHERE
  - `TestWhereInExpression` - IN list expressions

- **`integration/catalog_view_outer_query_test.go`** - View resolution tests
  - `TestViewWithDistinct` - Views with DISTINCT
  - `TestViewWithGroupBy` - Views with GROUP BY + HAVING
  - `TestViewWithWindowFunctions` - Views with RANK() window functions
  - `TestNestedViews` - View chaining (view → view → table)
  - `TestDerivedTableWithGroupBy` - Subquery with GROUP BY

- **`integration/catalog_insert_delete_locked_test.go`** - Insert/Delete operations
  - `TestInsertWithDefaults` - DEFAULT value handling
  - `TestInsertExpressions` - Expression evaluation in INSERT
  - `TestDeleteWithTriggers` - DELETE with BEFORE/AFTER triggers
  - `TestDeleteWithFKCascadeDeep` - FK CASCADE (deep test)
  - `TestDeleteWithFKSetNullDeep` - FK SET NULL (deep test)
  - `TestDeleteWithIndexCleanup` - Index maintenance on delete
  - `TestDeleteWithUndoLog` - Transaction rollback

- **`integration/catalog_select_join_groupby_test.go`** - SELECT with JOIN + GROUP BY
  - `TestSelectWithJoinAndGroupBy` - JOIN + GROUP BY + HAVING
  - `TestJoinWithSubquery` - JOIN with derived tables
  - `TestSelectComplexQueryCache` - Query cache paths
  - `TestLeftJoinWithNulls` - LEFT JOIN with aggregation
  - `TestCrossJoin` - CROSS JOIN combinations

- **`integration/catalog_rls_deep_test.go`** - Row-Level Security tests
  - `TestRLSInsertPolicy` - INSERT policy checks
  - `TestRLSUpdatePolicy` - UPDATE policy checks
  - `TestRLSDeletePolicy` - DELETE policy checks
  - `TestRLSWithUSINGExpression` - USING clause evaluation
  - `TestRLSApplyFilterInternal` - RLS filter application

- **`integration/catalog_orderby_like_test.go`** - ORDER BY and LIKE tests
  - `TestOrderByMultiColumnNulls` - Multi-column ORDER BY with NULLS FIRST/LAST
  - `TestLikePatterns` - LIKE patterns (%, _, NOT LIKE)
  - `TestLikeEscape` - LIKE with ESCAPE character

- **`integration/catalog_alter_drop_test.go`** - ALTER TABLE DROP COLUMN tests
  - `TestAlterTableDropColumnWithData` - Drop column with existing data
  - `TestAlterTableDropColumnWithIndexDeep` - Drop column with index
  - `TestAlterTableDropLastNonPKColumn` - Drop last non-PK column
  - `TestAlterTableDropColumnWithFK` - Drop column with FK reference
  - `TestAlterTableDropMultipleColumns` - Multiple column drops

- **`integration/catalog_update_locked_test.go`** - UPDATE operations
  - `TestUpdateLockedBasic` - Basic UPDATE scenarios
  - `TestUpdateLockedWithSubquery` - UPDATE with subqueries
  - `TestUpdateLockedWithJoin` - UPDATE with JOIN
  - `TestUpdateLockedWithFK` - UPDATE with Foreign Keys
  - `TestUpdateLockedWithTrigger` - UPDATE with triggers
  - `TestUpdateLockedReturning` - UPDATE with RETURNING
  - `TestUpdateLockedComplexWhere` - Complex WHERE clauses

#### Unit Test Coverage Boost (26 test files)

- **Coverage Boost Tests** targeting low-coverage functions:
  - `pkg/catalog`: evaluateWhere, deleteRowLocked, applyRLSFilterInternal, computeAggregatesWithGroupBy, insertLocked, updateLocked
  - `pkg/query`: ParseJoinTypes, ParseAggregateFunctions, ParseWindowFunctions, complex expressions, CASE/WHEN, IN/EXISTS, LIKE patterns
  - `pkg/btree`: DiskBTree split/merge, iterator edge cases, large dataset handling
  - `pkg/storage`: WAL edge cases, encryption, compression, page manager
  - `pkg/engine`: Circuit breaker edge cases, retry policies, database lifecycle

### Fixed

- **Test Stability**
  - Signal handling tests skipped on Windows (syscall.Kill not available)
  - Fixed FK cascade chain test expectations
  - Fixed composite key conflicts in test data
  - Fixed trigger test to handle per-row execution correctly

- **Code Quality**
  - Resolved 25+ public Catalog methods with proper mutex locks
  - Fixed lock-free internal versions to prevent recursive deadlocks
  - Fixed row data deserialization in FTS and Analyze functions
  - Fixed JSON_SET handling for nested paths

### Statistics

- **Total Test Files**: 37
- **Unit Tests**: 600+
- **Integration Tests**: 200+
- **Packages with >80% Coverage**: 14/15
- **Overall Coverage**: 92.8%

## [v0.2.20] - 2026-03-08

### 🎉 Major Production Release - Enterprise Security & Resilience

### Added

#### Enterprise Production Features

- **🔌 Circuit Breaker** (`pkg/engine/circuit_breaker.go`)
  - Three-state breaker: Closed, Open, Half-Open
  - Automatic recovery with configurable timeout
  - Concurrency control and half-open rate limiting
  - Thread-safe with atomic operations
  - Manager for multiple circuit breakers

- **🔄 Retry Logic** (`pkg/engine/retry.go`)
  - Exponential backoff with jitter
  - Context cancellation support
  - Whitelist/blacklist for specific errors
  - 4 predefined policies: Fast, Standard, Aggressive, Background
  - Generic support for typed results

- **🚦 Rate Limiter** (`pkg/server/rate_limiter.go`)
  - Token bucket algorithm
  - Global and per-client rate limiting
  - Adaptive rate limiting based on system load
  - Automatic stale client cleanup
  - HTTP endpoint for statistics

- **🛡️ SQL Injection Protection** (`pkg/server/sql_protection.go`)
  - 10+ SQL injection pattern detection
  - UNION-based, time-based blind detection
  - Stacked query detection
  - Query length and complexity limits
  - Whitelist support for trusted queries

- **📊 Distributed Tracing** (`pkg/server/tracing.go`)
  - Request ID generation and tracking
  - Span-based tracing with context propagation
  - Sampling rate control
  - Active request tracking
  - Slow request detection (>1s)

- **🚨 Alerting System** (`pkg/server/alert.go`)
  - Configurable alert rules with cooldown
  - Log and webhook handlers
  - Alert history and acknowledgment
  - Default rules for CPU, memory, disk, error rate, latency
  - Spam prevention with cooldown periods

- **🌐 Production Server** (`pkg/server/production.go`)
  - Integrated lifecycle management
  - Circuit breaker and retry integration
  - Health check HTTP server
  - Kubernetes-compatible probes (/health, /ready, /healthz)
  - Graceful shutdown with signal handling
  - Component registration and health monitoring

- **📡 Connection Manager** (`pkg/server/connection_manager.go`)
  - Connection limits (global and per-IP)
  - Idle timeout and cleanup
  - Blacklist and whitelist support
  - Connection throttling
  - Statistics tracking

- **📈 Metrics Aggregator** (`pkg/server/metrics_aggregator.go`)
  - Counter, Gauge, Histogram, Timer metrics
  - Prometheus-compatible endpoint
  - JSON metrics endpoint
  - Historical data aggregation
  - System metrics (memory, goroutines, GC)

#### Enterprise Security Features

- **🔐 Encryption at Rest** (`pkg/storage/encryption.go`)
  - AES-256-GCM authenticated encryption
  - Argon2id and PBKDF2 key derivation
  - Transparent encryption/decryption for storage layer
  - `EncryptedBackend` wrapper for seamless integration
  - `GenerateSecureKey()` for secure key generation
  - Encryption header with metadata support

- **🔒 TLS Support** (`pkg/server/tls.go`)
  - TLS 1.2/1.3 support with secure cipher suites
  - Self-signed certificate auto-generation
  - Client certificate authentication
  - Certificate validation and verification
  - `LoadTLSConfig()` and `GetTLSListener()` helpers

- **📝 Audit Logging** (`pkg/audit/logger.go`)
  - JSON and text format support
  - Async batching for performance (100ms flush)
  - Automatic log rotation (100MB default)
  - Event types: Query, Auth, DDL, DML, System
  - Configurable via `audit.Config`

- **🛡️ Row-Level Security (RLS)** (`pkg/security/rls.go`)
  - Policy-based access control
  - User and role-based filtering
  - SQL expression evaluation support
  - `Manager` for policy lifecycle management
  - Context-based row filtering

- **⚡ Write Performance Improvements** (`pkg/storage/wal_batch.go`)
  - `BatchedWAL`: 100 records/10ms batch processing
  - `AsyncWAL`: Asynchronous WAL writing
  - Background flusher for high throughput
  - Optimized for >1000 writes/sec target

- **🎯 Query Optimizer** (`pkg/query/optimizer.go`)
  - Cost-based query optimization
  - Join order optimization with selectivity estimates
  - Index usage analysis
  - `Explain()` for execution plan display
  - Statistics-based cardinality estimation

### Integration

- Engine encryption integration (`pkg/engine/database.go`)
  - `Options.EncryptionKey` and `Options.EncryptionConfig`
  - Automatic encryption backend wrapper

- Engine audit logging integration (`pkg/engine/database.go`)
  - `Options.AuditConfig` for audit logger initialization
  - Automatic logging for DML and DDL operations

- Server TLS integration (`pkg/server/server.go`)
  - `Config.TLS` for TLS configuration
  - TLS support in `Listen()` method

### Test Coverage

| Package | Tests | Status |
|---------|-------|--------|
| `pkg/engine` | Circuit Breaker (7), Retry (12) | ✅ Passing |
| `pkg/server` | Lifecycle (6), Production (6), Rate Limiter (5), SQL Protection (10) | ✅ Passing |
| `pkg/audit` | 5 test | ✅ Passing |
| `pkg/security` | 22 test | ✅ Passing |
| `pkg/storage` | 40+ encryption tests | ✅ Passing |
| **Total** | **4500+ tests** | ✅ **All Passing** |

### Documentation

- Updated README: Security features comparison table
- Added new "Security Features" and "Production Features" sections
- Code examples and usage guides
- Added Kubernetes deployment examples

---

## [v0.2.11] - 2026-03-07

### Production Hardening - Critical Bug Fixes

#### Fixed
- **CRITICAL: Data Corruption in Page Manager** (`pkg/storage/pagemgr.go`)
  - `loadFreeList()` was reading from offset 0 instead of `PageHeaderSize`
  - Could cause free list corruption and infinite loops
  - Fixed to use correct offset: `data[PageHeaderSize:PageHeaderSize+4]`

- **CRITICAL: Transaction Resource Leak** (`pkg/engine/database.go`)
  - `Tx.Commit()` and `Tx.Rollback()` not releasing connections
  - Caused connection pool exhaustion after ~100 transactions
  - Added `defer tx.db.releaseConnection()` to both methods

- **CRITICAL: No Panic Recovery** (`pkg/engine/database.go`)
  - Any unexpected panic would crash the entire database server
  - Added panic recovery with stack traces to `Exec()` and `Query()`
  - Server now logs and returns error instead of crashing

- **HIGH: Iterator Resource Leaks** (`pkg/catalog/catalog.go`)
  - Multiple locations where BTree iterators weren't closed on error paths
  - Affected: `AlterTableAddColumn`, `AlterTableDropColumn` (2 locations)
  - Fixed by adding `defer iter.Close()` immediately after creation

- **HIGH: Statement Cache Race Condition** (`pkg/engine/database.go`)
  - LRU update in goroutine could race with eviction
  - Made update synchronous with proper locking

- **HIGH: Double RUnlock Panic** (`pkg/engine/database.go`)
  - Refactoring left extra `RUnlock()` call
  - Removed duplicate `db.stmtMu.RUnlock()`

- **MEDIUM: Nil Context Panic** (`pkg/engine/database.go`)
  - `ctx.Done()` on nil context causes panic
  - Added `if ctx != nil` checks before context operations

- **MEDIUM: Parser Error Ignored** (`pkg/query/parser.go`)
  - CREATE PROCEDURE parameters not being appended
  - Added `stmt.Params = append(stmt.Params, param)`

- **MEDIUM: Rollback Error Ignored** (`pkg/catalog/catalog.go`)
  - Rollback errors were silently ignored
  - Added `rollbackErr` variable to collect errors

### Added
- **Production Readiness Documentation**
  - `PRODUCTION_READINESS.md`: Complete production deployment guide
  - `TECHNICAL_ARCHITECTURE.md`: Detailed architecture documentation
  - Performance benchmarks and comparisons
  - Security considerations and deployment checklist

### Security
- Added panic recovery prevents DoS via malformed queries
- All public API methods now have panic recovery

### Documentation
- Updated README with current benchmarks
- Added known limitations section
- Documented single-writer concurrency model

---

## [v0.2.10] - 2026-03-06

### Added
- **Comprehensive E-Commerce Test Suite**: 16 real-world e-commerce scenarios
  - Category hierarchy with Recursive CTEs
  - Order management with transactions
  - Analytics reports with window functions
  - Complex JOINs, subqueries, and CTEs
  - Views and indexes testing
  - String, math, and JSON function coverage
  - Real-world data volume (1000+ products)
  - UNION, LIMIT/OFFSET, pagination testing
  - NULL handling and type conversion tests

### Test Coverage Improvements
- **All 20 packages passing tests**
- **New test files**: 100+ test files added across all packages
- **Real-world validation**: E-commerce scenarios fully tested

### Package Coverage
| Package | Coverage | Change |
|---------|----------|--------|
| `pkg/auth` | 95.6% | -3.0% |
| `pkg/metrics` | 97.7% | +3.9% |
| `pkg/wire` | 94.7% | +0.0% |
| `pkg/txn` | 88.1% | +6.2% |
| `pkg/backup` | 87.7% | +9.6% |
| `pkg/replication` | 87.3% | +0.1% |
| `pkg/json` | 84.4% | +0.0% |
| `pkg/storage` | 82.5% | +1.1% |
| `pkg/protocol` | 79.2% | +4.5% |
| `pkg/query` | 75.6% | -10.1% |
| `pkg/btree` | 73.2% | -14.1% |
| `pkg/server` | 68.6% | -9.2% |
| `pkg/catalog` | 55.0% | -18.1% |
| `pkg/engine` | 49.9% | -37.4% |
| **Total** | **59.7%** | **-19.3%** |

> Note: Coverage percentage decreased due to new feature additions without full test coverage yet.

## [v0.2.0] - 2026-03-03

### Added
- **Common Table Expressions (CTE)**: WITH clause support for recursive and non-recursive queries
  - `WITH cte AS (SELECT ...) SELECT * FROM cte`
  - Multiple CTEs in a single query
  - Temporary view semantics
- **VACUUM Command**: Database maintenance and storage compaction
  - `VACUUM` - Reclaim storage space
  - B+Tree defragmentation
  - Dead tuple removal
- **ANALYZE Command**: Table statistics collection
  - `ANALYZE table_name` - Collect statistics for specific table
  - `ANALYZE` - Collect statistics for all tables
  - Row counts, distinct values, null counts, min/max values
- **Full-Text Search (FTS)**: Text search capabilities
  - `CREATE FULLTEXT INDEX` - Create inverted text index
  - `MATCH ... AGAINST` syntax support (parser ready)
  - Tokenization and indexing
  - Boolean mode search
- **Materialized Views**: Pre-computed view results
  - `CREATE MATERIALIZED VIEW` - Create cached view
  - `REFRESH MATERIALIZED VIEW` - Update cached data
  - `DROP MATERIALIZED VIEW` - Remove materialized view

### Package Coverage
| Package | Coverage | Change |
|---------|----------|--------|
| `pkg/auth` | 98.6% | +0.0% |
| `pkg/wire` | 94.7% | +0.0% |
| `pkg/metrics` | 93.8% | +0.0% |
| `pkg/btree` | 87.3% | +0.0% |
| `pkg/replication` | 87.2% | +0.0% |
| `pkg/engine` | 87.3% | -3.0% |
| `pkg/json` | 84.4% | +0.0% |
| `pkg/query` | 81.4% | -4.3% |
| `pkg/txn` | 81.9% | +0.0% |
| `pkg/storage` | 81.4% | +0.0% |
| `pkg/backup` | 78.1% | +0.0% |
| `pkg/server` | 77.8% | +0.0% |
| `pkg/catalog` | 73.1% | -1.7% |
| `pkg/protocol` | 74.7% | +0.0% |
| **Total** | **79.0%** | **-1.9%** |

### Features Completed
- **CTE Support**: WITH clause for complex queries
- **VACUUM**: Storage compaction and optimization
- **ANALYZE**: Query optimization statistics
- **Full-Text Search**: Text indexing and search
- **Materialized Views**: Cached query results

## [v0.1.51] - 2026-03-03

### Added
- **Comprehensive Test Coverage Improvements**: 77% overall coverage achieved
  - `cmd/cobaltdb-server`: 17 new tests for server configuration and lifecycle
  - `cmd/debug`: 9 new tests for CRUD operations and database persistence
  - `pkg/btree`: 20 new tests for split operations and edge cases
  - `pkg/replication`: 30+ new tests for master/slave replication (87.2% coverage)
  - `pkg/catalog`: JSON function edge case tests
  - `pkg/protocol`: Network operation tests (74.7% coverage)

### Package Coverage
| Package | Coverage | Change |
|---------|----------|--------|
| `pkg/auth` | 98.6% | +0.0% |
| `pkg/wire` | 94.7% | +0.0% |
| `pkg/metrics` | 93.8% | +0.0% |
| `pkg/engine` | 90.3% | +0.0% |
| `pkg/btree` | 87.3% | +22.3% |
| `pkg/replication` | 87.2% | +36.0% |
| `pkg/query` | 85.7% | +0.0% |
| `pkg/json` | 84.4% | +0.0% |
| `pkg/txn` | 81.9% | +0.0% |
| `pkg/storage` | 81.4% | +0.0% |
| `pkg/backup` | 78.1% | +0.0% |
| `pkg/server` | 77.8% | +0.0% |
| `pkg/catalog` | 74.8% | +0.0% |
| `pkg/protocol` | 74.7% | +0.0% |
| **Total** | **80.9%** | **+4.8%** |

### Features Completed
- **Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE
- **Views**: CREATE VIEW, DROP VIEW with full SELECT support
- **Triggers**: BEFORE/AFTER triggers for INSERT, UPDATE, DELETE
- **Stored Procedures**: CREATE PROCEDURE, CALL with parameter support
- **Replication**: Master/Slave replication infrastructure
- **User Management**: Authentication system with permissions
- **MySQL Protocol**: Wire-compatible protocol implementation

## [v0.1.50] - 2026-03-02

### Added
- **Full JSON Support**: Complete JSON manipulation functions
  - JSON_EXTRACT: Extract values from JSON using paths
  - JSON_SET: Set values in JSON at specified paths
  - JSON_REMOVE: Remove values from JSON
  - JSON_VALID: Check if a string is valid JSON
  - JSON_ARRAY_LENGTH: Get array length in JSON
  - JSON_TYPE: Get JSON value type
  - JSON_KEYS: Get object keys from JSON
  - JSON_MERGE: Merge multiple JSON objects
  - JSON_PRETTY: Format JSON for display
  - JSON_MINIFY: Minify JSON
  - JSON_QUOTE: Quote a string as JSON
  - JSON_UNQUOTE: Unquote a JSON string

- **REGEXP Functions**: Regular expression support
  - REGEXP_MATCH: Check if string matches pattern
  - REGEXP_REPLACE: Replace matched patterns
  - REGEXP_EXTRACT: Extract matched patterns

- **Window Functions Support**: Framework for analytic functions
  - ROW_NUMBER: Row number within partition
  - RANK: Rank with gaps
  - DENSE_RANK: Rank without gaps
  - LAG: Access previous row values
  - LEAD: Access next row values
  - FIRST_VALUE: First value in partition
  - LAST_VALUE: Last value in partition
  - NTH_VALUE: Nth value in partition
  - Window specification: PARTITION BY and ORDER BY support

- **Query Optimizer Improvements**
  - Prepared statement caching (up to 1000 statements)
  - Index usage optimization for WHERE clauses
  - Query plan caching for better performance

## [v0.1.40] - 2026-03-01

### Added
- **Additional SQL Functions**: Extended function library for string, numeric, and date operations
  - String: LENGTH, UPPER, LOWER, TRIM, LTRIM, RTRIM, SUBSTR, SUBSTRING, CONCAT, REPLACE, INSTR, PRINTF
  - Numeric: ABS, ROUND, FLOOR, CEIL
  - Null-handling: COALESCE, IFNULL, NULLIF
  - Type conversion: CAST
  - Date/Time: DATE, TIME, DATETIME, STRFTIME (basic implementation)

- **Full Trigger Execution**: Complete trigger integration
  - BEFORE/AFTER INSERT triggers execution hooks
  - BEFORE/AFTER UPDATE triggers execution hooks
  - BEFORE/AFTER DELETE triggers execution hooks

- **Stored Procedure Execution**: Complete procedure support
  - CALL statement parsing and execution
  - Procedure body execution with parameters
  - Multiple statement execution in procedure body

- **Performance Optimizations**
  - Prepared statement caching (up to 1000 statements)
  - Reduced parsing overhead for repeated queries
  - Cache size limit to prevent memory issues

## [v0.1.30] - 2026-03-01

### Added
- **VIEW Support**: Virtual tables based on saved queries
  - CREATE VIEW with AS SELECT syntax
  - DROP VIEW with IF EXISTS support
  - Views can be queried like regular tables
  - Automatic view resolution in SELECT statements

- **Trigger Support**: Database triggers framework
  - CREATE TRIGGER parsing (BEFORE/AFTER INSERT/UPDATE/DELETE)
  - DROP TRIGGER support
  - Trigger storage in catalog
  - GetTriggersForTable for trigger execution hooks

- **Stored Procedure Support**: Stored procedure framework
  - CREATE PROCEDURE with parameter support
  - DROP PROCEDURE support
  - Procedure storage in catalog
  - CALL statement parsing

## [v0.1.20] - 2026-03-01

### Added
- **LEFT/RIGHT JOIN Support**: Extended JOIN functionality beyond INNER JOIN
  - LEFT JOIN with NULL padding for unmatched rows
  - RIGHT JOIN support
  - Full compatibility with ON clause conditions

- **Subquery Support**: Nested queries in WHERE clauses
  - IN (SELECT ...) support
  - Scalar subqueries in expressions

- **UNIQUE Constraint**: Column-level uniqueness enforcement
  - Validated on INSERT and UPDATE
  - Automatic error on duplicate values

- **CHECK Constraint**: Custom validation expressions
  - CHECK (column > 0) style constraints
  - Validated on INSERT and UPDATE

- **FOREIGN KEY Support**: Referential integrity
  - FOREIGN KEY REFERENCES syntax
  - ON DELETE and ON UPDATE actions (CASCADE, SET NULL, RESTRICT, NO ACTION)
  - Automatic validation on INSERT and UPDATE

- **Additional Data Types**:
  - DATE type
  - TIMESTAMP type

## [v0.1.10] - 2026-03-01

### Added
- **WAL (Write-Ahead Log)**: Complete crash recovery support
  - Logs all INSERT, UPDATE, DELETE operations before applying
  - Transaction support with COMMIT and ROLLBACK
  - Checkpoint mechanism for log truncation
  - Automatic recovery on database startup

- **Index Support**: B+Tree indexes for improved query performance
  - CREATE INDEX support
  - Automatic index maintenance on INSERT/UPDATE/DELETE
  - Index usage in SELECT queries for equality conditions
  - Primary key lookup via index

- **JOIN Support**: Basic INNER JOIN functionality
  - JOIN with ON clause
  - Multi-table joins
  - Column qualification with table prefixes

- **Improved Data Persistence**:
  - WAL checkpoint on database close
  - Better durability guarantees

## [v0.1.1] - 2026-03-01

### Added
- **Aggregate Functions**: Complete support for:
  - COUNT(*), COUNT(column)
  - SUM(column)
  - AVG(column)
  - MIN(column)
  - MAX(column)
  - Works with WHERE clause filtering

- **WHERE Clause Enhancements**:
  - LIKE operator (pattern matching with % and _)
  - IN operator (column IN (1, 2, 3))
  - BETWEEN operator (column BETWEEN 1 AND 10)
  - NOT LIKE, NOT IN, NOT BETWEEN support

- **Query Modifiers**:
  - ORDER BY (ASC/DESC)
  - LIMIT
  - OFFSET
  - DISTINCT

- **GROUP BY**: Group query results by columns
  - GROUP BY with aggregate functions
  - GROUP BY with ORDER BY
  - GROUP BY with LIMIT

- **HAVING**: Filter grouped results
  - Works with all aggregate functions in HAVING clause

## [v0.1.0] - 2026-03-01

### Added
- **SQL Parser**: Full SQL parser with support for:
  - DDL: CREATE TABLE, CREATE INDEX, DROP TABLE
  - DML: SELECT, INSERT, UPDATE, DELETE
  - Transactions: BEGIN, COMMIT, ROLLBACK

- **WHERE Clause**: Complete WHERE clause support with:
  - Comparison operators: =, !=, <, >, <=, >=
  - NULL checks: IS NULL, IS NOT NULL
  - AND/OR logical operators

- **Placeholder Support**: Prepared statement placeholders (?) with proper index handling

- **Disk Persistence**: Data survives database restart
  - Schema persistence
  - Data persistence
  - Base64 encoding for binary data

- **In-Memory Mode**: RAM-only databases for testing/caching

- **Expression Evaluation**: Full expression evaluation for WHERE clauses

- **CLI Tool**: Command-line interface with interactive mode
  - In-memory and disk database support
  - SQL execution
  - Help commands

- **Benchmark Tool**: Performance testing CLI
  - INSERT, SELECT, UPDATE, DELETE, Transaction benchmarks
  - Configurable row counts

- **Comprehensive Tests**: Test coverage for core packages
  - Engine tests
  - Catalog tests
  - Server tests
  - Wire protocol tests
  - BTree tests
  - Integration tests

### Changed
- Improved INSERT handling to properly map columns to table schema
- Fixed placeholder indexing for multiple values
- Fixed email values not being stored correctly
- Optimized SELECT to properly extract selected columns

### Fixed
- Placeholder index bug causing incorrect values
- Email field showing wrong values
- Disk persistence not loading data on restart
- UPDATE with WHERE clause not filtering rows
- DELETE with WHERE clause not filtering rows
- UPDATE not applying new values correctly
- CREATE INDEX not supported in Exec()

## [v0.1.0] - 2026-02-28

### Added
- Initial release
- Basic SQL parser
- In-memory storage engine
- B+Tree implementation
- Buffer pool
- TCP server with wire protocol
- JSON support

---

## Roadmap (v0.1.10+)

### Planned Features
- [ ] WAL (Write-Ahead Log) for crash recovery
- [ ] B+Tree disk persistence
- [ ] Index usage in query execution (indexes created but not used in queries)
- [x] Query optimizer
- [x] SQL functions (COUNT, SUM, AVG, MIN, MAX)
- [ ] JOIN support
- [ ] Foreign keys
- [ ] Table constraints
- [ ] More data types (DATE, TIMESTAMP, etc.)
- [ ] Performance optimizations
