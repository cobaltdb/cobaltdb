# CobaltDB — Comprehensive Documentation

> **Version:** 0.6.0 | **Go:** 1.25+ (toolchain 1.26.1) | **License:** MIT
> **Embedded + Server SQL Database Engine | MySQL Wire-Protocol Compatible**

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Storage Layer](#3-storage-layer)
   - 3.1 [Backend Interface](#31-backend-interface)
   - 3.2 [B+Tree](#32-btree)
   - 3.3 [Buffer Pool](#33-buffer-pool)
   - 3.4 [Write-Ahead Log (WAL)](#34-write-ahead-log-wal)
   - 3.5 [Paging System](#35-paging-system)
   - 3.6 [Encryption at Rest](#36-encryption-at-rest)
   - 3.7 [Compression](#37-compression)
4. [Query Engine](#4-query-engine)
   - 4.1 [SQL Parser & Lexer](#41-sql-parser--lexer)
   - 4.2 [AST Node System](#42-ast-node-system)
   - 4.3 [Optimizer](#43-optimizer)
   - 4.4 [Catalog (SQL Execution)](#44-catalog-sql-execution)
   - 4.5 [Query Plan Cache](#45-query-plan-cache)
   - 4.6 [EXPLAIN](#46-explain)
5. [Transaction System](#5-transaction-system)
   - 5.1 [MVCC & Snapshot Isolation](#51-mvcc--snapshot-isolation)
   - 5.2 [Transaction Manager](#52-transaction-manager)
   - 5.3 [Lock Manager & Deadlock Detection](#53-lock-manager--deadlock-detection)
   - 5.4 [Version Store](#54-version-store)
6. [Network & Protocol Layer](#6-network--protocol-layer)
   - 6.1 [MySQL Wire Protocol](#61-mysql-wire-protocol)
   - 6.2 [Server](#62-server)
   - 6.3 [Wire Protocol Package](#63-wire-protocol-package)
   - 6.4 [WebUI](#64-webui)
7. [Security](#7-security)
   - 7.1 [Authentication](#71-authentication)
   - 7.2 [Audit Logging](#72-audit-logging)
   - 7.3 [Row-Level Security (RLS)](#73-row-level-security-rls)
   - 7.4 [TLS](#74-tls)
   - 7.5 [SQL Injection Protection](#75-sql-injection-protection)
   - 7.6 [Rate Limiter & Circuit Breaker](#76-rate-limiter--circuit-breaker)
8. [Production Features](#8-production-features)
   - 8.1 [Production Server](#81-production-server)
   - 8.2 [Circuit Breaker](#82-circuit-breaker)
   - 8.3 [Retry Logic](#83-retry-logic)
   - 8.4 [Health Checks & Monitoring](#84-health-checks--monitoring)
   - 8.5 [Metrics Collection](#85-metrics-collection)
   - 8.6 [Slow Query Logging](#86-slow-query-logging)
   - 8.7 [Alerting](#87-alerting)
9. [Replication & High Availability](#9-replication--high-availability)
   - 9.1 [Statement-Based Replication](#91-statement-based-replication)
   - 9.2 [Replication Manager](#92-replication-manager)
   - 9.3 [Snapshot Transfer](#93-snapshot-transfer)
10. [Advanced Features](#10-advanced-features)
    - 10.1 [Vector Search (HNSW)](#101-vector-search-hnsw)
    - 10.2 [Full-Text Search](#102-full-text-search)
    - 10.3 [Temporal Queries](#103-temporal-queries)
    - 10.4 [Window Functions](#104-window-functions)
    - 10.5 [CTE (Common Table Expressions)](#105-cte-common-table-expressions)
    - 10.6 [Views & Materialized Views](#106-views--materialized-views)
    - 10.7 [Triggers & Stored Procedures](#107-triggers--stored-procedures)
    - 10.8 [Foreign Data Wrappers (FDW)](#108-foreign-data-wrappers-fdw)
    - 10.9 [JSON Operations](#109-json-operations)
    - 10.10 [WASM Compilation](#1010-wasm-compilation)
    - 10.11 [Partitioning](#1011-partitioning)
11. [Scheduler & Maintenance](#11-scheduler--maintenance)
    - 11.1 [Background Jobs](#111-background-jobs)
    - 11.2 [Auto-Vacuum](#112-auto-vacuum)
    - 11.3 [Auto-Checkpoint](#113-auto-checkpoint)
    - 11.4 [Index Advisor](#114-index-advisor)
    - 11.5 [Hot Backup](#115-hot-backup)
12. [SDKs & Client Libraries](#12-sdks--client-libraries)
    - 12.1 [Go SDK (Embedded)](#121-go-sdk-embedded)
    - 12.2 [Go SDK (database/sql Driver)](#122-go-sdk-databasesql-driver)
    - 12.3 [Python SDK](#123-python-sdk)
    - 12.4 [Node.js SDK](#124-nodejs-sdk)
    - 12.5 [Java SDK](#125-java-sdk)
13. [CLI & Tools](#13-cli--tools)
    - 13.1 [cobaltdb-server](#131-cobaltdb-server)
    - 13.2 [cobaltdb-cli](#132-cobaltdb-cli)
    - 13.3 [cobaltdb-bench](#133-cobaltdb-bench)
    - 13.4 [cobaltdb-migrate](#134-cobaltdb-migrate)
14. [Deployment](#14-deployment)
    - 14.1 [Standalone Binary](#141-standalone-binary)
    - 14.2 [Docker](#142-docker)
    - 14.3 [Docker Compose (with Monitoring)](#143-docker-compose-with-monitoring)
    - 14.4 [Kubernetes](#144-kubernetes)
    - 14.5 [Configuration](#145-configuration)
15. [Monitoring & Observability](#15-monitoring--observability)
    - 15.1 [Prometheus](#151-prometheus)
    - 15.2 [Grafana](#152-grafana)
    - 15.3 [Health Endpoints](#153-health-endpoints)
    - 15.4 [Structured Logging](#154-structured-logging)
16. [Testing & Quality](#16-testing--quality)
    - 16.1 [Test Coverage](#161-test-coverage)
    - 16.2 [Test Organization](#162-test-organization)
    - 16.3 [Chaos Engineering](#163-chaos-engineering)
17. [Performance](#17-performance)
    - 17.1 [B-Tree Benchmarks](#171-b-tree-benchmarks)
    - 17.2 [SQL Benchmarks](#172-sql-benchmarks)
    - 17.3 [Parser Benchmarks](#173-parser-benchmarks)
18. [Data Types & SQL Reference](#18-data-types--sql-reference)
    - 18.1 [Data Types](#181-data-types)
    - 18.2 [SQL Functions](#182-sql-functions)
    - 18.3 [Supported Statements](#183-supported-statements)
19. [Limitations & Known Issues](#19-limitations--known-issues)
20. [Appendix: Package Reference](#20-appendix-package-reference)

---

## 1. Project Overview

CobaltDB is a **modern SQL database engine** written entirely in **pure Go** (zero CGO). It operates in two modes:

| Mode | Description | Use Case |
|------|-------------|----------|
| **Embedded Library** | Import `pkg/engine` into any Go application | Go programs needing an embedded database |
| **Standalone Server** | MySQL wire-protocol compatible server | Any MySQL client/ORM/language connecting over TCP |

### Key Differentiators

- **Zero CGO, Zero Dependencies** — Cross-compile to any OS/arch from a single Go toolchain
- **Dual Mode** — Embedded library + MySQL-compatible server in one binary
- **Security-First Design** — AES-256-GCM encryption (data + WAL + audit logs), TLS 1.2+, Argon2id password hashing
- **ACID + MVCC** — Snapshot isolation, lock-free reads, WAL durability, deadlock detection
- **Advanced SQL** — Window functions, CTEs (recursive), JSON path queries, vector search (HNSW), temporal queries (AS OF), full-text search, triggers, stored procedures
- **Production-Oriented** — Circuit breaker, rate limiter, retry logic, health checks, Prometheus metrics, Grafana dashboards, 7,100+ test functions

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────────┐  │
│  │ Go SDK   │  │ Python   │  │ Node.js  │  │ Any MySQL       │  │
│  │ (Embed)  │  │ SDK      │  │ SDK      │  │ Client / ORM    │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬────────┘  │
│       │              │             │                  │          │
│  ┌────▼──────────────▼─────────────▼──────────────────▼────────┐ │
│  │                    NETWORK LAYER                            │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │ │
│  │  │ MySQL Wire   │  │ HTTP Server  │  │ WebUI            │   │ │
│  │  │ Protocol     │  │ (Health/API) │  │ (Admin Console)  │   │ │
│  │  └──────┬───────┘  └──────┬───────┘  └──────────────────┘   │ │
│  └─────────┼─────────────────┼──────────────────────────────────┘ │
│            │                 │                                    │
│  ┌─────────▼─────────────────▼──────────────────────────────────┐ │
│  │                    ENGINE LAYER                              │ │
│  │  ┌─────────────────────────────────────────────────────────┐ │ │
│  │  │               DB (Database Instance)                    │ │ │
│  │  │  ┌──────────┐  ┌───────────┐  ┌──────────────┐        │ │ │
│  │  │  │ Catalog  │  │  Txn Mgr  │  │  Optimizer   │        │ │ │
│  │  │  │(SQL Exec)│  │  (MVCC)   │  │  (Planner)   │        │ │ │
│  │  │  └────┬─────┘  └─────┬─────┘  └──────┬───────┘        │ │ │
│  │  │       │              │                │                │ │ │
│  │  │  ┌────▼──────────────▼────────────────▼───────────┐    │ │ │
│  │  │  │              STORAGE ENGINE                     │    │ │ │
│  │  │  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐ │    │ │ │
│  │  │  │  │  B+Tree  │  │ Buffer   │  │   WAL        │ │    │ │ │
│  │  │  │  │ (Row/Del)│  │ Pool     │  │ (Write-Ahead)│ │    │ │ │
│  │  │  │  └──────────┘  │ (LRU)    │  └──────────────┘ │    │ │ │
│  │  │  │                └──────────┘                    │    │ │ │
│  │  │  └────────────────────────────────────────────────┘    │ │ │
│  │  └─────────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  ┌───────────────────────────────────────────────────────────────┐ │
│  │              I/O LAYER                                       │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │ │
│  │  │  Disk Backend│  │  Memory      │  │  Encrypted       │   │ │
│  │  │  (File I/O)  │  │  Backend     │  │  Backend (AEAD)  │   │ │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘   │ │
│  └───────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Map

| Layer | Package(s) | Responsibility |
|-------|-----------|----------------|
| **Client** | `sdk/go`, `sdk/python`, `sdk/js`, `sdk/java` | Multi-language client libraries |
| **Network** | `pkg/protocol`, `pkg/server`, `webui` | MySQL wire protocol, HTTP server, admin WebUI |
| **Engine** | `pkg/engine` | DB instance, lifecycle, query dispatch, circuit breaker, retry |
| **SQL** | `pkg/query`, `pkg/catalog`, `pkg/optimizer` | Lexer, parser, AST, optimizer, catalog execution |
| **Transaction** | `pkg/txn` | MVCC, lock manager, deadlock detection, version store |
| **Storage** | `pkg/btree`, `pkg/storage` | B+Tree, buffer pool, WAL, page management, encryption, compression |
| **I/O** | `pkg/storage` (backend) | Disk and memory backends, encrypted backend wrapper |
| **Security** | `pkg/auth`, `pkg/audit`, `pkg/security` | Authentication, audit logging, RLS |
| **Replication** | `pkg/replication` | Master-slave statement-based replication |
| **Monitoring** | `pkg/metrics`, `monitoring/` | Prometheus metrics, Grafana dashboards, slow query logging |
| **Maintenance** | `pkg/scheduler`, `pkg/advisor` | Background jobs, auto-vacuum, index advisor |

---

## 3. Storage Layer

### 3.1 Backend Interface

**Package:** `pkg/storage` — `backend.go`

The `Backend` interface is the lowest-level abstraction for reading and writing bytes:

```go
type Backend interface {
    ReadAt(buf []byte, offset int64) (int, error)
    WriteAt(buf []byte, offset int64) (int, error)
    Sync() error
    Size() int64
    Truncate(size int64) error
    Close() error
}
```

**Implementations:**
- **`DiskBackend`** (`disk.go`) — File-based storage using `os.File`. Supports `Sync()`, `Truncate()`, and `Size()`. Used for persistent databases.
- **`MemoryBackend`** (`memory.go`) — In-memory `[]byte` slice. Used for `:memory:` databases. Zero persistence.
- **`EncryptedBackend`** (`encryption.go`) — Wraps any Backend with transparent AES-256-GCM authenticated encryption/decryption. Every 4KB page carries its own nonce and authentication tag.

**Helper functions:**
- `WriteFullAt(backend, buf, offset)` — Guarantees full write or error
- `ReadFullAt(backend, buf, offset)` — Guarantees full read or error

### 3.2 B+Tree

**Package:** `pkg/btree` — `btree.go`

The B+Tree is the core data structure for row storage and primary keys. It provides ordered key-value storage with range query support.

**Key features:**
- **Ordered storage** — Keys stored in sorted order for efficient range scans
- **Configurable order** — B+Tree branching factor configurable at creation
- **Concurrent access** — Read-write lock with sharded in-memory storage (256 shards)
- **LRU eviction** — Intrusive doubly-linked list with per-entry memory tracking
- **Overflow pages** — Values larger than one page spill to overflow pages
- **CRC64 integrity** — Every node carries a CRC64 checksum

**Core operations:**
- `Put(key, value)` / `PutStringNoCopy(key, value)` — Insert or update
- `Get(key)` — Point lookup
- `Delete(key)` — Remove key
- `Seek(key)` — Find first key >= given key (cursor)
- `Scan(startKey, endKey)` — Range scan
- `Ascend(fn)` / `Descend(fn)` — Ordered traversal

**Batch operations:**
- `PutBatch(entries)` — Atomic batch insert (used by transaction commit)

**Implemented in:** `pkg/btree/btree.go` (~1,500 lines)  
**Interfaces defined in:** `pkg/btree/interfaces.go`

### 3.3 Buffer Pool

**Package:** `pkg/storage` — `buffer_pool.go`

The buffer pool caches disk pages in memory, providing an LRU-evicted page cache.

**Key structures:**
- `CachedPage` — Wraps a page with its ID, pin count, dirty flag, and LRU list element
- `BufferPool` — LRU-evicted page cache with background flusher

**Key features:**
- LRU eviction with probabilistic touch tracking (throttled to every Nth access to reduce lock contention)
- Page pinning for sequential access patterns
- Background flusher goroutine that writes dirty pages at configurable intervals
- Stats tracking (hits, misses, evictions, flushes)
- `PauseBackgroundFlusher()` / `ResumeBackgroundFlusher()` for hot backup consistency

**Buffer pool stats** (`buffer_pool_stats.go`):
- `Stats()` — Returns hit rate, miss rate, eviction count, flush count, dirty page count
- `FlushErrorCount()` — Tracks write-back errors
- `AllocatedPageCount()` — Current total allocated page count

### 3.4 Write-Ahead Log (WAL)

**Package:** `pkg/storage` — `wal.go`

The WAL provides durability and crash recovery. Every mutation is appended to the log before it is applied to the B+Tree (write-ahead logging).

**Record types:**
| Type | Byte | Description |
|------|------|-------------|
| `WALInsert` | 0x01 | Row insert |
| `WALUpdate` | 0x02 | Row update |
| `WALDelete` | 0x03 | Row delete |
| `WALCommit` | 0x04 | Transaction commit marker |
| `WALRollback` | 0x05 | Transaction rollback marker |
| `WALCheckpoint` | 0x06 | Checkpoint marker |
| `WALUpdateCommit` | 0x07 | Combined update+commit (optimization) |

**On-disk format:**
```
[LSN:8][TxnID:8][Type:1][PageID:4][Offset:2][Length:2][Data:N][CRC:4]
```

**Key features:**
- **LSN-based sequencing** — Monotonically increasing log sequence numbers
- **Group commit** — Batches small writes into 1MB buffer flushes
- **WAL Encryption** — Optional AEAD encryption for WAL records
- **Crash recovery** — On restart, replays committed records and discards uncommitted ones
- **Recovery limits** — Cap on pending records (100K) and bytes (256MB) to prevent OOM from corrupt WAL
- **Checkpoint integration** — After checkpoint, WAL is truncated; checkpoint blocks concurrent commits to ensure consistency

### 3.5 Paging System

**Package:** `pkg/storage` — `page.go`

Fixed-size page management:

- **Default page size:** 4,096 bytes (4KB)
- **Maximum page size:** 65,536 bytes (64KB)
- **Page pool:** `sync.Pool` recycles page-sized buffers to reduce GC pressure
- **Error types:** `ErrInvalidPageID`, `ErrInvalidPageType`, `ErrPageCorrupted`, `ErrPageIDExhausted`

### 3.6 Encryption at Rest

**Package:** `pkg/storage` — `encryption.go`

**Algorithms:**
- **AES-256-GCM** — Authenticated encryption with 256-bit key
- **Argon2id** — Memory-hard key derivation function (recommended, configurable)
- **PBKDF2** — Fallback key derivation with configurable iteration count (default: 100,000)

**`EncryptionConfig`:**
```go
type EncryptionConfig struct {
    Enabled     bool    // Enable encryption
    Key         []byte  // 32 bytes for AES-256
    Salt        []byte  // Salt for key derivation
    Algorithm   string  // "aes-256-gcm" (default)
    UseArgon2   bool    // Use Argon2id (recommended)
    PBKDF2Iters int     // PBKDF2 iterations (default: 100000)
}
```

**How it works:**
- `EncryptedBackend` wraps any `Backend` implementation
- Each 4KB data page gets its own 12-byte random nonce stored before the ciphertext
- WAL can be independently encrypted (see `wal_encryption_test.go` and WAL header authentication)
- Key generation via `storage.GenerateSecureKey()`

### 3.7 Compression

**Package:** `pkg/storage` — `compression.go`

- Optional page-level compression
- Used for reducing storage footprint
- Configurable compression buffers
- Decompression on page load (transparent to higher layers)

---

## 4. Query Engine

### 4.1 SQL Parser & Lexer

**Package:** `pkg/query`

#### Lexer (`lexer.go`)
- Tokenizes SQL text into a stream of tokens
- Handles identifiers (including quoted identifiers), string literals (single and double quotes), numbers (integers, floats, hex), comments (line and block)
- **Performance:** ~499 ns per tokenization (~2.0M ops/sec)

#### Token Types (`token.go`)
Defines ~140+ token types including:
- **Keywords:** SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, JOIN, GROUP, ORDER, WHERE, HAVING, LIMIT, OFFSET, UNION, INTERSECT, EXCEPT, WITH, RECURSIVE, WINDOW, TRIGGER, PROCEDURE, VIEW, MATERIALIZED, VACUUM, ANALYZE, EXPLAIN, TRUNCATE, REPLACE, etc.
- **Operators:** +, -, *, /, =, <>, <, >, <=, >=, AND, OR, NOT, IN, BETWEEN, LIKE, IS, EXISTS, || (concat), &, |, <<, >>, ~ (bitwise)
- **Literals:** Identifier, String, Number, Boolean, Null
- **Special:** EOF, Illegal, Whitespace, Placeholder (?), Comment, SemiColon, Comma, Dot, Star, Parens

#### Parser (`parser.go`, `parser_ddl.go`, `parser_dml_select.go`, `parser_expression.go`)
- Recursive descent parser
- Parses SQL text into an Abstract Syntax Tree (AST)
- **Statements:** SELECT (including subqueries, JOINs, UNION/INTERSECT/EXCEPT, CTE, window functions), INSERT (including INSERT FROM SELECT, ON CONFLICT upsert), UPDATE (including FROM and JOIN), DELETE (including USING), CREATE TABLE/INDEX/VIEW/TRIGGER/PROCEDURE/DATABASE, ALTER TABLE, DROP TABLE/INDEX/VIEW/TRIGGER/PROCEDURE, TRUNCATE, EXPLAIN, ANALYZE, VACUUM, CALL, BEGIN/COMMIT/ROLLBACK/SAVEPOINT, SET, SHOW
- **Performance:** ~826 ns for simple SELECT parsing
- **Strict mode parser** — `parser_strict_test.go` validates error paths
- **Fuzz testing** — `fuzz_test.go` for parser robustness

### 4.2 AST Node System

**Package:** `pkg/query` — `ast.go`

The AST is organized as a hierarchy of typed node interfaces:

```
Node (base interface)
├── Statement (SQL statement)
│   ├── SelectStmt
│   ├── InsertStmt
│   ├── UpdateStmt
│   ├── DeleteStmt
│   ├── CreateTableStmt
│   ├── CreateIndexStmt
│   ├── CreateViewStmt
│   ├── CreateTriggerStmt
│   ├── CreateProcedureStmt
│   ├── AlterTableStmt
│   ├── DropStmt
│   ├── BeginStmt / CommitStmt / RollbackStmt
│   ├── SavepointStmt
│   ├── ExplainStmt
│   ├── CallStmt
│   ├── TruncateStmt
│   ├── SetStmt / ShowStmt
│   ├── AnalyzeStmt / VacuumStmt
│   ├── LockTablesStmt
│   └── ...
└── Expression (value expression)
    ├── BinaryExpr (+, -, *, /, AND, OR, =, <, >, etc.)
    ├── UnaryExpr (-, NOT)
    ├── FunctionCall (COUNT, SUM, JSON_EXTRACT, etc.)
    ├── Identifier / QualifiedIdentifier / ColumnRef
    ├── Literal types: StringLiteral, NumberLiteral, BooleanLiteral, NullLiteral, VectorLiteral
    ├── PlaceholderExpr (?)
    ├── InExpr / BetweenExpr / LikeExpr / IsNullExpr
    ├── CastExpr
    ├── CaseExpr
    ├── SubqueryExpr / ExistsExpr
    ├── StarExpr (*)
    ├── JSONPathExpr / JSONContainsExpr
    ├── AliasExpr
    ├── MatchExpr (Full-text search)
    ├── WindowExpr / WindowSpec
    └── DefaultExpr / IntervalExpr
```

**Visitor pattern:** `ExpressionVisitor` interface with methods for each expression type, enabling AST traversal and transformation via `Walk()`.

**Clone system:** All AST nodes support deep cloning via `Clone()` methods in `clone.go`.

**Query utilities** (`query_utils.go`): `StatementType()`, `TablesUsed()`, `ColumnsUsed()`, `GenerateQueryKey()`, etc.

### 4.3 Optimizer

**Package:** `pkg/optimizer` — `optimizer.go`

The query optimizer transforms parsed SQL into efficient execution plans.

**Optimizations:**
- **Predicate pushdown** — WHERE clause filters pushed to data access level
- **Join ordering** — Table order in JOINs optimized (smaller tables first)
- **Index selection** — Automatic index choice based on WHERE conditions
- **Constant folding** — Evaluate constant expressions at plan time
- **Subquery flattening** — Convert correlated subqueries to JOINs where possible

**Benchmarks:**
- `optimizer_benchmark_test.go` — Performance measurement
- `optimizer_reorder_fixes_test.go` — Join ordering correctness

### 4.4 Catalog (SQL Execution)

**Package:** `pkg/catalog` (~8,500 lines of Go code)

The catalog is the SQL execution engine — it takes parsed AST statements and executes them against the B+Tree storage.

#### Table & Schema Management (`catalog_core.go`, `catalog_ddl.go`)
- **TableDef** — Schema definition with columns, primary key, foreign keys, check constraints, auto-increment, partitioning
- **ColumnDef** — Column name, type (INTEGER, REAL, TEXT, BOOLEAN, JSON, VECTOR, DATE, TIMESTAMP), nullable, default value, constraints
- **PrimaryKey** — Supports composite primary keys
- **Foreign keys** — With ON DELETE/ON UPDATE (CASCADE, SET NULL, RESTRICT, NO ACTION)
- **CHECK constraints** — Arbitrary boolean expression
- **UNIQUE constraints** — Single and composite
- **Create/Drop/Alter table** — Full DDL support
- **Collections** — Schemaless document collections (like MongoDB collections)

#### DML Operations
- **INSERT** (`catalog_insert.go`) — Single row, batch, INSERT FROM SELECT, ON CONFLICT (upsert), RETURNING
- **UPDATE** (`catalog_update.go`) — With JOIN, FROM, WHERE subqueries, RETURNING
- **DELETE** (`catalog_delete.go`) — With JOIN, USING, WHERE subqueries, RETURNING
- **SELECT** (`catalog_select.go`, `catalog_select_helpers.go`) — Full query execution

#### Query Execution
- **SELECT processing:** FROM → JOIN → WHERE → GROUP BY → HAVING → WINDOW → ORDER BY → LIMIT/OFFSET
- **JOIN types:** INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL
- **Join algorithms:** Hash join (primary), nested loop join (fallback)
- **Set operations:** UNION, UNION ALL, INTERSECT, EXCEPT
- **Subqueries** — Correlated and uncorrelated, IN, EXISTS, scalar
- **CTE** (`catalog_cte.go`) — WITH clause, recursive CTE support
- **Views** (`catalog_view.go`) — CREATE VIEW, DROP VIEW, materialized views
- **Window functions** (`catalog_window.go`) — ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTILE, PERCENT_RANK, CUME_DIST, STDDEV, VARIANCE
- **Full-Text Search** (`catalog_fts.go`) — MATCH ... AGAINST
- **Vector search** (`catalog_vector.go`) — HNSW index for similarity queries
- **Temporal queries** (`temporal.go`) — AS OF SYSTEM TIME

#### Expression Evaluation (`catalog_eval.go`, `catalog_eval_string.go`, `catalog_eval_json.go`)
- Full expression evaluator that walks the AST and computes values
- **String functions:** LENGTH, UPPER, LOWER, TRIM, SUBSTR, CONCAT, REPLACE, INSTR, LIKE, GLOB
- **Numeric functions:** ABS, ROUND, FLOOR, CEIL, MOD, POWER, SQRT, EXP, LOG, RANDOM, ABS
- **Aggregate functions:** COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, STDDEV, VARIANCE
- **JSON functions:** JSON_EXTRACT, JSON_SET, JSON_REMOVE, JSON_VALID, JSON_ARRAY_LENGTH, JSON_MERGE, JSON_KEYS, JSON_CONTAINS, JSON_TYPE
- **Date/time functions:** DATE, TIME, DATETIME, STRFTIME, JULIANDAY, EXTRACT, DATE_ADD, DATE_SUB, DATEDIFF
- **Trig functions:** SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, DEGREES, RADIANS
- **Math functions:** CEIL, FLOOR, ROUND, TRUNC, SIGN, ABS, MOD, POW, SQRT, CBRT, EXP, LN, LOG, LOG2, LOG10
- **Window functions (evaluation):** Computed over partition frame with ordering
- **Type coercion** — Automatic conversion between INTEGER, REAL, TEXT, BOOLEAN, etc.
- **Bitwise operations** — &, |, ~, <<, >> operators
- **Special operators:** `<=>` (NULL-safe equality)

#### Index Management (`catalog_index.go`)
- **CREATE INDEX / DROP INDEX** on any column(s)
- **B+Tree secondary indexes** — Separate tree per index
- **Full-Text Search indexes** — Inverted index per FTS index
- **JSON indexes** — Index on JSON path expressions
- **Vector indexes** — HNSW (Hierarchical Navigable Small World) graph for similarity search
- **Automatic index usage** in query planning

#### Row-Level Security (`catalog_rls.go`)
- Policy evaluation during SELECT, INSERT, UPDATE, DELETE
- Integration with `pkg/security` manager

#### Transaction Integration (`catalog_txn.go`)
- BEGIN/COMMIT/ROLLBACK support
- SAVEPOINT and nested savepoints
- MVCC row versioning hooks
- Atomic batch writes on commit

#### FDW Integration (`catalog_fdw.go`)
- Foreign Data Wrapper execution
- Query pushdown to foreign tables
- CSV FDW support

#### JSON Utilities (`json_utils.go`)
- JSON path parsing and evaluation
- JSON array operations
- JSON type introspection

#### Fast Paths (`catalog_fastpath.go`)
- Optimized SELECT with LIMIT 1
- Optimized COUNT(*) scans
- Optimized simple point lookups

#### Query Cache (`catalog_query_local.go`)
- Integration with query plan cache
- Prepared statement caching

#### Maintenance (`catalog_maintenance.go`)
- VACUUM — Storage compaction
- ANALYZE — Statistics collection
- `rowCount` — Table size estimation
- `stats.go` — Column cardinality and distribution statistics

### 4.5 Query Plan Cache

**Package:** `pkg/engine` — `query_plan_cache.go`

An LRU cache for parsed query plans that avoids re-parsing and re-optimizing frequently executed queries.

```go
type QueryPlanCache struct {
    entries     map[string]*list.Element // Hash -> list element
    lruList     *list.List
    maxSize     int64       // Maximum cache size in bytes
    maxEntries  int         // Maximum number of entries
    // ...
}
```

- **LRU eviction** based on most recently used
- **Hash-keyed** by normalized SQL text (SHA-256)
- **Per-entry tracking:** access count, creation time, last access time, estimated size
- **Top-100 tracking** for statistics

### 4.6 EXPLAIN

**Package:** `pkg/engine` — `explain.go`

`EXPLAIN` produces a structured query execution plan:

```go
type QueryPlan struct {
    Nodes []PlanNode
}

type PlanNode struct {
    ID        int
    ParentID  int
    Operation string      // "SCAN", "SEEK", "JOIN", "AGGREGATE", etc.
    Detail    string      // Table name, index used, predicate
    Cost      float64     // Estimated cost
    Rows      int64       // Estimated row count
}
```

**Tree building:** `planBuilder` traverses the query structure and builds a node tree showing:
- Table scans (full or index)
- Index seeks
- Join operations with type
- Aggregate operations
- Sort/filter operations

---

## 5. Transaction System

### 5.1 MVCC & Snapshot Isolation

**Package:** `pkg/txn`

CobaltDB uses **Multi-Version Concurrency Control (MVCC)** with **Snapshot Isolation**:

- **Lock-free reads** — Readers see a consistent snapshot without blocking writers
- **Writers don't block readers** — Readers proceed concurrently with ongoing writes
- **First-committer-wins** — On conflict, second committer detects and aborts

### 5.2 Transaction Manager

**Package:** `pkg/txn` — `manager.go`

```go
type Manager struct {
    transactions  map[uint64]*Transaction
    nextID        uint64
    lockManager   LockManager
    versionStore  *VersionStore
    // ...
}
```

**Transaction states:** Active → Preparing → Committed | Aborted

**Key features:**
- Auto-incrementing transaction IDs (`nextTxnID`)
- Per-transaction version for MVCC snapshot isolation
- Timeout support — configurable per-transaction and lock wait timeouts
- Integration with WAL for durable commits

**Interfaces** (`txn/interfaces.go`):
```go
type LockManager interface {
    AcquireLock(txnID uint64, key string, timeout time.Duration) error
    AcquireLockMode(txnID uint64, key string, mode LockMode, timeout time.Duration) error
    ReleaseLock(txnID uint64, key string)
    ReleaseAllLocks(txnID uint64)
}

type TransactionManager interface {
    Begin(opts *Options) *Transaction
    Get(txnID uint64) (*Transaction, error)
    GetTransaction(txnID uint64) *Transaction
    GetCurrentVersion(key string) uint64
}
```

### 5.3 Lock Manager & Deadlock Detection

**Package:** `pkg/txn` — `manager.go`, `deadlock_test.go`

- **Lock modes:** Shared (multiple readers) and Exclusive (single writer)
- **Lock granularity:** Row-level locking via key hashing
- **Deadlock detection:** Wait-for graph with automatic cycle detection
- **Lock timeout:** Configurable via `LockWaitTimeout` option
- **Automatic release:** All locks released on transaction commit/rollback

**Wait cycle testing** (`wait_cycle_test.go`):
- Stress tests for concurrent lock acquisition
- Verifies that deadlocks are detected and resolved

### 5.4 Version Store

**Package:** `pkg/txn` — `version_store.go`

The version store maintains the multi-version history for MVCC:
- Stores previous row versions for active transactions
- Cleans up versions that are no longer visible to any active transaction
- Used for snapshot isolation (readers see consistent version from their snapshot time)

---

## 6. Network & Protocol Layer

### 6.1 MySQL Wire Protocol

**Package:** `pkg/protocol` — `mysql.go` (~2,634 lines)

Full implementation of the MySQL wire protocol (`COM_QUERY`, `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`, `COM_STMT_CLOSE`, `COM_PING`, `COM_QUIT`, etc.)

**Handshake sequence:**
1. Server sends initial handshake packet (protocol version, server version, connection ID, auth plugin data, capability flags)
2. Client responds with handshake response (username, auth response, database, capability flags)
3. Server authenticates and sends OK or ERR packet

**Supported commands:**
| Command | Description |
|---------|-------------|
| `COM_QUERY` | Execute SQL text directly |
| `COM_STMT_PREPARE` | Prepare parameterized statement |
| `COM_STMT_EXECUTE` | Execute prepared statement with parameters |
| `COM_STMT_CLOSE` | Close prepared statement |
| `COM_STMT_RESET` | Reset prepared statement (long data) |
| `COM_STMT_SEND_LONG_DATA` | Stream long data for prepared parameter |
| `COM_PING` | Health check |
| `COM_QUIT` | Connection close |
| `COM_INIT_DB` | Select database |
| `COM_FIELD_LIST` | List table columns |

**Authentication:**
- MySQL `mysql_native_password` protocol (SHA1-based challenge-response)
- `caching_sha2_password` support (full and fast authentication)
- Cleartext password (optional, for development)

**Security hardening:**
- Payload size limits enforced (16MB max, 1MB for command payloads)
- Parameter count limit (1024)
- Prepared statement count limit (1024)
- Long data aggregate limit (256MB per connection)
- Timeout enforcement (10s handshake, 5min command, 30s write)

**Coverage:** Test files include `mysql_test.go`, `mysql_more_test.go`, `mysql_stmt_test.go`, `mysql_extended_test.go`, `mysql_metadata_test.go`, `mysql_integration_test.go`, `mysql_coverage_test.go`, `mysql_deep_coverage_test.go`, `mysql_hardening_test.go`, `mysql_security_fixes_test.go`.

### 6.2 Server

**Package:** `pkg/server` — `server.go` (~998 lines)

The network server manages TCP connections, TLS, and client lifecycle:

```go
type Server struct {
    listener           net.Listener
    prodServer         *ProductionServer
    clients            map[uint64]*ClientConn
    auth               *auth.Authenticator
    sqlProtector       *SQLProtector
    // ...
}
```

**Server `Config`:**
| Field | Default | Description |
|-------|---------|-------------|
| `Address` | `:4200` | Listen address |
| `AuthEnabled` | `true` | Enable authentication |
| `RequireAuth` | `true` | Require authentication |
| `DefaultAdminUser` | `admin` | Default admin username |
| `MaxConnections` | 1000 | Maximum concurrent connections |
| `ReadTimeout` | 300s | Connection read timeout |
| `WriteTimeout` | 60s | Connection write timeout |
| `TLS` | nil | TLS configuration (nil = disabled) |
| `AllowCleartextAuth` | false | Allow cleartext auth without TLS (dev only) |

**Client lifecycle** (`lifecycle.go`):
- `GracefulShutdownHandler` — SIGTERM/SIGINT handling
- `ReadyCheck`, `LiveCheck` — Kubernetes liveness/readiness probes
- Configurable shutdown and drain timeouts

**TLS support** (`tls.go`):
- TLS 1.2/1.3
- Self-signed certificate generation
- Client certificate authentication

**SQL Protection** (`sql_protection.go`):
- SQL injection detection (15+ patterns)
- Block-on-detection mode
- Configurable query length, OR condition, UNION limits
- Whitelist support for trusted queries

**Rate Limiter** (`rate_limiter.go`):
- Token bucket algorithm
- Per-client and global rate limiting
- Adaptive rate limiting based on system load
- Configurable RPS, burst, cleanup interval, max clients

### 6.3 Wire Protocol Package

**Package:** `pkg/wire` — `protocol.go`

Low-level MySQL wire format helpers:
- Packet encoding/decoding
- Length-encoded integers
- Length-encoded strings
- Null-terminated strings
- Result set row encoding
- Error packet construction

### 6.4 WebUI

**Package:** `webui/`

An administrative web interface served alongside the database server:

**Files:**
| File | Purpose |
|------|---------|
| `server.go` | HTTP server with routing, static file serving |
| `tables.go` | Table listing and browsing |
| `auth.go` | WebUI authentication |
| `ratelimit.go` | WebUI rate limiting |
| `audit.go` | Audit log viewer |
| `templates/index.html` | Admin console HTML template |
| `static/app.js` | Client-side JavaScript |
| `static/style.css` | Styling |

**Endpoints:**
- `/` — Admin console home
- `/tables` — Browse tables
- `/query` — Execute SQL queries
- `/audit` — View audit logs
- `/config` — View/modify configuration

---

## 7. Security

### 7.1 Authentication

**Package:** `pkg/auth` — `auth.go` (~914 lines)

```go
type User struct {
    Username        string
    PasswordHash    string
    Salt            string
    MySQLNativeHash []byte    // SHA1(SHA1(password)) for MySQL protocol
    IsAdmin         bool
    CreatedAt       time.Time
    LastLogin       time.Time
    Permissions     []Permission
}
```

**Password hashing:**
- **Argon2id** — Modern memory-hard hash (recommended, default)
- **MySQL native** — SHA1(SHA1(password)) for MySQL protocol compatibility
- **Brute-force protection** — Account lockout after repeated failed attempts

**Session management:**
- Token-based sessions with expiry
- Per-user session limits (configurable)
- Permission-based authorization (per-database, per-table, per-action)

**Security hardening:**
- Random default password generation (16-char alphanumeric via `crypto/rand`)
- Rate-limited login attempts
- Password policy enforcement (length, complexity)
- Credential comparison via `subtle.ConstantTimeCompare`

### 7.2 Audit Logging

**Package:** `pkg/audit` — `logger.go` (~1,117 lines)

Encrypted, hash-chained, tamper-evident audit log:

```go
type Config struct {
    Enabled     bool
    LogFile     string       // Path to audit log
    LogFormat   string       // "json" or "text"
    Encryption  bool         // AES-256-GCM encryption
    MaxSize     int64        // Max file size before rotation (default: 100MB)
    MaxBackups  int          // Max rotated files to keep
    MaxAgeDays  int          // Max age of rotated files
}
```

**Event types:** Query, DDL, DML, Auth, Connection, Security, Admin

**Security features:**
- **Hash-chained** — Each entry includes SHA-256 hash of the previous entry
- **Encrypted** — Optional AES-256-GCM encryption of log entries
- **Offline verification** — `audit.VerifyLogFile()` function validates log integrity
- **Automatic rotation** — Rotates at configurable size (default 100MB)
- **Structured output** — JSON or text format

### 7.3 Row-Level Security (RLS)

**Package:** `pkg/security` — `rls.go` (~1,397 lines)

Policy-based row-level security:

```go
type Policy struct {
    Name        string
    TableName   string
    PolicyType  string      // "PERMISSIVE" or "RESTRICTIVE"
    Expression  string      // SQL boolean expression
    Principals  []string    // Users/groups the policy applies to
    // ...
}
```

**Features:**
- `USING` expressions applied to SELECT, INSERT, UPDATE, DELETE
- Policy-based filtering automatically appended to WHERE clauses
- Multi-policy support (permissive and restrictive types)
- Integration with authentication system (current user context)

**Hardening:**
- Bypass prevention in UPDATE...FROM and DELETE...USING (v0.3.0 fix)
- Expression validation against injection
- Maximum policy sizes enforced

### 7.4 TLS

**Package:** `pkg/server` — `tls.go`

```go
type TLSConfig struct {
    Enabled              bool
    GenerateSelfSigned   bool     // Auto-generate self-signed cert on start
    CertFile             string   // Path to TLS certificate
    KeyFile              string   // Path to TLS key
    ClientAuth           bool     // Require client certificate
}
```

- TLS 1.2+ minimum version
- Self-signed certificate generation for development
- Client certificate authentication support
- Proper cipher suite selection

### 7.5 SQL Injection Protection

**Package:** `pkg/server` — `sql_protection.go`

```go
type SQLProtectionConfig struct {
    Enabled             bool     // Enable SQL injection protection
    BlockOnDetection    bool     // Block query execution on detection
    MaxQueryLength      int      // Maximum allowed query length
    MaxORConditions     int      // Maximum OR conditions in WHERE
    MaxUNIONCount       int      // Maximum UNION clauses
    SuspiciousThreshold int      // Detection sensitivity
}
```

**Detection patterns (15+):**
- UNION-based injection
- Time-based blind (BENCHMARK, SLEEP, WAITFOR DELAY)
- Conditional blind (OR 1=1, AND 1=2)
- Comment-based evasion
- OOB exfiltration (INTO OUTFILE, COPY, LOAD_FILE, UTL_HTTP)
- Stacked queries
- Hex/char encoding evasion
- Comparison-based blind
- Information schema probing
- Boolean-based blind
- Heavy query detection
- Error-based detection

### 7.6 Rate Limiter & Circuit Breaker

See [Section 8 — Production Features](#8-production-features).

---

## 8. Production Features

### 8.1 Production Server

**Package:** `pkg/server` — `production.go`

The `ProductionServer` wraps the basic server with all resilience features:

```go
type ProductionConfig struct {
    Lifecycle             *LifecycleConfig
    EnableCircuitBreaker  bool
    CircuitBreaker        *CircuitBreakerConfig
    EnableRetry           bool
    Retry                 *RetryConfig
    EnableRateLimiter     bool
    RateLimiter           *RateLimiterConfig
    EnableSQLProtection   bool
    SQLProtection         *SQLProtectionConfig
    EnableHealthServer    bool
    HealthAddr            string     // Default: 127.0.0.1:8420
}
```

**`LifecycleConfig`:**
| Field | Default | Description |
|-------|---------|-------------|
| `ShutdownTimeout` | 30s | Time to wait for graceful shutdown |
| `DrainTimeout` | 10s | Time to wait for connection drain |

### 8.2 Circuit Breaker

**Package:** `pkg/engine` — `circuit_breaker.go`

Three-state circuit breaker for resilience:

```
Closed (normal) → Open (failing) → Half-Open (recovery test) → Closed
```

```go
type CircuitBreakerConfig struct {
    MaxFailures          int           // Default: 5
    MinSuccesses         int           // Default: 3
    ResetTimeout         time.Duration // Default: 30s
    MaxConcurrency       int           // Default: 100
    HalfOpenMaxRequests  int           // Default: 1/sec
}
```

**States:**
| State | Behavior |
|-------|----------|
| **Closed** | Normal operation; requests pass through |
| **Open** | Failures > threshold; requests rejected with `ErrCircuitOpen`; after `ResetTimeout`, transitions to Half-Open |
| **Half-Open** | Limited requests allowed; if `MinSuccesses` succeed, transitions to Closed; if any fails, back to Open |

### 8.3 Retry Logic

**Package:** `pkg/engine` — `retry.go`

```go
type RetryConfig struct {
    MaxAttempts  int           // Default varies by policy
    InitialDelay time.Duration // Default: 100ms
    MaxDelay     time.Duration // Default: 30s
    Multiplier   float64       // Default: 2.0
    Jitter       float64       // Default: 0.1 (10% randomization)
}
```

**Predefined policies:**
| Policy | MaxAttempts | InitialDelay | Use Case |
|--------|-------------|--------------|----------|
| **Fast** | 3 | 50ms | Quick retries |
| **Standard** | 3 | 100ms | General purpose |
| **Aggressive** | 5 | 500ms | Transient failures |
| **Background** | 10 | 1s | Background jobs |

**Functions:**
- `Retry(ctx, config, func) error` — Retry with no result
- `RetryWithResult(ctx, config, func) (T, error)` — Retry with typed result

### 8.4 Health Checks & Monitoring

**Package:** `pkg/server` — `lifecycle.go`

**Endpoints (HTTP on port 8420 by default):**

| Endpoint | Purpose | Returns |
|----------|---------|---------|
| `GET /health` | Kubernetes liveness probe | `{"status": "ok"}` |
| `GET /ready` | Kubernetes readiness probe | `{"status": "ready"}` |
| `GET /healthz` | Detailed health status | Full health report |
| `GET /stats` | System statistics | DB stats, connection count, etc. |
| `GET /circuit-breakers` | CB status | Circuit breaker states |
| `GET /rate-limits` | Rate limiter status | Current rate limit info |

### 8.5 Metrics Collection

**Package:** `pkg/metrics` — `metrics.go`, `prometheus.go`

```go
type Collector struct {
    // Query metrics
    queriesTotal         *prometheus.CounterVec
    queryDuration        *prometheus.HistogramVec
    // Connection metrics
    connectionsTotal     prometheus.Counter
    activeConnections    prometheus.Gauge
    // Transaction metrics
    transactionsTotal    prometheus.Counter
    activeTransactions   prometheus.Gauge
    // Storage metrics
    bufferPoolHits       prometheus.Counter
    bufferPoolMisses     prometheus.Counter
    walWritesTotal       prometheus.Counter
    // Error metrics
    errorsTotal          *prometheus.CounterVec
    // ...
}
```

**Recorded metrics:**
- **Queries:** Total executed, duration histogram, by type (SELECT, INSERT, UPDATE, DELETE)
- **Connections:** Total accepted, active count, rejected
- **Transactions:** Total begun, committed, aborted, active
- **Storage:** Buffer pool hits/misses, WAL writes, page reads/writes
- **Errors:** By error type and severity
- **Writes:** RecordWrite
- **Lock waits:** RecordLockWaitTime

**Prometheus endpoint:** Exposed at `/metrics` for Prometheus scraping.

### 8.6 Slow Query Logging

**Package:** `pkg/metrics` — `slow_query.go`

- Configurable slow query threshold (default: 100ms)
- Logs query text, duration, and connection info
- File I/O based logging (`slow_query_file_io_test.go`)
- Threshold configuration

### 8.7 Alerting

**Package:** `pkg/metrics` — `alerting.go`

- Configurable alert thresholds
- Alert types: query rate, error rate, connection saturation, latency
- Alerting counter management with delta tracking

---

## 9. Replication & High Availability

### 9.1 Statement-Based Replication

**Package:** `pkg/replication` — `replication.go` (~2,406 lines)

Master-slave replication using statement-based logical replication:

**Replication modes:**
| Mode | Description |
|------|-------------|
| **Async** | Best performance, some replication lag |
| **Sync** | Wait for at least one slave acknowledgment |
| **FullSync** | Wait for all slaves (slowest, safest) |

**Node roles:**
| Role | Description |
|------|-------------|
| **Standalone** | No replication (default) |
| **Master** | Accepts writes and ships SQL to slaves |
| **Slave** | Receives and replays changes from master |

**Payload** (`replication/payload.go`): StatementPayload with versioned SQL, args, timestamps.

### 9.2 Replication Manager

**Package:** `pkg/replication`

- Manages slave connections
- LSN (Log Sequence Number) tracking for ordering
- In-memory replication log buffer
- Automatic reconnection on failure
- Auth negotiation between master and slave

### 9.3 Snapshot Transfer

**Package:** `pkg/engine` — `replication_snapshot.go`

- Full database snapshot transfer for new slaves
- Atomic point-in-time snapshot via `BeginHotBackup` / `EndHotBackup`
- Used when slave falls behind the master's buffer

---

## 10. Advanced Features

### 10.1 Vector Search (HNSW)

**Packages:** `pkg/catalog` — `vector.go`, `catalog_vector.go` | `pkg/catalog/vector.go`

CobaltDB supports **vector similarity search** using HNSW (Hierarchical Navigable Small World) indexes.

```sql
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    vector VECTOR(128)
);
CREATE VECTOR INDEX idx_vec ON embeddings(vector);

-- Similarity search
SELECT * FROM embeddings ORDER BY vector <-> query_vector LIMIT 10;
```

**Features:**
- VECTOR(n) data type with n-dimensional float arrays
- HNSW index for approximate nearest neighbor search
- Cosine and Euclidean distance metrics
- Configurable index parameters (M, efConstruction, efSearch)

**Key files:**
- `catalog/vector.go` — VECTOR type definition, vector operations
- `catalog/catalog_vector.go` — Vector index management

### 10.2 Full-Text Search

**Package:** `pkg/catalog` — `catalog_fts.go`

```sql
CREATE FULLTEXT INDEX idx_fts ON articles(title, body);
SELECT * FROM articles WHERE MATCH(title, body) AGAINST('search query');
```

**Features:**
- Inverted index for text search
- MATCH ... AGAINST syntax
- Multiple columns per index
- Tokenization with configurable delimiters
- Ranked search results

### 10.3 Temporal Queries

**Package:** `pkg/catalog` — `temporal.go`

```sql
SELECT * FROM users AS OF SYSTEM TIME '2026-01-01 00:00:00';
SELECT * FROM users FOR SYSTEM_TIME AS OF '2026-01-01';
```

**Features:**
- Time-travel queries using MVCC version history
- `AS OF SYSTEM TIME` clause
- `FOR SYSTEM_TIME AS OF` syntax
- Configurable retention of historical versions

### 10.4 Window Functions

**Package:** `pkg/catalog` — `catalog_window.go`

```sql
SELECT 
    name, salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as rank,
    AVG(salary) OVER (PARTITION BY dept) as dept_avg,
    LAG(salary) OVER (ORDER BY salary) as prev_salary,
    FIRST_VALUE(name) OVER (ORDER BY salary DESC) as top_earner,
    NTILE(4) OVER (ORDER BY salary DESC) as quartile
FROM employees;
```

**Supported functions:**
| Function | Description |
|----------|-------------|
| `ROW_NUMBER()` | Sequential row number within partition |
| `RANK()` | Rank with gaps |
| `DENSE_RANK()` | Rank without gaps |
| `LAG(expr, offset, default)` | Access previous row value |
| `LEAD(expr, offset, default)` | Access next row value |
| `FIRST_VALUE(expr)` | First value in window frame |
| `LAST_VALUE(expr)` | Last value in window frame |
| `NTILE(n)` | Bucket rows into n groups |
| `PERCENT_RANK()` | Relative rank (0 to 1) |
| `CUME_DIST()` | Cumulative distribution |
| `STDDEV(expr)` | Standard deviation |
| `VARIANCE(expr)` | Statistical variance |

**Window frames:** ROWS, RANGE, UNBOUNDED PRECEDING, n PRECEDING, CURRENT ROW, n FOLLOWING, UNBOUNDED FOLLOWING.

### 10.5 CTE (Common Table Expressions)

**Package:** `pkg/catalog` — `catalog_cte.go`

```sql
-- Simple CTE
WITH regional_sales AS (
    SELECT region, SUM(amount) AS total_sales
    FROM orders GROUP BY region
)
SELECT * FROM regional_sales WHERE total_sales > 1000;

-- Recursive CTE
WITH RECURSIVE org_tree AS (
    SELECT id, name, manager_id, 1 AS depth
    FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, ot.depth + 1
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.id
)
SELECT * FROM org_tree;
```

### 10.6 Views & Materialized Views

**Package:** `pkg/catalog` — `catalog_view.go`

```sql
CREATE VIEW active_users AS 
    SELECT * FROM users WHERE status = 'active';

CREATE MATERIALIZED VIEW user_stats AS
    SELECT department, COUNT(*) as count, AVG(salary) as avg_sal
    FROM employees GROUP BY department;

REFRESH MATERIALIZED VIEW user_stats;
```

### 10.7 Triggers & Stored Procedures

```sql
CREATE TRIGGER audit_insert 
AFTER INSERT ON users
BEGIN
    INSERT INTO audit_log (table_name, action, record_id)
    VALUES ('users', 'INSERT', NEW.id);
END;

CREATE PROCEDURE transfer(sender INT, receiver INT, amount REAL)
BEGIN
    UPDATE accounts SET balance = balance - amount WHERE id = sender;
    UPDATE accounts SET balance = balance + amount WHERE id = receiver;
END;

CALL transfer(1, 2, 100.00);
```

**Trigger events:** BEFORE/AFTER, INSERT/UPDATE/DELETE
**Trigger types:** Row-level, INSTEAD OF (for views)
**Procedure support:** IN/OUT/INOUT parameters, result sets, variable assignment

### 10.8 Foreign Data Wrappers (FDW)

**Package:** `pkg/fdw` — `fdw.go`, `csv.go`

```sql
CREATE FOREIGN TABLE csv_data (
    id INTEGER,
    name TEXT
) SERVER csv_server OPTIONS (filename '/path/to/data.csv');
```

**Built-in FDWs:**
- **CSV FDW** — Query CSV files as virtual tables with predicate pushdown

**Key features:**
- Foreign table creation via SQL
- Query pushdown for WHERE filters
- Integration with catalog query execution

### 10.9 JSON Operations

**Package:** `pkg/catalog` — `catalog_eval_json.go`, `catalog_json.go`, `json_utils.go`

```sql
-- JSON column type
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    data JSON
);

-- Query JSON paths
SELECT JSON_EXTRACT(data, '$.price') FROM products;

-- Modify JSON
UPDATE products SET data = JSON_SET(data, '$.stock', 42);

-- JSON array operations
SELECT * FROM products WHERE JSON_ARRAY_LENGTH(data->'$.tags') > 2;

-- JSON path index
CREATE INDEX idx_json ON products(JSON_EXTRACT(data, '$.category'));
```

### 10.10 WASM Compilation

**Package:** `pkg/wasm` (build tag: `wasm_experimental`)

Experimental feature to compile SQL queries to WebAssembly bytecode:

| File | Purpose |
|------|---------|
| `compiler.go` | SQL → WASM bytecode compiler |
| `runtime.go` | WASM execution runtime |
| `host_functions.go` | Host function interface for WASM modules |

**Build requirement:** Requires `wasm_experimental` build tag.

### 10.11 Partitioning

**Package:** `pkg/catalog` — `catalog_core.go`

Table partitioning support via `PartitionInfo`:

```go
type PartitionInfo struct {
    Type       query.PartitionType
    Column     string
    NumParts   int
    Partitions []PartitionDef
}
```

- Range-based partitioning
- Partition pruning in query planning
- Per-partition storage management

---

## 11. Scheduler & Maintenance

### 11.1 Background Jobs

**Package:** `pkg/scheduler` — `scheduler.go`, `job.go`

The scheduler manages periodic background tasks:

```go
type Scheduler struct {
    jobs        []*Job
    workers     int
    jobQueue    chan func()
    // ...
}
```

**Configuration:**
- Enable/disable scheduler
- Worker count (default: 2)
- Analyze interval (default: 1 hour)

### 11.2 Auto-Vacuum

Automatic storage compaction that runs at configurable intervals:
- Reclaims space from deleted rows
- Configurable threshold (fraction of dead tuples)
- Configurable interval (default: 1 minute)

### 11.3 Auto-Checkpoint

Periodic WAL checkpoint to flush dirty pages and truncate the WAL:
- Configurable interval (default: 5 minutes)
- Serialized against concurrent commits for consistency
- Skipped during hot backup

### 11.4 Index Advisor

**Package:** `pkg/advisor` — `advisor.go`

The index advisor analyzes query patterns and suggests useful indexes:
- Analyzes WHERE conditions, JOIN predicates, and ORDER BY clauses
- Reports missing indexes with estimated improvement
- Available as SQL command or library call

### 11.5 Hot Backup

**Package:** `pkg/backup` — `backup.go`

```go
// Atomic point-in-time backup
err := db.BeginHotBackup()
// Copy database files...
err = db.EndHotBackup()
```

- Atomic point-in-time snapshot
- Blocks checkpoints and WAL truncation during backup
- Consistent file copy of data + WAL

---

## 12. SDKs & Client Libraries

### 12.1 Go SDK (Embedded)

**Import:** `github.com/cobaltdb/cobaltdb/pkg/engine`

The primary way to use CobaltDB as an embedded database in Go applications:

```go
import "github.com/cobaltdb/cobaltdb/pkg/engine"

db, err := engine.Open("mydb.cb", &engine.Options{...})
defer db.Close()

// Execute SQL
result, err := db.Exec(ctx, "INSERT INTO users VALUES (?, ?)", 1, "Alice")

// Query
rows, err := db.Query(ctx, "SELECT * FROM users WHERE id = ?", 1)
```

### 12.2 Go SDK (database/sql Driver)

**Package:** `sdk/go` — `cobaltdb.go`

Implements `database/sql/driver` interface for use with Go's standard `database/sql` package:

```go
import (
    "database/sql"
    _ "github.com/cobaltdb/cobaltdb/sdk/go"
)

db, _ := sql.Open("cobaltdb", "file://./data/mydb.cb?cache=1024")
```

### 12.3 Python SDK

**File:** `sdk/python/cobaltdb.py`

MySQL-connector wrapper for CobaltDB:

```python
import cobaltdb

conn = cobaltdb.connect(host='127.0.0.1', port=3307, user='admin')
cursor = conn.execute("SELECT * FROM users")
for row in cursor.fetchall():
    print(row)
```

### 12.4 Node.js SDK

**File:** `sdk/js/index.js`, `sdk/js/package.json`

MySQL2-based wrapper for CobaltDB:

```javascript
const cobaltdb = require('./sdk/js');

const conn = await cobaltdb.connect({ host: '127.0.0.1', port: 3307 });
const [rows] = await conn.execute('SELECT * FROM users');
```

### 12.5 Java SDK

**File:** `sdk/java/CobaltDB.java`

JDBC-based wrapper for CobaltDB:

```java
import com.cobaltdb.sdk.CobaltDB;

Connection conn = CobaltDB.connect("127.0.0.1", 3307, "admin", "");
ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM users");
```

---

## 13. CLI & Tools

### 13.1 cobaltdb-server

**Package:** `cmd/cobaltdb-server` — `main.go`

```bash
./cobaltdb-server -mysql-addr 127.0.0.1:3307 -admin-pass "StrongPass123!" -data ./data
```

**Flags:**
| Flag | Default | Description |
|------|---------|-------------|
| `-mysql-addr` | `:3307` | MySQL protocol listen address |
| `-http-addr` | `:4200` | HTTP admin/health listen address |
| `-data` | `./data` | Data directory |
| `-admin-pass` | (random) | Admin password (random if not set) |
| `-tls-cert` | "" | TLS certificate file |
| `-tls-key` | "" | TLS key file |
| `-allow-cleartext` | false | Allow non-TLS cleartext auth |

### 13.2 cobaltdb-cli

**Package:** `cmd/cobaltdb-cli` — `main.go`, `commands.go`

```bash
# Interactive shell
./cobaltdb-cli -path ./mydb.db

# Execute SQL directly
./cobaltdb-cli -memory "SELECT * FROM users"
```

**Commands:**
- `.tables` — List tables
- `.schema [table]` — Show table schema
- `.indexes [table]` — Show indexes
- `.quit` / `.exit` — Exit CLI

### 13.3 cobaltdb-bench

**Package:** `cmd/cobaltdb-bench` — `main.go`

Benchmarking tool for performance testing:
- Insert, select, update, delete benchmarks
- Configurable row counts and concurrency
- Results in ops/sec and latency

### 13.4 cobaltdb-migrate

**Package:** `cmd/cobaltdb-migrate` — `main.go`

Database migration tool:
- Schema migration between versions
- Data migration and transformation

---

## 14. Deployment

### 14.1 Standalone Binary

```bash
# Build
go build -o cobaltdb-server ./cmd/cobaltdb-server

# Run
./cobaltdb-server -mysql-addr :3307 -admin-pass "SecurePass123!"
```

### 14.2 Docker

**Dockerfile:** Multi-stage build producing a minimal image (scratch-based):

```bash
docker build -t cobaltdb .
docker run -d -p 3307:3307 -p 8420:8420 \
  -e COBALTDB_ADMIN_PASSWORD="SecurePass123!" \
  -v cobaltdb_data:/data/cobaltdb \
  cobaltdb
```

### 14.3 Docker Compose (with Monitoring)

**Files:** `docker-compose.yml`, `docker-compose.override.yml`, `docker-compose.prod.yml`

Full stack with:
- CobaltDB server
- Prometheus for metrics collection
- Grafana for dashboard visualization

```bash
export COBALTDB_ADMIN_PASSWORD="SecurePass123!"
docker compose up -d
```

### 14.4 Kubernetes

- Liveness and readiness probes at `/health` and `/ready`
- Configurable via environment variables
- Persistent volume for data directory
- Prometheus annotation for metrics scraping

### 14.5 Configuration

**File:** `config/cobaltdb.conf`

Configuration via TOML config file or environment variables.

**Key settings:**
- Data directory, page size, cache size
- WAL mode (enabled/disabled, sync mode)
- MySQL protocol address, TLS settings
- Authentication settings
- Logging configuration
- Metrics and monitoring

---

## 15. Monitoring & Observability

### 15.1 Prometheus

**File:** `monitoring/prometheus.yml`

Configuration for Prometheus to scrape CobaltDB metrics:
- Scrape target: CobaltDB server at `/metrics` endpoint
- Configurable scrape interval
- Job name: `cobaltdb`

### 15.2 Grafana

**Directory:** `monitoring/grafana/`

Ready-to-use dashboards:
- **`cobaltdb-dashboard.json`** — Full database dashboard with:
  - Query throughput (ops/sec)
  - Query latency (p50, p95, p99)
  - Connection count
  - Buffer pool hit rate
  - Transaction throughput
  - Error rate
  - WAL write throughput
- Pre-configured Prometheus data source

### 15.3 Health Endpoints

| Port | Endpoint | Purpose |
|------|----------|---------|
| 8420 | `/health` | Liveness probe |
| 8420 | `/ready` | Readiness probe |
| 8420 | `/healthz` | Detailed health |
| 8420 | `/stats` | DB statistics |
| 8420 | `/metrics` | Prometheus metrics |
| 8420 | `/circuit-breakers` | Circuit breaker status |
| 8420 | `/rate-limits` | Rate limiter stats |

### 15.4 Structured Logging

**Package:** `pkg/logger` — `logger.go`

Structured, leveled logging:
- **Levels:** Debug, Info, Warn, Error, Fatal
- **Format:** Text (default) or JSON
- **Output:** Configurable writer, file rotation with size/age limits
- **Prefix:** Optional component prefix for source identification

---

## 16. Testing & Quality

### 16.1 Test Coverage

CobaltDB has **7,100+ test functions** across 800+ test files covering the entire codebase.

**Current package coverage:**
| Package | Coverage | Package | Coverage |
|---------|----------|---------|----------|
| `pkg/logger` | 100.0% | `pkg/pool` | 97.5% |
| `pkg/advisor` | 96.8% | `pkg/auth` | 96.8% |
| `pkg/wire` | 94.7% | `pkg/metrics` | 94.3% |
| `pkg/fdw` | 93.9% | `pkg/optimizer` | 93.8% |
| `pkg/security` | 93.2% | `pkg/query` | 91.7% |
| `pkg/parallel` | 91.2% | `pkg/cache` | 90.9% |
| `pkg/audit` | 90.7% | `pkg/server` | 90.6% |
| `pkg/storage` | 88.4% | `pkg/replication` | 88.3% |
| `pkg/backup` | 88.2% | `pkg/txn` | 87.8% |
| `pkg/catalog` | 84.8% | `pkg/wasm` | 84.4% |
| `pkg/protocol` | 82.7% | `pkg/engine` | 80.8% |
| `pkg/btree` | 78.2% | | |

**Overall `pkg/...` coverage:** ~86.3%+

**Quality gates:**
- `make verify` — fmt-check, build, vet, test
- `make verify-security` — Full security gate with race detector, gosec, govulncheck
- Race detection is part of continuous testing
- `gofmt` enforced via `make fmt-check`

### 16.2 Test Organization

| Test Location | Type | Count |
|---------------|------|-------|
| `pkg/*/*_test.go` | Unit tests (per package) | 800+ files |
| `test/*_test.go` | Integration & system tests | 150+ files |
| `integration/*_test.go` | Cross-package integration tests | 50+ files |
| `cmd/*/*_test.go` | CLI tool tests | 10+ files |
| `sdk/*/*_test.go` | SDK tests | 10+ files |
| `webui/*_test.go` | WebUI tests | 10+ files |
| `examples/*/*_test.go` | Example tests | 5+ files |

### 16.3 Chaos Engineering

**File:** `integration/chaos_test.go`

Chaos engineering stress tests that verify:
- Concurrent writer stress (multiple goroutines)
- Connection storms
- Query injection fuzzing
- Restart recovery
- Transaction isolation under load

---

## 17. Performance

**Test Environment:** AMD Ryzen 9 9950X3D · Go 1.26 · Windows 11

### 17.1 B-Tree Benchmarks

| Operation | Latency | Throughput |
|-----------|---------|------------|
| PUT | ~641 ns | 1.56M ops/sec |
| PUT (Sequential) | ~694 ns | 1.44M ops/sec |
| GET (Point Lookup) | ~64 ns | 15.7M ops/sec |
| UPDATE | ~153 ns | 6.5M ops/sec |
| DELETE | ~197 ns | 5.1M ops/sec |
| SCAN (1K range) | ~270 µs | 3.7K ops/sec |

### 17.2 SQL Engine Benchmarks (10K rows)

| Operation | Latency | Notes |
|-----------|---------|-------|
| INSERT | ~2.0 µs | Single row with SQL parsing |
| Point Lookup (indexed) | ~2.1 µs | WHERE id = ? |
| Full Scan (1K) | ~598 µs | Custom fast decoder, no reflection |
| Full Scan (10K) | ~8.8 ms | 130K allocs (42% less than json.Unmarshal) |
| SUM/AVG | ~5.0 ms | Byte-level fast path, no JSON decode |
| COUNT(*) | ~4.4 ms | Fast path, skip row decode |
| WHERE (10K) | ~10.3 ms | Custom decoder + expression eval |
| Index vs No Index | 458 µs vs 8.7 ms | **19x faster with index** |
| Inner JOIN (1K) | ~724 µs | Hash join |
| 3-Way JOIN (1K×3) | ~2.0 ms | Hash join |
| Recursive CTE | ~3.6 µs | 1000 nodes |
| Concurrent Read (×20) | ~669 ns | Parallel goroutines |
| Transaction | ~347 µs | Single statement |
| Rollback | ~167 µs | 100 statements |
| DELETE (bulk 1K) | ~998 µs | WHERE age < 50 |
| UPDATE (bulk 10K) | ~9.2 ms | WHERE age < 50 |

### 17.3 Parser & Storage Benchmarks

| Component | Operation | Latency | Throughput |
|-----------|-----------|---------|------------|
| SQL Parser | Parse SELECT | ~826 ns | 1.2M ops/sec |
| SQL Parser | Parse INSERT | ~1.0 µs | 960K ops/sec |
| SQL Parser | Parse Complex Query | ~4.7 µs | 214K ops/sec |
| Lexer | Tokenize | ~499 ns | 2.0M ops/sec |
| Buffer Pool | Get Page | ~27 ns | 36.5M ops/sec |
| WAL | Append | ~192 µs | 5.2K ops/sec |

---

## 18. Data Types & SQL Reference

### 18.1 Data Types

| Type | Description | Range/Precision |
|------|-------------|-----------------|
| `INTEGER` | 64-bit signed integer | -2^63 to 2^63-1 |
| `REAL` | 64-bit floating point (IEEE 754) | ±5.0×10^-324 to ±1.7×10^308 |
| `TEXT` | Variable-length UTF-8 string | Up to 2^31-1 bytes |
| `BOOLEAN` | Boolean | TRUE / FALSE / NULL |
| `JSON` | Native JSON document | Flexible schema |
| `VECTOR(n)` | n-dimensional float vector | n dimensions (for embeddings) |
| `DATE` | Calendar date | ISO 8601 date |
| `TIMESTAMP` | Date with time | ISO 8601 datetime |

### 18.2 SQL Functions

**String Functions:** `LENGTH`, `UPPER`, `LOWER`, `TRIM`, `SUBSTR`, `CONCAT`, `REPLACE`, `INSTR`, `LIKE`, `GLOB`, `ASCII`, `SUBSTRING_INDEX`

**Numeric Functions:** `ABS`, `ROUND`, `FLOOR`, `CEIL`, `MOD`, `POWER`, `SQRT`, `EXP`, `LN`, `LOG`, `LOG2`, `LOG10`, `SIGN`, `TRUNC`, `CBRT`

**Trigonometric Functions:** `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `DEGREES`, `RADIANS`

**Aggregate Functions:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT`, `STDDEV`, `VARIANCE`

**JSON Functions:** `JSON_EXTRACT`, `JSON_SET`, `JSON_REMOVE`, `JSON_VALID`, `JSON_ARRAY_LENGTH`, `JSON_MERGE`, `JSON_KEYS`, `JSON_CONTAINS`, `JSON_TYPE`, `->` (JSON path operator)

**Window Functions:** `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTILE`, `PERCENT_RANK`, `CUME_DIST`, `STDDEV`, `VARIANCE`

**Date/Time Functions:** `DATE`, `TIME`, `DATETIME`, `STRFTIME`, `JULIANDAY`, `EXTRACT`, `DATE_ADD`, `DATE_SUB`, `DATEDIFF`, `CURRENT_TIMESTAMP`

**Conditional Functions:** `COALESCE`, `IFNULL`, `NULLIF`, `CASE` (expression)

**Type Conversion:** `CAST(expr AS type)`

**Miscellaneous:** `TYPEOF`, `TOTAL`, `RANDOM`, `ZEROBLOB`, `HEX`, `QUOTE`, `UNICODE`, `CHAR`, `PRINTF`

### 18.3 Supported Statements

**DDL:**
```sql
CREATE TABLE [IF NOT EXISTS] ... (column_def [, constraint] ...)
CREATE INDEX [IF NOT EXISTS] ... ON table (column [, ...])
CREATE UNIQUE INDEX ... ON table (column [, ...])
CREATE VIEW [IF NOT EXISTS] ... AS SELECT ...
CREATE MATERIALIZED VIEW ... AS SELECT ...
CREATE TRIGGER [IF NOT EXISTS] ... BEFORE|AFTER INSERT|UPDATE|DELETE ON table
CREATE PROCEDURE [IF NOT EXISTS] ... (params) BEGIN ... END
ALTER TABLE ... ADD|DROP|RENAME COLUMN ...
ALTER TABLE ... ADD|DROP CONSTRAINT ...
DROP TABLE|INDEX|VIEW|TRIGGER|PROCEDURE [IF EXISTS]
TRUNCATE TABLE ...
CREATE TABLE ... AS SELECT ...
```

**DML:**
```sql
INSERT INTO table [(columns)] VALUES (expr, ...) [ON CONFLICT ...]
INSERT INTO table SELECT ... [ON CONFLICT ...]
INSERT OR REPLACE INTO table ...
UPDATE table SET column = expr [, ...] [FROM ...] [WHERE ...] [RETURNING ...]
DELETE FROM table [USING ...] [WHERE ...] [RETURNING ...]
SELECT [DISTINCT] columns FROM table [JOIN ...] [WHERE ...]
  [GROUP BY ...] [HAVING ...] [WINDOW ...] [ORDER BY ...]
  [LIMIT n] [OFFSET n]
```

**DCL / TCL:**
```sql
BEGIN [TRANSACTION] | COMMIT | ROLLBACK [TO SAVEPOINT]
SAVEPOINT name
RELEASE SAVEPOINT name
SET [SESSION] variable = value
SHOW variables [LIKE pattern]
CALL procedure(args)
EXPLAIN [QUERY PLAN] SELECT ...
ANALYZE table
VACUUM
```

**Full-Text Search:**
```sql
CREATE FULLTEXT INDEX ... ON table(columns)
SELECT ... WHERE MATCH(columns) AGAINST('query')
```

**Vector Search:**
```sql
CREATE VECTOR INDEX ... ON table(column)
SELECT ... ORDER BY column <-> query_vector LIMIT n
SELECT ... ORDER BY column <=> query_vector LIMIT n
```

**Temporal:**
```sql
SELECT ... FROM table AS OF SYSTEM TIME 'timestamp'
SELECT ... FROM table FOR SYSTEM_TIME AS OF 'timestamp'
```

---

## 19. Limitations & Known Issues

### Replication
- Statement-based replication may diverge on non-deterministic SQL (e.g., `RANDOM()`)
- Replication log is in-memory on master; a full snapshot is needed after master restart
- Not suitable for automatic HA/failover without external orchestration

### Single-Node Focus
- No built-in distributed query execution or sharding
- No consensus protocol (Raft/Paxos) for multi-node coordination

### MySQL Compatibility
- Subset of MySQL wire protocol; not all features are implemented
- Validate specific ORMs/drivers against `docs/MYSQL_COMPATIBILITY.md`
- Stored procedure language is CobaltDB-specific, not MySQL PL/SQL

### Production Readiness
- CobaltDB is a **production-oriented single-node database candidate**
- Use TLS for non-loopback wire access
- Keep MySQL listener private or disabled on untrusted networks
- Circuit breaker, retry, and rate limiter are optional components

### Performance
- In-memory benchmarks; disk persistence adds ~20-40% overhead
- WAL append throughput (~5.2K ops/sec) is the primary write bottleneck
- Full table scans are slower than indexed lookups (expected)

---

## 20. Appendix: Package Reference

### `pkg/engine` — Database Engine
**Main file:** `database.go` (~4,671 lines)
**Key types:** `DB`, `Options`, `PanicRecovery`, `DBStats`
**Files:**
| File | Purpose |
|------|---------|
| `database.go` | Core DB struct, Open/Close, Exec/Query, transactions, connection management |
| `database_api.go` | Stats, HealthCheck, Checkpoint, public API wrappers |
| `database_lifecycle.go` | Open/load/create lifecycle, DefaultOptions, Options struct |
| `circuit_breaker.go` | Three-state circuit breaker |
| `retry.go` | Retry logic with exponential backoff |
| `explain.go` | Query plan builder for EXPLAIN |
| `query_plan_cache.go` | LRU query plan cache |
| `replication_master.go` | Master-side replication integration |
| `replication_snapshot.go` | Snapshot transfer for replication |

### `pkg/btree` — B+Tree Storage
**Main file:** `btree.go` (~1,493 lines)
**Key types:** `BTree`, `lruEntry`, `lruList`
**Files:**
| File | Purpose |
|------|---------|
| `btree.go` | Core B+Tree implementation |
| `interfaces.go` | Tree interface definitions |

### `pkg/storage` — Storage Layer
**Main file:** `storage.go` (multiple files)
**Key types:** `Backend` (interface), `DiskBackend`, `MemoryBackend`, `EncryptedBackend`, `BufferPool`, `CachedPage`, `WAL`, `WALRecord`
**Files:**
| File | Purpose |
|------|---------|
| `backend.go` | Backend interface |
| `disk.go` | File-based backend |
| `memory.go` | In-memory backend |
| `encryption.go` | AES-256-GCM encrypted backend |
| `buffer_pool.go` | LRU page cache |
| `buffer_pool_stats.go` | Buffer pool statistics |
| `wal.go` | Write-ahead log |
| `page.go` | Page size constants and pool |
| `compression.go` | Page-level compression |
| `conversions.go` | Byte conversion utilities |
| `interfaces.go` | Additional interfaces |

### `pkg/query` — SQL Parser & AST
**Main file:** `parser.go`, `ast.go`
**Key types:** `Statement`, `Expression`, `Node`, AST node types, `TokenType`, `Lexer`
**Files:**
| File | Purpose |
|------|---------|
| `lexer.go` | SQL tokenizer |
| `token.go` | Token type definitions (~140+ tokens) |
| `ast.go` | AST node interfaces and types |
| `parser.go` | Main SQL parser |
| `parser_ddl.go` | DDL statement parsing |
| `parser_dml_select.go` | SELECT/INSERT/UPDATE/DELETE parsing |
| `parser_expression.go` | Expression parsing |
| `clone.go` | Deep clone for AST nodes |
| `optimizer.go` | Query optimization utilities |
| `query_utils.go` | Statement analysis utilities |

### `pkg/catalog` — SQL Execution Engine
**Files:** ~115 files (~8,500 lines of Go code, ~20,000+ lines of tests)
**Key types:** `Catalog`, `TableDef`, `ColumnDef`, `ForeignKeyDef`, `CheckDef`, `PartitionInfo`
**Core files:**
| File | Purpose |
|------|---------|
| `catalog_core.go` | Core catalog, TableDef, helpers |
| `catalog_ddl.go` | CREATE/ALTER/DROP table, index, view, trigger, procedure |
| `catalog_insert.go` | INSERT execution |
| `catalog_update.go` | UPDATE execution |
| `catalog_delete.go` | DELETE execution |
| `catalog_select.go` | SELECT execution with JOINs |
| `catalog_aggregate.go` | GROUP BY and aggregate functions |
| `catalog_window.go` | Window function execution |
| `catalog_cte.go` | CTE execution (recursive and non-recursive) |
| `catalog_txn.go` | Transaction integration |
| `catalog_eval.go` | Expression evaluation |
| `catalog_eval_string.go` | String function evaluation |
| `catalog_eval_json.go` | JSON function evaluation |
| `catalog_index.go` | Index management |
| `catalog_fts.go` | Full-text search |
| `catalog_vector.go` | Vector search |
| `catalog_rls.go` | Row-level security |
| `catalog_view.go` | View and materialized view support |
| `catalog_fdw.go` | Foreign data wrapper integration |
| `catalog_fastpath.go` | Optimized query fast paths |
| `catalog_row.go` | Row encoding/decoding |
| `catalog_json.go` | JSON type support |
| `catalog_paths.go` | Catalog path utilities |
| `catalog_select_helpers.go` | SELECT helper functions |
| `catalog_maintenance.go` | VACUUM, ANALYZE |
| `catalog_clone.go` | Catalog data deep cloning |
| `catalog_sql_persistence.go` | SQL definition persistence |
| `catalog_returning.go` | RETURNING clause |
| `catalog_helpers.go` | General helpers |
| `catalog_query_local.go` | Query cache integration |
| `catalog_select_limits.go` | LIMIT/OFFSET optimization |
| `foreign_key.go` | Foreign key constraint management |
| `conversions.go` | Type conversion utilities |
| `stats.go` | Table statistics |
| `temporal.go` | Temporal query support |
| `json_utils.go` | JSON path parsing utilities |
| `vector.go` | Vector type utilities |
| `goid.go` | Goroutine ID utilities |

### `pkg/txn` — Transaction Manager
**Files:** `manager.go`, `interfaces.go`, `version_store.go`
**Key types:** `Manager`, `Transaction`, `LockManager`, `VersionStore`

### `pkg/server` — Network Server
**Files:** `server.go`, `lifecycle.go`, `production.go`, `tls.go`, `sql_protection.go`, `rate_limiter.go`
**Key types:** `Server`, `Config`, `ProductionServer`, `ProductionConfig`, `TLSConfig`, `SQLProtector`, `RateLimiter`

### `pkg/protocol` — MySQL Wire Protocol
**Main file:** `mysql.go` (~2,634 lines)
**Key types:** `MySQLHandler`, protocol constants

### `pkg/wire` — Wire Format Helpers
**Main file:** `protocol.go`

### `pkg/auth` — Authentication
**Main file:** `auth.go` (~914 lines)
**Key types:** `Authenticator`, `User`, `Session`, `Permission`

### `pkg/audit` — Audit Logging
**Main file:** `logger.go` (~1,117 lines), `verifier.go`
**Key types:** `Logger`, `Config`, `EventType`

### `pkg/security` — Row-Level Security
**Main file:** `rls.go` (~1,397 lines)
**Key types:** `Manager`, `Policy`

### `pkg/replication` — Master-Slave Replication
**Main file:** `replication.go` (~2,406 lines), `payload.go`
**Key types:** `Manager`, `Config`

### `pkg/metrics` — Metrics & Monitoring
**Files:** `metrics.go`, `prometheus.go`, `slow_query.go`, `txn_metrics.go`, `alerting.go`
**Key types:** `Collector`, `PrometheusExporter`

### `pkg/optimizer` — Query Optimizer
**Main file:** `optimizer.go`

### `pkg/scheduler` — Background Jobs
**Files:** `scheduler.go`, `job.go`
**Key types:** `Scheduler`, `Job`

### `pkg/backup` — Hot Backup
**Main file:** `backup.go`

### `pkg/fdw` — Foreign Data Wrappers
**Files:** `fdw.go`, `csv.go`

### `pkg/logger` — Structured Logging
**Main file:** `logger.go`

### `pkg/cache` — Query Cache
**Main file:** `query_cache.go`

### `pkg/advisor` — Index Advisor
**Main file:** `advisor.go`

### `pkg/parallel` — Parallel Execution
**Main file:** `executor.go`

### `pkg/pool` — Connection Pool
**Main file:** `connection_pool.go`

### `pkg/wasm` — WASM Compilation (Experimental)
**Files:** `compiler.go`, `runtime.go`, `host_functions.go`
**Build tag:** `wasm_experimental`

### `webui` — Web Admin Interface
**Files:** `server.go`, `tables.go`, `auth.go`, `audit.go`, `ratelimit.go`
**Templates:** `templates/index.html`
**Statics:** `static/app.js`, `static/style.css`

### `sdk` — Multi-Language SDKs
**Go:** `sdk/go/cobaltdb.go` — database/sql driver
**Python:** `sdk/python/cobaltdb.py` — MySQL connector wrapper
**Node.js:** `sdk/js/index.js` — mysql2 wrapper
**Java:** `sdk/java/CobaltDB.java` — JDBC wrapper

### `cmd` — Command-Line Tools
**cobaltdb-server:** `cmd/cobaltdb-server/main.go`
**cobaltdb-cli:** `cmd/cobaltdb-cli/main.go`, `commands.go`
**cobaltdb-bench:** `cmd/cobaltdb-bench/main.go`
**cobaltdb-migrate:** `cmd/cobaltdb-migrate/main.go`
**demo:** `cmd/demo/main.go`

### `monitoring` — Grafana & Prometheus
**Prometheus:** `monitoring/prometheus.yml`
**Grafana:** `monitoring/grafana/dashboards/cobaltdb-dashboard.json`
**Grafana datasource:** `monitoring/grafana/datasources/datasource.yml`

---

> **© 2026 CobaltDB. MIT License.**
>
> This documentation covers CobaltDB v0.6.0. For the latest version, check [the repository](https://github.com/cobaltdb/cobaltdb).
>
> **Built with ❤️ by Ersin KOÇ**
