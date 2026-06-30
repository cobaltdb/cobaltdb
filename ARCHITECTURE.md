# CobaltDB Architecture

## Overview

CobaltDB: pure-Go SQL database. It runs in two modes: embedded library and standalone server. Clients can use CLI, Web UI, custom wire protocol, MySQL protocol, or direct Go API. All paths converge on pkg/engine.

Module: github.com/cobaltdb/cobaltdb. Go target: 1.25. Toolchain: go1.26.4. Runtime goal: CGO-free database core.

## Runtime Layers

```text
Clients and applications
  -> cmd/cobaltdb-cli
  -> cmd/cobaltdb-server
  -> webui
  -> pkg/protocol (MySQL)
  -> embedded Go API
        |
        v
pkg/engine
  lifecycle, query API, transactions, cache, optimizer, metrics
        |
        v
pkg/catalog
  schema, tables, indexes, constraints, SQL execution, RLS, FDW
        |
        v
pkg/query + pkg/optimizer
  SQL tokens, AST, expressions, plans, rewrites, index choice
        |
        v
pkg/txn + pkg/storage + pkg/btree
  MVCC, WAL, pages, disk/memory backend, buffer pool, indexes
```

Key boundary: transport packages translate requests. Engine, catalog, transaction, and storage packages own database semantics.

## Command Entrypoints

| Path | Role |
| --- | --- |
| cmd/cobaltdb-server | Standalone server. Opens database, parses flags, configures auth/TLS/production options, starts custom and MySQL listeners. |
| cmd/cobaltdb-cli | Interactive and one-shot CLI for SQL and management commands. |
| cmd/cobaltdb-bench | Benchmark command. |
| cmd/cobaltdb-migrate | Migration command. |
| cmd/demo | Demo and development examples. |

## Core Packages

| Package | Role |
| --- | --- |
| pkg/engine | Primary database API and orchestration layer. Owns backend, catalog, WAL, buffer pool, transaction manager, cache, optimizer, metrics, replication hooks, and lifecycle state. |
| pkg/catalog | Metadata and SQL execution. Handles table definitions, indexes, constraints, CRUD, schema persistence, RLS, JSON, full-text, vector, FDW, and parallel execution hooks. |
| pkg/query | SQL lexer, parser, AST, statements, expressions, joins, CTEs, windows, procedures, triggers, and related syntax. |
| pkg/optimizer | Cost-based optimization, index selection, join reordering, filter pushdown, projection pruning, and statistics. |
| pkg/storage | Storage abstraction, disk and memory backends, pages, WAL, buffer pool, compression, encryption, and binary conversions. |
| pkg/txn | Transaction manager, MVCC version store, savepoints, deadlock and timeout handling, and WAL-backed transaction records. |
| pkg/btree | B-tree index implementation. |
| pkg/server | Network server, client lifecycle, production hardening, health server, circuit breaker, rate limiter, retry integration, and SQL protection. |
| pkg/protocol | MySQL-compatible protocol: handshake, auth, commands, prepared statements, and result encoding. |
| pkg/wire | CobaltDB custom MessagePack protocol structures. |

## Feature and Operations Packages

| Package | Role |
| --- | --- |
| pkg/auth | Users, sessions, permissions, authentication, and access-control primitives. |
| pkg/security | Row-level security policies and policy expression evaluation. |
| pkg/audit | Audit event logging and verification helpers. |
| pkg/backup | Hot backup, delta payloads, checksums, and restore validation helpers. |
| pkg/replication | Master/slave replication, WAL streaming, snapshots, resume handshakes, checksums, and frame limits. |
| pkg/metrics | Runtime counters, alerts, health measurements, and visibility primitives. |
| pkg/logger | Logging abstraction used by server and production components. |
| pkg/cache | Query result cache. |
| pkg/parallel | Parallel query execution helpers. |
| pkg/fdw | Foreign Data Wrapper registry and CSV/external scan support. |
| pkg/wasm | WebAssembly execution support. |
| pkg/advisor | Index and query advisory helpers. |
| pkg/pool, pkg/scheduler | Concurrency and resource-management support. |
| webui | Browser HTTP interface backed by engine and admin/auth primitives. |
| sdk/* | SDK documentation and client integration material. |
| integration, test | Cross-package integration and behavior tests. |

## Query Flow

```text
SQL text
  -> pkg/query lexer/parser
  -> AST statement
  -> pkg/optimizer rewrite or plan choice
  -> pkg/catalog execution
  -> pkg/txn visibility and write coordination
  -> pkg/storage backend, WAL, pages, buffer pool
  -> result rows or mutation result
```

Embedded mode skips network layers. Server mode adds connection lifecycle, authentication, packet framing, protocol conversion, limits, and timeouts before calling the same engine path.

## Engine Layer

pkg/engine composes database subsystems. It opens disk or memory databases, exposes query and exec APIs, tracks lifecycle state, integrates transactions, cache, optimizer, metrics, replication hooks, retry/circuit-breaker behavior, and protects public query APIs with panic recovery metadata.

Engine code should stay transport-neutral. It should not depend on CLI, Web UI, or protocol-specific request details.

## Catalog Layer

pkg/catalog owns schema and SQL semantics. It creates and alters tables, executes DML and DDL, enforces constraints, maintains indexes through pkg/btree, applies row-level security through pkg/security, integrates FDWs, and persists or loads metadata and data through storage abstractions.

Catalog is above storage. It understands tables and rows. Storage understands pages and bytes.

## Storage Layer

pkg/storage owns durable byte and page management. It defines backend interfaces, disk backend, memory backend, buffer pool, WAL, page helpers, compression paths, encryption primitives, and binary conversion helpers.

Storage should stay SQL-agnostic. It must not contain parser, optimizer, or catalog rules.

## Transaction and MVCC Layer

pkg/txn owns transaction state, MVCC version storage, savepoints, deadlock detection, timeouts, conflict handling, read-only enforcement, and transaction WAL records.

Writes are tied to transaction state and commit timestamps. Versioned values preserve older states for snapshot visibility. Deletes are represented as tombstones in version history.

## Protocol and Server Runtime

CobaltDB exposes the same engine through several boundaries:

- Embedded Go API: direct calls into pkg/engine.
- CLI: cmd/cobaltdb-cli opens the database and runs SQL or management commands.
- Custom server: pkg/server plus pkg/wire.
- MySQL endpoint: pkg/protocol implements handshake, auth, commands, prepared statements, and result formatting.
- Web UI: webui exposes browser endpoints and admin tooling.

pkg/server also contains production support: lifecycle configuration, health endpoint, rate limiting, circuit breaker and retry integration, SQL protection, metrics exposure controls, and admin-token handling.

## Security Model

Security is split by boundary:

- pkg/auth handles users, sessions, permissions, and authentication primitives.
- pkg/server and pkg/protocol enforce authentication at connection or request boundaries.
- webui handles browser sessions, admin tokens, rate limiting, audit endpoints, and token administration.
- pkg/security enforces row-level security policies.
- pkg/audit records and verifies audit events.
- pkg/storage includes encryption-at-rest primitives.

Lower database layers should receive identity and policy context, not raw transport credentials.

## Replication and Backup

pkg/replication implements WAL streaming, snapshots, resume handshakes, checksums, frame limits, and replication authentication timeouts.

pkg/backup implements hot-backup-oriented payload handling, full and delta data, gzip/checksum framing, chunk metadata, and restore validation helpers.

Replication and backup integrate with engine and storage state. They should not bypass transaction or catalog consistency rules.

## Observability and Operations

Operational support includes pkg/metrics for measurements, pkg/logger for logging, pkg/audit for audit trails, pkg/server for health and production hardening, and documentation under docs/. Important documents include docs/OPERATIONS_RUNBOOK.md, docs/PRODUCTION.md, docs/HA_FAILOVER.md, docs/PERFORMANCE.md, and docs/BENCHMARKS.md.

## Testing Layout

Tests are spread across package-local *_test.go files, integration/, test/, cmd/, and webui/. Use native Go commands for verification:

```bash
go test ./...
```

For focused changes, run targeted packages:

```bash
go test ./pkg/storage ./pkg/txn ./pkg/engine
```

## Architectural Rules

1. Transport packages must not duplicate SQL execution logic.
2. Engine composes subsystems but stays transport-neutral.
3. Catalog owns schema and SQL semantics.
4. Storage stays SQL-agnostic.
5. Transaction visibility and conflict rules stay in pkg/txn.
6. Authentication is enforced at boundaries and converted into execution context for lower layers.
7. Health, metrics, audit, backup, and replication integrate with engine state without bypassing consistency rules.

## Related Documentation

- README.md: product overview, features, quick start, and examples.
- docs/ARCHITECTURE_FULL.md: older long-form architecture notes.
- docs/API.md: API and command usage documentation.
- docs/SQL.md: SQL support reference.
- docs/MYSQL_COMPATIBILITY.md: MySQL protocol compatibility details.
- docs/PRODUCTION.md: production deployment guidance.
- docs/OPERATIONS_RUNBOOK.md: operational runbook.
- docs/HA_FAILOVER.md: replication and failover behavior.
- docs/PERFORMANCE.md and docs/BENCHMARKS.md: performance material.
- docs/VECTOR_PERSISTENCE.md, docs/FDW_LIMITS.md, and docs/PROCEDURE_TRIGGER_SEMANTICS.md: focused subsystem documentation.
