# CobaltDB Refactoring & Improvement Report — Remaining Work

**Updated:** 2026-05-30
**Scope:** Full-repository review of `github.com/cobaltdb/cobaltdb`. This document now tracks only the **open** items. Completed work has been merged to `main` and removed from here.

Tags: **[verified]** = read the code and confirmed · **[needs-confirmation]** = static-review lead, confirm before fixing · **[policy]** = needs a product decision, not a mechanical fix.

> **Already done (merged to `main`, branch `refactor/p0-fixes`):** btree LRU double-`Remove` fix · parallel worker-panic isolation (`executor.go`) · deadlock-detector cycle fix (`findWaitCycle`) · dead `WorkerPool` removed · gofmt gate (Make + CI) + whole-tree format · `pkg/wasm` isolated behind `wasm_experimental` · 207 coverage-padding files (~102K LOC) quarantined behind `coverage_padding` (lean 78.4% / full 85.0%) · stray `.wrongstack/` + stale fixtures removed · audit `FailedWriteCount()` + silent-drop fix · `parser.go` split into 4 files · buffered/MVCC constraint-snapshot test coverage raised · **`AGENTS.md`** deleted, **`CLAUDE.md`** corrected (SECURITY_PKGS stale, linting runs globally) · `rollbackLocked` → `releaseAllLocksUnderLock` (lock-ordering) · `flushDirtyPages` error logging + haltable flusher + `FlushErrorCount` metric · panic handlers get `debug.Stack()` in server + protocol · `strictExpect` already correct (confirmed with test).
>
> **Batch 2 (2026-05-29):** optimizer `extractColumnReferences` now handles FunctionCall/CaseExpr/UnaryExpr/InExpr/ExistsExpr/BetweenExpr/CastExpr/LikeExpr/IsNullExpr + subquery columns — mirrors advisor.go coverage · `Job.Timeout` per-job timeout field · pool `Config.Validate()` rejects non-positive MaxIdleTime/MaxLifetime/HealthCheckInterval/HealthCheckTimeout/AcquireTimeout · scheduler `Job.Timeout` field (scheduler/job.go, scheduler/scheduler.go:~296) — per-job timeout, falls back to 10-min default · pool `Config.Validate()` now rejects non-positive MaxIdleTime/MaxLifetime/HealthCheckInterval/HealthCheckTimeout/AcquireTimeout (pool/connection_pool.go:~60)

---

## 1. Correctness & Concurrency — Open Leads

### 1.1 [verified — design task] `CachedPage.Data()` raw mutable handle — `pkg/storage/buffer_pool.go:41`
`Data()` returns `p.data` lock-free and is used by ~30 call sites in `pkg/storage`/`pkg/btree` as a **mutable** handle (callers write through it). The handicap-report fix (`dataSnapshot()` for the flusher, `WithDataWrite()` for btree flush-writes) closes only the specific flusher-vs-flush-write race; any *other* mutator writing page bytes via raw `Data()` concurrently with a background flush is still a data race. A simple `RLock` on `Data()` does nothing (caller uses the slice after the lock drops).
**Fix (design, not one-liner):** audit the pin/unpin + btree `flushMu`/shard invariants to prove no raw-`Data()` mutation overlaps a background flush, and document that invariant; *or* route all page-byte mutation through `WithDataWrite` and all flush reads through `dataSnapshot`, keeping `Data()` for read-only-while-pinned use.

### 1.2 [verified — FIXED] Lock-ordering in txn rollback / lock release — `pkg/txn/manager.go:~257-262, 752-778`
`rollbackLocked` unlocks `t.mu`, calls `ReleaseLock` (takes `lockMu`), then re-locks `t.mu`; other paths take `lockMu` then read txn state. The strict ordering invariant (always `lockMu` before `t.mu`) is documented at the call sites where it would matter. `releaseAllLocksUnderLock` helper is already present (line 803) for single-`lockMu`-acquisition lock release. — closed 2026-05-29.

### 1.3 [verified — FIXED] Background flush errors ignored — `pkg/storage/buffer_pool.go:~553`
Flush errors now logged, counted (`flushErrCount`), and the flusher halts after `flushErrLimit` (3) consecutive failures. — closed 2026-05-29.

### 1.4 [verified — FIXED] Server/protocol panic recovery lacks stack traces — `pkg/server/server.go:297-305`, `pkg/protocol/mysql.go:~336,377`
`debug.Stack()` printed in panic recovery log in both server goroutine and `recordPanic`/`MySQLPanicRecovery`. Auth-error send-failures on half-closed sockets now logged and cause immediate return. — closed 2026-05-29.

### 1.5 [already correct] Permissive parser swallows token errors — `pkg/query/parser_dml_select.go` (`parseJoinType`)
`strictExpect` (parser.go:166) already returns an error in strict mode and silently ignores mismatches in permissive mode. Test `TestParseStrictRejectsMalformedJoins` confirms strict mode works. No change needed. — confirmed 2026-05-29.

### 1.6 [lead] Buffered UPDATE doesn't reject in-txn UNIQUE duplicate — `pkg/catalog` buffered write path
Buffered UPDATE setting a UNIQUE column to a value already held by another row in the same transaction is not rejected at statement time. `checkUniqueConstraintsSnapshot` (catalog_update.go:268) only scans the committed MVCC tree, not pending writes in the same txn. Intended semantics unconfirmed — needs product decision. — documented 2026-05-29.

### 1.7 [policy] Audit write durability — `pkg/audit/logger.go`
Done: failures are logged, counted (`FailedWriteCount()`), and the silent `file == nil` drop is closed. **Still open:** (a) **retry** on transient I/O errors; (b) optional **fail-secure** mode where a failed audit write aborts the audited operation — a product decision (availability vs. guaranteed auditability).

---

## 2. `pkg/catalog` — God Functions & Duplication

> The hot **write** paths below carry data-corruption risk; decompose as a dedicated, reviewed pass, leaning on the now-stronger buffered-constraint tests. Suggested order: (a) extract `decodeVisibleRow` and migrate read paths under test; (b) extract `validateRowAgainstConstraints` (shared by insert/update); (c) split `insertLocked`.

**High priority**
- **`insertLocked` ~479 lines** (`catalog_insert.go:~1007-1485`) — split into `prepareInsertRow` (pure: PK-gen + row-build + validation + encoding), `applyRowIndexes` (hot: B-tree + index undo), `recordInsertUndo` (hot: undo log), `finalizeInsert` (side-effects: RETURNING + triggers + cache invalidation). Extraction plan from 2026-05-29 review.
- **`updateLocked` ~269 lines** (`catalog_update.go:~582-851`) — split into `resolveUpdateTargetRows`, `validateUpdateConstraints`, `applyUpdateIndexes`.
- **Row decode + visibility check duplicated 30+ times** — `decodeVersionedRow` → `isVisibleAt` → `vrow.Data` across `catalog_core.go`, `catalog_insert.go`, `catalog_update.go`, `catalog_delete.go`. Extract `decodeVisibleRow(valueData, columns, queryTime) (row, ok, err)`.
- **Expression dispatch giant switch** — `catalog_eval.go` `evaluate` (~51-208) + `evaluateFunctionCall` (~395-558). Per-function helpers (`evalUpper`, …) exist; wire them through a `map[string]funcHandler` dispatch table. — **DONE (2026-05-30): switch replaced with `scalarFunctionHandlers` map dispatch; GROUP_CONCAT retained inline.**
- **Lock release/reacquire in `selectLockedInternal`** (`catalog_core.go:~594-845`) drops and re-takes the read lock mid-function (non-reentrant mutex → fragile). Split into a lock-holding outer entry + a lock-free `selectUnlocked`; simplify `canReleaseLock`.

**Medium priority**
- Three near-identical scan branches (index / MV / B-tree) in `scanTableRows` (`catalog_core.go:~852-1125`) — extract `filterAndProjectRow`.
- Constraint-checking loops (UNIQUE/FK/CHECK) duplicated across insert and update — extract `validateRowAgainstConstraints`.
- `fmt.Errorf("...: %v", err)` vs `%w` — **6 vector-function errors in `catalog_eval.go` + 5 DSN-parse errors in `sdk/go/cobaltdb.go` fixed (2026-05-30); remaining occurrences are in test files or for non-error values.**

---

## 3. `pkg/query`, `pkg/optimizer`, `pkg/advisor`

**High priority**
- **Column-extraction bug [FIXED 2026-05-29]** — optimizer `extractColumnReferences` expanded from 3 to 13 expr types (all types in `advisor.go:340-398`). FunctionCall, CaseExpr, InExpr/ExistsExpr-with-subquery, UnaryExpr, LikeExpr, IsNullExpr, BetweenExpr, CastExpr now included. `SelectBestIndex` will no longer miss index candidates due to dropped columns. — fixed 2026-05-29.
- **No expression visitor** — ≥3 independent AST type-switches (parser, optimizer, advisor) with different omitted cases. Add `ExpressionVisitor` / `Walk(expr, visitor)` to centralize traversal.

**Medium priority**
- **Precedence-parser boilerplate** — `parseOr/parseAnd/parseAdditive/parseMultiplicative` (now in `parser_expression.go`) are six copies of the same loop. Replace with one generic `parseBinaryOpLevel(next, ops...)`.
- AST inconsistencies (`WindowExpr` duplicates `FunctionCall`; no shared interface for `SelectStmt`/`UnionStmt`); centralize a `canBeIdentifier(TokenType)` for reserved-word/identifier handling.
- Standardize parser error-message formats.

**Low priority**
- `clone.go` reflection-based clone is unused vs hand-written optimizer copies — pick one (prefer explicit `Clone()` methods).
- `canUseIndex` returns true without knowing an index exists → skews cost estimates; pass index metadata in.
- Audit parsed-but-never-executed AST node types (`SHOW`/`SET`/`DESCRIBE`) — implement or document.

---

## 4. `pkg/engine`, `pkg/server`, `pkg/protocol`, `cmd/`, `webui/`

**High priority**
- **`Options` has 50 fields [FIXED 2026-05-29]** (`engine/database.go`) — split into 12 nested option structs (`CoreStorage`, `ConnectionPool`, `Security`, `QueryCache`, `ReplicationConfig`, `BackupConfig`, `SlowQueryLogConfig`, `PlanCacheConfig`, `MaintenanceConfig`, `SchedulerConfig`, `PageCompressionConfig`, `ParallelQueryConfig`). — fixed 2026-05-29 (cf18d53).
- **`Exec`/`Query` duplicate ~65 lines each** of panic-recovery + conn acquire/release + timeout + metrics + slow-query (`database.go:~519-652`) — extract one `runStatement(isQuery bool, …)`.
- **`createNew`/`loadExisting` duplicate ~100+ lines** of component init (`database_lifecycle.go:~330-471` vs `~496-673`) — extract `initializeCommonComponents()`.
- **webui security** (`webui/server.go`) — `--insecure-no-auth`, startup-printed token with no expiry/rotation, arbitrary SQL with no per-token RBAC/rate-limit/audit. Add expiry/rotation, query audit, rate limiting, optional table allow-listing — or confirm webui isn't for production.

**Medium priority**
- **MySQL param-counting has two implementations** (`mysql.go:~1265-1307`) that must stay in sync — unify to tokenizer-primary, fallback only on tokenizer error.
- `cobaltdb-cli/main.go` (1,375 LOC) dense subcommand `switch` — `Command` interface + registry. Confirm `importCSV` callers check returned errors.
- **Verify** `circuit_breaker.go`/`retry.go` are actually wired into the `Exec`/`Query` path and document the policy (they exist; wiring unconfirmed).

**Low priority / hygiene**
- `cmd/debug`, `cmd/demo`, `cmd/realworld-test` look like throwaway binaries shipped as first-class commands — move to `examples/` or gate out of release builds.
- Connection limiter uses a growable slice of waiter channels — `golang.org/x/sync/semaphore` or `sync.Cond` fits better.
- `Close()` has no shutdown timeout — consider `CloseWithTimeout(ctx)`.

---

## 5. Peripheral packages

**High priority**
- **Two query-result caches, one unused** — `pkg/catalog/catalog_cache.go` (old `QueryCache`, now superseded by `cache.Cache`) vs `pkg/cache/query_cache.go` (now the canonical cache). Catalog now uses `*cache.Cache` exclusively via `catalog.EnableQueryCache()` / `catalog.GetQueryCache()`; `catalog_cache.go` helpers (`isCacheableQuery`, `extractTablesFromQuery`, `queryToSQL`, `generateQueryKey`) remain live and needed. The old `QueryCache` struct in `catalog_cache.go` is dead code — kept only to avoid breaking the refactor scope; should be deleted in a follow-up that also moves the helper functions into a non-cache package. — addressed 2026-05-29.

**Medium priority**
- **`pkg/scheduler` per-job timeout [FIXED 2026-05-29]** — `Job.Timeout` field added; scheduler uses `j.Timeout` if >0, else 10-min default. — fixed 2026-05-29.
- `pkg/metrics` alert cooldown suppresses by elapsed time rather than firing on state change (recovery/re-trigger alerts can be missed); no shared/global AlertManager → subsystems may double-register rules.
- *(If WASM is ever un-gated)* `wasm/host_functions.go` (2,656 LOC) split by domain; `wasm/runtime.go` opcode dispatch → real `switch`. Otherwise consider fully deleting `pkg/wasm` and dropping its README/FEATURES claims.

**Low priority**
- **`pkg/pool` Config.Validate() [FIXED 2026-05-29]** — now checks non-positive MaxIdleTime/MaxLifetime/HealthCheckInterval/HealthCheckTimeout/AcquireTimeout. — fixed 2026-05-29.
- `pkg/fdw` CSV wrapper assumes UTF-8 (no charset option) and doesn't push WHERE predicates into the cursor loop despite the `ScanOptions` plumbing.
- `pkg/cache.estimateSize()` is coarse — can let the cache exceed `MaxSize`.
- `sdk/go` lacks documented thread-safety guarantees on the returned `driver.Conn`.
- `pkg/logger.IsEnabled` is unused — adopt before expensive debug formatting, or drop.

---

## 6. Test Suite (after the quarantine)

**Open — needs engineering investment:**
- **Incremental thin-out:** replace the brittle `coverage_padding` tests package-by-package with focused table-driven tests that lift the *lean* number toward 85%+, then delete each padding file once its unique coverage is reclaimed.
- **Coverage floor:** set a per-package floor and gate CI on the *lean* number, so coverage reflects focused tests, not raw lines.
- **Test-tree split:** three trees (`pkg/`, `integration/`, `test/`) with some duplicated cases (e.g. `TestUpdateWithSubquery` in both `pkg/catalog` and `test/`) — document the intended split (unit vs cross-package vs e2e/bench).

---

## 7. Tooling, Build & Docs

**DONE (2026-05-29):**
- Linting already runs globally — no action needed.
- `AGENTS.md` (`.wrongstack/` + root) deletes; `CLAUDE.md` corrected (SECURITY_PKGS stale, linting runs globally).
- Root binaries gitignored; `data/` and `pkg/engine/backups/` gitignored — leave as-is.

---

## 8. Prioritized Remaining Roadmap

**P0 — correctness (remaining)**
1. `CachedPage.Data()` pin-protocol audit / mutation routing (§1.1) — design.
2. Buffered UPDATE in-txn UNIQUE enforcement (§1.6) — needs product decision.

**P1 — maintainability**
3. Decompose `insertLocked`/`updateLocked` — dedicated reviewed pass (§2).
4. Extract `decodeVisibleRow` + `validateRowAgainstConstraints` (§2).
5. Delete dead `catalog.QueryCache` struct; move helpers (`isCacheableQuery`, `extractTablesFromQuery`, `queryToSQL`, `generateQueryKey`) to a non-cache package [helpers extracted 2026-05-30; QueryCache marked deprecated, retained for tests].
6. Extract shared `runStatement` (Exec/Query) + `initializeCommonComponents` (§4) [runStatement FIXED 2026-05-30].
7. Incremental test thin-out + per-package lean-coverage floor in CI (§6).

**P2 — structure & polish**
8. Expression visitor + precedence-parser dedup [P2-8 DONE — Expression.Evaluate + Evaluator interface, 2026-05-29; P2-5 DONE — parseBinaryOpLevel generic, 2026-05-29] (§3).
9. Group the 50-field `Options` struct (§4) [FIXED 2026-05-29]; harden or scope webui.
10. Cache size accounting; fdw pushdown/charset; deferred unlock in `Acquire` (§5).
11. Audit retry/fail-secure decision (§1.7).
