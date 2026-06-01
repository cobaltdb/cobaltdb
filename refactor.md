# CobaltDB Refactoring & Improvement Report — Remaining Work

**Updated:** 2026-05-30
**Scope:** Full-repository review of `github.com/cobaltdb/cobaltdb`. This document now tracks only the **open** items. Completed work has been merged to `main` and removed from here.

Tags: **[verified]** = read the code and confirmed · **[needs-confirmation]** = static-review lead, confirm before fixing · **[policy]** = needs a product decision, not a mechanical fix.

> **Already done (merged to `main`, branch `refactor/p0-fixes`):** btree LRU double-`Remove` fix · parallel worker-panic isolation (`executor.go`) · deadlock-detector cycle fix (`findWaitCycle`) · dead `WorkerPool` removed · gofmt gate (Make + CI) + whole-tree format · `pkg/wasm` isolated behind `wasm_experimental` · 207 coverage-padding files (~102K LOC) quarantined behind `coverage_padding` (lean 78.4% / full 85.0%) · stray `.wrongstack/` + stale fixtures removed · audit `FailedWriteCount()` + silent-drop fix · `parser.go` split into 4 files · buffered/MVCC constraint-snapshot test coverage raised · **`AGENTS.md`** deleted, **`CLAUDE.md`** corrected (SECURITY_PKGS stale, linting runs globally) · `rollbackLocked` → `releaseAllLocksUnderLock` (lock-ordering) · `flushDirtyPages` error logging + haltable flusher + `FlushErrorCount` metric · panic handlers get `debug.Stack()` in server + protocol · `strictExpect` already correct (confirmed with test).
>
> **Batch 2 (2026-05-29):** optimizer `extractColumnReferences` now handles FunctionCall/CaseExpr/UnaryExpr/InExpr/ExistsExpr/BetweenExpr/CastExpr/LikeExpr/IsNullExpr + subquery columns — mirrors advisor.go coverage · `Job.Timeout` per-job timeout field · pool `Config.Validate()` rejects non-positive MaxIdleTime/MaxLifetime/HealthCheckInterval/HealthCheckTimeout/AcquireTimeout · scheduler `Job.Timeout` field (scheduler/job.go, scheduler/scheduler.go:~296) — per-job timeout, falls back to 10-min default · pool `Config.Validate()` now rejects non-positive MaxIdleTime/MaxLifetime/HealthCheckInterval/HealthCheckTimeout/AcquireTimeout (pool/connection_pool.go:~60)

---

## 1. Correctness & Concurrency — Open Leads

### 1.1 [verified — RESOLVED BY ANALYSIS: no production race] `CachedPage.Data()` raw mutable handle — `pkg/storage/buffer_pool.go:44`
Audited 2026-06-02. The **production** storage path is race-safe:
- The only path the engine wires up is `DiskBackend`/`Memory` → `BufferPool` → `btree.BTree` (`btree.NewBTree(pool)`, `database_lifecycle.go:512`). The background flusher *is* started in production (`database_lifecycle.go:458`, 5 s).
- In `btree.go`, every raw `Data()` use (lines 257, 287, 341, 371) is **read-only** (it copies bytes *out* of the page). Every page-byte **write** goes through `WithDataWrite` (page `mu.Lock`, lines 1072, 1107). The flusher reads through `dataSnapshot` (page `mu.RLock`). So writer↔flusher is Lock↔RLock (safe) and flusher↔btree-read is read↔read (safe); the catalog's single-writer `RWMutex` serializes btree raw-reads against `WithDataWrite`. `go test -race ./pkg/...` is clean.
- The lock-free page-byte **mutations** this item worried about (`pagemgr.go:71,196-208,217,265,373`; `btree_disk.go:164,373-386,440-442`) live exclusively in `PageManager` + `DiskBTree`, which have **no production callers** — `NewPageManager`/`NewDiskBTree` are referenced only from `btree_disk.go` itself and tests. The engine never constructs them.

So this is not a production correctness risk; it is a latent footgun in unwired, test-only code. **Recommendation (not done — low priority):** either route those `PageManager`/`DiskBTree` mutations through `WithDataWrite` *if* `DiskBTree` is ever productionised, or delete the unused `DiskBTree`/`PageManager` (cf. the same dead-code question raised for `pkg/wasm`).

### 1.2 [verified — FIXED] Lock-ordering in txn rollback / lock release — `pkg/txn/manager.go:~257-262, 752-778`
`rollbackLocked` unlocks `t.mu`, calls `ReleaseLock` (takes `lockMu`), then re-locks `t.mu`; other paths take `lockMu` then read txn state. The strict ordering invariant (always `lockMu` before `t.mu`) is documented at the call sites where it would matter. `releaseAllLocksUnderLock` helper is already present (line 803) for single-`lockMu`-acquisition lock release. — closed 2026-05-29.

### 1.3 [verified — FIXED] Background flush errors ignored — `pkg/storage/buffer_pool.go:~553`
Flush errors now logged, counted (`flushErrCount`), and the flusher halts after `flushErrLimit` (3) consecutive failures. — closed 2026-05-29.

### 1.4 [verified — FIXED] Server/protocol panic recovery lacks stack traces — `pkg/server/server.go:297-305`, `pkg/protocol/mysql.go:~336,377`
`debug.Stack()` printed in panic recovery log in both server goroutine and `recordPanic`/`MySQLPanicRecovery`. Auth-error send-failures on half-closed sockets now logged and cause immediate return. — closed 2026-05-29.

### 1.5 [already correct] Permissive parser swallows token errors — `pkg/query/parser_dml_select.go` (`parseJoinType`)
`strictExpect` (parser.go:166) already returns an error in strict mode and silently ignores mismatches in permissive mode. Test `TestParseStrictRejectsMalformedJoins` confirms strict mode works. No change needed. — confirmed 2026-05-29.

### 1.6 [verified — FIXED] `TestConcurrentTransactions` silently swallowed insert errors — `race_detection_test.go:104`
The test recorded `txnErrs` but never checked it, masking concurrent insert failures. Test now asserts `txnErrs == 0` before checking row count. Root cause was the data race in `runStatement` (fixed in §1.7). — fixed 2026-05-30.

### 1.7 [verified — FIXED] Concurrent INSERT failures — `database.go:runStatement`
Root cause was a data race in `runStatement`: it stored the parsed statement in a mutex-protected DB field (`_parsedStmt`) and retrieved it after the defer was set up. Concurrent Exec/Query calls from different goroutines would overwrite each other's statements. Fix: stmt is already in closure scope — removed the mutex relay entirely. `_parsedStmt`/`_parsedStmtMu` fields removed from DB struct. TestConcurrentTransactions now passes consistently with `-race`. — fixed 2026-05-30.

### 1.8 [verified — FIXED] UPDATE silently broke UNIQUE among rows mutated together — `pkg/catalog` update path
Root cause was broader than first logged: the per-row UNIQUE check (`checkConstraintsForUpdate` for the buffered path, the inline loop in `processUpdateRowData` for autocommit) scanned **only the committed B-tree**, ignoring (a) other rows updated earlier in the *same statement* and (b) buffered writes from earlier statements in the *same txn*. Confirmed via probe that `UPDATE t SET code='C'` across two rows, and two `UPDATE`s in one txn, both silently committed duplicate UNIQUE values — even in plain autocommit, not just buffered mode. Fix: extracted `hasUpdateUniqueConflict` (catalog_update.go) which checks the staged statement entries and txn pending-writes (read-your-writes, so a legitimate value hand-off still succeeds) before the committed scan; both update paths call it. Regression tests in `buffered_constraints_test.go` (`TestBufferedUpdateUniqueConflictCrossStatement`, `TestUpdateUniqueConflictWithinStatement`, `TestBufferedUpdateUniqueValueHandoff`). The single- and multi-column UNIQUE **index** paths were probed for the same gap and found safe — the index tree gives a second line of defence at apply time and rolls the statement back, so only column-level `UNIQUE` (which has no backing index) was corrupting. — fixed 2026-06-02.

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
- Three near-identical scan branches (index / MV / B-tree) in `scanTableRows` (`catalog_core.go:~852-1125`) — extract `filterAndProjectRow`. — **DONE (2026-05-30): `filterAndProjectRow` extracted and used in index and general B-tree sequential branches; fast B-tree path intentionally keeps its own optimized `decodeVersionedRowFastEx` buffer-reuse path.**
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
- `canUseIndex` returns true without knowing an index exists → skews cost estimates; pass index metadata in. — Partially addressed: `optimizeProjections` (line 265-276) already checks `qo.stats.IndexStats` before setting `IndexHint = "auto"`. The `canUseIndex` helper itself does not verify index existence; fixing it properly requires wiring catalog index metadata into `qo.stats` (not just `IndexStats` which is never populated in normal use).
- Audit parsed-but-never-executed AST node types (`SHOW`/`SET`/`DESCRIBE`) — implement or document.

---

## 4. `pkg/engine`, `pkg/server`, `pkg/protocol`, `cmd/`, `webui/`

**High priority**
- **`Options` has 50 fields [FIXED 2026-05-29]** (`engine/database.go`) — split into 12 nested option structs (`CoreStorage`, `ConnectionPool`, `Security`, `QueryCache`, `ReplicationConfig`, `BackupConfig`, `SlowQueryLogConfig`, `PlanCacheConfig`, `MaintenanceConfig`, `SchedulerConfig`, `PageCompressionConfig`, `ParallelQueryConfig`). — fixed 2026-05-29 (cf18d53).
- **`Exec`/`Query` duplicate ~65 lines each** of panic-recovery + conn acquire/release + timeout + metrics + slow-query (`database.go:~519-652`) — extract one `runStatement(isQuery bool, …)`. — **`runStatement` already extracted and in use; Exec/Query both call it (database.go:558).**
- **`createNew`/`loadExisting` duplicate ~100+ lines** of component init (`database_lifecycle.go:~330-471` vs `~496-673`) — extract `initializeCommonComponents()`.
- **webui security** (`webui/server.go`) — `--insecure-no-auth`, startup-printed token with no expiry/rotation, arbitrary SQL with no per-token RBAC/rate-limit/audit. Add expiry/rotation, query audit, rate limiting, optional table allow-listing — or confirm webui isn't for production.

**Medium priority**
- **MySQL param-counting has two implementations** (`mysql.go:~1265-1307`) that must stay in sync — unify to tokenizer-primary, fallback only on tokenizer error. — **FIXED (2026-05-30): `countQuestionMarksOutsideQuotes` inlined as a labeled fallback inside `countPreparedParams`. One function now; no duplication risk.**
- `cobaltdb-cli/main.go` (1,375 LOC) dense subcommand `switch` — `Command` interface + registry. Confirm `importCSV` callers check returned errors. — **importCSV caller check DONE (2026-05-30): `runImportCommand` correctly handles error (stderr + exit 1). Switch→interface refactor remaining.**
- **Verify** `circuit_breaker.go`/`retry.go` are actually wired into the `Exec`/`Query` path and document the policy (they exist; wiring unconfirmed). — **CONFIRMED NOT WIRED (2026-05-30):** `CircuitBreakerManager` and `RetryConfig` exist in `ProductionConfig` but are not present in the `DB` struct and are not called from `Exec`/`Query`/`runStatement`. They are standalone utilities. Wiring would require adding them to `DB` struct and wrapping statement execution. Product decision needed: which operations should be wrapped.

**Low priority / hygiene**
- `cmd/debug`, `cmd/demo`, `cmd/realworld-test` look like throwaway binaries shipped as first-class commands — move to `examples/` or gate out of release builds.
- Connection limiter uses a growable slice of waiter channels — `golang.org/x/sync/semaphore` or `sync.Cond` fits better.
- `Close()` has no shutdown timeout — consider `CloseWithTimeout(ctx)`.

---

## 5. Peripheral packages

**High priority**
- **Two query-result caches, one unused** — `pkg/catalog/catalog_cache.go` (old `QueryCache`, now superseded by `cache.Cache`) vs `pkg/cache/query_cache.go` (now the canonical cache). Catalog now uses `*cache.Cache` exclusively via `catalog.EnableQueryCache()` / `catalog.GetQueryCache()`; `catalog_cache.go` helpers (`isCacheableQuery`, `extractTablesFromQuery`, `queryToSQL`, `generateQueryKey`) are deprecated wrappers delegating to `pkg/query`. The old `QueryCache` struct is deprecated. Cannot delete helpers until test files (`z_eval_test.go`, `z_catalog_coverage_test.go`, etc.) are updated to import `pkg/query` directly — currently they don't import it. — addressed 2026-05-29; follow-up needed.

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
1. ~~`CachedPage.Data()` pin-protocol audit~~ — RESOLVED BY ANALYSIS 2026-06-02 (§1.1): production path (BufferPool + `btree.BTree`) is race-safe; the lock-free mutations are confined to the unwired `PageManager`/`DiskBTree`. No production race; remaining work is low-priority dead-code hardening or removal.
2. ~~Buffered UPDATE in-txn UNIQUE enforcement~~ — FIXED 2026-06-02 (§1.8): turned out to be silent UNIQUE corruption in *all* UPDATE modes, not a semantics decision.

With both P0 items closed, there are no known open correctness/concurrency defects on the production path.

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
