# CobaltDB Refactoring & Improvement Report

**Date:** 2026-05-29
**Scope:** Full-repository review of `github.com/cobaltdb/cobaltdb` — every package under `pkg/`, all `cmd/` binaries, `webui/`, `sdk/`, the test trees, and build/repo tooling.
**Method:** Seven parallel subsystem deep-dives, followed by manual verification of the highest-impact findings against the actual source. Findings below are tagged **[verified]** (I read the code and confirmed), **[needs-confirmation]** (reported by analysis, not yet hand-verified — treat as a lead), or **[corrected]** (an initially-reported issue that turned out to be a false alarm — documented so it is not re-investigated).

---

## 1. Executive Summary

CobaltDB is a large, ambitious, single-node SQL engine (~74K non-test LOC across 119 files, 24 packages). The architecture is sound and the core paths are well-factored. `go vet ./...` passes and the build is clean. The most valuable refactoring work falls into five themes, in rough priority order:

1. **A handful of real correctness/concurrency bugs** in the storage and concurrency core that should be fixed immediately (Section 3).
2. **Silently-swallowed errors and panics** in critical paths (parallel workers, audit writes, replication callbacks, server connection handlers) that turn failures into invisible data loss (Section 4).
3. **Test-suite bloat:** 208 "coverage-boost" test files totalling ~101,810 lines (~33% of all test code) that pad coverage metrics without proportional signal. This is the single largest maintainability liability in the repo (Section 9).
4. **God files / god functions** in the catalog, parser, and engine that should be decomposed (Sections 5–8).
5. **A ~5,400-LOC orphaned WASM subsystem** that is imported nowhere and duplicates the query engine (Section 8).

There is also routine hygiene work: 6 non-test files are not `gofmt`-clean, a 15 MB binary and test artifacts are committed to the repo, and `AGENTS.md` duplicates `CLAUDE.md`.

A prioritized roadmap is in Section 11.

---

## 2. Repository Facts (measured)

| Metric | Value |
|---|---|
| Non-test source | ~74,255 LOC / 119 files |
| Test code | ~309,494 LOC / 661 files |
| Test-to-source ratio | ~4.2 : 1 |
| Coverage-padding test files | **208 files / ~101,810 LOC (~33% of test LOC)** |
| `go vet ./...` | passes |
| Non-test files failing `gofmt -l` | 6 |
| Largest source file | `pkg/query/parser.go` (3,672 LOC) |
| `Options` struct fields | 50 |
| Direct dependencies | 6 (lean) |

Largest source files (refactoring candidates): `parser.go` (3672), `engine/database.go` (2903), `wasm/host_functions.go` (2656), `catalog_core.go` (2425), `catalog_select.go` (2398), `catalog_insert.go` (1980), `catalog_update.go` (1941), `protocol/mysql.go` (1912), `replication.go` (1841), `catalog_eval.go` (1762).

---

## 3. Correctness & Concurrency Bugs — Fix First

These are the highest-severity findings. Several were hand-verified.

> **Status (2026-05-29):** §3.1 and §3.3 have been **fixed** on branch `refactor/p0-fixes` with regression tests; §3.2 has been **re-characterized** (the one-line fix was wrong — see below). The remaining items (§3.4–§3.6) are still open leads.

### 3.1 [verified — FIXED] Double LRU-list removal corrupts the cache list — `pkg/btree/btree.go:706-707`
In the `PutBatch` update path, an existing key's LRU entry is removed from the list **twice**:
```go
if entry, ok := sh.lruMap[kc]; ok {
    sh.lruList.Remove(entry)
    sh.lruList.Remove(entry)   // <-- duplicate; operates on a detached node
    delete(sh.lruMap, kc)
}
```
The single-key path (`putStringInternal`, ~line 578) correctly removes once. **Confirmed severity:** after the first `Remove` sets `e.prev`/`e.next` to nil, the second `Remove` takes the `e.prev == nil` branch and executes `l.head = e.next` → `l.head = nil`, **wiping the entire list head** and orphaning every other node — not a benign no-op.
**Fix applied:** deleted the duplicate `Remove`, and (allocation parity) reused the removed node when updating an existing key, mirroring the single-key path, instead of allocating a fresh `&lruEntry{}`. Verified by existing batch tests + `-race`.

### 3.2 [re-characterized] `CachedPage.Data()` is a raw mutable handle — partial fix already in place, latent race remains — `pkg/storage/buffer_pool.go:41-44`
```go
func (p *CachedPage) Data() []byte { return p.data }
```
**Correction to the original finding:** the one-line "add an `RLock` to `Data()`" fix is *wrong* and would be safety theater. `Data()` is used by ~30 call sites in `pkg/storage` and `pkg/btree` as a **mutable handle** — callers both read and write through the returned slice (e.g. `copy(page.Data(), ...)`, `oldRootPage.Data()[4] = ...`). Locking only the header read does nothing, because the caller keeps using the slice after the lock drops.

What actually exists: the handicap report's race fix added `dataSnapshot()` (RLock; used by the background flusher at `buffer_pool.go:308`) and `WithDataWrite()` (Lock; used by btree's flush-write path at `btree.go:1072,1107`). That pair correctly serializes *the specific* flusher-vs-flush-write race. **The latent risk:** every *other* mutator still writes page bytes through raw `Data()`, so any mutation that can run concurrently with the background flusher's snapshot is still a data race. Whether that is reachable depends on the pin/unpin protocol and btree shard/`flushMu` invariants.
**Correct fix (design, not one-liner):** audit the pin protocol to prove no raw-`Data()` mutation overlaps a background flush, and document that invariant; *or* route all page-byte mutation through `WithDataWrite` and all flush reads through `dataSnapshot`, then keep `Data()` for read-only-while-pinned use. Deferred — not a safe mechanical change.

### 3.3 [verified — FIXED, redirected] Parallel query workers had no panic isolation — `pkg/parallel/executor.go`
**Correction to the original finding:** the flagged `WorkerPool.run` swallow at `pkg/parallel/pool.go:148` is real but **moot** — `WorkerPool` is **dead code**, imported nowhere outside tests. The production parallel paths the catalog actually uses are `ParallelSelectRows` (`catalog_core.go:1081`) and `ParallelGroupBy` (`catalog_aggregate.go:270`), implemented in `executor.go` with **raw goroutines and no `recover` at all**. That is *worse* than a silent swallow: a panic during row decode / WHERE eval in a worker goroutine is unrecoverable and **crashes the entire database process**.
**Fix applied:** added a `panicCapture` helper to `executor.go` that records the first worker panic and re-raises it on the calling goroutine after `wg.Wait()`, in all three functions (`ParallelSelectRows`, `ParallelGroupBy`, `ParallelAggregate`). The engine's existing query-level `recover` (`engine/database.go:522,591`) then converts it into a failed query instead of a process crash. Added `pkg/parallel/panic_propagation_test.go` proving propagation on both the parallel and serial paths.
**Follow-up:** `WorkerPool` (pool.go) is unused — delete it, or fix its swallow if a use is planned (see §8).

### 3.4 [verified — FIXED, redirected] Deadlock-detector included path-into-cycle nodes as victims — `pkg/txn/manager.go`
**Correction to the original finding:** the claim that interior nodes are *omitted* (e.g. `{A,C}` without `B`) is **wrong** — the unwind appends actually produce `[A,C,B,A]` for `A→B→C→A`, which contains every member, so victim selection for a *pure* cycle was already correct. The **real** bug: the unwind also appends nodes on a path *leading into* the cycle. For `D→A→B→C→A`, starting DFS at the tail `D` yields `[A,C,B,A,D]`; if `D` (an innocent transaction merely blocked on a cycle member) has the highest `StartTS`, `resolveDeadlock` aborts `D` — which does **not** break the cycle and needlessly kills a transaction.
**Fix applied:** extracted the graph algorithm into a pure, unit-testable `findWaitCycle(waitingMap)` that, on detecting a back-edge, reconstructs **only** the true cycle members (walking `waitingFor → … → txnID`) and excludes path-into-cycle nodes. `checkForDeadlocks` is now a thin wrapper. Added `pkg/txn/wait_cycle_test.go` with a deterministic 200-iteration tail-exclusion test plus an end-to-end test asserting the victim is a cycle member, not the higher-`StartTS` tail. Existing deadlock tests still pass; `-race` clean.

### 3.5 [corrected] Group-commit flush window is **not** a bug — `pkg/storage/wal.go:681-733`
The original finding recommended "snapshot-and-swap the pending slice under the lock" as the fix. That pattern is **already implemented**: `flushPendingLocked` re-acquires `groupCommitMu`, snapshots `pending := w.pendingSyncs`, swaps in an empty slice, unlocks, then signals each `done` channel (wal.go:725-732). Every `done` is appended exactly once under the lock and removed exactly once by the atomic swap, so it cannot be lost or double-signalled; `append` growth preserves all elements and happens under the lock. Concurrent flushes (batch-full trigger vs. ticker) are safe and at worst do a redundant fsync. No change needed. (The `DisableGroupCommit` defer-at-top + manual unlock/relock dance at wal.go:643-659 is correct but stylistically confusing — optional cleanup, not a bug.)

### 3.5 [needs-confirmation] Group-commit flush releases the lock mid-mutation — `pkg/storage/wal.go:681-691`
`groupCommitAppend` appends a `done` channel to `pendingSyncs`, then unlocks `groupCommitMu` before calling `flushPendingLocked()`. There is a window where another caller can mutate/reallocate `pendingSyncs` between the unlock and the flush.
**Fix:** snapshot-and-swap the pending slice under the lock (`pending := w.pendingSyncs; w.pendingSyncs = w.pendingSyncs[:0]`), unlock, then flush the local slice. The same unlock/relock-around-`defer` anti-pattern appears in `DisableGroupCommit` (wal.go:643-659) and should be simplified the same way.

### 3.6 [needs-confirmation] Lock-ordering risk in txn rollback / lock release — `pkg/txn/manager.go:~257-262, 752-778`
`rollbackLocked` unlocks `t.mu`, calls `ReleaseLock` (which takes `lockMu`), then re-locks `t.mu`; meanwhile other paths take `lockMu` then read txn state. There is no single documented lock-ordering invariant, leaving a multi-party deadlock window.
**Fix:** establish and document a strict order (always `lockMu` before `t.mu`), and provide a `releaseAllLocksUnderLock` helper that frees all of a txn's locks in one `lockMu` acquisition.

### 3.7 [corrected] Backup verify is **not** off-by-one — `pkg/backup/backup.go:693`
An analysis pass flagged `LimitedReader{N: backup.Size + 1}` as allowing truncated backups to pass. **This is incorrect.** The `+1` lets the reader observe an oversized file, and the subsequent `readSize == backup.Size` equality rejects both truncated (`readSize < Size`) and oversized (`readSize == Size+1`) files. The code is correct as written — no change needed. (Documented here to avoid re-flagging.)

---

## 4. Swallowed Errors & Silent Failures (cross-cutting)

A recurring pattern across the codebase is discarding errors/panics in paths where silence equals data loss or a hung client. Beyond §3.3:

- **[needs-confirmation] Audit write failures are swallowed** — `pkg/audit/logger.go:299-317, 356-381`. When the async channel is full, the sync fallback stores the error in `lastErr` instead of surfacing it; `Log()` callers never learn the audit record was dropped. For a compliance feature this is fail-*open*. **Fix:** make critical-event logging write-through and return an error; add bounded retry with backoff; emit write failures to stderr/syslog.
- **[needs-confirmation] Replication callback panics swallowed** — `pkg/replication/replication.go:288-334`. `OnApply` panics are logged but execution continues without advancing `lastApplied`, risking master/slave LSN drift. **Fix:** return errors from callbacks; stop replication on apply failure rather than continuing.
- **[verified] Permissive parser swallows token errors** — `pkg/query/parser.go:428-462` (`parseJoinType`) uses `_, _ = p.expect(TokenJoin)` in 6 places, so malformed JOIN syntax silently mis-parses. Acceptable in permissive mode, but in `StrictSQLParsing` these should be hard errors. **Fix:** thread a strict flag (or return an error) so strict mode rejects what permissive mode tolerates.
- **[needs-confirmation] Background flush errors ignored** — `pkg/storage/buffer_pool.go:~553` (`_ = bp.FlushPage(page)`). A full disk silently marks pages clean. **Fix:** log + count failures; consider halting the flusher on persistent I/O error.
- **[needs-confirmation] Server/protocol panic recovery lacks stack traces** — `pkg/server/server.go:297-305`, `pkg/protocol/mysql.go:~336,377`. Panics in connection handlers are recovered but logged without `debug.Stack()`; auth-error send failures are dropped (`_ = sendErr`) so a half-closed socket can hang the next read. **Fix:** log full stack; return immediately on send failure.

**Recommendation:** add `errcheck` to the lint gate for *all* packages (currently scoped), and grep for `_ =` on function results in security/storage/txn paths as a one-time sweep.

---

## 5. `pkg/catalog` — God Functions & Duplication

The catalog is well-organized by file, but several functions are too large and several patterns are copy-pasted.

**High priority**
- **`insertLocked` is ~479 lines** (`catalog_insert.go:~1007-1485`) — validation + constraints + undo + buffered writes + indexes + triggers + RETURNING in one function. Split into `validateInsert`, `buildRowWithConstraints`, `recordInsertUndo`, `applyIndexUpdatesForInsert`.
- **`updateLocked` is ~269 lines** (`catalog_update.go:~582-851`) — split into `resolveUpdateTargetRows`, `validateUpdateConstraints`, `applyUpdateIndexes`.
- **Row decode + visibility check duplicated 30+ times** — the `decodeVersionedRow` → `isVisibleAt` → `vrow.Data` triple appears across `catalog_core.go`, `catalog_insert.go`, `catalog_update.go`, `catalog_delete.go`. Extract `decodeVisibleRow(valueData, columns, queryTime) (row, ok, err)`.
- **Expression dispatch is a giant switch** — `catalog_eval.go` `evaluate` (~lines 51-208) + `evaluateFunctionCall` (~395-558) dispatch 20+ node types and dozens of SQL functions via string matching. The per-function helpers already exist (`evalUpper`, etc.); wire them through a `map[string]funcHandler` dispatch table to flatten the switch and make the function set introspectable.
- **Lock release/reacquire in `selectLockedInternal`** (`catalog_core.go:~594-845`) conditionally drops and re-takes the read lock mid-function. Given the non-reentrant mutex this is fragile. Split into a lock-holding outer entry and a lock-free `selectUnlocked` inner path; simplify the `canReleaseLock` predicate.

**Medium priority**
- Three near-identical scan branches (index / MV / B-tree) in `scanTableRows` (`catalog_core.go:~852-1125`) share filter+project logic — extract `filterAndProjectRow`.
- Constraint-checking loops (UNIQUE/FK/CHECK) are duplicated across insert and update — extract `validateRowAgainstConstraints`.
- Audit `fmt.Errorf("...: %v", err)` vs `%w` usage for consistent error-chain support.

---

## 6. `pkg/query`, `pkg/optimizer`, `pkg/advisor` — Parser & Analysis

**High priority**
- **`parser.go` (3,672 LOC) has no internal structure** — split by concern into `parser_select.go`, `parser_dml.go`, `parser_ddl.go` (DDL alone is ~1,460 lines), `parser_expression.go`, `parser_helpers.go`, leaving the `Parser` struct and top-level `Parse()` in `parser.go`.
- **Column-extraction logic is duplicated and inconsistent** between the optimizer (`optimizer.go:~282-294`, handles 3 expression types) and the advisor (`advisor.go:~334-408`, handles 13). Neither is complete (both miss columns inside `FunctionCall`, `CaseExpr`, subqueries). **Fix:** one shared `ExtractColumnsFromExpr` in `pkg/query`, used by both.
- **No expression visitor** — at least three independent type-switches walk the AST (parser, optimizer, advisor), each with different omitted cases. Introduce an `ExpressionVisitor` / `Walk(expr, visitor)` to centralize traversal and prevent case-omission bugs.

**Medium priority**
- **Precedence-parser boilerplate** — `parseOr/parseAnd/parseAdditive/parseMultiplicative` (~lines 690-895) are six copies of the same loop. Replace with one generic `parseBinaryOpLevel(next, ops...)`.
- AST design inconsistencies (`WindowExpr` duplicates `FunctionCall` shape; no shared interface for `SelectStmt`/`UnionStmt`); reserved-word/identifier handling is partly centralized (`isStructuralKeyword`) but partly scattered through DDL parse functions — centralize a `canBeIdentifier(TokenType)`.
- Inconsistent parser error-message formats; standardize templates.

**Low priority**
- Reflection-based `clone.go` is unused in favor of hand-written copies in the optimizer — pick one (prefer explicit `Clone()` methods) and delete the other.
- `canUseIndex` (optimizer) optimistically returns true without knowing whether an index exists, skewing cost estimates — pass index metadata in.
- Audit for parsed-but-never-executed AST node types (e.g. some `SHOW`/`SET`/`DESCRIBE` statements) and either implement or document them.

---

## 7. `pkg/engine`, `pkg/server`, `pkg/protocol`, `cmd/`, `webui/`

**High priority**
- **`Options` has 50 fields** (`engine/database.go`) spanning ~12 subsystems. Group into nested option structs (`ReplicationOptions`, `BackupOptions`, `SlowQueryOptions`, …) — reduces API surface and clarifies ownership.
- **`Exec` and `Query` duplicate ~65 lines each** of panic-recovery + connection acquire/release + timeout + metrics + slow-query wrapping (`database.go:~519-652`). Extract one `runStatement(isQuery bool, ...)` wrapper.
- **`createNew` and `loadExisting` duplicate ~100+ lines** of component init (query cache, optimizer, replication, slow-query log) — `database_lifecycle.go:~330-471` vs `~496-673`. Extract `initializeCommonComponents()`.
- **webui security posture** (`webui/server.go`) — `--insecure-no-auth`, a startup-printed token with no expiry/rotation, and arbitrary SQL with no per-token RBAC, rate limiting, or audit trail. If shipped, add token expiry/rotation, query audit, rate limiting, and optional table allow-listing. Confirm whether webui is intended for production at all.

**Medium priority**
- **MySQL protocol param-counting has two implementations** (`mysql.go:~1265-1307`: tokenizer + `countQuestionMarksOutsideQuotes` fallback) that must stay in sync — unify to tokenizer-primary with fallback only on tokenizer error.
- `cobaltdb-cli/main.go` (1,375 LOC) dispatches subcommands via a dense `switch` — a `Command` interface + registry would simplify and standardize error handling. Confirm `importCSV` callers actually check returned errors.
- **[corrected/clarify]** An analysis pass claimed Exec/Query have "no circuit breaker or retry." In fact `pkg/engine/circuit_breaker.go` and `pkg/engine/retry.go` exist. The real action item is to **verify they are actually wired into the `Exec`/`Query` execution path** and document the policy — not to build them.

**Low priority / hygiene**
- `cmd/debug`, `cmd/demo`, `cmd/realworld-test` look like throwaway/example binaries shipped as first-class commands — move to `examples/` or gate out of release builds.
- Connection limiter uses a growable slice of waiter channels — a `golang.org/x/sync/semaphore` or `sync.Cond` is the idiomatic fit.
- `Close()` has no shutdown timeout; consider `CloseWithTimeout(ctx)`.

---

## 8. Peripheral packages (`wasm`, `fdw`, `parallel`, `cache`, `pool`, `scheduler`, `metrics`, `logger`, `sdk`)

**High priority**
- **[verified] `pkg/wasm` is orphaned — 0 imports outside its own package.** ~5,400 LOC of source (compiler.go 1490, runtime.go 1283, host_functions.go 2656) plus ~25 test files, none referenced by `engine`, `catalog`, `server`, or `sdk`. It is a parallel re-implementation of query execution that nothing calls. CLAUDE.md itself marks WASM "experimental." **Decision needed:** delete it, or move it to a separate module / behind a `//go:build wasm_experimental` tag and stop counting it toward coverage. This is the largest single chunk of dead weight in the repo. (See §3.3 — its worker model also swallows panics.)
- **Three overlapping cache layers** — `pkg/cache` (result cache, SHA256-keyed, table-dep tracking), `pkg/catalog/catalog_cache.go` (result cache, count-limited), and `pkg/engine/query_plan_cache.go` (parsed-plan cache). All re-implement LRU + RWMutex + hit/miss counters with **inconsistent invalidation** (fine-grained vs full-table vs none). Consolidate onto one cache core with pluggable key/value and a single invalidation signal.
- **[verified — FIXED] `pkg/parallel` worker panics now isolated** (§3.3). Separately, the unused `WorkerPool` type in `pool.go` (zero non-test callers) is dead code and should be deleted unless a use is planned.

**Medium priority**
- `wasm/host_functions.go` (2,656 LOC, ~62 functions) should be split by domain (table/join/aggregate/udf/vectorized) *if* the package is kept.
- `wasm/runtime.go` opcode dispatch uses chained `case opcode == 0xNN` comparisons instead of a `switch opcode { case ... }` — convert for O(1) dispatch and readability.
- `pkg/scheduler` hard-codes a 10-minute per-job context timeout (`scheduler.go:~296`) — make it a per-`Job` field.
- `pkg/metrics` alert cooldown suppresses by elapsed time rather than firing on state change, so recovery/re-trigger alerts can be missed; and there is no shared/global AlertManager so subsystems may double-register rules.

**Low priority**
- `pkg/pool` `Config.Validate()` doesn't reject non-positive `MaxIdleTime`/`MaxLifetime`/`HealthCheckInterval`; the lock/unlock pattern in `Acquire` is correct but defer-unlock would harden it against future edits.
- `pkg/fdw` CSV wrapper assumes UTF-8 (no charset option) and does not push WHERE predicates into the cursor loop despite the `ScanOptions` plumbing existing.
- `pkg/cache.estimateSize()` is a coarse heuristic that can let the cache exceed `MaxSize`.
- `sdk/go` lacks documented thread-safety guarantees on the returned `driver.Conn`.
- `pkg/logger.IsEnabled` exists but is unused — adopt it before expensive debug formatting, or drop it.

---

## 9. Test Suite — The Biggest Maintainability Liability

**[verified] 208 "coverage-boost" test files totalling ~101,810 LOC (~33% of all test code).** Naming patterns: `coverage_boost*`, `z_coverage_*`, `z_real_functional`, `z_integration_full`. `pkg/catalog` alone has well over 100 such files (e.g. `z_coverage_boost2..125`).

Characteristics observed in samples:
- Monolithic catch-all functions (e.g. `test/v108_coverage_boost_test.go`: 1,027 lines / 1 test function) that fire 100+ statements with weak or absent assertions.
- Non-deterministic "skip on empty" logging (`t.Logf("[SKIP-EMPTY] ...")`) that hides failures.
- Per-file duplicated helpers (`newBoostCatalog2`, `boost2CreateTable`, `boost2Query`, …) instead of shared fixtures.

**Why it matters:** these files inflate the line count 4:1, slow the suite, obscure which tests carry real signal, and make refactoring terrifying (changing a function breaks dozens of brittle boost tests for the wrong reasons). They optimize a coverage *number*, not correctness.

**Recommendation:**
1. Quarantine the 208 files behind a build tag (`//go:build coverage_padding`) so the default suite runs only meaningful tests; measure the real coverage that remains.
2. For each package, replace the boost files with a small number of focused, well-asserted table-driven tests targeting the genuinely-uncovered branches.
3. Centralize test helpers (one `catalog_test_helpers.go`, one `test/helpers.go`) and delete the per-file `boostN*` duplicates.
4. Set a coverage *floor* per production-critical package (CLAUDE.md targets 90%) but stop rewarding raw line coverage.

**Other test concerns:** three test trees (`pkg/`, `integration/`, `test/`) with some duplicated cases (e.g. `TestUpdateWithSubquery` in both `pkg/catalog` and `test/`); document the intended split (unit vs cross-package vs e2e/bench).

---

## 10. Tooling, Build & Repo Hygiene

- **[verified] 6 non-test source files are not `gofmt`-clean:** `catalog_eval.go`, `catalog_rls.go`, `catalog_select_helpers.go`, `catalog_window.go`, `foreign_key.go`, `scheduler/job.go` (plus several test files). Run `gofmt -w ./pkg ./cmd` and add a CI `gofmt -l` gate that fails on any output.
- **Linting is narrowly scoped.** `.golangci.yml` enables only ~5 linters (errcheck, gosimple, govet, ineffassign, staticcheck/unused), and the Makefile's `lint`/`gosec` target only runs over `SECURITY_PKGS`. Most packages are effectively unlinted. Widen `errcheck` and the linter set across all packages.
- **Committed artifacts that should be removed / gitignored:**
  - `cobaltdb-server` (~15 MB binary) and `cobaltdb-cli` at repo root — `git rm --cached` and ignore.
  - Test-generated data: `data/`, `test.cobalt.data/`, and `pkg/engine/backups/` (reported ~18 MB).
  - `.wrongstack/` (stray agent dir), `Dockerfile.backup` (dead duplicate).
- **`AGENTS.md` duplicates `CLAUDE.md`** (CLAUDE.md says so) and is stale. Consolidate to one source, or generate one from the other.
- **`go.mod` is healthy** — Go 1.25 / toolchain 1.26.3 pinned, only 6 lean direct deps.
- Multiple large narrative docs (`README.md` ~1000 lines, `CHANGELOG.md` ~1200, `FEATURES.md`, `COBALTDB_HANDICAP_REPORT.md`) — fine, but keep a single source of truth for feature status.

---

## 11. Prioritized Roadmap

**P0 — correctness (days)**
1. ✅ **DONE** — Fixed double LRU `Remove` (§3.1). `CachedPage.Data()` (§3.2) re-characterized as a design task, not a one-liner — deferred.
2. ✅ **DONE** — Parallel worker panics now isolated (§3.3). Still open: audit-write/replication-callback failures (§4).
3. ✅ **DONE** — Deadlock-detector now excludes path-into-cycle nodes from victim selection (§3.4, fixed + tests). Group-commit lock window (§3.5) investigated and **dismissed as a false positive** (the recommended fix already exists).
4. ✅ **PARTIAL** — `gofmt -w` applied to the 6 source files (§10). Still open: add `gofmt`/`errcheck`-all CI gates.

**P1 — maintainability (1–3 weeks)**
5. Quarantine + thin out the 208 coverage-padding test files; centralize helpers (§9).
6. Decide WASM's fate: delete or isolate behind a build tag (§8).
7. Decompose `insertLocked`/`updateLocked` and extract the row-decode helper (§5).
8. Extract shared `runStatement` for Exec/Query and `initializeCommonComponents` for create/load (§7).
9. Unify the three cache layers (§8) and the duplicated column-extraction logic (§6).

**P2 — structure & polish (ongoing)**
10. Split `parser.go` and add an expression visitor (§6).
11. Group the 50-field `Options` struct into nested configs (§7).
12. Harden webui (auth/expiry/RBAC/rate-limit) or scope it down (§7).
13. Remove committed binaries/artifacts; consolidate `AGENTS.md`/`CLAUDE.md`; widen linting (§10).
14. Per-job scheduler timeouts, fdw predicate pushdown, pool config validation, metrics alert-on-change (§8).

---

## 12. Verification Notes

Hand-verified against source in this review: §3.1 (btree double Remove), §3.2 (unlocked `Data()`), §3.3 (parallel panic swallow), §4 parser join errors, §8 WASM has zero external imports, §9 padding-file count (208) and LOC (~101,810), §10 gofmt offenders (6), §7 Options field count (50), and the existence of `circuit_breaker.go`/`retry.go`. One reported issue was **disproven** and excluded from action items: the backup checksum "off-by-one" (§3.7) is correct code. All findings tagged **[needs-confirmation]** are strong leads from static review that should be confirmed (ideally with a targeted regression test) before the fix lands.
