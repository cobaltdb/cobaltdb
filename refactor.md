# CobaltDB Refactoring & Improvement Report — Remaining Work

**Updated:** 2026-05-29
**Scope:** Full-repository review of `github.com/cobaltdb/cobaltdb`. This document now tracks only the **open** items. Completed work has been merged to `main` and removed from here.

Tags: **[verified]** = read the code and confirmed · **[needs-confirmation]** = static-review lead, confirm before fixing · **[policy]** = needs a product decision, not a mechanical fix.

> **Already done (merged to `main`, branch `refactor/p0-fixes`):** btree LRU double-`Remove` fix · parallel worker-panic isolation (`executor.go`) · deadlock-detector cycle fix (`findWaitCycle`) · dead `WorkerPool` removed · gofmt gate (Make + CI) + whole-tree format · `pkg/wasm` isolated behind `wasm_experimental` · 207 coverage-padding files (~102K LOC) quarantined behind `coverage_padding` (lean 78.4% / full 85.0%) · stray `.wrongstack/` + stale fixtures removed · audit `FailedWriteCount()` + silent-drop fix · `parser.go` split into 4 files · buffered/MVCC constraint-snapshot test coverage raised.
>
> **Disproven (do not re-investigate):** `CachedPage.Data()` needing a simple `RLock` (would be theater — see §1.1) · group-commit "flush window" (snapshot-and-swap already implemented) · backup verify "off-by-one" (correct) · replication apply-callback panics (already converted to errors, LSN not advanced on failure) · `Dockerfile.backup` "dead" (it's live — built by `docker-compose.yml`).

---

## 1. Correctness & Concurrency — Open Leads

### 1.1 [verified — design task] `CachedPage.Data()` raw mutable handle — `pkg/storage/buffer_pool.go:41`
`Data()` returns `p.data` lock-free and is used by ~30 call sites in `pkg/storage`/`pkg/btree` as a **mutable** handle (callers write through it). The handicap-report fix (`dataSnapshot()` for the flusher, `WithDataWrite()` for btree flush-writes) closes only the specific flusher-vs-flush-write race; any *other* mutator writing page bytes via raw `Data()` concurrently with a background flush is still a data race. A simple `RLock` on `Data()` does nothing (caller uses the slice after the lock drops).
**Fix (design, not one-liner):** audit the pin/unpin + btree `flushMu`/shard invariants to prove no raw-`Data()` mutation overlaps a background flush, and document that invariant; *or* route all page-byte mutation through `WithDataWrite` and all flush reads through `dataSnapshot`, keeping `Data()` for read-only-while-pinned use.

### 1.2 [needs-confirmation] Lock-ordering in txn rollback / lock release — `pkg/txn/manager.go:~257-262, 752-778`
`rollbackLocked` unlocks `t.mu`, calls `ReleaseLock` (takes `lockMu`), then re-locks `t.mu`; other paths take `lockMu` then read txn state. No single documented lock-ordering invariant → multi-party deadlock window.
**Fix:** establish and document a strict order (always `lockMu` before `t.mu`); add a `releaseAllLocksUnderLock` helper that frees all of a txn's locks under one `lockMu` acquisition.

### 1.3 [needs-confirmation] Background flush errors ignored — `pkg/storage/buffer_pool.go:~553`
`_ = bp.FlushPage(page)` in the background flusher: a full disk silently marks pages clean.
**Fix:** log + count failures; consider halting the flusher on persistent I/O error.

### 1.4 [needs-confirmation] Server/protocol panic recovery lacks stack traces — `pkg/server/server.go:297-305`, `pkg/protocol/mysql.go:~336,377`
Connection-handler panics are recovered but logged without `debug.Stack()`; auth-error send failures are dropped (`_ = sendErr`) so a half-closed socket can hang the next read.
**Fix:** log full stack; return immediately on send failure.

### 1.5 [verified] Permissive parser swallows token errors — `pkg/query/parser_dml_select.go` (`parseJoinType`)
`_, _ = p.expect(TokenJoin)` in ~6 places: malformed JOIN syntax silently mis-parses. Acceptable in permissive mode, but `StrictSQLParsing` should reject it.
**Fix:** thread a strict flag (or return an error) so strict mode rejects what permissive mode tolerates.

### 1.6 [lead — NEEDS investigation] Buffered UPDATE doesn't reject in-txn UNIQUE duplicate — `pkg/catalog` buffered write path
A buffered (in-transaction) `UPDATE` setting a `UNIQUE` column to a value already held by another row *in the same transaction* was **not** rejected at statement time (observed while writing `buffered_constraints_test.go`). Possibly uniqueness is only enforced at commit, or not at all for in-txn duplicates.
**Action:** confirm intended semantics; add enforcement + a test once decided. (The existing test deliberately does not lock in the current behavior.)

### 1.7 [policy] Audit write durability — `pkg/audit/logger.go`
Done: failures are logged, counted (`FailedWriteCount()`), and the silent `file == nil` drop is closed. **Still open:** (a) **retry** on transient I/O errors; (b) optional **fail-secure** mode where a failed audit write aborts the audited operation — a product decision (availability vs. guaranteed auditability).

---

## 2. `pkg/catalog` — God Functions & Duplication

> The hot **write** paths below carry data-corruption risk; decompose as a dedicated, reviewed pass, leaning on the now-stronger buffered-constraint tests. Suggested order: (a) extract `decodeVisibleRow` and migrate read paths under test; (b) extract `validateRowAgainstConstraints` (shared by insert/update); (c) split `insertLocked`.

**High priority**
- **`insertLocked` ~479 lines** (`catalog_insert.go:~1007-1485`) — split into `validateInsert`, `buildRowWithConstraints`, `recordInsertUndo`, `applyIndexUpdatesForInsert`.
- **`updateLocked` ~269 lines** (`catalog_update.go:~582-851`) — split into `resolveUpdateTargetRows`, `validateUpdateConstraints`, `applyUpdateIndexes`.
- **Row decode + visibility check duplicated 30+ times** — `decodeVersionedRow` → `isVisibleAt` → `vrow.Data` across `catalog_core.go`, `catalog_insert.go`, `catalog_update.go`, `catalog_delete.go`. Extract `decodeVisibleRow(valueData, columns, queryTime) (row, ok, err)`.
- **Expression dispatch giant switch** — `catalog_eval.go` `evaluate` (~51-208) + `evaluateFunctionCall` (~395-558). Per-function helpers (`evalUpper`, …) exist; wire them through a `map[string]funcHandler` dispatch table.
- **Lock release/reacquire in `selectLockedInternal`** (`catalog_core.go:~594-845`) drops and re-takes the read lock mid-function (non-reentrant mutex → fragile). Split into a lock-holding outer entry + a lock-free `selectUnlocked`; simplify `canReleaseLock`.

**Medium priority**
- Three near-identical scan branches (index / MV / B-tree) in `scanTableRows` (`catalog_core.go:~852-1125`) — extract `filterAndProjectRow`.
- Constraint-checking loops (UNIQUE/FK/CHECK) duplicated across insert and update — extract `validateRowAgainstConstraints`.
- `fmt.Errorf("...: %v", err)` vs `%w` — standardize on `%w` for error-chain support.

---

## 3. `pkg/query`, `pkg/optimizer`, `pkg/advisor`

**High priority**
- **Column-extraction duplicated & inconsistent** — optimizer (`optimizer.go:~282-294`, 3 expr types) vs advisor (`advisor.go:~334-408`, 13). Both miss columns inside `FunctionCall`, `CaseExpr`, subqueries. Extract one shared `ExtractColumnsFromExpr` in `pkg/query`.
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
- **`Options` has 50 fields** (`engine/database.go`) across ~12 subsystems — group into nested option structs (`ReplicationOptions`, `BackupOptions`, …).
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
- **Three overlapping cache layers** — `pkg/cache` (result, SHA256-keyed, table-dep), `pkg/catalog/catalog_cache.go` (result, count-limited), `pkg/engine/query_plan_cache.go` (parsed-plan). All re-implement LRU + RWMutex + hit/miss counters with **inconsistent invalidation**. Consolidate onto one cache core with pluggable key/value + a single invalidation signal.

**Medium priority**
- `pkg/scheduler` hard-codes a 10-min per-job context timeout (`scheduler.go:~296`) — make it a per-`Job` field.
- `pkg/metrics` alert cooldown suppresses by elapsed time rather than firing on state change (recovery/re-trigger alerts can be missed); no shared/global AlertManager → subsystems may double-register rules.
- *(If WASM is ever un-gated)* `wasm/host_functions.go` (2,656 LOC) split by domain; `wasm/runtime.go` opcode dispatch → real `switch`. Otherwise consider fully deleting `pkg/wasm` and dropping its README/FEATURES claims.

**Low priority**
- `pkg/pool` `Config.Validate()` doesn't reject non-positive `MaxIdleTime`/`MaxLifetime`/`HealthCheckInterval`; defer-unlock in `Acquire` would harden it.
- `pkg/fdw` CSV wrapper assumes UTF-8 (no charset option) and doesn't push WHERE predicates into the cursor loop despite the `ScanOptions` plumbing.
- `pkg/cache.estimateSize()` is coarse — can let the cache exceed `MaxSize`.
- `sdk/go` lacks documented thread-safety guarantees on the returned `driver.Conn`.
- `pkg/logger.IsEnabled` is unused — adopt before expensive debug formatting, or drop.

---

## 6. Test Suite (after the quarantine)

- **Incremental thin-out:** replace the brittle `coverage_padding` tests package-by-package with focused table-driven tests that lift the *lean* number toward 85%+, then delete each padding file once its unique coverage is reclaimed.
- **Coverage floor:** set a per-package floor and gate CI on the *lean* number, so coverage reflects focused tests, not raw lines.
- **Test-tree split:** three trees (`pkg/`, `integration/`, `test/`) with some duplicated cases (e.g. `TestUpdateWithSubquery` in both `pkg/catalog` and `test/`) — document the intended split (unit vs cross-package vs e2e/bench).

---

## 7. Tooling, Build & Docs

- **Widen linting:** `.golangci.yml` enables ~5 linters and the Makefile's `lint`/`gosec` only run over `SECURITY_PKGS`. Run `errcheck` + the linter set across **all** packages (the gofmt gate is already wired).
- **`AGENTS.md` duplicates `CLAUDE.md`** and is stale — consolidate to one source (or generate one from the other).
- Root binaries `cobaltdb-server`/`cobaltdb-cli` are already gitignored (not tracked); `data/` and `pkg/engine/backups/` are gitignored test artifacts — leave as-is.

---

## 8. Prioritized Remaining Roadmap

**P0 — correctness (confirm-then-fix)**
1. Lock-ordering invariant in txn rollback/lock release (§1.2).
2. Background flush error handling (§1.3); server/protocol panic stack traces + auth-send failure (§1.4).
3. Buffered UPDATE in-txn UNIQUE enforcement — investigate then decide (§1.6).
4. `CachedPage.Data()` pin-protocol audit / mutation routing (§1.1) — design.

**P1 — maintainability**
5. Decompose `insertLocked`/`updateLocked`; extract `decodeVisibleRow` + `validateRowAgainstConstraints` (§2) — dedicated reviewed pass.
6. Unify the three cache layers (§5) and the duplicated column-extraction logic (§3).
7. Extract shared `runStatement` (Exec/Query) and `initializeCommonComponents` (create/load) (§4).
8. Incremental test thin-out + lean-coverage gate (§6); widen linting (§7).

**P2 — structure & polish**
9. Expression visitor + precedence-parser dedup; AST consistency (§3).
10. Group the 50-field `Options` struct (§4); harden or scope webui (§4).
11. Audit/strict-parser hard errors (§1.5); audit retry/fail-secure decision (§1.7).
12. Scheduler per-job timeout, metrics alert-on-change, fdw pushdown/charset, pool validation, cache size accounting (§5); consolidate `AGENTS.md`/`CLAUDE.md` (§7).
