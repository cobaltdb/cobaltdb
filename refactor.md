# CobaltDB Refactoring & Improvement Report — Remaining Work

**Updated:** 2026-06-13
**Scope:** Full-repository review of `github.com/cobaltdb/cobaltdb`. This document now tracks only the **open** items. Completed work has been merged to `main` and removed from here.

Tags: **[verified]** = read the code and confirmed · **[needs-confirmation]** = static-review lead, confirm before fixing · **[policy]** = needs a product decision, not a mechanical fix.

> **Already done (merged to `main`, branch `refactor/p0-fixes`):** btree LRU double-`Remove` fix · parallel worker-panic isolation (`executor.go`) · deadlock-detector cycle fix (`findWaitCycle`) · dead `WorkerPool` removed · gofmt gate (Make + CI) + whole-tree format · `pkg/wasm` isolated behind `wasm_experimental` · 207 coverage-padding files (~102K LOC) quarantined behind `coverage_padding` (lean 78.4% / full 85.0%) · stray `.wrongstack/` + stale fixtures removed · audit `FailedWriteCount()` + silent-drop fix · `parser.go` split into 4 files · buffered/MVCC constraint-snapshot test coverage raised · **`AGENTS.md`** deleted, **`CLAUDE.md`** corrected (SECURITY_PKGS stale, linting runs globally) · `rollbackLocked` → `releaseAllLocksUnderLock` (lock-ordering) · `flushDirtyPages` error logging + haltable flusher + `FlushErrorCount` metric · panic handlers get `debug.Stack()` in server + protocol · `strictExpect` already correct (confirmed with test).
>
> **Phase 2 (merged to `main`, 2026-06-13, 8 commits, ~26K LOC across 198 files):**
> - `chore(catalog): drop 6 coverage_padding anti-pattern test files` — 6 of the quarantined `coverage_*_test.go` files were meta-coverage with no real signal; dropped 3,766 LOC of low-value tests.
> - `test: add focused tests for select bounds, FK defs, CTE resources, storage meta, btree integrity` — `validateSelectBounds` extraction, FK definition validation, CTE recursion depth, storage `MetaPage` checksum, `DiskBTree` corrupt-page rejection, REST API body-size cap.
> - `refactor(catalog): FK enforcer, write-path hardening, DDL expansion, visitor` — extracted `ForeignKeyEnforcer` (cascade action machinery, pending-action tracking, rollback); REPLACE tracks deleted index entries and restores on failure; UPDATE applies self-referential FK cascades and propagates RLS WITH CHECK; `catalog_ddl.go` adds `ALTER TABLE ADD FOREIGN KEY/CHECK`, `DROP CONSTRAINT`, `CREATE/DROP COLLECTION`; `checkColumnRefVisitor` walks CHECK expressions; path-traversal defenses in `catalog_paths.go`.
> - `feat(query): extend parser/AST for FK, CHECK, COLLECTION, CALL, locking, named args` — `UniqueConstraintDef`, `CheckConstraintDef`, `DropCollectionStmt`, `CallArg`, `SelectLockingClause` AST nodes; FK action parsing; view column aliases; named CALL arguments.
> - `refactor(storage): page header validation, WAL hardening, disk sync` — `validPageType`, `validatePageHeader`, `metaPageChecksum`; WAL rejects symlinks, non-monotonic LSNs, non-regular files, nil inputs; recovery buffer tracker; group-commit / checkpoint / sync interaction tests.
> - `refactor(server): connection lifecycle, MySQL hardening, rate limit, TLS, admin` — connection lifecycle hooks; production health/live/ready endpoints with configured-logger plumbing; MySQL wire protocol type encoding, NULL/zero-column robustness, sequence numbers; rate limiter + SQL protection.
> - `refactor: engine DDL surface, RLS hardening, backup/replication safety, auth flow` — engine DDL round-trip (TableSchema, FTSIndexDDL, VectorIndexDDL, etc.); RLS policy validation + error-returning operators; backup payload reader, staged WAL, restore path validation; replication state-file atomicity + symlink rejection + auth token constant-time compare; auth flow improvements.
> - `feat: CLI dump FKs, server admin auth, webui saved queries, docs updates` — CLI SQL dump emits FK ALTER statements in dependency order; `cliOutputFile` (staged-write) replaces the un-staged `createSecureOutputFile`; server admin-credentials validation; webui JSON body size cap, query validation, saved-query import validation; docs updated for FK actions, collections, named args, saved queries, RLS WITH CHECK.
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

### 1.9 [verified — FIXED] DELETE/UPDATE of a parent ignored same-txn pending children (dangling FK) — `pkg/catalog/foreign_key.go`
Same class as §1.8. `findReferencingRows` (foreign_key.go:262) located referencing child rows by scanning only the committed index/B-tree, never the current txn's pending writes. Confirmed via probe that, in one buffered txn, `INSERT` a child then `DELETE` its parent committed both — leaving a child row pointing at a deleted parent (dangling FK). The reverse false-positive was also possible (a committed child soft-deleted in-txn would wrongly block the parent delete). Fix: `findReferencingRows` now overlays `getCurrentTxn().getPendingWriteMap()[table]` (read-your-writes) — counts pending child inserts, skips pending-deleted/overridden rows, and disables the single-column index fast-path when pending writes exist. Covers all OnDelete/OnUpdate call sites (catalog_delete.go:515/617/767, catalog_update.go:1330). Regression tests `TestBufferedDeleteParentWithPendingChildRejected` and `TestBufferedDeleteParentAfterChildDeletedAllowed`. — fixed 2026-06-02.

### 1.10 [verified — FIXED] DELETE + re-INSERT of the same PK in one txn was rejected — `pkg/catalog/catalog_insert.go`, `pkg/catalog/catalog_txn.go`
Same class as §1.8/§1.9. In a buffered txn, `DELETE FROM t WHERE id=1` then `INSERT … VALUES (1, …)` failed with "duplicate primary key value", and committed an *empty* table (delete applied, re-insert rejected). Cause: the PK-conflict guards read committed state only — `resolvePKConflict` scanned the committed B-tree (where the row is still live because the delete is merely buffered), and `keyInPendingWrites` treated *any* pending entry as a conflict, including the pending soft-delete. Fix: `resolvePKConflict` now treats a key with a pending soft-delete (`DeletedAt>0`) as free, and `keyInPendingWrites` returns false for pending soft-deletes. Genuine in-txn duplicates (two live inserts of one PK) and committed duplicates are still rejected. Regression tests `TestBufferedDeleteThenReinsertSamePK`, `TestBufferedDoubleInsertSamePKRejected`. — fixed 2026-06-02.

### 1.11 [verified — FIXED] Pending-delete not honored by UNIQUE and FK-parent insert checks — `pkg/catalog/catalog_insert.go`, `pkg/catalog/foreign_key.go`
Two more instances of the read-your-writes class (cf. §1.8–§1.10), both confirmed by probe in a buffered txn:
- **UNIQUE freed by delete:** `DELETE` a row holding unique value `'A'`, then `INSERT` a new row with `'A'` — was rejected ("UNIQUE constraint failed") and committed an empty table. `checkUniqueConstraintsSnapshot` scanned the committed tree (row still live, delete only buffered) without skipping keys superseded by a pending write. Fix: hoist the pending-write map and skip pending-overridden keys in the committed scan, so a pending soft-delete frees the value.
- **FK parent deleted in-txn:** `DELETE` a parent then `INSERT` a child referencing it — both committed, leaving a dangling FK. `checkForeignKeyConstraintsSnapshot`'s committed parent scan neither skipped pending-deleted parents nor committed soft-deleted rows. Fix: skip parent keys overridden by a pending write (the pending loop already handles inserts/skips deletes) and skip committed `DeletedAt>0` rows.

Regression tests `TestBufferedUniqueValueFreedByDelete`, `TestBufferedInsertChildAfterParentDeletedRejected`. — fixed 2026-06-02.

> **Read-your-writes sweep (2026-06-02):** the buffered/MVCC write path had a systematic "constraint check scans committed state only" weakness. Found and fixed across UPDATE-UNIQUE (§1.8), FK-children on delete/update (§1.9), PK delete+reinsert (§1.10), and UNIQUE/FK-parent vs pending-delete (§1.11). Verified safe: INSERT multi-row UNIQUE/NOT NULL, COUNT(*), indexed SELECT, and auto-increment all already overlay pending writes correctly.

### 1.12 [verified — FIXED] UNIQUE-index DELETE + re-INSERT in one txn — statement check and commit ordering — `pkg/catalog/catalog_insert.go`, `pkg/catalog/catalog_txn.go`
Probing the §1.11 fix against explicit `CREATE UNIQUE INDEX` (single- and multi-column) surfaced two coupled defects:
1. **Statement-time:** `buildBufferedInsertIndexes`/`…Snapshot` rejected a re-insert of an index value freed by a pending delete, because they read the committed index tree (`idx.tree.Get`) where the entry is still present (the index delete is only buffered). Fixed by replacing the committed-Get + `indexKeyInPendingWrites` pair with a single net-state check (`indexKeyPendingState`): a committed slot counts only if not freed by a pending delete, and a pending insert always counts.
2. **Commit-time (latent, surfaced by fix 1):** `CommitTransaction` collected buffered index updates into separate `idxPuts`/`idxDels` batches and applied **all puts then all dels**. A delete+re-insert of the same unique key (del + put) therefore applied the put then the del, wiping the entry — the row existed with no index slot, so later duplicates went undetected and indexed lookups missed it. Fixed by collapsing per-key ops to their net effect (last op in `pendingWrites` order wins, via `pendingIdxOp`) so a key never lands in both batches.

Regression test `TestBufferedUniqueIndexValueFreedByDelete` (covers single/multi-column reuse, the post-commit index integrity via a still-rejected duplicate, and the still-rejected live in-txn duplicate). — fixed 2026-06-02.

### 1.13 [verified — FIXED] JSON function value/composition bugs — `pkg/catalog/catalog_eval_json.go`
Probing the JSON builtins found three real bugs, all from the dispatcher coercing arguments through `jsonArgString` (string-only): (1) `JSON_SET(doc,'$.k',31)` dropped non-string scalars to `""` (the number/bool was lost); (2) `JSON_ARRAY_LENGTH(JSON_EXTRACT(...))` returned 0 because `JSON_EXTRACT` returns a native Go slice that the string-only coercion couldn't see; (3) `JSON_TYPE(JSON_EXTRACT(...))` returned "unknown" for the same reason. Fixes: `jsonSetValueArg` renders numbers/bools/composites to JSON text for `JSON_SET`; `jsonDocArg` re-serializes a native (non-string) nested value to JSON text for `JSON_ARRAY_LENGTH`/`JSON_TYPE` while leaving string documents untouched (no double-encoding). Regression tests `TestJSONSetPreservesScalarTypes`, `TestJSONComposedExtract`. (Not changed: `JSON_UNQUOTE` of an already-unquoted scalar still errors — its erroring contract is covered by existing tests and was left intact rather than overreach into changing tested semantics.) — fixed 2026-06-02.

### 1.14 [verified — FIXED] Vector functions rejected string/JSON-array literals — `pkg/catalog/catalog_eval.go`
`toVector` had a `// For now, return error` stub for the `string` case, so `COSINE_SIMILARITY('[1,0]','[1,0]')`, `L2_DISTANCE`, and `INNER_PRODUCT` errored with "cannot convert string to vector" — the common literal form and string-typed column values were unusable; only already-parsed `[]interface{}` worked. Added `parseVectorString` (JSON-array → `[]float64`) and a `StringBox` case, so vector functions accept `'[1,2,3]'` literals and string columns. Verified COSINE/L2/INNER against known values (identical=1, orthogonal=0, L2([0,0],[3,4])=5, dot([1,2,3],[4,5,6])=32) in `TestVectorFunctionsAcceptStringLiterals`. (Full-text `MATCH … AGAINST` was probed at the same time and is correct.) — fixed 2026-06-02.

### 1.15 [verified — FIXED] `catalog.ExecuteQuery` dropped INSERT … RETURNING rows — `pkg/catalog/stats.go`
The `*query.InsertStmt` case returned `&QueryResult{}` and discarded the RETURNING rows, while the `UpdateStmt`/`DeleteStmt` cases correctly returned `GetLastReturning{Columns,Rows}`. So `INSERT … RETURNING` via `catalog.ExecuteQuery` (the public catalog API, used by tests and the SQL-persistence/replication dispatch) yielded no rows even though `insertLocked` had populated them. The engine's own `db.Query` path (`executeInsertReturning`) was already correct, so this was confined to the catalog API. Fixed to mirror UPDATE/DELETE; a plain INSERT still returns no rows because `insertLocked` calls `setLastReturning(nil,…)` unconditionally. Regression test `TestExecuteQueryInsertReturning` (incl. RETURNING-with-expression and the no-stale-rows plain-INSERT case). Note: `CREATE TRIGGER` is not handled by `catalog.ExecuteQuery`'s dispatch (returns "unsupported query type") — triggers are created through the engine/DDL path, not this convenience method. — fixed 2026-06-02.

### 1.7 [policy] Audit write durability — `pkg/audit/logger.go`
Done: failures are logged, counted (`FailedWriteCount()`), and the silent `file == nil` drop is closed. **Still open:** (a) **retry** on transient I/O errors; (b) optional **fail-secure** mode where a failed audit write aborts the audited operation — a product decision (availability vs. guaranteed auditability).

---

## 2. `pkg/catalog` — God Functions & Duplication

> The hot **write** paths below carry data-corruption risk; decompose as a dedicated, reviewed pass, leaning on the now-stronger buffered-constraint tests. Suggested order: (a) extract `decodeVisibleRow` and migrate read paths under test; (b) extract `validateRowAgainstConstraints` (shared by insert/update); (c) split `insertLocked`.

**High priority**
- **`insertLocked` ~479 lines** (`catalog_insert.go:~1007-1485`) — split into `prepareInsertRow` (pure: PK-gen + row-build + validation + encoding), `applyRowIndexes` (hot: B-tree + index undo), `recordInsertUndo` (hot: undo log), `finalizeInsert` (side-effects: RETURNING + triggers + cache invalidation). Extraction plan from 2026-05-29 review. — **PARTIAL (2026-06-13):** index-entry rollback on REPLACE failure (`deleteIndexEntryForRowTracked` / `restoreDeletedIndexEntries` / `deletedIndexEntry`) extracted; the four-method split is still on the table for a future dedicated pass.
- **`updateLocked` ~269 lines** (`catalog_update.go:~582-851`) — split into `resolveUpdateTargetRows`, `validateUpdateConstraints`, `applyUpdateIndexes`. — **PARTIAL (2026-06-13):** `applySelfReferentialUpdateCascades` + `selfUpdatedRowSatisfiesForeignKey` extracted; RLS WITH CHECK now evaluated before index writes; `bufferUpdateEntries` carries `ctx`. The three-method split is still on the table.
- **Row decode + visibility check duplicated 30+ times** — `decodeVersionedRow` → `isVisibleAt` → `vrow.Data` across `catalog_core.go`, `catalog_insert.go`, `catalog_update.go`, `catalog_delete.go`. Extract `decodeVisibleRow(valueData, columns, queryTime) (row, ok, err)`. — **OPEN.**
- **Expression dispatch giant switch** — `catalog_eval.go` `evaluate` (~51-208) + `evaluateFunctionCall` (~395-558). Per-function helpers (`evalUpper`, …) exist; wire them through a `map[string]funcHandler` dispatch table. — **DONE (2026-05-30): switch replaced with `scalarFunctionHandlers` map dispatch; GROUP_CONCAT retained inline.**
- **Lock release/reacquire in `selectLockedInternal`** (`catalog_core.go:~594-845`) drops and re-takes the read lock mid-function (non-reentrant mutex → fragile). Split into a lock-holding outer entry + a lock-free `selectUnlocked`; simplify `canReleaseLock`.

**Medium priority**
- Three near-identical scan branches (index / MV / B-tree) in `scanTableRows` (`catalog_core.go:~852-1125`) — extract `filterAndProjectRow`. — **DONE (2026-05-30): `filterAndProjectRow` extracted and used in index and general B-tree sequential branches; fast B-tree path intentionally keeps its own optimized `decodeVersionedRowFastEx` buffer-reuse path.**
- Constraint-checking loops (UNIQUE/FK/CHECK) duplicated across insert and update — extract `validateRowAgainstConstraints`. — **PARTIAL (2026-06-13):** `checkRowConstraints` extracted in insert path; `validateForeignKeyDefLocked` and `validateCheckConstraintsLocked` extracted for DDL. The shared insert/update check helper is still on the table.
- `fmt.Errorf("...: %v", err)` vs `%w` — **6 vector-function errors in `catalog_eval.go` + 5 DSN-parse errors in `sdk/go/cobaltdb.go` fixed (2026-05-30); remaining occurrences are in test files or for non-error values.**

---

## 3. `pkg/query`, `pkg/optimizer`, `pkg/advisor`

**High priority**
- **Column-extraction bug [FIXED 2026-05-29]** — optimizer `extractColumnReferences` expanded from 3 to 13 expr types (all types in `advisor.go:340-398`). FunctionCall, CaseExpr, InExpr/ExistsExpr-with-subquery, UnaryExpr, LikeExpr, IsNullExpr, BetweenExpr, CastExpr now included. `SelectBestIndex` will no longer miss index candidates due to dropped columns. — fixed 2026-05-29.
- **No expression visitor** — ≥3 independent AST type-switches (parser, optimizer, advisor) with different omitted cases. Add `ExpressionVisitor` / `Walk(expr, visitor)` to centralize traversal. — **PARTIAL (2026-06-13):** `checkColumnRefVisitor` (DDL CHECK expression walker) added in `pkg/catalog/catalog_ddl.go`; covers all 23 expression node types. A single shared `Walk` / `Visitor` interface across parser/optimizer/advisor is still on the table.

**Medium priority**
- **Precedence-parser boilerplate** — `parseOr/parseAnd/parseAdditive/parseMultiplicative` (now in `parser_expression.go`) are six copies of the same loop. Replace with one generic `parseBinaryOpLevel(next, ops...)`. — **DONE (2026-05-29).**
- AST inconsistencies (`WindowExpr` duplicates `FunctionCall`; no shared interface for `SelectStmt`/`UnionStmt`); centralize a `canBeIdentifier(TokenType)` for reserved-word/identifier handling. — **PARTIAL (2026-06-13):** new AST nodes (`UniqueConstraintDef`, `CheckConstraintDef`, `DropCollectionStmt`, `CallArg`, `SelectLockingClause`) are in place; the shared interface unification is still on the table.
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
- **`createNew`/`loadExisting` duplicate ~100+ lines** of component init (`database_lifecycle.go:~330-471` vs `~496-673`) — extract `initializeCommonComponents()`. — **PARTIAL (2026-06-13):** `runStatement` now also returns `context.Context` (the RLS-aware context) so the wider RLS user flows into the catalog; the explicit `initializeCommonComponents` extraction is still on the table.
- **webui security** (`webui/server.go`) — `--insecure-no-auth`, startup-printed token with no expiry/rotation, arbitrary SQL with no per-token RBAC/rate-limit/audit. Add expiry/rotation, query audit, rate limiting, optional table allow-listing — or confirm webui isn't for production. — **PARTIAL (2026-06-13):** `validateWebUIQuery` rejects obvious injection shapes; `decodeJSONRequest` / `decodeSingleJSON` centralize JSON body parsing with size limits; `decodeSavedQueriesImport` + `validateSavedQuery` gate the saved-query import; `setAPIToken` / `secureTokenCompare` for token rotation. Token expiry/rotation UI, per-token RBAC, rate limiting, and audit are still on the table.

**Medium priority**
- **`catalog.ExecuteQuery` is an incomplete convenience dispatch** (`stats.go`) — it does not handle `*query.UnionStmt` (UNION/INTERSECT/EXCEPT → "unsupported query type") or `CreateTriggerStmt`/some DDL. The engine's `db.Query`/`db.Exec` path is complete (`executeUnion`, trigger DDL), so this is not a user-facing gap; it only affects direct catalog-API callers (tests, SQL-persistence/replication replay, which never see set operations). Could wire `executeCTEUnion` in if the convenience API is meant to be complete. — observed 2026-06-02.
- **Scalar subquery returning >1 row** is treated as no-match (returns empty) rather than erroring per SQL (`Subquery returns more than 1 row`). Soft deviation, not corruption; tightening it risks changing existing query behavior. — observed 2026-06-02.
- **MySQL param-counting has two implementations** (`mysql.go:~1265-1307`) that must stay in sync — unify to tokenizer-primary, fallback only on tokenizer error. — **FIXED (2026-05-30): `countQuestionMarksOutsideQuotes` inlined as a labeled fallback inside `countPreparedParams`. One function now; no duplication risk.**
- `cobaltdb-cli/main.go` (1,375 LOC) dense subcommand `switch` — `Command` interface + registry. Confirm `importCSV` callers check returned errors. — **importCSV caller check DONE (2026-05-30): `runImportCommand` correctly handles error (stderr + exit 1). Switch→interface refactor remaining; 2026-06-13 split out `openCLIImportCSVFile` / `validateCSVRecord` / `writeDumpInsert` so the validation and IO boundaries are testable.**
- **Verify** `circuit_breaker.go`/`retry.go` are actually wired into the `Exec`/`Query` path and document the policy (they exist; wiring unconfirmed). — **CONFIRMED NOT WIRED (2026-05-30):** `CircuitBreakerManager` and `RetryConfig` exist in `ProductionConfig` but are not present in the `DB` struct and are not called from `Exec`/`Query`/`runStatement`. They are standalone utilities. Wiring would require adding them to `DB` struct and wrapping statement execution. Product decision needed: which operations should be wrapped.

- **Prepared-statement result metadata** (`mysql.go` `handleStmtPrepare`/`sendBinaryResultSetFromRows`):
  - **[FIXED 2026-06-02]** COM_STMT_PREPARE emitted placeholder `col0`/`col1` result column names; now reuses the real `rows.Columns()` names so prepare-time metadata (e.g. JDBC `getMetaData()`) matches the execute response.
  - **[FIXED 2026-06-02]** Binary execute and prepare result column definitions now declare real numeric types (TINY/SHORT/LONG/LONGLONG/FLOAT/DOUBLE) from `ColumnTypeHints`, and `buildBinaryRowPacket` encodes those in native little-endian/IEEE-754 form via `appendBinaryValue`. Type and encoding are kept in lock-step by `buildBinaryColumnDefinitions`. Strings/blobs/decimals/JSON remain length-encoded (their correct binary form); the packed temporal types (DATE/DATETIME/TIME/TIMESTAMP) are deliberately mapped to `VarString` and length-encoded as strings (the packed temporal binary form is not emitted — a documented simplification, not a mismatch). Verified end-to-end through go-sql-driver by `TestMySQLBinaryProtocolTypeRoundTrip` (incl. a >int32 BIGINT proving 8-byte encoding, DOUBLE, TEXT, and NULL).

**Low priority / hygiene**
- ~~`cmd/debug`, `cmd/realworld-test`~~ removed 2026-06-02 — they were hardcoded dev scratch scripts (e.g. `cmd/debug` ran a fixed `./test.cobalt` CRUD print and had a committed empty `test_debug.cobalt.wal`), never built/installed/released by the Makefile. `cmd/demo` kept (it has a README and tests; treat as a deliberate example). **Not removed: `pkg/btree` `DiskBTree` + `pkg/storage` `PageManager`** — although unused in production, they are a coherent, heavily-tested (~1,176 LOC impl + ~3,000 LOC tests) but unwired subsystem (disk-resident B-tree), i.e. likely intentional future scaffolding rather than junk; removing them is a deliberate "drop an unwired subsystem" decision (and needs surgical edits across mixed btree/storage test files), not routine dead-code cleanup.
- Connection limiter uses a growable slice of waiter channels — `golang.org/x/sync/semaphore` or `sync.Cond` fits better.
- `Close()` has no shutdown timeout — consider `CloseWithTimeout(ctx)`.

---

## 5. Peripheral packages

**High priority**
- **Two query-result caches, one unused** — `pkg/catalog/catalog_cache.go` (old `QueryCache`, now superseded by `cache.Cache`) vs `pkg/cache/query_cache.go` (now the canonical cache). Catalog now uses `*cache.Cache` exclusively via `catalog.EnableQueryCache()` / `catalog.GetQueryCache()`; `catalog_cache.go` helpers (`isCacheableQuery`, `extractTablesFromQuery`, `queryToSQL`, `generateQueryKey`) are deprecated wrappers delegating to `pkg/query`. The old `QueryCache` struct is deprecated. Cannot delete helpers until test files (`z_eval_test.go`, `z_catalog_coverage_test.go`, etc.) are updated to import `pkg/query` directly — currently they don't import it. — addressed 2026-05-29; follow-up needed. — **PARTIAL (2026-06-13):** `pkg/cache/query_cache.go` is now the canonical cache; `pkg/cache/query_cache_test.go` extended; `pkg/catalog/catalog_cache_test.go` slimmed. The deletion of the deprecated helpers is still on the table.

**Medium priority**
- **`pkg/scheduler` per-job timeout [FIXED 2026-05-29]** — `Job.Timeout` field added; scheduler uses `j.Timeout` if >0, else 10-min default. — fixed 2026-05-29.
- `pkg/metrics` alert cooldown suppresses by elapsed time rather than firing on state change (recovery/re-trigger alerts can be missed); no shared/global AlertManager → subsystems may double-register rules. — **PARTIAL (2026-06-13):** alerting.go and slow_query.go extended with new tests; the cooldown/AlertManager follow-up is still on the table.
- *(If WASM is ever un-gated)* `wasm/host_functions.go` (2,656 LOC) split by domain; `wasm/runtime.go` opcode dispatch → real `switch`. Otherwise consider fully deleting `pkg/wasm` and dropping its README/FEATURES claims. — **PARTIAL (2026-06-13):** `pkg/wasm/compiler.go` and `pkg/wasm/integration_test.go` extended; still build-tagged `wasm_experimental`.

**Low priority**
- **`pkg/pool` Config.Validate() [FIXED 2026-05-29]** — now checks non-positive MaxIdleTime/MaxLifetime/HealthCheckInterval/HealthCheckTimeout/AcquireTimeout. — fixed 2026-05-29.
- `pkg/fdw` CSV wrapper assumes UTF-8 (no charset option) and doesn't push WHERE predicates into the cursor loop despite the `ScanOptions` plumbing. — **PARTIAL (2026-06-13):** `pkg/fdw/csv.go` and `pkg/fdw/fdw_test.go` extended; charset option still on the table.
- `pkg/cache.estimateSize()` is coarse — can let the cache exceed `MaxSize`. — **PARTIAL (2026-06-13):** test surface extended; `estimateSize` itself still on the table.
- `sdk/go` lacks documented thread-safety guarantees on the returned `driver.Conn`. — **PARTIAL (2026-06-13):** DSN parse errors use `%w`; thread-safety docs still on the table.
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
3. Decompose `insertLocked`/`updateLocked` — dedicated reviewed pass (§2) [PARTIAL 2026-06-13: index-entry rollback on REPLACE, self-referential FK cascades, RLS WITH CHECK propagation extracted; 2026-06-13 also extracted `prepareInsertRow` from `insertLocked` (478→403 lines, -16%), and then `finalizeInsert` (403→373 lines, -22% cumulative). The function is now a 3-way structure: apply loop → per-row apply logic (buffered/direct path, triggers, indexes, undo) → `finalizeInsert()`. The remaining inline piece is the apply logic itself, which maps to the planned `applyRowIndexes` + `recordInsertUndo` helpers in the four-method split. They are the hardest to extract because they cross the buffered/direct path boundary; doing them as a separate, well-tested pass is the right next step.].
4. Extract `decodeVisibleRow` + `validateRowAgainstConstraints` (§2) [PARTIAL 2026-06-13: `checkRowConstraints` for insert, `validateForeignKeyDefLocked` / `validateCheckConstraintsLocked` for DDL extracted; 2026-06-13 also added `decodeLiveRow` (non-temporal version of `decodeVisibleRow`) and migrated 22 call sites across 9 files. 2026-06-13 also extracted `validateRowAgainstConstraints` (NOT NULL + CHECK + FK) and `validateRowNonFKConstraints` (NOT NULL + CHECK only) and migrated 3 call sites across insert + update paths. `checkInsertConstraints` marked Deprecated. The lock-free snapshot paths and the FK-enforcer's specialized checks remain on inline patterns because they use pre-captured snapshot refs that the live-table variants don't have access to.].
5. Delete dead `catalog.QueryCache` struct; move helpers (`isCacheableQuery`, `extractTablesFromQuery`, `queryToSQL`, `generateQueryKey`) to a non-cache package [helpers extracted 2026-05-30; QueryCache marked deprecated, retained for tests; `pkg/cache/query_cache.go` is the canonical cache after the 2026-06-13 batch].
6. Extract shared `runStatement` (Exec/Query) + `initializeCommonComponents` (§4) [runStatement FIXED 2026-05-30; 2026-06-13 also returns the RLS-aware context; `initializeCommonComponents` extraction still on the table].
7. Incremental test thin-out + per-package lean-coverage floor in CI (§6) [9 of 207 padding files deleted 2026-06-13 (6 in Phase 2: `coverage_easy_wins`, `coverage_final_boost`, `coverage_final_push`, `coverage_gap_closer`, `coverage_last_mile`, `coverage_remaining_paths`; 3 more in subsequent commit: `z_coverage_offsetlimit`, `z_coverage_values_equal117`, `z_coverage_final`); focused replacements added for select bounds, FK defs, CTE resources, storage meta, btree integrity, REST API body cap. Lean coverage unchanged at 69.1% (catalog) — padding files deleted only where untagged tests already cover the same behavior.].

**P2 — structure & polish**
8. Expression visitor + precedence-parser dedup [P2-8 DONE — Expression.Evaluate + Evaluator interface, 2026-05-29; P2-5 DONE — parseBinaryOpLevel generic, 2026-05-29; 2026-06-13 added `checkColumnRefVisitor` covering all 23 node types in DDL — shared Walk/Visitor interface across parser/optimizer/advisor still on the table] (§3).
9. Group the 50-field `Options` struct (§4) [FIXED 2026-05-29]; harden or scope webui [PARTIAL 2026-06-13: query validation, JSON body size cap, saved-query validation, token rotation; expiry UI, per-token RBAC, rate limit, audit still on the table].
10. Cache size accounting; fdw pushdown/charset; deferred unlock in `Acquire` (§5) [PARTIAL 2026-06-13: FDW and cache test surface extended; the underlying size-accounting and charset options still on the table].
11. Audit retry/fail-secure decision (§1.7).
