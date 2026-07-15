# CobaltDB — Action Plan from Comprehensive Review

**Based on:** COBALTDB_COMPREHENSIVE_REVIEW.md (2026-07-15)  
**Version targeted:** v0.6.0 → v0.7.0  
**Planning date:** 2026-07-15 (last updated: 2026-07-15)  
**Total action items:** 23 (3 critical, 7 high, 8 medium, 5 low)  
**Completed:** 9 items (+1 false positive retracted) — Phase 0 all (5), Phase 1 items 1.2, 1.3, 1.5 (3), Phase 2 items 2.1 partial (database_procedure.go, database_api.go), 2.2 (1). P1.4 (TCP keepalive) retracted as already implemented. 1.5/3 Phase 2 complete.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Workstream Structure](#2-workstream-structure)
3. [Phase 0: Quick Wins (Days 1–3)](#3-phase-0-quick-wins-days-1-3)
4. [Phase 1: Security & Observability (Week 2–3)](#4-phase-1-security--observability-week-2-3)
5. [Phase 2: Code Health (Week 3–5)](#5-phase-2-code-health-week-3-5)
6. [Phase 3: Strategic Refactors (Month 2–3)](#6-phase-3-strategic-refactors-month-2-3)
7. [Phase 4: Ecosystem & Quality Gates (Month 3–4)](#7-phase-4-ecosystem--quality-gates-month-3-4)
8. [Effort Summary Table](#8-effort-summary-table)
9. [Dependency Graph](#9-dependency-graph)
10. [Risk Register](#10-risk-register)
11. [Recommended Sequencing (Gantt-style)](#11-recommended-sequencing-gantt-style)

---

## 1. Executive Summary

The comprehensive review identified **23 actionable findings** across 7 dimensions. The action plan organizes them into **5 sequential phases** grouped by theme, dependency, and risk profile.

### Guiding principles

- **Security first** — dependency updates and auth hardening land earliest (low effort, high impact)
- **Observability before scaling** — structured logging is a prerequisite for debugging the Catalog.mu refactor
- **Infrastructure before quantity** — consolidate test naming conventions before rationalizing test content
- **Surface before depth** — Docker config and docs warnings are trivial to fix and immediately improve first impressions

### Resource envelope (estimated)

| Metric | Estimate |
|--------|----------|
| **Total engineering hours** | ~340–480 h |
| **Calendar duration (parallelizable)** | ~14–16 weeks |
| **Calendar duration (single track)** | ~20–24 weeks |
| **Files touched** | ~200+ across all phases |
| **New files created** | ~40 (split files, new tests, Playwright spec, etc.) |

---

## 2. Workstream Structure

```
                       v0.7.0 Release
                      ┌─────────────┐
                      │  Phase 4    │ ◄── Ecosystem + Quality gates
                      │  (Month 4)  │
                      └──────┬──────┘
                             │
                      ┌──────▼──────┐
                      │  Phase 3    │ ◄── Strategic refactors
                      │  (Month 2–3)│     (Catalog.mu, load shedding)
                      └──────┬──────┘
                             │
                      ┌──────▼──────┐
                      │  Phase 2    │ ◄── Code health
                      │  (Week 3–5) │     (database.go split, tests, duplicated code)
                      └──────┬──────┘
                             │
                      ┌──────▼──────┐
                      │  Phase 1    │ ◄── Security + Observability
                      │  (Week 2–3) │     (logging, CSRF, E2E, unsafe.String)
                      └──────┬──────┘
                             │
                      ┌──────▼──────┐
                      │  Phase 0    │ ◄── Quick wins
                      │  (Days 1–3) │     (x/crypto, docker-compose, config, badges)
                      └─────────────┘
```

Each phase has a **deliverable gate** — no phase starts until its predecessor's gate is green.

---

## 3. Phase 0: Quick Wins (Days 1–3)

**Gate:** All patches merged, CI green on `main`.

Zero-risk, independently testable changes. Each item is a single commit.

### 0.1 — Update `golang.org/x/crypto` (Finding #3)

| Field | Detail |
|-------|--------|
| **Severity** | Critical |
| **Effort** | ~30 minutes |
| **Risk** | None — semver-compatible minor bump |
| **File(s)** | `go.mod`, `go.sum` |
| **Command** | `go get golang.org/x/crypto@latest && go mod tidy` |
| **Verification** | `make test && make vuln` — the vuln check must show zero `golang.org/x/crypto` advisories |
| **Outcome** | Current v0.53.0 → v0.36.x+ (closes CVE backlog in x/crypto) |

### 0.2 — Harden docker-compose auth (Finding #6)

| Field | Detail |
|-------|--------|
| **Severity** | High |
| **Effort** | ~20 minutes |
| **File(s)** | `docker-compose.yml`, `docker-compose.prod.yml` |
| **Action** | Remove `COBALTDB_ALLOW_CLEARTEXT_AUTH=true` from docker-compose.yml. In `docker-compose.override.yml`, add it commented out with a security warning. |
| **Verification** | `docker-compose config` shows no cleartext flag in base compose |
| **Outcome** | Production deployments no longer implicitly allow cleartext MySQL passwords |

### 0.3 — Wire or remove unused config file (Finding #18)

| Field | Detail |
|-------|--------|
| **Severity** | Medium |
| **Effort** | ~1 hour |
| **File(s)** | `config/cobaltdb.conf`, `cmd/cobaltdb-server/main.go` |
| **Action** | **Option A** (recommended): Write a minimal TOML/HCL parser that reads `config/cobaltdb.conf` and sets defaults before flag parsing, with flags overriding file values. **Option B**: Delete the file and note in README that configuration is via flags/env. |
| **Verification** | Server starts and respects both file and flag config |
| **Outcome** | Eliminates confusion — operators either have a working config file or no file to discover |

### 0.4 — Add README badges (Finding #22)

| Field | Detail |
|-------|--------|
| **Severity** | Low |
| **Effort** | ~15 minutes |
| **File(s)** | `README.md` |
| **Action** | Add [Go Report Card](https://goreplaycard.com/) and [FOSSA](https://app.fossa.com/) or [Snyk](https://snyk.io/) license scan badges |
| **Verification** | Visual check that badges render |
| **Outcome** | External evaluators see project health at a glance |

### 0.5 — Warn on in-memory backend outside tests (Finding #20)

| Field | Detail |
|-------|--------|
| **Severity** | Low |
| **Effort** | ~30 minutes |
| **File(s)** | `pkg/engine/database_lifecycle.go` |
| **Action** | Add a `log.Warn` when `InMemory: true` and `os.Args[0]` doesn't contain `".test"`. |
| **Verification** | Running `./cobaltdb-server -memory` emits a warning line |
| **Outcome** | Prevents accidental data-loss-in-a-reboot scenarios during development |

---

## 4. Phase 1: Security & Observability (Week 2–3)

**Gate:** Phase 0 merged. Code review completed for Phase 1 changes.

### 1.1 — Structured JSON logging (Finding #4)

| Field | Detail |
|-------|--------|
| **Severity** | High |
| **Effort** | ~24–40 hours |
| **Risk** | Medium — the custom logger is referenced from ~15 packages. Must maintain backward compat. |
| **Approach** | Add a `Format` option (`text`/`json`) to the logger config. Implement a JSON encoder that writes `{"level":"INFO","time":"...","msg":"..."}`. Keep the existing text format as default. No external dependency needed (use `encoding/json`). |
| **Key files** | `pkg/logger/logger.go` — add JSON output path; ~15 callers stay unchanged |
| **Verification** | `COBALTDB_LOG_FORMAT=json ./cobaltdb-server` produces valid JSON lines on stderr |
| **Outcome** | Production operators can pipe logs into fluentd/Loki/DataDog without custom parsers |

### 1.2 — WebUI CSRF protection (Finding #8)

| Field | Detail |
|-------|--------|
| **Severity** | High |
| **Effort** | ~4–8 hours |
| **Risk** | Low — additive middleware can't break existing flows |
| **Approach** | Two layers: (1) Add `SameSite=Strict` to the auth cookie. (2) Generate a CSRF token on login, return it as `X-CSRF-Token` header, verify on mutating endpoints (`/api/query`, `/api/admin/*`). The Go `html/template` already escapes output. |
| **Key files** | `webui/server.go` (cookie options), `webui/auth.go` (CSRF token gen + middleware) |
| **Verification** | CSRF test: mutating request without token → 403; with token → 200 |
| **Outcome** | Users of the Web UI behind a reverse proxy are protected from cross-origin writes |

### 1.3 — Fuzz `unsafe.String` call sites (Finding #10)

| Field | Detail |
|-------|--------|
| **Severity** | High |
| **Effort** | ~4–6 hours |
| **Risk** | Low — fuzzing finds bugs before they're exploited |
| **Approach** | Add Go fuzz targets (`FuzzXxx` functions) for the 3 call sites in `pkg/catalog/temporal.go` where `unsafe.String` is used in row decoding. Test with corrupt page data, truncated buffers, boundary offsets. |
| **Key files** | `pkg/catalog/temporal.go:catalog_temporal_fuzz_test.go` (new) |
| **Verification** | `go test -fuzz=FuzzTemporalRowDecode ./pkg/catalog/` runs without crashes for 10M iterations |
| **Outcome** | High-confidence that row decoding cannot read arbitrary memory |

### 1.4 — TCP keepalive on server connections (Finding #12)

| Field | Detail |
|-------|--------|
| **Severity** | Medium |
| **Effort** | ~1 hour |
| **Risk** | Low |
| **Approach** | In `pkg/server/server.go`'s `handleConnection`, add `tcpConn.SetKeepAlive(true)` and `tcpConn.SetKeepAlivePeriod(60 * time.Second)`. |
| **Key files** | `pkg/server/server.go` |
| **Verification** | `ss -tpo` shows keepalive enabled on server connections |
| **Outcome** | Zombie connections from crashed clients are cleaned up within ~2 minutes |

### 1.5 — Remove token URL from webui startup output (Finding #13)

| Field | Detail |
|-------|--------|
| **Severity** | Medium |
| **Effort** | ~1 hour |
| **Risk** | Low |
| **Approach** | Replace the startup log line. Instead of printing `http://.../?token=xxx`, write the full URL with token to a well-known file (`/var/run/cobaltdb-webui.token` or `./.webui-token`) with `0600` permissions. Print only the file path. |
| **Key files** | `webui/server.go` |
| **Verification** | Console shows `Token URL written to /var/run/cobaltdb-webui.token`; file has `0600` perms |
| **Outcome** | Token no longer leaks into shell history, log aggregators, or terminal scrollback |

---

## 5. Phase 2: Code Health (Week 3–5)

**Gate:** Phase 1 changes merged. No regressions on `make verify`.

### 2.1 — Split `database.go` (Finding #5)

| Field | Detail |
|-------|--------|
| **Severity** | High |
| **Effort** | ~16–24 hours |
| **Risk** | High — 4671-line file touched by many development branches. Must preserve all public API signatures. |
| **Progress** | **~60% done** (was 4671 lines, now 3244). Extractions completed: |
| | `database_result.go` (523 lines) — Result/Rows/Row/Tx types |
| | `database_schema.go` (468 lines) — Schema introspection (ShowTables, ShowColumns, etc.) |
| | `database_api.go` (462 lines) — Stats, HealthCheck, Backup, Replication, PlanCache, Vector, Optimizer |
| | `database_procedure.go` (438 lines) — Stored procedures (CALL, param substitution, OUT params) |
| | `database_lifecycle.go` (1265 lines) — Already existed (Open, Close, Options, lifecycle) |
| **Remaining** | `database_query.go` — Exec/Query/Prepare, SELECT/UNION/CTE dispatch, statement cache, connection management. DDL and DML functions also remain in database.go. |

### 2.2 — Consolidate duplicated helpers (Findings #15 + #23)

| Field | Detail |
|-------|--------|
| **Severity** | Medium |
| **Effort** | ~4–6 hours |
| **Risk** | Low — mechanical extraction |
| **Approach** | Create `pkg/util/strings.go` with exported `ToUpperFast()` and `ToLowerFast()` functions. Replace 5 duplicate implementations across `pkg/engine/database.go`, `pkg/security/rls.go`, `webui/server.go`, and `sdk/go/cobaltdb.go`. |
| **Verification** | `go build ./...` succeeds. `grep -r 'func toUpperFast\|func toLowerFast' pkg/` returns zero private duplicates |
| **Outcome** | Single canonical implementation; no more `toUpperFast` copy-paste drift |

### 2.3 — Standardize test file naming (Finding #14)

| Field | Detail |
|-------|--------|
| **Severity** | Medium |
| **Effort** | ~8–12 hours |
| **Risk** | Low — file renames only, no content changes |
| **Approach** | Adopt a single naming pattern: `<package>_<feature>_test.go`. Rename ~60+ files across the project:

| Current pattern | New pattern | Count |
|----------------|-------------|-------|
| `z_*_test.go` | Rename by feature (e.g. `z_foreign_key_test.go` → `catalog_foreign_key_test.go`) | ~40 |
| `v[0-9]+*_test.go` | Rename by feature or merge into existing files | ~17 |
| `*_coverage_test.go` | Rename to `*_coverage_test.go` stays but mark as coverage-only in file header | ~20 |

Note: Git mv preserves history. Use `git mv` in a single commit per directory. |
| **Verification** | `go test ./...` passes. `ls pkg/catalog/*_test.go | wc -l` shows the same count (or slightly fewer after merges) |
| **Outcome** | `test` directory is navigable. CI test filtering by feature is possible. |

---

## 6. Phase 3: Strategic Refactors (Month 2–3)

**Gate:** Phase 2 merged. `make bench-gate` shows no regression from structural changes.

### 3.1 — WebUI Playwright E2E tests (Finding #7)

| Field | Detail |
|-------|--------|
| **Severity** | High |
| **Effort** | ~24–40 hours |
| **Risk** | Medium — Web UI has no existing browser tests |
| **Approach** | Add Playwright tests in a new `webui/e2e/` directory:

| Test suite | Scenarios |
|-----------|-----------|
| `auth.spec.ts` | Token login, expired token redirect, admin-only routes |
| `query.spec.ts` | Execute SELECT, INSERT, verify results table |
| `schema.spec.ts` | Browse tables, view columns |
| `saved_queries.spec.ts` | Create, save, export, import saved queries |
| `admin.spec.ts` | Token minting, audit log viewing |
| `export.spec.ts` | CSV and JSON export correctness |

Test configuration: start the webui binary, connect with Playwright, verify DOM state. |
| **Key files** | `webui/e2e/` (new directory), `package.json` (webui, add playwright dep) |
| **Verification** | `npx playwright test` passes headlessly. CI job added. |
| **Outcome** | Web UI regressions are caught in CI before deployment |

### 3.2 — Catalog.mu Phase 1 (SELECT without Catalog.mu) (Finding #1)

| Field | Detail |
|-------|--------|
| **Severity** | Critical |
| **Effort** | ~80–120 hours |
| **Risk** | High — touches the most central mutex in the system |
| **Reference** | `docs/REFACTOR_ROADMAP.md` — Phase 1: SELECT Without Catalog.mu |
| **Approach** | Follow the existing roadmap. The key steps are:

1. **RCU for table metadata** — Replace `map[string]*TableInfo` with a `atomic.Pointer[map[string]*TableInfo]` that is swapped atomically on DDL.
2. **Snapshot-based schema reads** — SELECT paths read the schema pointer once then operate on the snapshot; no lock held across iteration.
3. **Remove `cat.mu.RLock()` from `Select()`** — the primary bottleneck.
4. **Validate with existing benchmarks** — the refactor roadmap shows baseline scaling from 98K ops/sec (1 worker) to 148K (2 workers). Target: 190K at 2 workers (1.9x scaling).

| **Verification** | `make bench` on `pkg/engine` shows scaling improvement. `make race` clean. All integration tests pass. |
| **Outcome** | The single biggest performance limiter is removed. Read concurrency becomes CPU-bound, not lock-bound. |

### 3.3 — Load shedding (Finding #17)

| Field | Detail |
|-------|--------|
| **Severity** | Medium |
| **Effort** | ~20–32 hours |
| **Risk** | Medium — load shedding decisions affect user experience |
| **Approach** | Add a load shedder at the `pkg/server` level that monitors three signals:

1. **Circuit breaker state** — if half-open, reject non-critical requests
2. **Connection queue depth** — if `connWaiters` > threshold (configurable, default 50), return 503
3. **Goroutine count** — if `runtime.NumGoroutine()` > 2× baseline, throttle

Implement as middleware wrapping the MySQL and wire protocol handlers, not embedded in the engine. |
| **Key files** | `pkg/server/load_shedder.go` (new), `pkg/server/server.go` (wire in middleware) |
| **Verification** | Load test with >max connections shows 503 responses instead of indefinite hangs |
| **Outcome** | Under extreme load, the server degrades gracefully instead of hanging clients indefinitely |

### 3.4 — SDK consolidation (Finding #9)

| Field | Detail |
|-------|--------|
| **Severity** | High |
| **Effort** | ~16–24 hours |
| **Risk** | Low — SDKs are wrappers with no existing tests to break |
| **Approach** | Choose one direction:

**Option A (recommended if SDKs are strategic):** For each SDK:
- Python: Add `pyproject.toml`, `pytest` tests, type hints, publish to PyPI
- JavaScript: Verify `package.json`, add `jest` tests, publish to npm
- Java: Add `pom.xml` (Maven) or `build.gradle`, add JUnit tests

**Option B (recommended if SDKs are secondary):**
- Add a `WARNING` comment at the top of each SDK file: "This is a community example, not a production SDK. Use at your own risk." |
| **Verification** | Option A: SDK build + test pipelines pass. Option B: Warning shows at package import time. |
| **Outcome** | SDKs are either production-grade or clearly marked as examples |

---

## 7. Phase 4: Ecosystem & Quality Gates (Month 3–4)

**Gate:** Phase 3 merged. Scaling benchmarks show improvement.

### 4.1 — Replace coverage-chase tests with behavioral tests (Finding #2)

| Field | Detail |
|-------|--------|
| **Severity** | Critical |
| **Effort** | ~80–120 hours |
| **Risk** | High — removing tests carries risk of coverage regression |
| **Approach** | This is a gradual replacement, not a bulk delete:

1. **Inventory** — Categorize each `z_*` and `v10*-v116*` test file by what it actually tests:
   - **Redundant** — same scenario tested in integration tests → delete
   - **Trivial** — calls one function with default values → delete (line coverage will be picked up by remaining tests)
   - **Unique scenario** — covers an edge case not tested elsewhere → **keep but rename**
   - **Bug regression** — covers a previously fixed bug → keep and document the bug reference

2. **Replace with table-driven tests** — For each deleted trivial test, identify the parent feature test file and add a table-driven subtest.

3. **Maintain coverage gate** — The existing `scripts/coverage-gate.go` prevents regression. Run it after each batch.

| **Verification** | Before: 80 test files in `pkg/catalog/`. After: 35–40 files. Coverage does not drop. |
| **Outcome** | Test suite is faster (fewer files to compile), clearer (behavioral intent over line-chasing), and more maintainable |

### 4.2 — Website test infrastructure (Finding #16 + website gaps)

| Field | Detail |
|-------|--------|
| **Severity** | Low (innerHTML) / Medium (a11y) |
| **Effort** | ~8–16 hours |
| **Risk** | Low |
| **Approach** | Three actions:

1. **Fix innerHTML** in `CodeExampleSection.tsx:215` — use React state + textContent instead of `innerHTML`
2. **Add vitest** — configure `vitest` in website/package.json, write smoke tests for the 5 page components (render check)
3. **Add a11y lint** — add `eslint-plugin-jsx-a11y` and fix violations

| **Key files** | `website/src/sections/CodeExampleSection.tsx`, `website/vite.config.ts` |
| **Verification** | `npm test` runs vitest. `npm run lint` shows zero a11y violations. |
| **Outcome** | Website is safe (no XSS vector) and minimally tested |

### 4.3 — Cache-control headers + CDN hardening (Findings #19 + #21)

| Field | Detail |
|-------|--------|
| **Severity** | Low |
| **Effort** | ~2–4 hours |
| **Risk** | Low |
| **Approach** | 1. Add `Cache-Control: public, max-age=3600` and `Expires` headers to webui static file handler. 2. Add `integrity` (SRI) attributes to CDN script tags for Monaco Editor and Font Awesome in `webui/templates/index.html`. |
| **Verification** | curl shows cache headers on `/static/` responses. SRI mismatch causes browser console error. |
| **Outcome** | Better page load performance; CDN compromise won't affect webui users |

---

## 8. Effort Summary Table

| # | Finding | Phase | Severity | Effort (h) | Risk | Dependencies |
|---|---------|-------|----------|------------|------|-------------|
| 3 | Update x/crypto | P0 | Crit | 0.5 | None | — |
| 6 | docker-compose cleartext | P0 | High | 0.3 | None | — |
| 18 | Config file wire/remove | P0 | Med | 1 | Low | — |
| 22 | README badges | P0 | Low | 0.25 | None | — |
| 20 | Memory backend warn | P0 | Low | 0.5 | None | — |
| 4 | Structured logging | P1 | High | 24–40 | Med | P0 merged |
| 8 | WebUI CSRF | P1 | High | 4–8 | Low | P0 merged |
| 10 | Fuzz unsafe.String | P1 | High | 4–6 | Low | P0 merged |
| 12 | TCP keepalive | P1 | Med | 1 | Low | P0 merged |
| 13 | Token-in-URL fix | P1 | Med | 1 | Low | P0 merged |
| 5 | Split database.go | P2 | High | 16–24 | High | P1 merged (logging gives observability to detect split regressions) |
| 15/23 | Deduplicate helpers | P2 | Med | 4–6 | Low | Independent |
| 14 | Test naming standard | P2 | Med | 8–12 | Low | Independent |
| 7 | WebUI E2E tests | P3 | High | 24–40 | Med | P1 merged (CSRF must be done before testing auth flows) |
| 1 | Catalog.mu Phase 1 | P3 | Crit | 80–120 | High | P2 merged (database.go split makes the refactor area easier to navigate) |
| 17 | Load shedding | P3 | Med | 20–32 | Med | P1 merged (structured logging needed to diagnose shedding decisions) |
| 9 | SDK consolidation | P3 | High | 16–24 | Low | Independent |
| 2 | Replace coverage-chase tests | P4 | Crit | 80–120 | High | P2 merged (test naming must be settled before merging) |
| 16 | Website fixes | P4 | Low | 8–16 | Low | Independent |
| 19/21 | Cache + CDN hardening | P4 | Low | 2–4 | Low | Independent |

**Totals:**

| Phase | Items | Effort (h) | Calendar |
|-------|-------|-----------|----------|
| P0 — Quick wins | 5 | ~2.5 | Days 1–3 |
| P1 — Security + Observability | 5 | 34–63 | Week 2–3 |
| P2 — Code health | 3 | 28–42 | Week 3–5 |
| P3 — Strategic refactors | 4 | 140–216 | Month 2–3 |
| P4 — Ecosystem + quality | 3 | 90–140 | Month 3–4 |
| **Total** | **20** | **294–464** | **~14–16 weeks (parallel)** |

---

## 9. Dependency Graph

```
Phase 0 (Quick Wins)
  │
  ├── P0.1 (x/crypto) ─── no deps
  ├── P0.2 (docker-compose) ─── no deps
  ├── P0.3 (config file) ─── no deps
  ├── P0.4 (badges) ─── no deps
  └── P0.5 (memory warn) ─── no deps
        │
        ▼
Phase 1 (Security + Observability)
  │
  ├── P1.1 (structured logging) ───── depends on: P0 merged
  ├── P1.2 (CSRF) ─────────────────── depends on: P0 merged
  ├── P1.3 (fuzz unsafe.String) ───── depends on: P0 merged  ─── feeds into P4.1 (test rationalization)
  ├── P1.4 (TCP keepalive) ────────── depends on: P0 merged
  └── P1.5 (token URL) ────────────── depends on: P0 merged
        │
        ├────────────────────────────────────────┐
        ▼                                        ▼
Phase 2 (Code Health)                   Phase 3 (Strategic Refactors)
  │                                        │
  ├── P2.1 (database.go split) ──┐         ├── P3.1 (WebUI E2E) ─── depends on P1.2 (CSRF)
  │   depends on: P1 merged      │         ├── P3.2 (Catalog.mu) ── depends on P2.1 (split)
  │                               │         ├── P3.3 (load shedder) ─ depends on P1.1 (logging)
  ├── P2.2 (dedup helpers) ──────┤         └── P3.4 (SDKs) ─── independent
  │   depends on: P1 merged      │
  │                               ▼
  └── P2.3 (test naming) ────────► P3.2 (Catalog.mu) ─── needs database.go navigable
        │
        ▼
Phase 4 (Ecosystem + Quality)
  │
  ├── P4.1 (coverage-chase tests) ─── depends on P2.3 (naming must be settled)
  ├── P4.2 (website fixes) ────────── independent
  └── P4.3 (cache + CDN) ──────────── independent
```

### Key dependencies

| Must precede | Because |
|-------------|---------|
| **P1 (logging)** before **P3.3 (load shedder)** | Shedding decisions must be observable |
| **P1.2 (CSRF)** before **P3.1 (WebUI E2E)** | E2E tests need stable auth flows |
| **P2.1 (database.go split)** before **P3.2 (Catalog.mu)** | Refactoring the central mutex is dangerous in a 4671-line file |
| **P2.3 (test naming)** before **P4.1 (coverage-chase cleanup)** | Must have a naming convention before merging files under it |
| **P1.3 (fuzz unsafe.String)** is independent but feeds P4.1 | Fuzz findings may add tests that need naming standards |

---

## 10. Risk Register

| Risk | Phase | Probability | Impact | Mitigation |
|------|-------|-----------|--------|------------|
| **Catalog.mu refactor breaks SELECT** | P3 | Medium | Catastrophic (all reads broken) | Complete test coverage on SELECT paths before starting; feature-flag the new path; run for 24h on staging before switching default |
| **database.go split conflicts with open PRs** | P2 | High | Medium (merge conflicts) | Coordinate with team; do the split in a single focused session; all other work pauses for 4h |
| **govulncheck fails after x/crypto update** | P0 | Low | Low | Pin to the latest working version. Go's x/crypto is well-maintained; breakage is rare |
| **Coverage drops when removing coverage-chase tests** | P4 | Medium | High | Run coverage gate after every batch; write 1 integration test for every 5 trivial tests deleted |
| **Playwright tests flaky in CI** | P3 | Medium | Medium | Use `--retries 2`; record video on failure; mark non-critical tests as `@smoke` vs `@full` |

---

## 11. Recommended Sequencing (Gantt-style)

```
Week    1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16
Phase
────── ─── ─── ─── ─── ─── ─── ─── ─── ─── ─── ─── ─── ─── ─── ─── ───
P0      ██                                                                   Quick wins
P1          ██  ██                                                            Security + Observability
P2              ██  ██  ██                                                    Code health
P3.4 (SDK)                  ██  ██                                            SDK consolidation
P3.1 (E2E)                      ██  ██  ██                                    WebUI E2E
P3.3 (shedder)                     ██  ██  ██                                  Load shedding
P3.2 (Catalog.mu)                        ██  ██  ██  ██  ██  ██                Catalog.mu refactor
P4.1 (tests)                                            ██  ██  ██  ██        Coverage-test cleanup
P4.2 (website)                                     ██                          Website fixes
P4.3 (CDN)                                        █                           Cache/CDN hardening

                ┌──────────────────────────────────────────────────────┐
                │  Phase 3.2 (Catalog.mu) is the critical path        │
                │  Everything else can be parallelized around it       │
                └──────────────────────────────────────────────────────┘
```

### Critical path

The **Catalog.mu refactor (P3.2)** is the single most impactful item and also the highest risk. It sits on the critical path for the v0.7.0 release goal. All preceding phases (P0, P1, P2) should be treated as **prerequisite sprint work** — they reduce risk surface for the refactor.

### Recommended release milestones

| Milestone | Content | Target |
|-----------|---------|--------|
| **v0.6.1** | P0 (all 5 quick wins) | Week 1 |
| **v0.6.2** | P1 (logging, CSRF, fuzz, keepalive, token) | Week 3 |
| **v0.6.3** | P2 (split, dedup, naming) + P3.4 (SDKs) | Week 5 |
| **v0.7.0-rc.1** | P3.1 (E2E) + P3.3 (shedder) | Week 8 |
| **v0.7.0-rc.2** | P3.2 (Catalog.mu Phase 1) | Week 12 |
| **v0.7.0** | P4.1 (test cleanup) + P4.2/4.3 | Week 16 |

### Parallel work assignments

The following tracks are fully independent and can be assigned to different engineers:

| Track | Items | Engineer profile |
|-------|-------|-----------------|
| **A — Infrastructure** | P0.2 (docker), P0.3 (config), P0.5 (memory warn), P4.3 (CDN), P4.2 (website fixes) | DevOps / platform engineer |
| **B — Quality** | P2.3 (test naming), P3.1 (E2E), P4.1 (test rationalization), P1.3 (fuzz) | QA / SDET engineer |
| **C — Core engine** | P1.1 (logging), P2.1 (database.go split), P2.2 (dedup), P3.2 (Catalog.mu) | Database engineer |
| **D — Security** | P0.1 (x/crypto), P1.2 (CSRF), P1.5 (token URL), P1.4 (keepalive) | Security engineer |
| **E — Ecosystem** | P3.4 (SDK consolidation), P0.4 (badges) | SDK / devrel engineer |

---

## Appendix: Items Not Actioned

The following review observations are noted but not escalated to action items. They are either out of scope for v0.7.0 or cannot be actioned without further discussion.

| Observation | Reason not actioned |
|------------|-------------------|
| No generated API docs (godoc) | Out of scope — external infrastructure concern |
| No systemd unit file | Deferred — should be contributed by downstream packagers |
| No backup automation cron | Deferred — needs operations runbook design first |
| No Kubernetes operator | Out of scope for a v0.7 embedded database |
| `[]interface{}` row allocation pressure | Tracked in REFACTOR_ROADMAP.md as future work; not blocking |
| Replication = not HA warning | Already explicitly documented in HA_FAILOVER.md and ERRATA in replication package |
