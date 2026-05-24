# CobaltDB Production Readiness Report

**Date:** 2026-05-24
**Scope:** Local repository review, test gates, race testing, recovery drills, backup drills, benchmark gate, and operations documentation.
**Status:** Production-oriented single-node candidate. Not yet certified for automated HA/failover or strict MySQL wire compatibility.

## Executive Summary

CobaltDB is much closer to a production-ready single-node embedded/server database after this hardening pass. The most important blocker found in the current iteration was not documentation: `go test -race ./...` exposed real transaction recycle and page flush races. Those races are now fixed and the full race suite passes.

Current readiness estimate:

| Area | Status |
|---|---|
| Core package tests | Passing |
| Full race suite | Passing |
| Crash recovery drills | Covered for committed WAL writes and open transactions |
| Backup/restore drills | Covered for full, incremental, and differential restore-open flows |
| Bounded soak/load drill | Added, including explicit txns, checkpoint, backup, source reopen, restore reopen |
| SQL compatibility baseline | Added for supported and unsupported syntax surface |
| Benchmark regression gate | Added via `scripts/benchmark-gate.sh` |
| Operations runbook | Added |
| HA/failover certification | Not ready |
| Strict MySQL protocol compatibility | Not ready |

**Production readiness level:** about **82/100** for single-node production-candidate use, assuming documented constraints are acceptable. It is **not** a 95+/100 database for high-concurrency OLTP, automatic failover, or broad MySQL client compatibility yet.

## Work Completed In This Pass

Recent hardening commits:

| Commit | Area | Result |
|---|---|---|
| `b6bf701` | Storage/txn race safety | Fixed transaction recycle map races and page flush/data mutation races |
| `c6c0ab7` | WAL recovery | Added crash drill for open/uncommitted transaction recovery |
| `87e8e86` | Backup restore | Verified full/incremental/differential restore chains open as databases |
| `55f1106` | Soak/load | Added bounded production soak with txns, checkpoint, backup, reopen |
| `104bb3a` | SQL compatibility | Added supported/unsupported SQL corpus baseline |
| `69d21a1` | Benchmarks | Added bounded benchmark regression gate |
| `17af3ef` | Operations | Added operational runbook and incident playbooks |

Validation performed during this pass:

```bash
go test ./pkg/txn ./pkg/storage ./pkg/btree
go test -race ./integration -run 'TestConcurrentTransactions|TestAutoVacuumDisabled' -count=1
go test -race ./...
go test ./pkg/engine -run 'TestWALRecoversCommittedWritesAfterProcessExit|TestWALCrashRecoveryIgnoresOpenTransaction|TestIncrementalBackupRestoreOpensAsDatabase' -count=1
go test ./pkg/engine -run TestProductionSoakBoundedCheckpointBackupReopen -count=1
go test -race ./pkg/engine -run TestProductionSoakBoundedCheckpointBackupReopen -count=1
go test ./test -run 'TestSQLCompatibility' -count=1
BENCHTIME=1ms COUNT=1 ./scripts/benchmark-gate.sh /tmp/cobaltdb-bench-smoke
```

Previously completed gates in this work stream:

```bash
go test ./...
go vet ./...
staticcheck ./...
gosec -exclude=G104 ./...
govulncheck ./...
```

## Remaining Production Risks

### 1. Single-Writer And Coarse Catalog Locking

CobaltDB still has a single-writer/coarse catalog locking profile. This is acceptable for embedded, edge, operational, and moderate write workloads, but it is a hard ceiling for high-concurrency OLTP.

Impact:

- Long-running read or DDL-heavy workflows can increase write latency.
- DDL can block DML.
- Sustained write throughput will not match row-locking or MVCC-first engines.

Next work:

- Continue catalog lock granularity work.
- Add workload-specific contention benchmarks to the benchmark gate.
- Track p95/p99 write latency under concurrent readers.

### 2. HA / Clustering Is Not Production-Grade

Replication exists, but CobaltDB should not be presented as automatic HA infrastructure.

Missing or not certified:

- Raft/Paxos-style consensus.
- Automatic leader election.
- Split-brain protection.
- Automated failover runbook with proven RPO/RTO.
- Cross-node backup/recovery drills.

Production stance:

- Use replication as transport/read scaling where acceptable.
- Do not rely on it as managed failover until a dedicated HA certification pass is complete.

### 3. MySQL Wire Compatibility Is Still Narrow

The server supports useful MySQL protocol paths, but broad client compatibility is not complete.

Known gaps:

- Prepared statement execution over the wire remains limited.
- Unsupported MySQL commands return simplified errors.
- Session variables and advanced MySQL metadata flows are incomplete.

Production stance:

- Keep MySQL listener private or disabled unless the exact client behavior is validated.
- Prefer native/embedded API paths for production-critical integrations.

### 4. SQL Parser Strictness

The compatibility corpus now locks a supported/unsupported baseline, but the parser is still permissive in some areas. Some unsupported trailing constructs can be accepted as aliases or ignored by older parser paths.

Production stance:

- Treat this as a correctness hardening item.
- Add strict EOF/statement-boundary parsing behind a compatibility flag or after completing currently partial syntax support.
- Keep `test/sql_compat_corpus_test.go` updated for every syntax expansion.

### 5. Advanced Feature Certification

Some advanced features are broad but need workload-specific certification before being treated as primary production pillars:

- WASM SQL execution beyond selected paths.
- Vector/HNSW persistence and rebuild behavior.
- FDW memory behavior for large external data.
- Stored procedure execution semantics.
- Composite/advanced constraint cases.

## Release Gate

Required before any release candidate:

```bash
go test ./...
go test -race ./...
go vet ./...
staticcheck ./...
gosec -exclude=G104 ./...
govulncheck ./...
./scripts/benchmark-gate.sh
```

Required drills:

```bash
go test ./pkg/engine -run 'TestWALRecoversCommittedWritesAfterProcessExit|TestWALCrashRecoveryIgnoresOpenTransaction' -count=1
go test ./pkg/engine -run 'TestIncrementalBackupRestoreOpensAsDatabase|TestProductionSoakBoundedCheckpointBackupReopen' -count=1
go test ./test -run 'TestSQLCompatibility' -count=1
```

Block release on:

- Any race detector failure.
- WAL crash recovery failure.
- Backup restore that cannot be opened and queried.
- SQL compatibility baseline drift without explicit review.
- `govulncheck` reachable high-severity issue.
- Benchmark regression above 20% in core paths without documented tradeoff.

## Next Iterations

Priority order:

1. Parser strict statement-boundary mode and tests.
2. MySQL wire prepared statement execution and client compatibility matrix.
3. Catalog lock granularity and p95/p99 write-latency benchmarks.
4. HA/failover design and failure-injection tests.
5. Vector/HNSW persistence certification.
6. Large FDW streaming/materialization limits.
7. Procedure/trigger execution semantics certification.

## Final Decision

CobaltDB can be described as a **production-oriented single-node candidate** with strong test coverage, race-clean current suite, WAL crash drills, restore-open drills, and an operations runbook.

It should **not** be described as fully production-certified for automated HA, high-write OLTP at scale, or broad MySQL drop-in compatibility until the remaining risks above are closed.
