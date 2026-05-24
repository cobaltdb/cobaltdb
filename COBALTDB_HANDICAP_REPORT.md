# CobaltDB Production Readiness Report

**Date:** 2026-05-24
**Scope:** Local repository review, test gates, race testing, recovery drills, backup drills, SQL parser hardening, MySQL prepared statement hardening, external MySQL driver certification, write-latency benchmark gating, replication disconnect failure injection, HA readiness guards, vector index persistence certification, FDW materialization limits, procedure/trigger semantics certification, and operations documentation.
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
| Strict SQL parsing mode | Added as opt-in production mode |
| MySQL prepared statements | Parameterized `COM_STMT_EXECUTE` covered for core scalar types and binary result rows |
| External MySQL driver | `database/sql` + `github.com/go-sql-driver/mysql` baseline covered |
| Benchmark regression gate | Added via `scripts/benchmark-gate.sh`, now including write p95/p99 under readers |
| Replication disconnect detection | Slave status clears connection state after master disconnect |
| HA readiness guards | API reports explicit blockers and refuses unsafe in-process promotion |
| Vector index persistence | HNSW metadata persists across create, post-index DML, reopen, and backup/restore |
| FDW streaming/pushdown groundwork | CSV scans expose cursor streaming and apply safe simple predicates |
| Procedure/trigger semantics | `CALL` placeholder args, exact arity, complex param substitution, and BEFORE/AFTER trigger row images are covered |
| Operations runbook | Added |
| HA/failover certification | Not ready |
| Strict MySQL protocol compatibility | Not ready |

**Production readiness level:** about **92/100** for single-node production-candidate use, assuming documented constraints are acceptable. It is **not** a 95+/100 database for high-concurrency OLTP, automatic failover, or broad MySQL client/ORM compatibility yet.

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
| `aa3b989` | SQL parser | Added opt-in strict statement-boundary parsing for production deployments |
| `d48b30c` | MySQL protocol | Added binary prepared statement parameter decoding and compatibility matrix |
| `2e0db02` | Benchmarks | Added write-latency p50/p95/p99 metrics under background readers |
| `ffef1b4` | Replication | Added master-disconnect failure injection and HA/failover boundary doc |
| `784c699` | Vector indexes | Added HNSW metadata persistence fixes and reopen/drop drills |
| `6999583` | FDW | Added CSV streaming-read materialization limits and SQL-level limit drill |
| `1225721` | Procedures/triggers | Added `CALL` placeholder/arity fixes and BEFORE/AFTER trigger semantics certification |
| `74437e6` | MySQL compatibility | Added real Go MySQL driver baseline and fixed prepared-statement packet compatibility |
| `825b765` | HA boundary | Added failover readiness API and unsafe promotion guard |
| `1270975` | Vector indexes | Added post-index DML persistence and backup/restore rebuild drill |
| `2c67dd6` | FDW | Added streaming cursor API and CSV cursor implementation |
| `fa2a30e` | FDW pushdown | Added simple WHERE predicate extraction into streaming scan options |
| Current iteration | FDW pushdown | Added safe CSV wrapper-side predicate filtering |

Validation performed during this pass:

```bash
go test ./pkg/txn ./pkg/storage ./pkg/btree
go test -race ./integration -run 'TestConcurrentTransactions|TestAutoVacuumDisabled' -count=1
go test -race ./...
go test ./pkg/engine -run 'TestWALRecoversCommittedWritesAfterProcessExit|TestWALCrashRecoveryIgnoresOpenTransaction|TestIncrementalBackupRestoreOpensAsDatabase' -count=1
go test ./pkg/engine -run TestProductionSoakBoundedCheckpointBackupReopen -count=1
go test -race ./pkg/engine -run TestProductionSoakBoundedCheckpointBackupReopen -count=1
go test ./test -run 'TestSQLCompatibility' -count=1
go test ./pkg/query ./pkg/engine -run 'TestParseStrict|TestStrictSQL|TestDefaultSQLParsing' -count=1
go test ./pkg/protocol -run 'TestCountPreparedParams|TestPreparedStmtParseExecuteArgs|TestHandleStmtPrepare|TestHandleStmtExecute' -count=1
go test ./test -run TestMySQLPreparedStatementExecuteWithParameters -count=1
go test ./integration -run 'TestMySQLGoSQLDriverCompatibility|TestMySQLProtocolE2E' -count=1
go test ./pkg/engine -run '^$' -bench BenchmarkWriteLatencyUnderReaders -benchtime=10x -count=1
go test ./pkg/replication -run 'TestSlaveStatusClearsConnectionOnMasterDisconnect|TestReplicateWALWithSlaves|TestWaitForSlavesFullSyncMode' -count=1
go test ./pkg/replication -run 'TestFailoverReadinessReportsTransportIsNotHA|TestPromoteToMasterRequiresExternalFencing' -count=1
go test -race ./pkg/replication -run TestSlaveStatusClearsConnectionOnMasterDisconnect -count=1
go test ./pkg/replication -count=1
go test ./pkg/catalog -run TestVectorIndexMetadataPersistsOnCreateAndDrop -count=1
go test ./pkg/engine -run 'TestVectorIndexPersistsAcrossReopen|TestVectorIndexLargeRebuildAndBackupRestore' -count=1
go test -race ./pkg/catalog ./pkg/engine -run 'TestVectorIndexMetadataPersistsOnCreateAndDrop|TestVectorIndexPersistsAcrossReopen|TestVectorIndexLargeRebuildAndBackupRestore' -count=1
go test ./pkg/fdw -count=1
go test -race ./pkg/fdw -count=1
go test ./pkg/catalog -run TestFDWScanOptionsCarrySimpleWherePredicates -count=1
go test ./integration -run 'TestFDWCSVSelect|TestFDWCSVMaxRowsLimitViaSQL' -count=1
go test ./pkg/query -run TestParseCallProcedure -count=1
go test ./test -run 'TestStoredProcedure|TestTrigger_BeforeAfterOrderAndRowImages|TestInsteadOfTrigger' -count=1
go test ./pkg/catalog -run 'TestExecuteTriggers|TestResolveTriggerRefs|TestResolveTriggerExpr' -count=1
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
- Track p95/p99 write latency under concurrent readers.

### 2. HA / Clustering Is Not Production-Grade

Replication exists, but CobaltDB should not be presented as automatic HA infrastructure.

Missing or not certified:

- Raft/Paxos-style consensus.
- Automatic leader election.
- Split-brain protection.
- Automated failover implementation with proven RPO/RTO.
- Cross-node backup/recovery drills.

Production stance:

- Use replication as transport/read scaling where acceptable.
- Use `GetFailoverReadiness` to keep replication transport status separate from HA readiness.
- Do not rely on it as managed failover until a dedicated HA certification pass is complete.
- Use `docs/HA_FAILOVER.md` as the current replication and failover boundary.

### 3. MySQL Wire Compatibility Is Still Narrow

The server supports useful MySQL protocol paths, but broad client compatibility is not complete.

Known gaps:

- Prepared statement execution now supports core scalar binary parameters and binary result rows, but temporal parameters, long-data streaming, cursors, and richer metadata are not certified.
- Unsupported MySQL commands return simplified errors.
- Session variables and advanced MySQL metadata flows are incomplete.
- The Go MySQL driver is covered; additional ORMs and non-Go drivers still require explicit certification.

Production stance:

- Keep MySQL listener private or disabled unless the exact client behavior is validated.
- Prefer native/embedded API paths for production-critical integrations.
- Use `docs/MYSQL_COMPATIBILITY.md` as the compatibility contract for any MySQL-facing deployment.

### 4. SQL Parser Strictness

The compatibility corpus now locks a supported/unsupported baseline, and production callers can enable strict statement-boundary parsing with `engine.Options.StrictSQLParsing` or call `query.ParseStrict` directly. The default parser remains permissive for compatibility, so this risk is reduced but not fully eliminated for deployments that leave strict mode disabled.

Production stance:

- Enable `StrictSQLParsing` for production workloads that do not depend on legacy permissive parser behavior.
- Treat default permissive parsing as a compatibility mode, not the recommended production gate.
- Keep `test/sql_compat_corpus_test.go` updated for every syntax expansion.

### 5. Advanced Feature Certification

Some advanced features are broad but need workload-specific certification before being treated as primary production pillars:

- WASM SQL execution beyond selected paths.
- Very large Vector/HNSW rebuild behavior beyond the 512-row backup/restore drill.
- FDW still materializes rows into a temporary query-engine B-tree, but CSV scans now stream into that tree, apply safe advisory simple predicates, and have row/byte limits.
- Procedure body result-set and mutable `OUT`/`INOUT` parameter semantics beyond the certified DML contract.
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

1. Catalog lock granularity improvements.
2. Additional MySQL ORM and non-Go driver certification runs.
3. Actual HA consensus, fencing, and externally orchestrated promotion implementation.
4. Thousand-plus vector mixed workload certification.
5. FDW projection pushdown and wrapper-side predicate filtering for CSV.

## Final Decision

CobaltDB can be described as a **production-oriented single-node candidate** with strong test coverage, race-clean current suite, WAL crash drills, restore-open drills, and an operations runbook.

It should **not** be described as fully production-certified for automated HA, high-write OLTP at scale, or broad MySQL drop-in compatibility until the remaining risks above are closed.
