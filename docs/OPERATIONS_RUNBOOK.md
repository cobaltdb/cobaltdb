# CobaltDB Operations Runbook

This runbook defines the minimum operational checks for a production-oriented
CobaltDB deployment. It is intentionally command-focused so release, on-call,
and incident work use the same gates.

## Release Gate

Run these before cutting a release candidate:

```bash
go test ./...
go test -race ./...
go vet ./...
staticcheck ./...
gosec -exclude=G104 ./...
govulncheck ./...
./scripts/benchmark-gate.sh
```

Required drill tests:

```bash
go test ./pkg/engine -run 'TestWALRecoversCommittedWritesAfterProcessExit|TestWALCrashRecoveryIgnoresOpenTransaction' -count=1
go test ./pkg/engine -run 'TestIncrementalBackupRestoreOpensAsDatabase|TestProductionSoakBoundedCheckpointBackupReopen' -count=1
go test ./test -run 'TestSQLCompatibility' -count=1
go test ./pkg/protocol -run 'TestHandleStmt|TestPreparedStmt|TestCountPreparedParams' -count=1
go test ./test -run 'TestMySQLPreparedStatementExecuteWithParameters|TestMySQL' -count=1
go test ./integration -run 'TestMySQLGoSQLDriverCompatibility|TestMySQLProtocolE2E' -count=1
go test ./pkg/replication -run 'TestSlaveStatusClearsConnectionOnMasterDisconnect|TestReplicateWALWithSlaves|TestWaitForSlavesFullSyncMode' -count=1
go test ./pkg/replication -run 'TestFailoverReadinessReportsTransportIsNotHA|TestPromoteToMasterRequiresExternalFencing' -count=1
go test ./pkg/catalog -run TestVectorIndexMetadataPersistsOnCreateAndDrop -count=1
go test ./pkg/engine -run 'TestVectorIndexPersistsAcrossReopen|TestVectorIndexLargeRebuildAndBackupRestore|TestVectorIndexThousandPlusMixedDMLReopen' -count=1
go test ./pkg/fdw -count=1
go test -race ./pkg/fdw -count=1
go test ./pkg/catalog -run 'TestFDWScanOptionsCarrySimpleWherePredicates|TestFDWProjectionPushdownExpandsRowsForLocalEvaluation|TestFDWMaterializedByteLimit' -count=1
go test ./integration -run 'TestFDWCSVSelect|TestFDWCSVMaxRowsLimitViaSQL|TestFDWCSVProjectionPushdownViaSQL' -count=1
go test ./pkg/query -run TestParseCallProcedure -count=1
go test ./test -run 'TestStoredProcedure|TestTrigger_BeforeAfterOrderAndRowImages|TestInsteadOfTrigger' -count=1
```

Release blockers:

- Failing `go test -race ./...`.
- Crash recovery cannot recover committed WAL writes.
- Open/uncommitted transactions appear after recovery.
- Backup restore cannot be opened as a database.
- Statistically significant benchmark regression above 20% in core paths without
  a documented tradeoff.
- Known high-severity `govulncheck` finding in reachable code.

## Startup

1. Start with WAL enabled for disk databases.
2. Use a page-count `CacheSize`; `1024` means 1024 pages, not bytes.
3. Enable `StrictSQLParsing` unless the workload depends on legacy permissive
   parser behavior.
4. Set explicit `BackupDir`, `MaxBackups`, and `BackupRetention` values.
5. Enable health/admin endpoints on a loopback or protected network interface.
6. Configure TLS and authentication before exposing wire or MySQL protocol
   ports outside a trusted network.

Health checks:

```bash
curl -fsS http://127.0.0.1:8420/health
curl -fsS http://127.0.0.1:8420/ready
curl -fsS http://127.0.0.1:8420/healthz
curl -fsS http://127.0.0.1:8420/metrics/prometheus
```

## Backups

Create and list backups:

```bash
cobaltdb-cli -path ./data/cobalt.cb backup create full
cobaltdb-cli -path ./data/cobalt.cb backup create incremental
cobaltdb-cli -path ./data/cobalt.cb backup create differential
cobaltdb-cli -path ./data/cobalt.cb backup list
```

Restore drill:

```bash
cobaltdb-cli -path ./data/cobalt.cb backup restore <backup-id>
cobaltdb-cli -path ./data/cobalt.cb.restored "SELECT COUNT(*) FROM critical_table"
```

Operational policy:

- Run a restore drill after changing backup configuration.
- Keep at least one recent full backup plus the required incremental or
  differential chain.
- Store backups on storage isolated from the live database volume.
- Alert on failed backup creation, missing backup files, and restore drill
  failures.

## Checkpoint And Recovery

Manual checkpoint from embedded/admin code:

```go
if err := db.Checkpoint(); err != nil {
    return err
}
```

CLI workaround when an explicit checkpoint command is not exposed:

```bash
cobaltdb-cli -path ./data/cobalt.cb backup create full
```

Crash recovery drill:

```bash
go test ./pkg/engine -run TestWALRecoversCommittedWritesAfterProcessExit -count=1
go test ./pkg/engine -run TestWALCrashRecoveryIgnoresOpenTransaction -count=1
```

If recovery fails:

1. Stop writers immediately.
2. Copy the database file and `.wal` file/directory before retrying recovery.
3. Open the copied database in an isolated environment.
4. Compare row counts and checksums against the most recent verified backup.
5. Restore from backup if committed data cannot be recovered safely.

## Monitoring

Minimum signals:

- `/ready` status for load balancer routing.
- `/health` status for liveness.
- `/metrics/prometheus` for query, storage, transaction, slow query, and runtime
  metrics.
- `/transaction-metrics` for deadlock aborts, lock wait timeouts, transaction
  timeouts, and long-running transactions.
- Slow query log entries above the configured threshold.

Alert thresholds should be tuned per workload, but start with:

- readiness failures for more than 2 consecutive checks;
- any sustained increase in transaction aborts or deadlock aborts;
- slow query rate above expected baseline;
- WAL/checkpoint errors;
- backup age exceeding the recovery point objective;
- disk free space below 20%.

## Incident Playbooks

### Replication Disconnect

1. Check master and slave `/metrics/prometheus` and replication status.
2. Confirm the slave reports `Connected=false` after the disconnect.
3. Compare slave `last_applied_lsn` with the master's current LSN.
4. If the retained WAL window no longer covers the slave LSN, run a snapshot or
   backup restore before resuming traffic.
5. Do not promote a slave automatically; use an external runbook with fencing
   until HA consensus and failover certification are implemented.

### Readiness Failure

1. Check `/healthz` and process logs.
2. Confirm disk space and file permissions for the database, WAL, backup, and
   slow query paths.
3. Check transaction metrics for lock waits or deadlocks.
4. Drain traffic and restart only after preserving logs and WAL files.

### High Write Latency

1. Check slow query log and `/transaction-metrics`.
2. Verify no long-running transaction is blocking the single-writer path.
3. Run `CHECKPOINT` during a low-traffic window if WAL growth is high.
4. Compare current benchmark gate output against the last release baseline.

### Suspected Data Corruption

1. Stop writes and copy database plus WAL files.
2. Open the copy read-only or in an isolated environment.
3. Run critical count/sum checks.
4. Restore the latest verified backup if checks fail.
5. Keep the corrupted copy for root-cause analysis.

## Known Operational Constraints

- Single-writer behavior still limits sustained write scalability.
- Catalog-level locking means DDL can block DML.
- Replication is master-slave transport, not automated failover consensus.
- Parser compatibility is broad but not strict SQL-standard validation; keep the
  SQL compatibility corpus updated when adding syntax.
