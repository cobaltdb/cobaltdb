# HA And Failover Status

CobaltDB replication is a master-slave transport. It is useful for shipping WAL
entries and maintaining read-oriented replicas, but it is not an automated HA
cluster manager.

## Current Guarantees

| Area | Status |
|---|---|
| Master accepts slave connections | Implemented |
| Slave resume from last applied LSN | Implemented with optional state file |
| WAL retention bounds | Implemented by entry count and retained bytes |
| Snapshot fallback for stale slaves | Implemented when a snapshot callback is configured |
| Disconnect detection | Implemented; slave status clears `Connected` after master disconnect |
| Async replication | Implemented |
| Sync/full-sync wait semantics | Implemented for connected slaves |
| HA readiness API | Implemented; reports explicit blockers and no automatic failover |
| Unsafe in-process promotion guard | Implemented; `PromoteToMaster` returns an unsupported-failover error |

## Not Yet HA

| Area | Status |
|---|---|
| Leader election | Not implemented |
| Quorum/consensus | Not implemented |
| Split-brain prevention | Not implemented |
| Automatic failover | Not implemented |
| Fencing old primaries | Not implemented |
| Cross-node RPO/RTO certification | Not implemented |

## Required Failure Drills

```bash
go test ./pkg/replication -run 'TestSlaveStatusClearsConnectionOnMasterDisconnect|TestReplicateWALWithSlaves|TestWaitForSlavesFullSyncMode' -count=1
go test ./pkg/replication -run 'TestFailoverReadinessReportsTransportIsNotHA|TestPromoteToMasterRequiresExternalFencing' -count=1
```

Operational stance:

- Do not advertise CobaltDB replication as automatic HA.
- Use `GetFailoverReadiness` in operational health surfaces to keep transport
  status separate from HA readiness.
- Treat promotion/failover as an external orchestration task until consensus,
  fencing, and promotion drills are implemented.
- Keep a verified backup/restore path even when replication is enabled.
