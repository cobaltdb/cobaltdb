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
| Externally fenced manual promotion | Implemented via `PromoteToMasterWithFencing` |
| Cooperative primary fencing guard | Implemented via `FencePrimary`; fenced masters reject new WAL entries |
| Former primary rejoin as replica | Implemented via `RejoinAsReplica` after fencing |

## Not Yet HA

| Area | Status |
|---|---|
| Leader election | Not implemented |
| Quorum/consensus | Not implemented |
| Split-brain prevention | Not implemented |
| Automatic failover | Not implemented |
| Built-in fencing of old primaries | Not implemented |
| Cross-node RPO/RTO certification | Not implemented |

## Manual Promotion Contract

CobaltDB can perform a local slave-to-master role transition only when an
external HA control plane provides a `PromotionRequest` proving:

- a non-empty fencing token,
- a monotonically increasing fencing epoch,
- explicit confirmation that the old primary has been fenced,
- an unexpired token when `ExpiresAt` is set,
- a replica LSN at or ahead of the requested promotion LSN,
- no active master connection unless the caller explicitly allows it.

The legacy `PromoteToMaster` API still refuses promotion because CobaltDB does
not run leader election or quorum consensus. `PromoteToMasterWithFencing` is for
externally orchestrated promotion only.

`FencePrimary` is a cooperative local guard for old primaries that are still
reachable during an orchestrated failover. Once fenced, `ReplicateWALEntry`
returns `ErrPrimaryFenced` and the manager does not advance its master LSN.
This does not replace infrastructure-level fencing such as VM power-off,
storage fencing, or network isolation.

After the failover decision, `RejoinAsReplica` can demote a fenced former
primary into a replica of the new master. It clears local master state and WAL
retention buffers, records the new `MasterAddr`, and persists the replica LSN
when a state file is configured. It does not perform data reconciliation by
itself; the normal slave resume/snapshot path must still validate or refresh the
data set.

## Required Failure Drills

```bash
go test ./pkg/replication -run 'TestSlaveStatusClearsConnectionOnMasterDisconnect|TestReplicateWALWithSlaves|TestWaitForSlavesFullSyncMode' -count=1
go test ./pkg/replication -run 'TestFailoverReadinessReportsTransportIsNotHA|TestPromoteToMasterRequiresExternalFencing|TestPromoteToMasterWithFencing|TestFencePrimary|TestExternallyOrchestratedFailoverDrill|TestRejoinAsReplica' -count=1
```

Operational stance:

- Do not advertise CobaltDB replication as automatic HA.
- Use `GetFailoverReadiness` in operational health surfaces to keep transport
  status separate from HA readiness.
- Treat promotion/failover as an external orchestration task until consensus and
  built-in fencing are implemented.
- Keep a verified backup/restore path even when replication is enabled.
