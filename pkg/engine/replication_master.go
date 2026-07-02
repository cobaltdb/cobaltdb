package engine

import (
	"fmt"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/replication"
)

// defaultReplicationSyncTimeout bounds how long a sync-mode write waits for
// slave acknowledgements when Options.Replication.SyncTimeout is unset.
const defaultReplicationSyncTimeout = 5 * time.Second

// Statement-based logical replication (master side)
//
// Successful write statements are shipped to slaves as versioned
// {SQL, args, timestamp} payloads (see replication.StatementPayload), tagged
// with a monotonically increasing LSN by the replication manager.
//
// Ordering and transaction semantics:
//   - Autocommit statements are captured immediately after their implicit
//     transaction commits, in commit order.
//   - Statements inside an explicit transaction (BEGIN...COMMIT or DB.Begin)
//     are buffered and shipped in order when the transaction commits;
//     a rollback discards them, ROLLBACK TO SAVEPOINT truncates them.
//   - Slaves replay statements sequentially in LSN order, so a replica
//     converges to the master state; intermediate transaction states may be
//     briefly visible on the replica (statements are applied individually).
//
// Known limitations (documented, statement-based replication):
//   - Non-deterministic SQL (e.g. RANDOM()) may diverge between master and
//     replica; deterministic statements replay exactly.
//   - The replication log is in-memory on the master; after a master restart
//     slaves whose position is ahead of the master's restarted LSN counter
//     are resynchronized via a full snapshot.

// replicationMasterManager returns the replication manager when this node is
// an active replication master, otherwise nil.
func (db *DB) replicationMasterManager() *replication.Manager {
	mgr := db.replicationMgr
	if mgr == nil || mgr.Role() != replication.RoleMaster {
		return nil
	}
	return mgr
}

// isReplicatedWriteStmt reports whether a successfully executed statement of
// this type must be shipped to replicas.
func isReplicatedWriteStmt(stmt query.Statement) bool {
	switch stmt.(type) {
	case *query.InsertStmt, *query.UpdateStmt, *query.DeleteStmt,
		*query.CreateTableStmt, *query.DropTableStmt,
		*query.CreateForeignTableStmt,
		*query.CreateCollectionStmt, *query.DropCollectionStmt,
		*query.CreateIndexStmt, *query.DropIndexStmt,
		*query.CreateViewStmt, *query.DropViewStmt,
		*query.CreateTriggerStmt, *query.DropTriggerStmt,
		*query.CreateProcedureStmt, *query.DropProcedureStmt,
		*query.CreatePolicyStmt, *query.DropPolicyStmt,
		*query.AlterTableStmt,
		*query.CreateMaterializedViewStmt, *query.DropMaterializedViewStmt,
		*query.RefreshMaterializedViewStmt,
		*query.CreateFTSIndexStmt, *query.CreateVectorIndexStmt,
		*query.CallProcedureStmt:
		return true
	}
	return false
}

// replicateStatement captures a successfully committed write statement.
// When buffered is true (statement ran inside an explicit transaction) the
// payload is queued until COMMIT; otherwise it is appended to the replication
// stream immediately.
//
// It returns needSyncWait=true when entries were appended and the caller must
// invoke replicationSyncWait AFTER releasing replCaptureMu (and any other
// engine locks): waiting for slave ACKs while holding the capture lock can
// deadlock against a snapshot-sending slave handler that holds the slave
// connection mutex and needs the capture lock.
func (db *DB) replicateStatement(sqlText string, args []interface{}, buffered bool) (needSyncWait bool, err error) {
	mgr := db.replicationMasterManager()
	if mgr == nil || sqlText == "" {
		return false, nil
	}

	payload, err := replication.EncodeStatementPayload(sqlText, args, time.Now())
	if err != nil {
		return false, fmt.Errorf("replication: failed to encode statement: %w", err)
	}

	if buffered {
		db.replMu.Lock()
		db.replPending = append(db.replPending, payload)
		db.replMu.Unlock()
		return false, nil
	}

	if err := mgr.ReplicateWALEntry(payload); err != nil {
		return false, fmt.Errorf("replication: %w", err)
	}
	return true, nil
}

// replicationSyncWait implements the sync-mode durability contract: in "sync"
// and "full_sync" modes the write path pushes buffered entries immediately and
// waits for slave acknowledgements. On timeout, "full_sync" (and "sync" with
// SyncStrict) return an error — the write is committed locally but was NOT
// acknowledged by replicas; plain "sync" degrades to async with a warning.
//
// MUST be called without holding replCaptureMu or flushMu (see
// replicateStatement).
func (db *DB) replicationSyncWait(mgr *replication.Manager) error {
	mode := mgr.Mode()
	if mode == replication.ModeAsync {
		return nil
	}

	mgr.FlushWALBuffer()

	timeout := db.options.Replication.SyncTimeout
	if timeout <= 0 {
		timeout = defaultReplicationSyncTimeout
	}
	err := mgr.WaitForSlaves(timeout)
	if err == nil {
		return nil
	}
	if mode == replication.ModeFullSync || db.options.Replication.SyncStrict {
		return fmt.Errorf("replication sync: write committed locally but not acknowledged by replicas: %w", err)
	}
	if log := db.options.CoreStorage.Logger; log != nil {
		log.Warnf("replication sync degraded to async: %v", err)
	}
	return nil
}

// replTxnMarkSavepoint records the pending-buffer position of a savepoint so
// ROLLBACK TO SAVEPOINT can truncate the replicated statement list.
func (db *DB) replTxnMarkSavepoint(name string) {
	if db.replicationMasterManager() == nil {
		return
	}
	db.replMu.Lock()
	defer db.replMu.Unlock()
	if db.replSavepoints == nil {
		db.replSavepoints = make(map[string]int)
	}
	db.replSavepoints[name] = len(db.replPending)
}

// replTxnReleaseSavepoint forgets a savepoint mark (its statements remain
// pending, matching RELEASE SAVEPOINT semantics).
func (db *DB) replTxnReleaseSavepoint(name string) {
	if db.replicationMasterManager() == nil {
		return
	}
	db.replMu.Lock()
	defer db.replMu.Unlock()
	delete(db.replSavepoints, name)
}

// replTxnRollbackToSavepoint drops pending statements captured after the
// savepoint and invalidates later savepoint marks.
func (db *DB) replTxnRollbackToSavepoint(name string) {
	if db.replicationMasterManager() == nil {
		return
	}
	db.replMu.Lock()
	defer db.replMu.Unlock()
	mark, ok := db.replSavepoints[name]
	if !ok || mark > len(db.replPending) {
		return
	}
	for i := mark; i < len(db.replPending); i++ {
		db.replPending[i] = nil
	}
	db.replPending = db.replPending[:mark]
	for other, pos := range db.replSavepoints {
		if pos > mark {
			delete(db.replSavepoints, other)
		}
	}
}

// replTxnDiscard drops all pending replication statements (transaction rolled
// back or aborted).
func (db *DB) replTxnDiscard() {
	if db.replicationMgr == nil {
		return
	}
	db.replMu.Lock()
	defer db.replMu.Unlock()
	db.replPending = nil
	db.replSavepoints = nil
}

// replTxnFlush appends all pending replication statements to the stream after
// a successful explicit-transaction commit. Like replicateStatement, it only
// assigns LSNs; when it returns needSyncWait=true the caller must invoke
// replicationSyncWait once for the batch AFTER releasing engine locks.
func (db *DB) replTxnFlush() (needSyncWait bool, err error) {
	mgr := db.replicationMasterManager()
	if mgr == nil {
		return false, nil
	}
	db.replMu.Lock()
	pending := db.replPending
	db.replPending = nil
	db.replSavepoints = nil
	db.replMu.Unlock()

	if len(pending) == 0 {
		return false, nil
	}
	for _, payload := range pending {
		if err := mgr.ReplicateWALEntry(payload); err != nil {
			return false, fmt.Errorf("replication: %w", err)
		}
	}
	return true, nil
}
