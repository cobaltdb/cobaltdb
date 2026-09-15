package txn

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/metrics"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

var (
	ErrTxnCommitted     = errors.New("transaction already committed")
	ErrTxnAborted       = errors.New("transaction already aborted")
	ErrConflict         = errors.New("transaction conflict")
	ErrTxnNotFound      = errors.New("transaction not found")
	ErrDeadlockDetected = errors.New("deadlock detected")
	ErrTxnTimeout       = errors.New("transaction timeout")
	ErrReadOnlyTxn      = errors.New("read-only transaction cannot write")
	// ErrTxnCancelled is returned when a transaction's context was cancelled by
	// the caller (as opposed to expiring via deadline, which is ErrTxnTimeout).
	// It wraps context.Canceled so errors.Is(err, context.Canceled) works.
	ErrTxnCancelled = fmt.Errorf("transaction cancelled: %w", context.Canceled)
)

const maxTxnWALRecordDataBytes = 1<<16 - 1 // storage WAL payload length is uint16

func txnWALRecordDataLen(keyLen, valueLen int) (int, error) {
	if keyLen < 0 {
		return 0, fmt.Errorf("WAL key length must be non-negative: %d", keyLen)
	}
	if valueLen < 0 {
		return 0, fmt.Errorf("WAL value length must be non-negative: %d", valueLen)
	}
	if keyLen > maxTxnWALRecordDataBytes-4 {
		return 0, fmt.Errorf("WAL record data size exceeds maximum (%d bytes): key length %d",
			maxTxnWALRecordDataBytes, keyLen)
	}
	if valueLen > maxTxnWALRecordDataBytes-4-keyLen {
		return 0, fmt.Errorf("WAL record data size exceeds maximum (%d bytes): key length %d, value length %d",
			maxTxnWALRecordDataBytes, keyLen, valueLen)
	}
	return 4 + keyLen + valueLen, nil
}

// walDataPool reuses small byte buffers for the WAL fast path, eliminating
// one heap allocation per single-write transaction commit.
var walDataPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 256)
		return &b
	},
}

// IsolationLevel represents transaction isolation levels
type IsolationLevel uint8

const (
	ReadCommitted     IsolationLevel = 0x01
	SnapshotIsolation IsolationLevel = 0x02 // Default
	Serializable      IsolationLevel = 0x03
)

// TxnState represents transaction states
type TxnState uint8

const (
	TxnActive    TxnState = 0x01
	TxnCommitted TxnState = 0x02
	TxnAborted   TxnState = 0x03
)

// Options contains transaction options
type Options struct {
	Isolation       IsolationLevel
	ReadOnly        bool
	Timeout         time.Duration // Transaction timeout (0 = no timeout)
	LockWaitTimeout time.Duration // Max time to wait for a lock (0 = default 5s)
}

// defaultOptions is the shared read-only default configuration.
// Manager.Begin/BeginWithContext use it directly when opts is nil,
// avoiding a heap allocation on every transaction start.
var defaultOptions = Options{
	Isolation:       SnapshotIsolation,
	ReadOnly:        false,
	Timeout:         0,               // No default timeout
	LockWaitTimeout: 5 * time.Second, // 5 second lock wait timeout
}

// DefaultOptions returns a newly allocated copy of the default transaction
// options for callers that need to mutate the result.
func DefaultOptions() *Options {
	cp := defaultOptions
	return &cp
}

// WriteKey is a composite key that avoids string concatenation allocations.
type WriteKey struct {
	TreeName string
	Key      string
}

// Transaction represents a database transaction
type Transaction struct {
	ID        uint64
	State     TxnState
	Isolation IsolationLevel
	ReadOnly  bool
	StartTS   uint64
	ReadSet   map[WriteKey]uint64 // key → version read
	WriteSet  map[WriteKey][]byte // key → new value (buffered writes)
	mu        sync.Mutex
	manager   *Manager

	// Deadlock detection and timeout fields
	ctx        context.Context    // Transaction context for timeout/cancellation
	cancel     context.CancelFunc // Cancel function for cleanup
	waitingFor uint64             // Transaction ID this txn is waiting for (deadlock detection)
	locksHeld  map[string]bool    // Keys this transaction currently holds locks on

	// beginSeq is the value of Manager.commitSeq observed when this transaction
	// began. Any version with commitSeq <= beginSeq was committed before this
	// transaction started, so a read of that key by this transaction records a
	// readVersion >= that version and can never conflict on it. pruneVersions
	// uses the minimum beginSeq across active transactions as its watermark.
	beginSeq uint64

	// lockWaitTimeout is the default lock wait applied by AcquireLockMode when
	// the caller passes timeout <= 0. Copied from Options at Begin.
	lockWaitTimeout time.Duration
}

// SetWaitingFor sets which transaction this one is waiting for (deadlock detection)
func (t *Transaction) SetWaitingFor(txnID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.waitingFor = txnID
}

// setWaitingForID sets the wait-for edge only if the transaction still has the
// expected ID. Transactions are recycled through a sync.Pool, so a stale
// pointer may refer to a different (re-begun) transaction; the ID check
// prevents corrupting the new incarnation's deadlock-detection state.
func (t *Transaction) setWaitingForID(expectID, txnID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ID != expectID {
		return
	}
	t.waitingFor = txnID
}

// GetWaitingFor returns which transaction this one is waiting for
func (t *Transaction) GetWaitingFor() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.waitingFor
}

// AddLockHeld records that this transaction holds a lock on a key
func (t *Transaction) AddLockHeld(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.locksHeld == nil {
		t.locksHeld = make(map[string]bool)
	}
	t.locksHeld[key] = true
}

// AddLockHeldIfActive records a lock only while the transaction is still active.
func (t *Transaction) AddLockHeldIfActive(key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.addLockHeldIfActiveLocked(key)
}

// addLockHeldIfActiveID records a lock only while the transaction is still
// active AND still has the expected ID. Because transactions are recycled via
// a sync.Pool, a pointer captured before a wait can refer to a different,
// newer transaction by the time the lock is granted; without the ID check a
// lock could be recorded against (and a lock-table entry granted to) a dead
// transaction ID, orphaning the entry forever.
func (t *Transaction) addLockHeldIfActiveID(expectID uint64, key string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ID != expectID {
		return ErrTxnNotFound
	}
	return t.addLockHeldIfActiveLocked(key)
}

func (t *Transaction) addLockHeldIfActiveLocked(key string) error {
	switch t.State {
	case TxnActive:
	case TxnCommitted:
		return ErrTxnCommitted
	case TxnAborted:
		return ErrTxnAborted
	default:
		return ErrTxnNotFound
	}
	if t.locksHeld == nil {
		t.locksHeld = make(map[string]bool)
	}
	t.locksHeld[key] = true
	return nil
}

// RemoveLockHeld removes a lock record
func (t *Transaction) RemoveLockHeld(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.locksHeld, key)
}

// removeLockHeldIfID removes a lock record only if the transaction still has
// the expected ID (guards against sync.Pool recycling, see addLockHeldIfActiveID).
func (t *Transaction) removeLockHeldIfID(expectID uint64, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ID != expectID {
		return
	}
	delete(t.locksHeld, key)
}

// GetLocksHeld returns all keys this transaction holds locks on
func (t *Transaction) GetLocksHeld() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := make([]string, 0, len(t.locksHeld))
	for k := range t.locksHeld {
		keys = append(keys, k)
	}
	return keys
}

// Context returns the transaction context (for timeout/cancellation)
func (t *Transaction) Context() context.Context {
	return t.ctx
}

// IsTimedOut returns true if the transaction has exceeded its timeout
func (t *Transaction) IsTimedOut() bool {
	if t.ctx == nil {
		return false
	}
	return t.ctx.Err() != nil
}

// activeStateErrorForID returns nil if the transaction is active and still has
// the expected ID. A mismatched ID means the *Transaction was recycled through
// the sync.Pool and now belongs to a different logical transaction, so the
// original transaction is gone (ErrTxnNotFound).
func (t *Transaction) activeStateErrorForID(expectID uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.ID != expectID {
		return ErrTxnNotFound
	}
	switch t.State {
	case TxnActive:
		return nil
	case TxnCommitted:
		return ErrTxnCommitted
	case TxnAborted:
		return ErrTxnAborted
	default:
		return ErrTxnNotFound
	}
}

// Commit commits the transaction
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.State == TxnCommitted {
		return ErrTxnCommitted
	}
	if t.State == TxnAborted {
		return ErrTxnAborted
	}

	// Check for timeout / cancellation. Deadline expiry is a timeout; explicit
	// caller cancellation is reported distinctly so callers can tell "the
	// transaction ran too long" apart from "I cancelled it".
	if t.ctx != nil {
		if ctxErr := t.ctx.Err(); ctxErr != nil {
			_ = t.rollbackLocked()
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				metrics.GetTransactionMetrics().RecordTxnTimeout()
				return ErrTxnTimeout
			}
			return ErrTxnCancelled
		}
	}

	startTime := time.Now()

	// Apply writes atomically with conflict detection so no transaction
	// can sneak in between the read-check and the write.
	if err := t.manager.commitWithConflictDetection(t); err != nil {
		// The transaction is active here, so rollbackLocked cannot fail: its only
		// error case is an already-committed transaction, rejected above.
		_ = t.rollbackLocked()
		return err
	}

	t.State = TxnCommitted

	// Release all locks held by this transaction using the key list available
	// under the already-held t.mu. Calling ReleaseAllLocks(t.ID) here would not
	// work: it looks the transaction up in the active set (already racy once we
	// removeActive below) and its found-path calls GetLocksHeld, which would
	// self-deadlock on t.mu. The old code always fell through to ReleaseAllLocks'
	// O(entire-lock-table) fallback scan on every commit.
	if len(t.locksHeld) > 0 {
		locks := make([]string, 0, len(t.locksHeld))
		for k := range t.locksHeld {
			locks = append(locks, k)
		}
		clear(t.locksHeld)
		t.manager.releaseLockEntries(t.ID, locks)
	}

	t.manager.removeActive(t.ID)

	// Cancel context to release resources
	if t.cancel != nil {
		t.cancel()
	}

	// Record metrics
	recordTxnCommitMetrics(time.Since(startTime))

	// Periodically prune versions map to prevent unbounded memory growth
	if t.manager.commitCount.Add(1)%1000 == 0 {
		t.manager.pruneVersions()
	}

	return nil
}

func recordTxnCommitMetrics(duration time.Duration) {
	metrics.GetTransactionMetrics().RecordTxnCommit(duration)
	if duration > time.Second {
		metrics.GetTransactionMetrics().RecordLongRunningTxn()
	}
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.rollbackLocked()
}

// rollbackLocked performs rollback while caller already holds t.mu
func (t *Transaction) rollbackLocked() error {
	if t.State == TxnCommitted {
		return ErrTxnCommitted
	}
	if t.State == TxnAborted {
		return nil // Already aborted
	}

	// Get locks and manager reference before clearing state
	locks := make([]string, 0, len(t.locksHeld))
	for k := range t.locksHeld {
		locks = append(locks, k)
	}
	clear(t.locksHeld)
	mgr := t.manager

	t.State = TxnAborted
	// Clear maps so the backing storage can be reused by sync.Pool.
	clear(t.WriteSet)
	clear(t.ReadSet)

	// Release all locks under a single lockMu acquisition. releaseLockEntries
	// acquires lockMu internally and never touches t.mu, so it is safe to call
	// with t.mu held (the global lock order is t.mu → lockMu; no code path
	// acquires t.mu while holding lockMu).
	if len(locks) > 0 {
		mgr.releaseLockEntries(t.ID, locks)
	}

	mgr.removeActive(t.ID)

	// Cancel context to release resources
	if t.cancel != nil {
		t.cancel()
	}

	// Record metrics
	metrics.GetTransactionMetrics().RecordTxnAbort()

	return nil
}

// Recycle returns the transaction to its Manager's sync.Pool for reuse.
// It is safe to call multiple times — subsequent calls are no-ops.
func (t *Transaction) Recycle() {
	if t.manager != nil {
		t.manager.RecycleTxn(t)
	}
}

// GetReadVersion returns the version read for a key
func (t *Transaction) GetReadVersion(treeName, key string) (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var wk WriteKey
	wk.TreeName = treeName
	wk.Key = key
	v, ok := t.ReadSet[wk]
	return v, ok
}

// SetReadVersion records a read version for a key
func (t *Transaction) SetReadVersion(treeName, key string, version uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.ReadSet == nil {
		t.ReadSet = make(map[WriteKey]uint64)
	}
	var wk WriteKey
	wk.TreeName = treeName
	wk.Key = key
	t.ReadSet[wk] = version
}

// GetWrite returns the buffered write for a key
func (t *Transaction) GetWrite(treeName, key string) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var wk WriteKey
	wk.TreeName = treeName
	wk.Key = key
	v, ok := t.WriteSet[wk]
	return cloneBytes(v), ok
}

// SetWrite buffers a write for a key
func (t *Transaction) SetWrite(treeName, key string, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.WriteSet == nil {
		t.WriteSet = make(map[WriteKey][]byte)
	}
	var wk WriteKey
	wk.TreeName = treeName
	wk.Key = key
	t.WriteSet[wk] = cloneBytes(value)
}

// RemoveWrite drops a buffered write from the WriteSet. Used by savepoint
// rollback so a key written only after the savepoint is not WAL-logged or
// version-published at commit (otherwise a rolled-back row would resurrect on
// crash recovery).
func (t *Transaction) RemoveWrite(treeName, key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.WriteSet == nil {
		return
	}
	delete(t.WriteSet, WriteKey{TreeName: treeName, Key: key})
}

const numVersionShards = 256
const numActiveShards = 64

type versionShard struct {
	mu       sync.Mutex
	versions map[WriteKey]uint64
}

type activeShard struct {
	sync.RWMutex
	m map[uint64]*Transaction
}

func activeShardIdx(id uint64) int {
	return int(id & (numActiveShards - 1))
}

func (m *Manager) activeTxn(txnID uint64) (*Transaction, bool) {
	shard := activeShardIdx(txnID)
	m.activeShards[shard].RLock()
	txn, exists := m.activeShards[shard].m[txnID]
	m.activeShards[shard].RUnlock()
	return txn, exists
}

// versionShardIdx returns a deterministic shard index for the given treeName+key
// without allocating. It implements FNV-1a 32-bit inline.
func versionShardIdx(treeName, key string) int {
	var h uint32 = 2166136261
	for i := 0; i < len(treeName); i++ {
		h ^= uint32(treeName[i])
		h *= 16777619
	}
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h & 0xFF)
}

// Manager manages all transactions
type Manager struct {
	counter       uint64 // atomic, monotonic transaction IDs
	commitSeq     uint64 // atomic, monotonic commit sequence numbers (used for versions)
	activeShards  [numActiveShards]activeShard
	versionShards [numVersionShards]versionShard
	versionStore  *VersionStore // MVCC version chain storage
	commitCount   atomic.Int64
	wal           interface{} // WAL

	// Deadlock detection
	deadlockCheckInterval time.Duration
	stopDeadlockDetector  chan struct{}
	deadlockDetectorOnce  sync.Once
	stopOnce              sync.Once

	// Lock management for deadlock detection
	lockEntries map[string]*lockEntry // key → lock state (shared + exclusive)
	lockMu      sync.RWMutex

	// Transaction recycling pool to eliminate per-txn heap allocations.
	txnPool sync.Pool
}

type lockEntry struct {
	shared    map[uint64]bool // txnIDs holding shared locks
	exclusive uint64          // txnID holding exclusive lock (0 if none)
}

// NewManager creates a new transaction manager
func NewManager(wal interface{}) *Manager {
	m := &Manager{
		versionStore:          NewVersionStore(),
		wal:                   wal,
		deadlockCheckInterval: 100 * time.Millisecond, // Check every 100ms
		stopDeadlockDetector:  make(chan struct{}),
		lockEntries:           make(map[string]*lockEntry),
	}
	for i := range m.versionShards {
		m.versionShards[i].versions = make(map[WriteKey]uint64, 128)
	}
	for i := range m.activeShards {
		m.activeShards[i].m = make(map[uint64]*Transaction, 64)
	}
	return m
}

// Start starts the deadlock detector background goroutine
func (m *Manager) Start() {
	m.deadlockDetectorOnce.Do(func() {
		go m.deadlockDetector()
	})
}

// Stop stops the deadlock detector
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopDeadlockDetector)
	})
}

// deadlockDetector runs periodically to detect and resolve deadlocks
func (m *Manager) deadlockDetector() {
	ticker := time.NewTicker(m.deadlockCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkForDeadlocks()
		case <-m.stopDeadlockDetector:
			return
		}
	}
}

// checkForDeadlocks detects cycles in the wait-for graph and aborts transactions to break them
func (m *Manager) checkForDeadlocks() {
	// Snapshot active transaction pointers under the shard locks, but read the
	// t.mu-guarded waiting state only AFTER releasing the shard lock. Holding a
	// shard RLock while acquiring t.mu deadlocks against Commit/rollbackLocked,
	// which hold t.mu while calling removeActive (shard write lock).
	activeTxns := make(map[uint64]*Transaction)
	for i := range m.activeShards {
		m.activeShards[i].RLock()
		for id, txn := range m.activeShards[i].m {
			activeTxns[id] = txn
		}
		m.activeShards[i].RUnlock()
	}

	waitingMap := make(map[uint64]uint64, len(activeTxns))
	for id, txn := range activeTxns {
		// Validate the ID under t.mu: the pointer may have been recycled through
		// the sync.Pool between the snapshot and this read.
		txn.mu.Lock()
		w := txn.waitingFor
		if txn.ID != id {
			w = 0
		}
		txn.mu.Unlock()
		waitingMap[id] = w
	}

	// Find a cycle in the wait-for graph and break it by aborting the youngest
	// transaction on the cycle. Only one deadlock is resolved per pass.
	if cycle := findWaitCycle(waitingMap); len(cycle) > 0 {
		m.resolveDeadlock(cycle, activeTxns)
	}
}

// findWaitCycle returns the transaction IDs that form a cycle in the wait-for
// graph (txn -> the txn it is blocked on), or nil if there is none. Only nodes
// that lie ON the cycle are returned: transactions on a path leading INTO the
// cycle are excluded, so resolveDeadlock always picks a victim that is actually
// part of the deadlock. A waiting target of 0 means "not waiting" (a root).
func findWaitCycle(waitingMap map[uint64]uint64) []uint64 {
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)
	var cycle []uint64

	var dfs func(uint64) bool
	dfs = func(txnID uint64) bool {
		visited[txnID] = true
		recStack[txnID] = true

		if waitingFor := waitingMap[txnID]; waitingFor != 0 {
			if !visited[waitingFor] {
				if dfs(waitingFor) {
					return true
				}
			} else if recStack[waitingFor] {
				// The back-edge txnID -> waitingFor closes a cycle. waitingFor is an
				// ancestor on the current DFS path, so walking waitingMap from
				// waitingFor reaches txnID again; those nodes are exactly the cycle.
				cycle = cycle[:0]
				for n := waitingFor; ; n = waitingMap[n] {
					cycle = append(cycle, n)
					if n == txnID {
						return true
					}
				}
			}
		}

		recStack[txnID] = false
		return false
	}

	for txnID := range waitingMap {
		if !visited[txnID] {
			if dfs(txnID) {
				return cycle
			}
		}
	}
	return nil
}

// resolveDeadlock aborts the youngest transaction in the cycle
func (m *Manager) resolveDeadlock(cycle []uint64, activeTxns map[uint64]*Transaction) {
	if len(cycle) == 0 {
		return
	}

	// Record deadlock detection
	metrics.GetTransactionMetrics().RecordDeadlock()

	// Find the youngest transaction (highest ID) to abort
	var victimID uint64 = 0
	var maxStartTS uint64 = 0

	for _, txnID := range cycle {
		if txn, ok := activeTxns[txnID]; ok {
			txn.mu.Lock()
			startTS := txn.StartTS
			// Guard against sync.Pool recycling: the snapshot pointer may now
			// belong to a different (newer) transaction.
			if txn.ID != txnID {
				startTS = 0
			}
			txn.mu.Unlock()
			if startTS > maxStartTS {
				maxStartTS = startTS
				victimID = txnID
			}
		}
	}

	if victimID != 0 {
		// Re-fetch by ID from the active set rather than trusting the snapshot
		// pointer, then re-validate ID and state under t.mu before aborting.
		// A stale recycled pointer would otherwise abort an unrelated, newer
		// transaction (sync.Pool ABA).
		if victim, ok := m.activeTxn(victimID); ok {
			victim.mu.Lock()
			if victim.ID == victimID && victim.State == TxnActive {
				victim.waitingFor = 0       // Clear waiting state
				_ = victim.rollbackLocked() // Abort the victim
			}
			victim.mu.Unlock()
		}
	}
}

// AcquireLock acquires an exclusive lock (backward compatible).
func (m *Manager) AcquireLock(txnID uint64, key string, timeout time.Duration) error {
	return m.AcquireLockMode(txnID, key, LockExclusive, timeout)
}

// AcquireLockMode acquires a lock in the specified mode (shared or exclusive).
// A timeout <= 0 means "use the transaction's configured LockWaitTimeout"
// (Options.LockWaitTimeout, default 5s) — it does NOT mean fail immediately.
func validateLockWaiter(txn *Transaction, txnID uint64) error {
	if err := txn.activeStateErrorForID(txnID); err != nil {
		txn.setWaitingForID(txnID, 0)
		return err
	}
	return nil
}

func (m *Manager) recordGrantedLock(txn *Transaction, txnID uint64, key string) error {
	if err := txn.addLockHeldIfActiveID(txnID, key); err != nil {
		m.ReleaseLock(txnID, key)
		return err
	}
	return nil
}

func (m *Manager) AcquireLockMode(txnID uint64, key string, mode LockMode, timeout time.Duration) error {
	txn, exists := m.activeTxn(txnID)
	if !exists {
		return ErrTxnNotFound
	}
	if txn.IsTimedOut() {
		return ErrTxnTimeout
	}

	if timeout <= 0 {
		// lockWaitTimeout is immutable after Begin; the activeTxn lookup above
		// provides the happens-before edge for reading it here.
		timeout = txn.lockWaitTimeout
		if timeout <= 0 {
			timeout = defaultOptions.LockWaitTimeout
		}
	}

	m.lockMu.Lock()

	entry := m.lockEntries[key]
	if entry == nil {
		entry = &lockEntry{shared: make(map[uint64]bool)}
		m.lockEntries[key] = entry
	}

	// Check if we can acquire immediately
	canAcquire := false
	switch mode {
	case LockShared:
		// Shared lock: allowed if no exclusive lock, or we hold the exclusive lock
		if entry.exclusive == 0 || entry.exclusive == txnID {
			canAcquire = true
		}
	case LockExclusive:
		// Exclusive lock: allowed if no holders, or we already hold exclusive
		if entry.exclusive == txnID {
			canAcquire = true
		} else if entry.exclusive == 0 && len(entry.shared) == 0 {
			canAcquire = true
		} else if len(entry.shared) == 1 && entry.shared[txnID] {
			// Upgrade: we hold the only shared lock → exclusive
			canAcquire = true
		}
	}

	if canAcquire {
		switch mode {
		case LockShared:
			entry.shared[txnID] = true
		case LockExclusive:
			entry.exclusive = txnID
		}
		m.lockMu.Unlock()

		return m.recordGrantedLock(txn, txnID, key)
	}

	// Determine who is blocking us
	blockerID := lockEntryBlocker(entry, txnID)
	m.lockMu.Unlock()

	// The wait-for graph is guarded by per-transaction mutexes, not lockMu, so
	// this check (and SetWaitingFor below) must run after lockMu is released:
	// the global lock order is t.mu → lockMu (Commit and rollbackLocked hold
	// t.mu while releasing lock entries), and wouldCauseDeadlock acquires t.mu
	// of chain members. The check is advisory — the background deadlock
	// detector remains the backstop for edges formed after this point.
	if m.wouldCauseDeadlock(txnID, blockerID) {
		return ErrDeadlockDetected
	}

	// Wait for the lock with timeout
	if _, ok := m.activeTxn(blockerID); ok {
		txn.setWaitingForID(txnID, blockerID)
	}
	if err := validateLockWaiter(txn, txnID); err != nil {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var txnDone <-chan struct{}
	if ctx := txn.Context(); ctx != nil {
		txnDone = ctx.Done()
	}

	for {
		select {
		case <-txnDone:
			txn.setWaitingForID(txnID, 0)
			return ErrTxnTimeout
		case <-timer.C:
			txn.setWaitingForID(txnID, 0)
			return fmt.Errorf("lock acquisition timeout")
		case <-ticker.C:
			if err := txn.activeStateErrorForID(txnID); err != nil {
				txn.setWaitingForID(txnID, 0)
				return err
			}
			m.lockMu.Lock()
			e := m.lockEntries[key]
			if e == nil {
				e = &lockEntry{shared: make(map[uint64]bool)}
				m.lockEntries[key] = e
			}
			// Re-check if we can acquire
			ok := false
			switch mode {
			case LockShared:
				if e.exclusive == 0 || e.exclusive == txnID {
					ok = true
				}
			case LockExclusive:
				if e.exclusive == txnID || (e.exclusive == 0 && len(e.shared) == 0) || (len(e.shared) == 1 && e.shared[txnID]) {
					ok = true
				}
			}
			if ok {
				switch mode {
				case LockShared:
					e.shared[txnID] = true
				case LockExclusive:
					e.exclusive = txnID
				}
				m.lockMu.Unlock()
				txn.setWaitingForID(txnID, 0)
				return m.recordGrantedLock(txn, txnID, key)
			}
			// Still blocked: refresh the wait-for edge. The blocker recorded at
			// wait entry may have committed/aborted, with a different transaction
			// now holding the lock; a stale edge makes the deadlock detector chase
			// the wrong node and miss real deadlocks.
			newBlocker := lockEntryBlocker(e, txnID)
			m.lockMu.Unlock()
			txn.setWaitingForID(txnID, newBlocker)
		}
	}
}

// lockEntryBlocker returns the ID of a transaction blocking txnID on the
// given lock entry: the exclusive holder if any, otherwise any shared holder
// other than txnID, otherwise 0. Caller must hold lockMu.
func lockEntryBlocker(entry *lockEntry, txnID uint64) uint64 {
	if entry.exclusive != 0 && entry.exclusive != txnID {
		return entry.exclusive
	}
	for id := range entry.shared {
		if id != txnID {
			return id
		}
	}
	return 0
}

// wouldCauseDeadlock checks if txnID waiting for ownerID would create a cycle
func (m *Manager) wouldCauseDeadlock(txnID, ownerID uint64) bool {
	visited := make(map[uint64]bool)
	current := ownerID

	for {
		if current == txnID {
			// Found a cycle back to the original transaction
			return true
		}
		if visited[current] {
			// Already visited this node, no cycle
			return false
		}
		visited[current] = true

		txn, exists := m.activeTxn(current)
		if !exists {
			return false
		}

		// Read the edge with an ID re-validation: the pointer may have been
		// recycled (sync.Pool) into a different transaction after the lookup.
		txn.mu.Lock()
		waitingFor := txn.waitingFor
		if txn.ID != current {
			waitingFor = 0
		}
		txn.mu.Unlock()
		if waitingFor == 0 {
			// End of chain
			return false
		}
		current = waitingFor
	}
}

// ReleaseLock releases a lock held by a transaction
func (m *Manager) ReleaseLock(txnID uint64, key string) {
	m.lockMu.Lock()

	entry := m.lockEntries[key]
	if entry == nil {
		m.lockMu.Unlock()
		return
	}

	released := false
	if entry.exclusive == txnID {
		entry.exclusive = 0
		released = true
	}
	if entry.shared[txnID] {
		delete(entry.shared, txnID)
		released = true
	}

	if released && entry.exclusive == 0 && len(entry.shared) == 0 {
		delete(m.lockEntries, key)
	}
	// Update the transaction's locksHeld map only after releasing lockMu:
	// acquiring t.mu while holding lockMu would invert the global lock order
	// (t.mu → lockMu, see Commit/rollbackLocked) and allow an ABBA deadlock.
	m.lockMu.Unlock()

	if released {
		if txn, exists := m.activeTxn(txnID); exists {
			txn.removeLockHeldIfID(txnID, key)
		}
	}
}

// ReleaseAllLocks releases all locks held by a transaction
func (m *Manager) ReleaseAllLocks(txnID uint64) {
	shard := activeShardIdx(txnID)
	m.activeShards[shard].RLock()
	txn, exists := m.activeShards[shard].m[txnID]
	m.activeShards[shard].RUnlock()

	if !exists {
		m.lockMu.Lock()
		for key, entry := range m.lockEntries {
			if entry.exclusive == txnID {
				entry.exclusive = 0
			}
			delete(entry.shared, txnID)
			if entry.exclusive == 0 && len(entry.shared) == 0 {
				delete(m.lockEntries, key)
			}
		}
		m.lockMu.Unlock()
		return
	}

	locks := txn.GetLocksHeld()
	for _, key := range locks {
		m.ReleaseLock(txnID, key)
	}
}

// releaseLockEntries releases the lock-table entries for txnID for the given
// keys under a single lockMu acquisition. It never touches a transaction's mu,
// so callers may hold t.mu (Commit / rollbackLocked do); the caller is
// responsible for clearing the transaction's own locksHeld map.
//
// This replaces the old releaseAllLocksUnderLock, whose contract said "caller
// must hold lockMu" but whose only caller never acquired it — mutating
// m.lockEntries unsynchronized against AcquireLockMode/ReleaseLock.
func (m *Manager) releaseLockEntries(txnID uint64, keys []string) {
	if len(keys) == 0 {
		return
	}
	m.lockMu.Lock()
	defer m.lockMu.Unlock()
	for _, key := range keys {
		entry := m.lockEntries[key]
		if entry == nil {
			continue
		}
		if entry.exclusive == txnID {
			entry.exclusive = 0
		}
		delete(entry.shared, txnID)
		if entry.exclusive == 0 && len(entry.shared) == 0 {
			delete(m.lockEntries, key)
		}
	}
}

// RecycleTxn resets a transaction and returns it to the pool for reuse.
func (m *Manager) RecycleTxn(txn *Transaction) {
	if txn == nil {
		return
	}

	txn.mu.Lock()
	if txn.manager == nil {
		txn.mu.Unlock()
		return
	}

	// Fields that Begin overwrites are left as-is so that post-commit/rollback
	// state inspection in tests and callers remains valid.
	txn.manager = nil
	cancel := txn.cancel
	txn.cancel = nil
	txn.ctx = nil
	txn.waitingFor = 0
	if txn.locksHeld != nil {
		clear(txn.locksHeld)
	}
	if txn.ReadSet != nil {
		clear(txn.ReadSet)
	}
	if txn.WriteSet != nil {
		clear(txn.WriteSet)
	}
	txn.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.txnPool.Put(txn)
}

// acquireTxn retrieves a pooled Transaction or allocates a new one.
func (m *Manager) acquireTxn() *Transaction {
	if v := m.txnPool.Get(); v != nil {
		return v.(*Transaction)
	}
	return &Transaction{
		// Pre-allocate maps with capacity 1 for the common single-read/write
		// case. This avoids a separate bucket allocation on first insert.
		ReadSet:  make(map[WriteKey]uint64, 1),
		WriteSet: make(map[WriteKey][]byte, 1),
	}
}

// Begin starts a new transaction
func (m *Manager) Begin(opts *Options) *Transaction {
	if opts == nil {
		opts = &defaultOptions
	}

	id := atomic.AddUint64(&m.counter, 1)

	// Create context with timeout if specified
	var cancel func()
	ctx := context.Background()
	if opts.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
	}

	txn := m.initTxn(id, ctx, cancel, opts)

	// Record metrics
	metrics.GetTransactionMetrics().RecordTxnStart()

	return txn
}

// initTxn initializes a (possibly pool-recycled) transaction and publishes it
// in the active set. Fields are set under txn.mu so that ID re-validation by
// holders of stale recycled pointers (see setWaitingForID and friends) is
// well-defined under the race detector.
func (m *Manager) initTxn(id uint64, ctx context.Context, cancel context.CancelFunc, opts *Options) *Transaction {
	txn := m.acquireTxn()
	txn.mu.Lock()
	txn.ID = id
	txn.State = TxnActive
	txn.Isolation = opts.Isolation
	txn.ReadOnly = opts.ReadOnly
	txn.StartTS = id
	// Snapshot base for pruneVersions' watermark; see Transaction.beginSeq.
	txn.beginSeq = atomic.LoadUint64(&m.commitSeq)
	txn.lockWaitTimeout = opts.LockWaitTimeout
	txn.manager = m
	txn.ctx = ctx
	txn.cancel = cancel
	txn.mu.Unlock()

	shard := activeShardIdx(id)
	m.activeShards[shard].Lock()
	m.activeShards[shard].m[id] = txn
	m.activeShards[shard].Unlock()

	return txn
}

// BeginWithContext starts a new transaction with a user-provided context
func (m *Manager) BeginWithContext(ctx context.Context, opts *Options) *Transaction {
	if opts == nil {
		opts = &defaultOptions
	}

	id := atomic.AddUint64(&m.counter, 1)

	// Use provided context, optionally with timeout
	var cancel func()
	if opts.Timeout > 0 {
		var c context.CancelFunc
		ctx, c = context.WithTimeout(ctx, opts.Timeout)
		cancel = c
	}

	return m.initTxn(id, ctx, cancel, opts)
}

// commitWithConflictDetection atomically checks for conflicts, durably writes
// the WAL, then publishes version state. WAL must succeed before versions are
// visible; otherwise a failed commit can leave stale conflict state behind.
func (m *Manager) commitWithConflictDetection(txn *Transaction) error {
	if txn.ReadOnly && len(txn.WriteSet) > 0 {
		return ErrReadOnlyTxn
	}

	// Fast path: single-shard transaction (the common case for single-row
	// INSERT, UPDATE, DELETE). Avoids shard-array collection, sorting,
	// and multi-shard lock/unlock loops.
	if len(txn.WriteSet) == 1 {
		var writeWk WriteKey
		for wk := range txn.WriteSet {
			writeWk = wk
		}
		shard := versionShardIdx(writeWk.TreeName, writeWk.Key)

		if len(txn.ReadSet) == 0 {
			m.versionShards[shard].mu.Lock()
			if err := m.writeWALForCommit(txn); err != nil {
				m.versionShards[shard].mu.Unlock()
				return err
			}
			seq := atomic.AddUint64(&m.commitSeq, 1)
			m.versionShards[shard].versions[writeWk] = seq
			m.versionShards[shard].mu.Unlock()
			return nil
		}

		if len(txn.ReadSet) == 1 {
			var readWk WriteKey
			var readVersion uint64
			for wk, rv := range txn.ReadSet {
				readWk = wk
				readVersion = rv
			}
			if readWk == writeWk {
				m.versionShards[shard].mu.Lock()
				if txn.Isolation >= SnapshotIsolation {
					currentVersion, exists := m.versionShards[shard].versions[readWk]
					if exists && currentVersion > readVersion {
						m.versionShards[shard].mu.Unlock()
						return ErrConflict
					}
				}
				if err := m.writeWALForCommit(txn); err != nil {
					m.versionShards[shard].mu.Unlock()
					return err
				}
				seq := atomic.AddUint64(&m.commitSeq, 1)
				m.versionShards[shard].versions[writeWk] = seq
				m.versionShards[shard].mu.Unlock()
				return nil
			}
		}
	}

	// 1. Collect all version shards touched by this transaction.
	// Use a small stack array for the common case (1-2 shards) to avoid a
	// map allocation per commit.
	var shardArr [8]int
	var shardExtra []int
	shardCount := 0
	addShard := func(s int) {
		for i := 0; i < shardCount; i++ {
			if shardArr[i] == s {
				return
			}
		}
		for i := range shardExtra {
			if shardExtra[i] == s {
				return
			}
		}
		if shardCount < len(shardArr) {
			shardArr[shardCount] = s
			shardCount++
		} else {
			shardExtra = append(shardExtra, s)
		}
	}
	for wk := range txn.ReadSet {
		addShard(versionShardIdx(wk.TreeName, wk.Key))
	}
	for wk := range txn.WriteSet {
		addShard(versionShardIdx(wk.TreeName, wk.Key))
	}
	var sorted []int
	if len(shardExtra) == 0 {
		sorted = shardArr[:shardCount]
	} else {
		sorted = make([]int, 0, shardCount+len(shardExtra))
		sorted = append(sorted, shardArr[:shardCount]...)
		sorted = append(sorted, shardExtra...)
	}
	sort.Ints(sorted)

	// 2. Lock shards in order to avoid deadlocks.
	for _, s := range sorted {
		m.versionShards[s].mu.Lock()
	}

	// 3. Conflict detection.
	if txn.Isolation >= SnapshotIsolation {
		for wk, readVersion := range txn.ReadSet {
			currentVersion, exists := m.versionShards[versionShardIdx(wk.TreeName, wk.Key)].versions[wk]
			if !exists {
				continue
			}
			if currentVersion > readVersion {
				for i := len(sorted) - 1; i >= 0; i-- {
					m.versionShards[sorted[i]].mu.Unlock()
				}
				return ErrConflict
			}
		}
	}

	// 4. WAL durability. Keep version locks held until WAL succeeds so failed
	// commits cannot publish transient versions into conflict detection state.
	if err := m.writeWALForCommit(txn); err != nil {
		for i := len(sorted) - 1; i >= 0; i-- {
			m.versionShards[sorted[i]].mu.Unlock()
		}
		return err
	}

	// 5. Update versions. VersionStore is write-only in production (no callers
	// of GetAtSnapshot / GetCurrent / GetLatestVersion outside tests), so we
	// skip the per-commit allocation entirely.
	seq := atomic.AddUint64(&m.commitSeq, 1)
	for wk := range txn.WriteSet {
		m.versionShards[versionShardIdx(wk.TreeName, wk.Key)].versions[wk] = seq
	}

	for i := len(sorted) - 1; i >= 0; i-- {
		m.versionShards[sorted[i]].mu.Unlock()
	}

	return nil
}

// writeWALForCommit writes WAL records for a committed transaction.
// It is extracted so that both the single-shard fast path and the general
// path can share the same WAL logic without a goto.
func (m *Manager) writeWALForCommit(txn *Transaction) error {
	if m.wal == nil {
		return nil
	}
	wal, ok := m.wal.(*storage.WAL)
	if !ok || wal == nil {
		return nil
	}
	// Fast path: single-write transaction with stack-allocated records.
	// This avoids two heap-allocated WALRecord structs for the common case.
	if len(txn.WriteSet) == 1 {
		var recArr [2]storage.WALRecord
		var records [2]*storage.WALRecord
		var walDataBuf *[]byte
		for wk, value := range txn.WriteSet {
			tnLen := len(wk.TreeName)
			kLen := len(wk.Key)
			totalKeyLen := tnLen + 1 + kLen
			need, err := txnWALRecordDataLen(totalKeyLen, len(value))
			if err != nil {
				return err
			}
			var data []byte
			if need <= 256 {
				walDataBuf = walDataPool.Get().(*[]byte)
				data = (*walDataBuf)[:need]
				binary.LittleEndian.PutUint32(data[0:4], uint32(totalKeyLen)) // #nosec G115 -- txnWALRecordDataLen above bounds totalKeyLen to <= 65531.
				copy(data[4:4+tnLen], wk.TreeName)
				data[4+tnLen] = ':'
				copy(data[4+tnLen+1:], wk.Key)
				copy(data[4+totalKeyLen:], value)
			} else {
				data = make([]byte, need)
				binary.LittleEndian.PutUint32(data[0:4], uint32(totalKeyLen)) // #nosec G115 -- txnWALRecordDataLen above bounds totalKeyLen to <= 65531.
				copy(data[4:4+tnLen], wk.TreeName)
				data[4+tnLen] = ':'
				copy(data[4+tnLen+1:], wk.Key)
				copy(data[4+totalKeyLen:], value)
			}

			recArr[0] = storage.WALRecord{
				TxnID: txn.ID,
				Type:  storage.WALUpdateCommit,
				Data:  data,
			}
			records[0] = &recArr[0]
		}
		if err := wal.AppendBatch(records[:1]); err != nil {
			if walDataBuf != nil {
				walDataPool.Put(walDataBuf)
			}
			return fmt.Errorf("failed to append WAL records: %w", err)
		}
		if walDataBuf != nil {
			walDataPool.Put(walDataBuf)
		}
	} else {
		records := make([]*storage.WALRecord, 0, len(txn.WriteSet)+1)
		for wk, value := range txn.WriteSet {
			tnLen := len(wk.TreeName)
			kLen := len(wk.Key)
			totalKeyLen := tnLen + 1 + kLen
			need, err := txnWALRecordDataLen(totalKeyLen, len(value))
			if err != nil {
				return err
			}
			data := make([]byte, need)
			binary.LittleEndian.PutUint32(data[0:4], uint32(totalKeyLen)) // #nosec G115 -- txnWALRecordDataLen above bounds totalKeyLen to <= 65531.
			copy(data[4:4+tnLen], wk.TreeName)
			data[4+tnLen] = ':'
			copy(data[4+tnLen+1:], wk.Key)
			copy(data[4+totalKeyLen:], value)

			records = append(records, &storage.WALRecord{
				TxnID: txn.ID,
				Type:  storage.WALUpdate,
				Data:  data,
			})
		}
		records = append(records, &storage.WALRecord{
			TxnID: txn.ID,
			Type:  storage.WALCommit,
		})
		if err := wal.AppendBatch(records); err != nil {
			return fmt.Errorf("failed to append WAL records: %w", err)
		}
	}
	return nil
}

// pruneVersions removes version entries that are no longer needed by any
// active transaction, using a commit-sequence watermark.
//
// Safety argument (see the conflict checks in detectConflicts and
// commitWithConflictDetection): a stored entry {key: V} triggers a conflict
// for a committing transaction iff V > readVersion(key); an ABSENT entry is
// treated as "no conflict". Removing {key: V} is therefore only safe when,
// for every transaction that could still commit, "absent" yields the same
// decision as "present with V" — i.e. V <= its recorded readVersion. A
// transaction whose beginSeq >= V began after V was committed, so any read it
// performs on key observes version >= V (readVersion >= V), making V > read
// impossible either way. Hence entries with V <= min(beginSeq of all active
// transactions) can be deleted without changing any conflict decision.
//
// The watermark is loaded from commitSeq BEFORE scanning the active shards.
// The scan is shard-by-shard, so a transaction that begins in an
// already-scanned shard is invisible to the scan — but its beginSeq is >= the
// pre-scan commitSeq (commitSeq is monotonic), so it is still protected by the
// watermark. This closes the old race where a Begin during the scan could be
// missed and ALL version state cleared while that transaction held
// readVersions, silently disabling conflict detection (lost update).
func (m *Manager) pruneVersions() {
	// Load the watermark candidates BEFORE scanning active shards (see above).
	watermark := atomic.LoadUint64(&m.commitSeq)
	minActiveStartTS := atomic.LoadUint64(&m.counter)

	for i := range m.activeShards {
		m.activeShards[i].RLock()
		for _, txn := range m.activeShards[i].m {
			// beginSeq/StartTS are immutable after Begin publishes the txn into
			// this shard map, so reading them under the shard RLock is race-free.
			if txn.beginSeq < watermark {
				watermark = txn.beginSeq
			}
			if txn.StartTS < minActiveStartTS {
				minActiveStartTS = txn.StartTS
			}
		}
		m.activeShards[i].RUnlock()
	}

	// Delete every version entry at or below the watermark. This also runs
	// while transactions are active (the old code never pruned the
	// versionShards maps in that case, so they grew by one entry per distinct
	// key ever written — unbounded memory growth under a steady workload).
	for i := range m.versionShards {
		s := &m.versionShards[i]
		s.mu.Lock()
		for wk, v := range s.versions {
			if v <= watermark {
				delete(s.versions, wk)
			}
		}
		s.mu.Unlock()
	}

	// Prune old MVCC version-chain entries. NOTE: VersionStore versions are
	// commit timestamps supplied by its callers, which in this codebase are
	// transaction IDs (Manager.counter clock) — so the minimum active StartTS
	// is the matching watermark. Do not pass commitSeq-clock values here.
	if m.versionStore != nil {
		m.versionStore.Prune(minActiveStartTS)
	}
}

// removeActive removes a transaction from the active set
func (m *Manager) removeActive(id uint64) {
	shard := activeShardIdx(id)
	m.activeShards[shard].Lock()
	delete(m.activeShards[shard].m, id)
	m.activeShards[shard].Unlock()
}

// Get retrieves an active transaction
func (m *Manager) Get(id uint64) (*Transaction, error) {
	shard := activeShardIdx(id)
	m.activeShards[shard].RLock()
	defer m.activeShards[shard].RUnlock()

	txn, ok := m.activeShards[shard].m[id]
	if !ok {
		return nil, ErrTxnNotFound
	}
	return txn, nil
}

// GetTransaction retrieves an active transaction (alias for Get)
func (m *Manager) GetTransaction(id uint64) *Transaction {
	shard := activeShardIdx(id)
	m.activeShards[shard].RLock()
	defer m.activeShards[shard].RUnlock()

	txn, ok := m.activeShards[shard].m[id]
	if !ok {
		return nil
	}
	return txn
}

// GetCurrentVersion returns the current version of a key
func (m *Manager) GetCurrentVersion(treeName, key string) uint64 {
	var wk WriteKey
	wk.TreeName = treeName
	wk.Key = key
	idx := versionShardIdx(treeName, key)
	m.versionShards[idx].mu.Lock()
	defer m.versionShards[idx].mu.Unlock()
	return m.versionShards[idx].versions[wk]
}

// GetVersionStore returns the MVCC version store for snapshot reads.
func (m *Manager) GetVersionStore() *VersionStore {
	return m.versionStore
}
