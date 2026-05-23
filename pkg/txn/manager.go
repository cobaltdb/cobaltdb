package txn

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"math"
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
)

func txnUint32Len(n int, name string) (uint32, error) {
	if n < 0 || n > 1<<32-1 {
		return 0, fmt.Errorf("%s exceeds uint32: %d", name, n)
	}
	return uint32(n), nil // #nosec G115 - range checked above.
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
	ctx          context.Context    // Transaction context for timeout/cancellation
	cancel       context.CancelFunc // Cancel function for cleanup
	waitingFor   uint64             // Transaction ID this txn is waiting for (deadlock detection)
	waitingSince time.Time          // When this txn started waiting
	locksHeld    map[string]bool    // Keys this transaction currently holds locks on
}

// SetWaitingFor sets which transaction this one is waiting for (deadlock detection)
func (t *Transaction) SetWaitingFor(txnID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.waitingFor = txnID
	if txnID != 0 {
		t.waitingSince = time.Now()
	}
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

// RemoveLockHeld removes a lock record
func (t *Transaction) RemoveLockHeld(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
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

	// Check for timeout
	if t.IsTimedOut() {
		_ = t.rollbackLocked()
		metrics.GetTransactionMetrics().RecordTxnTimeout()
		return ErrTxnTimeout
	}

	startTime := time.Now()

	// Apply writes atomically with conflict detection so no transaction
	// can sneak in between the read-check and the write.
	if err := t.manager.commitWithConflictDetection(t); err != nil {
		rbErr := t.rollbackLocked()
		if rbErr != nil {
			return fmt.Errorf("commit failed and rollback failed: %v; %w", rbErr, err)
		}
		return err
	}

	t.State = TxnCommitted
	t.manager.removeActive(t.ID)

	// Release all locks held by this transaction
	t.manager.ReleaseAllLocks(t.ID)

	// Cancel context to release resources
	if t.cancel != nil {
		t.cancel()
	}

	// Record metrics
	duration := time.Since(startTime)
	metrics.GetTransactionMetrics().RecordTxnCommit(duration)
	if duration > time.Second {
		metrics.GetTransactionMetrics().RecordLongRunningTxn()
	}

	// Periodically prune versions map to prevent unbounded memory growth
	if t.manager.commitCount.Add(1)%1000 == 0 {
		t.manager.pruneVersions()
	}

	return nil
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
	mgr := t.manager

	t.State = TxnAborted
	// Clear maps so the backing storage can be reused by sync.Pool.
	clear(t.WriteSet)
	clear(t.ReadSet)

	// Release all locks without holding transaction mutex to avoid deadlock
	t.mu.Unlock()
	for _, key := range locks {
		mgr.ReleaseLock(t.ID, key)
	}
	t.mu.Lock()

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
	return v, ok
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
	t.WriteSet[wk] = value
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
	pool          interface{} // BufferPool (using interface{} to avoid import cycle)
	wal           interface{} // WAL

	// Deadlock detection
	deadlockCheckInterval time.Duration
	stopDeadlockDetector  chan struct{}
	deadlockDetectorOnce  sync.Once

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
func NewManager(pool, wal interface{}) *Manager {
	m := &Manager{
		versionStore:          NewVersionStore(),
		pool:                  pool,
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
	close(m.stopDeadlockDetector)
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
	// Snapshot active transactions and their waiting states
	activeTxns := make(map[uint64]*Transaction)
	waitingMap := make(map[uint64]uint64)
	for i := range m.activeShards {
		m.activeShards[i].RLock()
		for id, txn := range m.activeShards[i].m {
			activeTxns[id] = txn
			waitingMap[id] = txn.waitingFor
		}
		m.activeShards[i].RUnlock()
	}

	// Build wait-for graph and detect cycles
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)

	var cycle []uint64
	var detectCycle func(uint64) bool

	detectCycle = func(txnID uint64) bool {
		visited[txnID] = true
		recStack[txnID] = true

		waitingFor := waitingMap[txnID]
		if waitingFor != 0 {
			if !visited[waitingFor] {
				if detectCycle(waitingFor) {
					cycle = append(cycle, txnID)
					return true
				}
			} else if recStack[waitingFor] {
				// Found a cycle
				cycle = []uint64{waitingFor, txnID}
				return true
			}
		}

		recStack[txnID] = false
		return false
	}

	// Check for cycles starting from each transaction
	for txnID := range activeTxns {
		if !visited[txnID] {
			cycle = nil
			if detectCycle(txnID) && len(cycle) > 0 {
				// Found deadlock - abort the youngest transaction
				m.resolveDeadlock(cycle, activeTxns)
				return // Only resolve one deadlock at a time
			}
		}
	}
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
			txn.mu.Unlock()
			if startTS > maxStartTS {
				maxStartTS = startTS
				victimID = txnID
			}
		}
	}

	if victimID != 0 {
		if victim, ok := activeTxns[victimID]; ok {
			victim.SetWaitingFor(0) // Clear waiting state
			_ = victim.Rollback()   // Abort the victim
		}
	}
}

// AcquireLock acquires an exclusive lock (backward compatible).
func (m *Manager) AcquireLock(txnID uint64, key string, timeout time.Duration) error {
	return m.AcquireLockMode(txnID, key, LockExclusive, timeout)
}

// AcquireLockMode acquires a lock in the specified mode (shared or exclusive).
func (m *Manager) AcquireLockMode(txnID uint64, key string, mode LockMode, timeout time.Duration) error {
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

		shard := activeShardIdx(txnID)
		m.activeShards[shard].RLock()
		txn, exists := m.activeShards[shard].m[txnID]
		m.activeShards[shard].RUnlock()
		if exists {
			txn.AddLockHeld(key)
		}
		return nil
	}

	// Determine who is blocking us
	blockerID := entry.exclusive
	if blockerID == 0 {
		// Blocked by shared lock holders — pick any
		for id := range entry.shared {
			if id != txnID {
				blockerID = id
				break
			}
		}
	}

	shard := activeShardIdx(txnID)
	m.activeShards[shard].RLock()
	txn, exists := m.activeShards[shard].m[txnID]
	m.activeShards[shard].RUnlock()
	blockerShard := activeShardIdx(blockerID)
	m.activeShards[blockerShard].RLock()
	waitingTxn, waitingExists := m.activeShards[blockerShard].m[blockerID]
	m.activeShards[blockerShard].RUnlock()

	if !exists {
		m.lockMu.Unlock()
		return ErrTxnNotFound
	}

	if waitingExists {
		txn.SetWaitingFor(blockerID)
		_ = waitingTxn
	}

	if m.wouldCauseDeadlock(txnID, blockerID) {
		m.lockMu.Unlock()
		txn.SetWaitingFor(0)
		return ErrDeadlockDetected
	}

	// Wait for the lock with timeout
	if timeout > 0 {
		m.lockMu.Unlock()
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timer.C:
				m.lockMu.Lock()
				txn.SetWaitingFor(0)
				m.lockMu.Unlock()
				return fmt.Errorf("lock acquisition timeout")
			case <-ticker.C:
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
					txn.SetWaitingFor(0)
					txn.AddLockHeld(key)
					return nil
				}
				m.lockMu.Unlock()
			}
		}
	}

	m.lockMu.Unlock()
	return fmt.Errorf("lock is held by transaction %d", blockerID)
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

		shard := activeShardIdx(current)
		m.activeShards[shard].RLock()
		txn, exists := m.activeShards[shard].m[current]
		m.activeShards[shard].RUnlock()

		if !exists {
			return false
		}

		waitingFor := txn.GetWaitingFor()
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
	defer m.lockMu.Unlock()

	entry := m.lockEntries[key]
	if entry == nil {
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

	if released {
		if entry.exclusive == 0 && len(entry.shared) == 0 {
			delete(m.lockEntries, key)
		}

		shard := activeShardIdx(txnID)
		m.activeShards[shard].RLock()
		txn, exists := m.activeShards[shard].m[txnID]
		m.activeShards[shard].RUnlock()
		if exists {
			txn.RemoveLockHeld(key)
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

// RecycleTxn resets a transaction and returns it to the pool for reuse.
func (m *Manager) RecycleTxn(txn *Transaction) {
	if txn == nil || txn.manager == nil {
		return
	}
	// Fields that Begin overwrites are left as-is so that post-commit/rollback
	// state inspection in tests and callers remains valid.
	txn.manager = nil
	if txn.cancel != nil {
		txn.cancel()
		txn.cancel = nil
	}
	txn.ctx = nil
	txn.waitingFor = 0
	txn.waitingSince = time.Time{}
	if txn.locksHeld != nil {
		clear(txn.locksHeld)
	}
	if txn.ReadSet != nil {
		clear(txn.ReadSet)
	}
	if txn.WriteSet != nil {
		clear(txn.WriteSet)
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

	txn := m.acquireTxn()
	txn.ID = id
	txn.State = TxnActive
	txn.Isolation = opts.Isolation
	txn.ReadOnly = opts.ReadOnly
	txn.StartTS = id
	txn.manager = m
	txn.ctx = ctx
	txn.cancel = cancel

	shard := activeShardIdx(id)
	m.activeShards[shard].Lock()
	m.activeShards[shard].m[id] = txn
	m.activeShards[shard].Unlock()

	// Record metrics
	metrics.GetTransactionMetrics().RecordTxnStart()

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

	txn := m.acquireTxn()
	txn.ID = id
	txn.State = TxnActive
	txn.Isolation = opts.Isolation
	txn.ReadOnly = opts.ReadOnly
	txn.StartTS = id
	txn.manager = m
	txn.ctx = ctx
	txn.cancel = cancel

	shard := activeShardIdx(id)
	m.activeShards[shard].Lock()
	m.activeShards[shard].m[id] = txn
	m.activeShards[shard].Unlock()

	return txn
}

// detectConflicts checks for write-write conflicts
func (m *Manager) detectConflicts(txn *Transaction) error {
	if txn.Isolation < SnapshotIsolation {
		return nil
	}

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
	var sorted []int
	if len(shardExtra) == 0 {
		sorted = shardArr[:shardCount]
	} else {
		sorted = make([]int, 0, shardCount+len(shardExtra))
		sorted = append(sorted, shardArr[:shardCount]...)
		sorted = append(sorted, shardExtra...)
	}
	sort.Ints(sorted)

	for _, s := range sorted {
		m.versionShards[s].mu.Lock()
	}
	defer func() {
		for i := len(sorted) - 1; i >= 0; i-- {
			m.versionShards[sorted[i]].mu.Unlock()
		}
	}()

	for wk, readVersion := range txn.ReadSet {
		currentVersion, exists := m.versionShards[versionShardIdx(wk.TreeName, wk.Key)].versions[wk]
		if !exists {
			continue
		}
		if currentVersion > readVersion {
			return ErrConflict
		}
	}

	return nil
}

// commitWithConflictDetection atomically checks for conflicts and updates
// versions, then releases the lock before writing WAL records.  This allows
// other transactions to commit while WAL I/O is in progress, significantly
// improving concurrency under high write load.
func (m *Manager) commitWithConflictDetection(txn *Transaction) error {
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
			seq := atomic.AddUint64(&m.commitSeq, 1)
			m.versionShards[shard].versions[writeWk] = seq
			m.versionShards[shard].mu.Unlock()
			return m.writeWALForCommit(txn)
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
				seq := atomic.AddUint64(&m.commitSeq, 1)
				m.versionShards[shard].versions[writeWk] = seq
				m.versionShards[shard].mu.Unlock()
				return m.writeWALForCommit(txn)
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

	// 4. Update versions.
	// Note: VersionStore is write-only in production (no callers of
	// GetAtSnapshot / GetCurrent / GetLatestVersion outside tests),
	// so we skip the per-commit allocation entirely.
	seq := atomic.AddUint64(&m.commitSeq, 1)
	for wk := range txn.WriteSet {
		m.versionShards[versionShardIdx(wk.TreeName, wk.Key)].versions[wk] = seq
	}

	// Release version locks before WAL append so other commits can proceed.
	for i := len(sorted) - 1; i >= 0; i-- {
		m.versionShards[sorted[i]].mu.Unlock()
	}

	// 5. WAL durability (outside version locks so other commits can proceed).
	return m.writeWALForCommit(txn)
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
			encodedKeyLen, err := txnUint32Len(totalKeyLen, "WAL key length")
			if err != nil {
				return err
			}
			need := 4 + totalKeyLen + len(value)
			var data []byte
			if need <= 256 {
				walDataBuf = walDataPool.Get().(*[]byte)
				data = (*walDataBuf)[:need]
				binary.LittleEndian.PutUint32(data[0:4], encodedKeyLen)
				copy(data[4:4+tnLen], wk.TreeName)
				data[4+tnLen] = ':'
				copy(data[4+tnLen+1:], wk.Key)
				copy(data[4+totalKeyLen:], value)
			} else {
				data = make([]byte, need)
				binary.LittleEndian.PutUint32(data[0:4], encodedKeyLen)
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
			encodedKeyLen, err := txnUint32Len(totalKeyLen, "WAL key length")
			if err != nil {
				return err
			}
			data := make([]byte, 4+totalKeyLen+len(value))
			binary.LittleEndian.PutUint32(data[0:4], encodedKeyLen)
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

// pruneVersions removes version entries that are no longer needed by any active transaction
func (m *Manager) pruneVersions() {
	minActive := uint64(math.MaxUint64)
	for i := range m.activeShards {
		m.activeShards[i].RLock()
		for _, txn := range m.activeShards[i].m {
			if txn.StartTS < minActive {
				minActive = txn.StartTS
			}
		}
		m.activeShards[i].RUnlock()
	}

	// If no active transactions, clear maps in place instead of reallocating.
	if minActive == math.MaxUint64 {
		for i := range m.versionShards {
			m.versionShards[i].mu.Lock()
			clear(m.versionShards[i].versions)
			m.versionShards[i].mu.Unlock()
		}
		if m.versionStore != nil {
			m.versionStore.Clear()
		}
		return
	}

	// Prune old version chain entries
	if m.versionStore != nil {
		m.versionStore.Prune(minActive)
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
