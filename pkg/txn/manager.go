package txn

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/metrics"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

var (
	ErrTxnCommitted    = errors.New("transaction already committed")
	ErrTxnAborted      = errors.New("transaction already aborted")
	ErrConflict        = errors.New("transaction conflict")
	ErrTxnNotFound     = errors.New("transaction not found")
	ErrDeadlockDetected = errors.New("deadlock detected")
	ErrTxnTimeout      = errors.New("transaction timeout")
)

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
	Isolation        IsolationLevel
	ReadOnly         bool
	Timeout          time.Duration // Transaction timeout (0 = no timeout)
	LockWaitTimeout  time.Duration // Max time to wait for a lock (0 = default 5s)
}

// DefaultOptions returns default transaction options
func DefaultOptions() *Options {
	return &Options{
		Isolation:       SnapshotIsolation,
		ReadOnly:        false,
		Timeout:         0,             // No default timeout
		LockWaitTimeout: 5 * time.Second, // 5 second lock wait timeout
	}
}

// Transaction represents a database transaction
type Transaction struct {
	ID        uint64
	State     TxnState
	Isolation IsolationLevel
	ReadOnly  bool
	StartTS   uint64
	ReadSet   map[string]uint64 // key → version read
	WriteSet  map[string][]byte // key → new value (buffered writes)
	mu        sync.Mutex
	manager   *Manager

	// Deadlock detection and timeout fields
	ctx           context.Context    // Transaction context for timeout/cancellation
	cancel        context.CancelFunc // Cancel function for cleanup
	waitingFor    uint64             // Transaction ID this txn is waiting for (deadlock detection)
	waitingSince  time.Time          // When this txn started waiting
	locksHeld     map[string]bool    // Keys this transaction currently holds locks on
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

	// For snapshot isolation, check for conflicts
	if t.Isolation >= SnapshotIsolation {
		if err := t.manager.detectConflicts(t); err != nil {
			if rbErr := t.rollbackLocked(); rbErr != nil {
				return fmt.Errorf("conflict detection failed and rollback failed: %v; %w", rbErr, err)
			}
			return err
		}
	}

	// Apply writes
	if err := t.manager.applyWrites(t); err != nil {
		if rbErr := t.rollbackLocked(); rbErr != nil {
			return fmt.Errorf("apply writes failed and rollback failed: %v; %w", rbErr, err)
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
		go t.manager.pruneVersions()
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
	t.WriteSet = nil
	t.ReadSet = nil

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

// GetReadVersion returns the version read for a key
func (t *Transaction) GetReadVersion(key string) (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	v, ok := t.ReadSet[key]
	return v, ok
}

// SetReadVersion records a read version for a key
func (t *Transaction) SetReadVersion(key string, version uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.ReadSet == nil {
		t.ReadSet = make(map[string]uint64)
	}
	t.ReadSet[key] = version
}

// GetWrite returns the buffered write for a key
func (t *Transaction) GetWrite(key string) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	v, ok := t.WriteSet[key]
	return v, ok
}

// SetWrite buffers a write for a key
func (t *Transaction) SetWrite(key string, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.WriteSet == nil {
		t.WriteSet = make(map[string][]byte)
	}
	t.WriteSet[key] = value
}

// Manager manages all transactions
type Manager struct {
	counter     uint64 // atomic, monotonic
	active      map[uint64]*Transaction
	versions    map[string]uint64 // key → latest committed version
	mu          sync.RWMutex
	commitCount atomic.Int64
	pool        interface{} // BufferPool (using interface{} to avoid import cycle)
	wal         interface{} // WAL

	// Deadlock detection
	deadlockCheckInterval time.Duration
	stopDeadlockDetector  chan struct{}
	deadlockDetectorOnce  sync.Once

	// Lock management for deadlock detection
	lockOwners map[string]uint64 // key → transaction ID that holds the lock
	lockMu     sync.RWMutex
}

// NewManager creates a new transaction manager
func NewManager(pool, wal interface{}) *Manager {
	m := &Manager{
		active:                make(map[uint64]*Transaction),
		versions:              make(map[string]uint64),
		pool:                  pool,
		wal:                   wal,
		deadlockCheckInterval: 100 * time.Millisecond, // Check every 100ms
		stopDeadlockDetector:  make(chan struct{}),
		lockOwners:            make(map[string]uint64),
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
	m.mu.RLock()
	activeTxns := make(map[uint64]*Transaction)
	waitingMap := make(map[uint64]uint64)
	for id, txn := range m.active {
		activeTxns[id] = txn
		// Read waiting state without holding individual lock
		waitingMap[id] = txn.waitingFor
	}
	m.mu.RUnlock()

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

// AcquireLock attempts to acquire a lock on a key for a transaction
// Returns ErrDeadlockDetected if acquiring this lock would cause a deadlock
func (m *Manager) AcquireLock(txnID uint64, key string, timeout time.Duration) error {
	m.lockMu.Lock()
	defer m.lockMu.Unlock()

	// Check who owns this lock
	ownerID, locked := m.lockOwners[key]
	if !locked || ownerID == txnID {
		// Lock is available or already held by this transaction
		m.lockOwners[key] = txnID

		// Record in transaction
		m.mu.RLock()
		txn, exists := m.active[txnID]
		m.mu.RUnlock()
		if exists {
			txn.AddLockHeld(key)
		}
		return nil
	}

	// Lock is held by another transaction - set up waiting
	m.mu.RLock()
	txn, exists := m.active[txnID]
	waitingTxn, waitingExists := m.active[ownerID]
	m.mu.RUnlock()

	if !exists {
		return ErrTxnNotFound
	}

	// Set waiting relationship for deadlock detection
	if waitingExists {
		txn.SetWaitingFor(ownerID)
		_ = waitingTxn // Avoid unused variable error
	}

	// Check if this would cause a deadlock immediately
	if m.wouldCauseDeadlock(txnID, ownerID) {
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
				return fmt.Errorf("lock acquisition timeout")
			case <-ticker.C:
				m.lockMu.Lock()
				currentOwner, stillLocked := m.lockOwners[key]
				if !stillLocked || currentOwner == txnID {
					// Lock became available
					m.lockOwners[key] = txnID
					m.lockMu.Unlock()
					txn.SetWaitingFor(0)
					txn.AddLockHeld(key)
					return nil
				}
				m.lockMu.Unlock()
			}
		}
	}

	return fmt.Errorf("lock is held by transaction %d", ownerID)
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

		m.mu.RLock()
		txn, exists := m.active[current]
		m.mu.RUnlock()

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

	if owner, ok := m.lockOwners[key]; ok && owner == txnID {
		delete(m.lockOwners, key)

		// Clear from transaction's held locks
		m.mu.RLock()
		txn, exists := m.active[txnID]
		m.mu.RUnlock()
		if exists {
			txn.RemoveLockHeld(key)
		}
	}
}

// ReleaseAllLocks releases all locks held by a transaction
func (m *Manager) ReleaseAllLocks(txnID uint64) {
	// Get locks before any state changes
	m.mu.RLock()
	txn, exists := m.active[txnID]
	m.mu.RUnlock()

	if !exists {
		// Transaction might already be removed, try to clean up from lockOwners
		m.lockMu.Lock()
		for key, owner := range m.lockOwners {
			if owner == txnID {
				delete(m.lockOwners, key)
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

// Begin starts a new transaction
func (m *Manager) Begin(opts *Options) *Transaction {
	if opts == nil {
		opts = DefaultOptions()
	}

	id := atomic.AddUint64(&m.counter, 1)

	// Create context with timeout if specified
	ctx, cancel := context.Background(), func() {}
	if opts.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
	}

	txn := &Transaction{
		ID:        id,
		State:     TxnActive,
		Isolation: opts.Isolation,
		ReadOnly:  opts.ReadOnly,
		StartTS:   id,
		ReadSet:   make(map[string]uint64),
		WriteSet:  make(map[string][]byte),
		manager:   m,
		ctx:       ctx,
		cancel:    cancel,
		locksHeld: make(map[string]bool),
	}

	m.mu.Lock()
	m.active[id] = txn
	m.mu.Unlock()

	// Record metrics
	metrics.GetTransactionMetrics().RecordTxnStart()

	return txn
}

// BeginWithContext starts a new transaction with a user-provided context
func (m *Manager) BeginWithContext(ctx context.Context, opts *Options) *Transaction {
	if opts == nil {
		opts = DefaultOptions()
	}

	id := atomic.AddUint64(&m.counter, 1)

	// Use provided context, optionally with timeout
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		_ = cancel // Will be called when transaction ends (stored in txn)
	}

	txn := &Transaction{
		ID:        id,
		State:     TxnActive,
		Isolation: opts.Isolation,
		ReadOnly:  opts.ReadOnly,
		StartTS:   id,
		ReadSet:   make(map[string]uint64),
		WriteSet:  make(map[string][]byte),
		manager:   m,
		ctx:       ctx,
		cancel:    func() {}, // No-op since we don't own the context
		locksHeld: make(map[string]bool),
	}

	m.mu.Lock()
	m.active[id] = txn
	m.mu.Unlock()

	return txn
}

// detectConflicts checks for write-write conflicts
func (m *Manager) detectConflicts(txn *Transaction) error {
	if txn.Isolation < SnapshotIsolation {
		return nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if any key we read has been modified by another transaction
	for key, readVersion := range txn.ReadSet {
		currentVersion, exists := m.versions[key]
		if !exists {
			continue
		}
		if currentVersion > readVersion {
			return ErrConflict
		}
	}

	return nil
}

// applyWrites applies all writes from a transaction to actual storage
func (m *Manager) applyWrites(txn *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update versions for all written keys
	for key := range txn.WriteSet {
		m.versions[key] = txn.ID
	}

	// Apply writes to storage via BufferPool if available
	if m.pool != nil {
		if bp, ok := m.pool.(*storage.BufferPool); ok && bp != nil {
			for key, value := range txn.WriteSet {
				// Parse table name from key (format: "tableName:pk" or similar)
				// For now, we just update the version tracking
				// Actual storage write is handled by Catalog layer
				_ = bp
				_ = key
				_ = value
			}
		}
	}

	// Mark writes as durable in WAL if available
	if m.wal != nil {
		if wal, ok := m.wal.(*storage.WAL); ok && wal != nil {
			for key, value := range txn.WriteSet {
				// Encode key with length prefix to preserve key/value boundary
				keyBytes := []byte(key)
				data := make([]byte, 4+len(keyBytes)+len(value))
				binary.LittleEndian.PutUint32(data[0:4], uint32(len(keyBytes)))
				copy(data[4:4+len(keyBytes)], keyBytes)
				copy(data[4+len(keyBytes):], value)

				record := &storage.WALRecord{
					TxnID: txn.ID,
					Type:  storage.WALUpdate,
					Data:  data,
				}
				if err := wal.AppendWithoutSync(record); err != nil {
					return fmt.Errorf("failed to append WAL record: %w", err)
				}
			}
			// Write commit record so crash recovery knows this transaction committed
			commitRecord := &storage.WALRecord{
				TxnID: txn.ID,
				Type:  storage.WALCommit,
			}
			if err := wal.Append(commitRecord); err != nil {
				return fmt.Errorf("failed to append WAL commit record: %w", err)
			}
		}
	}

	return nil
}

// pruneVersions removes version entries that are no longer needed by any active transaction
func (m *Manager) pruneVersions() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find minimum active transaction start timestamp
	minActive := uint64(math.MaxUint64)
	for _, txn := range m.active {
		if txn.StartTS < minActive {
			minActive = txn.StartTS
		}
	}

	// If no active transactions, we can clear all versions
	if minActive == math.MaxUint64 {
		m.versions = make(map[string]uint64)
		return
	}
}

// removeActive removes a transaction from the active set
func (m *Manager) removeActive(id uint64) {
	m.mu.Lock()
	delete(m.active, id)
	m.mu.Unlock()
}

// Get retrieves an active transaction
func (m *Manager) Get(id uint64) (*Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	txn, ok := m.active[id]
	if !ok {
		return nil, ErrTxnNotFound
	}
	return txn, nil
}

// GetTransaction retrieves an active transaction (alias for Get)
func (m *Manager) GetTransaction(id uint64) *Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()

	txn, ok := m.active[id]
	if !ok {
		return nil
	}
	return txn
}

// GetCurrentVersion returns the current version of a key
func (m *Manager) GetCurrentVersion(key string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.versions[key]
}
