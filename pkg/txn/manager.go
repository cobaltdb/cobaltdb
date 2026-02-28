package txn

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrTxnCommitted  = errors.New("transaction already committed")
	ErrTxnAborted    = errors.New("transaction already aborted")
	ErrConflict      = errors.New("transaction conflict")
	ErrTxnNotFound   = errors.New("transaction not found")
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
	Isolation IsolationLevel
	ReadOnly  bool
}

// DefaultOptions returns default transaction options
func DefaultOptions() *Options {
	return &Options{
		Isolation: SnapshotIsolation,
		ReadOnly:  false,
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

	// For snapshot isolation, check for conflicts
	if t.Isolation >= SnapshotIsolation {
		if err := t.manager.detectConflicts(t); err != nil {
			t.Rollback()
			return err
		}
	}

	// Apply writes
	if err := t.manager.applyWrites(t); err != nil {
		t.Rollback()
		return err
	}

	t.State = TxnCommitted
	t.manager.removeActive(t.ID)
	return nil
}

// Rollback rolls back the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.State == TxnCommitted {
		return ErrTxnCommitted
	}
	if t.State == TxnAborted {
		return nil // Already aborted
	}

	t.State = TxnAborted
	t.WriteSet = nil
	t.ReadSet = nil
	t.manager.removeActive(t.ID)
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
	counter uint64 // atomic, monotonic
	active  map[uint64]*Transaction
	versions map[string]uint64 // key → latest committed version
	mu      sync.RWMutex
	pool    interface{} // BufferPool (using interface{} to avoid import cycle)
	wal     interface{} // WAL
}

// NewManager creates a new transaction manager
func NewManager(pool, wal interface{}) *Manager {
	return &Manager{
		active:   make(map[uint64]*Transaction),
		versions: make(map[string]uint64),
		pool:     pool,
		wal:      wal,
	}
}

// Begin starts a new transaction
func (m *Manager) Begin(opts *Options) *Transaction {
	if opts == nil {
		opts = DefaultOptions()
	}

	id := atomic.AddUint64(&m.counter, 1)
	txn := &Transaction{
		ID:        id,
		State:     TxnActive,
		Isolation: opts.Isolation,
		ReadOnly:  opts.ReadOnly,
		StartTS:   id,
		ReadSet:   make(map[string]uint64),
		WriteSet:  make(map[string][]byte),
		manager:   m,
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

// applyWrites applies all writes from a transaction
func (m *Manager) applyWrites(txn *Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update versions for all written keys
	for key := range txn.WriteSet {
		m.versions[key] = txn.ID
	}

	// TODO: Apply writes to actual storage (B+Tree)

	return nil
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

// GetCurrentVersion returns the current version of a key
func (m *Manager) GetCurrentVersion(key string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.versions[key]
}
