package metrics

import (
	"sync/atomic"
	"time"
)

// TransactionMetrics tracks transaction-related metrics
type TransactionMetrics struct {
	// Active transactions
	ActiveTxns atomic.Int64

	// Committed transactions
	CommittedTxns atomic.Int64

	// Aborted transactions (including deadlocks)
	AbortedTxns atomic.Int64

	// Deadlock count
	DeadlocksDetected atomic.Int64

	// Lock wait timeouts
	LockTimeouts atomic.Int64

	// Transaction timeouts
	TxnTimeouts atomic.Int64

	// Total lock acquisition time
	TotalLockWaitTime atomic.Int64 // nanoseconds

	// Long-running transaction count (>1s)
	LongRunningTxns atomic.Int64

	// Average transaction duration
	totalTxnTime atomic.Int64 // nanoseconds
}

// Global transaction metrics instance
var txnMetrics = &TransactionMetrics{}

// GetTransactionMetrics returns the global transaction metrics
func GetTransactionMetrics() *TransactionMetrics {
	return txnMetrics
}

// RecordTxnStart records the start of a transaction
func (m *TransactionMetrics) RecordTxnStart() {
	m.ActiveTxns.Add(1)
}

// RecordTxnCommit records a committed transaction
func (m *TransactionMetrics) RecordTxnCommit(duration time.Duration) {
	m.ActiveTxns.Add(-1)
	m.CommittedTxns.Add(1)
	m.totalTxnTime.Add(int64(duration))
}

// RecordTxnAbort records an aborted transaction
func (m *TransactionMetrics) RecordTxnAbort() {
	m.ActiveTxns.Add(-1)
	m.AbortedTxns.Add(1)
}

// RecordDeadlock records a detected deadlock
func (m *TransactionMetrics) RecordDeadlock() {
	m.DeadlocksDetected.Add(1)
}

// RecordLockTimeout records a lock acquisition timeout
func (m *TransactionMetrics) RecordLockTimeout() {
	m.LockTimeouts.Add(1)
}

// RecordTxnTimeout records a transaction timeout
func (m *TransactionMetrics) RecordTxnTimeout() {
	m.TxnTimeouts.Add(1)
}

// RecordLockWaitTime records lock wait time
func (m *TransactionMetrics) RecordLockWaitTime(duration time.Duration) {
	m.TotalLockWaitTime.Add(int64(duration))
}

// RecordLongRunningTxn records a long-running transaction
func (m *TransactionMetrics) RecordLongRunningTxn() {
	m.LongRunningTxns.Add(1)
}

// GetStats returns current transaction statistics
func (m *TransactionMetrics) GetStats() TransactionStats {
	return TransactionStats{
		ActiveTxns:        m.ActiveTxns.Load(),
		CommittedTxns:     m.CommittedTxns.Load(),
		AbortedTxns:       m.AbortedTxns.Load(),
		DeadlocksDetected: m.DeadlocksDetected.Load(),
		LockTimeouts:      m.LockTimeouts.Load(),
		TxnTimeouts:       m.TxnTimeouts.Load(),
		LongRunningTxns:   m.LongRunningTxns.Load(),
	}
}

// TransactionStats holds transaction statistics
type TransactionStats struct {
	ActiveTxns        int64 `json:"active_txns"`
	CommittedTxns     int64 `json:"committed_txns"`
	AbortedTxns       int64 `json:"aborted_txns"`
	DeadlocksDetected int64 `json:"deadlocks_detected"`
	LockTimeouts      int64 `json:"lock_timeouts"`
	TxnTimeouts       int64 `json:"txn_timeouts"`
	LongRunningTxns   int64 `json:"long_running_txns"`
}

// Reset resets all metrics to zero (useful for testing)
func (m *TransactionMetrics) Reset() {
	m.ActiveTxns.Store(0)
	m.CommittedTxns.Store(0)
	m.AbortedTxns.Store(0)
	m.DeadlocksDetected.Store(0)
	m.LockTimeouts.Store(0)
	m.TxnTimeouts.Store(0)
	m.LongRunningTxns.Store(0)
	m.TotalLockWaitTime.Store(0)
	m.totalTxnTime.Store(0)
}
