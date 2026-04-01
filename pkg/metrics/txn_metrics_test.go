package metrics

import (
	"testing"
	"time"
)

func TestTransactionMetrics(t *testing.T) {
	m := &TransactionMetrics{}
	m.Reset()

	// Test txn start
	m.RecordTxnStart()
	if m.ActiveTxns.Load() != 1 {
		t.Errorf("Expected 1 active txn, got %d", m.ActiveTxns.Load())
	}

	// Test txn commit
	m.RecordTxnCommit(100 * time.Millisecond)
	if m.ActiveTxns.Load() != 0 {
		t.Errorf("Expected 0 active txn, got %d", m.ActiveTxns.Load())
	}
	if m.CommittedTxns.Load() != 1 {
		t.Errorf("Expected 1 committed txn, got %d", m.CommittedTxns.Load())
	}

	// Test txn abort
	m.RecordTxnStart()
	m.RecordTxnAbort()
	if m.ActiveTxns.Load() != 0 {
		t.Errorf("Expected 0 active txn, got %d", m.ActiveTxns.Load())
	}
	if m.AbortedTxns.Load() != 1 {
		t.Errorf("Expected 1 aborted txn, got %d", m.AbortedTxns.Load())
	}

	// Test deadlock
	m.RecordDeadlock()
	if m.DeadlocksDetected.Load() != 1 {
		t.Errorf("Expected 1 deadlock, got %d", m.DeadlocksDetected.Load())
	}

	// Test timeouts
	m.RecordLockTimeout()
	if m.LockTimeouts.Load() != 1 {
		t.Errorf("Expected 1 lock timeout, got %d", m.LockTimeouts.Load())
	}

	m.RecordTxnTimeout()
	if m.TxnTimeouts.Load() != 1 {
		t.Errorf("Expected 1 txn timeout, got %d", m.TxnTimeouts.Load())
	}

	// Test long-running txn
	m.RecordLongRunningTxn()
	if m.LongRunningTxns.Load() != 1 {
		t.Errorf("Expected 1 long-running txn, got %d", m.LongRunningTxns.Load())
	}
}

func TestTransactionMetricsGetStats(t *testing.T) {
	m := &TransactionMetrics{}
	m.Reset()

	// Record various metrics - net active should be 1 (2 starts - 1 commit)
	m.RecordTxnStart()
	m.RecordTxnStart()
	m.RecordTxnCommit(100 * time.Millisecond)
	m.RecordTxnAbort()
	m.RecordDeadlock()
	m.RecordLockTimeout()
	m.RecordTxnTimeout()
	m.RecordLongRunningTxn()

	// Get stats
	stats := m.GetStats()

	// 2 starts - 1 commit - 1 abort = 0 active
	if stats.ActiveTxns != 0 {
		t.Errorf("Expected 0 active txn in stats, got %d", stats.ActiveTxns)
	}
	if stats.CommittedTxns != 1 {
		t.Errorf("Expected 1 committed txn in stats, got %d", stats.CommittedTxns)
	}
	if stats.AbortedTxns != 1 {
		t.Errorf("Expected 1 aborted txn in stats, got %d", stats.AbortedTxns)
	}
	if stats.DeadlocksDetected != 1 {
		t.Errorf("Expected 1 deadlock in stats, got %d", stats.DeadlocksDetected)
	}
	if stats.LockTimeouts != 1 {
		t.Errorf("Expected 1 lock timeout in stats, got %d", stats.LockTimeouts)
	}
	if stats.TxnTimeouts != 1 {
		t.Errorf("Expected 1 txn timeout in stats, got %d", stats.TxnTimeouts)
	}
	if stats.LongRunningTxns != 1 {
		t.Errorf("Expected 1 long-running txn in stats, got %d", stats.LongRunningTxns)
	}
}

func TestGetTransactionMetrics(t *testing.T) {
	m1 := GetTransactionMetrics()
	m2 := GetTransactionMetrics()

	// Should return the same instance
	if m1 != m2 {
		t.Error("GetTransactionMetrics should return the same instance")
	}
}
