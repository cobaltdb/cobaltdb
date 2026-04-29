package txn

import "time"

// LockMode represents the mode of a lock.
type LockMode int

const (
	LockShared    LockMode = iota // Multiple readers can hold simultaneously
	LockExclusive                 // Only one holder allowed
)

// LockManager defines the interface for lock acquisition and release.
type LockManager interface {
	AcquireLock(txnID uint64, key string, timeout time.Duration) error
	AcquireLockMode(txnID uint64, key string, mode LockMode, timeout time.Duration) error
	ReleaseLock(txnID uint64, key string)
	ReleaseAllLocks(txnID uint64)
}

// TransactionManager defines the interface for transaction lifecycle management.
type TransactionManager interface {
	Begin(opts *Options) *Transaction
	Get(txnID uint64) (*Transaction, error)
	GetTransaction(txnID uint64) *Transaction
	GetCurrentVersion(key string) uint64
}
