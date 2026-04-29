package storage

import "time"

// WriteAheadLog defines the interface for WAL operations.
type WriteAheadLog interface {
	Append(record *WALRecord) error
	Sync() error
	Checkpoint(bp *BufferPool) error
	Recover(bp *BufferPool) error
	Close() error
	LSN() uint64
	CheckpointLSN() uint64
	EnableGroupCommit(batchSize int, interval time.Duration)
	DisableGroupCommit()
}

// BufferPoolManager defines the interface for buffer pool page management.
type BufferPoolManager interface {
	GetPage(pageID uint32) ([]byte, error)
	PutPage(pageID uint32, data []byte) error
	FlushAll() error
	Close() error
}
