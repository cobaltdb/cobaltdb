package storage

import (
	"sync"
	"time"
)

// BatchedWAL wraps a WAL with batching for better write throughput
type BatchedWAL struct {
	wal        *WAL
	batchSize  int
	flushDelay time.Duration
	batch      []*WALRecord
	mu         sync.Mutex
	flushCond  *sync.Cond
	stopChan   chan struct{}
	wg         sync.WaitGroup
}

// NewBatchedWAL creates a new batched WAL writer
func NewBatchedWAL(wal *WAL, batchSize int, flushDelay time.Duration) *BatchedWAL {
	if batchSize <= 0 {
		batchSize = 100
	}
	if flushDelay <= 0 {
		flushDelay = 10 * time.Millisecond
	}

	bw := &BatchedWAL{
		wal:        wal,
		batchSize:  batchSize,
		flushDelay: flushDelay,
		batch:      make([]*WALRecord, 0, batchSize),
		stopChan:   make(chan struct{}),
	}
	bw.flushCond = sync.NewCond(&bw.mu)

	// Start background flusher
	bw.wg.Add(1)
	go bw.flusher()

	return bw
}

// Append adds a record to the batch
func (bw *BatchedWAL) Append(record *WALRecord) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	// Add to batch
	bw.batch = append(bw.batch, record)

	// Flush if batch is full
	if len(bw.batch) >= bw.batchSize {
		return bw.flushLocked()
	}

	// Signal flusher that we have data
	bw.flushCond.Signal()
	return nil
}

// Flush writes all pending records to WAL
func (bw *BatchedWAL) Flush() error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	return bw.flushLocked()
}

func (bw *BatchedWAL) flushLocked() error {
	if len(bw.batch) == 0 {
		return nil
	}

	// Write all records
	for _, record := range bw.batch {
		if err := bw.wal.Append(record); err != nil {
			return err
		}
	}

	// Sync to disk
	if err := bw.wal.file.Sync(); err != nil {
		return err
	}

	// Clear batch
	bw.batch = bw.batch[:0]
	return nil
}

// flusher is a background goroutine that periodically flushes the batch
func (bw *BatchedWAL) flusher() {
	defer bw.wg.Done()

	ticker := time.NewTicker(bw.flushDelay)
	defer ticker.Stop()

	for {
		select {
		case <-bw.stopChan:
			bw.Flush()
			return

		case <-ticker.C:
			bw.Flush()

		default:
			bw.mu.Lock()
			for len(bw.batch) == 0 {
				select {
				case <-bw.stopChan:
					bw.mu.Unlock()
					return
				default:
				}
				bw.flushCond.Wait()
			}
			bw.mu.Unlock()
		}
	}
}

// Close closes the batched WAL
func (bw *BatchedWAL) Close() error {
	close(bw.stopChan)
	bw.wg.Wait()
	return nil
}

// GetWAL returns the underlying WAL
func (bw *BatchedWAL) GetWAL() *WAL {
	return bw.wal
}

// AsyncWAL provides asynchronous WAL writes for even better performance
type AsyncWAL struct {
	wal        *WAL
	recordChan chan *WALRecord
	stopChan   chan struct{}
	wg         sync.WaitGroup
	batchSize  int
}

// NewAsyncWAL creates a new async WAL writer
func NewAsyncWAL(wal *WAL, bufferSize, batchSize int) *AsyncWAL {
	if bufferSize <= 0 {
		bufferSize = 1000
	}
	if batchSize <= 0 {
		batchSize = 100
	}

	aw := &AsyncWAL{
		wal:        wal,
		recordChan: make(chan *WALRecord, bufferSize),
		stopChan:   make(chan struct{}),
		batchSize:  batchSize,
	}

	aw.wg.Add(1)
	go aw.writer()

	return aw
}

// Append queues a record for async writing
func (aw *AsyncWAL) Append(record *WALRecord) error {
	select {
	case aw.recordChan <- record:
		return nil
	case <-aw.stopChan:
		return ErrWALClosed
	default:
		// Channel full, write synchronously
		return aw.wal.Append(record)
	}
}

func (aw *AsyncWAL) writer() {
	defer aw.wg.Done()

	batch := make([]*WALRecord, 0, aw.batchSize)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		for _, record := range batch {
			if err := aw.wal.Append(record); err != nil {
				// Log error but continue
				continue
			}
		}
		aw.wal.file.Sync()
		batch = batch[:0]
	}

	for {
		select {
		case record, ok := <-aw.recordChan:
			if !ok {
				flush()
				return
			}
			batch = append(batch, record)
			if len(batch) >= aw.batchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-aw.stopChan:
			flush()
			return
		}
	}
}

// Close closes the async WAL
func (aw *AsyncWAL) Close() error {
	close(aw.stopChan)
	aw.wg.Wait()
	return nil
}

// Sync flushes all pending writes
func (aw *AsyncWAL) Sync() error {
	// Drain the channel
	for len(aw.recordChan) > 0 {
		time.Sleep(1 * time.Millisecond)
	}
	return aw.wal.file.Sync()
}
