package storage

import (
	"sync"
	"sync/atomic"
	"time"
)

// GroupCommitConfig configures the group commit behavior
type GroupCommitConfig struct {
	Enabled           bool          // Whether group commit is enabled
	MaxWaitTime       time.Duration // Maximum time to wait before flushing (default: 5ms)
	MaxBatchSize      int           // Maximum number of commits to batch (default: 100)
	MinBatchSize      int           // Minimum batch size to trigger early flush (default: 10)
	MaxPendingCommits int           // Maximum pending commits before blocking (default: 1000)
}

// DefaultGroupCommitConfig returns the default configuration
func DefaultGroupCommitConfig() *GroupCommitConfig {
	return &GroupCommitConfig{
		Enabled:           true,
		MaxWaitTime:       5 * time.Millisecond,
		MaxBatchSize:      100,
		MinBatchSize:      10,
		MaxPendingCommits: 1000,
	}
}

// CommitRequest represents a single commit waiting to be synced
type CommitRequest struct {
	LSN       uint64
	Done      chan struct{}
	Err       error
	Timestamp time.Time
}

// GroupCommitter manages batching of commit operations
type GroupCommitter struct {
	wal    *WAL
	config *GroupCommitConfig

	mu            sync.Mutex
	pending       []*CommitRequest
	flushTimer    *time.Timer
	flushPending  bool
	stopped       bool
	stopCh        chan struct{}
	flushComplete chan struct{}

	// Metrics
	totalBatches  uint64
	totalCommits  uint64
	avgBatchSize  uint64
	lastFlushTime time.Time
	batchSizeHist []int // History of batch sizes for avg calculation
}

// NewGroupCommitter creates a new group committer
func NewGroupCommitter(wal *WAL, config *GroupCommitConfig) *GroupCommitter {
	if config == nil {
		config = DefaultGroupCommitConfig()
	}

	gc := &GroupCommitter{
		wal:           wal,
		config:        config,
		pending:       make([]*CommitRequest, 0, config.MaxBatchSize),
		stopCh:        make(chan struct{}),
		flushComplete: make(chan struct{}),
		batchSizeHist: make([]int, 0, 100),
	}

	if config.Enabled {
		go gc.flusherLoop()
	}

	return gc
}

// Stop stops the group committer
func (gc *GroupCommitter) Stop() {
	gc.mu.Lock()
	gc.stopped = true
	if gc.flushTimer != nil {
		gc.flushTimer.Stop()
	}
	gc.mu.Unlock()

	close(gc.stopCh)

	// Final flush of any pending commits
	gc.Flush()
}

// SubmitCommit submits a commit for group commit processing
// Returns when the commit has been synced to disk
func (gc *GroupCommitter) SubmitCommit(lsn uint64) error {
	if !gc.config.Enabled {
		// Fall back to direct sync
		return gc.wal.Sync()
	}

	req := &CommitRequest{
		LSN:       lsn,
		Done:      make(chan struct{}),
		Timestamp: time.Now(),
	}

	gc.mu.Lock()

	if gc.stopped {
		gc.mu.Unlock()
		return ErrWALClosed
	}

	gc.pending = append(gc.pending, req)
	shouldFlush := len(gc.pending) >= gc.config.MinBatchSize
	queueLen := len(gc.pending)

	// Start timer if not already running
	if !gc.flushPending && gc.flushTimer == nil {
		gc.flushPending = true
		gc.flushTimer = time.AfterFunc(gc.config.MaxWaitTime, func() {
			gc.triggerFlush()
		})
	}

	gc.mu.Unlock()

	// If queue is getting full, trigger flush
	if queueLen >= gc.config.MaxPendingCommits {
		gc.Flush()
	} else if shouldFlush {
		// Trigger immediate flush if we have enough commits
		gc.Flush()
	}

	// Wait for commit to be synced
	<-req.Done

	return req.Err
}

// triggerFlush triggers a flush asynchronously
func (gc *GroupCommitter) triggerFlush() {
	select {
	case gc.flushComplete <- struct{}{}:
	default:
	}
}

// Flush immediately flushes all pending commits
func (gc *GroupCommitter) Flush() error {
	gc.mu.Lock()

	if len(gc.pending) == 0 {
		gc.mu.Unlock()
		return nil
	}

	// Stop timer if running
	if gc.flushTimer != nil {
		gc.flushTimer.Stop()
		gc.flushTimer = nil
	}
	gc.flushPending = false

	// Take ownership of pending commits
	batch := gc.pending
	gc.pending = make([]*CommitRequest, 0, gc.config.MaxBatchSize)
	gc.mu.Unlock()

	// Perform the actual sync
	err := gc.wal.Sync()

	// Notify all waiters
	for _, req := range batch {
		req.Err = err
		close(req.Done)
	}

	// Update metrics
	atomic.AddUint64(&gc.totalBatches, 1)
	atomic.AddUint64(&gc.totalCommits, uint64(len(batch)))
	gc.updateAvgBatchSize(len(batch))

	return err
}

// flusherLoop runs in background and periodically flushes commits
func (gc *GroupCommitter) flusherLoop() {
	ticker := time.NewTicker(gc.config.MaxWaitTime)
	defer ticker.Stop()

	for {
		select {
		case <-gc.stopCh:
			return
		case <-ticker.C:
			// Periodic flush on timeout
			gc.mu.Lock()
			hasPending := len(gc.pending) > 0
			if hasPending {
				if gc.flushTimer != nil {
					gc.flushTimer.Stop()
					gc.flushTimer = nil
				}
				gc.flushPending = false
			}
			gc.mu.Unlock()
			if hasPending {
				gc.Flush()
			}
		case <-gc.flushComplete:
			// Immediate flush triggered
			gc.Flush()
		}
	}
}

// updateAvgBatchSize updates the average batch size
func (gc *GroupCommitter) updateAvgBatchSize(size int) {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.batchSizeHist = append(gc.batchSizeHist, size)
	if len(gc.batchSizeHist) > 100 {
		gc.batchSizeHist = gc.batchSizeHist[1:]
	}

	var sum int
	for _, s := range gc.batchSizeHist {
		sum += s
	}
	atomic.StoreUint64(&gc.avgBatchSize, uint64(sum)/uint64(len(gc.batchSizeHist)))
}

// GetStats returns group commit statistics
func (gc *GroupCommitter) GetStats() GroupCommitStats {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	return GroupCommitStats{
		Enabled:        gc.config.Enabled,
		PendingCommits: len(gc.pending),
		TotalBatches:   atomic.LoadUint64(&gc.totalBatches),
		TotalCommits:   atomic.LoadUint64(&gc.totalCommits),
		AvgBatchSize:   atomic.LoadUint64(&gc.avgBatchSize),
		MaxWaitTime:    gc.config.MaxWaitTime,
		MaxBatchSize:   gc.config.MaxBatchSize,
	}
}

// GroupCommitStats holds group commit statistics
type GroupCommitStats struct {
	Enabled        bool          `json:"enabled"`
	PendingCommits int           `json:"pending_commits"`
	TotalBatches   uint64        `json:"total_batches"`
	TotalCommits   uint64        `json:"total_commits"`
	AvgBatchSize   uint64        `json:"avg_batch_size"`
	MaxWaitTime    time.Duration `json:"max_wait_time"`
	MaxBatchSize   int           `json:"max_batch_size"`
}

// WALWithGroupCommit wraps a WAL with group commit support
type WALWithGroupCommit struct {
	wal *WAL
	gc  *GroupCommitter
}

// NewWALWithGroupCommit creates a new WAL with group commit support
func NewWALWithGroupCommit(wal *WAL, config *GroupCommitConfig) *WALWithGroupCommit {
	return &WALWithGroupCommit{
		wal: wal,
		gc:  NewGroupCommitter(wal, config),
	}
}

// AppendCommit appends a commit record and waits for group commit sync
func (w *WALWithGroupCommit) AppendCommit(record *WALRecord) error {
	// Append the record normally (but without sync)
	if err := w.wal.AppendWithoutSync(record); err != nil {
		return err
	}

	// Submit for group commit
	return w.gc.SubmitCommit(record.LSN)
}

// Append adds a record to the WAL (passthrough)
func (w *WALWithGroupCommit) Append(record *WALRecord) error {
	return w.wal.Append(record)
}

// AppendWithoutSync adds a record without syncing (passthrough)
func (w *WALWithGroupCommit) AppendWithoutSync(record *WALRecord) error {
	return w.wal.AppendWithoutSync(record)
}

// Sync syncs the WAL (passthrough)
func (w *WALWithGroupCommit) Sync() error {
	return w.wal.Sync()
}

// Checkpoint performs a checkpoint (passthrough)
func (w *WALWithGroupCommit) Checkpoint(bp *BufferPool) error {
	return w.wal.Checkpoint(bp)
}

// Recover recovers from the WAL (passthrough)
func (w *WALWithGroupCommit) Recover(bp *BufferPool) error {
	return w.wal.Recover(bp)
}

// Close closes the WAL
func (w *WALWithGroupCommit) Close() error {
	if w.gc != nil {
		w.gc.Stop()
	}
	return w.wal.Close()
}

// GetGroupCommitStats returns group commit statistics
func (w *WALWithGroupCommit) GetGroupCommitStats() GroupCommitStats {
	return w.gc.GetStats()
}
