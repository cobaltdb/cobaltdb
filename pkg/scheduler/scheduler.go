package scheduler

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// Scheduler manages a collection of timed jobs and executes them
// on a configurable worker pool.
type Scheduler struct {
	jobs         map[string]*Job
	mu           sync.RWMutex
	workers      int
	stopCh       chan struct{}
	wg           sync.WaitGroup
	ticker       *time.Ticker
	tickInterval time.Duration
	logger       Logger
	started      bool
	stopped      bool // set by Stop(); cleared by Start(). Guards Trigger-after-Stop.
	startMu      sync.Mutex
	// runCtx is cancelled by Stop() so in-flight job functions (and the retry
	// backoff) abort promptly instead of blocking shutdown for up to a job's
	// full timeout plus retry delays.
	runCtx    context.Context
	runCancel context.CancelFunc
}

const maxSchedulerWorkers = 1024

// Logger is a minimal logging interface.
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// noopLogger discards all log output.
type noopLogger struct{}

func (n *noopLogger) Infof(format string, args ...interface{})  {}
func (n *noopLogger) Warnf(format string, args ...interface{})  {}
func (n *noopLogger) Errorf(format string, args ...interface{}) {}

// New creates a Scheduler with the given number of workers.
// If workers <= 0, defaults to 2.
func New(workers int, log Logger) *Scheduler {
	return NewWithInterval(workers, log, 1*time.Second)
}

// NewWithInterval creates a Scheduler with a custom dispatcher tick interval.
// Smaller intervals give finer scheduling resolution at the cost of more CPU.
func NewWithInterval(workers int, log Logger, tick time.Duration) *Scheduler {
	if workers <= 0 {
		workers = 2
	}
	if workers > maxSchedulerWorkers {
		workers = maxSchedulerWorkers
	}
	if log == nil {
		log = &noopLogger{}
	}
	if tick <= 0 {
		tick = 1 * time.Second
	}
	return &Scheduler{
		jobs:         make(map[string]*Job),
		workers:      workers,
		stopCh:       make(chan struct{}),
		logger:       log,
		tickInterval: tick,
	}
}

// Register adds a job to the scheduler. Returns error if a job with
// the same ID already exists or the job is invalid.
func (s *Scheduler) Register(j *Job) error {
	if err := j.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[j.ID]; exists {
		return fmt.Errorf("job %s already registered", j.ID)
	}
	registered := *j
	registered.LastRun = time.Time{}
	registered.LastError = nil
	registered.RunCount = 0
	registered.FailCount = 0
	registered.Status = JobStatusIdle
	if !registered.Enabled {
		registered.Status = JobStatusDisabled
	}
	registered.NextRun = time.Now().Add(registered.Interval)
	s.jobs[registered.ID] = &registered
	s.logger.Infof("Registered job %s (%s) interval=%v", registered.ID, registered.Name, registered.Interval)
	return nil
}

// Unregister removes a job from the scheduler.
func (s *Scheduler) Unregister(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, jobID)
}

// Get returns a snapshot of a registered job.
func (s *Scheduler) Get(jobID string) (JobSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return JobSnapshot{}, false
	}
	return j.Snapshot(), true
}

// List returns snapshots of all registered jobs.
func (s *Scheduler) List() []JobSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]JobSnapshot, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j.Snapshot())
	}
	return out
}

// Enable re-enables a disabled job.
func (s *Scheduler) Enable(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return false
	}
	j.Enabled = true
	// A run currently in flight keeps its Running status: resetting it to
	// Idle here would let the dispatcher (or Trigger) start a second
	// concurrent execution of the same job. runJob's deferred block settles
	// the status when the in-flight run finishes.
	if j.Status != JobStatusRunning {
		j.Status = JobStatusIdle
	}
	j.NextRun = time.Now().Add(j.Interval)
	return true
}

// Disable prevents a job from running.
func (s *Scheduler) Disable(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return false
	}
	j.Enabled = false
	j.Status = JobStatusDisabled
	return true
}

// Trigger executes a job immediately, outside its normal schedule.
// Triggered runs on a started scheduler are registered in the shutdown
// WaitGroup so Stop() waits for them instead of returning mid-job.
// Triggering a stopped scheduler returns an error.
func (s *Scheduler) Trigger(jobID string) error {
	// Register with the WaitGroup under startMu so the Add cannot race with
	// Stop's wg.Wait (Stop holds startMu for its entire duration). If Stop is
	// in progress, this blocks until it finishes and then reports "stopped".
	s.startMu.Lock()
	if s.stopped {
		s.startMu.Unlock()
		return fmt.Errorf("scheduler is stopped")
	}
	if s.started {
		s.wg.Add(1)
		defer s.wg.Done()
	}
	s.startMu.Unlock()

	s.mu.Lock()
	j, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("job %s not found", jobID)
	}
	if j.Status == JobStatusRunning {
		s.mu.Unlock()
		return fmt.Errorf("job %s is already running", jobID)
	}
	j.Status = JobStatusRunning
	s.mu.Unlock()

	return s.runJob(j)
}

// Start begins the scheduling loop. It is safe to call multiple times;
// only the first call has effect.
func (s *Scheduler) Start() {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.started {
		return
	}
	s.started = true
	s.stopped = false
	s.runCtx, s.runCancel = context.WithCancel(context.Background())

	// Use a configurable resolution ticker — coarse enough to be cheap,
	// fine enough for typical maintenance intervals (minutes+).
	s.ticker = time.NewTicker(s.tickInterval)

	workCh := make(chan *Job, s.workers*4)
	ticker := s.ticker
	stopCh := s.stopCh

	// Start dispatcher
	s.wg.Add(1)
	go s.dispatchLoop(workCh, ticker, stopCh)

	// Start workers
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker(workCh)
	}
}

// Stop halts the scheduler and waits for in-flight jobs.
func (s *Scheduler) Stop() {
	s.startMu.Lock()
	if !s.started {
		s.stopped = true
		s.startMu.Unlock()
		return
	}
	s.started = false
	s.stopped = true

	close(s.stopCh)
	if s.runCancel != nil {
		s.runCancel() // signal in-flight job functions to abort
	}
	if s.ticker != nil {
		s.ticker.Stop()
		s.ticker = nil
	}
	s.wg.Wait()
	s.stopCh = make(chan struct{})

	// The dispatcher marks jobs Running before enqueueing them; a job selected
	// but never dispatched (stop raced the enqueue) would otherwise be stranded
	// in Running forever — skipped by the dispatcher after a restart and
	// refused by Trigger. All genuinely running jobs have finished (wg.Wait
	// above and runJob's deferred status update), so anything still marked
	// Running here was never executed: reset it.
	s.mu.Lock()
	for _, j := range s.jobs {
		if j.Status == JobStatusRunning {
			if j.Enabled {
				j.Status = JobStatusIdle
			} else {
				j.Status = JobStatusDisabled
			}
		}
	}
	s.mu.Unlock()

	s.startMu.Unlock()
	s.logger.Infof("Scheduler stopped")
}

// dispatchLoop ticks every second and sends overdue jobs to workers.
func (s *Scheduler) dispatchLoop(workCh chan<- *Job, ticker *time.Ticker, stopCh <-chan struct{}) {
	defer s.wg.Done()
	for {
		select {
		case <-stopCh:
			close(workCh)
			return
		case <-ticker.C:
		}

		now := time.Now()
		s.mu.Lock()
		var ready []*Job
		for _, j := range s.jobs {
			if !j.Enabled || j.Status == JobStatusRunning {
				continue
			}
			if now.After(j.NextRun) || now.Equal(j.NextRun) {
				j.Status = JobStatusRunning
				ready = append(ready, j)
			}
		}
		s.mu.Unlock()

		for _, j := range ready {
			select {
			case workCh <- j:
			case <-stopCh:
				close(workCh)
				return
			}
		}
	}
}

// worker pulls jobs from the channel and executes them.
func (s *Scheduler) worker(ch <-chan *Job) {
	defer s.wg.Done()
	for j := range ch {
		if j == nil {
			return
		}
		s.mu.RLock()
		enabled := j.Enabled
		s.mu.RUnlock()
		if !enabled {
			s.mu.Lock()
			if !j.Enabled {
				j.Status = JobStatusDisabled
			}
			s.mu.Unlock()
			continue
		}
		if err := s.runJob(j); err != nil {
			s.logger.Errorf("Job %s failed: %v", j.ID, err)
		}
	}
}

// runJob executes a single job with retries and panic recovery.
// Each attempt (including retries) is individually protected against panics:
// a panicking attempt counts as a failed attempt and is retried up to
// MaxRetries like any other failure, and the recovered error includes the
// panic stack trace.
func (s *Scheduler) runJob(j *Job) (err error) {
	s.mu.Lock()
	j.Status = JobStatusRunning
	s.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			// Defensive: panics from job functions are recovered per-attempt in
			// runAttempt below; this only catches panics in the scheduler's own
			// retry plumbing.
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
		s.mu.Lock()
		j.LastRun = time.Now()
		j.NextRun = j.LastRun.Add(j.Interval)
		j.RunCount++
		if err != nil {
			j.LastError = err
			j.FailCount++
			j.Status = JobStatusFailed
			s.logger.Errorf("Job %s failed: %v", j.ID, err)
		} else if !j.Enabled {
			j.Status = JobStatusDisabled
		} else {
			j.LastError = nil
			j.Status = JobStatusIdle
		}
		s.mu.Unlock()
	}()

	// Derive from runCtx so Stop() cancellation propagates into the job and the
	// retry backoff. Fall back to Background if the scheduler was not started
	// via Start() (e.g. RunNow in tests).
	parent := s.runCtx
	if parent == nil {
		parent = context.Background()
	}
	timeout := 10 * time.Minute
	if j.Timeout > 0 {
		timeout = j.Timeout
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	maxRetries := j.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	// runAttempt wraps a single attempt in its own recovering closure so a
	// panic does not bypass the retry loop (MaxRetries is honored) and the
	// stack trace of the panic site is preserved in the error.
	runAttempt := func() (attemptErr error) {
		defer func() {
			if r := recover(); r != nil {
				attemptErr = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
			}
		}()
		return j.Fn(ctx)
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = runAttempt()
		if err == nil {
			return nil
		}
		if attempt < maxRetries {
			s.logger.Warnf("Job %s attempt %d failed, retrying in %v: %v", j.ID, attempt+1, j.RetryDelay, err)
			select {
			case <-time.After(j.RetryDelay):
			case <-ctx.Done():
				return err // shutdown or timeout: stop retrying
			}
		}
	}
	return err
}
