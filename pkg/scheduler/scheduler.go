package scheduler

import (
	"context"
	"fmt"
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
	startMu      sync.Mutex
}

// Logger is a minimal logging interface.
type Logger interface {
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// noopLogger discards all log output.
type noopLogger struct{}

func (n *noopLogger) Infof(format string, args ...interface{}) {}
func (n *noopLogger) Warnf(format string, args ...interface{}) {}
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
	j.Status = JobStatusIdle
	if !j.Enabled {
		j.Status = JobStatusDisabled
	}
	j.NextRun = time.Now().Add(j.Interval)
	s.jobs[j.ID] = j
	s.logger.Infof("Registered job %s (%s) interval=%v", j.ID, j.Name, j.Interval)
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
	j.Status = JobStatusIdle
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
func (s *Scheduler) Trigger(jobID string) error {
	s.mu.RLock()
	j, ok := s.jobs[jobID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("job %s not found", jobID)
	}
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

	// Use a configurable resolution ticker — coarse enough to be cheap,
	// fine enough for typical maintenance intervals (minutes+).
	s.ticker = time.NewTicker(s.tickInterval)

	workCh := make(chan *Job, s.workers*4)

	// Start dispatcher
	s.wg.Add(1)
	go s.dispatchLoop(workCh)

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
		s.startMu.Unlock()
		return
	}
	s.started = false
	s.startMu.Unlock()

	close(s.stopCh)
	if s.ticker != nil {
		s.ticker.Stop()
	}
	s.wg.Wait()
	s.logger.Infof("Scheduler stopped")
}

// dispatchLoop ticks every second and sends overdue jobs to workers.
func (s *Scheduler) dispatchLoop(workCh chan<- *Job) {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			close(workCh)
			return
		case <-s.ticker.C:
		}

		now := time.Now()
		s.mu.RLock()
		var ready []*Job
		for _, j := range s.jobs {
			if !j.Enabled || j.Status == JobStatusRunning {
				continue
			}
			if now.After(j.NextRun) || now.Equal(j.NextRun) {
				ready = append(ready, j)
			}
		}
		s.mu.RUnlock()

		for _, j := range ready {
			select {
			case workCh <- j:
			case <-s.stopCh:
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
		if err := s.runJob(j); err != nil {
			s.logger.Errorf("Job %s failed: %v", j.ID, err)
		}
	}
}

// runJob executes a single job with retries and panic recovery.
func (s *Scheduler) runJob(j *Job) (err error) {
	s.mu.Lock()
	j.Status = JobStatusRunning
	s.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
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
		} else {
			j.LastError = nil
			j.Status = JobStatusIdle
		}
		s.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	maxRetries := j.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err = j.Fn(ctx)
		if err == nil {
			return nil
		}
		if attempt < maxRetries {
			s.logger.Warnf("Job %s attempt %d failed, retrying in %v: %v", j.ID, attempt+1, j.RetryDelay, err)
			time.Sleep(j.RetryDelay)
		}
	}
	return err
}
