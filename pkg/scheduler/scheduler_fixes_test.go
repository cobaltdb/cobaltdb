package scheduler

// Regression tests for the scheduler shutdown/retry fixes:
//   a. jobs marked Running but never dispatched are reset on Stop
//   b. Trigger-launched jobs are tracked by the shutdown WaitGroup;
//      Trigger after Stop returns an error
//   c. panics count as failed attempts (MaxRetries honored) and the
//      recovered error carries the panic stack

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestStopResetsStrandedRunningJobs simulates the dispatcher marking a job
// Running without it ever reaching a worker (stop raced the enqueue). After
// Stop, the job must be back to Idle so a later Start/Trigger can run it.
func TestStopResetsStrandedRunningJobs(t *testing.T) {
	var count atomic.Int32
	s := NewWithInterval(1, nil, 10*time.Millisecond)
	job := &Job{
		ID:       "stranded",
		Name:     "Stranded",
		Type:     JobTypeCustom,
		Interval: time.Hour,
		Enabled:  true,
		Fn: func(ctx context.Context) error {
			count.Add(1)
			return nil
		},
	}
	if err := s.Register(job); err != nil {
		t.Fatalf("register: %v", err)
	}

	s.Start()
	// Simulate a job the dispatcher selected (Status=Running) but whose
	// dispatch was lost to shutdown.
	s.mu.Lock()
	s.jobs["stranded"].Status = JobStatusRunning
	s.mu.Unlock()

	s.Stop()

	snap, ok := s.Get("stranded")
	if !ok {
		t.Fatal("job missing")
	}
	if snap.Status != string(JobStatusIdle) {
		t.Fatalf("stranded job status after Stop = %s, want idle", snap.Status)
	}

	// After a restart the job must be runnable again (old behavior: dispatcher
	// skipped it and Trigger refused, forever).
	s.Start()
	if err := s.Trigger("stranded"); err != nil {
		t.Fatalf("trigger after restart: %v", err)
	}
	s.Stop()
	if count.Load() != 1 {
		t.Fatalf("expected 1 run after restart, got %d", count.Load())
	}
}

// TestStopResetsStrandedDisabledJob covers the disabled variant of the sweep.
func TestStopResetsStrandedDisabledJob(t *testing.T) {
	s := NewWithInterval(1, nil, 10*time.Millisecond)
	job := &Job{
		ID:       "stranded-disabled",
		Interval: time.Hour,
		Enabled:  true,
		Fn:       func(ctx context.Context) error { return nil },
	}
	if err := s.Register(job); err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Start()
	s.mu.Lock()
	s.jobs["stranded-disabled"].Status = JobStatusRunning
	s.jobs["stranded-disabled"].Enabled = false
	s.mu.Unlock()
	s.Stop()

	snap, _ := s.Get("stranded-disabled")
	if snap.Status != string(JobStatusDisabled) {
		t.Fatalf("stranded disabled job status after Stop = %s, want disabled", snap.Status)
	}
}

// TestStopWaitsForTriggeredJob verifies Stop() blocks until a Trigger-launched
// job has fully finished (old code only tracked dispatcher/worker goroutines,
// so Stop could return mid-job).
func TestStopWaitsForTriggeredJob(t *testing.T) {
	entered := make(chan struct{})
	var finished atomic.Bool
	s := NewWithInterval(1, nil, time.Hour) // dispatcher effectively idle
	job := &Job{
		ID:       "tracked",
		Interval: time.Hour,
		Enabled:  true,
		Timeout:  10 * time.Minute,
		Fn: func(ctx context.Context) error {
			close(entered)
			<-ctx.Done() // released by Stop's runCtx cancellation
			time.Sleep(20 * time.Millisecond)
			finished.Store(true)
			return nil
		},
	}
	if err := s.Register(job); err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Start()

	triggerDone := make(chan error, 1)
	go func() { triggerDone <- s.Trigger("tracked") }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("triggered job did not start")
	}

	stopDone := make(chan struct{})
	go func() { s.Stop(); close(stopDone) }()

	select {
	case <-stopDone:
		if !finished.Load() {
			t.Fatal("Stop() returned while the triggered job was still running")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not complete")
	}
	if err := <-triggerDone; err != nil {
		t.Fatalf("trigger: %v", err)
	}
}

// TestTriggerAfterStopRejected verifies Trigger on a stopped scheduler errors
// instead of racing the WaitGroup / running unsupervised.
func TestTriggerAfterStopRejected(t *testing.T) {
	var count atomic.Int32
	s := NewWithInterval(1, nil, time.Hour)
	job := &Job{
		ID:       "after-stop",
		Interval: time.Hour,
		Enabled:  true,
		Fn: func(ctx context.Context) error {
			count.Add(1)
			return nil
		},
	}
	if err := s.Register(job); err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Start()
	s.Stop()

	if err := s.Trigger("after-stop"); err == nil {
		t.Fatal("expected Trigger after Stop to fail")
	}
	if count.Load() != 0 {
		t.Fatalf("job ran after Stop: %d", count.Load())
	}

	// Start clears the stopped state; Trigger works again.
	s.Start()
	if err := s.Trigger("after-stop"); err != nil {
		t.Fatalf("trigger after restart: %v", err)
	}
	s.Stop()
	if count.Load() != 1 {
		t.Fatalf("expected 1 run after restart, got %d", count.Load())
	}
}

// TestPanicCountsAsFailedAttemptAndRetries verifies a panicking attempt is
// retried per MaxRetries (old code: the first panic bypassed the retry loop
// entirely) and that a recovering attempt can still succeed.
func TestPanicCountsAsFailedAttemptAndRetries(t *testing.T) {
	var attempts atomic.Int32
	s := NewWithInterval(1, nil, time.Hour)
	job := &Job{
		ID:         "panic-retry",
		Interval:   time.Hour,
		Enabled:    true,
		MaxRetries: 2,
		RetryDelay: 5 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			if attempts.Add(1) <= 2 {
				panic("attempt panic")
			}
			return nil
		},
	}
	if err := s.Register(job); err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Start()
	defer s.Stop()

	if err := s.Trigger("panic-retry"); err != nil {
		t.Fatalf("expected success on third attempt, got %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attempts (2 panics + 1 success), got %d", got)
	}
	snap, _ := s.Get("panic-retry")
	if snap.Status != string(JobStatusIdle) {
		t.Fatalf("job status = %s, want idle", snap.Status)
	}
}

// TestPanicErrorIncludesStack verifies the recovered panic error preserves the
// stack trace of the panic site.
func TestPanicErrorIncludesStack(t *testing.T) {
	var attempts atomic.Int32
	s := NewWithInterval(1, nil, time.Hour)
	job := &Job{
		ID:         "panic-stack",
		Interval:   time.Hour,
		Enabled:    true,
		MaxRetries: 1,
		RetryDelay: time.Millisecond,
		Fn: func(ctx context.Context) error {
			attempts.Add(1)
			panic("boom with stack")
		},
	}
	if err := s.Register(job); err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Start()
	defer s.Stop()

	err := s.Trigger("panic-stack")
	if err == nil {
		t.Fatal("expected error from always-panicking job")
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 attempts (MaxRetries=1), got %d", got)
	}
	if !strings.Contains(err.Error(), "boom with stack") {
		t.Fatalf("error does not contain panic value: %v", err)
	}
	if !strings.Contains(err.Error(), "goroutine ") {
		t.Fatalf("error does not contain a stack trace: %v", err)
	}
	snap, _ := s.Get("panic-stack")
	if snap.Status != string(JobStatusFailed) {
		t.Fatalf("job status = %s, want failed", snap.Status)
	}
	if !strings.Contains(snap.LastError, "panic") {
		t.Fatalf("snapshot LastError missing panic info: %s", snap.LastError)
	}
}
