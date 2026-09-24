package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestEnableDuringRunKeepsSingleExecutionInvariant pins the scheduler's
// single-execution invariant across an Enable call: at most one execution of
// a job may run at a time, even when Enable lands while a run is in flight.
//
// Regression: Enable unconditionally reset the job's status to Idle. The
// single-run invariant is enforced purely through Status == Running (the
// dispatcher skips Running jobs; Trigger refuses them), so calling Enable
// during a long-running job clobbered that marker and a second concurrent
// execution of the same job could start — via the ticker once NextRun passed
// or immediately via Trigger.
func TestEnableDuringRunKeepsSingleExecutionInvariant(t *testing.T) {
	release := make(chan struct{})
	var concurrent, maxConcurrent int64

	fn := func(ctx context.Context) error {
		n := atomic.AddInt64(&concurrent, 1)
		for {
			m := atomic.LoadInt64(&maxConcurrent)
			if n <= m || atomic.CompareAndSwapInt64(&maxConcurrent, m, n) {
				break
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		atomic.AddInt64(&concurrent, -1)
		return nil
	}

	s := NewWithInterval(2, nil, 20*time.Millisecond)
	job := &Job{
		ID:       "enable-during-run",
		Name:     "enable-during-run",
		Type:     JobTypeCustom,
		Interval: 50 * time.Millisecond,
		Enabled:  true,
		Fn:       fn,
	}
	if err := s.Register(job); err != nil {
		t.Fatalf("register: %v", err)
	}
	s.Start()
	defer s.Stop()

	// Wait until the first scheduled run is in flight.
	deadline := time.Now().Add(3 * time.Second)
	for {
		snap, ok := s.Get(job.ID)
		if ok && snap.Status == string(JobStatusRunning) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first scheduled run never started")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The regression: Enable during the run must not clobber the Running
	// marker.
	s.Enable(job.ID)
	if snap, _ := s.Get(job.ID); snap.Status != string(JobStatusRunning) {
		t.Fatalf("Enable during a run reset the status to %q, want running", snap.Status)
	}

	// Trigger must therefore refuse a second concurrent execution. Decide the
	// outcome BEFORE releasing the first run: either the attempt is refused
	// (triggerDone closes with an error) or a second execution enters Fn
	// (maxConcurrent exceeds 1 — the pre-fix behavior). Closing release
	// earlier would let the in-flight run finish and make the refusal check
	// racy.
	triggerDone := make(chan struct{})
	var triggerErr error
	go func() {
		defer close(triggerDone)
		triggerErr = s.Trigger(job.ID)
	}()

	deadline = time.Now().Add(3 * time.Second)
	decided := false
	for !decided && time.Now().Before(deadline) {
		select {
		case <-triggerDone:
			decided = true
		default:
		}
		if atomic.LoadInt64(&maxConcurrent) > 1 {
			decided = true
		}
		if !decided {
			time.Sleep(2 * time.Millisecond)
		}
	}
	if !decided {
		t.Fatal("trigger attempt was never decided (neither refused nor admitted)")
	}

	close(release)
	select {
	case <-triggerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("trigger did not complete")
	}

	if triggerErr == nil {
		t.Fatal("Trigger during a running job was accepted, want refusal")
	}
	if max := atomic.LoadInt64(&maxConcurrent); max > 1 {
		t.Fatalf("observed %d concurrent executions of the same job", max)
	}
}
