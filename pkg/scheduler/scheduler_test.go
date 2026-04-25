package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerRegisterAndRun(t *testing.T) {
	var count atomic.Int32
	s := NewWithInterval(1, nil, 50*time.Millisecond)

	job := &Job{
		ID:       "test-job",
		Name:     "Test Job",
		Type:     JobTypeCustom,
		Interval: 100 * time.Millisecond,
		Enabled:  true,
		Fn: func(ctx context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(job); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	s.Start()
	time.Sleep(350 * time.Millisecond)
	s.Stop()

	if count.Load() < 2 {
		t.Fatalf("expected at least 2 runs, got %d", count.Load())
	}
}

func TestSchedulerDisableEnable(t *testing.T) {
	var count atomic.Int32
	s := NewWithInterval(1, nil, 50*time.Millisecond)

	job := &Job{
		ID:       "toggle-job",
		Name:     "Toggle Job",
		Type:     JobTypeCustom,
		Interval: 100 * time.Millisecond,
		Enabled:  true,
		Fn: func(ctx context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(job); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	s.Start()
	time.Sleep(150 * time.Millisecond)

	s.Disable("toggle-job")
	before := count.Load()
	time.Sleep(300 * time.Millisecond)

	if count.Load() != before {
		t.Fatalf("expected no runs while disabled, got %d", count.Load())
	}

	s.Enable("toggle-job")
	time.Sleep(250 * time.Millisecond)
	s.Stop()

	if count.Load() <= before {
		t.Fatalf("expected runs after re-enable, got %d", count.Load())
	}
}

func TestSchedulerTrigger(t *testing.T) {
	var count atomic.Int32
	s := New(1, nil)

	job := &Job{
		ID:       "trigger-job",
		Name:     "Trigger Job",
		Type:     JobTypeCustom,
		Interval: 10 * time.Minute,
		Enabled:  true,
		Fn: func(ctx context.Context) error {
			count.Add(1)
			return nil
		},
	}

	if err := s.Register(job); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	s.Start()
	if err := s.Trigger("trigger-job"); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	s.Stop()

	if count.Load() != 1 {
		t.Fatalf("expected 1 run, got %d", count.Load())
	}
}

func TestSchedulerRetry(t *testing.T) {
	var count atomic.Int32
	s := New(1, nil)

	job := &Job{
		ID:         "retry-job",
		Name:       "Retry Job",
		Type:       JobTypeCustom,
		Interval:   10 * time.Minute,
		Enabled:    true,
		MaxRetries: 2,
		RetryDelay: 10 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			if count.Add(1) <= 2 {
				return errors.New("temporary failure")
			}
			return nil
		},
	}

	if err := s.Register(job); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	s.Start()
	if err := s.Trigger("retry-job"); err != nil {
		t.Fatalf("trigger failed: %v", err)
	}
	s.Stop()

	if count.Load() != 3 {
		t.Fatalf("expected 3 attempts (2 retries), got %d", count.Load())
	}
}

func TestSchedulerPanicRecovery(t *testing.T) {
	var count atomic.Int32
	s := New(1, nil)

	job := &Job{
		ID:       "panic-job",
		Name:     "Panic Job",
		Type:     JobTypeCustom,
		Interval: 10 * time.Minute,
		Enabled:  true,
		Fn: func(ctx context.Context) error {
			count.Add(1)
			panic("intentional panic")
		},
	}

	if err := s.Register(job); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	s.Start()
	if err := s.Trigger("panic-job"); err == nil {
		t.Fatal("expected error from panic recovery")
	}
	s.Stop()

	if count.Load() != 1 {
		t.Fatalf("expected 1 run attempt, got %d", count.Load())
	}

	snap, ok := s.Get("panic-job")
	if !ok {
		t.Fatal("expected job snapshot")
	}
	if snap.Status != string(JobStatusFailed) {
		t.Fatalf("expected failed status, got %s", snap.Status)
	}
}

func TestSchedulerDuplicateID(t *testing.T) {
	s := New(1, nil)
	j1 := &Job{ID: "dup", Interval: time.Second, Fn: func(ctx context.Context) error { return nil }}
	j2 := &Job{ID: "dup", Interval: time.Second, Fn: func(ctx context.Context) error { return nil }}

	if err := s.Register(j1); err != nil {
		t.Fatalf("first register failed: %v", err)
	}
	if err := s.Register(j2); err == nil {
		t.Fatal("expected duplicate ID error")
	}
}

func TestSchedulerInvalidJob(t *testing.T) {
	s := New(1, nil)
	if err := s.Register(&Job{ID: "bad", Interval: 0, Fn: func(ctx context.Context) error { return nil }}); err == nil {
		t.Fatal("expected interval validation error")
	}
	if err := s.Register(&Job{ID: "bad2", Interval: time.Second}); err == nil {
		t.Fatal("expected fn validation error")
	}
}

func TestSchedulerList(t *testing.T) {
	s := New(1, nil)
	j1 := &Job{ID: "a", Name: "A", Interval: time.Second, Fn: func(ctx context.Context) error { return nil }}
	j2 := &Job{ID: "b", Name: "B", Interval: time.Second, Fn: func(ctx context.Context) error { return nil }}

	_ = s.Register(j1)
	_ = s.Register(j2)

	list := s.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(list))
	}
}

func TestSchedulerStopIdempotent(t *testing.T) {
	s := New(1, nil)
	s.Stop() // should not panic
	s.Stop() // should not panic
}

func TestSchedulerUnregister(t *testing.T) {
	s := New(1, nil)
	j := &Job{ID: "u", Interval: time.Second, Fn: func(ctx context.Context) error { return nil }}
	_ = s.Register(j)

	if len(s.List()) != 1 {
		t.Fatalf("expected 1 job before unregister")
	}

	s.Unregister("u")
	if len(s.List()) != 0 {
		t.Fatalf("expected 0 jobs after unregister")
	}

	// Unregister non-existent should not panic
	s.Unregister("missing")
}

func TestSchedulerGetMissing(t *testing.T) {
	s := New(1, nil)
	_, ok := s.Get("missing")
	if ok {
		t.Error("expected false for missing job")
	}
}

func TestSchedulerTriggerMissing(t *testing.T) {
	s := New(1, nil)
	err := s.Trigger("missing")
	if err == nil {
		t.Error("expected error for missing job")
	}
}

func TestSchedulerEnableDisableMissing(t *testing.T) {
	s := New(1, nil)
	if s.Enable("missing") {
		t.Error("expected false for enabling missing job")
	}
	if s.Disable("missing") {
		t.Error("expected false for disabling missing job")
	}
}

func TestSchedulerStartIdempotent(t *testing.T) {
	s := New(1, nil)
	s.Start()
	s.Start() // second start should be no-op
	s.Stop()
}

func TestSchedulerNewWithIntervalDefaults(t *testing.T) {
	// workers <= 0 should default to 2
	s := NewWithInterval(0, nil, 100*time.Millisecond)
	if s.workers != 2 {
		t.Errorf("expected workers=2, got %d", s.workers)
	}

	// tick <= 0 should default to 1s
	s2 := NewWithInterval(1, nil, 0)
	if s2.tickInterval != 1*time.Second {
		t.Errorf("expected tickInterval=1s, got %v", s2.tickInterval)
	}
}

func TestSchedulerWorkerNilJob(t *testing.T) {
	s := New(1, nil)
	ch := make(chan *Job, 1)
	ch <- nil
	close(ch)

	s.wg.Add(1)
	s.worker(ch)
	// should return early on nil job without panic
}

func TestJobValidateMissingID(t *testing.T) {
	j := &Job{Interval: time.Second, Fn: func(ctx context.Context) error { return nil }}
	if err := j.Validate(); err == nil {
		t.Error("expected error for missing ID")
	}
}
