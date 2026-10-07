package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerStopOwnsPrestartTriggers(t *testing.T) {
	for _, mode := range []string{"started-control", "never-started", "start-during-run", "disabled-manual"} {
		t.Run(mode, func(t *testing.T) {
			s := NewWithInterval(1, nil, time.Hour)
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			if err := s.Register(&Job{ID: "manual", Interval: time.Hour, Enabled: mode != "disabled-manual", Fn: func(ctx context.Context) error {
				if calls.Add(1) > 1 {
					return ctx.Err() // A restarted run must have a fresh, live parent.
				}
				close(entered)
				select {
				case <-ctx.Done():
					close(canceled)
				case <-release:
					return nil
				}
				<-release
				return nil
			}}); err != nil {
				t.Fatal(err)
			}
			if mode == "started-control" {
				s.Start()
			}
			triggerDone := make(chan error, 1)
			go func() { triggerDone <- s.Trigger("manual") }()
			<-entered
			if err := s.Trigger("manual"); err == nil {
				close(release)
				<-triggerDone
				s.Stop()
				t.Fatal("accepted concurrent execution")
			}
			if mode == "start-during-run" {
				s.Start()
				s.Start() // Initial Start remains idempotent while a manual run is active.
			}
			stopDone := make(chan struct{})
			go func() { s.Stop(); close(stopDone) }()
			canceledBeforeReturn, waited := false, false
			select {
			case <-canceled:
				canceledBeforeReturn = true
				select {
				case <-stopDone:
				default:
					waited = true
				}
			case <-stopDone:
			case <-time.After(5 * time.Second): // Watchdog only.
				close(release)
				t.Fatal("lifecycle watchdog expired")
			}
			snap, _ := s.Get("manual")
			close(release)
			if err := <-triggerDone; err != nil {
				t.Fatal(err)
			}
			<-stopDone
			if !canceledBeforeReturn || !waited || snap.Status != string(JobStatusRunning) {
				t.Fatal("Stop failed to own the accepted manual run")
			}
			s.Stop() // Repeated Stop must not close an already closed channel.
			if err := s.Trigger("manual"); err == nil {
				t.Fatal("accepted Trigger after Stop")
			}
			s.Start()
			if err := s.Trigger("manual"); err != nil {
				t.Fatal("restart context:", err)
			}
			s.Stop()
			snap, _ = s.Get("manual")
			wantStatus := string(JobStatusIdle)
			if mode == "disabled-manual" {
				wantStatus = string(JobStatusDisabled)
			}
			if calls.Load() != 2 || snap.RunCount != 2 || snap.FailCount != 0 || snap.Status != wantStatus {
				t.Fatalf("incorrect final ownership/state: calls=%d snapshot=%+v", calls.Load(), snap)
			}
		})
	}
	t.Run("missing-job-prestart", func(t *testing.T) {
		s := New(1, nil)
		if err := s.Trigger("missing"); err == nil {
			t.Fatal("missing job accepted")
		}
		done := make(chan struct{})
		go func() { s.Stop(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("failed Trigger leaked shutdown accounting")
		}
	})
}
