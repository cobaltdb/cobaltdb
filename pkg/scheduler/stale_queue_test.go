package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerSkipsUnregisteredAndReplacedQueuedJobs(t *testing.T) {
	for _, mode := range []string{"control", "removed", "replaced", "disabled", "repeated", "empty"} {
		t.Run(mode, func(t *testing.T) {
			s := New(1, nil)
			entered, release := make(chan struct{}), make(chan struct{})
			var oldCalls, newCalls, controlCalls atomic.Int32
			for _, job := range []*Job{
				{ID: "block", Interval: time.Hour, Enabled: true, Fn: func(context.Context) error { close(entered); <-release; return nil }},
				{ID: "queued", Interval: time.Hour, Enabled: true, Fn: func(context.Context) error { oldCalls.Add(1); return nil }},
				{ID: "control", Interval: time.Hour, Enabled: true, Fn: func(context.Context) error { controlCalls.Add(1); return nil }},
			} {
				if err := s.Register(job); err != nil {
					t.Fatal(err)
				}
			}
			work := make(chan *Job, 5)
			s.wg.Add(1)
			go s.worker(work)
			s.mu.Lock()
			old := s.jobs["queued"]
			old.Status = JobStatusRunning
			currentControl := s.jobs["control"]
			work <- s.jobs["block"]
			s.mu.Unlock()
			<-entered
			if mode != "empty" {
				work <- old
			}
			if mode == "repeated" {
				work <- old
			}
			if mode == "removed" || mode == "replaced" || mode == "repeated" || mode == "empty" {
				s.Unregister("queued")
			}
			if mode == "disabled" {
				s.Disable("queued")
			}
			if mode == "replaced" || mode == "repeated" {
				if err := s.Register(&Job{ID: "queued", Interval: time.Hour, Enabled: true, Fn: func(context.Context) error { newCalls.Add(1); return nil }}); err != nil {
					t.Fatal(err)
				}
				s.mu.Lock()
				replacement := s.jobs["queued"]
				replacement.Status = JobStatusRunning
				s.mu.Unlock()
				work <- replacement
			}
			work <- currentControl
			close(work)
			close(release) // Old queue entries complete only after removal/replacement.
			s.wg.Wait()
			wantOld, wantNew := int32(0), int32(0)
			if mode == "control" {
				wantOld = 1
			}
			if mode == "replaced" || mode == "repeated" {
				wantNew = 1
			}
			if oldCalls.Load() != wantOld || newCalls.Load() != wantNew || controlCalls.Load() != 1 {
				t.Fatal("unexpected callback ownership")
			}
			snap, exists := s.Get("queued")
			if mode == "removed" || mode == "empty" {
				if exists {
					t.Fatal("removed instance reappeared")
				}
			}
			if wantNew == 1 && (!exists || snap.RunCount != 1 || snap.Status != string(JobStatusIdle)) {
				t.Fatalf("replacement state corrupted: %+v", snap)
			}
			if mode == "disabled" && (!exists || snap.RunCount != 0 || snap.Status != string(JobStatusDisabled)) {
				t.Fatalf("disabled state: %+v", snap)
			}
			s.Stop()
			s.Start()
			if err := s.Trigger("control"); err != nil {
				t.Fatal(err)
			}
			s.Stop()
			if controlCalls.Load() != 2 {
				t.Fatal("restart control failed")
			}
		})
	}
}
