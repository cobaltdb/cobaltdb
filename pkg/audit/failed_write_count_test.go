package audit

import (
	"testing"
)

// A healthy logger reports zero failed writes; the counter is the
// programmatic signal operators alert on when audit records are dropped.
func TestFailedWriteCountStartsZeroAndCountsFailures(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Enabled:   true,
		LogFile:   dir + "/audit.log",
		LogFormat: "json",
	}
	al, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer al.Close()

	if got := al.FailedWriteCount(); got != 0 {
		t.Fatalf("expected 0 failed writes on a healthy logger, got %d", got)
	}

	// Inject a write failure deterministically: close the underlying file so
	// writeEvent fails, then drive a flush directly. This exercises the
	// per-event write-error accounting without relying on writer-goroutine
	// timing or filling the async channel.
	al.mu.Lock()
	if al.file != nil {
		_ = al.file.Close()
	}
	al.mu.Unlock()

	al.flushBatch([]*Event{
		{Type: EventAdmin, User: "tester", Action: "A1"},
		{Type: EventAdmin, User: "tester", Action: "A2"},
	})

	if got := al.FailedWriteCount(); got < 2 {
		t.Fatalf("expected FailedWriteCount >= 2 after forced write failures, got %d", got)
	}

	// The file==nil path must also be counted, not silently dropped.
	before := al.FailedWriteCount()
	al.mu.Lock()
	al.file = nil
	al.mu.Unlock()
	al.flushBatch([]*Event{{Type: EventAdmin, User: "tester", Action: "A3"}})
	if got := al.FailedWriteCount(); got != before+1 {
		t.Fatalf("expected file==nil drop to be counted (%d -> %d), got %d", before, before+1, got)
	}
}
