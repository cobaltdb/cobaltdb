package audit

// Regression tests for the AUTO-rotation boundary (Config.MaxFileSize reached
// inside writeEvent, as opposed to a manual Rotate() call):
//
//   The event whose write triggers the rotation must join the NEW file's hash
//   chain. Previously it kept its pre-rotation PrevHash (the rotated file's
//   final hash), so the new segment's second entry failed VerifyLogFile with
//   "previous hash mismatch" and — because loadLastHash verifies the log
//   fail-closed at startup — the logger could not be reopened after a restart.
//   With encryption enabled the entry failed even earlier: it was sealed with
//   the old anchor as GCM AAD but verification decrypts with the expected
//   previous hash.

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestAutoRotationReanchorsTriggeringEvent(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.log")
	cfg := &Config{
		Enabled:         true,
		LogFile:         logFile,
		LogFormat:       "json",
		RotationEnabled: true,
		// Sized so entries 1 and 2 fit together but entry 3 overflows:
		// exactly one auto-rotation, triggered by entry 3's writeEvent.
		MaxFileSize: 1100,
		MaxBackups:  10,
		MaxAge:      30,
	}

	al, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	longAction := strings.Repeat("A", 150)
	writeEventSync(t, al, "E1_"+longAction)
	writeEventSync(t, al, "E2_"+longAction) // file grows to ~2 entries
	writeEventSync(t, al, "E3_"+longAction) // overflows MaxFileSize -> auto-rotate

	if err := al.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backups := findRotatedBackups(t, logFile)
	if len(backups) != 1 {
		t.Fatalf("expected exactly 1 auto-rotated backup, got %d: %v", len(backups), backups)
	}

	// THE regression: the auto-rotated current segment must verify. Pre-fix
	// this failed with "audit log line 2: previous hash mismatch".
	seg, err := VerifyLogFile(logFile, nil)
	if err != nil {
		t.Fatalf("VerifyLogFile on auto-rotated segment: %v", err)
	}
	// continuation + the rotation-triggering entry
	if seg.Entries != 2 {
		t.Errorf("current segment entries = %d, want 2", seg.Entries)
	}

	// The rotated-away segment is unaffected but must still verify.
	oldSeg, err := VerifyLogFile(backups[0], nil)
	if err != nil {
		t.Fatalf("VerifyLogFile on rotated-away segment: %v", err)
	}
	if oldSeg.Entries != 2 {
		t.Errorf("rotated-away segment entries = %d, want 2", oldSeg.Entries)
	}

	// Cross-file chain: the continuation record must embed the rotated
	// segment's final hash.
	if seg.PrevSegmentHash != oldSeg.LastHash {
		t.Errorf("chain boundary mismatch: continuation says %q, rotated segment ends at %q",
			seg.PrevSegmentHash, oldSeg.LastHash)
	}

	// Restart must succeed: loadLastHash verifies the auto-rotated segment.
	al2, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("reopen after auto-rotation: %v", err)
	}
	if err := al2.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}
}

func TestAutoRotationReanchorsTriggeringEventEncrypted(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.enc.log")
	key := []byte("0123456789abcdef0123456789abcdef") // AES-256
	cfg := &Config{
		Enabled:         true,
		LogFile:         logFile,
		LogFormat:       "json",
		RotationEnabled: true,
		MaxFileSize:     1100,
		MaxBackups:      10,
		MaxAge:          30,
		EncryptionKey:   key,
	}

	al, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	longAction := strings.Repeat("B", 150)
	writeEventSync(t, al, "K1_"+longAction)
	writeEventSync(t, al, "K2_"+longAction)
	writeEventSync(t, al, "K3_"+longAction) // overflows MaxFileSize -> auto-rotate

	if err := al.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backups := findRotatedBackups(t, logFile)
	if len(backups) != 1 {
		t.Fatalf("expected exactly 1 auto-rotated backup, got %d: %v", len(backups), backups)
	}

	// Pre-fix this failed at line 2 with "decrypt entry" (the triggering
	// entry was sealed with the pre-rotation anchor as AAD).
	seg, err := VerifyLogFile(logFile, key)
	if err != nil {
		t.Fatalf("VerifyLogFile on auto-rotated encrypted segment: %v", err)
	}
	if seg.Entries != 2 {
		t.Errorf("current segment entries = %d, want 2", seg.Entries)
	}

	oldSeg, err := VerifyLogFile(backups[0], key)
	if err != nil {
		t.Fatalf("VerifyLogFile on rotated-away encrypted segment: %v", err)
	}
	if seg.PrevSegmentHash != oldSeg.LastHash {
		t.Errorf("encrypted chain boundary mismatch: %q vs %q", seg.PrevSegmentHash, oldSeg.LastHash)
	}

	al2, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("reopen after encrypted auto-rotation: %v", err)
	}
	if err := al2.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}
}
