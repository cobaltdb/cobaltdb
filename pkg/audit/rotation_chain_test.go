package audit

// Regression tests for audit log rotation:
//   - The hash chain must reset per file so a rotated log both verifies and
//     can be reopened after a restart (previously the new file's first entry
//     carried PrevHash from the old file and loadLastHash/VerifyLogFile
//     rejected it, making the logger fail to reopen).
//   - Rotation must prune backups beyond MaxBackups and older than MaxAge.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeEventSync writes an event through the real writeEvent path (hash chain,
// rotation checks) without depending on the async writer's flush timing.
func writeEventSync(t *testing.T, al *Logger, action string) {
	t.Helper()
	al.mu.Lock()
	defer al.mu.Unlock()
	ev := &Event{
		Timestamp: time.Now().UTC(),
		Type:      EventQuery,
		EventID:   generateEventID(),
		User:      "tester",
		Action:    action,
		Status:    "SUCCESS",
	}
	if err := al.writeEvent(ev); err != nil {
		t.Fatalf("writeEvent(%s): %v", action, err)
	}
	if err := al.file.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func findRotatedBackups(t *testing.T, logFile string) []string {
	t.Helper()
	matches, err := filepath.Glob(logFile + ".*")
	if err != nil {
		t.Fatalf("glob backups: %v", err)
	}
	return matches
}

func TestRotationChainReopenAndVerify(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.log")
	cfg := &Config{
		Enabled:         true,
		LogFile:         logFile,
		LogFormat:       "json",
		RotationEnabled: true,
		MaxFileSize:     1024 * 1024, // large: rotate manually
		MaxBackups:      10,
		MaxAge:          30,
	}

	al, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	writeEventSync(t, al, "EVENT_1")
	writeEventSync(t, al, "EVENT_2")

	if err := al.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	writeEventSync(t, al, "EVENT_3")
	writeEventSync(t, al, "EVENT_4")

	if err := al.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Regression: reopening a rotated audit log must succeed. Before the fix,
	// loadLastHash -> VerifyLogFile rejected the new file's first entry
	// (PrevHash carried over from the pre-rotation file).
	al2, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("reopen after rotation must succeed, got: %v", err)
	}
	writeEventSync(t, al2, "EVENT_5")
	if err := al2.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}

	// Both segments must verify independently.
	backups := findRotatedBackups(t, logFile)
	if len(backups) != 1 {
		t.Fatalf("expected 1 rotated backup, got %d: %v", len(backups), backups)
	}
	seg1, err := VerifyLogFile(backups[0], nil)
	if err != nil {
		t.Fatalf("verify rotated segment: %v", err)
	}
	if seg1.Entries != 2 {
		t.Errorf("rotated segment entries = %d, want 2", seg1.Entries)
	}

	seg2, err := VerifyLogFile(logFile, nil)
	if err != nil {
		t.Fatalf("verify current segment: %v", err)
	}
	// continuation + EVENT_3 + EVENT_4 + EVENT_5
	if seg2.Entries != 4 {
		t.Errorf("current segment entries = %d, want 4", seg2.Entries)
	}

	// And the cross-file chain must be checkable: the current segment's
	// continuation record embeds the rotated segment's final hash.
	if seg2.PrevSegmentHash == "" {
		t.Fatal("current segment is missing the chain-continuation boundary hash")
	}
	if seg2.PrevSegmentHash != seg1.LastHash {
		t.Errorf("chain boundary mismatch: continuation says %q, rotated segment ends at %q",
			seg2.PrevSegmentHash, seg1.LastHash)
	}
	if seg2.PrevSegmentFile != backups[0] {
		t.Errorf("continuation previous_file = %q, want %q", seg2.PrevSegmentFile, backups[0])
	}
}

func TestRotationChainWithEncryption(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.enc.log")
	key := []byte("0123456789abcdef0123456789abcdef") // AES-256
	cfg := &Config{
		Enabled:         true,
		LogFile:         logFile,
		LogFormat:       "json",
		RotationEnabled: true,
		MaxFileSize:     1024 * 1024,
		MaxBackups:      10,
		MaxAge:          30,
		EncryptionKey:   key,
	}

	al, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	writeEventSync(t, al, "ENC_EVENT_1")
	if err := al.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	writeEventSync(t, al, "ENC_EVENT_2")
	if err := al.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen must succeed and both segments must verify with the key.
	al2, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("reopen encrypted rotated log: %v", err)
	}
	if err := al2.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}

	backups := findRotatedBackups(t, logFile)
	if len(backups) != 1 {
		t.Fatalf("expected 1 rotated backup, got %v", backups)
	}
	seg1, err := VerifyLogFile(backups[0], key)
	if err != nil {
		t.Fatalf("verify rotated encrypted segment: %v", err)
	}
	seg2, err := VerifyLogFile(logFile, key)
	if err != nil {
		t.Fatalf("verify current encrypted segment: %v", err)
	}
	if seg2.PrevSegmentHash != seg1.LastHash {
		t.Errorf("encrypted chain boundary mismatch: %q vs %q", seg2.PrevSegmentHash, seg1.LastHash)
	}
}

func TestPruneBackupsMaxBackupsAndMaxAge(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.log")
	cfg := &Config{
		Enabled:    false, // prune logic only needs config
		LogFile:    logFile,
		LogFormat:  "json",
		MaxBackups: 2,
		MaxAge:     7,
	}
	al, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	now := time.Now()
	mkBackup := func(ts time.Time) string {
		name := logFile + "." + ts.Format("20060102_150405")
		if err := os.WriteFile(name, []byte("backup\n"), 0600); err != nil {
			t.Fatalf("write backup %s: %v", name, err)
		}
		return name
	}

	// Three recent backups (count pruning should delete the oldest one) and
	// one 30-day-old backup (age pruning should delete it regardless).
	newest := mkBackup(now.Add(-1 * time.Minute))
	middle := mkBackup(now.Add(-2 * time.Minute))
	oldest := mkBackup(now.Add(-3 * time.Minute))
	ancient := mkBackup(now.AddDate(0, 0, -30))
	// A non-backup sibling file must never be touched.
	unrelated := logFile + ".notatimestamp"
	if err := os.WriteFile(unrelated, []byte("keep\n"), 0600); err != nil {
		t.Fatalf("write unrelated: %v", err)
	}

	al.mu.Lock()
	al.pruneBackupsLocked()
	al.mu.Unlock()

	assertExists := func(path string, want bool) {
		t.Helper()
		_, err := os.Stat(path)
		exists := err == nil
		if exists != want {
			t.Errorf("%s exists = %v, want %v", path, exists, want)
		}
	}
	assertExists(newest, true)
	assertExists(middle, true)
	assertExists(oldest, false)  // beyond MaxBackups=2
	assertExists(ancient, false) // older than MaxAge=7d
	assertExists(unrelated, true)
}

func TestRotationEnforcesMaxBackups(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.log")
	cfg := &Config{
		Enabled:         true,
		LogFile:         logFile,
		LogFormat:       "json",
		RotationEnabled: true,
		MaxFileSize:     1024 * 1024,
		MaxBackups:      2,
		MaxAge:          30,
	}
	al, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := al.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	// Rotation names backups with second granularity; pre-create older backup
	// files so this rotation triggers pruning deterministically.
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("%s.%s", logFile, time.Now().Add(-time.Duration(i)*time.Hour).Format("20060102_150405"))
		if err := os.WriteFile(name, []byte("old backup\n"), 0600); err != nil {
			t.Fatalf("write old backup: %v", err)
		}
	}

	writeEventSync(t, al, "EVENT_A")
	if err := al.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	backups := findRotatedBackups(t, logFile)
	if len(backups) > cfg.MaxBackups {
		t.Errorf("after rotation %d backups remain, want <= %d: %v", len(backups), cfg.MaxBackups, backups)
	}
}
