package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestRotationRetentionKeepsNamedChainPredecessor pins that backup retention
// never prunes the segment a live chain-continuation record names.
//
// pruneBackupsLocked used to order deletion candidates by the timestamp
// PARSED FROM THE NAME. When prune freed a plain <second> stamp and a later
// same-second rotation adopted it (the rename collision check only tests
// current existence), that recycled name parsed as <second>.000000000 — the
// parse-OLDEST — so retention deleted the NEWEST-created segment while older
// ones survived. That violated prune's own "newest kept" contract and left
// the current segment's continuation record naming a file that no longer
// existed with a previous_last_hash matching no surviving segment — breaking
// VerifyLogFile's documented cross-segment contract ("verify the previous
// segment and check its LastHash equals PrevSegmentHash").
//
// The fix orders candidates by file ModTime (rotations serialize under al.mu,
// so last-write order is creation order) with the parsed stamp and path as
// deterministic tiebreaks, and never selects the backup renamed by the
// current rotateLocked call.
func TestRotationRetentionKeepsNamedChainPredecessor(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	run := func(t *testing.T, name string, maxFileSize int64, events, maxBackups int) *VerificationResult {
		t.Helper()
		logPath := filepath.Join(t.TempDir(), "audit.log")
		cfg := DefaultConfig()
		cfg.Enabled = true
		cfg.LogFile = logPath
		cfg.RotationEnabled = true
		cfg.MaxFileSize = maxFileSize
		cfg.MaxBackups = maxBackups
		cfg.EncryptionKey = key
		al, err := New(cfg, nil)
		if err != nil {
			t.Fatalf("%s: new: %v", name, err)
		}
		for i := 0; i < events; i++ {
			al.Log(EventQuery, "alice", "SELECT", WithQuery(fmt.Sprintf("SELECT * FROM t%d WHERE id = %d", i%5, i)))
		}
		if err := al.Close(); err != nil {
			t.Fatalf("%s: close: %v", name, err)
		}
		cur, err := VerifyLogFile(logPath, key)
		if err != nil {
			t.Fatalf("%s: verify current: %v", name, err)
		}
		if cur.PrevSegmentFile == "" {
			t.Fatalf("%s: current segment has no continuation record (no rotation happened?)", name)
		}
		return cur
	}

	// Discriminator: enough same-second auto-rotations to exceed MaxBackups —
	// the named predecessor must survive and its LastHash must match.
	cur := run(t, "rotations", 2048, 40, 10)
	named := filepath.Base(cur.PrevSegmentFile)
	namedPath := filepath.Join(filepath.Dir(cur.PrevSegmentFile), named)
	res, err := VerifyLogFile(namedPath, key)
	if err != nil {
		t.Fatalf("FAIL: named previous segment %q was pruned or fails verification: %v", named, err)
	}
	if res.LastHash != cur.PrevSegmentHash {
		t.Fatalf("FAIL: PrevSegmentHash=%s != named.LastHash=%s — cross-segment chain is broken", cur.PrevSegmentHash, res.LastHash)
	}

	// Control: a single auto-rotation with retention slack — linkage intact.
	curCtl := run(t, "control", 8192, 20, 10)
	namedCtl := filepath.Base(curCtl.PrevSegmentFile)
	namedCtlPath := filepath.Join(filepath.Dir(curCtl.PrevSegmentFile), namedCtl)
	resCtl, err := VerifyLogFile(namedCtlPath, key)
	if err != nil {
		t.Fatalf("control: named previous segment fails verification: %v", err)
	}
	if resCtl.LastHash != curCtl.PrevSegmentHash {
		t.Fatalf("control: hash mismatch %s != %s", resCtl.LastHash, curCtl.PrevSegmentHash)
	}
}

func TestRetentionKeepsCurrentPredecessorWithEqualModTimes(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "audit.log")
	al := &Logger{config: &Config{LogFile: logFile, MaxBackups: 1}}
	stamp := time.Now().Truncate(time.Second)
	newest := logFile + "." + stamp.Format("20060102_150405")
	older := newest + ".999999999"
	for _, path := range []string{newest, older} {
		if err := os.WriteFile(path, []byte("backup\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	al.pruneBackupsLocked(newest)
	if _, err := os.Stat(newest); err != nil {
		t.Fatalf("current chain predecessor was pruned: %v", err)
	}
	if _, err := os.Stat(older); !os.IsNotExist(err) {
		t.Fatalf("older backup was not pruned: %v", err)
	}
}
