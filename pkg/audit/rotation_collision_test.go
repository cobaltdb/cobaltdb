package audit

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const rotationStampLayout = "20060102_150405"

// TestRotateSameSecondCollisionPreservesSegments pins the contract that two
// rotations within the same wall-clock second must both preserve their
// rotated segments and leave a verifiable cross-file hash chain.
//
// Regression: rotateLocked named backups with a second-resolution timestamp
// (log.<20060102_150405>) and os.Rename silently REPLACES an existing
// destination, so the second same-second rotation destroyed the first
// rotated audit segment and invalidated the chain boundary recorded in the
// segment's continuation record.
func TestRotateSameSecondCollisionPreservesSegments(t *testing.T) {
	for attempt := 0; attempt < 5; attempt++ {
		if runCollisionAttempt(t) {
			return
		}
	}
	t.Fatal("could not achieve a same-second double rotation in 5 attempts")
}

// runCollisionAttempt performs one full double-rotation sequence. It reports
// whether the same-second collision was exercised and verified; a false
// return means a second boundary was crossed mid-sequence and the attempt
// must be retried.
func runCollisionAttempt(t *testing.T) bool {
	t.Helper()
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.log")
	al, err := New(&Config{
		Enabled:         true,
		LogFile:         logFile,
		LogFormat:       "json",
		RotationEnabled: true,
		MaxFileSize:     100 << 20, // keep auto-rotation out of the picture
		MaxBackups:      10,
		MaxAge:          30,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	waitRotationSecondBoundary()

	// The async writer flushes its batch at exactly batch size 100, so a
	// 100-event segment flushes immediately and deterministically.
	writeMarkedSegment(t, al, logFile, "SEGMENT_ONE_MARKER")
	if err := al.Rotate(); err != nil {
		t.Fatalf("rotate #1: %v", err)
	}
	backups1 := rotatedBackups(logFile)
	if len(backups1) != 1 {
		t.Fatalf("expected 1 backup after first rotation, got %d", len(backups1))
	}
	s1 := strings.TrimPrefix(backups1[0], logFile+".")

	writeMarkedSegment(t, al, logFile, "SEGMENT_TWO_MARKER")

	// The sequence is only meaningful if rotate #2 lands in the same
	// wall-clock second as rotate #1.
	if time.Now().Format(rotationStampLayout) != s1 {
		al.Close()
		return false // second boundary crossed; retry
	}
	if err := al.Rotate(); err != nil {
		t.Fatalf("rotate #2: %v", err)
	}
	defer al.Close()

	backups := rotatedBackups(logFile)
	for _, b := range backups {
		if !strings.HasPrefix(b, logFile+"."+s1) {
			return false // boundary crossed mid-sequence; retry
		}
	}

	segmentOneFound := false
	for _, b := range backups {
		data, err := os.ReadFile(b)
		if err != nil {
			t.Fatalf("read backup %s: %v", b, err)
		}
		if bytes.Contains(data, []byte("SEGMENT_ONE_MARKER")) {
			segmentOneFound = true
		}
	}
	if !segmentOneFound {
		t.Fatalf("%d backup(s) after two same-second rotations and none contains the first segment's records", len(backups))
	}
	verifyCrossFileChain(t, logFile, backups, nil)
	return true
}

func waitRotationSecondBoundary() {
	cur := time.Now().Format(rotationStampLayout)
	for time.Now().Format(rotationStampLayout) == cur {
		runtime.Gosched()
	}
}

func writeMarkedSegment(t *testing.T, al *Logger, logFile, marker string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		user := "filler"
		if i == 99 {
			user = marker
		}
		al.Log(EventQuery, user, "action")
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(logFile); err == nil && bytes.Contains(data, []byte(marker)) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("marker %q never flushed to %s", marker, logFile)
}

func rotatedBackups(logFile string) []string {
	matches, err := filepath.Glob(logFile + ".*")
	if err != nil {
		return nil
	}
	return matches
}

func verifyCrossFileChain(t *testing.T, logFile string, backups []string, key []byte) {
	t.Helper()
	type entry struct {
		path string
		res  *VerificationResult
	}
	var all []entry
	for _, b := range backups {
		res, err := VerifyLogFile(b, key)
		if err != nil {
			t.Fatalf("verify %s: %v", b, err)
		}
		all = append(all, entry{b, res})
	}
	cur, err := VerifyLogFile(logFile, key)
	if err != nil {
		t.Fatalf("verify current log: %v", err)
	}
	all = append(all, entry{logFile, cur})
	for _, e := range all {
		if e.res.PrevSegmentFile == "" {
			continue
		}
		linked := false
		for _, cand := range all {
			if cand.path == e.res.PrevSegmentFile && cand.res.LastHash == e.res.PrevSegmentHash {
				linked = true
				break
			}
		}
		if !linked {
			t.Fatalf("cross-file boundary from %s references %q/%q with no matching segment",
				e.path, e.res.PrevSegmentFile, e.res.PrevSegmentHash)
		}
	}
}

// TestPruneBackupsParsesDisambiguatedNames pins that pruneBackupsLocked
// recognizes collision-disambiguated backup names
// (log.<20060102_150405>.<nanoseconds>) alongside the plain-second names, so
// disambiguated segments are ordered and pruned instead of accumulating
// forever, while non-backup siblings are never touched.
func TestPruneBackupsParsesDisambiguatedNames(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.log")
	al, err := New(&Config{
		Enabled:    true,
		LogFile:    logFile,
		LogFormat:  "json",
		MaxBackups: 2,
		MaxAge:     7,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer al.Close()

	now := time.Now()
	makeFile := func(name string) {
		if err := os.WriteFile(name, []byte("backup\n"), 0600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	disambiguated := logFile + "." + now.Add(-30*time.Second).Format(rotationStampLayout) + ".123456789"
	newest := logFile + "." + now.Add(-1*time.Minute).Format(rotationStampLayout)
	middle := logFile + "." + now.Add(-2*time.Minute).Format(rotationStampLayout)
	unrelated := logFile + ".notatimestamp"
	makeFile(disambiguated)
	makeFile(newest)
	makeFile(middle)
	makeFile(unrelated)

	al.mu.Lock()
	al.pruneBackupsLocked()
	al.mu.Unlock()

	// MaxBackups=2 keeps the two newest segments (the disambiguated one at
	// -30s and the plain one at -1m); middle is pruned; the unrelated
	// sibling must never be touched.
	for _, keep := range []string{disambiguated, newest, unrelated} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatalf("expected %s to survive pruning: %v", keep, err)
		}
	}
	if _, err := os.Stat(middle); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be pruned, stat err: %v", middle, err)
	}
}
