//go:build unix

package audit_test

import (
	"bufio"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/audit"
)

// These tests cover the failure-accounting contract through the PUBLIC surface
// only (external audit_test package: it cannot reach al.file, flushBatch, or
// any other internal).
//
// Why an external package: pkg/audit has no injectable seam —
// openAuditLogForAppend calls os.OpenFile directly and Logger.file is a
// concrete *os.File, not an interface. The existing
// TestFailedWriteCountStartsZeroAndCountsFailures therefore injects failure by
// closing al.file and calling flushBatch directly, which only works from
// inside the package and so never exercises the path a real caller takes.
//
// These tests inject a REAL kernel-enforced write failure instead: a soft
// RLIMIT_FSIZE below the log's eventual size makes write(2) fail with EFBIG
// for this process. SIGXFSZ is ignored so the test binary survives and can
// report what the logger recorded.
//
// The contract: audit events that could NOT be durably written must be
// counted by FailedWriteCount() and surfaced by LastWriteError(), so operators
// alerting on those signals detect dropped records.

// fsizeLimitBytes is small enough that a handful of JSON audit entries exceed
// it, so only a few events persist and the rest fail at the file seam.
const fsizeLimitBytes = 512

// setFsizeLimit installs a soft RLIMIT_FSIZE and returns a restore func.
// Ignoring SIGXFSZ is required: exceeding RLIMIT_FSIZE otherwise delivers
// SIGXFSZ, whose default disposition would terminate the test binary.
func setFsizeLimit(t *testing.T, limit uint64) func() {
	t.Helper()
	var saved syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &saved); err != nil {
		t.Skipf("cannot read RLIMIT_FSIZE on this platform: %v", err)
	}
	next := saved
	next.Cur = limit
	if next.Max != 0 && next.Cur > next.Max {
		next.Cur = next.Max
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &next); err != nil {
		t.Skipf("cannot lower RLIMIT_FSIZE on this platform: %v", err)
	}
	ignored := signal.Ignored(syscall.SIGXFSZ)
	signal.Ignore(syscall.SIGXFSZ)
	var once sync.Once
	restore := func() {
		once.Do(func() {
			if !ignored {
				signal.Reset(syscall.SIGXFSZ)
			}
			if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &saved); err != nil {
				t.Fatalf("restore RLIMIT_FSIZE: %v", err)
			}
		})
	}
	t.Cleanup(restore)
	return restore
}

func auditTestConfig(logFile string) *audit.Config {
	cfg := audit.DefaultConfig()
	cfg.Enabled = true
	cfg.LogFile = logFile
	cfg.LogFormat = "json"
	cfg.RotationEnabled = false
	return cfg
}

// persistedLines counts the audit entries actually on disk.
func persistedLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open audit log %s: %v", path, err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) > 0 {
			n++
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan audit log %s: %v", path, err)
	}
	return n
}

// A healthy logger must report no failures. This is the control that proves the
// failure assertions below are not trivially satisfied by a counter that is
// simply always non-zero.
func TestFailedWriteAccounting_HealthyLoggerReportsNoFailure(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "audit.log")

	al, err := audit.New(auditTestConfig(logFile), nil)
	if err != nil {
		t.Fatalf("audit.New: %v", err)
	}

	const events = 20
	for i := 0; i < events; i++ {
		al.Log(audit.EventQuery, "alice", "EXECUTE")
	}
	// Close drains the async writer, making the flush deterministic.
	if err := al.Close(); err != nil {
		t.Fatalf("Close on healthy logger: %v", err)
	}

	if got := al.FailedWriteCount(); got != 0 {
		t.Errorf("FailedWriteCount() = %d, want 0 on a healthy logger", got)
	}
	if err := al.LastWriteError(); err != nil {
		t.Errorf("LastWriteError() = %v, want nil on a healthy logger", err)
	}
	if got := persistedLines(t, logFile); got != events {
		t.Errorf("persisted entries = %d, want %d", got, events)
	}
}

// A write failure at the file seam — one that never reaches disk — must be
// counted by FailedWriteCount() and surfaced by LastWriteError().
func TestFailedWriteAccounting_WriteFailureAtFileSeamIsCountedAndSurfaced(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "audit.log")

	// Open the logger BEFORE lowering the limit so file creation succeeds.
	al, err := audit.New(auditTestConfig(logFile), nil)
	if err != nil {
		t.Fatalf("audit.New: %v", err)
	}

	restore := setFsizeLimit(t, fsizeLimitBytes)
	submitted := 0
	for i := 0; i < 200; i++ {
		al.Log(audit.EventQuery, "alice", "EXECUTE")
		submitted++
	}
	// Close drains the writer while the limit is still active, so the failing
	// writes are observed deterministically.
	_ = al.Close()
	restore() // lift the limit before inspecting the log

	persisted := persistedLines(t, logFile)
	failed := al.FailedWriteCount()
	lastErr := al.LastWriteError()
	t.Logf("submitted=%d persisted=%d failedWrites=%d", submitted, persisted, failed)

	// The file seam genuinely failed: not every event made it to disk.
	if persisted >= submitted {
		t.Fatalf("test setup: expected the write limit to drop events, "+
			"but persisted=%d >= submitted=%d; the limit was not effective",
			persisted, submitted)
	}

	// The contract: every event that failed at the file seam is counted.
	if failed == 0 {
		t.Errorf("FailedWriteCount() = 0 after %d events were dropped at the file "+
			"seam (persisted=%d, submitted=%d): write failures are silently lost",
			submitted-persisted, persisted, submitted)
	}

	// And the failure must be surfaced, not just counted.
	if lastErr == nil {
		t.Errorf("LastWriteError() = nil despite %d failed write(s): "+
			"the failure is not surfaced to the caller", failed)
	} else if !strings.Contains(lastErr.Error(), "audit") {
		t.Errorf("LastWriteError() = %v, want an audit-log write failure", lastErr)
	}
}

// Close must also surface a write failure that happened during teardown, so a
// caller that only checks Close's error is not misled into thinking the audit
// log was written.
func TestFailedWriteAccounting_CloseSurfacesWriteFailure(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "audit.log")

	al, err := audit.New(auditTestConfig(logFile), nil)
	if err != nil {
		t.Fatalf("audit.New: %v", err)
	}

	restore := setFsizeLimit(t, fsizeLimitBytes)
	for i := 0; i < 200; i++ {
		al.Log(audit.EventQuery, "alice", "EXECUTE")
	}
	closeErr := al.Close()
	restore()

	if closeErr == nil {
		t.Fatalf("Close() = nil, but %d events could not be written at the file seam "+
			"(persisted=%d): teardown hides audit-log loss",
			al.FailedWriteCount(), persistedLines(t, logFile))
	}
	if al.FailedWriteCount() == 0 {
		t.Errorf("Close() reported %v but FailedWriteCount() = 0: the failure is uncounted", closeErr)
	}
}
