package metrics

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestCoverageSlowQueryEnsureFileOpenFailingLstat(t *testing.T) {
	old := slowQueryLstat
	defer func() { slowQueryLstat = old }()
	slowQueryLstat = func(name string) (os.FileInfo, error) {
		return nil, errors.New("lstat failed")
	}
	s := NewSlowQueryLog(true, time.Second, 10, "/nonexistent/dir/slow.log")
	if err := s.ensureFileOpenLocked(); err == nil {
		t.Fatal("expected lstat error")
	}
}

func TestCoverageSlowQueryFileOpenFailure(t *testing.T) {
	dir := t.TempDir()
	old := slowQueryOpenFile
	defer func() { slowQueryOpenFile = old }()
	slowQueryOpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errors.New("open failed")
	}
	_, _, err := openSlowQueryLogFile(dir + "/open-fail.log")
	if err == nil {
		t.Fatal("expected open error")
	}
}

func TestCoverageSlowQueryCloseHandlesFileErrors(t *testing.T) {
	s := NewSlowQueryLog(true, time.Millisecond, 100, "/tmp/test-slow-close.log")
	if err := s.Close(); err != nil {
		t.Fatalf("close on nil file: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
}

func TestCoverageSlowQueryLogDisabled(t *testing.T) {
	s := NewSlowQueryLog(false, time.Millisecond, 100, "")
	s.Log("SELECT 1", time.Second, 0, 0)
	if len(s.GetEntries(10)) != 0 {
		t.Fatal("disabled log should not record entries")
	}
}

func TestCoverageSlowQueryGetStats(t *testing.T) {
	s := NewSlowQueryLog(true, time.Millisecond, 100, "")
	_, count := s.GetStats()
	if count != 0 {
		t.Fatalf("expected 0 entries, got %d", count)
	}
}
