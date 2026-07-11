package replication

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteStateFileAtomicFaultInjection(t *testing.T) {
	originalCreate := replicationCreateTemp
	originalChmod := replicationFileChmod
	originalSync := replicationFileSync
	originalClose := replicationFileClose
	originalRename := replicationRename
	originalRemove := replicationRemove
	originalOpenDir := replicationOpenDir
	originalMkdirAll := replicationMkdirAll
	originalStat := replicationStat
	originalLstat := replicationLstat
	originalFileStat := replicationFileStat
	originalOpenFile := replicationOpenFile
	defer func() {
		replicationCreateTemp = originalCreate
		replicationFileChmod = originalChmod
		replicationFileSync = originalSync
		replicationFileClose = originalClose
		replicationRename = originalRename
		replicationRemove = originalRemove
		replicationOpenDir = originalOpenDir
		replicationMkdirAll = originalMkdirAll
		replicationStat = originalStat
		replicationLstat = originalLstat
		replicationFileStat = originalFileStat
		replicationOpenFile = originalOpenFile
	}()

	dir := t.TempDir()
	stateFile := filepath.Join(dir, "state.json")

	// CreateTemp failure
	replicationCreateTemp = func(_, _ string) (*os.File, error) {
		return nil, fmt.Errorf("create temp failed")
	}
	if err := writeReplicationStateFileAtomic(stateFile, []byte("x")); err == nil {
		t.Fatal("expected create temp error")
	}
	replicationCreateTemp = originalCreate

	// Chmod failure
	replicationFileChmod = func(f *os.File, mode os.FileMode) error {
		return fmt.Errorf("chmod failed")
	}
	if err := writeReplicationStateFileAtomic(stateFile, []byte("x")); err == nil {
		t.Fatal("expected chmod error")
	}
	replicationFileChmod = originalChmod

	// Sync failure
	replicationFileSync = func(f *os.File) error {
		return fmt.Errorf("sync failed")
	}
	if err := writeReplicationStateFileAtomic(stateFile, []byte("x")); err == nil {
		t.Fatal("expected sync error")
	}
	replicationFileSync = originalSync

	// Close failure
	replicationFileClose = func(f *os.File) error {
		return fmt.Errorf("close failed")
	}
	if err := writeReplicationStateFileAtomic(stateFile, []byte("x")); err == nil {
		t.Fatal("expected close error")
	}
	replicationFileClose = originalClose

	// Write full data to disk first
	_ = writeReplicationStateFileAtomic(stateFile, []byte("old"))

	// Rename failure (cleanup should remove temp but keep old state)
	replicationRename = func(_, _ string) error {
		return fmt.Errorf("rename failed")
	}
	if err := writeReplicationStateFileAtomic(stateFile, []byte("new")); err == nil {
		t.Fatal("expected rename error")
	}
	data, _ := os.ReadFile(stateFile)
	if string(data) != "old" {
		t.Fatalf("state should be unchanged after rename failure, got %q", string(data))
	}
	replicationRename = originalRename

	// SyncReplicationStateDir: open dir failure
	replicationOpenDir = func(name string) (*os.File, error) {
		return nil, fmt.Errorf("open dir failed")
	}
	if err := syncReplicationStateDir(stateFile); err == nil {
		t.Fatal("expected open dir error")
	}
	replicationOpenDir = func(name string) (*os.File, error) { return os.Open(name) }

	// PrepareReplicationStateDir: MkdirAll failure
	replicationMkdirAll = func(path string, perm os.FileMode) error {
		return fmt.Errorf("mkdirall failed")
	}
	badPath := filepath.Join(dir, "newdir", "state.json")
	if err := prepareReplicationStateDir(badPath); err == nil {
		t.Fatal("expected mkdirall error")
	}
	replicationMkdirAll = originalMkdirAll

	// prepareReplicationStateDir: pre-existing non-directory parent
	if err := prepareReplicationStateDir("/dev/null/extra"); err == nil {
		t.Fatal("expected non-dir parent error")
	}
}

func TestOpenReplicationStateFileFaultInjection(t *testing.T) {
	originalLstat := replicationLstat
	originalOpenFile := replicationOpenFile
	originalFileStat := replicationFileStat
	originalFileChmod := replicationFileChmod
	originalFileClose := replicationFileClose
	defer func() {
		replicationLstat = originalLstat
		replicationOpenFile = originalOpenFile
		replicationFileStat = originalFileStat
		replicationFileChmod = originalFileChmod
		replicationFileClose = originalFileClose
	}()

	dir := t.TempDir()
	stateFile := filepath.Join(dir, "state.json")
	if err := os.WriteFile(stateFile, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}

	// Lstat failure
	replicationLstat = func(name string) (os.FileInfo, error) {
		return nil, fmt.Errorf("lstat failed")
	}
	if _, err := openReplicationStateFile(stateFile); err == nil {
		t.Fatal("expected lstat error")
	}
	replicationLstat = originalLstat

	// File open failure
	replicationOpenFile = func(name string) (*os.File, error) {
		return nil, fmt.Errorf("open failed")
	}
	if _, err := openReplicationStateFile(stateFile); err == nil {
		t.Fatal("expected open error")
	}
	replicationOpenFile = originalOpenFile

	// Stat failure after open
	badStatFile := filepath.Join(dir, "badstat.json")
	if err := os.WriteFile(badStatFile, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	replicationFileStat = func(f *os.File) (os.FileInfo, error) {
		return nil, fmt.Errorf("stat failed")
	}
	if _, err := openReplicationStateFile(badStatFile); err == nil {
		t.Fatal("expected stat error")
	}
	replicationFileStat = originalFileStat

	// Chmod failure after open
	replicationFileChmod = func(f *os.File, mode os.FileMode) error {
		return fmt.Errorf("chmod failed")
	}
	if _, err := openReplicationStateFile(stateFile); err == nil {
		t.Fatal("expected chmod error")
	}
	replicationFileChmod = originalFileChmod
}

func TestReplicationStateFileWindowFaults(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	_ = mgr.loadReplicationState()

	dir := t.TempDir()
	stateFile := filepath.Join(dir, "replication-state.json")
	mgr = NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	atomic.StoreUint64(&mgr.lastApplied, 42)
	if err := mgr.saveReplicationState(); err != nil {
		t.Fatalf("save: %v", err)
	}
	mgr2 := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	if err := mgr2.loadReplicationState(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if mgr2.lastApplied != 42 {
		t.Fatalf("expected 42, got %d", mgr2.lastApplied)
	}
}

func TestReplicationTokenAndShortWrite(t *testing.T) {
	if !replicationAuthTokenEqual("secret", "secret") {
		t.Fatal("expected valid auth")
	}
	if replicationAuthTokenEqual("secret", "wrong") {
		t.Fatal("expected invalid auth")
	}
	if len(replicationAuthTokenDigest("")) == 0 {
		t.Fatal("expected digest")
	}

	var short bytesSink
	short.limit = 3
	if _, err := writeReplicationFull(&short, []byte("hello")); err == nil {
		t.Fatal("expected short write error")
	}
}

type bytesSink struct {
	buf   []byte
	limit int
}

func (s *bytesSink) Write(p []byte) (int, error) {
	if s.limit > 0 && len(p) > s.limit {
		return s.limit, nil
	}
	s.buf = append(s.buf, p...)
	return len(p), nil
}

func TestDropConnectionsCloseError(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster})
	mgr.slaves["s1"] = &SlaveConnection{ID: "s1", Conn: &closeErrConn{err: fmt.Errorf("close error")}}
	mgr.DropConnections()
}

func TestListenAddrNoListener(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster})
	if addr := mgr.ListenAddr(); addr != "" {
		t.Fatalf("expected empty addr, got %q", addr)
	}
}

func TestJitteredDelay(t *testing.T) {
	d := jitteredDelay(100 * time.Millisecond)
	if d < 0 || d > 2*100*time.Millisecond {
		t.Fatalf("jittered delay out of range: %v", d)
	}
	d = jitteredDelay(0)
	if d != 0 {
		t.Fatalf("expected zero delay for zero input, got %v", d)
	}
}

func TestReplicateWALEntryFenced(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	atomic.StoreUint64(&mgr.fencedEpoch, 1)
	if err := mgr.ReplicateWALEntry([]byte("x")); err == nil {
		t.Fatal("expected fenced error")
	}
}
