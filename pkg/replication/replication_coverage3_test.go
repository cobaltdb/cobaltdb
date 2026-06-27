package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestApplyWALData tests applyWALData with valid entries
func TestApplyWALData(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	// Create WAL entries
	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
		{LSN: 2, Timestamp: time.Now(), Data: []byte("test2"), Checksum: calculateCRC32([]byte("test2"))},
	}

	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("Failed to encode entries: %v", err)
	}

	// Track applied entries
	var appliedCount int
	mgr.OnApply = func(entry *WALEntry) error {
		appliedCount++
		return nil
	}

	// Apply WAL data
	err = mgr.applyWALData(string(data))
	if err != nil {
		t.Errorf("applyWALData failed: %v", err)
	}

	if appliedCount != 2 {
		t.Errorf("Expected 2 applied entries, got %d", appliedCount)
	}

	// Verify metrics
	metrics := mgr.GetMetrics()
	if metrics.AppliedEntries != 2 {
		t.Errorf("Expected 2 applied entries in metrics, got %d", metrics.AppliedEntries)
	}
}

// TestApplyWALDataWithError tests applyWALData when OnApply returns error
func TestApplyWALDataWithError(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
	}

	data, _ := encodeWALEntries(entries)

	mgr.OnApply = func(entry *WALEntry) error {
		return errors.New("apply error")
	}

	err := mgr.applyWALData(string(data))
	if err == nil {
		t.Error("Expected error from applyWALData when OnApply fails")
	}
}

func TestApplyWALDataCallbackPanicReturnsError(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
	}
	data, _ := encodeWALEntries(entries)

	mgr.OnApply = func(entry *WALEntry) error {
		panic("apply panic")
	}

	err := mgr.applyWALDataBytes(data)
	if err == nil || !strings.Contains(err.Error(), "apply callback panic") {
		t.Fatalf("expected callback panic error, got %v", err)
	}
	if mgr.lastApplied != 0 {
		t.Fatalf("lastApplied should not advance after callback panic, got %d", mgr.lastApplied)
	}
}

// TestApplyWALDataInvalidData tests applyWALData with invalid data
func TestApplyWALDataInvalidData(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	err := mgr.applyWALData("invalid data")
	if err == nil {
		t.Error("Expected error from applyWALData with invalid data")
	}
}

// TestApplyWALDataNoCallback tests applyWALData without OnApply callback
func TestApplyWALDataNoCallback(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
	}

	data, _ := encodeWALEntries(entries)

	// No OnApply callback set
	err := mgr.applyWALData(string(data))
	if err != nil {
		t.Errorf("applyWALData failed without callback: %v", err)
	}

	// Verify lastApplied was updated
	if mgr.lastApplied != 1 {
		t.Errorf("Expected lastApplied=1, got %d", mgr.lastApplied)
	}
}

func TestReadMasterFrameBinaryWALAndAck(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	conn := &mockConn{}
	mgr.masterConn = conn

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
		{LSN: 2, Timestamp: time.Now(), Data: []byte("test2"), Checksum: calculateCRC32([]byte("test2"))},
	}

	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("Failed to encode entries: %v", err)
	}

	var frame bytes.Buffer
	if err := binary.Write(&frame, binary.BigEndian, uint32(len(data))); err != nil {
		t.Fatalf("Failed to write frame length: %v", err)
	}
	frame.Write(data)

	appliedCount := 0
	mgr.OnApply = func(entry *WALEntry) error {
		appliedCount++
		return nil
	}

	if err := mgr.readMasterFrame(bufio.NewReader(&frame)); err != nil {
		t.Fatalf("readMasterFrame failed: %v", err)
	}

	if appliedCount != 2 {
		t.Fatalf("Expected 2 applied entries, got %d", appliedCount)
	}
	if mgr.lastApplied != 2 {
		t.Fatalf("Expected lastApplied=2, got %d", mgr.lastApplied)
	}
	if !strings.Contains(string(conn.writeData), "ACK 2\n") {
		t.Fatalf("Expected ACK 2, got %q", string(conn.writeData))
	}
}

func TestApplyWALDataSkipsAlreadyAppliedEntries(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	mgr.lastApplied = 2

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("old1"), Checksum: calculateCRC32([]byte("old1"))},
		{LSN: 2, Timestamp: time.Now(), Data: []byte("old2"), Checksum: calculateCRC32([]byte("old2"))},
		{LSN: 3, Timestamp: time.Now(), Data: []byte("new"), Checksum: calculateCRC32([]byte("new"))},
	}

	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("Failed to encode entries: %v", err)
	}

	applied := 0
	mgr.OnApply = func(entry *WALEntry) error {
		applied++
		if entry.LSN != 3 {
			t.Fatalf("Expected only LSN 3 to be applied, got %d", entry.LSN)
		}
		return nil
	}

	if err := mgr.applyWALDataBytes(data); err != nil {
		t.Fatalf("applyWALDataBytes failed: %v", err)
	}
	if applied != 1 {
		t.Fatalf("Expected 1 applied entry, got %d", applied)
	}
}

func TestReadSlaveAcksUpdatesLastLSN(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster})
	slave := &SlaveConnection{
		ID:     "slave-1",
		Reader: bufio.NewReader(strings.NewReader("ACK 5\nPONG 4\nACK 9\nINVALID\n")),
	}

	mgr.readSlaveAcks(slave)

	if slave.LastLSN != 9 {
		t.Fatalf("Expected LastLSN=9, got %d", slave.LastLSN)
	}
}

func TestSendInitialSnapshotMarksSlaveCaughtUp(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster})
	var out bytes.Buffer
	slave := &SlaveConnection{
		ID:     "slave-1",
		Writer: bufio.NewWriter(&out),
	}

	if err := mgr.sendInitialSnapshot(slave, 42); err != nil {
		t.Fatalf("sendInitialSnapshot failed: %v", err)
	}

	if slave.LastLSN != 42 {
		t.Fatalf("Expected LastLSN=42, got %d", slave.LastLSN)
	}
	if got := out.String(); got != "START 42\n" {
		t.Fatalf("Expected START frame, got %q", got)
	}
}

func TestPrepareSlaveResumeUsesSnapshotForGap(t *testing.T) {
	mgr := NewManager(&Config{
		Role:                RoleMaster,
		Mode:                ModeAsync,
		MaxWALBufferEntries: 2,
	})
	mgr.OnSnapshot = func() ([]byte, uint64, error) {
		return []byte("snapshot"), 0, nil
	}
	for i := 0; i < 5; i++ {
		if err := mgr.ReplicateWALEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}

	var out bytes.Buffer
	slave := &SlaveConnection{
		ID:     "slave-1",
		Writer: bufio.NewWriter(&out),
	}

	if err := mgr.prepareSlaveResume(slave, 2); err != nil {
		t.Fatalf("prepareSlaveResume failed: %v", err)
	}
	if !slave.NeedsSnapshot {
		t.Fatal("Expected slave to be marked for snapshot")
	}
	if out.Len() != 0 {
		t.Fatalf("Expected no RESYNC response when snapshot is available, got %q", out.String())
	}
}

func TestSendInitialSnapshotSendsSnapshotFrame(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster})
	mgr.currentLSN = 7
	mgr.OnSnapshot = func() ([]byte, uint64, error) {
		return []byte("snap"), 7, nil
	}

	var out bytes.Buffer
	slave := &SlaveConnection{
		ID:            "slave-1",
		Writer:        bufio.NewWriter(&out),
		NeedsSnapshot: true,
	}

	if err := mgr.sendInitialSnapshot(slave, 2); err != nil {
		t.Fatalf("sendInitialSnapshot failed: %v", err)
	}
	if slave.LastLSN != 7 {
		t.Fatalf("Expected LastLSN=7, got %d", slave.LastLSN)
	}
	if slave.NeedsSnapshot {
		t.Fatal("Expected NeedsSnapshot to be cleared")
	}
	if got := out.String(); got != "SNAPSHOT 7 4\nsnap" {
		t.Fatalf("Expected snapshot frame, got %q", got)
	}
}

func TestReadMasterFrameAppliesSnapshot(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	conn := &mockConn{}
	mgr.masterConn = conn

	var applied []byte
	var appliedLSN uint64
	mgr.OnApplySnapshot = func(data []byte, lsn uint64) error {
		applied = append([]byte(nil), data...)
		appliedLSN = lsn
		return nil
	}

	reader := bufio.NewReader(strings.NewReader("SNAPSHOT 13 7\npayload"))
	if err := mgr.readMasterFrame(reader); err != nil {
		t.Fatalf("readMasterFrame failed: %v", err)
	}

	if string(applied) != "payload" {
		t.Fatalf("Expected snapshot payload, got %q", string(applied))
	}
	if appliedLSN != 13 {
		t.Fatalf("Expected applied LSN 13, got %d", appliedLSN)
	}
	if mgr.lastApplied != 13 {
		t.Fatalf("Expected lastApplied=13, got %d", mgr.lastApplied)
	}
	if got := string(conn.writeData); got != "ACK 13\n" {
		t.Fatalf("Expected ACK 13, got %q", got)
	}
}

func TestReadMasterFrameRejectsOversizedControlLine(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	line := "PING " + strings.Repeat("1", maxReplicationControlLineBytes) + "\n"

	err := mgr.readMasterFrame(bufio.NewReader(strings.NewReader(line)))
	if err == nil || !strings.Contains(err.Error(), "control line too large") {
		t.Fatalf("expected oversized control line error, got %v", err)
	}
}

func TestReadMasterFrameApplySnapshotPanicReturnsError(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	mgr.masterConn = &mockConn{}
	mgr.OnApplySnapshot = func(data []byte, lsn uint64) error {
		panic("snapshot panic")
	}

	reader := bufio.NewReader(strings.NewReader("SNAPSHOT 13 7\npayload"))
	err := mgr.readMasterFrame(reader)
	if err == nil || !strings.Contains(err.Error(), "apply snapshot callback panic") {
		t.Fatalf("expected apply snapshot panic error, got %v", err)
	}
	if mgr.lastApplied != 0 {
		t.Fatalf("lastApplied should not advance after snapshot panic, got %d", mgr.lastApplied)
	}
}

func TestReadMasterFrameRequiresSnapshotApplier(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	conn := &mockConn{}
	mgr.masterConn = conn

	reader := bufio.NewReader(strings.NewReader("SNAPSHOT 13 7\npayload"))
	err := mgr.readMasterFrame(reader)
	if err == nil || !strings.Contains(err.Error(), "snapshot applier not configured") {
		t.Fatalf("expected missing snapshot applier error, got %v", err)
	}
	if mgr.lastApplied != 0 {
		t.Fatalf("lastApplied should not advance without snapshot applier, got %d", mgr.lastApplied)
	}
	if got := string(conn.writeData); got != "" {
		t.Fatalf("snapshot without applier should not be acknowledged, got %q", got)
	}
}

func TestReplicateWALOnLagPanicRecovered(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	mgr.ReplicateWALEntry([]byte("test"))

	mw := &mockWriter{failAfter: 0}
	slave := &SlaveConnection{
		ID:       "lagging-slave",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  0,
		LastPing: time.Now().Add(-time.Hour),
	}
	mgr.slaves["lagging-slave"] = slave
	mgr.OnLag = func(slave string, lag time.Duration) {
		panic("lag panic")
	}

	mgr.replicateWAL()
}

func TestReceiveResumeRequest(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster})
	slave := &SlaveConnection{
		ID:     "slave-1",
		Reader: bufio.NewReader(strings.NewReader("RESUME 17\n")),
	}

	req, err := mgr.receiveResumeRequest(slave)
	if err != nil {
		t.Fatalf("receiveResumeRequest failed: %v", err)
	}
	if req.LSN != 17 {
		t.Fatalf("Expected LSN 17, got %d", req.LSN)
	}
	if req.RequireSnapshot {
		t.Fatal("Expected normal resume request")
	}
}

func TestReceiveResumeRequestRequiresSnapshot(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster})
	slave := &SlaveConnection{
		ID:     "slave-1",
		Reader: bufio.NewReader(strings.NewReader("RESUME_SNAPSHOT 0\n")),
	}

	req, err := mgr.receiveResumeRequest(slave)
	if err != nil {
		t.Fatalf("receiveResumeRequest failed: %v", err)
	}
	if req.LSN != 0 {
		t.Fatalf("Expected LSN 0, got %d", req.LSN)
	}
	if !req.RequireSnapshot {
		t.Fatal("Expected snapshot resume request")
	}
}

func TestReceiveResumeRequestRejectsOversizedControlLine(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster})
	line := "RESUME " + strings.Repeat("1", maxReplicationControlLineBytes) + "\n"
	slave := &SlaveConnection{Reader: bufio.NewReader(strings.NewReader(line))}

	_, err := mgr.receiveResumeRequest(slave)
	if err == nil || !strings.Contains(err.Error(), "control line too large") {
		t.Fatalf("expected oversized control line error, got %v", err)
	}
}

func TestPrepareSlaveResumeAllowsRetainedWindow(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	for i := 0; i < 3; i++ {
		if err := mgr.ReplicateWALEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}

	var out bytes.Buffer
	slave := &SlaveConnection{
		ID:     "slave-1",
		Writer: bufio.NewWriter(&out),
	}

	if err := mgr.prepareSlaveResume(slave, 1); err != nil {
		t.Fatalf("prepareSlaveResume failed: %v", err)
	}
	if slave.LastLSN != 1 {
		t.Fatalf("Expected LastLSN=1, got %d", slave.LastLSN)
	}
	if out.Len() != 0 {
		t.Fatalf("Expected no RESYNC response, got %q", out.String())
	}
}

func TestPrepareSlaveResumeRejectsGap(t *testing.T) {
	mgr := NewManager(&Config{
		Role:                RoleMaster,
		Mode:                ModeAsync,
		MaxWALBufferEntries: 2,
	})
	for i := 0; i < 5; i++ {
		if err := mgr.ReplicateWALEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}

	var out bytes.Buffer
	slave := &SlaveConnection{
		ID:     "slave-1",
		Writer: bufio.NewWriter(&out),
	}

	if err := mgr.prepareSlaveResume(slave, 2); err == nil {
		t.Fatal("Expected gap error")
	}
	if got := out.String(); got != "RESYNC 5\n" {
		t.Fatalf("Expected RESYNC 5, got %q", got)
	}
}

func TestHandleMasterMessageRESYNC(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})

	err := mgr.handleMasterMessage("RESYNC 9\n")
	if err == nil {
		t.Fatal("Expected RESYNC error")
	}
	if !strings.Contains(err.Error(), "resync required") {
		t.Fatalf("Unexpected RESYNC error: %v", err)
	}
}

func TestReplicationStateSaveLoad(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "replication-state.json")

	mgr := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	mgr.lastApplied = 42
	if err := mgr.saveReplicationState(); err != nil {
		t.Fatalf("saveReplicationState failed: %v", err)
	}
	info, err := os.Stat(stateFile)
	if err != nil {
		t.Fatalf("stat replication state failed: %v", err)
	}
	if info.Mode().Perm() != replicationStateFilePerm {
		t.Fatalf("Expected replication state permissions %o, got %o", replicationStateFilePerm, info.Mode().Perm())
	}

	reloaded := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	if err := reloaded.loadReplicationState(); err != nil {
		t.Fatalf("loadReplicationState failed: %v", err)
	}
	if reloaded.lastApplied != 42 {
		t.Fatalf("Expected lastApplied=42, got %d", reloaded.lastApplied)
	}
}

func TestSaveReplicationStateIgnoresLegacyTempSymlink(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "replication-state.json")
	legacyTempPath := stateFile + ".tmp"
	victimPath := filepath.Join(tempDir, "victim")
	original := []byte("do not overwrite")

	if err := os.WriteFile(victimPath, original, replicationStateFilePerm); err != nil {
		t.Fatalf("WriteFile victim failed: %v", err)
	}
	if err := os.Symlink(victimPath, legacyTempPath); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	mgr := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	mgr.lastApplied = 77
	if err := mgr.saveReplicationState(); err != nil {
		t.Fatalf("saveReplicationState failed: %v", err)
	}

	victimContent, err := os.ReadFile(victimPath)
	if err != nil {
		t.Fatalf("ReadFile victim failed: %v", err)
	}
	if !bytes.Equal(victimContent, original) {
		t.Fatalf("legacy temp symlink target was overwritten: got %q", victimContent)
	}

	reloaded := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	if err := reloaded.loadReplicationState(); err != nil {
		t.Fatalf("loadReplicationState failed: %v", err)
	}
	if reloaded.lastApplied != 77 {
		t.Fatalf("Expected lastApplied=77, got %d", reloaded.lastApplied)
	}
}

func TestSaveReplicationStateRejectsSymlinkDirectory(t *testing.T) {
	tempDir := t.TempDir()
	targetDir := filepath.Join(tempDir, "target")
	linkDir := filepath.Join(tempDir, "state")
	if err := os.Mkdir(targetDir, replicationStateDirPerm); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	stateFile := filepath.Join(linkDir, "replication-state.json")
	mgr := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	mgr.lastApplied = 42
	err := mgr.saveReplicationState()
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("saveReplicationState symlink dir error = %v, want symlink rejection", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "replication-state.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state was written through symlink, stat err=%v", err)
	}
}

func TestPrepareReplicationStateDirCreatesRestrictiveDirectory(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "nested", "state", "replication-state.json")
	if err := prepareReplicationStateDir(stateFile); err != nil {
		t.Fatalf("prepareReplicationStateDir: %v", err)
	}

	info, err := os.Stat(filepath.Dir(stateFile))
	if err != nil {
		t.Fatalf("stat state dir: %v", err)
	}
	if got := info.Mode().Perm(); got != replicationStateDirPerm {
		t.Fatalf("replication state dir mode = %o, want %o", got, replicationStateDirPerm)
	}
}

func TestWriteReplicationStateFullRejectsShortWrite(t *testing.T) {
	writer := &shortReplicationStateWriter{limit: 6}

	n, err := writeReplicationStateFull(writer, []byte("abcdefghi"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeReplicationStateFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 6 {
		t.Fatalf("writeReplicationStateFull wrote %d bytes, want 6", n)
	}
}

func TestLoadReplicationStateRejectsUnsafeFile(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "replication-state.json")
	linkFile := filepath.Join(tempDir, "replication-state-link.json")
	if err := os.WriteFile(stateFile, []byte(`{"last_applied":42}`), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := os.Symlink(stateFile, linkFile); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	mgr := NewManager(&Config{Role: RoleSlave, StateFile: linkFile})
	err := mgr.loadReplicationState()
	if err == nil {
		t.Fatal("Expected symlink state file to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("Expected symlink rejection, got %v", err)
	}

	mgr = NewManager(&Config{Role: RoleSlave, StateFile: tempDir})
	err = mgr.loadReplicationState()
	if err == nil {
		t.Fatal("Expected directory state file to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("Expected regular file rejection, got %v", err)
	}
}

func TestLoadReplicationStateRestrictsFilePermissions(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "replication-state.json")
	if err := os.WriteFile(stateFile, []byte(`{"last_applied":42}`), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	mgr := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	if err := mgr.loadReplicationState(); err != nil {
		t.Fatalf("loadReplicationState failed: %v", err)
	}
	info, err := os.Stat(stateFile)
	if err != nil {
		t.Fatalf("stat replication state failed: %v", err)
	}
	if info.Mode().Perm() != replicationStateFilePerm {
		t.Fatalf("Expected replication state permissions %o, got %o", replicationStateFilePerm, info.Mode().Perm())
	}
}

func TestLoadReplicationStateRejectsOversizedFile(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "replication-state.json")
	if err := os.WriteFile(stateFile, []byte(strings.Repeat("x", maxReplicationStateFileBytes+1)), replicationStateFilePerm); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	mgr := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	err := mgr.loadReplicationState()
	if err == nil {
		t.Fatal("Expected oversized replication state file to be rejected")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("Expected too large error, got %v", err)
	}
}

func TestLoadReplicationStateRejectsTrailingData(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "replication-state.json")
	if err := os.WriteFile(stateFile, []byte(`{"last_applied":42} {"last_applied":43}`), replicationStateFilePerm); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	mgr := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	err := mgr.loadReplicationState()
	if err == nil {
		t.Fatal("Expected trailing replication state JSON to be rejected")
	}
	if !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("Expected trailing data error, got %v", err)
	}
}

func TestSyncReplicationStateDir(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "replication-state.json")
	if err := os.WriteFile(stateFile, []byte("{}"), replicationStateFilePerm); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := syncReplicationStateDir(stateFile); err != nil {
		t.Fatalf("syncReplicationStateDir failed: %v", err)
	}
	if err := syncReplicationStateDir(filepath.Join(t.TempDir(), "missing", "replication-state.json")); err == nil {
		t.Fatal("syncReplicationStateDir should fail for missing parent directory")
	}
}

func TestApplyWALDataPersistsReplicationState(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "replication-state.json")
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync, StateFile: stateFile})

	entries := []*WALEntry{
		{LSN: 7, Timestamp: time.Now(), Data: []byte("test"), Checksum: calculateCRC32([]byte("test"))},
	}
	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("Failed to encode entries: %v", err)
	}

	if err := mgr.applyWALDataBytes(data); err != nil {
		t.Fatalf("applyWALDataBytes failed: %v", err)
	}

	reloaded := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})
	if err := reloaded.loadReplicationState(); err != nil {
		t.Fatalf("loadReplicationState failed: %v", err)
	}
	if reloaded.lastApplied != 7 {
		t.Fatalf("Expected persisted lastApplied=7, got %d", reloaded.lastApplied)
	}
}

func TestStartMessagePersistsReplicationState(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "replication-state.json")
	mgr := NewManager(&Config{Role: RoleSlave, StateFile: stateFile})

	if err := mgr.handleMasterMessage("START 11\n"); err != nil {
		t.Fatalf("handleMasterMessage START failed: %v", err)
	}

	content, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("Failed to read state file: %v", err)
	}
	if !strings.Contains(string(content), `"last_applied": 11`) {
		t.Fatalf("Expected persisted last_applied 11, got %s", string(content))
	}
}

type shortReplicationStateWriter struct {
	limit int
}

func (w *shortReplicationStateWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		return w.limit, nil
	}
	return len(p), nil
}

func TestWriteReplicationFullRejectsShortWrite(t *testing.T) {
	writer := &shortReplicationWriter{limit: 4}

	n, err := writeReplicationFull(writer, []byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeReplicationFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 4 {
		t.Fatalf("writeReplicationFull wrote %d bytes, want 4", n)
	}
}

func TestAuthenticateSlaveRejectsShortSuccessWrite(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, AuthToken: "secret"})
	conn := &shortReplicationConn{limit: 3}

	err := mgr.authenticateSlaveWithReader(bufio.NewReader(strings.NewReader("secret\n")), conn)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("authenticateSlaveWithReader short write error = %v, want %v", err, io.ErrShortWrite)
	}
}

type authDeadlineConn struct {
	mockConn
	deadlines      []time.Time
	writeDeadlines []time.Time
	readData       []byte
}

func (c *authDeadlineConn) Read(p []byte) (int, error) {
	if len(c.readData) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.readData)
	c.readData = c.readData[n:]
	return n, nil
}

func (c *authDeadlineConn) SetReadDeadline(t time.Time) error {
	c.deadlines = append(c.deadlines, t)
	return nil
}

func (c *authDeadlineConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadlines = append(c.writeDeadlines, t)
	return nil
}

func TestAuthenticateSlaveSetsReadDeadline(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, AuthToken: "secret"})
	conn := &authDeadlineConn{}

	if err := mgr.authenticateSlaveWithReader(bufio.NewReader(strings.NewReader("secret\n")), conn); err != nil {
		t.Fatalf("authenticateSlaveWithReader failed: %v", err)
	}
	if len(conn.deadlines) != 2 {
		t.Fatalf("expected auth deadline set and cleared, got %d calls", len(conn.deadlines))
	}
	if conn.deadlines[0].IsZero() {
		t.Fatal("expected non-zero auth read deadline")
	}
	if !conn.deadlines[1].IsZero() {
		t.Fatalf("expected auth read deadline to be cleared, got %v", conn.deadlines[1])
	}
	if got := string(conn.writeData); got != "AUTH_OK\n" {
		t.Fatalf("expected AUTH_OK response, got %q", got)
	}
}

func TestStartSlaveUsesDialTimeoutAndHandshakeDeadlines(t *testing.T) {
	originalDial := replicationDial
	defer func() { replicationDial = originalDial }()

	conn := &authDeadlineConn{readData: []byte("AUTH_OK\n")}
	var gotNetwork, gotAddress string
	var gotTimeout time.Duration
	replicationDial = func(network, address string, timeout time.Duration) (net.Conn, error) {
		gotNetwork = network
		gotAddress = address
		gotTimeout = timeout
		return conn, nil
	}

	mgr := NewManager(&Config{
		Role:       RoleSlave,
		MasterAddr: "127.0.0.1:9999",
		AuthToken:  "secret",
	})
	if err := mgr.startSlave(); err != nil {
		t.Fatalf("startSlave failed: %v", err)
	}
	if gotNetwork != "tcp" || gotAddress != "127.0.0.1:9999" {
		t.Fatalf("dial called with network=%q address=%q", gotNetwork, gotAddress)
	}
	if gotTimeout != replicationAuthTimeout {
		t.Fatalf("dial timeout = %v, want %v", gotTimeout, replicationAuthTimeout)
	}
	if len(conn.deadlines) != 2 {
		t.Fatalf("expected slave auth deadline set and cleared, got %d calls", len(conn.deadlines))
	}
	if conn.deadlines[0].IsZero() {
		t.Fatal("expected non-zero slave auth read deadline")
	}
	if !conn.deadlines[1].IsZero() {
		t.Fatalf("expected slave auth read deadline to be cleared, got %v", conn.deadlines[1])
	}
	if !strings.Contains(string(conn.writeData), "secret\n") {
		t.Fatalf("expected slave to send auth token, writes=%q", string(conn.writeData))
	}
	if !strings.Contains(string(conn.writeData), "RESUME 0\n") {
		t.Fatalf("expected slave to send resume request, writes=%q", string(conn.writeData))
	}
	if len(conn.writeDeadlines) != 4 {
		t.Fatalf("expected auth and resume write deadlines set and cleared, got %d calls", len(conn.writeDeadlines))
	}
	if conn.writeDeadlines[0].IsZero() || conn.writeDeadlines[2].IsZero() {
		t.Fatalf("expected non-zero write deadlines, got %v", conn.writeDeadlines)
	}
	if !conn.writeDeadlines[1].IsZero() || !conn.writeDeadlines[3].IsZero() {
		t.Fatalf("expected write deadlines to be cleared, got %v", conn.writeDeadlines)
	}
	mgr.Stop()
}

func TestSendWALToSlaveRejectsShortWrite(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	writer := &shortReplicationWriter{limit: 4}
	slave := &SlaveConnection{
		ID:       "short-writer",
		Writer:   bufio.NewWriterSize(writer, 1024),
		LastPing: time.Now(),
	}

	err := mgr.sendWALToSlave(slave, []byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("sendWALToSlave short write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestSendWALToSlaveSetsWriteDeadline(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	conn := &authDeadlineConn{}
	slave := &SlaveConnection{
		ID:       "deadline-writer",
		Conn:     conn,
		Writer:   bufio.NewWriter(conn),
		LastPing: time.Now(),
	}

	if err := mgr.sendWALToSlave(slave, []byte("abcdef")); err != nil {
		t.Fatalf("sendWALToSlave failed: %v", err)
	}
	if len(conn.writeDeadlines) != 2 {
		t.Fatalf("expected write deadline set and cleared, got %d calls", len(conn.writeDeadlines))
	}
	if conn.writeDeadlines[0].IsZero() {
		t.Fatal("expected non-zero replication write deadline")
	}
	if !conn.writeDeadlines[1].IsZero() {
		t.Fatalf("expected replication write deadline to be cleared, got %v", conn.writeDeadlines[1])
	}
}

func TestSendAckSetsWriteDeadline(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	conn := &authDeadlineConn{}
	mgr.setMasterConn(conn)
	atomic.StoreUint64(&mgr.lastApplied, 12)

	if err := mgr.sendAck(); err != nil {
		t.Fatalf("sendAck failed: %v", err)
	}
	if len(conn.writeDeadlines) != 2 {
		t.Fatalf("expected ACK write deadline set and cleared, got %d calls", len(conn.writeDeadlines))
	}
	if conn.writeDeadlines[0].IsZero() {
		t.Fatal("expected non-zero ACK write deadline")
	}
	if !conn.writeDeadlines[1].IsZero() {
		t.Fatalf("expected ACK write deadline to be cleared, got %v", conn.writeDeadlines[1])
	}
	if got := string(conn.writeData); got != "ACK 12\n" {
		t.Fatalf("expected ACK write, got %q", got)
	}
}

func TestReadMasterFrameSetsReadDeadline(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	conn := &authDeadlineConn{}
	mgr.setMasterConn(conn)

	reader := bufio.NewReader(strings.NewReader("PING 7\n"))
	if err := mgr.readMasterFrame(reader); err != nil {
		t.Fatalf("readMasterFrame failed: %v", err)
	}
	if len(conn.deadlines) != 2 {
		t.Fatalf("expected stream read deadline set and cleared, got %d calls", len(conn.deadlines))
	}
	if conn.deadlines[0].IsZero() {
		t.Fatal("expected non-zero stream read deadline")
	}
	if !conn.deadlines[1].IsZero() {
		t.Fatalf("expected stream read deadline to be cleared, got %v", conn.deadlines[1])
	}
	if got := string(conn.writeData); got != "PONG 0\n" {
		t.Fatalf("expected PONG response, got %q", got)
	}
}

func TestReplicateWALSendsOnlyEntriesAfterSlaveLSN(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	for i := 0; i < 3; i++ {
		if err := mgr.ReplicateWALEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}

	var out bytes.Buffer
	slave := &SlaveConnection{
		ID:       "slave-1",
		Writer:   bufio.NewWriter(&out),
		LastLSN:  2,
		LastPing: time.Now(),
	}
	mgr.slaves[slave.ID] = slave

	mgr.replicateWAL()

	var frameLen uint32
	if err := binary.Read(&out, binary.BigEndian, &frameLen); err != nil {
		t.Fatalf("Failed to read frame length: %v", err)
	}

	frame := make([]byte, frameLen)
	if _, err := out.Read(frame); err != nil {
		t.Fatalf("Failed to read frame: %v", err)
	}

	entries, err := decodeWALEntries(frame)
	if err != nil {
		t.Fatalf("Failed to decode WAL frame: %v", err)
	}
	if len(entries) != 1 || entries[0].LSN != 3 {
		t.Fatalf("Expected only LSN 3, got %+v", entries)
	}
}

func TestPruneWALBufferKeepsOnlyUnacknowledgedEntries(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	for i := 0; i < 5; i++ {
		if err := mgr.ReplicateWALEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}

	mgr.slaves["slow"] = &SlaveConnection{ID: "slow", LastLSN: 3}
	mgr.slaves["fast"] = &SlaveConnection{ID: "fast", LastLSN: 5}

	mgr.pruneWALBuffer()

	if len(mgr.walBuffer) != 2 {
		t.Fatalf("Expected 2 retained entries, got %d", len(mgr.walBuffer))
	}
	if mgr.walBuffer[0].LSN != 4 || mgr.walBuffer[1].LSN != 5 {
		t.Fatalf("Expected retained LSNs 4 and 5, got %+v", mgr.walBuffer)
	}

	mgr.slaves["slow"].LastLSN = 5
	mgr.pruneWALBuffer()

	if len(mgr.walBuffer) != 0 {
		t.Fatalf("Expected fully pruned WAL buffer, got %d entries", len(mgr.walBuffer))
	}
}

func TestWALRetentionBoundsBufferWithoutSlaves(t *testing.T) {
	mgr := NewManager(&Config{
		Role:                RoleMaster,
		Mode:                ModeAsync,
		MaxWALBufferEntries: 3,
	})

	for i := 0; i < 5; i++ {
		if err := mgr.ReplicateWALEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}

	if len(mgr.walBuffer) != 3 {
		t.Fatalf("Expected retained buffer length 3, got %d", len(mgr.walBuffer))
	}
	for i, entry := range mgr.walBuffer {
		expectedLSN := uint64(i + 3)
		if entry.LSN != expectedLSN {
			t.Fatalf("Entry %d: expected LSN %d, got %d", i, expectedLSN, entry.LSN)
		}
	}
}

func TestWALRetentionBoundsBufferWithLaggingSlave(t *testing.T) {
	mgr := NewManager(&Config{
		Role:                RoleMaster,
		Mode:                ModeAsync,
		MaxWALBufferEntries: 2,
	})
	mgr.slaves["lagging"] = &SlaveConnection{ID: "lagging", LastLSN: 1}

	for i := 0; i < 5; i++ {
		if err := mgr.ReplicateWALEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}

	if len(mgr.walBuffer) != 2 {
		t.Fatalf("Expected retained buffer length 2, got %d", len(mgr.walBuffer))
	}
	if mgr.walBuffer[0].LSN != 4 || mgr.walBuffer[1].LSN != 5 {
		t.Fatalf("Expected retained LSNs 4 and 5, got %+v", mgr.walBuffer)
	}
}

func TestWALRetentionBoundsBufferByBytes(t *testing.T) {
	mgr := NewManager(&Config{
		Role:                RoleMaster,
		Mode:                ModeAsync,
		MaxWALBufferEntries: 100,
		MaxWALBufferBytes:   55,
	})

	for i := 0; i < 3; i++ {
		if err := mgr.ReplicateWALEntry([]byte{byte('a' + i)}); err != nil {
			t.Fatalf("ReplicateWALEntry failed: %v", err)
		}
	}

	if len(mgr.walBuffer) != 2 {
		t.Fatalf("Expected retained buffer length 2, got %d", len(mgr.walBuffer))
	}
	if mgr.walBufferBytes != 50 {
		t.Fatalf("Expected retained WAL bytes 50, got %d", mgr.walBufferBytes)
	}
	if mgr.walBuffer[0].LSN != 2 || mgr.walBuffer[1].LSN != 3 {
		t.Fatalf("Expected retained LSNs 2 and 3, got %+v", mgr.walBuffer)
	}
}

func TestWALRetentionDropsOversizedSingleEntry(t *testing.T) {
	mgr := NewManager(&Config{
		Role:                RoleMaster,
		Mode:                ModeAsync,
		MaxWALBufferEntries: 100,
		MaxWALBufferBytes:   10,
	})

	if err := mgr.ReplicateWALEntry([]byte("too-large")); err != nil {
		t.Fatalf("ReplicateWALEntry failed: %v", err)
	}

	if len(mgr.walBuffer) != 0 {
		t.Fatalf("Expected oversized entry to be dropped, got %d retained entries", len(mgr.walBuffer))
	}
	if mgr.walBufferBytes != 0 {
		t.Fatalf("Expected retained WAL bytes 0, got %d", mgr.walBufferBytes)
	}
}

// TestWALEntryEncodeDecodeVariations tests Encode/Decode with various scenarios
func TestWALEntryEncodeDecodeVariations(t *testing.T) {
	// Test with valid entry first
	entry := &WALEntry{
		LSN:       1,
		Timestamp: time.Now(),
		Data:      []byte("test"),
		Checksum:  12345,
	}

	data, err := entry.Encode()
	if err != nil {
		t.Errorf("Encode failed for valid entry: %v", err)
	}
	if len(data) == 0 {
		t.Error("Encode returned empty data for valid entry")
	}

	// Test round-trip
	decoded := &WALEntry{}
	err = decoded.Decode(data)
	if err != nil {
		t.Errorf("Decode failed: %v", err)
	}
	if decoded.LSN != entry.LSN {
		t.Errorf("LSN mismatch: expected %d, got %d", entry.LSN, decoded.LSN)
	}
	if string(decoded.Data) != string(entry.Data) {
		t.Errorf("Data mismatch: expected %s, got %s", entry.Data, decoded.Data)
	}
	if decoded.Checksum != entry.Checksum {
		t.Errorf("Checksum mismatch: expected %d, got %d", entry.Checksum, decoded.Checksum)
	}
}

// TestEncodeWALEntriesMultiple tests encodeWALEntries with multiple entries
func TestEncodeWALEntriesMultiple(t *testing.T) {
	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("entry1"), Checksum: 100},
		{LSN: 2, Timestamp: time.Now(), Data: []byte("entry2"), Checksum: 200},
		{LSN: 3, Timestamp: time.Now(), Data: []byte("entry3"), Checksum: 300},
	}

	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("encodeWALEntries failed: %v", err)
	}

	// Decode and verify
	decoded, err := decodeWALEntries(data)
	if err != nil {
		t.Fatalf("decodeWALEntries failed: %v", err)
	}

	if len(decoded) != 3 {
		t.Errorf("Expected 3 decoded entries, got %d", len(decoded))
	}

	for i, entry := range decoded {
		if entry.LSN != uint64(i+1) {
			t.Errorf("Entry %d: expected LSN=%d, got %d", i, i+1, entry.LSN)
		}
	}
}

// TestEncodeWALEntriesError tests encodeWALEntries error handling
func TestEncodeWALEntriesError(t *testing.T) {
	// Create an entry that will fail to encode
	// This is hard to trigger since bytes.Buffer rarely fails
	// But we can test the error propagation path

	entries := []*WALEntry{}
	data, err := encodeWALEntries(entries)
	if err != nil {
		t.Errorf("encodeWALEntries failed for empty slice: %v", err)
	}

	// Verify empty encoding
	decoded, err := decodeWALEntries(data)
	if err != nil {
		t.Errorf("decodeWALEntries failed for empty data: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("Expected 0 decoded entries, got %d", len(decoded))
	}
}

// TestDecodeWALEntriesErrorCases tests decodeWALEntries error scenarios
func TestDecodeWALEntriesErrorCases(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "truncated header",
			data: []byte{0x00, 0x00}, // Only 2 bytes, need 4 for numEntries
		},
		{
			name: "entry count exceeds remaining payload",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint32(1000)) // 1000 entries
				return buf.Bytes()
			}(),
		},
		{
			name: "invalid entry length",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint32(1))    // 1 entry
				binary.Write(buf, binary.BigEndian, uint32(1000)) // But length says 1000 bytes
				return buf.Bytes()
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeWALEntries(tt.data)
			if err == nil {
				t.Error("Expected error for invalid data")
			}
		})
	}
}

func TestDecodeWALEntriesRejectsOversizedEntryLengthBeforeAllocation(t *testing.T) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))  // 1 entry
	binary.Write(buf, binary.BigEndian, ^uint32(0)) // Impossible entry length
	buf.Write([]byte("short"))                      // Small payload

	_, err := decodeWALEntries(buf.Bytes())
	if err == nil || !strings.Contains(err.Error(), "exceeds remaining payload") {
		t.Fatalf("expected oversized entry length error, got %v", err)
	}
}

// TestWALEntryDecodeErrors tests WALEntry.Decode error scenarios
func TestWALEntryDecodeErrors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "truncated LSN",
			data: []byte{0x00, 0x00, 0x00}, // Only 3 bytes, need 8 for LSN
		},
		{
			name: "truncated timestamp",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint64(1)) // LSN
				return buf.Bytes()
			}(),
		},
		{
			name: "truncated data length",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint64(1))                    // LSN
				binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano())) // Timestamp
				return buf.Bytes()
			}(),
		},
		{
			name: "truncated data content",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint64(1))                    // LSN
				binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano())) // Timestamp
				binary.Write(buf, binary.BigEndian, uint32(100))                  // Data length = 100
				buf.Write([]byte("short"))                                        // But only 5 bytes
				return buf.Bytes()
			}(),
		},
		{
			name: "truncated checksum",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.BigEndian, uint64(1))                    // LSN
				binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano())) // Timestamp
				binary.Write(buf, binary.BigEndian, uint32(4))                    // Data length = 4
				buf.Write([]byte("test"))                                         // 4 bytes of data
				// Missing checksum
				return buf.Bytes()
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &WALEntry{}
			err := entry.Decode(tt.data)
			if err == nil {
				t.Error("Expected error for invalid data")
			}
		})
	}
}

func TestWALEntryDecodeRejectsOversizedDataLengthBeforeAllocation(t *testing.T) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint64(1))                    // LSN
	binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano())) // Timestamp
	binary.Write(buf, binary.BigEndian, ^uint32(0))                   // Impossible data length
	buf.Write([]byte("short"))
	binary.Write(buf, binary.BigEndian, uint32(0)) // Checksum bytes are present

	entry := &WALEntry{}
	err := entry.Decode(buf.Bytes())
	if err == nil || !strings.Contains(err.Error(), "WAL entry data too large") {
		t.Fatalf("expected oversized data length error, got %v", err)
	}
	if entry.Data != nil {
		t.Fatalf("entry data should not be allocated on invalid length, got %d bytes", len(entry.Data))
	}
}

// TestReplicateWALEntryNotMaster tests ReplicateWALEntry when not master
func TestReplicateWALEntryNotMaster(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	err := mgr.ReplicateWALEntry([]byte("test"))
	if err != nil {
		t.Error("ReplicateWALEntry should return nil when not master")
	}

	// Verify buffer is empty
	if len(mgr.walBuffer) != 0 {
		t.Error("WAL buffer should be empty for slave")
	}
}

// TestReplicateWALEntryMaster tests ReplicateWALEntry as master
func TestReplicateWALEntryMaster(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})

	data := []byte("test data")
	err := mgr.ReplicateWALEntry(data)
	if err != nil {
		t.Errorf("ReplicateWALEntry failed: %v", err)
	}

	// Verify entry was added to buffer
	if len(mgr.walBuffer) != 1 {
		t.Errorf("Expected 1 entry in buffer, got %d", len(mgr.walBuffer))
	}

	entry := mgr.walBuffer[0]
	if string(entry.Data) != string(data) {
		t.Errorf("Data mismatch: expected %s, got %s", data, entry.Data)
	}
	if entry.LSN != 1 {
		t.Errorf("Expected LSN=1, got %d", entry.LSN)
	}
	if entry.Checksum != calculateCRC32(data) {
		t.Error("Checksum mismatch")
	}
}

func TestReplicateWALEntryCopiesInputData(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})

	data := []byte("stable data")
	originalChecksum := calculateCRC32(data)
	if err := mgr.ReplicateWALEntry(data); err != nil {
		t.Fatalf("ReplicateWALEntry failed: %v", err)
	}

	data[0] = 'X'

	entry := mgr.walBuffer[0]
	if got := string(entry.Data); got != "stable data" {
		t.Fatalf("entry data was mutated through caller slice: got %q", got)
	}
	if entry.Checksum != originalChecksum {
		t.Fatalf("entry checksum changed: got %d, want %d", entry.Checksum, originalChecksum)
	}
}

func TestNewManagerCopiesConfigSlaves(t *testing.T) {
	slaves := []string{"slave-a"}
	mgr := NewManager(&Config{
		Role:   RoleMaster,
		Mode:   ModeAsync,
		Slaves: slaves,
	})

	slaves[0] = "mutated"

	if got := mgr.config.Slaves[0]; got != "slave-a" {
		t.Fatalf("manager config slaves was mutated through caller slice: got %q", got)
	}
}

// TestReplicateWALEntryMultiple tests multiple entries
func TestReplicateWALEntryMultiple(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})

	for i := 1; i <= 5; i++ {
		data := []byte(string(rune('a' + i)))
		err := mgr.ReplicateWALEntry(data)
		if err != nil {
			t.Errorf("ReplicateWALEntry failed at iteration %d: %v", i, err)
		}
	}

	if len(mgr.walBuffer) != 5 {
		t.Errorf("Expected 5 entries in buffer, got %d", len(mgr.walBuffer))
	}

	// Verify LSNs are sequential
	for i, entry := range mgr.walBuffer {
		expectedLSN := uint64(i + 1)
		if entry.LSN != expectedLSN {
			t.Errorf("Entry %d: expected LSN=%d, got %d", i, expectedLSN, entry.LSN)
		}
	}
}

// TestCalculateCRC32 tests CRC32 calculation
func TestCalculateCRC32Variations(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint32
	}{
		{
			name:     "empty",
			data:     []byte{},
			expected: 0,
		},
		{
			name:     "single byte",
			data:     []byte{0x01},
			expected: calculateCRC32([]byte{0x01}),
		},
		{
			name:     "multiple bytes",
			data:     []byte("hello world"),
			expected: calculateCRC32([]byte("hello world")),
		},
		{
			name:     "binary data",
			data:     []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC},
			expected: calculateCRC32([]byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateCRC32(tt.data)
			if result != tt.expected {
				t.Errorf("CRC32 mismatch: expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// TestGetMetricsAppliedEntries tests GetMetrics with applied entries
func TestGetMetricsAppliedEntries(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})

	// Apply some entries
	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test1"), Checksum: calculateCRC32([]byte("test1"))},
		{LSN: 2, Timestamp: time.Now(), Data: []byte("test2"), Checksum: calculateCRC32([]byte("test2"))},
	}

	data, _ := encodeWALEntries(entries)
	mgr.applyWALData(string(data))

	// Get metrics
	metrics := mgr.GetMetrics()
	if metrics.AppliedEntries != 2 {
		t.Errorf("Expected 2 applied entries, got %d", metrics.AppliedEntries)
	}
	if metrics.LastAppliedTime == 0 {
		t.Fatal("Expected last applied time to be recorded")
	}

	previousAppliedTime := metrics.LastAppliedTime
	if err := mgr.applyWALData(string(data)); err != nil {
		t.Fatalf("Failed to reapply duplicate WAL data: %v", err)
	}
	metrics = mgr.GetMetrics()
	if metrics.LastAppliedTime != previousAppliedTime {
		t.Fatalf("Expected duplicate WAL data not to update last applied time, got %d want %d",
			metrics.LastAppliedTime, previousAppliedTime)
	}
}

func TestGetMetricsComputesSlaveReplicationLag(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	atomic.StoreInt64(&mgr.metrics.LastAppliedTime, time.Now().Add(-2*time.Second).Unix())

	metrics := mgr.GetMetrics()
	if metrics.LastAppliedTime == 0 {
		t.Fatal("Expected last applied time to be exposed")
	}
	if metrics.ReplicationLag <= 0 {
		t.Fatalf("Expected positive slave replication lag, got %d", metrics.ReplicationLag)
	}
}

func TestGetMetricsComputesMasterReplicationLag(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	atomic.StoreUint64(&mgr.currentLSN, 10)
	mgr.slaves["s1"] = &SlaveConnection{
		ID:       "s1",
		LastLSN:  5,
		LastPing: time.Now().Add(-2 * time.Second),
	}
	mgr.slaves["s2"] = &SlaveConnection{
		ID:       "s2",
		LastLSN:  10,
		LastPing: time.Now().Add(-10 * time.Second),
	}

	metrics := mgr.GetMetrics()
	if metrics.ReplicationLag <= 0 {
		t.Fatalf("Expected positive master replication lag for behind slave, got %d", metrics.ReplicationLag)
	}
}
