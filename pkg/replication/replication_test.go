package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWALEntryEncodeDecode(t *testing.T) {
	entry := &WALEntry{
		LSN:       12345,
		Timestamp: time.Now(),
		Data:      []byte("test data for WAL entry"),
		Checksum:  calculateCRC32([]byte("test data for WAL entry")),
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Failed to encode WAL entry: %v", err)
	}

	decoded := &WALEntry{}
	if err := decoded.Decode(encoded); err != nil {
		t.Fatalf("Failed to decode WAL entry: %v", err)
	}

	if decoded.LSN != entry.LSN {
		t.Errorf("LSN mismatch: got %d, want %d", decoded.LSN, entry.LSN)
	}

	if string(decoded.Data) != string(entry.Data) {
		t.Errorf("Data mismatch: got %s, want %s", decoded.Data, entry.Data)
	}

	if decoded.Checksum != entry.Checksum {
		t.Errorf("Checksum mismatch: got %d, want %d", decoded.Checksum, entry.Checksum)
	}
}

func TestWALEntryDecodeRejectsTrailingData(t *testing.T) {
	entry := &WALEntry{
		LSN:       12345,
		Timestamp: time.Now(),
		Data:      []byte("test data for WAL entry"),
		Checksum:  calculateCRC32([]byte("test data for WAL entry")),
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	encoded = append(encoded, 0xde, 0xad, 0xbe, 0xef)

	var decoded WALEntry
	err = decoded.Decode(encoded)
	if err == nil {
		t.Fatal("expected trailing WAL entry data to be rejected")
	}
	if !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("expected trailing data error, got %v", err)
	}
}

func TestWALEntryEncodeRejectsOversizedData(t *testing.T) {
	entry := &WALEntry{
		LSN:       1,
		Timestamp: time.Now(),
		Data:      make([]byte, maxWALEntryDataBytes+1),
	}

	_, err := entry.Encode()
	if err == nil {
		t.Fatal("expected oversized WAL entry data to be rejected")
	}
	if !strings.Contains(err.Error(), "WAL entry data too large") {
		t.Fatalf("expected WAL entry size error, got %v", err)
	}
}

func TestWALEntryDecodeRejectsOversizedDataBeforeAllocation(t *testing.T) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, uint64(1)); err != nil {
		t.Fatalf("write LSN: %v", err)
	}
	if err := binary.Write(buf, binary.BigEndian, time.Now().UnixNano()); err != nil {
		t.Fatalf("write timestamp: %v", err)
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(maxWALEntryDataBytes+1)); err != nil {
		t.Fatalf("write data length: %v", err)
	}
	buf.Write(make([]byte, 4))

	var decoded WALEntry
	err := decoded.Decode(buf.Bytes())
	if err == nil {
		t.Fatal("expected oversized WAL entry data to be rejected")
	}
	if !strings.Contains(err.Error(), "WAL entry data too large") {
		t.Fatalf("expected WAL entry size error, got %v", err)
	}
}

func TestDecodeWALEntriesRejectsTrailingPayload(t *testing.T) {
	entry := &WALEntry{
		LSN:       12345,
		Timestamp: time.Now(),
		Data:      []byte("replicated payload"),
		Checksum:  calculateCRC32([]byte("replicated payload")),
	}

	encoded, err := encodeWALEntries([]*WALEntry{entry})
	if err != nil {
		t.Fatalf("encodeWALEntries: %v", err)
	}
	encoded = append(encoded, 0xde, 0xad, 0xbe, 0xef)

	_, err = decodeWALEntries(encoded)
	if err == nil {
		t.Fatal("expected trailing WAL entries payload to be rejected")
	}
	if !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("expected trailing data error, got %v", err)
	}
}

func TestManagerCreation(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.ListenAddr = "127.0.0.1:0"

	mgr := NewManager(config)
	if mgr == nil {
		t.Fatal("Failed to create manager")
	}

	if mgr.config.Role != RoleMaster {
		t.Errorf("Role mismatch: got %v, want %v", mgr.config.Role, RoleMaster)
	}
}

func TestReplicationListenAddressRequiresAuth(t *testing.T) {
	tests := []struct {
		address string
		want    bool
	}{
		{"127.0.0.1:0", false},
		{"[::1]:0", false},
		{"localhost:0", false},
		{":0", true},
		{"0.0.0.0:0", true},
		{"[::]:0", true},
		{"192.0.2.10:9000", true},
		{"replica.example.com:9000", true},
		{"invalid:address:format", false},
	}

	for _, tt := range tests {
		t.Run(tt.address, func(t *testing.T) {
			if got := replicationListenAddressRequiresAuth(tt.address); got != tt.want {
				t.Fatalf("replicationListenAddressRequiresAuth(%q) = %v, want %v", tt.address, got, tt.want)
			}
		})
	}
}

func TestStartMasterRejectsUnauthenticatedNonLoopbackListener(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.ListenAddr = "0.0.0.0:0"

	mgr := NewManager(config)
	err := mgr.startMaster()
	if err == nil {
		_ = mgr.Stop()
		t.Fatal("expected unauthenticated non-loopback listener to be rejected")
	}
	if !strings.Contains(err.Error(), "auth token is required") {
		t.Fatalf("expected auth token error, got %v", err)
	}
}

func TestReplicationAuthTokenEqual(t *testing.T) {
	if !replicationAuthTokenEqual("secret-token", "secret-token") {
		t.Fatal("matching tokens should authenticate")
	}

	tests := []struct {
		name     string
		provided string
		expected string
	}{
		{name: "different content", provided: "secret-tokem", expected: "secret-token"},
		{name: "shorter provided", provided: "secret", expected: "secret-token"},
		{name: "longer provided", provided: "secret-token-extra", expected: "secret-token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if replicationAuthTokenEqual(tt.provided, tt.expected) {
				t.Fatalf("tokens %q and %q should not authenticate", tt.provided, tt.expected)
			}
		})
	}
}

func TestReplicationAsyncMode(t *testing.T) {
	// Create master
	masterConfig := DefaultConfig()
	masterConfig.Role = RoleMaster
	masterConfig.ListenAddr = "127.0.0.1:0"
	masterConfig.Mode = ModeAsync

	master := NewManager(masterConfig)
	if err := master.Start(); err != nil {
		t.Fatalf("Failed to start master: %v", err)
	}
	defer master.Stop()

	// Get actual address
	addr := master.listener.Addr().String()

	// Create slave
	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = addr

	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("Failed to start slave: %v", err)
	}
	defer slave.Stop()

	// Wait for connection
	time.Sleep(100 * time.Millisecond)

	// Verify slave connected
	metrics := master.GetMetrics()
	if metrics.ActiveSlaves != 1 {
		t.Errorf("Expected 1 active slave, got %d", metrics.ActiveSlaves)
	}
}

func TestReplicateWALEntry(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	entry := []byte("INSERT INTO test VALUES (1, 'data')")
	if err := mgr.ReplicateWALEntry(entry); err != nil {
		t.Fatalf("Failed to replicate WAL entry: %v", err)
	}

	if len(mgr.walBuffer) != 1 {
		t.Errorf("Expected 1 entry in WAL buffer, got %d", len(mgr.walBuffer))
	}

	if mgr.currentLSN != 1 {
		t.Errorf("Expected LSN 1, got %d", mgr.currentLSN)
	}
}

func TestGetStatus(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	status := mgr.GetStatus()
	if status.Role != "master" {
		t.Errorf("Expected role 'master', got %s", status.Role)
	}
}

func TestReplicationModeString(t *testing.T) {
	tests := []struct {
		mode     ReplicationMode
		expected string
	}{
		{ModeAsync, "async"},
		{ModeSync, "sync"},
		{ModeFullSync, "full_sync"},
		{ReplicationMode(999), "unknown"},
	}

	for _, test := range tests {
		result := replicationModeString(test.mode)
		if result != test.expected {
			t.Errorf("replicationModeString(%v) = %s, want %s", test.mode, result, test.expected)
		}
	}
}

// TestWALEntryEncodeEmptyData covers Encode/Decode with empty data.
// Ported from coverage_boost_replication_test.go — the existing
// TestWALEntryEncodeDecode only covers non-empty data.
func TestWALEntryEncodeEmptyData(t *testing.T) {
	entry := &WALEntry{
		LSN:       1,
		Timestamp: time.Now(),
		Data:      []byte{},
		Checksum:  0,
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Failed to encode empty data entry: %v", err)
	}

	decoded := &WALEntry{}
	if err := decoded.Decode(encoded); err != nil {
		t.Fatalf("Failed to decode empty data entry: %v", err)
	}

	if len(decoded.Data) != 0 {
		t.Errorf("Expected empty data, got %d bytes", len(decoded.Data))
	}
}

// TestWALEntryEncodeLargeData covers Encode/Decode with a 1MB payload.
// The existing tests use small payloads. Ported from
// coverage_boost_replication_test.go.
func TestWALEntryEncodeLargeData(t *testing.T) {
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	entry := &WALEntry{
		LSN:       999999,
		Timestamp: time.Now(),
		Data:      largeData,
		Checksum:  calculateCRC32(largeData),
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Failed to encode large data entry: %v", err)
	}

	decoded := &WALEntry{}
	if err := decoded.Decode(encoded); err != nil {
		t.Fatalf("Failed to decode large data entry: %v", err)
	}

	if !bytes.Equal(decoded.Data, largeData) {
		t.Error("Large data mismatch after decode")
	}
}

// TestWALEntryEncodeZeroValues covers Encode/Decode when LSN, Timestamp,
// Data, and Checksum are all zero/nil. Ported from
// coverage_boost_replication_test.go.
func TestWALEntryEncodeZeroValues(t *testing.T) {
	entry := &WALEntry{
		LSN:       0,
		Timestamp: time.Time{},
		Data:      nil,
		Checksum:  0,
	}

	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Failed to encode zero values entry: %v", err)
	}

	decoded := &WALEntry{}
	if err := decoded.Decode(encoded); err != nil {
		t.Fatalf("Failed to decode zero values entry: %v", err)
	}

	if decoded.LSN != 0 {
		t.Errorf("Expected LSN=0, got %d", decoded.LSN)
	}
}

// TestEncodeWALEntriesSingle covers the encodeWALEntries + decodeWALEntries
// round-trip for a single entry. Ported from
// coverage_boost_replication_test.go — the existing tests in
// replication_coverage_test.go cover multiple-entry cases.
func TestEncodeWALEntriesSingle(t *testing.T) {
	entries := []*WALEntry{
		{
			LSN:       1,
			Timestamp: time.Now(),
			Data:      []byte("single entry"),
			Checksum:  calculateCRC32([]byte("single entry")),
		},
	}

	encoded, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("Failed to encode single entry: %v", err)
	}

	// Verify header has count=1
	count := binary.BigEndian.Uint32(encoded[0:4])
	if count != 1 {
		t.Errorf("Expected count=1, got %d", count)
	}

	decoded, err := decodeWALEntries(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if len(decoded) != 1 {
		t.Errorf("Expected 1 decoded entry, got %d", len(decoded))
	}
}

// TestEncodeWALEntriesMany covers encodeWALEntries with 50 entries.
// Ported from coverage_boost_replication_test.go.
func TestEncodeWALEntriesMany(t *testing.T) {
	entries := make([]*WALEntry, 50)
	for i := 0; i < 50; i++ {
		data := []byte(string(rune('A' + i%26)))
		entries[i] = &WALEntry{
			LSN:       uint64(i + 1),
			Timestamp: time.Now().Add(time.Duration(i) * time.Second),
			Data:      data,
			Checksum:  calculateCRC32(data),
		}
	}

	encoded, err := encodeWALEntries(entries)
	if err != nil {
		t.Fatalf("Failed to encode many entries: %v", err)
	}

	decoded, err := decodeWALEntries(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if len(decoded) != 50 {
		t.Errorf("Expected 50 decoded entries, got %d", len(decoded))
	}
}

// TestHandleMasterMessageSTART covers handleMasterMessage with a START
// message that updates lastApplied. Ported from
// coverage_boost_replication_test.go.
func TestHandleMasterMessageSTART(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	mgr.masterConn = &mockConn{}

	err := mgr.handleMasterMessage("START 12345\n")
	if err != nil {
		t.Errorf("handleMasterMessage START failed: %v", err)
	}

	if mgr.lastApplied != 12345 {
		t.Errorf("Expected lastApplied=12345, got %d", mgr.lastApplied)
	}
}

// TestHandleMasterMessagePING covers handleMasterMessage with a PING
// message. Ported from coverage_boost_replication_test.go.
func TestHandleMasterMessagePING(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	mgr.masterConn = &mockConn{}
	mgr.lastApplied = 999

	err := mgr.handleMasterMessage("PING 0\n")
	if err != nil {
		t.Errorf("handleMasterMessage PING failed: %v", err)
	}
}

// TestHandleMasterMessageInvalid covers handleMasterMessage with
// short, empty, and malformed messages. Ported from
// coverage_boost_replication_test.go.
func TestHandleMasterMessageInvalid(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	mgr.masterConn = &mockConn{}

	tests := []struct {
		name    string
		msg     string
		wantErr bool
	}{
		{name: "too short", msg: "AB\n", wantErr: true},
		{name: "exactly 4 chars but unknown", msg: "XXXX 123\n", wantErr: false},
		{name: "empty message", msg: "", wantErr: true},
		{name: "START without LSN", msg: "START\n", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.handleMasterMessage(tt.msg)
			if tt.wantErr && err == nil {
				t.Error("Expected error but got nil")
			}
		})
	}
}

// TestReplicateFromMasterStop covers replicateFromMaster's stop
// channel handling. Ported from coverage_boost_replication_test.go.
func TestReplicateFromMasterStop(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	mgr := NewManager(config)
	mgr.stopCh = make(chan struct{})

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	mgr.masterConn = client

	mgr.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.replicateFromMaster()
	}()

	close(mgr.stopCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("replicateFromMaster did not stop in time")
	}
}

// TestReplicateFromMasterReadError covers replicateFromMaster's
// OnDisconnect callback when the master closes the connection.
// Ported from coverage_boost_replication_test.go.
func TestReplicateFromMasterReadError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	mgr := NewManager(config)

	client, server := net.Pipe()
	server.Close() // Causes read error on client

	mgr.masterConn = client

	disconnectCalled := false
	mgr.OnDisconnect = func(slave string, err error) {
		disconnectCalled = true
	}

	mgr.wg.Add(1)
	mgr.replicateFromMaster()

	if !disconnectCalled {
		t.Error("Expected OnDisconnect to be called on read error")
	}
}

// TestReplicateWALGapForcesResync verifies that when WAL retention has evicted
// entries a still-connected lagging slave needs, the master does NOT silently
// stream the surviving suffix (which would diverge the slave); instead it flags
// the slave for a snapshot resync and sends a RESYNC control message.
func TestReplicateWALGapForcesResync(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	m := NewManager(config)

	// Buffer retains LSN 5,6,7 — entries 1..4 were pruned by retention.
	m.walBuffer = []*WALEntry{
		{LSN: 5, Data: []byte("e5")},
		{LSN: 6, Data: []byte("e6")},
		{LSN: 7, Data: []byte("e7")},
	}
	m.currentLSN = 7

	var buf bytes.Buffer
	slave := &SlaveConnection{
		ID:       "lagging",
		Writer:   bufio.NewWriter(&buf),
		LastLSN:  2, // needs entry 3, which was pruned -> gap
		LastPing: time.Now(),
	}
	m.slaves = map[string]*SlaveConnection{slave.ID: slave}

	m.replicateWAL()

	slave.mu.Lock()
	needsSnap := slave.NeedsSnapshot
	slave.mu.Unlock()
	if !needsSnap {
		t.Fatal("expected slave to be flagged NeedsSnapshot after a WAL gap")
	}
	out := buf.String()
	if !strings.Contains(out, "RESYNC") {
		t.Fatalf("expected a RESYNC control message, got %q", out)
	}
	if strings.Contains(out, "e5") || strings.Contains(out, "e6") || strings.Contains(out, "e7") {
		t.Fatalf("gapped WAL data must not be streamed to the slave, got %q", out)
	}
}

// TestReplicateWALContiguousNoResync verifies the gap guard does NOT fire for a
// slave whose next-needed entry is still retained (normal streaming).
func TestReplicateWALContiguousNoResync(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	m := NewManager(config)

	m.walBuffer = []*WALEntry{
		{LSN: 3, Data: []byte("e3")},
		{LSN: 4, Data: []byte("e4")},
	}
	m.currentLSN = 4

	var buf bytes.Buffer
	slave := &SlaveConnection{
		ID:       "caught-up",
		Writer:   bufio.NewWriter(&buf),
		LastLSN:  2, // needs entry 3, which IS retained -> no gap
		LastPing: time.Now(),
	}
	m.slaves = map[string]*SlaveConnection{slave.ID: slave}

	m.replicateWAL()

	slave.mu.Lock()
	needsSnap := slave.NeedsSnapshot
	slave.mu.Unlock()
	if needsSnap {
		t.Fatal("gap guard fired for a slave with no gap")
	}
	if strings.Contains(buf.String(), "RESYNC") {
		t.Fatalf("unexpected RESYNC for a contiguous slave: %q", buf.String())
	}
}

// TestSendHeartbeatError covers sendHeartbeat with a writer that
// fails immediately. Ported from coverage_boost_replication_test.go.
func TestSendHeartbeatError(t *testing.T) {
	mw := &mockWriter{failAfter: 0}

	slave := &SlaveConnection{
		ID:       "test",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  100,
		LastPing: time.Now(),
	}

	mgr := NewManager(DefaultConfig())
	err := mgr.sendHeartbeat(slave)
	if err == nil {
		t.Error("Expected error with failing writer")
	}
}

// TestSendInitialSnapshotError covers sendInitialSnapshot with a
// failing writer. Ported from coverage_boost_replication_test.go.
func TestSendInitialSnapshotError(t *testing.T) {
	mw := &mockWriter{failAfter: 0}

	slave := &SlaveConnection{
		ID:     "test-slave",
		Writer: bufio.NewWriter(mw),
	}

	mgr := NewManager(DefaultConfig())
	err := mgr.sendInitialSnapshot(slave, 100)
	if err == nil {
		t.Error("Expected error with failing writer")
	}
}

// TestSendInitialSnapshotSuccess covers sendInitialSnapshot with a
// writer that succeeds. Ported from coverage_boost_replication_test.go.
func TestSendInitialSnapshotSuccess(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	mw := &mockWriter{failAfter: 100}

	slave := &SlaveConnection{
		ID:     "test",
		Writer: bufio.NewWriter(mw),
	}

	err := mgr.sendInitialSnapshot(slave, 12345)
	if err != nil {
		t.Errorf("sendInitialSnapshot failed: %v", err)
	}
}

// TestSendInitialSnapshotFlushError covers sendInitialSnapshot when
// write fails (the bufio.Writer flush will surface the error).
// Ported from coverage_boost_replication_test.go.
func TestSendInitialSnapshotFlushError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	mw := &mockWriter{failAfter: 0}

	slave := &SlaveConnection{
		ID:     "test",
		Writer: bufio.NewWriter(mw),
	}

	err := mgr.sendInitialSnapshot(slave, 12345)
	if err == nil {
		t.Error("Expected error when write fails")
	}
}

// TestSyncWALWithSlaves covers replicateWAL with a connected slave
// whose writer fails. Must not panic. Ported from
// coverage_boost_replication_test.go.
func TestSyncWALWithSlaves(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	for i := 0; i < 3; i++ {
		mgr.ReplicateWALEntry([]byte("test data"))
	}

	mw := &mockWriter{failAfter: 0}
	slave := &SlaveConnection{
		ID:       "test-slave",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  0,
		LastPing: time.Now(),
	}
	mgr.slaves["test-slave"] = slave

	mgr.replicateWAL()
}

// TestSendWALToSlaveError covers sendWALToSlave with a failing writer.
// Ported from coverage_boost_replication_test.go.
func TestSendWALToSlaveError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	mw := &mockWriter{failAfter: 0}
	slave := &SlaveConnection{
		ID:       "test",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  0,
		LastPing: time.Now(),
	}

	err := mgr.sendWALToSlave(slave, []byte("test data"))
	if err == nil {
		t.Error("Expected error with failing writer")
	}
}

// TestReplicateWALNoSlaves covers replicateWAL when the slaves map
// is empty. Ported from coverage_boost_replication_test.go.
func TestReplicateWALNoSlaves(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)
	mgr.ReplicateWALEntry([]byte("test"))
	mgr.replicateWAL()
}

// TestReplicateWALNoEntries covers replicateWAL when no WAL entries
// are buffered. Ported from coverage_boost_replication_test.go.
func TestReplicateWALNoEntries(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	mw := &mockWriter{failAfter: 0}
	slave := &SlaveConnection{
		ID:       "test",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  0,
		LastPing: time.Now(),
	}
	mgr.slaves["test"] = slave

	mgr.replicateWAL()
}

// TestReplicateWALWithOnLagCallback covers the OnLag callback path
// when a slave has a stale LastPing. Ported from
// coverage_boost_replication_test.go.
func TestReplicateWALWithOnLagCallback(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)
	mgr.ReplicateWALEntry([]byte("test"))

	mw := &mockWriter{failAfter: 0}
	slave := &SlaveConnection{
		ID:       "lagging-slave",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  0,
		LastPing: time.Now().Add(-time.Hour),
	}
	mgr.slaves["lagging-slave"] = slave

	lagCalled := false
	mgr.OnLag = func(slave string, lag time.Duration) {
		lagCalled = true
	}

	mgr.replicateWAL()

	if !lagCalled {
		t.Error("Expected OnLag to be called for lagging slave")
	}
}

// TestAuthenticateSlaveFailure covers authenticateSlave with a wrong
// token. Ported from coverage_boost_replication_test.go.
func TestAuthenticateSlaveFailure(t *testing.T) {
	config := DefaultConfig()
	config.AuthToken = "correct_token"

	mgr := NewManager(config)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		client.Write([]byte("wrong_token\n"))
		buf := make([]byte, 100)
		client.Read(buf)
	}()

	err := mgr.authenticateSlave(server)
	if err == nil {
		t.Error("Expected authentication error with wrong token")
	}
}

// TestAuthenticateSlaveReadError covers authenticateSlave when the
// client disconnects before sending a token. Ported from
// coverage_boost_replication_test.go.
func TestAuthenticateSlaveReadError(t *testing.T) {
	config := DefaultConfig()
	config.AuthToken = "token"

	mgr := NewManager(config)

	client, server := net.Pipe()
	client.Close()

	err := mgr.authenticateSlave(server)
	if err == nil {
		t.Error("Expected error when client disconnects during auth")
	}
}

// TestStartSlaveAuthWriteError covers startSlave when the master
// closes the connection during auth handshake. Ported from
// coverage_boost_replication_test.go.
func TestStartSlaveAuthWriteError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = listener.Addr().String()
	config.AuthToken = "token"

	mgr := NewManager(config)
	err = mgr.Start()
	if err == nil {
		mgr.Stop()
		t.Error("Expected error when master closes connection during auth")
	}
}

// TestStartSlaveConnectionFailure covers startSlave with a port
// nothing is listening on. Ported from
// coverage_boost_replication_test.go.
func TestStartSlaveConnectionFailure(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = "127.0.0.1:1"

	mgr := NewManager(config)
	err := mgr.Start()
	if err == nil {
		mgr.Stop()
		t.Error("Expected error when connecting to invalid master address")
	}
}

// TestStartSlaveInvalidAddress covers startSlave with a malformed
// address. Ported from coverage_boost_replication_test.go.
func TestStartSlaveInvalidAddress(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = "not_a_valid_address:::::"

	mgr := NewManager(config)
	err := mgr.Start()
	if err == nil {
		mgr.Stop()
		t.Error("Expected error with invalid address format")
	}
}

// TestHandleSlaveWithAuth covers handleSlave's authentication path:
// a slave connecting with a wrong token must not be added. Ported
// from coverage_boost_replication_test.go.
func TestHandleSlaveWithAuth(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.AuthToken = "secret"

	mgr := NewManager(config)
	mgr.stopCh = make(chan struct{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, _ := net.Dial("tcp", listener.Addr().String())
		if conn != nil {
			conn.Write([]byte("wrong_secret\n"))
			conn.Close()
		}
	}()

	conn, _ := listener.Accept()
	if conn != nil {
		mgr.handleSlave(conn)
	}

	if len(mgr.slaves) != 0 {
		t.Error("Expected no slaves after auth failure")
	}
}

// TestAcceptSlavesStop covers acceptSlaves' stop channel handling.
// Ported from coverage_boost_replication_test.go.
func TestAcceptSlavesStop(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)
	mgr.stopCh = make(chan struct{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	mgr.listener = listener

	done := make(chan struct{})
	mgr.wg.Add(1)
	go func() {
		defer close(done)
		mgr.acceptSlaves()
	}()

	close(mgr.stopCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("acceptSlaves did not stop in time")
	}
}

// TestSyncWALStop covers syncWAL's stop channel handling. Ported
// from coverage_boost_replication_test.go.
func TestSyncWALStop(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.SyncInterval = 10 * time.Millisecond

	mgr := NewManager(config)
	mgr.stopCh = make(chan struct{})

	done := make(chan struct{})
	mgr.wg.Add(1)
	go func() {
		defer close(done)
		mgr.syncWAL()
	}()

	time.Sleep(50 * time.Millisecond)
	close(mgr.stopCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("syncWAL did not stop in time")
	}
}

// TestSyncWALDefaultInterval covers syncWAL when SyncInterval is 0
// (the helper must pick a default). Ported from
// coverage_boost_replication_test.go.
func TestSyncWALDefaultInterval(t *testing.T) {
	m := NewManager(DefaultConfig())
	m.config.SyncInterval = 0
	m.wg.Add(1)
	go m.syncWAL()
	time.Sleep(5 * time.Millisecond)
	close(m.stopCh)
}

// TestGetStatusMasterWithMultipleSlaves covers GetStatus when the
// master has multiple slaves at different LSN positions. Ported
// from coverage_boost_replication_test.go.
func TestGetStatusMasterWithMultipleSlaves(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.Mode = ModeSync

	mgr := NewManager(config)
	mgr.currentLSN = 500

	for i := 0; i < 3; i++ {
		slave := &SlaveConnection{
			ID:       string(rune('A' + i)),
			LastLSN:  uint64(i * 100),
			LastPing: time.Now().Add(-time.Duration(i) * time.Second),
		}
		mgr.slaves[slave.ID] = slave
	}

	status := mgr.GetStatus()

	if status.ActiveSlaves != 3 {
		t.Errorf("Expected 3 active slaves, got %d", status.ActiveSlaves)
	}
	if len(status.Slaves) != 3 {
		t.Errorf("Expected 3 slaves in status, got %d", len(status.Slaves))
	}
	if status.CurrentMaster != 500 {
		t.Errorf("Expected CurrentMaster=500, got %d", status.CurrentMaster)
	}
}

// TestGetStatusModeVariations covers GetStatus's mode string for
// Async/Sync/FullSync. Ported from coverage_boost_replication_test.go.
func TestGetStatusModeVariations(t *testing.T) {
	tests := []struct {
		mode     ReplicationMode
		expected string
	}{
		{ModeAsync, "async"},
		{ModeSync, "sync"},
		{ModeFullSync, "full_sync"},
	}

	for _, tt := range tests {
		config := DefaultConfig()
		config.Role = RoleMaster
		config.Mode = tt.mode

		mgr := NewManager(config)
		status := mgr.GetStatus()

		if status.Mode != tt.expected {
			t.Errorf("Mode %v: expected %s, got %s", tt.mode, tt.expected, status.Mode)
		}
	}
}

// TestApplyWALDataEmptyEntries covers applyWALData when the encoded
// payload has zero entries — should be a no-op. Ported from
// coverage_boost_replication_test.go.
func TestApplyWALDataEmptyEntries(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})

	data, _ := encodeWALEntries([]*WALEntry{})

	err := mgr.applyWALData(string(data))
	if err != nil {
		t.Errorf("applyWALData with empty entries failed: %v", err)
	}
}

// TestApplyWALDataWithCallbackError covers applyWALData when the
// OnApply callback returns an error. Ported from
// coverage_boost_replication_test.go.
func TestApplyWALDataWithCallbackError(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("test"), Checksum: calculateCRC32([]byte("test"))},
	}
	data, _ := encodeWALEntries(entries)

	mgr.OnApply = func(entry *WALEntry) error {
		return errors.New("apply failed")
	}

	err := mgr.applyWALData(string(data))
	if err == nil {
		t.Error("Expected error when OnApply fails")
	}
}

// TestHandleMasterMessageWALData covers handleMasterMessage with
// arbitrary data that won't match START/PING/RESUME — it falls
// through to applyWALData. Ported from
// coverage_boost_replication_test.go.
func TestHandleMasterMessageWALData(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	mgr.masterConn = &mockConn{}

	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("wal data"), Checksum: calculateCRC32([]byte("wal data"))},
	}
	data, _ := encodeWALEntries(entries)

	err := mgr.handleMasterMessage(string(data))
	t.Logf("handleMasterMessage with WAL data returned: %v", err)
}

// TestWaitForSlavesWithLaggingSlave covers WaitForSlaves when a
// slave is behind the current LSN — must time out. Ported from
// coverage_boost_replication_test.go.
func TestWaitForSlavesWithLaggingSlave(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.Mode = ModeSync

	mgr := NewManager(config)
	mgr.currentLSN = 100

	slave := &SlaveConnection{
		ID:       "lagging",
		LastLSN:  50,
		LastPing: time.Now(),
	}
	mgr.slaves["lagging"] = slave

	err := mgr.WaitForSlaves(100 * time.Millisecond)
	if err == nil {
		t.Error("Expected timeout error with lagging slave")
	}
}

// TestWaitForSlavesCaughtUp covers WaitForSlaves when the slave has
// already reached the current LSN — must return nil. Ported from
// coverage_boost_replication_test.go.
func TestWaitForSlavesCaughtUp(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.Mode = ModeSync

	mgr := NewManager(config)
	mgr.currentLSN = 100

	slave := &SlaveConnection{
		ID:       "caughtup",
		LastLSN:  100,
		LastPing: time.Now(),
	}
	mgr.slaves["caughtup"] = slave

	err := mgr.WaitForSlaves(1 * time.Second)
	if err != nil {
		t.Errorf("Expected no error when slave is caught up: %v", err)
	}
}

// TestSaveReplicationStateErrors covers saveReplicationState's error
// paths (state file is a directory, so Create/Rename fail). Ported
// from coverage_boost_replication_test.go.
func TestSaveReplicationStateErrors(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.StateFile = filepath.Join(tempDir, "state", "repl.json")
	mgr := NewManager(config)

	if err := mgr.saveReplicationState(); err != nil {
		t.Fatalf("saveReplicationState failed: %v", err)
	}

	os.Remove(config.StateFile)
	if err := os.MkdirAll(config.StateFile, 0755); err != nil {
		t.Fatalf("failed to create state dir: %v", err)
	}
	if err := mgr.saveReplicationState(); err == nil {
		t.Fatal("expected error when state file is directory")
	}

	os.RemoveAll(config.StateFile)
	if err := os.MkdirAll(config.StateFile, 0755); err != nil {
		t.Fatalf("failed to create state dir for rename test: %v", err)
	}
	if err := mgr.saveReplicationState(); err == nil {
		t.Fatal("expected error when rename fails")
	}
}

// TestLoadReplicationStateErrors covers loadReplicationState's error
// paths (missing file returns nil, invalid JSON returns error).
// Ported from coverage_boost_replication_test.go.
func TestLoadReplicationStateErrors(t *testing.T) {
	tempDir := t.TempDir()
	config := DefaultConfig()
	config.StateFile = filepath.Join(tempDir, "state", "repl.json")
	mgr := NewManager(config)

	if err := mgr.loadReplicationState(); err != nil {
		t.Fatalf("loadReplicationState should return nil for missing file: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(config.StateFile), 0755); err != nil {
		t.Fatalf("failed to create state dir: %v", err)
	}
	if err := os.WriteFile(config.StateFile, []byte("not json"), 0644); err != nil {
		t.Fatalf("failed to write bad state: %v", err)
	}
	if err := mgr.loadReplicationState(); err == nil {
		t.Fatal("expected error for invalid state JSON")
	}
}

// TestRetainedWALBytesNil covers retainedWALBytes(nil) — must return
// 0. Ported from coverage_boost_replication_test.go.
func TestRetainedWALBytesNil(t *testing.T) {
	if retainedWALBytes(nil) != 0 {
		t.Fatal("retainedWALBytes(nil) should be 0")
	}
}

// TestFilterWALEntriesAfterNil covers filterWALEntriesAfter when all
// entries are at or before lastLSN — must return nil. Ported from
// coverage_boost_replication_test.go.
func TestFilterWALEntriesAfterNil(t *testing.T) {
	entries := []*WALEntry{{LSN: 1}, {LSN: 2}, {LSN: 3}}
	if filterWALEntriesAfter(entries, 5) != nil {
		t.Fatal("expected nil when all entries <= lastLSN")
	}
}

// TestCanResumeFromLocked covers canResumeFromLocked edge cases:
// requestedLSN > currentLSN returns false, and an empty walBuffer
// also returns false. Ported from coverage_boost_replication_test.go.
func TestCanResumeFromLocked(t *testing.T) {
	m := NewManager(DefaultConfig())

	if m.canResumeFromLocked(10, 5) {
		t.Fatal("expected false when requestedLSN > currentLSN")
	}
	if m.canResumeFromLocked(3, 5) {
		t.Fatal("expected false when walBuffer is empty")
	}
}

// TestPrepareSlaveResumeFutureLSN covers prepareSlaveResume when the
// requested LSN is in the future. With OnSnapshot set, the slave
// must be marked NeedsSnapshot; without, an error is returned.
// Ported from coverage_boost_replication_test.go.
func TestPrepareSlaveResumeFutureLSN(t *testing.T) {
	m := NewManager(DefaultConfig())
	m.OnSnapshot = func() ([]byte, error) { return []byte("data"), nil }
	slave := &SlaveConnection{}

	if err := m.prepareSlaveResume(slave, 100); err != nil {
		t.Fatalf("expected nil when OnSnapshot is set, got %v", err)
	}
	if !slave.NeedsSnapshot {
		t.Fatal("expected slave to need snapshot")
	}

	m.OnSnapshot = nil
	slave2 := &SlaveConnection{Writer: bufio.NewWriter(&bytes.Buffer{})}
	if err := m.prepareSlaveResume(slave2, 100); err == nil {
		t.Fatal("expected error when OnSnapshot is nil and LSN is future")
	}
}

// TestReceiveResumeRequestErrors covers receiveResumeRequest's error
// paths: invalid message, read failure, oversized control line.
// Ported from coverage_boost_replication_test.go.
func TestReceiveResumeRequestErrors(t *testing.T) {
	m := NewManager(DefaultConfig())

	reader := bufio.NewReader(bytes.NewReader([]byte("INVALID\n")))
	slave := &SlaveConnection{Reader: reader}
	if _, err := m.receiveResumeRequest(slave); err == nil {
		t.Fatal("expected error for invalid RESUME message")
	}

	errReader := &mockErrorReader{err: errors.New("read error")}
	slave2 := &SlaveConnection{Reader: bufio.NewReader(errReader)}
	if _, err := m.receiveResumeRequest(slave2); err == nil {
		t.Fatal("expected error for read failure")
	}

	oversized := strings.Repeat("A", maxReplicationControlLineBytes+1) + "\n"
	slave3 := &SlaveConnection{Reader: bufio.NewReader(strings.NewReader(oversized))}
	if _, err := m.receiveResumeRequest(slave3); err == nil || !strings.Contains(err.Error(), "control line too large") {
		t.Fatalf("expected oversized control line error, got %v", err)
	}
}

// TestSendAckPongNilConn covers sendAck/sendPong when masterConn is
// nil — both must return nil. Ported from
// coverage_boost_replication_test.go.
func TestSendAckPongNilConn(t *testing.T) {
	m := NewManager(DefaultConfig())
	if err := m.sendAck(); err != nil {
		t.Fatalf("sendAck with nil conn should return nil, got %v", err)
	}
	if err := m.sendPong(); err != nil {
		t.Fatalf("sendPong with nil conn should return nil, got %v", err)
	}
}

// TestDropWALPrefixLockedNoOp covers dropWALPrefixLocked with a
// non-positive drop count — must not modify the buffer. Ported
// from coverage_boost_replication_test.go.
func TestDropWALPrefixLockedNoOp(t *testing.T) {
	m := NewManager(DefaultConfig())
	m.walBuffer = []*WALEntry{{LSN: 1}}
	m.walBufferBytes = 10

	m.dropWALPrefixLocked(0)
	if len(m.walBuffer) != 1 {
		t.Fatal("dropWALPrefixLocked(0) should not modify buffer")
	}
	m.dropWALPrefixLocked(-1)
	if len(m.walBuffer) != 1 {
		t.Fatal("dropWALPrefixLocked(-1) should not modify buffer")
	}
}

// TestStopWithSlaves covers Stop closing slave connections. Ported
// from coverage_boost_replication_test.go.
func TestStopWithSlaves(t *testing.T) {
	m := NewManager(DefaultConfig())
	c1, _ := net.Pipe()
	m.slaves = map[string]*SlaveConnection{"s1": {Conn: c1}}
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

// TestStopReturnsConnectionCloseErrors covers Stop surfacing close
// errors from slaves and master connections, and confirms Stop is
// idempotent. Ported from coverage_boost_replication_test.go.
func TestStopReturnsConnectionCloseErrors(t *testing.T) {
	m := NewManager(DefaultConfig())
	slaveErr := errors.New("slave close failed")
	masterErr := errors.New("master close failed")
	m.slaves = map[string]*SlaveConnection{
		"s1": {Conn: &closeErrConn{err: slaveErr}},
	}
	m.masterConn = &closeErrConn{err: masterErr}

	err := m.Stop()
	if !errors.Is(err, slaveErr) {
		t.Fatalf("expected slave close error, got %v", err)
	}
	if !errors.Is(err, masterErr) {
		t.Fatalf("expected master close error, got %v", err)
	}
	if err := m.Stop(); err != nil {
		t.Fatalf("second Stop should be idempotent, got %v", err)
	}
}

// TestStartMasterInvalidAddr covers startMaster with an invalid
// listen address. Ported from coverage_boost_replication_test.go.
func TestStartMasterInvalidAddr(t *testing.T) {
	m := NewManager(DefaultConfig())
	m.config.ListenAddr = "invalid:address:format"
	if err := m.startMaster(); err == nil {
		t.Fatal("expected error for invalid listen address")
	}
}

// TestWALEntryDecodeInvalidDataLength covers Decode when the
// declared data length exceeds the buffer. Ported from
// coverage_boost_replication_test.go.
func TestWALEntryDecodeInvalidDataLength(t *testing.T) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint64(1))
	binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano()))
	binary.Write(buf, binary.BigEndian, uint32(1000000))
	buf.Write([]byte("short"))

	entry := &WALEntry{}
	if err := entry.Decode(buf.Bytes()); err == nil {
		t.Error("Expected error for invalid data length")
	}
}

// TestSendSnapshotLockedErrors covers sendSnapshotLocked's error
// paths: nil OnSnapshot, OnSnapshot error, OnSnapshot panic,
// snapshot too large, and various write/flush failures.
// Ported from coverage_boost_replication_test.go.
func TestSendSnapshotLockedErrors(t *testing.T) {
	slave := &SlaveConnection{
		Writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	m := NewManager(DefaultConfig())

	if err := m.sendSnapshotLocked(slave, 1); err == nil {
		t.Fatal("expected error when OnSnapshot is nil")
	}

	m.OnSnapshot = func() ([]byte, error) {
		return nil, errors.New("snapshot error")
	}
	if err := m.sendSnapshotLocked(slave, 1); err == nil {
		t.Fatal("expected error when OnSnapshot fails")
	}

	m.OnSnapshot = func() ([]byte, error) {
		panic("snapshot panic")
	}
	if err := m.sendSnapshotLocked(slave, 1); err == nil || !strings.Contains(err.Error(), "snapshot callback panic") {
		t.Fatalf("expected panic error when OnSnapshot panics, got %v", err)
	}

	m.OnSnapshot = func() ([]byte, error) {
		return make([]byte, maxReplicationSnapshotSize+1), nil
	}
	if err := m.sendSnapshotLocked(slave, 1); err == nil {
		t.Fatal("expected error when snapshot too large")
	}

	errWriter := &mockErrorWriter{err: errors.New("write error")}
	slave2 := &SlaveConnection{Writer: bufio.NewWriterSize(errWriter, 1)}
	m.OnSnapshot = func() ([]byte, error) { return []byte("x"), nil }
	if err := m.sendSnapshotLocked(slave2, 1); err == nil {
		t.Fatal("expected error on WriteString")
	}

	errWriter2 := &mockErrorWriter{err: errors.New("write error")}
	slave3 := &SlaveConnection{Writer: bufio.NewWriterSize(errWriter2, 16)}
	if err := m.sendSnapshotLocked(slave3, 1); err == nil {
		t.Fatal("expected error on Write")
	}

	errWriter3 := &mockErrorWriter{err: errors.New("write error")}
	slave4 := &SlaveConnection{Writer: bufio.NewWriterSize(errWriter3, 100)}
	if err := m.sendSnapshotLocked(slave4, 1); err == nil {
		t.Fatal("expected error on Flush")
	}
}
