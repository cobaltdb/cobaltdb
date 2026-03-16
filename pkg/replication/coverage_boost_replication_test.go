package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// TestWALEntryEncodeEmptyData tests encoding with empty data
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

	// Verify we can decode it back
	decoded := &WALEntry{}
	if err := decoded.Decode(encoded); err != nil {
		t.Fatalf("Failed to decode empty data entry: %v", err)
	}

	if len(decoded.Data) != 0 {
		t.Errorf("Expected empty data, got %d bytes", len(decoded.Data))
	}
}

// TestWALEntryEncodeLargeData tests encoding with large data
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

// TestWALEntryEncodeZeroValues tests encoding with zero values
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

// TestEncodeWALEntriesSingle tests encoding a single entry
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

// TestEncodeWALEntriesMany tests encoding many entries
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
		t.Fatalf("Failed to decode many entries: %v", err)
	}

	if len(decoded) != 50 {
		t.Errorf("Expected 50 decoded entries, got %d", len(decoded))
	}
}

// TestStartSlaveConnectionFailure tests startSlave with invalid master address
func TestStartSlaveConnectionFailure(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = "127.0.0.1:1" // Invalid port, should fail

	mgr := NewManager(config)
	err := mgr.Start()
	if err == nil {
		mgr.Stop()
		t.Error("Expected error when connecting to invalid master address")
	}
}

// TestStartSlaveInvalidAddress tests startSlave with malformed address
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

// TestSendInitialSnapshotError tests sendInitialSnapshot with a failing writer
func TestSendInitialSnapshotError(t *testing.T) {
	// Create a mock writer that fails
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

// TestHandleMasterMessageSTART tests handleMasterMessage with START message
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

// TestHandleMasterMessagePING tests handleMasterMessage with PING message
func TestHandleMasterMessagePING(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	mgr.masterConn = &mockConn{}
	mgr.lastApplied = 999

	err := mgr.handleMasterMessage("PING 0\n")
	if err != nil {
		t.Errorf("handleMasterMessage PING failed: %v", err)
	}
}

// TestHandleMasterMessageInvalid tests handleMasterMessage with invalid messages
func TestHandleMasterMessageInvalid(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	mgr.masterConn = &mockConn{}

	tests := []struct {
		name    string
		msg     string
		wantErr bool
	}{
		{
			name:    "too short",
			msg:     "AB\n",
			wantErr: true,
		},
		{
			name:    "exactly 4 chars but unknown",
			msg:     "XXXX 123\n",
			wantErr: false, // treated as WAL data, may fail at applyWALData
		},
		{
			name:    "empty message",
			msg:     "",
			wantErr: true,
		},
		{
			name:    "START without LSN",
			msg:     "START\n",
			wantErr: false, // sscanf will parse what it can
		},
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

// TestReplicateFromMasterStop tests replicateFromMaster stops on stopCh
func TestReplicateFromMasterStop(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	mgr := NewManager(config)
	mgr.stopCh = make(chan struct{})

	// Create a pipe so we have a valid connection
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	mgr.masterConn = client

	// Start replicateFromMaster
	mgr.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.replicateFromMaster()
	}()

	// Close stopCh to trigger shutdown
	close(mgr.stopCh)

	// Wait for goroutine to finish
	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("replicateFromMaster did not stop in time")
	}
}

// TestReplicateFromMasterReadError tests replicateFromMaster handles read errors
func TestReplicateFromMasterReadError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	mgr := NewManager(config)

	// Create a pipe and close it to simulate read error
	client, server := net.Pipe()
	server.Close() // Close server end to cause read error on client

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

// TestSendHeartbeatError tests sendHeartbeat error handling
func TestSendHeartbeatError(t *testing.T) {
	// Create a mock writer that fails
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

// TestAuthenticateSlaveFailure tests authenticateSlave with wrong token
func TestAuthenticateSlaveFailure(t *testing.T) {
	config := DefaultConfig()
	config.AuthToken = "correct_token"

	mgr := NewManager(config)

	// Create a pipe
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Write wrong token from client side
	go func() {
		client.Write([]byte("wrong_token\n"))
		// Read response
		buf := make([]byte, 100)
		client.Read(buf)
	}()

	err := mgr.authenticateSlave(server)
	if err == nil {
		t.Error("Expected authentication error with wrong token")
	}
}

// TestAuthenticateSlaveReadError tests authenticateSlave with read error
func TestAuthenticateSlaveReadError(t *testing.T) {
	config := DefaultConfig()
	config.AuthToken = "token"

	mgr := NewManager(config)

	// Create a pipe and close client immediately
	client, server := net.Pipe()
	client.Close()

	err := mgr.authenticateSlave(server)
	if err == nil {
		t.Error("Expected error when client disconnects during auth")
	}
}

// TestSyncWALWithSlaves tests syncWAL with connected slaves
func TestSyncWALWithSlaves(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	// Add some WAL entries
	for i := 0; i < 3; i++ {
		mgr.ReplicateWALEntry([]byte("test data"))
	}

	// Create mock slave connection with failing writer
	mw := &mockWriter{failAfter: 0}

	slave := &SlaveConnection{
		ID:       "test-slave",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  0,
		LastPing: time.Now(),
	}

	mgr.slaves["test-slave"] = slave

	// Call replicateWAL - should handle error gracefully
	mgr.replicateWAL()

	// Test passes if no panic
}

// TestSendWALToSlaveError tests sendWALToSlave with write errors
func TestSendWALToSlaveError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	// Create a mock writer that fails
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

// TestReplicateWALNoSlaves tests replicateWAL with no slaves
func TestReplicateWALNoSlaves(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	// Add WAL entries
	mgr.ReplicateWALEntry([]byte("test"))

	// Call replicateWAL with no slaves - should return early
	mgr.replicateWAL()

	// Test passes if no panic
}

// TestReplicateWALNoEntries tests replicateWAL with no entries
func TestReplicateWALNoEntries(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	// Create mock slave
	mw := &mockWriter{failAfter: 0}

	slave := &SlaveConnection{
		ID:       "test",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  0,
		LastPing: time.Now(),
	}

	mgr.slaves["test"] = slave

	// Call replicateWAL with no entries - should return early
	mgr.replicateWAL()

	// Test passes if no panic
}

// TestWaitForSlavesWithLaggingSlave tests WaitForSlaves with a lagging slave
func TestWaitForSlavesWithLaggingSlave(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.Mode = ModeSync

	mgr := NewManager(config)
	mgr.currentLSN = 100

	// Add a lagging slave
	slave := &SlaveConnection{
		ID:       "lagging",
		LastLSN:  50, // Behind currentLSN
		LastPing: time.Now(),
	}
	mgr.slaves["lagging"] = slave

	// Should timeout because slave is behind
	err := mgr.WaitForSlaves(100 * time.Millisecond)
	if err == nil {
		t.Error("Expected timeout error with lagging slave")
	}
}

// TestWaitForSlavesCaughtUp tests WaitForSlaves when slave is caught up
func TestWaitForSlavesCaughtUp(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.Mode = ModeSync

	mgr := NewManager(config)
	mgr.currentLSN = 100

	// Add a caught-up slave
	slave := &SlaveConnection{
		ID:       "caughtup",
		LastLSN:  100,
		LastPing: time.Now(),
	}
	mgr.slaves["caughtup"] = slave

	// Should succeed immediately
	err := mgr.WaitForSlaves(1 * time.Second)
	if err != nil {
		t.Errorf("Expected no error when slave is caught up: %v", err)
	}
}

// TestHandleSlaveWithAuth tests handleSlave with authentication
func TestHandleSlaveWithAuth(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.AuthToken = "secret"

	mgr := NewManager(config)
	mgr.stopCh = make(chan struct{})

	// Create listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	// Connect with wrong auth
	go func() {
		conn, _ := net.Dial("tcp", listener.Addr().String())
		if conn != nil {
			conn.Write([]byte("wrong_secret\n"))
			conn.Close()
		}
	}()

	// Accept and handle
	conn, _ := listener.Accept()
	if conn != nil {
		mgr.handleSlave(conn)
	}

	// Slave should not be added due to auth failure
	if len(mgr.slaves) != 0 {
		t.Error("Expected no slaves after auth failure")
	}
}

// TestGetStatusMasterWithMultipleSlaves tests GetStatus with multiple slaves
func TestGetStatusMasterWithMultipleSlaves(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.Mode = ModeSync

	mgr := NewManager(config)
	mgr.currentLSN = 500

	// Add multiple slaves
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

// TestGetStatusModeVariations tests GetStatus with different modes
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

// TestApplyWALDataEmptyEntries tests applyWALData with empty entries
func TestApplyWALDataEmptyEntries(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})

	// Encode empty entries
	data, _ := encodeWALEntries([]*WALEntry{})

	err := mgr.applyWALData(string(data))
	if err != nil {
		t.Errorf("applyWALData with empty entries failed: %v", err)
	}
}

// TestApplyWALDataWithCallbackError tests applyWALData when callback returns error
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

// mockConn is a mock net.Conn for testing
type mockConn struct {
	net.Conn
	writeData []byte
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// mockWriter is a writer that fails after a certain number of writes
type mockWriter struct {
	writeCount int
	failAfter  int
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	if m.writeCount >= m.failAfter {
		return 0, errors.New("mock write error")
	}
	m.writeCount++
	return len(p), nil
}

// TestWALEntryDecodeInvalidDataLength tests Decode with invalid data length
func TestWALEntryDecodeInvalidDataLength(t *testing.T) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint64(1))                    // LSN
	binary.Write(buf, binary.BigEndian, int64(time.Now().UnixNano())) // Timestamp
	binary.Write(buf, binary.BigEndian, uint32(1000000))              // Data length too large
	buf.Write([]byte("short"))                                        // Not enough data

	entry := &WALEntry{}
	err := entry.Decode(buf.Bytes())
	if err == nil {
		t.Error("Expected error for invalid data length")
	}
}

// TestHandleMasterMessageWALData tests handleMasterMessage with WAL data
func TestHandleMasterMessageWALData(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave})
	mgr.masterConn = &mockConn{}

	// Create valid WAL entries
	entries := []*WALEntry{
		{LSN: 1, Timestamp: time.Now(), Data: []byte("wal data"), Checksum: calculateCRC32([]byte("wal data"))},
	}
	data, _ := encodeWALEntries(entries)

	// This will be treated as WAL data (not START or PING)
	err := mgr.handleMasterMessage(string(data))
	// May succeed or fail depending on data format, but should not panic
	t.Logf("handleMasterMessage with WAL data returned: %v", err)
}

// TestReplicateWALWithOnLagCallback tests replicateWAL with OnLag callback
func TestReplicateWALWithOnLagCallback(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	// Add WAL entries
	mgr.ReplicateWALEntry([]byte("test"))

	// Create a mock slave that will fail to receive data
	mw := &mockWriter{failAfter: 0}

	slave := &SlaveConnection{
		ID:       "lagging-slave",
		Writer:   bufio.NewWriter(mw),
		LastLSN:  0,
		LastPing: time.Now().Add(-time.Hour), // Very old ping
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

// TestStartSlaveAuthWriteError tests startSlave auth write error
func TestStartSlaveAuthWriteError(t *testing.T) {
	// Start a master that will close connection immediately
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			conn.Close() // Close immediately
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

// TestAcceptSlavesStop tests acceptSlaves stops on stopCh
func TestAcceptSlavesStop(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)
	mgr.stopCh = make(chan struct{})

	// Create a listener
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

	// Close stopCh to trigger shutdown
	close(mgr.stopCh)

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("acceptSlaves did not stop in time")
	}
}

// TestSyncWALStop tests syncWAL stops on stopCh
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

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Close stopCh to trigger shutdown
	close(mgr.stopCh)

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("syncWAL did not stop in time")
	}
}

// TestSendInitialSnapshotSuccess tests sendInitialSnapshot success
func TestSendInitialSnapshotSuccess(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	// Create a mock writer that succeeds
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

// TestSendInitialSnapshotFlushError tests sendInitialSnapshot flush error
func TestSendInitialSnapshotFlushError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	mgr := NewManager(config)

	// Create a mock writer that fails on first write
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
