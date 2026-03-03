package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// mockConn implements net.Conn for testing
type mockConn struct {
	readBuf  []byte
	writeBuf []byte
	closed   bool
	mu       sync.Mutex
}

func newMockConn() *mockConn {
	return &mockConn{
		readBuf:  make([]byte, 0),
		writeBuf: make([]byte, 0),
		closed:   false,
	}
}

func (m *mockConn) Read(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, io.EOF
	}
	if len(m.readBuf) == 0 {
		return 0, io.EOF
	}
	n = copy(p, m.readBuf)
	m.readBuf = m.readBuf[n:]
	return n, nil
}

func (m *mockConn) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	m.writeBuf = append(m.writeBuf, p...)
	return len(p), nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4201} }
func (m *mockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func (m *mockConn) setReadData(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readBuf = data
}

func (m *mockConn) getWrittenData() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeBuf
}

func TestRoleConstants(t *testing.T) {
	if RoleStandalone != 0 {
		t.Errorf("Expected RoleStandalone = 0, got %d", RoleStandalone)
	}
	if RoleMaster != 1 {
		t.Errorf("Expected RoleMaster = 1, got %d", RoleMaster)
	}
	if RoleSlave != 2 {
		t.Errorf("Expected RoleSlave = 2, got %d", RoleSlave)
	}
}

func TestStateConstants(t *testing.T) {
	if StateDisconnected != 0 {
		t.Errorf("Expected StateDisconnected = 0, got %d", StateDisconnected)
	}
	if StateConnecting != 1 {
		t.Errorf("Expected StateConnecting = 1, got %d", StateConnecting)
	}
	if StateConnected != 2 {
		t.Errorf("Expected StateConnected = 2, got %d", StateConnected)
	}
	if StateReplicating != 3 {
		t.Errorf("Expected StateReplicating = 3, got %d", StateReplicating)
	}
	if StateError != 4 {
		t.Errorf("Expected StateError = 4, got %d", StateError)
	}
}

func TestNewManagerWithNilConfig(t *testing.T) {
	walPath := fmt.Sprintf("test_wal_nil_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(nil, wal)
	if manager == nil {
		t.Fatal("Manager is nil")
	}

	// Should use default config
	if manager.config == nil {
		t.Error("Expected default config to be set")
	}

	if manager.role != RoleStandalone {
		t.Errorf("Expected role Standalone, got %d", manager.role)
	}
}

func TestPromoteNotSlave(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleStandalone

	walPath := fmt.Sprintf("test_wal_promote_standalone_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Promoting a standalone should fail
	err = manager.Promote()
	if err == nil {
		t.Error("Expected error when promoting standalone, got nil")
	}

	// Role should still be Standalone
	if manager.role != RoleStandalone {
		t.Errorf("Expected role Standalone, got %d", manager.role)
	}
}

func TestPromoteAlreadyMaster(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_promote_master_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Promoting a master should fail
	err = manager.Promote()
	if err == nil {
		t.Error("Expected error when promoting master, got nil")
	}
}

func TestSendHandshake(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_handshake_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	conn := newMockConn()
	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: nil,
		LSN:    0,
	}

	err = manager.sendHandshake(slave)
	if err != nil {
		t.Errorf("sendHandshake failed: %v", err)
	}

	// Check written data
	data := conn.getWrittenData()
	if len(data) != 8 {
		t.Errorf("Expected 8 bytes written, got %d", len(data))
	}

	// Check magic number
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != 0x434F4241 {
		t.Errorf("Expected magic 0x434F4241, got 0x%x", magic)
	}

	// Check version
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != 1 {
		t.Errorf("Expected version 1, got %d", version)
	}
}

func TestSlaveConnSetLSN(t *testing.T) {
	conn := newMockConn()
	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: nil,
		LSN:    0,
	}

	// Set LSN
	slave.mu.Lock()
	slave.LSN = 100
	slave.mu.Unlock()

	// Verify LSN
	slave.mu.Lock()
	lsn := slave.LSN
	slave.mu.Unlock()

	if lsn != 100 {
		t.Errorf("Expected LSN 100, got %d", lsn)
	}
}

func TestApplyEvent(t *testing.T) {
	config := DefaultConfig()

	walPath := fmt.Sprintf("test_wal_apply_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	event := &ReplicationEvent{
		LSN:  1,
		Type: storage.WALInsert,
		Data: []byte("test data"),
	}

	// Apply event
	err = manager.applyEvent(event)
	if err != nil {
		t.Errorf("applyEvent failed: %v", err)
	}

	// Event should be sent to channel
	select {
	case received := <-manager.eventChan:
		if received.Type != storage.WALInsert {
			t.Errorf("Expected type WALInsert, got %d", received.Type)
		}
		if string(received.Data) != "test data" {
			t.Errorf("Expected data 'test data', got %s", string(received.Data))
		}
	default:
		t.Error("Expected event in channel, but none received")
	}
}

func TestApplyEventWithStop(t *testing.T) {
	config := DefaultConfig()

	walPath := fmt.Sprintf("test_wal_apply_stop_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Close stop channel to simulate stop
	close(manager.stopChan)

	event := &ReplicationEvent{
		LSN:  1,
		Type: storage.WALInsert,
		Data: []byte("test data"),
	}

	// Apply event should not block even with stop
	err = manager.applyEvent(event)
	if err != nil {
		t.Errorf("applyEvent failed: %v", err)
	}
}

func TestSendWALRecords(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_send_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	conn := newMockConn()
	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: nil,
		LSN:    0,
	}

	// sendWALRecords is a placeholder, should return nil
	err = manager.sendWALRecords(slave)
	if err != nil {
		t.Errorf("sendWALRecords failed: %v", err)
	}
}

func TestConfigValues(t *testing.T) {
	config := DefaultConfig()

	if config.SyncTimeout != 30*time.Second {
		t.Errorf("Expected SyncTimeout 30s, got %v", config.SyncTimeout)
	}

	if config.ReconnectDelay != 5*time.Second {
		t.Errorf("Expected ReconnectDelay 5s, got %v", config.ReconnectDelay)
	}

	if config.BufferSize != 1000 {
		t.Errorf("Expected BufferSize 1000, got %d", config.BufferSize)
	}
}

func TestGetSlaveCountAsSlave(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := fmt.Sprintf("test_wal_slave_count_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Slave should always report 0 slaves
	count := manager.GetSlaveCount()
	if count != 0 {
		t.Errorf("Expected 0 slaves for slave role, got %d", count)
	}
}

func TestReplicationEventWithDifferentTypes(t *testing.T) {
	testCases := []struct {
		name string
		typ  storage.WALRecordType
	}{
		{"Insert", storage.WALInsert},
		{"Update", storage.WALUpdate},
		{"Delete", storage.WALDelete},
		{"Checkpoint", storage.WALCheckpoint},
		{"Commit", storage.WALCommit},
		{"Rollback", storage.WALRollback},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := &ReplicationEvent{
				LSN:  1,
				Type: tc.typ,
				Data: []byte("test"),
			}

			if event.Type != tc.typ {
				t.Errorf("Expected type %d, got %d", tc.typ, event.Type)
			}
		})
	}
}

func TestManagerConcurrentStateAccess(t *testing.T) {
	config := DefaultConfig()

	walPath := fmt.Sprintf("test_wal_concurrent_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = manager.GetState()
		}()
	}

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			manager.setState(ReplicationState(idx % 5))
		}(i)
	}

	wg.Wait()

	// Final state should be valid
	state := manager.GetState()
	if state < 0 || state > 4 {
		t.Errorf("Invalid final state: %d", state)
	}
}

func TestManagerConcurrentSlaveCount(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_concurrent_slave_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	var wg sync.WaitGroup
	numGoroutines := 50

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = manager.GetSlaveCount()
		}()
	}

	// Concurrent modifications (simulated)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			manager.slaveMu.Lock()
			// Just lock/unlock to test concurrency
			manager.slaveMu.Unlock()
		}()
	}

	wg.Wait()
}

func TestMockConnReadWrite(t *testing.T) {
	conn := newMockConn()

	// Test write
	data := []byte("hello world")
	n, err := conn.Write(data)
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected %d bytes written, got %d", len(data), n)
	}

	// Test read
	conn.setReadData(data)
	buf := make([]byte, len(data))
	n, err = conn.Read(buf)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected %d bytes read, got %d", len(data), n)
	}
	if string(buf) != string(data) {
		t.Errorf("Expected %s, got %s", string(data), string(buf))
	}
}

func TestMockConnClose(t *testing.T) {
	conn := newMockConn()

	// Close connection
	err := conn.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Read should return EOF
	buf := make([]byte, 10)
	_, err = conn.Read(buf)
	if err != io.EOF {
		t.Errorf("Expected EOF after close, got %v", err)
	}

	// Write should return error
	_, err = conn.Write([]byte("test"))
	if err != io.ErrClosedPipe {
		t.Errorf("Expected ErrClosedPipe after close, got %v", err)
	}
}

func TestMockConnAddrs(t *testing.T) {
	conn := newMockConn()

	local := conn.LocalAddr()
	if local == nil {
		t.Error("LocalAddr returned nil")
	}

	remote := conn.RemoteAddr()
	if remote == nil {
		t.Error("RemoteAddr returned nil")
	}
}

func TestMockConnDeadlines(t *testing.T) {
	conn := newMockConn()

	// These should not error
	if err := conn.SetDeadline(time.Now()); err != nil {
		t.Errorf("SetDeadline failed: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now()); err != nil {
		t.Errorf("SetReadDeadline failed: %v", err)
	}

	if err := conn.SetWriteDeadline(time.Now()); err != nil {
		t.Errorf("SetWriteDeadline failed: %v", err)
	}
}
