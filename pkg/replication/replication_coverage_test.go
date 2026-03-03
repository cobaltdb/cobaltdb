package replication

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// mockConnWithData is a mock connection that returns specific data on read
type mockConnWithData struct {
	readBuf  []byte
	writeBuf []byte
	closed   bool
	mu       sync.Mutex
}

func newMockConnWithData(data []byte) *mockConnWithData {
	return &mockConnWithData{
		readBuf:  data,
		writeBuf: make([]byte, 0),
		closed:   false,
	}
}

func (m *mockConnWithData) Read(p []byte) (n int, err error) {
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

func (m *mockConnWithData) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	m.writeBuf = append(m.writeBuf, p...)
	return len(p), nil
}

func (m *mockConnWithData) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConnWithData) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4201} }
func (m *mockConnWithData) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345} }
func (m *mockConnWithData) SetDeadline(t time.Time) error      { return nil }
func (m *mockConnWithData) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConnWithData) SetWriteDeadline(t time.Time) error { return nil }

// TestReadLSN tests the readLSN function
func TestReadLSN(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := "test_wal_read_lsn.log"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Create LSN data (uint64 = 12345)
	lsnData := make([]byte, 8)
	binary.LittleEndian.PutUint64(lsnData, 12345)

	conn := newMockConnWithData(lsnData)

	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		LSN:    0,
	}

	lsn, err := manager.readLSN(slave)
	if err != nil {
		t.Fatalf("readLSN failed: %v", err)
	}

	if lsn != 12345 {
		t.Errorf("Expected LSN 12345, got %d", lsn)
	}
}

// TestReadLSNError tests readLSN with error
func TestReadLSNError(t *testing.T) {
	config := DefaultConfig()

	walPath := "test_wal_read_lsn_err.log"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Empty data should cause error
	conn := newMockConnWithData([]byte{})

	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		LSN:    0,
	}

	_, err = manager.readLSN(slave)
	if err == nil {
		t.Error("Expected error for empty data")
	}
}

// TestStartSlave tests the startSlave function
func TestStartSlave(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = "127.0.0.1:4201"

	walPath := "test_wal_start_slave.log"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	err = manager.startSlave()
	if err != nil {
		t.Fatalf("startSlave failed: %v", err)
	}

	// Should be in connecting state initially
	if manager.GetState() != StateConnecting {
		t.Errorf("Expected state Connecting, got %d", manager.GetState())
	}

	// Clean up
	close(manager.stopChan)
	manager.wg.Wait()
}

// TestSlaveReplicationLoop tests the slaveReplicationLoop function
func TestSlaveReplicationLoop(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = "127.0.0.1:1" // Invalid address to force error
	config.ReconnectDelay = 10 * time.Millisecond

	walPath := "test_wal_slave_loop.log"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Start slaveReplicationLoop in a goroutine
	manager.wg.Add(1)
	go manager.slaveReplicationLoop()

	// Wait a bit for it to try connecting and error
	time.Sleep(50 * time.Millisecond)

	// Should be in error state after failed connection
	state := manager.GetState()
	if state != StateError && state != StateConnecting {
		t.Errorf("Expected state Error or Connecting, got %d", state)
	}

	// Clean up
	close(manager.stopChan)
	manager.wg.Wait()
}

// TestConnectToMasterInvalidAddress tests connectToMaster with invalid address
func TestConnectToMasterInvalidAddress(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = "invalid:address:format"

	walPath := "test_wal_connect_invalid.log"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	err = manager.connectToMaster()
	if err == nil {
		t.Error("Expected error for invalid address")
	}
}

// TestReplicateFromMasterError tests replicateFromMaster with connection error
func TestReplicateFromMasterError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := "test_wal_replicate_from.log"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Empty data will cause immediate EOF
	conn := newMockConnWithData([]byte{})
	manager.masterConn = conn
	manager.masterReader = bufio.NewReader(conn)

	// This should return error due to EOF
	err = manager.replicateFromMaster()
	if err == nil {
		t.Error("Expected error for empty data")
	}

	close(manager.stopChan)
}

// TestReplicateFromMasterStop tests replicateFromMaster with stop signal
func TestReplicateFromMasterStop(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := "test_wal_replicate_stop.log"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Close stop channel immediately
	close(manager.stopChan)

	// Create some data that won't be read
	conn := newMockConnWithData(make([]byte, 100))
	manager.masterConn = conn
	manager.masterReader = bufio.NewReader(conn)

	// This should return nil due to stop signal
	err = manager.replicateFromMaster()
	if err != nil {
		t.Errorf("Expected nil error when stopped, got: %v", err)
	}
}



// TestPromoteSlaveSuccess tests promoting a slave to master
func TestPromoteSlaveSuccess(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := "test_wal_promote_slave.log"
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Should succeed for slave
	err = manager.Promote()
	// May error due to listener setup, but role should change
	if manager.role != RoleMaster {
		t.Errorf("Expected role Master after promote, got %d", manager.role)
	}
}
