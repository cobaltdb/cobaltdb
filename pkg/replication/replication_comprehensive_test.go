package replication

import (
	"bufio"
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

// mockListener implements net.Listener for testing
type mockListener struct {
	acceptChan chan net.Conn
	closed     bool
	mu         sync.Mutex
	addr       net.Addr
	stopChan   chan struct{}
}

func newMockListener() *mockListener {
	return &mockListener{
		acceptChan: make(chan net.Conn, 10),
		addr:       &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4201},
		stopChan:   make(chan struct{}),
	}
}

func (m *mockListener) Accept() (net.Conn, error) {
	select {
	case conn, ok := <-m.acceptChan:
		if !ok {
			return nil, io.EOF
		}
		return conn, nil
	case <-m.stopChan:
		return nil, io.EOF
	}
}

func (m *mockListener) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.stopChan)
		close(m.acceptChan)
	}
	return nil
}

func (m *mockListener) Addr() net.Addr {
	return m.addr
}

func (m *mockListener) addConn(conn net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		select {
		case m.acceptChan <- conn:
		default:
		}
	}
}

// errorListener is a listener that always returns errors
type errorListener struct{}

func (e *errorListener) Accept() (net.Conn, error) { return nil, fmt.Errorf("accept error") }
func (e *errorListener) Close() error              { return nil }
func (e *errorListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4201}
}

// TestStartMasterListenerError tests startMaster with listener error
func TestStartMasterListenerError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster
	config.ReplicationPort = "invalid:port:format:too:many:colons"

	walPath := fmt.Sprintf("test_wal_master_err_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	err = manager.Start()
	if err == nil {
		t.Error("Expected error for invalid port, got nil")
	}
}

// TestAcceptSlaves tests the acceptSlaves function
func TestAcceptSlaves(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_accept_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	listener := newMockListener()

	// Add a mock connection first
	mockConn := newMockConn()
	listener.addConn(mockConn)

	// Start acceptSlaves
	manager.wg.Add(1)
	go manager.acceptSlaves(listener)

	// Wait for connection to be processed
	time.Sleep(200 * time.Millisecond)

	// Clean up - close listener first to interrupt Accept
	listener.Close()
	close(manager.stopChan)

	// Use timeout for wait
	done := make(chan struct{})
	go func() {
		manager.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test passes if acceptSlaves runs and exits cleanly
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for acceptSlaves to stop")
	}
}

// TestAcceptSlavesStop tests acceptSlaves with stop signal
func TestAcceptSlavesStop(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_accept_stop_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	listener := newMockListener()

	// Start acceptSlaves
	manager.wg.Add(1)
	go manager.acceptSlaves(listener)

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	// Stop by closing the listener (this interrupts Accept)
	listener.Close()
	close(manager.stopChan)

	// Use timeout for wait
	done := make(chan struct{})
	go func() {
		manager.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for acceptSlaves to stop")
	}
}

// TestHandleSlave tests handleSlave function
func TestHandleSlave(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_handle_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Create mock connection with LSN data
	conn := newMockConn()
	lsnData := make([]byte, 8)
	binary.LittleEndian.PutUint64(lsnData, 100)
	conn.setReadData(lsnData)

	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		LSN:    0,
	}

	manager.slaves["test-slave"] = slave

	// Start handleSlave
	manager.wg.Add(1)
	go manager.handleSlave(slave)

	// Wait for handshake to be sent
	time.Sleep(100 * time.Millisecond)

	// Verify handshake was sent
	written := conn.getWrittenData()

	// Clean up first to avoid hanging
	close(manager.stopChan)
	manager.wg.Wait()

	// Check after cleanup
	if len(written) < 8 {
		t.Errorf("Expected handshake (8 bytes), got %d bytes", len(written))
	}
}

// TestHandleSlaveReadLSNError tests handleSlave when readLSN fails
func TestHandleSlaveReadLSNError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_handle_lsn_err_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Create mock connection with incomplete LSN data
	conn := newMockConn()
	conn.setReadData([]byte{1, 2, 3}) // Only 3 bytes, need 8

	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		LSN:    0,
	}

	// Start handleSlave
	manager.wg.Add(1)
	go manager.handleSlave(slave)

	// Wait for it to process
	time.Sleep(50 * time.Millisecond)

	// Clean up
	close(manager.stopChan)
	manager.wg.Wait()
}

// TestReplicateToSlave tests replicateToSlave function
func TestReplicateToSlave(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_replicate_slave_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	conn := newMockConn()
	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		LSN:    0,
	}

	// Start replicateToSlave
	manager.wg.Add(1)
	go manager.replicateToSlave(slave)

	// Let it run briefly
	time.Sleep(150 * time.Millisecond)

	// Clean up
	close(manager.stopChan)
	manager.wg.Wait()
}

// TestConnectToMasterInvalidMagic tests connectToMaster with invalid magic
func TestConnectToMasterInvalidMagic(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = "127.0.0.1:4201"
	config.SyncTimeout = 100 * time.Millisecond

	walPath := fmt.Sprintf("test_wal_bad_magic_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Create a listener that sends invalid magic
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("Cannot create listener:", err)
	}
	defer listener.Close()

	// Update config to use actual port
	config.MasterAddr = listener.Addr().String()

	// Start server that sends bad magic
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Send invalid magic
		header := make([]byte, 8)
		binary.LittleEndian.PutUint32(header[0:4], 0xBADBAD) // Wrong magic
		binary.LittleEndian.PutUint32(header[4:8], 1)
		conn.Write(header)

		// Wait for client to close
		time.Sleep(100 * time.Millisecond)
	}()

	err = manager.connectToMaster()
	if err == nil {
		t.Error("Expected error for invalid magic, got nil")
	}
}

// TestConnectToMasterInvalidVersion tests connectToMaster with invalid version
func TestConnectToMasterInvalidVersion(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.SyncTimeout = 100 * time.Millisecond

	walPath := fmt.Sprintf("test_wal_bad_version_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Create a listener that sends invalid version
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("Cannot create listener:", err)
	}
	defer listener.Close()

	// Update config to use actual port
	config.MasterAddr = listener.Addr().String()

	// Start server that sends bad version
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Send valid magic but invalid version
		header := make([]byte, 8)
		binary.LittleEndian.PutUint32(header[0:4], 0x434F4241) // Correct magic
		binary.LittleEndian.PutUint32(header[4:8], 999)        // Wrong version
		conn.Write(header)

		// Wait for client to close
		time.Sleep(100 * time.Millisecond)
	}()

	err = manager.connectToMaster()
	if err == nil {
		t.Error("Expected error for invalid version, got nil")
	}
}

// TestConnectToMasterSuccess tests successful connectToMaster
func TestConnectToMasterSuccess(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave
	config.SyncTimeout = 1 * time.Second

	walPath := fmt.Sprintf("test_wal_connect_ok_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.currentLSN = 42

	// Create a listener that sends valid handshake
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("Cannot create listener:", err)
	}
	defer listener.Close()

	// Update config to use actual port
	config.MasterAddr = listener.Addr().String()

	receivedLSN := make(chan uint64, 1)

	// Start server that sends valid handshake and receives LSN
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Send valid handshake
		header := make([]byte, 8)
		binary.LittleEndian.PutUint32(header[0:4], 0x434F4241) // Correct magic
		binary.LittleEndian.PutUint32(header[4:8], 1)          // Version 1
		conn.Write(header)

		// Read LSN from client
		lsnBuf := make([]byte, 8)
		_, err = io.ReadFull(conn, lsnBuf)
		if err != nil {
			return
		}
		lsn := binary.LittleEndian.Uint64(lsnBuf)
		receivedLSN <- lsn
	}()

	err = manager.connectToMaster()
	if err != nil {
		t.Fatalf("connectToMaster failed: %v", err)
	}

	// Verify LSN was sent
	select {
	case lsn := <-receivedLSN:
		if lsn != 42 {
			t.Errorf("Expected LSN 42, got %d", lsn)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Timeout waiting for LSN")
	}

	// Verify connection was stored
	if manager.masterConn == nil {
		t.Error("Expected masterConn to be set")
	}
	if manager.masterReader == nil {
		t.Error("Expected masterReader to be set")
	}
}

// TestReplicateFromMasterReadRecord tests replicateFromMaster reading a record
func TestReplicateFromMasterReadRecord(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := fmt.Sprintf("test_wal_replicate_record_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Create WAL record data: [Type:1][DataLen:4][Data...]
	recordData := []byte("test record data")
	data := make([]byte, 1+4+len(recordData))
	data[0] = byte(storage.WALInsert)
	binary.LittleEndian.PutUint32(data[1:5], uint32(len(recordData)))
	copy(data[5:], recordData)

	conn := newMockConnWithData(data)
	manager.masterConn = conn
	manager.masterReader = bufio.NewReader(conn)

	// Start replicateFromMaster in goroutine
	done := make(chan error, 1)
	go func() {
		done <- manager.replicateFromMaster()
	}()

	// Wait for record to be processed
	time.Sleep(100 * time.Millisecond)

	// Stop replication
	close(manager.stopChan)

	// Wait for completion
	select {
	case err := <-done:
		// Expected - EOF after reading record
		_ = err
	case <-time.After(500 * time.Millisecond):
		t.Error("Timeout waiting for replicateFromMaster")
	}
}

// TestReplicateFromMasterReadTypeError tests replicateFromMaster with type read error
func TestReplicateFromMasterReadTypeError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := fmt.Sprintf("test_wal_replicate_type_err_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Empty data will cause EOF on type read
	conn := newMockConnWithData([]byte{})
	manager.masterConn = conn
	manager.masterReader = bufio.NewReader(conn)

	err = manager.replicateFromMaster()
	if err == nil {
		t.Error("Expected error for EOF on type read")
	}
}

// TestReplicateFromMasterReadLengthError tests replicateFromMaster with length read error
func TestReplicateFromMasterReadLengthError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := fmt.Sprintf("test_wal_replicate_len_err_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Only type byte, no length - will cause EOF on length read
	conn := newMockConnWithData([]byte{byte(storage.WALInsert)})
	manager.masterConn = conn
	manager.masterReader = bufio.NewReader(conn)

	err = manager.replicateFromMaster()
	if err == nil {
		t.Error("Expected error for EOF on length read")
	}
}

// TestReplicateFromMasterReadDataError tests replicateFromMaster with data read error
func TestReplicateFromMasterReadDataError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := fmt.Sprintf("test_wal_replicate_data_err_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Type + length saying 100 bytes, but only 2 bytes of data
	data := make([]byte, 1+4+2)
	data[0] = byte(storage.WALInsert)
	binary.LittleEndian.PutUint32(data[1:5], 100) // Expect 100 bytes
	data[5] = 1
	data[6] = 2

	conn := newMockConnWithData(data)
	manager.masterConn = conn
	manager.masterReader = bufio.NewReader(conn)

	err = manager.replicateFromMaster()
	if err == nil {
		t.Error("Expected error for incomplete data read")
	}
}

// TestReplicateFromMasterMultipleRecords tests replicateFromMaster with multiple records
func TestReplicateFromMasterMultipleRecords(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleSlave

	walPath := fmt.Sprintf("test_wal_replicate_multi_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)
	manager.stopChan = make(chan struct{})

	// Create two WAL records
	var allData []byte

	record1 := []byte("record 1")
	data1 := make([]byte, 1+4+len(record1))
	data1[0] = byte(storage.WALInsert)
	binary.LittleEndian.PutUint32(data1[1:5], uint32(len(record1)))
	copy(data1[5:], record1)
	allData = append(allData, data1...)

	record2 := []byte("record 2")
	data2 := make([]byte, 1+4+len(record2))
	data2[0] = byte(storage.WALUpdate)
	binary.LittleEndian.PutUint32(data2[1:5], uint32(len(record2)))
	copy(data2[5:], record2)
	allData = append(allData, data2...)

	conn := newMockConnWithData(allData)
	manager.masterConn = conn
	manager.masterReader = bufio.NewReader(conn)

	// Process records in goroutine
	done := make(chan error, 1)
	go func() {
		done <- manager.replicateFromMaster()
	}()

	// Wait for records to be processed
	time.Sleep(100 * time.Millisecond)

	// Stop replication
	close(manager.stopChan)

	select {
	case <-done:
		// Expected
	case <-time.After(500 * time.Millisecond):
		t.Error("Timeout waiting for replicateFromMaster")
	}

	// Verify LSN was incremented
	if manager.currentLSN != 2 {
		t.Errorf("Expected LSN 2 after 2 records, got %d", manager.currentLSN)
	}
}

// TestSendHandshakeError tests sendHandshake with write error
func TestSendHandshakeError(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleMaster

	walPath := fmt.Sprintf("test_wal_handshake_err_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Create closed connection
	conn := newMockConn()
	conn.Close()

	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: nil,
		LSN:    0,
	}

	err = manager.sendHandshake(slave)
	if err == nil {
		t.Error("Expected error for closed connection")
	}
}

// TestManagerFullLifecycle tests the full lifecycle of a manager
func TestManagerFullLifecycle(t *testing.T) {
	// Use standalone mode to avoid network listener issues
	config := DefaultConfig()
	config.Role = RoleStandalone

	walPath := fmt.Sprintf("test_wal_lifecycle_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	// Start (should succeed immediately for standalone)
	err = manager.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Stop
	err = manager.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify state
	if manager.GetState() != StateDisconnected {
		t.Errorf("Expected state Disconnected, got %d", manager.GetState())
	}
}

// TestSlaveConnConcurrentAccess tests concurrent access to SlaveConn
func TestSlaveConnConcurrentAccess(t *testing.T) {
	conn := newMockConn()
	slave := &SlaveConn{
		ID:     "test-slave",
		Conn:   conn,
		Reader: bufio.NewReader(conn),
		LSN:    0,
	}

	var wg sync.WaitGroup

	// Concurrent LSN reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slave.mu.Lock()
			_ = slave.LSN
			slave.mu.Unlock()
		}()
	}

	// Concurrent LSN writes
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(val uint64) {
			defer wg.Done()
			slave.mu.Lock()
			slave.LSN = val
			slave.mu.Unlock()
		}(uint64(i))
	}

	wg.Wait()
}

// TestManagerWithNilWAL tests manager with nil WAL
func TestManagerWithNilWAL(t *testing.T) {
	config := DefaultConfig()
	config.Role = RoleStandalone

	manager := NewManager(config, nil)
	if manager == nil {
		t.Fatal("Manager is nil")
	}

	if manager.wal != nil {
		t.Error("Expected nil WAL")
	}

	// Should still work
	err := manager.Start()
	if err != nil {
		t.Errorf("Start failed: %v", err)
	}

	err = manager.Stop()
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

// TestReplicationEventChannel tests the event channel
func TestReplicationEventChannel(t *testing.T) {
	config := DefaultConfig()
	config.BufferSize = 10

	manager := NewManager(config, nil)

	// Send multiple events
	for i := 0; i < 5; i++ {
		event := &ReplicationEvent{
			LSN:  uint64(i),
			Type: storage.WALInsert,
			Data: []byte(fmt.Sprintf("data %d", i)),
		}
		manager.eventChan <- event
	}

	// Read events back
	for i := 0; i < 5; i++ {
		select {
		case event := <-manager.eventChan:
			if event.LSN != uint64(i) {
				t.Errorf("Expected LSN %d, got %d", i, event.LSN)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Timeout waiting for event %d", i)
		}
	}
}

// TestAllRoles tests all role types
func TestAllRoles(t *testing.T) {
	roles := []Role{RoleStandalone, RoleMaster, RoleSlave}
	roleNames := []string{"Standalone", "Master", "Slave"}

	for i, role := range roles {
		t.Run(roleNames[i], func(t *testing.T) {
			config := DefaultConfig()
			config.Role = role

			walPath := fmt.Sprintf("test_wal_role_%s_%d.log", roleNames[i], os.Getpid())
			wal, err := storage.OpenWAL(walPath)
			if err != nil {
				t.Skip("Cannot create WAL:", err)
			}
			defer wal.Close()
			defer os.Remove(walPath)

			manager := NewManager(config, wal)
			if manager.role != role {
				t.Errorf("Expected role %d, got %d", role, manager.role)
			}
		})
	}
}

// TestAllStates tests all state types
func TestAllStates(t *testing.T) {
	states := []ReplicationState{
		StateDisconnected,
		StateConnecting,
		StateConnected,
		StateReplicating,
		StateError,
	}
	stateNames := []string{"Disconnected", "Connecting", "Connected", "Replicating", "Error"}

	config := DefaultConfig()

	walPath := fmt.Sprintf("test_wal_states_%d.log", os.Getpid())
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Skip("Cannot create WAL:", err)
	}
	defer wal.Close()
	defer os.Remove(walPath)

	manager := NewManager(config, wal)

	for i, state := range states {
		t.Run(stateNames[i], func(t *testing.T) {
			manager.setState(state)
			if manager.GetState() != state {
				t.Errorf("Expected state %d, got %d", state, manager.GetState())
			}
		})
	}
}
