package replication

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// Role represents the role of a node in replication
type Role int

const (
	RoleStandalone Role = iota
	RoleMaster
	RoleSlave
)

// ReplicationState represents the current replication state
type ReplicationState int

const (
	StateDisconnected ReplicationState = iota
	StateConnecting
	StateConnected
	StateReplicating
	StateError
)

// Config contains replication configuration
type Config struct {
	Role            Role
	MasterAddr      string
	ReplicationPort string
	SyncTimeout     time.Duration
	ReconnectDelay  time.Duration
	BufferSize      int
}

// DefaultConfig returns default replication configuration
func DefaultConfig() *Config {
	return &Config{
		Role:            RoleStandalone,
		MasterAddr:      "",
		ReplicationPort: ":4201",
		SyncTimeout:     30 * time.Second,
		ReconnectDelay:  5 * time.Second,
		BufferSize:      1000,
	}
}

// ReplicationEvent represents a replication event
type ReplicationEvent struct {
	LSN  uint64
	Type storage.WALRecordType
	Data []byte
}

// Manager handles replication
type Manager struct {
	config  *Config
	role    Role
	state   ReplicationState
	stateMu sync.RWMutex

	// Master fields
	listeners []net.Listener
	slaves    map[string]*SlaveConn
	slaveMu   sync.RWMutex
	wal       *storage.WAL

	// Slave fields
	masterConn   net.Conn
	masterReader *bufio.Reader
	currentLSN   uint64

	// Channels
	eventChan chan *ReplicationEvent
	stopChan  chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

// SlaveConn represents a connected slave
type SlaveConn struct {
	ID     string
	Conn   net.Conn
	Reader *bufio.Reader
	LSN    uint64
	mu     sync.Mutex
}

// NewManager creates a new replication manager
func NewManager(config *Config, wal *storage.WAL) *Manager {
	if config == nil {
		config = DefaultConfig()
	}

	return &Manager{
		config:    config,
		role:      config.Role,
		state:     StateDisconnected,
		slaves:    make(map[string]*SlaveConn),
		wal:       wal,
		eventChan: make(chan *ReplicationEvent, config.BufferSize),
		stopChan:  make(chan struct{}),
	}
}

// Start starts the replication manager
func (m *Manager) Start() error {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	switch m.role {
	case RoleMaster:
		return m.startMaster()
	case RoleSlave:
		return m.startSlave()
	default:
		return nil // Standalone mode, nothing to do
	}
}

// Stop stops the replication manager
func (m *Manager) Stop() error {
	m.stopOnce.Do(func() { close(m.stopChan) })
	m.wg.Wait()

	m.slaveMu.Lock()
	for _, slave := range m.slaves {
		slave.Conn.Close()
	}
	m.slaves = make(map[string]*SlaveConn)
	m.slaveMu.Unlock()

	for _, listener := range m.listeners {
		listener.Close()
	}
	m.listeners = nil

	if m.masterConn != nil {
		m.masterConn.Close()
		m.masterConn = nil
	}

	m.setState(StateDisconnected)
	return nil
}

// startMaster starts the master replication server
func (m *Manager) startMaster() error {
	listener, err := net.Listen("tcp", m.config.ReplicationPort)
	if err != nil {
		return fmt.Errorf("failed to start replication listener: %w", err)
	}

	m.listeners = append(m.listeners, listener)
	// Set state directly since this is called with lock held
	m.state = StateConnected

	m.wg.Add(1)
	go m.acceptSlaves(listener)

	return nil
}

// acceptSlaves accepts incoming slave connections
func (m *Manager) acceptSlaves(listener net.Listener) {
	defer m.wg.Done()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-m.stopChan:
				return
			default:
				continue
			}
		}

		slaveID := conn.RemoteAddr().String()
		slave := &SlaveConn{
			ID:     slaveID,
			Conn:   conn,
			Reader: bufio.NewReader(conn),
			LSN:    0,
		}

		m.slaveMu.Lock()
		m.slaves[slaveID] = slave
		m.slaveMu.Unlock()

		m.wg.Add(1)
		go m.handleSlave(slave)
	}
}

// handleSlave handles a slave connection
func (m *Manager) handleSlave(slave *SlaveConn) {
	defer m.wg.Done()
	defer func() {
		slave.Conn.Close()
		m.slaveMu.Lock()
		delete(m.slaves, slave.ID)
		m.slaveMu.Unlock()
	}()

	// Send initial handshake
	if err := m.sendHandshake(slave); err != nil {
		return
	}

	// Read slave's current LSN
	lsn, err := m.readLSN(slave)
	if err != nil {
		return
	}

	slave.mu.Lock()
	slave.LSN = lsn
	slave.mu.Unlock()

	// Start replicating WAL records
	m.wg.Add(1)
	go m.replicateToSlave(slave)

	// Wait for stop
	<-m.stopChan
}

// sendHandshake sends the initial handshake to a slave
func (m *Manager) sendHandshake(slave *SlaveConn) error {
	// Simple protocol: send magic number and version
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], 0x434F4241) // "COBA" magic
	binary.LittleEndian.PutUint32(header[4:8], 1)          // Version 1
	_, err := slave.Conn.Write(header)
	return err
}

// readLSN reads the slave's current LSN
func (m *Manager) readLSN(slave *SlaveConn) (uint64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(slave.Reader, buf); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

// replicateToSlave replicates WAL records to a slave
func (m *Manager) replicateToSlave(slave *SlaveConn) {
	defer m.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			// Check for new WAL records and send them
			if err := m.sendWALRecords(slave); err != nil {
				return // Error sending, close connection
			}
		}
	}
}

// sendWALRecords sends new WAL records to a slave
func (m *Manager) sendWALRecords(slave *SlaveConn) error {
	slave.mu.Lock()
	defer slave.mu.Unlock()

	// In a real implementation, we would read from the WAL file
	// and send records newer than slave.LSN
	// For now, this is a placeholder

	return nil
}

// startSlave starts the slave replication client
func (m *Manager) startSlave() error {
	m.setState(StateConnecting)

	m.wg.Add(1)
	go m.slaveReplicationLoop()

	return nil
}

// slaveReplicationLoop maintains the connection to the master
func (m *Manager) slaveReplicationLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.stopChan:
			return
		default:
		}

		if err := m.connectToMaster(); err != nil {
			m.setState(StateError)
			time.Sleep(m.config.ReconnectDelay)
			continue
		}

		m.setState(StateReplicating)

		// Replicate until error or stop
		if err := m.replicateFromMaster(); err != nil {
			m.setState(StateError)
			if m.masterConn != nil {
				m.masterConn.Close()
				m.masterConn = nil
			}
			time.Sleep(m.config.ReconnectDelay)
		}
	}
}

// connectToMaster connects to the master server
func (m *Manager) connectToMaster() error {
	conn, err := net.DialTimeout("tcp", m.config.MasterAddr, m.config.SyncTimeout)
	if err != nil {
		return err
	}

	// Read handshake
	header := make([]byte, 8)
	if _, err := io.ReadFull(conn, header); err != nil {
		conn.Close()
		return err
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	version := binary.LittleEndian.Uint32(header[4:8])

	if magic != 0x434F4241 {
		conn.Close()
		return fmt.Errorf("invalid handshake magic: %x", magic)
	}

	if version != 1 {
		conn.Close()
		return fmt.Errorf("unsupported protocol version: %d", version)
	}

	// Send our current LSN
	lsnBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lsnBuf, m.currentLSN)
	if _, err := conn.Write(lsnBuf); err != nil {
		conn.Close()
		return err
	}

	m.masterConn = conn
	m.masterReader = bufio.NewReader(conn)

	return nil
}

// replicateFromMaster replicates data from the master
func (m *Manager) replicateFromMaster() error {
	for {
		select {
		case <-m.stopChan:
			return nil
		default:
		}

		// Read WAL record from master
		// Format: [Type:1][DataLen:4][Data...]
		typeBuf := make([]byte, 1)
		if _, err := io.ReadFull(m.masterReader, typeBuf); err != nil {
			return err
		}

		recordType := storage.WALRecordType(typeBuf[0])

		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(m.masterReader, lenBuf); err != nil {
			return err
		}
		dataLen := binary.LittleEndian.Uint32(lenBuf)

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(m.masterReader, data); err != nil {
			return err
		}

		// Apply the replication event
		event := &ReplicationEvent{
			Type: recordType,
			Data: data,
		}

		if err := m.applyEvent(event); err != nil {
			return err
		}

		m.currentLSN++
	}
}

// applyEvent applies a replication event to the local database
func (m *Manager) applyEvent(event *ReplicationEvent) error {
	// In a real implementation, this would apply the WAL record
	// to the local database
	// For now, just send to event channel
	select {
	case m.eventChan <- event:
	case <-m.stopChan:
	}
	return nil
}

// GetState returns the current replication state
func (m *Manager) GetState() ReplicationState {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	return m.state
}

// setState sets the replication state
func (m *Manager) setState(state ReplicationState) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.state = state
}

// GetSlaveCount returns the number of connected slaves (master only)
func (m *Manager) GetSlaveCount() int {
	if m.role != RoleMaster {
		return 0
	}

	m.slaveMu.RLock()
	defer m.slaveMu.RUnlock()
	return len(m.slaves)
}

// Promote promotes a slave to master
func (m *Manager) Promote() error {
	if m.role != RoleSlave {
		return fmt.Errorf("only slaves can be promoted")
	}

	// Stop slave operations
	if err := m.Stop(); err != nil {
		return err
	}

	// Change role
	m.role = RoleMaster
	m.config.Role = RoleMaster

	// Start master operations
	return m.Start()
}
