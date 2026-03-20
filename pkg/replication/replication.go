// Package replication provides Master-Slave replication for CobaltDB
package replication

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ReplicationMode defines how replication operates
type ReplicationMode int

const (
	// ModeAsync replicates asynchronously (best performance, some lag)
	ModeAsync ReplicationMode = iota
	// ModeSync waits for at least one slave to acknowledge
	ModeSync
	// ModeFullSync waits for all slaves to acknowledge (slowest, safest)
	ModeFullSync
)

// Role defines the role of a node in replication
type Role int

const (
	// RoleStandalone means no replication
	RoleStandalone Role = iota
	// RoleMaster accepts writes and replicates to slaves
	RoleMaster
	// RoleSlave receives changes from master
	RoleSlave
)

// Config holds replication configuration
type Config struct {
	Role           Role
	Mode           ReplicationMode
	ListenAddr     string              // For master: address to listen on
	MasterAddr     string              // For slave: master address to connect
	Slaves         []string            // For master: list of slave addresses
	MaxLag         time.Duration       // Maximum allowed replication lag
	SyncInterval   time.Duration       // How often to sync WAL
	AuthToken      string              // Authentication token
	Compress       bool                // Compress replication stream
	SSLCert        string              // SSL certificate file
	SSLKey         string              // SSL key file
	SSLCA          string              // SSL CA certificate
}

// DefaultConfig returns default replication configuration
func DefaultConfig() *Config {
	return &Config{
		Role:         RoleStandalone,
		Mode:         ModeAsync,
		SyncInterval: 100 * time.Millisecond,
		MaxLag:       30 * time.Second,
		Compress:     true,
	}
}

// WALEntry represents a single WAL entry for replication
type WALEntry struct {
	LSN       uint64    // Log Sequence Number
	Timestamp time.Time
	Data      []byte    // Serialized operation
	Checksum  uint32    // CRC32 checksum
}

// Encode serializes the WAL entry
func (e *WALEntry) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write LSN
	if err := binary.Write(buf, binary.BigEndian, e.LSN); err != nil {
		return nil, err
	}

	// Write timestamp
	ts := e.Timestamp.UnixNano()
	if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
		return nil, err
	}

	// Write data length and data
	if err := binary.Write(buf, binary.BigEndian, uint32(len(e.Data))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(e.Data); err != nil {
		return nil, err
	}

	// Write checksum
	if err := binary.Write(buf, binary.BigEndian, e.Checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode deserializes a WAL entry
func (e *WALEntry) Decode(data []byte) error {
	buf := bytes.NewReader(data)

	// Read LSN
	if err := binary.Read(buf, binary.BigEndian, &e.LSN); err != nil {
		return err
	}

	// Read timestamp
	var ts int64
	if err := binary.Read(buf, binary.BigEndian, &ts); err != nil {
		return err
	}
	e.Timestamp = time.Unix(0, ts)

	// Read data length and data
	var dataLen uint32
	if err := binary.Read(buf, binary.BigEndian, &dataLen); err != nil {
		return err
	}
	e.Data = make([]byte, dataLen)
	if _, err := io.ReadFull(buf, e.Data); err != nil {
		return err
	}

	// Read checksum
	if err := binary.Read(buf, binary.BigEndian, &e.Checksum); err != nil {
		return err
	}

	return nil
}

// Manager handles replication logic
type Manager struct {
	config *Config
	role   Role

	// Master fields
	mu          sync.RWMutex
	slaves      map[string]*SlaveConnection
	slaveWALPos map[string]uint64
	walBuffer   []*WALEntry
	currentLSN  uint64
	listener    net.Listener

	// Slave fields
	masterConn   net.Conn
	lastApplied  uint64
	replicaWAL   []*WALEntry

	// Common fields
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	metrics  *Metrics

	// Callbacks
	OnApply    func(entry *WALEntry) error
	OnLag      func(slave string, lag time.Duration)
	OnDisconnect func(slave string, err error)
}

// SlaveConnection represents a connection to a slave
type SlaveConnection struct {
	ID       string
	Conn     net.Conn
	Writer   *bufio.Writer
	Reader   *bufio.Reader
	LastLSN  uint64
	LastPing time.Time
	mu       sync.Mutex
}

// Metrics holds replication metrics
type Metrics struct {
	ReplicatedBytes   uint64
	AppliedEntries    uint64
	ActiveSlaves      int32
	ReplicationLag    int64 // in milliseconds
	LastAppliedTime   int64 // Unix timestamp
}

// NewManager creates a new replication manager
func NewManager(config *Config) *Manager {
	return &Manager{
		config:      config,
		role:        config.Role,
		slaves:      make(map[string]*SlaveConnection),
		slaveWALPos: make(map[string]uint64),
		walBuffer:   make([]*WALEntry, 0),
		stopCh:      make(chan struct{}),
		metrics:     &Metrics{},
	}
}

// Start begins replication
func (m *Manager) Start() error {
	switch m.config.Role {
	case RoleMaster:
		return m.startMaster()
	case RoleSlave:
		return m.startSlave()
	default:
		return nil // Standalone mode, nothing to do
	}
}

// Stop gracefully shuts down replication
func (m *Manager) Stop() error {
	m.stopOnce.Do(func() { close(m.stopCh) })

	// Close listener first to unblock acceptSlaves()
	if m.listener != nil {
		m.listener.Close()
	}

	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, slave := range m.slaves {
		slave.Conn.Close()
	}

	if m.masterConn != nil {
		m.masterConn.Close()
	}

	return nil
}

// startMaster initializes master replication
func (m *Manager) startMaster() error {
	// Start listening for slave connections
	listener, err := net.Listen("tcp", m.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to start replication listener: %w", err)
	}
	m.listener = listener

	m.wg.Add(1)
	go m.acceptSlaves()

	// Start WAL sync goroutine
	m.wg.Add(1)
	go m.syncWAL()

	return nil
}

// acceptSlaves accepts incoming slave connections
func (m *Manager) acceptSlaves() {
	defer m.wg.Done()

	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		conn, err := m.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				return // Listener closed
			}
			continue
		}

		go m.handleSlave(conn)
	}
}

// handleSlave handles a single slave connection
func (m *Manager) handleSlave(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			// Log panic but don't crash the master
			_ = r
		}
	}()

	slaveID := conn.RemoteAddr().String()

	// Authenticate if needed
	if m.config.AuthToken != "" {
		if err := m.authenticateSlave(conn); err != nil {
			conn.Close()
			return
		}
	}

	slave := &SlaveConnection{
		ID:       slaveID,
		Conn:     conn,
		Writer:   bufio.NewWriter(conn),
		Reader:   bufio.NewReader(conn),
		LastPing: time.Now(),
	}

	m.mu.Lock()
	m.slaves[slaveID] = slave
	atomic.AddInt32(&m.metrics.ActiveSlaves, 1)
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.slaves, slaveID)
		atomic.AddInt32(&m.metrics.ActiveSlaves, -1)
		m.mu.Unlock()
		conn.Close()

		if m.OnDisconnect != nil {
			m.OnDisconnect(slaveID, nil)
		}
	}()

	// Send initial WAL position
	m.mu.RLock()
	startLSN := m.currentLSN
	m.mu.RUnlock()

	if err := m.sendInitialSnapshot(slave, startLSN); err != nil {
		return
	}

	// Keep connection alive and handle heartbeats
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			if err := m.sendHeartbeat(slave); err != nil {
				return
			}
		}
	}
}

// authenticateSlave authenticates a slave connection
func (m *Manager) authenticateSlave(conn net.Conn) error {
	// Simple token-based auth
	reader := bufio.NewReader(conn)
	token, err := reader.ReadString('\n')
	if err != nil {
		return err
	}

	token = token[:len(token)-1] // Remove newline
	if token != m.config.AuthToken {
		conn.Write([]byte("AUTH_FAILED\n"))
		return fmt.Errorf("authentication failed")
	}

	_, err = conn.Write([]byte("AUTH_OK\n"))
	return err
}

// sendInitialSnapshot sends current database state to a new slave
func (m *Manager) sendInitialSnapshot(slave *SlaveConnection, startLSN uint64) error {
	// Send START message with LSN
	msg := fmt.Sprintf("START %d\n", startLSN)
	if _, err := slave.Writer.WriteString(msg); err != nil {
		return err
	}
	return slave.Writer.Flush()
}

// sendHeartbeat sends a heartbeat to a slave
func (m *Manager) sendHeartbeat(slave *SlaveConnection) error {
	slave.mu.Lock()
	defer slave.mu.Unlock()

	msg := fmt.Sprintf("PING %d\n", slave.LastLSN)
	if _, err := slave.Writer.WriteString(msg); err != nil {
		return err
	}
	return slave.Writer.Flush()
}

// syncWAL periodically syncs WAL entries to slaves
func (m *Manager) syncWAL() {
	defer m.wg.Done()

	syncInterval := m.config.SyncInterval
	if syncInterval <= 0 {
		syncInterval = 100 * time.Millisecond // Default value
	}

	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.replicateWAL()
		}
	}
}

// replicateWAL sends buffered WAL entries to all connected slaves
func (m *Manager) replicateWAL() {
	m.mu.RLock()
	entries := make([]*WALEntry, len(m.walBuffer))
	copy(entries, m.walBuffer)
	slaves := make([]*SlaveConnection, 0, len(m.slaves))
	for _, s := range m.slaves {
		slaves = append(slaves, s)
	}
	m.mu.RUnlock()

	if len(entries) == 0 || len(slaves) == 0 {
		return
	}

	// Encode entries
	data, err := encodeWALEntries(entries)
	if err != nil {
		return
	}

	// Send to all slaves
	for _, slave := range slaves {
		if err := m.sendWALToSlave(slave, data); err != nil {
			// Mark slave as lagging
			if m.OnLag != nil {
				m.OnLag(slave.ID, time.Since(slave.LastPing))
			}
		}
	}
}

// sendWALToSlave sends WAL data to a specific slave
func (m *Manager) sendWALToSlave(slave *SlaveConnection, data []byte) error {
	slave.mu.Lock()
	defer slave.mu.Unlock()

	// Send WAL data
	if err := binary.Write(slave.Writer, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err := slave.Writer.Write(data); err != nil {
		return err
	}

	if err := slave.Writer.Flush(); err != nil {
		return err
	}

	// Update slave's LSN
	slave.LastLSN = m.currentLSN
	slave.LastPing = time.Now()

	atomic.AddUint64(&m.metrics.ReplicatedBytes, uint64(len(data)))

	return nil
}

// startSlave initializes slave replication
func (m *Manager) startSlave() error {
	// Connect to master
	conn, err := net.Dial("tcp", m.config.MasterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	m.masterConn = conn

	// Authenticate
	if m.config.AuthToken != "" {
		if _, err := fmt.Fprintf(conn, "%s\n", m.config.AuthToken); err != nil {
			conn.Close()
			return err
		}

		// Read auth response
		reader := bufio.NewReader(conn)
		response, err := reader.ReadString('\n')
		if err != nil {
			conn.Close()
			return err
		}

		if response != "AUTH_OK\n" {
			conn.Close()
			return fmt.Errorf("authentication failed")
		}
	}

	// Start replication goroutine
	m.wg.Add(1)
	go m.replicateFromMaster()

	return nil
}

// replicateFromMaster handles replication stream from master
func (m *Manager) replicateFromMaster() {
	defer m.wg.Done()

	reader := bufio.NewReader(m.masterConn)

	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		// Read message
		line, err := reader.ReadString('\n')
		if err != nil {
			if m.OnDisconnect != nil {
				m.OnDisconnect("master", err)
			}
			return
		}

		// Handle message
		if err := m.handleMasterMessage(line); err != nil {
			continue
		}
	}
}

// handleMasterMessage processes messages from master
func (m *Manager) handleMasterMessage(msg string) error {
	// Parse message type
	if len(msg) < 4 {
		return fmt.Errorf("invalid message")
	}

	type_ := msg[:4]

	switch type_ {
	case "STAR":
		// START <LSN>
		var lsn uint64
		fmt.Sscanf(msg, "START %d", &lsn)
		m.lastApplied = lsn

	case "PING":
		// Heartbeat - respond with current position
		if _, err := fmt.Fprintf(m.masterConn, "PONG %d\n", m.lastApplied); err != nil {
			return err
		}

	default:
		// WAL data
		return m.applyWALData(msg)
	}

	return nil
}

// applyWALData applies WAL data received from master
func (m *Manager) applyWALData(data string) error {
	// Decode entries
	entries, err := decodeWALEntries([]byte(data))
	if err != nil {
		return err
	}

	// Apply each entry
	for _, entry := range entries {
		if m.OnApply != nil {
			if err := m.OnApply(entry); err != nil {
				return err
			}
		}

		m.lastApplied = entry.LSN
		atomic.AddUint64(&m.metrics.AppliedEntries, 1)
	}

	atomic.StoreInt64(&m.metrics.LastAppliedTime, time.Now().Unix())

	return nil
}

// ReplicateWALEntry adds a WAL entry for replication (called by master)
func (m *Manager) ReplicateWALEntry(data []byte) error {
	if m.config.Role != RoleMaster {
		return nil // Not a master, ignore
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry := &WALEntry{
		LSN:       atomic.AddUint64(&m.currentLSN, 1),
		Timestamp: time.Now(),
		Data:      data,
		Checksum:  calculateCRC32(data),
	}

	m.walBuffer = append(m.walBuffer, entry)

	return nil
}

// GetMetrics returns current replication metrics
func (m *Manager) GetMetrics() *Metrics {
	return &Metrics{
		ReplicatedBytes: atomic.LoadUint64(&m.metrics.ReplicatedBytes),
		AppliedEntries:  atomic.LoadUint64(&m.metrics.AppliedEntries),
		ActiveSlaves:    atomic.LoadInt32(&m.metrics.ActiveSlaves),
		ReplicationLag:  atomic.LoadInt64(&m.metrics.ReplicationLag),
	}
}

// WaitForSlaves blocks until all connected slaves have caught up
func (m *Manager) WaitForSlaves(timeout time.Duration) error {
	if m.config.Mode == ModeAsync {
		return nil // Async mode doesn't wait
	}

	done := make(chan struct{})
	go func() {
		for {
			m.mu.RLock()
			allCaughtUp := true
			for _, slave := range m.slaves {
				if slave.LastLSN < m.currentLSN {
					allCaughtUp = false
					break
				}
			}
			m.mu.RUnlock()

			if allCaughtUp {
				close(done)
				return
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for slaves")
	}
}

// Helper functions

func encodeWALEntries(entries []*WALEntry) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write number of entries
	if err := binary.Write(buf, binary.BigEndian, uint32(len(entries))); err != nil {
		return nil, err
	}

	// Write each entry
	for _, entry := range entries {
		data, err := entry.Encode()
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.BigEndian, uint32(len(data))); err != nil {
			return nil, err
		}

		if _, err := buf.Write(data); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func decodeWALEntries(data []byte) ([]*WALEntry, error) {
	buf := bytes.NewReader(data)

	// Read number of entries
	var numEntries uint32
	if err := binary.Read(buf, binary.BigEndian, &numEntries); err != nil {
		return nil, err
	}

	entries := make([]*WALEntry, numEntries)

	for i := uint32(0); i < numEntries; i++ {
		// Read entry length
		var entryLen uint32
		if err := binary.Read(buf, binary.BigEndian, &entryLen); err != nil {
			return nil, err
		}

		// Read entry data
		entryData := make([]byte, entryLen)
		if _, err := io.ReadFull(buf, entryData); err != nil {
			return nil, err
		}

		// Decode entry
		entry := &WALEntry{}
		if err := entry.Decode(entryData); err != nil {
			return nil, err
		}

		entries[i] = entry
	}

	return entries, nil
}

func calculateCRC32(data []byte) uint32 {
	// Simple CRC32 implementation (use hash/crc32 in production)
	var crc uint32 = 0xFFFFFFFF
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xEDB88320
			} else {
				crc >>= 1
			}
		}
	}
	return ^crc
}

// JSON helpers for metadata

type ReplicationStatus struct {
	Role          string            `json:"role"`
	Mode          string            `json:"mode"`
	Connected     bool              `json:"connected"`
	ActiveSlaves  int               `json:"active_slaves,omitempty"`
	LastApplied   uint64            `json:"last_applied_lsn"`
	CurrentMaster uint64            `json:"current_master_lsn,omitempty"`
	Slaves        []SlaveStatus     `json:"slaves,omitempty"`
	Lag           time.Duration     `json:"replication_lag,omitempty"`
}

type SlaveStatus struct {
	ID       string        `json:"id"`
	LastLSN  uint64        `json:"last_lsn"`
	LastPing time.Time     `json:"last_ping"`
	Lag      time.Duration `json:"lag"`
}

// GetStatus returns current replication status
func (m *Manager) GetStatus() *ReplicationStatus {
	status := &ReplicationStatus{
		LastApplied: m.lastApplied,
	}

	switch m.config.Role {
	case RoleMaster:
		status.Role = "master"
		status.Mode = replicationModeString(m.config.Mode)

		m.mu.RLock()
		status.ActiveSlaves = len(m.slaves)
		status.CurrentMaster = m.currentLSN
		status.Slaves = make([]SlaveStatus, 0, len(m.slaves))
		for id, slave := range m.slaves {
			lag := time.Since(slave.LastPing)
			status.Slaves = append(status.Slaves, SlaveStatus{
				ID:       id,
				LastLSN:  slave.LastLSN,
				LastPing: slave.LastPing,
				Lag:      lag,
			})
		}
		m.mu.RUnlock()

	case RoleSlave:
		status.Role = "slave"
		status.Connected = m.masterConn != nil
	}

	return status
}

func replicationModeString(mode ReplicationMode) string {
	switch mode {
	case ModeAsync:
		return "async"
	case ModeSync:
		return "sync"
	case ModeFullSync:
		return "full_sync"
	default:
		return "unknown"
	}
}
