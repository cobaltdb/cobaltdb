// Package replication provides Master-Slave replication for CobaltDB
package replication

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
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

func replicationUint32Len(n int, name string) (uint32, error) {
	if n < 0 || n > 1<<32-1 {
		return 0, fmt.Errorf("%s exceeds uint32: %d", name, n)
	}
	return uint32(n), nil // #nosec G115 - range checked above.
}

const maxReplicationFrameSize = 64 << 20 // 64 MiB

const defaultMaxWALBufferEntries = 10000
const defaultMaxWALBufferBytes int64 = 64 << 20 // 64 MiB
const replicationAuthTimeout = 5 * time.Second
const replicationReadTimeout = 30 * time.Second
const replicationWriteTimeout = 5 * time.Second
const resumeHandshakeTimeout = 5 * time.Second
const walEntryMetadataBytes = 24 // LSN + timestamp + data length + checksum
const maxWALEntryDataBytes = maxReplicationFrameSize - walEntryMetadataBytes
const maxReplicationSnapshotSize = 1 << 30 // 1 GiB
const maxReplicationControlLineBytes = 4096
const maxReplicationStateFileBytes = 4096
const replicationStateDirPerm = 0750
const replicationStateFilePerm = 0600

// ErrAutomaticFailoverUnsupported is returned by APIs that would require
// consensus, fencing, or safe promotion semantics that this replication
// transport intentionally does not provide.
var ErrAutomaticFailoverUnsupported = errors.New("automatic failover is not supported: consensus, fencing, and safe promotion are not implemented")

var replicationDial = net.DialTimeout

// ErrPromotionRejected is returned when an externally orchestrated promotion
// request does not provide the fencing and freshness guarantees required to
// avoid split-brain.
var ErrPromotionRejected = errors.New("promotion rejected")

// ErrPrimaryFenced is returned when a fenced master is asked to accept new WAL.
var ErrPrimaryFenced = errors.New("primary is fenced")

// Config holds replication configuration
type Config struct {
	Role                Role
	Mode                ReplicationMode
	ListenAddr          string        // For master: address to listen on
	MasterAddr          string        // For slave: master address to connect
	Slaves              []string      // For master: list of slave addresses
	MaxLag              time.Duration // Maximum allowed replication lag
	SyncInterval        time.Duration // How often to sync WAL
	MaxWALBufferEntries int           // Maximum master WAL entries retained for disconnected/lagging slaves
	MaxWALBufferBytes   int64         // Maximum encoded master WAL bytes retained for disconnected/lagging slaves
	AuthToken           string        // Authentication token
	Compress            bool          // Compress replication stream
	SSLCert             string        // SSL certificate file
	SSLKey              string        // SSL key file
	SSLCA               string        // SSL CA certificate
	StateFile           string        // Optional slave state file for last applied LSN
}

// DefaultConfig returns default replication configuration
func DefaultConfig() *Config {
	return &Config{
		Role:                RoleStandalone,
		Mode:                ModeAsync,
		SyncInterval:        100 * time.Millisecond,
		MaxLag:              30 * time.Second,
		MaxWALBufferEntries: defaultMaxWALBufferEntries,
		MaxWALBufferBytes:   defaultMaxWALBufferBytes,
		Compress:            true,
	}
}

// WALEntry represents a single WAL entry for replication
type WALEntry struct {
	LSN       uint64 // Log Sequence Number
	Timestamp time.Time
	Data      []byte // Serialized operation
	Checksum  uint32 // CRC32 checksum
}

// Encode serializes the WAL entry
func (e *WALEntry) Encode() ([]byte, error) {
	if len(e.Data) > maxWALEntryDataBytes {
		return nil, fmt.Errorf("WAL entry data too large: %d bytes (max %d)", len(e.Data), maxWALEntryDataBytes)
	}

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
	dataLen, err := replicationUint32Len(len(e.Data), "WAL entry data length")
	if err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, dataLen); err != nil {
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

	remaining := buf.Len()
	if remaining < 4 {
		return fmt.Errorf("WAL entry truncated before checksum")
	}
	if dataLen > maxWALEntryDataBytes {
		return fmt.Errorf("WAL entry data too large: %d bytes (max %d)", dataLen, maxWALEntryDataBytes)
	}
	if uint64(dataLen) > uint64(remaining-4) {
		return fmt.Errorf("WAL entry data length %d exceeds remaining payload %d", dataLen, remaining-4)
	}

	e.Data = make([]byte, int(dataLen))
	if _, err := io.ReadFull(buf, e.Data); err != nil {
		return err
	}

	// Read checksum
	if err := binary.Read(buf, binary.BigEndian, &e.Checksum); err != nil {
		return err
	}
	if buf.Len() != 0 {
		return fmt.Errorf("WAL entry contains trailing data: %d bytes", buf.Len())
	}

	return nil
}

// Manager handles replication logic
type Manager struct {
	config *Config
	role   Role

	// Master fields
	mu             sync.RWMutex
	slaves         map[string]*SlaveConnection
	slaveWALPos    map[string]uint64
	walBuffer      []*WALEntry
	walBufferBytes int64
	currentLSN     uint64
	listener       net.Listener

	// Slave fields
	masterConn      net.Conn
	lastApplied     uint64
	requireSnapshot uint32

	// Common fields
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	metrics  *Metrics
	// promotionEpoch is non-zero after a successful externally fenced
	// promotion. It is intentionally local state; CobaltDB still does not run
	// consensus or leader election.
	promotionEpoch uint64
	fencedEpoch    uint64

	// Callbacks
	OnApply func(entry *WALEntry) error
	// OnSnapshot returns the current database state and the WAL LSN at the moment
	// the snapshot was captured. Both must be returned atomically — the caller captures
	// the LSN BEFORE calling this function, so the implementation must return the LSN
	// that corresponds to the data it returns. Returning the wrong LSN causes slaves
	// to double-apply or miss writes.
	OnSnapshot      func() (data []byte, lsn uint64, err error)
	OnApplySnapshot func(data []byte, lsn uint64) error
	OnLag           func(slave string, lag time.Duration)
	OnDisconnect    func(slave string, err error)
}

// SlaveConnection represents a connection to a slave
type SlaveConnection struct {
	ID            string
	Conn          net.Conn
	Writer        *bufio.Writer
	Reader        *bufio.Reader
	LastLSN       uint64
	LastPing      time.Time
	NeedsSnapshot bool
	mu            sync.Mutex
}

// Metrics holds replication metrics
type Metrics struct {
	ReplicatedBytes uint64
	AppliedEntries  uint64
	ActiveSlaves    int32
	ReplicationLag  int64 // in milliseconds
	LastAppliedTime int64 // Unix timestamp
}

type replicationState struct {
	LastApplied     uint64    `json:"last_applied"`
	RequireSnapshot bool      `json:"require_snapshot,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// PromotionRequest is the contract an external orchestrator must satisfy before
// CobaltDB will perform a manual slave-to-master transition.
type PromotionRequest struct {
	FencingToken       string
	Epoch              uint64
	OldPrimaryFenced   bool
	ExpiresAt          time.Time
	RequiredLSN        uint64
	AllowConnectedPeer bool
}

// PrimaryFenceRequest marks a master as fenced by an external HA control plane.
// A fenced master refuses new WAL replication entries.
type PrimaryFenceRequest struct {
	FencingToken string
	Epoch        uint64
	ExpiresAt    time.Time
}

// RejoinRequest converts a fenced old primary into a replica of the new
// externally selected master.
type RejoinRequest struct {
	FencingToken    string
	Epoch           uint64
	NewMasterAddr   string
	LastAppliedLSN  uint64
	RequireSnapshot bool
}

type resumeRequest struct {
	LSN             uint64
	RequireSnapshot bool
}

func (m *Manager) callOnSnapshot() (data []byte, lsn uint64, err error) {
	if m.OnSnapshot == nil {
		return nil, 0, fmt.Errorf("snapshot provider not configured")
	}
	defer func() {
		if r := recover(); r != nil {
			data = nil
			lsn = 0
			err = fmt.Errorf("replication snapshot callback panic: %v", r)
		}
	}()
	return m.OnSnapshot()
}

func (m *Manager) callOnApplySnapshot(data []byte, lsn uint64) (err error) {
	if m.OnApplySnapshot == nil {
		return fmt.Errorf("snapshot applier not configured")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("replication apply snapshot callback panic: %v", r)
		}
	}()
	return m.OnApplySnapshot(data, lsn)
}

func (m *Manager) callOnApply(entry *WALEntry) (err error) {
	if m.OnApply == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("replication apply callback panic: %v", r)
		}
	}()
	return m.OnApply(entry)
}

func (m *Manager) callOnLag(slave string, lag time.Duration) {
	if m.OnLag == nil {
		return
	}
	defer func() { _ = recover() }()
	m.OnLag(slave, lag)
}

func (m *Manager) callOnDisconnect(peer string, err error) {
	if m.OnDisconnect == nil {
		return
	}
	defer func() { _ = recover() }()
	m.OnDisconnect(peer, err)
}

func (m *Manager) setMasterConn(conn net.Conn) {
	m.mu.Lock()
	m.masterConn = conn
	m.mu.Unlock()
}

func (m *Manager) getMasterConn() net.Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterConn
}

func (m *Manager) closeMasterConn() {
	m.mu.Lock()
	conn := m.masterConn
	m.masterConn = nil
	m.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
}

// NewManager creates a new replication manager
func NewManager(config *Config) *Manager {
	if config == nil {
		config = DefaultConfig()
	} else {
		config = normalizeConfig(config)
	}

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

func normalizeConfig(config *Config) *Config {
	defaults := DefaultConfig()
	normalized := *config
	normalized.Slaves = append([]string(nil), config.Slaves...)
	if normalized.MaxLag <= 0 {
		normalized.MaxLag = defaults.MaxLag
	}
	if normalized.SyncInterval <= 0 {
		normalized.SyncInterval = defaults.SyncInterval
	}
	if normalized.MaxWALBufferEntries <= 0 {
		normalized.MaxWALBufferEntries = defaultMaxWALBufferEntries
	}
	if normalized.MaxWALBufferBytes <= 0 {
		normalized.MaxWALBufferBytes = defaultMaxWALBufferBytes
	}
	return &normalized
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

	var errs []error

	// Close listener first to unblock acceptSlaves()
	if m.listener != nil {
		if err := m.listener.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close replication listener: %w", err))
		}
		m.listener = nil
	}

	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, slave := range m.slaves {
		if slave.Conn != nil {
			if err := slave.Conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close slave %s: %w", id, err))
			}
		}
		delete(m.slaves, id)
	}

	if m.masterConn != nil {
		if err := m.masterConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close master connection: %w", err))
		}
		m.masterConn = nil
	}

	return errors.Join(errs...)
}

// startMaster initializes master replication
func (m *Manager) startMaster() error {
	if m.config.AuthToken == "" && replicationListenAddressRequiresAuth(m.config.ListenAddr) {
		return fmt.Errorf("replication auth token is required for non-loopback listen address %q", m.config.ListenAddr)
	}

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

func replicationListenAddressRequiresAuth(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return true
	}
	return !ip.IsLoopback()
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

		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.handleSlave(conn)
		}()
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

	slave := &SlaveConnection{
		ID:       slaveID,
		Conn:     conn,
		Writer:   bufio.NewWriter(conn),
		Reader:   bufio.NewReader(conn),
		LastPing: time.Now(),
	}

	// Authenticate if needed
	if m.config.AuthToken != "" {
		if err := m.authenticateSlaveWithReader(slave.Reader, conn); err != nil {
			_ = conn.Close()
			return
		}
	}

	resumeReq, err := m.receiveResumeRequest(slave)
	if err != nil {
		_ = conn.Close()
		return
	}
	if err := m.prepareSlaveResumeRequest(slave, resumeReq); err != nil {
		_ = conn.Close()
		return
	}

	m.mu.Lock()
	m.slaves[slaveID] = slave
	atomic.AddInt32(&m.metrics.ActiveSlaves, 1)
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.slaves, slaveID)
		atomic.AddInt32(&m.metrics.ActiveSlaves, -1)
		m.pruneWALBufferLocked()
		m.mu.Unlock()
		_ = conn.Close()

		m.callOnDisconnect(slaveID, nil)
	}()

	if err := m.sendInitialSnapshot(slave, resumeReq.LSN); err != nil {
		return
	}

	ackDone := make(chan struct{})
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.readSlaveAcks(slave)
		close(ackDone)
	}()

	// Keep connection alive and handle heartbeats
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ackDone:
			return
		case <-ticker.C:
			if err := m.sendHeartbeat(slave); err != nil {
				return
			}
		}
	}
}

func (m *Manager) receiveResumeRequest(slave *SlaveConnection) (resumeRequest, error) {
	if slave.Conn != nil {
		if err := slave.Conn.SetReadDeadline(time.Now().Add(resumeHandshakeTimeout)); err != nil {
			return resumeRequest{}, err
		}
		defer func() { _ = slave.Conn.SetReadDeadline(time.Time{}) }()
	}

	line, err := readReplicationControlLine(slave.Reader)
	if err != nil {
		return resumeRequest{}, err
	}

	if strings.HasPrefix(line, "RESUME_SNAPSHOT ") {
		var lsn uint64
		if _, err := fmt.Sscanf(line, "RESUME_SNAPSHOT %d", &lsn); err != nil {
			return resumeRequest{}, fmt.Errorf("invalid RESUME_SNAPSHOT message: %w", err)
		}
		return resumeRequest{LSN: lsn, RequireSnapshot: true}, nil
	}

	var lsn uint64
	if _, err := fmt.Sscanf(line, "RESUME %d", &lsn); err != nil {
		return resumeRequest{}, fmt.Errorf("invalid RESUME message: %w", err)
	}
	return resumeRequest{LSN: lsn}, nil
}

func (m *Manager) prepareSlaveResume(slave *SlaveConnection, requestedLSN uint64) error {
	return m.prepareSlaveResumeRequest(slave, resumeRequest{LSN: requestedLSN})
}

func (m *Manager) prepareSlaveResumeRequest(slave *SlaveConnection, req resumeRequest) error {
	requestedLSN := req.LSN
	currentLSN := atomic.LoadUint64(&m.currentLSN)
	if req.RequireSnapshot {
		if m.prepareSlaveSnapshot(slave, currentLSN) {
			return nil
		}
		_ = m.sendResyncRequired(slave, currentLSN)
		return fmt.Errorf("slave requested snapshot refresh but snapshot provider is not configured")
	}

	if requestedLSN > currentLSN {
		if m.prepareSlaveSnapshot(slave, currentLSN) {
			return nil
		}
		_ = m.sendResyncRequired(slave, currentLSN)
		return fmt.Errorf("slave requested future LSN %d, current LSN %d", requestedLSN, currentLSN)
	}

	m.mu.RLock()
	canResume := m.canResumeFromLocked(requestedLSN, currentLSN)
	m.mu.RUnlock()

	if !canResume {
		if m.prepareSlaveSnapshot(slave, currentLSN) {
			return nil
		}
		_ = m.sendResyncRequired(slave, currentLSN)
		return fmt.Errorf("slave requested LSN %d outside retained WAL window", requestedLSN)
	}

	slave.mu.Lock()
	slave.LastLSN = requestedLSN
	slave.LastPing = time.Now()
	slave.mu.Unlock()

	return nil
}

func (m *Manager) prepareSlaveSnapshot(slave *SlaveConnection, currentLSN uint64) bool {
	if m.OnSnapshot == nil {
		return false
	}

	slave.mu.Lock()
	slave.LastLSN = 0
	slave.LastPing = time.Now()
	slave.NeedsSnapshot = true
	slave.mu.Unlock()

	return true
}

func (m *Manager) canResumeFromLocked(requestedLSN, currentLSN uint64) bool {
	if requestedLSN == currentLSN {
		return true
	}
	if requestedLSN > currentLSN {
		return false
	}
	if len(m.walBuffer) == 0 {
		return false
	}

	firstRetained := m.walBuffer[0].LSN
	lastRetained := m.walBuffer[len(m.walBuffer)-1].LSN
	return requestedLSN+1 >= firstRetained && currentLSN <= lastRetained
}

func (m *Manager) sendResyncRequired(slave *SlaveConnection, currentLSN uint64) error {
	slave.mu.Lock()
	defer slave.mu.Unlock()

	if _, err := writeReplicationFull(slave.Writer, []byte(fmt.Sprintf("RESYNC %d\n", currentLSN))); err != nil {
		return err
	}
	return slave.Writer.Flush()
}

// authenticateSlave authenticates a slave connection
//
//lint:ignore U1000 used by coverage tests
func (m *Manager) authenticateSlave(conn net.Conn) error {
	return m.authenticateSlaveWithReader(bufio.NewReader(conn), conn)
}

func (m *Manager) authenticateSlaveWithReader(reader *bufio.Reader, conn net.Conn) error {
	if conn != nil {
		if err := conn.SetReadDeadline(time.Now().Add(replicationAuthTimeout)); err != nil {
			return err
		}
		defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	}

	// Simple token-based auth
	token, err := readReplicationControlLine(reader)
	if err != nil {
		return err
	}

	token = strings.TrimSuffix(token, "\n")
	token = strings.TrimSuffix(token, "\r")
	if !replicationAuthTokenEqual(token, m.config.AuthToken) {
		if _, writeErr := writeReplicationFull(conn, []byte("AUTH_FAILED\n")); writeErr != nil {
			return fmt.Errorf("authentication failed: %w", writeErr)
		}
		return fmt.Errorf("authentication failed")
	}

	_, err = writeReplicationFull(conn, []byte("AUTH_OK\n"))
	return err
}

func replicationAuthTokenEqual(provided, expected string) bool {
	providedDigest := replicationAuthTokenDigest(provided)
	expectedDigest := replicationAuthTokenDigest(expected)
	return subtle.ConstantTimeCompare(providedDigest[:], expectedDigest[:]) == 1
}

func replicationAuthTokenDigest(token string) [sha256.Size]byte {
	var lengthPrefix [8]byte
	binary.BigEndian.PutUint64(lengthPrefix[:], uint64(len(token)))

	hasher := sha256.New()
	_, _ = hasher.Write(lengthPrefix[:])
	_, _ = hasher.Write([]byte(token))

	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

func writeReplicationFull(writer io.Writer, data []byte) (int, error) {
	n, err := writer.Write(data)
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

// sendInitialSnapshot sends current database state to a new slave
func (m *Manager) sendInitialSnapshot(slave *SlaveConnection, startLSN uint64) error {
	slave.mu.Lock()
	defer slave.mu.Unlock()

	if slave.NeedsSnapshot {
		return m.sendSnapshotLocked(slave)
	}

	clearDeadline, err := setReplicationWriteDeadline(slave.Conn)
	if err != nil {
		return err
	}
	defer clearDeadline()

	// Send START message with LSN
	msg := fmt.Sprintf("START %d\n", startLSN)
	if _, err := writeReplicationFull(slave.Writer, []byte(msg)); err != nil {
		return err
	}
	if err := slave.Writer.Flush(); err != nil {
		return err
	}
	slave.LastLSN = startLSN
	slave.LastPing = time.Now()
	return nil
}

func (m *Manager) sendSnapshotLocked(slave *SlaveConnection) error {
	if m.OnSnapshot == nil {
		return fmt.Errorf("snapshot provider not configured")
	}

	// The LSN returned by callOnSnapshot is captured inside db.mu — it is the LSN at
	// the moment the snapshot content was taken. This eliminates the race where the
	// caller samples the LSN before calling the snapshot function (a concurrent write
	// between sampling and content capture made the snapshot newer than its label).
	data, snapshotLSN, err := m.callOnSnapshot()
	if err != nil {
		return fmt.Errorf("failed to create replication snapshot: %w", err)
	}
	if len(data) > maxReplicationSnapshotSize {
		return fmt.Errorf("replication snapshot too large: %d bytes", len(data))
	}

	clearDeadline, err := setReplicationWriteDeadline(slave.Conn)
	if err != nil {
		return err
	}
	defer clearDeadline()

	if _, err := writeReplicationFull(slave.Writer, []byte(fmt.Sprintf("SNAPSHOT %d %d\n", snapshotLSN, len(data)))); err != nil {
		return err
	}
	if _, err := writeReplicationFull(slave.Writer, data); err != nil {
		return err
	}
	if err := slave.Writer.Flush(); err != nil {
		return err
	}

	slave.LastLSN = snapshotLSN
	slave.LastPing = time.Now()
	slave.NeedsSnapshot = false
	atomic.AddUint64(&m.metrics.ReplicatedBytes, uint64(len(data)))
	return nil
}

// sendHeartbeat sends a heartbeat to a slave
func (m *Manager) sendHeartbeat(slave *SlaveConnection) error {
	slave.mu.Lock()
	defer slave.mu.Unlock()

	clearDeadline, err := setReplicationWriteDeadline(slave.Conn)
	if err != nil {
		return err
	}
	defer clearDeadline()

	msg := fmt.Sprintf("PING %d\n", slave.LastLSN)
	if _, err := writeReplicationFull(slave.Writer, []byte(msg)); err != nil {
		return err
	}
	return slave.Writer.Flush()
}

// readSlaveAcks receives ACK/PONG messages from a slave and updates the
// master's view of the slave's applied LSN.
func (m *Manager) readSlaveAcks(slave *SlaveConnection) {
	for {
		line, err := readReplicationControlLine(slave.Reader)
		if err != nil {
			return
		}

		var lsn uint64
		if _, err := fmt.Sscanf(line, "ACK %d", &lsn); err != nil {
			if _, err := fmt.Sscanf(line, "PONG %d", &lsn); err != nil {
				continue
			}
		}

		slave.mu.Lock()
		if lsn > slave.LastLSN {
			slave.LastLSN = lsn
		}
		slave.LastPing = time.Now()
		slave.mu.Unlock()

		m.pruneWALBuffer()
	}
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

	// Send to all slaves
	for _, slave := range slaves {
		slave.mu.Lock()
		lastLSN := slave.LastLSN
		slave.mu.Unlock()

		pending := filterWALEntriesAfter(entries, lastLSN)
		if len(pending) == 0 {
			continue
		}

		// Gap guard: if the earliest pending entry is past the slave's
		// next-needed LSN, intermediate entries were evicted from the WAL buffer
		// by retention while the slave was still connected (lagging). Streaming
		// this suffix would silently skip the pruned entries and diverge the
		// slave from the master. Force a snapshot resync instead — same recovery
		// the connect-time `canResumeFromLocked` path uses for an out-of-window
		// request.
		if pending[0].LSN > lastLSN+1 {
			slave.mu.Lock()
			slave.NeedsSnapshot = true
			slave.LastLSN = 0
			slave.mu.Unlock()
			_ = m.sendResyncRequired(slave, atomic.LoadUint64(&m.currentLSN))
			continue
		}

		data, err := encodeWALEntries(pending)
		if err != nil {
			continue
		}
		if err := m.sendWALToSlave(slave, data); err != nil {
			// Mark slave as lagging
			slave.mu.Lock()
			lastPing := slave.LastPing
			slave.mu.Unlock()
			m.callOnLag(slave.ID, time.Since(lastPing))
		}
	}
}

func filterWALEntriesAfter(entries []*WALEntry, lastLSN uint64) []*WALEntry {
	for i, entry := range entries {
		if entry.LSN > lastLSN {
			return entries[i:]
		}
	}
	return nil
}

// sendWALToSlave sends WAL data to a specific slave
func (m *Manager) sendWALToSlave(slave *SlaveConnection, data []byte) error {
	slave.mu.Lock()
	defer slave.mu.Unlock()

	clearDeadline, err := setReplicationWriteDeadline(slave.Conn)
	if err != nil {
		return err
	}
	defer clearDeadline()

	dataLen, err := replicationUint32Len(len(data), "WAL frame length")
	if err != nil {
		return err
	}
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame[:4], dataLen)
	copy(frame[4:], data)
	if _, err := writeReplicationFull(slave.Writer, frame); err != nil {
		return err
	}

	if err := slave.Writer.Flush(); err != nil {
		return err
	}

	slave.LastPing = time.Now()

	atomic.AddUint64(&m.metrics.ReplicatedBytes, uint64(len(data)))

	return nil
}

// startSlave initializes slave replication
func (m *Manager) startSlave() error {
	if err := m.loadReplicationState(); err != nil {
		return fmt.Errorf("failed to load replication state: %w", err)
	}

	// Connect to master
	conn, err := replicationDial("tcp", m.config.MasterAddr, replicationAuthTimeout)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	m.setMasterConn(conn)
	reader := bufio.NewReader(conn)

	// Authenticate
	if m.config.AuthToken != "" {
		if err := conn.SetReadDeadline(time.Now().Add(replicationAuthTimeout)); err != nil {
			m.closeMasterConn()
			return err
		}
		if err := writeReplicationControl(conn, "%s\n", m.config.AuthToken); err != nil {
			m.closeMasterConn()
			return err
		}

		// Read auth response
		response, err := readReplicationControlLine(reader)
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			m.closeMasterConn()
			return err
		}

		if response != "AUTH_OK\n" {
			m.closeMasterConn()
			return fmt.Errorf("authentication failed")
		}
	}

	lastApplied := atomic.LoadUint64(&m.lastApplied)
	if atomic.LoadUint32(&m.requireSnapshot) > 0 {
		if err := writeReplicationControl(conn, "RESUME_SNAPSHOT %d\n", lastApplied); err != nil {
			m.closeMasterConn()
			return err
		}
	} else if err := writeReplicationControl(conn, "RESUME %d\n", lastApplied); err != nil {
		m.closeMasterConn()
		return err
	}

	// Start replication goroutine
	m.wg.Add(1)
	go m.replicateFromMasterWithReader(reader)

	return nil
}

// replicateFromMaster handles replication stream from master
//
//lint:ignore U1000 used by coverage tests
func (m *Manager) replicateFromMaster() {
	m.replicateFromMasterWithReader(bufio.NewReader(m.masterConn))
}

func (m *Manager) replicateFromMasterWithReader(reader *bufio.Reader) {
	defer m.wg.Done()

	for {
		select {
		case <-m.stopCh:
			return
		default:
		}

		if err := m.readMasterFrame(reader); err != nil {
			m.closeMasterConn()
			m.callOnDisconnect("master", err)
			return
		}
	}
}

// readMasterFrame reads either a text control message or a length-prefixed WAL
// frame from the master.
func (m *Manager) readMasterFrame(reader *bufio.Reader) error {
	clearDeadline, err := setReplicationReadDeadline(m.getMasterConn())
	if err != nil {
		return err
	}
	defer clearDeadline()

	next, err := reader.Peek(1)
	if err != nil {
		return err
	}

	switch next[0] {
	case 'S', 'P', 'R':
		line, err := readReplicationControlLine(reader)
		if err != nil {
			return err
		}
		if len(line) >= len("SNAP") && line[:4] == "SNAP" {
			return m.handleSnapshotMessage(reader, line)
		}
		return m.handleMasterMessage(line)
	default:
		var frameLen uint32
		if err := binary.Read(reader, binary.BigEndian, &frameLen); err != nil {
			return err
		}
		if frameLen > maxReplicationFrameSize {
			return fmt.Errorf("replication frame too large: %d bytes", frameLen)
		}

		data := make([]byte, frameLen)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}
		if err := m.applyWALDataBytes(data); err != nil {
			return err
		}
		return m.sendAck()
	}
}

func readReplicationControlLine(reader *bufio.Reader) (string, error) {
	var line []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			if len(line)+len(chunk) > maxReplicationControlLineBytes {
				return "", fmt.Errorf("replication control line too large")
			}
			line = append(line, chunk...)
		}
		if err == nil {
			return string(line), nil
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		return "", err
	}
}

func (m *Manager) handleSnapshotMessage(reader *bufio.Reader, msg string) error {
	var lsn uint64
	var size uint64
	if _, err := fmt.Sscanf(msg, "SNAPSHOT %d %d", &lsn, &size); err != nil {
		return fmt.Errorf("invalid SNAPSHOT message: %w", err)
	}
	if size > maxReplicationSnapshotSize {
		return fmt.Errorf("replication snapshot too large: %d bytes", size)
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return fmt.Errorf("failed to read replication snapshot: %w", err)
	}

	if err := m.callOnApplySnapshot(data, lsn); err != nil {
		return err
	}

	atomic.StoreUint64(&m.lastApplied, lsn)
	atomic.StoreUint32(&m.requireSnapshot, 0)
	atomic.StoreInt64(&m.metrics.LastAppliedTime, time.Now().Unix())
	if err := m.saveReplicationState(); err != nil {
		return err
	}
	return m.sendAck()
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
		if _, err := fmt.Sscanf(msg, "START %d", &lsn); err != nil {
			return fmt.Errorf("invalid START message: %w", err)
		}
		atomic.StoreUint64(&m.lastApplied, lsn)
		atomic.StoreInt64(&m.metrics.LastAppliedTime, time.Now().Unix())
		return m.saveReplicationState()

	case "PING":
		// Heartbeat - respond with current position
		return m.sendPong()

	case "RESY":
		var lsn uint64
		if _, err := fmt.Sscanf(msg, "RESYNC %d", &lsn); err != nil {
			return fmt.Errorf("invalid RESYNC message: %w", err)
		}
		return fmt.Errorf("replication resync required at master LSN %d", lsn)

	default:
		// WAL data
		return m.applyWALData(msg)
	}
}

// applyWALData applies WAL data received from master
func (m *Manager) applyWALData(data string) error {
	return m.applyWALDataBytes([]byte(data))
}

func (m *Manager) applyWALDataBytes(data []byte) error {
	// Decode entries
	entries, err := decodeWALEntries(data)
	if err != nil {
		return err
	}

	// Apply each entry
	var advanced bool
	for _, entry := range entries {
		if entry.LSN <= atomic.LoadUint64(&m.lastApplied) {
			continue
		}

		if err := m.callOnApply(entry); err != nil {
			return err
		}

		atomic.StoreUint64(&m.lastApplied, entry.LSN)
		atomic.AddUint64(&m.metrics.AppliedEntries, 1)
		advanced = true
	}

	if advanced {
		atomic.StoreInt64(&m.metrics.LastAppliedTime, time.Now().Unix())
		return m.saveReplicationState()
	}

	return nil
}

func (m *Manager) loadReplicationState() (err error) {
	if m.config.StateFile == "" {
		return nil
	}

	stateFile, err := cleanReplicationStatePath(m.config.StateFile)
	if err != nil {
		return err
	}
	file, err := openReplicationStateFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() {
		if closeErr := file.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	var state replicationState
	decoder := json.NewDecoder(io.LimitReader(file, maxReplicationStateFileBytes+1))
	if err := decoder.Decode(&state); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("replication state contains trailing JSON value")
		}
		return fmt.Errorf("replication state contains trailing data: %w", err)
	}
	atomic.StoreUint64(&m.lastApplied, state.LastApplied)
	if state.RequireSnapshot {
		atomic.StoreUint32(&m.requireSnapshot, 1)
	} else {
		atomic.StoreUint32(&m.requireSnapshot, 0)
	}
	return nil
}

func (m *Manager) saveReplicationState() error {
	if m.config.StateFile == "" {
		return nil
	}

	stateFile, err := cleanReplicationStatePath(m.config.StateFile)
	if err != nil {
		return err
	}

	state := replicationState{
		LastApplied:     atomic.LoadUint64(&m.lastApplied),
		RequireSnapshot: atomic.LoadUint32(&m.requireSnapshot) > 0,
		UpdatedAt:       time.Now(),
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return err
	}
	return writeReplicationStateFileAtomic(stateFile, buf.Bytes())
}

func writeReplicationStateFileAtomic(stateFile string, data []byte) error {
	if err := prepareReplicationStateDir(stateFile); err != nil {
		return err
	}

	dir := filepath.Dir(stateFile)
	base := filepath.Base(stateFile)
	file, err := os.CreateTemp(dir, "."+base+".tmp-*") // #nosec G304 - state file path is explicit replication config and directory is validated before use.
	if err != nil {
		return err
	}
	tmpPath := file.Name()
	if err := file.Chmod(replicationStateFilePerm); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	if _, err := writeReplicationStateFull(file, data); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, stateFile); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := syncReplicationStateDir(stateFile); err != nil {
		return err
	}

	return nil
}

func writeReplicationStateFull(writer io.Writer, data []byte) (int, error) {
	n, err := writer.Write(data)
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func prepareReplicationStateDir(stateFile string) error {
	dir := filepath.Dir(stateFile)
	if err := rejectReplicationStateDirSymlinks(dir); err != nil {
		return err
	}

	info, statErr := os.Lstat(dir)
	preexisting := statErr == nil
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			return fmt.Errorf("failed to stat replication state directory: %w", statErr)
		}
	} else {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("replication state directory must not be a symlink: %s", dir)
		}
		if !info.IsDir() {
			return fmt.Errorf("replication state directory must be a directory: %s", dir)
		}
	}

	if err := os.MkdirAll(dir, replicationStateDirPerm); err != nil {
		return err
	}
	if !preexisting {
		if err := os.Chmod(dir, replicationStateDirPerm); err != nil {
			return err
		}
	}
	if err := rejectReplicationStateDirSymlinks(dir); err != nil {
		return err
	}

	openedInfo, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !openedInfo.IsDir() {
		return fmt.Errorf("replication state directory must be a directory: %s", dir)
	}
	if preexisting && !os.SameFile(info, openedInfo) {
		return fmt.Errorf("replication state directory changed while opening: %s", dir)
	}
	return nil
}

func rejectReplicationStateDirSymlinks(path string) error {
	path = filepath.Clean(path)
	if path == "." || path == string(os.PathSeparator) {
		return nil
	}

	current := "."
	if filepath.IsAbs(path) {
		current = string(os.PathSeparator)
		path = strings.TrimPrefix(path, string(os.PathSeparator))
	}
	for _, part := range strings.Split(path, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to stat replication state directory component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("replication state directory component must not be a symlink: %s", current)
		}
	}
	return nil
}

func cleanReplicationStatePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("replication state path cannot be empty")
	}
	return filepath.Clean(path), nil
}

func openReplicationStateFile(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("replication state file must not be a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("replication state file must be a regular file: %s", path)
	}
	if info.Size() > maxReplicationStateFileBytes {
		return nil, fmt.Errorf("replication state file is too large: %d bytes (max %d)", info.Size(), maxReplicationStateFileBytes)
	}

	file, err := os.Open(path) // #nosec G304 - state file path is explicit replication config and validated before use.
	if err != nil {
		return nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("replication state file must be a regular file: %s", path)
	}
	if openedInfo.Size() > maxReplicationStateFileBytes {
		_ = file.Close()
		return nil, fmt.Errorf("replication state file is too large: %d bytes (max %d)", openedInfo.Size(), maxReplicationStateFileBytes)
	}
	if !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return nil, fmt.Errorf("replication state file changed while opening: %s", path)
	}
	if err := file.Chmod(replicationStateFilePerm); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func syncReplicationStateDir(path string) error {
	dir := filepath.Dir(path)
	// #nosec G304 -- state path is validated as explicit replication configuration.
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func (m *Manager) sendAck() error {
	conn := m.getMasterConn()
	if conn == nil {
		return nil
	}
	return writeReplicationControl(conn, "ACK %d\n", atomic.LoadUint64(&m.lastApplied))
}

func (m *Manager) sendPong() error {
	conn := m.getMasterConn()
	if conn == nil {
		return nil
	}
	return writeReplicationControl(conn, "PONG %d\n", atomic.LoadUint64(&m.lastApplied))
}

func writeReplicationControl(conn net.Conn, format string, args ...any) error {
	clearDeadline, err := setReplicationWriteDeadline(conn)
	if err != nil {
		return err
	}
	defer clearDeadline()
	msg := fmt.Sprintf(format, args...)
	_, err = writeReplicationFull(conn, []byte(msg))
	return err
}

func setReplicationWriteDeadline(conn net.Conn) (func(), error) {
	if conn == nil {
		return func() {}, nil
	}
	if err := conn.SetWriteDeadline(time.Now().Add(replicationWriteTimeout)); err != nil {
		return nil, err
	}
	return func() { _ = conn.SetWriteDeadline(time.Time{}) }, nil
}

func setReplicationReadDeadline(conn net.Conn) (func(), error) {
	if conn == nil {
		return func() {}, nil
	}
	if err := conn.SetReadDeadline(time.Now().Add(replicationReadTimeout)); err != nil {
		return nil, err
	}
	return func() { _ = conn.SetReadDeadline(time.Time{}) }, nil
}

// ReplicateWALEntry adds a WAL entry for replication (called by master)
func (m *Manager) ReplicateWALEntry(data []byte) error {
	if m.config.Role != RoleMaster {
		return nil // Not a master, ignore
	}
	if atomic.LoadUint64(&m.fencedEpoch) > 0 {
		return ErrPrimaryFenced
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if atomic.LoadUint64(&m.fencedEpoch) > 0 {
		return ErrPrimaryFenced
	}

	entry := &WALEntry{
		LSN:       atomic.AddUint64(&m.currentLSN, 1),
		Timestamp: time.Now(),
		Data:      append([]byte(nil), data...),
		Checksum:  calculateCRC32(data),
	}

	m.walBuffer = append(m.walBuffer, entry)
	m.walBufferBytes += retainedWALBytes(entry)
	m.enforceWALRetentionLocked()

	return nil
}

func (m *Manager) pruneWALBuffer() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneWALBufferLocked()
}

func (m *Manager) pruneWALBufferLocked() {
	if len(m.walBuffer) == 0 {
		return
	}

	if len(m.slaves) == 0 {
		m.enforceWALRetentionLocked()
		return
	}

	minApplied := ^uint64(0)
	for _, slave := range m.slaves {
		slave.mu.Lock()
		lastLSN := slave.LastLSN
		slave.mu.Unlock()
		if lastLSN < minApplied {
			minApplied = lastLSN
		}
	}

	pruneCount := 0
	for pruneCount < len(m.walBuffer) && m.walBuffer[pruneCount].LSN <= minApplied {
		pruneCount++
	}
	if pruneCount == 0 {
		return
	}

	m.dropWALPrefixLocked(pruneCount)
	m.enforceWALRetentionLocked()
}

func clearWALEntries(entries []*WALEntry) {
	for i := range entries {
		entries[i] = nil
	}
}

func (m *Manager) enforceWALRetentionLocked() {
	for m.exceedsWALRetentionLocked() {
		m.dropWALPrefixLocked(1)
	}
}

func (m *Manager) exceedsWALRetentionLocked() bool {
	if len(m.walBuffer) == 0 {
		return false
	}
	if maxEntries := m.config.MaxWALBufferEntries; maxEntries > 0 && len(m.walBuffer) > maxEntries {
		return true
	}
	return m.config.MaxWALBufferBytes > 0 && m.walBufferBytes > m.config.MaxWALBufferBytes
}

func (m *Manager) dropWALPrefixLocked(dropCount int) {
	if dropCount <= 0 {
		return
	}
	if dropCount >= len(m.walBuffer) {
		clearWALEntries(m.walBuffer)
		m.walBuffer = m.walBuffer[:0]
		m.walBufferBytes = 0
		return
	}

	droppedBytes := retainedWALEntriesBytes(m.walBuffer[:dropCount])
	clearWALEntries(m.walBuffer[:dropCount])
	copy(m.walBuffer, m.walBuffer[dropCount:])
	clearWALEntries(m.walBuffer[len(m.walBuffer)-dropCount:])
	m.walBuffer = m.walBuffer[:len(m.walBuffer)-dropCount]
	m.walBufferBytes -= droppedBytes
	if m.walBufferBytes < 0 {
		m.walBufferBytes = retainedWALEntriesBytes(m.walBuffer)
	}
}

func retainedWALEntriesBytes(entries []*WALEntry) int64 {
	var total int64
	for _, entry := range entries {
		total += retainedWALBytes(entry)
	}
	return total
}

func retainedWALBytes(entry *WALEntry) int64 {
	if entry == nil {
		return 0
	}
	return int64(walEntryMetadataBytes + len(entry.Data))
}

// GetMetrics returns current replication metrics
func (m *Manager) GetMetrics() *Metrics {
	replicationLag := m.currentReplicationLagMillis(time.Now())
	atomic.StoreInt64(&m.metrics.ReplicationLag, replicationLag)

	return &Metrics{
		ReplicatedBytes: atomic.LoadUint64(&m.metrics.ReplicatedBytes),
		AppliedEntries:  atomic.LoadUint64(&m.metrics.AppliedEntries),
		ActiveSlaves:    atomic.LoadInt32(&m.metrics.ActiveSlaves),
		ReplicationLag:  replicationLag,
		LastAppliedTime: atomic.LoadInt64(&m.metrics.LastAppliedTime),
	}
}

func (m *Manager) currentReplicationLagMillis(now time.Time) int64 {
	switch m.role {
	case RoleMaster:
		return m.currentMasterLagMillis(now)
	case RoleSlave:
		lastApplied := atomic.LoadInt64(&m.metrics.LastAppliedTime)
		if lastApplied == 0 {
			return 0
		}
		lag := now.Sub(time.Unix(lastApplied, 0))
		if lag <= 0 {
			return 0
		}
		return lag.Milliseconds()
	default:
		return atomic.LoadInt64(&m.metrics.ReplicationLag)
	}
}

func (m *Manager) currentMasterLagMillis(now time.Time) int64 {
	currentLSN := atomic.LoadUint64(&m.currentLSN)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var maxLag time.Duration
	for _, slave := range m.slaves {
		slave.mu.Lock()
		lastLSN := slave.LastLSN
		lastPing := slave.LastPing
		slave.mu.Unlock()

		if lastLSN >= currentLSN || lastPing.IsZero() {
			continue
		}
		if lag := now.Sub(lastPing); lag > maxLag {
			maxLag = lag
		}
	}
	if maxLag <= 0 {
		return 0
	}
	return maxLag.Milliseconds()
}

// WaitForSlaves blocks until all connected slaves have caught up
func (m *Manager) WaitForSlaves(timeout time.Duration) error {
	if m.config.Mode == ModeAsync {
		return nil // Async mode doesn't wait
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if m.slavesCaughtUp() {
			return nil
		}

		select {
		case <-m.stopCh:
			return fmt.Errorf("replication stopped while waiting for slaves")
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for slaves")
		case <-ticker.C:
		}
	}
}

func (m *Manager) slavesCaughtUp() bool {
	currentLSN := atomic.LoadUint64(&m.currentLSN)

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.slaves) == 0 {
		return true
	}

	caughtUp := 0
	for _, slave := range m.slaves {
		slave.mu.Lock()
		lastLSN := slave.LastLSN
		slave.mu.Unlock()
		if lastLSN >= currentLSN {
			caughtUp++
		}
	}

	if m.config.Mode == ModeSync {
		return caughtUp > 0
	}
	return caughtUp == len(m.slaves)
}

// Helper functions

func encodeWALEntries(entries []*WALEntry) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write number of entries
	entryCount, err := replicationUint32Len(len(entries), "WAL entry count")
	if err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, entryCount); err != nil {
		return nil, err
	}

	// Write each entry
	for _, entry := range entries {
		data, err := entry.Encode()
		if err != nil {
			return nil, err
		}

		dataLen, err := replicationUint32Len(len(data), "encoded WAL entry length")
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.BigEndian, dataLen); err != nil {
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

	if uint64(numEntries) > uint64(buf.Len())/4 {
		return nil, fmt.Errorf("WAL entry count %d exceeds remaining payload %d", numEntries, buf.Len())
	}

	entries := make([]*WALEntry, numEntries)

	for i := uint32(0); i < numEntries; i++ {
		// Read entry length
		var entryLen uint32
		if err := binary.Read(buf, binary.BigEndian, &entryLen); err != nil {
			return nil, err
		}
		if uint64(entryLen) > uint64(buf.Len()) {
			return nil, fmt.Errorf("WAL entry length %d exceeds remaining payload %d", entryLen, buf.Len())
		}

		// Read entry data
		entryData := make([]byte, int(entryLen))
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

	if buf.Len() != 0 {
		return nil, fmt.Errorf("WAL entries payload contains trailing data: %d bytes", buf.Len())
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
	Role           string        `json:"role"`
	Mode           string        `json:"mode"`
	Connected      bool          `json:"connected"`
	PrimaryFenced  bool          `json:"primary_fenced,omitempty"`
	ActiveSlaves   int           `json:"active_slaves,omitempty"`
	LastApplied    uint64        `json:"last_applied_lsn"`
	CurrentMaster  uint64        `json:"current_master_lsn,omitempty"`
	PromotionEpoch uint64        `json:"promotion_epoch,omitempty"`
	FencedEpoch    uint64        `json:"fenced_epoch,omitempty"`
	Slaves         []SlaveStatus `json:"slaves,omitempty"`
	Lag            time.Duration `json:"replication_lag,omitempty"`
}

type SlaveStatus struct {
	ID       string        `json:"id"`
	LastLSN  uint64        `json:"last_lsn"`
	LastPing time.Time     `json:"last_ping"`
	Lag      time.Duration `json:"lag"`
}

// FailoverReadiness describes whether this replication manager can safely
// perform automated failover. Today it is deliberately a negative contract:
// CobaltDB replication is a transport, not a consensus-backed HA manager.
type FailoverReadiness struct {
	Role              string   `json:"role"`
	AutomaticFailover bool     `json:"automatic_failover"`
	Consensus         bool     `json:"consensus"`
	Fencing           bool     `json:"fencing"`
	SafePromotion     bool     `json:"safe_promotion"`
	Blockers          []string `json:"blockers"`
}

// GetStatus returns current replication status
func (m *Manager) GetStatus() *ReplicationStatus {
	status := &ReplicationStatus{
		LastApplied:    atomic.LoadUint64(&m.lastApplied),
		PromotionEpoch: atomic.LoadUint64(&m.promotionEpoch),
		FencedEpoch:    atomic.LoadUint64(&m.fencedEpoch),
	}
	status.PrimaryFenced = status.FencedEpoch > 0

	switch m.config.Role {
	case RoleMaster:
		status.Role = "master"
		status.Mode = replicationModeString(m.config.Mode)

		m.mu.RLock()
		status.ActiveSlaves = len(m.slaves)
		status.CurrentMaster = atomic.LoadUint64(&m.currentLSN)
		status.Slaves = make([]SlaveStatus, 0, len(m.slaves))
		for id, slave := range m.slaves {
			slave.mu.Lock()
			lastLSN := slave.LastLSN
			lastPing := slave.LastPing
			slave.mu.Unlock()
			lag := time.Since(lastPing)
			status.Slaves = append(status.Slaves, SlaveStatus{
				ID:       id,
				LastLSN:  lastLSN,
				LastPing: lastPing,
				Lag:      lag,
			})
		}
		m.mu.RUnlock()

	case RoleSlave:
		status.Role = "slave"
		status.Connected = m.getMasterConn() != nil
	}

	return status
}

// GetFailoverReadiness returns the explicit HA readiness contract for this
// manager. It prevents replication transport health from being mistaken for
// automatic HA capability.
func (m *Manager) GetFailoverReadiness() FailoverReadiness {
	status := m.GetStatus()
	role := status.Role
	if role == "" {
		role = "standalone"
	}
	externallyFenced := status.PromotionEpoch > 0
	blockers := []string{
		"leader election is not implemented",
		"quorum consensus is not implemented",
	}
	if !externallyFenced {
		blockers = append(blockers,
			"old primary fencing is not implemented",
			"safe promotion is not implemented",
		)
	}
	return FailoverReadiness{
		Role:              role,
		AutomaticFailover: false,
		Consensus:         false,
		Fencing:           externallyFenced,
		SafePromotion:     externallyFenced && role == "master",
		Blockers:          blockers,
	}
}

// PromoteToMaster intentionally refuses unsafe in-process promotion. Operators
// must use external orchestration with fencing and a validated RPO/RTO plan.
func (m *Manager) PromoteToMaster() error {
	return ErrAutomaticFailoverUnsupported
}

// PromoteToMasterWithFencing performs a manual promotion only after an external
// orchestrator provides proof that the old primary has been fenced. CobaltDB
// still does not perform leader election or quorum consensus; callers must
// obtain the epoch and fencing token from their own HA control plane.
func (m *Manager) PromoteToMasterWithFencing(req PromotionRequest) error {
	if m.role != RoleSlave && m.config.Role != RoleSlave {
		return fmt.Errorf("%w: only a slave can be promoted", ErrPromotionRejected)
	}
	if strings.TrimSpace(req.FencingToken) == "" {
		return fmt.Errorf("%w: fencing token is required", ErrPromotionRejected)
	}
	if !req.OldPrimaryFenced {
		return fmt.Errorf("%w: old primary must be fenced before promotion", ErrPromotionRejected)
	}
	if req.Epoch == 0 {
		return fmt.Errorf("%w: fencing epoch is required", ErrPromotionRejected)
	}
	if currentEpoch := atomic.LoadUint64(&m.promotionEpoch); currentEpoch >= req.Epoch {
		return fmt.Errorf("%w: fencing epoch %d is not newer than current epoch %d", ErrPromotionRejected, req.Epoch, currentEpoch)
	}
	if !req.ExpiresAt.IsZero() && !time.Now().Before(req.ExpiresAt) {
		return fmt.Errorf("%w: fencing token expired", ErrPromotionRejected)
	}
	lastApplied := atomic.LoadUint64(&m.lastApplied)
	if req.RequiredLSN > 0 && lastApplied < req.RequiredLSN {
		return fmt.Errorf("%w: replica LSN %d is behind required LSN %d", ErrPromotionRejected, lastApplied, req.RequiredLSN)
	}
	if !req.AllowConnectedPeer && m.getMasterConn() != nil {
		return fmt.Errorf("%w: master connection is still active", ErrPromotionRejected)
	}

	m.mu.Lock()
	conn := m.masterConn
	m.masterConn = nil
	m.role = RoleMaster
	m.config.Role = RoleMaster
	m.config.MasterAddr = ""
	if m.slaves == nil {
		m.slaves = make(map[string]*SlaveConnection)
	}
	if m.slaveWALPos == nil {
		m.slaveWALPos = make(map[string]uint64)
	}
	atomic.StoreUint64(&m.currentLSN, lastApplied)
	atomic.StoreUint64(&m.promotionEpoch, req.Epoch)
	m.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	return nil
}

// FencePrimary marks this master as fenced. It is a cooperative local guard
// that complements external fencing systems: after this succeeds, the manager
// refuses new WAL entries through ReplicateWALEntry.
func (m *Manager) FencePrimary(req PrimaryFenceRequest) error {
	if m.role != RoleMaster && m.config.Role != RoleMaster {
		return fmt.Errorf("%w: only a master can be fenced", ErrPromotionRejected)
	}
	if strings.TrimSpace(req.FencingToken) == "" {
		return fmt.Errorf("%w: fencing token is required", ErrPromotionRejected)
	}
	if req.Epoch == 0 {
		return fmt.Errorf("%w: fencing epoch is required", ErrPromotionRejected)
	}
	if currentEpoch := atomic.LoadUint64(&m.fencedEpoch); currentEpoch >= req.Epoch {
		return fmt.Errorf("%w: fencing epoch %d is not newer than current epoch %d", ErrPromotionRejected, req.Epoch, currentEpoch)
	}
	if !req.ExpiresAt.IsZero() && !time.Now().Before(req.ExpiresAt) {
		return fmt.Errorf("%w: fencing token expired", ErrPromotionRejected)
	}
	atomic.StoreUint64(&m.fencedEpoch, req.Epoch)
	return nil
}

// RejoinAsReplica reconfigures a fenced former primary as a slave of the new
// primary selected by an external orchestrator. It performs only local state
// transition; callers can subsequently start slave replication using the updated
// manager config.
func (m *Manager) RejoinAsReplica(req RejoinRequest) error {
	if m.role != RoleMaster && m.config.Role != RoleMaster {
		return fmt.Errorf("%w: only a master can rejoin as replica", ErrPromotionRejected)
	}
	if strings.TrimSpace(req.FencingToken) == "" {
		return fmt.Errorf("%w: fencing token is required", ErrPromotionRejected)
	}
	if strings.TrimSpace(req.NewMasterAddr) == "" {
		return fmt.Errorf("%w: new master address is required", ErrPromotionRejected)
	}
	fencedEpoch := atomic.LoadUint64(&m.fencedEpoch)
	if fencedEpoch == 0 {
		return fmt.Errorf("%w: primary must be fenced before rejoin", ErrPromotionRejected)
	}
	if req.Epoch < fencedEpoch {
		return fmt.Errorf("%w: rejoin epoch %d is older than fenced epoch %d", ErrPromotionRejected, req.Epoch, fencedEpoch)
	}
	if req.LastAppliedLSN == 0 && !req.RequireSnapshot {
		return fmt.Errorf("%w: rejoin requires a validated resume LSN or an explicit snapshot refresh", ErrPromotionRejected)
	}

	m.mu.Lock()
	listener := m.listener
	m.listener = nil
	conn := m.masterConn
	m.masterConn = nil
	slaves := m.slaves
	m.slaves = make(map[string]*SlaveConnection)
	m.slaveWALPos = make(map[string]uint64)
	clearWALEntries(m.walBuffer)
	m.walBuffer = m.walBuffer[:0]
	m.walBufferBytes = 0
	m.role = RoleSlave
	m.config.Role = RoleSlave
	m.config.MasterAddr = req.NewMasterAddr
	m.config.ListenAddr = ""
	lsn := req.LastAppliedLSN
	atomic.StoreUint64(&m.lastApplied, lsn)
	if req.RequireSnapshot {
		atomic.StoreUint32(&m.requireSnapshot, 1)
	} else {
		atomic.StoreUint32(&m.requireSnapshot, 0)
	}
	atomic.StoreUint64(&m.currentLSN, 0)
	atomic.StoreUint64(&m.fencedEpoch, 0)
	atomic.StoreUint64(&m.promotionEpoch, 0)
	m.mu.Unlock()

	if listener != nil {
		_ = listener.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
	for _, slave := range slaves {
		if slave != nil && slave.Conn != nil {
			_ = slave.Conn.Close()
		}
	}
	if err := m.saveReplicationState(); err != nil {
		return err
	}
	return nil
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
