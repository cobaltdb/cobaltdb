package replication

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNoReplicaAvailable = errors.New("no read replica available")
	ErrReplicaNotReady    = errors.New("replica is not ready")
	ErrReplicaLagExceeded = errors.New("replica lag exceeded")
)

// ReplicaStatus represents the status of a read replica
type ReplicaStatus int

const (
	ReplicaStatusUnknown ReplicaStatus = iota
	ReplicaStatusConnecting
	ReplicaStatusSyncing
	ReplicaStatusReady
	ReplicaStatusLagExceeded
	ReplicaStatusError
	ReplicaStatusOffline
)

func (s ReplicaStatus) String() string {
	switch s {
	case ReplicaStatusConnecting:
		return "connecting"
	case ReplicaStatusSyncing:
		return "syncing"
	case ReplicaStatusReady:
		return "ready"
	case ReplicaStatusLagExceeded:
		return "lag_exceeded"
	case ReplicaStatusError:
		return "error"
	case ReplicaStatusOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// ReadReplica represents a single read replica node
type ReadReplica struct {
	ID       string
	Address  string
	Status   ReplicaStatus
	LagMs    int64 // Replication lag in milliseconds
	Weight   int   // Load balancing weight
	Priority int   // Failover priority (lower = higher priority)

	mu          sync.RWMutex
	lastCheck   time.Time
	lastError   error
	totalReads  uint64
	totalErrors uint64

	// Connection pool for this replica
	connPool    chan *ReplicaConnection
	maxConns    int
	activeConns atomic.Int32
}

// ReplicaConnection represents a connection to a replica
type ReplicaConnection struct {
	ReplicaID string
	Address   string
	Conn      net.Conn
	LastUsed  time.Time
}

// IsReady returns true if the replica is ready to serve reads
func (r *ReadReplica) IsReady() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Status == ReplicaStatusReady
}

// GetLag returns the current replication lag
func (r *ReadReplica) GetLag() int64 {
	return atomic.LoadInt64(&r.LagMs)
}

// SetLag updates the replication lag
func (r *ReadReplica) SetLag(lagMs int64) {
	atomic.StoreInt64(&r.LagMs, lagMs)

	r.mu.Lock()
	defer r.mu.Unlock()

	if lagMs > 5000 && r.Status == ReplicaStatusReady {
		r.Status = ReplicaStatusLagExceeded
	} else if lagMs <= 5000 && r.Status == ReplicaStatusLagExceeded {
		r.Status = ReplicaStatusReady
	}
}

// RecordRead records a successful read operation
func (r *ReadReplica) RecordRead() {
	atomic.AddUint64(&r.totalReads, 1)
}

// RecordError records a read error
func (r *ReadReplica) RecordError(err error) {
	atomic.AddUint64(&r.totalErrors, 1)
	r.mu.Lock()
	r.lastError = err
	r.mu.Unlock()
}

// GetStats returns replica statistics
func (r *ReadReplica) GetStats() ReplicaStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return ReplicaStats{
		ID:          r.ID,
		Status:      r.Status,
		StatusStr:   r.Status.String(),
		LagMs:       r.GetLag(),
		Weight:      r.Weight,
		TotalReads:  atomic.LoadUint64(&r.totalReads),
		TotalErrors: atomic.LoadUint64(&r.totalErrors),
		LastCheck:   r.lastCheck,
		LastError:   r.lastError,
		ActiveConns: int(r.activeConns.Load()),
		MaxConns:    r.maxConns,
	}
}

// ReplicaStats holds statistics for a replica
type ReplicaStats struct {
	ID          string        `json:"id"`
	Status      ReplicaStatus `json:"status"`
	StatusStr   string        `json:"status_str"`
	LagMs       int64         `json:"lag_ms"`
	Weight      int           `json:"weight"`
	TotalReads  uint64        `json:"total_reads"`
	TotalErrors uint64        `json:"total_errors"`
	LastCheck   time.Time     `json:"last_check"`
	LastError   error         `json:"last_error,omitempty"`
	ActiveConns int           `json:"active_conns"`
	MaxConns    int           `json:"max_conns"`
}

// ReadReplicaConfig configures the read replica manager
type ReadReplicaConfig struct {
	MaxLagMs                 int64         // Maximum acceptable lag (default: 5000ms)
	HealthCheckInterval      time.Duration // Health check interval (default: 10s)
	LoadBalanceStrategy      string        // "round_robin", "weighted", "least_lag"
	AutoFailover             bool          // Enable automatic failover
	ConnectionTimeout        time.Duration // Connection timeout (default: 5s)
	MaxConnectionsPerReplica int           // Max connections per replica (default: 10)
	ReplicaConnectRetry      int           // Retry attempts for replica connection (default: 3)
}

// DefaultReadReplicaConfig returns default configuration
func DefaultReadReplicaConfig() *ReadReplicaConfig {
	return &ReadReplicaConfig{
		MaxLagMs:                 5000,
		HealthCheckInterval:      10 * time.Second,
		LoadBalanceStrategy:      "weighted",
		AutoFailover:             true,
		ConnectionTimeout:        5 * time.Second,
		MaxConnectionsPerReplica: 10,
		ReplicaConnectRetry:      3,
	}
}

// ReadReplicaManager manages read replicas and routes queries
type ReadReplicaManager struct {
	config   *ReadReplicaConfig
	replicas map[string]*ReadReplica
	mu       sync.RWMutex

	// Load balancing
	rrIndex uint64 // Round robin index

	// Health checking
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewReadReplicaManager creates a new read replica manager
func NewReadReplicaManager(config *ReadReplicaConfig) *ReadReplicaManager {
	if config == nil {
		config = DefaultReadReplicaConfig()
	}

	m := &ReadReplicaManager{
		config:   config,
		replicas: make(map[string]*ReadReplica),
		stopCh:   make(chan struct{}),
	}

	// Start health checking
	m.wg.Add(1)
	go m.healthCheckLoop()

	return m
}

// AddReplica adds a new read replica
func (m *ReadReplicaManager) AddReplica(id, address string, weight, priority int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.replicas[id]; exists {
		return fmt.Errorf("replica with ID %s already exists", id)
	}

	replica := &ReadReplica{
		ID:       id,
		Address:  address,
		Status:   ReplicaStatusConnecting,
		Weight:   weight,
		Priority: priority,
		maxConns: m.config.MaxConnectionsPerReplica,
		connPool: make(chan *ReplicaConnection, m.config.MaxConnectionsPerReplica),
	}

	m.replicas[id] = replica

	// Perform initial health check
	go m.checkReplica(replica)

	return nil
}

// RemoveReplica removes a read replica
func (m *ReadReplicaManager) RemoveReplica(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	replica, exists := m.replicas[id]
	if !exists {
		return fmt.Errorf("replica with ID %s not found", id)
	}

	// Close all connections in the pool
	close(replica.connPool)
	for conn := range replica.connPool {
		if conn.Conn != nil {
			conn.Conn.Close()
		}
	}

	delete(m.replicas, id)
	return nil
}

// GetReplica returns a replica by ID
func (m *ReadReplicaManager) GetReplica(id string) (*ReadReplica, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	replica, exists := m.replicas[id]
	if !exists {
		return nil, fmt.Errorf("replica with ID %s not found", id)
	}

	return replica, nil
}

// SelectReplica selects a replica for a read operation
func (m *ReadReplicaManager) SelectReplica(ctx context.Context) (*ReadReplica, error) {
	m.mu.RLock()
	replicas := make([]*ReadReplica, 0, len(m.replicas))
	for _, r := range m.replicas {
		if r.IsReady() {
			replicas = append(replicas, r)
		}
	}
	m.mu.RUnlock()

	if len(replicas) == 0 {
		return nil, ErrNoReplicaAvailable
	}

	switch m.config.LoadBalanceStrategy {
	case "round_robin":
		return m.selectRoundRobin(replicas)
	case "weighted":
		return m.selectWeighted(replicas)
	case "least_lag":
		return m.selectLeastLag(replicas)
	default:
		return m.selectWeighted(replicas)
	}
}

// selectRoundRobin selects a replica using round-robin
func (m *ReadReplicaManager) selectRoundRobin(replicas []*ReadReplica) (*ReadReplica, error) {
	if len(replicas) == 0 {
		return nil, ErrNoReplicaAvailable
	}

	idx := atomic.AddUint64(&m.rrIndex, 1) % uint64(len(replicas))
	return replicas[idx], nil
}

// selectWeighted selects a replica using weighted random selection
func (m *ReadReplicaManager) selectWeighted(replicas []*ReadReplica) (*ReadReplica, error) {
	if len(replicas) == 0 {
		return nil, ErrNoReplicaAvailable
	}

	totalWeight := 0
	for _, r := range replicas {
		totalWeight += r.Weight
	}

	if totalWeight == 0 {
		return replicas[0], nil
	}

	// Simple weighted selection
	idx := atomic.AddUint64(&m.rrIndex, 1) % uint64(totalWeight)
	currentWeight := 0
	for _, r := range replicas {
		currentWeight += r.Weight
		if int(idx) < currentWeight {
			return r, nil
		}
	}

	return replicas[0], nil
}

// selectLeastLag selects the replica with the least lag
func (m *ReadReplicaManager) selectLeastLag(replicas []*ReadReplica) (*ReadReplica, error) {
	if len(replicas) == 0 {
		return nil, ErrNoReplicaAvailable
	}

	var best *ReadReplica
	minLag := int64(^uint64(0) >> 1) // Max int64

	for _, r := range replicas {
		lag := r.GetLag()
		if lag < minLag {
			minLag = lag
			best = r
		}
	}

	return best, nil
}

// GetAllReplicas returns all registered replicas
func (m *ReadReplicaManager) GetAllReplicas() []*ReadReplica {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*ReadReplica, 0, len(m.replicas))
	for _, r := range m.replicas {
		result = append(result, r)
	}
	return result
}

// GetReadyReplicas returns only ready replicas
func (m *ReadReplicaManager) GetReadyReplicas() []*ReadReplica {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*ReadReplica, 0)
	for _, r := range m.replicas {
		if r.IsReady() {
			result = append(result, r)
		}
	}
	return result
}

// GetStats returns statistics for all replicas
func (m *ReadReplicaManager) GetStats() ReadReplicaManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := ReadReplicaManagerStats{
		TotalReplicas: len(m.replicas),
		ReplicaStats:  make([]ReplicaStats, 0, len(m.replicas)),
	}

	for _, r := range m.replicas {
		rs := r.GetStats()
		stats.ReplicaStats = append(stats.ReplicaStats, rs)

		switch r.Status {
		case ReplicaStatusReady:
			stats.ReadyReplicas++
		case ReplicaStatusError, ReplicaStatusOffline:
			stats.UnhealthyReplicas++
		}
	}

	return stats
}

// healthCheckLoop periodically checks replica health
func (m *ReadReplicaManager) healthCheckLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkReplicas()
		}
	}
}

// checkReplicas performs health checks on all replicas
func (m *ReadReplicaManager) checkReplicas() {
	m.mu.RLock()
	replicas := make([]*ReadReplica, 0, len(m.replicas))
	for _, r := range m.replicas {
		replicas = append(replicas, r)
	}
	m.mu.RUnlock()

	for _, r := range replicas {
		m.checkReplica(r)
	}
}

// checkReplica checks the health of a single replica
func (m *ReadReplicaManager) checkReplica(r *ReadReplica) {
	r.mu.Lock()
	r.lastCheck = time.Now()
	r.mu.Unlock()

	// Try to establish a TCP connection to verify replica is alive
	conn, err := net.DialTimeout("tcp", r.Address, m.config.ConnectionTimeout)
	if err != nil {
		r.RecordError(err)
		r.mu.Lock()
		if r.Status != ReplicaStatusOffline {
			r.Status = ReplicaStatusError
		}
		r.mu.Unlock()
		return
	}
	conn.Close()

	// Update status based on lag
	lag := r.GetLag()
	r.mu.Lock()
	if lag > m.config.MaxLagMs {
		r.Status = ReplicaStatusLagExceeded
	} else if r.Status == ReplicaStatusConnecting || r.Status == ReplicaStatusError {
		r.Status = ReplicaStatusSyncing
		// After successful connection, mark as ready
		r.Status = ReplicaStatusReady
	} else if r.Status == ReplicaStatusLagExceeded && lag <= m.config.MaxLagMs {
		r.Status = ReplicaStatusReady
	}
	r.mu.Unlock()
}

// Close shuts down the read replica manager
func (m *ReadReplicaManager) Close() error {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})

	m.wg.Wait()

	// Close all replica connections
	m.mu.RLock()
	replicas := make([]*ReadReplica, 0, len(m.replicas))
	for _, r := range m.replicas {
		replicas = append(replicas, r)
	}
	m.mu.RUnlock()

	for _, r := range replicas {
		if r.connPool != nil {
			close(r.connPool)
			for conn := range r.connPool {
				if conn.Conn != nil {
					conn.Conn.Close()
				}
			}
		}
	}

	return nil
}

// ReadReplicaManagerStats holds statistics for the manager
type ReadReplicaManagerStats struct {
	TotalReplicas     int            `json:"total_replicas"`
	ReadyReplicas     int            `json:"ready_replicas"`
	UnhealthyReplicas int            `json:"unhealthy_replicas"`
	ReplicaStats      []ReplicaStats `json:"replica_stats"`
}

// IsReadQuery returns true if the SQL is a read-only query
func IsReadQuery(sql string) bool {
	// Trim leading whitespace
	sql = strings.TrimSpace(sql)
	if len(sql) == 0 {
		return false
	}

	// Get first word (case-insensitive)
	firstWordEnd := 0
	for i := 0; i < len(sql) && i < 10; i++ {
		c := sql[i]
		if c >= 'A' && c <= 'Z' {
			// Already uppercase
			firstWordEnd = i + 1
		} else if c >= 'a' && c <= 'z' {
			// Lowercase, will need to convert
			firstWordEnd = i + 1
		} else {
			// Non-letter, end of first word
			break
		}
	}

	if firstWordEnd == 0 {
		return false
	}

	// Convert first word to uppercase for comparison
	firstWord := make([]byte, firstWordEnd)
	for i := 0; i < firstWordEnd; i++ {
		c := sql[i]
		if c >= 'a' && c <= 'z' {
			firstWord[i] = c - 32
		} else {
			firstWord[i] = c
		}
	}

	// Check for read-only operations
	switch string(firstWord) {
	case "SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH":
		return true
	default:
		return false
	}
}

// GetConnection gets a connection from the replica's connection pool
func (r *ReadReplica) GetConnection(timeout time.Duration) (*ReplicaConnection, error) {
	select {
	case conn := <-r.connPool:
		conn.LastUsed = time.Now()
		r.activeConns.Add(1)
		return conn, nil
	case <-time.After(timeout):
		// Create a new connection if pool is empty
		conn, err := net.DialTimeout("tcp", r.Address, timeout)
		if err != nil {
			return nil, err
		}
		r.activeConns.Add(1)
		return &ReplicaConnection{
			ReplicaID: r.ID,
			Address:   r.Address,
			Conn:      conn,
			LastUsed:  time.Now(),
		}, nil
	}
}

// ReleaseConnection returns a connection to the pool
func (r *ReadReplica) ReleaseConnection(conn *ReplicaConnection) {
	r.activeConns.Add(-1)

	// Check if connection is still valid
	if conn.Conn == nil {
		return
	}

	// Try to return to pool, but don't block
	select {
	case r.connPool <- conn:
		// Returned to pool
	default:
		// Pool is full, close the connection
		conn.Conn.Close()
	}
}

// UpdateLagFromReplication updates lag from replication status
// This should be called by the replication protocol when it receives lag info
func (r *ReadReplica) UpdateLagFromReplication(lagMs int64) {
	r.SetLag(lagMs)
}
