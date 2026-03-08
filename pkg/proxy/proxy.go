package proxy

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// BackendType represents the type of database backend
type BackendType string

const (
	BackendTypePrimary BackendType = "primary"
	BackendTypeReplica BackendType = "replica"
)

// Backend represents a database backend node
type Backend struct {
	ID        string
	Host      string
	Port      int
	Type      BackendType
	Weight    int // For weighted load balancing
	Database  string
	Username  string
	Password  string
	TLSConfig *tls.Config

	// Health tracking
	mu          sync.RWMutex
	Healthy     bool
	LastCheck   time.Time
	ActiveConns int32
	TotalConns  int64
	FailedConns int64
	Latency     time.Duration
}

// Address returns the backend address
func (b *Backend) Address() string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

// IsHealthy returns true if the backend is healthy
func (b *Backend) IsHealthy() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Healthy
}

// SetHealth sets the health status
func (b *Backend) SetHealth(healthy bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Healthy = healthy
	b.LastCheck = time.Now()
}

// IncrementConnections increments the active connection count
func (b *Backend) IncrementConnections() {
	atomic.AddInt32(&b.ActiveConns, 1)
	atomic.AddInt64(&b.TotalConns, 1)
}

// DecrementConnections decrements the active connection count
func (b *Backend) DecrementConnections() {
	atomic.AddInt32(&b.ActiveConns, -1)
}

// ConnectionPool manages connections to a backend
type ConnectionPool struct {
	backend     *Backend
	maxConns    int
	maxIdle     int
	idleTimeout time.Duration

	mu          sync.Mutex
	connections []PooledConnection
	idleConns   chan PooledConnection
}

// PooledConnection represents a pooled database connection
type PooledConnection struct {
	ID       string
	Backend  *Backend
	Conn     net.Conn
	Created  time.Time
	LastUsed time.Time
	InUse    bool
}

// ProxyConfig configures the SQL proxy
type ProxyConfig struct {
	ListenAddress        string
	ListenPort           int
	MaxConnections       int
	DefaultTimeout       time.Duration
	HealthCheckInterval  time.Duration
	HealthCheckTimeout   time.Duration
	LoadBalanceStrategy  string // "round_robin", "least_connections", "weighted"
	EnableReadWriteSplit bool
}

// DefaultProxyConfig returns default configuration
func DefaultProxyConfig() *ProxyConfig {
	return &ProxyConfig{
		ListenAddress:        "0.0.0.0",
		ListenPort:           5433,
		MaxConnections:       1000,
		DefaultTimeout:       30 * time.Second,
		HealthCheckInterval:  10 * time.Second,
		HealthCheckTimeout:   5 * time.Second,
		LoadBalanceStrategy:  "round_robin",
		EnableReadWriteSplit: true,
	}
}

// SQLProxy provides SQL proxy and load balancing
type SQLProxy struct {
	config   *ProxyConfig
	backends map[string]*Backend
	primary  *Backend
	replicas []*Backend

	// Load balancing state
	rrIndex atomic.Uint64 // Round-robin index

	// Connection tracking
	clientConns map[string]*ClientConn
	connCount   atomic.Int32
	maxConns    int

	mu       sync.RWMutex
	listener net.Listener
	running  atomic.Bool
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// ClientConn represents a client connection
type ClientConn struct {
	ID          string
	Conn        net.Conn
	Backend     *Backend
	BackendConn net.Conn
	Created     time.Time
	IsWrite     bool // Whether connection is for writes
}

// NewSQLProxy creates a new SQL proxy
func NewSQLProxy(config *ProxyConfig) *SQLProxy {
	if config == nil {
		config = DefaultProxyConfig()
	}

	return &SQLProxy{
		config:      config,
		backends:    make(map[string]*Backend),
		replicas:    make([]*Backend, 0),
		clientConns: make(map[string]*ClientConn),
		maxConns:    config.MaxConnections,
		stopCh:      make(chan struct{}),
	}
}

// AddBackend adds a backend to the proxy
func (p *SQLProxy) AddBackend(backend *Backend) error {
	if backend.ID == "" {
		return errors.New("backend ID is required")
	}
	if backend.Host == "" {
		return errors.New("backend host is required")
	}
	if backend.Port == 0 {
		return errors.New("backend port is required")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.backends[backend.ID] = backend

	if backend.Type == BackendTypePrimary {
		p.primary = backend
	} else {
		p.replicas = append(p.replicas, backend)
	}

	logger.Default().Infof("Backend added: %s (%s:%d) as %s", backend.ID, backend.Host, backend.Port, backend.Type)
	return nil
}

// RemoveBackend removes a backend from the proxy
func (p *SQLProxy) RemoveBackend(backendID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	backend, exists := p.backends[backendID]
	if !exists {
		return fmt.Errorf("backend not found: %s", backendID)
	}

	delete(p.backends, backendID)

	if p.primary == backend {
		p.primary = nil
	} else {
		// Remove from replicas
		newReplicas := make([]*Backend, 0, len(p.replicas)-1)
		for _, r := range p.replicas {
			if r != backend {
				newReplicas = append(newReplicas, r)
			}
		}
		p.replicas = newReplicas
	}

	logger.Default().Infof("Backend removed: %s", backendID)
	return nil
}

// Start starts the SQL proxy
func (p *SQLProxy) Start() error {
	if p.running.Load() {
		return errors.New("proxy already running")
	}

	address := fmt.Sprintf("%s:%d", p.config.ListenAddress, p.config.ListenPort)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	p.listener = listener
	p.running.Store(true)

	// Start health checker
	p.wg.Add(1)
	go p.healthCheckLoop()

	// Start accepting connections
	p.wg.Add(1)
	go p.acceptLoop()

	logger.Default().Infof("SQL proxy started on %s", address)
	return nil
}

// Stop stops the SQL proxy
func (p *SQLProxy) Stop() error {
	if !p.running.CompareAndSwap(true, false) {
		return nil
	}

	close(p.stopCh)

	if p.listener != nil {
		p.listener.Close()
	}

	// Close all client connections
	p.mu.Lock()
	for _, conn := range p.clientConns {
		conn.Conn.Close()
		if conn.BackendConn != nil {
			conn.BackendConn.Close()
		}
	}
	p.mu.Unlock()

	p.wg.Wait()
	logger.Default().Info("SQL proxy stopped")
	return nil
}

// acceptLoop accepts incoming client connections
func (p *SQLProxy) acceptLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		default:
		}

		conn, err := p.listener.Accept()
		if err != nil {
			if p.running.Load() {
				logger.Default().Errorf("Accept error: %v", err)
			}
			continue
		}

		// Check connection limit
		if int(p.connCount.Load()) >= p.maxConns {
			logger.Default().Warn("Max connections reached, rejecting connection")
			conn.Close()
			continue
		}

		p.wg.Add(1)
		go p.handleClient(conn)
	}
}

// handleClient handles a client connection
func (p *SQLProxy) handleClient(clientConn net.Conn) {
	defer p.wg.Done()

	p.connCount.Add(1)
	defer p.connCount.Add(-1)

	clientID := generateConnID()
	client := &ClientConn{
		ID:      clientID,
		Conn:    clientConn,
		Created: time.Now(),
	}

	p.mu.Lock()
	p.clientConns[clientID] = client
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.clientConns, clientID)
		p.mu.Unlock()
		clientConn.Close()
		if client.BackendConn != nil {
			client.BackendConn.Close()
			if client.Backend != nil {
				client.Backend.DecrementConnections()
			}
		}
	}()

	// First, peek at the initial message to determine query type
	buf := make([]byte, 8192)
	n, err := clientConn.Read(buf)
	if err != nil {
		logger.Default().Errorf("Failed to read from client: %v", err)
		return
	}

	// Determine if this is a write or read query
	isWrite := p.isWriteQuery(string(buf[:n]))
	client.IsWrite = isWrite

	// Select appropriate backend
	backend := p.selectBackend(isWrite)
	if backend == nil {
		logger.Default().Error("No healthy backend available")
		return
	}

	// Connect to backend
	backendConn, err := net.DialTimeout("tcp", backend.Address(), p.config.DefaultTimeout)
	if err != nil {
		logger.Default().Errorf("Failed to connect to backend %s: %v", backend.ID, err)
		backend.SetHealth(false)
		return
	}

	client.Backend = backend
	client.BackendConn = backendConn
	backend.IncrementConnections()

	// Send initial data to backend
	_, err = backendConn.Write(buf[:n])
	if err != nil {
		logger.Default().Errorf("Failed to write to backend: %v", err)
		return
	}

	// Start bidirectional proxying
	errChan := make(chan error, 2)

	// Client -> Backend
	go func() {
		_, err := p.proxy(clientConn, backendConn)
		errChan <- err
	}()

	// Backend -> Client
	go func() {
		_, err := p.proxy(backendConn, clientConn)
		errChan <- err
	}()

	// Wait for either direction to close
	<-errChan
}

// proxy proxies data between two connections
func (p *SQLProxy) proxy(src, dst net.Conn) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64

	for {
		select {
		case <-p.stopCh:
			return written, nil
		default:
		}

		nr, err := src.Read(buf)
		if nr > 0 {
			nw, err := dst.Write(buf[:nr])
			if err != nil {
				return written, err
			}
			written += int64(nw)
		}
		if err != nil {
			return written, err
		}
	}
}

// selectBackend selects a backend based on the load balancing strategy
func (p *SQLProxy) selectBackend(isWrite bool) *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Writes always go to primary
	if isWrite || !p.config.EnableReadWriteSplit {
		if p.primary != nil && p.primary.IsHealthy() {
			return p.primary
		}
		return nil
	}

	// Reads go to replicas if available
	healthyReplicas := make([]*Backend, 0)
	for _, r := range p.replicas {
		if r.IsHealthy() {
			healthyReplicas = append(healthyReplicas, r)
		}
	}

	if len(healthyReplicas) == 0 {
		// Fall back to primary if no healthy replicas
		if p.primary != nil && p.primary.IsHealthy() {
			return p.primary
		}
		return nil
	}

	switch p.config.LoadBalanceStrategy {
	case "least_connections":
		return p.selectLeastConnections(healthyReplicas)
	case "weighted":
		return p.selectWeighted(healthyReplicas)
	default: // round_robin
		return p.selectRoundRobin(healthyReplicas)
	}
}

// selectRoundRobin selects a backend using round-robin
func (p *SQLProxy) selectRoundRobin(backends []*Backend) *Backend {
	if len(backends) == 0 {
		return nil
	}
	idx := p.rrIndex.Add(1) % uint64(len(backends))
	return backends[idx]
}

// selectLeastConnections selects the backend with least active connections
func (p *SQLProxy) selectLeastConnections(backends []*Backend) *Backend {
	if len(backends) == 0 {
		return nil
	}

	var selected *Backend
	minConns := int32(^uint32(0) >> 1) // Max int32

	for _, b := range backends {
		conns := atomic.LoadInt32(&b.ActiveConns)
		if conns < minConns {
			minConns = conns
			selected = b
		}
	}

	return selected
}

// selectWeighted selects a backend using weighted round-robin
func (p *SQLProxy) selectWeighted(backends []*Backend) *Backend {
	if len(backends) == 0 {
		return nil
	}

	totalWeight := 0
	for _, b := range backends {
		totalWeight += b.Weight
	}

	if totalWeight == 0 {
		return p.selectRoundRobin(backends)
	}

	idx := p.rrIndex.Add(1) % uint64(totalWeight)
	currentWeight := uint64(0)

	for _, b := range backends {
		currentWeight += uint64(b.Weight)
		if idx < currentWeight {
			return b
		}
	}

	return backends[0]
}

// isWriteQuery determines if a query is a write operation
func (p *SQLProxy) isWriteQuery(query string) bool {
	// Simple heuristic based on query keywords
	upperQuery := strings.ToUpper(strings.TrimSpace(query))

	writeKeywords := []string{
		"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
		"TRUNCATE", "GRANT", "REVOKE", "LOCK", "UNLOCK",
	}

	for _, kw := range writeKeywords {
		if strings.HasPrefix(upperQuery, kw) {
			return true
		}
	}

	return false
}

// healthCheckLoop performs periodic health checks
func (p *SQLProxy) healthCheckLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	// Check immediately on start
	p.checkAllBackends()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.checkAllBackends()
		}
	}
}

// checkAllBackends checks health of all backends
func (p *SQLProxy) checkAllBackends() {
	p.mu.RLock()
	backends := make([]*Backend, 0, len(p.backends))
	for _, b := range p.backends {
		backends = append(backends, b)
	}
	p.mu.RUnlock()

	for _, backend := range backends {
		p.checkBackend(backend)
	}
}

// checkBackend checks the health of a single backend
func (p *SQLProxy) checkBackend(backend *Backend) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", backend.Address(), p.config.HealthCheckTimeout)
	latency := time.Since(start)

	if err != nil {
		if backend.IsHealthy() {
			logger.Default().Warnf("Backend %s is unhealthy: %v", backend.ID, err)
			backend.SetHealth(false)
			atomic.AddInt64(&backend.FailedConns, 1)
		}
		return
	}
	conn.Close()

	backend.mu.Lock()
	backend.Latency = latency
	backend.mu.Unlock()

	if !backend.IsHealthy() {
		logger.Default().Infof("Backend %s is now healthy (latency: %v)", backend.ID, latency)
		backend.SetHealth(true)
	}
}

// GetStats returns proxy statistics
func (p *SQLProxy) GetStats() ProxyStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := ProxyStats{
		ActiveConnections: int(p.connCount.Load()),
		MaxConnections:    p.maxConns,
		TotalBackends:     len(p.backends),
		HealthyBackends:   0,
		BackendStats:      make(map[string]BackendStats),
	}

	for _, b := range p.backends {
		if b.IsHealthy() {
			stats.HealthyBackends++
		}
		stats.BackendStats[b.ID] = BackendStats{
			Type:        string(b.Type),
			Healthy:     b.IsHealthy(),
			ActiveConns: int(atomic.LoadInt32(&b.ActiveConns)),
			TotalConns:  atomic.LoadInt64(&b.TotalConns),
			FailedConns: atomic.LoadInt64(&b.FailedConns),
		}
	}

	return stats
}

// ProxyStats contains proxy statistics
type ProxyStats struct {
	ActiveConnections int
	MaxConnections    int
	TotalBackends     int
	HealthyBackends   int
	BackendStats      map[string]BackendStats
}

// BackendStats contains backend statistics
type BackendStats struct {
	Type        string
	Healthy     bool
	ActiveConns int
	TotalConns  int64
	FailedConns int64
}

// generateConnID generates a unique connection ID
var connIDCounter atomic.Uint64

func generateConnID() string {
	return fmt.Sprintf("conn_%d_%d", time.Now().UnixNano(), connIDCounter.Add(1))
}
