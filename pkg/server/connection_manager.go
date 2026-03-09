// Advanced Connection Management
// Handles connection pooling, limits, and monitoring

package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrConnectionLimitReached = errors.New("connection limit reached")
	ErrConnectionTimeout      = errors.New("connection timeout")
	ErrServerShuttingDown     = errors.New("server is shutting down")
)

// ConnectionManagerConfig configures connection management
type ConnectionManagerConfig struct {
	// Max connections (default: 1000)
	MaxConnections int

	// Max connections per client IP (default: 100)
	MaxConnectionsPerIP int

	// Connection timeout (default: 30s)
	ConnectionTimeout time.Duration

	// Idle timeout (default: 300s)
	IdleTimeout time.Duration

	// Read timeout (default: 300s)
	ReadTimeout time.Duration

	// Write timeout (default: 60s)
	WriteTimeout time.Duration

	// Enable connection limits (default: true)
	EnableLimits bool

	// Enable idle timeout (default: true)
	EnableIdleTimeout bool
}

// DefaultConnectionManagerConfig returns default configuration
func DefaultConnectionManagerConfig() *ConnectionManagerConfig {
	return &ConnectionManagerConfig{
		MaxConnections:      1000,
		MaxConnectionsPerIP: 100,
		ConnectionTimeout:   30 * time.Second,
		IdleTimeout:         300 * time.Second,
		ReadTimeout:         300 * time.Second,
		WriteTimeout:        60 * time.Second,
		EnableLimits:        true,
		EnableIdleTimeout:   true,
	}
}

// ConnectionManager manages client connections
type ConnectionManager struct {
	config *ConnectionManagerConfig

	// All active connections
	connections   map[uint64]*ManagedConn
	connectionsMu sync.RWMutex

	// Connections per IP
	ipConnections   map[string]int
	ipConnectionsMu sync.RWMutex

	// Connection ID counter
	nextID atomic.Uint64

	// Shutdown state
	shuttingDown atomic.Bool
	shutdownCh   chan struct{}
	stopOnce     sync.Once

	// Statistics
	stats ConnectionStats
}

// ManagedConn wraps a net.Conn with metadata
type ManagedConn struct {
	ID         uint64
	Conn       net.Conn
	ClientAddr string
	ClientIP   string
	CreatedAt  time.Time
	LastActive time.Time
	mu         sync.RWMutex
}

// UpdateActivity updates the last activity timestamp
func (mc *ManagedConn) UpdateActivity() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.LastActive = time.Now()
}

// IsIdle checks if connection is idle
func (mc *ManagedConn) IsIdle(timeout time.Duration) bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return time.Since(mc.LastActive) > timeout
}

// ConnectionStats holds connection statistics
type ConnectionStats struct {
	TotalConnections     atomic.Uint64
	ActiveConnections    atomic.Uint64
	RejectedConnections  atomic.Uint64
	ClosedIdleConnections atomic.Uint64
	TotalBytesIn         atomic.Uint64
	TotalBytesOut        atomic.Uint64
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(config *ConnectionManagerConfig) *ConnectionManager {
	if config == nil {
		config = DefaultConnectionManagerConfig()
	}

	cm := &ConnectionManager{
		config:        config,
		connections:   make(map[uint64]*ManagedConn),
		ipConnections: make(map[string]int),
		shutdownCh:    make(chan struct{}),
	}

	// Start idle connection cleanup
	if config.EnableIdleTimeout {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("recovered panic in idle cleanup: %v", r)
				}
			}()
			cm.idleCleanupLoop()
		}()
	}

	return cm
}

// Stop stops the connection manager
func (cm *ConnectionManager) Stop() {
	cm.shuttingDown.Store(true)
	cm.stopOnce.Do(func() { close(cm.shutdownCh) })

	// Close all connections
	cm.connectionsMu.Lock()
	for _, mc := range cm.connections {
		mc.Conn.Close()
	}
	cm.connections = make(map[uint64]*ManagedConn)
	cm.connectionsMu.Unlock()
}

// Accept accepts a new connection
func (cm *ConnectionManager) Accept(conn net.Conn) (*ManagedConn, error) {
	if cm.shuttingDown.Load() {
		conn.Close()
		return nil, ErrServerShuttingDown
	}

	// Check connection limits
	if cm.config.EnableLimits {
		if err := cm.checkLimits(conn); err != nil {
			cm.stats.RejectedConnections.Add(1)
			conn.Close()
			return nil, err
		}
	}

	// Create managed connection
	clientAddr := conn.RemoteAddr().String()
	clientIP, _, _ := net.SplitHostPort(clientAddr)
	if clientIP == "" {
		clientIP = clientAddr
	}

	id := cm.nextID.Add(1)
	mc := &ManagedConn{
		ID:         id,
		Conn:       conn,
		ClientAddr: clientAddr,
		ClientIP:   clientIP,
		CreatedAt:  time.Now(),
		LastActive: time.Now(),
	}

	// Register connection
	cm.connectionsMu.Lock()
	cm.connections[id] = mc
	cm.connectionsMu.Unlock()

	// Update IP count
	cm.ipConnectionsMu.Lock()
	cm.ipConnections[clientIP]++
	cm.ipConnectionsMu.Unlock()

	// Update stats
	cm.stats.TotalConnections.Add(1)
	cm.stats.ActiveConnections.Add(1)

	return mc, nil
}

// Release releases a connection
func (cm *ConnectionManager) Release(mc *ManagedConn) {
	if mc == nil {
		return
	}

	// Unregister connection
	cm.connectionsMu.Lock()
	delete(cm.connections, mc.ID)
	cm.connectionsMu.Unlock()

	// Update IP count
	cm.ipConnectionsMu.Lock()
	cm.ipConnections[mc.ClientIP]--
	if cm.ipConnections[mc.ClientIP] <= 0 {
		delete(cm.ipConnections, mc.ClientIP)
	}
	cm.ipConnectionsMu.Unlock()

	// Close connection
	mc.Conn.Close()

	// Update stats
	cm.stats.ActiveConnections.Add(^uint64(0)) // Decrement
}

// checkLimits checks if connection is within limits
func (cm *ConnectionManager) checkLimits(conn net.Conn) error {
	// Check global limit
	cm.connectionsMu.RLock()
	currentCount := len(cm.connections)
	cm.connectionsMu.RUnlock()

	if currentCount >= cm.config.MaxConnections {
		return ErrConnectionLimitReached
	}

	// Check per-IP limit
	clientAddr := conn.RemoteAddr().String()
	clientIP, _, _ := net.SplitHostPort(clientAddr)
	if clientIP == "" {
		clientIP = clientAddr
	}

	cm.ipConnectionsMu.RLock()
	ipCount := cm.ipConnections[clientIP]
	cm.ipConnectionsMu.RUnlock()

	if ipCount >= cm.config.MaxConnectionsPerIP {
		return fmt.Errorf("connection limit reached for IP %s", clientIP)
	}

	return nil
}

// idleCleanupLoop periodically cleans up idle connections
func (cm *ConnectionManager) idleCleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.shutdownCh:
			return
		case <-ticker.C:
			cm.cleanupIdleConnections()
		}
	}
}

// cleanupIdleConnections closes idle connections
func (cm *ConnectionManager) cleanupIdleConnections() {
	// Phase 1: Collect idle connections under the connection lock only
	var idleConns []*ManagedConn
	cm.connectionsMu.Lock()
	for id, mc := range cm.connections {
		if mc.IsIdle(cm.config.IdleTimeout) {
			delete(cm.connections, id)
			idleConns = append(idleConns, mc)
		}
	}
	cm.connectionsMu.Unlock()

	// Close connections outside the lock to avoid I/O under lock
	for _, mc := range idleConns {
		mc.Conn.Close()
	}

	// Phase 2: Update IP counts separately to avoid nested lock ordering
	if len(idleConns) > 0 {
		cm.ipConnectionsMu.Lock()
		for _, mc := range idleConns {
			cm.ipConnections[mc.ClientIP]--
			if cm.ipConnections[mc.ClientIP] <= 0 {
				delete(cm.ipConnections, mc.ClientIP)
			}
		}
		cm.ipConnectionsMu.Unlock()

		// Phase 3: Update stats
		for range idleConns {
			cm.stats.ClosedIdleConnections.Add(1)
			cm.stats.ActiveConnections.Add(^uint64(0))
		}
	}
}

// GetConnectionCount returns current connection count
func (cm *ConnectionManager) GetConnectionCount() int {
	cm.connectionsMu.RLock()
	defer cm.connectionsMu.RUnlock()
	return len(cm.connections)
}

// GetConnectionCountForIP returns connection count for a specific IP
func (cm *ConnectionManager) GetConnectionCountForIP(ip string) int {
	cm.ipConnectionsMu.RLock()
	defer cm.ipConnectionsMu.RUnlock()
	return cm.ipConnections[ip]
}

// GetActiveConnections returns all active connections
func (cm *ConnectionManager) GetActiveConnections() []*ManagedConn {
	cm.connectionsMu.RLock()
	defer cm.connectionsMu.RUnlock()

	conns := make([]*ManagedConn, 0, len(cm.connections))
	for _, mc := range cm.connections {
		conns = append(conns, mc)
	}
	return conns
}

// GetStats returns connection statistics
func (cm *ConnectionManager) GetStats() ConnectionStatsInfo {
	return ConnectionStatsInfo{
		TotalConnections:      cm.stats.TotalConnections.Load(),
		ActiveConnections:     cm.stats.ActiveConnections.Load(),
		RejectedConnections:   cm.stats.RejectedConnections.Load(),
		ClosedIdleConnections: cm.stats.ClosedIdleConnections.Load(),
	}
}

// ConnectionStatsInfo holds connection statistics
type ConnectionStatsInfo struct {
	TotalConnections      uint64 `json:"total_connections"`
	ActiveConnections     uint64 `json:"active_connections"`
	RejectedConnections   uint64 `json:"rejected_connections"`
	ClosedIdleConnections uint64 `json:"closed_idle_connections"`
}

// Blacklist manages blacklisted IPs
type Blacklist struct {
	ips   map[string]time.Time // IP -> expiration time
	mu    sync.RWMutex
}

// NewBlacklist creates a new blacklist
func NewBlacklist() *Blacklist {
	return &Blacklist{
		ips: make(map[string]time.Time),
	}
}

// Add adds an IP to the blacklist
func (bl *Blacklist) Add(ip string, duration time.Duration) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	bl.ips[ip] = time.Now().Add(duration)
}

// Remove removes an IP from the blacklist
func (bl *Blacklist) Remove(ip string) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	delete(bl.ips, ip)
}

// IsBlacklisted checks if an IP is blacklisted
func (bl *Blacklist) IsBlacklisted(ip string) bool {
	bl.mu.RLock()
	expiration, exists := bl.ips[ip]
	bl.mu.RUnlock()

	if !exists {
		return false
	}

	// Check if expired
	if time.Now().After(expiration) {
		bl.Remove(ip)
		return false
	}

	return true
}

// Cleanup removes expired entries
func (bl *Blacklist) Cleanup() {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	now := time.Now()
	for ip, expiration := range bl.ips {
		if now.After(expiration) {
			delete(bl.ips, ip)
		}
	}
}

// Whitelist manages whitelisted IPs
type Whitelist struct {
	ips map[string]bool
	mu  sync.RWMutex
}

// NewWhitelist creates a new whitelist
func NewWhitelist() *Whitelist {
	return &Whitelist{
		ips: make(map[string]bool),
	}
}

// Add adds an IP to the whitelist
func (wl *Whitelist) Add(ip string) {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	wl.ips[ip] = true
}

// Remove removes an IP from the whitelist
func (wl *Whitelist) Remove(ip string) {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	delete(wl.ips, ip)
}

// IsWhitelisted checks if an IP is whitelisted
func (wl *Whitelist) IsWhitelisted(ip string) bool {
	wl.mu.RLock()
	defer wl.mu.RUnlock()
	return wl.ips[ip]
}

// ConnectionThrottler throttles connections from specific IPs
type ConnectionThrottler struct {
	attempts   map[string][]time.Time // IP -> attempt timestamps
	mu         sync.RWMutex
	window     time.Duration
	maxAttempts int
}

// NewConnectionThrottler creates a new connection throttler
func NewConnectionThrottler(window time.Duration, maxAttempts int) *ConnectionThrottler {
	return &ConnectionThrottler{
		attempts:    make(map[string][]time.Time),
		window:      window,
		maxAttempts: maxAttempts,
	}
}

// RecordAttempt records a connection attempt
func (ct *ConnectionThrottler) RecordAttempt(ip string) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	now := time.Now()
	ct.attempts[ip] = append(ct.attempts[ip], now)

	// Clean old attempts
	cutoff := now.Add(-ct.window)
	valid := ct.attempts[ip][:0]
	for _, t := range ct.attempts[ip] {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
	ct.attempts[ip] = valid
}

// IsThrottled checks if an IP should be throttled
func (ct *ConnectionThrottler) IsThrottled(ip string) bool {
	ct.mu.RLock()
	defer ct.mu.RUnlock()

	return len(ct.attempts[ip]) >= ct.maxAttempts
}
