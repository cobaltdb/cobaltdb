package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrPoolClosed    = errors.New("connection pool is closed")
	ErrPoolExhausted = errors.New("connection pool exhausted")
	ErrConnBusy      = errors.New("connection is busy")
)

// PoolConfig configures the connection pool
type PoolConfig struct {
	MinConns            int           // Minimum number of connections (default: 1)
	MaxConns            int           // Maximum number of connections (default: 10)
	MaxIdleTime         time.Duration // Maximum idle time before closing (default: 30 min)
	MaxLifetime         time.Duration // Maximum lifetime of a connection (default: 1 hour)
	AcquireTimeout      time.Duration // Timeout for acquiring connection (default: 30 sec)
	HealthCheckInterval time.Duration // Health check interval (default: 5 min)
}

// DefaultPoolConfig returns default pool configuration
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MinConns:            1,
		MaxConns:            10,
		MaxIdleTime:         30 * time.Minute,
		MaxLifetime:         1 * time.Hour,
		AcquireTimeout:      30 * time.Second,
		HealthCheckInterval: 5 * time.Minute,
	}
}

// PooledConnection represents a connection from the pool
type PooledConnection struct {
	id         uint64
	createdAt  time.Time
	lastUsedAt time.Time
	inUse      atomic.Bool
	healthy    atomic.Bool
	db         *DB
}

// ConnectionPool manages a pool of reusable connections
type ConnectionPool struct {
	db     *DB
	config *PoolConfig

	mu        sync.RWMutex
	conns     map[uint64]*PooledConnection
	available chan *PooledConnection
	nextID    uint64
	closed    bool

	// Background management
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// Stats
	stats PoolStats
}

// PoolStats holds connection pool statistics
type PoolStats struct {
	TotalConns      int32         `json:"total_connections"`
	ActiveConns     int32         `json:"active_connections"`
	IdleConns       int32         `json:"idle_connections"`
	WaitCount       uint64        `json:"wait_count"`
	WaitDuration    time.Duration `json:"total_wait_duration"`
	AcquireCount    uint64        `json:"acquire_count"`
	ReleaseCount    uint64        `json:"release_count"`
	CreateCount     uint64        `json:"create_count"`
	DestroyCount    uint64        `json:"destroy_count"`
	HealthCheckFail uint64        `json:"health_check_failures"`
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(db *DB, config *PoolConfig) (*ConnectionPool, error) {
	if config == nil {
		config = DefaultPoolConfig()
	}

	if config.MinConns < 1 {
		config.MinConns = 1
	}
	if config.MaxConns < config.MinConns {
		config.MaxConns = config.MinConns
	}

	pool := &ConnectionPool{
		db:        db,
		config:    config,
		conns:     make(map[uint64]*PooledConnection),
		available: make(chan *PooledConnection, config.MaxConns),
		stopCh:    make(chan struct{}),
	}

	// Create minimum connections
	for i := 0; i < config.MinConns; i++ {
		conn, err := pool.createConnection()
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to create initial connection: %w", err)
		}
		pool.available <- conn
	}

	// Start background maintenance (only if health check interval is set)
	if config.HealthCheckInterval > 0 {
		pool.wg.Add(1)
		go pool.maintenance()
	}

	return pool, nil
}

// Acquire gets a connection from the pool
func (p *ConnectionPool) Acquire(ctx context.Context) (*PooledConnection, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	start := time.Now()
	atomic.AddUint64(&p.stats.WaitCount, 1)

	// Try to get from available channel without blocking first
	select {
	case conn := <-p.available:
		if !conn.healthy.Load() {
			// Replace unhealthy connection
			p.destroyConnection(conn)
			newConn, err := p.createConnection()
			if err != nil {
				return nil, err
			}
			conn = newConn
		}
		conn.inUse.Store(true)
		conn.lastUsedAt = time.Now()

		waitTime := time.Since(start)
		atomic.AddInt32(&p.stats.ActiveConns, 1)
		atomic.AddInt32(&p.stats.IdleConns, -1)
		atomic.AddUint64(&p.stats.AcquireCount, 1)
		atomic.AddInt64((*int64)(&p.stats.WaitDuration), int64(waitTime))

		return conn, nil
	default:
		// No idle connection available - try to create a new one
		if newConn, err := p.createConnection(); err == nil {
			newConn.inUse.Store(true)
			newConn.lastUsedAt = time.Now()

			waitTime := time.Since(start)
			atomic.AddInt32(&p.stats.ActiveConns, 1)
			atomic.AddUint64(&p.stats.AcquireCount, 1)
			atomic.AddInt64((*int64)(&p.stats.WaitDuration), int64(waitTime))

			return newConn, nil
		}
	}

	// Pool is at max capacity, wait for a connection to be returned
	timeout := p.config.AcquireTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}

	select {
	case conn := <-p.available:
		if !conn.healthy.Load() {
			p.destroyConnection(conn)
			newConn, err := p.createConnection()
			if err != nil {
				return nil, err
			}
			conn = newConn
		}
		conn.inUse.Store(true)
		conn.lastUsedAt = time.Now()

		waitTime := time.Since(start)
		atomic.AddInt32(&p.stats.ActiveConns, 1)
		atomic.AddInt32(&p.stats.IdleConns, -1)
		atomic.AddUint64(&p.stats.AcquireCount, 1)
		atomic.AddInt64((*int64)(&p.stats.WaitDuration), int64(waitTime))

		return conn, nil

	case <-time.After(timeout):
		return nil, ErrPoolExhausted

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-p.stopCh:
		return nil, ErrPoolClosed
	}
}

// Release returns a connection to the pool
func (p *ConnectionPool) Release(conn *PooledConnection) {
	if conn == nil {
		return
	}

	if !conn.inUse.CompareAndSwap(true, false) {
		return // Already released
	}

	atomic.AddInt32(&p.stats.ActiveConns, -1)
	atomic.AddInt32(&p.stats.IdleConns, 1)
	atomic.AddUint64(&p.stats.ReleaseCount, 1)

	conn.lastUsedAt = time.Now()

	// Check if connection is still healthy
	if !conn.healthy.Load() || p.isExpired(conn) {
		p.destroyConnection(conn)
		// Create replacement if below minimum
		p.mu.Lock()
		if !p.closed && len(p.conns) < p.config.MinConns {
			p.mu.Unlock()
			if newConn, err := p.createConnection(); err == nil {
				select {
				case p.available <- newConn:
				default:
					p.destroyConnection(newConn)
				}
			}
		} else {
			p.mu.Unlock()
		}
		return
	}

	// Return to pool
	select {
	case p.available <- conn:
	default:
		// Pool is full, destroy this connection
		p.destroyConnection(conn)
	}
}

// createConnection creates a new pooled connection
func (p *ConnectionPool) createConnection() (*PooledConnection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrPoolClosed
	}

	if len(p.conns) >= p.config.MaxConns {
		return nil, ErrPoolExhausted
	}

	id := atomic.AddUint64(&p.nextID, 1)
	conn := &PooledConnection{
		id:         id,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
		db:         p.db,
	}
	conn.inUse.Store(false)
	conn.healthy.Store(true)

	p.conns[id] = conn
	atomic.AddInt32(&p.stats.TotalConns, 1)
	atomic.AddInt32(&p.stats.IdleConns, 1)
	atomic.AddUint64(&p.stats.CreateCount, 1)

	return conn, nil
}

// destroyConnection removes a connection from the pool
func (p *ConnectionPool) destroyConnection(conn *PooledConnection) {
	p.mu.Lock()
	delete(p.conns, conn.id)
	p.mu.Unlock()

	conn.healthy.Store(false)

	atomic.AddInt32(&p.stats.TotalConns, -1)
	atomic.AddInt32(&p.stats.IdleConns, -1)
	atomic.AddUint64(&p.stats.DestroyCount, 1)
}

// isExpired checks if a connection has exceeded its lifetime or idle time
func (p *ConnectionPool) isExpired(conn *PooledConnection) bool {
	now := time.Now()
	if now.Sub(conn.createdAt) > p.config.MaxLifetime {
		return true
	}
	if now.Sub(conn.lastUsedAt) > p.config.MaxIdleTime {
		return true
	}
	return false
}

// maintenance runs background maintenance tasks
func (p *ConnectionPool) maintenance() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.performMaintenance()
		}
	}
}

// performMaintenance performs health checks and cleanup
func (p *ConnectionPool) performMaintenance() {
	p.mu.RLock()
	conns := make([]*PooledConnection, 0, len(p.conns))
	for _, conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.RUnlock()

	// Check each connection
	for _, conn := range conns {
		if conn.inUse.Load() {
			continue // Skip busy connections
		}

		// Check health
		if !p.healthCheck(conn) {
			conn.healthy.Store(false)
			atomic.AddUint64(&p.stats.HealthCheckFail, 1)
		}

		// Expire old connections (but maintain minimum)
		if p.isExpired(conn) {
			p.mu.RLock()
			count := len(p.conns)
			p.mu.RUnlock()

			if count > p.config.MinConns {
				p.destroyConnection(conn)
			}
		}
	}

	// Ensure minimum connections
	p.mu.Lock()
	currentCount := len(p.conns)
	p.mu.Unlock()

	for currentCount < p.config.MinConns {
		if newConn, err := p.createConnection(); err == nil {
			select {
			case p.available <- newConn:
			default:
				p.destroyConnection(newConn)
			}
		} else {
			break
		}
		currentCount++
	}
}

// healthCheck performs a health check on a connection
func (p *ConnectionPool) healthCheck(conn *PooledConnection) bool {
	if p.db == nil {
		return false
	}

	// Perform actual database health check with SELECT 1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to execute a simple query to verify connectivity
	// This will fail if the database is not accessible
	_, err := p.db.Exec(ctx, "SELECT 1")
	if err != nil {
		// Database is not responding
		return false
	}

	return true
}

// Stats returns pool statistics
func (p *ConnectionPool) Stats() PoolStats {
	return PoolStats{
		TotalConns:      atomic.LoadInt32(&p.stats.TotalConns),
		ActiveConns:     atomic.LoadInt32(&p.stats.ActiveConns),
		IdleConns:       atomic.LoadInt32(&p.stats.IdleConns),
		WaitCount:       atomic.LoadUint64(&p.stats.WaitCount),
		WaitDuration:    time.Duration(atomic.LoadInt64((*int64)(&p.stats.WaitDuration))),
		AcquireCount:    atomic.LoadUint64(&p.stats.AcquireCount),
		ReleaseCount:    atomic.LoadUint64(&p.stats.ReleaseCount),
		CreateCount:     atomic.LoadUint64(&p.stats.CreateCount),
		DestroyCount:    atomic.LoadUint64(&p.stats.DestroyCount),
		HealthCheckFail: atomic.LoadUint64(&p.stats.HealthCheckFail),
	}
}

// Close closes the connection pool
func (p *ConnectionPool) Close() error {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})

	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	// Close all connections
	for _, conn := range p.conns {
		conn.healthy.Store(false)
	}
	close(p.available)

	return nil
}

// DB returns the underlying database
func (pc *PooledConnection) DB() *DB {
	return pc.db
}

// ID returns the connection ID
func (pc *PooledConnection) ID() uint64 {
	return pc.id
}

// IsHealthy returns true if the connection is healthy
func (pc *PooledConnection) IsHealthy() bool {
	return pc.healthy.Load()
}

// MarkUnhealthy marks the connection as unhealthy
func (pc *PooledConnection) MarkUnhealthy() {
	pc.healthy.Store(false)
}
