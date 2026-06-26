// Package pool provides connection pooling for CobaltDB
package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPoolClosed is returned when the pool is closed
	ErrPoolClosed = errors.New("connection pool is closed")
	// ErrPoolExhausted is returned when the pool is at capacity
	ErrPoolExhausted = errors.New("connection pool exhausted")
	// ErrConnClosed is returned when a connection is closed
	ErrConnClosed = errors.New("connection is closed")
	// ErrTimeout is returned when acquire times out
	ErrTimeout = errors.New("connection acquire timeout")
)

const (
	maxPoolConnections = 100000
	maxPoolWaiters     = 1000000
)

// Config holds pool configuration
type Config struct {
	// MinConns is the minimum number of connections to maintain
	MinConns int
	// MaxConns is the maximum number of connections allowed
	MaxConns int
	// MaxIdleTime is the maximum time a connection can be idle
	MaxIdleTime time.Duration
	// MaxLifetime is the maximum lifetime of a connection
	MaxLifetime time.Duration
	// HealthCheckInterval is how often to check connection health
	HealthCheckInterval time.Duration
	// HealthCheckTimeout is the timeout for health checks
	HealthCheckTimeout time.Duration
	// AcquireTimeout is the timeout for acquiring a connection
	AcquireTimeout time.Duration
	// WaitQueueSize is the maximum number of waiting clients
	WaitQueueSize int
}

// DefaultConfig returns default pool configuration
func DefaultConfig() *Config {
	return &Config{
		MinConns:            5,
		MaxConns:            100,
		MaxIdleTime:         30 * time.Minute,
		MaxLifetime:         1 * time.Hour,
		HealthCheckInterval: 5 * time.Minute,
		HealthCheckTimeout:  5 * time.Second,
		AcquireTimeout:      10 * time.Second,
		WaitQueueSize:       1000,
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.MinConns < 0 {
		return fmt.Errorf("min connections cannot be negative")
	}
	if c.MaxConns <= 0 {
		return fmt.Errorf("max connections must be positive")
	}
	if c.MaxConns > maxPoolConnections {
		return fmt.Errorf("max connections exceeds maximum (%d)", maxPoolConnections)
	}
	if c.MinConns > c.MaxConns {
		return fmt.Errorf("min connections cannot exceed max connections")
	}
	if c.MaxIdleTime < 0 {
		return fmt.Errorf("max idle time cannot be negative")
	}
	if c.MaxLifetime < 0 {
		return fmt.Errorf("max lifetime cannot be negative")
	}
	if c.HealthCheckInterval < 0 {
		return fmt.Errorf("health check interval cannot be negative")
	}
	if c.HealthCheckTimeout < 0 {
		return fmt.Errorf("health check timeout cannot be negative")
	}
	if c.AcquireTimeout < 0 {
		return fmt.Errorf("acquire timeout cannot be negative")
	}
	if c.WaitQueueSize < 0 {
		return fmt.Errorf("wait queue size cannot be negative")
	}
	if c.WaitQueueSize > maxPoolWaiters {
		return fmt.Errorf("wait queue size exceeds maximum (%d)", maxPoolWaiters)
	}
	return nil
}

// Conn represents a pooled connection
type Conn struct {
	net.Conn
	pool           *Pool
	createdAt      time.Time
	lastUsedAtNano int64
	inUse          int32
	closed         int32
	id             uint64
}

// newConn wraps a net.Conn in a pooled connection
func newConn(conn net.Conn, pool *Pool, id uint64) *Conn {
	now := time.Now()
	return &Conn{
		Conn:           conn,
		pool:           pool,
		createdAt:      now,
		lastUsedAtNano: now.UnixNano(),
		id:             id,
	}
}

// Release returns the connection to the pool
func (c *Conn) Release() {
	if c.pool == nil || atomic.LoadInt32(&c.closed) == 1 {
		return
	}
	c.pool.release(c)
}

// Close closes the connection and removes it from the pool
func (c *Conn) Close() error {
	if c.pool == nil {
		if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
			return c.Conn.Close()
		}
		return nil
	}
	if atomic.LoadInt32(&c.closed) == 0 {
		return c.pool.removeConn(c)
	}
	return nil
}

func (c *Conn) closeUnderlying() error {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return c.Conn.Close()
	}
	return nil
}

// IsHealthy checks if the connection is healthy
func (c *Conn) IsHealthy(timeout time.Duration) bool {
	if atomic.LoadInt32(&c.closed) == 1 {
		return false
	}

	// Set read deadline
	if err := c.Conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return false
	}
	defer func() {
		_ = c.Conn.SetReadDeadline(time.Time{})
	}()

	// Try to read (this is a simple health check)
	// In production, you might send a ping/pong
	return true
}

// IsExpired checks if the connection has exceeded its lifetime
func (c *Conn) IsExpired(maxLifetime, maxIdleTime time.Duration) bool {
	if maxLifetime > 0 && time.Since(c.createdAt) > maxLifetime {
		return true
	}
	if maxIdleTime > 0 && atomic.LoadInt32(&c.inUse) == 0 {
		lastUsed := atomic.LoadInt64(&c.lastUsedAtNano)
		if time.Now().UnixNano()-lastUsed > int64(maxIdleTime) {
			return true
		}
	}
	return false
}

// Pool manages a pool of connections
type Pool struct {
	config *Config

	// Connection factory
	dialer func() (net.Conn, error)

	// Pool state
	mu        sync.RWMutex
	conns     []*Conn
	available chan *Conn
	closed    int32
	connIDSeq uint64

	// Waiting clients
	waiters   []chan *Conn
	waitersMu sync.Mutex

	// Metrics
	stats Stats

	// Background tasks
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Stats holds pool statistics
type Stats struct {
	TotalConns    int32  `json:"total_connections"`
	IdleConns     int32  `json:"idle_connections"`
	ActiveConns   int32  `json:"active_connections"`
	WaitQueueLen  int32  `json:"wait_queue_length"`
	TotalAcquires uint64 `json:"total_acquires"`
	TotalReleases uint64 `json:"total_releases"`
	TotalTimeouts uint64 `json:"total_timeouts"`
	TotalCreates  uint64 `json:"total_creates"`
	TotalDestroys uint64 `json:"total_destroys"`
}

// New creates a new connection pool
func New(config *Config, dialer func() (net.Conn, error)) (*Pool, error) {
	if config == nil {
		config = DefaultConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}
	config = normalizeConfig(config)

	if dialer == nil {
		return nil, fmt.Errorf("dialer function is required")
	}

	pool := &Pool{
		config:    config,
		dialer:    dialer,
		conns:     make([]*Conn, 0, config.MaxConns),
		available: make(chan *Conn, config.MaxConns),
		waiters:   make([]chan *Conn, 0),
		stopCh:    make(chan struct{}),
	}

	// Create minimum connections
	for i := 0; i < config.MinConns; i++ {
		if err := pool.createConnection(); err != nil {
			err = errors.Join(err, pool.Close())
			return nil, fmt.Errorf("failed to create initial connections: %w", err)
		}
	}

	// Start background tasks
	pool.wg.Add(1)
	go pool.healthCheckLoop()

	pool.wg.Add(1)
	go pool.maintainMinConns()

	return pool, nil
}

func normalizeConfig(config *Config) *Config {
	defaults := DefaultConfig()
	normalized := *config
	if normalized.MaxIdleTime <= 0 {
		normalized.MaxIdleTime = defaults.MaxIdleTime
	}
	if normalized.MaxLifetime <= 0 {
		normalized.MaxLifetime = defaults.MaxLifetime
	}
	if normalized.AcquireTimeout <= 0 {
		normalized.AcquireTimeout = defaults.AcquireTimeout
	}
	if normalized.HealthCheckInterval <= 0 {
		normalized.HealthCheckInterval = defaults.HealthCheckInterval
	}
	if normalized.HealthCheckTimeout <= 0 {
		normalized.HealthCheckTimeout = defaults.HealthCheckTimeout
	}
	if normalized.WaitQueueSize <= 0 {
		normalized.WaitQueueSize = defaults.WaitQueueSize
	}
	return &normalized
}

// Close shuts down the pool
func (p *Pool) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil // Already closed
	}

	close(p.stopCh)
	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Close all connections
	var closeErrs []error
	for _, conn := range p.conns {
		if err := conn.closeUnderlying(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("close connection %d: %w", conn.id, err))
		}
	}
	p.conns = p.conns[:0]

	// Drain the available channel WITHOUT closing it. release() and
	// createConnection() may still send here from caller goroutines that
	// Close() does not wait for (p.wg only tracks background loops); closing
	// would panic those sends ("send on closed channel"). They instead observe
	// the closed flag and route through removeConn().
	for {
		select {
		case conn := <-p.available:
			if conn != nil {
				_ = conn.closeUnderlying()
			}
		default:
			goto availableDrained
		}
	}
availableDrained:

	// Notify waiting clients
	p.waitersMu.Lock()
	for _, ch := range p.waiters {
		close(ch)
	}
	p.waiters = p.waiters[:0]
	p.waitersMu.Unlock()

	return errors.Join(closeErrs...)
}

// Acquire gets a connection from the pool
func (p *Pool) Acquire(ctx context.Context) (*Conn, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, ErrPoolClosed
	}

	atomic.AddUint64(&p.stats.TotalAcquires, 1)

	// Try to get from available channel
	select {
	case conn := <-p.available:
		if conn != nil && atomic.LoadInt32(&conn.closed) == 0 {
			atomic.StoreInt32(&conn.inUse, 1)
			atomic.StoreInt64(&conn.lastUsedAtNano, time.Now().UnixNano())
			atomic.AddInt32(&p.stats.IdleConns, -1)
			atomic.AddInt32(&p.stats.ActiveConns, 1)
			return conn, nil
		}
	default:
	}

	// Try to create a new connection
	p.mu.Lock()
	if len(p.conns) < p.config.MaxConns {
		p.mu.Unlock()
		if err := p.createConnection(); err != nil {
			return nil, err
		}

		// Get the newly created connection
		select {
		case conn := <-p.available:
			if conn != nil {
				atomic.StoreInt32(&conn.inUse, 1)
				atomic.StoreInt64(&conn.lastUsedAtNano, time.Now().UnixNano())
				atomic.AddInt32(&p.stats.IdleConns, -1)
				atomic.AddInt32(&p.stats.ActiveConns, 1)
				return conn, nil
			}
		default:
		}
	} else {
		p.mu.Unlock()
	}

	// Wait for a connection to become available
	return p.waitForConnection(ctx)
}

// waitForConnection waits for an available connection
func (p *Pool) waitForConnection(ctx context.Context) (*Conn, error) {
	// Create waiter channel
	waiter := make(chan *Conn, 1)

	p.waitersMu.Lock()
	if len(p.waiters) >= p.config.WaitQueueSize {
		p.waitersMu.Unlock()
		atomic.AddUint64(&p.stats.TotalTimeouts, 1)
		return nil, ErrPoolExhausted
	}
	p.waiters = append(p.waiters, waiter)
	atomic.AddInt32(&p.stats.WaitQueueLen, 1)
	p.waitersMu.Unlock()

	// Wait with timeout
	timeout := p.config.AcquireTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case conn := <-waiter:
		// release() (or Close()) popped us from p.waiters and decremented
		// WaitQueueLen, then delivered this connection (nil from Close()). The
		// connection is counted as idle; transition it to active.
		if conn == nil {
			return nil, ErrPoolClosed
		}
		atomic.StoreInt32(&conn.inUse, 1)
		atomic.StoreInt64(&conn.lastUsedAtNano, time.Now().UnixNano())
		atomic.AddInt32(&p.stats.IdleConns, -1)
		atomic.AddInt32(&p.stats.ActiveConns, 1)
		return conn, nil
	case <-timer.C:
		atomic.AddUint64(&p.stats.TotalTimeouts, 1)
		return nil, p.abandonWaiter(waiter, ErrTimeout)
	case <-ctx.Done():
		return nil, p.abandonWaiter(waiter, ctx.Err())
	}
}

// abandonWaiter removes a timed-out / cancelled waiter from the queue. If a
// concurrent release() already popped this waiter (and therefore will deliver a
// connection into its buffered channel), abandonWaiter blocks to reclaim that
// connection and returns it to the pool so it is not lost from circulation.
func (p *Pool) abandonWaiter(waiter chan *Conn, err error) error {
	p.waitersMu.Lock()
	removed := false
	for i, w := range p.waiters {
		if w == waiter {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			removed = true
			break
		}
	}
	p.waitersMu.Unlock()

	if removed {
		// No release() popped us, so no connection is in flight to this waiter.
		atomic.AddInt32(&p.stats.WaitQueueLen, -1)
		return err
	}

	// A release() (or Close()) already popped us under waitersMu and will send
	// exactly one value into the buffered waiter channel (a connection, or nil
	// from Close()); it already adjusted WaitQueueLen. The connection is counted
	// as idle — re-queue it (without re-running release()'s active accounting)
	// so it is not lost from circulation.
	if conn := <-waiter; conn != nil {
		p.requeueIdleConn(conn)
	}
	return err
}

// sendToWaiter attempts a non-blocking send on the waiter channel.
// Returns true if the waiter received the connection; false if the waiter
// has already timed out (no receiver on the channel).
func (p *Pool) sendToWaiter(waiter chan *Conn, conn *Conn) bool {
	select {
	case waiter <- conn:
		return true
	default:
		return false
	}
}

// release returns a connection to the pool
func (p *Pool) release(conn *Conn) {
	if atomic.LoadInt32(&p.closed) == 1 {
		_ = p.removeConn(conn)
		return
	}

	atomic.StoreInt32(&conn.inUse, 0)
	atomic.StoreInt64(&conn.lastUsedAtNano, time.Now().UnixNano())
	atomic.AddInt32(&p.stats.ActiveConns, -1)
	atomic.AddInt32(&p.stats.IdleConns, 1)
	atomic.AddUint64(&p.stats.TotalReleases, 1)

	// The connection is now idle (counted in IdleConns). Hand it off — to a
	// waiter, or to the idle channel — but never to both.
	p.requeueIdleConn(conn)
}

// requeueIdleConn places an already-idle connection (counted in IdleConns,
// inUse==0) either directly into a waiting client or back into the idle
// channel. It must hand the connection to exactly one destination. A waiter
// that receives it transitions it idle->active; a waiter that has abandoned
// reclaims and re-queues it via this same path.
func (p *Pool) requeueIdleConn(conn *Conn) {
	if atomic.LoadInt32(&p.closed) == 1 {
		_ = p.removeConn(conn) // inUse==0 -> decrements IdleConns
		return
	}

	// Try to give to a waiting client first.
	p.waitersMu.Lock()
	if len(p.waiters) > 0 {
		waiter := p.waiters[0]
		p.waiters = p.waiters[1:]
		atomic.AddInt32(&p.stats.WaitQueueLen, -1)
		p.waitersMu.Unlock()
		// The waiter channel is buffered (cap 1) so this always succeeds and
		// the connection is now owned exclusively by that waiter. Do NOT also
		// place it in the idle channel.
		p.sendToWaiter(waiter, conn)
		return
	}
	p.waitersMu.Unlock()

	// No waiters: return to the idle channel.
	select {
	case p.available <- conn:
	default:
		// Channel full: drop the connection (removeConn decrements IdleConns).
		_ = p.removeConn(conn)
	}
}

// createConnection creates a new connection
func (p *Pool) createConnection() error {
	netConn, err := p.dialer()
	if err != nil {
		return err
	}

	id := atomic.AddUint64(&p.connIDSeq, 1)
	conn := newConn(netConn, p, id)

	p.mu.Lock()
	p.conns = append(p.conns, conn)
	atomic.AddInt32(&p.stats.TotalConns, 1)
	atomic.AddInt32(&p.stats.IdleConns, 1)
	atomic.AddUint64(&p.stats.TotalCreates, 1)
	p.mu.Unlock()

	// Add to available channel
	select {
	case p.available <- conn:
	default:
	}

	return nil
}

// removeConn removes a connection from the pool
func (p *Pool) removeConn(conn *Conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, c := range p.conns {
		if c == conn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			atomic.AddInt32(&p.stats.TotalConns, -1)
			if atomic.LoadInt32(&conn.inUse) == 0 {
				atomic.AddInt32(&p.stats.IdleConns, -1)
			} else {
				atomic.AddInt32(&p.stats.ActiveConns, -1)
			}
			atomic.AddUint64(&p.stats.TotalDestroys, 1)
			break
		}
	}

	return conn.closeUnderlying()
}

// healthCheckLoop periodically checks connection health
func (p *Pool) healthCheckLoop() {
	defer p.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			// Log panic but don't crash the pool
			_ = r
		}
	}()

	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.performHealthCheck()
		}
	}
}

// performHealthCheck checks and cleans up unhealthy connections
func (p *Pool) performHealthCheck() {
	p.mu.Lock()
	conns := make([]*Conn, len(p.conns))
	copy(conns, p.conns)
	p.mu.Unlock()

	for _, conn := range conns {
		// Check if connection is expired
		if conn.IsExpired(p.config.MaxLifetime, p.config.MaxIdleTime) {
			_ = p.removeConn(conn)
			continue
		}

		// Check health if idle
		if atomic.LoadInt32(&conn.inUse) == 0 {
			if !conn.IsHealthy(p.config.HealthCheckTimeout) {
				_ = p.removeConn(conn)
			}
		}
	}
}

// maintainMinConns ensures minimum connections are maintained
func (p *Pool) maintainMinConns() {
	defer p.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.mu.RLock()
			connCount := len(p.conns)
			p.mu.RUnlock()

			if connCount < p.config.MinConns {
				for i := connCount; i < p.config.MinConns; i++ {
					if err := p.createConnection(); err != nil {
						break
					}
				}
			}
		}
	}
}

// Stats returns pool statistics
func (p *Pool) Stats() Stats {
	return Stats{
		TotalConns:    atomic.LoadInt32(&p.stats.TotalConns),
		IdleConns:     atomic.LoadInt32(&p.stats.IdleConns),
		ActiveConns:   atomic.LoadInt32(&p.stats.ActiveConns),
		WaitQueueLen:  atomic.LoadInt32(&p.stats.WaitQueueLen),
		TotalAcquires: atomic.LoadUint64(&p.stats.TotalAcquires),
		TotalReleases: atomic.LoadUint64(&p.stats.TotalReleases),
		TotalTimeouts: atomic.LoadUint64(&p.stats.TotalTimeouts),
		TotalCreates:  atomic.LoadUint64(&p.stats.TotalCreates),
		TotalDestroys: atomic.LoadUint64(&p.stats.TotalDestroys),
	}
}
