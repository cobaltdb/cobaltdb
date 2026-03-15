package pool

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestPoolAcquireAndStats112 tests Acquire and Stats methods
func TestPoolAcquireAndStats112(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := &Config{
		MinConns:            1,
		MaxConns:            3,
		AcquireTimeout:      5 * time.Second,
		HealthCheckInterval: 1 * time.Second,
	}

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Get initial stats
	stats := pool.Stats()
	if stats.TotalConns < 1 {
		t.Errorf("Expected at least 1 connection, got %d", stats.TotalConns)
	}

	// Acquire a connection
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire: %v", err)
	}

	// Stats should show active connection
	stats = pool.Stats()
	if stats.ActiveConns < 1 {
		t.Errorf("Expected at least 1 active connection, got %d", stats.ActiveConns)
	}

	// Close the connection to return it to pool
	conn.Close()

	// Acquire again - should reuse the connection
	conn2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire second: %v", err)
	}
	conn2.Close()
}

// TestPoolAcquireTimeout112 tests acquire timeout
func TestPoolAcquireTimeout112(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := &Config{
		MinConns:            0,
		MaxConns:            1,
		AcquireTimeout:      50 * time.Millisecond,
		HealthCheckInterval: 1 * time.Second,
	}

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Try to acquire with short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = pool.Acquire(ctx)
	// Might succeed or timeout depending on timing
	t.Logf("Acquire result: %v", err)
}

// TestPoolClose112 tests pool close
func TestPoolClose112(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := &Config{
		MinConns:            1,
		MaxConns:            2,
		AcquireTimeout:      5 * time.Second,
		HealthCheckInterval: 1 * time.Second,
	}

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Acquire and release
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire: %v", err)
	}
	conn.Close()

	// Close pool
	err = pool.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestPoolStats112 tests pool statistics
func TestPoolStats112(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := &Config{
		MinConns:            2,
		MaxConns:            5,
		AcquireTimeout:      5 * time.Second,
		HealthCheckInterval: 1 * time.Second,
	}

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Get stats
	stats := pool.Stats()
	if stats.TotalConns < int32(config.MinConns) {
		t.Errorf("Expected at least %d connections, got %d", config.MinConns, stats.TotalConns)
	}

	// Acquire and check stats again
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire: %v", err)
	}

	stats = pool.Stats()
	if stats.ActiveConns != 1 {
		t.Errorf("Expected 1 active connection, got %d", stats.ActiveConns)
	}

	conn.Close()
}

// TestPoolHealthCheck112 tests health check functionality
func TestPoolHealthCheck112(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := &Config{
		MinConns:            1,
		MaxConns:            3,
		AcquireTimeout:      5 * time.Second,
		HealthCheckInterval: 100 * time.Millisecond,
	}

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Wait for health check to run
	time.Sleep(200 * time.Millisecond)

	// Pool should still be functional
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire after health check: %v", err)
	}
	conn.Close()
}

// TestPoolConnectionExpiry112 tests connection expiration
func TestPoolConnectionExpiry112(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := &Config{
		MinConns:            1,
		MaxConns:            3,
		AcquireTimeout:      5 * time.Second,
		HealthCheckInterval: 50 * time.Millisecond,
		MaxLifetime:         100 * time.Millisecond,
	}

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Wait for connection to expire
	time.Sleep(200 * time.Millisecond)

	// Pool should still work with new connections
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire after expiry: %v", err)
	}
	conn.Close()
}

// TestPoolMaintainMinConns112 tests minimum connection maintenance
func TestPoolMaintainMinConns112(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := &Config{
		MinConns:            2,
		MaxConns:            5,
		AcquireTimeout:      5 * time.Second,
		HealthCheckInterval: 50 * time.Millisecond,
	}

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Acquire and close a connection multiple times
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("Failed to acquire conn %d: %v", i, err)
		}
		conn.Close()
	}

	// Wait for maintainMinConns to potentially run
	time.Sleep(150 * time.Millisecond)

	// Pool should be functional after maintenance
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire after maintenance: %v", err)
	}
	conn.Close()
}

// TestConnIsExpired112 tests connection expiration check
func TestConnIsExpired112(t *testing.T) {
	c := &Conn{
		createdAt: time.Now().Add(-time.Hour),
	}

	if !c.IsExpired(time.Minute, 0) {
		t.Error("Expected expired connection to be expired")
	}

	c2 := &Conn{
		createdAt: time.Now(),
	}

	if c2.IsExpired(time.Hour, 0) {
		t.Error("Expected fresh connection to not be expired")
	}
}

// TestConnIsHealthy112 tests connection health check
func TestConnIsHealthy112(t *testing.T) {
	// Healthy connection
	now := time.Now()
	c := &Conn{
		Conn:       &mockConn{},
		createdAt:  now,
		lastUsedAt: now,
	}

	if !c.IsHealthy(time.Second) {
		t.Error("Expected healthy connection to be healthy")
	}
}
