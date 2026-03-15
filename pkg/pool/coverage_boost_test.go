package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// mockConnWithClose is a mock that tracks close calls
type mockConnWithClose struct {
	closed bool
	mu     sync.Mutex
}

func (m *mockConnWithClose) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockConnWithClose) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockConnWithClose) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
func (m *mockConnWithClose) LocalAddr() net.Addr                { return nil }
func (m *mockConnWithClose) RemoteAddr() net.Addr               { return nil }
func (m *mockConnWithClose) SetDeadline(t time.Time) error      { return nil }
func (m *mockConnWithClose) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConnWithClose) SetWriteDeadline(t time.Time) error { return nil }
func (m *mockConnWithClose) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// TestConfigValidateErrors tests config validation errors
func TestConfigValidateErrors(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr string
	}{
		{
			name: "negative min connections",
			config: &Config{
				MinConns: -1,
				MaxConns: 10,
			},
			wantErr: "min connections cannot be negative",
		},
		{
			name: "zero max connections",
			config: &Config{
				MinConns: 0,
				MaxConns: 0,
			},
			wantErr: "max connections must be positive",
		},
		{
			name: "min greater than max",
			config: &Config{
				MinConns: 10,
				MaxConns: 5,
			},
			wantErr: "min connections cannot exceed max connections",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err == nil {
				t.Errorf("Validate() expected error, got nil")
				return
			}
			if err.Error() != tt.wantErr {
				t.Errorf("Validate() error = %v, want %v", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestPoolCreationWithDialerError tests pool creation when dialer fails
func TestPoolCreationWithDialerError(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return nil, errors.New("connection refused")
	}

	config := DefaultConfig()
	config.MinConns = 3

	_, err := New(config, dialer)
	if err == nil {
		t.Error("Expected error when dialer fails")
	}
}

// TestAcquireTimeout tests connection acquire timeout
func TestAcquireTimeout(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 1
	config.MaxConns = 1
	config.AcquireTimeout = 100 * time.Millisecond

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Acquire the only connection
	conn1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire first connection: %v", err)
	}
	defer conn1.Release()

	// Mark it as in use
	conn1.inUse = 1

	// Second acquire should timeout
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, err = pool.Acquire(ctx2)
	if err == nil {
		t.Error("Expected timeout error for second acquire")
	}
}

// TestPoolClosed tests operations on closed pool
func TestPoolClosed(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 1

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Close the pool
	pool.Close()

	// Acquire should fail on closed pool
	ctx := context.Background()
	_, err = pool.Acquire(ctx)
	if err != ErrPoolClosed {
		t.Errorf("Expected ErrPoolClosed, got %v", err)
	}
}

// TestConnClose tests connection close
func TestConnClose(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConnWithClose{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 1
	config.MaxConns = 1

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}

	// Close the connection instead of releasing
	if err := conn.Close(); err != nil {
		t.Errorf("Failed to close connection: %v", err)
	}

	// Second close should not error
	if err := conn.Close(); err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}

// TestConnIsHealthy tests connection health check
func TestConnIsHealthy(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 1

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	defer conn.Release()

	// Healthy connection should pass
	if !conn.IsHealthy(1 * time.Second) {
		t.Error("Connection should be healthy")
	}

	// Close and check again
	conn.Close()
	if conn.IsHealthy(1 * time.Second) {
		t.Error("Closed connection should not be healthy")
	}
}

// TestConnIsExpired tests connection expiration
func TestConnIsExpired(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 1

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	defer conn.Release()

	// Not expired with long lifetime
	if conn.IsExpired(1*time.Hour, 1*time.Hour) {
		t.Error("New connection should not be expired")
	}

	// Manually set createdAt to past
	conn.createdAt = time.Now().Add(-2 * time.Hour)

	// Should be expired now
	if !conn.IsExpired(1*time.Hour, 0) {
		t.Error("Connection should be expired after max lifetime")
	}
}

// TestPoolStats tests pool statistics
func TestPoolStats(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 2
	config.MaxConns = 5

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	stats := pool.Stats()

	if stats.TotalConns != 2 {
		t.Errorf("Expected 2 total connections, got %d", stats.TotalConns)
	}

	if stats.ActiveConns != 0 {
		t.Errorf("Expected 0 active connections, got %d", stats.ActiveConns)
	}

	if stats.IdleConns != 2 {
		t.Errorf("Expected 2 idle connections, got %d", stats.IdleConns)
	}

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}

	stats = pool.Stats()
	if stats.ActiveConns != 1 {
		t.Errorf("Expected 1 active connection, got %d", stats.ActiveConns)
	}

	conn.Release()
}

// TestAcquireReleaseConcurrency tests concurrent acquire/release
func TestAcquireReleaseConcurrency(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 2
	config.MaxConns = 10

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Start multiple goroutines that acquire and release
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Errorf("Failed to acquire: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
			conn.Release()
		}()
	}

	wg.Wait()

	stats := pool.Stats()
	if stats.ActiveConns != 0 {
		t.Errorf("Expected 0 active connections after all released, got %d", stats.ActiveConns)
	}
}

// TestPoolScaleUp tests pool scaling up
func TestPoolScaleUp(t *testing.T) {
	connCount := 0
	dialer := func() (net.Conn, error) {
		connCount++
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 1
	config.MaxConns = 5

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Should have min connections initially
	if connCount != 1 {
		t.Errorf("Expected 1 initial connection, got %d", connCount)
	}

	ctx := context.Background()

	// Acquire more connections than min
	conns := make([]*Conn, 0, 5)
	for i := 0; i < 5; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("Failed to acquire connection %d: %v", i, err)
		}
		conns = append(conns, conn)
	}

	// Should have created up to max connections
	if connCount > 5 {
		t.Errorf("Should not exceed max connections, got %d", connCount)
	}

	// Release all
	for _, conn := range conns {
		conn.Release()
	}
}

// TestReleaseNilPool tests releasing connection with nil pool
func TestReleaseNilPool(t *testing.T) {
	conn := &Conn{
		Conn:       &mockConn{},
		pool:       nil,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
	}

	// Should not panic
	conn.Release()
}

// TestReleaseClosedConnection tests releasing already closed connection
func TestReleaseClosedConnection(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 1

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire: %v", err)
	}

	// Close the connection
	conn.Close()

	// Release should not panic
	conn.Release()
}
