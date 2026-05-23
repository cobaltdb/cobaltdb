package pool

import (
	"context"
	"net"
	"testing"
	"time"
)

// mockConn is a mock net.Conn for testing
type mockConn struct {
	closed bool
}

func (m *mockConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockConn) Close() error                       { m.closed = true; return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if config.MinConns != 5 {
		t.Errorf("Expected min conns 5, got %d", config.MinConns)
	}
	if config.MaxConns != 100 {
		t.Errorf("Expected max conns 100, got %d", config.MaxConns)
	}
	if config.AcquireTimeout != 10*time.Second {
		t.Errorf("Expected acquire timeout 10s, got %v", config.AcquireTimeout)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  &Config{MinConns: 5, MaxConns: 100},
			wantErr: false,
		},
		{
			name:    "negative min conns",
			config:  &Config{MinConns: -1, MaxConns: 100},
			wantErr: true,
		},
		{
			name:    "zero max conns",
			config:  &Config{MinConns: 0, MaxConns: 0},
			wantErr: true,
		},
		{
			name:    "min greater than max",
			config:  &Config{MinConns: 100, MaxConns: 50},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if (err != nil) != test.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestPoolCreationNormalizesPartialConfigDurations(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := &Config{MinConns: 0, MaxConns: 1}
	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pool.Close()

	defaults := DefaultConfig()
	if pool.config.AcquireTimeout != defaults.AcquireTimeout {
		t.Fatalf("expected default acquire timeout, got %v", pool.config.AcquireTimeout)
	}
	if pool.config.HealthCheckInterval != defaults.HealthCheckInterval {
		t.Fatalf("expected default health check interval, got %v", pool.config.HealthCheckInterval)
	}
	if pool.config.HealthCheckTimeout != defaults.HealthCheckTimeout {
		t.Fatalf("expected default health check timeout, got %v", pool.config.HealthCheckTimeout)
	}
	if pool.config.MaxIdleTime != defaults.MaxIdleTime {
		t.Fatalf("expected default max idle time, got %v", pool.config.MaxIdleTime)
	}
	if pool.config.MaxLifetime != defaults.MaxLifetime {
		t.Fatalf("expected default max lifetime, got %v", pool.config.MaxLifetime)
	}
	if pool.config.WaitQueueSize != defaults.WaitQueueSize {
		t.Fatalf("expected default wait queue size, got %d", pool.config.WaitQueueSize)
	}
	if config.AcquireTimeout != 0 || config.HealthCheckInterval != 0 || config.HealthCheckTimeout != 0 ||
		config.MaxIdleTime != 0 || config.MaxLifetime != 0 || config.WaitQueueSize != 0 {
		t.Fatal("New should not mutate caller config")
	}
}

func TestPoolCreation(t *testing.T) {
	connCount := 0
	dialer := func() (net.Conn, error) {
		connCount++
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 3

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	if connCount != 3 {
		t.Errorf("Expected 3 initial connections, got %d", connCount)
	}

	stats := pool.Stats()
	if stats.TotalConns != 3 {
		t.Errorf("Expected 3 total connections, got %d", stats.TotalConns)
	}
}

func TestPoolStatsWhenReleaseServesWaiter(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}

	config := DefaultConfig()
	config.MinConns = 1
	config.MaxConns = 1
	config.WaitQueueSize = 1
	config.AcquireTimeout = time.Second
	config.HealthCheckInterval = time.Hour

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pool.Close()

	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}

	type acquireResult struct {
		conn *Conn
		err  error
	}
	done := make(chan acquireResult, 1)
	go func() {
		conn, err := pool.Acquire(context.Background())
		done <- acquireResult{conn: conn, err: err}
	}()

	deadline := time.After(time.Second)
	for {
		if pool.Stats().WaitQueueLen == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for waiter to enqueue")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	first.Release()

	var result acquireResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for waiter acquire")
	}
	if result.err != nil {
		t.Fatalf("waiter Acquire failed: %v", result.err)
	}
	defer result.conn.Release()

	stats := pool.Stats()
	if stats.WaitQueueLen != 0 {
		t.Fatalf("WaitQueueLen = %d, want 0", stats.WaitQueueLen)
	}
	if stats.ActiveConns != 1 {
		t.Fatalf("ActiveConns = %d, want 1", stats.ActiveConns)
	}
	if stats.IdleConns != 0 {
		t.Fatalf("IdleConns = %d, want 0", stats.IdleConns)
	}
}

func TestAcquireRelease(t *testing.T) {
	dialer := func() (net.Conn, error) {
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

	ctx := context.Background()

	// Acquire a connection
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}

	if conn == nil {
		t.Fatal("Connection should not be nil")
	}

	stats := pool.Stats()
	if stats.ActiveConns != 1 {
		t.Errorf("Expected 1 active connection, got %d", stats.ActiveConns)
	}

	// Release connection
	conn.Release()

	// Give some time for release to process
	time.Sleep(10 * time.Millisecond)

	stats = pool.Stats()
	if stats.ActiveConns != 0 {
		t.Errorf("Expected 0 active connections after release, got %d", stats.ActiveConns)
	}
}
