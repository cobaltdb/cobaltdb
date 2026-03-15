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
