package pool

import (
	"context"
	"net"
	"testing"
)

func TestConnCloseNilPool(t *testing.T) {
	// A Conn with pool == nil should close the underlying connection
	c := &Conn{Conn: &mockConn{}}
	err := c.Close()
	if err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
	if !c.Conn.(*mockConn).closed {
		t.Error("underlying connection was not closed")
	}
}

func TestConnCloseNilPoolAlreadyClosed(t *testing.T) {
	// Calling Close twice with nil pool is a no-op on second call
	c := &Conn{Conn: &mockConn{}}
	_ = c.Close()
	err := c.Close()
	if err != nil {
		t.Fatalf("second Close() returned error: %v", err)
	}
}

func TestConnCloseWithPool(t *testing.T) {
	dialer := func() (net.Conn, error) {
		return &mockConn{}, nil
	}
	config := DefaultConfig()
	config.MinConns = 1
	config.MaxConns = 5

	pool, err := New(config, dialer)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Close returns the connection to the pool
	if err := conn.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}
