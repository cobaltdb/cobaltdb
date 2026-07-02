package pool

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pipeDialer returns a dialer producing in-memory connections; the peer ends
// are retained so tests can close them to simulate dead sockets.
func pipeDialer() (func() (net.Conn, error), *sync.Map) {
	peers := &sync.Map{}
	var seq int64
	dialer := func() (net.Conn, error) {
		client, server := net.Pipe()
		peers.Store(atomic.AddInt64(&seq, 1), server)
		return client, nil
	}
	return dialer, peers
}

// TestPoolMaxConnsNotExceededUnderConcurrentAcquire hammers Acquire from many
// goroutines and asserts the pool never creates more than MaxConns
// connections (the old check-then-create TOCTOU allowed overshoot).
// Run with -race.
func TestPoolMaxConnsNotExceededUnderConcurrentAcquire(t *testing.T) {
	const maxConns = 8

	var live atomic.Int64
	var peakDialed atomic.Int64
	dialer := func() (net.Conn, error) {
		n := live.Add(1)
		for {
			peak := peakDialed.Load()
			if n <= peak || peakDialed.CompareAndSwap(peak, n) {
				break
			}
		}
		client, server := net.Pipe()
		go func() {
			// Track close of the client side via the server side blocking read.
			buf := make([]byte, 1)
			_, _ = server.Read(buf)
			live.Add(-1)
			_ = server.Close()
		}()
		return client, nil
	}

	p, err := New(&Config{
		MinConns:            0,
		MaxConns:            maxConns,
		AcquireTimeout:      500 * time.Millisecond,
		HealthCheckInterval: time.Hour, // keep the health checker out of this test
		MaxIdleTime:         time.Hour,
		MaxLifetime:         time.Hour,
		WaitQueueSize:       1000,
	}, dialer)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	defer p.Close()

	const goroutines = 64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				conn, err := p.Acquire(context.Background())
				if err != nil {
					continue // timeout under contention is fine
				}
				time.Sleep(time.Microsecond * 50)
				conn.Release()
			}
		}()
	}
	wg.Wait()

	if got := p.Stats().TotalConns; got > maxConns {
		t.Fatalf("TotalConns %d exceeds MaxConns %d", got, maxConns)
	}
	if peak := peakDialed.Load(); peak > maxConns {
		t.Fatalf("dialed %d simultaneous connections, MaxConns is %d", peak, maxConns)
	}
}

// TestHealthCheckNeverClosesInUseConnections verifies that a connection past
// MaxLifetime that is checked while IN USE is not closed mid-query; it is
// marked and retired on Release instead. Run with -race.
func TestHealthCheckNeverClosesInUseConnections(t *testing.T) {
	dialer, _ := pipeDialer()

	p, err := New(&Config{
		MinConns:            0,
		MaxConns:            4,
		MaxLifetime:         time.Nanosecond, // everything is instantly "expired"
		MaxIdleTime:         time.Hour,
		HealthCheckInterval: time.Hour, // we trigger checks manually
		HealthCheckTimeout:  10 * time.Millisecond,
		AcquireTimeout:      time.Second,
		WaitQueueSize:       10,
	}, dialer)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	defer p.Close()

	conn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	time.Sleep(2 * time.Millisecond) // exceed MaxLifetime

	// Health checks while the connection is in use must not close it.
	for i := 0; i < 3; i++ {
		p.performHealthCheck()
	}
	if atomic.LoadInt32(&conn.closed) == 1 {
		t.Fatal("health check closed an in-use connection")
	}
	// The connection must still be usable: a write on an open net.Pipe with a
	// closed... skip IO; the closed flag is authoritative for the pool.

	// It must have been marked for retirement, and destroyed on Release.
	if atomic.LoadInt32(&conn.retire) != 1 {
		t.Fatal("expired in-use connection was not marked for retirement")
	}
	conn.Release()
	if atomic.LoadInt32(&conn.closed) != 1 {
		t.Fatal("retired connection was not closed on Release")
	}
	if got := p.Stats().TotalConns; got != 0 {
		t.Fatalf("expected 0 connections after retirement, got %d", got)
	}
}

// TestIsHealthyDetectsDeadConnection verifies the liveness probe actually
// probes: a peer-closed connection reports unhealthy, an open quiet one
// reports healthy.
func TestIsHealthyDetectsDeadConnection(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	c := newConn(client, nil, 1)
	if !c.IsHealthy(20 * time.Millisecond) {
		t.Fatal("open, quiet connection reported unhealthy")
	}

	deadClient, deadServer := net.Pipe()
	_ = deadServer.Close()
	d := newConn(deadClient, nil, 2)
	if d.IsHealthy(20 * time.Millisecond) {
		t.Fatal("peer-closed connection reported healthy")
	}
}

// TestHealthCheckRemovesDeadIdleConnections verifies the health check culls
// idle connections whose peer has gone away.
func TestHealthCheckRemovesDeadIdleConnections(t *testing.T) {
	dialer, peers := pipeDialer()

	p, err := New(&Config{
		MinConns:            2,
		MaxConns:            4,
		MaxLifetime:         time.Hour,
		MaxIdleTime:         time.Hour,
		HealthCheckInterval: time.Hour,
		HealthCheckTimeout:  10 * time.Millisecond,
		AcquireTimeout:      time.Second,
		WaitQueueSize:       10,
	}, dialer)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	defer p.Close()

	if got := p.Stats().TotalConns; got != 2 {
		t.Fatalf("expected 2 initial connections, got %d", got)
	}

	// Kill the peers of every idle connection.
	peers.Range(func(_, v interface{}) bool {
		_ = v.(net.Conn).Close()
		return true
	})

	p.performHealthCheck()

	if got := p.Stats().TotalConns; got != 0 {
		t.Fatalf("expected dead idle connections to be removed, got %d", got)
	}
}
