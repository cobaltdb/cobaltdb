package pool

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigValidateNegativeAndOverflowPaths(t *testing.T) {
	tests := []struct {
		name string
		fn   func(c *Config)
		want string
	}{
		{"NegativeMinConns", func(c *Config) { c.MinConns = -1 }, "negative"},
		{"MaxConnsZero", func(c *Config) { c.MaxConns = 0 }, "positive"},
		{"MaxConnsOverflow", func(c *Config) { c.MaxConns = maxPoolConnections + 1 }, "exceeds maximum"},
		{"MinGtMax", func(c *Config) { c.MinConns = 10; c.MaxConns = 5 }, "exceed max"},
		{"NegativeMaxIdleTime", func(c *Config) { c.MaxIdleTime = -1 }, "negative"},
		{"NegativeMaxLifetime", func(c *Config) { c.MaxLifetime = -1 }, "negative"},
		{"NegativeHealthCheckInterval", func(c *Config) { c.HealthCheckInterval = -1 }, "negative"},
		{"NegativeHealthCheckTimeout", func(c *Config) { c.HealthCheckTimeout = -1 }, "negative"},
		{"NegativeAcquireTimeout", func(c *Config) { c.AcquireTimeout = -1 }, "negative"},
		{"NegativeWaitQueueSize", func(c *Config) { c.WaitQueueSize = -1 }, "negative"},
		{"WaitQueueOverflow", func(c *Config) { c.WaitQueueSize = maxPoolWaiters + 1 }, "exceeds maximum"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfig()
			tt.fn(c)
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestNormalizeConfigPopulatesDefaults(t *testing.T) {
	c := normalizeConfig(&Config{
		MinConns:            1,
		MaxConns:            10,
		MaxIdleTime:         0,
		MaxLifetime:         0,
		AcquireTimeout:      0,
		HealthCheckInterval: 0,
		HealthCheckTimeout:  0,
		WaitQueueSize:       0,
	})
	def := DefaultConfig()
	if c.MaxIdleTime != def.MaxIdleTime {
		t.Fatalf("MaxIdleTime = %v, want default %v", c.MaxIdleTime, def.MaxIdleTime)
	}
	if c.MaxLifetime != def.MaxLifetime {
		t.Fatalf("MaxLifetime = %v, want default %v", c.MaxLifetime, def.MaxLifetime)
	}
	if c.AcquireTimeout != def.AcquireTimeout {
		t.Fatalf("AcquireTimeout = %v, want default %v", c.AcquireTimeout, def.AcquireTimeout)
	}
	if c.HealthCheckInterval != def.HealthCheckInterval {
		t.Fatalf("HealthCheckInterval = %v, want default %v", c.HealthCheckInterval, def.HealthCheckInterval)
	}
	if c.HealthCheckTimeout != def.HealthCheckTimeout {
		t.Fatalf("HealthCheckTimeout = %v, want default %v", c.HealthCheckTimeout, def.HealthCheckTimeout)
	}
	if c.WaitQueueSize != def.WaitQueueSize {
		t.Fatalf("WaitQueueSize = %v, want default %v", c.WaitQueueSize, def.WaitQueueSize)
	}
}

func TestPoolNewRejectsInvalidConfigAndNilDialer(t *testing.T) {
	if _, err := New(&Config{}, nil); err == nil {
		t.Fatal("New with invalid config should fail")
	}
	if _, err := New(nil, nil); err == nil {
		t.Fatal("New with nil dialer should fail")
	}
}

func TestConnCloseWithPoolRemoval(t *testing.T) {
	dialer, _ := pipeDialer()
	p, err := New(DefaultConfig(), dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	conn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close with pool failed: %v", err)
	}
	// Second close should be a no-op
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}
}

func TestConnReleaseHandlesClosed(t *testing.T) {
	p, err := New(DefaultConfig(), func() (net.Conn, error) {
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()

	// A connection whose pool is closed should release silently
	closedConn := &Conn{pool: p, closed: 1}
	closedConn.Release() // should be a no-op (no panic)
}

func TestPoolAcquireRejectsClosedPool(t *testing.T) {
	dialer, _ := pipeDialer()
	p, err := New(DefaultConfig(), dialer)
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	if _, err := p.Acquire(context.Background()); err != ErrPoolClosed {
		t.Fatalf("Acquire on closed pool = %v, want ErrPoolClosed", err)
	}
}

func TestPoolWaitForConnectionExhaustsQueue(t *testing.T) {
	dialer, _ := pipeDialer()
	p, err := New(&Config{
		MinConns:            0,
		MaxConns:            1,
		MaxIdleTime:         time.Minute,
		MaxLifetime:         time.Hour,
		AcquireTimeout:      time.Second,
		HealthCheckInterval: time.Minute,
		HealthCheckTimeout:  time.Second,
		WaitQueueSize:       1,
	}, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Acquire the one connection
	conn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Second acquire should time out or be cancelled when context is exhausted
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Acquire(ctx); err == nil {
		t.Fatal("second Acquire should fail")
	}

	// Return the first connection
	conn.Release()

	// Now we should be able to acquire again
	if _, err := p.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire after release = %v", err)
	}
}

func TestPoolCreateConnectionFailsOnDialError(t *testing.T) {
	var shouldFail bool
	p, err := New(&Config{
		MinConns:            0,
		MaxConns:            5,
		MaxIdleTime:         time.Minute,
		MaxLifetime:         time.Hour,
		AcquireTimeout:      time.Second,
		HealthCheckInterval: time.Hour,
		HealthCheckTimeout:  0,
		WaitQueueSize:       10,
	}, func() (net.Conn, error) {
		if shouldFail {
			return nil, net.ErrClosed
		}
		client, server := net.Pipe()
		_ = server.Close()
		return client, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	shouldFail = true
	if _, err := p.Acquire(context.Background()); err == nil {
		t.Fatal("Acquire should fail when dial fails")
	}
}

func TestPoolReleaseOnClosedCallsRemoveConn(t *testing.T) {
	dialer, _ := pipeDialer()
	p, err := New(DefaultConfig(), dialer)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	p.Close()
	// Release after close should call removeConn (inUse==1, closed pool)
	conn.Release()
}

func TestPoolStats(t *testing.T) {
	dialer, _ := pipeDialer()
	p, err := New(&Config{
		MinConns:            0,
		MaxConns:            5,
		MaxIdleTime:         time.Minute,
		MaxLifetime:         time.Hour,
		AcquireTimeout:      time.Second,
		HealthCheckInterval: time.Hour,
		HealthCheckTimeout:  0,
		WaitQueueSize:       10,
	}, dialer)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	s := p.Stats()
	if s.TotalConns != 0 {
		t.Fatalf("expected 0 connections, got %d", s.TotalConns)
	}

	conn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s = p.Stats()
	if s.TotalConns != 1 || s.ActiveConns != 1 {
		t.Fatalf("expected 1 total/active, got total=%d active=%d", s.TotalConns, s.ActiveConns)
	}

	conn.Release()
	s = p.Stats()
	if s.IdleConns != 1 || s.ActiveConns != 0 {
		t.Fatalf("expected 1 idle/0 active, got idle=%d active=%d", s.IdleConns, s.ActiveConns)
	}
}

func TestConnIsExpiredByLifetimeAndIdle(t *testing.T) {
	now := time.Now()
	conn := &Conn{createdAt: now.Add(-2 * time.Hour), lastUsedAtNano: now.UnixNano()}
	if !conn.IsExpired(time.Hour, 0) {
		t.Fatal("conn past maxLifetime should be expired")
	}
	if conn.IsExpired(0, 0) {
		t.Fatal("conn with no limits should not be expired")
	}

	conn2 := &Conn{createdAt: now}
	atomic.StoreInt64(&conn2.lastUsedAtNano, now.Add(-10*time.Minute).UnixNano())
	if !conn2.IsExpired(0, time.Minute) {
		t.Fatal("idle conn past maxIdleTime should be expired")
	}
	if conn2.IsExpired(0, time.Hour) {
		t.Fatal("idle conn within maxIdleTime should not be expired")
	}
}
