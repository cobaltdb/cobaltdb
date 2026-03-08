package engine

import (
	"context"
	"testing"
	"time"
)

func TestConnectionPoolBasic(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &PoolConfig{
		MinConns:            2,
		MaxConns:            5,
		MaxIdleTime:         1 * time.Minute,
		MaxLifetime:         5 * time.Minute,
		AcquireTimeout:      5 * time.Second,
		HealthCheckInterval: 10 * time.Second,
	}

	pool, err := NewConnectionPool(db, config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Check initial stats
	stats := pool.Stats()
	if stats.TotalConns != 2 {
		t.Errorf("Expected 2 initial connections, got %d", stats.TotalConns)
	}
	if stats.IdleConns != 2 {
		t.Errorf("Expected 2 idle connections, got %d", stats.IdleConns)
	}
}

func TestConnectionPoolAcquireRelease(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &PoolConfig{
		MinConns:       1,
		MaxConns:       3,
		AcquireTimeout: 1 * time.Second,
	}

	pool, err := NewConnectionPool(db, config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Acquire a connection
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}

	if !conn.IsHealthy() {
		t.Error("Expected connection to be healthy")
	}

	// Check stats after acquire
	stats := pool.Stats()
	if stats.ActiveConns != 1 {
		t.Errorf("Expected 1 active connection, got %d", stats.ActiveConns)
	}
	if stats.IdleConns != 0 {
		t.Errorf("Expected 0 idle connections, got %d", stats.IdleConns)
	}

	// Release the connection
	pool.Release(conn)

	// Check stats after release
	stats = pool.Stats()
	if stats.ActiveConns != 0 {
		t.Errorf("Expected 0 active connections after release, got %d", stats.ActiveConns)
	}
}

func TestConnectionPoolExhaustion(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &PoolConfig{
		MinConns:       1,
		MaxConns:       2,
		AcquireTimeout: 100 * time.Millisecond,
	}

	pool, err := NewConnectionPool(db, config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Acquire all connections
	conn1, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire first connection: %v", err)
	}
	defer pool.Release(conn1)

	conn2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire second connection: %v", err)
	}
	defer pool.Release(conn2)

	// Try to acquire third - should timeout
	_, err = pool.Acquire(ctx)
	if err != ErrPoolExhausted {
		t.Errorf("Expected ErrPoolExhausted, got %v", err)
	}
}

func TestConnectionPoolConcurrent(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &PoolConfig{
		MinConns:       2,
		MaxConns:       5,
		AcquireTimeout: 5 * time.Second,
	}

	pool, err := NewConnectionPool(db, config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	// Concurrent acquisitions
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- struct{}{} }()

			ctx := context.Background()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Logf("Failed to acquire: %v", err)
				return
			}

			time.Sleep(10 * time.Millisecond) // Simulate work
			pool.Release(conn)
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Check final stats
	stats := pool.Stats()
	if stats.AcquireCount != 10 {
		t.Errorf("Expected 10 acquires, got %d", stats.AcquireCount)
	}
	if stats.ReleaseCount != 10 {
		t.Errorf("Expected 10 releases, got %d", stats.ReleaseCount)
	}
}

func TestConnectionPoolClose(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &PoolConfig{
		MinConns: 2,
		MaxConns: 5,
	}

	pool, err := NewConnectionPool(db, config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Close should succeed
	if err := pool.Close(); err != nil {
		t.Errorf("Failed to close pool: %v", err)
	}

	// Second close should succeed (idempotent)
	if err := pool.Close(); err != nil {
		t.Errorf("Second close failed: %v", err)
	}

	// Acquire after close should fail
	ctx := context.Background()
	_, err = pool.Acquire(ctx)
	if err != ErrPoolClosed {
		t.Errorf("Expected ErrPoolClosed, got %v", err)
	}
}

func TestPooledConnectionMarkUnhealthy(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &PoolConfig{
		MinConns: 1,
		MaxConns: 3,
	}

	pool, err := NewConnectionPool(db, config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}

	// Mark as unhealthy
	conn.MarkUnhealthy()

	if conn.IsHealthy() {
		t.Error("Expected connection to be unhealthy after marking")
	}

	pool.Release(conn)
}

func BenchmarkConnectionPoolAcquireRelease(b *testing.B) {
	db, err := Open(":memory:", nil)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &PoolConfig{
		MinConns:       5,
		MaxConns:       10,
		AcquireTimeout: 5 * time.Second,
	}

	pool, err := NewConnectionPool(db, config)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				b.Logf("Acquire failed: %v", err)
				continue
			}
			pool.Release(conn)
		}
	})
}
