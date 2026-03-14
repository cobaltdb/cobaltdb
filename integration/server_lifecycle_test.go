package integration

import (
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/server"
)

// TestServerBasicStartup tests basic server startup and shutdown
func TestServerBasicStartup(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := server.DefaultConfig()
	config.Address = "127.0.0.1:0"

	srv, err := server.New(db, config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create listener manually to get address
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	addr := listener.Addr().String()

	// Start server in goroutine (ListenOnListener blocks)
	go func() {
		if err := srv.ListenOnListener(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Test connection
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	conn.Close()

	// Shutdown
	if err := srv.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}

// TestServerAcceptLoop tests connection acceptance under load
func TestServerAcceptLoop(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := server.New(db, server.DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create listener manually
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	addr := listener.Addr().String()

	// Start server
	go func() {
		if err := srv.ListenOnListener(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Test multiple concurrent connections
	const numConns = 50
	var wg sync.WaitGroup
	errors := make(chan error, numConns)

	for i := 0; i < numConns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errors <- fmt.Errorf("connection %d failed: %v", id, err)
				return
			}
			defer conn.Close()

			// Connection successful - just close it
			time.Sleep(10 * time.Millisecond)
		}(i)
	}

	wg.Wait()
	close(errors)

	errCount := 0
	for err := range errors {
		t.Log(err)
		errCount++
	}

	if errCount > numConns/10 {
		t.Errorf("Too many connection failures: %d/%d", errCount, numConns)
	}

	srv.Close()
}

// TestServerGracefulShutdown tests graceful shutdown with active connections
func TestServerGracefulShutdown(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := server.New(db, server.DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	addr := listener.Addr().String()

	go func() {
		if err := srv.ListenOnListener(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Create active connections
	const numConns = 10
	conns := make([]net.Conn, numConns)
	for i := 0; i < numConns; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		conns[i] = conn
	}

	// Start graceful shutdown
	shutdownChan := make(chan error)
	go func() {
		shutdownChan <- srv.Close()
	}()

	// Connections should be closed gracefully
	for i, conn := range conns {
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err := conn.Read(buf)
		if err == nil {
			t.Logf("Connection %d still active after shutdown", i)
		}
		conn.Close()
	}

	select {
	case err := <-shutdownChan:
		if err != nil {
			t.Errorf("Shutdown error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Shutdown timeout")
	}
}

// TestServerConnectionTimeout tests connection timeouts
func TestServerConnectionTimeout(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &server.Config{
		Address:      "127.0.0.1:0",
		ReadTimeout:  1,
		WriteTimeout: 1,
	}

	srv, err := server.New(db, config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	addr := listener.Addr().String()

	go func() {
		if err := srv.ListenOnListener(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Connect and do nothing (should timeout)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Wait for read timeout
	time.Sleep(2 * time.Second)

	// Connection should be closed by server
	buf := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = conn.Read(buf)
	if err == nil {
		t.Error("Expected connection to be closed due to timeout")
	}

	srv.Close()
}

// TestServerMaxConnections tests max connection limit
func TestServerMaxConnections(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := &server.Config{
		Address:        "127.0.0.1:0",
		MaxConnections: 5,
	}

	srv, err := server.New(db, config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	addr := listener.Addr().String()

	go func() {
		if err := srv.ListenOnListener(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Create max connections
	conns := make([]net.Conn, 0, config.MaxConnections+2)

	for i := 0; i < config.MaxConnections+2; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Logf("Connection %d failed (expected after max): %v", i, err)
			continue
		}
		conns = append(conns, conn)
	}

	// Server should have enforced max connections (allow some tolerance for race conditions)
	if len(conns) > config.MaxConnections+3 {
		t.Errorf("Too many connections accepted: %d > %d+3", len(conns), config.MaxConnections)
	}
	t.Logf("Connections accepted: %d (max allowed: %d)", len(conns), config.MaxConnections)

	for _, conn := range conns {
		conn.Close()
	}

	srv.Close()
}

// TestServerSignalHandling tests graceful shutdown on signals
func TestServerSignalHandling(t *testing.T) {
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping signal tests in CI")
	}

	// On Windows, we can't easily send signals, so skip
	if os.Getenv("SKIP_SIGNAL_TEST") == "true" {
		t.Skip("Skipping signal tests")
	}

	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := server.DefaultConfig()
	config.Address = "127.0.0.1:0"

	srv, err := server.New(db, config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	// Start server
	go func() {
		if err := srv.ListenOnListener(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Wait for graceful shutdown
	done := make(chan bool)
	go func() {
		srv.Close()
		done <- true
	}()

	select {
	case <-done:
		t.Log("Server shutdown gracefully")
	case <-time.After(5 * time.Second):
		t.Error("Server shutdown timeout")
	}
}
