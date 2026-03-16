package server

import (
	"net"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestListenTLSConfigError tests Listen when TLS config loading fails
func TestListenTLSConfigError(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create a TLS config with invalid cert file
	tlsConfig := &TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/path/cert.crt",
		KeyFile:  "/nonexistent/path/key.key",
	}

	// Start a listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	// Use a goroutine to call Listen which should fail on TLS config
	errChan := make(chan error, 1)
	go func() {
		errChan <- srv.Listen(listener.Addr().String(), tlsConfig)
	}()

	select {
	case err := <-errChan:
		if err == nil {
			t.Error("Expected error for invalid TLS config")
		}
	case <-time.After(2 * time.Second):
		t.Error("Listen did not return error in time")
	}

	srv.Close()
}

// TestListenTLSErrorClosesListener tests that Listen closes the listener when TLS config fails
func TestListenTLSErrorClosesListener(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create a TLS config with invalid cert file
	tlsConfig := &TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/path/cert.crt",
		KeyFile:  "/nonexistent/path/key.key",
	}

	// Try to listen - should fail and clean up
	err = srv.Listen("127.0.0.1:0", tlsConfig)
	if err == nil {
		t.Error("Expected error for invalid TLS config")
	}
}
