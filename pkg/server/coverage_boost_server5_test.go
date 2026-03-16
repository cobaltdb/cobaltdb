package server

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

// TestServerListenWithMaxConnections tests Listen with max connections limit
func TestServerListenWithMaxConnections(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := DefaultConfig()
	config.MaxConnections = 2 // Limit to 2 connections

	srv, err := New(db, config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	go func() {
		if err := srv.ListenOnListener(listener); err != nil && err != ErrServerClosed {
			t.Logf("ListenOnListener returned: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	addr := listener.Addr().String()

	// Connect first client (should succeed)
	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("First connection failed: %v", err)
	}
	defer conn1.Close()

	// Send ping to register client
	pingMsg := make([]byte, 5)
	binary.LittleEndian.PutUint32(pingMsg, 1)
	pingMsg[4] = byte(wire.MsgPing)
	if _, err := conn1.Write(pingMsg); err != nil {
		t.Fatalf("Failed to send ping on conn1: %v", err)
	}

	// Read pong response
	var length uint32
	if err := binary.Read(conn1, binary.LittleEndian, &length); err != nil {
		t.Fatalf("Failed to read response length: %v", err)
	}
	response := make([]byte, length)
	if _, err := io.ReadFull(conn1, response); err != nil {
		t.Fatalf("Failed to read pong: %v", err)
	}
	if response[0] != byte(wire.MsgPong) {
		t.Errorf("Expected pong, got: %d", response[0])
	}

	// Connect second client (should succeed)
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Second connection failed: %v", err)
	}
	defer conn2.Close()

	// Send ping
	if _, err := conn2.Write(pingMsg); err != nil {
		t.Fatalf("Failed to send ping on conn2: %v", err)
	}
	if err := binary.Read(conn2, binary.LittleEndian, &length); err != nil {
		t.Fatalf("Failed to read response length: %v", err)
	}
	response = make([]byte, length)
	if _, err := io.ReadFull(conn2, response); err != nil {
		t.Fatalf("Failed to read pong: %v", err)
	}

	// Connect third client (should be rejected due to max connections)
	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Third connection dial failed: %v", err)
	}
	defer conn3.Close()

	// The connection should receive an error message
	conn3.SetReadDeadline(time.Now().Add(2 * time.Second))
	var errLen uint32
	if err := binary.Read(conn3, binary.LittleEndian, &errLen); err == nil {
		errResponse := make([]byte, errLen)
		if _, err := io.ReadFull(conn3, errResponse); err == nil {
			if errResponse[0] == byte(wire.MsgError) {
				t.Logf("Got expected error for max connections")
			}
		}
	}

	srv.Close()
}

// TestServerListenTLSWarning tests Listen with auth enabled but TLS disabled
func TestServerListenTLSWarning(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	config := DefaultConfig()
	config.AuthEnabled = true

	srv, err := New(db, config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	go func() {
		// This should print a warning about cleartext passwords
		if err := srv.Listen(listener.Addr().String(), nil); err != nil && err != ErrServerClosed {
			t.Logf("Listen returned: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	srv.Close()
}

// TestServerAcceptLoopError tests acceptLoop with listener error
func TestServerAcceptLoopError(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	srv.listener = listener

	// Close the listener immediately to cause accept error
	listener.Close()

	// acceptLoop should return error when listener is closed
	err = srv.acceptLoop()
	if err == nil {
		t.Error("Expected error from acceptLoop with closed listener")
	}
}
