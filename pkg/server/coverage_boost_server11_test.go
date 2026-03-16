package server

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

// TestSendMessageAuthSuccess tests sendMessage with AuthSuccessMessage
func TestSendMessageAuthSuccess(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := &ClientConn{
		ID:     1,
		Conn:   serverConn,
		Server: srv,
		authed: true,
		reader: bufio.NewReader(serverConn),
	}

	// Send AuthSuccessMessage in background
	done := make(chan error, 1)
	go func() {
		done <- client.sendMessage(wire.NewAuthSuccessMessage("test-token-12345"))
	}()

	// Read the response with timeout
	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var length uint32
	if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
		t.Fatalf("Failed to read length: %v", err)
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify message type
	if response[0] != byte(wire.MsgAuthSuccess) {
		t.Errorf("Expected MsgAuthSuccess (%d), got %d", wire.MsgAuthSuccess, response[0])
	}

	// Wait for send to complete
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for sendMessage")
	}
}

// TestSendMessageWriteDeadlineError tests sendMessage when SetWriteDeadline fails
func TestSendMessageWriteDeadlineError(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create a connection that will fail on write deadline
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	client := &ClientConn{
		ID:     1,
		Conn:   serverConn,
		Server: srv,
		authed: true,
		reader: bufio.NewReader(serverConn),
	}

	// Close the server side to cause errors
	serverConn.Close()
	time.Sleep(50 * time.Millisecond)

	// Try to send - should fail on write deadline or write
	err = client.sendMessage(wire.MsgPong)
	if err == nil {
		t.Error("Expected error when sending to closed connection")
	}
}

// TestSendMessageBinaryWriteError tests sendMessage when binary.Write fails
func TestSendMessageBinaryWriteError(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	client := &ClientConn{
		ID:     1,
		Conn:   serverConn,
		Server: srv,
		authed: true,
		reader: bufio.NewReader(serverConn),
	}

	// Close server side to cause write errors
	serverConn.Close()
	time.Sleep(50 * time.Millisecond)

	// Try to send OK message
	okMsg := wire.NewOKMessage(1, 1)
	err = client.sendMessage(okMsg)
	if err == nil {
		t.Error("Expected error when writing to closed connection")
	}
}

// TestSendMessageAllTypes tests sendMessage with all message types
func TestSendMessageAllTypes(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := &ClientConn{
		ID:     1,
		Conn:   serverConn,
		Server: srv,
		authed: true,
		reader: bufio.NewReader(serverConn),
	}

	// Test all message types
	messages := []interface{}{
		wire.MsgPong,
		wire.NewResultMessage([]string{"col"}, [][]interface{}{{1}}),
		wire.NewOKMessage(1, 1),
		wire.NewErrorMessage(1, "test error"),
		wire.NewAuthSuccessMessage("token"),
	}

	for _, msg := range messages {
		// Send in background
		done := make(chan error, 1)
		go func(m interface{}) {
			done <- client.sendMessage(m)
		}(msg)

		// Read with timeout
		clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		var length uint32
		if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
			t.Fatalf("Failed to read length for %T: %v", msg, err)
		}
		response := make([]byte, length)
		if _, err := io.ReadFull(clientConn, response); err != nil {
			t.Fatalf("Failed to read response for %T: %v", msg, err)
		}

		// Wait for send to complete
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Failed to send message %T: %v", msg, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for sendMessage %T", msg)
		}
	}
}
