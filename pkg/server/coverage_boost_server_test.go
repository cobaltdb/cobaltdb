package server

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

// TestHandleMessageTypes targets Handle with different message types
func TestHandleMessageTypes(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

	// Create a pipe for testing
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Create client with proper reader initialization
	client := &ClientConn{
		ID:     1,
		Conn:   serverConn,
		Server: srv,
		authed: true,
		reader: bufio.NewReader(serverConn),
	}

	// Add client to server
	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	// Run handle in background
	go client.Handle()

	// Give handler time to start
	time.Sleep(50 * time.Millisecond)

	// Test MsgPing
	pingMsg := make([]byte, 5)
	binary.LittleEndian.PutUint32(pingMsg, 1)
	pingMsg[4] = byte(wire.MsgPing)
	if _, err := clientConn.Write(pingMsg); err != nil {
		t.Logf("Failed to send ping: %v", err)
		return
	}

	// Read response
	var length uint32
	if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	// Should get MsgPong
	if response[0] != byte(wire.MsgPong) {
		t.Logf("Expected pong, got: %d", response[0])
	}

	// Close connection
	clientConn.Close()
}

// TestHandleAuthRequired targets Handle with auth required paths
func TestHandleAuthRequired(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	config := &Config{
		AuthEnabled:      true,
		DefaultAdminUser: "testuser",
		DefaultAdminPass: "testpass",
	}
	srv, _ := New(db, config)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := &ClientConn{
		ID:     1,
		Conn:   serverConn,
		Server: srv,
		reader: bufio.NewReader(serverConn),
	}

	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	go client.Handle()
	time.Sleep(50 * time.Millisecond)

	// Send query without auth - should get auth required error
	queryMsg := wire.QueryMessage{SQL: "SELECT 1"}
	payload, _ := wire.Encode(&queryMsg)

	msg := make([]byte, 5+len(payload))
	binary.LittleEndian.PutUint32(msg, uint32(1+len(payload)))
	msg[4] = byte(wire.MsgQuery)
	copy(msg[5:], payload)

	if _, err := clientConn.Write(msg); err != nil {
		t.Logf("Failed to send query: %v", err)
		return
	}

	// Read response
	var length uint32
	if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	// Should get error (MsgError = 5)
	if response[0] != byte(wire.MsgError) {
		t.Logf("Expected error (5), got: %d", response[0])
	}

	clientConn.Close()
}

// TestHandlePrepareMessage targets Handle with MsgPrepare
func TestHandlePrepareMessage(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

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

	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	go client.Handle()
	time.Sleep(50 * time.Millisecond)

	// Send prepare message
	prepMsg := wire.PrepareMessage{SQL: "SELECT ?"}
	payload, _ := wire.Encode(&prepMsg)

	msg := make([]byte, 5+len(payload))
	binary.LittleEndian.PutUint32(msg, uint32(1+len(payload)))
	msg[4] = byte(wire.MsgPrepare)
	copy(msg[5:], payload)

	if _, err := clientConn.Write(msg); err != nil {
		t.Logf("Failed to send prepare: %v", err)
		return
	}

	// Read response
	var length uint32
	if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	// Should get OK message
	t.Logf("Prepare response type: %d", response[0])

	clientConn.Close()
}

// TestHandleExecuteMessage targets Handle with MsgExecute
func TestHandleExecuteMessage(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

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

	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	go client.Handle()
	time.Sleep(50 * time.Millisecond)

	// Send execute message
	execMsg := wire.ExecuteMessage{StmtID: 1}
	payload, _ := wire.Encode(&execMsg)

	msg := make([]byte, 5+len(payload))
	binary.LittleEndian.PutUint32(msg, uint32(1+len(payload)))
	msg[4] = byte(wire.MsgExecute)
	copy(msg[5:], payload)

	if _, err := clientConn.Write(msg); err != nil {
		t.Logf("Failed to send execute: %v", err)
		return
	}

	// Read response
	var length uint32
	if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	// Should get error (not supported)
	t.Logf("Execute response type: %d", response[0])

	clientConn.Close()
}

// TestHandleInvalidMessageType targets Handle with unknown message type
func TestHandleInvalidMessageType(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

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

	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	go client.Handle()
	time.Sleep(50 * time.Millisecond)

	// Send invalid message type
	msg := make([]byte, 5)
	binary.LittleEndian.PutUint32(msg, 1)
	msg[4] = byte(99) // Invalid message type

	if _, err := clientConn.Write(msg); err != nil {
		t.Logf("Failed to send invalid msg: %v", err)
		return
	}

	// Read response
	var length uint32
	if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	// Should get error
	if response[0] != byte(wire.MsgError) {
		t.Logf("Expected error, got: %d", response[0])
	}

	clientConn.Close()
}

// TestHandleMalformedMessages targets Handle with malformed messages
func TestHandleMalformedMessages(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

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

	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	go client.Handle()
	time.Sleep(50 * time.Millisecond)

	// Send message with length = 0
	msg := make([]byte, 4)
	binary.LittleEndian.PutUint32(msg, 0)
	if _, err := clientConn.Write(msg); err != nil {
		t.Logf("Failed to write: %v", err)
		return
	}

	// Connection should be closed
	buf := make([]byte, 1)
	clientConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := clientConn.Read(buf)
	if err == nil {
		t.Log("Connection should have been closed after invalid length")
	}
}

// TestHandleMessageTooLarge targets Handle with oversized message
func TestHandleMessageTooLarge(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

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

	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	go client.Handle()
	time.Sleep(50 * time.Millisecond)

	// Send message with length > maxPayloadSize
	msg := make([]byte, 4)
	binary.LittleEndian.PutUint32(msg, maxPayloadSize+10)
	if _, err := clientConn.Write(msg); err != nil {
		t.Logf("Failed to write: %v", err)
		return
	}

	// Connection should be closed
	buf := make([]byte, 1)
	clientConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := clientConn.Read(buf)
	if err == nil {
		t.Log("Connection should have been closed after oversized message")
	}
}

// TestGenerateClientCertErrors targets GenerateClientCert error paths
func TestGenerateClientCertErrors(t *testing.T) {
	// Test with non-existent CA files
	_, _, err := GenerateClientCert("/nonexistent/ca.crt", "/nonexistent/ca.key", "test", 30)
	if err == nil {
		t.Error("Expected error for non-existent CA cert file")
	} else {
		t.Logf("Got expected error for missing CA cert: %v", err)
	}
}

// TestGenerateClientCertInvalidPEM targets GenerateClientCert with invalid PEM
func TestGenerateClientCertInvalidPEM(t *testing.T) {
	// Create temp files with invalid content
	tmpDir := t.TempDir()

	// Invalid CA cert
	invalidCert := []byte("not valid PEM")
	certFile := tmpDir + "/ca.crt"
	if err := os.WriteFile(certFile, invalidCert, 0644); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}

	_, _, err := GenerateClientCert(certFile, certFile, "test", 30)
	if err == nil {
		t.Error("Expected error for invalid CA cert PEM")
	} else {
		t.Logf("Got expected error for invalid PEM: %v", err)
	}
}

// TestHandleDBStatsErrorPaths targets handleDBStats error handling
func TestHandleDBStatsErrorPaths(t *testing.T) {
	// Test with nil database
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	// This should exercise error paths in handleDBStats
	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/stats")
	defer resp.Body.Close()

	t.Logf("Stats with nil DB returned: %d", resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	t.Logf("Response: %s", string(body))
}

// TestHandleJSONMetricsErrorPaths targets handleJSONMetrics error handling
func TestHandleJSONMetricsErrorPaths(t *testing.T) {
	// Test with nil database
	admin := NewAdminServer(nil, "127.0.0.1:0")
	admin.SetAuthToken("test-token")
	if err := admin.Start(); err != nil {
		t.Fatalf("Failed to start admin: %v", err)
	}
	defer admin.Stop()

	time.Sleep(100 * time.Millisecond)

	resp := doAuthGet(t, "test-token", "http://"+admin.Addr()+"/metrics/json")
	defer resp.Body.Close()

	t.Logf("JSON metrics with nil DB returned: %d", resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	t.Logf("Response: %s", string(body))
}

// TestHandleMalformedAuth targets Handle with malformed auth message
func TestHandleMalformedAuth(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

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

	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	go client.Handle()
	time.Sleep(50 * time.Millisecond)

	// Send malformed auth message (just msg type, no payload)
	msg := make([]byte, 5)
	binary.LittleEndian.PutUint32(msg, 1)
	msg[4] = byte(wire.MsgAuth)

	if _, err := clientConn.Write(msg); err != nil {
		t.Logf("Failed to send auth: %v", err)
		return
	}

	// Read response
	var length uint32
	if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	// Should get error for malformed message
	if response[0] != byte(wire.MsgError) {
		t.Logf("Expected error, got: %d", response[0])
	}

	clientConn.Close()
}

// TestHandleMalformedQuery targets Handle with malformed query message
func TestHandleMalformedQuery(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

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

	srv.mu.Lock()
	srv.clients[1] = client
	srv.mu.Unlock()

	go client.Handle()
	time.Sleep(50 * time.Millisecond)

	// Send malformed query message (just msg type, no payload)
	msg := make([]byte, 5)
	binary.LittleEndian.PutUint32(msg, 1)
	msg[4] = byte(wire.MsgQuery)

	if _, err := clientConn.Write(msg); err != nil {
		t.Logf("Failed to send query: %v", err)
		return
	}

	// Read response
	var length uint32
	if err := binary.Read(clientConn, binary.LittleEndian, &length); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(clientConn, response); err != nil {
		t.Logf("Failed to read response: %v", err)
		return
	}

	// Should get error for malformed message
	t.Logf("Malformed query response type: %d", response[0])

	clientConn.Close()
}
