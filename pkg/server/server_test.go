package server

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

func TestNewServer(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	server, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server == nil {
		t.Fatal("Server is nil")
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Address != ":4200" {
		t.Errorf("Expected address ':4200', got %q", config.Address)
	}
}

func TestServerClose(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	server, err := New(db, DefaultConfig())
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create server: %v", err)
	}

	err = server.Close()
	if err != nil {
		db.Close()
		t.Fatalf("Failed to close server: %v", err)
	}

	// Close again should not error
	err = server.Close()
	if err != nil {
		db.Close()
		t.Fatalf("Failed to close server twice: %v", err)
	}

	db.Close()
}

func TestServerWithNilConfig(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, err := New(db, nil)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	if srv == nil {
		t.Fatal("Server is nil")
	}
}

func TestHandlePing(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	response := client.handleMessage(wire.MsgPing, nil)
	if response != wire.MsgPong {
		t.Errorf("Expected Pong, got %v", response)
	}
}

func TestHandleUnknownMessage(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	response := client.handleMessage(wire.MsgType(99), nil)
	errMsg, ok := response.(*wire.ErrorMessage)
	if !ok {
		t.Fatal("Expected error message")
	}
	if errMsg.Code != 3 {
		t.Errorf("Expected error code 3, got %d", errMsg.Code)
	}
}

func TestHandleQueryCreate(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "CREATE TABLE test (id INTEGER)",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	okMsg, ok := response.(*wire.OKMessage)
	if !ok {
		t.Fatalf("Expected OK message, got %T", response)
	}
	if okMsg.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected for CREATE, got %d", okMsg.RowsAffected)
	}
}

func TestHandleQueryInsert(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Create table first
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "INSERT INTO test (id, name) VALUES (1, 'Alice')",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	okMsg, ok := response.(*wire.OKMessage)
	if !ok {
		t.Fatalf("Expected OK message, got %T", response)
	}
	if okMsg.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", okMsg.RowsAffected)
	}
}

func TestHandleQuerySelect(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")
	db.Exec(context.Background(), "INSERT INTO test (id, name) VALUES (1, 'Alice')")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "SELECT id, name FROM test",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	resultMsg, ok := response.(*wire.ResultMessage)
	if !ok {
		t.Fatalf("Expected Result message, got %T", response)
	}
	if len(resultMsg.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(resultMsg.Columns))
	}
	if len(resultMsg.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(resultMsg.Rows))
	}
}

func TestHandleQueryWithParams(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")
	db.Exec(context.Background(), "INSERT INTO test (id, name) VALUES (1, 'Alice')")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL:    "SELECT id, name FROM test WHERE id = ?",
		Params: []interface{}{1},
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	resultMsg, ok := response.(*wire.ResultMessage)
	if !ok {
		t.Fatalf("Expected Result message, got %T", response)
	}
	if len(resultMsg.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(resultMsg.Rows))
	}
}

func TestHandleQueryError(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "SELECT * FROM nonexistent",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	errMsg, ok := response.(*wire.ErrorMessage)
	if !ok {
		t.Fatal("Expected error message")
	}
	if errMsg.Code == 0 {
		t.Error("Expected non-zero error code")
	}
}

func TestHandleInvalidQueryMessage(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	// Invalid payload
	response := client.handleMessage(wire.MsgQuery, []byte{0xFF, 0xFE})
	errMsg, ok := response.(*wire.ErrorMessage)
	if !ok {
		t.Fatal("Expected error message")
	}
	if errMsg.Code != 2 {
		t.Errorf("Expected error code 2, got %d", errMsg.Code)
	}
}

func TestRemoveClient(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)

	// Add a client manually
	srv.mu.Lock()
	srv.clients[1] = &ClientConn{ID: 1}
	srv.mu.Unlock()

	// Remove the client
	srv.removeClient(1)

	// Verify removal
	srv.mu.RLock()
	if _, exists := srv.clients[1]; exists {
		t.Error("Client should have been removed")
	}
	srv.mu.RUnlock()
}
