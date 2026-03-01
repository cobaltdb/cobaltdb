package server

import (
	"context"
	"fmt"
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

func TestHandleQueryUpdate(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT, age INTEGER)")
	db.Exec(context.Background(), "INSERT INTO test (id, name, age) VALUES (1, 'Alice', 25)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "UPDATE test SET age = ? WHERE name = ?",
		Params: []interface{}{26, "Alice"},
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

func TestHandleQueryDelete(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT, age INTEGER)")
	db.Exec(context.Background(), "INSERT INTO test (id, name, age) VALUES (1, 'Alice', 25)")
	db.Exec(context.Background(), "INSERT INTO test (id, name, age) VALUES (2, 'Bob', 30)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "DELETE FROM test WHERE age > ?",
		Params: []interface{}{25},
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	okMsg, ok := response.(*wire.OKMessage)
	if !ok {
		t.Fatalf("Expected OK message, got %T", response)
	}
	if okMsg.RowsAffected != 1 {
		t.Errorf("Expected 1 row deleted, got %d", okMsg.RowsAffected)
	}
}

func TestHandleQueryEmptyResult(t *testing.T) {
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

	// Query that returns no results
	query := &wire.QueryMessage{
		SQL: "SELECT * FROM test WHERE name = ?",
		Params: []interface{}{"NonExistent"},
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	resultMsg, ok := response.(*wire.ResultMessage)
	if !ok {
		t.Fatalf("Expected Result message, got %T", response)
	}
	if len(resultMsg.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(resultMsg.Rows))
	}
}

func TestHandleQueryMultipleRows(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")
	for i := 0; i < 100; i++ {
		db.Exec(context.Background(), "INSERT INTO test (id, name) VALUES (?, ?)", i, fmt.Sprintf("user-%d", i))
	}

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "SELECT * FROM test",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	resultMsg, ok := response.(*wire.ResultMessage)
	if !ok {
		t.Fatalf("Expected Result message, got %T", response)
	}
	if len(resultMsg.Rows) != 100 {
		t.Errorf("Expected 100 rows, got %d", len(resultMsg.Rows))
	}
}

func TestHandleQueryWhereCondition(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, age INTEGER)")
	for i := 0; i < 100; i++ {
		db.Exec(context.Background(), "INSERT INTO test (id, age) VALUES (?, ?)", i, i%10)
	}

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL:    "SELECT * FROM test WHERE age >= ?",
		Params: []interface{}{5},
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	resultMsg, ok := response.(*wire.ResultMessage)
	if !ok {
		t.Fatalf("Expected Result message, got %T", response)
	}
	// Should get rows with age 5,6,7,8,9 (10 rows each = 50 rows)
	if len(resultMsg.Rows) != 50 {
		t.Errorf("Expected 50 rows, got %d", len(resultMsg.Rows))
	}
}

func TestHandleMultipleQueries(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	// First query - create table
	q1 := &wire.QueryMessage{SQL: "CREATE TABLE test (id INTEGER, name TEXT)"}
	p1, _ := wire.Encode(q1)
	r1 := client.handleMessage(wire.MsgQuery, p1)
	if _, ok := r1.(*wire.OKMessage); !ok {
		t.Fatalf("Expected OK for CREATE")
	}

	// Second query - insert
	q2 := &wire.QueryMessage{SQL: "INSERT INTO test (id, name) VALUES (1, 'Alice')"}
	p2, _ := wire.Encode(q2)
	r2 := client.handleMessage(wire.MsgQuery, p2)
	if _, ok := r2.(*wire.OKMessage); !ok {
		t.Fatalf("Expected OK for INSERT")
	}

	// Third query - select
	q3 := &wire.QueryMessage{SQL: "SELECT * FROM test"}
	p3, _ := wire.Encode(q3)
	r3 := client.handleMessage(wire.MsgQuery, p3)
	resultMsg, ok := r3.(*wire.ResultMessage)
	if !ok {
		t.Fatalf("Expected Result for SELECT")
	}
	if len(resultMsg.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(resultMsg.Rows))
	}
}

func TestHandleDropTable(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "DROP TABLE test",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	_, ok := response.(*wire.OKMessage)
	if !ok {
		t.Fatalf("Expected OK message, got %T", response)
	}

	// Verify table is dropped
	_, err := db.Query(context.Background(), "SELECT * FROM test")
	if err == nil {
		t.Error("Expected error after dropping table")
	}
}

func TestHandleCreateIndex(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
	}

	query := &wire.QueryMessage{
		SQL: "CREATE INDEX idx_name ON test(name)",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	_, ok := response.(*wire.OKMessage)
	if !ok {
		if errMsg, ok := response.(*wire.ErrorMessage); ok {
			t.Fatalf("Expected OK message, got error: %s", errMsg.Message)
		}
		t.Fatalf("Expected OK message, got %T", response)
	}
}
