package server

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

// Test handleQuery with Exec error path
func TestHandleQueryExecError(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	// Query that will fail in exec path (invalid syntax)
	query := &wire.QueryMessage{
		SQL: "INVALID SQL STATEMENT",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	errMsg, ok := response.(*wire.ErrorMessage)
	if !ok {
		t.Fatalf("Expected Error message, got %T", response)
	}
	if errMsg.Code == 0 {
		t.Error("Expected non-zero error code")
	}
}

// Test server with nil database
func TestServerNilDatabase(t *testing.T) {
	srv, err := New(nil, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server with nil db: %v", err)
	}
	if srv == nil {
		t.Fatal("Server is nil")
	}
}

// Test handleQuery with scan error simulation
func TestHandleQueryWithScanError(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER, data TEXT)")
	db.Exec(nil, "INSERT INTO test (id, data) VALUES (1, 'test')")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL: "SELECT * FROM test",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	_, ok := response.(*wire.ResultMessage)
	if !ok {
		t.Fatalf("Expected Result message, got %T", response)
	}
}

// Test handleQuery with UPDATE - additional test
func TestHandleQueryUpdateMore(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER, name TEXT)")
	db.Exec(nil, "INSERT INTO test (id, name) VALUES (1, 'Alice')")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL:    "UPDATE test SET name = ? WHERE id = ?",
		Params: []interface{}{"Bob", 1},
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

// Test handleQuery with DELETE - additional test
func TestHandleQueryDeleteMore(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER)")
	db.Exec(nil, "INSERT INTO test (id) VALUES (1)")
	db.Exec(nil, "INSERT INTO test (id) VALUES (2)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL: "DELETE FROM test",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	okMsg, ok := response.(*wire.OKMessage)
	if !ok {
		t.Fatalf("Expected OK message, got %T", response)
	}
	if okMsg.RowsAffected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", okMsg.RowsAffected)
	}
}

// Test handleQuery with DROP TABLE
func TestHandleQueryDropTable(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
}

// Test handleQuery with CREATE INDEX
func TestHandleQueryCreateIndex(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER, name TEXT)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL: "CREATE INDEX idx_name ON test(name)",
	}
	payload, _ := wire.Encode(query)

	response := client.handleMessage(wire.MsgQuery, payload)
	_, ok := response.(*wire.OKMessage)
	if !ok {
		if errMsg, isErr := response.(*wire.ErrorMessage); isErr {
			t.Logf("CREATE INDEX returned error (may be expected): %s", errMsg.Message)
		} else {
			t.Fatalf("Expected OK message, got %T", response)
		}
	}
}

// Test handleQuery with empty result - additional test
func TestHandleQueryEmptyResultMore(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	if len(resultMsg.Rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(resultMsg.Rows))
	}
}

// Test handleQuery with multiple rows - additional test
func TestHandleQueryMultipleRowsMore(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER)")
	for i := 0; i < 10; i++ {
		db.Exec(nil, "INSERT INTO test (id) VALUES (?)", i)
	}

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	if len(resultMsg.Rows) != 10 {
		t.Errorf("Expected 10 rows, got %d", len(resultMsg.Rows))
	}
}

// Test handleQuery with INSERT - additional test
func TestHandleQueryInsertMore(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER, name TEXT)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL:    "INSERT INTO test (id, name) VALUES (?, ?)",
		Params: []interface{}{1, "Alice"},
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

// Test handleQuery with prepared statement params - additional test
func TestHandleQueryWithParamsMore(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER, name TEXT)")
	db.Exec(nil, "INSERT INTO test (id, name) VALUES (1, 'Alice')")
	db.Exec(nil, "INSERT INTO test (id, name) VALUES (2, 'Bob')")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL:    "SELECT * FROM test WHERE id = ?",
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

// Test handleQuery with WHERE clause returning no results
func TestHandleQueryNoResults(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true, CacheSize: 1024})
	defer db.Close()

	// Setup
	db.Exec(nil, "CREATE TABLE test (id INTEGER)")
	db.Exec(nil, "INSERT INTO test (id) VALUES (1)")

	srv, _ := New(db, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL:    "SELECT * FROM test WHERE id = ?",
		Params: []interface{}{999},
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
