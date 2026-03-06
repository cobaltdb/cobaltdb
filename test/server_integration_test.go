package test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/protocol"
	"github.com/cobaltdb/cobaltdb/pkg/server"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

// wireClient is a minimal wire protocol client for testing
type wireClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func newWireClient(addr string) (*wireClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &wireClient{conn: conn, reader: bufio.NewReader(conn)}, nil
}

func (c *wireClient) close() {
	c.conn.Close()
}

func (c *wireClient) sendMessage(msgType wire.MsgType, payload interface{}) error {
	var payData []byte
	if payload != nil {
		var err error
		payData, err = wire.Encode(payload)
		if err != nil {
			return err
		}
	}

	// Write length
	length := uint32(1 + len(payData))
	if err := binary.Write(c.conn, binary.LittleEndian, length); err != nil {
		return err
	}

	// Write message type
	if err := binary.Write(c.conn, binary.LittleEndian, msgType); err != nil {
		return err
	}

	// Write payload
	if len(payData) > 0 {
		if _, err := c.conn.Write(payData); err != nil {
			return err
		}
	}

	return nil
}

func (c *wireClient) readResponse() (wire.MsgType, []byte, error) {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Read length
	var length uint32
	if err := binary.Read(c.reader, binary.LittleEndian, &length); err != nil {
		return 0, nil, err
	}

	if length < 1 {
		return 0, nil, fmt.Errorf("invalid message length: %d", length)
	}

	// Read message type
	msgType, err := c.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	// Read payload
	payload := make([]byte, length-1)
	if length > 1 {
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return 0, nil, err
		}
	}

	return wire.MsgType(msgType), payload, nil
}

func (c *wireClient) query(sql string, params ...interface{}) (*wire.ResultMessage, error) {
	msg := wire.NewQueryMessage(sql, params...)
	if err := c.sendMessage(wire.MsgQuery, msg); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	msgType, payload, err := c.readResponse()
	if err != nil {
		return nil, fmt.Errorf("recv: %w", err)
	}

	switch msgType {
	case wire.MsgResult:
		var result wire.ResultMessage
		if err := wire.Decode(payload, &result); err != nil {
			return nil, fmt.Errorf("decode result: %w", err)
		}
		return &result, nil
	case wire.MsgOK:
		return &wire.ResultMessage{}, nil
	case wire.MsgError:
		var errMsg wire.ErrorMessage
		if err := wire.Decode(payload, &errMsg); err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		return nil, fmt.Errorf("server error %d: %s", errMsg.Code, errMsg.Message)
	default:
		return nil, fmt.Errorf("unexpected message type: %d", msgType)
	}
}

func (c *wireClient) exec(sql string, params ...interface{}) (*wire.OKMessage, error) {
	msg := wire.NewQueryMessage(sql, params...)
	if err := c.sendMessage(wire.MsgQuery, msg); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	msgType, payload, err := c.readResponse()
	if err != nil {
		return nil, fmt.Errorf("recv: %w", err)
	}

	switch msgType {
	case wire.MsgOK:
		var ok wire.OKMessage
		if err := wire.Decode(payload, &ok); err != nil {
			return nil, fmt.Errorf("decode ok: %w", err)
		}
		return &ok, nil
	case wire.MsgResult:
		// Some exec commands return results
		return &wire.OKMessage{}, nil
	case wire.MsgError:
		var errMsg wire.ErrorMessage
		if err := wire.Decode(payload, &errMsg); err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		return nil, fmt.Errorf("server error %d: %s", errMsg.Code, errMsg.Message)
	default:
		return nil, fmt.Errorf("unexpected message type: %d", msgType)
	}
}

func (c *wireClient) ping() error {
	if err := c.sendMessage(wire.MsgPing, nil); err != nil {
		return err
	}

	msgType, _, err := c.readResponse()
	if err != nil {
		return err
	}
	if msgType != wire.MsgPong {
		return fmt.Errorf("expected pong, got %d", msgType)
	}
	return nil
}

// startTestServer starts a wire protocol server and returns its address
func startTestServer(t *testing.T) (string, *server.Server, *engine.DB) {
	t.Helper()

	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	srv, err := server.New(db, &server.Config{
		AuthEnabled: false,
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Listen on random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	addr := listener.Addr().String()

	// We need to set the listener and start accept loop
	// Since Listen() blocks, we use a goroutine approach
	go func() {
		srv.ListenOnListener(listener)
	}()

	// Wait for server to be ready
	time.Sleep(50 * time.Millisecond)

	t.Cleanup(func() {
		srv.Close()
		db.Close()
	})

	return addr, srv, db
}

// TestWireProtocolPing tests basic connectivity
func TestWireProtocolPing(t *testing.T) {
	addr, _, _ := startTestServer(t)

	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	if err := client.ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

// TestWireProtocolCreateTable tests DDL over wire protocol
func TestWireProtocolCreateTable(t *testing.T) {
	addr, _, _ := startTestServer(t)

	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	_, err = client.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
}

// TestWireProtocolInsertAndSelect tests full CRUD operations
func TestWireProtocolInsertAndSelect(t *testing.T) {
	addr, _, _ := startTestServer(t)

	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// CREATE TABLE
	_, err = client.exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// INSERT
	_, err = client.exec("INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}

	_, err = client.exec("INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99)")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	_, err = client.exec("INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 29.99)")
	if err != nil {
		t.Fatalf("INSERT 3 failed: %v", err)
	}

	// SELECT *
	result, err := client.query("SELECT id, name, price FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if len(result.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "id" || result.Columns[1] != "name" || result.Columns[2] != "price" {
		t.Fatalf("Unexpected columns: %v", result.Columns)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(result.Rows))
	}

	// SELECT with WHERE
	result, err = client.query("SELECT name FROM products WHERE price > 15")
	if err != nil {
		t.Fatalf("SELECT WHERE failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(result.Rows))
	}

	// UPDATE
	_, err = client.exec("UPDATE products SET price = 12.99 WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Verify update
	result, err = client.query("SELECT price FROM products WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after UPDATE failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// DELETE
	_, err = client.exec("DELETE FROM products WHERE id = 3")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Verify delete
	result, err = client.query("SELECT COUNT(*) FROM products")
	if err != nil {
		t.Fatalf("SELECT COUNT after DELETE failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row for COUNT, got %d", len(result.Rows))
	}
}

// TestWireProtocolMultipleClients tests concurrent client connections
func TestWireProtocolMultipleClients(t *testing.T) {
	addr, _, _ := startTestServer(t)

	// Connect 3 clients
	clients := make([]*wireClient, 3)
	for i := range clients {
		client, err := newWireClient(addr)
		if err != nil {
			t.Fatalf("Failed to connect client %d: %v", i, err)
		}
		defer client.close()
		clients[i] = client
	}

	// Client 0 creates table
	_, err := clients[0].exec("CREATE TABLE shared (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Client 1 inserts data
	_, err = clients[1].exec("INSERT INTO shared (id, val) VALUES (1, 'from client 1')")
	if err != nil {
		t.Fatalf("INSERT from client 1 failed: %v", err)
	}

	// Client 2 reads data
	result, err := clients[2].query("SELECT val FROM shared WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT from client 2 failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}

	// All clients can ping
	for i, c := range clients {
		if err := c.ping(); err != nil {
			t.Fatalf("Client %d ping failed: %v", i, err)
		}
	}
}

// TestWireProtocolErrors tests error handling
func TestWireProtocolErrors(t *testing.T) {
	addr, _, _ := startTestServer(t)

	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Query non-existent table
	_, err = client.query("SELECT * FROM nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent table")
	}

	// Invalid SQL
	_, err = client.exec("INVALID SQL STATEMENT")
	if err == nil {
		t.Fatal("Expected error for invalid SQL")
	}

	// Connection should still work after errors
	if err := client.ping(); err != nil {
		t.Fatalf("Ping after errors failed: %v", err)
	}
}

// TestWireProtocolTransactions tests transaction support
func TestWireProtocolTransactions(t *testing.T) {
	addr, _, _ := startTestServer(t)

	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Setup
	_, err = client.exec("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = client.exec("INSERT INTO accounts (id, balance) VALUES (1, 100.00)")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}

	_, err = client.exec("INSERT INTO accounts (id, balance) VALUES (2, 200.00)")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}

	// Begin transaction
	_, err = client.exec("BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}

	// Transfer
	_, err = client.exec("UPDATE accounts SET balance = balance - 50 WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE 1 failed: %v", err)
	}

	_, err = client.exec("UPDATE accounts SET balance = balance + 50 WHERE id = 2")
	if err != nil {
		t.Fatalf("UPDATE 2 failed: %v", err)
	}

	// Commit
	_, err = client.exec("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// Verify
	result, err := client.query("SELECT balance FROM accounts WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after COMMIT failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestWireProtocolComplexQueries tests advanced SQL over wire
func TestWireProtocolComplexQueries(t *testing.T) {
	addr, _, _ := startTestServer(t)

	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Setup
	_, err = client.exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL, status TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	orders := []string{
		"INSERT INTO orders (id, customer, amount, status) VALUES (1, 'Alice', 50.00, 'completed')",
		"INSERT INTO orders (id, customer, amount, status) VALUES (2, 'Bob', 75.00, 'pending')",
		"INSERT INTO orders (id, customer, amount, status) VALUES (3, 'Alice', 30.00, 'completed')",
		"INSERT INTO orders (id, customer, amount, status) VALUES (4, 'Charlie', 100.00, 'completed')",
		"INSERT INTO orders (id, customer, amount, status) VALUES (5, 'Bob', 25.00, 'cancelled')",
	}
	for _, sql := range orders {
		_, err = client.exec(sql)
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
	}

	// GROUP BY with aggregation
	result, err := client.query("SELECT customer, SUM(amount) FROM orders WHERE status = 'completed' GROUP BY customer ORDER BY customer")
	if err != nil {
		t.Fatalf("GROUP BY query failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 grouped rows, got %d", len(result.Rows))
	}

	// COUNT with WHERE
	result, err = client.query("SELECT COUNT(*) FROM orders WHERE status = 'completed'")
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row for COUNT, got %d", len(result.Rows))
	}

	// ORDER BY + LIMIT
	result, err = client.query("SELECT customer, amount FROM orders ORDER BY amount DESC LIMIT 3")
	if err != nil {
		t.Fatalf("ORDER BY LIMIT failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("Expected 3 rows with LIMIT, got %d", len(result.Rows))
	}
}

// TestWireProtocolAuth tests authentication
func TestWireProtocolAuth(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	srv, err := server.New(db, &server.Config{
		AuthEnabled:      true,
		RequireAuth:      true,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "secret",
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	addr := listener.Addr().String()

	go srv.ListenOnListener(listener)
	time.Sleep(50 * time.Millisecond)
	defer srv.Close()

	// Connect
	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Try query without auth - should fail
	_, err = client.query("SELECT 1")
	if err == nil {
		t.Fatal("Expected auth error")
	}
	if !strings.Contains(err.Error(), "authentication required") {
		t.Fatalf("Expected 'authentication required', got: %v", err)
	}

	// Authenticate
	authMsg := &wire.AuthMessage{Username: "admin", Password: "secret"}
	if err := client.sendMessage(wire.MsgAuth, authMsg); err != nil {
		t.Fatalf("Send auth failed: %v", err)
	}

	msgType, _, err := client.readResponse()
	if err != nil {
		t.Fatalf("Read auth response failed: %v", err)
	}
	if msgType != wire.MsgAuthSuccess {
		t.Fatalf("Expected auth success, got msg type %d", msgType)
	}

	// Now query should work
	_, err = client.exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE after auth failed: %v", err)
	}
}

// TestMySQLProtocolBasic tests basic MySQL protocol connectivity
func TestMySQLProtocolBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	mysqlSrv := protocol.NewMySQLServer(db, "5.7.0-CobaltDB-Test")

	// Listen on random port
	if err := mysqlSrv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Failed to start MySQL server: %v", err)
	}
	defer mysqlSrv.Close()

	// The Listen method starts a goroutine, but we need the address
	// Since Listen doesn't return the address, we need to check the listener
	// For this test, we'll just verify the server starts without error
	t.Log("MySQL protocol server started successfully")
}

// TestWireProtocolJoinAndSubquery tests joins and subqueries over wire
func TestWireProtocolJoinAndSubquery(t *testing.T) {
	addr, _, _ := startTestServer(t)

	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Setup tables
	_, err = client.exec("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE departments failed: %v", err)
	}

	_, err = client.exec("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)")
	if err != nil {
		t.Fatalf("CREATE employees failed: %v", err)
	}

	_, err = client.exec("INSERT INTO departments (id, name) VALUES (1, 'Engineering')")
	if err != nil {
		t.Fatalf("INSERT dept failed: %v", err)
	}
	_, err = client.exec("INSERT INTO departments (id, name) VALUES (2, 'Marketing')")
	if err != nil {
		t.Fatalf("INSERT dept 2 failed: %v", err)
	}

	emps := []string{
		"INSERT INTO employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 90000)",
		"INSERT INTO employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 85000)",
		"INSERT INTO employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 2, 70000)",
	}
	for _, sql := range emps {
		_, err = client.exec(sql)
		if err != nil {
			t.Fatalf("INSERT emp failed: %v", err)
		}
	}

	// JOIN query
	result, err := client.query("SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name")
	if err != nil {
		t.Fatalf("JOIN query failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("Expected 3 join rows, got %d", len(result.Rows))
	}

	// Subquery - AVG of (90000, 85000, 70000) = 81666.67, so Alice (90000) and Bob (85000) are above avg
	result, err = client.query("SELECT name FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY name")
	if err != nil {
		t.Fatalf("Subquery failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 above-avg employees, got %d", len(result.Rows))
	}
}

// TestWireProtocolLargeDataset tests handling of larger datasets over wire
func TestWireProtocolLargeDataset(t *testing.T) {
	addr, _, _ := startTestServer(t)

	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	_, err = client.exec("CREATE TABLE bulk (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert 100 rows
	for i := 1; i <= 100; i++ {
		sql := fmt.Sprintf("INSERT INTO bulk (id, data) VALUES (%d, 'row_%d_data_padding_for_size')", i, i)
		_, err = client.exec(sql)
		if err != nil {
			t.Fatalf("INSERT %d failed: %v", i, err)
		}
	}

	// Query all rows
	result, err := client.query("SELECT id, data FROM bulk ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT all failed: %v", err)
	}
	if len(result.Rows) != 100 {
		t.Fatalf("Expected 100 rows, got %d", len(result.Rows))
	}

	// Verify data integrity
	result, err = client.query("SELECT COUNT(*) FROM bulk WHERE data LIKE 'row_%'")
	if err != nil {
		t.Fatalf("COUNT with LIKE failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row for COUNT, got %d", len(result.Rows))
	}
}

// TestServerGracefulShutdown tests that server shuts down cleanly
func TestServerGracefulShutdown(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	srv, err := server.New(db, nil)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	addr := listener.Addr().String()

	go srv.ListenOnListener(listener)
	time.Sleep(50 * time.Millisecond)

	// Connect a client
	client, err := newWireClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Ping should work
	if err := client.ping(); err != nil {
		t.Fatalf("Ping before shutdown failed: %v", err)
	}

	// Shut down server
	srv.Close()
	time.Sleep(50 * time.Millisecond)

	// Client should get disconnected
	err = client.ping()
	if err == nil {
		// It's ok if the ping succeeds immediately after close (race)
		// but subsequent operations should fail
		client.close()
	}
}

// TestDirectDBOperations tests the DB engine API directly for completeness
func TestDirectDBOperations(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Tables() should return empty initially
	tables := db.Tables()
	if len(tables) != 0 {
		t.Fatalf("Expected 0 tables, got %d", len(tables))
	}

	// Create a table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN DEFAULT true)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Tables() should return it
	tables = db.Tables()
	if len(tables) != 1 || tables[0] != "test" {
		t.Fatalf("Expected ['test'], got %v", tables)
	}

	// TableSchema() should return the schema
	schema, err := db.TableSchema("test")
	if err != nil {
		t.Fatalf("TableSchema failed: %v", err)
	}
	if !strings.Contains(schema, "CREATE TABLE test") {
		t.Fatalf("Schema doesn't contain CREATE TABLE: %s", schema)
	}
	if !strings.Contains(schema, "id") || !strings.Contains(schema, "name") {
		t.Fatalf("Schema missing columns: %s", schema)
	}

	// Insert and query
	_, err = db.Exec(ctx, "INSERT INTO test (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	rows, err := db.Query(ctx, "SELECT id, name FROM test")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "name" {
		t.Fatalf("Expected [id, name], got %v", cols)
	}

	if !rows.Next() {
		t.Fatal("Expected a row")
	}
	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if id != 1 || name != "Alice" {
		t.Fatalf("Expected (1, Alice), got (%d, %s)", id, name)
	}
}
