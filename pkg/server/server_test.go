package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

func TestNewServer(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	server, err := New(ps, DefaultConfig())
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
	if config.MaxConnections != defaultMaxConnections {
		t.Errorf("Expected MaxConnections %d, got %d", defaultMaxConnections, config.MaxConnections)
	}
}

func TestNewRejectsEmptyAdminPassword(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	_, err = New(ps, &Config{
		AuthEnabled:      true,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "",
	})
	if err == nil {
		t.Fatal("expected empty admin password to be rejected")
	}
}

func TestNewRequireAuthEnablesAuthentication(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	server, err := New(ps, &Config{
		RequireAuth:      true,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "Str0ng!Pass#2026",
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if !server.auth.IsEnabled() {
		t.Fatal("RequireAuth should enable authentication")
	}
	if _, err := server.auth.Authenticate("admin", "Str0ng!Pass#2026"); err != nil {
		t.Fatalf("default admin should be able to authenticate: %v", err)
	}
}

func TestNewRejectsInvalidResourceConfig(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name   string
		config *Config
		want   string
	}{
		{
			name:   "negative max connections",
			config: &Config{MaxConnections: -1},
			want:   "max connections must be non-negative",
		},
		{
			name:   "negative read timeout",
			config: &Config{ReadTimeout: -1},
			want:   "read timeout must be non-negative",
		},
		{
			name:   "negative write timeout",
			config: &Config{WriteTimeout: -1},
			want:   "write timeout must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := NewProductionServer(db, DefaultProductionConfig())
			server, err := New(ps, tt.config)
			if err == nil {
				t.Fatalf("New succeeded with invalid config: %#v", server)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestNewRejectsTimeoutOverflow(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("timeout overflow cannot be represented by Config int on this architecture")
	}
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tooLarge := int(maxServerTimeoutSeconds + 1)
	tests := []struct {
		name   string
		config *Config
		want   string
	}{
		{
			name:   "read timeout overflow",
			config: &Config{ReadTimeout: tooLarge},
			want:   "read timeout too large",
		},
		{
			name:   "write timeout overflow",
			config: &Config{WriteTimeout: tooLarge},
			want:   "write timeout too large",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := NewProductionServer(db, DefaultProductionConfig())
			_, err := New(ps, tt.config)
			if err == nil {
				t.Fatal("expected invalid timeout to be rejected")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestNewDefaultsZeroResourceConfig(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	server, err := New(ps, &Config{})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if server.maxConnections != defaultMaxConnections {
		t.Fatalf("maxConnections = %d, want %d", server.maxConnections, defaultMaxConnections)
	}
	if server.readTimeout != 300*time.Second {
		t.Fatalf("readTimeout = %v, want 300s", server.readTimeout)
	}
	if server.writeTimeout != 60*time.Second {
		t.Fatalf("writeTimeout = %v, want 60s", server.writeTimeout)
	}
	server, err = New(ps, &Config{MaxConnections: 42})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if server.maxConnections != 42 {
		t.Fatalf("explicit maxConnections = %d, want 42", server.maxConnections)
	}
}

func TestServerClose(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	ps := NewProductionServer(db, DefaultProductionConfig())
	server, err := New(ps, DefaultConfig())
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

type closeTrackingListener struct {
	closed bool
}

func (l *closeTrackingListener) Accept() (net.Conn, error) {
	return nil, net.ErrClosed
}

func (l *closeTrackingListener) Close() error {
	l.closed = true
	return nil
}

func (l *closeTrackingListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func TestServerRejectsListenAfterClose(t *testing.T) {
	server, err := New(nil, DefaultConfig())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := server.Listen("127.0.0.1:0", nil); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("expected ErrServerClosed from Listen after Close, got %v", err)
	}
}

func TestServerClosesProvidedListenerAfterClose(t *testing.T) {
	server, err := New(nil, DefaultConfig())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	listener := &closeTrackingListener{}
	if err := server.ListenOnListener(listener); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("expected ErrServerClosed from ListenOnListener after Close, got %v", err)
	}
	if !listener.closed {
		t.Fatal("ListenOnListener should close a provided listener when server is already closed")
	}
}

func TestListenOnListenerRejectsNilListener(t *testing.T) {
	server, err := New(nil, DefaultConfig())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	err = server.ListenOnListener(nil)
	if err == nil {
		t.Fatal("expected ListenOnListener to reject nil listener")
	}
	if !strings.Contains(err.Error(), "listener cannot be nil") {
		t.Fatalf("expected nil listener error, got %v", err)
	}
	if server.listener != nil {
		t.Fatal("ListenOnListener should not install a nil listener")
	}
}

func TestValidateServerAuthTransport(t *testing.T) {
	tests := []struct {
		name               string
		address            string
		authEnabled        bool
		tlsEnabled         bool
		allowCleartextAuth bool
		wantErr            bool
	}{
		{
			name:        "auth cleartext wildcard rejected",
			address:     "0.0.0.0:4200",
			authEnabled: true,
			wantErr:     true,
		},
		{
			name:        "auth cleartext empty host rejected",
			address:     ":4200",
			authEnabled: true,
			wantErr:     true,
		},
		{
			name:        "auth cleartext loopback allowed",
			address:     "127.0.0.1:4200",
			authEnabled: true,
		},
		{
			name:        "auth cleartext localhost allowed",
			address:     "localhost:4200",
			authEnabled: true,
		},
		{
			name:        "auth with TLS allowed",
			address:     "0.0.0.0:4200",
			authEnabled: true,
			tlsEnabled:  true,
		},
		{
			name:               "explicit cleartext auth allowed",
			address:            "0.0.0.0:4200",
			authEnabled:        true,
			allowCleartextAuth: true,
		},
		{
			name:    "auth disabled allowed",
			address: "0.0.0.0:4200",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServerAuthTransport(tt.address, tt.authEnabled, tt.tlsEnabled, tt.allowCleartextAuth)
			if tt.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected validation to pass, got %v", err)
			}
		})
	}
}

func TestServerListenRejectsAuthCleartextOnNonLoopback(t *testing.T) {
	server, err := New(nil, &Config{
		AuthEnabled:      true,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "Str0ng!Pass#2026",
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	err = server.Listen("0.0.0.0:0", nil)
	if err == nil {
		t.Fatal("expected Listen to reject authenticated cleartext non-loopback address")
	}
	if !strings.Contains(err.Error(), "authentication without TLS") {
		t.Fatalf("expected auth transport error, got %v", err)
	}
}

func TestListenOnListenerRejectsAndClosesAuthCleartextNonLoopback(t *testing.T) {
	server, err := New(nil, &Config{
		AuthEnabled:      true,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "Str0ng!Pass#2026",
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	listener := &closeTrackingListener{}
	err = server.ListenOnListener(listener)
	if err == nil {
		t.Fatal("expected ListenOnListener to reject authenticated cleartext non-loopback listener")
	}
	if !strings.Contains(err.Error(), "authentication without TLS") {
		t.Fatalf("expected auth transport error, got %v", err)
	}
	if !listener.closed {
		t.Fatal("ListenOnListener should close rejected listener")
	}
}

func TestIsBenignNetworkCloseError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "wrapped net err closed",
			err:  fmt.Errorf("close listener: %w", net.ErrClosed),
			want: true,
		},
		{
			name: "tcp already closed message",
			err:  errors.New("close tcp 127.0.0.1:1->127.0.0.1:2: use of closed network connection"),
			want: true,
		},
		{
			name: "actionable close error",
			err:  errors.New("permission denied"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBenignNetworkCloseError(tt.err); got != tt.want {
				t.Fatalf("isBenignNetworkCloseError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestServerWithNilConfig(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, err := New(ps, nil)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	if srv == nil {
		t.Fatal("Server is nil")
	}
}

func TestHandlePing(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	response := client.handleMessage(wire.MsgPing, nil)
	if response != wire.MsgPong {
		t.Errorf("Expected Pong, got %v", response)
	}
}

func TestHandleUnknownMessage(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Create table first
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")
	db.Exec(context.Background(), "INSERT INTO test (id, name) VALUES (1, 'Alice')")

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")
	db.Exec(context.Background(), "INSERT INTO test (id, name) VALUES (1, 'Alice')")

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)

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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT, age INTEGER)")
	db.Exec(context.Background(), "INSERT INTO test (id, name, age) VALUES (1, 'Alice', 25)")

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL:    "UPDATE test SET age = ? WHERE name = ?",
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT, age INTEGER)")
	db.Exec(context.Background(), "INSERT INTO test (id, name, age) VALUES (1, 'Alice', 25)")
	db.Exec(context.Background(), "INSERT INTO test (id, name, age) VALUES (2, 'Bob', 30)")

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	query := &wire.QueryMessage{
		SQL:    "DELETE FROM test WHERE age > ?",
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")
	db.Exec(context.Background(), "INSERT INTO test (id, name) VALUES (1, 'Alice')")

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
	}

	// Query that returns no results
	query := &wire.QueryMessage{
		SQL:    "SELECT * FROM test WHERE name = ?",
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")
	for i := 0; i < 100; i++ {
		db.Exec(context.Background(), "INSERT INTO test (id, name) VALUES (?, ?)", i, fmt.Sprintf("user-%d", i))
	}

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
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
	if len(resultMsg.Rows) != 100 {
		t.Errorf("Expected 100 rows, got %d", len(resultMsg.Rows))
	}
}

func TestHandleQueryWhereCondition(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, age INTEGER)")
	for i := 0; i < 100; i++ {
		db.Exec(context.Background(), "INSERT INTO test (id, age) VALUES (?, ?)", i, i%10)
	}

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
	client := &ClientConn{
		ID:     1,
		Server: srv,
		authed: true,
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
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER)")

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
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

	// Verify table is dropped
	_, err := db.Query(context.Background(), "SELECT * FROM test")
	if err == nil {
		t.Error("Expected error after dropping table")
	}
}

func TestHandleCreateIndex(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024}})
	defer db.Close()

	// Setup
	db.Exec(context.Background(), "CREATE TABLE test (id INTEGER, name TEXT)")

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, nil)
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
		if errMsg, ok := response.(*wire.ErrorMessage); ok {
			t.Fatalf("Expected OK message, got error: %s", errMsg.Message)
		}
		t.Fatalf("Expected OK message, got %T", response)
	}
}
