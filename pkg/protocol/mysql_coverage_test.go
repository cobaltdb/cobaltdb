package protocol

import (
	"bufio"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/auth"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestSetAuthenticator(t *testing.T) {
	server := NewMySQLServer(nil, "")
	if server.auth != nil {
		t.Error("auth should be nil initially")
	}

	a := auth.NewAuthenticator()
	server.SetAuthenticator(a)
	if server.auth == nil {
		t.Error("auth should be set")
	}
}

func TestSetAllowCleartextAuth(t *testing.T) {
	server := NewMySQLServer(nil, "")
	if server.allowCleartextAuth {
		t.Fatal("cleartext auth should be disabled by default")
	}

	server.SetAllowCleartextAuth(true)
	if !server.allowCleartextAuth {
		t.Fatal("cleartext auth should be enabled after setter")
	}
}

func TestValidateMySQLAuthTransport(t *testing.T) {
	tests := []struct {
		name               string
		address            string
		authEnabled        bool
		allowCleartextAuth bool
		wantErr            bool
	}{
		{
			name:        "auth wildcard rejected",
			address:     "0.0.0.0:3306",
			authEnabled: true,
			wantErr:     true,
		},
		{
			name:        "auth empty host rejected",
			address:     ":3306",
			authEnabled: true,
			wantErr:     true,
		},
		{
			name:        "auth loopback allowed",
			address:     "127.0.0.1:3306",
			authEnabled: true,
		},
		{
			name:        "auth localhost allowed",
			address:     "localhost:3306",
			authEnabled: true,
		},
		{
			name:               "explicit cleartext allowed",
			address:            "0.0.0.0:3306",
			authEnabled:        true,
			allowCleartextAuth: true,
		},
		{
			name:    "auth disabled allowed",
			address: "0.0.0.0:3306",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMySQLAuthTransport(tt.address, tt.authEnabled, tt.allowCleartextAuth)
			if tt.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected validation to pass, got %v", err)
			}
		})
	}
}

func TestMySQLListenRejectsAuthenticatedNonLoopbackCleartext(t *testing.T) {
	server := NewMySQLServer(nil, "")
	a := auth.NewAuthenticator()
	a.Enable()
	server.SetAuthenticator(a)

	err := server.Listen("0.0.0.0:0")
	if err == nil {
		t.Fatal("expected Listen to reject authenticated non-loopback cleartext address")
	}
	if !strings.Contains(err.Error(), "MySQL authentication on non-loopback") {
		t.Fatalf("expected auth transport error, got %v", err)
	}
	if server.listener != nil {
		t.Fatal("listener should not be opened after auth transport rejection")
	}
}

func TestMySQLServerConcurrentConfigAccess(t *testing.T) {
	server := NewMySQLServer(nil, "")
	a := auth.NewAuthenticator()
	a.Enable()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(4)
		go func(i int) {
			defer wg.Done()
			server.SetAuthenticator(a)
		}(i)
		go func(i int) {
			defer wg.Done()
			server.SetAllowCleartextAuth(i%2 == 0)
		}(i)
		go func() {
			defer wg.Done()
			_ = server.authEnabled()
		}()
		go func() {
			defer wg.Done()
			_ = server.Addr()
		}()
	}
	wg.Wait()
}

func TestAddrNilListener(t *testing.T) {
	server := NewMySQLServer(nil, "")
	addr := server.Addr()
	if addr != nil {
		t.Error("expected nil addr when listener is nil")
	}
}

func TestSanitizeMySQLError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "no path",
			err:      errors.New("column not found"),
			expected: "column not found",
		},
		{
			name:     "unix path",
			err:      errors.New("error opening /var/data/file.db: permission denied"),
			expected: "error opening (internal error)",
		},
		{
			name:     "windows C path",
			err:      errors.New("error opening C:\\Users\\data\\file.db: permission denied"),
			expected: "error opening (internal error)",
		},
		{
			name:     "windows D path",
			err:      errors.New("error at D:\\Codebox\\cobaltdb\\test"),
			expected: "error at (internal error)",
		},
		{
			name:     "empty error",
			err:      errors.New(""),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeMySQLError(tt.err)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestWriteLenEncIntCoverage(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		minBytes int
		maxBytes int
	}{
		{"zero", 0, 1, 1},
		{"small", 100, 1, 1},
		{"250", 250, 1, 1},
		{"251", 251, 3, 3},         // 0xFC + 2 bytes
		{"65535", 65535, 3, 3},     // 0xFC + 2 bytes
		{"65536", 65536, 4, 4},     // 0xFD + 3 bytes
		{"large", 1<<24 + 1, 9, 9}, // 0xFE + 8 bytes
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendLenEncInt(nil, tt.value)
			if len(result) < tt.minBytes || len(result) > tt.maxBytes {
				t.Errorf("value %d: expected %d-%d bytes, got %d", tt.value, tt.minBytes, tt.maxBytes, len(result))
			}
		})
	}
}

func TestWriteLenEncString(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"hello", "hello"},
		{"unicode", "日本語"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := writeLenEncString(tt.input)
			if result == nil {
				t.Error("expected non-nil result")
			}
			// Result should have length prefix + string bytes
			if len(result) < len(tt.input) {
				t.Errorf("result too short: %d < %d", len(result), len(tt.input))
			}
		})
	}
}

// --- binaryFloat64 ---

func TestBinaryFloat64(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		wantVal float64
		wantOK  bool
	}{
		{name: "float64", input: float64(3.14), wantVal: 3.14, wantOK: true},
		{name: "float32", input: float32(2.5), wantVal: 2.5, wantOK: true},
		{name: "int64", input: int64(42), wantVal: 42.0, wantOK: true},
		{name: "int", input: 42, wantVal: 42.0, wantOK: true},
		{name: "int32", input: int32(42), wantVal: 42.0, wantOK: true},
		{name: "uint64", input: uint64(42), wantVal: 42.0, wantOK: true},
		{name: "string", input: "hello", wantVal: 0, wantOK: false},
		{name: "bool", input: true, wantVal: 0, wantOK: false},
		{name: "nil", input: nil, wantVal: 0, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := binaryFloat64(tt.input)
			if ok != tt.wantOK {
				t.Errorf("binaryFloat64(%v) ok = %v, want %v", tt.input, ok, tt.wantOK)
			}
			if ok && got != tt.wantVal {
				t.Errorf("binaryFloat64(%v) = %v, want %v", tt.input, got, tt.wantVal)
			}
		})
	}
}

// --- handleStatistics ---

func TestMySQLHandleStatistics(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:        conn,
		reader:      bufio.NewReader(conn),
		server:      server,
		connectTime: time.Now(),
	}

	err := client.handleStatistics()
	if err != nil {
		t.Fatalf("handleStatistics failed: %v", err)
	}

	data := conn.writeBuf.Bytes()
	if len(data) == 0 {
		t.Fatal("expected write data from handleStatistics")
	}
	// Should contain "Uptime:" in the packet payload
	if !bytesContains(data, []byte("Uptime:")) {
		t.Errorf("expected statistics string containing Uptime:, got %q", string(data))
	}
}

func bytesContains(b, substr []byte) bool {
	return len(b) >= len(substr) && containsBytes(b, substr)
}

func containsBytes(b, substr []byte) bool {
	for i := 0; i <= len(b)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if b[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// --- handleProcessInfo ---

func TestMySQLHandleProcessInfo(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleProcessInfo()
	if err != nil {
		t.Fatalf("handleProcessInfo failed: %v", err)
	}

	data := conn.writeBuf.Bytes()
	if len(data) < 5 {
		t.Fatal("expected write data from handleProcessInfo (header + payload)")
	}
	// After 4-byte MySQL packet header, the column count (8) is encoded as 0x08
	if data[4] != 0x08 {
		t.Errorf("expected column count 0x08 at offset 4, got 0x%02x", data[4])
	}
}

// --- handleResetConnection ---

func TestMySQLHandleResetConnection(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:       conn,
		reader:     bufio.NewReader(conn),
		server:     server,
		database:   "testdb",
		stmts:      map[uint32]*preparedStmt{1: {}},
		nextStmtID: 5,
	}

	err := client.handleResetConnection()
	if err != nil {
		t.Fatalf("handleResetConnection failed: %v", err)
	}

	// Verify state is cleared
	if client.database != "" {
		t.Errorf("expected database to be cleared, got %q", client.database)
	}
	if len(client.stmts) != 0 {
		t.Errorf("expected stmts to be cleared, got %d entries", len(client.stmts))
	}
	if client.nextStmtID != 0 {
		t.Errorf("expected nextStmtID to be 0, got %d", client.nextStmtID)
	}

	// Verify OK packet was written
	data := conn.writeBuf.Bytes()
	if len(data) < 5 {
		t.Fatal("expected write data from handleResetConnection (header + payload)")
	}
	// After 4-byte MySQL packet header, the OK packet starts with 0x00
	if data[4] != 0x00 {
		t.Errorf("expected OK packet header 0x00 at offset 4, got 0x%02x", data[4])
	}
}
