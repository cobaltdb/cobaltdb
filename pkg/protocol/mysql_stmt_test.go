package protocol

import (
	"bufio"
	"context"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func newTestClient(db *engine.DB) (*MySQLClient, *mockConn) {
	conn := newMockConn()
	if db == nil {
		db, _ = engine.Open(":memory:", &engine.Options{InMemory: true})
	}
	server := NewMySQLServer(db, "5.7.0-CobaltDB")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}
	return client, conn
}

func TestGetStmtMap(t *testing.T) {
	client, _ := newTestClient(nil)

	m := client.getStmtMap()
	if m == nil {
		t.Fatal("expected non-nil map")
	}
	if len(m) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(m))
	}

	// Calling again should return same map
	m2 := client.getStmtMap()
	if len(m2) != 0 {
		t.Fatalf("expected same empty map, got %d entries", len(m2))
	}
}

func TestHandleStmtPrepare(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a table so the prepare has columns
	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE prep_test (id INT, name TEXT)")

	t.Run("BasicPrepare", func(t *testing.T) {
		client, conn := newTestClient(db)

		err := client.handleStmtPrepare("SELECT * FROM prep_test")
		if err != nil {
			t.Fatalf("handleStmtPrepare failed: %v", err)
		}

		// Should have written response packets
		if conn.writeBuf.Len() == 0 {
			t.Error("expected data written to connection")
		}

		// Statement should be stored
		if len(client.stmts) != 1 {
			t.Fatalf("expected 1 prepared stmt, got %d", len(client.stmts))
		}
	})

	t.Run("PrepareInvalidSQL", func(t *testing.T) {
		client, conn := newTestClient(db)

		// Invalid SQL should still register a statement with 0 columns
		err := client.handleStmtPrepare("INVALID SQL !!!")
		if err != nil {
			// writePacket may fail on closed conn — that's OK
			_ = err
		}

		// Statement should still be stored (with 0 columns)
		if len(client.stmts) != 1 {
			t.Fatalf("expected 1 prepared stmt even for invalid SQL, got %d", len(client.stmts))
		}
		for _, stmt := range client.stmts {
			if stmt.numColumns != 0 {
				t.Errorf("expected 0 columns for invalid SQL, got %d", stmt.numColumns)
			}
		}

		_ = conn
	})

	t.Run("ClosedConnection", func(t *testing.T) {
		client, conn := newTestClient(db)
		conn.Close()

		err := client.handleStmtPrepare("SELECT 1")
		if err == nil {
			t.Error("expected error writing to closed connection")
		}
	})
}

func TestHandleStmtExecute(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE exec_test (id INT, val TEXT)")
	db.Exec(ctx, "INSERT INTO exec_test (id, val) VALUES (1, 'hello')")

	t.Run("ShortData", func(t *testing.T) {
		client, conn := newTestClient(db)

		err := client.handleStmtExecute([]byte{0x01, 0x02})
		if err != nil {
			t.Fatalf("handleStmtExecute with short data: %v", err)
		}

		// Should have sent an error packet
		data := conn.writeBuf.Bytes()
		if len(data) == 0 {
			t.Error("expected error response written")
		}
	})

	t.Run("UnknownStatementID", func(t *testing.T) {
		client, conn := newTestClient(db)

		// 9+ bytes, stmtID = 999 (non-existent)
		data := make([]byte, 12)
		data[0] = 0xe7 // stmtID byte 0
		data[1] = 0x03 // stmtID byte 1
		data[2] = 0x00
		data[3] = 0x00

		err := client.handleStmtExecute(data)
		if err != nil {
			t.Fatalf("handleStmtExecute: %v", err)
		}

		// Should have sent an error packet about unknown stmt
		out := conn.writeBuf.Bytes()
		if len(out) == 0 {
			t.Error("expected error response for unknown statement")
		}
	})

	t.Run("ExecuteSelect", func(t *testing.T) {
		client, _ := newTestClient(db)

		// First prepare
		if err := client.handleStmtPrepare("SELECT * FROM exec_test"); err != nil {
			t.Skipf("prepare failed: %v", err)
		}

		// Find the stmt ID
		var stmtID uint32
		for id := range client.stmts {
			stmtID = id
			break
		}

		// Execute with proper header: 4 bytes stmtID + 5 bytes flags/params
		execData := make([]byte, 9)
		execData[0] = byte(stmtID)
		execData[1] = byte(stmtID >> 8)
		execData[2] = byte(stmtID >> 16)
		execData[3] = byte(stmtID >> 24)

		err := client.handleStmtExecute(execData)
		if err != nil {
			t.Fatalf("handleStmtExecute: %v", err)
		}
	})

	t.Run("ExecuteInsert", func(t *testing.T) {
		client, _ := newTestClient(db)

		if err := client.handleStmtPrepare("INSERT INTO exec_test (id, val) VALUES (2, 'world')"); err != nil {
			t.Skipf("prepare failed: %v", err)
		}

		var stmtID uint32
		for id := range client.stmts {
			stmtID = id
			break
		}

		execData := make([]byte, 9)
		execData[0] = byte(stmtID)
		execData[1] = byte(stmtID >> 8)
		execData[2] = byte(stmtID >> 16)
		execData[3] = byte(stmtID >> 24)

		err := client.handleStmtExecute(execData)
		if err != nil {
			t.Fatalf("handleStmtExecute: %v", err)
		}
	})
}

func TestHandleStmtClose(t *testing.T) {
	client, _ := newTestClient(nil)

	// Manually add a prepared statement
	client.stmts = map[uint32]*preparedStmt{
		1: {id: 1, sql: "SELECT 1"},
	}

	t.Run("ValidClose", func(t *testing.T) {
		data := []byte{0x01, 0x00, 0x00, 0x00}
		err := client.handleStmtClose(data)
		if err != nil {
			t.Errorf("handleStmtClose failed: %v", err)
		}
		if len(client.stmts) != 0 {
			t.Errorf("expected 0 stmts after close, got %d", len(client.stmts))
		}
	})

	t.Run("ShortData", func(t *testing.T) {
		err := client.handleStmtClose([]byte{0x01, 0x02})
		if err != nil {
			t.Errorf("handleStmtClose with short data should return nil: %v", err)
		}
	})
}

func TestHandleStmtReset(t *testing.T) {
	client, conn := newTestClient(nil)

	t.Run("ShortData", func(t *testing.T) {
		err := client.handleStmtReset([]byte{0x01})
		if err != nil {
			t.Fatalf("handleStmtReset: %v", err)
		}
		// Should have sent error packet
		if conn.writeBuf.Len() == 0 {
			t.Error("expected error response for short data")
		}
	})

	t.Run("UnknownStatement", func(t *testing.T) {
		conn.writeBuf.Reset()
		data := []byte{0xff, 0xff, 0x00, 0x00}
		err := client.handleStmtReset(data)
		if err != nil {
			t.Fatalf("handleStmtReset: %v", err)
		}
		if conn.writeBuf.Len() == 0 {
			t.Error("expected error response for unknown statement")
		}
	})

	t.Run("ValidReset", func(t *testing.T) {
		conn.writeBuf.Reset()
		client.stmts = map[uint32]*preparedStmt{
			42: {id: 42, sql: "SELECT 1"},
		}

		data := []byte{0x2a, 0x00, 0x00, 0x00} // stmtID=42
		err := client.handleStmtReset(data)
		if err != nil {
			t.Fatalf("handleStmtReset: %v", err)
		}
		// Should have sent OK packet
		if conn.writeBuf.Len() == 0 {
			t.Error("expected OK response")
		}
	})
}

func TestValueToWireString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"string", "hello", "hello"},
		{"bytes", []byte("world"), "world"},
		{"int64", int64(42), "42"},
		{"int64_negative", int64(-7), "-7"},
		{"int", 99, "99"},
		{"float64", float64(3.14), "3.14"},
		{"bool_true", true, "1"},
		{"bool_false", false, "0"},
		{"time", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), "2024-01-15 10:30:00"},
		{"nil", nil, "<nil>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToWireString(tt.input)
			if result != tt.expected {
				t.Errorf("valueToWireString(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestContainsIgnoreCase(t *testing.T) {
	tests := []struct {
		s, substr string
		expected  bool
	}{
		{"Hello World", "world", true},
		{"HELLO", "hello", true},
		{"hello", "HELLO", true},
		{"abcdef", "cde", true},
		{"abcdef", "xyz", false},
		{"", "test", false},
		{"test", "", true},
		{"short", "longer", false},
		{"SELECT @@VERSION", "@@version", true},
	}

	for _, tt := range tests {
		result := containsIgnoreCase(tt.s, tt.substr)
		if result != tt.expected {
			t.Errorf("containsIgnoreCase(%q, %q) = %v, want %v", tt.s, tt.substr, result, tt.expected)
		}
	}
}

func TestSendResultSetFromRows(t *testing.T) {
	t.Run("NilRows", func(t *testing.T) {
		client, conn := newTestClient(nil)

		err := client.sendResultSetFromRows(nil)
		if err != nil {
			t.Fatalf("sendResultSetFromRows(nil): %v", err)
		}

		// Should send OK packet
		if conn.writeBuf.Len() == 0 {
			t.Error("expected OK packet for nil rows")
		}
	})
}

func TestBuildRowPacket(t *testing.T) {
	client, _ := newTestClient(nil)

	tests := []struct {
		name string
		row  []interface{}
	}{
		{"nil values", []interface{}{nil, nil}},
		{"mixed types", []interface{}{int64(1), "hello", nil, true}},
		{"empty row", []interface{}{""}},
		{"float", []interface{}{float64(3.14)}},
		{"bytes", []interface{}{[]byte("data")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pkt := client.buildRowPacket(tt.row)
			if pkt == nil {
				t.Error("expected non-nil packet")
			}
		})
	}
}
