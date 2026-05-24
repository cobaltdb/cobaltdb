package protocol

import (
	"bufio"
	"context"
	"encoding/binary"
	"math"
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

func TestCountPreparedParams(t *testing.T) {
	tests := []struct {
		sql  string
		want int
	}{
		{"SELECT * FROM users WHERE id = ? AND name = ?", 2},
		{"SELECT '?' AS literal, col FROM users WHERE id = ?", 1},
		{"INSERT INTO t VALUES (?, '?', ?)", 2},
		{"SELECT 1", 0},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if got := countPreparedParams(tt.sql); got != tt.want {
				t.Fatalf("countPreparedParams() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPreparedStmtParseExecuteArgs(t *testing.T) {
	stmt := &preparedStmt{numParams: 3}
	data := make([]byte, 0, 64)
	data = append(data, 0x01, 0x00, 0x00, 0x00) // stmt id
	data = append(data, 0x00)                   // flags
	data = append(data, 0x01, 0x00, 0x00, 0x00) // iteration count
	data = append(data, 0x00)                   // null bitmap
	data = append(data, 0x01)                   // new params bound
	data = append(data, MySQLTypeLongLong, 0x00)
	data = append(data, MySQLTypeVarString, 0x00)
	data = append(data, MySQLTypeDouble, 0x00)

	var intBuf [8]byte
	binary.LittleEndian.PutUint64(intBuf[:], uint64(42))
	data = append(data, intBuf[:]...)
	data = appendLenEncString(data, "hello")
	var floatBuf [8]byte
	binary.LittleEndian.PutUint64(floatBuf[:], math.Float64bits(9.5))
	data = append(data, floatBuf[:]...)

	args, err := stmt.parseExecuteArgs(data)
	if err != nil {
		t.Fatalf("parseExecuteArgs: %v", err)
	}
	if len(args) != 3 || args[0] != int64(42) || args[1] != "hello" || args[2] != 9.5 {
		t.Fatalf("unexpected args: %#v", args)
	}

	reuseTypes := make([]byte, 0, 32)
	reuseTypes = append(reuseTypes, data[:9]...)
	reuseTypes = append(reuseTypes, 0x01) // first parameter is NULL
	reuseTypes = append(reuseTypes, 0x00) // reuse previous parameter types
	reuseTypes = appendLenEncString(reuseTypes, "again")
	binary.LittleEndian.PutUint64(floatBuf[:], math.Float64bits(1.25))
	reuseTypes = append(reuseTypes, floatBuf[:]...)

	args, err = stmt.parseExecuteArgs(reuseTypes)
	if err != nil {
		t.Fatalf("parseExecuteArgs with cached types: %v", err)
	}
	if args[0] != nil || args[1] != "again" || args[2] != 1.25 {
		t.Fatalf("unexpected cached-type args: %#v", args)
	}
}

func TestPreparedStmtParseExecuteTemporalArgs(t *testing.T) {
	stmt := &preparedStmt{numParams: 4}
	data := buildStmtExecutePacket(
		77,
		[]byte{MySQLTypeDate, MySQLTypeDateTime, MySQLTypeTimestamp, MySQLTypeTime},
		[]interface{}{
			time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 24, 18, 42, 33, 123456000, time.UTC),
			time.Date(2026, 5, 24, 18, 43, 1, 0, time.UTC),
			26*time.Hour + 3*time.Minute + 4*time.Second + 567*time.Microsecond,
		},
	)

	args, err := stmt.parseExecuteArgs(data)
	if err != nil {
		t.Fatalf("parseExecuteArgs: %v", err)
	}
	want := []interface{}{
		"2026-05-24",
		"2026-05-24 18:42:33.123456",
		"2026-05-24 18:43:01",
		"26:03:04.000567",
	}
	if len(args) != len(want) {
		t.Fatalf("len(args) = %d, want %d: %#v", len(args), len(want), args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args[%d] = %#v, want %#v; all args=%#v", i, args[i], want[i], args)
		}
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

	t.Run("PrepareWithParams", func(t *testing.T) {
		client, _ := newTestClient(db)

		err := client.handleStmtPrepare("SELECT name FROM prep_test WHERE id = ? AND name = ?")
		if err != nil {
			t.Fatalf("handleStmtPrepare failed: %v", err)
		}
		if len(client.stmts) != 1 {
			t.Fatalf("expected 1 prepared stmt, got %d", len(client.stmts))
		}
		for _, stmt := range client.stmts {
			if stmt.numParams != 2 {
				t.Fatalf("expected 2 params, got %d", stmt.numParams)
			}
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

	t.Run("ExecuteWithParams", func(t *testing.T) {
		client, _ := newTestClient(db)

		if err := client.handleStmtPrepare("INSERT INTO exec_test (id, val) VALUES (?, ?)"); err != nil {
			t.Skipf("prepare failed: %v", err)
		}

		var stmtID uint32
		for id := range client.stmts {
			stmtID = id
			break
		}

		execData := buildStmtExecutePacket(stmtID, []byte{MySQLTypeLongLong, MySQLTypeVarString}, []interface{}{int64(3), "bound"})
		if err := client.handleStmtExecute(execData); err != nil {
			t.Fatalf("handleStmtExecute: %v", err)
		}

		var got string
		if err := db.QueryRow(ctx, "SELECT val FROM exec_test WHERE id = ?", 3).Scan(&got); err != nil {
			t.Fatalf("query inserted param row: %v", err)
		}
		if got != "bound" {
			t.Fatalf("expected bound value, got %q", got)
		}
	})

	t.Run("ExecuteWithLongData", func(t *testing.T) {
		client, _ := newTestClient(db)

		if err := client.handleStmtPrepare("INSERT INTO exec_test (id, val) VALUES (?, ?)"); err != nil {
			t.Skipf("prepare failed: %v", err)
		}

		var stmtID uint32
		for id := range client.stmts {
			stmtID = id
			break
		}
		if err := client.handleStmtSendLongData(buildStmtLongDataPacket(stmtID, 1, []byte("long-"))); err != nil {
			t.Fatalf("handleStmtSendLongData chunk 1: %v", err)
		}
		if err := client.handleStmtSendLongData(buildStmtLongDataPacket(stmtID, 1, []byte("payload"))); err != nil {
			t.Fatalf("handleStmtSendLongData chunk 2: %v", err)
		}

		execData := buildStmtExecutePacket(stmtID, []byte{MySQLTypeLongLong, MySQLTypeLongBlob}, []interface{}{int64(4), nil})
		if err := client.handleStmtExecute(execData); err != nil {
			t.Fatalf("handleStmtExecute: %v", err)
		}

		var got string
		if err := db.QueryRow(ctx, "SELECT val FROM exec_test WHERE id = ?", 4).Scan(&got); err != nil {
			t.Fatalf("query inserted long data row: %v", err)
		}
		if got != "long-payload" {
			t.Fatalf("expected long data value, got %q", got)
		}
		if len(client.stmts[stmtID].longData) != 0 {
			t.Fatalf("long data was not cleared after execute: %#v", client.stmts[stmtID].longData)
		}
	})
}

func buildStmtExecutePacket(stmtID uint32, types []byte, values []interface{}) []byte {
	data := make([]byte, 0, 64)
	var stmtIDBuf [4]byte
	binary.LittleEndian.PutUint32(stmtIDBuf[:], stmtID)
	data = append(data, stmtIDBuf[:]...)
	data = append(data, 0x00)
	data = append(data, 0x01, 0x00, 0x00, 0x00)
	data = append(data, make([]byte, (len(types)+7)/8)...)
	data = append(data, 0x01)
	for _, typ := range types {
		data = append(data, typ, 0x00)
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		switch types[i] {
		case MySQLTypeLongLong:
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(value.(int64)))
			data = append(data, buf[:]...)
		case MySQLTypeVarString:
			data = appendLenEncString(data, value.(string))
		case MySQLTypeDate, MySQLTypeNewDate:
			data = appendStmtExecuteDate(data, value.(time.Time))
		case MySQLTypeDateTime, MySQLTypeTimestamp:
			data = appendStmtExecuteDateTime(data, value.(time.Time))
		case MySQLTypeTime:
			data = appendStmtExecuteTime(data, value.(time.Duration))
		}
	}
	return data
}

func buildStmtLongDataPacket(stmtID uint32, paramID uint16, payload []byte) []byte {
	data := make([]byte, 0, 6+len(payload))
	var stmtIDBuf [4]byte
	binary.LittleEndian.PutUint32(stmtIDBuf[:], stmtID)
	data = append(data, stmtIDBuf[:]...)
	var paramIDBuf [2]byte
	binary.LittleEndian.PutUint16(paramIDBuf[:], paramID)
	data = append(data, paramIDBuf[:]...)
	data = append(data, payload...)
	return data
}

func appendStmtExecuteDate(dst []byte, value time.Time) []byte {
	dst = append(dst, 4)
	var year [2]byte
	binary.LittleEndian.PutUint16(year[:], uint16(value.Year()))
	dst = append(dst, year[:]...)
	dst = append(dst, byte(value.Month()), byte(value.Day()))
	return dst
}

func appendStmtExecuteDateTime(dst []byte, value time.Time) []byte {
	micro := value.Nanosecond() / int(time.Microsecond)
	if micro == 0 {
		dst = append(dst, 7)
	} else {
		dst = append(dst, 11)
	}
	var year [2]byte
	binary.LittleEndian.PutUint16(year[:], uint16(value.Year()))
	dst = append(dst, year[:]...)
	dst = append(dst, byte(value.Month()), byte(value.Day()), byte(value.Hour()), byte(value.Minute()), byte(value.Second()))
	if micro != 0 {
		var microBuf [4]byte
		binary.LittleEndian.PutUint32(microBuf[:], uint32(micro))
		dst = append(dst, microBuf[:]...)
	}
	return dst
}

func appendStmtExecuteTime(dst []byte, value time.Duration) []byte {
	negative := value < 0
	if negative {
		value = -value
	}
	micro := (value % time.Second) / time.Microsecond
	wholeSeconds := int64(value / time.Second)
	days := wholeSeconds / (24 * 60 * 60)
	wholeSeconds %= 24 * 60 * 60
	hours := wholeSeconds / (60 * 60)
	wholeSeconds %= 60 * 60
	minutes := wholeSeconds / 60
	seconds := wholeSeconds % 60
	if micro == 0 {
		dst = append(dst, 8)
	} else {
		dst = append(dst, 12)
	}
	if negative {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	var daysBuf [4]byte
	binary.LittleEndian.PutUint32(daysBuf[:], uint32(days))
	dst = append(dst, daysBuf[:]...)
	dst = append(dst, byte(hours), byte(minutes), byte(seconds))
	if micro != 0 {
		var microBuf [4]byte
		binary.LittleEndian.PutUint32(microBuf[:], uint32(micro))
		dst = append(dst, microBuf[:]...)
	}
	return dst
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
			42: {id: 42, sql: "SELECT 1", longData: map[int][]byte{0: []byte("stale")}},
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
		if len(client.stmts[42].longData) != 0 {
			t.Fatalf("reset did not clear long data: %#v", client.stmts[42].longData)
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
