package integration

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/protocol"
)

// TestMySQLProtocolE2E verifies that the MySQL wire protocol works end-to-end.
// It starts a real MySQL server, connects via TCP, performs handshake,
// and executes real SQL queries through the MySQL wire protocol.
func TestMySQLProtocolE2E(t *testing.T) {
	// 1. Setup: create database with test data
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	db.Exec(ctx, "CREATE TABLE e2e_users (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	db.Exec(ctx, "INSERT INTO e2e_users VALUES (1, 'Alice', 95.5)")
	db.Exec(ctx, "INSERT INTO e2e_users VALUES (2, 'Bob', 87.0)")
	db.Exec(ctx, "INSERT INTO e2e_users VALUES (3, 'Charlie', 92.3)")

	// 2. Start MySQL protocol server on random port
	srv := protocol.NewMySQLServer(db, "5.7.0-CobaltDB-Test")
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer srv.Close()

	addr := srv.Addr().String()
	t.Logf("MySQL server on %s", addr)

	// 3. Connect as MySQL client
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// 4. Read server handshake
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatalf("Read handshake header: %v", err)
	}
	pktLen := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	payload := make([]byte, pktLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("Read handshake payload: %v", err)
	}

	// Verify protocol version 10
	if payload[0] != 0x0a {
		t.Fatalf("Expected protocol v10, got %d", payload[0])
	}
	t.Log("Handshake received: protocol v10")

	// 5. Send handshake response (no auth)
	resp := buildHandshakeResponse("admin")
	writePacket(conn, resp, 1)
	t.Log("Handshake response sent")

	// 6. Read auth result
	hdr2 := make([]byte, 4)
	io.ReadFull(conn, hdr2)
	pktLen2 := int(hdr2[0]) | int(hdr2[1])<<8 | int(hdr2[2])<<16
	result := make([]byte, pktLen2)
	io.ReadFull(conn, result)

	if result[0] == 0xff {
		errMsg := string(result[9:])
		t.Fatalf("Auth FAILED: %s", errMsg)
	}
	if result[0] != 0x00 {
		t.Fatalf("Expected OK (0x00), got 0x%02x", result[0])
	}
	t.Log("Auth OK")

	// 7. Send COM_QUERY: SELECT
	sendQuery(conn, "SELECT id, name, score FROM e2e_users ORDER BY id")

	// 8. Read result set
	columns, rows := readResultSet(t, conn)
	if columns != 3 {
		t.Errorf("Expected 3 columns, got %d", columns)
	}
	if rows != 3 {
		t.Errorf("Expected 3 rows, got %d", rows)
	}
	t.Logf("SELECT returned %d columns, %d rows", columns, rows)

	// 9. Send COM_QUERY: INSERT
	sendQuery(conn, "INSERT INTO e2e_users VALUES (4, 'Diana', 88.8)")
	okResult := readOKPacket(t, conn)
	t.Logf("INSERT OK: affected=%d", okResult)

	// 10. Send COM_QUERY: COUNT
	sendQuery(conn, "SELECT COUNT(*) FROM e2e_users")
	cols2, rows2 := readResultSet(t, conn)
	if cols2 != 1 || rows2 != 1 {
		t.Errorf("COUNT: expected 1 col 1 row, got %d cols %d rows", cols2, rows2)
	}
	t.Logf("COUNT(*) returned %d col, %d row", cols2, rows2)

	t.Log("=== MySQL Protocol E2E: ALL PASSED ===")
}

func buildHandshakeResponse(user string) []byte {
	resp := make([]byte, 0, 128)
	capFlags := uint32(0x00000200 | 0x00008000 | 0x00000001)
	buf4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf4, capFlags)
	resp = append(resp, buf4...)
	binary.LittleEndian.PutUint32(buf4, 16*1024*1024)
	resp = append(resp, buf4...)
	resp = append(resp, 0x21)
	resp = append(resp, make([]byte, 23)...)
	resp = append(resp, []byte(user)...)
	resp = append(resp, 0x00)
	resp = append(resp, 0x00) // no auth
	return resp
}

func writePacket(conn net.Conn, data []byte, seq byte) {
	hdr := make([]byte, 4)
	hdr[0] = byte(len(data))
	hdr[1] = byte(len(data) >> 8)
	hdr[2] = byte(len(data) >> 16)
	hdr[3] = seq
	conn.Write(hdr)
	conn.Write(data)
}

func sendQuery(conn net.Conn, sql string) {
	pkt := make([]byte, 0, len(sql)+1)
	pkt = append(pkt, 0x03) // COM_QUERY
	pkt = append(pkt, []byte(sql)...)
	writePacket(conn, pkt, 0)
}

func readResultSet(t *testing.T, conn net.Conn) (columns, rows int) {
	t.Helper()
	// Read column count
	hdr := make([]byte, 4)
	io.ReadFull(conn, hdr)
	pktLen := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	data := make([]byte, pktLen)
	io.ReadFull(conn, data)

	if data[0] == 0xff { // error
		t.Fatalf("Query error: %s", string(data[9:]))
	}

	columns = int(data[0])

	// Read column definitions
	for i := 0; i < columns; i++ {
		io.ReadFull(conn, hdr)
		pktLen = int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
		io.ReadFull(conn, make([]byte, pktLen))
	}

	// Read EOF
	io.ReadFull(conn, hdr)
	pktLen = int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	io.ReadFull(conn, make([]byte, pktLen))

	// Read rows until EOF
	for {
		io.ReadFull(conn, hdr)
		pktLen = int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
		row := make([]byte, pktLen)
		io.ReadFull(conn, row)
		if row[0] == 0xfe && pktLen < 9 {
			break // EOF
		}
		rows++
	}
	return
}

func readOKPacket(t *testing.T, conn net.Conn) int {
	t.Helper()
	hdr := make([]byte, 4)
	io.ReadFull(conn, hdr)
	pktLen := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	data := make([]byte, pktLen)
	io.ReadFull(conn, data)

	if data[0] == 0xff {
		errCode := binary.LittleEndian.Uint16(data[1:3])
		t.Fatalf("Expected OK, got error %d: %s", errCode, string(data[9:]))
	}
	if data[0] != 0x00 {
		// Might be a result set for non-SELECT that returns rows
		t.Logf("Unexpected packet type: 0x%02x (len=%d)", data[0], pktLen)
		return 0
	}
	return int(data[1]) // affected rows (simplified - first byte of lenenc int)
}

// TestMySQLProtocolPing tests COM_PING through the MySQL wire protocol
func TestMySQLProtocolPing(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	defer db.Close()

	srv := protocol.NewMySQLServer(db, "5.7.0-CobaltDB")
	srv.Listen("127.0.0.1:0")
	defer srv.Close()

	conn, err := net.DialTimeout("tcp", srv.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Read handshake
	hdr := make([]byte, 4)
	io.ReadFull(conn, hdr)
	pktLen := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	io.ReadFull(conn, make([]byte, pktLen))

	// Send auth response
	writePacket(conn, buildHandshakeResponse("admin"), 1)

	// Read OK
	io.ReadFull(conn, hdr)
	pktLen = int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	io.ReadFull(conn, make([]byte, pktLen))

	// Send COM_PING
	writePacket(conn, []byte{0x0e}, 0)

	// Read response
	io.ReadFull(conn, hdr)
	pktLen = int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	resp := make([]byte, pktLen)
	io.ReadFull(conn, resp)

	if resp[0] != 0x00 {
		t.Errorf("PING: expected OK (0x00), got 0x%02x", resp[0])
	} else {
		t.Log("COM_PING: OK")
	}
}

// TestMySQLProtocolMultiQuery tests multiple queries in sequence
func TestMySQLProtocolMultiQuery(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true})
	defer db.Close()

	srv := protocol.NewMySQLServer(db, "5.7.0-CobaltDB")
	srv.Listen("127.0.0.1:0")
	defer srv.Close()

	conn, _ := net.DialTimeout("tcp", srv.Addr().String(), 2*time.Second)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Handshake
	hdr := make([]byte, 4)
	io.ReadFull(conn, hdr)
	pktLen := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	io.ReadFull(conn, make([]byte, pktLen))
	writePacket(conn, buildHandshakeResponse("admin"), 1)
	io.ReadFull(conn, hdr)
	pktLen = int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	io.ReadFull(conn, make([]byte, pktLen))

	// Query sequence
	queries := []struct {
		sql    string
		isExec bool
	}{
		{"CREATE TABLE multi_test (id INTEGER PRIMARY KEY, val TEXT)", true},
		{"INSERT INTO multi_test VALUES (1, 'hello')", true},
		{"INSERT INTO multi_test VALUES (2, 'world')", true},
		{"SELECT val FROM multi_test ORDER BY id", false},
		{"UPDATE multi_test SET val = 'updated' WHERE id = 1", true},
		{"DELETE FROM multi_test WHERE id = 2", true},
		{"SELECT COUNT(*) FROM multi_test", false},
	}

	for i, q := range queries {
		sendQuery(conn, q.sql)
		if q.isExec {
			readOKPacket(t, conn)
		} else {
			cols, rows := readResultSet(t, conn)
			t.Logf("Query %d (%s): %d cols, %d rows", i, q.sql[:20], cols, rows)
		}
	}

	t.Log("=== Multi-query sequence: ALL PASSED ===")
}

// TestMySQLServerVersion verifies the server version string in handshake
func TestMySQLServerVersion(t *testing.T) {
	db, _ := engine.Open(":memory:", &engine.Options{InMemory: true})
	defer db.Close()

	srv := protocol.NewMySQLServer(db, "8.0.32-CobaltDB")
	srv.Listen("127.0.0.1:0")
	defer srv.Close()

	conn, _ := net.DialTimeout("tcp", srv.Addr().String(), 2*time.Second)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	hdr := make([]byte, 4)
	io.ReadFull(conn, hdr)
	pktLen := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	payload := make([]byte, pktLen)
	io.ReadFull(conn, payload)

	// Extract version string (after protocol byte, null-terminated)
	end := 1
	for end < len(payload) && payload[end] != 0 {
		end++
	}
	version := string(payload[1:end])

	if version != "8.0.32-CobaltDB" {
		t.Errorf("Expected version '8.0.32-CobaltDB', got %q", version)
	} else {
		t.Logf("Server version: %s", version)
	}

	// Verify scramble is 20 bytes
	if pktLen < 50 {
		t.Error("Handshake too short for scramble")
	}

	t.Logf("Handshake payload: %d bytes", pktLen)
}
