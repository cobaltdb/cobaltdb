package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// mockConn is a mock net.Conn for testing
type mockConn struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	closed   bool
}

func newMockConn() *mockConn {
	return &mockConn{
		readBuf:  &bytes.Buffer{},
		writeBuf: &bytes.Buffer{},
		closed:   false,
	}
}

func (m *mockConn) Read(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.EOF
	}
	return m.readBuf.Read(p)
}

func (m *mockConn) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	return m.writeBuf.Write(p)
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3306} }
func (m *mockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

func TestMySQLServerNew(t *testing.T) {
	t.Run("NewWithDefaultVersion", func(t *testing.T) {
		db := &engine.DB{}
		server := NewMySQLServer(db, "")

		if server == nil {
			t.Fatal("Expected non-nil server")
		}

		if server.version != "5.7.0-CobaltDB" {
			t.Errorf("Expected default version, got %s", server.version)
		}
	})

	t.Run("NewWithCustomVersion", func(t *testing.T) {
		db := &engine.DB{}
		server := NewMySQLServer(db, "8.0.0-Custom")

		if server.version != "8.0.0-Custom" {
			t.Errorf("Expected custom version, got %s", server.version)
		}
	})
}

func TestMySQLClientSendHandshake(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0-CobaltDB")

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.sendHandshake()
	if err != nil {
		t.Fatalf("sendHandshake failed: %v", err)
	}

	// Check that something was written
	if conn.writeBuf.Len() == 0 {
		t.Error("Expected handshake data to be written")
	}

	// Verify packet structure (first 4 bytes are header)
	data := conn.writeBuf.Bytes()
	if len(data) < 4 {
		t.Fatal("Handshake packet too short")
	}

	// Parse packet header
	length := int(data[0]) | int(data[1])<<8 | int(data[2])<<8
	// sequence := data[3]

	payload := data[4:]
	if len(payload) != length {
		t.Errorf("Payload length mismatch: expected %d, got %d", length, len(payload))
	}

	// Check protocol version (first byte of payload should be 0x0a = 10)
	if payload[0] != 0x0a {
		t.Errorf("Expected protocol version 10, got %d", payload[0])
	}
}

func TestMySQLClientReadHandshakeResponse(t *testing.T) {
	t.Run("ValidResponse", func(t *testing.T) {
		conn := newMockConn()

		// Build a valid handshake response packet
		payload := []byte{
			0x85, 0xa2, 0x1a, 0x00, // capability flags
			0x00, 0x00, 0x00, 0x01, // max packet size
			0x21,       // character set (utf8mb4)
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved
		}
		// Add username
		payload = append(payload, []byte("testuser")...)
		payload = append(payload, 0x00)

		// Write packet header + payload
		header := make([]byte, 4)
		header[0] = byte(len(payload))
		header[1] = byte(len(payload) >> 8)
		header[2] = byte(len(payload) >> 16)
		header[3] = 1 // sequence

		conn.readBuf.Write(header)
		conn.readBuf.Write(payload)

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.readHandshakeResponse()
		if err != nil {
			t.Errorf("readHandshakeResponse failed: %v", err)
		}
	})

	t.Run("ShortHeader", func(t *testing.T) {
		conn := newMockConn()
		conn.readBuf.Write([]byte{0x01, 0x00}) // Only 2 bytes

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.readHandshakeResponse()
		if err == nil {
			t.Error("Expected error for short header")
		}
	})

	t.Run("EOF", func(t *testing.T) {
		conn := newMockConn()
		// Empty buffer

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.readHandshakeResponse()
		if err == nil {
			t.Error("Expected error for EOF")
		}
	})
}

func TestMySQLClientHandleCommand(t *testing.T) {
	buildPacket := func(payload []byte) []byte {
		header := make([]byte, 4)
		header[0] = byte(len(payload))
		header[1] = byte(len(payload) >> 8)
		header[2] = byte(len(payload) >> 16)
		header[3] = 0 // sequence
		return append(header, payload...)
	}

	t.Run("QuitCommand", func(t *testing.T) {
		conn := newMockConn()
		payload := []byte{MySQLComQuit}
		conn.readBuf.Write(buildPacket(payload))

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err != io.EOF {
			t.Errorf("Expected EOF for quit command, got %v", err)
		}
	})

	t.Run("PingCommand", func(t *testing.T) {
		conn := newMockConn()
		payload := []byte{MySQLComPing}
		conn.readBuf.Write(buildPacket(payload))

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err != nil {
			t.Errorf("handleCommand failed: %v", err)
		}

		// Should have written OK packet
		if conn.writeBuf.Len() == 0 {
			t.Error("Expected OK packet to be written")
		}
	})

	t.Run("InitDBCommand", func(t *testing.T) {
		conn := newMockConn()
		payload := []byte{MySQLComInitDB}
		payload = append(payload, []byte("testdb")...)
		conn.readBuf.Write(buildPacket(payload))

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err != nil {
			t.Errorf("handleCommand failed: %v", err)
		}

		if client.database != "testdb" {
			t.Errorf("Expected database 'testdb', got '%s'", client.database)
		}
	})

	t.Run("UnsupportedCommand", func(t *testing.T) {
		conn := newMockConn()
		payload := []byte{MySQLComShutdown}
		conn.readBuf.Write(buildPacket(payload))

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err != nil {
			t.Errorf("handleCommand failed: %v", err)
		}

		// Should have written error packet
		if conn.writeBuf.Len() == 0 {
			t.Error("Expected error packet to be written")
		}
	})

	t.Run("EmptyPacket", func(t *testing.T) {
		conn := newMockConn()
		payload := []byte{}
		conn.readBuf.Write(buildPacket(payload))

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err == nil {
			t.Error("Expected error for empty packet")
		}
	})

	t.Run("ShortHeader", func(t *testing.T) {
		conn := newMockConn()
		conn.readBuf.Write([]byte{0x01, 0x00}) // Only 2 bytes

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err == nil {
			t.Error("Expected error for short header")
		}
	})
}

func TestMySQLClientSendOKPacket(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.sendOKPacket(1, 0)
	if err != nil {
		t.Fatalf("sendOKPacket failed: %v", err)
	}

	// Verify packet structure
	data := conn.writeBuf.Bytes()
	if len(data) < 4 {
		t.Fatal("OK packet too short")
	}

	// Parse header
	length := int(data[0]) | int(data[1])<<8 | int(data[2])<<8
	payload := data[4:]

	if len(payload) != length {
		t.Errorf("Payload length mismatch: expected %d, got %d", length, len(payload))
	}

	// First byte should be 0x00 for OK packet
	if payload[0] != 0x00 {
		t.Errorf("Expected OK packet header 0x00, got 0x%02x", payload[0])
	}
}

func TestMySQLClientSendErrorPacket(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.sendErrorPacket(1064, "Syntax error")
	if err != nil {
		t.Fatalf("sendErrorPacket failed: %v", err)
	}

	// Verify packet structure
	data := conn.writeBuf.Bytes()
	if len(data) < 4 {
		t.Fatal("Error packet too short")
	}

	payload := data[4:]

	// First byte should be 0xff for error packet
	if payload[0] != 0xff {
		t.Errorf("Expected error packet header 0xff, got 0x%02x", payload[0])
	}

	// Check error code (bytes 1-2, little endian)
	errorCode := binary.LittleEndian.Uint16(payload[1:3])
	if errorCode != 1064 {
		t.Errorf("Expected error code 1064, got %d", errorCode)
	}
}

func TestMySQLClientWritePacket(t *testing.T) {
	t.Run("SmallPacket", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		payload := []byte("test payload")
		err := client.writePacket(payload, 0)
		if err != nil {
			t.Fatalf("writePacket failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) != 4+len(payload) {
			t.Errorf("Expected %d bytes, got %d", 4+len(payload), len(data))
		}
	})

	t.Run("LargePacket", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		// Payload larger than max packet size (16MB)
		payload := make([]byte, 17*1024*1024)
		for i := range payload {
			payload[i] = byte(i % 256)
		}

		err := client.writePacket(payload, 0)
		if err != nil {
			t.Fatalf("writePacket failed: %v", err)
		}

		// Should be split into multiple packets
		data := conn.writeBuf.Bytes()
		if len(data) <= len(payload) {
			t.Error("Expected packet splitting for large payload")
		}
	})

	t.Run("ClosedConnection", func(t *testing.T) {
		conn := newMockConn()
		conn.Close()

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.writePacket([]byte("test"), 0)
		if err == nil {
			t.Error("Expected error for closed connection")
		}
	})
}

func TestMySQLClientSendResultSet(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	// sendResultSet takes an interface{}, typically sql.Rows
	// For testing, we pass nil which should return OK packet
	err := client.sendResultSet(nil)
	if err != nil {
		t.Fatalf("sendResultSet failed: %v", err)
	}

	// Verify something was written (OK packet)
	if conn.writeBuf.Len() == 0 {
		t.Error("Expected result set data to be written")
	}

	data := conn.writeBuf.Bytes()
	if len(data) < 4 {
		t.Fatal("Result set too short")
	}
}

func TestMySQLClientHandleQuery(t *testing.T) {
	t.Run("SimpleQuery", func(t *testing.T) {
		conn := newMockConn()
		// Create a properly initialized DB
		db, err := engine.Open("memory", nil)
		if err != nil {
			t.Skipf("Failed to create DB: %v", err)
			return
		}
		defer db.Close()

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(db, "5.7.0"),
		}

		// This should work with a real DB
		err = client.handleQuery("SELECT 1")
		// May error due to SQL parsing but should not panic
		t.Logf("Query result: %v", err)
	})

	t.Run("EmptyQuery", func(t *testing.T) {
		conn := newMockConn()
		db, err := engine.Open("memory", nil)
		if err != nil {
			t.Skipf("Failed to create DB: %v", err)
			return
		}
		defer db.Close()

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(db, "5.7.0"),
		}

		err = client.handleQuery("")
		// May or may not error
		t.Logf("Empty query result: %v", err)
	})
}

func TestMySQLConstants(t *testing.T) {
	// Verify some key constants
	if MySQLComQuery != 0x03 {
		t.Errorf("Expected MySQLComQuery = 0x03, got 0x%02x", MySQLComQuery)
	}

	if MySQLComQuit != 0x01 {
		t.Errorf("Expected MySQLComQuit = 0x01, got 0x%02x", MySQLComQuit)
	}

	if MySQLComPing != 0x0e {
		t.Errorf("Expected MySQLComPing = 0x0e, got 0x%02x", MySQLComPing)
	}

	if MySQLTypeLong != 0x03 {
		t.Errorf("Expected MySQLTypeLong = 0x03, got 0x%02x", MySQLTypeLong)
	}

	if MySQLTypeVarchar != 0x0f {
		t.Errorf("Expected MySQLTypeVarchar = 0x0f, got 0x%02x", MySQLTypeVarchar)
	}
}

func TestMySQLServerClose(t *testing.T) {
	t.Run("CloseWithListener", func(t *testing.T) {
		server := NewMySQLServer(&engine.DB{}, "5.7.0")

		// Start listening on a random port
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Failed to create listener: %v", err)
		}
		server.listener = listener

		err = server.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	t.Run("CloseWithoutListener", func(t *testing.T) {
		server := NewMySQLServer(&engine.DB{}, "5.7.0")

		err := server.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})
}

