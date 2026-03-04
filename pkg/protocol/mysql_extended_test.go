package protocol

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestMySQLServerListenAndClose tests Listen and Close methods
func TestMySQLServerListenAndClose(t *testing.T) {
	t.Run("ListenAndClose", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		err = server.Listen("127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen failed: %v", err)
		}

		// Verify listener is set
		if server.listener == nil {
			t.Error("Expected listener to be set")
		}

		// Close should succeed
		err = server.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	t.Run("CloseWithoutListen", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")

		// Close without Listen should not panic
		err = server.Close()
		if err != nil {
			t.Errorf("Close failed: %v", err)
		}
	})

	t.Run("DoubleClose", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		err = server.Listen("127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen failed: %v", err)
		}

		// First close
		err = server.Close()
		if err != nil {
			t.Errorf("First close failed: %v", err)
		}

		// Second close may or may not error
		_ = server.Close()
	})
}

// TestMySQLClientMethods tests various client methods
func TestMySQLClientMethods(t *testing.T) {
	t.Run("ClientInitialization", func(t *testing.T) {
		conn := newMockConn()
		server := NewMySQLServer(&engine.DB{}, "5.7.0")
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: server,
		}

		if client.conn != conn {
			t.Error("Client conn mismatch")
		}
		if client.server != server {
			t.Error("Client server mismatch")
		}
		if client.database != "" {
			t.Error("Expected empty database initially")
		}
	})

	t.Run("ClientWithDatabase", func(t *testing.T) {
		conn := newMockConn()
		server := NewMySQLServer(&engine.DB{}, "5.7.0")
		client := &MySQLClient{
			conn:     conn,
			reader:   bufio.NewReader(conn),
			server:   server,
			database: "testdb",
		}

		if client.database != "testdb" {
			t.Errorf("Expected database 'testdb', got '%s'", client.database)
		}
	})
}

// TestWritePacketVariations tests writePacket with various inputs
func TestWritePacketVariations(t *testing.T) {
	t.Run("EmptyData", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.writePacket([]byte{}, 0)
		if err != nil {
			t.Errorf("writePacket(empty) failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) != 4 {
			t.Errorf("Expected 4 bytes (header only), got %d", len(data))
		}
	})

	t.Run("SequenceNumber", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.writePacket([]byte("test"), 42)
		if err != nil {
			t.Fatalf("writePacket failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if data[3] != 42 {
			t.Errorf("Expected sequence 42, got %d", data[3])
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

// TestSendOKPacketVariations tests sendOKPacket with various values
func TestSendOKPacketVariations(t *testing.T) {
	t.Run("ZeroValues", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.sendOKPacket(0, 0)
		if err != nil {
			t.Fatalf("sendOKPacket failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) < 4 {
			t.Fatal("OK packet too short")
		}

		// Check payload starts with 0x00
		payload := data[4:]
		if payload[0] != 0x00 {
			t.Errorf("Expected OK packet header 0x00, got 0x%02x", payload[0])
		}
	})

	t.Run("NonZeroValues", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.sendOKPacket(100, 50)
		if err != nil {
			t.Fatalf("sendOKPacket failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) < 4 {
			t.Fatal("OK packet too short")
		}
	})

	t.Run("MaxUint16", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.sendOKPacket(65535, 65535)
		if err != nil {
			t.Fatalf("sendOKPacket failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) < 4 {
			t.Fatal("OK packet too short")
		}
	})
}

// TestSendErrorPacketVariations tests sendErrorPacket with various values
func TestSendErrorPacketVariations(t *testing.T) {
	t.Run("SimpleError", func(t *testing.T) {
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

		data := conn.writeBuf.Bytes()
		if len(data) < 4 {
			t.Fatal("Error packet too short")
		}

		payload := data[4:]
		if payload[0] != 0xff {
			t.Errorf("Expected error packet header 0xff, got 0x%02x", payload[0])
		}
	})

	t.Run("EmptyMessage", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.sendErrorPacket(0, "")
		if err != nil {
			t.Fatalf("sendErrorPacket failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) < 4 {
			t.Fatal("Error packet too short")
		}
	})

	t.Run("LongMessage", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		longMsg := make([]byte, 500)
		for i := range longMsg {
			longMsg[i] = 'a'
		}

		err := client.sendErrorPacket(1064, string(longMsg))
		if err != nil {
			t.Fatalf("sendErrorPacket failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) < 4 {
			t.Fatal("Error packet too short")
		}
	})
}

// TestHandleCommandVariations tests handleCommand with various scenarios
func TestHandleCommandVariations(t *testing.T) {
	buildPacket := func(payload []byte) []byte {
		header := make([]byte, 4)
		header[0] = byte(len(payload))
		header[1] = byte(len(payload) >> 8)
		header[2] = byte(len(payload) >> 16)
		header[3] = 0
		return append(header, payload...)
	}

	t.Run("QuitCommand", func(t *testing.T) {
		conn := newMockConn()
		payload := []byte{MySQLComQuit}
		conn.setReadData(buildPacket(payload))

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
		conn.setReadData(buildPacket(payload))

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err != nil {
			t.Errorf("handleCommand failed: %v", err)
		}

		if conn.writeBuf.Len() == 0 {
			t.Error("Expected OK packet to be written")
		}
	})

	t.Run("InitDBCommand", func(t *testing.T) {
		conn := newMockConn()
		payload := []byte{MySQLComInitDB}
		payload = append(payload, []byte("testdb")...)
		conn.setReadData(buildPacket(payload))

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
		conn.setReadData(buildPacket(payload))

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err != nil {
			t.Errorf("handleCommand failed: %v", err)
		}

		if conn.writeBuf.Len() == 0 {
			t.Error("Expected error packet to be written")
		}
	})

	t.Run("EmptyPacket", func(t *testing.T) {
		conn := newMockConn()
		payload := []byte{}
		conn.setReadData(buildPacket(payload))

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
		conn.setReadData([]byte{0x01, 0x00}) // Only 2 bytes

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

	t.Run("EOF", func(t *testing.T) {
		conn := newMockConn()
		// Empty buffer

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.handleCommand()
		if err == nil {
			t.Error("Expected error for EOF")
		}
	})
}

// TestReadLenEncIntVariations tests readLenEncInt edge cases
func TestReadLenEncIntVariations(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected uint64
		bytes    int
	}{
		{"Zero", []byte{0x00}, 0, 1},
		{"One", []byte{0x01}, 1, 1},
		{"Max1Byte", []byte{0xfa}, 250, 1},
		{"Min2Byte", []byte{0xfc, 0xfb, 0x00}, 251, 3},
		{"Max2Byte", []byte{0xfc, 0xff, 0xff}, 65535, 3},
		{"Min3Byte", []byte{0xfd, 0x00, 0x00, 0x01}, 65536, 4},
		{"Max3Byte", []byte{0xfd, 0xff, 0xff, 0xff}, 16777215, 4},
		{"Min8Byte", []byte{0xfe, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}, 16777216, 9},
		{"Null", []byte{0xfb}, 0, 1},
		{"Empty", []byte{}, 0, 0},
		{"Short2Byte", []byte{0xfc, 0x01}, 0, 0},
		{"Short3Byte", []byte{0xfd, 0x01, 0x00}, 0, 0},
		{"Short8Byte", []byte{0xfe, 0x01, 0x00, 0x00, 0x00}, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, bytes := readLenEncInt(tt.data)
			if value != tt.expected || bytes != tt.bytes {
				t.Errorf("readLenEncInt(%v) = %d, %d; expected %d, %d",
					tt.data, value, bytes, tt.expected, tt.bytes)
			}
		})
	}
}

// TestWriteLenEncIntVariations tests writeLenEncInt edge cases
func TestWriteLenEncIntVariations(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		expected []byte
	}{
		{"Zero", 0, []byte{0x00}},
		{"One", 1, []byte{0x01}},
		{"Max1Byte", 250, []byte{0xfa}},
		{"Min2Byte", 251, []byte{0xfc, 0xfb, 0x00}},
		{"Max2Byte", 65535, []byte{0xfc, 0xff, 0xff}},
		{"Min3Byte", 65536, []byte{0xfd, 0x00, 0x00, 0x01}},
		{"Max3Byte", 16777215, []byte{0xfd, 0xff, 0xff, 0xff}},
		{"Min8Byte", 16777216, []byte{0xfe, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := writeLenEncInt(tt.value)
			if !bytes.Equal(result, tt.expected) {
				t.Errorf("writeLenEncInt(%d) = %v; expected %v",
					tt.value, result, tt.expected)
			}
		})
	}
}

// TestScramblePasswordVariations tests scramblePassword edge cases
func TestScramblePasswordVariations(t *testing.T) {
	t.Run("EmptyPassword", func(t *testing.T) {
		result := scramblePassword([]byte{}, []byte("scramble"))
		if result != nil {
			t.Errorf("Expected nil for empty password, got %v", result)
		}
	})

	t.Run("NormalPassword", func(t *testing.T) {
		password := []byte("password123")
		scramble := []byte("random_scramble_data")

		result := scramblePassword(password, scramble)
		if result == nil {
			t.Fatal("Expected non-nil result")
		}
		if len(result) != 20 {
			t.Errorf("Expected 20 bytes, got %d", len(result))
		}
	})

	t.Run("Deterministic", func(t *testing.T) {
		password := []byte("testpass")
		scramble := []byte("testscramble")

		result1 := scramblePassword(password, scramble)
		result2 := scramblePassword(password, scramble)

		if !bytes.Equal(result1, result2) {
			t.Error("scramblePassword should be deterministic")
		}
	})
}

// TestSendHandshakeVariations tests sendHandshake edge cases
func TestSendHandshakeVariations(t *testing.T) {
	t.Run("DefaultVersion", func(t *testing.T) {
		conn := newMockConn()
		server := NewMySQLServer(&engine.DB{}, "")

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: server,
		}

		err := client.sendHandshake()
		if err != nil {
			t.Fatalf("sendHandshake failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) < 4 {
			t.Fatal("Handshake packet too short")
		}

		// Check protocol version
		payload := data[4:]
		if payload[0] != 0x0a {
			t.Errorf("Expected protocol version 10, got %d", payload[0])
		}
	})

	t.Run("CustomVersion", func(t *testing.T) {
		conn := newMockConn()
		server := NewMySQLServer(&engine.DB{}, "8.0.0-Custom")

		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: server,
		}

		err := client.sendHandshake()
		if err != nil {
			t.Fatalf("sendHandshake failed: %v", err)
		}

		data := conn.writeBuf.Bytes()
		if len(data) < 4 {
			t.Fatal("Handshake packet too short")
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

		err := client.sendHandshake()
		if err == nil {
			t.Error("Expected error for closed connection")
		}
	})
}

// TestReadHandshakeResponseVariations tests readHandshakeResponse edge cases
func TestReadHandshakeResponseVariations(t *testing.T) {
	t.Run("ValidResponse", func(t *testing.T) {
		conn := newMockConn()

		payload := []byte{
			0x85, 0xa2, 0x1a, 0x00,
			0x00, 0x00, 0x00, 0x01,
			0x21,
		}
		payload = append(payload, make([]byte, 23)...)
		payload = append(payload, []byte("user")...)
		payload = append(payload, 0x00)

		header := []byte{byte(len(payload)), 0, 0, 1}
		conn.setReadData(header)
		conn.setReadData(payload)

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
		conn.setReadData([]byte{0x01, 0x00})

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

// TestAcceptLoopVariations tests acceptLoop scenarios
func TestAcceptLoopVariations(t *testing.T) {
	t.Run("AcceptAndClose", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Skip("Cannot create listener:", err)
		}

		server.listener = listener

		go server.acceptLoop()

		time.Sleep(50 * time.Millisecond)

		server.Close()
	})
}

// TestHandleConnectionVariations tests handleConnection scenarios
func TestHandleConnectionVariations(t *testing.T) {
	t.Run("ImmediateEOF", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		conn := newMockConn()

		server.handleConnection(conn)
	})

	t.Run("WithHandshakeResponse", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		conn := newMockConn()

		response := make([]byte, 0, 128)
		response = append(response, 0x00, 0x00, 0x00, 0x00)
		response = append(response, 0x00, 0x00, 0x00, 0x01)
		response = append(response, 0x21)
		response = append(response, make([]byte, 23)...)
		response = append(response, []byte("user")...)
		response = append(response, 0x00)

		pktLen := len(response)
		pkt := make([]byte, 4+pktLen)
		pkt[0] = byte(pktLen)
		pkt[1] = byte(pktLen >> 8)
		pkt[2] = byte(pktLen >> 16)
		pkt[3] = 1
		copy(pkt[4:], response)

		conn.setReadData(pkt)

		server.handleConnection(conn)

		if conn.writeBuf.Len() == 0 {
			t.Error("Expected handshake to be sent")
		}
	})
}

// TestSendResultSetVariations tests sendResultSet scenarios
func TestSendResultSetVariations(t *testing.T) {
	t.Run("NilRows", func(t *testing.T) {
		conn := newMockConn()
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: NewMySQLServer(&engine.DB{}, "5.7.0"),
		}

		err := client.sendResultSet(nil)
		if err != nil {
			t.Errorf("sendResultSet(nil) failed: %v", err)
		}

		if conn.writeBuf.Len() == 0 {
			t.Error("Expected data to be written")
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

		err := client.sendResultSet(nil)
		if err == nil {
			t.Error("Expected error for closed connection")
		}
	})
}

// TestMockConnImplementation tests the mock connection
func TestMockConnImplementation(t *testing.T) {
	t.Run("ReadWrite", func(t *testing.T) {
		conn := newMockConn()

		// Write
		n, err := conn.Write([]byte("hello"))
		if err != nil {
			t.Errorf("Write failed: %v", err)
		}
		if n != 5 {
			t.Errorf("Expected 5 bytes written, got %d", n)
		}

		// Read
		conn.setReadData([]byte("world"))
		buf := make([]byte, 5)
		n, err = conn.Read(buf)
		if err != nil {
			t.Errorf("Read failed: %v", err)
		}
		if n != 5 {
			t.Errorf("Expected 5 bytes read, got %d", n)
		}
	})

	t.Run("Close", func(t *testing.T) {
		conn := newMockConn()
		conn.Close()

		_, err := conn.Write([]byte("test"))
		if err != io.ErrClosedPipe {
			t.Errorf("Expected ErrClosedPipe, got %v", err)
		}

		_, err = conn.Read(make([]byte, 10))
		if err != io.EOF {
			t.Errorf("Expected EOF, got %v", err)
		}
	})

	t.Run("Addresses", func(t *testing.T) {
		conn := newMockConn()

		if conn.LocalAddr() == nil {
			t.Error("Expected non-nil LocalAddr")
		}

		if conn.RemoteAddr() == nil {
			t.Error("Expected non-nil RemoteAddr")
		}
	})

	t.Run("Deadlines", func(t *testing.T) {
		conn := newMockConn()

		if err := conn.SetDeadline(time.Now()); err != nil {
			t.Errorf("SetDeadline failed: %v", err)
		}

		if err := conn.SetReadDeadline(time.Now()); err != nil {
			t.Errorf("SetReadDeadline failed: %v", err)
		}

		if err := conn.SetWriteDeadline(time.Now()); err != nil {
			t.Errorf("SetWriteDeadline failed: %v", err)
		}
	})
}
