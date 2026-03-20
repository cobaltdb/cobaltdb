package protocol

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestListenCoverage tests the Listen function for coverage
func TestListenCoverage(t *testing.T) {
	t.Run("ListenSuccess", func(t *testing.T) {
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
		defer server.Close()

		if server.listener == nil {
			t.Error("Expected listener to be set")
		}
	})

	t.Run("ListenFailure", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		err = server.Listen("invalid:address:format:too:many:colons")
		if err == nil {
			t.Error("Expected error for invalid address")
		}
	})
}

// TestAcceptLoopCoverage tests the acceptLoop function for coverage
func TestAcceptLoopCoverage(t *testing.T) {
	t.Run("AcceptLoopWithConnection", func(t *testing.T) {
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

		// Start acceptLoop
		go server.acceptLoop()

		// Give it time to start
		time.Sleep(100 * time.Millisecond)

		// Connect to trigger accept
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Logf("Connection failed (expected): %v", err)
		} else {
			conn.Close()
		}

		server.Close()
	})

	t.Run("AcceptLoopNilListener", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		// Don't set listener

		// This should return immediately
		go server.acceptLoop()

		time.Sleep(50 * time.Millisecond)
	})
}

// TestHandleConnectionCoverage tests the handleConnection function for coverage
func TestHandleConnectionCoverage(t *testing.T) {
	t.Run("HandleConnectionComplete", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		conn := newMockConn()

		// Build a complete handshake response
		response := make([]byte, 0, 128)
		response = append(response, 0x00, 0x00, 0x00, 0x00) // capability flags
		response = append(response, 0x00, 0x00, 0x00, 0x01) // max packet size
		response = append(response, 0x21)                   // character set
		response = append(response, make([]byte, 23)...)    // reserved
		response = append(response, []byte("testuser")...)  // username
		response = append(response, 0x00)

		pktLen := len(response)
		pkt := make([]byte, 4+pktLen)
		pkt[0] = byte(pktLen)
		pkt[1] = byte(pktLen >> 8)
		pkt[2] = byte(pktLen >> 16)
		pkt[3] = 1
		copy(pkt[4:], response)

		conn.setReadData(pkt)

		// Add a ping command to test command handling
		pingPkt := []byte{0x01, 0x00, 0x00, 0x00, MySQLComPing}
		conn.setReadData(pingPkt)

		// This will run until EOF
		server.handleConnection(conn)

		// Check handshake was sent
		if conn.writeBuf.Len() == 0 {
			t.Error("Expected handshake to be sent")
		}
	})

	t.Run("HandleConnectionSendHandshakeError", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		conn := newMockConn()
		conn.Close() // Close immediately to cause write error

		// Should exit cleanly without panic
		server.handleConnection(conn)
	})

	t.Run("HandleConnectionReadResponseError", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		server := NewMySQLServer(db, "test")
		conn := newMockConn()

		// Don't add any response data - will cause EOF after handshake

		server.handleConnection(conn)

		// Check handshake was sent
		if conn.writeBuf.Len() == 0 {
			t.Error("Expected handshake to be sent")
		}
	})
}

// TestHandleQueryCoverage tests the handleQuery function for coverage
func TestHandleQueryCoverage(t *testing.T) {
	t.Run("HandleQuerySelect", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		conn := newMockConn()
		server := NewMySQLServer(db, "test")
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: server,
		}

		// Test a SELECT query
		err = client.handleQuery("SELECT 1")
		// May error but shouldn't panic
		t.Logf("Query result: %v", err)
	})

	t.Run("HandleQueryExec", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		conn := newMockConn()
		server := NewMySQLServer(db, "test")
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: server,
		}

		// Test a CREATE TABLE query
		err = client.handleQuery("CREATE TABLE test (id INT)")
		// May error but shouldn't panic
		t.Logf("Exec result: %v", err)
	})

	t.Run("HandleQueryWhitespace", func(t *testing.T) {
		db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
		if err != nil {
			t.Skip("Cannot open database:", err)
		}
		defer db.Close()

		conn := newMockConn()
		server := NewMySQLServer(db, "test")
		client := &MySQLClient{
			conn:   conn,
			reader: bufio.NewReader(conn),
			server: server,
		}

		// Test query with whitespace
		err = client.handleQuery("  SELECT 1  ")
		t.Logf("Whitespace query result: %v", err)
	})
}

// TestFullServerLifecycle tests a complete server lifecycle
func TestFullServerLifecycle(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Skip("Cannot open database:", err)
	}
	defer db.Close()

	server := NewMySQLServer(db, "5.7.0-CobaltDB")

	// Start server
	err = server.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}

	// Get server address
	addr := server.listener.Addr().String()

	// Connect to server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Set read deadline
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// Read handshake packet header first (4 bytes)
	header := make([]byte, 4)
	_, err = io.ReadFull(conn, header)
	if err != nil {
		t.Fatalf("Failed to read handshake header: %v", err)
	}

	// Parse handshake length
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16

	// Read the rest of the handshake packet
	payload := make([]byte, length)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		t.Fatalf("Failed to read handshake payload: %v", err)
	}

	// Combine header and payload
	handshake := append(header, payload...)

	// Verify protocol version
	if handshake[4] != 0x0a {
		t.Errorf("Expected protocol version 10, got %d", handshake[4])
	}

	// Send handshake response
	response := make([]byte, 0, 128)
	response = append(response, 0x00, 0x00, 0x00, 0x00) // capability flags
	response = append(response, 0x00, 0x00, 0x00, 0x01) // max packet size
	response = append(response, 0x21)                   // character set
	response = append(response, make([]byte, 23)...)    // reserved
	response = append(response, []byte("testuser")...)  // username
	response = append(response, 0x00)

	respLen := len(response)
	respPkt := make([]byte, 4+respLen)
	respPkt[0] = byte(respLen)
	respPkt[1] = byte(respLen >> 8)
	respPkt[2] = byte(respLen >> 16)
	respPkt[3] = 1
	copy(respPkt[4:], response)

	_, err = conn.Write(respPkt)
	if err != nil {
		t.Fatalf("Failed to write response: %v", err)
	}

	// Read OK packet
	okPacket := make([]byte, 32)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(okPacket)
	if err != nil {
		t.Fatalf("Failed to read OK packet: %v", err)
	}
	if n < 4 {
		t.Fatal("OK packet too short")
	}

	// Send ping command
	ping := []byte{0x01, 0x00, 0x00, 0x00, MySQLComPing}
	_, err = conn.Write(ping)
	if err != nil {
		t.Fatalf("Failed to write ping: %v", err)
	}

	// Read ping response (OK packet)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = conn.Read(okPacket)
	if err != nil {
		t.Fatalf("Failed to read ping response: %v", err)
	}

	// Send query command
	query := "SELECT 1"
	queryPkt := make([]byte, 0, 5+len(query))
	queryPkt = append(queryPkt, byte(1+len(query)), 0, 0, 0, MySQLComQuery)
	queryPkt = append(queryPkt, []byte(query)...)
	_, err = conn.Write(queryPkt)
	if err != nil {
		t.Fatalf("Failed to write query: %v", err)
	}

	// Read query response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	queryResp := make([]byte, 256)
	n, err = conn.Read(queryResp)
	if err != nil && err != io.EOF {
		t.Logf("Query response error (may be expected): %v", err)
	}
	t.Logf("Query response bytes: %d", n)

	// Close server
	server.Close()
}

// TestServerAddress tests server address methods
func TestServerAddress(t *testing.T) {
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
	defer server.Close()

	// Verify we can get the address
	if server.listener == nil {
		t.Fatal("Listener is nil")
	}

	addr := server.listener.Addr()
	if addr == nil {
		t.Fatal("Address is nil")
	}

	t.Logf("Server listening on: %s", addr.String())
}

// TestConcurrentConnections tests multiple concurrent connections
func TestConcurrentConnections(t *testing.T) {
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
	defer server.Close()

	addr := server.listener.Addr().String()

	// Connect multiple times concurrently
	for i := 0; i < 3; i++ {
		go func(id int) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Logf("Connection %d failed: %v", id, err)
				return
			}
			defer conn.Close()

			// Read handshake
			handshake := make([]byte, 256)
			conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, err := conn.Read(handshake)
			if err != nil {
				t.Logf("Connection %d handshake error: %v", id, err)
				return
			}
			t.Logf("Connection %d read %d bytes", id, n)
		}(i)
	}

	// Wait for connections
	time.Sleep(500 * time.Millisecond)
}

// TestConnectionTimeout tests connection timeout scenarios
func TestConnectionTimeout(t *testing.T) {
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
	defer server.Close()

	addr := server.listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Set short timeout
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

	// Read handshake
	handshake := make([]byte, 256)
	_, err = conn.Read(handshake)
	if err != nil {
		t.Logf("Handshake read error (may be expected): %v", err)
	}
}

// TestPacketStructure tests MySQL packet structure
func TestPacketStructure(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	// Send OK packet
	err := client.sendOKPacket(42, 123)
	if err != nil {
		t.Fatalf("sendOKPacket failed: %v", err)
	}

	data := conn.writeBuf.Bytes()

	// Verify packet header
	if len(data) < 4 {
		t.Fatal("Packet too short")
	}

	length := int(data[0]) | int(data[1])<<8 | int(data[2])<<8
	sequence := data[3]

	if sequence != 1 {
		t.Errorf("Expected sequence 1, got %d", sequence)
	}

	payload := data[4:]
	if len(payload) != length {
		t.Errorf("Payload length mismatch: expected %d, got %d", length, len(payload))
	}

	// Verify OK packet structure
	if payload[0] != 0x00 {
		t.Errorf("Expected OK packet header 0x00, got 0x%02x", payload[0])
	}
}

// TestErrorPacketStructure tests error packet structure
func TestErrorPacketStructure(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.sendErrorPacket(1064, "Test error message")
	if err != nil {
		t.Fatalf("sendErrorPacket failed: %v", err)
	}

	data := conn.writeBuf.Bytes()
	if len(data) < 4 {
		t.Fatal("Packet too short")
	}

	payload := data[4:]

	// Verify error packet structure
	if payload[0] != 0xff {
		t.Errorf("Expected error packet header 0xff, got 0x%02x", payload[0])
	}

	// Check error code
	errorCode := uint16(payload[1]) | uint16(payload[2])<<8
	if errorCode != 1064 {
		t.Errorf("Expected error code 1064, got %d", errorCode)
	}

	// Check SQL state marker
	if payload[3] != '#' {
		t.Errorf("Expected SQL state marker '#', got '%c'", payload[3])
	}
}

// TestHandshakePacketStructure tests handshake packet structure
func TestHandshakePacketStructure(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0-Test")
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
		t.Fatal("Packet too short")
	}

	payload := data[4:]

	// Verify protocol version
	if payload[0] != 0x0a {
		t.Errorf("Expected protocol version 10, got %d", payload[0])
	}

	// Find server version (null-terminated string starting at position 1)
	i := 1
	for i < len(payload) && payload[i] != 0 {
		i++
	}
	version := string(payload[1:i])
	if version != "5.7.0-Test" {
		t.Errorf("Expected version '5.7.0-Test', got '%s'", version)
	}
}

// TestCommandTypes tests all MySQL command types
func TestCommandTypes(t *testing.T) {
	commands := []struct {
		name byte
		desc string
	}{
		{MySQLComQuit, "Quit"},
		{MySQLComInitDB, "InitDB"},
		{MySQLComQuery, "Query"},
		{MySQLComFieldList, "FieldList"},
		{MySQLComCreateDB, "CreateDB"},
		{MySQLComDropDB, "DropDB"},
		{MySQLComRefresh, "Refresh"},
		{MySQLComShutdown, "Shutdown"},
		{MySQLComStatistics, "Statistics"},
		{MySQLComProcessInfo, "ProcessInfo"},
		{MySQLComConnect, "Connect"},
		{MySQLComProcessKill, "ProcessKill"},
		{MySQLComDebug, "Debug"},
		{MySQLComPing, "Ping"},
		{MySQLComChangeUser, "ChangeUser"},
	}

	for _, cmd := range commands {
		t.Run(cmd.desc, func(t *testing.T) {
			// Just verify the constant is defined
			t.Logf("Command %s = 0x%02x", cmd.desc, cmd.name)
		})
	}
}

// TestFieldTypes tests all MySQL field types
func TestFieldTypes(t *testing.T) {
	types := []struct {
		name byte
		desc string
	}{
		{MySQLTypeDecimal, "Decimal"},
		{MySQLTypeTiny, "Tiny"},
		{MySQLTypeShort, "Short"},
		{MySQLTypeLong, "Long"},
		{MySQLTypeFloat, "Float"},
		{MySQLTypeDouble, "Double"},
		{MySQLTypeNull, "Null"},
		{MySQLTypeTimestamp, "Timestamp"},
		{MySQLTypeLongLong, "LongLong"},
		{MySQLTypeInt24, "Int24"},
		{MySQLTypeDate, "Date"},
		{MySQLTypeTime, "Time"},
		{MySQLTypeDateTime, "DateTime"},
		{MySQLTypeYear, "Year"},
		{MySQLTypeNewDate, "NewDate"},
		{MySQLTypeVarchar, "Varchar"},
		{MySQLTypeBit, "Bit"},
		{MySQLTypeJSON, "JSON"},
		{MySQLTypeNewDecimal, "NewDecimal"},
		{MySQLTypeEnum, "Enum"},
		{MySQLTypeSet, "Set"},
		{MySQLTypeTinyBlob, "TinyBlob"},
		{MySQLTypeMediumBlob, "MediumBlob"},
		{MySQLTypeLongBlob, "LongBlob"},
		{MySQLTypeBlob, "Blob"},
		{MySQLTypeVarString, "VarString"},
		{MySQLTypeString, "String"},
		{MySQLTypeGeometry, "Geometry"},
	}

	for _, ft := range types {
		t.Run(ft.desc, func(t *testing.T) {
			t.Logf("Field type %s = 0x%02x", ft.desc, ft.name)
		})
	}
}

// TestServerStatusFlags tests MySQL server status flags
func TestServerStatusFlags(t *testing.T) {
	flags := []struct {
		value uint16
		desc  string
	}{
		{MySQLServerStatusInTrans, "InTrans"},
		{MySQLServerStatusAutocommit, "Autocommit"},
		{MySQLServerStatusMoreResultsExists, "MoreResultsExists"},
		{MySQLServerStatusNoGoodIndexUsed, "NoGoodIndexUsed"},
		{MySQLServerStatusNoIndexUsed, "NoIndexUsed"},
		{MySQLServerStatusCursorExists, "CursorExists"},
		{MySQLServerStatusLastRowSent, "LastRowSent"},
		{MySQLServerStatusDBDropped, "DBDropped"},
		{MySQLServerStatusNoBackslashEscapes, "NoBackslashEscapes"},
		{MySQLServerStatusMetadataChanged, "MetadataChanged"},
	}

	for _, flag := range flags {
		t.Run(flag.desc, func(t *testing.T) {
			t.Logf("Status flag %s = 0x%04x", flag.desc, flag.value)
		})
	}
}
