package protocol

import (
	"bufio"
	"context"
	"crypto/sha1"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/auth"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// --- verifyMySQLNativeAuth tests ---

func TestVerifyMySQLNativeAuth_EmptyAuthEmptyHash(t *testing.T) {
	// Empty auth response + empty stored hash -> password is empty, should pass
	client := &MySQLClient{
		authResponse: []byte{},
		scramble:     make([]byte, 20),
	}
	if !client.verifyMySQLNativeAuth(nil) {
		t.Error("empty auth with nil hash should succeed (no password)")
	}
	if !client.verifyMySQLNativeAuth([]byte{}) {
		t.Error("empty auth with empty hash should succeed (no password)")
	}
}

func TestVerifyMySQLNativeAuth_EmptyAuthNonEmptyHash(t *testing.T) {
	// Empty auth but stored hash exists -> fail
	client := &MySQLClient{
		authResponse: []byte{},
		scramble:     make([]byte, 20),
	}
	storedHash := make([]byte, 20)
	storedHash[0] = 0x01
	if client.verifyMySQLNativeAuth(storedHash) {
		t.Error("empty auth with non-empty hash should fail")
	}
}

func TestVerifyMySQLNativeAuth_NonEmptyAuthEmptyHash(t *testing.T) {
	// Non-empty auth but no stored hash -> fail
	client := &MySQLClient{
		authResponse: make([]byte, 20),
		scramble:     make([]byte, 20),
	}
	if client.verifyMySQLNativeAuth(nil) {
		t.Error("non-empty auth with nil hash should fail")
	}
	if client.verifyMySQLNativeAuth([]byte{}) {
		t.Error("non-empty auth with empty hash should fail")
	}
}

func TestVerifyMySQLNativeAuth_WrongAuthLength(t *testing.T) {
	// Auth response not 20 bytes
	client := &MySQLClient{
		authResponse: make([]byte, 10),
		scramble:     make([]byte, 20),
	}
	storedHash := make([]byte, 20)
	if client.verifyMySQLNativeAuth(storedHash) {
		t.Error("auth response of wrong length should fail")
	}
}

func TestVerifyMySQLNativeAuth_WrongScrambleLength(t *testing.T) {
	// Scramble not 20 bytes
	client := &MySQLClient{
		authResponse: make([]byte, 20),
		scramble:     make([]byte, 10),
	}
	storedHash := make([]byte, 20)
	if client.verifyMySQLNativeAuth(storedHash) {
		t.Error("scramble of wrong length should fail")
	}
}

func TestVerifyMySQLNativeAuth_ValidAuth(t *testing.T) {
	// Simulate a valid auth exchange.
	// 1. Generate scramble
	scramble := []byte("12345678901234567890") // exactly 20 bytes

	// 2. Compute what the server stores: SHA1(SHA1(password))
	password := []byte("mysecret")
	// #nosec G401
	h1 := sha1.New()
	h1.Write(password)
	hash1 := h1.Sum(nil) // SHA1(password)

	// #nosec G401
	h2 := sha1.New()
	h2.Write(hash1)
	storedHash := h2.Sum(nil) // SHA1(SHA1(password))

	// 3. Compute what the client sends: SHA1(password) XOR SHA1(scramble + storedHash)
	// #nosec G401
	h3 := sha1.New()
	h3.Write(scramble)
	h3.Write(storedHash)
	scrambledHash := h3.Sum(nil)

	authResponse := make([]byte, 20)
	for i := range scrambledHash {
		authResponse[i] = hash1[i] ^ scrambledHash[i]
	}

	client := &MySQLClient{
		authResponse: authResponse,
		scramble:     scramble,
	}
	if !client.verifyMySQLNativeAuth(storedHash) {
		t.Error("valid auth should succeed")
	}
}

func TestVerifyMySQLNativeAuth_InvalidAuth(t *testing.T) {
	scramble := []byte("12345678901234567890")

	password := []byte("mysecret")
	// #nosec G401
	h1 := sha1.New()
	h1.Write(password)
	hash1 := h1.Sum(nil)

	// #nosec G401
	h2 := sha1.New()
	h2.Write(hash1)
	storedHash := h2.Sum(nil)

	// Build a deliberately wrong auth response
	authResponse := make([]byte, 20)
	for i := range authResponse {
		authResponse[i] = 0xff
	}

	client := &MySQLClient{
		authResponse: authResponse,
		scramble:     scramble,
	}
	if client.verifyMySQLNativeAuth(storedHash) {
		t.Error("invalid auth should fail")
	}
}

// --- handleSelectVariable tests ---

func TestHandleSelectVariable_Version(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0-CobaltDB")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleSelectVariable("SELECT @@VERSION")
	if err != nil {
		t.Fatalf("handleSelectVariable(@@VERSION) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written for @@VERSION")
	}
}

func TestHandleSelectVariable_VersionComment(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleSelectVariable("SELECT @@VERSION_COMMENT")
	if err != nil {
		t.Fatalf("handleSelectVariable(@@VERSION_COMMENT) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written for @@VERSION_COMMENT")
	}
}

func TestHandleSelectVariable_MaxAllowedPacket(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleSelectVariable("SELECT @@MAX_ALLOWED_PACKET")
	if err != nil {
		t.Fatalf("handleSelectVariable(@@MAX_ALLOWED_PACKET) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written")
	}
}

func TestHandleSelectVariable_CharacterSet(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleSelectVariable("SELECT @@CHARACTER_SET_CLIENT")
	if err != nil {
		t.Fatalf("handleSelectVariable(@@CHARACTER_SET) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written")
	}
}

func TestHandleSelectVariable_Collation(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleSelectVariable("SELECT @@COLLATION_CONNECTION")
	if err != nil {
		t.Fatalf("handleSelectVariable(@@COLLATION) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written")
	}
}

func TestHandleSelectVariable_AutoIncrement(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleSelectVariable("SELECT @@SESSION.AUTO_INCREMENT_INCREMENT")
	if err != nil {
		t.Fatalf("handleSelectVariable(@@SESSION.AUTO_INCREMENT_INCREMENT) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written")
	}
}

func TestHandleSelectVariable_Autocommit(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleSelectVariable("SELECT @@AUTOCOMMIT")
	if err != nil {
		t.Fatalf("handleSelectVariable(@@AUTOCOMMIT) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written")
	}
}

func TestHandleSelectVariable_Unknown(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	err := client.handleSelectVariable("SELECT @@SOME_UNKNOWN_VAR")
	if err != nil {
		t.Fatalf("handleSelectVariable(unknown) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written for unknown variable")
	}
}

func TestHandleSelectVariable_LowerCase(t *testing.T) {
	conn := newMockConn()
	server := NewMySQLServer(&engine.DB{}, "5.7.0-Test")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	// The function uppercases the SQL before matching, so lowercase should work
	err := client.handleSelectVariable("select @@version")
	if err != nil {
		t.Fatalf("handleSelectVariable(lowercase @@version) failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected data written")
	}
}

func TestHandleSelectVariable_ClosedConn(t *testing.T) {
	conn := newMockConn()
	conn.Close()
	server := NewMySQLServer(&engine.DB{}, "5.7.0")
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
	}

	// Should return an error when writing to a closed connection
	err := client.handleSelectVariable("SELECT @@VERSION")
	if err == nil {
		t.Error("expected error for closed connection")
	}
}

// --- readHandshakeResponse edge cases ---

func TestReadHandshakeResponse_WithAuthAndDatabase(t *testing.T) {
	conn := newMockConn()

	// Build a complete handshake response with username, auth, and database
	payload := make([]byte, 0, 128)
	// capability flags (4 bytes)
	payload = append(payload, 0x85, 0xa2, 0x1a, 0x00)
	// max packet size (4 bytes)
	payload = append(payload, 0x00, 0x00, 0x00, 0x01)
	// charset (1 byte)
	payload = append(payload, 0x21)
	// reserved (23 bytes)
	payload = append(payload, make([]byte, 23)...)
	// username (null-terminated)
	payload = append(payload, []byte("admin")...)
	payload = append(payload, 0x00)
	// auth response length + data (20 bytes of auth)
	payload = append(payload, 20) // length prefix
	authData := make([]byte, 20)
	for i := range authData {
		authData[i] = byte(i + 1)
	}
	payload = append(payload, authData...)
	// database (null-terminated)
	payload = append(payload, []byte("mydb")...)
	payload = append(payload, 0x00)

	// Write packet header + payload
	header := make([]byte, 4)
	header[0] = byte(len(payload))
	header[1] = byte(len(payload) >> 8)
	header[2] = byte(len(payload) >> 16)
	header[3] = 1
	conn.readBuf.Write(header)
	conn.readBuf.Write(payload)

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.readHandshakeResponse()
	if err != nil {
		t.Fatalf("readHandshakeResponse failed: %v", err)
	}
	if client.username != "admin" {
		t.Errorf("expected username 'admin', got %q", client.username)
	}
	if len(client.authResponse) != 20 {
		t.Errorf("expected 20-byte auth response, got %d", len(client.authResponse))
	}
	if client.database != "mydb" {
		t.Errorf("expected database 'mydb', got %q", client.database)
	}
}

func TestReadHandshakeResponse_TooShortPayload(t *testing.T) {
	conn := newMockConn()

	// Payload shorter than 32 bytes - should return nil (accepted anyway)
	payload := make([]byte, 10)

	header := make([]byte, 4)
	header[0] = byte(len(payload))
	header[1] = 0
	header[2] = 0
	header[3] = 1
	conn.readBuf.Write(header)
	conn.readBuf.Write(payload)

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.readHandshakeResponse()
	if err != nil {
		t.Errorf("expected nil error for short payload, got %v", err)
	}
}

func TestReadHandshakeResponse_InvalidPayloadLength(t *testing.T) {
	conn := newMockConn()

	// Length = 0 should trigger the invalid length error
	header := []byte{0x00, 0x00, 0x00, 1} // length = 0
	conn.readBuf.Write(header)

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.readHandshakeResponse()
	if err == nil {
		t.Error("expected error for zero length payload")
	}
}

func TestReadHandshakeResponse_TruncatedPayload(t *testing.T) {
	conn := newMockConn()

	// Header says 100 bytes, but only 10 available
	header := []byte{100, 0, 0, 1}
	conn.readBuf.Write(header)
	conn.readBuf.Write(make([]byte, 10))

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.readHandshakeResponse()
	if err == nil {
		t.Error("expected error for truncated payload")
	}
}

func TestReadHandshakeResponse_AuthResponseTruncated(t *testing.T) {
	conn := newMockConn()

	// Build payload where auth length claims more bytes than available
	payload := make([]byte, 0, 64)
	// cap + max_packet + charset + reserved = 32 bytes
	payload = append(payload, make([]byte, 32)...)
	// username (null-terminated)
	payload = append(payload, []byte("user")...)
	payload = append(payload, 0x00)
	// auth length says 20, but only 5 bytes follow
	payload = append(payload, 20) // length prefix claims 20
	payload = append(payload, make([]byte, 5)...)

	header := make([]byte, 4)
	header[0] = byte(len(payload))
	header[1] = byte(len(payload) >> 8)
	header[2] = byte(len(payload) >> 16)
	header[3] = 1
	conn.readBuf.Write(header)
	conn.readBuf.Write(payload)

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.readHandshakeResponse()
	if err != nil {
		t.Fatalf("readHandshakeResponse failed: %v", err)
	}
	// Auth response should be empty since data was truncated
	if len(client.authResponse) != 0 {
		t.Errorf("expected empty auth response due to truncation, got %d bytes", len(client.authResponse))
	}
}

func TestReadHandshakeResponse_NoDatabase(t *testing.T) {
	conn := newMockConn()

	// Build payload with username and auth but no database
	payload := make([]byte, 0, 64)
	payload = append(payload, make([]byte, 32)...)
	payload = append(payload, []byte("testuser")...)
	payload = append(payload, 0x00)
	payload = append(payload, 0) // auth length = 0

	header := make([]byte, 4)
	header[0] = byte(len(payload))
	header[1] = byte(len(payload) >> 8)
	header[2] = byte(len(payload) >> 16)
	header[3] = 1
	conn.readBuf.Write(header)
	conn.readBuf.Write(payload)

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.readHandshakeResponse()
	if err != nil {
		t.Fatalf("readHandshakeResponse failed: %v", err)
	}
	if client.username != "testuser" {
		t.Errorf("expected username 'testuser', got %q", client.username)
	}
	if client.database != "" {
		t.Errorf("expected empty database, got %q", client.database)
	}
}

// --- handleConnection edge cases ---

func TestHandleConnection_WithAuth_AccessDenied(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Skip("Cannot open database:", err)
	}
	defer db.Close()

	a := auth.NewAuthenticator()
	a.Enable()

	server := NewMySQLServer(db, "test")
	server.SetAuthenticator(a)

	conn := newMockConn()

	// Build a handshake response with a user that doesn't exist
	response := make([]byte, 0, 128)
	response = append(response, make([]byte, 32)...)
	response = append(response, []byte("nonexistent_user")...)
	response = append(response, 0x00)
	response = append(response, 0) // empty auth

	pktLen := len(response)
	pkt := make([]byte, 4+pktLen)
	pkt[0] = byte(pktLen)
	pkt[1] = byte(pktLen >> 8)
	pkt[2] = byte(pktLen >> 16)
	pkt[3] = 1
	copy(pkt[4:], response)

	conn.setReadData(pkt)

	// This should send handshake, read response, fail auth, send error
	server.handleConnection(conn)

	if conn.writeBuf.Len() == 0 {
		t.Error("expected handshake + error packet to be written")
	}
}

func TestHandleConnection_WithAuth_FailedVerification(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Skip("Cannot open database:", err)
	}
	defer db.Close()

	a := auth.NewAuthenticator()
	a.Enable()
	// Create user with a password
	_ = a.CreateUser("testuser", "correctpass", false)

	server := NewMySQLServer(db, "test")
	server.SetAuthenticator(a)

	conn := newMockConn()

	// Build a handshake response with testuser but wrong auth data
	response := make([]byte, 0, 128)
	response = append(response, make([]byte, 32)...)
	response = append(response, []byte("testuser")...)
	response = append(response, 0x00)
	// Provide a 20-byte wrong auth response
	response = append(response, 20) // length prefix
	wrongAuth := make([]byte, 20)
	for i := range wrongAuth {
		wrongAuth[i] = 0xAB
	}
	response = append(response, wrongAuth...)

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
		t.Error("expected handshake + error packet to be written")
	}
}

func TestHandleConnection_WithQuitCommand(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Skip("Cannot open database:", err)
	}
	defer db.Close()

	server := NewMySQLServer(db, "test")
	conn := newMockConn()

	// Build handshake response
	response := make([]byte, 0, 128)
	response = append(response, make([]byte, 32)...)
	response = append(response, []byte("user")...)
	response = append(response, 0x00)
	response = append(response, 0) // empty auth

	pktLen := len(response)
	pkt := make([]byte, 4+pktLen)
	pkt[0] = byte(pktLen)
	pkt[1] = byte(pktLen >> 8)
	pkt[2] = byte(pktLen >> 16)
	pkt[3] = 1
	copy(pkt[4:], response)
	conn.setReadData(pkt)

	// Add a Quit command after the handshake
	quitPkt := []byte{0x01, 0x00, 0x00, 0x00, MySQLComQuit}
	conn.setReadData(quitPkt)

	server.handleConnection(conn)

	if conn.writeBuf.Len() == 0 {
		t.Error("expected handshake + OK to be written")
	}
}

func TestHandleConnection_PanicRecovery(t *testing.T) {
	// Test that a panicking connection handler doesn't crash
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Skip("Cannot open database:", err)
	}
	defer db.Close()

	server := NewMySQLServer(db, "test")
	conn := newMockConn()

	// Give it no data at all - will fail at handshake read
	server.handleConnection(conn)
}

// --- handleQuery with @@variable routing ---

func TestHandleQuery_SelectVariable(t *testing.T) {
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
		ctx:    context.Background(),
	}

	// This should be routed to handleSelectVariable
	err = client.handleQuery("SELECT @@version_comment")
	if err != nil {
		t.Fatalf("handleQuery for @@variable failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected result to be written")
	}
}

func TestHandleQuery_SelectAtVariable(t *testing.T) {
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
		ctx:    context.Background(),
	}

	// SELECT @variable (single @) also routes to handleSelectVariable
	err = client.handleQuery("SELECT @myvar")
	if err != nil {
		t.Fatalf("handleQuery for @variable failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected result to be written")
	}
}

func TestHandleQuery_NilContext(t *testing.T) {
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
		ctx:    nil, // nil context to exercise the fallback path
	}

	// Should use context.Background() fallback
	_ = client.handleQuery("SELECT 1")
}

// --- handleCommand edge cases ---

func TestHandleCommand_InvalidPayloadLength(t *testing.T) {
	conn := newMockConn()
	// Write a header that claims a very large payload (>16MB)
	header := []byte{0x01, 0x00, 0x01, 0x00} // length = 65537 (valid, under 16MB)
	// But don't provide enough data -> will get read error
	conn.setReadData(header)

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.handleCommand()
	if err == nil {
		t.Error("expected error for truncated payload")
	}
}

// --- buildRowPacket ---

func TestBuildRowPacket_WithNilValues(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	pkt := client.buildRowPacket([]interface{}{"hello", nil, 42, nil, "world"})
	if len(pkt) == 0 {
		t.Error("expected non-empty row packet")
	}
}

func TestBuildRowPacket_Empty(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	pkt := client.buildRowPacket([]interface{}{})
	if len(pkt) != 0 {
		t.Error("expected empty packet for empty row")
	}
}

// --- sendEOFPacket ---

func TestSendEOFPacket(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: NewMySQLServer(&engine.DB{}, "5.7.0"),
	}

	err := client.sendEOFPacket(5)
	if err != nil {
		t.Fatalf("sendEOFPacket failed: %v", err)
	}
	data := conn.writeBuf.Bytes()
	if len(data) < 4 {
		t.Fatal("EOF packet too short")
	}
	// Check sequence number
	if data[3] != 5 {
		t.Errorf("expected sequence 5, got %d", data[3])
	}
	// Check EOF marker
	if data[4] != 0xfe {
		t.Errorf("expected EOF marker 0xfe, got 0x%02x", data[4])
	}
}

// --- Full end-to-end with @@variable queries via command ---

func TestHandleCommandQuery_WithSelectVariable(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Skip("Cannot open database:", err)
	}
	defer db.Close()

	conn := newMockConn()
	server := NewMySQLServer(db, "5.7.0-CobaltDB")

	query := "SELECT @@VERSION"
	payload := make([]byte, 0, 1+len(query))
	payload = append(payload, MySQLComQuery)
	payload = append(payload, []byte(query)...)

	header := make([]byte, 4)
	header[0] = byte(len(payload))
	header[1] = byte(len(payload) >> 8)
	header[2] = byte(len(payload) >> 16)
	header[3] = 0
	conn.setReadData(append(header, payload...))

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: server,
		ctx:    context.Background(),
	}

	err = client.handleCommand()
	if err != nil {
		t.Fatalf("handleCommand with SELECT @@VERSION failed: %v", err)
	}
	if conn.writeBuf.Len() == 0 {
		t.Error("expected result to be written")
	}
}

// --- Addr with listener ---

func TestAddr_WithListener(t *testing.T) {
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

	addr := server.Addr()
	if addr == nil {
		t.Error("expected non-nil addr when listening")
	}
}

// --- Full lifecycle with SELECT @@variable ---

func TestFullLifecycleWithSelectVariable(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Skip("Cannot open database:", err)
	}
	defer db.Close()

	server := NewMySQLServer(db, "5.7.0-CobaltDB")
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
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	// Read handshake
	handshakeHeader := make([]byte, 4)
	_, err = io.ReadFull(conn, handshakeHeader)
	if err != nil {
		t.Fatalf("Failed to read handshake header: %v", err)
	}
	hsLen := int(handshakeHeader[0]) | int(handshakeHeader[1])<<8 | int(handshakeHeader[2])<<16
	handshakePayload := make([]byte, hsLen)
	_, err = io.ReadFull(conn, handshakePayload)
	if err != nil {
		t.Fatalf("Failed to read handshake: %v", err)
	}

	// Send handshake response
	response := make([]byte, 0, 128)
	response = append(response, make([]byte, 32)...)
	response = append(response, []byte("testuser")...)
	response = append(response, 0x00)
	response = append(response, 0) // empty auth

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

	// Read OK
	okBuf := make([]byte, 32)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(okBuf)
	if err != nil {
		t.Fatalf("Failed to read OK: %v", err)
	}

	// Send SELECT @@version query command
	query := "SELECT @@version_comment"
	queryPayload := make([]byte, 0, 5+len(query))
	queryPayload = append(queryPayload, byte(1+len(query)), 0, 0, 0, MySQLComQuery)
	queryPayload = append(queryPayload, []byte(query)...)
	_, err = conn.Write(queryPayload)
	if err != nil {
		t.Fatalf("Failed to write query: %v", err)
	}

	// Read response
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resultBuf := make([]byte, 512)
	n, err := conn.Read(resultBuf)
	if err != nil && err != io.EOF {
		t.Logf("Query response error (may be expected): %v", err)
	}
	if n == 0 {
		t.Error("expected query response data")
	}
}
