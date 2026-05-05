package protocol

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- MySQL native password protocol requires SHA-1 compatibility.
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/auth"
	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

const (
	// maxMySQLPayloadSize is the maximum allowed MySQL packet payload size (16 MB)
	maxMySQLPayloadSize = 16 * 1024 * 1024
)

// MySQL protocol constants
const (
	// Command types
	MySQLComQuit        byte = 0x01
	MySQLComInitDB      byte = 0x02
	MySQLComQuery       byte = 0x03
	MySQLComFieldList   byte = 0x04
	MySQLComCreateDB    byte = 0x05
	MySQLComDropDB      byte = 0x06
	MySQLComRefresh     byte = 0x07
	MySQLComShutdown    byte = 0x08
	MySQLComStatistics  byte = 0x09
	MySQLComProcessInfo byte = 0x0a
	MySQLComConnect     byte = 0x0b
	MySQLComProcessKill byte = 0x0c
	MySQLComDebug       byte = 0x0d
	MySQLComPing        byte = 0x0e
	MySQLComChangeUser  byte = 0x11
		MySQLComStmtPrepare byte = 0x16
		MySQLComStmtExecute byte = 0x17
		MySQLComStmtSendLongData byte = 0x18
		MySQLComStmtClose   byte = 0x19
		MySQLComStmtReset   byte = 0x1a
	MySQLComResetConnection byte = 0x1f

	// Server status flags
	MySQLServerStatusInTrans            uint16 = 0x0001
	MySQLServerStatusAutocommit         uint16 = 0x0002
	MySQLServerStatusMoreResultsExists  uint16 = 0x0008
	MySQLServerStatusNoGoodIndexUsed    uint16 = 0x0010
	MySQLServerStatusNoIndexUsed        uint16 = 0x0020
	MySQLServerStatusCursorExists       uint16 = 0x0040
	MySQLServerStatusLastRowSent        uint16 = 0x0080
	MySQLServerStatusDBDropped          uint16 = 0x0100
	MySQLServerStatusNoBackslashEscapes uint16 = 0x0200
	MySQLServerStatusMetadataChanged    uint16 = 0x0400

	// Field types
	MySQLTypeDecimal    byte = 0x00
	MySQLTypeTiny       byte = 0x01
	MySQLTypeShort      byte = 0x02
	MySQLTypeLong       byte = 0x03
	MySQLTypeFloat      byte = 0x04
	MySQLTypeDouble     byte = 0x05
	MySQLTypeNull       byte = 0x06
	MySQLTypeTimestamp  byte = 0x07
	MySQLTypeLongLong   byte = 0x08
	MySQLTypeInt24      byte = 0x09
	MySQLTypeDate       byte = 0x0a
	MySQLTypeTime       byte = 0x0b
	MySQLTypeDateTime   byte = 0x0c
	MySQLTypeYear       byte = 0x0d
	MySQLTypeNewDate    byte = 0x0e
	MySQLTypeVarchar    byte = 0x0f
	MySQLTypeBit        byte = 0x10
	MySQLTypeJSON       byte = 0xf5
	MySQLTypeNewDecimal byte = 0xf6
	MySQLTypeEnum       byte = 0xf7
	MySQLTypeSet        byte = 0xf8
	MySQLTypeTinyBlob   byte = 0xf9
	MySQLTypeMediumBlob byte = 0xfa
	MySQLTypeLongBlob   byte = 0xfb
	MySQLTypeBlob       byte = 0xfc
	MySQLTypeVarString  byte = 0xfd
	MySQLTypeString     byte = 0xfe
	MySQLTypeGeometry   byte = 0xff
)

// MySQLServer implements a MySQL-compatible server
type MySQLServer struct {
	db       *engine.DB
	listener net.Listener
	version  string
	mu       sync.Mutex
	clients  map[uint32]net.Conn
	nextID   uint32
	auth     *auth.Authenticator
	wg       sync.WaitGroup
	stopChan chan struct{}
	closed   bool
}

// NewMySQLServer creates a new MySQL-compatible server
func NewMySQLServer(db *engine.DB, version string) *MySQLServer {
	if version == "" {
		version = "5.7.0-CobaltDB"
	}
	return &MySQLServer{
		db:       db,
		version:  version,
		clients:  make(map[uint32]net.Conn),
		stopChan: make(chan struct{}),
	}
}

// SetAuthenticator sets the authenticator for the MySQL server.
// When set and enabled, connections must provide valid credentials.
// If not set or not enabled, all connections are accepted (backward compatible).
func (s *MySQLServer) SetAuthenticator(a *auth.Authenticator) {
	s.auth = a
}

// Listen starts listening for MySQL connections
func (s *MySQLServer) Listen(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	s.listener = listener

	go s.acceptLoop()
	return nil
}

// Addr returns the listener address (useful for tests)
func (s *MySQLServer) Addr() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Close stops the MySQL server and all client connections
func (s *MySQLServer) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	// Signal accept loop to stop
	close(s.stopChan)

	// Close all active client connections
	for id, conn := range s.clients {
		_ = conn.Close()
		delete(s.clients, id)
	}
	s.mu.Unlock()

	// Wait for all client handlers to finish
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// acceptLoop accepts incoming connections.
//
// Register the loop itself in s.wg *before* accepting any connections. This
// gives Close()'s s.wg.Wait() a non-zero counter to synchronize against, so
// later s.wg.Add(1) calls for per-connection handlers can't race with Wait
// (the classic "positive-delta-while-zero-concurrent-with-Wait" WaitGroup
// race). If we're already closed, return without touching wg.
func (s *MySQLServer) acceptLoop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	listener := s.listener
	stopChan := s.stopChan
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	if listener == nil {
		return
	}
	for {
		select {
		case <-stopChan:
			return
		default:
		}
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-stopChan:
				return
			default:
				return
			}
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConnection(c)
		}(conn)
	}
}

// handleConnection handles a MySQL client connection
func (s *MySQLServer) handleConnection(conn net.Conn) {
	// Register connection
	s.mu.Lock()
	s.nextID++
	connID := s.nextID
	s.clients[connID] = conn
	s.mu.Unlock()

	var client *MySQLClient

	defer func() {
		if r := recover(); r != nil {
			// Prevent a panicking client from crashing the server
			_ = r
		}
		if client != nil && client.cancel != nil {
			client.cancel()
		}
		_ = conn.Close()
		s.mu.Lock()
		delete(s.clients, connID)
		s.mu.Unlock()
	}()

	client = &MySQLClient{
		conn:        conn,
		reader:      bufio.NewReader(conn),
		server:      s,
		connID:      connID,
		connectTime: time.Now(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())

	// Send handshake
	if err := client.sendHandshake(); err != nil {
		return
	}

	// Read handshake response
	if err := client.readHandshakeResponse(); err != nil {
		return
	}

	// Authenticate if an authenticator is configured and enabled (FIX-004)
	if s.auth != nil && s.auth.IsEnabled() {
		storedHash, err := s.auth.GetMySQLNativeHash(client.username)
		if err != nil {
			if sendErr := client.sendErrorPacket(1045, fmt.Sprintf("Access denied for user '%s'", client.username)); sendErr != nil {
				_ = sendErr
			}
			return
		}
		if !client.verifyMySQLNativeAuth(storedHash) {
			if sendErr := client.sendErrorPacket(1045, fmt.Sprintf("Access denied for user '%s'", client.username)); sendErr != nil {
				_ = sendErr
			}
			return
		}
	}

	// Send OK packet
	if err := client.sendOKPacket(0, 0); err != nil {
		return
	}

	// Handle commands
	for {
		if err := client.handleCommand(); err != nil {
			return
		}
	}
}

// MySQLClient represents a MySQL client connection
type MySQLClient struct {
	conn         net.Conn
	reader       *bufio.Reader
	server       *MySQLServer
	connID       uint32
	ctx          context.Context
	cancel       context.CancelFunc
	username     string
	database     string
	authResponse []byte // raw auth response from client handshake
	scramble     []byte // 20-byte random challenge sent in handshake (FIX-004)
	sequence     byte   // packet sequence number for proper protocol flow
	connectTime  time.Time
	stmts        map[uint32]*preparedStmt
	nextStmtID   uint32
}

// sendHandshake sends the initial handshake packet
func (c *MySQLClient) sendHandshake() error {
	// Protocol version (1 byte)
	// Server version (null-terminated string)
	// Connection ID (4 bytes)
	// Auth plugin data part 1 (8 bytes)
	// Filler (1 byte)
	// Capability flags lower 2 bytes (2 bytes)
	// Character set (1 byte)
	// Status flags (2 bytes)
	// Capability flags upper 2 bytes (2 bytes)
	// Auth plugin data length (1 byte)
	// Reserved (10 bytes)
	// Auth plugin data part 2 (minimum 12 bytes)
	// Auth plugin name (null-terminated string)

	// Generate 20-byte random scramble for challenge-response auth (FIX-004)
	c.scramble = make([]byte, 20)
	if _, err := rand.Read(c.scramble); err != nil {
		return fmt.Errorf("failed to generate auth scramble: %w", err)
	}

	pkt := make([]byte, 0, 128)

	// Protocol version 10
	pkt = append(pkt, 0x0a)

	// Server version
	pkt = append(pkt, []byte(c.server.version)...)
	pkt = append(pkt, 0x00)

	// Connection ID
	connIDBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(connIDBuf, c.connID)
	pkt = append(pkt, connIDBuf...)

	// Auth plugin data part 1 (first 8 bytes of scramble)
	pkt = append(pkt, c.scramble[:8]...)

	// Filler
	pkt = append(pkt, 0x00)

	// Capability flags lower
	pkt = append(pkt, 0xff, 0xf7)

	// Character set (utf8mb4)
	pkt = append(pkt, 0x21)

	// Status flags
	pkt = append(pkt, 0x02, 0x00)

	// Capability flags upper
	pkt = append(pkt, 0xff, 0x81)

	// Auth plugin data length
	pkt = append(pkt, 0x15)

	// Reserved
	pkt = append(pkt, make([]byte, 10)...)

	// Auth plugin data part 2 (remaining 12 bytes of scramble + null terminator)
	pkt = append(pkt, c.scramble[8:]...)
	pkt = append(pkt, 0x00) // null terminator for auth data part 2

	// Auth plugin name
	pkt = append(pkt, []byte("mysql_native_password")...)
	pkt = append(pkt, 0x00)

	c.sequence = 0
	return c.writePacket(pkt, 0)
}

// readHandshakeResponse reads the client's handshake response
func (c *MySQLClient) readHandshakeResponse() error {
	// Read packet header
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return err
	}

	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	c.sequence = header[3] // track sequence from client

	// Validate payload size to prevent DoS via unbounded allocation
	if length <= 0 || length > maxMySQLPayloadSize {
		return fmt.Errorf("invalid handshake payload length: %d", length)
	}

	// Read packet payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return err
	}

	// Parse handshake response
	// Format: [capFlags:4][maxPacketSize:4][charset:1][reserved:23][username:NUL][authResp...][database:NUL]
	if len(payload) < 32 {
		return nil // Too short, accept anyway
	}

	offset := 4 + 4 + 1 + 23 // skip capFlags(4) + maxPacketSize(4) + charset(1) + reserved(23) = 32

	// Read username (null-terminated)
	if offset < len(payload) {
		end := offset
		for end < len(payload) && payload[end] != 0 {
			end++
		}
		c.username = string(payload[offset:end])
		offset = end + 1 // skip null terminator
	}

	// Read auth response (length-prefixed)
	if offset < len(payload) {
		authLen := int(payload[offset])
		offset++
		if offset+authLen <= len(payload) {
			c.authResponse = make([]byte, authLen)
			copy(c.authResponse, payload[offset:offset+authLen])
		}
		offset += authLen
	}

	// Read database if present
	if offset < len(payload) {
		end := offset
		for end < len(payload) && payload[end] != 0 {
			end++
		}
		if end > offset {
			c.database = string(payload[offset:end])
		}
	}

	return nil
}

// verifyMySQLNativeAuth verifies the client's mysql_native_password auth response (FIX-004).
// storedHash is SHA1(SHA1(password)) from the auth system.
// The client sends: SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
func (c *MySQLClient) verifyMySQLNativeAuth(storedHash []byte) bool {
	if len(c.authResponse) == 0 {
		// Empty auth response — only valid if user has empty password (no hash stored)
		return len(storedHash) == 0
	}
	if len(storedHash) == 0 || len(c.authResponse) != 20 || len(c.scramble) != 20 {
		return false
	}

	// Compute SHA1(scramble + storedHash)
	// #nosec G401 -- MySQL native password protocol requires SHA-1 compatibility.
	h := sha1.New()
	h.Write(c.scramble)
	h.Write(storedHash)
	scrambledHash := h.Sum(nil)

	// XOR with client response to recover candidate SHA1(password)
	candidate := make([]byte, 20)
	for i := range scrambledHash {
		candidate[i] = c.authResponse[i] ^ scrambledHash[i]
	}

	// SHA1(candidate) should equal storedHash
	// #nosec G401 -- MySQL native password protocol requires SHA-1 compatibility.
	check := sha1.Sum(candidate)
	return subtle.ConstantTimeCompare(check[:], storedHash) == 1
}

// handleCommand handles a MySQL command
func (c *MySQLClient) handleCommand() error {
	// Read packet header
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return err
	}

	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	c.sequence = header[3] // track command sequence

	// Validate payload size to prevent DoS via unbounded allocation
	if length <= 0 || length > maxMySQLPayloadSize {
		return fmt.Errorf("invalid command payload length: %d", length)
	}

	// Read packet payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return err
	}

	if length == 0 {
		return fmt.Errorf("empty packet")
	}

	command := payload[0]
	data := payload[1:]

	switch command {
	case MySQLComQuit:
		return io.EOF

	case MySQLComQuery:
		return c.handleQuery(string(data))

	case MySQLComPing:
		return c.sendOKPacket(0, 0)

	case MySQLComInitDB:
		c.database = string(data)
		return c.sendOKPacket(0, 0)

	case MySQLComStmtPrepare:
		return c.handleStmtPrepare(string(data))

	case MySQLComStmtExecute:
		return c.handleStmtExecute(data)

	case MySQLComStmtClose:
		return c.handleStmtClose(data)

	case MySQLComStmtReset:
		return c.handleStmtReset(data)

	case MySQLComStatistics:
		return c.handleStatistics()

	case MySQLComFieldList:
		return c.handleFieldList(data)

	case MySQLComProcessInfo:
		return c.handleProcessInfo()

	case MySQLComResetConnection:
		return c.handleResetConnection()

	case MySQLComRefresh:
		return c.sendOKPacket(0, 0)

	case MySQLComShutdown:
		return c.sendErrorPacket(1047, "SHUTDOWN not supported")

	default:
		return c.sendErrorPacket(1047, "Unsupported command")
	}
}

// handleQuery handles a SQL query
func (c *MySQLClient) handleQuery(sql string) error {
	sql = strings.TrimSpace(sql)

	// Handle MySQL client initialization queries that may not parse
	if hasPrefixIgnoreCase(sql, "SELECT @@") || hasPrefixIgnoreCase(sql, "SELECT @") {
		// MySQL clients query session variables like @@version_comment, @@max_allowed_packet
		return c.handleSelectVariable(sql)
	}

	baseCtx := c.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 30*time.Second)
	defer cancel()

	// Try to execute as query first (SELECT, SHOW, DESCRIBE)
	rows, err := c.server.db.Query(ctx, sql)
	if err == nil {
		defer rows.Close()
		return c.sendResultSetFromRows(rows)
	}

	// Try to execute as exec (INSERT, UPDATE, DELETE, SET, USE, CREATE, etc.)
	result, err := c.server.db.Exec(ctx, sql)
	if err != nil {
		return c.sendErrorPacket(1, sanitizeMySQLError(err))
	}

	rowsAffected := uint64(0)
	if result.RowsAffected > 0 {
		rowsAffected = uint64(result.RowsAffected)
	}
	lastInsertID := uint64(0)
	if result.LastInsertID > 0 {
		lastInsertID = uint64(result.LastInsertID)
	}

	return c.sendOKPacket(rowsAffected, lastInsertID)
}

// handleSelectVariable handles SELECT @@variable queries from MySQL clients
func (c *MySQLClient) handleSelectVariable(sql string) error {
	// Return sensible defaults for common MySQL session variables
	seq := byte(1)

	colName := ""
	value := ""

	switch {
	case containsIgnoreCase(sql, "@@VERSION_COMMENT"):
		colName = "@@version_comment"
		value = "CobaltDB"
	case containsIgnoreCase(sql, "@@VERSION"):
		colName = "@@version"
		value = c.server.version
	case containsIgnoreCase(sql, "@@MAX_ALLOWED_PACKET"):
		colName = "@@max_allowed_packet"
		value = "67108864"
	case containsIgnoreCase(sql, "@@CHARACTER_SET"):
		colName = "@@character_set_client"
		value = "utf8mb4"
	case containsIgnoreCase(sql, "@@COLLATION"):
		colName = "@@collation_connection"
		value = "utf8mb4_general_ci"
	case containsIgnoreCase(sql, "@@SESSION.AUTO_INCREMENT_INCREMENT"):
		colName = "@@session.auto_increment_increment"
		value = "1"
	case containsIgnoreCase(sql, "@@AUTOCOMMIT"):
		colName = "@@autocommit"
		value = "1"
	default:
		colName = "@@unknown"
		value = ""
	}

	// Send single column, single row result
	countPkt := appendLenEncInt(nil, 1)
	if err := c.writePacket(countPkt, seq); err != nil {
		return err
	}
	seq++

	colPkt := c.buildColumnDefPacket(colName)
	if err := c.writePacket(colPkt, seq); err != nil {
		return err
	}
	seq++

	if err := c.sendEOFPacket(seq); err != nil {
		return err
	}
	seq++

	rowPkt := c.buildRowPacket([]interface{}{value})
	if err := c.writePacket(rowPkt, seq); err != nil {
		return err
	}
	seq++

	return c.sendEOFPacket(seq)
}

// sendResultSetFromRows sends a MySQL result set from engine.Rows
func (c *MySQLClient) sendResultSetFromRows(rows *engine.Rows) error {
	if rows == nil {
		return c.sendOKPacket(0, 0)
	}
	columns := rows.Columns()
	seq := byte(1)

	// 1. Send column count packet
	countPkt := appendLenEncInt(nil, uint64(len(columns)))
	if err := c.writePacket(countPkt, seq); err != nil {
		return err
	}
	seq++

	// 2. Send column definition packets
	for _, colName := range columns {
		pkt := c.buildColumnDefPacket(colName)
		if err := c.writePacket(pkt, seq); err != nil {
			return err
		}
		seq++
	}

	// 3. Send EOF packet (end of column definitions)
	if err := c.sendEOFPacket(seq); err != nil {
		return err
	}
	seq++

	// 4. Send row data packets
	var scanErrors int
	for rows.Next() {
		row := make([]interface{}, len(columns))
		dest := make([]interface{}, len(columns))
		for i := range dest {
			dest[i] = &row[i]
		}

		if err := rows.Scan(dest...); err != nil {
			scanErrors++
			continue
		}

		pkt := c.buildRowPacket(row)
		if err := c.writePacket(pkt, seq); err != nil {
			return err
		}
		seq++
	}
	_ = scanErrors

	// 5. Send EOF packet (end of rows)
	return c.sendEOFPacket(seq)
}

// buildColumnDefPacket builds a column definition packet
func (c *MySQLClient) buildColumnDefPacket(name string) []byte {
	var pkt []byte

	// catalog (lenenc_str) - "def"
	pkt = appendLenEncString(pkt, "def")
	// schema (lenenc_str) - empty
	pkt = appendLenEncString(pkt, "")
	// table (lenenc_str) - empty
	pkt = appendLenEncString(pkt, "")
	// org_table (lenenc_str) - empty
	pkt = appendLenEncString(pkt, "")
	// name (lenenc_str)
	pkt = appendLenEncString(pkt, name)
	// org_name (lenenc_str)
	pkt = appendLenEncString(pkt, name)

	// length of fixed-length fields [0c]
	pkt = append(pkt, 0x0c)

	// character set (utf8mb4 = 0x2d00)
	pkt = append(pkt, 0x21, 0x00)

	// column length (4 bytes)
	pkt = append(pkt, 0xff, 0xff, 0x00, 0x00)

	// column type (VARCHAR)
	pkt = append(pkt, MySQLTypeVarString)

	// flags (2 bytes)
	pkt = append(pkt, 0x00, 0x00)

	// decimals
	pkt = append(pkt, 0x00)

	// filler (2 bytes)
	pkt = append(pkt, 0x00, 0x00)

	return pkt
}

// buildRowPacket builds a row data packet (text protocol)
func (c *MySQLClient) buildRowPacket(row []interface{}) []byte {
	var pkt []byte
	for _, val := range row {
		if val == nil {
			pkt = append(pkt, 0xfb) // NULL
		} else {
			pkt = appendLenEncString(pkt, valueToWireString(val))
		}
	}
	return pkt
}

// valueToWireString converts a value to its string representation for the MySQL
// wire protocol. Uses strconv for common numeric types to avoid fmt.Sprintf
// reflection overhead.
func valueToWireString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "1"
		}
		return "0"
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return catalog.ValueToStringKey(val)
	}
}

// sendEOFPacket sends an EOF packet
func (c *MySQLClient) sendEOFPacket(seq byte) error {
	pkt := []byte{
		0xfe,       // EOF marker
		0x00, 0x00, // warnings
		0x02, 0x00, // status flags (SERVER_STATUS_AUTOCOMMIT)
	}
	return c.writePacket(pkt, seq)
}

// appendLenEncString appends a length-encoded string to dst.
func appendLenEncString(dst []byte, s string) []byte {
	dst = appendLenEncInt(dst, uint64(len(s)))
	return append(dst, s...)
}

//nolint:unused // used by coverage tests
// writeLenEncString returns a newly allocated length-encoded string.
// Prefer appendLenEncString for zero-allocation appending.
func writeLenEncString(s string) []byte {
	return appendLenEncString(nil, s)
}

// writePacket writes a MySQL protocol packet
func (c *MySQLClient) writePacket(data []byte, sequence byte) error {
	// Packet header: 3 bytes length + 1 byte sequence
	length := len(data)
	var header [4]byte
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = sequence

	if _, err := c.conn.Write(header[:]); err != nil {
		return err
	}

	if _, err := c.conn.Write(data); err != nil {
		return err
	}

	return nil
}

// sendOKPacket sends an OK packet
func (c *MySQLClient) sendOKPacket(affectedRows, lastInsertID uint64) error {
	pkt := c.buildOKPacket(affectedRows, lastInsertID)
	seq := c.sequence + 1
	c.sequence = seq
	return c.writePacket(pkt, seq)
}

func (c *MySQLClient) buildOKPacket(affectedRows, lastInsertID uint64) []byte {
	pkt := make([]byte, 0, 32)
	pkt = append(pkt, 0x00)
	pkt = appendLenEncInt(pkt, affectedRows)
	pkt = appendLenEncInt(pkt, lastInsertID)
	pkt = append(pkt, 0x02, 0x00) // status flags
	pkt = append(pkt, 0x00, 0x00) // warnings
	return pkt
}

// sendErrorPacket sends an error packet
func (c *MySQLClient) sendErrorPacket(code uint16, message string) error {
	pkt := make([]byte, 0, 128)

	// Header 0xff
	pkt = append(pkt, 0xff)

	// Error code
	pkt = append(pkt, byte(code), byte(code>>8))

	// SQL state marker
	pkt = append(pkt, '#')

	// SQL state (5 bytes)
	pkt = append(pkt, []byte("42000")...)

	// Error message
	pkt = append(pkt, []byte(message)...)

	return c.writePacket(pkt, 0)
}

// scramblePassword scrambles a password using MySQL's algorithm.
//
//nolint:unused // retained for protocol compatibility tests.
func scramblePassword(password, scramble []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// SHA1(password)
	// #nosec G401 -- MySQL native password protocol requires SHA-1 compatibility.
	h1 := sha1.New()
	h1.Write(password)
	hash1 := h1.Sum(nil)

	// SHA1(SHA1(password))
	// #nosec G401 -- MySQL native password protocol requires SHA-1 compatibility.
	h2 := sha1.New()
	h2.Write(hash1)
	hash2 := h2.Sum(nil)

	// SHA1(scramble + SHA1(SHA1(password)))
	// #nosec G401 -- MySQL native password protocol requires SHA-1 compatibility.
	h3 := sha1.New()
	h3.Write(scramble)
	h3.Write(hash2)
	hash3 := h3.Sum(nil)

	// XOR
	result := make([]byte, len(hash3))
	for i := range hash3 {
		result[i] = hash1[i] ^ hash3[i]
	}

	return result
}

// readLenEncInt reads a length-encoded integer.
//
//nolint:unused // retained for protocol compatibility tests.
func readLenEncInt(data []byte) (uint64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	switch data[0] {
	case 0xfb:
		return 0, 1 // NULL
	case 0xfc:
		if len(data) < 3 {
			return 0, 0
		}
		return uint64(data[1]) | uint64(data[2])<<8, 3
	case 0xfd:
		if len(data) < 4 {
			return 0, 0
		}
		return uint64(data[1]) | uint64(data[2])<<8 | uint64(data[3])<<16, 4
	case 0xfe:
		if len(data) < 9 {
			return 0, 0
		}
		return binary.LittleEndian.Uint64(data[1:9]), 9
	default:
		return uint64(data[0]), 1
	}
}

// appendLenEncInt appends a length-encoded integer to dst and returns the extended slice.
// Avoids allocation for all but the largest values (>= 2^24).
func appendLenEncInt(dst []byte, value uint64) []byte {
	switch {
	case value < 251:
		return append(dst, byte(value))
	case value < 1<<16:
		return append(dst, 0xfc, byte(value), byte(value>>8))
	case value < 1<<24:
		return append(dst, 0xfd, byte(value), byte(value>>8), byte(value>>16))
	default:
		buf := make([]byte, 9)
		buf[0] = 0xfe
		binary.LittleEndian.PutUint64(buf[1:], value)
		return append(dst, buf...)
	}
}

// sanitizeMySQLError strips internal details from errors before sending to MySQL clients.
func sanitizeMySQLError(err error) string {
	msg := err.Error()
	for _, prefix := range []string{"/", "C:\\", "D:\\"} {
		if idx := strings.Index(msg, prefix); idx >= 0 {
			msg = msg[:idx] + "(internal error)"
		}
	}
	return msg
}

// hasPrefixIgnoreCase reports whether s starts with prefix, case-insensitively.
func hasPrefixIgnoreCase(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// containsIgnoreCase reports whether s contains substr, case-insensitively.
func containsIgnoreCase(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if strings.EqualFold(s[i:i+len(substr)], substr) {
			return true
		}
	}
	return false
}
// preparedStmt holds server-side prepared statement state.
type preparedStmt struct {
	id        uint32
	sql       string
	numParams int
	numColumns int
}

func (c *MySQLClient) getStmtMap() map[uint32]*preparedStmt {
	if c.stmts == nil {
		c.stmts = make(map[uint32]*preparedStmt)
	}
	return c.stmts
}

func (c *MySQLClient) handleStmtPrepare(sql string) error {
	sql = strings.TrimSpace(sql)

	baseCtx := c.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 30*time.Second)
	defer cancel()

	var numColumns int
	rows, err := c.server.db.Query(ctx, sql)
	if err == nil {
		numColumns = len(rows.Columns())
		_ = rows.Close()
	}

	c.nextStmtID++
	stmtID := c.nextStmtID
	stmt := &preparedStmt{
		id:         stmtID,
		sql:        sql,
		numParams:  0,
		numColumns: numColumns,
	}
	c.getStmtMap()[stmtID] = stmt

	pkt := make([]byte, 0, 64)
	pkt = append(pkt, 0x00)
	pkt = append(pkt, byte(stmtID), byte(stmtID>>8), byte(stmtID>>16), byte(stmtID>>24))
	pkt = append(pkt, byte(numColumns), byte(numColumns>>8))
	pkt = append(pkt, 0x00, 0x00)
	pkt = append(pkt, 0x00)
	pkt = append(pkt, 0x00, 0x00)

	seq := byte(0)
	if err := c.writePacket(pkt, seq); err != nil {
		return err
	}
	seq++

	for i := 0; i < numColumns; i++ {
		colPkt := c.buildColumnDefPacket(fmt.Sprintf("col%d", i))
		if err := c.writePacket(colPkt, seq); err != nil {
			return err
		}
		seq++
	}
	if numColumns > 0 {
		eof := []byte{0xfe, 0x00, 0x00, 0x00, 0x00}
		if err := c.writePacket(eof, seq); err != nil {
			return err
		}
	}

	return nil
}

func (c *MySQLClient) handleStmtExecute(data []byte) error {
	if len(data) < 9 {
		return c.sendErrorPacket(0, "malformed COM_STMT_EXECUTE")
	}

	stmtID := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	stmt, ok := c.getStmtMap()[stmtID]
	if !ok {
		return c.sendErrorPacket(0, "unknown prepared statement")
	}

	baseCtx := c.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 30*time.Second)
	defer cancel()

	rows, err := c.server.db.Query(ctx, stmt.sql)
	if err == nil {
		defer rows.Close()
		return c.sendResultSetFromRows(rows)
	}

	result, err := c.server.db.Exec(ctx, stmt.sql)
	if err != nil {
		return c.sendErrorPacket(1, sanitizeMySQLError(err))
	}

	rowsAffected := uint64(0)
	if result.RowsAffected > 0 {
		rowsAffected = uint64(result.RowsAffected)
	}
	lastInsertID := uint64(0)
	if result.LastInsertID > 0 {
		lastInsertID = uint64(result.LastInsertID)
	}

	return c.sendOKPacket(rowsAffected, lastInsertID)
}

func (c *MySQLClient) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil
	}
	stmtID := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	delete(c.getStmtMap(), stmtID)
	return nil
}

func (c *MySQLClient) handleStmtReset(data []byte) error {
	if len(data) < 4 {
		return c.sendErrorPacket(0, "malformed COM_STMT_RESET")
	}
	stmtID := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	if _, ok := c.getStmtMap()[stmtID]; !ok {
		return c.sendErrorPacket(0, "unknown prepared statement")
	}
	return c.sendOKPacket(0, 0)
}

// handleStatistics returns a simple statistics string for COM_STATISTICS.
func (c *MySQLClient) handleStatistics() error {
	stats := fmt.Sprintf("Uptime: %d  Threads: %d  Queries: %d",
		int64(time.Since(c.connectTime).Seconds()),
		len(c.server.clients),
		0, // Query count not tracked per-server in this version
	)
	pkt := []byte(stats)
	return c.writePacket(pkt, 0)
}

// handleFieldList handles COM_FIELD_LIST by describing table columns.
// Payload: table_name\0[wildcard]
func (c *MySQLClient) handleFieldList(data []byte) error {
	// Extract null-terminated table name
	nullIdx := bytes.IndexByte(data, 0)
	if nullIdx < 0 {
		nullIdx = len(data)
	}
	tableName := string(data[:nullIdx])
	if tableName == "" {
		return c.sendErrorPacket(1046, "No database selected")
	}

	baseCtx := c.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 30*time.Second)
	defer cancel()

	// Use DESCRIBE to get column info
	rows, err := c.server.db.Query(ctx, "DESCRIBE "+tableName)
	if err != nil {
		return c.sendErrorPacket(1, sanitizeMySQLError(err))
	}
	defer rows.Close()

	seq := byte(1)
	for rows.Next() {
		row := make([]interface{}, 3)
		dest := make([]interface{}, 3)
		for i := range dest {
			dest[i] = &row[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return c.sendErrorPacket(1, sanitizeMySQLError(err))
		}
		colName := ""
		if row[0] != nil {
			colName = fmt.Sprintf("%v", row[0])
		}
		colPkt := c.buildColumnDefPacket(colName)
		if err := c.writePacket(colPkt, seq); err != nil {
			return err
		}
		seq++
	}

	return c.sendEOFPacket(seq)
}

// handleProcessInfo handles COM_PROCESS_INFO by returning active connections.
func (c *MySQLClient) handleProcessInfo() error {
	columns := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
	var resultRows [][]interface{}

	c.server.mu.Lock()
	for id := range c.server.clients {
		resultRows = append(resultRows, []interface{}{
			id,      // Id
			"",      // User
			"",      // Host
			"",      // db
			"Sleep", // Command
			0,       // Time
			"",      // State
			"",      // Info
		})
	}
	c.server.mu.Unlock()

	seq := byte(1)

	// Send column count
	countPkt := appendLenEncInt(nil, uint64(len(columns)))
	if err := c.writePacket(countPkt, seq); err != nil {
		return err
	}
	seq++

	// Send column definitions
	for _, colName := range columns {
		colPkt := c.buildColumnDefPacket(colName)
		if err := c.writePacket(colPkt, seq); err != nil {
			return err
		}
		seq++
	}

	// EOF after columns
	if err := c.sendEOFPacket(seq); err != nil {
		return err
	}
	seq++

	// Send rows
	for _, row := range resultRows {
		rowPkt := c.buildRowPacket(row)
		if err := c.writePacket(rowPkt, seq); err != nil {
			return err
		}
		seq++
	}

	return c.sendEOFPacket(seq)
}

// handleResetConnection handles COM_RESET_CONNECTION by clearing session state.
func (c *MySQLClient) handleResetConnection() error {
	c.database = ""
	c.stmts = make(map[uint32]*preparedStmt)
	c.nextStmtID = 0
	return c.sendOKPacket(0, 0)
}

