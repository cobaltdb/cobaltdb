package protocol

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
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
}

// NewMySQLServer creates a new MySQL-compatible server
func NewMySQLServer(db *engine.DB, version string) *MySQLServer {
	if version == "" {
		version = "5.7.0-CobaltDB"
	}
	return &MySQLServer{
		db:      db,
		version: version,
	}
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

// Close stops the MySQL server
func (s *MySQLServer) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// acceptLoop accepts incoming connections
func (s *MySQLServer) acceptLoop() {
	if s.listener == nil {
		return
	}
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}

		go s.handleConnection(conn)
	}
}

// handleConnection handles a MySQL client connection
func (s *MySQLServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	client := &MySQLClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		server: s,
	}

	// Send handshake
	if err := client.sendHandshake(); err != nil {
		return
	}

	// Read handshake response
	if err := client.readHandshakeResponse(); err != nil {
		return
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
	conn     net.Conn
	reader   *bufio.Reader
	server   *MySQLServer
	username string
	database string
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

	pkt := make([]byte, 0, 128)

	// Protocol version 10
	pkt = append(pkt, 0x0a)

	// Server version
	pkt = append(pkt, []byte(c.server.version)...)
	pkt = append(pkt, 0x00)

	// Connection ID
	pkt = append(pkt, 0x01, 0x00, 0x00, 0x00)

	// Auth plugin data part 1 (8 bytes)
	pkt = append(pkt, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)

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

	// Auth plugin data part 2 (12 bytes)
	pkt = append(pkt, make([]byte, 12)...)

	// Auth plugin name
	pkt = append(pkt, []byte("mysql_native_password")...)
	pkt = append(pkt, 0x00)

	return c.writePacket(pkt, 0)
}

// readHandshakeResponse reads the client's handshake response
func (c *MySQLClient) readHandshakeResponse() error {
	// Read packet header
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return err
	}

	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<8
	// sequence := header[3]

	// Read packet payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return err
	}

	// Parse response (simplified)
	// In a real implementation, we would parse capability flags,
	// max packet size, character set, reserved, username, auth response, database, auth plugin name

	return nil
}

// handleCommand handles a MySQL command
func (c *MySQLClient) handleCommand() error {
	// Read packet header
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return err
	}

	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<8
	// sequence := header[3]

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

	default:
		// Send error for unsupported commands
		return c.sendErrorPacket(0, "Unsupported command")
	}
}

// handleQuery handles a SQL query
func (c *MySQLClient) handleQuery(sql string) error {
	sql = strings.TrimSpace(sql)

	// Execute the query
	ctx := make(map[string]interface{})
	_ = ctx

	// Try to execute as query first
	rows, err := c.server.db.Query(nil, sql)
	if err == nil {
		defer rows.Close()
		return c.sendResultSet(rows)
	}

	// Try to execute as exec
	result, err := c.server.db.Exec(nil, sql)
	if err != nil {
		return c.sendErrorPacket(1, err.Error())
	}

	return c.sendOKPacket(uint16(result.RowsAffected), uint16(result.LastInsertID))
}

// sendResultSet sends a result set to the client
func (c *MySQLClient) sendResultSet(rows interface{}) error {
	// Simplified implementation - in a real implementation,
	// we would send column definitions and row data

	// Send OK packet with no results for now
	return c.sendOKPacket(0, 0)
}

// writePacket writes a MySQL protocol packet
func (c *MySQLClient) writePacket(data []byte, sequence byte) error {
	// Packet header: 3 bytes length + 1 byte sequence
	length := len(data)
	header := make([]byte, 4)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = sequence

	if _, err := c.conn.Write(header); err != nil {
		return err
	}

	if _, err := c.conn.Write(data); err != nil {
		return err
	}

	return nil
}

// sendOKPacket sends an OK packet
func (c *MySQLClient) sendOKPacket(affectedRows, lastInsertID uint16) error {
	pkt := make([]byte, 0, 32)

	// Header 0x00
	pkt = append(pkt, 0x00)

	// Affected rows (length encoded integer)
	pkt = append(pkt, byte(affectedRows))

	// Last insert ID (length encoded integer)
	pkt = append(pkt, byte(lastInsertID))

	// Status flags
	pkt = append(pkt, 0x02, 0x00)

	// Warnings
	pkt = append(pkt, 0x00, 0x00)

	return c.writePacket(pkt, 0)
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

// scramblePassword scrambles a password using MySQL's algorithm
func scramblePassword(password, scramble []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// SHA1(password)
	h1 := sha1.New()
	h1.Write(password)
	hash1 := h1.Sum(nil)

	// SHA1(SHA1(password))
	h2 := sha1.New()
	h2.Write(hash1)
	hash2 := h2.Sum(nil)

	// SHA1(scramble + SHA1(SHA1(password)))
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

// readLenEncInt reads a length-encoded integer
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

// writeLenEncInt writes a length-encoded integer
func writeLenEncInt(value uint64) []byte {
	switch {
	case value < 251:
		return []byte{byte(value)}
	case value < 1<<16:
		return []byte{0xfc, byte(value), byte(value >> 8)}
	case value < 1<<24:
		return []byte{0xfd, byte(value), byte(value >> 8), byte(value >> 16)}
	default:
		buf := make([]byte, 9)
		buf[0] = 0xfe
		binary.LittleEndian.PutUint64(buf[1:], value)
		return buf
	}
}
