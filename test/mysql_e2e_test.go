package test

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/protocol"
)

// mysqlTestClient simulates a MySQL client for e2e testing
type mysqlTestClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func newMySQLTestClient(addr string) (*mysqlTestClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &mysqlTestClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}, nil
}

func (c *mysqlTestClient) close() {
	c.conn.Close()
}

// readPacket reads a MySQL packet (4-byte header + payload)
func (c *mysqlTestClient) readPacket() ([]byte, byte, error) {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	header := make([]byte, 4)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return nil, 0, fmt.Errorf("read header: %w", err)
	}

	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	seq := header[3]

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return nil, 0, fmt.Errorf("read payload: %w", err)
		}
	}

	return payload, seq, nil
}

// writePacket writes a MySQL packet
func (c *mysqlTestClient) writePacket(data []byte, seq byte) error {
	length := len(data)
	header := make([]byte, 4)
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = seq

	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	if _, err := c.conn.Write(data); err != nil {
		return err
	}
	return nil
}

// readHandshake reads and parses the server's handshake
func (c *mysqlTestClient) readHandshake() (string, uint32, error) {
	payload, _, err := c.readPacket()
	if err != nil {
		return "", 0, err
	}

	if len(payload) < 6 {
		return "", 0, fmt.Errorf("handshake too short: %d bytes", len(payload))
	}

	// Protocol version
	protoVersion := payload[0]
	if protoVersion != 0x0a {
		return "", 0, fmt.Errorf("unexpected protocol version: %d", protoVersion)
	}

	// Server version (null-terminated)
	versionEnd := 1
	for versionEnd < len(payload) && payload[versionEnd] != 0 {
		versionEnd++
	}
	serverVersion := string(payload[1:versionEnd])

	// Connection ID (4 bytes after null terminator)
	offset := versionEnd + 1
	if offset+4 > len(payload) {
		return serverVersion, 0, nil
	}
	connID := binary.LittleEndian.Uint32(payload[offset : offset+4])

	return serverVersion, connID, nil
}

// sendHandshakeResponse sends the client handshake response
func (c *mysqlTestClient) sendHandshakeResponse(username, database string) error {
	var pkt []byte

	// Capability flags (4 bytes)
	pkt = append(pkt, 0x85, 0xa6, 0xff, 0x01) // CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB etc

	// Max packet size (4 bytes)
	pkt = append(pkt, 0x00, 0x00, 0x00, 0x01) // 16MB

	// Character set (1 byte) - utf8mb4
	pkt = append(pkt, 0x21)

	// Reserved (23 bytes)
	pkt = append(pkt, make([]byte, 23)...)

	// Username (null-terminated)
	pkt = append(pkt, []byte(username)...)
	pkt = append(pkt, 0x00)

	// Auth response length + data (empty for no auth)
	pkt = append(pkt, 0x00)

	// Database (null-terminated)
	if database != "" {
		pkt = append(pkt, []byte(database)...)
		pkt = append(pkt, 0x00)
	}

	return c.writePacket(pkt, 1)
}

// readOKOrError reads an OK or Error packet
func (c *mysqlTestClient) readOKOrError() (bool, string, error) {
	payload, _, err := c.readPacket()
	if err != nil {
		return false, "", err
	}

	if len(payload) == 0 {
		return false, "empty packet", nil
	}

	switch payload[0] {
	case 0x00: // OK
		return true, "OK", nil
	case 0xff: // Error
		if len(payload) >= 3 {
			errCode := binary.LittleEndian.Uint16(payload[1:3])
			msg := ""
			if len(payload) > 9 {
				msg = string(payload[9:]) // Skip error code, # marker, and SQL state
			}
			return false, fmt.Sprintf("error %d: %s", errCode, msg), nil
		}
		return false, "unknown error", nil
	default:
		return false, fmt.Sprintf("unexpected response type: 0x%02x", payload[0]), nil
	}
}

// sendQuery sends a COM_QUERY command
func (c *mysqlTestClient) sendQuery(sql string) error {
	var pkt []byte
	pkt = append(pkt, 0x03) // COM_QUERY
	pkt = append(pkt, []byte(sql)...)
	return c.writePacket(pkt, 0)
}

func (c *mysqlTestClient) sendStmtPrepare(sql string) error {
	var pkt []byte
	pkt = append(pkt, protocol.MySQLComStmtPrepare)
	pkt = append(pkt, []byte(sql)...)
	return c.writePacket(pkt, 0)
}

func (c *mysqlTestClient) readStmtPrepareOK() (uint32, int, int, error) {
	payload, _, err := c.readPacket()
	if err != nil {
		return 0, 0, 0, err
	}
	if len(payload) < 12 {
		return 0, 0, 0, fmt.Errorf("prepare response too short: %d", len(payload))
	}
	if payload[0] == 0xff {
		msg := ""
		if len(payload) > 9 {
			msg = string(payload[9:])
		}
		return 0, 0, 0, fmt.Errorf("prepare error: %s", msg)
	}
	if payload[0] != 0x00 {
		return 0, 0, 0, fmt.Errorf("unexpected prepare response: 0x%02x", payload[0])
	}

	stmtID := binary.LittleEndian.Uint32(payload[1:5])
	numColumns := int(binary.LittleEndian.Uint16(payload[5:7]))
	numParams := int(binary.LittleEndian.Uint16(payload[7:9]))

	for i := 0; i < numParams; i++ {
		if _, _, err := c.readPacket(); err != nil {
			return 0, 0, 0, fmt.Errorf("read parameter definition %d: %w", i, err)
		}
	}
	if numParams > 0 {
		if _, _, err := c.readPacket(); err != nil {
			return 0, 0, 0, fmt.Errorf("read parameter EOF: %w", err)
		}
	}

	for i := 0; i < numColumns; i++ {
		if _, _, err := c.readPacket(); err != nil {
			return 0, 0, 0, fmt.Errorf("read column definition %d: %w", i, err)
		}
	}
	if numColumns > 0 {
		if _, _, err := c.readPacket(); err != nil {
			return 0, 0, 0, fmt.Errorf("read column EOF: %w", err)
		}
	}

	return stmtID, numColumns, numParams, nil
}

func (c *mysqlTestClient) readStmtExecuteCursorMetadata(numColumns int) error {
	payload, _, err := c.readPacket()
	if err != nil {
		return fmt.Errorf("read cursor column count: %w", err)
	}
	if len(payload) == 0 || payload[0] == 0xff {
		return fmt.Errorf("unexpected cursor execute response: %q", string(payload))
	}
	if int(payload[0]) != numColumns {
		return fmt.Errorf("cursor column count = %d, want %d", int(payload[0]), numColumns)
	}
	for i := 0; i < numColumns; i++ {
		if _, _, err := c.readPacket(); err != nil {
			return fmt.Errorf("read cursor column definition %d: %w", i, err)
		}
	}
	eof, _, err := c.readPacket()
	if err != nil {
		return fmt.Errorf("read cursor metadata EOF: %w", err)
	}
	if len(eof) == 0 || eof[0] != 0xfe {
		return fmt.Errorf("expected cursor metadata EOF, got %q", string(eof))
	}
	return nil
}

func (c *mysqlTestClient) sendStmtExecute(stmtID uint32, types []byte, values []interface{}) error {
	return c.sendStmtExecuteWithFlags(stmtID, 0x00, types, values)
}

func (c *mysqlTestClient) sendStmtExecuteWithFlags(stmtID uint32, flags byte, types []byte, values []interface{}) error {
	var pkt []byte
	pkt = append(pkt, protocol.MySQLComStmtExecute)
	var stmtIDBuf [4]byte
	binary.LittleEndian.PutUint32(stmtIDBuf[:], stmtID)
	pkt = append(pkt, stmtIDBuf[:]...)
	pkt = append(pkt, flags)                  // flags
	pkt = append(pkt, 0x01, 0x00, 0x00, 0x00) // iteration count
	pkt = append(pkt, make([]byte, (len(types)+7)/8)...)
	pkt = append(pkt, 0x01) // new params bound
	for _, typ := range types {
		pkt = append(pkt, typ, 0x00)
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		switch types[i] {
		case protocol.MySQLTypeLongLong:
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(value.(int64)))
			pkt = append(pkt, buf[:]...)
		case protocol.MySQLTypeVarString:
			pkt = appendMySQLLenEncString(pkt, value.(string))
		case protocol.MySQLTypeDate, protocol.MySQLTypeNewDate:
			pkt = appendMySQLStmtDate(pkt, value.(time.Time))
		case protocol.MySQLTypeDateTime, protocol.MySQLTypeTimestamp:
			pkt = appendMySQLStmtDateTime(pkt, value.(time.Time))
		case protocol.MySQLTypeTime:
			pkt = appendMySQLStmtTime(pkt, value.(time.Duration))
		default:
			return fmt.Errorf("test encoder does not support type 0x%02x", types[i])
		}
	}
	return c.writePacket(pkt, 0)
}

func (c *mysqlTestClient) sendStmtFetch(stmtID uint32, rowCount uint32) error {
	var pkt []byte
	pkt = append(pkt, protocol.MySQLComStmtFetch)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], stmtID)
	pkt = append(pkt, buf[:]...)
	binary.LittleEndian.PutUint32(buf[:], rowCount)
	pkt = append(pkt, buf[:]...)
	return c.writePacket(pkt, 0)
}

func (c *mysqlTestClient) sendStmtLongData(stmtID uint32, paramID uint16, payload []byte) error {
	var pkt []byte
	pkt = append(pkt, protocol.MySQLComStmtSendLongData)
	var stmtIDBuf [4]byte
	binary.LittleEndian.PutUint32(stmtIDBuf[:], stmtID)
	pkt = append(pkt, stmtIDBuf[:]...)
	var paramIDBuf [2]byte
	binary.LittleEndian.PutUint16(paramIDBuf[:], paramID)
	pkt = append(pkt, paramIDBuf[:]...)
	pkt = append(pkt, payload...)
	return c.writePacket(pkt, 0)
}

func appendMySQLLenEncString(dst []byte, s string) []byte {
	if len(s) >= 251 {
		panic("test helper only supports short strings")
	}
	dst = append(dst, byte(len(s)))
	return append(dst, s...)
}

func appendMySQLStmtDate(dst []byte, value time.Time) []byte {
	dst = append(dst, 4)
	var year [2]byte
	binary.LittleEndian.PutUint16(year[:], uint16(value.Year()))
	dst = append(dst, year[:]...)
	dst = append(dst, byte(value.Month()), byte(value.Day()))
	return dst
}

func appendMySQLStmtDateTime(dst []byte, value time.Time) []byte {
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

func appendMySQLStmtTime(dst []byte, value time.Duration) []byte {
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

// sendPing sends a COM_PING command
func (c *mysqlTestClient) sendPing() error {
	return c.writePacket([]byte{0x0e}, 0)
}

// sendQuit sends a COM_QUIT command
func (c *mysqlTestClient) sendQuit() error {
	return c.writePacket([]byte{0x01}, 0)
}

// readResultSet reads a full MySQL result set (column count + column defs + EOF + rows + EOF)
func (c *mysqlTestClient) readResultSet() ([]string, [][]string, error) {
	// First packet: column count or OK/Error
	payload, _, err := c.readPacket()
	if err != nil {
		return nil, nil, fmt.Errorf("read column count: %w", err)
	}

	if len(payload) == 0 {
		return nil, nil, fmt.Errorf("empty response")
	}

	// Check for OK (for exec-type queries)
	if payload[0] == 0x00 {
		return nil, nil, nil // OK packet, no result set
	}

	// Check for Error
	if payload[0] == 0xff {
		msg := ""
		if len(payload) > 9 {
			msg = string(payload[9:])
		}
		return nil, nil, fmt.Errorf("server error: %s", msg)
	}

	// Parse column count (length-encoded integer)
	colCount := int(payload[0])
	if colCount == 0xfc && len(payload) >= 3 {
		colCount = int(payload[1]) | int(payload[2])<<8
	}

	// Read column definitions
	columns := make([]string, colCount)
	for i := 0; i < colCount; i++ {
		colPayload, _, err := c.readPacket()
		if err != nil {
			return nil, nil, fmt.Errorf("read column %d: %w", i, err)
		}

		// Parse column definition - extract column name
		// Format: catalog(lenenc) + schema(lenenc) + table(lenenc) + org_table(lenenc) + name(lenenc) + org_name(lenenc)
		offset := 0
		for skip := 0; skip < 4; skip++ { // Skip catalog, schema, table, org_table
			if offset >= len(colPayload) {
				break
			}
			strLen := int(colPayload[offset])
			offset += 1 + strLen
		}
		// Now read name
		if offset < len(colPayload) {
			nameLen := int(colPayload[offset])
			offset++
			if offset+nameLen <= len(colPayload) {
				columns[i] = string(colPayload[offset : offset+nameLen])
			}
		}
	}

	// Read EOF packet (end of column definitions)
	eofPayload, _, err := c.readPacket()
	if err != nil {
		return nil, nil, fmt.Errorf("read column EOF: %w", err)
	}
	if len(eofPayload) == 0 || eofPayload[0] != 0xfe {
		return nil, nil, fmt.Errorf("expected EOF, got 0x%02x", eofPayload[0])
	}

	// Read row data packets until EOF
	var rows [][]string
	for {
		rowPayload, _, err := c.readPacket()
		if err != nil {
			return nil, nil, fmt.Errorf("read row: %w", err)
		}

		if len(rowPayload) == 0 {
			break
		}

		// Check for EOF (end of rows)
		if rowPayload[0] == 0xfe && len(rowPayload) < 9 {
			break
		}

		// Parse row data. COM_QUERY returns text-protocol rows, while
		// COM_STMT_EXECUTE returns binary-protocol rows.
		row := make([]string, 0, colCount)
		offset := 0
		nullBitmap := []byte(nil)
		if rowPayload[0] == 0x00 {
			nullBitmapLen := (colCount + 7 + 2) / 8
			if len(rowPayload) < 1+nullBitmapLen {
				return nil, nil, fmt.Errorf("malformed binary row")
			}
			nullBitmap = rowPayload[1 : 1+nullBitmapLen]
			offset = 1 + nullBitmapLen
		}
		for j := 0; j < colCount && offset < len(rowPayload); j++ {
			if nullBitmap != nil {
				bit := j + 2
				if nullBitmap[bit/8]&(1<<uint(bit%8)) != 0 {
					row = append(row, "NULL")
					continue
				}
			}
			if nullBitmap == nil && rowPayload[offset] == 0xfb {
				row = append(row, "NULL")
				offset++
			} else {
				strLen := int(rowPayload[offset])
				offset++
				if offset+strLen <= len(rowPayload) {
					row = append(row, string(rowPayload[offset:offset+strLen]))
					offset += strLen
				}
			}
		}
		rows = append(rows, row)
	}

	return columns, rows, nil
}

// startMySQLTestServer starts a MySQL server on a random port and returns its address
func startMySQLTestServer(t *testing.T) (string, *protocol.MySQLServer, *engine.DB) {
	t.Helper()

	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}

	mysqlSrv := protocol.NewMySQLServer(db, "5.7.0-CobaltDB-Test")
	if err := mysqlSrv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Failed to start MySQL server: %v", err)
	}

	addr := mysqlSrv.Addr().String()
	time.Sleep(50 * time.Millisecond)

	t.Cleanup(func() {
		mysqlSrv.Close()
		db.Close()
	})

	return addr, mysqlSrv, db
}

// TestMySQLHandshake tests the MySQL handshake protocol
func TestMySQLHandshake(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Read handshake
	version, connID, err := client.readHandshake()
	if err != nil {
		t.Fatalf("Read handshake failed: %v", err)
	}

	if !strings.Contains(version, "CobaltDB") {
		t.Fatalf("Expected CobaltDB in version, got: %s", version)
	}
	if connID == 0 {
		t.Fatal("Expected non-zero connection ID")
	}

	t.Logf("Server version: %s, Connection ID: %d", version, connID)

	// Send handshake response
	if err := client.sendHandshakeResponse("testuser", "testdb"); err != nil {
		t.Fatalf("Send handshake response failed: %v", err)
	}

	// Read OK
	ok, msg, err := client.readOKOrError()
	if err != nil {
		t.Fatalf("Read OK failed: %v", err)
	}
	if !ok {
		t.Fatalf("Expected OK after handshake, got: %s", msg)
	}
}

// TestMySQLPing tests COM_PING
func TestMySQLPing(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// Ping
	if err := client.sendPing(); err != nil {
		t.Fatalf("Send ping failed: %v", err)
	}

	ok, msg, err := client.readOKOrError()
	if err != nil {
		t.Fatalf("Read ping response failed: %v", err)
	}
	if !ok {
		t.Fatalf("Expected OK for ping, got: %s", msg)
	}
}

// TestMySQLQueryCreateTable tests DDL over MySQL protocol
func TestMySQLQueryCreateTable(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// CREATE TABLE
	if err := client.sendQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"); err != nil {
		t.Fatalf("Send CREATE TABLE failed: %v", err)
	}

	ok, msg, err := client.readOKOrError()
	if err != nil {
		t.Fatalf("Read CREATE response failed: %v", err)
	}
	if !ok {
		t.Fatalf("Expected OK for CREATE TABLE, got: %s", msg)
	}
}

// TestMySQLQueryInsertAndSelect tests DML over MySQL protocol
func TestMySQLQueryInsertAndSelect(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// CREATE TABLE
	client.sendQuery("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
	client.readOKOrError()

	// INSERT rows
	inserts := []string{
		"INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)",
		"INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99)",
		"INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 29.99)",
	}

	for _, sql := range inserts {
		if err := client.sendQuery(sql); err != nil {
			t.Fatalf("Send INSERT failed: %v", err)
		}
		ok, msg, err := client.readOKOrError()
		if err != nil {
			t.Fatalf("Read INSERT response failed: %v", err)
		}
		if !ok {
			t.Fatalf("Expected OK for INSERT, got: %s", msg)
		}
	}

	// SELECT
	if err := client.sendQuery("SELECT id, name, price FROM products ORDER BY id"); err != nil {
		t.Fatalf("Send SELECT failed: %v", err)
	}

	columns, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("Read result set failed: %v", err)
	}

	// Verify columns
	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d: %v", len(columns), columns)
	}
	if columns[0] != "id" || columns[1] != "name" || columns[2] != "price" {
		t.Fatalf("Unexpected columns: %v", columns)
	}

	// Verify rows
	if len(rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(rows))
	}

	if rows[0][0] != "1" || rows[0][1] != "Widget" {
		t.Fatalf("Unexpected first row: %v", rows[0])
	}
	if rows[2][0] != "3" || rows[2][1] != "Doohickey" {
		t.Fatalf("Unexpected third row: %v", rows[2])
	}

	t.Logf("MySQL result set: %d columns, %d rows", len(columns), len(rows))
	for i, row := range rows {
		t.Logf("  Row %d: %v", i, row)
	}
}

func TestMySQLPreparedStatementExecuteWithParameters(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	client.sendQuery("CREATE TABLE ps_users (id INTEGER PRIMARY KEY, name TEXT)")
	if ok, msg, err := client.readOKOrError(); err != nil {
		t.Fatalf("CREATE response failed: %v", err)
	} else if !ok {
		t.Fatalf("CREATE expected OK, got: %s", msg)
	}

	if err := client.sendStmtPrepare("INSERT INTO ps_users (id, name) VALUES (?, ?)"); err != nil {
		t.Fatalf("send prepare insert: %v", err)
	}
	insertStmtID, cols, params, err := client.readStmtPrepareOK()
	if err != nil {
		t.Fatalf("read prepare insert: %v", err)
	}
	if cols != 0 || params != 2 {
		t.Fatalf("prepare insert metadata cols=%d params=%d, want cols=0 params=2", cols, params)
	}

	if err := client.sendStmtExecute(
		insertStmtID,
		[]byte{protocol.MySQLTypeLongLong, protocol.MySQLTypeVarString},
		[]interface{}{int64(1), "Ada"},
	); err != nil {
		t.Fatalf("execute insert: %v", err)
	}
	if ok, msg, err := client.readOKOrError(); err != nil {
		t.Fatalf("read execute insert: %v", err)
	} else if !ok {
		t.Fatalf("execute insert expected OK, got: %s", msg)
	}

	if err := client.sendStmtPrepare("SELECT name FROM ps_users WHERE id = ?"); err != nil {
		t.Fatalf("send prepare select: %v", err)
	}
	selectStmtID, cols, params, err := client.readStmtPrepareOK()
	if err != nil {
		t.Fatalf("read prepare select: %v", err)
	}
	if cols != 1 || params != 1 {
		t.Fatalf("prepare select metadata cols=%d params=%d, want cols=1 params=1", cols, params)
	}

	if err := client.sendStmtExecute(
		selectStmtID,
		[]byte{protocol.MySQLTypeLongLong},
		[]interface{}{int64(1)},
	); err != nil {
		t.Fatalf("execute select: %v", err)
	}
	columns, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("read prepared select result: %v", err)
	}
	if len(columns) != 1 || len(rows) != 1 || rows[0][0] != "Ada" {
		t.Fatalf("unexpected prepared select result columns=%v rows=%v", columns, rows)
	}
}

func TestMySQLPreparedStatementTemporalParameters(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	client.sendQuery("CREATE TABLE ps_temporal (id INTEGER PRIMARY KEY, d TEXT, dt TEXT, ts TEXT, tm TEXT)")
	if ok, msg, err := client.readOKOrError(); err != nil {
		t.Fatalf("CREATE response failed: %v", err)
	} else if !ok {
		t.Fatalf("CREATE expected OK, got: %s", msg)
	}

	if err := client.sendStmtPrepare("INSERT INTO ps_temporal (id, d, dt, ts, tm) VALUES (?, ?, ?, ?, ?)"); err != nil {
		t.Fatalf("send prepare insert: %v", err)
	}
	insertStmtID, cols, params, err := client.readStmtPrepareOK()
	if err != nil {
		t.Fatalf("read prepare insert: %v", err)
	}
	if cols != 0 || params != 5 {
		t.Fatalf("prepare insert metadata cols=%d params=%d, want cols=0 params=5", cols, params)
	}

	if err := client.sendStmtExecute(
		insertStmtID,
		[]byte{protocol.MySQLTypeLongLong, protocol.MySQLTypeDate, protocol.MySQLTypeDateTime, protocol.MySQLTypeTimestamp, protocol.MySQLTypeTime},
		[]interface{}{
			int64(1),
			time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 5, 24, 18, 45, 30, 123456000, time.UTC),
			time.Date(2026, 5, 24, 18, 46, 0, 0, time.UTC),
			26*time.Hour + 3*time.Minute + 4*time.Second + 567*time.Microsecond,
		},
	); err != nil {
		t.Fatalf("execute insert: %v", err)
	}
	if ok, msg, err := client.readOKOrError(); err != nil {
		t.Fatalf("read execute insert: %v", err)
	} else if !ok {
		t.Fatalf("execute insert expected OK, got: %s", msg)
	}

	client.sendQuery("SELECT d, dt, ts, tm FROM ps_temporal WHERE id = 1")
	columns, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("read temporal select result: %v", err)
	}
	want := []string{"2026-05-24", "2026-05-24 18:45:30.123456", "2026-05-24 18:46:00", "26:03:04.000567"}
	if len(columns) != 4 || len(rows) != 1 || len(rows[0]) != 4 {
		t.Fatalf("unexpected temporal result columns=%v rows=%v", columns, rows)
	}
	for i := range want {
		if rows[0][i] != want[i] {
			t.Fatalf("temporal column %d = %q, want %q; rows=%v", i, rows[0][i], want[i], rows)
		}
	}
}

func TestMySQLPreparedStatementLongData(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	client.sendQuery("CREATE TABLE ps_long_data (id INTEGER PRIMARY KEY, body TEXT)")
	if ok, msg, err := client.readOKOrError(); err != nil {
		t.Fatalf("CREATE response failed: %v", err)
	} else if !ok {
		t.Fatalf("CREATE expected OK, got: %s", msg)
	}

	if err := client.sendStmtPrepare("INSERT INTO ps_long_data (id, body) VALUES (?, ?)"); err != nil {
		t.Fatalf("send prepare insert: %v", err)
	}
	insertStmtID, cols, params, err := client.readStmtPrepareOK()
	if err != nil {
		t.Fatalf("read prepare insert: %v", err)
	}
	if cols != 0 || params != 2 {
		t.Fatalf("prepare insert metadata cols=%d params=%d, want cols=0 params=2", cols, params)
	}

	if err := client.sendStmtLongData(insertStmtID, 1, []byte("chunk-")); err != nil {
		t.Fatalf("send long data chunk 1: %v", err)
	}
	if err := client.sendStmtLongData(insertStmtID, 1, []byte("payload")); err != nil {
		t.Fatalf("send long data chunk 2: %v", err)
	}
	if err := client.sendStmtExecute(
		insertStmtID,
		[]byte{protocol.MySQLTypeLongLong, protocol.MySQLTypeLongBlob},
		[]interface{}{int64(1), nil},
	); err != nil {
		t.Fatalf("execute insert: %v", err)
	}
	if ok, msg, err := client.readOKOrError(); err != nil {
		t.Fatalf("read execute insert: %v", err)
	} else if !ok {
		t.Fatalf("execute insert expected OK, got: %s", msg)
	}

	client.sendQuery("SELECT body FROM ps_long_data WHERE id = 1")
	columns, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("read long data select result: %v", err)
	}
	if len(columns) != 1 || len(rows) != 1 || rows[0][0] != "chunk-payload" {
		t.Fatalf("unexpected long data result columns=%v rows=%v", columns, rows)
	}
}

func TestMySQLPreparedStatementCursorFetch(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	client.sendQuery("CREATE TABLE ps_cursor (id INTEGER PRIMARY KEY, body TEXT)")
	if ok, msg, err := client.readOKOrError(); err != nil {
		t.Fatalf("CREATE response failed: %v", err)
	} else if !ok {
		t.Fatalf("CREATE expected OK, got: %s", msg)
	}
	client.sendQuery("INSERT INTO ps_cursor VALUES (1, 'row')")
	if ok, msg, err := client.readOKOrError(); err != nil {
		t.Fatalf("INSERT response failed: %v", err)
	} else if !ok {
		t.Fatalf("INSERT expected OK, got: %s", msg)
	}

	if err := client.sendStmtPrepare("SELECT body FROM ps_cursor WHERE id = ?"); err != nil {
		t.Fatalf("send prepare select: %v", err)
	}
	stmtID, cols, params, err := client.readStmtPrepareOK()
	if err != nil {
		t.Fatalf("read prepare select: %v", err)
	}
	if cols != 1 || params != 1 {
		t.Fatalf("prepare select metadata cols=%d params=%d, want cols=1 params=1", cols, params)
	}

	if err := client.sendStmtExecuteWithFlags(stmtID, 0x01, []byte{protocol.MySQLTypeLongLong}, []interface{}{int64(1)}); err != nil {
		t.Fatalf("execute select with cursor flag: %v", err)
	}
	if err := client.readStmtExecuteCursorMetadata(cols); err != nil {
		t.Fatalf("read cursor execute metadata: %v", err)
	}

	if err := client.sendStmtFetch(stmtID, 1); err != nil {
		t.Fatalf("fetch cursor row: %v", err)
	}
	rowPayload, _, err := client.readPacket()
	if err != nil {
		t.Fatalf("read cursor row packet: %v", err)
	}
	if !strings.Contains(string(rowPayload), "row") {
		t.Fatalf("expected fetched binary row packet to contain row value, got %q", string(rowPayload))
	}
	eofPayload, _, err := client.readPacket()
	if err != nil {
		t.Fatalf("read cursor EOF packet: %v", err)
	}
	if len(eofPayload) == 0 || eofPayload[0] != 0xfe {
		t.Fatalf("expected cursor EOF packet, got %q", string(eofPayload))
	}

	if err := client.sendStmtExecute(stmtID, []byte{protocol.MySQLTypeLongLong}, []interface{}{int64(1)}); err != nil {
		t.Fatalf("execute select without cursor flag: %v", err)
	}
	columns, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("read prepared select result after cursor fetch: %v", err)
	}
	if len(columns) != 1 || len(rows) != 1 || rows[0][0] != "row" {
		t.Fatalf("unexpected prepared select result after cursor fetch columns=%v rows=%v", columns, rows)
	}
}

// TestMySQLQueryWithWhere tests SELECT with WHERE over MySQL protocol
func TestMySQLQueryWithWhere(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// Setup
	client.sendQuery("CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price REAL)")
	client.readOKOrError()

	for i := 1; i <= 5; i++ {
		cat := "A"
		if i > 3 {
			cat = "B"
		}
		sql := fmt.Sprintf("INSERT INTO items (id, category, price) VALUES (%d, '%s', %d.99)", i, cat, i*10)
		client.sendQuery(sql)
		client.readOKOrError()
	}

	// SELECT with WHERE
	client.sendQuery("SELECT id, category, price FROM items WHERE category = 'A' ORDER BY id")
	columns, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("Read result set failed: %v", err)
	}

	if len(columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(columns))
	}
	if len(rows) != 3 {
		t.Fatalf("Expected 3 rows for category A, got %d", len(rows))
	}
}

// TestMySQLQueryAggregation tests aggregation queries over MySQL protocol
func TestMySQLQueryAggregation(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// Setup
	client.sendQuery("CREATE TABLE sales (id INTEGER PRIMARY KEY, amount REAL)")
	client.readOKOrError()

	for i := 1; i <= 5; i++ {
		sql := fmt.Sprintf("INSERT INTO sales (id, amount) VALUES (%d, %d.00)", i, i*10)
		client.sendQuery(sql)
		client.readOKOrError()
	}

	// COUNT
	client.sendQuery("SELECT COUNT(*) FROM sales")
	_, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if len(rows) != 1 || rows[0][0] != "5" {
		t.Fatalf("Expected COUNT=5, got %v", rows)
	}

	// SUM
	client.sendQuery("SELECT SUM(amount) FROM sales")
	_, rows, err = client.readResultSet()
	if err != nil {
		t.Fatalf("SUM query failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row for SUM, got %d", len(rows))
	}
	t.Logf("SUM result: %s", rows[0][0])
}

// TestMySQLMultipleQueries tests multiple sequential queries
func TestMySQLMultipleQueries(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// Multiple DDL + DML operations
	queries := []struct {
		sql      string
		isSelect bool
	}{
		{"CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)", false},
		{"INSERT INTO t1 (id, val) VALUES (1, 'hello')", false},
		{"INSERT INTO t1 (id, val) VALUES (2, 'world')", false},
		{"SELECT val FROM t1 ORDER BY id", true},
		{"UPDATE t1 SET val = 'HELLO' WHERE id = 1", false},
		{"SELECT val FROM t1 WHERE id = 1", true},
		{"DELETE FROM t1 WHERE id = 2", false},
		{"SELECT COUNT(*) FROM t1", true},
	}

	for _, q := range queries {
		if err := client.sendQuery(q.sql); err != nil {
			t.Fatalf("Send query '%s' failed: %v", q.sql, err)
		}

		if q.isSelect {
			cols, rows, err := client.readResultSet()
			if err != nil {
				t.Fatalf("Query '%s' result set failed: %v", q.sql, err)
			}
			t.Logf("Query '%s': %d cols, %d rows", q.sql, len(cols), len(rows))
		} else {
			ok, msg, err := client.readOKOrError()
			if err != nil {
				t.Fatalf("Query '%s' response failed: %v", q.sql, err)
			}
			if !ok {
				t.Fatalf("Query '%s' expected OK, got: %s", q.sql, msg)
			}
		}
	}
}

// TestMySQLQuit tests COM_QUIT
func TestMySQLQuit(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// Quit
	if err := client.sendQuit(); err != nil {
		t.Fatalf("Send quit failed: %v", err)
	}

	// Connection should be closed by server
	time.Sleep(50 * time.Millisecond)
	_, _, err = client.readPacket()
	if err == nil {
		t.Log("Connection still open (may be OK if response was buffered)")
	}
}

// TestMySQLErrorHandling tests error responses
func TestMySQLErrorHandling(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// Query non-existent table
	client.sendQuery("SELECT * FROM nonexistent_table")
	// Should get either error or empty result
	payload, _, err := client.readPacket()
	if err != nil {
		t.Fatalf("Read error response failed: %v", err)
	}

	if len(payload) > 0 && payload[0] == 0xff {
		t.Log("Got expected error for non-existent table")
	} else {
		t.Log("Got response (may be handled differently)")
	}

	// Connection should still work after error
	client.sendQuery("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	ok, _, err := client.readOKOrError()
	if err != nil {
		t.Fatalf("Post-error query failed: %v", err)
	}
	if !ok {
		t.Fatal("Expected OK after error recovery")
	}
}

// TestMySQLShowTables tests SHOW TABLES via MySQL protocol
func TestMySQLShowTables(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// Create tables
	client.sendQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	client.readOKOrError()
	client.sendQuery("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
	client.readOKOrError()

	// SHOW TABLES
	client.sendQuery("SHOW TABLES")
	cols, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}

	t.Logf("SHOW TABLES: columns=%v, rows=%d", cols, len(rows))
	if len(rows) < 2 {
		t.Fatalf("Expected at least 2 tables, got %d", len(rows))
	}
}

// TestMySQLShowCreateTable tests SHOW CREATE TABLE via MySQL protocol
func TestMySQLShowCreateTable(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// Create table
	client.sendQuery("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)")
	client.readOKOrError()

	// SHOW CREATE TABLE
	client.sendQuery("SHOW CREATE TABLE products")
	cols, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}

	if len(cols) != 2 {
		t.Fatalf("Expected 2 columns (Table, Create Table), got %d", len(cols))
	}
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}

	t.Logf("Table: %s", rows[0][0])
	t.Logf("CREATE: %s", rows[0][1])

	if !strings.Contains(rows[0][1], "CREATE TABLE products") {
		t.Fatalf("Expected CREATE TABLE statement, got: %s", rows[0][1])
	}
}

// TestMySQLSetCommand tests SET commands via MySQL protocol
func TestMySQLSetCommand(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// SET NAMES utf8 - common MySQL client initialization
	client.sendQuery("SET NAMES utf8")
	ok, msg, err := client.readOKOrError()
	if err != nil {
		t.Fatalf("SET NAMES failed: %v", err)
	}
	if !ok {
		t.Fatalf("Expected OK for SET NAMES, got: %s", msg)
	}

	// SET character_set_client = utf8mb4
	client.sendQuery("SET character_set_client = utf8mb4")
	ok, msg, err = client.readOKOrError()
	if err != nil {
		t.Fatalf("SET variable failed: %v", err)
	}
	if !ok {
		t.Fatalf("Expected OK for SET variable, got: %s", msg)
	}
}

// TestMySQLUseCommand tests USE command via MySQL protocol
func TestMySQLUseCommand(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// USE database
	client.sendQuery("USE cobaltdb")
	ok, msg, err := client.readOKOrError()
	if err != nil {
		t.Fatalf("USE failed: %v", err)
	}
	if !ok {
		t.Fatalf("Expected OK for USE, got: %s", msg)
	}
}

// TestMySQLSelectVariable tests SELECT @@variable via MySQL protocol
func TestMySQLSelectVariable(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// Handshake
	client.readHandshake()
	client.sendHandshakeResponse("test", "")
	client.readOKOrError()

	// SELECT @@version
	client.sendQuery("SELECT @@version")
	cols, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("SELECT @@version failed: %v", err)
	}

	if len(cols) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(cols))
	}
	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}

	version := rows[0][0]
	if !strings.Contains(version, "CobaltDB") {
		t.Fatalf("Expected version to contain 'CobaltDB', got: %s", version)
	}
	t.Logf("Version: %s", version)
}

// TestMySQLFullClientWorkflow simulates a real MySQL client connection workflow
func TestMySQLFullClientWorkflow(t *testing.T) {
	addr, _, _ := startMySQLTestServer(t)

	client, err := newMySQLTestClient(addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.close()

	// 1. Handshake
	version, connID, err := client.readHandshake()
	if err != nil {
		t.Fatalf("Handshake failed: %v", err)
	}
	t.Logf("Connected: version=%s connID=%d", version, connID)

	client.sendHandshakeResponse("root", "cobaltdb")
	ok, _, _ := client.readOKOrError()
	if !ok {
		t.Fatal("Auth failed")
	}

	// 2. MySQL client init queries
	initQueries := []struct {
		sql      string
		isSelect bool
	}{
		{"SET NAMES utf8", false},
		{"SET character_set_results = utf8", false},
		{"SELECT @@version_comment", true},
		{"SELECT @@max_allowed_packet", true},
	}

	for _, q := range initQueries {
		client.sendQuery(q.sql)
		if q.isSelect {
			_, _, err := client.readResultSet()
			if err != nil {
				t.Fatalf("Init query '%s' failed: %v", q.sql, err)
			}
		} else {
			ok, msg, _ := client.readOKOrError()
			if !ok {
				t.Fatalf("Init query '%s' failed: %s", q.sql, msg)
			}
		}
	}

	// 3. DDL
	client.sendQuery("CREATE TABLE employees (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, department TEXT, salary REAL CHECK (salary > 0))")
	ok, _, _ = client.readOKOrError()
	if !ok {
		t.Fatal("CREATE TABLE failed")
	}

	// 4. SHOW TABLES
	client.sendQuery("SHOW TABLES")
	_, showRows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}
	if len(showRows) < 1 {
		t.Fatal("Expected at least 1 table in SHOW TABLES")
	}

	// 5. Insert data
	inserts := []string{
		"INSERT INTO employees (name, department, salary) VALUES ('Alice', 'Engineering', 95000)",
		"INSERT INTO employees (name, department, salary) VALUES ('Bob', 'Marketing', 75000)",
		"INSERT INTO employees (name, department, salary) VALUES ('Carol', 'Engineering', 105000)",
		"INSERT INTO employees (name, department, salary) VALUES ('Dave', 'Sales', 65000)",
		"INSERT INTO employees (name, department, salary) VALUES ('Eve', 'Engineering', 115000)",
	}

	for _, sql := range inserts {
		client.sendQuery(sql)
		ok, msg, _ := client.readOKOrError()
		if !ok {
			t.Fatalf("INSERT failed: %s", msg)
		}
	}

	// 6. Complex queries
	client.sendQuery("SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department ORDER BY department")
	cols, rows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("GROUP BY query failed: %v", err)
	}
	t.Logf("GROUP BY result: %d columns, %d rows", len(cols), len(rows))
	for _, row := range rows {
		t.Logf("  %v", row)
	}

	// 7. DESCRIBE
	client.sendQuery("DESCRIBE employees")
	descCols, descRows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("DESCRIBE failed: %v", err)
	}
	t.Logf("DESCRIBE: %d columns, %d rows", len(descCols), len(descRows))
	if len(descRows) != 4 { // id, name, department, salary
		t.Fatalf("Expected 4 columns in DESCRIBE, got %d", len(descRows))
	}

	// 8. SHOW CREATE TABLE
	client.sendQuery("SHOW CREATE TABLE employees")
	_, createRows, err := client.readResultSet()
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}
	if len(createRows) < 1 {
		t.Fatal("Expected result from SHOW CREATE TABLE")
	}
	t.Logf("CREATE TABLE: %s", createRows[0][1])

	// 9. Quit
	client.sendQuit()
	t.Log("Full MySQL client workflow completed successfully")
}
