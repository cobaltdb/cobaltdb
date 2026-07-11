package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

type protocolFaultConn struct {
	readData         []byte
	writes           bytes.Buffer
	readErr          error
	writeErr         error
	shortWrite       bool
	readDeadlineErr  error
	writeDeadlineErr error
	closeErr         error
}

func (c *protocolFaultConn) Read(p []byte) (int, error) {
	if len(c.readData) > 0 {
		n := copy(p, c.readData)
		c.readData = c.readData[n:]
		return n, nil
	}
	if c.readErr != nil {
		return 0, c.readErr
	}
	return 0, io.EOF
}
func (c *protocolFaultConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.shortWrite && len(p) > 0 {
		return len(p) - 1, nil
	}
	return c.writes.Write(p)
}
func (c *protocolFaultConn) Close() error                     { return c.closeErr }
func (c *protocolFaultConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *protocolFaultConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *protocolFaultConn) SetDeadline(time.Time) error      { return nil }
func (c *protocolFaultConn) SetReadDeadline(time.Time) error  { return c.readDeadlineErr }
func (c *protocolFaultConn) SetWriteDeadline(time.Time) error { return c.writeDeadlineErr }

func protocolClient(conn net.Conn) *MySQLClient {
	return &MySQLClient{conn: conn, reader: bufio.NewReader(conn), server: NewMySQLServer(&engine.DB{}, "test")}
}

func commandPacket(cmd byte, data []byte) []byte {
	n := 1 + len(data)
	return append([]byte{byte(n), byte(n >> 8), byte(n >> 16), 0, cmd}, data...)
}

func TestProtocolRemainingScalarBranches(t *testing.T) {
	if _, err := quoteMySQLIdentifier("has_wildcard"); err == nil {
		t.Fatal("wildcard accepted")
	}
	if _, err := mysqlUint16(-1, "x"); err == nil {
		t.Fatal("negative accepted")
	}
	var nilServer *MySQLServer
	if nilServer.LastPanicRecovery() != nil {
		t.Fatal("nil recovery")
	}
	nilServer.recordPanic(1, "ignored")
	if enabled, clear := nilServer.authTransportConfig(); enabled || clear {
		t.Fatal("nil config")
	}
	if a, enabled := nilServer.authSnapshot(); a != nil || enabled {
		t.Fatal("nil snapshot")
	}
	if !isLoopbackMySQLListenAddress("127.0.0.1") {
		t.Fatal("bare loopback rejected")
	}

	s := NewMySQLServer(nil, "")
	s.recordPanic(7, "boom")
	if got := s.LastPanicRecovery(); got == nil || got.ConnID != 7 || got.Value != "boom" || got.Stack == "" || got.Timestamp.IsZero() {
		t.Fatalf("bad recovery: %+v", got)
	}
}

func TestProtocolColumnAndBinaryTables(t *testing.T) {
	types := []string{"BOOL", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "DATE", "DATETIME", "TIMESTAMP", "TIME", "JSON", "BLOB", "TEXT"}
	for _, typ := range types {
		if got, _, _, _, _ := mysqlColumnTypeForSQL(typ + " (10)"); got == MySQLTypeNull {
			t.Fatalf("%s mapped null", typ)
		}
	}
	row := []interface{}{"id", "BIGINT", "NO", "UNI", nil, "auto_increment"}
	def := mysqlColumnDefinitionFromDescribe("t", row)
	if def.flags&(mysqlColumnFlagNotNull|mysqlColumnFlagUniqueKey|mysqlColumnFlagAutoIncrement) == 0 {
		t.Fatalf("flags missing: %x", def.flags)
	}
	row[3] = "PRI"
	if mysqlColumnDefinitionFromDescribe("t", row).flags&mysqlColumnFlagPriKey == 0 {
		t.Fatal("primary flag missing")
	}
	if mysqlValueString(row, -1) != "" || mysqlValueString(row, 4) != "" {
		t.Fatal("invalid/nil value")
	}

	intCases := []interface{}{int64(1), int(2), int32(3), int16(4), int8(5), uint64(6), uint(7), uint32(8), uint16(9), uint8(10), true, false, float64(11), float32(12), "bad"}
	for _, v := range intCases {
		_, _ = binaryInt64(v)
	}
	encCases := []struct {
		typ byte
		val interface{}
	}{{MySQLTypeTiny, int8(-1)}, {MySQLTypeShort, int16(-2)}, {MySQLTypeLong, int32(-3)}, {MySQLTypeLongLong, int64(-4)}, {MySQLTypeFloat, float32(1.5)}, {MySQLTypeDouble, 2.5}, {MySQLTypeVarString, "x"}}
	for _, tc := range encCases {
		if len(appendBinaryValue(nil, tc.val, tc.typ)) == 0 {
			t.Fatalf("empty encoding %x", tc.typ)
		}
	}

	c := protocolClient(newMockConn())
	defs := c.buildBinaryColumnDefinitions([]string{"d", "plain"}, []string{"DATE"})
	if defs[0].typ != MySQLTypeVarString || defs[1].typ != MySQLTypeVarString {
		t.Fatalf("bad defs: %+v", defs)
	}
	pkt := c.buildBinaryRowPacket([]interface{}{nil, "x"}, nil)
	if len(pkt) < 3 || pkt[1]&(1<<2) == 0 {
		t.Fatalf("bad null bitmap: %v", pkt)
	}
	if mysqlWireValueLen(nil) != 0 || mysqlWireValueLen([]byte("xx")) != 2 {
		t.Fatal("wire lengths")
	}
}

func TestProtocolPreparedValueDecodingAllTypes(t *testing.T) {
	good := []struct {
		typ      byte
		unsigned bool
		data     []byte
	}{
		{MySQLTypeNull, false, nil}, {MySQLTypeTiny, true, []byte{255}}, {MySQLTypeTiny, false, []byte{255}},
		{MySQLTypeShort, true, []byte{1, 2}}, {MySQLTypeShort, false, []byte{1, 2}},
		{MySQLTypeLong, true, []byte{1, 2, 3, 4}}, {MySQLTypeLong, false, []byte{1, 2, 3, 4}},
		{MySQLTypeLongLong, true, make([]byte, 8)}, {MySQLTypeLongLong, false, make([]byte, 8)},
		{MySQLTypeFloat, false, binary.LittleEndian.AppendUint32(nil, math.Float32bits(1.25))},
		{MySQLTypeDouble, false, binary.LittleEndian.AppendUint64(nil, math.Float64bits(2.5))},
		{MySQLTypeString, false, []byte{1, 'x'}}, {MySQLTypeDate, false, []byte{4, 0xe8, 0x07, 2, 3}},
		{MySQLTypeDateTime, false, []byte{7, 0xe8, 0x07, 2, 3, 4, 5, 6}},
		{MySQLTypeTime, false, []byte{8, 1, 1, 0, 0, 0, 2, 3, 4}},
	}
	for _, tc := range good {
		if _, _, err := readStmtExecuteValue(tc.data, 0, tc.typ, tc.unsigned); err != nil {
			t.Fatalf("type %x: %v", tc.typ, err)
		}
	}
	badTypes := []byte{MySQLTypeTiny, MySQLTypeShort, MySQLTypeLong, MySQLTypeLongLong, MySQLTypeFloat, MySQLTypeDouble, MySQLTypeString, MySQLTypeDate, MySQLTypeDateTime, MySQLTypeTime, 0xaa}
	for _, typ := range badTypes {
		if _, _, err := readStmtExecuteValue(nil, 0, typ, false); err == nil {
			t.Fatalf("type %x accepted", typ)
		}
	}

	if _, _, err := readStmtExecuteString([]byte{5, 'x'}); err == nil {
		t.Fatal("long string")
	}
	for _, data := range [][]byte{nil, {3, 1}, {5, 1, 2, 3, 4, 5}} {
		_, _, _ = readStmtExecuteDate(data)
	}
	for _, data := range [][]byte{nil, {3}, {4, 0xe8, 7, 1, 2}, {11, 0xe8, 7, 1}} {
		_, _, _ = readStmtExecuteDateTime(data)
	}
	dt11 := []byte{11, 0xe8, 7, 1, 2, 3, 4, 5, 1, 0, 0, 0}
	if _, _, err := readStmtExecuteDateTime(dt11); err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{nil, {3}, {8, 0}, {12, 0, 0}} {
		_, _, _ = readStmtExecuteTime(data)
	}
	tm12 := []byte{12, 0, 0, 0, 0, 0, 1, 2, 3, 1, 0, 0, 0}
	if _, _, err := readStmtExecuteTime(tm12); err != nil {
		t.Fatal(err)
	}
}

func TestProtocolPreparedStatementEdges(t *testing.T) {
	for _, sql := range []string{"SELECT ?", "'unterminated ? \\? ?", "\"unterminated ?", "`unterminated ?"} {
		_ = countPreparedParams(sql)
	}
	s := &preparedStmt{numParams: 1}
	if _, err := s.parseExecuteArgs(nil); err == nil {
		t.Fatal("short execute")
	}
	data := make([]byte, 11)
	data[10] = 0
	if _, err := s.parseExecuteArgs(data); err == nil {
		t.Fatal("missing types")
	}
	data[10] = 1
	if _, err := s.parseExecuteArgs(data); err == nil {
		t.Fatal("truncated types")
	}

	conn := newMockConn()
	c := protocolClient(conn)
	_ = c.handleStmtFetch(nil)
	_ = c.handleStmtFetch(append(make([]byte, 4), make([]byte, 4)...))
	c.stmts = map[uint32]*preparedStmt{1: {id: 1, numParams: 1}}
	_ = c.handleStmtFetch(append([]byte{1, 0, 0, 0}, []byte{0, 0, 0, 0}...))
	_ = c.handleStmtSendLongData(nil)
	_ = c.handleStmtSendLongData([]byte{9, 0, 0, 0, 0, 0})
	_ = c.handleStmtSendLongData([]byte{1, 0, 0, 0, 1, 0})
	c.longDataTotal = maxConnLongDataBytes
	_ = c.handleStmtSendLongData([]byte{1, 0, 0, 0, 0, 0, 'x'})
	c.longDataTotal = 0
	c.stmts[1].longData = map[int][]byte{0: make([]byte, maxMySQLLongDataBytes)}
	_ = c.handleStmtSendLongData([]byte{1, 0, 0, 0, 0, 0, 'x'})
	_ = c.handleStmtReset(nil)
	_ = c.handleStmtReset([]byte{9, 0, 0, 0})
	_ = c.handleStmtClose(nil)
}

func TestProtocolCommandDispatchAndReadErrors(t *testing.T) {
	commands := []struct {
		cmd  byte
		data []byte
	}{
		{MySQLComQuit, nil}, {MySQLComPing, nil}, {MySQLComInitDB, []byte("db")}, {MySQLComRefresh, nil},
		{MySQLComShutdown, nil}, {0xff, nil}, {MySQLComStmtClose, nil}, {MySQLComStmtReset, nil},
		{MySQLComStmtExecute, nil}, {MySQLComStmtSendLongData, nil}, {MySQLComStmtFetch, nil},
		{MySQLComStatistics, nil}, {MySQLComResetConnection, nil},
	}
	for _, tc := range commands {
		fc := &protocolFaultConn{readData: commandPacket(tc.cmd, tc.data)}
		_ = protocolClient(fc).handleCommand()
	}
	fc := &protocolFaultConn{readDeadlineErr: errors.New("deadline")}
	if err := protocolClient(fc).handleCommand(); err == nil {
		t.Fatal("deadline error lost")
	}
	for _, raw := range [][]byte{{0, 0, 0, 0}, {1, 0, 0, 0}, {2, 0, 0, 0, MySQLComPing}, {2, 0, 0, 0, MySQLComPing, 1}} {
		c := protocolClient(&protocolFaultConn{readData: raw})
		_, _, _ = c.readCommandPacket()
	}
	for _, cmd := range []byte{MySQLComQuery, MySQLComInitDB, MySQLComStmtSendLongData, MySQLComStmtExecute, MySQLComFieldList, MySQLComQuit, MySQLComStmtClose, MySQLComStmtFetch, 0xff} {
		if maxMySQLCommandPayloadFor(cmd) <= 0 {
			t.Fatal("bad max")
		}
	}
}

func TestProtocolWriteAndHandlerFailures(t *testing.T) {
	for _, fc := range []*protocolFaultConn{{writeDeadlineErr: errors.New("deadline")}, {writeErr: errors.New("write")}, {shortWrite: true}} {
		if err := protocolClient(fc).writePacket([]byte("x"), 0); err == nil {
			t.Fatal("write failure lost")
		}
	}
	if _, err := writeMySQLFull(&protocolFaultConn{writeErr: errors.New("x")}, []byte("x")); err == nil {
		t.Fatal("writer error")
	}
	if _, err := writeMySQLFull(&protocolFaultConn{shortWrite: true}, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write: %v", err)
	}

	fc := &protocolFaultConn{writeErr: errors.New("x")}
	c := protocolClient(fc)
	_ = c.handleSelectVariable("SELECT @@VERSION")
	_ = c.sendResultSetFromRows(nil)
	_ = c.sendBinaryResultSetFromRows(nil)
	_ = c.openStmtCursor(&preparedStmt{}, nil)
	_ = c.handleProcessInfo()
	_ = c.handleStatistics()
	_ = c.handleFieldList([]byte("bad_name"))
}

func TestProtocolHandshakeMalformedFields(t *testing.T) {
	makePacket := func(payload []byte) []byte {
		n := len(payload)
		return append([]byte{byte(n), byte(n >> 8), byte(n >> 16), 1}, payload...)
	}
	cases := [][]byte{}
	p := make([]byte, 32)
	p = append(p, bytes.Repeat([]byte{'u'}, maxMySQLUsernameBytes+1)...)
	p = append(p, 0)
	cases = append(cases, makePacket(p))
	p = make([]byte, 32)
	p = append(p, 'u', 0, maxMySQLAuthResponseBytes+1)
	cases = append(cases, makePacket(p))
	p = make([]byte, 32)
	p = append(p, 'u', 0, 0)
	p = append(p, bytes.Repeat([]byte{'d'}, maxMySQLDatabaseBytes+1)...)
	cases = append(cases, makePacket(p))
	p = append(make([]byte, 32), 'u')
	cases = append(cases, makePacket(p))
	for _, raw := range cases {
		c := protocolClient(&protocolFaultConn{readData: raw})
		if err := c.readHandshakeResponse(); err == nil {
			t.Fatal("malformed handshake accepted")
		}
	}
}

func TestProtocolConnectionLimitsAndCloseErrors(t *testing.T) {
	s := NewMySQLServer(nil, "")
	s.maxConnections = 1
	s.clients[1] = &protocolFaultConn{}
	conn := &protocolFaultConn{}
	s.handleConnection(conn)
	if !errors.Is(s.Close(), nil) {
		t.Fatal("unexpected close")
	}

	s = NewMySQLServer(nil, "")
	s.clients[1] = &protocolFaultConn{closeErr: errors.New("close")}
	if err := s.Close(); err == nil || !strings.Contains(err.Error(), "close client") {
		t.Fatalf("close error: %v", err)
	}
}
