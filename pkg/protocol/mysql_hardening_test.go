package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

type protocolTestAddr string

func (a protocolTestAddr) Network() string { return "test" }
func (a protocolTestAddr) String() string  { return string(a) }

type deadlineErrConn struct {
	readDeadlineErr  error
	writeDeadlineErr error
	closeErr         error
}

func (c *deadlineErrConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *deadlineErrConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *deadlineErrConn) Close() error                     { return c.closeErr }
func (c *deadlineErrConn) LocalAddr() net.Addr              { return protocolTestAddr("local") }
func (c *deadlineErrConn) RemoteAddr() net.Addr             { return protocolTestAddr("remote") }
func (c *deadlineErrConn) SetDeadline(time.Time) error      { return nil }
func (c *deadlineErrConn) SetReadDeadline(time.Time) error  { return c.readDeadlineErr }
func (c *deadlineErrConn) SetWriteDeadline(time.Time) error { return c.writeDeadlineErr }

type panicWriteConn struct {
	deadlineErrConn
}

func (c *panicWriteConn) Write([]byte) (int, error) {
	panic("write panic")
}

type shortWriteConn struct {
	deadlineErrConn
	limit int
}

func (c *shortWriteConn) Write(p []byte) (int, error) {
	if len(p) > c.limit {
		return c.limit, nil
	}
	return len(p), nil
}

func TestMySQLServerCloseReturnsClientCloseErrors(t *testing.T) {
	closeErr := errors.New("close failed")
	server := NewMySQLServer(nil, "test")
	server.clients[7] = &deadlineErrConn{closeErr: closeErr}

	err := server.Close()
	if !errors.Is(err, closeErr) {
		t.Fatalf("expected close error, got %v", err)
	}
}

func TestMySQLServerCloseIgnoresBenignNetworkCloseErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "wrapped net err closed",
			err:  net.ErrClosed,
		},
		{
			name: "tcp already closed message",
			err:  errors.New("close tcp 127.0.0.1:1->127.0.0.1:2: use of closed network connection"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewMySQLServer(nil, "test")
			server.clients[7] = &deadlineErrConn{closeErr: tt.err}

			if err := server.Close(); err != nil {
				t.Fatalf("expected benign close error to be ignored, got %v", err)
			}
		})
	}
}

func TestMySQLClientHandleCommandReturnsDeadlineError(t *testing.T) {
	deadlineErr := errors.New("read deadline failed")
	client := &MySQLClient{
		conn:   &deadlineErrConn{readDeadlineErr: deadlineErr},
		reader: bufio.NewReader(bytes.NewReader(nil)),
	}

	err := client.handleCommand()
	if !errors.Is(err, deadlineErr) {
		t.Fatalf("expected read deadline error, got %v", err)
	}
}

func TestMySQLClientWritePacketReturnsDeadlineError(t *testing.T) {
	deadlineErr := errors.New("write deadline failed")
	client := &MySQLClient{conn: &deadlineErrConn{writeDeadlineErr: deadlineErr}}

	err := client.writePacket([]byte("payload"), 0)
	if !errors.Is(err, deadlineErr) {
		t.Fatalf("expected write deadline error, got %v", err)
	}
}

func TestMySQLClientWritePacketRejectsNilConnection(t *testing.T) {
	client := &MySQLClient{}

	err := client.writePacket([]byte("payload"), 0)
	if err == nil || !strings.Contains(err.Error(), "connection is nil") {
		t.Fatalf("expected nil connection error, got %v", err)
	}
}

func TestMySQLClientWritePacketRejectsShortHeaderWrite(t *testing.T) {
	client := &MySQLClient{conn: &shortWriteConn{limit: 3}}

	err := client.writePacket([]byte("payload"), 0)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writePacket short header write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestMySQLClientWritePacketRejectsShortPayloadWrite(t *testing.T) {
	client := &MySQLClient{conn: &shortWriteConn{limit: 4}}

	err := client.writePacket([]byte("payload"), 0)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writePacket short payload write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestWriteMySQLFullRejectsShortWrite(t *testing.T) {
	writer := &shortWriteConn{limit: 2}

	n, err := writeMySQLFull(writer, []byte("abcd"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeMySQLFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 2 {
		t.Fatalf("writeMySQLFull wrote %d bytes, want 2", n)
	}
}

func TestMySQLClientWritePacketExactMaxPayloadWritesTerminator(t *testing.T) {
	conn := newMockConn()
	client := &MySQLClient{conn: conn}
	payload := make([]byte, maxMySQLPayloadSize)

	if err := client.writePacket(payload, 7); err != nil {
		t.Fatalf("writePacket exact max payload failed: %v", err)
	}

	got := conn.writeBuf.Bytes()
	wantLen := 4 + maxMySQLPayloadSize + 4
	if len(got) != wantLen {
		t.Fatalf("packet bytes = %d, want %d", len(got), wantLen)
	}
	firstLen := int(got[0]) | int(got[1])<<8 | int(got[2])<<16
	if firstLen != maxMySQLPayloadSize {
		t.Fatalf("first packet length = %d, want %d", firstLen, maxMySQLPayloadSize)
	}
	if got[3] != 7 {
		t.Fatalf("first packet sequence = %d, want 7", got[3])
	}

	term := got[4+maxMySQLPayloadSize:]
	if term[0] != 0 || term[1] != 0 || term[2] != 0 {
		t.Fatalf("terminator length bytes = %v, want zero length", term[:3])
	}
	if term[3] != 8 {
		t.Fatalf("terminator sequence = %d, want 8", term[3])
	}
}

func TestMySQLClientReadCommandPacketConsumesExactMaxTerminator(t *testing.T) {
	var input bytes.Buffer
	payload := make([]byte, maxMySQLPayloadSize)
	payload[0] = MySQLComStmtSendLongData
	for i := 1; i < len(payload); i++ {
		payload[i] = byte(i)
	}

	input.Write([]byte{0xff, 0xff, 0xff, 0})
	input.Write(payload)
	input.Write([]byte{0, 0, 0, 1})

	client := &MySQLClient{reader: bufio.NewReader(&input)}
	command, data, err := client.readCommandPacket()
	if err != nil {
		t.Fatalf("readCommandPacket exact max payload failed: %v", err)
	}
	if command != MySQLComStmtSendLongData {
		t.Fatalf("command = 0x%02x, want 0x%02x", command, MySQLComStmtSendLongData)
	}
	if len(data) != maxMySQLPayloadSize-1 {
		t.Fatalf("data length = %d, want %d", len(data), maxMySQLPayloadSize-1)
	}
	if input.Len() != 0 {
		t.Fatalf("terminator was not consumed, %d bytes remain", input.Len())
	}
}

func TestMySQLServerRecordsRecoveredClientPanics(t *testing.T) {
	server := NewMySQLServer(nil, "test")

	server.handleConnection(&panicWriteConn{})

	recovery := server.LastPanicRecovery()
	if recovery == nil {
		t.Fatal("expected recovered panic to be recorded")
	}
	if recovery.ConnID == 0 {
		t.Fatalf("expected recovered panic conn id, got %d", recovery.ConnID)
	}
	if recovery.Value != "write panic" {
		t.Fatalf("expected write panic value, got %v", recovery.Value)
	}
}
