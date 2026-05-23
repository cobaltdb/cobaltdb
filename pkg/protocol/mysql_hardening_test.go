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

func TestMySQLServerCloseReturnsClientCloseErrors(t *testing.T) {
	closeErr := errors.New("close failed")
	server := NewMySQLServer(nil, "test")
	server.clients[7] = &deadlineErrConn{closeErr: closeErr}

	err := server.Close()
	if !errors.Is(err, closeErr) {
		t.Fatalf("expected close error, got %v", err)
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
