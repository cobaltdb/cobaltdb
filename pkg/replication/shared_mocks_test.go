package replication

import (
	"errors"
	"io"
	"net"
	"time"
)

// mockConn is a mock net.Conn for testing
type mockConn struct {
	net.Conn
	writeData []byte
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return 0, io.EOF
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	return nil
}

//lint:ignore U1000 retained for replication close-error coverage tests.
type closeErrConn struct {
	mockConn
	err error
}

//lint:ignore U1000 retained for replication close-error coverage tests.
func (c *closeErrConn) Close() error {
	return c.err
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// mockWriter is a writer that fails after a certain number of writes
type mockWriter struct {
	writeCount int
	failAfter  int
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	if m.writeCount >= m.failAfter {
		return 0, errors.New("mock write error")
	}
	m.writeCount++
	return len(p), nil
}
