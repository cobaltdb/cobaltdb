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

type shortReplicationWriter struct {
	limit int
}

func (w *shortReplicationWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		return w.limit, nil
	}
	return len(p), nil
}

type shortReplicationConn struct {
	mockConn
	limit int
}

func (c *shortReplicationConn) Write(p []byte) (int, error) {
	if len(p) > c.limit {
		return c.limit, nil
	}
	return len(p), nil
}

// mockErrorReader returns the configured error from every Read call.
type mockErrorReader struct {
	err error
}

func (e *mockErrorReader) Read(p []byte) (int, error) {
	return 0, e.err
}

// mockErrorWriter returns the configured error from every Write call.
type mockErrorWriter struct {
	err error
}

func (e *mockErrorWriter) Write(p []byte) (int, error) {
	return 0, e.err
}
