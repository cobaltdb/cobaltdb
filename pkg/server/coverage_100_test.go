package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

type coverageAddr string

func (a coverageAddr) Network() string { return "test" }
func (a coverageAddr) String() string  { return string(a) }

type coverageListener struct {
	conn                net.Conn
	acceptErr, closeErr error
	addr                net.Addr
}

func (l *coverageListener) Accept() (net.Conn, error) {
	if l.conn != nil {
		c := l.conn
		l.conn = nil
		return c, nil
	}
	return nil, l.acceptErr
}
func (l *coverageListener) Close() error { return l.closeErr }
func (l *coverageListener) Addr() net.Addr {
	if l.addr != nil {
		return l.addr
	}
	return coverageAddr("127.0.0.1:0")
}

type coverageConn struct {
	readData                                 []byte
	writes                                   []byte
	readErr, writeErr, closeErr, deadlineErr error
	short                                    bool
}

func (c *coverageConn) Read(p []byte) (int, error) {
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
func (c *coverageConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	n := len(p)
	if c.short && n > 0 {
		n--
	}
	c.writes = append(c.writes, p[:n]...)
	return n, nil
}
func (c *coverageConn) Close() error                     { return c.closeErr }
func (c *coverageConn) LocalAddr() net.Addr              { return coverageAddr("127.0.0.1:1") }
func (c *coverageConn) RemoteAddr() net.Addr             { return coverageAddr("127.0.0.1:2") }
func (c *coverageConn) SetDeadline(time.Time) error      { return c.deadlineErr }
func (c *coverageConn) SetReadDeadline(time.Time) error  { return c.deadlineErr }
func (c *coverageConn) SetWriteDeadline(time.Time) error { return c.deadlineErr }

type coverageHTTPWriter struct{ h http.Header }

func (w *coverageHTTPWriter) Header() http.Header {
	if w.h == nil {
		w.h = make(http.Header)
	}
	return w.h
}
func (*coverageHTTPWriter) WriteHeader(int)           {}
func (*coverageHTTPWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type timeoutStartComponent struct{}

func (timeoutStartComponent) Name() string                    { return "timeout" }
func (timeoutStartComponent) Start(ctx context.Context) error { <-ctx.Done(); return nil }
func (timeoutStartComponent) Stop(context.Context) error      { return nil }
func (timeoutStartComponent) Health() HealthStatus            { return HealthStatus{Healthy: true} }

func TestCoverageServerValidationAndWireValues(t *testing.T) {
	if _, err := messagePacketLength(-1); err == nil {
		t.Fatal("negative payload accepted")
	}
	if _, err := messagePacketLength(maxPayloadBytes); err == nil {
		t.Fatal("large payload accepted")
	}
	if _, err := New(nil, &Config{AuthEnabled: true, DefaultAdminUser: strings.Repeat("u", 1025), DefaultAdminPass: "x"}); err == nil {
		t.Fatal("invalid user accepted")
	}
	params := []interface{}{nil, true, int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1), "x", []byte("x")}
	if validateWireParams(params) != nil {
		t.Fatal("valid params rejected")
	}
	if validateWireParams([]interface{}{make([]byte, maxWireParamBytes+1)}) == nil {
		t.Fatal("large bytes accepted")
	}
	if maxWireInboundPayloadFor(wire.MsgPing) != 0 || maxWireInboundPayloadFor(wire.MsgAuth) != maxWireAuthPayloadBytes || maxWireInboundPayloadFor(wire.MsgQuery) != maxWireInboundPayloadBytes || maxWireInboundPayloadFor(255) != 0 {
		t.Fatal("payload limits")
	}
	if isTLSListener(nil) {
		t.Fatal("nil is TLS")
	}
	for _, v := range []interface{}{nil, "abc", []byte("abc"), []interface{}{"x", []byte("y")}, map[string]interface{}{"k": "v"}, map[interface{}]interface{}{"k": "v"}, 42} {
		if wireResultValueSize(v) < 0 {
			t.Fatal(v)
		}
	}
	big := strings.Repeat("x", maxWireResultValueBytes+1)
	if !wireResultRowValueTooLarge([]interface{}{[]interface{}{big}}) || !wireResultRowValueTooLarge([]interface{}{map[string]interface{}{"k": big}}) || !wireResultRowValueTooLarge([]interface{}{map[interface{}]interface{}{"k": big}}) {
		t.Fatal("nested result limit")
	}
	if got := sanitizeError(errors.New(`bad C:\secret`)); !strings.Contains(got, "internal error") {
		t.Fatal(got)
	}
	if got := sanitizeError(errors.New(`bad D:\secret`)); !strings.Contains(got, "internal error") {
		t.Fatal(got)
	}
}

func TestCoverageServerListenerAndConnectionErrors(t *testing.T) {
	log := logger.New(logger.InfoLevel, io.Discard)
	s, _ := New(nil, &Config{Logger: log, AllowCleartextAuth: true})
	s.logWarnf("warn")
	s.logErrorf("err")
	if err := s.Listen("invalid::address", nil); err == nil {
		t.Fatal("invalid listen succeeded")
	}
	badTLS := &TLSConfig{Enabled: true, CertFile: "missing", KeyFile: "missing"}
	if err := s.Listen("127.0.0.1:0", badTLS); err == nil {
		t.Fatal("bad TLS succeeded")
	}

	s2, _ := New(nil, nil)
	s2.closed = true
	if err := s2.ListenOnListener(&coverageListener{closeErr: errors.New("close")}); err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("closed listener: %v", err)
	}
	s3, _ := New(nil, &Config{AuthEnabled: true, DefaultAdminUser: "a", DefaultAdminPass: "password123"})
	if err := s3.ListenOnListener(&coverageListener{addr: coverageAddr("10.0.0.1:1"), closeErr: errors.New("close")}); err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("transport close: %v", err)
	}

	if err := s.acceptLoop(); err != nil {
		t.Fatal(err)
	}
	s.listener = &coverageListener{acceptErr: errors.New("accept")}
	if err := s.acceptLoop(); err == nil {
		t.Fatal("accept error lost")
	}

	conn := &coverageConn{}
	s4, _ := New(nil, &Config{MaxConnections: 1})
	s4.clients[1] = &ClientConn{ID: 1, Conn: &coverageConn{}}
	s4.listener = &coverageListener{conn: conn, acceptErr: net.ErrClosed}
	if err := s4.acceptLoop(); err == nil {
		t.Fatal("expected terminal accept error")
	}
	if len(conn.writes) == 0 {
		t.Fatal("max connection rejection not written")
	}

	closeErr := errors.New("close failed")
	s5, _ := New(nil, nil)
	s5.clients[1] = &ClientConn{ID: 1, Conn: &coverageConn{closeErr: closeErr}}
	s5.listener = &coverageListener{closeErr: closeErr}
	if err := s5.Close(); err == nil || !strings.Contains(err.Error(), "close client") {
		t.Fatalf("close errors: %v", err)
	}

	for _, c := range []*coverageConn{{deadlineErr: errors.New("deadline")}, {readData: []byte{1, 0, 0, 0}, readErr: errors.New("type")}, {readData: []byte{2, 0, 0, 0, byte(wire.MsgPing)}, readErr: errors.New("payload")}, {readData: []byte{1, 0, 0, 0, byte(wire.MsgPing)}, deadlineErr: errors.New("write deadline")}} {
		srv, _ := New(nil, nil)
		client := &ClientConn{ID: 1, Conn: c, Server: srv, reader: bufio.NewReader(c), authed: true}
		srv.clients[1] = client
		client.Handle()
	}
}

func TestCoverageSendMessageErrors(t *testing.T) {
	s, _ := New(nil, nil)
	for _, tc := range []struct {
		conn *coverageConn
		msg  interface{}
	}{
		{&coverageConn{deadlineErr: errors.New("deadline")}, wire.MsgPong},
		{&coverageConn{writeErr: errors.New("write")}, wire.NewOKMessage(0, 0)},
		{&coverageConn{short: true}, wire.NewAuthSuccessMessage("x")},
	} {
		c := &ClientConn{Conn: tc.conn, Server: s}
		if err := c.sendMessage(tc.msg); err == nil {
			t.Fatalf("expected send error for %+v", tc.conn)
		}
	}
	if _, err := writeServerFull(&coverageConn{writeErr: errors.New("write")}, []byte("x")); err == nil {
		t.Fatal("write error lost")
	}
}

func TestCoverageLifecycleEdges(t *testing.T) {
	if cloneShutdownSignals(nil) != nil {
		t.Fatal("nil clone")
	}
	log := logger.New(logger.InfoLevel, io.Discard)
	l := NewLifecycle(&LifecycleConfig{Logger: log, StartupTimeout: time.Nanosecond, DrainTimeout: time.Nanosecond, ShutdownTimeout: time.Millisecond, HealthCheckInterval: time.Millisecond})
	l.logInfof("x")
	l.logWarnf("x")
	l.logErrorf("x")
	l.RegisterComponent(timeoutStartComponent{})
	time.Sleep(time.Millisecond)
	if err := l.Start(); err == nil || !strings.Contains(err.Error(), "startup timeout") {
		t.Fatalf("timeout start: %v", err)
	}

	cleanup := NewLifecycle(&LifecycleConfig{StartupTimeout: time.Second, DrainTimeout: time.Nanosecond, ShutdownTimeout: time.Second, HealthCheckInterval: time.Second})
	cleanup.RegisterComponent(&MockComponent{name: "one", healthy: true, stopErr: errors.New("cleanup")})
	cleanup.RegisterComponent(&MockComponent{name: "two", healthy: true, startErr: errors.New("start")})
	if err := cleanup.Start(); err == nil || !strings.Contains(err.Error(), "cleanup failed") {
		t.Fatalf("cleanup: %v", err)
	}

	h := NewLifecycle(&LifecycleConfig{StartupTimeout: time.Second, DrainTimeout: time.Nanosecond, ShutdownTimeout: time.Second, HealthCheckInterval: time.Millisecond, Logger: log})
	h.setState(StateStopped)
	h.checkHealth()
	h.setState(StateRunning)
	h.RegisterComponent(&MockComponent{name: "bad", healthy: false, healthMessage: "bad"})
	h.checkHealth()
	h.OnStateChange(StateStarting, func() { panic("boom") })
	h.setState(StateStarting)
	h.hookWg.Wait()
	h.cancel()
	h.healthMonitor()

	fw := &coverageHTTPWriter{}
	stopping := NewLifecycle(&LifecycleConfig{DrainTimeout: time.Nanosecond, ShutdownTimeout: time.Second, HealthCheckInterval: time.Second, Logger: log})
	stopping.GracefulShutdownHandler()(fw, httptest.NewRequest(http.MethodPost, "/", nil))
	stopping.ReadyCheck()(fw, httptest.NewRequest(http.MethodGet, "/", nil))
	stopping.LiveCheck()(fw, httptest.NewRequest(http.MethodGet, "/", nil))
	stopping.setState(StateStopped)
	stopping.LiveCheck()(fw, httptest.NewRequest(http.MethodGet, "/", nil))

	nilDB := NewDBComponent("nil", nil)
	if nilDB.Start(context.Background()) == nil || nilDB.Health().Healthy {
		t.Fatal("nil DB healthy")
	}
}

func TestCoverageProductionHandlersAndBreaker(t *testing.T) {
	log := logger.New(logger.InfoLevel, io.Discard)
	ps := NewProductionServer(nil, &ProductionConfig{Logger: log, EnableHealthServer: false})
	ps.logErrorf("x")
	if tok, ok := adminTokenFromAuthorizationHeader("Bearer "); ok || tok != "" {
		t.Fatal("empty bearer")
	}
	ps.Lifecycle.RegisterComponent(&MockComponent{name: "fail", startErr: errors.New("start")})
	if err := ps.Start(); err == nil {
		t.Fatal("lifecycle failure lost")
	}

	for _, tc := range []struct {
		name   string
		h      http.HandlerFunc
		method string
	}{
		{"health", ps.healthHandler(), http.MethodGet}, {"ready", ps.readyHandler(), http.MethodGet}, {"stats", ps.statsHandler(), http.MethodGet}, {"cb", ps.circuitBreakerHandler(), http.MethodGet}, {"rate", ps.rateLimitsHandler(), http.MethodGet}, {"txn", ps.transactionMetricsHandler(), http.MethodGet},
	} {
		t.Run(tc.name+"WriteError", func(t *testing.T) { tc.h(&coverageHTTPWriter{}, httptest.NewRequest(tc.method, "/", nil)) })
	}

	for _, h := range []http.HandlerFunc{ps.healthHandler(), ps.readyHandler(), ps.healthzHandler(), ps.statsHandler(), ps.circuitBreakerHandler(), ps.rateLimitsHandler(), ps.transactionMetricsHandler()} {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodPost, "/", nil))
		if w.Code != 405 {
			t.Fatal(w.Code)
		}
	}

	noCB := NewProductionServer(nil, &ProductionConfig{})
	if err := noCB.executeWithClassifiedBreaker("x", func() error { return errors.New("x") }); err == nil {
		t.Fatal("direct breaker path")
	}
	cfg := engine.DefaultCircuitBreakerConfig()
	cfg.MaxFailures = 1
	withCB := NewProductionServer(nil, &ProductionConfig{EnableCircuitBreaker: true, CircuitBreaker: cfg})
	if err := withCB.executeWithClassifiedBreaker("x", func() error { return errors.New("storage unavailable") }); err == nil {
		t.Fatal("failure missing")
	}
	if err := withCB.executeWithClassifiedBreaker("x", func() error { return nil }); err == nil {
		t.Fatal("open breaker allowed")
	}
	if withCB.execWriteRetryable(nil) || withCB.execWriteRetryable(context.Canceled) || withCB.execWriteRetryable(context.DeadlineExceeded) {
		t.Fatal("unsafe retry")
	}
	marker := errors.New("explicit")
	withCB.Config.Retry = &engine.RetryConfig{RetryableErrors: []error{marker}}
	if !withCB.execWriteRetryable(marker) || !withCB.execWriteRetryable(errors.New("connection limit")) {
		t.Fatal("safe retry rejected")
	}
}

func TestCoverageRateLimiterEdges(t *testing.T) {
	log := logger.New(logger.InfoLevel, io.Discard)
	rl := NewRateLimiter(&RateLimiterConfig{RPS: 1, Burst: 1, PerClient: true, ClientHeader: "X", CleanupInterval: time.Hour, MaxClients: 1, Logger: log})
	defer rl.Stop()
	rl.logErrorf("x")
	defaults := normalizeRateLimiterConfig(&RateLimiterConfig{})
	if defaults.RPS <= 0 || defaults.Burst <= 0 || defaults.ClientHeader == "" || defaults.CleanupInterval <= 0 || defaults.MaxClients <= 0 {
		t.Fatal(defaults)
	}
	if !rl.AllowN("a", 1) || rl.AllowN("a", 1) {
		t.Fatal("AllowN budget")
	}
	if rl.AllowN("a", -1) || !rl.AllowN("a", 0) {
		t.Fatal("AllowN bounds")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rl.Wait(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait=%v", err)
	}
	rl.clientsMu.Lock()
	rl.clients["old"] = &clientLimiter{bucket: rl.global, lastAccess: time.Time{}}
	rl.clientsMu.Unlock()
	rl.cleanup()
	rl.global.mu.Lock()
	rl.global.tokens = 0
	rl.global.lastUpdate = time.Now()
	rl.global.mu.Unlock()
	if rl.global.allowN(2) {
		t.Fatal("allowed unavailable tokens")
	}
}

func TestCoverageSQLProtectorEdges(t *testing.T) {
	sp := NewSQLProtector(nil)
	sp.config.MaxUNIONCount = 1
	if r := sp.CheckSQL("SELECT 1 UNION SELECT 2 UNION SELECT 3"); len(r.Violations) == 0 {
		t.Fatal("union violation")
	}
	if r := sp.CheckSQL("SELECT /* broken"); len(r.Violations) == 0 {
		t.Fatal("comment violation")
	}
	sp.stats.ViolationsByType = nil
	sp.recordViolations([]Violation{{Type: "x"}})
	if countIgnoreCase("x", "") != 0 {
		t.Fatal("empty count")
	}
	if !strings.HasSuffix(SanitizeSQL(strings.Repeat("x", 501)), "...") {
		t.Fatal("sanitize cap")
	}
}

func TestCoverageTLSFilesystemAndCertificateEdges(t *testing.T) {
	if cloneCipherSuites(nil) != nil {
		t.Fatal("nil cipher clone")
	}
	if err := verifyCertificate(&tls.Certificate{Certificate: [][]byte{[]byte("bad")}}); err == nil {
		t.Fatal("bad DER accepted")
	}
	dir := t.TempDir()
	if err := prepareTLSFileDir(""); err == nil {
		t.Fatal("empty path accepted")
	}
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := prepareTLSFileDir(filepath.Join(file, "child")); err == nil {
		t.Fatal("file as dir accepted")
	}
	if _, err := cleanTLSFilePath(""); err == nil {
		t.Fatal("empty TLS path")
	}
	if _, err := readRegularTLSFile(filepath.Join(dir, "missing"), tlsCertFilePerm); err == nil {
		t.Fatal("missing read")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readRegularTLSFile(link, tlsCertFilePerm); err == nil {
		t.Fatal("symlink read")
	}
	if err := writeTLSFileAtomic(filepath.Join(link, "x"), []byte("x"), 0600); err == nil {
		t.Fatal("symlink parent write")
	}
	if err := syncTLSDir(file); err == nil {
		t.Fatal("sync regular file")
	}

	badCA := filepath.Join(dir, "bad.crt")
	badKey := filepath.Join(dir, "bad.key")
	if err := os.WriteFile(badCA, []byte("-----BEGIN CERTIFICATE-----\nbad\n-----END CERTIFICATE-----"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(badKey, []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := GenerateClientCert(badCA, badKey, "client", 1); err == nil {
		t.Fatal("bad CA accepted")
	}

	cert := &x509.Certificate{NotBefore: time.Now().Add(time.Hour), NotAfter: time.Now().Add(2 * time.Hour)}
	der := cert.Raw
	_ = der
}
