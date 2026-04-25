package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestAdminStartAlreadyStarted tests Start when already started
func TestAdminStartAlreadyStarted(t *testing.T) {
	admin := NewAdminServer(nil, "127.0.0.1:0")
	if err := admin.Start(); err != nil {
		t.Fatalf("First start failed: %v", err)
	}
	defer admin.Stop()

	if err := admin.Start(); err == nil {
		t.Error("Expected error for double start")
	}
}

// TestAcceptLoopClosed tests acceptLoop when server is already closed
func TestAcceptLoopClosed(t *testing.T) {
	srv := &Server{closed: true}
	if err := srv.acceptLoop(); err != nil {
		t.Errorf("Expected nil error for closed server, got %v", err)
	}
}

// TestAcceptLoopNilListener tests acceptLoop with nil listener
func TestAcceptLoopNilListener(t *testing.T) {
	srv := &Server{}
	if err := srv.acceptLoop(); err != nil {
		t.Errorf("Expected nil error for nil listener, got %v", err)
	}
}

// TestHandleInvalidLength tests Handle with invalid message lengths
func TestHandleInvalidLength(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, &Config{})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	t.Run("LengthZero", func(t *testing.T) {
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, uint32(0))
		client := &ClientConn{
			ID:     1,
			Server: srv,
			reader: bufio.NewReader(&buf),
			Conn:   &nopConn{},
		}
		client.Handle() // should return early on length < 1
	})

	t.Run("LengthTooLarge", func(t *testing.T) {
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, maxPayloadSize+1)
		client := &ClientConn{
			ID:     2,
			Server: srv,
			reader: bufio.NewReader(&buf),
			Conn:   &nopConn{},
		}
		client.Handle() // should return early on length > maxPayloadSize
	})
}

// nopAddr implements net.Addr
type nopAddr struct{}

func (n *nopAddr) Network() string { return "nop" }
func (n *nopAddr) String() string  { return "nop" }

// nopConn is a minimal net.Conn implementation for testing
type nopConn struct{}

func (n *nopConn) Read(b []byte) (int, error)   { return 0, errors.New("nop") }
func (n *nopConn) Write(b []byte) (int, error)  { return len(b), nil }
func (n *nopConn) Close() error                 { return nil }
func (n *nopConn) LocalAddr() net.Addr          { return &nopAddr{} }
func (n *nopConn) RemoteAddr() net.Addr         { return &nopAddr{} }
func (n *nopConn) SetDeadline(time.Time) error  { return nil }
func (n *nopConn) SetReadDeadline(time.Time) error  { return nil }
func (n *nopConn) SetWriteDeadline(time.Time) error { return nil }

// TestNewSQLProtectorNilConfig tests NewSQLProtector with nil config
func TestNewSQLProtectorNilConfig(t *testing.T) {
	sp := NewSQLProtector(nil)
	if sp == nil {
		t.Fatal("Expected protector")
	}
	if sp.config == nil {
		t.Fatal("Expected default config")
	}
}

// TestCheckSQLUnionCount tests CheckSQL with too many UNIONs
func TestCheckSQLUnionCount(t *testing.T) {
	config := &SQLProtectionConfig{
		Enabled:             true,
		MaxUNIONCount:       2,
		BlockOnDetection:    true,
		SuspiciousThreshold: 1,
	}
	sp := NewSQLProtector(config)

	query := "SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4"
	result := sp.CheckSQL(query)
	if result.Allowed {
		t.Error("Expected query to be blocked for too many UNIONs")
	}
}

// TestCheckSQLSuspiciousComments tests CheckSQL with suspicious comments
func TestCheckSQLSuspiciousComments(t *testing.T) {
	config := &SQLProtectionConfig{
		Enabled:             true,
		BlockOnDetection:    true,
		SuspiciousThreshold: 1,
	}
	sp := NewSQLProtector(config)

	query := "SELECT /*!32302 1*/ FROM users"
	result := sp.CheckSQL(query)
	if result.Allowed {
		t.Error("Expected query to be blocked for suspicious comments")
	}
}

// TestSanitizeSQLTruncation tests SanitizeSQL with long query
func TestSanitizeSQLTruncation(t *testing.T) {
	// Use unquoted content so truncation actually triggers
	longQuery := "SELECT " + strings.Repeat("a", 600) + " FROM users"
	sanitized := SanitizeSQL(longQuery)
	if len(sanitized) <= 500 {
		t.Error("Expected sanitized query to be truncated")
	}
	if !strings.HasSuffix(sanitized, "...") {
		t.Error("Expected truncation suffix")
	}
}

// TestLifecycleCheckHealthNonRunning tests checkHealth when not running
func TestLifecycleCheckHealthNonRunning(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})
	l.setState(StateStopped)
	l.checkHealth() // should return early without panic
}

// TestLifecycleSetStateHookPanic tests panic recovery in setState hook
func TestLifecycleSetStateHookPanic(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})
	l.OnStateChange(StateRunning, func() {
		panic("intentional panic")
	})

	l.setState(StateRunning)
	time.Sleep(50 * time.Millisecond) // let hook goroutine run

	if l.State() != StateRunning {
		t.Errorf("Expected state running, got %s", l.State())
	}
}

// TestProductionServerCircuitBreakerOpen tests ExecuteWithCircuitBreaker when open
func TestProductionServerCircuitBreakerOpen(t *testing.T) {
	ps := NewProductionServer(nil, &ProductionConfig{
		EnableCircuitBreaker: true,
		CircuitBreaker: &engine.CircuitBreakerConfig{
			MaxFailures:  1,
			ResetTimeout: 1 * time.Second,
		},
	})

	// Trip the breaker
	_ = ps.ExecuteWithCircuitBreaker("test", func() error {
		return errors.New("failure")
	})

	// Second call should fail due to open breaker
	err := ps.ExecuteWithCircuitBreaker("test", func() error {
		return nil
	})
	if err == nil {
		t.Error("Expected error from open circuit breaker")
	}
}

// TestProductionServerStartLifecycleError tests Start when lifecycle fails
func TestProductionServerStartLifecycleError(t *testing.T) {
	ps := NewProductionServer(nil, &ProductionConfig{
		EnableHealthServer: false,
		Lifecycle: &LifecycleConfig{
			StartupTimeout:      5 * time.Second,
			HealthCheckInterval: 1 * time.Second,
		},
	})

	// Add a failing component
	ps.Lifecycle.RegisterComponent(&failingComponent{name: "fail"})

	err := ps.Start()
	if err == nil {
		t.Error("Expected error from failing lifecycle component")
	}
}

// TestProductionServerStopLifecycleError tests Stop when lifecycle Stop fails
func TestProductionServerStopLifecycleError(t *testing.T) {
	ps := NewProductionServer(nil, &ProductionConfig{
		EnableHealthServer: false,
		Lifecycle: &LifecycleConfig{
			StartupTimeout:      5 * time.Second,
			DrainTimeout:        1 * time.Millisecond,
			ShutdownTimeout:     1 * time.Second,
			HealthCheckInterval: 1 * time.Second,
		},
	})

	ps.Lifecycle.RegisterComponent(&failingStopComponent{name: "fail-stop"})
	_ = ps.Start()

	err := ps.Stop()
	if err == nil {
		t.Error("Expected error from failing lifecycle stop")
	}
}

// TestAuthMiddlewareBearerToken tests authMiddleware with Bearer token format
func TestAuthMiddlewareBearerToken(t *testing.T) {
	admin := NewAdminServer(nil, ":0")
	admin.SetAuthToken("secret-token")

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := admin.authMiddleware(testHandler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for valid Bearer token, got %d", w.Code)
	}
}

// failingComponent is a component that fails to start
type failingComponent struct {
	name string
}

func (f *failingComponent) Name() string                                 { return f.name }
func (f *failingComponent) Start(ctx context.Context) error              { return errors.New("start failed") }
func (f *failingComponent) Stop(ctx context.Context) error               { return nil }
func (f *failingComponent) Health() HealthStatus                         { return HealthStatus{Healthy: false} }

// failingStopComponent is a component that fails to stop
type failingStopComponent struct {
	name string
}

func (f *failingStopComponent) Name() string                { return f.name }
func (f *failingStopComponent) Start(ctx context.Context) error { return nil }
func (f *failingStopComponent) Stop(ctx context.Context) error  { return errors.New("stop failed") }
func (f *failingStopComponent) Health() HealthStatus            { return HealthStatus{Healthy: true} }
