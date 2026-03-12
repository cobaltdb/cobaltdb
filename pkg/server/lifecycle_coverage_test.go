package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestDefaultLifecycleConfig(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected 30s shutdown timeout, got %v", cfg.ShutdownTimeout)
	}
	if cfg.DrainTimeout != 10*time.Second {
		t.Errorf("expected 10s drain timeout, got %v", cfg.DrainTimeout)
	}
	if !cfg.EnableSignalHandling {
		t.Error("expected signal handling enabled")
	}
}

func TestLifecycleReadyCheck(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	cfg.EnableSignalHandling = false
	cfg.ShutdownTimeout = 100 * time.Millisecond
	cfg.DrainTimeout = 100 * time.Millisecond
	lc := NewLifecycle(cfg)

	// Before start — should be unavailable
	handler := lc.ReadyCheck()
	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("before start expected 503, got %d", w.Code)
	}

	// Start lifecycle
	go func() {
		_ = lc.Start()
	}()
	// Give it time to transition to Running
	time.Sleep(50 * time.Millisecond)

	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("after start expected 200, got %d", w.Code)
	}

	// Stop
	_ = lc.Stop()
}

func TestLifecycleLiveCheck(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	cfg.EnableSignalHandling = false
	lc := NewLifecycle(cfg)

	handler := lc.LiveCheck()

	// Before stop — should be alive (not stopped)
	req := httptest.NewRequest("GET", "/live", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 (alive), got %d", w.Code)
	}
}

func TestLifecycleGracefulShutdownHandler(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	cfg.EnableSignalHandling = false
	lc := NewLifecycle(cfg)

	handler := lc.GracefulShutdownHandler()

	// GET should fail
	req := httptest.NewRequest("GET", "/shutdown", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET expected 405, got %d", w.Code)
	}

	// POST should trigger shutdown
	req = httptest.NewRequest("POST", "/shutdown", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("POST expected 202, got %d", w.Code)
	}
}

func TestDBComponentLifecycle(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}

	comp := NewDBComponent("testdb", db)

	if comp.Name() != "testdb" {
		t.Errorf("expected name 'testdb', got %q", comp.Name())
	}

	// Start
	if err := comp.Start(context.Background()); err != nil {
		t.Errorf("Start failed: %v", err)
	}

	// Health should be healthy
	health := comp.Health()
	if !health.Healthy {
		t.Error("expected healthy")
	}

	// Stop
	if err := comp.Stop(context.Background()); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestDBComponentNilDB(t *testing.T) {
	comp := NewDBComponent("nildb", nil)

	// Start should fail
	if err := comp.Start(context.Background()); err == nil {
		t.Error("expected error for nil db")
	}

	// Health should be unhealthy
	health := comp.Health()
	if health.Healthy {
		t.Error("expected unhealthy for nil db")
	}
}

func TestLifecycleStateTransitions(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	cfg.EnableSignalHandling = false
	cfg.ShutdownTimeout = 100 * time.Millisecond
	cfg.DrainTimeout = 100 * time.Millisecond
	lc := NewLifecycle(cfg)

	if lc.State() != StateInitializing {
		t.Errorf("initial state should be Initializing, got %v", lc.State())
	}

	// Start in goroutine
	go func() {
		_ = lc.Start()
	}()
	time.Sleep(50 * time.Millisecond)

	if lc.State() != StateRunning {
		t.Errorf("after start should be Running, got %v", lc.State())
	}

	// Stop
	_ = lc.Stop()
	time.Sleep(50 * time.Millisecond)

	if lc.State() != StateStopped {
		t.Errorf("after stop should be Stopped, got %v", lc.State())
	}
}

func TestLifecycleLiveCheckStopped(t *testing.T) {
	cfg := DefaultLifecycleConfig()
	cfg.EnableSignalHandling = false
	cfg.ShutdownTimeout = 100 * time.Millisecond
	cfg.DrainTimeout = 100 * time.Millisecond
	lc := NewLifecycle(cfg)

	// Start and stop
	go func() { _ = lc.Start() }()
	time.Sleep(50 * time.Millisecond)
	_ = lc.Stop()
	time.Sleep(50 * time.Millisecond)

	handler := lc.LiveCheck()
	req := httptest.NewRequest("GET", "/live", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("stopped state expected 503, got %d", w.Code)
	}
}
