package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestDBComponentName tests DBComponent.Name
func TestDBComponentName(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	comp := NewDBComponent("test-db", db)
	if comp.Name() != "test-db" {
		t.Errorf("Expected name 'test-db', got %s", comp.Name())
	}
}

// TestDBComponentStart tests DBComponent.Start
func TestDBComponentStart(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	comp := NewDBComponent("test-db", db)
	ctx := context.Background()

	err = comp.Start(ctx)
	if err != nil {
		t.Errorf("Start failed: %v", err)
	}
}

// TestDBComponentStartNilDB tests DBComponent.Start with nil DB
func TestDBComponentStartNilDB(t *testing.T) {
	comp := NewDBComponent("test-db", nil)
	ctx := context.Background()

	err := comp.Start(ctx)
	if err == nil {
		t.Error("Expected error when starting with nil DB")
	}
}

// TestDBComponentStop tests DBComponent.Stop
func TestDBComponentStop(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	comp := NewDBComponent("test-db", db)
	ctx := context.Background()

	err = comp.Stop(ctx)
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

// TestDBComponentHealth tests DBComponent.Health
func TestDBComponentHealth(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	comp := NewDBComponent("test-db", db)
	status := comp.Health()

	if !status.Healthy {
		t.Errorf("Expected healthy status, got: %s", status.Message)
	}
}

// TestDBComponentHealthNilDB tests DBComponent.Health with nil DB
func TestDBComponentHealthNilDB(t *testing.T) {
	comp := NewDBComponent("test-db", nil)
	status := comp.Health()

	if status.Healthy {
		t.Error("Expected unhealthy status with nil DB")
	}
}

// TestNewDBComponent tests NewDBComponent constructor
func TestNewDBComponent(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	comp := NewDBComponent("my-component", db)
	if comp == nil {
		t.Fatal("NewDBComponent returned nil")
	}

	if comp.name != "my-component" {
		t.Errorf("Expected name 'my-component', got %s", comp.name)
	}

	if comp.db != db {
		t.Error("DB not set correctly")
	}

	if comp.ctx == nil {
		t.Error("Context not initialized")
	}
}

// TestLifecycleReadyCheck tests ReadyCheck handler
func TestLifecycleReadyCheck2(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})

	// Test when not running
	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	handler := l.ReadyCheck()
	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", w.Code)
	}
}

// TestLifecycleLiveCheck tests LiveCheck handler
func TestLifecycleLiveCheck2(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})

	// Test when not stopped
	req := httptest.NewRequest("GET", "/live", nil)
	w := httptest.NewRecorder()

	handler := l.LiveCheck()
	handler(w, req)

	// Should be healthy if not stopped
	if w.Code != http.StatusOK && w.Code != http.StatusServiceUnavailable {
		t.Errorf("Unexpected status: %d", w.Code)
	}
}

// TestLifecycleGracefulShutdownHandler tests GracefulShutdownHandler
func TestLifecycleGracefulShutdownHandler2(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})

	// Test with POST
	req := httptest.NewRequest("POST", "/shutdown", nil)
	w := httptest.NewRecorder()

	handler := l.GracefulShutdownHandler()
	handler(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Expected status 202, got %d", w.Code)
	}
}

// TestLifecycleGracefulShutdownHandlerMethodNotAllowed tests handler with wrong method
func TestLifecycleGracefulShutdownHandlerMethodNotAllowed(t *testing.T) {
	l := NewLifecycle(&LifecycleConfig{})

	// Test with GET (should fail)
	req := httptest.NewRequest("GET", "/shutdown", nil)
	w := httptest.NewRecorder()

	handler := l.GracefulShutdownHandler()
	handler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}
