package server

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

func captureStdoutForTest(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	os.Stdout = w

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close stdout writer: %v", err)
	}
	os.Stdout = orig

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close stdout reader: %v", err)
	}
	return string(out)
}

func TestServerAuthWarningDoesNotWriteStdoutWithoutLogger(t *testing.T) {
	srv, err := New(nil, &Config{
		AuthEnabled:      true,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "Str0ng!Pass#2026",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	stdout := captureStdoutForTest(t, func() {
		_ = srv.Listen("bad address", nil)
	})

	if stdout != "" {
		t.Fatalf("expected no stdout output, got %q", stdout)
	}
}

func TestServerAuthWarningUsesConfiguredLogger(t *testing.T) {
	var logs bytes.Buffer
	srv, err := New(nil, &Config{
		AuthEnabled:      true,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "Str0ng!Pass#2026",
		Logger:           logger.New(logger.WarnLevel, &logs),
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	_ = srv.Listen("bad address", nil)

	if !strings.Contains(logs.String(), "authentication is enabled but TLS is disabled") {
		t.Fatalf("expected auth warning in logger output, got %q", logs.String())
	}
}

func TestLifecycleHealthCheckDoesNotWriteStdoutWithoutLogger(t *testing.T) {
	lifecycle := NewLifecycle(&LifecycleConfig{})
	lifecycle.RegisterComponent(&MockComponent{
		name:          "unhealthy",
		healthy:       false,
		healthMessage: "not ready",
	})
	lifecycle.setState(StateRunning)

	stdout := captureStdoutForTest(t, func() {
		lifecycle.checkHealth()
	})

	if stdout != "" {
		t.Fatalf("expected no stdout output, got %q", stdout)
	}
}

func TestProductionServerPropagatesLoggerToLifecycle(t *testing.T) {
	var logs bytes.Buffer
	log := logger.New(logger.WarnLevel, &logs)
	ps := NewProductionServer(nil, &ProductionConfig{
		Logger:             log,
		EnableHealthServer: false,
		Lifecycle:          &LifecycleConfig{},
	})

	if ps.Lifecycle.logger != log {
		t.Fatal("expected production logger to be propagated to lifecycle")
	}
}

func TestRateLimiterStoresConfiguredLogger(t *testing.T) {
	var logs bytes.Buffer
	log := logger.New(logger.ErrorLevel, &logs)
	rl := NewRateLimiter(&RateLimiterConfig{
		RPS:             1,
		Burst:           1,
		PerClient:       true,
		ClientHeader:    "X-Client-ID",
		CleanupInterval: time.Hour,
		MaxClients:      10,
		Logger:          log,
	})
	defer rl.Stop()

	if rl.logger != log {
		t.Fatal("expected rate limiter logger to be stored")
	}
}
