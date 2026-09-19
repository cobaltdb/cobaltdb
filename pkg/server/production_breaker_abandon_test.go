package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestClassifiedBreakerAbandonsCancelledProbe pins the ctx.Canceled handling
// of executeWithClassifiedBreaker: a cancelled caller says nothing about
// backend health (no report), but the consumed half-open probe slot must
// still be abandoned. Pre-fix the token leaked and a HalfOpenMaxRequests=1
// breaker wedged in half-open forever: no probe could run, so no report
// could ever move the state.
func TestClassifiedBreakerAbandonsCancelledProbe(t *testing.T) {
	ps := &ProductionServer{
		Config: &ProductionConfig{
			CircuitBreaker: &engine.CircuitBreakerConfig{
				MaxFailures:         1,
				MinSuccesses:        1,
				ResetTimeout:        20 * time.Millisecond,
				MaxConcurrency:      10,
				HalfOpenMaxRequests: 1,
			},
		},
		CircuitBreakers: engine.NewCircuitBreakerManager(),
	}

	// Trip the SELECT breaker with an infrastructure failure.
	if err := ps.executeWithClassifiedBreaker("SELECT", func() error { return errors.New("timeout") }); err == nil {
		t.Fatal("expected infrastructure failure")
	}
	cb, ok := ps.CircuitBreakers.Get("SELECT")
	if !ok {
		t.Fatal("SELECT breaker not tracked")
	}
	if cb.State() != engine.CircuitOpen {
		t.Fatalf("state after failure: %s, want open", cb.State())
	}

	time.Sleep(50 * time.Millisecond) // > ResetTimeout

	// Cancelled probe: the backend surfaces context.Canceled (caller gave up).
	if err := ps.executeWithClassifiedBreaker("SELECT", func() error { return context.Canceled }); err == nil {
		t.Fatal("expected context.Canceled to surface")
	}

	// The breaker must admit the next probe and recover.
	if err := ps.executeWithClassifiedBreaker("SELECT", func() error { return nil }); err != nil {
		t.Fatalf("breaker wedged after cancelled probe: %v (state=%s)", err, cb.State())
	}
	if cb.State() != engine.CircuitClosed {
		t.Fatalf("state after recovery probe: %s, want closed", cb.State())
	}
}
