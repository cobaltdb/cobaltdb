package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestExecuteCancelledProbeDoesNotWedgeBreaker pins half-open probe-slot
// reclamation: Execute's ctx.Done path discards fn's late result without
// reporting (caller cancellation says nothing about backend health), and the
// consumed half-open probe token must still be returned. Pre-fix the token
// leaked: with the default HalfOpenMaxRequests=1 the breaker wedged in
// half-open — every later Allow returned ErrCircuitOpen, no probe could run,
// and no report could therefore ever move the state (the Open-state reset
// path only applies from Open).
func TestExecuteCancelledProbeDoesNotWedgeBreaker(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		MaxFailures:         1,
		MinSuccesses:        1,
		ResetTimeout:        20 * time.Millisecond,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	})
	ctx := context.Background()
	probeErr := errors.New("boom")

	// Trip the breaker.
	if err := cb.Execute(ctx, func() error { return probeErr }); err == nil {
		t.Fatal("expected probe failure")
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("state after failure: %s, want open", cb.State())
	}

	time.Sleep(50 * time.Millisecond) // > ResetTimeout

	// Cancelled half-open probe: fn still running when the caller cancels.
	ctx2, cancel := context.WithCancel(ctx)
	fnGate := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- cb.Execute(ctx2, func() error {
			<-fnGate
			return nil
		})
	}()
	time.Sleep(25 * time.Millisecond) // let Execute consume the half-open slot
	cancel()
	if err := <-errCh; err == nil {
		t.Fatal("expected ctx.Err from the cancelled Execute")
	}
	close(fnGate) // fn finishes; the late goroutine reclaims the slot
	time.Sleep(40 * time.Millisecond)

	// The breaker must still admit a probe and recover.
	if err := cb.Execute(ctx, func() error { return nil }); err != nil {
		t.Fatalf("breaker wedged after cancelled half-open probe: %v (state=%s)", err, cb.State())
	}
	if cb.State() != CircuitClosed {
		t.Fatalf("state after recovery probe: %s, want closed", cb.State())
	}
}
