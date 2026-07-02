package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestCircuitBreakerHalfOpenFailureBackoff verifies that a failure in
// half-open state stamps lastFailure, so the breaker enforces a full
// ResetTimeout backoff before the next half-open probe instead of
// immediately re-probing.
func TestCircuitBreakerHalfOpenFailureBackoff(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		MaxFailures:         1,
		MinSuccesses:        1,
		ResetTimeout:        150 * time.Millisecond,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	})
	defer cb.Stop()

	// Open the circuit.
	if err := cb.Allow(); err != nil {
		t.Fatalf("initial Allow: %v", err)
	}
	cb.ReportFailure()
	cb.Release()
	if cb.State() != CircuitOpen {
		t.Fatalf("expected open, got %s", cb.State())
	}

	// Wait for the reset timeout, take the half-open probe, and fail it.
	time.Sleep(200 * time.Millisecond)
	if err := cb.Allow(); err != nil {
		t.Fatalf("half-open probe Allow: %v", err)
	}
	cb.ReportFailure() // reopens the circuit
	cb.Release()
	if cb.State() != CircuitOpen {
		t.Fatalf("expected open after half-open failure, got %s", cb.State())
	}

	// Immediately after the half-open failure, the breaker must NOT allow
	// another probe: lastFailure was just stamped by openCircuit.
	if err := cb.Allow(); !errors.Is(err, ErrCircuitOpen) {
		if err == nil {
			cb.Release()
		}
		t.Fatalf("expected ErrCircuitOpen immediately after half-open failure, got %v", err)
	}

	// After another full ResetTimeout the probe is allowed again.
	time.Sleep(200 * time.Millisecond)
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected probe after backoff, got %v", err)
	}
	cb.ReportSuccess()
	cb.Release()
}

// TestCircuitBreakerHalfOpenTokensRefilledOnTransition verifies the breaker
// cannot wedge in a token-starved half-open state: even when a previous
// half-open cycle "lost" its token (success report landed after the circuit
// closed), the next transition into half-open refills the bucket.
func TestCircuitBreakerHalfOpenTokensRefilledOnTransition(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		MaxFailures:         1,
		MinSuccesses:        1,
		ResetTimeout:        50 * time.Millisecond,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	})
	defer cb.Stop()

	for cycle := 0; cycle < 3; cycle++ {
		// Open the circuit.
		if err := cb.Allow(); err != nil {
			t.Fatalf("cycle %d: Allow: %v", cycle, err)
		}
		cb.ReportFailure()
		cb.Release()
		if cb.State() != CircuitOpen {
			t.Fatalf("cycle %d: expected open, got %s", cycle, cb.State())
		}

		time.Sleep(80 * time.Millisecond)

		// The half-open probe must be allowed on every cycle. With the old
		// per-report token return, a token lost in a previous cycle would
		// leave the bucket empty forever and this Allow would fail.
		if err := cb.Allow(); err != nil {
			t.Fatalf("cycle %d: half-open probe not allowed (token bucket wedged?): %v", cycle, err)
		}
		// Close the circuit; the token is deliberately not returned by
		// ReportSuccess in this path.
		cb.ReportSuccess()
		cb.Release()
		if cb.State() != CircuitClosed {
			t.Fatalf("cycle %d: expected closed, got %s", cycle, cb.State())
		}
	}
}

// TestCircuitBreakerExecuteContextCancelNotAFailure verifies that caller
// context cancellation does not count toward the breaker's failure threshold.
func TestCircuitBreakerExecuteContextCancelNotAFailure(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		MaxFailures:         2,
		MinSuccesses:        1,
		ResetTimeout:        time.Hour,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	})
	defer cb.Stop()

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := cb.Execute(ctx, func() error {
			time.Sleep(time.Hour)
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	}

	if cb.State() != CircuitClosed {
		t.Fatalf("caller cancellations opened the circuit: state=%s", cb.State())
	}
	if got := cb.Stats().Failures; got != 0 {
		t.Fatalf("expected 0 failures from cancellations, got %d", got)
	}
}

// TestCircuitBreakerManagerBounded verifies GetOrCreate cannot grow the
// breaker map without bound; keys beyond the cap share an overflow breaker.
func TestCircuitBreakerManagerBounded(t *testing.T) {
	m := NewCircuitBreakerManager()

	var overflow *CircuitBreaker
	for i := 0; i < circuitBreakerManagerMaxBreakers*4; i++ {
		cb := m.GetOrCreate(fmt.Sprintf("key-%d", i), nil)
		if cb == nil {
			t.Fatalf("GetOrCreate returned nil at %d", i)
		}
		if i >= circuitBreakerManagerMaxBreakers {
			if overflow == nil {
				overflow = cb
			} else if cb != overflow {
				t.Fatalf("expected shared overflow breaker at key-%d", i)
			}
		}
	}

	stats := m.AllStats()
	if len(stats) > circuitBreakerManagerMaxBreakers {
		t.Fatalf("breaker map exceeded cap: %d > %d", len(stats), circuitBreakerManagerMaxBreakers)
	}
	if _, ok := m.Get(circuitBreakerOverflowKey); !ok {
		t.Fatal("expected overflow breaker to be registered")
	}

	// Existing keys keep returning their original breaker.
	a := m.GetOrCreate("key-1", nil)
	b := m.GetOrCreate("key-1", nil)
	if a != b {
		t.Fatal("GetOrCreate not stable for existing key")
	}
}
