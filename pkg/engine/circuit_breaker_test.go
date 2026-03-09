package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitBreakerStateTransitions(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         3,
		MinSuccesses:        2,
		ResetTimeout:        100 * time.Millisecond,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	}

	cb := NewCircuitBreaker(config)

	// Initially closed
	if cb.State() != CircuitClosed {
		t.Errorf("expected initial state to be Closed, got %s", cb.State())
	}

	// Report failures to open circuit
	for i := 0; i < 3; i++ {
		cb.ReportFailure()
	}

	if cb.State() != CircuitOpen {
		t.Errorf("expected state to be Open after 3 failures, got %s", cb.State())
	}

	// Allow should fail when open (may allow one through in half-open)
	// First call after timeout may succeed if it acquires the half-open token
	// So we try multiple times to ensure circuit is really open
	time.Sleep(50 * time.Millisecond)
	errors := 0
	for i := 0; i < 5; i++ {
		err := cb.Allow()
		if err == ErrCircuitOpen {
			errors++
		} else if err == nil {
			cb.Release()
		}
	}
	if errors == 0 {
		t.Error("expected some ErrCircuitOpen errors when circuit is open")
	}

	// Wait for reset timeout
	time.Sleep(150 * time.Millisecond)

	// After timeout, should transition to half-open on allow
	// Since we have 1 token, first allow may succeed
	err := cb.Allow()
	if err != nil && err != ErrCircuitOpen {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCircuitBreakerSuccessResets(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         3,
		MinSuccesses:        2,
		ResetTimeout:        50 * time.Millisecond,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 2,
	}

	cb := NewCircuitBreaker(config)

	// Open the circuit
	for i := 0; i < 3; i++ {
		cb.ReportFailure()
	}

	if cb.State() != CircuitOpen {
		t.Fatal("circuit should be open")
	}

	// Wait for reset
	time.Sleep(100 * time.Millisecond)

	// Manually transition to half-open by trying allow
	// This is tricky due to token bucket, so we'll use the internal method
	cb.tryHalfOpen()

	// Report successes to close circuit
	for i := 0; i < 2; i++ {
		cb.ReportSuccess()
	}

	if cb.State() != CircuitClosed {
		t.Errorf("expected state to be Closed after 2 successes, got %s", cb.State())
	}
}

func TestCircuitBreakerConcurrencyLimit(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         5,
		MinSuccesses:        3,
		ResetTimeout:        time.Second,
		MaxConcurrency:      2,
		HalfOpenMaxRequests: 1,
	}

	cb := NewCircuitBreaker(config)

	// Allow up to MaxConcurrency
	if err := cb.Allow(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := cb.Allow(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Third should fail
	if err := cb.Allow(); err != ErrCircuitTooMany {
		t.Errorf("expected ErrCircuitTooMany, got %v", err)
	}

	// Release one
	cb.Release()

	// Now should allow
	if err := cb.Allow(); err != nil {
		t.Errorf("unexpected error after release: %v", err)
	}
}

func TestCircuitBreakerExecute(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	cb := NewCircuitBreaker(config)

	// Successful execution
	err := cb.Execute(context.Background(), func() error {
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Failed execution
	testErr := errors.New("test error")
	err = cb.Execute(context.Background(), func() error {
		return testErr
	})
	if err != testErr {
		t.Errorf("expected test error, got %v", err)
	}
}

func TestCircuitBreakerManager(t *testing.T) {
	manager := NewCircuitBreakerManager()

	config := DefaultCircuitBreakerConfig()

	// Get or create
	cb1 := manager.GetOrCreate("service1", config)
	if cb1 == nil {
		t.Fatal("expected circuit breaker")
	}

	// Get existing
	cb2 := manager.GetOrCreate("service1", config)
	if cb1 != cb2 {
		t.Error("expected same circuit breaker instance")
	}

	// Get different
	cb3 := manager.GetOrCreate("service2", config)
	if cb1 == cb3 {
		t.Error("expected different circuit breaker instance")
	}

	// AllStats
	stats := manager.AllStats()
	if len(stats) != 2 {
		t.Errorf("expected 2 stats, got %d", len(stats))
	}

	// Remove
	manager.Remove("service1")
	stats = manager.AllStats()
	if len(stats) != 1 {
		t.Errorf("expected 1 stat after remove, got %d", len(stats))
	}
}

func TestCircuitBreakerParallel(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         10,
		MinSuccesses:        3,
		ResetTimeout:        time.Second,
		MaxConcurrency:      100,
		HalfOpenMaxRequests: 10,
	}

	cb := NewCircuitBreaker(config)

	var successCount, failureCount atomic.Int32
	var wg sync.WaitGroup

	// Run parallel operations
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			if err := cb.Allow(); err != nil {
				failureCount.Add(1)
				return
			}
			defer cb.Release()

			// Simulate work
			time.Sleep(time.Millisecond)

			if id%10 == 0 {
				cb.ReportFailure()
				successCount.Add(1)
			} else {
				cb.ReportSuccess()
			}
		}(i)
	}

	wg.Wait()

	// Verify stats
	stats := cb.Stats()
	if stats.Concurrency != 0 {
		t.Errorf("expected concurrency to be 0, got %d", stats.Concurrency)
	}
}

func TestCircuitBreakerStats(t *testing.T) {
	cb := NewCircuitBreaker(nil)

	// Initial stats
	stats := cb.Stats()
	if stats.State != "closed" {
		t.Errorf("expected state 'closed', got %s", stats.State)
	}

	// Report some failures
	cb.ReportFailure()
	cb.ReportFailure()

	stats = cb.Stats()
	if stats.Failures != 2 {
		t.Errorf("expected 2 failures, got %d", stats.Failures)
	}
}

func BenchmarkCircuitBreakerAllow(b *testing.B) {
	cb := NewCircuitBreaker(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Allow()
		cb.Release()
	}
}
