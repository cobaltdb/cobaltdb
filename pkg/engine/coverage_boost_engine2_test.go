package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRetryWithResultRetryThenSuccess tests RetryWithResult with retries then success
func TestRetryWithResultRetryThenSuccess(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0.0,
	}

	ctx := context.Background()
	callCount := 0

	result, err := RetryWithResult(ctx, config, func() (string, error) {
		callCount++
		if callCount < 2 {
			return "", errors.New("transient error")
		}
		return "success", nil
	})

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if result != "success" {
		t.Errorf("Expected 'success', got: %s", result)
	}
	if callCount != 2 {
		t.Errorf("Expected 2 calls, got: %d", callCount)
	}
}

// TestRetryWithResultAllAttemptsFail tests RetryWithResult when all attempts fail
func TestRetryWithResultAllAttemptsFail(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0.0,
	}

	ctx := context.Background()
	callCount := 0

	_, err := RetryWithResult(ctx, config, func() (string, error) {
		callCount++
		return "", errors.New("persistent error")
	})

	if err == nil {
		t.Error("Expected error after all retries exhausted")
	}
	if callCount != 3 {
		t.Errorf("Expected 3 calls, got: %d", callCount)
	}
}

// TestRetryWithResultContextCancelledDuringRetry tests RetryWithResult with context cancelled during retry
func TestRetryWithResultContextCancelledDuringRetry(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     500 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0.0,
	}

	ctx, cancel := context.WithCancel(context.Background())

	callCount := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := RetryWithResult(ctx, config, func() (string, error) {
		callCount++
		return "", errors.New("error")
	})

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got: %v", err)
	}
}

// TestRetryWithResultNilConfig tests RetryWithResult with nil config
func TestRetryWithResultNilConfig(t *testing.T) {
	ctx := context.Background()

	result, err := RetryWithResult(ctx, nil, func() (string, error) {
		return "success", nil
	})

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if result != "success" {
		t.Errorf("Expected 'success', got: %s", result)
	}
}

// TestCircuitBreakerReportFailureInHalfOpen tests ReportFailure in half-open state
func TestCircuitBreakerReportFailureInHalfOpen(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         1,
		MinSuccesses:        1,
		ResetTimeout:        10 * time.Millisecond,
		MaxConcurrency:      100,
		HalfOpenMaxRequests: 1,
	}

	cb := NewCircuitBreaker(config)

	// Trigger failure to open circuit
	cb.Allow()
	cb.ReportFailure()

	// Wait for reset timeout
	time.Sleep(20 * time.Millisecond)

	// Allow should transition to half-open and allow request
	err := cb.Allow()
	if err != nil {
		t.Fatalf("Expected Allow to succeed in half-open, got: %v", err)
	}

	// Report failure in half-open should reopen circuit
	cb.ReportFailure()

	// Circuit should be open again
	if cb.State() != CircuitOpen {
		t.Errorf("Expected circuit to be open, got: %v", cb.State())
	}
}

// TestCircuitBreakerReportFailureInClosed tests ReportFailure in closed state
func TestCircuitBreakerReportFailureInClosed(t *testing.T) {
	config := &CircuitBreakerConfig{
		MaxFailures:         2,
		MinSuccesses:        1,
		ResetTimeout:        30 * time.Second,
		MaxConcurrency:      100,
		HalfOpenMaxRequests: 1,
	}

	cb := NewCircuitBreaker(config)

	// Allow request
	err := cb.Allow()
	if err != nil {
		t.Fatalf("Expected Allow to succeed, got: %v", err)
	}

	// Report single failure - should not open circuit yet
	cb.ReportFailure()

	if cb.State() != CircuitClosed {
		t.Errorf("Expected circuit to still be closed, got: %v", cb.State())
	}

	// Allow another request
	cb.Allow()

	// Report second failure - should open circuit
	cb.ReportFailure()

	if cb.State() != CircuitOpen {
		t.Errorf("Expected circuit to be open, got: %v", cb.State())
	}
}

// TestCircuitBreakerStopped tests that stopped circuit breaker prevents reports
func TestCircuitBreakerStopped(t *testing.T) {
	config := DefaultCircuitBreakerConfig()
	cb := NewCircuitBreaker(config)

	// Stop the circuit breaker
	cb.Stop()

	// ReportSuccess and ReportFailure should be no-ops
	cb.ReportSuccess()
	cb.ReportFailure()

	// State should still be closed
	if cb.State() != CircuitClosed {
		t.Errorf("Expected circuit to be closed, got: %v", cb.State())
	}
}

// TestRetryConfigIsRetryableWithRetryableErrors tests IsRetryable with specific retryable errors
func TestRetryConfigIsRetryableWithRetryableErrors(t *testing.T) {
	retryableErr := errors.New("retryable error")
	nonRetryableErr := errors.New("non-retryable error")

	config := &RetryConfig{
		RetryableErrors:    []error{retryableErr},
		NonRetryableErrors: []error{nonRetryableErr},
	}

	// Retryable error should be retryable
	if !config.IsRetryable(retryableErr) {
		t.Error("Expected retryable error to be retryable")
	}

	// Non-retryable error should not be retryable
	if config.IsRetryable(nonRetryableErr) {
		t.Error("Expected non-retryable error to not be retryable")
	}

	// Other errors should not be retryable when list is specified
	otherErr := errors.New("other error")
	if config.IsRetryable(otherErr) {
		t.Error("Expected other error to not be retryable when list is specified")
	}
}

// TestRetryConfigIsRetryableNilError tests IsRetryable with nil error
func TestRetryConfigIsRetryableNilError(t *testing.T) {
	config := DefaultRetryConfig()

	if config.IsRetryable(nil) {
		t.Error("Expected nil error to not be retryable")
	}
}

// TestGetRetryConfigAllPolicies tests all retry policies
func TestGetRetryConfigAllPolicies(t *testing.T) {
	policies := []RetryPolicy{
		RetryPolicyFast,
		RetryPolicyStandard,
		RetryPolicyAggressive,
		RetryPolicyBackground,
		RetryPolicy(999), // Unknown policy
	}

	for _, policy := range policies {
		config := GetRetryConfig(policy)
		if config == nil {
			t.Errorf("GetRetryConfig(%v) returned nil", policy)
		}
	}
}

// TestCircuitBreakerManagerRemove tests CircuitBreakerManager Remove method
func TestCircuitBreakerManagerRemove(t *testing.T) {
	mgr := NewCircuitBreakerManager()
	config := DefaultCircuitBreakerConfig()

	// Create and then remove
	mgr.GetOrCreate("test", config)
	mgr.Remove("test")

	// Should no longer exist
	_, exists := mgr.Get("test")
	if exists {
		t.Error("Expected breaker to be removed")
	}
}

// TestCircuitBreakerManagerAllStats tests CircuitBreakerManager AllStats method
func TestCircuitBreakerManagerAllStats(t *testing.T) {
	mgr := NewCircuitBreakerManager()
	config := DefaultCircuitBreakerConfig()

	// Create some breakers
	mgr.GetOrCreate("cb1", config)
	mgr.GetOrCreate("cb2", config)

	// Get all stats
	stats := mgr.AllStats()
	if len(stats) != 2 {
		t.Errorf("Expected 2 stats entries, got: %d", len(stats))
	}

	if _, ok := stats["cb1"]; !ok {
		t.Error("Expected stats for cb1")
	}
	if _, ok := stats["cb2"]; !ok {
		t.Error("Expected stats for cb2")
	}
}
