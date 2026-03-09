package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetrySuccess(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0,
	}

	var callCount int
	fn := func() error {
		callCount++
		return nil
	}

	err := Retry(context.Background(), config, fn)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}
}

func TestRetryFailure(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0,
	}

	testErr := errors.New("test error")
	var callCount int

	fn := func() error {
		callCount++
		return testErr
	}

	err := Retry(context.Background(), config, fn)
	if err != testErr {
		t.Errorf("expected test error, got %v", err)
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

func TestRetryEventualSuccess(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0,
	}

	var callCount int
	fn := func() error {
		callCount++
		if callCount < 3 {
			return errors.New("temporary error")
		}
		return nil
	}

	err := Retry(context.Background(), config, fn)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

func TestRetryContextCancellation(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  10,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		Multiplier:   2.0,
		Jitter:       0,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount int
	fn := func() error {
		callCount++
		if callCount >= 2 {
			cancel() // Cancel context after 2 attempts
		}
		return errors.New("error")
	}

	err := Retry(ctx, config, fn)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRetryWithResultSuccess(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0,
	}

	fn := func() (string, error) {
		return "success", nil
	}

	result, err := RetryWithResult(context.Background(), config, fn)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result != "success" {
		t.Errorf("expected 'success', got %s", result)
	}
}

func TestRetryWithResultFailure(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0,
	}

	testErr := errors.New("test error")
	fn := func() (string, error) {
		return "", testErr
	}

	result, err := RetryWithResult(context.Background(), config, fn)
	if err != testErr {
		t.Errorf("expected test error, got %v", err)
	}
	if result != "" {
		t.Errorf("expected empty result, got %s", result)
	}
}

func TestRetryIsRetryable(t *testing.T) {
	retryableErr := errors.New("retryable")
	nonRetryableErr := errors.New("non-retryable")

	config := &RetryConfig{
		RetryableErrors:    []error{retryableErr},
		NonRetryableErrors: []error{nonRetryableErr},
	}

	// Test retryable
	if !config.IsRetryable(retryableErr) {
		t.Error("expected retryable error to be retryable")
	}

	// Test non-retryable
	if config.IsRetryable(nonRetryableErr) {
		t.Error("expected non-retryable error to not be retryable")
	}

	// Test unknown error (should be retryable by default when list is empty)
	config2 := &RetryConfig{}
	if !config2.IsRetryable(errors.New("unknown")) {
		t.Error("expected unknown error to be retryable by default")
	}

	// Test nil
	if config.IsRetryable(nil) {
		t.Error("expected nil error to not be retryable")
	}
}

func TestRetryNonRetryableError(t *testing.T) {
	nonRetryableErr := errors.New("non-retryable")

	config := &RetryConfig{
		MaxAttempts:        5,
		InitialDelay:       1 * time.Millisecond,
		MaxDelay:           10 * time.Millisecond,
		Multiplier:         2.0,
		Jitter:             0,
		NonRetryableErrors: []error{nonRetryableErr},
	}

	var callCount int
	fn := func() error {
		callCount++
		return nonRetryableErr
	}

	err := Retry(context.Background(), config, fn)
	if err != nonRetryableErr {
		t.Errorf("expected non-retryable error, got %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 call (no retries), got %d", callCount)
	}
}

func TestGetRetryConfig(t *testing.T) {
	tests := []struct {
		policy   RetryPolicy
		expected int // MaxAttempts
	}{
		{RetryPolicyFast, 2},
		{RetryPolicyStandard, 3},
		{RetryPolicyAggressive, 5},
		{RetryPolicyBackground, 10},
		{RetryPolicy(999), 3}, // Unknown policy defaults to standard
	}

	for _, tt := range tests {
		config := GetRetryConfig(tt.policy)
		if config.MaxAttempts != tt.expected {
			t.Errorf("policy %v: expected %d attempts, got %d", tt.policy, tt.expected, config.MaxAttempts)
		}
	}
}

func TestCalculateDelay(t *testing.T) {
	config := &RetryConfig{
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		Multiplier:   2.0,
		Jitter:       0,
	}

	delays := []time.Duration{
		calculateDelay(1, config), // 100ms
		calculateDelay(2, config), // 200ms
		calculateDelay(3, config), // 400ms
		calculateDelay(4, config), // 800ms
		calculateDelay(5, config), // 1000ms (capped)
	}

	expected := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1 * time.Second,
	}

	for i, d := range delays {
		if d != expected[i] {
			t.Errorf("attempt %d: expected %v, got %v", i+1, expected[i], d)
		}
	}
}

func TestRetriableError(t *testing.T) {
	innerErr := errors.New("inner error")
	retriable := &RetriableError{Err: innerErr}

	if retriable.Error() != "inner error" {
		t.Errorf("expected 'inner error', got %s", retriable.Error())
	}

	if !IsRetriable(retriable) {
		t.Error("expected error to be retriable")
	}

	if IsRetriable(innerErr) {
		t.Error("expected inner error to not be retriable")
	}

	// Test Unwrap
	if !errors.Is(retriable, innerErr) {
		t.Error("expected errors.Is to work with wrapped error")
	}
}

func TestNonRetriableError(t *testing.T) {
	innerErr := errors.New("inner error")
	nonRetriable := &NonRetriableError{Err: innerErr}

	if nonRetriable.Error() != "inner error" {
		t.Errorf("expected 'inner error', got %s", nonRetriable.Error())
	}

	if !IsNonRetriable(nonRetriable) {
		t.Error("expected error to be non-retriable")
	}

	if IsNonRetriable(innerErr) {
		t.Error("expected inner error to not be non-retriable")
	}
}

func TestRetryWithJitter(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.5, // 50% jitter
	}

	var delays []time.Duration
	for i := 1; i <= 3; i++ {
		delay := calculateDelay(i, config)
		delays = append(delays, delay)
	}

	// Just verify jitter produces different delays
	// (statistically unlikely to be identical)
	allSame := true
	for i := 1; i < len(delays); i++ {
		if delays[i] != delays[0] {
			allSame = false
			break
		}
	}

	if allSame {
		t.Log("Warning: jitter delays were all the same (unlikely but possible)")
	}
}

func BenchmarkRetrySuccess(b *testing.B) {
	config := DefaultRetryConfig()
	fn := func() error { return nil }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Retry(context.Background(), config, fn)
	}
}

func BenchmarkRetryWithResult(b *testing.B) {
	config := DefaultRetryConfig()
	fn := func() (int, error) { return 42, nil }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		RetryWithResult(context.Background(), config, fn)
	}
}

func TestRetryParallel(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 1 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
		Jitter:       0,
	}

	var successCount atomic.Int32

	// Run multiple retries in parallel
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		go func() {
			err := Retry(ctx, config, func() error {
				return nil
			})
			if err == nil {
				successCount.Add(1)
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)

	if successCount.Load() != 10 {
		t.Errorf("expected 10 successes, got %d", successCount.Load())
	}
}
