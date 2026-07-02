package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestRetryContextErrorsNeverRetryable verifies context errors are never
// retryable, even when explicitly listed as retryable.
func TestRetryContextErrorsNeverRetryable(t *testing.T) {
	config := &RetryConfig{
		RetryableErrors: []error{context.Canceled, context.DeadlineExceeded},
	}
	if config.IsRetryable(context.Canceled) {
		t.Error("context.Canceled must never be retryable")
	}
	if config.IsRetryable(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded must never be retryable")
	}
	if config.IsRetryable(fmt.Errorf("wrap: %w", context.DeadlineExceeded)) {
		t.Error("wrapped context.DeadlineExceeded must never be retryable")
	}
}

// TestRetryDefaultNonRetryableClassification verifies deterministic errors
// are not retried under the default (empty-lists) configuration, while
// unclassified errors remain retryable.
func TestRetryDefaultNonRetryableClassification(t *testing.T) {
	config := &RetryConfig{}

	nonRetryable := []error{
		errors.New("parse error: unexpected token: FRM"),
		errors.New("syntax error near SELECT"),
		errors.New("UNIQUE constraint failed: id"),
		errors.New("duplicate key value"),
		errors.New("permission denied for table users"),
		errors.New("access denied"),
		errors.New("table not found"),
		errors.New("unknown column 'nope'"),
		errors.New("table already exists"),
		errors.New("database is closed"),
	}
	for _, err := range nonRetryable {
		if config.IsRetryable(err) {
			t.Errorf("expected %q to be classified non-retryable", err)
		}
	}

	retryable := []error{
		errors.New("connection reset by peer"),
		errors.New("some transient thing"),
	}
	for _, err := range retryable {
		if !config.IsRetryable(err) {
			t.Errorf("expected %q to remain retryable by default", err)
		}
	}
}

// TestRetryDoesNotRetryDeterministicError verifies Retry stops after the
// first attempt for a classified deterministic error.
func TestRetryDoesNotRetryDeterministicError(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:  5,
		InitialDelay: time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2.0,
	}
	parseErr := errors.New("parse error: bad statement")
	calls := 0
	err := Retry(context.Background(), config, func() error {
		calls++
		return parseErr
	})
	if !errors.Is(err, parseErr) {
		t.Fatalf("expected parse error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("deterministic error was retried: %d calls", calls)
	}
}
