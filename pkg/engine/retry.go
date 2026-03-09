// Retry logic with exponential backoff for production resilience
// Handles transient failures gracefully

package engine

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"time"
)

// RetryConfig configures retry behavior
type RetryConfig struct {
	// MaxAttempts is the maximum number of attempts (default: 3)
	MaxAttempts int

	// InitialDelay is the initial delay between retries (default: 100ms)
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries (default: 30s)
	MaxDelay time.Duration

	// Multiplier is the exponential backoff multiplier (default: 2.0)
	Multiplier float64

	// Jitter adds randomization to prevent thundering herd (default: 0.1 = 10%)
	Jitter float64

	// RetryableErrors is a list of errors that should be retried
	// If empty, all errors are retried
	RetryableErrors []error

	// NonRetryableErrors is a list of errors that should NOT be retried
	NonRetryableErrors []error
}

// DefaultRetryConfig returns sensible defaults
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		Jitter:       0.1,
	}
}

// IsRetryable checks if an error should be retried
func (c *RetryConfig) IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Check non-retryable first (higher priority)
	for _, nr := range c.NonRetryableErrors {
		if errors.Is(err, nr) {
			return false
		}
	}

	// If retryable list is specified, only retry those
	if len(c.RetryableErrors) > 0 {
		for _, r := range c.RetryableErrors {
			if errors.Is(err, r) {
				return true
			}
		}
		return false
	}

	// Retry all by default
	return true
}

// Retry executes a function with retry logic
func Retry(ctx context.Context, config *RetryConfig, fn func() error) error {
	if config == nil {
		config = DefaultRetryConfig()
	}

	var lastErr error

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		// Check context before attempting
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if we should retry this error
		if !config.IsRetryable(err) {
			return err
		}

		// Don't retry after last attempt
		if attempt >= config.MaxAttempts {
			break
		}

		// Calculate delay with exponential backoff
		delay := calculateDelay(attempt, config)

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return lastErr
}

// RetryWithResult executes a function that returns a result with retry logic
func RetryWithResult[T any](ctx context.Context, config *RetryConfig, fn func() (T, error)) (T, error) {
	if config == nil {
		config = DefaultRetryConfig()
	}

	var lastErr error
	var zero T

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}

		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		if !config.IsRetryable(err) {
			return zero, err
		}

		if attempt >= config.MaxAttempts {
			break
		}

		delay := calculateDelay(attempt, config)

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(delay):
		}
	}

	return zero, lastErr
}

// calculateDelay computes the delay for a given attempt
func calculateDelay(attempt int, config *RetryConfig) time.Duration {
	// Calculate exponential delay
	delay := float64(config.InitialDelay) * math.Pow(config.Multiplier, float64(attempt-1))

	// Apply max delay cap
	if delay > float64(config.MaxDelay) {
		delay = float64(config.MaxDelay)
	}

	// Add jitter to prevent thundering herd (using crypto/rand for unpredictability)
	if config.Jitter > 0 {
		jitterAmount := delay * config.Jitter
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err == nil {
			randFloat := float64(binary.LittleEndian.Uint64(buf[:])) / float64(^uint64(0)) // 0.0 to 1.0
			jitter := (randFloat*2 - 1) * jitterAmount                                     // Random between -jitter and +jitter
			delay += jitter
		}
	}

	return time.Duration(delay)
}

// RetryPolicy defines different retry policies for different scenarios
type RetryPolicy int

const (
	// RetryPolicyFast for fast operations (low latency requirement)
	RetryPolicyFast RetryPolicy = iota

	// RetryPolicyStandard for standard operations (balanced)
	RetryPolicyStandard

	// RetryPolicyAggressive for critical operations (high reliability)
	RetryPolicyAggressive

	// RetryPolicyBackground for background operations (can wait longer)
	RetryPolicyBackground
)

// GetRetryConfig returns retry config for a policy
func GetRetryConfig(policy RetryPolicy) *RetryConfig {
	switch policy {
	case RetryPolicyFast:
		return &RetryConfig{
			MaxAttempts:  2,
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     1 * time.Second,
			Multiplier:   1.5,
			Jitter:       0.1,
		}

	case RetryPolicyStandard:
		return DefaultRetryConfig()

	case RetryPolicyAggressive:
		return &RetryConfig{
			MaxAttempts:  5,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     60 * time.Second,
			Multiplier:   2.0,
			Jitter:       0.2,
		}

	case RetryPolicyBackground:
		return &RetryConfig{
			MaxAttempts:  10,
			InitialDelay: 1 * time.Second,
			MaxDelay:     5 * time.Minute,
			Multiplier:   2.0,
			Jitter:       0.3,
		}

	default:
		return DefaultRetryConfig()
	}
}

// RetriableError wraps an error to mark it as retriable
type RetriableError struct {
	Err error
}

func (e *RetriableError) Error() string {
	return e.Err.Error()
}

func (e *RetriableError) Unwrap() error {
	return e.Err
}

// IsRetriable checks if an error is marked as retriable
func IsRetriable(err error) bool {
	var re *RetriableError
	return errors.As(err, &re)
}

// NonRetriableError wraps an error to mark it as non-retriable
type NonRetriableError struct {
	Err error
}

func (e *NonRetriableError) Error() string {
	return e.Err.Error()
}

func (e *NonRetriableError) Unwrap() error {
	return e.Err
}

// IsNonRetriable checks if an error is marked as non-retriable
func IsNonRetriable(err error) bool {
	var nre *NonRetriableError
	return errors.As(err, &nre)
}
