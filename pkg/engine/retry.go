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
	config = normalizeRetryConfig(config)

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

	if delay < 0 {
		delay = 0
	}

	return time.Duration(delay)
}

func normalizeRetryConfig(config *RetryConfig) *RetryConfig {
	defaults := DefaultRetryConfig()
	if config == nil {
		return defaults
	}

	normalized := *config
	if normalized.MaxAttempts <= 0 {
		normalized.MaxAttempts = defaults.MaxAttempts
	}
	if normalized.InitialDelay < 0 {
		normalized.InitialDelay = defaults.InitialDelay
	}
	if normalized.MaxDelay <= 0 {
		normalized.MaxDelay = defaults.MaxDelay
	}
	if normalized.Multiplier <= 0 {
		normalized.Multiplier = defaults.Multiplier
	}
	if normalized.Jitter < 0 {
		normalized.Jitter = 0
	} else if normalized.Jitter > 1 {
		normalized.Jitter = 1
	}

	return &normalized
}
