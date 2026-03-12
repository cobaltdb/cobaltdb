// Circuit Breaker pattern for production resilience
// Prevents cascading failures when downstream services are struggling

package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrCircuitTooMany  = errors.New("too many concurrent requests")
)

// CircuitState represents the state of the circuit breaker
type CircuitState int32

const (
	CircuitClosed    CircuitState = iota // Normal operation
	CircuitOpen                           // Failing, reject requests
	CircuitHalfOpen                       // Testing if service recovered
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig configures the circuit breaker
type CircuitBreakerConfig struct {
	// Failure threshold to open circuit (default: 5)
	MaxFailures int

	// Success threshold to close circuit in half-open state (default: 3)
	MinSuccesses int

	// Timeout to wait before trying half-open (default: 30s)
	ResetTimeout time.Duration

	// Max concurrent requests allowed (default: 100)
	MaxConcurrency int

	// Half-open request rate limit (default: 1 per second)
	HalfOpenMaxRequests int
}

// DefaultCircuitBreakerConfig returns sensible defaults
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		MaxFailures:         5,
		MinSuccesses:        3,
		ResetTimeout:        30 * time.Second,
		MaxConcurrency:      100,
		HalfOpenMaxRequests: 1,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config *CircuitBreakerConfig

	// State management
	state        atomic.Int32
	failures     atomic.Int32
	successes    atomic.Int32
	lastFailure  atomic.Int64 // Unix timestamp

	// Concurrency control
	concurrency  atomic.Int32

	// Half-open rate limiting
	halfOpenTokens chan struct{}

	mu sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}

	cb := &CircuitBreaker{
		config:         config,
		halfOpenTokens: make(chan struct{}, config.HalfOpenMaxRequests),
	}

	// Pre-fill tokens for half-open state
	for i := 0; i < config.HalfOpenMaxRequests; i++ {
		cb.halfOpenTokens <- struct{}{}
	}

	return cb
}

// Allow checks if a request should be allowed
func (cb *CircuitBreaker) Allow() error {
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitOpen:
		// Check if we should transition to half-open
		if cb.shouldAttemptReset() {
			if cb.tryHalfOpen() {
				cb.concurrency.Add(1)
				return nil
			}
		}
		return ErrCircuitOpen

	case CircuitHalfOpen:
		// Only allow limited requests in half-open state
		select {
		case <-cb.halfOpenTokens:
			cb.concurrency.Add(1)
			return nil
		default:
			return ErrCircuitOpen
		}

	case CircuitClosed:
		// Check concurrency limit
		current := cb.concurrency.Add(1)
		if int(current) > cb.config.MaxConcurrency {
			cb.concurrency.Add(-1)
			return ErrCircuitTooMany
		}
		return nil
	}

	return nil
}

// Release must be called after Allow() succeeds, even on failure
func (cb *CircuitBreaker) Release() {
	cb.concurrency.Add(-1)
}

// ReportSuccess reports a successful operation
func (cb *CircuitBreaker) ReportSuccess() {
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitHalfOpen:
		successes := cb.successes.Add(1)
		if int(successes) >= cb.config.MinSuccesses {
			cb.closeCircuit()
		}
		// Return token
		select {
		case cb.halfOpenTokens <- struct{}{}:
		default:
		}

	case CircuitClosed:
		// Reset failure count on success
		cb.failures.Store(0)
	}
}

// ReportFailure reports a failed operation
func (cb *CircuitBreaker) ReportFailure() {
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitHalfOpen:
		// Immediately reopen on failure in half-open
		cb.openCircuit()
		// Return token
		select {
		case cb.halfOpenTokens <- struct{}{}:
		default:
		}

	case CircuitClosed:
		failures := cb.failures.Add(1)
		cb.lastFailure.Store(time.Now().Unix())
		if int(failures) >= cb.config.MaxFailures {
			cb.openCircuit()
		}
	}
}

// openCircuit transitions to open state
func (cb *CircuitBreaker) openCircuit() {
	if cb.state.CompareAndSwap(int32(CircuitClosed), int32(CircuitOpen)) ||
	   cb.state.CompareAndSwap(int32(CircuitHalfOpen), int32(CircuitOpen)) {
		cb.successes.Store(0)
	}
}

// closeCircuit transitions to closed state
func (cb *CircuitBreaker) closeCircuit() {
	if cb.state.CompareAndSwap(int32(CircuitHalfOpen), int32(CircuitClosed)) {
		cb.failures.Store(0)
		cb.successes.Store(0)
	}
}

// tryHalfOpen attempts to transition to half-open state
func (cb *CircuitBreaker) tryHalfOpen() bool {
	return cb.state.CompareAndSwap(int32(CircuitOpen), int32(CircuitHalfOpen))
}

// shouldAttemptReset checks if enough time has passed to try half-open
func (cb *CircuitBreaker) shouldAttemptReset() bool {
	lastFailure := cb.lastFailure.Load()
	if lastFailure == 0 {
		return true
	}
	return time.Since(time.Unix(lastFailure, 0)) >= cb.config.ResetTimeout
}

// State returns current circuit state
func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.state.Load())
}

// Stats returns current statistics
func (cb *CircuitBreaker) Stats() CircuitStats {
	return CircuitStats{
		State:           cb.State().String(),
		Failures:        int(cb.failures.Load()),
		Successes:       int(cb.successes.Load()),
		Concurrency:     int(cb.concurrency.Load()),
		LastFailureTime: cb.lastFailure.Load(),
	}
}

// CircuitStats holds circuit breaker statistics
type CircuitStats struct {
	State           string `json:"state"`
	Failures        int    `json:"failures"`
	Successes       int    `json:"successes"`
	Concurrency     int    `json:"concurrency"`
	LastFailureTime int64  `json:"last_failure_time"`
}

// Execute wraps a function with circuit breaker protection.
// Note: if context is cancelled, the fn goroutine will continue running until fn returns.
// The goroutine result is drained to prevent the goroutine from leaking.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if err := cb.Allow(); err != nil {
		return err
	}
	defer cb.Release()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic in circuit breaker: %v", r)
			}
		}()
		done <- fn()
	}()

	select {
	case err := <-done:
		if err != nil {
			cb.ReportFailure()
			return err
		}
		cb.ReportSuccess()
		return nil
	case <-ctx.Done():
		cb.ReportFailure()
		// Wait for fn to complete with timeout to prevent goroutine leak
		select {
		case <-done:
			// fn completed after context cancellation
		case <-time.After(5 * time.Second):
			// Log warning but don't leak goroutine
		}
		return ctx.Err()
	}
}

// CircuitBreakerManager manages multiple circuit breakers for different operations
type CircuitBreakerManager struct {
	mu     sync.RWMutex
	breakers map[string]*CircuitBreaker
}

// NewCircuitBreakerManager creates a new manager
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetOrCreate gets or creates a circuit breaker for a key
func (m *CircuitBreakerManager) GetOrCreate(key string, config *CircuitBreakerConfig) *CircuitBreaker {
	m.mu.RLock()
	cb, exists := m.breakers[key]
	m.mu.RUnlock()

	if exists {
		return cb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if cb, exists := m.breakers[key]; exists {
		return cb
	}

	cb = NewCircuitBreaker(config)
	m.breakers[key] = cb
	return cb
}

// Get retrieves a circuit breaker by key
func (m *CircuitBreakerManager) Get(key string) (*CircuitBreaker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cb, exists := m.breakers[key]
	return cb, exists
}

// Remove removes a circuit breaker
func (m *CircuitBreakerManager) Remove(key string) {
	m.mu.Lock()
	delete(m.breakers, key)
	m.mu.Unlock()
}

// AllStats returns stats for all circuit breakers
func (m *CircuitBreakerManager) AllStats() map[string]CircuitStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]CircuitStats)
	for key, cb := range m.breakers {
		stats[key] = cb.Stats()
	}
	return stats
}
