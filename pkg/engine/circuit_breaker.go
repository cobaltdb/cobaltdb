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
	ErrCircuitOpen    = errors.New("circuit breaker is open")
	ErrCircuitTooMany = errors.New("too many concurrent requests")
)

// CircuitState represents the state of the circuit breaker
type CircuitState int32

const (
	CircuitClosed   CircuitState = iota // Normal operation
	CircuitOpen                         // Failing, reject requests
	CircuitHalfOpen                     // Testing if service recovered
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
	state       atomic.Int32
	failures    atomic.Int32
	successes   atomic.Int32
	lastFailure atomic.Int64 // Unix timestamp in nanoseconds

	// Concurrency control
	concurrency atomic.Int32

	// Half-open rate limiting
	halfOpenTokens chan struct{}

	// stopped guards against operations after teardown.
	// When true, ReportSuccess/ReportFailure become no-ops,
	// preventing sends on halfOpenTokens after the breaker is stopped.
	stopped atomic.Bool
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	config = normalizeCircuitBreakerConfig(config)

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

func normalizeCircuitBreakerConfig(config *CircuitBreakerConfig) *CircuitBreakerConfig {
	defaults := DefaultCircuitBreakerConfig()
	if config == nil {
		return defaults
	}

	normalized := *config
	if normalized.MaxFailures <= 0 {
		normalized.MaxFailures = defaults.MaxFailures
	}
	if normalized.MinSuccesses <= 0 {
		normalized.MinSuccesses = defaults.MinSuccesses
	}
	if normalized.ResetTimeout <= 0 {
		normalized.ResetTimeout = defaults.ResetTimeout
	}
	if normalized.MaxConcurrency <= 0 {
		normalized.MaxConcurrency = defaults.MaxConcurrency
	}
	if normalized.HalfOpenMaxRequests <= 0 {
		normalized.HalfOpenMaxRequests = defaults.HalfOpenMaxRequests
	}

	return &normalized
}

// Allow checks if a request should be allowed
func (cb *CircuitBreaker) Allow() error {
	// The loop re-dispatches after an open→half-open transition so the
	// transitioning caller goes through the same token accounting as every
	// other half-open probe (it must consume one of the freshly refilled
	// tokens rather than getting a free pass). It terminates in at most a
	// few iterations: open either stays open (return) or moves forward to
	// half-open/closed, both of which return.
	for {
		state := CircuitState(cb.state.Load())

		switch state {
		case CircuitOpen:
			// Check if we should transition to half-open
			if !cb.shouldAttemptReset() {
				return ErrCircuitOpen
			}
			cb.tryHalfOpen() // refills half-open tokens on the winning CAS
			if CircuitState(cb.state.Load()) == CircuitOpen {
				return ErrCircuitOpen
			}
			continue // state advanced (half-open or closed); re-dispatch

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

		default:
			return nil
		}
	}
}

// Release must be called after Allow() succeeds, even on failure
func (cb *CircuitBreaker) Release() {
	cb.concurrency.Add(-1)
}

// ReportSuccess reports a successful operation
func (cb *CircuitBreaker) ReportSuccess() {
	if cb.stopped.Load() {
		return
	}
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitHalfOpen:
		successes := cb.successes.Add(1)
		if int(successes) >= cb.config.MinSuccesses {
			cb.closeCircuit()
			// Do NOT return the token: tokens are refilled wholesale on the
			// next transition into half-open (tryHalfOpen), so tokens that
			// are "lost" across a state change can never wedge the breaker.
			return
		}
		// Still half-open: return the token so further probes can proceed
		// (needed when MinSuccesses > HalfOpenMaxRequests).
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
	if cb.stopped.Load() {
		return
	}
	state := CircuitState(cb.state.Load())

	switch state {
	case CircuitHalfOpen:
		// Immediately reopen on failure in half-open. No token return is
		// needed: tokens are refilled on the next transition into half-open.
		cb.openCircuit()

	case CircuitClosed:
		failures := cb.failures.Add(1)
		cb.lastFailure.Store(time.Now().UnixNano())
		if int(failures) >= cb.config.MaxFailures {
			cb.openCircuit()
		}
	}
}

// Abandon releases a half-open probe slot without recording an outcome, for
// outcomes that say nothing about backend health (caller context
// cancellation). Without it the consumed probe token is never returned: with
// the default HalfOpenMaxRequests=1 the breaker wedges in half-open, because
// every later Allow is rejected for lack of a token and no probe can
// therefore ever run to produce the report that would move the state. The
// non-blocking send is bounded by the channel capacity and harmless in other
// states — refillHalfOpenTokens drains stale tokens on the next entry.
func (cb *CircuitBreaker) Abandon() {
	if cb.stopped.Load() {
		return
	}
	select {
	case cb.halfOpenTokens <- struct{}{}:
	default:
	}
}

// openCircuit transitions to open state.
// lastFailure is stamped on the transition so shouldAttemptReset enforces a
// full ResetTimeout backoff before the next half-open probe — including after
// a half-open probe failure, which previously reopened the circuit without
// updating lastFailure and allowed an immediate (backoff-free) retry.
func (cb *CircuitBreaker) openCircuit() {
	if cb.state.CompareAndSwap(int32(CircuitClosed), int32(CircuitOpen)) ||
		cb.state.CompareAndSwap(int32(CircuitHalfOpen), int32(CircuitOpen)) {
		cb.lastFailure.Store(time.Now().UnixNano())
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

// tryHalfOpen attempts to transition to half-open state.
// On the winning transition the half-open token bucket is refilled to
// exactly HalfOpenMaxRequests. Refilling on every transition (instead of
// relying on each token holder to return its token via ReportSuccess/
// ReportFailure) guarantees the breaker can never wedge in a permanently
// token-starved half-open state when a report lands after a state change.
func (cb *CircuitBreaker) tryHalfOpen() bool {
	if cb.state.CompareAndSwap(int32(CircuitOpen), int32(CircuitHalfOpen)) {
		cb.successes.Store(0)
		cb.refillHalfOpenTokens()
		return true
	}
	return false
}

// refillHalfOpenTokens drains and refills the half-open token bucket to its
// configured capacity. Called only from the CAS-guarded state transition into
// half-open, so refills cannot race each other; a concurrent stale token
// return from ReportSuccess is bounded by the channel capacity.
func (cb *CircuitBreaker) refillHalfOpenTokens() {
drain:
	for {
		select {
		case <-cb.halfOpenTokens:
		default:
			break drain
		}
	}
	for i := 0; i < cap(cb.halfOpenTokens); i++ {
		select {
		case cb.halfOpenTokens <- struct{}{}:
		default:
			return
		}
	}
}

// shouldAttemptReset checks if enough time has passed to try half-open
func (cb *CircuitBreaker) shouldAttemptReset() bool {
	lastFailure := cb.lastFailure.Load()
	if lastFailure == 0 {
		return true
	}
	// Nanosecond precision: second-granularity timestamps made sub-second
	// ResetTimeouts (and the first second of longer ones) effectively random.
	return time.Since(time.Unix(0, lastFailure)) >= cb.config.ResetTimeout
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
		LastFailureTime: cb.lastFailure.Load() / int64(time.Second), // Unix seconds for compatibility
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
//
// Caller context cancellation is NOT reported as a backend failure: it says
// nothing about the health of the protected resource, and counting it would
// let impatient clients trip the breaker for everyone. Only fn's own errors
// (including recovered panics) count as failures.
//
// If ctx is cancelled while fn is still running, Execute returns immediately
// with ctx.Err(); the goroutine running fn finishes on its own (the buffered
// done channel guarantees its final send never blocks) and its late result is
// discarded without affecting breaker state.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	// Fast-fail before consuming a breaker slot when the caller has already
	// given up, avoiding a pointless goroutine spawn.
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := cb.Allow(); err != nil {
		return err
	}

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
		cb.Release()
		if err != nil {
			cb.ReportFailure()
			return err
		}
		cb.ReportSuccess()
		return nil
	case <-ctx.Done():
		// Release the concurrency slot when fn eventually finishes, so a
		// slow backend still bounds true concurrent work; do not block the
		// cancelled caller waiting for it. Abandon the half-open probe slot
		// at the same time: the caller's cancellation says nothing about
		// backend health, and without Abandon the consumed probe token is
		// never returned, wedging a HalfOpenMaxRequests=1 breaker in
		// half-open forever (no probe can run to produce the report that
		// would move the state).
		go func() {
			<-done
			cb.Release()
			cb.Abandon()
		}()
		return ctx.Err()
	}
}

// Stop marks the circuit breaker as stopped. After Stop is called,
// ReportSuccess and ReportFailure become no-ops, preventing sends
// on the halfOpenTokens channel after teardown.
func (cb *CircuitBreaker) Stop() {
	cb.stopped.Store(true)
}

// circuitBreakerManagerMaxBreakers caps the number of distinct breakers a
// manager will track. Callers are expected to key breakers by a small, fixed
// set of operation classes; the cap is a defense-in-depth bound so untrusted
// or unbounded key material can never grow the map without limit.
const circuitBreakerManagerMaxBreakers = 128

// circuitBreakerOverflowKey is the shared breaker used for any key requested
// after the manager reached its capacity.
const circuitBreakerOverflowKey = "__overflow__"

// CircuitBreakerManager manages multiple circuit breakers for different operations
type CircuitBreakerManager struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
}

// NewCircuitBreakerManager creates a new manager
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetOrCreate gets or creates a circuit breaker for a key. When the manager
// already tracks circuitBreakerManagerMaxBreakers distinct keys, requests for
// new keys share a single overflow breaker instead of growing the map.
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

	// Enforce the size cap: route new keys to a shared overflow breaker.
	// (Reserve one slot for the overflow breaker itself.)
	if len(m.breakers) >= circuitBreakerManagerMaxBreakers-1 {
		if cb, exists := m.breakers[circuitBreakerOverflowKey]; exists {
			return cb
		}
		cb = NewCircuitBreaker(config)
		m.breakers[circuitBreakerOverflowKey] = cb
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
