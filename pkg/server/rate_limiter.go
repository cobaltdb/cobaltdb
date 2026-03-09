// Rate Limiter for production traffic control
// Prevents overload and ensures fair resource allocation

package server

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

// RateLimiterConfig configures rate limiting behavior
type RateLimiterConfig struct {
	// Requests per second (default: 1000)
	RPS int

	// Burst size for token bucket (default: 100)
	Burst int

	// Per-client rate limiting (default: true)
	PerClient bool

	// Client identification header (default: X-Client-ID)
	ClientHeader string

	// Cleanup interval for stale clients (default: 5m)
	CleanupInterval time.Duration

	// Max clients to track (default: 10000)
	MaxClients int
}

// DefaultRateLimiterConfig returns sensible defaults
func DefaultRateLimiterConfig() *RateLimiterConfig {
	return &RateLimiterConfig{
		RPS:             1000,
		Burst:           100,
		PerClient:       true,
		ClientHeader:    "X-Client-ID",
		CleanupInterval: 5 * time.Minute,
		MaxClients:      10000,
	}
}

// RateLimiter implements token bucket rate limiting
type RateLimiter struct {
	config *RateLimiterConfig

	// Global limiter
	global *tokenBucket

	// Per-client limiters
	clients   map[string]*clientLimiter
	clientsMu sync.RWMutex

	// Cleanup ticker
	cleanupTicker *time.Ticker
	stopCh        chan struct{}
	stopOnce      sync.Once
}

// tokenBucket implements the token bucket algorithm
type tokenBucket struct {
	rate       float64   // tokens per second
	burst      int       // maximum tokens
	tokens     float64   // current tokens
	lastUpdate time.Time // last time tokens were added
	mu         sync.Mutex
}

// clientLimiter tracks a client's rate limit and last access
type clientLimiter struct {
	bucket     *tokenBucket
	mu         sync.Mutex
	lastAccess time.Time
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(config *RateLimiterConfig) *RateLimiter {
	if config == nil {
		config = DefaultRateLimiterConfig()
	}

	rl := &RateLimiter{
		config: config,
		global: &tokenBucket{
			rate:       float64(config.RPS),
			burst:      config.Burst,
			tokens:     float64(config.Burst),
			lastUpdate: time.Now(),
		},
		clients:       make(map[string]*clientLimiter),
		cleanupTicker: time.NewTicker(config.CleanupInterval),
		stopCh:        make(chan struct{}),
	}

	// Start cleanup goroutine
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("recovered panic in rate limiter cleanup: %v", r)
			}
		}()
		rl.cleanupLoop()
	}()

	return rl
}

// Stop stops the rate limiter
func (rl *RateLimiter) Stop() {
	rl.stopOnce.Do(func() { close(rl.stopCh) })
	rl.cleanupTicker.Stop()
}

// Allow checks if a request should be allowed
func (rl *RateLimiter) Allow(clientID string) bool {
	// Check global limit first
	if !rl.global.allow() {
		return false
	}

	// Check per-client limit if enabled
	if rl.config.PerClient && clientID != "" {
		cl := rl.getClientLimiter(clientID)
		if !cl.bucket.allow() {
			return false
		}
		cl.mu.Lock()
		cl.lastAccess = time.Now()
		cl.mu.Unlock()
	}

	return true
}

// AllowN checks if n requests should be allowed
func (rl *RateLimiter) AllowN(clientID string, n int) bool {
	if !rl.global.allowN(n) {
		return false
	}

	if rl.config.PerClient && clientID != "" {
		cl := rl.getClientLimiter(clientID)
		if !cl.bucket.allowN(n) {
			return false
		}
		cl.mu.Lock()
		cl.lastAccess = time.Now()
		cl.mu.Unlock()
	}

	return true
}

// Wait blocks until the request is allowed or context is cancelled
func (rl *RateLimiter) Wait(ctx context.Context, clientID string) error {
	// Try immediate allow
	if rl.Allow(clientID) {
		return nil
	}

	// Wait for tokens
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if rl.Allow(clientID) {
				return nil
			}
		}
	}
}

// GetStats returns rate limiter statistics.
// Reads all float64 token/rate fields under proper lock protection (FIX-042).
func (rl *RateLimiter) GetStats() RateLimiterStats {
	rl.clientsMu.RLock()
	clientCount := len(rl.clients)
	rl.clientsMu.RUnlock()

	rl.global.mu.Lock()
	globalTokens := rl.global.tokens
	currentRate := int(rl.global.rate)
	rl.global.mu.Unlock()

	return RateLimiterStats{
		GlobalTokens: globalTokens,
		ClientCount:  clientCount,
		RPS:          currentRate,
		Burst:        rl.config.Burst,
	}
}

// getClientLimiter gets or creates a client limiter.
// Uses a single write lock for the entire check-and-create operation
// to avoid TOCTOU races on the MaxClients check (FIX-041).
func (rl *RateLimiter) getClientLimiter(clientID string) *clientLimiter {
	rl.clientsMu.Lock()
	defer rl.clientsMu.Unlock()

	if cl, exists := rl.clients[clientID]; exists {
		return cl
	}

	// Check max clients atomically with creation
	if len(rl.clients) >= rl.config.MaxClients {
		// Return global limiter as fallback
		return &clientLimiter{
			bucket:     rl.global,
			lastAccess: time.Now(),
		}
	}

	cl := &clientLimiter{
		bucket: &tokenBucket{
			rate:       float64(rl.config.RPS) / 10, // Each client gets 1/10th of global
			burst:      rl.config.Burst / 10,
			tokens:     float64(rl.config.Burst / 10),
			lastUpdate: time.Now(),
		},
		lastAccess: time.Now(),
	}
	rl.clients[clientID] = cl
	return cl
}

// cleanupLoop removes stale client limiters
func (rl *RateLimiter) cleanupLoop() {
	for {
		select {
		case <-rl.stopCh:
			return
		case <-rl.cleanupTicker.C:
			rl.cleanup()
		}
	}
}

// cleanup removes stale clients
func (rl *RateLimiter) cleanup() {
	cutoff := time.Now().Add(-rl.config.CleanupInterval)

	rl.clientsMu.Lock()
	defer rl.clientsMu.Unlock()

	for id, cl := range rl.clients {
		cl.mu.Lock()
		stale := cl.lastAccess.Before(cutoff)
		cl.mu.Unlock()
		if stale {
			delete(rl.clients, id)
		}
	}
}

// allow checks if one token is available
func (tb *tokenBucket) allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// allowN checks if n tokens are available
func (tb *tokenBucket) allowN(n int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}
	return false
}

// refill adds tokens based on elapsed time
func (tb *tokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastUpdate).Seconds()
	tb.lastUpdate = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > float64(tb.burst) {
		tb.tokens = float64(tb.burst)
	}
}

// RateLimiterStats holds rate limiter statistics
type RateLimiterStats struct {
	GlobalTokens float64 `json:"global_tokens"`
	ClientCount  int     `json:"client_count"`
	RPS          int     `json:"rps"`
	Burst        int     `json:"burst"`
}

// AdaptiveRateLimiter adjusts rate based on system load
type AdaptiveRateLimiter struct {
	*RateLimiter
	minRPS      int
	maxRPS      int
	loadMonitor func() float64 // returns 0.0-1.0 load
}

// NewAdaptiveRateLimiter creates an adaptive rate limiter
func NewAdaptiveRateLimiter(config *RateLimiterConfig, minRPS, maxRPS int, loadMonitor func() float64) *AdaptiveRateLimiter {
	return &AdaptiveRateLimiter{
		RateLimiter: NewRateLimiter(config),
		minRPS:      minRPS,
		maxRPS:      maxRPS,
		loadMonitor: loadMonitor,
	}
}

// Adjust adjusts the rate based on current load.
// Uses the global token bucket lock for both reading and writing shared
// state to avoid races with concurrent Allow/GetStats calls (FIX-043).
func (arl *AdaptiveRateLimiter) Adjust() {
	load := arl.loadMonitor()

	// Higher load = lower rate
	targetRPS := int(float64(arl.maxRPS-arl.minRPS) * (1 - load))
	if targetRPS < arl.minRPS {
		targetRPS = arl.minRPS
	}

	arl.global.mu.Lock()
	arl.global.rate = float64(targetRPS)
	// Cap current tokens to new burst if rate decreased significantly
	maxTokens := float64(arl.config.Burst)
	if arl.global.tokens > maxTokens {
		arl.global.tokens = maxTokens
	}
	arl.global.mu.Unlock()
}
