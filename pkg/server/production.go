package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// ProductionStats holds server statistics
type ProductionStats struct {
	IsRunning bool   `json:"is_running"`
	IsHealthy bool   `json:"is_healthy"`
	State     string `json:"state"`
}

// ProductionConfig holds configuration for the production server
type ProductionConfig struct {
	Lifecycle            *LifecycleConfig
	CircuitBreaker       *engine.CircuitBreakerConfig
	Retry                *engine.RetryConfig
	HealthAddr           string
	EnableCircuitBreaker bool
	EnableRetry          bool
	EnableRateLimiter    bool
	EnableSQLProtection  bool
	EnableHealthServer   bool
}

// DefaultProductionConfig returns a default production configuration
func DefaultProductionConfig() *ProductionConfig {
	return &ProductionConfig{
		Lifecycle: &LifecycleConfig{
			ShutdownTimeout:     30 * time.Second,
			DrainTimeout:        10 * time.Second,
			HealthCheckInterval: 10 * time.Second,
			StartupTimeout:      30 * time.Second,
		},
		CircuitBreaker:       engine.DefaultCircuitBreakerConfig(),
		Retry:                engine.DefaultRetryConfig(),
		HealthAddr:           ":8420",
		EnableCircuitBreaker: true,
		EnableRetry:          true,
		EnableRateLimiter:    false,
		EnableSQLProtection:  false,
		EnableHealthServer:   true,
	}
}

// ProductionServer provides production-ready features
type ProductionServer struct {
	DB              *engine.DB
	Config          *ProductionConfig
	Lifecycle       *Lifecycle
	CircuitBreaker  *engine.CircuitBreaker
	CircuitBreakers *engine.CircuitBreakerManager
	RateLimiter     *RateLimiter
	SQLProtector    *SQLProtector
	healthServer    *http.Server
	mu              sync.RWMutex
	running         bool
	wg              sync.WaitGroup
}

// NewProductionServer creates a new production server
func NewProductionServer(db *engine.DB, config *ProductionConfig) *ProductionServer {
	if config == nil {
		config = DefaultProductionConfig()
	}

	ps := &ProductionServer{
		DB:        db,
		Config:    config,
		Lifecycle: NewLifecycle(config.Lifecycle),
	}

	if config.EnableCircuitBreaker {
		ps.CircuitBreaker = engine.NewCircuitBreaker(config.CircuitBreaker)
		ps.CircuitBreakers = engine.NewCircuitBreakerManager()
	}

	if config.EnableRateLimiter {
		ps.RateLimiter = NewRateLimiter(DefaultRateLimiterConfig())
	}

	if config.EnableSQLProtection {
		ps.SQLProtector = NewSQLProtector(DefaultSQLProtectionConfig())
	}

	return ps
}

// Start starts the production server
func (ps *ProductionServer) Start() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.running {
		return fmt.Errorf("server already running")
	}

	ps.running = true

	// Start lifecycle first
	if err := ps.Lifecycle.Start(); err != nil {
		ps.running = false
		return fmt.Errorf("failed to start lifecycle: %w", err)
	}

	// Start health check server if enabled
	if ps.Config.EnableHealthServer && ps.Config.HealthAddr != "" {
		ps.startHealthServer()
	}

	return nil
}

// startHealthServer starts the health check HTTP server
func (ps *ProductionServer) startHealthServer() {
	mux := http.NewServeMux()

	// Health endpoints
	mux.HandleFunc("/health", ps.healthHandler())
	mux.HandleFunc("/ready", ps.readyHandler())
	mux.HandleFunc("/healthz", ps.healthzHandler())

	// Stats endpoints
	mux.HandleFunc("/stats", ps.statsHandler())
	mux.HandleFunc("/circuit-breakers", ps.circuitBreakerHandler())
	mux.HandleFunc("/rate-limits", ps.rateLimitsHandler())

	ps.healthServer = &http.Server{
		Addr:         ps.Config.HealthAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		if err := ps.healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("health server stopped with error: %v\n", err)
		}
	}()
}

// Wait waits for the server to be signaled to stop
func (ps *ProductionServer) Wait() {
	ps.Lifecycle.Wait()
}

// Stop gracefully stops the production server
func (ps *ProductionServer) Stop() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if !ps.running {
		return nil
	}

	ps.running = false

	// Shutdown health server
	if ps.healthServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ps.healthServer.Shutdown(ctx); err != nil {
			fmt.Printf("health server shutdown error: %v\n", err)
		}
	}

	// Stop lifecycle
	if err := ps.Lifecycle.Stop(); err != nil {
		return err
	}

	// Wait for goroutines
	ps.wg.Wait()

	return nil
}

// IsHealthy returns true if the server is healthy
func (ps *ProductionServer) IsHealthy() bool {
	return ps.Lifecycle.IsRunning()
}

// IsRunning returns true if the server is running
func (ps *ProductionServer) IsRunning() bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.running
}

// ExecuteWithRetry executes a function with retry logic
func (ps *ProductionServer) ExecuteWithRetry(ctx context.Context, fn func() error) error {
	if ps.Config.Retry == nil {
		return fn()
	}
	return engine.Retry(ctx, ps.Config.Retry, fn)
}

// ExecuteWithCircuitBreaker executes a function with circuit breaker protection
func (ps *ProductionServer) ExecuteWithCircuitBreaker(key string, fn func() error) error {
	if ps.CircuitBreakers == nil {
		return fn()
	}
	cb := ps.CircuitBreakers.GetOrCreate(key, ps.Config.CircuitBreaker)
	if err := cb.Allow(); err != nil {
		return err
	}
	defer cb.Release()

	err := fn()
	if err != nil {
		cb.ReportFailure()
	} else {
		cb.ReportSuccess()
	}
	return err
}

// GetStats returns server statistics
func (ps *ProductionServer) GetStats() ProductionStats {
	return ProductionStats{
		IsRunning: ps.IsRunning(),
		IsHealthy: ps.IsHealthy(),
		State:     ps.Lifecycle.State().String(),
	}
}

// HTTP handlers
func (ps *ProductionServer) healthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"status":"healthy"}`)); err != nil {
			http.Error(w, "failed to write health", http.StatusInternalServerError)
		}
	}
}

func (ps *ProductionServer) readyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if ps.IsHealthy() {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"ready":true}`)); err != nil {
				http.Error(w, "failed to write ready", http.StatusInternalServerError)
			}
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte(`{"ready":false}`)); err != nil {
				http.Error(w, "failed to write ready", http.StatusInternalServerError)
			}
		}
	}
}

func (ps *ProductionServer) healthzHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		state := ps.Lifecycle.State()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"state":"%s","healthy":%t}`, state.String(), ps.IsHealthy())
	}
}

func (ps *ProductionServer) statsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"stats":{}}`)); err != nil {
			http.Error(w, "failed to write stats", http.StatusInternalServerError)
		}
	}
}

func (ps *ProductionServer) circuitBreakerHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if ps.CircuitBreaker == nil {
			http.Error(w, "Circuit breaker disabled", http.StatusServiceUnavailable)
			return
		}
		stats := map[string]interface{}{
			"enabled": true,
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, "failed to encode stats", http.StatusInternalServerError)
		}
	}
}

func (ps *ProductionServer) rateLimitsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"rate_limits":{}}`)); err != nil {
			http.Error(w, "failed to write rate limits", http.StatusInternalServerError)
		}
	}
}

// authRequiredHandler wraps a handler with admin token authentication
func (ps *ProductionServer) authRequiredHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// In production, this should verify the admin token
		// For now, just restrict to localhost
		host := r.RemoteAddr
		if !strings.HasPrefix(host, "127.0.0.1:") && !strings.HasPrefix(host, "[::1]:") {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}
