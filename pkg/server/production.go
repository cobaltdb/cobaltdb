package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
	"github.com/cobaltdb/cobaltdb/pkg/metrics"
)

const (
	productionHTTPMaxHeaderBytes     = 1 << 20
	maxAdminTokenBytes               = 1024
	maxAdminAuthorizationHeaderBytes = len("Bearer ") + maxAdminTokenBytes
)

func adminTokenFromAuthorizationHeader(authHeader string) (string, bool) {
	if authHeader == "" || len(authHeader) > maxAdminAuthorizationHeaderBytes {
		return "", false
	}

	providedToken := authHeader
	if parts := strings.SplitN(authHeader, " ", 2); len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		providedToken = parts[1]
	}
	if providedToken == "" || len(providedToken) > maxAdminTokenBytes {
		return "", false
	}
	return providedToken, true
}

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
	RateLimiter          *RateLimiterConfig
	Retry                *engine.RetryConfig
	HealthAddr           string
	EnableCircuitBreaker bool
	EnableRetry          bool
	EnableRateLimiter    bool
	EnableSQLProtection  bool
	EnableHealthServer   bool
	AllowRemoteMetrics   bool
	AdminToken           string
	Logger               *logger.Logger
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
		RateLimiter:          DefaultRateLimiterConfig(),
		Retry:                engine.DefaultRetryConfig(),
		HealthAddr:           "127.0.0.1:8420",
		EnableCircuitBreaker: true,
		EnableRetry:          true,
		EnableRateLimiter:    false,
		EnableSQLProtection:  false,
		EnableHealthServer:   true,
		AllowRemoteMetrics:   false,
	}
}

// ProductionServer provides production-ready features
type ProductionServer struct {
	db               *engine.DB
	Config           *ProductionConfig
	Lifecycle        *Lifecycle
	CircuitBreaker   *engine.CircuitBreaker
	CircuitBreakers  *engine.CircuitBreakerManager
	RateLimiter      *RateLimiter
	SQLProtector     *SQLProtector
	healthServer     *http.Server
	logger           *logger.Logger
	adminTokenDigest [sha256.Size]byte
	adminTokenSet    bool
	mu               sync.RWMutex
	running          bool
	wg               sync.WaitGroup
}

// NewProductionServer creates a new production server
func NewProductionServer(db *engine.DB, config *ProductionConfig) *ProductionServer {
	config = cloneProductionConfig(config)
	adminToken := config.AdminToken
	config.AdminToken = ""
	if config.Logger != nil {
		if config.Lifecycle == nil {
			config.Lifecycle = DefaultLifecycleConfig()
		}
		if config.Lifecycle.Logger == nil {
			lifecycleConfig := *config.Lifecycle
			lifecycleConfig.Logger = config.Logger
			config.Lifecycle = &lifecycleConfig
		}
	}

	ps := &ProductionServer{
		db:        db,
		Config:    config,
		Lifecycle: NewLifecycle(config.Lifecycle),
		logger:    config.Logger,
	}
	ps.SetAdminToken(adminToken)

	if config.EnableCircuitBreaker {
		ps.CircuitBreaker = engine.NewCircuitBreaker(config.CircuitBreaker)
		ps.CircuitBreakers = engine.NewCircuitBreakerManager()
	}

	if config.EnableRateLimiter {
		rateLimiterConfig := cloneRateLimiterConfig(config.RateLimiter)
		if rateLimiterConfig == nil {
			rateLimiterConfig = DefaultRateLimiterConfig()
		}
		if rateLimiterConfig.Logger == nil {
			rateLimiterConfig.Logger = config.Logger
		}
		ps.RateLimiter = NewRateLimiter(rateLimiterConfig)
	}

	if config.EnableSQLProtection {
		ps.SQLProtector = NewSQLProtector(DefaultSQLProtectionConfig())
	}

	return ps
}

// SetAdminToken configures the admin API token without retaining the raw secret.
func (ps *ProductionServer) SetAdminToken(token string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	token = strings.TrimSpace(token)
	if token == "" || len(token) > maxAdminTokenBytes {
		ps.adminTokenDigest = [sha256.Size]byte{}
		ps.adminTokenSet = false
		return
	}
	ps.adminTokenDigest = adminTokenDigest(token)
	ps.adminTokenSet = true
}

func cloneProductionConfig(config *ProductionConfig) *ProductionConfig {
	if config == nil {
		config = DefaultProductionConfig()
	}

	cloned := *config
	cloned.Lifecycle = cloneLifecycleConfig(config.Lifecycle)
	cloned.CircuitBreaker = cloneCircuitBreakerConfig(config.CircuitBreaker)
	cloned.RateLimiter = cloneRateLimiterConfig(config.RateLimiter)
	cloned.Retry = cloneRetryConfig(config.Retry)
	return &cloned
}

func cloneLifecycleConfig(config *LifecycleConfig) *LifecycleConfig {
	if config == nil {
		return nil
	}
	return normalizeLifecycleConfig(config)
}

func cloneCircuitBreakerConfig(config *engine.CircuitBreakerConfig) *engine.CircuitBreakerConfig {
	if config == nil {
		return nil
	}
	cloned := *config
	return &cloned
}

func cloneRateLimiterConfig(config *RateLimiterConfig) *RateLimiterConfig {
	if config == nil {
		return nil
	}
	cloned := *config
	return &cloned
}

func cloneRetryConfig(config *engine.RetryConfig) *engine.RetryConfig {
	if config == nil {
		return nil
	}
	cloned := *config
	cloned.RetryableErrors = append([]error(nil), config.RetryableErrors...)
	cloned.NonRetryableErrors = append([]error(nil), config.NonRetryableErrors...)
	return &cloned
}

func (ps *ProductionServer) logErrorf(format string, args ...interface{}) {
	if ps != nil && ps.logger != nil {
		ps.logger.Errorf(format, args...)
	}
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
		if err := ps.startHealthServer(); err != nil {
			ps.running = false
			if stopErr := ps.Lifecycle.Stop(); stopErr != nil {
				ps.stopRateLimiter()
				return fmt.Errorf("failed to start health server: %w; lifecycle stop failed: %v", err, stopErr)
			}
			ps.stopRateLimiter()
			return fmt.Errorf("failed to start health server: %w", err)
		}
	}

	return nil
}

// startHealthServer starts the health check HTTP server
func (ps *ProductionServer) startHealthServer() error {
	if ps.Config.AllowRemoteMetrics && !ps.adminTokenSet {
		return fmt.Errorf("remote metrics require an admin token")
	}

	ps.healthServer = &http.Server{
		Addr:              ps.Config.HealthAddr,
		Handler:           ps.healthMux(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    productionHTTPMaxHeaderBytes,
	}

	listener, err := net.Listen("tcp", ps.Config.HealthAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", ps.Config.HealthAddr, err)
	}

	ps.wg.Add(1)
	go func() {
		defer ps.wg.Done()
		if err := ps.healthServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			ps.logErrorf("health server stopped with error: %v", err)
		}
	}()

	return nil
}

func (ps *ProductionServer) healthMux() http.Handler {
	mux := http.NewServeMux()

	// Health endpoints
	mux.HandleFunc("/health", ps.healthHandler())
	mux.HandleFunc("/ready", ps.readyHandler())
	mux.HandleFunc("/healthz", ps.healthzHandler())

	// Admin endpoints
	mux.HandleFunc("/stats", ps.authRequiredHandler(ps.statsHandler()))
	mux.HandleFunc("/circuit-breakers", ps.authRequiredHandler(ps.circuitBreakerHandler()))
	mux.HandleFunc("/rate-limits", ps.authRequiredHandler(ps.rateLimitsHandler()))
	mux.HandleFunc("/transaction-metrics", ps.authRequiredHandler(ps.transactionMetricsHandler()))
	mux.HandleFunc("/metrics/prometheus", ps.prometheusMetricsHandler())

	return ps.rateLimitHandler(mux)
}

func (ps *ProductionServer) rateLimitHandler(next http.Handler) http.Handler {
	if ps == nil || ps.RateLimiter == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := r.Header.Get(ps.RateLimiter.config.ClientHeader)
		if clientID == "" {
			if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
				clientID = host
			} else {
				clientID = r.RemoteAddr
			}
		}
		if !ps.RateLimiter.Allow(clientID) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
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
		ps.stopRateLimiter()
		return nil
	}

	ps.running = false

	// Shutdown health server
	var shutdownErr error
	if ps.healthServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ps.healthServer.Shutdown(ctx); err != nil {
			shutdownErr = fmt.Errorf("health server shutdown: %w", err)
		}
	}

	// Stop lifecycle
	var lifecycleErr error
	if err := ps.Lifecycle.Stop(); err != nil {
		lifecycleErr = err
	}

	ps.stopRateLimiter()

	// Wait for goroutines
	ps.wg.Wait()

	return errors.Join(shutdownErr, lifecycleErr)
}

func (ps *ProductionServer) stopRateLimiter() {
	if ps.RateLimiter != nil {
		ps.RateLimiter.Stop()
	}
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

// DB returns the underlying database instance. Use for operations that should
// bypass circuit breaker / retry (e.g., internal abort, recovery).
func (ps *ProductionServer) DB() *engine.DB {
	return ps.db
}

// circuitBreakerKey returns the circuit breaker key for a SQL statement.
func (ps *ProductionServer) circuitBreakerKey(sql string) string {
	// Use the first meaningful word of the SQL as the key so statements of
	// the same type share a circuit breaker (e.g., all SELECTs share one,
// all INSERTs share one, etc.). This prevents a slow DELETE from killing
// all other queries.
	if len(sql) == 0 {
		return "unknown"
	}
	i := 0
	for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r') {
		i++
	}
	end := i
	for end < len(sql) && sql[end] > ' ' {
		end++
	}
	key := sql[i:end]
	if key == "" {
		return "unknown"
	}
	return key
}

// Exec executes a SQL statement with circuit breaker and retry protection.
func (ps *ProductionServer) Exec(ctx context.Context, sql string, args ...interface{}) (engine.Result, error) {
	key := ps.circuitBreakerKey(sql)
	var result engine.Result
	err := ps.ExecuteWithCircuitBreaker(key, func() error {
		return ps.ExecuteWithRetry(ctx, func() error {
			res, err := ps.db.Exec(ctx, sql, args...)
			if err == nil {
				result = res
			}
			return err
		})
	})
	return result, err
}

// Query executes a SQL query with circuit breaker and retry protection.
func (ps *ProductionServer) Query(ctx context.Context, sql string, args ...interface{}) (*engine.Rows, error) {
	key := ps.circuitBreakerKey(sql)
	var rows *engine.Rows
	err := ps.ExecuteWithCircuitBreaker(key, func() error {
		return ps.ExecuteWithRetry(ctx, func() error {
			r, err := ps.db.Query(ctx, sql, args...)
			if err == nil {
				rows = r
			}
			return err
		})
	})
	return rows, err
}

// QueryRow executes a single-row SQL query with circuit breaker and retry protection.
func (ps *ProductionServer) QueryRow(ctx context.Context, sql string, args ...interface{}) (*engine.Row, error) {
	key := ps.circuitBreakerKey(sql)
	var row *engine.Row
	err := ps.ExecuteWithCircuitBreaker(key, func() error {
		return ps.ExecuteWithRetry(ctx, func() error {
			r := ps.db.QueryRow(ctx, sql, args...)
			row = r
			return nil
		})
	})
	return row, err
}

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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"status":"healthy"}`)); err != nil {
			ps.logErrorf("failed to write health response: %v", err)
		}
	}
}

func (ps *ProductionServer) readyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if ps.IsHealthy() {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"ready":true}`)); err != nil {
				ps.logErrorf("failed to write ready response: %v", err)
			}
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte(`{"ready":false}`)); err != nil {
				ps.logErrorf("failed to write ready response: %v", err)
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
			ps.logErrorf("failed to write stats response: %v", err)
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
			ps.logErrorf("failed to encode circuit breaker stats: %v", err)
		}
	}
}

func (ps *ProductionServer) rateLimitsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if ps.RateLimiter == nil {
			http.Error(w, "Rate limiter disabled", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ps.RateLimiter.GetStats()); err != nil {
			ps.logErrorf("failed to encode rate limits response: %v", err)
		}
	}
}

func (ps *ProductionServer) transactionMetricsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		stats := metrics.GetTransactionMetrics().GetStats()

		if err := json.NewEncoder(w).Encode(stats); err != nil {
			ps.logErrorf("failed to encode transaction metrics: %v", err)
		}
	}
}

func (ps *ProductionServer) prometheusMetricsHandler() http.HandlerFunc {
	handler := metrics.GetPrometheusHandler()
	if ps.Config != nil && ps.Config.AllowRemoteMetrics {
		return ps.adminTokenRequiredHandler(handler, true)
	}
	return ps.loopbackOnly(handler)
}

// loopbackOnly restricts access to loopback addresses only
func (ps *ProductionServer) loopbackOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !isLoopbackRemoteAddr(r.RemoteAddr) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// authRequiredHandler wraps a handler with admin token authentication.
//
//nolint:unused // retained for production server compatibility tests.
func (ps *ProductionServer) authRequiredHandler(next http.HandlerFunc) http.HandlerFunc {
	return ps.adminTokenRequiredHandler(next, false)
}

func (ps *ProductionServer) adminTokenRequiredHandler(next http.HandlerFunc, allowRemote bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !allowRemote && !isLoopbackRemoteAddr(r.RemoteAddr) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		ps.mu.RLock()
		tokenDigest := ps.adminTokenDigest
		tokenSet := ps.adminTokenSet
		ps.mu.RUnlock()
		if !tokenSet {
			http.Error(w, "admin endpoint disabled until admin token configured", http.StatusServiceUnavailable)
			return
		}

		providedToken, ok := adminTokenFromAuthorizationHeader(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		providedDigest := adminTokenDigest(providedToken)
		if subtle.ConstantTimeCompare(providedDigest[:], tokenDigest[:]) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

func isLoopbackRemoteAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func adminTokenEqual(provided, expected string) bool {
	providedDigest := adminTokenDigest(provided)
	expectedDigest := adminTokenDigest(expected)
	return subtle.ConstantTimeCompare(providedDigest[:], expectedDigest[:]) == 1
}

func adminTokenDigest(token string) [sha256.Size]byte {
	var lengthPrefix [8]byte
	binary.BigEndian.PutUint64(lengthPrefix[:], uint64(len(token)))

	hasher := sha256.New()
	_, _ = hasher.Write(lengthPrefix[:])
	_, _ = hasher.Write([]byte(token))

	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}
