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

	"github.com/cobaltdb/cobaltdb/pkg/catalog"
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
	EnableLoadShedding   bool
	LoadShedQueueDepth   int
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
		Retry:                defaultProductionRetryConfig(),
		HealthAddr:           "127.0.0.1:8420",
		EnableCircuitBreaker: true,
		EnableRetry:          true,
		EnableRateLimiter:    false,
		EnableSQLProtection:  false,
		EnableHealthServer:   true,
		EnableLoadShedding:   true,
		LoadShedQueueDepth:   defaultLoadShedQueueDepth,
		AllowRemoteMetrics:   false,
	}
}

// defaultProductionRetryConfig returns the retry configuration used by the
// production server. In addition to the engine's built-in classification
// (context errors and deterministic parse/constraint/permission/schema errors
// are never retried), it pins well-known sentinel errors as non-retryable so
// the classification does not depend on message text alone.
func defaultProductionRetryConfig() *engine.RetryConfig {
	config := engine.DefaultRetryConfig()
	config.NonRetryableErrors = []error{
		context.Canceled,
		context.DeadlineExceeded,
		engine.ErrDatabaseClosed,
		catalog.ErrTableNotFound,
		catalog.ErrColumnNotFound,
		catalog.ErrTableExists,
		catalog.ErrIndexExists,
		catalog.ErrIndexNotFound,
	}
	return config
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
	LoadShedder      *LoadShedder
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
	if config.EnableLoadShedding {
		ps.LoadShedder = NewLoadShedder(db, ps.CircuitBreaker, ps.CircuitBreakers, config.LoadShedQueueDepth)
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

	return ps.loadShedHTTPHandler(ps.rateLimitHandler(mux))
}

func (ps *ProductionServer) loadShedHTTPHandler(next http.Handler) http.Handler {
	if ps == nil || ps.LoadShedder == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := ps.LoadShedder.Admit(true); err != nil {
			w.Header().Set("Retry-After", "1")
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
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

// Admit performs the shared production overload check used by every SQL
// transport (queue depth and goroutine safety limits). Circuit-breaker
// half-open probing is intentionally not a shed condition: the breakers
// bound their own probe concurrency and reject the excess with
// ErrCircuitOpen, so shedding here would only prevent recovery.
func (ps *ProductionServer) Admit(critical bool) error {
	if ps == nil || ps.LoadShedder == nil {
		return nil
	}
	return ps.LoadShedder.Admit(critical)
}

// circuitBreakerKey returns the circuit breaker key for a SQL statement.
// Keys come from a bounded, fixed set of statement classes (SELECT / INSERT /
// UPDATE / DELETE / DDL / OTHER) — never from raw client tokens — so a client
// cannot grow the breaker map or target an arbitrary breaker with crafted SQL.
// Statements of the same class share a breaker, so a struggling write path
// cannot open the read path's breaker and vice versa.
func (ps *ProductionServer) circuitBreakerKey(sql string) string {
	i := 0
	for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r' || sql[i] == '(') {
		i++
	}
	end := i
	for end < len(sql) && ((sql[end] >= 'a' && sql[end] <= 'z') || (sql[end] >= 'A' && sql[end] <= 'Z')) {
		end++
	}
	switch strings.ToUpper(sql[i:end]) {
	case "SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN":
		return "SELECT"
	case "INSERT", "REPLACE", "UPSERT":
		return "INSERT"
	case "UPDATE":
		return "UPDATE"
	case "DELETE":
		return "DELETE"
	case "CREATE", "ALTER", "DROP", "TRUNCATE":
		return "DDL"
	default:
		return "OTHER"
	}
}

// infrastructureFailureSubstrings identifies internal/infrastructure failures
// that indicate the backend itself is unhealthy. Only these (plus timeout
// sentinels) count toward the circuit breaker.
var infrastructureFailureSubstrings = []string{
	"timeout",
	"timed out",
	"unavailable",
	"storage",
	"buffer pool",
	"wal",
	"i/o error",
	"corrupt",
	"internal error",
	"out of memory",
	"disk",
}

// isCircuitBreakerFailure reports whether an error should count as a breaker
// failure. Client-caused errors — syntax/parse errors, unknown tables or
// columns, constraint violations, permission/RLS denials — say nothing about
// backend health; counting them would let a handful of malformed statements
// from any single client open the shared breaker and block all traffic of
// that statement class. Only infrastructure/internal failures (timeouts,
// storage errors, unavailability) count.
func isCircuitBreakerFailure(err error) bool {
	if err == nil {
		return false
	}
	// Caller cancellation is not a backend failure.
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, engine.ErrDatabaseClosed) {
		return true
	}
	// Client-side / deterministic errors never count.
	if errors.Is(err, catalog.ErrTableNotFound) || errors.Is(err, catalog.ErrColumnNotFound) ||
		errors.Is(err, catalog.ErrTableExists) || errors.Is(err, catalog.ErrIndexExists) ||
		errors.Is(err, catalog.ErrIndexNotFound) {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, sub := range infrastructureFailureSubstrings {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	// Default: do not count. Failing open on unclassified errors is the safe
	// direction for a shared breaker — an unhealthy backend will surface via
	// the positively classified infrastructure errors above.
	return false
}

// executeWithClassifiedBreaker is the breaker wrapper used on the SQL query
// path. Unlike the generic ExecuteWithCircuitBreaker (which counts every
// error), it only reports infrastructure failures to the breaker; client
// errors are reported as successes because the backend demonstrably processed
// the request.
func (ps *ProductionServer) executeWithClassifiedBreaker(key string, fn func() error) error {
	if ps.CircuitBreakers == nil {
		return fn()
	}
	cb := ps.CircuitBreakers.GetOrCreate(key, ps.Config.CircuitBreaker)
	if err := cb.Allow(); err != nil {
		return err
	}
	defer cb.Release()

	err := fn()
	switch {
	case err == nil:
		cb.ReportSuccess()
	case isCircuitBreakerFailure(err):
		cb.ReportFailure()
	case errors.Is(err, context.Canceled):
		// Caller gave up; no signal about backend health either way. Still
		// abandon the half-open probe slot: without Abandon the consumed
		// token is never returned and a HalfOpenMaxRequests=1 breaker wedges
		// in half-open forever.
		cb.Abandon()
	default:
		// Client-caused error: the backend responded, so this is evidence of
		// health (important in half-open, where the probe must be resolved).
		cb.ReportSuccess()
	}
	return err
}

// execWriteRetryable reports whether a failed non-idempotent statement (Exec)
// may be retried.
//
// Reasoning: an INSERT/UPDATE/DELETE that fails with a timeout may or may not
// have been applied — blindly retrying it risks double-applying the write.
// Therefore Exec retries only errors that are (a) positively classified as
// transient AND (b) known to occur BEFORE the statement touches any data
// (admission-stage failures), plus any errors the operator explicitly
// opted into via RetryConfig.RetryableErrors.
func (ps *ProductionServer) execWriteRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if ps.Config.Retry != nil {
		for _, r := range ps.Config.Retry.RetryableErrors {
			if errors.Is(err, r) {
				return true
			}
		}
	}
	// Admission-stage transient failures: these are returned before the
	// statement is parsed/executed, so the write is known not-applied.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection limit") ||
		strings.Contains(msg, "too many connections") ||
		strings.Contains(msg, "connection acquire timeout")
}

// errTransientWriteNotApplied is a retry-marker sentinel: Exec's retry config
// lists ONLY this sentinel as retryable, so engine.Retry re-attempts a write
// exclusively when the error was positively wrapped as transient-not-applied.
var errTransientWriteNotApplied = errors.New("transient write error (statement not applied)")

// transientWriteError marks a write error as safe to retry. It preserves the
// original error message and chain, and additionally matches the
// errTransientWriteNotApplied sentinel for retry classification.
type transientWriteError struct{ err error }

func (e *transientWriteError) Error() string { return e.err.Error() }
func (e *transientWriteError) Unwrap() error { return e.err }
func (e *transientWriteError) Is(target error) bool {
	return target == errTransientWriteNotApplied
}

// Exec executes a SQL statement with circuit breaker protection.
//
// Exec statements are non-idempotent, so they are NOT blanket-retried: a
// failed write is retried only when execWriteRetryable positively classifies
// the error as transient AND known not-applied (see its doc comment). All
// other errors — including timeouts, where the write may already have been
// applied — are returned to the caller on the first attempt.
func (ps *ProductionServer) Exec(ctx context.Context, sql string, args ...interface{}) (engine.Result, error) {
	if err := ps.Admit(false); err != nil {
		return engine.Result{}, err
	}
	key := ps.circuitBreakerKey(sql)
	var result engine.Result
	err := ps.executeWithClassifiedBreaker(key, func() error {
		retryConfig := ps.Config.Retry
		if retryConfig == nil {
			res, execErr := ps.db.Exec(ctx, sql, args...)
			if execErr == nil {
				result = res
			}
			return execErr
		}
		// Whitelist-only retry: errors are re-attempted only when wrapped
		// with the transient-not-applied marker below.
		writeRetry := *retryConfig
		writeRetry.RetryableErrors = []error{errTransientWriteNotApplied}
		return engine.Retry(ctx, &writeRetry, func() error {
			res, execErr := ps.db.Exec(ctx, sql, args...)
			if execErr == nil {
				result = res
				return nil
			}
			if ps.execWriteRetryable(execErr) {
				return &transientWriteError{err: execErr}
			}
			return execErr
		})
	})
	var marked *transientWriteError
	if errors.As(err, &marked) {
		err = marked.err
	}
	return result, err
}

// Query executes a SQL query with circuit breaker and retry protection.
// Reads are idempotent, so transient failures may be retried freely (the
// retry configuration still excludes deterministic errors).
func (ps *ProductionServer) Query(ctx context.Context, sql string, args ...interface{}) (*engine.Rows, error) {
	if err := ps.Admit(false); err != nil {
		return nil, err
	}
	key := ps.circuitBreakerKey(sql)
	var rows *engine.Rows
	err := ps.executeWithClassifiedBreaker(key, func() error {
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
	if err := ps.Admit(false); err != nil {
		return nil, err
	}
	key := ps.circuitBreakerKey(sql)
	var row *engine.Row
	err := ps.executeWithClassifiedBreaker(key, func() error {
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
