package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// AdminServer provides HTTP endpoints for monitoring and metrics
type AdminServer struct {
	db        *engine.DB
	server    *http.Server
	mu        sync.RWMutex
	started   bool
	addr      string
	authToken string
}

// NewAdminServer creates a new admin server
func NewAdminServer(db *engine.DB, addr string) *AdminServer {
	if addr == "" {
		addr = ":8420" // Default admin port
	}

	return &AdminServer{
		db:   db,
		addr: addr,
	}
}

// SetAuthToken sets the authentication token for the admin API
func (a *AdminServer) SetAuthToken(token string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.authToken = token
}

// Start starts the admin server
func (a *AdminServer) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.started {
		return fmt.Errorf("admin server already started")
	}

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", a.handleHealth)

	// Metrics endpoints
	mux.HandleFunc("/metrics", a.handleMetrics)
	mux.HandleFunc("/metrics/prometheus", a.handlePrometheusMetrics)
	mux.HandleFunc("/metrics/json", a.handleJSONMetrics)

	// Database stats
	mux.HandleFunc("/stats", a.handleStats)
	mux.HandleFunc("/stats/db", a.handleDBStats)

	// System info
	mux.HandleFunc("/system", a.handleSystem)

	// Ready check
	mux.HandleFunc("/ready", a.handleReady)

	// Create listener first to get the actual address (for port 0)
	listener, err := net.Listen("tcp", a.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", a.addr, err)
	}

	a.server = &http.Server{
		Handler:      a.authMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	a.addr = listener.Addr().String()
	a.started = true

	// Start in background
	go func() {
		log.Printf("[Admin] Starting admin server on %s", a.addr)
		if err := a.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("[Admin] Server error: %v", err)
		}
	}()

	return nil
}

// Stop stops the admin server
func (a *AdminServer) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.started {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.server.Shutdown(ctx); err != nil {
		return err
	}

	a.started = false
	log.Printf("[Admin] Server stopped")
	return nil
}

// authMiddleware adds authentication to endpoints
func (a *AdminServer) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.mu.RLock()
		token := a.authToken
		a.mu.RUnlock()

		// If auth token is set, require it
		if token != "" {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			// Support "Bearer <token>" or just "<token>"
			parts := strings.SplitN(authHeader, " ", 2)
			var providedToken string
			if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
				providedToken = parts[1]
			} else {
				providedToken = authHeader
			}

			if providedToken != token {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		// CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// handleHealth returns health status
func (a *AdminServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	if a.db != nil {
		status["database"] = "connected"
	} else {
		status["database"] = "disconnected"
		status["status"] = "degraded"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// handleReady returns readiness status
func (a *AdminServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if a.db == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "not ready",
			"reason": "database not initialized",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ready",
	})
}

// handleMetrics redirects to Prometheus format by default
func (a *AdminServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	a.handlePrometheusMetrics(w, r)
}

// handlePrometheusMetrics returns metrics in Prometheus format
func (a *AdminServer) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	var output strings.Builder

	// Write header
	output.WriteString("# CobaltDB Metrics\n")
	output.WriteString("# Generated: " + time.Now().UTC().Format(time.RFC3339) + "\n\n")

	if a.db == nil {
		w.Write([]byte(output.String()))
		return
	}

	collector := a.db.GetMetricsCollector()
	if collector == nil {
		w.Write([]byte(output.String()))
		return
	}

	// Query metrics
	output.WriteString("# HELP cobaltdb_queries_total Total number of queries executed\n")
	output.WriteString("# TYPE cobaltdb_queries_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_queries_total %d\n\n", collector.QueryCounter.Get()))

	output.WriteString("# HELP cobaltdb_slow_queries_total Total number of slow queries\n")
	output.WriteString("# TYPE cobaltdb_slow_queries_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_slow_queries_total %d\n\n", collector.SlowQueries.Get()))

	// Cache metrics
	output.WriteString("# HELP cobaltdb_cache_hits_total Total query cache hits\n")
	output.WriteString("# TYPE cobaltdb_cache_hits_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_cache_hits_total %d\n\n", collector.CacheHits.Get()))

	output.WriteString("# HELP cobaltdb_cache_misses_total Total query cache misses\n")
	output.WriteString("# TYPE cobaltdb_cache_misses_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_cache_misses_total %d\n\n", collector.CacheMisses.Get()))

	// Connection metrics
	output.WriteString("# HELP cobaltdb_connections_active Current active connections\n")
	output.WriteString("# TYPE cobaltdb_connections_active gauge\n")
	output.WriteString(fmt.Sprintf("cobaltdb_connections_active %d\n\n", collector.ActiveConnections.Get()))

	output.WriteString("# HELP cobaltdb_connections_total Total connections accepted\n")
	output.WriteString("# TYPE cobaltdb_connections_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_connections_total %d\n\n", collector.TotalConnections.Get()))

	// Transaction metrics
	output.WriteString("# HELP cobaltdb_transactions_committed_total Total committed transactions\n")
	output.WriteString("# TYPE cobaltdb_transactions_committed_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_transactions_committed_total %d\n\n", collector.TransactionsCommitted.Get()))

	output.WriteString("# HELP cobaltdb_transactions_rolled_back_total Total rolled back transactions\n")
	output.WriteString("# TYPE cobaltdb_transactions_rolled_back_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_transactions_rolled_back_total %d\n\n", collector.TransactionsRolledBack.Get()))

	// Write metrics
	output.WriteString("# HELP cobaltdb_writes_total Total write operations\n")
	output.WriteString("# TYPE cobaltdb_writes_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_writes_total %d\n\n", collector.WriteCounter.Get()))

	// Error metrics
	output.WriteString("# HELP cobaltdb_errors_total Total errors\n")
	output.WriteString("# TYPE cobaltdb_errors_total counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_errors_total %d\n\n", collector.ErrorsTotal.Get()))

	// Query duration histogram
	output.WriteString("# HELP cobaltdb_query_duration_seconds Query duration histogram\n")
	output.WriteString("# TYPE cobaltdb_query_duration_seconds histogram\n")
	hist := collector.QueryHistogram.GetSnapshot()
	for bucket, count := range hist.Buckets {
		output.WriteString(fmt.Sprintf("cobaltdb_query_duration_seconds_bucket{le=\"%s\"} %d\n", bucket, count))
	}
	output.WriteString(fmt.Sprintf("cobaltdb_query_duration_seconds_sum %f\n", hist.Sum))
	output.WriteString(fmt.Sprintf("cobaltdb_query_duration_seconds_count %d\n\n", hist.Count))

	// Write duration histogram
	output.WriteString("# HELP cobaltdb_write_duration_seconds Write operation duration histogram\n")
	output.WriteString("# TYPE cobaltdb_write_duration_seconds histogram\n")
	writeHist := collector.WriteHistogram.GetSnapshot()
	for bucket, count := range writeHist.Buckets {
		output.WriteString(fmt.Sprintf("cobaltdb_write_duration_seconds_bucket{le=\"%s\"} %d\n", bucket, count))
	}
	output.WriteString(fmt.Sprintf("cobaltdb_write_duration_seconds_sum %f\n", writeHist.Sum))
	output.WriteString(fmt.Sprintf("cobaltdb_write_duration_seconds_count %d\n\n", writeHist.Count))

	// Buffer pool metrics
	output.WriteString("# HELP cobaltdb_buffer_pool_hits Buffer pool hits\n")
	output.WriteString("# TYPE cobaltdb_buffer_pool_hits counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_buffer_pool_hits %d\n\n", collector.BufferPoolHits.Get()))

	output.WriteString("# HELP cobaltdb_buffer_pool_misses Buffer pool misses\n")
	output.WriteString("# TYPE cobaltdb_buffer_pool_misses counter\n")
	output.WriteString(fmt.Sprintf("cobaltdb_buffer_pool_misses %d\n\n", collector.BufferPoolMisses.Get()))

	w.Write([]byte(output.String()))
}

// handleJSONMetrics returns metrics in JSON format
func (a *AdminServer) handleJSONMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if a.db == nil {
		json.NewEncoder(w).Encode(map[string]string{
			"error": "database not initialized",
		})
		return
	}

	collector := a.db.GetMetricsCollector()
	if collector == nil {
		json.NewEncoder(w).Encode(map[string]string{
			"error": "metrics not enabled",
		})
		return
	}

	queryHist := collector.QueryHistogram.GetSnapshot()
	writeHist := collector.WriteHistogram.GetSnapshot()

	metrics := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"queries": map[string]interface{}{
			"total":       collector.QueryCounter.Get(),
			"slow":        collector.SlowQueries.Get(),
			"cache_hits":  collector.CacheHits.Get(),
			"cache_miss":  collector.CacheMisses.Get(),
			"duration_ms": queryHist,
		},
		"connections": map[string]interface{}{
			"active": collector.ActiveConnections.Get(),
			"total":  collector.TotalConnections.Get(),
		},
		"transactions": map[string]interface{}{
			"committed":  collector.TransactionsCommitted.Get(),
			"rolledback": collector.TransactionsRolledBack.Get(),
		},
		"writes": map[string]interface{}{
			"total":       collector.WriteCounter.Get(),
			"duration_ms": writeHist,
		},
		"errors": map[string]interface{}{
			"total": collector.ErrorsTotal.Get(),
		},
		"buffer_pool": map[string]interface{}{
			"hits":   collector.BufferPoolHits.Get(),
			"misses": collector.BufferPoolMisses.Get(),
		},
	}

	json.NewEncoder(w).Encode(metrics)
}

// handleStats returns database statistics
func (a *AdminServer) handleStats(w http.ResponseWriter, r *http.Request) {
	a.handleDBStats(w, r)
}

// handleDBStats returns database-specific statistics
func (a *AdminServer) handleDBStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if a.db == nil {
		json.NewEncoder(w).Encode(map[string]string{
			"error": "database not initialized",
		})
		return
	}

	stats, err := a.db.Stats()
	if err != nil {
		json.NewEncoder(w).Encode(map[string]string{
			"error": err.Error(),
		})
		return
	}

	result := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"stats":     stats,
	}

	json.NewEncoder(w).Encode(result)
}

// handleSystem returns system information
func (a *AdminServer) handleSystem(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	info := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"go": map[string]interface{}{
			"version":    runtime.Version(),
			"goroutines": runtime.NumGoroutine(),
			"cpu_count":  runtime.NumCPU(),
		},
		"memory": map[string]interface{}{
			"alloc_mb":      formatFloat(float64(m.Alloc) / 1024 / 1024),
			"sys_mb":        formatFloat(float64(m.Sys) / 1024 / 1024),
			"heap_alloc_mb": formatFloat(float64(m.HeapAlloc) / 1024 / 1024),
			"heap_sys_mb":   formatFloat(float64(m.HeapSys) / 1024 / 1024),
			"gc_count":      m.NumGC,
		},
	}

	json.NewEncoder(w).Encode(info)
}

func formatFloat(f float64) float64 {
	v, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", f), 64)
	return v
}

// IsRunning returns true if the admin server is running
func (a *AdminServer) IsRunning() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.started
}

// Addr returns the server address
func (a *AdminServer) Addr() string {
	return a.addr
}
