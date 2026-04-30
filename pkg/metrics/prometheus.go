package metrics

import (
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// PrometheusMetrics provides Prometheus-compatible metrics export
type PrometheusMetrics struct {
	startTime time.Time
}

// StorageMetrics contains the storage counters exported by Prometheus.
type StorageMetrics struct {
	Capacity      int
	PageCount     int
	DirtyCount    int
	PinnedCount   int32
	FreeCount     int
	HitCount      uint64
	MissCount     uint64
	HitRatio      float64
	ReadCount     uint64
	WriteCount    uint64
	EvictionCount uint64
}

// NewPrometheusMetrics creates a new Prometheus metrics exporter
func NewPrometheusMetrics() *PrometheusMetrics {
	return &PrometheusMetrics{
		startTime: time.Now(),
	}
}

// Handler returns an HTTP handler for Prometheus scraping
func (p *PrometheusMetrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		// Write all metrics in Prometheus format
		p.writeTransactionMetrics(w)
		p.writeSystemMetrics(w)
		p.writeQueryMetrics(w)
		p.writeStorageMetrics(w)
	}
}

// writeTransactionMetrics writes transaction-related metrics
func (p *PrometheusMetrics) writeTransactionMetrics(w http.ResponseWriter) {
	txnStats := GetTransactionMetrics().GetStats()

	// Active transactions gauge
	fmt.Fprintf(w, "# HELP cobaltdb_transactions_active Number of active transactions\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_transactions_active gauge\n")
	fmt.Fprintf(w, "cobaltdb_transactions_active %d\n", txnStats.ActiveTxns)

	// Committed transactions counter
	fmt.Fprintf(w, "# HELP cobaltdb_transactions_committed_total Total number of committed transactions\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_transactions_committed_total counter\n")
	fmt.Fprintf(w, "cobaltdb_transactions_committed_total %d\n", txnStats.CommittedTxns)

	// Aborted transactions counter
	fmt.Fprintf(w, "# HELP cobaltdb_transactions_aborted_total Total number of aborted transactions\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_transactions_aborted_total counter\n")
	fmt.Fprintf(w, "cobaltdb_transactions_aborted_total %d\n", txnStats.AbortedTxns)

	// Deadlocks detected counter
	fmt.Fprintf(w, "# HELP cobaltdb_deadlocks_detected_total Total number of deadlocks detected\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_deadlocks_detected_total counter\n")
	fmt.Fprintf(w, "cobaltdb_deadlocks_detected_total %d\n", txnStats.DeadlocksDetected)

	// Lock timeouts counter
	fmt.Fprintf(w, "# HELP cobaltdb_lock_timeouts_total Total number of lock acquisition timeouts\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_lock_timeouts_total counter\n")
	fmt.Fprintf(w, "cobaltdb_lock_timeouts_total %d\n", txnStats.LockTimeouts)

	// Transaction timeouts counter
	fmt.Fprintf(w, "# HELP cobaltdb_transaction_timeouts_total Total number of transaction timeouts\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_transaction_timeouts_total counter\n")
	fmt.Fprintf(w, "cobaltdb_transaction_timeouts_total %d\n", txnStats.TxnTimeouts)

	// Long running transactions gauge
	fmt.Fprintf(w, "# HELP cobaltdb_transactions_long_running Number of long-running transactions (>1s)\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_transactions_long_running gauge\n")
	fmt.Fprintf(w, "cobaltdb_transactions_long_running %d\n", txnStats.LongRunningTxns)
}

// writeSystemMetrics writes system/runtime metrics
func (p *PrometheusMetrics) writeSystemMetrics(w http.ResponseWriter) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Uptime
	fmt.Fprintf(w, "# HELP cobaltdb_uptime_seconds Time since server started\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_uptime_seconds counter\n")
	fmt.Fprintf(w, "cobaltdb_uptime_seconds %d\n", int64(time.Since(p.startTime).Seconds()))

	// Goroutines
	fmt.Fprintf(w, "# HELP cobaltdb_go_goroutines Number of goroutines\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_go_goroutines gauge\n")
	fmt.Fprintf(w, "cobaltdb_go_goroutines %d\n", runtime.NumGoroutine())

	// Memory metrics
	fmt.Fprintf(w, "# HELP cobaltdb_go_memstats_alloc_bytes Number of bytes allocated\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_go_memstats_alloc_bytes gauge\n")
	fmt.Fprintf(w, "cobaltdb_go_memstats_alloc_bytes %d\n", m.Alloc)

	fmt.Fprintf(w, "# HELP cobaltdb_go_memstats_heap_alloc_bytes Number of heap bytes allocated\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_go_memstats_heap_alloc_bytes gauge\n")
	fmt.Fprintf(w, "cobaltdb_go_memstats_heap_alloc_bytes %d\n", m.HeapAlloc)

	fmt.Fprintf(w, "# HELP cobaltdb_go_memstats_heap_inuse_bytes Number of heap bytes in use\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_go_memstats_heap_inuse_bytes gauge\n")
	fmt.Fprintf(w, "cobaltdb_go_memstats_heap_inuse_bytes %d\n", m.HeapInuse)

	fmt.Fprintf(w, "# HELP cobaltdb_go_memstats_sys_bytes Number of bytes obtained from system\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_go_memstats_sys_bytes gauge\n")
	fmt.Fprintf(w, "cobaltdb_go_memstats_sys_bytes %d\n", m.Sys)

	// GC metrics
	fmt.Fprintf(w, "# HELP cobaltdb_go_gc_count_total Total number of GC cycles\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_go_gc_count_total counter\n")
	fmt.Fprintf(w, "cobaltdb_go_gc_count_total %d\n", m.NumGC)

	fmt.Fprintf(w, "# HELP cobaltdb_go_gc_pause_ns_total Total GC pause time in nanoseconds\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_go_gc_pause_ns_total counter\n")
	fmt.Fprintf(w, "cobaltdb_go_gc_pause_ns_total %d\n", m.PauseTotalNs)
}

// writeQueryMetrics writes query-related metrics
func (p *PrometheusMetrics) writeQueryMetrics(w http.ResponseWriter) {
	// Get slow query stats if available
	if slowLog := GetSlowQueryLog(); slowLog != nil {
		total, _ := slowLog.GetStats()

		fmt.Fprintf(w, "# HELP cobaltdb_slow_queries_total Total number of slow queries\n")
		fmt.Fprintf(w, "# TYPE cobaltdb_slow_queries_total counter\n")
		fmt.Fprintf(w, "cobaltdb_slow_queries_total %d\n", total)
	}
}

// writeStorageMetrics writes storage-related metrics
func (p *PrometheusMetrics) writeStorageMetrics(w http.ResponseWriter) {
	stats := StorageMetrics{}
	globalStorageMetrics.RLock()
	provider := globalStorageMetrics.provider
	globalStorageMetrics.RUnlock()
	if provider != nil {
		stats = provider()
	}

	fmt.Fprintf(w, "# HELP cobaltdb_storage_pages_total Total number of storage pages\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_pages_total gauge\n")
	fmt.Fprintf(w, "cobaltdb_storage_pages_total %d\n", stats.PageCount)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_pages_dirty Number of dirty pages\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_pages_dirty gauge\n")
	fmt.Fprintf(w, "cobaltdb_storage_pages_dirty %d\n", stats.DirtyCount)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_pages_pinned Number of pinned pages\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_pages_pinned gauge\n")
	fmt.Fprintf(w, "cobaltdb_storage_pages_pinned %d\n", stats.PinnedCount)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_pages_free Number of free buffer slots\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_pages_free gauge\n")
	fmt.Fprintf(w, "cobaltdb_storage_pages_free %d\n", stats.FreeCount)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_cache_hits_total Total buffer pool cache hits\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_cache_hits_total counter\n")
	fmt.Fprintf(w, "cobaltdb_storage_cache_hits_total %d\n", stats.HitCount)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_cache_misses_total Total buffer pool cache misses\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_cache_misses_total counter\n")
	fmt.Fprintf(w, "cobaltdb_storage_cache_misses_total %d\n", stats.MissCount)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_cache_hit_ratio Buffer pool cache hit ratio\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_cache_hit_ratio gauge\n")
	fmt.Fprintf(w, "cobaltdb_storage_cache_hit_ratio %.6f\n", stats.HitRatio)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_reads_total Total storage reads\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_reads_total counter\n")
	fmt.Fprintf(w, "cobaltdb_storage_reads_total %d\n", stats.ReadCount)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_writes_total Total storage writes\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_writes_total counter\n")
	fmt.Fprintf(w, "cobaltdb_storage_writes_total %d\n", stats.WriteCount)

	fmt.Fprintf(w, "# HELP cobaltdb_storage_evictions_total Total buffer pool evictions\n")
	fmt.Fprintf(w, "# TYPE cobaltdb_storage_evictions_total counter\n")
	fmt.Fprintf(w, "cobaltdb_storage_evictions_total %d\n", stats.EvictionCount)
}

// Global Prometheus metrics instance
var globalPrometheusMetrics = NewPrometheusMetrics()

var globalSlowQueryLog struct {
	sync.RWMutex
	log        *SlowQueryLog
	generation uint64
}

var globalStorageMetrics struct {
	sync.RWMutex
	provider   func() StorageMetrics
	generation uint64
}

// GetPrometheusHandler returns the global Prometheus metrics HTTP handler
func GetPrometheusHandler() http.HandlerFunc {
	return globalPrometheusMetrics.Handler()
}

// SlowQueryStats holds statistics about slow queries
type SlowQueryStats struct {
	TotalQueries   int
	CurrentEntries int
}

// RegisterSlowQueryLog exposes the engine slow query log to global metrics
// exporters. Passing nil clears the current registration.
func RegisterSlowQueryLog(log *SlowQueryLog) func() {
	globalSlowQueryLog.Lock()
	defer globalSlowQueryLog.Unlock()
	globalSlowQueryLog.generation++
	generation := globalSlowQueryLog.generation
	globalSlowQueryLog.log = log
	return func() {
		globalSlowQueryLog.Lock()
		defer globalSlowQueryLog.Unlock()
		if globalSlowQueryLog.generation == generation {
			globalSlowQueryLog.log = nil
		}
	}
}

// GetSlowQueryLog returns the slow query log registered by the engine.
func GetSlowQueryLog() *SlowQueryLog {
	globalSlowQueryLog.RLock()
	defer globalSlowQueryLog.RUnlock()
	return globalSlowQueryLog.log
}

// RegisterStorageMetricsProvider exposes live storage metrics to global
// Prometheus exporters. Passing nil clears the current registration.
func RegisterStorageMetricsProvider(provider func() StorageMetrics) func() {
	globalStorageMetrics.Lock()
	defer globalStorageMetrics.Unlock()
	globalStorageMetrics.generation++
	generation := globalStorageMetrics.generation
	globalStorageMetrics.provider = provider
	return func() {
		globalStorageMetrics.Lock()
		defer globalStorageMetrics.Unlock()
		if globalStorageMetrics.generation == generation {
			globalStorageMetrics.provider = nil
		}
	}
}
