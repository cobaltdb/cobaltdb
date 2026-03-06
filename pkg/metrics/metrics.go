package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// MetricType represents the type of metric
type MetricType int

const (
	Counter MetricType = iota
	Gauge
	Histogram
	Timer
)

// Metric represents a single metric
type Metric struct {
	Name        string            `json:"name"`
	Type        MetricType        `json:"type"`
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels"`
	Value       float64           `json:"value"`
	Timestamp   time.Time         `json:"timestamp"`
}

// CounterMetric is a monotonically increasing counter
type CounterMetric struct {
	name   string
	desc   string
	labels map[string]string
	value  uint64
}

// NewCounter creates a new counter metric
func NewCounter(name, desc string, labels map[string]string) *CounterMetric {
	return &CounterMetric{
		name:   name,
		desc:   desc,
		labels: labels,
		value:  0,
	}
}

// Inc increments the counter by 1
func (c *CounterMetric) Inc() {
	atomic.AddUint64(&c.value, 1)
}

// Add adds a value to the counter
func (c *CounterMetric) Add(delta uint64) {
	atomic.AddUint64(&c.value, delta)
}

// Get returns the current counter value
func (c *CounterMetric) Get() uint64 {
	return atomic.LoadUint64(&c.value)
}

// GaugeMetric is a metric that can go up and down
type GaugeMetric struct {
	name   string
	desc   string
	labels map[string]string
	value  int64
}

// NewGauge creates a new gauge metric
func NewGauge(name, desc string, labels map[string]string) *GaugeMetric {
	return &GaugeMetric{
		name:   name,
		desc:   desc,
		labels: labels,
		value:  0,
	}
}

// Set sets the gauge value
func (g *GaugeMetric) Set(val int64) {
	atomic.StoreInt64(&g.value, val)
}

// Inc increments the gauge by 1
func (g *GaugeMetric) Inc() {
	atomic.AddInt64(&g.value, 1)
}

// Dec decrements the gauge by 1
func (g *GaugeMetric) Dec() {
	atomic.AddInt64(&g.value, -1)
}

// Add adds a value to the gauge
func (g *GaugeMetric) Add(delta int64) {
	atomic.AddInt64(&g.value, delta)
}

// Get returns the current gauge value
func (g *GaugeMetric) Get() int64 {
	return atomic.LoadInt64(&g.value)
}

// HistogramMetric tracks the distribution of values
type HistogramMetric struct {
	name    string
	desc    string
	labels  map[string]string
	mu      sync.RWMutex
	values  []float64
	buckets []float64
}

// NewHistogram creates a new histogram metric
func NewHistogram(name, desc string, labels map[string]string, buckets []float64) *HistogramMetric {
	if buckets == nil {
		// Default buckets for query latency in milliseconds
		buckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
	}
	return &HistogramMetric{
		name:    name,
		desc:    desc,
		labels:  labels,
		values:  make([]float64, 0, 1000),
		buckets: buckets,
	}
}

// Observe adds a value to the histogram
func (h *HistogramMetric) Observe(val float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.values = append(h.values, val)
	// Limit the number of stored values to prevent memory growth
	if len(h.values) > 10000 {
		h.values = h.values[len(h.values)-5000:]
	}
}

// GetSnapshot returns a snapshot of histogram statistics
func (h *HistogramMetric) GetSnapshot() HistogramSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	snapshot := HistogramSnapshot{
		Count:   uint64(len(h.values)),
		Buckets: make(map[string]uint64),
	}

	if len(h.values) == 0 {
		return snapshot
	}

	// Calculate sum and sort for percentiles
	var sum float64
	for _, v := range h.values {
		sum += v
	}
	snapshot.Sum = sum

	// Calculate bucket counts
	for _, v := range h.values {
		for _, bucket := range h.buckets {
			if v <= bucket {
				key := fmt.Sprintf("%.2f", bucket)
				snapshot.Buckets[key]++
			}
		}
	}

	return snapshot
}

// HistogramSnapshot contains histogram statistics
type HistogramSnapshot struct {
	Count   uint64            `json:"count"`
	Sum     float64           `json:"sum"`
	Buckets map[string]uint64 `json:"buckets"`
}

// TimerMetric measures durations
type TimerMetric struct {
	histogram *HistogramMetric
}

// NewTimer creates a new timer metric
func NewTimer(name, desc string, labels map[string]string) *TimerMetric {
	return &TimerMetric{
		histogram: NewHistogram(name, desc, labels, nil),
	}
}

// Time records a duration
func (t *TimerMetric) Time(duration time.Duration) {
	t.histogram.Observe(float64(duration.Milliseconds()))
}

// Start starts a new timer and returns a function to stop it
func (t *TimerMetric) Start() func() {
	start := time.Now()
	return func() {
		t.Time(time.Since(start))
	}
}

// Registry holds all metrics
type Registry struct {
	counters   map[string]*CounterMetric
	gauges     map[string]*GaugeMetric
	histograms map[string]*HistogramMetric
	timers     map[string]*TimerMetric
	mu         sync.RWMutex
}

// NewRegistry creates a new metrics registry
func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*CounterMetric),
		gauges:     make(map[string]*GaugeMetric),
		histograms: make(map[string]*HistogramMetric),
		timers:     make(map[string]*TimerMetric),
	}
}

// RegisterCounter registers a counter metric
func (r *Registry) RegisterCounter(name, desc string, labels map[string]string) *CounterMetric {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.metricKey(name, labels)
	if counter, exists := r.counters[key]; exists {
		return counter
	}

	counter := NewCounter(name, desc, labels)
	r.counters[key] = counter
	return counter
}

// RegisterGauge registers a gauge metric
func (r *Registry) RegisterGauge(name, desc string, labels map[string]string) *GaugeMetric {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.metricKey(name, labels)
	if gauge, exists := r.gauges[key]; exists {
		return gauge
	}

	gauge := NewGauge(name, desc, labels)
	r.gauges[key] = gauge
	return gauge
}

// RegisterHistogram registers a histogram metric
func (r *Registry) RegisterHistogram(name, desc string, labels map[string]string, buckets []float64) *HistogramMetric {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.metricKey(name, labels)
	if histogram, exists := r.histograms[key]; exists {
		return histogram
	}

	histogram := NewHistogram(name, desc, labels, buckets)
	r.histograms[key] = histogram
	return histogram
}

// RegisterTimer registers a timer metric
func (r *Registry) RegisterTimer(name, desc string, labels map[string]string) *TimerMetric {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.metricKey(name, labels)
	if timer, exists := r.timers[key]; exists {
		return timer
	}

	timer := NewTimer(name, desc, labels)
	r.timers[key] = timer
	return timer
}

// metricKey generates a unique key for a metric
func (r *Registry) metricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}

	key := name
	for k, v := range labels {
		key += fmt.Sprintf(";%s=%s", k, v)
	}
	return key
}

// GetAllMetrics returns all metrics as a map
func (r *Registry) GetAllMetrics() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]interface{})

	for key, counter := range r.counters {
		result[key] = map[string]interface{}{
			"type":  "counter",
			"value": counter.Get(),
		}
	}

	for key, gauge := range r.gauges {
		result[key] = map[string]interface{}{
			"type":  "gauge",
			"value": gauge.Get(),
		}
	}

	for key, histogram := range r.histograms {
		result[key] = map[string]interface{}{
			"type":     "histogram",
			"snapshot": histogram.GetSnapshot(),
		}
	}

	return result
}

// ExportJSON exports all metrics as JSON
func (r *Registry) ExportJSON() ([]byte, error) {
	metrics := r.GetAllMetrics()
	return json.MarshalIndent(metrics, "", "  ")
}

// Collector manages database metrics collection
type Collector struct {
	registry *Registry
	interval time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
	mu       sync.RWMutex

	// Database metrics
	QueryCounter           *CounterMetric
	QueryTimer             *TimerMetric
	ActiveConnections      *GaugeMetric
	TotalConnections       *CounterMetric
	CacheHits              *CounterMetric
	CacheMisses            *CounterMetric
	TransactionsStarted    *CounterMetric
	TransactionsCommitted  *CounterMetric
	TransactionsRolledBack *CounterMetric
	ErrorsTotal            *CounterMetric
	SlowQueries            *CounterMetric

	// Storage metrics
	PagesRead    *CounterMetric
	PagesWritten *CounterMetric
	PagesInCache *GaugeMetric
}

// NewCollector creates a new metrics collector
func NewCollector(interval time.Duration) *Collector {
	if interval == 0 {
		interval = 10 * time.Second
	}

	registry := NewRegistry()

	return &Collector{
		registry:               registry,
		interval:               interval,
		stopCh:                 make(chan struct{}),
		QueryCounter:           registry.RegisterCounter("queries_total", "Total number of queries", nil),
		QueryTimer:             registry.RegisterTimer("query_duration_ms", "Query execution time in milliseconds", nil),
		ActiveConnections:      registry.RegisterGauge("connections_active", "Number of active connections", nil),
		TotalConnections:       registry.RegisterCounter("connections_total", "Total number of connections", nil),
		CacheHits:              registry.RegisterCounter("cache_hits_total", "Total cache hits", nil),
		CacheMisses:            registry.RegisterCounter("cache_misses_total", "Total cache misses", nil),
		TransactionsStarted:    registry.RegisterCounter("transactions_started_total", "Total transactions started", nil),
		TransactionsCommitted:  registry.RegisterCounter("transactions_committed_total", "Total transactions committed", nil),
		TransactionsRolledBack: registry.RegisterCounter("transactions_rolled_back_total", "Total transactions rolled back", nil),
		ErrorsTotal:            registry.RegisterCounter("errors_total", "Total errors", nil),
		SlowQueries:            registry.RegisterCounter("slow_queries_total", "Total slow queries", nil),
		PagesRead:              registry.RegisterCounter("pages_read_total", "Total pages read from disk", nil),
		PagesWritten:           registry.RegisterCounter("pages_written_total", "Total pages written to disk", nil),
		PagesInCache:           registry.RegisterGauge("pages_in_cache", "Number of pages in cache", nil),
	}
}

// Start begins metrics collection
func (c *Collector) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.collectRuntimeMetrics()
		}
	}
}

// Stop stops metrics collection (safe to call multiple times)
func (c *Collector) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

// collectRuntimeMetrics collects Go runtime metrics
func (c *Collector) collectRuntimeMetrics() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Register runtime metrics if not already registered
	c.registry.RegisterGauge("runtime_memory_alloc_bytes", "Allocated memory in bytes", nil).Set(int64(m.Alloc))
	c.registry.RegisterGauge("runtime_memory_sys_bytes", "System memory in bytes", nil).Set(int64(m.Sys))
	c.registry.RegisterGauge("runtime_gc_pause_ns", "Last GC pause in nanoseconds", nil).Set(int64(m.PauseNs[(m.NumGC+255)%256]))
	c.registry.RegisterGauge("runtime_goroutines", "Number of goroutines", nil).Set(int64(runtime.NumGoroutine()))
}

// RecordQuery records query metrics
func (c *Collector) RecordQuery(duration time.Duration, isSlow bool) {
	c.QueryCounter.Inc()
	c.QueryTimer.Time(duration)
	if isSlow {
		c.SlowQueries.Inc()
	}
}

// RecordCacheHit records a cache hit
func (c *Collector) RecordCacheHit() {
	c.CacheHits.Inc()
}

// RecordCacheMiss records a cache miss
func (c *Collector) RecordCacheMiss() {
	c.CacheMisses.Inc()
}

// RecordTransaction records transaction metrics
func (c *Collector) RecordTransaction(committed bool) {
	c.TransactionsStarted.Inc()
	if committed {
		c.TransactionsCommitted.Inc()
	} else {
		c.TransactionsRolledBack.Inc()
	}
}

// RecordError records an error
func (c *Collector) RecordError() {
	c.ErrorsTotal.Inc()
}

// RecordPageRead records a page read from disk
func (c *Collector) RecordPageRead() {
	c.PagesRead.Inc()
}

// RecordPageWrite records a page write to disk
func (c *Collector) RecordPageWrite() {
	c.PagesWritten.Inc()
}

// SetPagesInCache updates the pages in cache gauge
func (c *Collector) SetPagesInCache(count int64) {
	c.PagesInCache.Set(count)
}

// ConnectionAcquired records a new connection
func (c *Collector) ConnectionAcquired() {
	c.TotalConnections.Inc()
	c.ActiveConnections.Inc()
}

// ConnectionReleased records a connection release
func (c *Collector) ConnectionReleased() {
	c.ActiveConnections.Dec()
}

// GetRegistry returns the metrics registry
func (c *Collector) GetRegistry() *Registry {
	return c.registry
}

// ExportJSON exports all metrics as JSON
func (c *Collector) ExportJSON() ([]byte, error) {
	return c.registry.ExportJSON()
}

// Snapshot represents a point-in-time snapshot of all metrics
type Snapshot struct {
	Timestamp time.Time              `json:"timestamp"`
	Metrics   map[string]interface{} `json:"metrics"`
}

// GetSnapshot returns a snapshot of all metrics
func (c *Collector) GetSnapshot() *Snapshot {
	return &Snapshot{
		Timestamp: time.Now(),
		Metrics:   c.registry.GetAllMetrics(),
	}
}

// SnapshotJSON returns a JSON snapshot of all metrics
func (c *Collector) SnapshotJSON() ([]byte, error) {
	snapshot := c.GetSnapshot()
	return json.MarshalIndent(snapshot, "", "  ")
}
