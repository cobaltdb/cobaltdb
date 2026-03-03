package metrics

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCounterMetric(t *testing.T) {
	counter := NewCounter("test_counter", "Test counter", map[string]string{"env": "test"})

	// Test initial value
	if counter.Get() != 0 {
		t.Errorf("Expected initial value 0, got %d", counter.Get())
	}

	// Test Inc
	counter.Inc()
	if counter.Get() != 1 {
		t.Errorf("Expected value 1 after Inc, got %d", counter.Get())
	}

	// Test Add
	counter.Add(5)
	if counter.Get() != 6 {
		t.Errorf("Expected value 6 after Add(5), got %d", counter.Get())
	}
}

func TestGaugeMetric(t *testing.T) {
	gauge := NewGauge("test_gauge", "Test gauge", map[string]string{"env": "test"})

	// Test initial value
	if gauge.Get() != 0 {
		t.Errorf("Expected initial value 0, got %d", gauge.Get())
	}

	// Test Set
	gauge.Set(10)
	if gauge.Get() != 10 {
		t.Errorf("Expected value 10 after Set, got %d", gauge.Get())
	}

	// Test Inc
	gauge.Inc()
	if gauge.Get() != 11 {
		t.Errorf("Expected value 11 after Inc, got %d", gauge.Get())
	}

	// Test Dec
	gauge.Dec()
	if gauge.Get() != 10 {
		t.Errorf("Expected value 10 after Dec, got %d", gauge.Get())
	}

	// Test Add
	gauge.Add(5)
	if gauge.Get() != 15 {
		t.Errorf("Expected value 15 after Add(5), got %d", gauge.Get())
	}
}

func TestHistogramMetric(t *testing.T) {
	buckets := []float64{10, 50, 100, 500, 1000}
	histogram := NewHistogram("test_histogram", "Test histogram", map[string]string{"env": "test"}, buckets)

	// Test initial state
	snapshot := histogram.GetSnapshot()
	if snapshot.Count != 0 {
		t.Errorf("Expected count 0, got %d", snapshot.Count)
	}

	// Test Observe
	histogram.Observe(25)
	histogram.Observe(75)
	histogram.Observe(150)

	snapshot = histogram.GetSnapshot()
	if snapshot.Count != 3 {
		t.Errorf("Expected count 3, got %d", snapshot.Count)
	}

	if snapshot.Sum != 250 {
		t.Errorf("Expected sum 250, got %f", snapshot.Sum)
	}

	// Check buckets
	if snapshot.Buckets["10.00"] != 0 {
		t.Errorf("Expected 0 values in bucket 10.00, got %d", snapshot.Buckets["10.00"])
	}
	if snapshot.Buckets["50.00"] != 1 {
		t.Errorf("Expected 1 value in bucket 50.00, got %d", snapshot.Buckets["50.00"])
	}
	if snapshot.Buckets["100.00"] != 2 {
		t.Errorf("Expected 2 values in bucket 100.00, got %d", snapshot.Buckets["100.00"])
	}
}

func TestTimerMetric(t *testing.T) {
	timer := NewTimer("test_timer", "Test timer", map[string]string{"env": "test"})

	// Test Time
	timer.Time(50 * time.Millisecond)
	timer.Time(100 * time.Millisecond)
	timer.Time(150 * time.Millisecond)

	snapshot := timer.histogram.GetSnapshot()
	if snapshot.Count != 3 {
		t.Errorf("Expected count 3, got %d", snapshot.Count)
	}

	// Test Start/Stop
	stop := timer.Start()
	time.Sleep(5 * time.Millisecond)
	stop()

	snapshot = timer.histogram.GetSnapshot()
	if snapshot.Count != 4 {
		t.Errorf("Expected count 4 after Start/Stop, got %d", snapshot.Count)
	}
}

func TestRegistry(t *testing.T) {
	registry := NewRegistry()

	// Test RegisterCounter
	counter := registry.RegisterCounter("queries", "Total queries", nil)
	if counter == nil {
		t.Fatal("Expected non-nil counter")
	}

	// Test duplicate registration returns same counter
	counter2 := registry.RegisterCounter("queries", "Total queries", nil)
	if counter != counter2 {
		t.Error("Expected same counter instance for duplicate registration")
	}

	// Test RegisterGauge
	gauge := registry.RegisterGauge("connections", "Active connections", nil)
	if gauge == nil {
		t.Fatal("Expected non-nil gauge")
	}

	// Test RegisterHistogram
	histogram := registry.RegisterHistogram("latency", "Request latency", nil, nil)
	if histogram == nil {
		t.Fatal("Expected non-nil histogram")
	}

	// Test RegisterTimer
	timer := registry.RegisterTimer("duration", "Request duration", nil)
	if timer == nil {
		t.Fatal("Expected non-nil timer")
	}

	// Test GetAllMetrics
	counter.Inc()
	gauge.Set(10)
	histogram.Observe(50)
	timer.Time(100 * time.Millisecond)

	metrics := registry.GetAllMetrics()
	// Timer creates its own histogram (not registered), so we have 3 registered metrics
	if len(metrics) != 3 {
		t.Errorf("Expected 3 metrics, got %d", len(metrics))
	}

	// Test ExportJSON
	json, err := registry.ExportJSON()
	if err != nil {
		t.Fatalf("Failed to export JSON: %v", err)
	}
	if len(json) == 0 {
		t.Error("Expected non-empty JSON")
	}
}

func TestRegistryWithLabels(t *testing.T) {
	registry := NewRegistry()

	// Register metrics with different labels
	counter1 := registry.RegisterCounter("queries", "Total queries", map[string]string{"method": "GET"})
	counter2 := registry.RegisterCounter("queries", "Total queries", map[string]string{"method": "POST"})

	// Should be different instances
	if counter1 == counter2 {
		t.Error("Expected different counter instances for different labels")
	}

	counter1.Inc()
	counter2.Inc()
	counter2.Inc()

	if counter1.Get() != 1 {
		t.Errorf("Expected counter1 value 1, got %d", counter1.Get())
	}
	if counter2.Get() != 2 {
		t.Errorf("Expected counter2 value 2, got %d", counter2.Get())
	}
}

func TestCollector(t *testing.T) {
	collector := NewCollector(100 * time.Millisecond)

	// Test metrics exist
	if collector.QueryCounter == nil {
		t.Error("Expected QueryCounter to be initialized")
	}
	if collector.QueryTimer == nil {
		t.Error("Expected QueryTimer to be initialized")
	}
	if collector.ActiveConnections == nil {
		t.Error("Expected ActiveConnections to be initialized")
	}

	// Test RecordQuery
	collector.RecordQuery(50*time.Millisecond, false)
	if collector.QueryCounter.Get() != 1 {
		t.Errorf("Expected query count 1, got %d", collector.QueryCounter.Get())
	}

	// Test RecordQuery with slow query
	collector.RecordQuery(2*time.Second, true)
	if collector.SlowQueries.Get() != 1 {
		t.Errorf("Expected slow query count 1, got %d", collector.SlowQueries.Get())
	}

	// Test RecordCacheHit/Miss
	collector.RecordCacheHit()
	collector.RecordCacheMiss()
	if collector.CacheHits.Get() != 1 {
		t.Errorf("Expected cache hits 1, got %d", collector.CacheHits.Get())
	}
	if collector.CacheMisses.Get() != 1 {
		t.Errorf("Expected cache misses 1, got %d", collector.CacheMisses.Get())
	}

	// Test RecordTransaction
	collector.RecordTransaction(true)
	collector.RecordTransaction(false)
	if collector.TransactionsStarted.Get() != 2 {
		t.Errorf("Expected transactions started 2, got %d", collector.TransactionsStarted.Get())
	}
	if collector.TransactionsCommitted.Get() != 1 {
		t.Errorf("Expected transactions committed 1, got %d", collector.TransactionsCommitted.Get())
	}
	if collector.TransactionsRolledBack.Get() != 1 {
		t.Errorf("Expected transactions rolled back 1, got %d", collector.TransactionsRolledBack.Get())
	}

	// Test RecordError
	collector.RecordError()
	if collector.ErrorsTotal.Get() != 1 {
		t.Errorf("Expected errors 1, got %d", collector.ErrorsTotal.Get())
	}

	// Test ConnectionAcquired/Released
	collector.ConnectionAcquired()
	if collector.ActiveConnections.Get() != 1 {
		t.Errorf("Expected active connections 1, got %d", collector.ActiveConnections.Get())
	}
	if collector.TotalConnections.Get() != 1 {
		t.Errorf("Expected total connections 1, got %d", collector.TotalConnections.Get())
	}

	collector.ConnectionReleased()
	if collector.ActiveConnections.Get() != 0 {
		t.Errorf("Expected active connections 0, got %d", collector.ActiveConnections.Get())
	}

	// Test SetPagesInCache
	collector.SetPagesInCache(100)
	if collector.PagesInCache.Get() != 100 {
		t.Errorf("Expected pages in cache 100, got %d", collector.PagesInCache.Get())
	}

	// Test GetSnapshot
	snapshot := collector.GetSnapshot()
	if snapshot == nil {
		t.Fatal("Expected non-nil snapshot")
	}
	if snapshot.Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}
	if len(snapshot.Metrics) == 0 {
		t.Error("Expected non-empty metrics")
	}

	// Test SnapshotJSON
	json, err := collector.SnapshotJSON()
	if err != nil {
		t.Fatalf("Failed to get snapshot JSON: %v", err)
	}
	if len(json) == 0 {
		t.Error("Expected non-empty JSON")
	}
}

func TestCollectorStartStop(t *testing.T) {
	collector := NewCollector(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start collector in background
	go collector.Start(ctx)

	// Let it run for a bit
	time.Sleep(150 * time.Millisecond)

	// Collector should have collected runtime metrics
	metrics := collector.GetRegistry().GetAllMetrics()
	foundRuntimeMetric := false
	for key := range metrics {
		if strings.Contains(key, "runtime_") {
			foundRuntimeMetric = true
			break
		}
	}
	if !foundRuntimeMetric {
		t.Error("Expected runtime metrics to be collected")
	}
}

func TestDefaultCollectorInterval(t *testing.T) {
	collector := NewCollector(0)
	if collector.interval != 10*time.Second {
		t.Errorf("Expected default interval 10s, got %v", collector.interval)
	}
}

func TestConcurrentCounterAccess(t *testing.T) {
	counter := NewCounter("concurrent", "Concurrent counter", nil)

	// Run multiple goroutines that increment the counter
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				counter.Inc()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	if counter.Get() != 1000 {
		t.Errorf("Expected counter value 1000, got %d", counter.Get())
	}
}

func TestConcurrentGaugeAccess(t *testing.T) {
	gauge := NewGauge("concurrent", "Concurrent gauge", nil)

	// Run multiple goroutines that modify the gauge
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				gauge.Inc()
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	if gauge.Get() != 1000 {
		t.Errorf("Expected gauge value 1000, got %d", gauge.Get())
	}
}
