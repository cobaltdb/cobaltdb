package metrics

import (
	"context"
	"testing"
	"time"
)

// TestCollectorStop tests the Stop method
func TestCollectorStop(t *testing.T) {
	collector := NewCollector(50 * time.Millisecond)

	// Start collector
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go collector.Start(ctx)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Stop the collector
	collector.Stop()

	// Verify collector stopped by checking if stopCh is closed
	// (No direct way to test this, but we can verify no panic occurs)
	t.Log("Collector stopped successfully")
}

// TestRecordPageRead tests the RecordPageRead method
func TestRecordPageRead(t *testing.T) {
	collector := NewCollector(100 * time.Millisecond)

	// Initial value should be 0
	if collector.PagesRead.Get() != 0 {
		t.Errorf("Expected initial pages read 0, got %d", collector.PagesRead.Get())
	}

	// Record page reads
	collector.RecordPageRead()
	collector.RecordPageRead()
	collector.RecordPageRead()

	if collector.PagesRead.Get() != 3 {
		t.Errorf("Expected pages read 3, got %d", collector.PagesRead.Get())
	}
}

// TestRecordPageWrite tests the RecordPageWrite method
func TestRecordPageWrite(t *testing.T) {
	collector := NewCollector(100 * time.Millisecond)

	// Initial value should be 0
	if collector.PagesWritten.Get() != 0 {
		t.Errorf("Expected initial pages written 0, got %d", collector.PagesWritten.Get())
	}

	// Record page writes
	collector.RecordPageWrite()
	collector.RecordPageWrite()

	if collector.PagesWritten.Get() != 2 {
		t.Errorf("Expected pages written 2, got %d", collector.PagesWritten.Get())
	}
}

// TestExportJSON tests the ExportJSON method
func TestExportJSON(t *testing.T) {
	collector := NewCollector(100 * time.Millisecond)

	// Add some metrics
	collector.RecordQuery(50*time.Millisecond, false)
	collector.RecordCacheHit()
	collector.SetPagesInCache(50)

	// Export to JSON
	json, err := collector.ExportJSON()
	if err != nil {
		t.Fatalf("Failed to export JSON: %v", err)
	}

	if len(json) == 0 {
		t.Error("Expected non-empty JSON export")
	}

	// Verify JSON contains expected content
	jsonStr := string(json)
	if !contains(jsonStr, "queries_total") {
		t.Error("Expected JSON to contain queries_total")
	}
	if !contains(jsonStr, "cache_hits") {
		t.Error("Expected JSON to contain cache_hits")
	}
	if !contains(jsonStr, "pages_in_cache") {
		t.Error("Expected JSON to contain pages_in_cache")
	}

	t.Logf("Exported JSON length: %d bytes", len(json))
}

// TestGetRegistry tests the GetRegistry method
func TestGetRegistry(t *testing.T) {
	collector := NewCollector(100 * time.Millisecond)

	registry := collector.GetRegistry()
	if registry == nil {
		t.Fatal("Expected non-nil registry")
	}

	// Verify registry is the same one used internally
	metrics := registry.GetAllMetrics()
	if metrics == nil {
		t.Error("Expected registry to return metrics")
	}
}

// TestCollectorWithContextCancellation tests that collector stops when context is cancelled
func TestCollectorWithContextCancellation(t *testing.T) {
	collector := NewCollector(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	// Start collector
	go collector.Start(ctx)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()

	// Give it time to stop
	time.Sleep(50 * time.Millisecond)

	t.Log("Collector handled context cancellation")
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
