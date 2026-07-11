package metrics

import (
	"math"
	"testing"
)

func TestCoverageCloneNilAndFloatSlices(t *testing.T) {
	if got := cloneAlertRule(nil); got != nil {
		t.Fatal("nil alert rule should remain nil")
	}
	src := AlertRule{Name: "test", Threshold: 90.0}
	cloned := cloneAlertRule(&src)
	if cloned.Name != src.Name || cloned.Threshold != src.Threshold {
		t.Fatal("cloned rule differs")
	}
	cloned.Threshold = 50
	if src.Threshold == 50 {
		t.Fatal("clone must be a deep copy")
	}
	if got := cloneFloat64Slice(nil); got != nil {
		t.Fatal("nil slice should remain nil")
	}
	if got := cloneFloat64Slice([]float64{}); len(got) != 0 {
		t.Fatal("empty slice clone")
	}
	if got := cloneFloat64Slice([]float64{1.5, 2.5}); len(got) != 2 || got[1] != 2.5 {
		t.Fatal("float64 slice clone broke")
	}
}

func TestCoverageClampUint64ToInt64(t *testing.T) {
	if got := clampUint64ToInt64(0); got != 0 {
		t.Fatalf("clamp(0) = %d", got)
	}
	if got := clampUint64ToInt64(42); got != 42 {
		t.Fatalf("clamp(42) = %d", got)
	}
	if got := clampUint64ToInt64(uint64(math.MaxInt64)); got != math.MaxInt64 {
		t.Fatalf("clamp(maxInt64) = %d", got)
	}
	if got := clampUint64ToInt64(uint64(math.MaxInt64) + 1); got != math.MaxInt64 {
		t.Fatalf("clamp(maxInt64+1) = %d", got)
	}
	if got := clampUint64ToInt64(1 << 63); got != math.MaxInt64 {
		t.Fatalf("clamp(1<<63) = %d", got)
	}
}

func TestCoverageHistogramObserveEdgeCases(t *testing.T) {
	h := NewHistogram("h", "test", nil, nil)
	if sn := h.GetSnapshot(); sn.Count != 0 {
		t.Fatal("snapshot on empty histogram should have zero count")
	}
	h.Observe(0.5)
	sn := h.GetSnapshot()
	if sn.Count != 1 || sn.Sum != 0.5 {
		t.Fatalf("after observe: count=%d sum=%f", sn.Count, sn.Sum)
	}
}

func TestCoverageAlertingEdgePaths(t *testing.T) {
	// Create manager with a rule that has nil condition (skipped path)
	am := &AlertManager{rules: make(map[string]*AlertRule), stopCh: make(chan struct{})}
	am.RegisterRule(&AlertRule{Name: "nil-cond"})
	// Muted rule (skipped path in checkRules)
	am.RegisterRule(&AlertRule{Name: "muted", Threshold: 1, Condition: func() (bool, float64) { return true, 1 }, muted: true})
	// Active rule that fires
	fired := false
	am.RegisterRule(&AlertRule{Name: "active", Threshold: 1, Condition: func() (bool, float64) { fired = true; return true, 1 }})
	am.checkRules()
	if !fired {
		t.Fatal("active rule should have fired")
	}
	// GetAlerts on manager with alerts
	alerts := am.GetAlerts(10)
	if len(alerts) == 0 {
		t.Fatal("expected at least one alert after checkRules")
	}
	// stopManager should clean up
	am.Stop()
}

func TestCoverageHandlerSlotCapacity(t *testing.T) {
	am := &AlertManager{handlers: make([]AlertHandler, 10)}
	// tryAcquireHandlerSlot at capacity: handlers array is full of nil entries
	// so len(am.handlers) < handlerSlotLimit passes
	if !am.tryAcquireHandlerSlot() {
		t.Fatal("handler slot should be available")
	}
	am.releaseHandlerSlot()
}

func TestCoverageRegisterMetricsEdgePaths(t *testing.T) {
	r := NewRegistry()

	r.RegisterHistogram("h", "test", nil, nil)
	r.RegisterTimer("t", "test", nil)

	// Verify GetAllMetrics returns at least our registered metrics
	all := r.GetAllMetrics()
	if all == nil {
		t.Fatal("GetAllMetrics returned nil")
	}
	if _, ok := all["h"]; !ok {
		t.Fatal("histogram 'h' not found in metrics")
	}
}
