package metrics

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestCollectorRecordWrite tests the RecordWrite method
func TestCollectorRecordWrite(t *testing.T) {
	collector := NewCollector(100 * time.Millisecond)
	if collector == nil {
		t.Fatal("Collector is nil")
	}

	// This should not panic
	collector.RecordWrite(50 * time.Millisecond)
	collector.RecordWrite(100 * time.Millisecond)
	collector.RecordWrite(200 * time.Millisecond)
}

// TestCollectorRecordBufferPoolHit tests the RecordBufferPoolHit method
func TestCollectorRecordBufferPoolHit(t *testing.T) {
	collector := NewCollector(100 * time.Millisecond)
	if collector == nil {
		t.Fatal("Collector is nil")
	}

	// This should not panic
	collector.RecordBufferPoolHit()
	collector.RecordBufferPoolHit()
	collector.RecordBufferPoolHit()
}

// TestCollectorRecordBufferPoolMiss tests the RecordBufferPoolMiss method
func TestCollectorRecordBufferPoolMiss(t *testing.T) {
	collector := NewCollector(100 * time.Millisecond)
	if collector == nil {
		t.Fatal("Collector is nil")
	}

	// This should not panic
	collector.RecordBufferPoolMiss()
	collector.RecordBufferPoolMiss()
}

// TestTransactionMetricsRecordLockWaitTime tests the RecordLockWaitTime method
func TestTransactionMetricsRecordLockWaitTime(t *testing.T) {
	tm := GetTransactionMetrics()
	if tm == nil {
		t.Fatal("TransactionMetrics is nil")
	}

	// This should not panic
	tm.RecordLockWaitTime(10 * time.Millisecond)
	tm.RecordLockWaitTime(50 * time.Millisecond)
	tm.RecordLockWaitTime(100 * time.Millisecond)
}

// TestAlertManagerCheckRules tests the checkRules function
func TestAlertManagerCheckRules(t *testing.T) {
	am := NewAlertManager()

	// Create a rule that always fires
	var fireCount int64
	rule := &AlertRule{
		Name:        "test_always_fire",
		Description: "Test rule that always fires",
		Severity:    SeverityInfo,
		Threshold:   1.0,
		Cooldown:    0, // No cooldown for testing
		Condition: func() (bool, float64) {
			return true, 5.0
		},
	}

	// Create a callback handler to catch the alert
	handler := &CallbackAlertHandler{
		Callback: func(a Alert) {
			atomic.AddInt64(&fireCount, 1)
		},
	}

	am.RegisterRule(rule)
	am.RegisterHandler(handler)

	// Manually call checkRules
	am.checkRules()

	// Wait a bit for async handler
	time.Sleep(50 * time.Millisecond)

	// Check that the rule fired
	if atomic.LoadInt64(&fireCount) == 0 {
		t.Error("Expected rule to fire at least once")
	}
}

// TestAlertManagerCheckRulesMuted tests that muted rules don't fire
func TestAlertManagerCheckRulesMuted(t *testing.T) {
	am := NewAlertManager()

	// Create a rule that always fires
	var fireCount int64
	rule := &AlertRule{
		Name:        "test_muted_rule",
		Description: "Test muted rule",
		Severity:    SeverityInfo,
		Threshold:   1.0,
		Cooldown:    0,
		Condition: func() (bool, float64) {
			return true, 5.0
		},
	}

	handler := &CallbackAlertHandler{
		Callback: func(a Alert) {
			atomic.AddInt64(&fireCount, 1)
		},
	}

	am.RegisterRule(rule)
	am.RegisterHandler(handler)
	am.MuteRule("test_muted_rule")

	// Manually call checkRules
	am.checkRules()

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	// Check that the muted rule didn't fire
	if atomic.LoadInt64(&fireCount) != 0 {
		t.Error("Muted rule should not fire")
	}
}

// TestAlertManagerCheckRulesCooldown tests cooldown functionality
func TestAlertManagerCheckRulesCooldown(t *testing.T) {
	am := NewAlertManager()

	var fireCount int64
	rule := &AlertRule{
		Name:        "test_cooldown_rule",
		Description: "Test rule with cooldown",
		Severity:    SeverityInfo,
		Threshold:   1.0,
		Cooldown:    1 * time.Hour, // Long cooldown
		Condition: func() (bool, float64) {
			return true, 5.0
		},
	}

	handler := &CallbackAlertHandler{
		Callback: func(a Alert) {
			atomic.AddInt64(&fireCount, 1)
		},
	}

	am.RegisterRule(rule)
	am.RegisterHandler(handler)

	// First call - should fire
	am.checkRules()
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt64(&fireCount) != 1 {
		t.Errorf("Expected 1 fire, got %d", atomic.LoadInt64(&fireCount))
	}

	// Second call immediately - should not fire due to cooldown
	am.checkRules()
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt64(&fireCount) != 1 {
		t.Errorf("Expected still 1 fire due to cooldown, got %d", atomic.LoadInt64(&fireCount))
	}
}

// TestAlertManagerCheckRulesNoCondition tests rule with nil condition
func TestAlertManagerCheckRulesNoCondition(t *testing.T) {
	am := NewAlertManager()

	rule := &AlertRule{
		Name:        "test_no_condition",
		Description: "Test rule with no condition",
		Severity:    SeverityInfo,
		Threshold:   1.0,
		Cooldown:    0,
		Condition:   nil, // No condition
	}

	am.RegisterRule(rule)

	// Should not panic
	am.checkRules()
}
