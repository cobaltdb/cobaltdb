package metrics

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestCounterDeltaConditionSingleSpikeDoesNotFireForever verifies that a
// cumulative counter crossing the threshold once produces exactly one firing
// evaluation, not an alert on every subsequent check (the old rules compared
// lifetime totals against the threshold).
func TestCounterDeltaConditionSingleSpikeDoesNotFireForever(t *testing.T) {
	var counter int64
	cond := counterDeltaCondition(func() int64 { return atomic.LoadInt64(&counter) }, 10)

	// First evaluation records the baseline only.
	if fired, _ := cond(); fired {
		t.Fatal("first evaluation must not fire (baseline)")
	}

	// Counter spikes past the threshold once.
	atomic.AddInt64(&counter, 50)
	fired, value := cond()
	if !fired {
		t.Fatal("expected delta spike to fire")
	}
	if value != 50 {
		t.Fatalf("expected delta value 50, got %v", value)
	}

	// The counter stays at its (high) lifetime total; deltas are now zero and
	// the rule must recover instead of firing on every check.
	for i := 0; i < 10; i++ {
		if fired, value := cond(); fired {
			t.Fatalf("evaluation %d fired on stale lifetime total (delta value %v)", i, value)
		}
	}

	// Small increments below the threshold do not fire.
	atomic.AddInt64(&counter, 5)
	if fired, _ := cond(); fired {
		t.Fatal("sub-threshold delta fired")
	}

	// A second genuine spike fires again.
	atomic.AddInt64(&counter, 100)
	if fired, _ := cond(); !fired {
		t.Fatal("second spike did not fire")
	}
}

// TestCounterDeltaConditionHandlesReset verifies counter resets (e.g. metrics
// source restart) do not produce huge negative or bogus deltas.
func TestCounterDeltaConditionHandlesReset(t *testing.T) {
	var counter int64 = 1000
	cond := counterDeltaCondition(func() int64 { return atomic.LoadInt64(&counter) }, 10)
	cond() // baseline at 1000

	atomic.StoreInt64(&counter, 0) // reset
	if fired, value := cond(); fired || value != 0 {
		t.Fatalf("counter reset mis-handled: fired=%v value=%v", fired, value)
	}
}

// TestDefaultAlertRulesCounterRulesUseDeltas sanity-checks the wired default
// rules: with no new activity, the counter-backed rules must not fire even if
// the lifetime totals are large, and via the manager a single spike alerts
// once and then recovers (no alert every check interval forever).
func TestDefaultAlertRulesCounterRulesUseDeltas(t *testing.T) {
	tm := GetTransactionMetrics()

	rules := DefaultAlertRules()
	var deadlockRule *AlertRule
	for _, r := range rules {
		if r.Name == "high_deadlock_rate" {
			deadlockRule = r
		}
	}
	if deadlockRule == nil {
		t.Fatal("high_deadlock_rate rule missing")
	}

	deadlockRule.Condition() // baseline

	// Spike the deadlock counter past the threshold.
	for i := 0; i < 25; i++ {
		tm.RecordDeadlock()
	}
	if fired, _ := deadlockRule.Condition(); !fired {
		t.Fatal("expected deadlock spike to fire")
	}
	// No further deadlocks: the rule must recover even though the lifetime
	// total remains above the threshold.
	for i := 0; i < 5; i++ {
		if fired, value := deadlockRule.Condition(); fired {
			t.Fatalf("rule keeps firing on lifetime total (delta %v)", value)
		}
	}
}

// TestAlertManagerCounterSpikeAlertsOnce runs the full manager loop against a
// delta-based rule and verifies a single spike produces one alert, not one per
// check.
func TestAlertManagerCounterSpikeAlertsOnce(t *testing.T) {
	am := NewAlertManager()

	var counter int64
	rule := &AlertRule{
		Name:        "delta_spike",
		Description: "delta spike test",
		Severity:    SeverityWarning,
		Threshold:   10,
		// Cooldown 0 so the manager's separate OK→ALERT rapid-retrigger
		// suppression (covered by its own tests) doesn't defer the first
		// firing; this test isolates the delta-condition behavior.
		Cooldown:  0,
		Condition: counterDeltaCondition(func() int64 { return atomic.LoadInt64(&counter) }, 10),
	}
	am.RegisterRule(rule)

	am.checkRules() // baseline
	atomic.AddInt64(&counter, 100)
	am.checkRules() // fires once
	for i := 0; i < 8; i++ {
		am.checkRules() // must not fire again: delta is zero
	}

	time.Sleep(50 * time.Millisecond) // let handler goroutines settle
	alerts := am.GetAlerts(0)
	count := 0
	for _, a := range alerts {
		if a.RuleName == "delta_spike" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 alert for a single counter spike, got %d", count)
	}
}
