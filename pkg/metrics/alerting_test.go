package metrics

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// TestAlertSeverityString tests the String method for AlertSeverity
func TestAlertSeverityString(t *testing.T) {
	tests := []struct {
		severity AlertSeverity
		expected string
	}{
		{SeverityInfo, "INFO"},
		{SeverityWarning, "WARNING"},
		{SeverityCritical, "CRITICAL"},
		{AlertSeverity(99), "UNKNOWN"},
	}

	for _, tc := range tests {
		result := tc.severity.String()
		if result != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, result)
		}
	}
}

// TestNewAlertManager tests creating a new alert manager
func TestNewAlertManager(t *testing.T) {
	am := NewAlertManager()
	if am == nil {
		t.Fatal("AlertManager is nil")
	}
	if am.rules == nil {
		t.Error("rules map is nil")
	}
	if am.alerts == nil {
		t.Error("alerts slice is nil")
	}
	if am.handlers == nil {
		t.Error("handlers slice is nil")
	}
	if am.stopCh == nil {
		t.Error("stopCh is nil")
	}
	if am.checkInterval != 10*time.Second {
		t.Errorf("Expected checkInterval 10s, got %v", am.checkInterval)
	}
}

// TestAlertManagerRegisterRule tests registering alert rules
func TestAlertManagerRegisterRule(t *testing.T) {
	am := NewAlertManager()

	rule := &AlertRule{
		Name:        "test_rule",
		Description: "Test rule",
		Severity:    SeverityWarning,
		Threshold:   100,
		Cooldown:    5 * time.Minute,
		Condition: func() (bool, float64) {
			return false, 0
		},
	}

	am.RegisterRule(rule)

	// Check rule was registered
	am.mu.RLock()
	if _, exists := am.rules["test_rule"]; !exists {
		t.Error("Rule was not registered")
	}
	am.mu.RUnlock()
}

func TestAlertManagerRegisterNilRule(t *testing.T) {
	am := NewAlertManager()

	am.RegisterRule(nil)

	am.mu.RLock()
	defer am.mu.RUnlock()
	if len(am.rules) != 0 {
		t.Fatalf("expected nil rule to be ignored, got %d rules", len(am.rules))
	}
}

func TestAlertManagerRegisterRuleCopiesInput(t *testing.T) {
	am := NewAlertManager()

	rule := &AlertRule{
		Name:        "copy_rule",
		Description: "original",
		Severity:    SeverityWarning,
		Threshold:   100,
		Cooldown:    time.Minute,
		Condition: func() (bool, float64) {
			return false, 0
		},
	}

	am.RegisterRule(rule)
	rule.Description = "mutated"
	rule.Severity = SeverityCritical
	rule.Threshold = 1
	rule.Cooldown = 0
	rule.Condition = func() (bool, float64) {
		return true, 999
	}

	am.mu.RLock()
	registered := am.rules["copy_rule"]
	am.mu.RUnlock()

	if registered.Description != "original" {
		t.Fatalf("registered rule description = %q, want original", registered.Description)
	}
	if registered.Severity != SeverityWarning {
		t.Fatalf("registered rule severity = %v, want %v", registered.Severity, SeverityWarning)
	}
	if registered.Threshold != 100 {
		t.Fatalf("registered rule threshold = %f, want 100", registered.Threshold)
	}
	if registered.Cooldown != time.Minute {
		t.Fatalf("registered rule cooldown = %v, want %v", registered.Cooldown, time.Minute)
	}
	fired, value := registered.Condition()
	if fired || value != 0 {
		t.Fatalf("registered rule condition was mutated: fired=%v value=%f", fired, value)
	}
}

// TestAlertManagerUnregisterRule tests unregistering alert rules
func TestAlertManagerUnregisterRule(t *testing.T) {
	am := NewAlertManager()

	rule := &AlertRule{
		Name:     "test_rule",
		Severity: SeverityWarning,
		Condition: func() (bool, float64) {
			return false, 0
		},
	}

	am.RegisterRule(rule)
	am.UnregisterRule("test_rule")

	// Check rule was unregistered
	am.mu.RLock()
	if _, exists := am.rules["test_rule"]; exists {
		t.Error("Rule was not unregistered")
	}
	am.mu.RUnlock()
}

// TestAlertManagerRegisterHandler tests registering alert handlers
func TestAlertManagerRegisterHandler(t *testing.T) {
	am := NewAlertManager()

	handler := &LogAlertHandler{}
	am.RegisterHandler(handler)

	// Check handler was registered
	am.mu.RLock()
	if len(am.handlers) != 1 {
		t.Errorf("Expected 1 handler, got %d", len(am.handlers))
	}
	am.mu.RUnlock()
}

func TestAlertManagerRegisterNilHandler(t *testing.T) {
	am := NewAlertManager()

	am.RegisterHandler(nil)

	am.mu.RLock()
	defer am.mu.RUnlock()
	if len(am.handlers) != 0 {
		t.Fatalf("expected nil handler to be ignored, got %d handlers", len(am.handlers))
	}
}

// TestLogAlertHandlerHandle tests the LogAlertHandler
func TestLogAlertHandlerHandle(t *testing.T) {
	handler := &LogAlertHandler{}

	alert := Alert{
		ID:        "TEST-123",
		RuleName:  "test_rule",
		Severity:  SeverityInfo,
		Message:   "Test message",
		Timestamp: time.Now(),
		Value:     50.5,
		Threshold: 100.0,
	}

	err := handler.Handle(alert)
	if err != nil {
		t.Errorf("Handle returned error: %v", err)
	}
}

func TestLogAlertHandlerUsesConfiguredLogger(t *testing.T) {
	var logs bytes.Buffer
	handler := NewLogAlertHandler(logger.New(logger.WarnLevel, &logs))

	alert := Alert{
		ID:        "TEST-LOG",
		RuleName:  "test_rule",
		Severity:  SeverityWarning,
		Message:   "Test message",
		Timestamp: time.Now(),
		Value:     50.5,
		Threshold: 100.0,
	}

	if err := handler.Handle(alert); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if !strings.Contains(logs.String(), "[ALERT] WARNING | test_rule") {
		t.Fatalf("expected alert log, got %q", logs.String())
	}
}

type failingAlertHandler struct {
	err error
}

func (h failingAlertHandler) Handle(Alert) error {
	return h.err
}

type panickingAlertHandler struct {
	called chan struct{}
}

func (h panickingAlertHandler) Handle(Alert) error {
	close(h.called)
	panic("handler panic")
}

type blockingAlertHandler struct {
	started *int64
	release <-chan struct{}
}

func (h blockingAlertHandler) Handle(Alert) error {
	atomic.AddInt64(h.started, 1)
	<-h.release
	return nil
}

type signalWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
	ch  chan struct{}
}

func newSignalWriter() *signalWriter {
	return &signalWriter{ch: make(chan struct{}, 1)}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.buf.Write(p)
	w.mu.Unlock()
	select {
	case w.ch <- struct{}{}:
	default:
	}
	return n, err
}

func (w *signalWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestAlertManagerHandlerErrorUsesConfiguredLogger(t *testing.T) {
	logs := newSignalWriter()
	am := NewAlertManager()
	am.SetLogger(logger.New(logger.ErrorLevel, logs))
	am.RegisterHandler(failingAlertHandler{err: errors.New("handler failed")})
	am.RegisterRule(&AlertRule{
		Name:        "test_rule",
		Description: "Test rule",
		Severity:    SeverityCritical,
		Threshold:   1,
		Condition: func() (bool, float64) {
			return true, 2
		},
	})

	am.checkRules()

	select {
	case <-logs.ch:
	case <-time.After(time.Second):
		t.Fatalf("expected handler error log, got %q", logs.String())
	}
	if !strings.Contains(logs.String(), "handler failed") {
		t.Fatalf("expected handler error log, got %q", logs.String())
	}
}

func TestAlertManagerCheckRulesSkipsNilHandlers(t *testing.T) {
	am := NewAlertManager()
	am.RegisterRule(&AlertRule{
		Name:        "test_rule",
		Description: "Test rule",
		Severity:    SeverityCritical,
		Threshold:   1,
		Condition: func() (bool, float64) {
			return true, 2
		},
	})

	am.mu.Lock()
	am.handlers = append(am.handlers, nil)
	am.mu.Unlock()

	am.checkRules()
}

func TestAlertManagerHandlerPanicIsRecovered(t *testing.T) {
	am := NewAlertManager()
	called := make(chan struct{})
	am.RegisterHandler(panickingAlertHandler{called: called})
	am.RegisterRule(&AlertRule{
		Name:        "test_rule",
		Description: "Test rule",
		Severity:    SeverityCritical,
		Threshold:   1,
		Condition: func() (bool, float64) {
			return true, 2
		},
	})

	am.checkRules()

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("expected panicking handler to be called")
	}
}

func TestAlertManagerLimitsConcurrentHandlers(t *testing.T) {
	am := NewAlertManager()
	am.handlerSem = make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)

	var started int64
	for i := 0; i < 10; i++ {
		am.RegisterHandler(blockingAlertHandler{
			started: &started,
			release: release,
		})
	}
	am.RegisterRule(&AlertRule{
		Name:        "test_rule",
		Description: "Test rule",
		Severity:    SeverityCritical,
		Threshold:   1,
		Condition: func() (bool, float64) {
			return true, 2
		},
	})

	am.checkRules()

	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt64(&started) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt64(&started); got != 2 {
		t.Fatalf("expected exactly 2 handlers to start, got %d", got)
	}

	time.Sleep(25 * time.Millisecond)
	if got := atomic.LoadInt64(&started); got != 2 {
		t.Fatalf("handler concurrency limit was exceeded, got %d started handlers", got)
	}
}

// TestCallbackAlertHandlerHandle tests the CallbackAlertHandler
func TestCallbackAlertHandlerHandle(t *testing.T) {
	var called bool
	var receivedAlert Alert

	handler := &CallbackAlertHandler{
		Callback: func(a Alert) {
			called = true
			receivedAlert = a
		},
	}

	alert := Alert{
		ID:       "TEST-456",
		RuleName: "test_rule",
		Severity: SeverityWarning,
	}

	err := handler.Handle(alert)
	if err != nil {
		t.Errorf("Handle returned error: %v", err)
	}

	if !called {
		t.Error("Callback was not called")
	}
	if receivedAlert.ID != "TEST-456" {
		t.Error("Wrong alert received in callback")
	}
}

// TestCallbackAlertHandlerHandleNilCallback tests CallbackAlertHandler with nil callback
func TestCallbackAlertHandlerHandleNilCallback(t *testing.T) {
	handler := &CallbackAlertHandler{
		Callback: nil,
	}

	alert := Alert{
		ID:       "TEST-789",
		RuleName: "test_rule",
	}

	err := handler.Handle(alert)
	if err != nil {
		t.Errorf("Handle returned error: %v", err)
	}
}

// TestAlertManagerGetAlerts tests getting alerts
func TestAlertManagerGetAlerts(t *testing.T) {
	am := NewAlertManager()

	// Add some test alerts
	am.mu.Lock()
	am.alerts = []Alert{
		{ID: "1", RuleName: "rule1", Severity: SeverityInfo},
		{ID: "2", RuleName: "rule2", Severity: SeverityWarning},
		{ID: "3", RuleName: "rule3", Severity: SeverityCritical},
	}
	am.mu.Unlock()

	// Test getting all alerts
	alerts := am.GetAlerts(0)
	if len(alerts) != 3 {
		t.Errorf("Expected 3 alerts, got %d", len(alerts))
	}

	// Test getting limited alerts
	alerts = am.GetAlerts(2)
	if len(alerts) != 2 {
		t.Errorf("Expected 2 alerts, got %d", len(alerts))
	}

	// Test getting more than available
	alerts = am.GetAlerts(100)
	if len(alerts) != 3 {
		t.Errorf("Expected 3 alerts, got %d", len(alerts))
	}
}

// TestAlertManagerMuteUnmuteRule tests muting and unmuting rules
func TestAlertManagerMuteUnmuteRule(t *testing.T) {
	am := NewAlertManager()

	rule := &AlertRule{
		Name:     "test_rule",
		Severity: SeverityWarning,
		muted:    false,
		Condition: func() (bool, float64) {
			return false, 0
		},
	}

	am.RegisterRule(rule)

	// Mute the rule
	am.MuteRule("test_rule")

	am.mu.RLock()
	if !am.rules["test_rule"].muted {
		t.Error("Rule should be muted")
	}
	am.mu.RUnlock()

	// Unmute the rule
	am.UnmuteRule("test_rule")

	am.mu.RLock()
	if am.rules["test_rule"].muted {
		t.Error("Rule should not be muted")
	}
	am.mu.RUnlock()
}

// TestAlertManagerMuteUnmuteNonExistentRule tests muting non-existent rules
func TestAlertManagerMuteUnmuteNonExistentRule(t *testing.T) {
	am := NewAlertManager()

	// Should not panic
	am.MuteRule("non_existent")
	am.UnmuteRule("non_existent")
}

// TestDefaultAlertRules tests the default alert rules
func TestDefaultAlertRules(t *testing.T) {
	rules := DefaultAlertRules()
	if len(rules) == 0 {
		t.Error("Expected default alert rules, got none")
	}

	// Check each rule has required fields
	for _, rule := range rules {
		if rule.Name == "" {
			t.Error("Rule name is empty")
		}
		if rule.Description == "" {
			t.Error("Rule description is empty")
		}
		if rule.Condition == nil {
			t.Error("Rule condition is nil")
		}

		// Test the condition doesn't panic
		_, _ = rule.Condition()
	}
}

// TestGetAlertManager tests the global alert manager
func TestGetAlertManager(t *testing.T) {
	am1 := GetAlertManager()
	if am1 == nil {
		t.Fatal("GetAlertManager returned nil")
	}

	// Should return the same instance
	am2 := GetAlertManager()
	if am1 != am2 {
		t.Error("GetAlertManager should return the same instance")
	}
}

// TestAlertManagerStartStop tests starting and stopping the alert manager
func TestAlertManagerStartStop(t *testing.T) {
	am := NewAlertManager()

	// Start should not block
	am.Start()
	am.Start()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Stop should not block
	am.Stop()
	am.Stop()
}

// TestGenerateAlertID tests alert ID generation
func TestGenerateAlertID(t *testing.T) {
	id1 := generateAlertID()
	id2 := generateAlertID()

	if id1 == "" {
		t.Error("Generated ID is empty")
	}
	if id1 == id2 {
		t.Error("Generated IDs should be unique")
	}
}

func TestGenerateAlertIDConcurrent(t *testing.T) {
	const goroutines = 64

	var wg sync.WaitGroup
	ids := make(chan string, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids <- generateAlertID()
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, goroutines)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate alert ID generated: %s", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != goroutines {
		t.Fatalf("expected %d IDs, got %d", goroutines, len(seen))
	}
}

// countingAlertHandler counts invocations and signals a WaitGroup when Handle is called.
type countingAlertHandler struct {
	wg *sync.WaitGroup
	n  *int64
}

func (h *countingAlertHandler) Handle(Alert) error {
	atomic.AddInt64(h.n, 1)
	if h.wg != nil {
		h.wg.Done()
	}
	return nil
}

// TestAlertManagerCooldownSustainedFiring verifies that the cooldown does NOT suppress
// alerts when the condition keeps firing continuously. The cooldown should only suppress
// rapid re-triggering AFTER a recovery, not during sustained firing.
// Bug: the original code set lastFired on every fire but never reset it on recovery,
// so sustained firing incorrectly appeared to "cooldown" and subsequent evaluations
// would be suppressed until the cooldown elapsed.
func TestAlertManagerCooldownSustainedFiring(t *testing.T) {
	am := NewAlertManager()

	var n int64
	var wg sync.WaitGroup
	h := &countingAlertHandler{wg: &wg, n: &n}

	rule := &AlertRule{
		Name:        "sustained",
		Description: "Sustained test rule",
		Severity:    SeverityWarning,
		Condition:   func() (bool, float64) { return true, 1.0 },
		Threshold:   0,
		Cooldown:    5 * time.Minute, // deliberately long; should never suppress
	}
	am.RegisterRule(rule)
	am.handlers = []AlertHandler{h}

	// 10 evaluations with condition always firing
	for i := 0; i < 10; i++ {
		wg.Add(1)
		am.checkRules()
		wg.Wait() // wait for goroutines to finish
		time.Sleep(10 * time.Millisecond)
	}

	// Original bug: only 1 alert would fire because lastFired was set on every fire
	// but never reset on recovery, so time.Since(lastFired) stayed small and
	// subsequent fires were suppressed by the long cooldown.
	// Fix: wasFiring tracks whether the condition was firing in the previous
	// evaluation; continuous firing bypasses the cooldown check.
	if got := atomic.LoadInt64(&n); got < 5 {
		t.Errorf("expected ≥5 alerts during sustained firing, got %d", got)
	}
}

// TestAlertManagerCooldownSuppressesRapidReTrigger verifies that the cooldown DOES
// suppress rapid re-triggering when the condition oscillates ALERT→OK→ALERT within
// the cooldown window.
func TestAlertManagerCooldownSuppressesRapidReTrigger(t *testing.T) {
	am := NewAlertManager()

	var n int64
	var wg sync.WaitGroup
	h := &countingAlertHandler{wg: &wg, n: &n}

	rule := &AlertRule{
		Name:        "rapid",
		Description: "Rapid oscillation test rule",
		Severity:    SeverityWarning,
		Condition:   func() (bool, float64) { return true, 1.0 },
		Threshold:   0,
		Cooldown:    1 * time.Second,
	}
	am.RegisterRule(rule)
	am.handlers = []AlertHandler{h}

	// First fire
	wg.Add(1)
	am.checkRules()
	wg.Wait()

	// Recover immediately: must update am.rules["rapid"] (clone) not local rule.
	am.mu.Lock()
	am.rules["rapid"].Condition = func() (bool, float64) { return false, 0 }
	am.mu.Unlock()
	am.checkRules()

	// Fire again immediately (within cooldown window) — should be suppressed
	am.mu.Lock()
	am.rules["rapid"].Condition = func() (bool, float64) { return true, 1.0 }
	am.mu.Unlock()
	am.checkRules() // no wg.Add: if suppressed no goroutine runs; if fired goroutine is orphaned
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&n); got != 1 {
		t.Errorf("rapid re-trigger within cooldown: got %d alerts, want 1 (suppressed)", got)
	}
}

// TestAlertManagerCooldownAllowsFireAfterCooldown verifies that after the cooldown
// elapses, the alert fires normally.
func TestAlertManagerCooldownAllowsFireAfterCooldown(t *testing.T) {
	am := NewAlertManager()

	var n int64
	var wg sync.WaitGroup
	h := &countingAlertHandler{wg: &wg, n: &n}

	rule := &AlertRule{
		Name:        "after",
		Description: "Fire after cooldown test rule",
		Severity:    SeverityWarning,
		Condition:   func() (bool, float64) { return true, 1.0 },
		Threshold:   0,
		Cooldown:    50 * time.Millisecond,
	}
	am.RegisterRule(rule)
	am.handlers = []AlertHandler{h}

	// First fire
	wg.Add(1)
	am.checkRules()
	wg.Wait()

	// Recover (no alert fires, no goroutine)
	am.mu.Lock()
	am.rules["after"].Condition = func() (bool, float64) { return false, 0 }
	am.mu.Unlock()
	am.checkRules()

	// Wait for cooldown to elapse
	time.Sleep(75 * time.Millisecond)

	// Fire again after cooldown — should fire
	am.mu.Lock()
	am.rules["after"].Condition = func() (bool, float64) { return true, 1.0 }
	am.mu.Unlock()
	wg.Add(1)
	am.checkRules()
	wg.Wait()

	if got := atomic.LoadInt64(&n); got != 2 {
		t.Errorf("fire after cooldown elapsed: got %d alerts, want 2", got)
	}
}
