package metrics

import (
	"bytes"
	"errors"
	"strings"
	"sync"
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
