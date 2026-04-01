package metrics

import (
	"testing"
	"time"
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

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Stop should not block
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
