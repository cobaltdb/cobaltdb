package engine

import (
	"context"
	"testing"
	"time"
)

func TestDefaultAlertManagerConfig(t *testing.T) {
	config := DefaultAlertManagerConfig()

	if !config.Enabled {
		t.Error("Expected alert manager to be enabled by default")
	}

	if config.EvaluationInterval != 30*time.Second {
		t.Errorf("Expected evaluation interval 30s, got %v", config.EvaluationInterval)
	}

	if config.RepeatInterval != 4*time.Hour {
		t.Errorf("Expected repeat interval 4h, got %v", config.RepeatInterval)
	}
}

func TestAlertRuleEvaluate(t *testing.T) {
	rule := &AlertRule{
		Condition: ">",
		Threshold: 80,
	}

	// Value above threshold
	if !rule.Evaluate(85) {
		t.Error("Should fire when value > threshold")
	}

	// Value below threshold
	if rule.Evaluate(75) {
		t.Error("Should not fire when value < threshold")
	}

	// Test other conditions
	rule.Condition = "<"
	if !rule.Evaluate(75) {
		t.Error("Should fire when value < threshold")
	}

	rule.Condition = ">="
	if !rule.Evaluate(80) {
		t.Error("Should fire when value >= threshold")
	}

	rule.Condition = "<="
	if !rule.Evaluate(80) {
		t.Error("Should fire when value <= threshold")
	}

	rule.Condition = "=="
	if !rule.Evaluate(80) {
		t.Error("Should fire when value == threshold")
	}

	rule.Condition = "!="
	if !rule.Evaluate(75) {
		t.Error("Should fire when value != threshold")
	}
}

func TestAlertIsResolved(t *testing.T) {
	alert := &Alert{
		State: AlertStateFiring,
	}

	if alert.IsResolved() {
		t.Error("Firing alert should not be resolved")
	}

	alert.State = AlertStateResolved
	if !alert.IsResolved() {
		t.Error("Resolved alert should be resolved")
	}
}

func TestAlertDuration(t *testing.T) {
	start := time.Now().Add(-5 * time.Minute)
	alert := &Alert{
		StartsAt: start,
		State:    AlertStateFiring,
	}

	duration := alert.Duration()
	if duration < 4*time.Minute || duration > 6*time.Minute {
		t.Errorf("Expected duration ~5m, got %v", duration)
	}

	// Resolved alert
	end := time.Now()
	alert.EndsAt = &end
	alert.StartsAt = end.Add(-10 * time.Minute)

	duration = alert.Duration()
	if duration != 10*time.Minute {
		t.Errorf("Expected duration 10m, got %v", duration)
	}
}

func TestSilenceMatches(t *testing.T) {
	silence := &Silence{
		Matchers: map[string]string{
			"severity": "critical",
			"category": "system",
		},
	}

	alert := &Alert{
		Labels: map[string]string{
			"severity": "critical",
			"category": "system",
			"host":     "server1",
		},
	}

	if !silence.Matches(alert) {
		t.Error("Silence should match alert with all labels")
	}

	// Non-matching alert
	alert2 := &Alert{
		Labels: map[string]string{
			"severity": "warning",
			"category": "system",
		},
	}

	if silence.Matches(alert2) {
		t.Error("Silence should not match alert with different severity")
	}
}

func TestSilenceIsActive(t *testing.T) {
	now := time.Now()

	silence := &Silence{
		StartsAt: now.Add(-1 * time.Hour),
		EndsAt:   now.Add(1 * time.Hour),
	}

	if !silence.IsActive() {
		t.Error("Silence should be active")
	}

	// Past silence
	silence.EndsAt = now.Add(-1 * time.Hour)
	if silence.IsActive() {
		t.Error("Past silence should not be active")
	}

	// Future silence
	silence.StartsAt = now.Add(1 * time.Hour)
	silence.EndsAt = now.Add(2 * time.Hour)
	if silence.IsActive() {
		t.Error("Future silence should not be active")
	}
}

func TestAlertManagerAddRule(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	rule := &AlertRule{
		ID:      "test_rule",
		Name:    "Test Rule",
		Query:   "cpu_usage",
		Enabled: true,
	}

	err := am.AddRule(rule)
	if err != nil {
		t.Fatalf("Failed to add rule: %v", err)
	}

	// Rule without ID
	invalidRule := &AlertRule{
		Name: "No ID",
	}
	err = am.AddRule(invalidRule)
	if err == nil {
		t.Error("Expected error for rule without ID")
	}

	// Rule without name
	invalidRule2 := &AlertRule{
		ID: "test2",
	}
	err = am.AddRule(invalidRule2)
	if err == nil {
		t.Error("Expected error for rule without name")
	}
}

func TestAlertManagerRemoveRule(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	am.AddRule(&AlertRule{
		ID:   "rule1",
		Name: "Rule 1",
	})

	err := am.RemoveRule("rule1")
	if err != nil {
		t.Errorf("Failed to remove rule: %v", err)
	}

	err = am.RemoveRule("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent rule")
	}
}

func TestAlertManagerAddChannel(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	channel := &NotificationChannel{
		ID:   "log_channel",
		Name: "Log Channel",
		Type: "log",
	}

	err := am.AddChannel(channel)
	if err != nil {
		t.Fatalf("Failed to add channel: %v", err)
	}

	// Channel without ID
	invalidChannel := &NotificationChannel{
		Name: "No ID",
	}
	err = am.AddChannel(invalidChannel)
	if err == nil {
		t.Error("Expected error for channel without ID")
	}
}

func TestAlertManagerAddSilence(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	silence := &Silence{
		Matchers:  map[string]string{"severity": "critical"},
		StartsAt:  time.Now(),
		EndsAt:    time.Now().Add(1 * time.Hour),
		Comment:   "Maintenance window",
		CreatedBy: "admin",
	}

	err := am.AddSilence(silence)
	if err != nil {
		t.Fatalf("Failed to add silence: %v", err)
	}

	if silence.ID == "" {
		t.Error("Expected silence ID to be generated")
	}

	// Remove silence
	err = am.RemoveSilence(silence.ID)
	if err != nil {
		t.Errorf("Failed to remove silence: %v", err)
	}

	err = am.RemoveSilence("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent silence")
	}
}

func TestNotificationChannelSendLog(t *testing.T) {
	channel := &NotificationChannel{
		ID:   "log",
		Name: "Log",
		Type: "log",
	}

	alert := &Alert{
		Name:      "Test Alert",
		Severity:  SeverityCritical,
		Summary:   "Test summary",
		Value:     90,
		Threshold: 80,
	}

	ctx := context.Background()
	err := channel.Send(ctx, alert)
	if err != nil {
		t.Errorf("Failed to send log notification: %v", err)
	}
}

func TestNotificationChannelDisabled(t *testing.T) {
	channel := &NotificationChannel{
		ID:       "log",
		Name:     "Log",
		Type:     "log",
		Disabled: true,
	}

	alert := &Alert{Name: "Test"}

	ctx := context.Background()
	err := channel.Send(ctx, alert)
	if err != nil {
		t.Error("Disabled channel should return no error")
	}
}

func TestAlertManagerStartStop(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.EvaluationInterval = 100 * time.Millisecond
	am := NewAlertManager(config)

	// Start
	am.Start()
	if !am.running.Load() {
		t.Error("Expected alert manager to be running")
	}

	// Let it run briefly
	time.Sleep(150 * time.Millisecond)

	// Stop
	am.Stop()
	if am.running.Load() {
		t.Error("Expected alert manager to be stopped")
	}
}

func TestAlertManagerCreateDefaultRules(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	am.CreateDefaultRules()

	stats := am.GetStats()
	if stats.TotalRules == 0 {
		t.Error("Expected default rules to be created")
	}

	if stats.EnabledRules == 0 {
		t.Error("Expected some enabled rules")
	}
}

func TestAlertManagerGetActiveAlerts(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	// Initially empty
	alerts := am.GetActiveAlerts()
	if len(alerts) != 0 {
		t.Errorf("Expected 0 alerts initially, got %d", len(alerts))
	}

	// Manually add an alert
	am.mu.Lock()
	am.alerts["test"] = &Alert{
		ID:       "test",
		Name:     "Test",
		State:    AlertStateFiring,
		StartsAt: time.Now(),
	}
	am.mu.Unlock()

	alerts = am.GetActiveAlerts()
	if len(alerts) != 1 {
		t.Errorf("Expected 1 active alert, got %d", len(alerts))
	}
}

func TestAlertManagerGetAllAlerts(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	// Add both firing and resolved alerts
	am.mu.Lock()
	am.alerts["firing"] = &Alert{
		ID:       "firing",
		Name:     "Firing",
		State:    AlertStateFiring,
		StartsAt: time.Now(),
	}
	am.alerts["resolved"] = &Alert{
		ID:       "resolved",
		Name:     "Resolved",
		State:    AlertStateResolved,
		StartsAt: time.Now(),
		EndsAt:   &[]time.Time{time.Now()}[0],
	}
	am.mu.Unlock()

	alerts := am.GetAllAlerts()
	if len(alerts) != 2 {
		t.Errorf("Expected 2 alerts, got %d", len(alerts))
	}
}

func TestAlertManagerGetSilences(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	// Initially empty
	silences := am.GetSilences()
	if len(silences) != 0 {
		t.Errorf("Expected 0 silences initially, got %d", len(silences))
	}

	// Add a silence
	am.AddSilence(&Silence{
		Matchers: map[string]string{"severity": "critical"},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(1 * time.Hour),
	})

	silences = am.GetSilences()
	if len(silences) != 1 {
		t.Errorf("Expected 1 silence, got %d", len(silences))
	}
}

func TestAlertManagerGetStats(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	// Empty stats
	stats := am.GetStats()
	if stats.TotalRules != 0 {
		t.Errorf("Expected 0 rules, got %d", stats.TotalRules)
	}

	// Add rules
	am.AddRule(&AlertRule{
		ID:      "rule1",
		Name:    "Rule 1",
		Enabled: true,
	})
	am.AddRule(&AlertRule{
		ID:      "rule2",
		Name:    "Rule 2",
		Enabled: false,
	})

	// Add channel
	am.AddChannel(&NotificationChannel{
		ID:   "ch1",
		Name: "Channel 1",
		Type: "log",
	})

	stats = am.GetStats()
	if stats.TotalRules != 2 {
		t.Errorf("Expected 2 rules, got %d", stats.TotalRules)
	}
	if stats.EnabledRules != 1 {
		t.Errorf("Expected 1 enabled rule, got %d", stats.EnabledRules)
	}
	if stats.TotalChannels != 1 {
		t.Errorf("Expected 1 channel, got %d", stats.TotalChannels)
	}
}

func TestAlertManagerIsSilenced(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	// Add active silence
	silence := &Silence{
		ID:       "sil1",
		Matchers: map[string]string{"severity": "critical"},
		StartsAt: time.Now().Add(-1 * time.Hour),
		EndsAt:   time.Now().Add(1 * time.Hour),
	}
	am.AddSilence(silence)

	// Check if alert is silenced
	alert := &Alert{
		Labels: map[string]string{"severity": "critical"},
		State:  AlertStateFiring,
	}

	am.mu.RLock()
	isSilenced := am.isSilenced(alert)
	am.mu.RUnlock()

	if !isSilenced {
		t.Error("Alert should be silenced")
	}

	if alert.State != AlertStateSilenced {
		t.Error("Alert state should be changed to silenced")
	}

	// Non-matching alert
	alert2 := &Alert{
		Labels: map[string]string{"severity": "warning"},
		State:  AlertStateFiring,
	}

	am.mu.RLock()
	isSilenced2 := am.isSilenced(alert2)
	am.mu.RUnlock()

	if isSilenced2 {
		t.Error("Non-matching alert should not be silenced")
	}
}

func TestAlertManagerProcessRuleEvaluation(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	am := NewAlertManager(config)

	// Add a rule with zero duration
	rule := &AlertRule{
		ID:        "cpu_high",
		Name:      "CPU High",
		Condition: ">",
		Threshold: 80,
		Duration:  0,
		Enabled:   true,
	}
	am.AddRule(rule)

	// Trigger alert - first call starts pending
	am.processRuleEvaluation(rule, 90)

	// Alert should be pending, not firing yet (need second evaluation with duration=0)
	am.mu.RLock()
	_, exists := am.alerts["cpu_high:CPU High"]
	am.mu.RUnlock()

	if exists {
		t.Fatal("Alert should not exist yet, should be pending")
	}

	// Second call transitions from pending to firing
	am.processRuleEvaluation(rule, 90)

	am.mu.RLock()
	alert, exists := am.alerts["cpu_high:CPU High"]
	am.mu.RUnlock()

	if !exists {
		t.Fatal("Alert should exist after second evaluation")
	}

	if alert.State != AlertStateFiring {
		t.Errorf("Alert should be firing, got %s", alert.State)
	}

	// Resolve the alert
	am.processRuleEvaluation(rule, 70)

	am.mu.RLock()
	alert = am.alerts["cpu_high:CPU High"]
	am.mu.RUnlock()

	if alert.State != AlertStateResolved {
		t.Errorf("Alert should be resolved, got %s", alert.State)
	}
}

func TestAlertManagerCleanupOldAlerts(t *testing.T) {
	config := DefaultAlertManagerConfig()
	config.Enabled = false
	config.Retention = 1 * time.Hour
	am := NewAlertManager(config)

	// Add old resolved alert
	oldTime := time.Now().Add(-2 * time.Hour)
	am.mu.Lock()
	am.alerts["old"] = &Alert{
		ID:     "old",
		Name:   "Old",
		State:  AlertStateResolved,
		EndsAt: &oldTime,
	}
	am.mu.Unlock()

	// Cleanup
	am.cleanupOldAlerts()

	// Old alert should be removed
	am.mu.RLock()
	_, exists := am.alerts["old"]
	am.mu.RUnlock()

	if exists {
		t.Error("Old alert should be cleaned up")
	}
}
