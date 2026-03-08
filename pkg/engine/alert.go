package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// AlertSeverity represents the severity of an alert
type AlertSeverity string

const (
	SeverityCritical AlertSeverity = "critical"
	SeverityWarning  AlertSeverity = "warning"
	SeverityInfo     AlertSeverity = "info"
)

// AlertState represents the state of an alert
type AlertState string

const (
	AlertStatePending  AlertState = "pending"
	AlertStateFiring   AlertState = "firing"
	AlertStateResolved AlertState = "resolved"
	AlertStateSilenced AlertState = "silenced"
)

// Alert represents a single alert instance
type Alert struct {
	ID           string
	Name         string
	Severity     AlertSeverity
	State        AlertState
	Summary      string
	Description  string
	Labels       map[string]string
	Value        float64
	Threshold    float64
	StartsAt     time.Time
	EndsAt       *time.Time
	GeneratorURL string
}

// IsResolved returns true if the alert is resolved
func (a *Alert) IsResolved() bool {
	return a.State == AlertStateResolved
}

// Duration returns how long the alert has been firing
func (a *Alert) Duration() time.Duration {
	if a.EndsAt != nil {
		return a.EndsAt.Sub(a.StartsAt)
	}
	return time.Since(a.StartsAt)
}

// AlertRule defines a rule for generating alerts
type AlertRule struct {
	ID          string
	Name        string
	Severity    AlertSeverity
	Summary     string
	Description string
	Query       string // Metric query or condition
	Condition   string // ">", "<", "==", "!=", ">=", "<="
	Threshold   float64
	Duration    time.Duration // How long condition must be true before firing
	Labels      map[string]string
	Annotations map[string]string
	Enabled     bool
	Group       string // Alert group for grouping related alerts
}

// Evaluate evaluates the alert rule against a value
func (ar *AlertRule) Evaluate(value float64) bool {
	switch ar.Condition {
	case ">":
		return value > ar.Threshold
	case "<":
		return value < ar.Threshold
	case ">=":
		return value >= ar.Threshold
	case "<=":
		return value <= ar.Threshold
	case "==":
		return value == ar.Threshold
	case "!=":
		return value != ar.Threshold
	default:
		return false
	}
}

// NotificationChannel defines how alerts should be sent
type NotificationChannel struct {
	ID       string
	Name     string
	Type     string // "webhook", "email", "log", "slack"
	Config   map[string]string
	Disabled bool
}

// Send sends a notification through this channel
func (nc *NotificationChannel) Send(ctx context.Context, alert *Alert) error {
	if nc.Disabled {
		return nil
	}

	switch nc.Type {
	case "webhook":
		return nc.sendWebhook(ctx, alert)
	case "log":
		return nc.sendLog(alert)
	case "slack":
		return nc.sendSlack(ctx, alert)
	default:
		return fmt.Errorf("unknown notification type: %s", nc.Type)
	}
}

// sendWebhook sends alert via HTTP webhook
func (nc *NotificationChannel) sendWebhook(ctx context.Context, alert *Alert) error {
	url, ok := nc.Config["url"]
	if !ok {
		return errors.New("webhook URL not configured")
	}

	payload, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

// sendLog logs the alert
func (nc *NotificationChannel) sendLog(alert *Alert) error {
	logMsg := fmt.Sprintf("ALERT [%s] %s: %s (value=%.2f, threshold=%.2f)",
		alert.Severity, alert.Name, alert.Summary, alert.Value, alert.Threshold)

	switch alert.Severity {
	case SeverityCritical:
		logger.Default().Error(logMsg)
	case SeverityWarning:
		logger.Default().Warn(logMsg)
	default:
		logger.Default().Info(logMsg)
	}

	return nil
}

// sendSlack sends alert to Slack
func (nc *NotificationChannel) sendSlack(ctx context.Context, alert *Alert) error {
	webhookURL, ok := nc.Config["webhook_url"]
	if !ok {
		return errors.New("Slack webhook URL not configured")
	}

	color := "good"
	if alert.Severity == SeverityWarning {
		color = "warning"
	} else if alert.Severity == SeverityCritical {
		color = "danger"
	}

	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{
			{
				"color": color,
				"title": fmt.Sprintf("[%s] %s", alert.Severity, alert.Name),
				"text":  alert.Description,
				"fields": []map[string]interface{}{
					{"title": "Value", "value": fmt.Sprintf("%.2f", alert.Value), "short": true},
					{"title": "Threshold", "value": fmt.Sprintf("%.2f", alert.Threshold), "short": true},
					{"title": "Duration", "value": alert.Duration().String(), "short": true},
				},
			},
		},
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", webhookURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

// Silence represents an alert silence period
type Silence struct {
	ID        string
	Matchers  map[string]string // Label matchers
	StartsAt  time.Time
	EndsAt    time.Time
	Comment   string
	CreatedBy string
}

// Matches checks if an alert matches this silence
func (s *Silence) Matches(alert *Alert) bool {
	for key, pattern := range s.Matchers {
		value, exists := alert.Labels[key]
		if !exists || value != pattern {
			return false
		}
	}
	return true
}

// IsActive returns true if the silence is currently active
func (s *Silence) IsActive() bool {
	now := time.Now()
	return now.After(s.StartsAt) && now.Before(s.EndsAt)
}

// AlertGroup groups related alerts
type AlertGroup struct {
	Name      string
	Labels    map[string]string
	Alerts    []*Alert
	Receivers []string // Notification channel IDs
}

// AlertManagerConfig configures the alert manager
type AlertManagerConfig struct {
	Enabled            bool
	EvaluationInterval time.Duration
	GroupWait          time.Duration
	GroupInterval      time.Duration
	RepeatInterval     time.Duration
	Retention          time.Duration
}

// DefaultAlertManagerConfig returns default configuration
func DefaultAlertManagerConfig() *AlertManagerConfig {
	return &AlertManagerConfig{
		Enabled:            true,
		EvaluationInterval: 30 * time.Second,
		GroupWait:          30 * time.Second,
		GroupInterval:      5 * time.Minute,
		RepeatInterval:     4 * time.Hour,
		Retention:          7 * 24 * time.Hour,
	}
}

// AlertManager manages alerts and notifications
type AlertManager struct {
	config   *AlertManagerConfig
	rules    map[string]*AlertRule
	channels map[string]*NotificationChannel
	alerts   map[string]*Alert
	silences map[string]*Silence
	groups   map[string]*AlertGroup

	// Pending alerts waiting for duration
	pending map[string]time.Time // alert ID -> when it started pending

	mu      sync.RWMutex
	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Metrics collector callback
	metricsFn func(string) (float64, error)
}

// NewAlertManager creates a new alert manager
func NewAlertManager(config *AlertManagerConfig) *AlertManager {
	if config == nil {
		config = DefaultAlertManagerConfig()
	}

	return &AlertManager{
		config:   config,
		rules:    make(map[string]*AlertRule),
		channels: make(map[string]*NotificationChannel),
		alerts:   make(map[string]*Alert),
		silences: make(map[string]*Silence),
		groups:   make(map[string]*AlertGroup),
		pending:  make(map[string]time.Time),
		stopCh:   make(chan struct{}),
	}
}

// SetMetricsCollector sets the function for collecting metrics
func (am *AlertManager) SetMetricsCollector(fn func(string) (float64, error)) {
	am.metricsFn = fn
}

// Start starts the alert manager
func (am *AlertManager) Start() {
	if !am.config.Enabled {
		return
	}

	if am.running.CompareAndSwap(false, true) {
		am.wg.Add(1)
		go am.evaluationLoop()
		logger.Default().Info("Alert manager started")
	}
}

// Stop stops the alert manager
func (am *AlertManager) Stop() {
	if am.running.CompareAndSwap(true, false) {
		close(am.stopCh)
		am.wg.Wait()
		logger.Default().Info("Alert manager stopped")
	}
}

// evaluationLoop evaluates alert rules periodically
func (am *AlertManager) evaluationLoop() {
	defer am.wg.Done()

	ticker := time.NewTicker(am.config.EvaluationInterval)
	defer ticker.Stop()

	// Evaluate immediately on start
	am.evaluateRules()

	for {
		select {
		case <-am.stopCh:
			return
		case <-ticker.C:
			am.evaluateRules()
			am.cleanupOldAlerts()
		}
	}
}

// evaluateRules evaluates all alert rules
func (am *AlertManager) evaluateRules() {
	am.mu.RLock()
	rules := make([]*AlertRule, 0, len(am.rules))
	for _, rule := range am.rules {
		if rule.Enabled {
			rules = append(rules, rule)
		}
	}
	am.mu.RUnlock()

	for _, rule := range rules {
		if am.metricsFn == nil {
			continue
		}

		value, err := am.metricsFn(rule.Query)
		if err != nil {
			logger.Default().Warnf("Failed to evaluate rule %s: %v", rule.Name, err)
			continue
		}

		am.processRuleEvaluation(rule, value)
	}
}

// processRuleEvaluation processes the result of a rule evaluation
func (am *AlertManager) processRuleEvaluation(rule *AlertRule, value float64) {
	alertID := fmt.Sprintf("%s:%s", rule.ID, rule.Name)

	am.mu.Lock()
	defer am.mu.Unlock()

	firing := rule.Evaluate(value)
	existingAlert, exists := am.alerts[alertID]

	if firing {
		if !exists {
			// Check if already pending
			if pendingSince, pending := am.pending[alertID]; pending {
				// Check if duration has elapsed
				if time.Since(pendingSince) >= rule.Duration {
					// Transition to firing
					delete(am.pending, alertID)

					alert := &Alert{
						ID:          alertID,
						Name:        rule.Name,
						Severity:    rule.Severity,
						State:       AlertStateFiring,
						Summary:     rule.Summary,
						Description: rule.Description,
						Labels:      rule.Labels,
						Value:       value,
						Threshold:   rule.Threshold,
						StartsAt:    time.Now(),
					}

					// Check silences
					if !am.isSilenced(alert) {
						am.alerts[alertID] = alert
						am.notify(alert)
					}
				}
			} else {
				// Start pending
				am.pending[alertID] = time.Now()
			}
		} else {
			// Update existing alert value
			existingAlert.Value = value
		}
	} else {
		// Not firing
		if exists {
			// Resolve the alert
			now := time.Now()
			existingAlert.State = AlertStateResolved
			existingAlert.EndsAt = &now

			// Send resolution notification
			am.notify(existingAlert)

			// Keep resolved alert for a while, then clean up
			go func() {
				time.Sleep(5 * time.Minute)
				am.mu.Lock()
				if am.alerts[alertID] != nil && am.alerts[alertID].State == AlertStateResolved {
					delete(am.alerts, alertID)
				}
				am.mu.Unlock()
			}()
		}

		// Remove from pending if present
		delete(am.pending, alertID)
	}
}

// isSilenced checks if an alert is silenced
func (am *AlertManager) isSilenced(alert *Alert) bool {
	for _, silence := range am.silences {
		if silence.IsActive() && silence.Matches(alert) {
			alert.State = AlertStateSilenced
			return true
		}
	}
	return false
}

// notify sends notifications for an alert
func (am *AlertManager) notify(alert *Alert) {
	for _, channel := range am.channels {
		if channel.Disabled {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := channel.Send(ctx, alert); err != nil {
			logger.Default().Errorf("Failed to send notification to %s: %v", channel.Name, err)
		}
		cancel()
	}
}

// AddRule adds an alert rule
func (am *AlertManager) AddRule(rule *AlertRule) error {
	if rule.ID == "" {
		return errors.New("rule ID is required")
	}
	if rule.Name == "" {
		return errors.New("rule name is required")
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	am.rules[rule.ID] = rule
	logger.Default().Infof("Alert rule added: %s", rule.Name)
	return nil
}

// RemoveRule removes an alert rule
func (am *AlertManager) RemoveRule(ruleID string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.rules[ruleID]; !exists {
		return fmt.Errorf("rule not found: %s", ruleID)
	}

	delete(am.rules, ruleID)
	return nil
}

// AddChannel adds a notification channel
func (am *AlertManager) AddChannel(channel *NotificationChannel) error {
	if channel.ID == "" {
		return errors.New("channel ID is required")
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	am.channels[channel.ID] = channel
	logger.Default().Infof("Notification channel added: %s", channel.Name)
	return nil
}

// RemoveChannel removes a notification channel
func (am *AlertManager) RemoveChannel(channelID string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.channels[channelID]; !exists {
		return fmt.Errorf("channel not found: %s", channelID)
	}

	delete(am.channels, channelID)
	return nil
}

// AddSilence adds a silence
func (am *AlertManager) AddSilence(silence *Silence) error {
	if silence.ID == "" {
		silence.ID = fmt.Sprintf("silence_%d", time.Now().UnixNano())
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	am.silences[silence.ID] = silence
	logger.Default().Infof("Silence added: %s", silence.Comment)
	return nil
}

// RemoveSilence removes a silence
func (am *AlertManager) RemoveSilence(silenceID string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.silences[silenceID]; !exists {
		return fmt.Errorf("silence not found: %s", silenceID)
	}

	delete(am.silences, silenceID)
	return nil
}

// GetActiveAlerts returns all active (firing) alerts
func (am *AlertManager) GetActiveAlerts() []*Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	alerts := make([]*Alert, 0)
	for _, alert := range am.alerts {
		if alert.State == AlertStateFiring {
			alerts = append(alerts, alert)
		}
	}
	return alerts
}

// GetAllAlerts returns all alerts including resolved
func (am *AlertManager) GetAllAlerts() []*Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	alerts := make([]*Alert, 0, len(am.alerts))
	for _, alert := range am.alerts {
		alerts = append(alerts, alert)
	}
	return alerts
}

// GetSilences returns all silences
func (am *AlertManager) GetSilences() []*Silence {
	am.mu.RLock()
	defer am.mu.RUnlock()

	silences := make([]*Silence, 0, len(am.silences))
	for _, silence := range am.silences {
		silences = append(silences, silence)
	}
	return silences
}

// cleanupOldAlerts removes old resolved alerts
func (am *AlertManager) cleanupOldAlerts() {
	if am.config.Retention <= 0 {
		return
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	cutoff := time.Now().Add(-am.config.Retention)
	for id, alert := range am.alerts {
		if alert.State == AlertStateResolved && alert.EndsAt != nil && alert.EndsAt.Before(cutoff) {
			delete(am.alerts, id)
		}
	}
}

// AlertManagerStats contains alert manager statistics
type AlertManagerStats struct {
	ActiveAlerts   int
	PendingAlerts  int
	ResolvedAlerts int
	SilencedAlerts int
	TotalRules     int
	EnabledRules   int
	TotalChannels  int
	ActiveSilences int
}

// GetStats returns alert manager statistics
func (am *AlertManager) GetStats() AlertManagerStats {
	am.mu.RLock()
	defer am.mu.RUnlock()

	stats := AlertManagerStats{
		TotalRules:    len(am.rules),
		TotalChannels: len(am.channels),
	}

	for _, rule := range am.rules {
		if rule.Enabled {
			stats.EnabledRules++
		}
	}

	for _, alert := range am.alerts {
		switch alert.State {
		case AlertStateFiring:
			stats.ActiveAlerts++
		case AlertStatePending:
			stats.PendingAlerts++
		case AlertStateResolved:
			stats.ResolvedAlerts++
		case AlertStateSilenced:
			stats.SilencedAlerts++
		}
	}

	for _, silence := range am.silences {
		if silence.IsActive() {
			stats.ActiveSilences++
		}
	}

	return stats
}

// CreateDefaultRules creates built-in alert rules
func (am *AlertManager) CreateDefaultRules() {
	rules := []*AlertRule{
		{
			ID:          "high_cpu_usage",
			Name:        "High CPU Usage",
			Severity:    SeverityWarning,
			Summary:     "CPU usage is above 80%",
			Description: "The system CPU usage has exceeded 80% for more than 5 minutes",
			Query:       "cpu_usage_percent",
			Condition:   ">",
			Threshold:   80,
			Duration:    5 * time.Minute,
			Labels:      map[string]string{"category": "system"},
			Enabled:     true,
		},
		{
			ID:          "high_memory_usage",
			Name:        "High Memory Usage",
			Severity:    SeverityWarning,
			Summary:     "Memory usage is above 85%",
			Description: "The system memory usage has exceeded 85% for more than 5 minutes",
			Query:       "memory_usage_percent",
			Condition:   ">",
			Threshold:   85,
			Duration:    5 * time.Minute,
			Labels:      map[string]string{"category": "system"},
			Enabled:     true,
		},
		{
			ID:          "low_disk_space",
			Name:        "Low Disk Space",
			Severity:    SeverityCritical,
			Summary:     "Disk space is below 10%",
			Description: "The disk free space has fallen below 10%",
			Query:       "disk_free_percent",
			Condition:   "<",
			Threshold:   10,
			Duration:    1 * time.Minute,
			Labels:      map[string]string{"category": "system"},
			Enabled:     true,
		},
		{
			ID:          "slow_queries",
			Name:        "Slow Query Rate",
			Severity:    SeverityWarning,
			Summary:     "High rate of slow queries",
			Description: "More than 10 slow queries per minute detected",
			Query:       "slow_queries_per_minute",
			Condition:   ">",
			Threshold:   10,
			Duration:    3 * time.Minute,
			Labels:      map[string]string{"category": "performance"},
			Enabled:     true,
		},
		{
			ID:          "deadlock_detected",
			Name:        "Deadlock Detected",
			Severity:    SeverityCritical,
			Summary:     "A deadlock has been detected",
			Description: "A transaction deadlock was detected in the database",
			Query:       "deadlock_count",
			Condition:   ">",
			Threshold:   0,
			Duration:    0,
			Labels:      map[string]string{"category": "error"},
			Enabled:     true,
		},
		{
			ID:          "connection_limit",
			Name:        "Connection Limit Approaching",
			Severity:    SeverityWarning,
			Summary:     "Connection count is above 80% of limit",
			Description: "Active connections are approaching the maximum limit",
			Query:       "connection_usage_percent",
			Condition:   ">",
			Threshold:   80,
			Duration:    2 * time.Minute,
			Labels:      map[string]string{"category": "system"},
			Enabled:     true,
		},
		{
			ID:          "replication_lag",
			Name:        "Replication Lag",
			Severity:    SeverityWarning,
			Summary:     "Replication lag is above 5 seconds",
			Description: "The read replica is lagging behind the primary by more than 5 seconds",
			Query:       "replication_lag_seconds",
			Condition:   ">",
			Threshold:   5,
			Duration:    5 * time.Minute,
			Labels:      map[string]string{"category": "replication"},
			Enabled:     true,
		},
		{
			ID:          "backup_failed",
			Name:        "Backup Failure",
			Severity:    SeverityCritical,
			Summary:     "A scheduled backup has failed",
			Description: "The most recent backup attempt did not complete successfully",
			Query:       "backup_success",
			Condition:   "==",
			Threshold:   0,
			Duration:    0,
			Labels:      map[string]string{"category": "backup"},
			Enabled:     true,
		},
	}

	for _, rule := range rules {
		am.AddRule(rule)
	}
}
