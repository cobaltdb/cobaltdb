// Alerting and Notification System
// Monitors system health and sends alerts when thresholds are exceeded

package server

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// AlertSeverity represents alert severity levels
type AlertSeverity int

const (
	SeverityInfo AlertSeverity = iota
	SeverityWarning
	SeverityError
	SeverityCritical
)

func (s AlertSeverity) String() string {
	switch s {
	case SeverityInfo:
		return "info"
	case SeverityWarning:
		return "warning"
	case SeverityError:
		return "error"
	case SeverityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// Alert represents an alert event
type Alert struct {
	ID          string
	Severity    AlertSeverity
	Title       string
	Message     string
	Timestamp   time.Time
	Source      string
	Metadata    map[string]string
	Acknowledged bool
}

// AlertRule defines when to trigger an alert
type AlertRule struct {
	ID          string
	Name        string
	Description string
	Severity    AlertSeverity
	Condition   func(*MetricsSnapshot) bool
	Cooldown    time.Duration
	lastFired   time.Time
	mu          sync.Mutex
}

// CanFire checks if the rule can fire (respects cooldown)
func (r *AlertRule) CanFire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if time.Since(r.lastFired) < r.Cooldown {
		return false
	}

	r.lastFired = time.Now()
	return true
}

// AlertHandler handles alert notifications
type AlertHandler interface {
	Handle(alert *Alert) error
}

// AlertManager manages alerts and notifications
type AlertManager struct {
	rules    []*AlertRule
	handlers []AlertHandler
	rulesMu  sync.RWMutex // protects rules and handlers slices

	alertHistory []*Alert
	historyMu    sync.RWMutex
	maxHistory   int

	activeAlerts map[string]*Alert
	activeMu     sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

// NewAlertManager creates a new alert manager
func NewAlertManager() *AlertManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &AlertManager{
		rules:        make([]*AlertRule, 0),
		handlers:     make([]AlertHandler, 0),
		alertHistory: make([]*Alert, 0),
		maxHistory:   1000,
		activeAlerts: make(map[string]*Alert),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// RegisterRule registers an alert rule
func (am *AlertManager) RegisterRule(rule *AlertRule) {
	am.rulesMu.Lock()
	defer am.rulesMu.Unlock()
	am.rules = append(am.rules, rule)
}

// RegisterHandler registers an alert handler
func (am *AlertManager) RegisterHandler(handler AlertHandler) {
	am.rulesMu.Lock()
	defer am.rulesMu.Unlock()
	am.handlers = append(am.handlers, handler)
}

// Start starts the alert manager
func (am *AlertManager) Start() {
	// Start cleanup goroutine
	go am.cleanupLoop()
}

// Stop stops the alert manager
func (am *AlertManager) Stop() {
	am.cancel()
}

// Evaluate evaluates all rules against current metrics
func (am *AlertManager) Evaluate(metrics *MetricsSnapshot) {
	am.rulesMu.RLock()
	rules := make([]*AlertRule, len(am.rules))
	copy(rules, am.rules)
	am.rulesMu.RUnlock()

	for _, rule := range rules {
		if rule.Condition(metrics) && rule.CanFire() {
			alert := &Alert{
				ID:        generateAlertID(),
				Severity:  rule.Severity,
				Title:     rule.Name,
				Message:   rule.Description,
				Timestamp: time.Now(),
				Source:    "alert_manager",
				Metadata:  make(map[string]string),
			}

			am.FireAlert(alert)
		}
	}
}

// FireAlert fires an alert to all handlers
func (am *AlertManager) FireAlert(alert *Alert) {
	// Add to active alerts
	am.activeMu.Lock()
	am.activeAlerts[alert.ID] = alert
	am.activeMu.Unlock()

	// Add to history
	am.historyMu.Lock()
	am.alertHistory = append(am.alertHistory, alert)
	if len(am.alertHistory) > am.maxHistory {
		am.alertHistory = am.alertHistory[len(am.alertHistory)-am.maxHistory:]
	}
	am.historyMu.Unlock()

	// Send to handlers
	am.rulesMu.RLock()
	handlers := make([]AlertHandler, len(am.handlers))
	copy(handlers, am.handlers)
	am.rulesMu.RUnlock()

	for _, handler := range handlers {
		go func(h AlertHandler) {
			if err := h.Handle(alert); err != nil {
				fmt.Printf("Alert handler error: %v\n", err)
			}
		}(handler)
	}
}

// AcknowledgeAlert acknowledges an alert
func (am *AlertManager) AcknowledgeAlert(alertID string) bool {
	am.activeMu.Lock()
	defer am.activeMu.Unlock()

	if alert, ok := am.activeAlerts[alertID]; ok {
		alert.Acknowledged = true
		return true
	}
	return false
}

// ResolveAlert resolves an active alert
func (am *AlertManager) ResolveAlert(alertID string) bool {
	am.activeMu.Lock()
	defer am.activeMu.Unlock()

	if _, ok := am.activeAlerts[alertID]; ok {
		delete(am.activeAlerts, alertID)
		return true
	}
	return false
}

// GetActiveAlerts returns all active alerts
func (am *AlertManager) GetActiveAlerts() []*Alert {
	am.activeMu.RLock()
	defer am.activeMu.RUnlock()

	alerts := make([]*Alert, 0, len(am.activeAlerts))
	for _, alert := range am.activeAlerts {
		alerts = append(alerts, alert)
	}
	return alerts
}

// GetAlertHistory returns alert history
func (am *AlertManager) GetAlertHistory(limit int) []*Alert {
	am.historyMu.RLock()
	defer am.historyMu.RUnlock()

	if limit <= 0 || limit > len(am.alertHistory) {
		limit = len(am.alertHistory)
	}

	// Return most recent first
	start := len(am.alertHistory) - limit
	if start < 0 {
		start = 0
	}

	result := make([]*Alert, limit)
	copy(result, am.alertHistory[start:])

	// Reverse
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// cleanupLoop periodically cleans up old alerts
func (am *AlertManager) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-am.ctx.Done():
			return
		case <-ticker.C:
			am.cleanup()
		}
	}
}

// cleanup removes resolved alerts from active list
func (am *AlertManager) cleanup() {
	am.activeMu.Lock()
	defer am.activeMu.Unlock()

	// Remove alerts older than 24 hours
	cutoff := time.Now().Add(-24 * time.Hour)
	for id, alert := range am.activeAlerts {
		if alert.Timestamp.Before(cutoff) {
			delete(am.activeAlerts, id)
		}
	}
}

// MetricsSnapshot holds system metrics for alert evaluation
type MetricsSnapshot struct {
	Timestamp       time.Time
	CPUUsage        float64
	MemoryUsage     float64
	DiskUsage       float64
	ActiveConnections int
	QueryLatency    time.Duration
	ErrorRate       float64
	QPS             float64
}

// Common alert rules
func DefaultAlertRules() []*AlertRule {
	return []*AlertRule{
		{
			ID:          "high_cpu",
			Name:        "High CPU Usage",
			Description: "CPU usage is above 80%",
			Severity:    SeverityWarning,
			Condition:   func(m *MetricsSnapshot) bool { return m.CPUUsage > 80 },
			Cooldown:    5 * time.Minute,
		},
		{
			ID:          "high_memory",
			Name:        "High Memory Usage",
			Description: "Memory usage is above 85%",
			Severity:    SeverityWarning,
			Condition:   func(m *MetricsSnapshot) bool { return m.MemoryUsage > 85 },
			Cooldown:    5 * time.Minute,
		},
		{
			ID:          "high_disk",
			Name:        "High Disk Usage",
			Description: "Disk usage is above 90%",
			Severity:    SeverityError,
			Condition:   func(m *MetricsSnapshot) bool { return m.DiskUsage > 90 },
			Cooldown:    10 * time.Minute,
		},
		{
			ID:          "high_error_rate",
			Name:        "High Error Rate",
			Description: "Error rate is above 5%",
			Severity:    SeverityError,
			Condition:   func(m *MetricsSnapshot) bool { return m.ErrorRate > 0.05 },
			Cooldown:    2 * time.Minute,
		},
		{
			ID:          "high_latency",
			Name:        "High Query Latency",
			Description: "Average query latency is above 1 second",
			Severity:    SeverityWarning,
			Condition:   func(m *MetricsSnapshot) bool { return m.QueryLatency > time.Second },
			Cooldown:    3 * time.Minute,
		},
		{
			ID:          "too_many_connections",
			Name:        "Too Many Connections",
			Description: "Active connections exceed 1000",
			Severity:    SeverityWarning,
			Condition:   func(m *MetricsSnapshot) bool { return m.ActiveConnections > 1000 },
			Cooldown:    1 * time.Minute,
		},
	}
}

// LogHandler logs alerts to stdout
type LogHandler struct{}

func (h *LogHandler) Handle(alert *Alert) error {
	fmt.Printf("[ALERT] %s [%s] %s: %s\n",
		alert.Timestamp.Format(time.RFC3339),
		alert.Severity.String(),
		alert.Title,
		alert.Message)
	return nil
}

// WebhookHandler sends alerts to a webhook
type WebhookHandler struct {
	URL     string
	Headers map[string]string
}

func (h *WebhookHandler) Handle(alert *Alert) error {
	// Implementation would POST to webhook URL
	fmt.Printf("[WEBHOOK] Would send alert to %s: %s\n", h.URL, alert.Title)
	return nil
}

func generateAlertID() string {
	return fmt.Sprintf("alert-%d", time.Now().UnixNano())
}
