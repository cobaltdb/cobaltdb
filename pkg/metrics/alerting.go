package metrics

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// AlertSeverity represents the severity of an alert
type AlertSeverity int

const (
	SeverityInfo AlertSeverity = iota
	SeverityWarning
	SeverityCritical
)

func (s AlertSeverity) String() string {
	switch s {
	case SeverityInfo:
		return "INFO"
	case SeverityWarning:
		return "WARNING"
	case SeverityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// Alert represents a single alert
type Alert struct {
	ID        string
	RuleName  string
	Severity  AlertSeverity
	Message   string
	Timestamp time.Time
	Value     float64
	Threshold float64
}

// AlertRule defines a rule for generating alerts
type AlertRule struct {
	Name        string
	Description string
	Severity    AlertSeverity
	Condition   func() (bool, float64)
	Threshold   float64
	Cooldown    time.Duration
	lastFired   time.Time
	muted       bool
}

// AlertManager manages alerting rules and notifications
type AlertManager struct {
	rules         map[string]*AlertRule
	alerts        []Alert
	handlers      []AlertHandler
	mu            sync.RWMutex
	stopCh        chan struct{}
	checkInterval time.Duration
}

// AlertHandler handles alert notifications
type AlertHandler interface {
	Handle(alert Alert) error
}

// LogAlertHandler logs alerts to standard logger
type LogAlertHandler struct{}

func (h *LogAlertHandler) Handle(alert Alert) error {
	log.Printf("[ALERT] %s | %s | %s | Value: %.2f | Threshold: %.2f",
		alert.Severity, alert.RuleName, alert.Message, alert.Value, alert.Threshold)
	return nil
}

// CallbackAlertHandler calls a function when alert fires
type CallbackAlertHandler struct {
	Callback func(Alert)
}

func (h *CallbackAlertHandler) Handle(alert Alert) error {
	if h.Callback != nil {
		h.Callback(alert)
	}
	return nil
}

// NewAlertManager creates a new alert manager
func NewAlertManager() *AlertManager {
	return &AlertManager{
		rules:         make(map[string]*AlertRule),
		alerts:        make([]Alert, 0),
		handlers:      make([]AlertHandler, 0),
		stopCh:        make(chan struct{}),
		checkInterval: 10 * time.Second,
	}
}

// RegisterRule registers an alert rule
func (am *AlertManager) RegisterRule(rule *AlertRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.rules[rule.Name] = rule
}

// UnregisterRule removes an alert rule
func (am *AlertManager) UnregisterRule(name string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.rules, name)
}

// RegisterHandler registers an alert handler
func (am *AlertManager) RegisterHandler(handler AlertHandler) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.handlers = append(am.handlers, handler)
}

// Start starts the alert manager
func (am *AlertManager) Start() {
	go am.run()
}

// Stop stops the alert manager
func (am *AlertManager) Stop() {
	close(am.stopCh)
}

// run is the main alert checking loop
func (am *AlertManager) run() {
	ticker := time.NewTicker(am.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			am.checkRules()
		case <-am.stopCh:
			return
		}
	}
}

// checkRules evaluates all registered rules
func (am *AlertManager) checkRules() {
	am.mu.RLock()
	rules := make([]*AlertRule, 0, len(am.rules))
	for _, rule := range am.rules {
		rules = append(rules, rule)
	}
	handlers := make([]AlertHandler, len(am.handlers))
	copy(handlers, am.handlers)
	am.mu.RUnlock()

	for _, rule := range rules {
		if rule.muted {
			continue
		}

		// Check cooldown
		if time.Since(rule.lastFired) < rule.Cooldown {
			continue
		}

		// Evaluate condition
		if rule.Condition == nil {
			continue
		}

		fired, value := rule.Condition()
		if fired {
			rule.lastFired = time.Now()

			alert := Alert{
				ID:        generateAlertID(),
				RuleName:  rule.Name,
				Severity:  rule.Severity,
				Message:   rule.Description,
				Timestamp: time.Now(),
				Value:     value,
				Threshold: rule.Threshold,
			}

			// Store alert
			am.mu.Lock()
			am.alerts = append(am.alerts, alert)
			// Keep only last 1000 alerts
			if len(am.alerts) > 1000 {
				am.alerts = am.alerts[len(am.alerts)-1000:]
			}
			am.mu.Unlock()

			// Notify handlers
			for _, handler := range handlers {
				go func(h AlertHandler, a Alert) {
					if err := h.Handle(a); err != nil {
						log.Printf("[ALERT] Handler error: %v", err)
					}
				}(handler, alert)
			}
		}
	}
}

// GetAlerts returns recent alerts
func (am *AlertManager) GetAlerts(limit int) []Alert {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if limit <= 0 || limit > len(am.alerts) {
		limit = len(am.alerts)
	}

	// Return most recent alerts first
	start := len(am.alerts) - limit
	if start < 0 {
		start = 0
	}

	result := make([]Alert, limit)
	for i := 0; i < limit; i++ {
		result[i] = am.alerts[start+i]
	}
	return result
}

// MuteRule temporarily disables a rule
func (am *AlertManager) MuteRule(name string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if rule, ok := am.rules[name]; ok {
		rule.muted = true
	}
}

// UnmuteRule re-enables a muted rule
func (am *AlertManager) UnmuteRule(name string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	if rule, ok := am.rules[name]; ok {
		rule.muted = false
	}
}

// generateAlertID generates a unique alert ID
var alertIDCounter int64

func generateAlertID() string {
	alertIDCounter++
	return fmt.Sprintf("ALERT-%d-%d", time.Now().Unix(), alertIDCounter)
}

// DefaultAlertRules returns a set of default alert rules for CobaltDB
func DefaultAlertRules() []*AlertRule {
	return []*AlertRule{
		{
			Name:        "high_deadlock_rate",
			Description: "Deadlock rate is above threshold",
			Severity:    SeverityWarning,
			Threshold:   10,
			Cooldown:    5 * time.Minute,
			Condition: func() (bool, float64) {
				stats := GetTransactionMetrics().GetStats()
				// Alert if more than 10 deadlocks detected
				return stats.DeadlocksDetected > 10, float64(stats.DeadlocksDetected)
			},
		},
		{
			Name:        "high_transaction_timeout_rate",
			Description: "Transaction timeout rate is above threshold",
			Severity:    SeverityCritical,
			Threshold:   100,
			Cooldown:    5 * time.Minute,
			Condition: func() (bool, float64) {
				stats := GetTransactionMetrics().GetStats()
				return stats.TxnTimeouts > 100, float64(stats.TxnTimeouts)
			},
		},
		{
			Name:        "many_long_running_transactions",
			Description: "Too many long-running transactions detected",
			Severity:    SeverityWarning,
			Threshold:   10,
			Cooldown:    1 * time.Minute,
			Condition: func() (bool, float64) {
				stats := GetTransactionMetrics().GetStats()
				return stats.LongRunningTxns > 10, float64(stats.LongRunningTxns)
			},
		},
		{
			Name:        "high_memory_usage",
			Description: "Memory usage is above 85%",
			Severity:    SeverityWarning,
			Threshold:   85,
			Cooldown:    5 * time.Minute,
			Condition: func() (bool, float64) {
				// This is a placeholder - real implementation would check actual memory
				return false, 0
			},
		},
		{
			Name:        "high_error_rate",
			Description: "High rate of aborted transactions",
			Severity:    SeverityCritical,
			Threshold:   1000,
			Cooldown:    5 * time.Minute,
			Condition: func() (bool, float64) {
				stats := GetTransactionMetrics().GetStats()
				return stats.AbortedTxns > 1000, float64(stats.AbortedTxns)
			},
		},
	}
}

// Global alert manager instance
var globalAlertManager *AlertManager
var alertManagerOnce sync.Once

// GetAlertManager returns the global alert manager
func GetAlertManager() *AlertManager {
	alertManagerOnce.Do(func() {
		globalAlertManager = NewAlertManager()
		// Register default rules
		for _, rule := range DefaultAlertRules() {
			globalAlertManager.RegisterRule(rule)
		}
		// Register default log handler
		globalAlertManager.RegisterHandler(&LogAlertHandler{})
	})
	return globalAlertManager
}
