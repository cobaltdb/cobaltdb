package metrics

import (
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
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
	startOnce     sync.Once
	stopOnce      sync.Once
	wg            sync.WaitGroup
	checkInterval time.Duration
	logger        *logger.Logger
}

// AlertHandler handles alert notifications
type AlertHandler interface {
	Handle(alert Alert) error
}

type alertRuleSnapshot struct {
	Name        string
	Description string
	Severity    AlertSeverity
	Condition   func() (bool, float64)
	Threshold   float64
	Cooldown    time.Duration
	lastFired   time.Time
}

// LogAlertHandler logs alerts to a configured logger.
type LogAlertHandler struct {
	Logger *logger.Logger
}

// NewLogAlertHandler creates an alert handler backed by the given logger.
func NewLogAlertHandler(log *logger.Logger) *LogAlertHandler {
	return &LogAlertHandler{Logger: log}
}

func (h *LogAlertHandler) Handle(alert Alert) error {
	if h.Logger != nil {
		h.Logger.Warnf("[ALERT] %s | %s | %s | Value: %.2f | Threshold: %.2f",
			alert.Severity, alert.RuleName, alert.Message, alert.Value, alert.Threshold)
	}
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

// SetLogger sets the optional logger used for alert manager internal errors.
func (am *AlertManager) SetLogger(log *logger.Logger) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.logger = log
}

func (am *AlertManager) logErrorf(format string, args ...interface{}) {
	am.mu.RLock()
	log := am.logger
	am.mu.RUnlock()
	if log != nil {
		log.Errorf(format, args...)
	}
}

// RegisterRule registers an alert rule
func (am *AlertManager) RegisterRule(rule *AlertRule) {
	if rule == nil {
		return
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	am.rules[rule.Name] = cloneAlertRule(rule)
}

func cloneAlertRule(rule *AlertRule) *AlertRule {
	if rule == nil {
		return nil
	}
	cloned := *rule
	return &cloned
}

// UnregisterRule removes an alert rule
func (am *AlertManager) UnregisterRule(name string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.rules, name)
}

// RegisterHandler registers an alert handler
func (am *AlertManager) RegisterHandler(handler AlertHandler) {
	if handler == nil {
		return
	}
	am.mu.Lock()
	defer am.mu.Unlock()
	am.handlers = append(am.handlers, handler)
}

// Start starts the alert manager
func (am *AlertManager) Start() {
	am.startOnce.Do(func() {
		am.wg.Add(1)
		go func() {
			defer am.wg.Done()
			am.run()
		}()
	})
}

// Stop stops the alert manager
func (am *AlertManager) Stop() {
	am.stopOnce.Do(func() {
		close(am.stopCh)
	})
	am.wg.Wait()
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
	rules := make([]alertRuleSnapshot, 0, len(am.rules))
	for _, rule := range am.rules {
		if rule == nil || rule.muted {
			continue
		}
		rules = append(rules, alertRuleSnapshot{
			Name:        rule.Name,
			Description: rule.Description,
			Severity:    rule.Severity,
			Condition:   rule.Condition,
			Threshold:   rule.Threshold,
			Cooldown:    rule.Cooldown,
			lastFired:   rule.lastFired,
		})
	}
	handlers := make([]AlertHandler, len(am.handlers))
	copy(handlers, am.handlers)
	am.mu.RUnlock()

	for _, rule := range rules {
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
			now := time.Now()

			am.mu.Lock()
			currentRule, ok := am.rules[rule.Name]
			if !ok || currentRule == nil || currentRule.muted || time.Since(currentRule.lastFired) < currentRule.Cooldown {
				am.mu.Unlock()
				continue
			}
			currentRule.lastFired = now
			am.mu.Unlock()

			alert := Alert{
				ID:        generateAlertID(),
				RuleName:  rule.Name,
				Severity:  rule.Severity,
				Message:   rule.Description,
				Timestamp: now,
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
				if handler == nil {
					continue
				}
				go func(h AlertHandler, a Alert) {
					defer func() {
						if r := recover(); r != nil {
							am.logErrorf("[ALERT] Handler panic: %v", r)
						}
					}()
					if err := h.Handle(a); err != nil {
						am.logErrorf("[ALERT] Handler error: %v", err)
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
	id := atomic.AddInt64(&alertIDCounter, 1)
	return fmt.Sprintf("ALERT-%d-%d", time.Now().Unix(), id)
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
				limit := debug.SetMemoryLimit(-1)
				if limit <= 0 || limit == math.MaxInt64 {
					return false, 0
				}
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				usage := (float64(mem.Sys) / float64(limit)) * 100
				return usage > 85, usage
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
		globalAlertManager.RegisterHandler(NewLogAlertHandler(nil))
	})
	return globalAlertManager
}
