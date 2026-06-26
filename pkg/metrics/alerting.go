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

const maxConcurrentAlertHandlers = 64

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
	Name          string
	Description   string
	Severity      AlertSeverity
	Condition     func() (bool, float64)
	Threshold     float64
	Cooldown      time.Duration
	lastFired     time.Time
	lastRecovered time.Time // when the condition last returned false (0 = never recovered)
	wasFiring     bool      // whether the condition was firing at the end of the last checkRules call
	muted         bool
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
	handlerSem    chan struct{}
}

// AlertHandler handles alert notifications
type AlertHandler interface {
	Handle(alert Alert) error
}

type alertRuleSnapshot struct {
	Name          string
	Description   string
	Severity      AlertSeverity
	Condition     func() (bool, float64)
	Threshold     float64
	Cooldown      time.Duration
	lastFired     time.Time
	lastRecovered time.Time
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
		handlerSem:    make(chan struct{}, maxConcurrentAlertHandlers),
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
			Name:          rule.Name,
			Description:   rule.Description,
			Severity:      rule.Severity,
			Condition:     rule.Condition,
			Threshold:     rule.Threshold,
			Cooldown:      rule.Cooldown,
			lastFired:     rule.lastFired,
			lastRecovered: rule.lastRecovered,
		})
	}
	handlers := make([]AlertHandler, len(am.handlers))
	copy(handlers, am.handlers)
	am.mu.RUnlock()

	// Updated wasFiring per rule; applied to live map after the loop.
	updatedWasFiring := make(map[string]bool)

	for _, rule := range rules {
		if rule.Condition == nil {
			continue
		}

		fired, value := rule.Condition()

		if !fired {
			// Condition recovered: write lastRecovered and wasFiring to the live map
			// directly (not via updatedWasFiring) so the value is correct for any
			// subsequent checkRules() call that starts before the deferred update.
			am.mu.Lock()
			if r, ok := am.rules[rule.Name]; ok && r != nil {
				r.lastRecovered = time.Now()
				r.wasFiring = false
			}
			am.mu.Unlock()
			updatedWasFiring[rule.Name] = false
			continue
		}

		// Condition is firing. Check cooldown using the LIVE MAP (not the snapshot)
		// to avoid goroutine race: the notification goroutine from a previous
		// checkRules call may not have updated wasFiring before this call starts.
		now := time.Now()
		am.mu.Lock()
		currentRule, ok := am.rules[rule.Name]
		if !ok || currentRule == nil || currentRule.muted {
			am.mu.Unlock()
			continue
		}

		// Cooldown: only suppress if this is a transition OK→ALERT AND
		// not enough time has passed since the last recovery (lastRecovered).
		// Sustained firing (wasFiring=true) never suppresses.
		if !currentRule.wasFiring && time.Since(currentRule.lastRecovered) < currentRule.Cooldown {
			// Suppressed: transition OK→ALERT within cooldown window.
			currentRule.wasFiring = true
			updatedWasFiring[rule.Name] = true
			am.mu.Unlock()
			continue
		}

		// OK→ALERT transition: start the cooldown clock. Continuous ALERT→ALERT:
		// do NOT update lastFired (keeps the original transition time).
		if !currentRule.wasFiring {
			currentRule.lastFired = now
		}
		currentRule.wasFiring = true
		updatedWasFiring[rule.Name] = true
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
		if len(am.alerts) > 1000 {
			am.alerts = am.alerts[len(am.alerts)-1000:]
		}
		am.mu.Unlock()

		am.notifyHandlers(alert, handlers)
	}

	// Apply updated wasFiring to live map after all rules have been evaluated.
	am.mu.Lock()
	for name, wf := range updatedWasFiring {
		if r, ok := am.rules[name]; ok && r != nil {
			r.wasFiring = wf
		}
	}
	am.mu.Unlock()
}

func (am *AlertManager) notifyHandlers(alert Alert, handlers []AlertHandler) {
	for _, handler := range handlers {
		if handler == nil {
			continue
		}
		if !am.tryAcquireHandlerSlot() {
			am.logErrorf("[ALERT] Dropping handler notification for %s: handler concurrency limit reached", alert.RuleName)
			continue
		}
		go func(h AlertHandler, a Alert) {
			defer am.releaseHandlerSlot()
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

func (am *AlertManager) tryAcquireHandlerSlot() bool {
	if am == nil || am.handlerSem == nil {
		return true
	}
	select {
	case am.handlerSem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (am *AlertManager) releaseHandlerSlot() {
	if am == nil || am.handlerSem == nil {
		return
	}
	<-am.handlerSem
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
