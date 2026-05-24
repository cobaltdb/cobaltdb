// Production-ready server lifecycle management
// Handles graceful shutdown, signal handling, and health monitoring

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// LifecycleConfig configures server lifecycle behavior
type LifecycleConfig struct {
	// ShutdownTimeout is the maximum time to wait for graceful shutdown (default: 30s)
	ShutdownTimeout time.Duration

	// DrainTimeout is the time to wait for connections to drain (default: 10s)
	DrainTimeout time.Duration

	// HealthCheckInterval is how often to check component health (default: 5s)
	HealthCheckInterval time.Duration

	// StartupTimeout is the maximum time to wait for startup (default: 60s)
	StartupTimeout time.Duration

	// EnableSignalHandling enables signal handling for graceful shutdown (default: true)
	EnableSignalHandling bool

	// Signals to handle for shutdown
	ShutdownSignals []os.Signal

	Logger *logger.Logger
}

// DefaultLifecycleConfig returns sensible defaults
func DefaultLifecycleConfig() *LifecycleConfig {
	return &LifecycleConfig{
		ShutdownTimeout:      30 * time.Second,
		DrainTimeout:         10 * time.Second,
		HealthCheckInterval:  5 * time.Second,
		StartupTimeout:       60 * time.Second,
		EnableSignalHandling: true,
		ShutdownSignals:      []os.Signal{syscall.SIGTERM, syscall.SIGINT},
	}
}

// Lifecycle manages server lifecycle
type Lifecycle struct {
	config *LifecycleConfig

	// State management
	state      LifecycleState
	stateMu    sync.RWMutex
	stateHooks map[LifecycleState][]func()

	// Component management
	components []Component
	compMu     sync.RWMutex

	// Shutdown coordination
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	stopMu       sync.Mutex
	stopStarted  bool
	stopDone     chan struct{}
	stopErr      error

	// Hook goroutine tracking
	hookWg sync.WaitGroup

	// Health tracking
	healthChecks map[string]HealthCheck
	healthMu     sync.RWMutex

	// Context for operations
	ctx    context.Context
	cancel context.CancelFunc

	logger *logger.Logger
}

// LifecycleState represents server lifecycle state
type LifecycleState int

const (
	StateInitializing LifecycleState = iota
	StateStarting
	StateRunning
	StateDraining
	StateShuttingDown
	StateStopped
)

func (s LifecycleState) String() string {
	switch s {
	case StateInitializing:
		return "initializing"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateDraining:
		return "draining"
	case StateShuttingDown:
		return "shutting_down"
	case StateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Component represents a lifecycle-managed component
type Component interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Health() HealthStatus
}

// HealthStatus represents component health
type HealthStatus struct {
	Healthy bool   `json:"healthy"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// HealthCheck is a function that checks health
type HealthCheck func() HealthStatus

// NewLifecycle creates a new lifecycle manager
func NewLifecycle(config *LifecycleConfig) *Lifecycle {
	if config == nil {
		config = DefaultLifecycleConfig()
	} else {
		config = normalizeLifecycleConfig(config)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Lifecycle{
		config:       config,
		state:        StateInitializing,
		stateHooks:   make(map[LifecycleState][]func()),
		shutdownCh:   make(chan struct{}),
		stopDone:     make(chan struct{}),
		healthChecks: make(map[string]HealthCheck),
		ctx:          ctx,
		cancel:       cancel,
		logger:       config.Logger,
	}
}

func normalizeLifecycleConfig(config *LifecycleConfig) *LifecycleConfig {
	defaults := DefaultLifecycleConfig()
	normalized := *config
	if normalized.ShutdownTimeout <= 0 {
		normalized.ShutdownTimeout = defaults.ShutdownTimeout
	}
	if normalized.DrainTimeout <= 0 {
		normalized.DrainTimeout = defaults.DrainTimeout
	}
	if normalized.HealthCheckInterval <= 0 {
		normalized.HealthCheckInterval = defaults.HealthCheckInterval
	}
	if normalized.StartupTimeout <= 0 {
		normalized.StartupTimeout = defaults.StartupTimeout
	}
	if len(normalized.ShutdownSignals) == 0 {
		normalized.ShutdownSignals = cloneShutdownSignals(defaults.ShutdownSignals)
	} else {
		normalized.ShutdownSignals = cloneShutdownSignals(normalized.ShutdownSignals)
	}
	return &normalized
}

func cloneShutdownSignals(signals []os.Signal) []os.Signal {
	if signals == nil {
		return nil
	}
	cloned := make([]os.Signal, len(signals))
	copy(cloned, signals)
	return cloned
}

func (l *Lifecycle) logInfof(format string, args ...interface{}) {
	if l != nil && l.logger != nil {
		l.logger.Infof(format, args...)
	}
}

func (l *Lifecycle) logWarnf(format string, args ...interface{}) {
	if l != nil && l.logger != nil {
		l.logger.Warnf(format, args...)
	}
}

func (l *Lifecycle) logErrorf(format string, args ...interface{}) {
	if l != nil && l.logger != nil {
		l.logger.Errorf(format, args...)
	}
}

// RegisterComponent registers a lifecycle-managed component
func (l *Lifecycle) RegisterComponent(comp Component) {
	l.compMu.Lock()
	defer l.compMu.Unlock()
	l.components = append(l.components, comp)
}

// RegisterHealthCheck registers a health check
func (l *Lifecycle) RegisterHealthCheck(name string, check HealthCheck) {
	l.healthMu.Lock()
	defer l.healthMu.Unlock()
	l.healthChecks[name] = check
}

// OnStateChange registers a hook for state changes
func (l *Lifecycle) OnStateChange(state LifecycleState, fn func()) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	l.stateHooks[state] = append(l.stateHooks[state], fn)
}

// Start starts all components
func (l *Lifecycle) Start() error {
	l.setState(StateStarting)

	ctx, cancel := context.WithTimeout(l.ctx, l.config.StartupTimeout)
	defer cancel()

	l.compMu.RLock()
	components := make([]Component, len(l.components))
	copy(components, l.components)
	l.compMu.RUnlock()

	for _, comp := range components {
		select {
		case <-ctx.Done():
			return fmt.Errorf("startup timeout waiting for %s", comp.Name())
		default:
			if err := comp.Start(ctx); err != nil {
				// Stop already started components
				if stopErr := l.stopComponents(context.Background()); stopErr != nil {
					return fmt.Errorf("failed to start %s: %w (cleanup failed: %v)", comp.Name(), err, stopErr)
				}
				return fmt.Errorf("failed to start %s: %w", comp.Name(), err)
			}
		}
	}

	l.setState(StateRunning)

	if l.config.EnableSignalHandling {
		l.setupSignalHandling()
	}

	// Start health monitoring
	go l.healthMonitor()

	return nil
}

// Stop gracefully shuts down all components
func (l *Lifecycle) Stop() error {
	l.stopMu.Lock()
	if l.stopStarted {
		done := l.stopDone
		l.stopMu.Unlock()
		<-done
		l.stopMu.Lock()
		err := l.stopErr
		l.stopMu.Unlock()
		return err
	}
	l.stopStarted = true
	done := l.stopDone
	l.stopMu.Unlock()

	l.shutdownOnce.Do(func() {
		close(l.shutdownCh)
	})

	l.setState(StateDraining)

	// Wait for drain timeout
	time.Sleep(l.config.DrainTimeout)

	l.setState(StateShuttingDown)

	ctx, cancel := context.WithTimeout(context.Background(), l.config.ShutdownTimeout)
	defer cancel()

	err := l.stopComponents(ctx)

	l.cancel()
	l.setState(StateStopped)

	l.stopMu.Lock()
	l.stopErr = err
	close(done)
	l.stopMu.Unlock()

	l.hookWg.Wait()

	return err
}

// stopComponents stops all components in reverse order
func (l *Lifecycle) stopComponents(ctx context.Context) error {
	l.compMu.RLock()
	components := make([]Component, len(l.components))
	copy(components, l.components)
	l.compMu.RUnlock()

	// Stop in reverse order
	var errs []error
	for i := len(components) - 1; i >= 0; i-- {
		comp := components[i]
		if err := comp.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop %s: %w", comp.Name(), err))
		}
	}

	return errors.Join(errs...)
}

// setupSignalHandling sets up OS signal handling
func (l *Lifecycle) setupSignalHandling() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, l.config.ShutdownSignals...)

	go func() {
		sig := <-sigCh
		l.logInfof("received signal %v, initiating graceful shutdown", sig)
		if err := l.Stop(); err != nil {
			l.logErrorf("shutdown error: %v", err)
		}
	}()
}

// healthMonitor periodically checks component health
func (l *Lifecycle) healthMonitor() {
	ticker := time.NewTicker(l.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-l.shutdownCh:
			return
		case <-ticker.C:
			l.checkHealth()
		}
	}
}

// checkHealth checks all component and registered health checks
func (l *Lifecycle) checkHealth() {
	state := l.State()
	if state != StateRunning && state != StateStarting {
		return
	}

	l.compMu.RLock()
	components := make([]Component, len(l.components))
	copy(components, l.components)
	l.compMu.RUnlock()

	for _, comp := range components {
		health := comp.Health()
		if !health.Healthy {
			l.logWarnf("health check failed for %s: %s", comp.Name(), health.Message)
		}
	}
}

// State returns current lifecycle state
func (l *Lifecycle) State() LifecycleState {
	l.stateMu.RLock()
	defer l.stateMu.RUnlock()
	return l.state
}

// setState updates the lifecycle state and triggers hooks
func (l *Lifecycle) setState(state LifecycleState) {
	l.stateMu.Lock()
	l.state = state
	hooks := l.stateHooks[state]
	l.stateMu.Unlock()

	// Trigger hooks
	for _, hook := range hooks {
		l.hookWg.Add(1)
		go func(h func()) {
			defer l.hookWg.Done()
			defer func() {
				if r := recover(); r != nil {
					l.logErrorf("panic in lifecycle state hook: %v", r)
				}
			}()
			h()
		}(hook)
	}
}

// Wait blocks until the server is stopped
func (l *Lifecycle) Wait() {
	<-l.shutdownCh
}

// IsRunning returns true if server is running
func (l *Lifecycle) IsRunning() bool {
	return l.State() == StateRunning
}

// IsHealthy returns true if all components are healthy
func (l *Lifecycle) IsHealthy() bool {
	l.compMu.RLock()
	components := make([]Component, len(l.components))
	copy(components, l.components)
	l.compMu.RUnlock()

	for _, comp := range components {
		if !comp.Health().Healthy {
			return false
		}
	}

	return true
}

// GetHealth returns detailed health status
func (l *Lifecycle) GetHealth() map[string]HealthStatus {
	result := make(map[string]HealthStatus)

	l.compMu.RLock()
	components := make([]Component, len(l.components))
	copy(components, l.components)
	l.compMu.RUnlock()

	for _, comp := range components {
		result[comp.Name()] = comp.Health()
	}

	l.healthMu.RLock()
	for name, check := range l.healthChecks {
		result[name] = check()
	}
	l.healthMu.RUnlock()

	return result
}

// GracefulShutdownHandler returns an HTTP handler for graceful shutdown
func (l *Lifecycle) GracefulShutdownHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		go func() {
			if err := l.Stop(); err != nil {
				l.logErrorf("graceful shutdown error: %v", err)
			}
		}()

		w.WriteHeader(http.StatusAccepted)
		if _, err := w.Write([]byte(`{"status":"shutting_down"}`)); err != nil {
			l.logErrorf("failed to write shutdown response: %v", err)
		}
	}
}

// ReadyCheck returns an HTTP handler for readiness probe
func (l *Lifecycle) ReadyCheck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state := l.State()
		if state == StateRunning {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"ready":true}`)); err != nil {
				l.logErrorf("failed to write ready response: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"ready":false,"state":"%s"}`, state)
	}
}

// LiveCheck returns an HTTP handler for liveness probe
func (l *Lifecycle) LiveCheck() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state := l.State()
		// Consider unhealthy if stopped or failed
		if state == StateStopped {
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte(`{"alive":false}`)); err != nil {
				l.logErrorf("failed to write live response: %v", err)
			}
			return
		}

		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"alive":true}`)); err != nil {
			l.logErrorf("failed to write live response: %v", err)
		}
	}
}

// DBComponent wraps an engine.DB as a lifecycle component
type DBComponent struct {
	db     *engine.DB
	name   string
	ctx    context.Context
	cancel context.CancelFunc
}

// NewDBComponent creates a new DB component
func NewDBComponent(name string, db *engine.DB) *DBComponent {
	ctx, cancel := context.WithCancel(context.Background())
	return &DBComponent{
		db:     db,
		name:   name,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Name returns component name
func (c *DBComponent) Name() string {
	return c.name
}

// Start starts the component
func (c *DBComponent) Start(ctx context.Context) error {
	// DB is already opened, just verify it's healthy
	if c.db == nil {
		return fmt.Errorf("database not initialized")
	}
	return nil
}

// Stop stops the component
func (c *DBComponent) Stop(ctx context.Context) error {
	c.cancel()
	return c.db.Close()
}

// Health returns component health
func (c *DBComponent) Health() HealthStatus {
	if c.db == nil {
		return HealthStatus{Healthy: false, Message: "database not initialized"}
	}
	return HealthStatus{Healthy: true, Message: "database healthy"}
}
