package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// ReloadableConfig defines configuration that can be hot-reloaded
type ReloadableConfig struct {
	// Logging
	LogLevel  string `json:"log_level"`
	LogFormat string `json:"log_format"`

	// Query settings
	QueryTimeout      string `json:"query_timeout"`
	MaxConnections    int    `json:"max_connections"`
	ConnectionTimeout string `json:"connection_timeout"`

	// Metrics
	MetricsEnabled  bool   `json:"metrics_enabled"`
	MetricsInterval string `json:"metrics_interval"`

	// Security
	RequireAuth     bool   `json:"require_auth"`
	MaxFailedLogins int    `json:"max_failed_logins"`
	LockoutDuration string `json:"lockout_duration"`

	// Performance
	CacheSize          int    `json:"cache_size"`
	QueryCacheEnabled  bool   `json:"query_cache_enabled"`
	QueryCacheSize     int    `json:"query_cache_size"`
	SlowQueryThreshold string `json:"slow_query_threshold"`
}

// DefaultReloadableConfig returns default configuration
func DefaultReloadableConfig() *ReloadableConfig {
	return &ReloadableConfig{
		LogLevel:           "info",
		LogFormat:          "json",
		QueryTimeout:       "60s",
		MaxConnections:     100,
		ConnectionTimeout:  "30s",
		MetricsEnabled:     true,
		MetricsInterval:    "10s",
		RequireAuth:        true,
		MaxFailedLogins:    5,
		LockoutDuration:    "15m",
		CacheSize:          1024,
		QueryCacheEnabled:  true,
		QueryCacheSize:     1000,
		SlowQueryThreshold: "1s",
	}
}

// ConfigReloader handles hot configuration reloading
type ConfigReloader struct {
	mu            sync.RWMutex
	configPath    string
	currentConfig *ReloadableConfig

	// Callbacks for config changes
	onLogLevelChange     func(string)
	onQueryTimeoutChange func(time.Duration)
	onMetricsChange      func(bool)
	onCacheSizeChange    func(int)

	// File watching
	lastModTime  time.Time
	watchEnabled atomic.Bool
	stopCh       chan struct{}
	stopOnce     sync.Once
	wg           sync.WaitGroup

	// Reload statistics
	reloadCount  atomic.Uint64
	reloadErrors atomic.Uint64
	lastReload   atomic.Value // time.Time
}

// NewConfigReloader creates a new config reloader
func NewConfigReloader(configPath string) (*ConfigReloader, error) {
	cr := &ConfigReloader{
		configPath:    configPath,
		currentConfig: DefaultReloadableConfig(),
		stopCh:        make(chan struct{}),
	}

	// Load initial config
	if err := cr.Load(); err != nil {
		return nil, fmt.Errorf("failed to load initial config: %w", err)
	}

	return cr, nil
}

// SetCallbacks sets the callbacks for configuration changes
func (cr *ConfigReloader) SetCallbacks(
	onLogLevel func(string),
	onQueryTimeout func(time.Duration),
	onMetrics func(bool),
	onCacheSize func(int),
) {
	cr.onLogLevelChange = onLogLevel
	cr.onQueryTimeoutChange = onQueryTimeout
	cr.onMetricsChange = onMetrics
	cr.onCacheSizeChange = onCacheSize
}

// Load loads configuration from file
func (cr *ConfigReloader) Load() error {
	data, err := os.ReadFile(cr.configPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Use default config
			return nil
		}
		return err
	}

	newConfig := &ReloadableConfig{}
	if err := json.Unmarshal(data, newConfig); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Validate new configuration
	if err := cr.validate(newConfig); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	// Apply changes
	if err := cr.applyChanges(newConfig); err != nil {
		return fmt.Errorf("failed to apply config changes: %w", err)
	}

	cr.mu.Lock()
	cr.currentConfig = newConfig
	cr.mu.Unlock()

	// Update file modification time
	stat, _ := os.Stat(cr.configPath)
	if stat != nil {
		cr.lastModTime = stat.ModTime()
	}

	cr.lastReload.Store(time.Now())
	cr.reloadCount.Add(1)

	return nil
}

// Reload triggers a configuration reload
func (cr *ConfigReloader) Reload() error {
	return cr.Load()
}

// ReloadFromJSON reloads configuration from JSON data
func (cr *ConfigReloader) ReloadFromJSON(data []byte) error {
	newConfig := &ReloadableConfig{}
	if err := json.Unmarshal(data, newConfig); err != nil {
		cr.reloadErrors.Add(1)
		return fmt.Errorf("failed to parse config JSON: %w", err)
	}

	if err := cr.validate(newConfig); err != nil {
		cr.reloadErrors.Add(1)
		return fmt.Errorf("config validation failed: %w", err)
	}

	if err := cr.applyChanges(newConfig); err != nil {
		cr.reloadErrors.Add(1)
		return fmt.Errorf("failed to apply config changes: %w", err)
	}

	cr.mu.Lock()
	cr.currentConfig = newConfig
	cr.mu.Unlock()

	cr.lastReload.Store(time.Now())
	cr.reloadCount.Add(1)

	return nil
}

// validate validates the configuration
func (cr *ConfigReloader) validate(cfg *ReloadableConfig) error {
	// Validate log level
	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[cfg.LogLevel] {
		return fmt.Errorf("invalid log level: %s", cfg.LogLevel)
	}

	// Validate timeouts (only if set)
	if cfg.QueryTimeout != "" {
		if _, err := time.ParseDuration(cfg.QueryTimeout); err != nil {
			return fmt.Errorf("invalid query timeout: %w", err)
		}
	}

	if cfg.ConnectionTimeout != "" {
		if _, err := time.ParseDuration(cfg.ConnectionTimeout); err != nil {
			return fmt.Errorf("invalid connection timeout: %w", err)
		}
	}

	// Validate metrics interval (only if set)
	if cfg.MetricsInterval != "" {
		if _, err := time.ParseDuration(cfg.MetricsInterval); err != nil {
			return fmt.Errorf("invalid metrics interval: %w", err)
		}
	}

	// Validate cache sizes
	if cfg.CacheSize < 0 {
		return fmt.Errorf("cache size cannot be negative")
	}

	if cfg.QueryCacheSize < 0 {
		return fmt.Errorf("query cache size cannot be negative")
	}

	// Validate max connections (only if set)
	if cfg.MaxConnections < 0 {
		return fmt.Errorf("max connections cannot be negative")
	}

	return nil
}

// applyChanges applies configuration changes
func (cr *ConfigReloader) applyChanges(newConfig *ReloadableConfig) error {
	cr.mu.RLock()
	oldConfig := cr.currentConfig
	cr.mu.RUnlock()

	// Apply log level change
	if oldConfig.LogLevel != newConfig.LogLevel && cr.onLogLevelChange != nil {
		cr.onLogLevelChange(newConfig.LogLevel)
	}

	// Apply query timeout change
	if oldConfig.QueryTimeout != newConfig.QueryTimeout && cr.onQueryTimeoutChange != nil {
		duration, _ := time.ParseDuration(newConfig.QueryTimeout)
		cr.onQueryTimeoutChange(duration)
	}

	// Apply metrics change
	if oldConfig.MetricsEnabled != newConfig.MetricsEnabled && cr.onMetricsChange != nil {
		cr.onMetricsChange(newConfig.MetricsEnabled)
	}

	// Apply cache size change
	if oldConfig.CacheSize != newConfig.CacheSize && cr.onCacheSizeChange != nil {
		cr.onCacheSizeChange(newConfig.CacheSize)
	}

	return nil
}

// GetConfig returns the current configuration
func (cr *ConfigReloader) GetConfig() *ReloadableConfig {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	// Return a copy
	copy := *cr.currentConfig
	return &copy
}

// GetConfigJSON returns the current configuration as JSON
func (cr *ConfigReloader) GetConfigJSON() ([]byte, error) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	return json.MarshalIndent(cr.currentConfig, "", "  ")
}

// StartWatching starts watching the config file for changes
func (cr *ConfigReloader) StartWatching(interval time.Duration) {
	if cr.watchEnabled.Load() {
		return
	}

	cr.watchEnabled.Store(true)
	cr.wg.Add(1)
	go cr.watchLoop(interval)
}

// StopWatching stops watching the config file
func (cr *ConfigReloader) StopWatching() {
	cr.watchEnabled.Store(false)
	cr.stopOnce.Do(func() {
		close(cr.stopCh)
	})
	cr.wg.Wait()
}

func (cr *ConfigReloader) watchLoop(interval time.Duration) {
	defer cr.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-cr.stopCh:
			return
		case <-ticker.C:
			if !cr.watchEnabled.Load() {
				return
			}
			cr.checkAndReload()
		}
	}
}

func (cr *ConfigReloader) checkAndReload() {
	stat, err := os.Stat(cr.configPath)
	if err != nil {
		return
	}

	if stat.ModTime().After(cr.lastModTime) {
		if err := cr.Load(); err != nil {
			logger.Default().Errorf("Config reload failed: %v", err)
		}
	}
}

// GetStats returns reload statistics
func (cr *ConfigReloader) GetStats() ReloadStats {
	lastReload := cr.lastReload.Load()
	var lastReloadTime time.Time
	if lastReload != nil {
		lastReloadTime = lastReload.(time.Time)
	}

	cr.mu.RLock()
	defer cr.mu.RUnlock()

	return ReloadStats{
		ReloadCount:  cr.reloadCount.Load(),
		ErrorCount:   cr.reloadErrors.Load(),
		LastReload:   lastReloadTime,
		WatchEnabled: cr.watchEnabled.Load(),
	}
}

// ReloadStats contains reload statistics
type ReloadStats struct {
	ReloadCount  uint64    `json:"reload_count"`
	ErrorCount   uint64    `json:"error_count"`
	LastReload   time.Time `json:"last_reload"`
	WatchEnabled bool      `json:"watch_enabled"`
}

// ConfigValidator provides validation for configuration values
type ConfigValidator struct {
	errors []string
}

// NewConfigValidator creates a new config validator
func NewConfigValidator() *ConfigValidator {
	return &ConfigValidator{
		errors: make([]string, 0),
	}
}

// ValidateLogLevel validates log level
func (v *ConfigValidator) ValidateLogLevel(level string) bool {
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true, "fatal": true}
	if !validLevels[level] {
		v.errors = append(v.errors, fmt.Sprintf("invalid log level: %s", level))
		return false
	}
	return true
}

// ValidateDuration validates a duration string
func (v *ConfigValidator) ValidateDuration(name, value string) bool {
	if _, err := time.ParseDuration(value); err != nil {
		v.errors = append(v.errors, fmt.Sprintf("invalid %s: %s", name, value))
		return false
	}
	return true
}

// ValidatePositiveInt validates a positive integer
func (v *ConfigValidator) ValidatePositiveInt(name string, value int) bool {
	if value <= 0 {
		v.errors = append(v.errors, fmt.Sprintf("%s must be positive, got %d", name, value))
		return false
	}
	return true
}

// ValidateNonNegativeInt validates a non-negative integer
func (v *ConfigValidator) ValidateNonNegativeInt(name string, value int) bool {
	if value < 0 {
		v.errors = append(v.errors, fmt.Sprintf("%s cannot be negative, got %d", name, value))
		return false
	}
	return true
}

// HasErrors returns true if there are validation errors
func (v *ConfigValidator) HasErrors() bool {
	return len(v.errors) > 0
}

// Errors returns validation errors
func (v *ConfigValidator) Errors() []string {
	return v.errors
}

// SaveConfig saves configuration to file
func SaveConfig(config *ReloadableConfig, path string) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}
