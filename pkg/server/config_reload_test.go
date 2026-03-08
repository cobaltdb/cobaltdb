package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigReloaderBasic(t *testing.T) {
	// Create temp config file
	tmpDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.json")

	// Create initial config
	config := DefaultReloadableConfig()
	config.LogLevel = "debug"
	if err := SaveConfig(config, configPath); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	cr, err := NewConfigReloader(configPath)
	if err != nil {
		t.Fatalf("Failed to create config reloader: %v", err)
	}

	cfg := cr.GetConfig()
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log level 'debug', got '%s'", cfg.LogLevel)
	}
}

func TestConfigReloaderDefaultConfig(t *testing.T) {
	// Test with non-existent file (should use defaults)
	cr, err := NewConfigReloader("/nonexistent/config.json")
	if err != nil {
		t.Fatalf("Failed to create config reloader: %v", err)
	}

	cfg := cr.GetConfig()
	if cfg.LogLevel != "info" {
		t.Errorf("Expected default log level 'info', got '%s'", cfg.LogLevel)
	}

	if cfg.MaxConnections != 100 {
		t.Errorf("Expected default max connections 100, got %d", cfg.MaxConnections)
	}
}

func TestConfigReloaderReload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.json")

	// Create initial config
	config := DefaultReloadableConfig()
	config.LogLevel = "info"
	if err := SaveConfig(config, configPath); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	cr, err := NewConfigReloader(configPath)
	if err != nil {
		t.Fatalf("Failed to create config reloader: %v", err)
	}

	// Update config file
	config.LogLevel = "error"
	if err := SaveConfig(config, configPath); err != nil {
		t.Fatalf("Failed to save updated config: %v", err)
	}

	// Reload
	if err := cr.Reload(); err != nil {
		t.Fatalf("Failed to reload config: %v", err)
	}

	cfg := cr.GetConfig()
	if cfg.LogLevel != "error" {
		t.Errorf("Expected log level 'error' after reload, got '%s'", cfg.LogLevel)
	}

	// Check stats
	stats := cr.GetStats()
	if stats.ReloadCount != 2 { // Initial load + reload
		t.Errorf("Expected 2 reloads, got %d", stats.ReloadCount)
	}
}

func TestConfigReloaderReloadFromJSON(t *testing.T) {
	cr, err := NewConfigReloader("/nonexistent/config.json")
	if err != nil {
		t.Fatalf("Failed to create config reloader: %v", err)
	}

	jsonData := []byte(`{
		"log_level": "warn",
		"max_connections": 200,
		"query_timeout": "30s"
	}`)

	if err := cr.ReloadFromJSON(jsonData); err != nil {
		t.Fatalf("Failed to reload from JSON: %v", err)
	}

	cfg := cr.GetConfig()
	if cfg.LogLevel != "warn" {
		t.Errorf("Expected log level 'warn', got '%s'", cfg.LogLevel)
	}

	if cfg.MaxConnections != 200 {
		t.Errorf("Expected max connections 200, got %d", cfg.MaxConnections)
	}
}

func TestConfigReloaderValidation(t *testing.T) {
	tests := []struct {
		name    string
		json    string
		wantErr bool
	}{
		{
			name:    "valid config",
			json:    `{"log_level": "info", "max_connections": 100}`,
			wantErr: false,
		},
		{
			name:    "invalid log level",
			json:    `{"log_level": "invalid"}`,
			wantErr: true,
		},
		{
			name:    "invalid query timeout",
			json:    `{"query_timeout": "not-a-duration"}`,
			wantErr: true,
		},
		{
			name:    "negative cache size",
			json:    `{"cache_size": -1}`,
			wantErr: true,
		},
		{
			name:    "negative max connections",
			json:    `{"max_connections": -1}`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cr, _ := NewConfigReloader("/nonexistent/config.json")
			err := cr.ReloadFromJSON([]byte(tc.json))
			if tc.wantErr && err == nil {
				t.Error("Expected error but got none")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestConfigReloaderCallbacks(t *testing.T) {
	cr, _ := NewConfigReloader("/nonexistent/config.json")

	logLevelChanged := false
	queryTimeoutChanged := false
	metricsChanged := false
	cacheSizeChanged := false

	cr.SetCallbacks(
		func(level string) {
			logLevelChanged = true
		},
		func(timeout time.Duration) {
			queryTimeoutChanged = true
		},
		func(enabled bool) {
			metricsChanged = true
		},
		func(size int) {
			cacheSizeChanged = true
		},
	)

	jsonData := []byte(`{
		"log_level": "error",
		"query_timeout": "120s",
		"metrics_enabled": false,
		"cache_size": 2048,
		"max_connections": 100
	}`)

	if err := cr.ReloadFromJSON(jsonData); err != nil {
		t.Fatalf("Failed to reload: %v", err)
	}

	if !logLevelChanged {
		t.Error("Expected log level callback to be called")
	}
	if !queryTimeoutChanged {
		t.Error("Expected query timeout callback to be called")
	}
	if !metricsChanged {
		t.Error("Expected metrics callback to be called")
	}
	if !cacheSizeChanged {
		t.Error("Expected cache size callback to be called")
	}
}

func TestConfigReloaderGetConfigJSON(t *testing.T) {
	cr, _ := NewConfigReloader("/nonexistent/config.json")

	jsonData, err := cr.GetConfigJSON()
	if err != nil {
		t.Fatalf("Failed to get config JSON: %v", err)
	}

	if len(jsonData) == 0 {
		t.Error("Expected non-empty JSON data")
	}

	// Verify it's valid JSON
	if err := cr.ReloadFromJSON(jsonData); err != nil {
		t.Errorf("Config JSON should be valid: %v", err)
	}
}

func TestConfigReloaderWatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.json")

	config := DefaultReloadableConfig()
	config.LogLevel = "info"
	if err := SaveConfig(config, configPath); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	cr, err := NewConfigReloader(configPath)
	if err != nil {
		t.Fatalf("Failed to create config reloader: %v", err)
	}

	// Start watching
	cr.StartWatching(50 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// Update config file
	config.LogLevel = "debug"
	if err := SaveConfig(config, configPath); err != nil {
		t.Fatalf("Failed to save updated config: %v", err)
	}

	// Wait for watcher to detect change
	time.Sleep(200 * time.Millisecond)

	// Stop watching
	cr.StopWatching()

	// Verify config was reloaded
	cfg := cr.GetConfig()
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log level 'debug' after watch reload, got '%s'", cfg.LogLevel)
	}
}

func TestDefaultReloadableConfig(t *testing.T) {
	cfg := DefaultReloadableConfig()

	if cfg.LogLevel != "info" {
		t.Errorf("Expected default log level 'info', got '%s'", cfg.LogLevel)
	}

	if cfg.MaxConnections != 100 {
		t.Errorf("Expected default max connections 100, got %d", cfg.MaxConnections)
	}

	if !cfg.MetricsEnabled {
		t.Error("Expected metrics to be enabled by default")
	}

	if cfg.CacheSize != 1024 {
		t.Errorf("Expected default cache size 1024, got %d", cfg.CacheSize)
	}
}

func TestConfigValidator(t *testing.T) {
	v := NewConfigValidator()

	// Valid validations
	if !v.ValidateLogLevel("info") {
		t.Error("Expected 'info' to be valid log level")
	}

	if !v.ValidateDuration("timeout", "30s") {
		t.Error("Expected '30s' to be valid duration")
	}

	if !v.ValidatePositiveInt("connections", 10) {
		t.Error("Expected 10 to be valid positive int")
	}

	if !v.ValidateNonNegativeInt("size", 0) {
		t.Error("Expected 0 to be valid non-negative int")
	}

	// Invalid validations
	if v.ValidateLogLevel("invalid") {
		t.Error("Expected 'invalid' to be invalid log level")
	}

	if v.ValidateDuration("timeout", "not-a-duration") {
		t.Error("Expected invalid duration to fail")
	}

	if v.ValidatePositiveInt("connections", 0) {
		t.Error("Expected 0 to be invalid positive int")
	}

	if v.ValidateNonNegativeInt("size", -1) {
		t.Error("Expected -1 to be invalid non-negative int")
	}

	if !v.HasErrors() {
		t.Error("Expected validation errors")
	}

	errors := v.Errors()
	if len(errors) == 0 {
		t.Error("Expected validation error messages")
	}
}

func TestSaveConfig(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "subdir", "config.json")

	config := DefaultReloadableConfig()
	config.LogLevel = "debug"

	if err := SaveConfig(config, configPath); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file should exist after save")
	}

	// Verify it can be loaded
	cr, err := NewConfigReloader(configPath)
	if err != nil {
		t.Fatalf("Failed to load saved config: %v", err)
	}

	cfg := cr.GetConfig()
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log level 'debug' in saved config, got '%s'", cfg.LogLevel)
	}
}
