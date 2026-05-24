package audit

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

func TestNewLogger(t *testing.T) {
	config := &Config{
		Enabled:   true,
		LogFile:   filepath.Join(t.TempDir(), "test_audit.log"),
		LogFormat: "json",
	}

	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer al.Close()

	if al == nil {
		t.Fatal("Audit logger is nil")
	}
}

func TestNewLoggerNormalizesPartialConfig(t *testing.T) {
	tempDir := t.TempDir()
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	defer os.Chdir(originalDir)

	config := &Config{Enabled: true}
	al, err := New(config, logger.Default())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer al.Close()

	defaults := DefaultConfig()
	if al.config.LogFile != defaults.LogFile {
		t.Fatalf("LogFile = %q, want %q", al.config.LogFile, defaults.LogFile)
	}
	if al.config.LogFormat != defaults.LogFormat {
		t.Fatalf("LogFormat = %q, want %q", al.config.LogFormat, defaults.LogFormat)
	}
	if al.config.MaxFileSize != defaults.MaxFileSize {
		t.Fatalf("MaxFileSize = %d, want %d", al.config.MaxFileSize, defaults.MaxFileSize)
	}
	if config.LogFile != "" || config.LogFormat != "" || config.MaxFileSize != 0 {
		t.Fatal("New should not mutate caller config")
	}
}

func TestNewLoggerCopiesMutableConfig(t *testing.T) {
	events := []EventType{EventQuery}
	sensitiveFields := []string{"session_id"}
	encryptionKey := []byte("0123456789abcdef0123456789abcdef")

	al, err := New(&Config{
		Enabled:         false,
		Events:          events,
		SensitiveFields: sensitiveFields,
		EncryptionKey:   encryptionKey,
	}, nil)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	events[0] = EventDDL
	sensitiveFields[0] = "mutated"
	encryptionKey[0] = 'X'

	if al.config.Events[0] != EventQuery {
		t.Fatalf("Events was mutated through caller slice: got %v", al.config.Events[0])
	}
	if al.config.SensitiveFields[0] != "session_id" {
		t.Fatalf("SensitiveFields was mutated through caller slice: got %q", al.config.SensitiveFields[0])
	}
	if string(al.config.EncryptionKey) != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("EncryptionKey was mutated through caller slice: got %q", al.config.EncryptionKey)
	}
}

func TestConfiguredSensitiveFieldsMaskMetadata(t *testing.T) {
	al := &Logger{config: &Config{SensitiveFields: []string{"session_id"}}}
	event := &Event{
		Metadata: map[string]interface{}{
			"session_id": "abc",
			"table":      "users",
		},
	}

	al.maskSensitiveData(event)

	if event.Metadata["session_id"] != "***MASKED***" {
		t.Fatalf("configured sensitive field was not masked: %v", event.Metadata["session_id"])
	}
	if event.Metadata["table"] != "users" {
		t.Fatalf("non-sensitive metadata changed: %v", event.Metadata["table"])
	}
}

func TestLogEvent(t *testing.T) {
	config := &Config{
		Enabled:   true,
		LogFile:   filepath.Join(t.TempDir(), "test_audit.log"),
		LogFormat: "text",
	}

	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer al.Close()

	// Log various events
	al.Log(EventQuery, "testuser", "SELECT", WithQuery("SELECT * FROM users"))
	al.Log(EventAuth, "testuser", "LOGIN", WithClientIP("127.0.0.1"))
	al.Log(EventDDL, "admin", "CREATE_TABLE", WithQuery("CREATE TABLE test (id INT)"))

	// Give time for async logging
	time.Sleep(100 * time.Millisecond)
}

func TestLogQuery(t *testing.T) {
	config := &Config{
		Enabled:    true,
		LogFile:    filepath.Join(t.TempDir(), "test_audit.log"),
		LogFormat:  "text",
		LogQueries: true,
	}

	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer al.Close()

	al.LogQuery("testuser", "SELECT * FROM users", 10*time.Millisecond, 5, nil)

	// Give time for async logging
	time.Sleep(100 * time.Millisecond)
}

func TestLogAuth(t *testing.T) {
	config := &Config{
		Enabled:         true,
		LogFile:         filepath.Join(t.TempDir(), "test_audit.log"),
		LogFormat:       "text",
		LogFailedLogins: true,
	}

	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer al.Close()

	// Successful login
	al.LogAuth("testuser", "LOGIN", true, "127.0.0.1", nil)

	// Failed login
	al.LogAuth("testuser", "LOGIN", false, "192.168.1.1", nil)

	// Give time for async logging
	time.Sleep(100 * time.Millisecond)
}

func TestDisabledLogger(t *testing.T) {
	config := &Config{
		Enabled: false,
	}

	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer al.Close()

	// Should not panic when disabled
	al.Log(EventQuery, "testuser", "SELECT")
	al.LogQuery("testuser", "SELECT * FROM users", 10*time.Millisecond, 5, nil)
}
