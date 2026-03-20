package audit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

func TestAuditLogEncryption(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "encrypted.log")

	key := []byte("0123456789abcdef0123456789abcdef") // AES-256

	log := logger.New(logger.InfoLevel, os.Stderr)
	al, err := New(&Config{
		Enabled:       true,
		LogFile:       logPath,
		LogFormat:     "json",
		EncryptionKey: key,
	}, log)
	if err != nil {
		t.Fatalf("Failed to create encrypted audit logger: %v", err)
	}

	al.Log(EventQuery, "testuser", "SELECT", WithQuery("SELECT * FROM users"))
	al.Close()

	data, _ := os.ReadFile(logPath)
	content := string(data)
	if len(content) == 0 {
		t.Fatal("Log file is empty")
	}
	if !strings.HasPrefix(strings.TrimSpace(content), "ENC:") {
		t.Logf("Content prefix: %q", content[:min(40, len(content))])
	}
	if strings.Contains(content, "SELECT * FROM users") {
		t.Error("Plaintext query found in encrypted log file")
	}
	t.Logf("Encrypted log: %d bytes", len(data))
}

func TestAuditLogEncryptionInvalidKey(t *testing.T) {
	log := logger.New(logger.InfoLevel, os.Stderr)
	_, err := New(&Config{
		Enabled:       true,
		LogFile:       filepath.Join(t.TempDir(), "test.log"),
		EncryptionKey: []byte("short"),
	}, log)
	if err == nil {
		t.Error("Expected error for invalid key length")
	}
}

func TestAuditLogPlaintext(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "plain.log")

	log := logger.New(logger.InfoLevel, os.Stderr)
	al, err := New(&Config{
		Enabled:   true,
		LogFile:   logPath,
		LogFormat: "json",
	}, log)
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}

	al.Log(EventQuery, "testuser", "SELECT", WithQuery("SELECT 1"))
	al.Close()

	data, _ := os.ReadFile(logPath)
	content := string(data)
	if strings.HasPrefix(strings.TrimSpace(content), "ENC:") {
		t.Error("Plaintext log should not be encrypted")
	}
}

func TestMaskMetadataValues(t *testing.T) {
	if maskMetadataValues(nil) != nil {
		t.Error("nil input should return nil")
	}

	meta := map[string]interface{}{
		"table":    "users",
		"password": "secret",
		"api_key":  "abc",
		"token":    "tok",
		"name":     "alice",
	}
	m := maskMetadataValues(meta)
	if m["table"] != "users" || m["name"] != "alice" {
		t.Error("Normal fields should pass through")
	}
	if m["password"] != "***MASKED***" {
		t.Error("password should be masked")
	}
	if m["api_key"] != "***MASKED***" {
		t.Error("api_key should be masked")
	}
	if m["token"] != "***MASKED***" {
		t.Error("token should be masked")
	}
}
