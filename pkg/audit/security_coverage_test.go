package audit

import (
	"encoding/json"
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

func TestAuditLogHashChain(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "chain.log")

	al, err := New(&Config{
		Enabled:    true,
		LogFile:    logPath,
		LogFormat:  "json",
		LogQueries: true,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}

	al.Log(EventQuery, "alice", "SELECT", WithQuery("SELECT 1"))
	al.Log(EventDDL, "bob", "CREATE_TABLE", WithQuery("CREATE TABLE t(id INT)"))
	if err := al.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read audit log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 audit lines, got %d: %q", len(lines), data)
	}

	var first, second Event
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("unmarshal first event: %v", err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("unmarshal second event: %v", err)
	}
	if first.PrevHash != "" {
		t.Fatalf("first prev hash = %q, want empty", first.PrevHash)
	}
	if first.Hash == "" {
		t.Fatal("first hash should not be empty")
	}
	if second.PrevHash != first.Hash {
		t.Fatalf("second prev hash = %q, want %q", second.PrevHash, first.Hash)
	}
	if second.Hash == "" || second.Hash == first.Hash {
		t.Fatalf("second hash should be non-empty and distinct, got %q", second.Hash)
	}

	originalHash := second.Hash
	second.Hash = ""
	payload, err := json.Marshal(&second)
	if err != nil {
		t.Fatalf("marshal second event without hash: %v", err)
	}
	if got := hashAuditPayload(second.PrevHash, payload); got != originalHash {
		t.Fatalf("recomputed second hash = %q, want %q", got, originalHash)
	}
}

func TestVerifyLogFilePlainJSON(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "chain.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   logPath,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	al.Log(EventQuery, "alice", "SELECT", WithQuery("SELECT 1"))
	al.Log(EventDML, "alice", "UPDATE", WithRowsAffected(1))
	if err := al.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	result, err := VerifyLogFile(logPath, nil)
	if err != nil {
		t.Fatalf("VerifyLogFile failed: %v", err)
	}
	if result.Entries != 2 {
		t.Fatalf("entries = %d, want 2", result.Entries)
	}
	if result.EncryptedEntries != 0 {
		t.Fatalf("encrypted entries = %d, want 0", result.EncryptedEntries)
	}
	if result.LastHash == "" {
		t.Fatal("last hash should not be empty")
	}
}

func TestVerifyLogFileRejectsUnsafePath(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "audit.log")
	linkPath := filepath.Join(tempDir, "audit-link.log")
	if err := os.WriteFile(logPath, []byte("{}\n"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := os.Symlink(logPath, linkPath); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	if _, err := VerifyLogFile(linkPath, nil); err == nil {
		t.Fatal("expected symlink audit log to be rejected")
	} else if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}

	if _, err := VerifyLogFile(tempDir, nil); err == nil {
		t.Fatal("expected directory audit log to be rejected")
	} else if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestVerifyLogFileRestrictsExistingPermissions(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "chain.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   logPath,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	al.Log(EventQuery, "alice", "SELECT", WithQuery("SELECT 1"))
	if err := al.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := os.Chmod(logPath, 0644); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	if _, err := VerifyLogFile(logPath, nil); err != nil {
		t.Fatalf("VerifyLogFile failed: %v", err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("audit log permissions = %v, want 0600", info.Mode().Perm())
	}
}

func TestVerifyLogFileEncryptedJSON(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "encrypted-chain.log")
	key := []byte("0123456789abcdef0123456789abcdef")

	al, err := New(&Config{
		Enabled:       true,
		LogFile:       logPath,
		LogFormat:     "json",
		EncryptionKey: key,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	al.Log(EventSecurity, "alice", "POLICY_CHECK")
	al.Log(EventAuth, "bob", "LOGIN")
	if err := al.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	result, err := VerifyLogFile(logPath, key)
	if err != nil {
		t.Fatalf("VerifyLogFile failed: %v", err)
	}
	if result.Entries != 2 {
		t.Fatalf("entries = %d, want 2", result.Entries)
	}
	if result.EncryptedEntries != 2 {
		t.Fatalf("encrypted entries = %d, want 2", result.EncryptedEntries)
	}
	if _, err := VerifyLogFile(logPath, nil); err == nil {
		t.Fatal("expected encrypted log verification without key to fail")
	}
}

func TestVerifyLogFileDetectsTampering(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "tampered.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   logPath,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	al.Log(EventQuery, "alice", "SELECT", WithQuery("SELECT 1"))
	if err := al.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	tampered := strings.Replace(string(data), `"action":"SELECT"`, `"action":"DELETE"`, 1)
	if err := os.WriteFile(logPath, []byte(tampered), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if _, err := VerifyLogFile(logPath, nil); err == nil {
		t.Fatal("expected tampered audit log to fail verification")
	}
}

func TestAuditHashChainContinuesAfterRestart(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "restart.log")

	firstLogger, err := New(&Config{
		Enabled:   true,
		LogFile:   logPath,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create first audit logger: %v", err)
	}
	firstLogger.Log(EventQuery, "alice", "SELECT", WithQuery("SELECT 1"))
	if err := firstLogger.Close(); err != nil {
		t.Fatalf("Close first logger failed: %v", err)
	}

	secondLogger, err := New(&Config{
		Enabled:   true,
		LogFile:   logPath,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create second audit logger: %v", err)
	}
	secondLogger.Log(EventDML, "alice", "UPDATE", WithRowsAffected(1))
	if err := secondLogger.Close(); err != nil {
		t.Fatalf("Close second logger failed: %v", err)
	}

	result, err := VerifyLogFile(logPath, nil)
	if err != nil {
		t.Fatalf("VerifyLogFile failed: %v", err)
	}
	if result.Entries != 2 {
		t.Fatalf("entries = %d, want 2", result.Entries)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var first, second Event
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("unmarshal first event: %v", err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("unmarshal second event: %v", err)
	}
	if second.PrevHash != first.Hash {
		t.Fatalf("second prev hash = %q, want %q", second.PrevHash, first.Hash)
	}
}

func TestEncryptedAuditHashChainContinuesAfterRestart(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "encrypted-restart.log")
	key := []byte("0123456789abcdef0123456789abcdef")

	firstLogger, err := New(&Config{
		Enabled:       true,
		LogFile:       logPath,
		LogFormat:     "json",
		EncryptionKey: key,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create first audit logger: %v", err)
	}
	firstLogger.Log(EventQuery, "alice", "SELECT", WithQuery("SELECT 1"))
	if err := firstLogger.Close(); err != nil {
		t.Fatalf("Close first logger failed: %v", err)
	}

	secondLogger, err := New(&Config{
		Enabled:       true,
		LogFile:       logPath,
		LogFormat:     "json",
		EncryptionKey: key,
	}, nil)
	if err != nil {
		t.Fatalf("Failed to create second audit logger: %v", err)
	}
	secondLogger.Log(EventSecurity, "alice", "POLICY_CHECK")
	if err := secondLogger.Close(); err != nil {
		t.Fatalf("Close second logger failed: %v", err)
	}

	result, err := VerifyLogFile(logPath, key)
	if err != nil {
		t.Fatalf("VerifyLogFile failed: %v", err)
	}
	if result.Entries != 2 {
		t.Fatalf("entries = %d, want 2", result.Entries)
	}
	if result.EncryptedEntries != 2 {
		t.Fatalf("encrypted entries = %d, want 2", result.EncryptedEntries)
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
