package audit

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// ============================================================
// cloneMetadataValue tests (43.8% -> target 100%)
// ============================================================

func TestCloneMetadataValueAllTypes(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{name: "string", input: "hello"},
		{name: "int64", input: int64(42)},
		{name: "float64", input: float64(3.14)},
		{name: "bool", input: true},
		{name: "nil", input: nil},
		{name: "int32", input: int32(100)},
		{name: "uint64", input: uint64(99)},
		{name: "int", input: 42},
		{name: "[]byte", input: []byte("binary")},
		{name: "[]string", input: []string{"a", "b"}},
		{name: "[]interface{}", input: []interface{}{"x", int64(1)}},
		{name: "map[string]interface{}", input: map[string]interface{}{"k": "v"}},
		{name: "map[string]string", input: map[string]string{"k": "v"}},
		{name: "empty []byte", input: []byte{}},
		{name: "empty []string", input: []string{}},
		{name: "empty []interface{}", input: []interface{}{}},
		{name: "nested []interface{} with map", input: []interface{}{map[string]interface{}{"a": int64(1)}}},
		{name: "nested map with []byte", input: map[string]interface{}{"data": []byte("nested")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cloneMetadataValue(tt.input)
			// Verify type preservation for every case
			switch v := tt.input.(type) {
			case []byte:
				cloned, ok := got.([]byte)
				if !ok {
					t.Fatalf("expected []byte, got %T", got)
				}
				if len(v) > 0 {
					v[0] = 0xFF
					if cloned[0] == 0xFF {
						t.Error("[]byte was not cloned")
					}
				}
			case []string:
				cloned, ok := got.([]string)
				if !ok {
					t.Fatalf("expected []string, got %T", got)
				}
				if len(v) > 0 {
					v[0] = "mutated"
					if cloned[0] == "mutated" {
						t.Error("[]string was not cloned")
					}
				}
			case []interface{}:
				cloned, ok := got.([]interface{})
				if !ok {
					t.Fatalf("expected []interface{}, got %T", got)
				}
				if len(cloned) != len(v) {
					t.Fatalf("length mismatch: %d vs %d", len(cloned), len(v))
				}
			case map[string]interface{}:
				cloned, ok := got.(map[string]interface{})
				if !ok {
					t.Fatalf("expected map[string]interface{}, got %T", got)
				}
				v["newkey"] = "mutated"
				if _, exists := cloned["newkey"]; exists {
					t.Error("map was not cloned")
				}
			case map[string]string:
				cloned, ok := got.(map[string]string)
				if !ok {
					t.Fatalf("expected map[string]string, got %T", got)
				}
				v["newkey"] = "mutated"
				if _, exists := cloned["newkey"]; exists {
					t.Error("map[string]string was not cloned")
				}
			default:
				if got != v {
					t.Errorf("unmodified value should be returned as-is: got %v, want %v", got, v)
				}
			}
		})
	}
}

// ============================================================
// openAuditLogForAppend tests (48.9% -> target 100%)
// ============================================================

func TestOpenAuditLogForAppendEmptyPath(t *testing.T) {
	_, err := openAuditLogForAppend("")
	if err == nil || !strings.Contains(err.Error(), "path cannot be empty") {
		t.Fatalf("expected empty path error, got %v", err)
	}
}

func TestOpenAuditLogForAppendNewFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "new_audit.log")
	file, err := openAuditLogForAppend(path)
	if err != nil {
		t.Fatalf("open new file: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("perms = %v, want 0600", info.Mode().Perm())
	}
}

func TestOpenAuditLogForAppendExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.log")
	if err := os.WriteFile(path, []byte("preexisting\n"), 0600); err != nil {
		t.Fatal(err)
	}

	file, err := openAuditLogForAppend(path)
	if err != nil {
		t.Fatalf("open existing file: %v", err)
	}
	defer file.Close()

	// Write to verify it opened for append
	if _, err := file.WriteString("appended\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "preexisting\nappended\n" {
		t.Fatalf("expected append, got %q", string(data))
	}
}

func TestOpenAuditLogForAppendRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.log")
	link := filepath.Join(dir, "link.log")
	if err := os.WriteFile(target, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	_, err := openAuditLogForAppend(link)
	if err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink error, got %v", err)
	}
}

func TestOpenAuditLogForAppendRejectsDirectory(t *testing.T) {
	dir := t.TempDir()
	_, err := openAuditLogForAppend(dir)
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file error, got %v", err)
	}
}

// ============================================================
// openAuditLogForRead tests (63.0% -> target 100%)
// ============================================================

func TestOpenAuditLogForReadEmptyPath(t *testing.T) {
	_, err := openAuditLogForRead("")
	if err == nil || !strings.Contains(err.Error(), "path cannot be empty") {
		t.Fatalf("expected empty path error, got %v", err)
	}
}

func TestOpenAuditLogForReadNonExistent(t *testing.T) {
	dir := t.TempDir()
	_, err := openAuditLogForRead(filepath.Join(dir, "nonexistent.log"))
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected not-exist error, got %v", err)
	}
}

func TestOpenAuditLogForReadRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.log")
	link := filepath.Join(dir, "link.log")
	if err := os.WriteFile(target, []byte("data\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	_, err := openAuditLogForRead(link)
	if err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink error, got %v", err)
	}
}

func TestOpenAuditLogForReadRejectsDirectory(t *testing.T) {
	dir := t.TempDir()
	_, err := openAuditLogForRead(dir)
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file error, got %v", err)
	}
}

func TestOpenAuditLogForReadNormalFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	if err := os.WriteFile(path, []byte("line1\nline2\n"), 0600); err != nil {
		t.Fatal(err)
	}

	file, err := openAuditLogForRead(path)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer file.Close()

	// Verify it restores perms to 0600
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("perms = %v, want 0600", info.Mode().Perm())
	}
}

// ============================================================
// rejectAuditLogDirSymlinkPathComponents tests (78.9% -> target 100%)
// ============================================================

func TestRejectAuditLogDirSymlinkPathComponentsNormal(t *testing.T) {
	dir := t.TempDir()
	err := rejectAuditLogDirSymlinkPathComponents(dir)
	if err != nil {
		t.Fatalf("normal path should not error: %v", err)
	}
}

func TestRejectAuditLogDirSymlinkPathComponentsRoot(t *testing.T) {
	err := rejectAuditLogDirSymlinkPathComponents("/")
	if err != nil {
		t.Fatalf("root path should not error: %v", err)
	}
}

func TestRejectAuditLogDirSymlinkPathComponentsDot(t *testing.T) {
	err := rejectAuditLogDirSymlinkPathComponents(".")
	if err != nil {
		t.Fatalf("dot path should not error: %v", err)
	}
}

func TestRejectAuditLogDirSymlinkPathComponentsNonExistent(t *testing.T) {
	// A non-existent path component should return nil (stops at first
	// non-existent component).
	dir := t.TempDir()
	err := rejectAuditLogDirSymlinkPathComponents(filepath.Join(dir, "nonexistent", "subdir"))
	if err != nil {
		t.Fatalf("non-existent path component should return nil: %v", err)
	}
}

func TestRejectAuditLogDirSymlinkPathComponentsRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(link, "subdir", "audit.log")
	err := rejectAuditLogDirSymlinkPathComponents(path)
	if err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink error, got %v", err)
	}
}

// ============================================================
// syncAuditLogParentDir tests (66.7% -> target 100%)
// ============================================================

func TestSyncAuditLogParentDirNonExistentDir(t *testing.T) {
	dir := t.TempDir()
	// Use a non-existent subdirectory
	badPath := filepath.Join(dir, "nonexistent", "audit.log")
	err := syncAuditLogParentDir(badPath)
	if err == nil {
		t.Fatal("expected error for non-existent parent dir")
	}
}

func TestSyncAuditLogParentDirNormal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	// Create the file first
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	err := syncAuditLogParentDir(path)
	if err != nil {
		t.Fatalf("sync parent dir: %v", err)
	}
}

// ============================================================
// loadLastHash tests (66.7% -> target 100%)
// ============================================================

func TestLoadLastHashNonExistentFile(t *testing.T) {
	dir := t.TempDir()
	al := &Logger{
		config: &Config{
			LogFile: filepath.Join(dir, "nonexistent.log"),
		},
	}
	err := al.loadLastHash()
	if err != nil {
		t.Fatalf("non-existent file should not error: %v", err)
	}
}

func TestLoadLastHashEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.log")
	if err := os.WriteFile(path, []byte{}, 0600); err != nil {
		t.Fatal(err)
	}
	al := &Logger{
		config: &Config{
			LogFile: path,
		},
	}
	err := al.loadLastHash()
	if err != nil {
		t.Fatalf("empty file should not error: %v", err)
	}
}

func TestLoadLastHashWithContentJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "content.log")

	// Create a logger, write an event, close to produce a valid log
	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	al.Log(EventQuery, "user1", "SELECT")
	al.Close()

	// Now loadLastHash on the existing file
	al2 := &Logger{
		config: &Config{
			LogFile: path,
		},
	}
	err = al2.loadLastHash()
	if err != nil {
		t.Fatalf("loadLastHash on existing file: %v", err)
	}
	if al2.lastHash == "" {
		t.Error("lastHash should not be empty after loading from existing file")
	}
}

// ============================================================
// Log function tests (58.8% -> target 90%+)
// ============================================================

func TestLogDisabledLoggerReturnsEarly(t *testing.T) {
	al := &Logger{
		config:    &Config{Enabled: false},
		eventChan: make(chan *Event, 10),
	}
	// Should not panic or block
	al.Log(EventQuery, "testuser", "SELECT")
}

func TestLogClosedLoggerReturnsEarly(t *testing.T) {
	al := &Logger{
		config:    &Config{Enabled: true},
		eventChan: make(chan *Event, 10),
		closed:    true,
	}
	al.Log(EventQuery, "testuser", "SELECT")
}

func TestLogEventFilteringNoMatch(t *testing.T) {
	al := &Logger{
		config: &Config{
			Enabled: true,
			Events:  []EventType{EventDDL},
		},
		eventChan: make(chan *Event, 10),
	}
	// Query event should be filtered out (only DDL is configured)
	al.Log(EventQuery, "testuser", "SELECT")
}

func TestLogWithNilMetadata(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "nilmeta.log")
	al, err := New(&Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Log an event with no options
	al.Log(EventSecurity, "user1", "CHECK")

	time.Sleep(200 * time.Millisecond)
	al.Close()
}

func TestLogChannelFullFallback(t *testing.T) {
	dir := t.TempDir()
	tmpFile := filepath.Join(dir, "fallback.log")

	// Create a Logger manually with a full channel and no writer goroutine,
	// so the Log call hits the default: branch (sync fallback).
	al := &Logger{
		config: &Config{
			Enabled:   true,
			LogFile:   tmpFile,
			LogFormat: "json",
		},
		eventChan: make(chan *Event, 1),
		stopChan:  make(chan struct{}),
	}

	// Fill the channel (buffer=1)
	al.eventChan <- &Event{Type: EventQuery, User: "blocker", Action: "FILL"}

	// This next Log should hit the default: branch (channel full)
	// Since file is nil, it records "audit log file is not open"
	al.Log(EventQuery, "testuser", "SELECT")
	if al.LastWriteError() == nil {
		t.Fatal("expected last write error from fallback path")
	}
	if !strings.Contains(al.LastWriteError().Error(), "audit log file is not open") {
		t.Fatalf("expected file-not-open error, got %v", al.LastWriteError())
	}
}

func TestLogChannelFullFallbackWithFile(t *testing.T) {
	dir := t.TempDir()
	tmpFile := filepath.Join(dir, "fallback2.log")

	// Create a real logger with a file but no writer goroutine
	file, err := os.OpenFile(tmpFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}

	al2 := &Logger{
		config: &Config{
			Enabled:   true,
			LogFile:   tmpFile,
			LogFormat: "json",
		},
		file:      file,
		eventChan: make(chan *Event, 1),
		stopChan:  make(chan struct{}),
	}

	// Fill the channel
	al2.eventChan <- &Event{Type: EventQuery, User: "blocker", Action: "FILL"}

	// This Log should hit the default branch and use the sync write path
	al2.Log(EventQuery, "testuser", "SELECT")
	if err := al2.LastWriteError(); err != nil {
		t.Fatalf("sync write should succeed, got: %v", err)
	}

	al2.mu.Lock()
	al2.file.Close()
	al2.mu.Unlock()

	data, _ := os.ReadFile(tmpFile)
	if !strings.Contains(string(data), "testuser") {
		t.Error("expected sync-written event to appear in file")
	}
}

// ============================================================
// openLogFile tests (67.6% -> target 90%+)
// ============================================================

func TestOpenLogFileWithExistingContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.log")

	// Create a file with a valid audit event using a real Logger first
	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	al.Log(EventQuery, "user1", "SELECT")
	al.Close()

	// Now use a logger config that points to this existing file.
	// openLogFile should call loadLastHash then openAuditLogForAppend.
	al2 := &Logger{
		config: &Config{
			Enabled:   true,
			LogFile:   path,
			LogFormat: "json",
		},
		eventChan: make(chan *Event, 10),
		stopChan:  make(chan struct{}),
	}
	err = al2.openLogFile()
	if err != nil {
		t.Fatalf("openLogFile on existing file: %v", err)
	}
	defer func() {
		if al2.file != nil {
			al2.file.Close()
		}
	}()
	if al2.file == nil {
		t.Fatal("file should not be nil after openLogFile")
	}
	if al2.lastHash == "" {
		t.Error("lastHash should not be empty after loading from existing file")
	}
}

// ============================================================
// rotateLocked tests (68.0% -> target 90%+)
// ============================================================

func TestRotateLockedNilFile(t *testing.T) {
	al := &Logger{
		config: &Config{
			LogFile: filepath.Join(t.TempDir(), "nil.log"),
		},
		eventChan: make(chan *Event, 10),
	}
	err := al.rotateLocked()
	if err != nil {
		t.Fatalf("rotateLocked with nil file should return nil, got: %v", err)
	}
}

func TestRotateLockedNormal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rotate.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Write some events
	al.mu.Lock()
	if err := al.writeEvent(&Event{
		Timestamp: time.Now().UTC(),
		Type:      EventQuery,
		User:      "testuser",
		Action:    "BEFORE_ROTATE",
		Status:    "SUCCESS",
	}); err != nil {
		al.mu.Unlock()
		t.Fatalf("writeEvent: %v", err)
	}
	al.mu.Unlock()

	time.Sleep(50 * time.Millisecond)

	if err := al.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Write after rotation
	al.mu.Lock()
	if err := al.writeEvent(&Event{
		Timestamp: time.Now().UTC(),
		Type:      EventQuery,
		User:      "testuser",
		Action:    "AFTER_ROTATE",
		Status:    "SUCCESS",
	}); err != nil {
		al.mu.Unlock()
		t.Fatalf("writeEvent after rotate: %v", err)
	}
	al.mu.Unlock()

	al.Close()

	// Verify the rotated backup exists
	matches, err := filepath.Glob(path + ".*")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) < 1 {
		t.Fatal("expected at least one rotated backup")
	}
}

// ============================================================
// writer tests (73.7% -> target 90%+)
// ============================================================

func TestWriterDrainsOnClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drain.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Log several events (fewer than batch size of 100)
	for i := 0; i < 5; i++ {
		al.Log(EventQuery, "user", "EVENT")
	}

	// Immediate close triggers the drain loop in writer()
	al.Close()

	// Verify events were written
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d: %q", len(lines), string(data))
	}
}

// ============================================================
// decryptAuditLogLine tests (76.9% -> target 100%)
// ============================================================

func newTestAEAD(t *testing.T) cipher.AEAD {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("NewGCM: %v", err)
	}
	return aead
}

func encryptForTest(t *testing.T, aead cipher.AEAD, plaintext, aad []byte) []byte {
	t.Helper()
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	ciphertext := aead.Seal(nonce, nonce, plaintext, aad)
	encoded := "ENC:" + base64.StdEncoding.EncodeToString(ciphertext)
	return []byte(encoded)
}

func TestDecryptAuditLogLineValid(t *testing.T) {
	aead := newTestAEAD(t)
	plaintext := []byte(`{"action":"SELECT","user":"alice"}`)
	aad := []byte("prevhash123")

	line := encryptForTest(t, aead, plaintext, aad)
	got, err := decryptAuditLogLine(aead, line, aad)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("got %q, want %q", string(got), string(plaintext))
	}
}

func TestDecryptAuditLogLineInvalidBase64(t *testing.T) {
	aead := newTestAEAD(t)
	_, err := decryptAuditLogLine(aead, []byte("ENC:!!!invalid-base64!!!"), nil)
	if err == nil {
		t.Fatal("expected error for invalid base64")
	}
}

func TestDecryptAuditLogLineShortCiphertext(t *testing.T) {
	aead := newTestAEAD(t)
	// Base64 encode something shorter than the nonce
	short := base64.StdEncoding.EncodeToString([]byte("short"))
	_, err := decryptAuditLogLine(aead, []byte("ENC:"+short), nil)
	if err == nil || !strings.Contains(err.Error(), "ciphertext shorter than nonce") {
		t.Fatalf("expected ciphertext-short error, got %v", err)
	}
}

func TestDecryptAuditLogLineAuthFailure(t *testing.T) {
	aead := newTestAEAD(t)
	plaintext := []byte(`{"action":"SELECT"}`)
	aad := []byte("correct-aad")

	line := encryptForTest(t, aead, plaintext, aad)

	// Decrypt with wrong AAD -> GCM auth failure
	_, err := decryptAuditLogLine(aead, line, []byte("wrong-aad"))
	if err == nil {
		t.Fatal("expected GCM auth error with wrong AAD")
	}
}

// ============================================================
// loadLastHash with text format
// ============================================================

func TestLoadLastHashTextFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "text_hash.log")

	// Create a text-format logger, write one event, close
	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "text",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	al.Log(EventQuery, "user1", "SELECT")
	al.Close()

	// Now reopen with text format — loadLastHash should use readLastTextAuditHash
	al2, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "text",
	}, nil)
	if err != nil {
		t.Fatalf("reopen with text format: %v", err)
	}
	al2.Close()
}

// ============================================================
// VerifyLogFile with a non-JSON / non-ENC line (error coverage)
// ============================================================

func TestVerifyLogFileRejectsNonJSONLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.log")
	// Write a non-JSON line (not starting with {)
	if err := os.WriteFile(path, []byte("not json\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := VerifyLogFile(path, nil)
	if err == nil || !strings.Contains(err.Error(), "only JSON audit logs can be verified") {
		t.Fatalf("expected JSON-only error, got %v", err)
	}
}

// ============================================================
// Log with encryption (exercises writeEvent encryption path)
// ============================================================

func TestLogWithEncryption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "encrypted_write.log")
	key := []byte("0123456789abcdef0123456789abcdef")

	al, err := New(&Config{
		Enabled:       true,
		LogFile:       path,
		LogFormat:     "json",
		EncryptionKey: key,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	al.Log(EventQuery, "testuser", "SELECT", WithQuery("SELECT * FROM users"))
	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("log file is empty")
	}
	if !strings.HasPrefix(strings.TrimSpace(string(data)), "ENC:") {
		t.Fatal("expected encrypted log entry")
	}
}

// ============================================================
// Additional VerifyLogFile coverage: error paths
// ============================================================

func TestVerifyLogFileMissingHash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no_hash.log")
	// Write a JSON line without a hash field
	if err := os.WriteFile(path, []byte(`{"action":"SELECT","user":"alice","hash":""}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := VerifyLogFile(path, nil)
	if err == nil || !strings.Contains(err.Error(), "missing hash") {
		t.Fatalf("expected missing hash error, got %v", err)
	}
}

func TestVerifyLogFilePrevHashMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "prev_hash_bad.log")

	// Create two valid events so we have a hash chain, then manually fix
	// the second event's PrevHash to mismatch.
	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	al.Log(EventQuery, "alice", "SELECT")
	al.Log(EventDML, "bob", "UPDATE")
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	// Parse the second event, corrupt its PrevHash
	if len(lines) < 2 {
		t.Fatal("need at least 2 lines")
	}
	var second Event
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	second.PrevHash = "0000000000000000000000000000000000000000000000000000000000000000"
	corrupted, err := json.Marshal(&second)
	if err != nil {
		t.Fatal(err)
	}
	lines[1] = string(corrupted)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0600); err != nil {
		t.Fatal(err)
	}

	_, err = VerifyLogFile(path, nil)
	if err == nil || !strings.Contains(err.Error(), "previous hash mismatch") {
		t.Fatalf("expected prev hash mismatch error, got %v", err)
	}
}

func TestVerifyLogFileHashMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hash_bad.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	al.Log(EventQuery, "alice", "SELECT")
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Tamper the action field
	tampered := strings.Replace(string(data), `"action":"SELECT"`, `"action":"DELETE"`, 1)
	if err := os.WriteFile(path, []byte(tampered), 0600); err != nil {
		t.Fatal(err)
	}

	_, err = VerifyLogFile(path, nil)
	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("expected hash mismatch error, got %v", err)
	}
}

func TestVerifyLogFileDecodeError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_json.log")
	// Write invalid JSON (starts with { but is malformed)
	if err := os.WriteFile(path, []byte("{bad json}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := VerifyLogFile(path, nil)
	if err == nil || !strings.Contains(err.Error(), "decode event") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

// ============================================================
// syncAuditLogParentDir: path with no directory component
// ============================================================

func TestSyncAuditLogParentDirNoDirComponent(t *testing.T) {
	// A bare filename (relative, no directory) should work in the
	// current directory. We chdir to a temp dir for this test.
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(origDir)

	// Create a bare file
	if err := os.WriteFile("bare.log", nil, 0600); err != nil {
		t.Fatal(err)
	}

	err = syncAuditLogParentDir("bare.log")
	if err != nil {
		t.Fatalf("expected no error for bare filename, got: %v", err)
	}
}

// ============================================================
// writer: verify close drain path
// ============================================================

func TestWriterFlushesOnCloseWithPendingEvents(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "writer_flush.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Log a mix of events that will be picked up by the writer
	al.Log(EventQuery, "u1", "Q1")
	al.Log(EventDDL, "u2", "D1")
	al.Log(EventAuth, "u3", "A1")

	// Close immediately — the writer drain loop picks up pending events
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected events to be written on close drain")
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 lines, got %d", len(lines))
	}
}

// ============================================================
// readLastTextAuditHash: encrypted line with decrypt error
// ============================================================

func TestReadLastTextAuditHashEncryptedDecryptError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad_enc_text.log")

	// Create a test AEAD with a known key
	aead := newTestAEAD(t)

	// Write an encrypted line that will fail to decrypt
	corrupted := "ENC:!!!corrupted!!!\n"
	if err := os.WriteFile(path, []byte(corrupted), 0600); err != nil {
		t.Fatal(err)
	}

	_, err := readLastTextAuditHash(path, aead)
	if err == nil {
		t.Fatal("expected decrypt error for corrupted encrypted line")
	}
}

// ============================================================
// indexIgnoreCase: empty substr returns 0
// ============================================================

func TestIndexIgnoreCaseEmptySubstr(t *testing.T) {
	if got := indexIgnoreCase("hello", ""); got != 0 {
		t.Fatalf("expected 0 for empty substr, got %d", got)
	}
}

// ============================================================
// loadLastHash: stat error (non-IsNotExist)
// ============================================================

func TestLoadLastHashStatError(t *testing.T) {
	al := &Logger{
		config: &Config{
			// Use a path where Stat will fail with a non-IsNotExist error.
			// A path like "/dev/null/subdir/nonexistent" or a file inside
			// a non-directory parent triggers ENOTDIR.
			LogFile: "/dev/null/audit.log",
		},
	}
	err := al.loadLastHash()
	if err == nil {
		t.Fatal("expected stat error for invalid path")
	}
	if !strings.Contains(err.Error(), "failed to stat audit log file") {
		t.Fatalf("expected stat error message, got: %v", err)
	}
}

func TestWriteEventFirstWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "first_write.log")

	al, err := New(&Config{
		Enabled:         true,
		LogFile:         path,
		LogFormat:       "text",
		RotationEnabled: true,
		MaxFileSize:     100,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Write a small event to a new file — the rotation check sees
	// info.Size() == 0 and skips rotation.
	al.mu.Lock()
	if err := al.writeEvent(&Event{
		Timestamp: time.Now().UTC(),
		Type:      EventQuery,
		User:      "testuser",
		Action:    "FIRST_WRITE",
		Status:    "SUCCESS",
	}); err != nil {
		al.mu.Unlock()
		t.Fatalf("writeEvent: %v", err)
	}
	al.mu.Unlock()

	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "FIRST_WRITE") {
		t.Fatal("expected event in log file")
	}
}

// ============================================================
// syncAuditLogParentDir: Lstat error for non-existent path
// ============================================================

func TestSyncAuditLogParentDirLstatError(t *testing.T) {
	// A path where the directory doesn't exist triggers Lstat error.
	err := syncAuditLogParentDir("/nonexistent_dir_xyz/audit.log")
	if err == nil {
		t.Fatal("expected error for non-existent parent dir")
	}
}

// ============================================================
// openAuditLogForRead: open failure due to permissions
// ============================================================

func TestOpenAuditLogForReadOpenError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no_perm.log")

	// Create file and remove all permissions
	if err := os.WriteFile(path, []byte("data\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0000); err != nil {
		t.Fatal(err)
	}

	_, err := openAuditLogForRead(path)
	if err == nil {
		t.Fatal("expected error for no-permissions file")
	}
}

// ============================================================
// pruneBackupsLocked: with logger configured (error logging)
// ============================================================

func TestPruneBackupsLoggerErrorPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "prune_test.log")

	// Create a Logger with a real logger
	realLog := logger.New(logger.InfoLevel, nil)
	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, realLog)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Write some events and rotate to create backups
	al.Log(EventQuery, "user", "EVENT_1")
	time.Sleep(50 * time.Millisecond)
	al.Rotate()

	al.Log(EventQuery, "user", "EVENT_2")
	time.Sleep(50 * time.Millisecond)
	al.Rotate()

	al.Log(EventQuery, "user", "EVENT_3")
	time.Sleep(50 * time.Millisecond)

	al.Close()

	// Verify that backups were created and pruned work
	matches, err := filepath.Glob(path + ".*")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) < 1 {
		t.Log("expected at least one backup")
	}
}

// ============================================================
// verifyLogFile: encrypted line without key (error)
// ============================================================

func TestVerifyLogFileEncryptedWithoutKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "enc_no_key.log")

	// Create an encrypted log file with a known key
	key := []byte("0123456789abcdef0123456789abcdef")
	al, err := New(&Config{
		Enabled:       true,
		LogFile:       path,
		LogFormat:     "json",
		EncryptionKey: key,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	al.Log(EventQuery, "alice", "SELECT")
	al.Close()

	// Verify without the encryption key should fail
	_, err = VerifyLogFile(path, nil)
	if err == nil || !strings.Contains(err.Error(), "encrypted entry requires encryption key") {
		t.Fatalf("expected encrypted-entry error, got %v", err)
	}
}

// ============================================================
// verifyLogFile: with text format (rejected)
// ============================================================

func TestVerifyLogFileTextFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "text_verify.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "text",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	al.Log(EventQuery, "alice", "SELECT")
	al.Close()

	// The line doesn't start with "{" so VerifyLogFile should reject it
	_, err = VerifyLogFile(path, nil)
	if err == nil || !strings.Contains(err.Error(), "only JSON audit logs can be verified") {
		t.Fatalf("expected JSON-only error, got %v", err)
	}
}

// ============================================================
// syncAuditLogParentDir: parent is a file (not dir)
// ============================================================

func TestSyncAuditLogParentDirParentIsFile(t *testing.T) {
	dir := t.TempDir()
	// Create a regular file at the "directory" path
	parentFile := filepath.Join(dir, "notadir")
	if err := os.WriteFile(parentFile, []byte("content"), 0600); err != nil {
		t.Fatal(err)
	}

	// The log path has the file as its parent directory
	logPath := filepath.Join(parentFile, "audit.log")
	err := syncAuditLogParentDir(logPath)
	if err == nil || !strings.Contains(err.Error(), "must be a directory") {
		t.Fatalf("expected directory error, got: %v", err)
	}
}

// ============================================================
// openLogFile: mkdir failure (parent is a file)
// ============================================================

func TestNewWithInvalidDirPath(t *testing.T) {
	dir := t.TempDir()
	// Create a file where the parent directory should be
	parentFile := filepath.Join(dir, "notadir")
	if err := os.WriteFile(parentFile, []byte("content"), 0600); err != nil {
		t.Fatal(err)
	}

	// Try to create a logger with this path (mkdir will fail)
	logPath := filepath.Join(parentFile, "audit.log")
	_, err := New(&Config{
		Enabled:   true,
		LogFile:   logPath,
		LogFormat: "json",
	}, nil)
	if err == nil {
		t.Fatal("expected error for invalid log path")
	}
}

// ============================================================
// rejectAuditLogDirSymlinkPathComponents: stat error (EACCES)
// ============================================================

func TestRejectAuditLogDirSymlinkPathComponentsStatError(t *testing.T) {
	dir := t.TempDir()
	// Create a subdirectory with no permissions so that traversing into
	// it fails with EACCES instead of ENOTEXIST.
	subdir := filepath.Join(dir, "locked")
	if err := os.Mkdir(subdir, 0000); err != nil {
		t.Fatal(err)
	}
	// Restore permissions on cleanup so TempDir can remove everything
	defer os.Chmod(subdir, 0700)

	// A path that requires traversing into the locked directory
	path := filepath.Join(subdir, "nested", "audit.log")
	err := rejectAuditLogDirSymlinkPathComponents(path)
	if err == nil || !strings.Contains(err.Error(), "failed to stat audit log directory component") {
		t.Fatalf("expected stat error, got: %v", err)
	}
}

// ============================================================
// maskMetadataValuesWithKeys: empty sensitive key skipped
// ============================================================

func TestMaskMetadataValuesWithKeysEmptySensitiveKey(t *testing.T) {
	meta := map[string]interface{}{
		"password": "secret",
		"name":     "alice",
	}
	// Extra sensitive keys with an empty string that should be skipped
	extraKeys := []string{"", "token"}
	result := maskMetadataValuesWithKeys(meta, extraKeys)
	if result["password"] != "***MASKED***" {
		t.Error("password should be masked")
	}
	if result["name"] != "alice" {
		t.Error("name should not be masked")
	}
}

// ============================================================
// openAuditLogForAppend: existing file open error (EACCES)
// ============================================================

func TestOpenAuditLogForAppendExistingOpenError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no_read.log")

	// Create a file with no read permission
	if err := os.WriteFile(path, []byte("content\n"), 0000); err != nil {
		t.Fatal(err)
	}

	// openAuditLogForAppend does O_APPEND|O_WRONLY, not O_RDONLY.
	// Write permission should be enough... But file.Chmod(0600) at
	// the end gives the owner read/write. Actually the issue is
	// os.OpenFile with O_WRONLY only needs write permission.
	// With 0000 permissions, the owner can still write? No, they can't.
	// On Linux, the owner needs write permission to open O_WRONLY.
	_, err := openAuditLogForAppend(path)
	if err == nil {
		t.Fatal("expected error for no-permissions file")
	}
}

// ============================================================
// writer: trigger maxBatchSize flush by sending many events
// ============================================================

func TestWriterBatchFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch_flush.log")

	al, err := New(&Config{
		Enabled:   true,
		LogFile:   path,
		LogFormat: "json",
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Send enough events to fill the batch (maxBatchSize=100)
	// and then some, ensuring the writer triggers a flush.
	for i := 0; i < 120; i++ {
		al.Log(EventQuery, "user", "BATCH_EVENT")
	}

	// Give writer goroutine time to process events before close
	time.Sleep(200 * time.Millisecond)

	// Close triggers drain of remaining events
	al.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 120 {
		t.Fatalf("expected 120 lines, got %d", len(lines))
	}
}

// ============================================================
// verifyLogFile: encrypted entries counter
// ============================================================

func TestVerifyLogFileEncryptedEntryCounter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "enc_counter.log")
	key := []byte("0123456789abcdef0123456789abcdef")

	al, err := New(&Config{
		Enabled:       true,
		LogFile:       path,
		LogFormat:     "json",
		EncryptionKey: key,
	}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	al.Log(EventSecurity, "alice", "CHECK")
	al.Log(EventAuth, "bob", "LOGIN")
	al.Close()

	result, err := VerifyLogFile(path, key)
	if err != nil {
		t.Fatalf("VerifyLogFile: %v", err)
	}
	if result.EncryptedEntries != 2 {
		t.Fatalf("encrypted entries = %d, want 2", result.EncryptedEntries)
	}
	if result.Entries != 2 {
		t.Fatalf("total entries = %d, want 2", result.Entries)
	}
}

// ============================================================
// verifyLogFile: encrypted line without valid JSON after decrypt
// ============================================================

func TestVerifyLogFileEncryptedLineNonJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "enc_nonjson.log")

	// Manually create an encrypted file with non-JSON content
	aead := newTestAEAD(t)
	plaintext := []byte("not json at all")
	line := encryptForTest(t, aead, plaintext, []byte(""))
	content := string(line) + "\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	// Verify with a matching key
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	_, err := VerifyLogFile(path, key)
	if err == nil || !strings.Contains(err.Error(), "only JSON audit logs can be verified") {
		t.Fatalf("expected JSON-only error, got %v", err)
	}
}
