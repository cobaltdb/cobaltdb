package audit

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// ---- DefaultConfig tests ----

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("DefaultConfig returned nil")
	}
	if cfg.Enabled {
		t.Error("Expected Enabled=false by default")
	}
	if cfg.LogFile == "" {
		t.Error("Expected non-empty LogFile")
	}
	if cfg.LogFormat != "json" {
		t.Errorf("Expected LogFormat=json, got %s", cfg.LogFormat)
	}
	if !cfg.RotationEnabled {
		t.Error("Expected RotationEnabled=true")
	}
	if cfg.MaxFileSize != 100*1024*1024 {
		t.Errorf("Expected MaxFileSize=104857600, got %d", cfg.MaxFileSize)
	}
	if cfg.MaxBackups != 10 {
		t.Errorf("Expected MaxBackups=10, got %d", cfg.MaxBackups)
	}
	if cfg.MaxAge != 30 {
		t.Errorf("Expected MaxAge=30, got %d", cfg.MaxAge)
	}
	if !cfg.LogQueries {
		t.Error("Expected LogQueries=true")
	}
	if !cfg.LogFailedLogins {
		t.Error("Expected LogFailedLogins=true")
	}
	if !cfg.LogDDL {
		t.Error("Expected LogDDL=true")
	}
	if !cfg.LogConnections {
		t.Error("Expected LogConnections=true")
	}
}

// ---- LogOption tests ----

func TestWithTable(t *testing.T) {
	e := &Event{}
	opt := WithTable("users")
	opt(e)
	if e.Table != "users" {
		t.Errorf("Expected table=users, got %s", e.Table)
	}
}

func TestWithDatabase(t *testing.T) {
	e := &Event{}
	opt := WithDatabase("mydb")
	opt(e)
	if e.Database != "mydb" {
		t.Errorf("Expected database=mydb, got %s", e.Database)
	}
}

func TestWithMetadata(t *testing.T) {
	e := &Event{}
	opt := WithMetadata("key1", "value1")
	opt(e)
	if e.Metadata == nil {
		t.Fatal("Metadata should not be nil")
	}
	if e.Metadata["key1"] != "value1" {
		t.Errorf("Expected metadata key1=value1, got %v", e.Metadata["key1"])
	}
}

func TestWithMetadataNilMap(t *testing.T) {
	e := &Event{Metadata: nil}
	opt := WithMetadata("k", "v")
	opt(e)
	if e.Metadata == nil || e.Metadata["k"] != "v" {
		t.Error("WithMetadata should initialize nil map")
	}
}

func TestWithClientIP(t *testing.T) {
	e := &Event{}
	opt := WithClientIP("10.0.0.1")
	opt(e)
	if e.ClientIP != "10.0.0.1" {
		t.Errorf("Expected clientIP=10.0.0.1, got %s", e.ClientIP)
	}
}

func TestWithDuration(t *testing.T) {
	e := &Event{}
	opt := WithDuration(5 * time.Second)
	opt(e)
	if e.Duration != 5*time.Second {
		t.Errorf("Expected duration=5s, got %v", e.Duration)
	}
}

func TestWithRowsAffected(t *testing.T) {
	e := &Event{}
	opt := WithRowsAffected(42)
	opt(e)
	if e.RowsAffected != 42 {
		t.Errorf("Expected rowsAffected=42, got %d", e.RowsAffected)
	}
}

func TestWithError(t *testing.T) {
	e := &Event{Status: "SUCCESS"}
	opt := WithError(errors.New("something broke"))
	opt(e)
	if e.Error != "something broke" {
		t.Errorf("Expected error text, got %s", e.Error)
	}
	if e.Status != "ERROR" {
		t.Errorf("Expected status=ERROR, got %s", e.Status)
	}
}

func TestWithErrorNil(t *testing.T) {
	e := &Event{Status: "SUCCESS"}
	opt := WithError(nil)
	opt(e)
	if e.Status != "SUCCESS" {
		t.Errorf("WithError(nil) should not change status, got %s", e.Status)
	}
}

func TestWithQuery(t *testing.T) {
	e := &Event{}
	opt := WithQuery("SELECT 1")
	opt(e)
	if e.Query != "SELECT 1" {
		t.Errorf("Expected query, got %s", e.Query)
	}
}

// ---- EventType String tests ----

func TestEventTypeString(t *testing.T) {
	tests := []struct {
		et   EventType
		want string
	}{
		{EventQuery, "QUERY"},
		{EventDDL, "DDL"},
		{EventDML, "DML"},
		{EventAuth, "AUTH"},
		{EventConnection, "CONNECTION"},
		{EventSecurity, "SECURITY"},
		{EventAdmin, "ADMIN"},
		{EventType(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		got := tt.et.String()
		if got != tt.want {
			t.Errorf("EventType(%d).String() = %s, want %s", tt.et, got, tt.want)
		}
	}
}

// ---- LogDDL test ----

func TestLogDDL(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_ddl.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
		LogDDL:    true,
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	al.LogDDL("admin", "CREATE_TABLE", "CREATE TABLE t(id INT)", nil)
	al.LogDDL("admin", "DROP_TABLE", "DROP TABLE t", errors.New("table not found"))

	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "CREATE_TABLE") {
		t.Error("Expected CREATE_TABLE in log output")
	}
	if !strings.Contains(content, "table not found") {
		t.Error("Expected error message in log output")
	}
}

// ---- LogConnection test ----

func TestLogConnection(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_conn.log"
	config := &Config{
		Enabled:        true,
		LogFile:        tmpFile,
		LogFormat:      "text",
		LogConnections: true,
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	al.LogConnection("192.168.1.1", "CONNECT", nil)
	al.LogConnection("192.168.1.1", "DISCONNECT", errors.New("timeout"))

	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "CONNECT") {
		t.Error("Expected CONNECT in log output")
	}
}

// ---- LogSecurity test ----

func TestLogSecurity(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_sec.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	meta := map[string]interface{}{
		"reason": "brute force",
		"count":  5,
	}
	al.LogSecurity("attacker", "BLOCK_IP", "10.0.0.99", meta)

	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "BLOCK_IP") {
		t.Error("Expected BLOCK_IP in log output")
	}
	if !strings.Contains(content, "brute force") {
		t.Error("Expected metadata in log output")
	}
}

// ---- LogSecurity with nil metadata ----

func TestLogSecurityNilMetadata(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_sec2.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	al.LogSecurity("user1", "ACCESS_DENIED", "10.0.0.1", nil)

	time.Sleep(200 * time.Millisecond)
	al.Close()
}

// ---- Rotate test ----

func TestRotate(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := tmpDir + "/audit_rotate.log"
	config := &Config{
		Enabled:         true,
		LogFile:         tmpFile,
		LogFormat:       "json",
		RotationEnabled: true,
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Write some data first
	al.Log(EventQuery, "user1", "SELECT", WithQuery("SELECT 1"))
	time.Sleep(200 * time.Millisecond)

	// Rotate
	err = al.Rotate()
	if err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	// Write more data after rotation
	al.Log(EventQuery, "user2", "SELECT", WithQuery("SELECT 2"))
	time.Sleep(200 * time.Millisecond)

	al.Close()

	// Verify new log file exists
	if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
		t.Error("New log file should exist after rotation")
	}
}

func TestRotateNilFile(t *testing.T) {
	config := &Config{
		Enabled: false,
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer al.Close()

	// Rotate on disabled logger (nil file) should return nil
	err = al.Rotate()
	if err != nil {
		t.Errorf("Rotate on nil file should return nil, got %v", err)
	}
}

// ---- maskPasswordPattern tests ----

func TestMaskPasswordPattern(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		keyword string
		want    string
	}{
		{
			name:    "single-quoted password",
			query:   "CREATE USER foo PASSWORD 'secret123'",
			keyword: "PASSWORD",
			want:    "CREATE USER foo PASSWORD '***'",
		},
		{
			name:    "double-quoted password",
			query:   `CREATE USER foo PASSWORD "secret123"`,
			keyword: "PASSWORD",
			want:    `CREATE USER foo PASSWORD "***"`,
		},
		{
			name:    "case insensitive",
			query:   "ALTER USER foo password 'mypass'",
			keyword: "password",
			want:    "ALTER USER foo password '***'",
		},
		{
			name:    "with equals sign",
			query:   "SET PASSWORD = 'newpass'",
			keyword: "PASSWORD",
			want:    "SET PASSWORD = '***'",
		},
		{
			name:    "no match",
			query:   "SELECT * FROM users",
			keyword: "PASSWORD",
			want:    "SELECT * FROM users",
		},
		{
			name:    "IDENTIFIED BY",
			query:   "CREATE USER foo IDENTIFIED BY 'secret'",
			keyword: "IDENTIFIED BY",
			want:    "CREATE USER foo IDENTIFIED BY '***'",
		},
		{
			name:    "API_KEY",
			query:   "SET API_KEY = 'abc123def'",
			keyword: "API_KEY",
			want:    "SET API_KEY = '***'",
		},
		{
			name:    "no value after keyword",
			query:   "PASSWORD",
			keyword: "PASSWORD",
			want:    "PASSWORD",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskPasswordPattern(tt.query, tt.keyword)
			if got != tt.want {
				t.Errorf("maskPasswordPattern(%q, %q) = %q, want %q", tt.query, tt.keyword, got, tt.want)
			}
		})
	}
}

// ---- maskKeyValuePair tests ----

func TestMaskKeyValuePair(t *testing.T) {
	tests := []struct {
		name  string
		query string
		key   string
		want  string
	}{
		{
			name:  "simple key=value",
			query: "password=secret123",
			key:   "password",
			want:  "password=***",
		},
		{
			name:  "key=value with trailing space",
			query: "password=secret123 host=localhost",
			key:   "password",
			want:  "password=*** host=localhost",
		},
		{
			name:  "key=value with comma separator",
			query: "password=secret123,host=localhost",
			key:   "password",
			want:  "password=***,host=localhost",
		},
		{
			name:  "key=value with semicolon",
			query: "password=secret123;host=localhost",
			key:   "password",
			want:  "password=***;host=localhost",
		},
		{
			name:  "key with spaces around equals",
			query: "password = secret123 extra",
			key:   "password",
			want:  "password = *** extra",
		},
		{
			name:  "pwd key",
			query: "pwd=s3cret",
			key:   "pwd",
			want:  "pwd=***",
		},
		{
			name:  "no match",
			query: "host=localhost",
			key:   "password",
			want:  "host=localhost",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskKeyValuePair(tt.query, tt.key)
			if got != tt.want {
				t.Errorf("maskKeyValuePair(%q, %q) = %q, want %q", tt.query, tt.key, got, tt.want)
			}
		})
	}
}

// ---- New with nil config ----

func TestNewWithNilConfig(t *testing.T) {
	al, err := New(nil, nil)
	if err != nil {
		t.Fatalf("New(nil, nil) should not error: %v", err)
	}
	defer al.Close()
	if al.config == nil {
		t.Error("config should be defaulted, not nil")
	}
}

// ---- Event filtering by configured event types ----

func TestLogEventFiltering(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_filter.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
		Events:    []EventType{EventDDL}, // Only log DDL events
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// This should NOT be logged (query event, not DDL)
	al.Log(EventQuery, "user1", "SELECT", WithQuery("SELECT 1"))
	// This SHOULD be logged
	al.Log(EventDDL, "admin", "CREATE", WithQuery("CREATE TABLE x(id INT)"))

	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}
	content := string(data)
	if strings.Contains(content, "SELECT 1") {
		t.Error("Query event should have been filtered out")
	}
	if !strings.Contains(content, "CREATE") {
		t.Error("DDL event should have been logged")
	}
}

// ---- Sensitive data masking integration ----

func TestMaskSensitiveDataIntegration(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_mask.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	al.Log(EventDDL, "admin", "CREATE_USER",
		WithQuery("CREATE USER foo PASSWORD 'supersecret' TOKEN 'abc123'"))

	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to read log: %v", err)
	}
	content := string(data)
	if strings.Contains(content, "supersecret") {
		t.Error("Password should be masked")
	}
	if strings.Contains(content, "abc123") {
		t.Error("Token should be masked")
	}
}

// ---- Close on disabled logger ----

func TestCloseDisabledLogger(t *testing.T) {
	config := &Config{Enabled: false}
	al, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = al.Close()
	if err != nil {
		t.Errorf("Close on disabled logger should not error: %v", err)
	}
}

// ---- Text format write ----

func TestWriteEventTextFormat(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_text.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "text",
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatal(err)
	}

	al.Log(EventQuery, "user1", "SELECT",
		WithQuery("SELECT * FROM t"),
		WithError(errors.New("oops")))

	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)
	if !strings.Contains(content, "user=user1") {
		t.Error("Expected user=user1 in text output")
	}
	if !strings.Contains(content, "QUERY") {
		t.Error("Expected QUERY in text output")
	}
	if !strings.Contains(content, "error=") {
		t.Error("Expected error= in text output")
	}
}

// ---- LogQuery with error ----

func TestLogQueryWithError(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_qerr.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatal(err)
	}

	al.LogQuery("testuser", "SELECT * FROM missing", 5*time.Millisecond, 0, errors.New("table not found"))

	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, _ := os.ReadFile(tmpFile)
	content := string(data)
	if !strings.Contains(content, "table not found") {
		t.Error("Expected error in log output")
	}
}

// ---- LogAuth with error ----

func TestLogAuthWithError(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_auth.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
	}
	log := logger.Default()
	al, err := New(config, log)
	if err != nil {
		t.Fatal(err)
	}

	al.LogAuth("baduser", "LOGIN", false, "192.168.1.1", errors.New("invalid password"))

	time.Sleep(200 * time.Millisecond)
	al.Close()

	data, _ := os.ReadFile(tmpFile)
	content := string(data)
	if !strings.Contains(content, "invalid password") {
		t.Error("Expected error in auth log")
	}
}

// ---- Log after close should not panic ----

func TestLogAfterClose(t *testing.T) {
	tmpFile := t.TempDir() + "/audit_closed.log"
	config := &Config{
		Enabled:   true,
		LogFile:   tmpFile,
		LogFormat: "json",
	}
	al, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}

	al.Close()

	// Should not panic
	al.Log(EventQuery, "user1", "SELECT")
}
