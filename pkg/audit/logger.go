// Package audit provides comprehensive audit logging for CobaltDB
package audit

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/logger"
)

// EventType represents the type of audit event
type EventType int

const (
	EventQuery EventType = iota
	EventDDL
	EventDML
	EventAuth
	EventConnection
	EventSecurity
	EventAdmin
)

func (e EventType) String() string {
	switch e {
	case EventQuery:
		return "QUERY"
	case EventDDL:
		return "DDL"
	case EventDML:
		return "DML"
	case EventAuth:
		return "AUTH"
	case EventConnection:
		return "CONNECTION"
	case EventSecurity:
		return "SECURITY"
	case EventAdmin:
		return "ADMIN"
	default:
		return "UNKNOWN"
	}
}

// Event represents a single audit event
type Event struct {
	Timestamp    time.Time              `json:"timestamp"`
	Type         EventType              `json:"type"`
	EventID      string                 `json:"event_id"`
	User         string                 `json:"user"`
	ClientIP     string                 `json:"client_ip,omitempty"`
	Database     string                 `json:"database,omitempty"`
	Table        string                 `json:"table,omitempty"`
	Action       string                 `json:"action"`
	Query        string                 `json:"query,omitempty"`
	Status       string                 `json:"status"`
	Duration     time.Duration          `json:"duration,omitempty"`
	RowsAffected int64                  `json:"rows_affected,omitempty"`
	Error        string                 `json:"error,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// Config holds audit logging configuration
type Config struct {
	Enabled         bool
	LogFile         string
	LogFormat       string // "json" or "text"
	RotationEnabled bool
	MaxFileSize     int64 // bytes
	MaxBackups      int
	MaxAge          int         // days
	Events          []EventType // Which events to log (empty = all)
	LogQueries      bool
	LogFailedLogins bool
	LogDDL          bool
	LogConnections  bool
	SensitiveFields []string // Fields to mask in logs
	EncryptionKey   []byte   // Optional: encrypt audit log entries (nil = plaintext)
}

// DefaultConfig returns default audit configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:         false,
		LogFile:         "cobaltdb_audit.log",
		LogFormat:       "json",
		RotationEnabled: true,
		MaxFileSize:     100 * 1024 * 1024, // 100MB
		MaxBackups:      10,
		MaxAge:          30,
		LogQueries:      true,
		LogFailedLogins: true,
		LogDDL:          true,
		LogConnections:  true,
	}
}

// Logger handles audit logging
type Logger struct {
	config    *Config
	file      *os.File
	logger    *logger.Logger
	mu        sync.Mutex
	eventChan chan *Event
	stopChan  chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closed    bool
	closeMu   sync.RWMutex
	cipher    cipher.AEAD // optional encryption for log entries
}

// New creates a new audit logger
func New(config *Config, log *logger.Logger) (*Logger, error) {
	if config == nil {
		config = DefaultConfig()
	}

	al := &Logger{
		config:    config,
		logger:    log,
		eventChan: make(chan *Event, 1000),
		stopChan:  make(chan struct{}),
	}

	// Initialize encryption if key provided
	if len(config.EncryptionKey) > 0 {
		block, err := aes.NewCipher(config.EncryptionKey)
		if err != nil {
			return nil, fmt.Errorf("audit log encryption setup failed: %w", err)
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("audit log GCM setup failed: %w", err)
		}
		al.cipher = gcm
	}

	if !config.Enabled {
		return al, nil
	}

	if err := al.openLogFile(); err != nil {
		return nil, err
	}

	al.wg.Add(1)
	go al.writer()

	return al, nil
}

func (al *Logger) openLogFile() error {
	dir := filepath.Dir(al.config.LogFile)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create audit log directory: %w", err)
		}
	}

	file, err := os.OpenFile(al.config.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("failed to open audit log file: %w", err)
	}

	al.file = file
	return nil
}

// Log creates and logs an audit event
func (al *Logger) Log(eventType EventType, user, action string, opts ...LogOption) {
	if !al.config.Enabled {
		return
	}

	// Check if logger is closed
	al.closeMu.RLock()
	closed := al.closed
	al.closeMu.RUnlock()
	if closed {
		return
	}

	if len(al.config.Events) > 0 {
		found := false
		for _, t := range al.config.Events {
			if t == eventType {
				found = true
				break
			}
		}
		if !found {
			return
		}
	}

	event := &Event{
		Timestamp: time.Now().UTC(),
		Type:      eventType,
		User:      user,
		Action:    action,
		EventID:   generateEventID(),
		Status:    "SUCCESS",
		Metadata:  make(map[string]interface{}),
	}

	for _, opt := range opts {
		opt(event)
	}

	al.maskSensitiveData(event)

	select {
	case al.eventChan <- event:
	default:
		// Channel full — write synchronously under lock to avoid data race on al.file
		al.mu.Lock()
		if err := al.writeEvent(event); err != nil && al.logger != nil {
			al.logger.Errorf("Failed to write audit event (sync fallback): %v", err)
		}
		al.mu.Unlock()
	}
}

func (al *Logger) writer() {
	defer al.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var batch []*Event
	maxBatchSize := 100

	for {
		select {
		case event := <-al.eventChan:
			batch = append(batch, event)
			if len(batch) >= maxBatchSize {
				al.flushBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				al.flushBatch(batch)
				batch = batch[:0]
			}

		case <-al.stopChan:
			for len(al.eventChan) > 0 {
				batch = append(batch, <-al.eventChan)
			}
			if len(batch) > 0 {
				al.flushBatch(batch)
			}
			return
		}
	}
}

func (al *Logger) flushBatch(events []*Event) {
	al.mu.Lock()
	defer al.mu.Unlock()

	if al.file == nil {
		return
	}

	for _, event := range events {
		if err := al.writeEvent(event); err != nil {
			if al.logger != nil {
				al.logger.Errorf("Failed to write audit event: %v", err)
			}
		}
	}

	if err := al.file.Sync(); err != nil && al.logger != nil {
		al.logger.Errorf("Failed to sync audit log: %v", err)
	}
}

func (al *Logger) writeEvent(event *Event) error {
	var line string

	switch al.config.LogFormat {
	case "text":
		line = fmt.Sprintf("[%s] %s %s user=%s action=%s status=%s",
			event.Timestamp.Format(time.RFC3339),
			event.Type,
			event.EventID,
			event.User,
			event.Action,
			event.Status,
		)
		if event.Query != "" {
			line += fmt.Sprintf(" query=%q", event.Query)
		}
		if event.Error != "" {
			line += fmt.Sprintf(" error=%q", event.Error)
		}
		line += "\n"

	default:
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}
		line = string(data) + "\n"
	}

	// Encrypt log line if cipher is configured
	if al.cipher != nil {
		nonce := make([]byte, al.cipher.NonceSize())
		if _, nonceErr := rand.Read(nonce); nonceErr != nil {
			return fmt.Errorf("audit log nonce generation failed: %w", nonceErr)
		}
		encrypted := al.cipher.Seal(nonce, nonce, []byte(line), nil)
		line = "ENC:" + base64.StdEncoding.EncodeToString(encrypted) + "\n"
	}

	_, err := al.file.WriteString(line)
	return err
}

func (al *Logger) maskSensitiveData(event *Event) {
	// Mask sensitive values in Metadata map
	event.Metadata = maskMetadataValues(event.Metadata)

	if event.Query == "" {
		return
	}

	query := event.Query

	// Mask passwords in CREATE/ALTER USER statements
	// Pattern: PASSWORD '...' or PASSWORD "..."
	query = maskPasswordPattern(query, "PASSWORD")
	query = maskPasswordPattern(query, "password")

	// Mask passwords in IDENTIFIED BY clauses
	// Pattern: IDENTIFIED BY '...' or IDENTIFIED BY "..."
	query = maskPasswordPattern(query, "IDENTIFIED BY")
	query = maskPasswordPattern(query, "identified by")

	// Mask API keys and tokens
	query = maskPasswordPattern(query, "API_KEY")
	query = maskPasswordPattern(query, "api_key")
	query = maskPasswordPattern(query, "TOKEN")
	query = maskPasswordPattern(query, "token")
	query = maskPasswordPattern(query, "SECRET")
	query = maskPasswordPattern(query, "secret")

	// Mask connection strings with passwords
	// Pattern: password=... or pwd=...
	query = maskKeyValuePair(query, "password")
	query = maskKeyValuePair(query, "pwd")

	event.Query = query
}

// maskPasswordPattern masks passwords after specific keywords
func maskPasswordPattern(query, keyword string) string {
	result := query
	offset := 0

	for {
		idx := strings.Index(strings.ToUpper(result[offset:]), strings.ToUpper(keyword))
		if idx == -1 {
			break
		}
		idx += offset

		// Find the start of the value (skip whitespace and optional =)
		pos := idx + len(keyword)
		for pos < len(result) && (result[pos] == ' ' || result[pos] == '\t' || result[pos] == '=') {
			pos++
		}

		// Check if value is quoted
		if pos < len(result) && (result[pos] == '\'' || result[pos] == '"') {
			quote := result[pos]
			start := pos
			pos++

			// Find closing quote
			for pos < len(result) && result[pos] != quote {
				pos++
			}

			if pos < len(result) {
				// Mask the content between quotes
				result = result[:start+1] + "***" + result[pos:]
			}
		}

		offset = idx + len(keyword)
	}

	return result
}

// maskKeyValuePair masks key=value patterns
func maskKeyValuePair(query, key string) string {
	result := query
	offset := 0

	for {
		// Look for key= or key:= pattern
		pattern := key + "="
		idx := strings.Index(strings.ToLower(result[offset:]), pattern)
		if idx == -1 {
			// Try with spaces around =
			pattern = key + " = "
			idx = strings.Index(strings.ToLower(result[offset:]), pattern)
		}
		if idx == -1 {
			break
		}
		idx += offset

		pos := idx + len(pattern)

		// Find end of value (space, comma, or end of string)
		start := pos
		for pos < len(result) && result[pos] != ' ' && result[pos] != ',' && result[pos] != ';' {
			pos++
		}

		if pos > start {
			// Mask the value
			result = result[:start] + "***" + result[pos:]
		}

		offset = idx + len(pattern)
	}

	return result
}

// maskMetadataValues masks values in metadata whose keys match sensitive patterns
func maskMetadataValues(metadata map[string]interface{}) map[string]interface{} {
	if metadata == nil {
		return nil
	}
	sensitiveKeys := []string{"password", "secret", "token", "key", "credential", "auth"}
	masked := make(map[string]interface{}, len(metadata))
	for k, v := range metadata {
		keyLower := strings.ToLower(k)
		isSensitive := false
		for _, sk := range sensitiveKeys {
			if strings.Contains(keyLower, sk) {
				isSensitive = true
				break
			}
		}
		if isSensitive {
			masked[k] = "***MASKED***"
		} else {
			masked[k] = v
		}
	}
	return masked
}

func generateEventID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Unix())
	}
	return fmt.Sprintf("%d-%x", time.Now().UnixNano(), b)
}

// LogOption is a functional option for logging
type LogOption func(*Event)

func WithQuery(query string) LogOption {
	return func(e *Event) {
		e.Query = query
	}
}

func WithTable(table string) LogOption {
	return func(e *Event) {
		e.Table = table
	}
}

func WithDatabase(db string) LogOption {
	return func(e *Event) {
		e.Database = db
	}
}

func WithClientIP(ip string) LogOption {
	return func(e *Event) {
		e.ClientIP = ip
	}
}

func WithDuration(d time.Duration) LogOption {
	return func(e *Event) {
		e.Duration = d
	}
}

func WithRowsAffected(n int64) LogOption {
	return func(e *Event) {
		e.RowsAffected = n
	}
}

func WithError(err error) LogOption {
	return func(e *Event) {
		if err != nil {
			e.Error = err.Error()
			e.Status = "ERROR"
		}
	}
}

func WithMetadata(key string, value interface{}) LogOption {
	return func(e *Event) {
		if e.Metadata == nil {
			e.Metadata = make(map[string]interface{})
		}
		e.Metadata[key] = value
	}
}

// LogQuery logs a query event
func (al *Logger) LogQuery(user, query string, duration time.Duration, rowsAffected int64, err error) {
	opts := []LogOption{
		WithQuery(query),
		WithDuration(duration),
		WithRowsAffected(rowsAffected),
	}
	if err != nil {
		opts = append(opts, WithError(err))
	}
	al.Log(EventQuery, user, "EXECUTE", opts...)
}

// LogDDL logs a DDL event
func (al *Logger) LogDDL(user, action, query string, err error) {
	opts := []LogOption{WithQuery(query)}
	if err != nil {
		opts = append(opts, WithError(err))
	}
	al.Log(EventDDL, user, action, opts...)
}

// LogAuth logs an authentication event
func (al *Logger) LogAuth(user, action string, success bool, clientIP string, err error) {
	opts := []LogOption{WithClientIP(clientIP)}
	if !success {
		opts = append(opts, WithError(fmt.Errorf("authentication failed")))
	}
	if err != nil {
		opts = append(opts, WithError(err))
	}
	al.Log(EventAuth, user, action, opts...)
}

// LogConnection logs a connection event
func (al *Logger) LogConnection(clientIP, action string, err error) {
	opts := []LogOption{WithClientIP(clientIP)}
	if err != nil {
		opts = append(opts, WithError(err))
	}
	al.Log(EventConnection, "system", action, opts...)
}

// LogSecurity logs a security event
func (al *Logger) LogSecurity(user, action, clientIP string, metadata map[string]interface{}) {
	opts := []LogOption{WithClientIP(clientIP)}
	for k, v := range metadata {
		opts = append(opts, WithMetadata(k, v))
	}
	al.Log(EventSecurity, user, action, opts...)
}

// Close closes the audit logger
func (al *Logger) Close() error {
	if !al.config.Enabled {
		return nil
	}

	var err error
	al.closeOnce.Do(func() {
		al.closeMu.Lock()
		al.closed = true
		al.closeMu.Unlock()

		close(al.stopChan)
		al.wg.Wait()

		al.mu.Lock()
		defer al.mu.Unlock()

		if al.file != nil {
			err = al.file.Close()
		}
	})
	return err
}

// Rotate manually rotates the log file
func (al *Logger) Rotate() error {
	al.mu.Lock()
	defer al.mu.Unlock()

	if al.file == nil {
		return nil
	}

	if err := al.file.Close(); err != nil {
		return fmt.Errorf("failed to close log file for rotation: %w", err)
	}
	al.file = nil

	timestamp := time.Now().Format("20060102_150405")
	backupName := al.config.LogFile + "." + timestamp
	if err := os.Rename(al.config.LogFile, backupName); err != nil {
		// Rename failed — reopen the original file so logging can continue
		_ = al.openLogFile()
		return fmt.Errorf("failed to rename log file: %w", err)
	}

	return al.openLogFile()
}
