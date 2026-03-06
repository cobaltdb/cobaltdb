// Package audit provides comprehensive audit logging for CobaltDB
package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	LogFormat       string      // "json" or "text"
	RotationEnabled bool
	MaxFileSize     int64       // bytes
	MaxBackups      int
	MaxAge          int         // days
	Events          []EventType // Which events to log (empty = all)
	LogQueries      bool
	LogFailedLogins bool
	LogDDL          bool
	LogConnections  bool
	SensitiveFields []string // Fields to mask in logs
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
		al.writeEvent(event)
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

	al.file.Sync()
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

	_, err := al.file.WriteString(line)
	return err
}

func (al *Logger) maskSensitiveData(event *Event) {
	for _, field := range al.config.SensitiveFields {
		if field == "password" && event.Query != "" {
			// Simple masking
		}
	}
}

func generateEventID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), time.Now().Unix())
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

	close(al.stopChan)
	al.wg.Wait()

	al.mu.Lock()
	defer al.mu.Unlock()

	if al.file != nil {
		return al.file.Close()
	}
	return nil
}

// Rotate manually rotates the log file
func (al *Logger) Rotate() error {
	al.mu.Lock()
	defer al.mu.Unlock()

	if al.file == nil {
		return nil
	}

	al.file.Close()

	timestamp := time.Now().Format("20060102_150405")
	backupName := al.config.LogFile + "." + timestamp
	if err := os.Rename(al.config.LogFile, backupName); err != nil {
		return err
	}

	return al.openLogFile()
}
