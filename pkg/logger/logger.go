// Package logger provides structured logging for CobaltDB
package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Level represents logging levels
type Level int

const (
	// DebugLevel is the most verbose logging level
	DebugLevel Level = iota
	// InfoLevel is for informational messages
	InfoLevel
	// WarnLevel is for warning messages
	WarnLevel
	// ErrorLevel is for error messages
	ErrorLevel
	// FatalLevel is for fatal errors that cause program termination
	FatalLevel
)

// String returns the string representation of a log level
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel parses a level string into a Level
func ParseLevel(s string) Level {
	switch s {
	case "DEBUG":
		return DebugLevel
	case "INFO":
		return InfoLevel
	case "WARN":
		return WarnLevel
	case "ERROR":
		return ErrorLevel
	case "FATAL":
		return FatalLevel
	default:
		return InfoLevel
	}
}

// Format controls how log entries are encoded.
type Format int

const (
	// TextFormat preserves the legacy human-readable log layout.
	TextFormat Format = iota
	// JSONFormat emits one JSON object per line.
	JSONFormat
)

// String returns the configuration name for a log format.
func (f Format) String() string {
	switch f {
	case TextFormat:
		return "text"
	case JSONFormat:
		return "json"
	default:
		return "unknown"
	}
}

// ParseFormat parses a case-insensitive format name.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "text":
		return TextFormat, nil
	case "json":
		return JSONFormat, nil
	default:
		return TextFormat, fmt.Errorf("unsupported log format %q (want text or json)", s)
	}
}

// Logger provides structured logging
type Logger struct {
	level  Level
	format Format
	output io.Writer
	// mu guards the logger's own state (level, output, fields).
	mu     sync.RWMutex
	fields map[string]interface{}
	// outMu serializes writes to the (shared) output writer. It is a pointer
	// shared with every logger derived via WithComponent/WithField/WithFields
	// so that a parent and its derived loggers never interleave bytes on the
	// same writer, which independent per-logger mutexes cannot guarantee.
	outMu     *sync.Mutex
	component string
}

// New creates a text logger with the given level and output. A nil output
// preserves the legacy behavior of writing to stdout.
func New(level Level, output io.Writer) *Logger {
	return NewWithFormat(level, output, TextFormat)
}

// NewWithFormat creates a logger with the requested output format.
func NewWithFormat(level Level, output io.Writer, format Format) *Logger {
	if output == nil {
		output = os.Stdout
	}
	if format != JSONFormat {
		format = TextFormat
	}
	return &Logger{
		level:  level,
		format: format,
		output: output,
		fields: make(map[string]interface{}),
		outMu:  &sync.Mutex{},
	}
}

// Default creates a new text logger with default settings.
func Default() *Logger {
	return New(InfoLevel, os.Stdout)
}

// WithComponent returns a new logger with the given component name
func (l *Logger) WithComponent(component string) *Logger {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return &Logger{
		level:     l.level,
		format:    l.format,
		output:    l.output,
		fields:    copyFields(l.fields),
		outMu:     l.sharedOutputMu(),
		component: component,
	}
}

// WithField returns a new logger with the given field
func (l *Logger) WithField(key string, value interface{}) *Logger {
	l.mu.RLock()
	defer l.mu.RUnlock()
	newFields := copyFields(l.fields)
	newFields[key] = value
	return &Logger{
		level:     l.level,
		format:    l.format,
		output:    l.output,
		fields:    newFields,
		outMu:     l.sharedOutputMu(),
		component: l.component,
	}
}

// WithFields returns a new logger with the given fields
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	l.mu.RLock()
	defer l.mu.RUnlock()
	newFields := copyFields(l.fields)
	for k, v := range fields {
		newFields[k] = v
	}
	return &Logger{
		level:     l.level,
		format:    l.format,
		output:    l.output,
		fields:    newFields,
		outMu:     l.sharedOutputMu(),
		component: l.component,
	}
}

// fallbackOutputMu serializes writes for zero-value Loggers that were not
// created via New (and therefore have no outMu). All such loggers share this
// single mutex, which is safe (if coarser than necessary) and avoids mutating
// the logger, so it can be called while l.mu is held in any mode.
var fallbackOutputMu sync.Mutex

// sharedOutputMu returns the logger's output mutex. It never locks or
// mutates the logger (callers may hold l.mu in read mode).
func (l *Logger) sharedOutputMu() *sync.Mutex {
	if l.outMu != nil {
		return l.outMu
	}
	return &fallbackOutputMu
}

// SetLevel sets the logging level
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// SetOutput sets the output writer
func (l *Logger) SetOutput(output io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = output
}

// IsEnabled returns true if the given level is enabled
func (l *Logger) IsEnabled(level Level) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return level >= l.level
}

// Debug logs a debug message
func (l *Logger) Debug(msg string) {
	l.log(DebugLevel, msg, nil)
}

// Debugf logs a formatted debug message
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log(DebugLevel, fmt.Sprintf(format, args...), nil)
}

// Info logs an info message
func (l *Logger) Info(msg string) {
	l.log(InfoLevel, msg, nil)
}

// Infof logs a formatted info message
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log(InfoLevel, fmt.Sprintf(format, args...), nil)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string) {
	l.log(WarnLevel, msg, nil)
}

// Warnf logs a formatted warning message
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log(WarnLevel, fmt.Sprintf(format, args...), nil)
}

// Error logs an error message
func (l *Logger) Error(msg string) {
	l.log(ErrorLevel, msg, nil)
}

// Errorf logs a formatted error message
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log(ErrorLevel, fmt.Sprintf(format, args...), nil)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(msg string) {
	l.log(FatalLevel, msg, nil)
	os.Exit(1)
}

// Fatalf logs a formatted fatal message and exits
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.log(FatalLevel, fmt.Sprintf(format, args...), nil)
	os.Exit(1)
}

// Log logs a message with error
func (l *Logger) Log(level Level, msg string, err error) {
	l.log(level, msg, err)
}

func (l *Logger) log(level Level, msg string, err error) {
	// Snapshot state under the read lock; the actual write is serialized by
	// the output mutex shared with derived loggers.
	l.mu.RLock()
	if level < l.level {
		l.mu.RUnlock()
		return
	}

	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	var line []byte
	if l.format == JSONFormat {
		line = renderJSON(timestamp, level, msg, l.component, err, l.fields)
	} else {
		line = renderText(timestamp, level, msg, l.component, err, l.fields)
	}

	output := l.output
	if output == nil {
		// Zero-value Loggers (not created via New) have no configured writer.
		// Preserve NewWithFormat's documented nil-output behavior of writing
		// to stdout instead of panicking on the nil interface.
		output = os.Stdout
	}
	outMu := l.outMu
	l.mu.RUnlock()

	// Write to output under the shared writer mutex so lines from this
	// logger and any derived loggers never interleave.
	if outMu == nil {
		outMu = l.sharedOutputMu()
	}
	outMu.Lock()
	_, _ = output.Write(line)
	outMu.Unlock()
}

func renderText(timestamp string, level Level, msg, component string, err error, fields map[string]interface{}) []byte {
	// Keep the legacy text layout byte-for-byte compatible.
	var sb strings.Builder
	sb.Grow(128)

	sb.WriteByte('[')
	sb.WriteString(timestamp)
	sb.WriteString("] ")
	sb.WriteString(level.String())

	if component != "" {
		sb.WriteString(" [")
		sb.WriteString(component)
		sb.WriteByte(']')
	}

	sb.WriteByte(' ')
	sb.WriteString(msg)

	if err != nil {
		sb.WriteString(" | error=")
		sb.WriteString(err.Error())
	}

	for k, v := range fields {
		sb.WriteString(" | ")
		sb.WriteString(k)
		sb.WriteByte('=')
		fmt.Fprintf(&sb, "%v", v)
	}

	sb.WriteByte('\n')
	return []byte(sb.String())
}

type jsonEntry struct {
	Time      string                     `json:"time"`
	Level     string                     `json:"level"`
	Message   string                     `json:"msg"`
	Component string                     `json:"component,omitempty"`
	Error     string                     `json:"error,omitempty"`
	Fields    map[string]json.RawMessage `json:"fields,omitempty"`
}

func renderJSON(timestamp string, level Level, msg, component string, err error, fields map[string]interface{}) []byte {
	entry := jsonEntry{
		Time:      timestamp,
		Level:     level.String(),
		Message:   msg,
		Component: component,
		Fields:    safeJSONFields(fields),
	}
	if err != nil {
		entry.Error = err.Error()
	}

	line, marshalErr := json.Marshal(entry)
	if marshalErr != nil {
		// All dynamic values have already been converted to validated
		// RawMessages, so this can only fail if the fixed envelope changes.
		// Retain the JSON-lines contract even in that defensive case.
		line = []byte(`{"time":"","level":"ERROR","msg":"failed to encode log entry"}`)
	}
	return append(line, '\n')
}

// safeJSONFields keeps caller-provided keys below a reserved "fields" object
// and validates each value independently. Unsupported JSON values are rendered
// as strings rather than invalidating or dropping the entire log entry.
func safeJSONFields(fields map[string]interface{}) map[string]json.RawMessage {
	if len(fields) == 0 {
		return nil
	}

	safe := make(map[string]json.RawMessage, len(fields))
	for key, value := range fields {
		encoded, err := json.Marshal(value)
		if err != nil {
			encoded, err = json.Marshal(fmt.Sprint(value))
			if err != nil {
				encoded = []byte(`"<unprintable>"`)
			}
		}
		safe[key] = encoded
	}
	return safe
}

// Writer returns an io.Writer that records each write as an informational log
// entry. It is intended for adapting the standard library log package.
func (l *Logger) Writer() io.Writer {
	return loggerWriter{logger: l}
}

type loggerWriter struct {
	logger *Logger
}

func (w loggerWriter) Write(p []byte) (int, error) {
	if w.logger != nil {
		w.logger.Info(strings.TrimSuffix(string(p), "\n"))
	}
	return len(p), nil
}

func copyFields(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

var globalLogger atomic.Pointer[Logger]

func init() {
	globalLogger.Store(Default())
}

// SetGlobalLogger sets the global logger
func SetGlobalLogger(l *Logger) {
	if l == nil {
		l = Default()
	}
	globalLogger.Store(l)
}

// GetGlobalLogger returns the global logger
func GetGlobalLogger() *Logger {
	l := globalLogger.Load()
	if l == nil {
		fallback := Default()
		globalLogger.Store(fallback)
		return fallback
	}
	return l
}

// Global logging functions

// Debug uses the global logger
func Debug(msg string) { GetGlobalLogger().Debug(msg) }

// Debugf uses the global logger
func Debugf(format string, args ...interface{}) { GetGlobalLogger().Debugf(format, args...) }

// Info uses the global logger
func Info(msg string) { GetGlobalLogger().Info(msg) }

// Infof uses the global logger
func Infof(format string, args ...interface{}) { GetGlobalLogger().Infof(format, args...) }

// Warn uses the global logger
func Warn(msg string) { GetGlobalLogger().Warn(msg) }

// Warnf uses the global logger
func Warnf(format string, args ...interface{}) { GetGlobalLogger().Warnf(format, args...) }

// Error uses the global logger
func Error(msg string) { GetGlobalLogger().Error(msg) }

// Errorf uses the global logger
func Errorf(format string, args ...interface{}) { GetGlobalLogger().Errorf(format, args...) }

// Fatal uses the global logger
func Fatal(msg string) { GetGlobalLogger().Fatal(msg) }

// Fatalf uses the global logger
func Fatalf(format string, args ...interface{}) { GetGlobalLogger().Fatalf(format, args...) }
