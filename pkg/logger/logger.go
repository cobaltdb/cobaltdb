// Package logger provides structured logging for CobaltDB
package logger

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
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

// Logger provides structured logging
type Logger struct {
	level     Level
	output    io.Writer
	mu        sync.Mutex
	fields    map[string]interface{}
	component string
}

// New creates a new logger with the given level and output
func New(level Level, output io.Writer) *Logger {
	if output == nil {
		output = os.Stdout
	}
	return &Logger{
		level:  level,
		output: output,
		fields: make(map[string]interface{}),
	}
}

// Default creates a new logger with default settings
func Default() *Logger {
	return New(InfoLevel, os.Stdout)
}

// WithComponent returns a new logger with the given component name
func (l *Logger) WithComponent(component string) *Logger {
	return &Logger{
		level:     l.level,
		output:    l.output,
		fields:    copyFields(l.fields),
		component: component,
	}
}

// WithField returns a new logger with the given field
func (l *Logger) WithField(key string, value interface{}) *Logger {
	newFields := copyFields(l.fields)
	newFields[key] = value
	return &Logger{
		level:     l.level,
		output:    l.output,
		fields:    newFields,
		component: l.component,
	}
}

// WithFields returns a new logger with the given fields
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	newFields := copyFields(l.fields)
	for k, v := range fields {
		newFields[k] = v
	}
	return &Logger{
		level:     l.level,
		output:    l.output,
		fields:    newFields,
		component: l.component,
	}
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
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

	// Build log entry using strings.Builder for better performance
	var sb strings.Builder
	sb.Grow(128) // Pre-allocate reasonable size

	sb.WriteByte('[')
	sb.WriteString(timestamp)
	sb.WriteString("] ")
	sb.WriteString(level.String())

	if l.component != "" {
		sb.WriteString(" [")
		sb.WriteString(l.component)
		sb.WriteByte(']')
	}

	sb.WriteByte(' ')
	sb.WriteString(msg)

	// Add error if present
	if err != nil {
		sb.WriteString(" | error=")
		sb.WriteString(err.Error())
	}

	// Add fields
	for k, v := range l.fields {
		sb.WriteString(" | ")
		sb.WriteString(k)
		sb.WriteByte('=')
		fmt.Fprintf(&sb, "%v", v)
	}

	sb.WriteByte('\n')

	// Write to output
	fmt.Fprint(l.output, sb.String())
}

func copyFields(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// Global logger instance
var globalLogger = Default()

// SetGlobalLogger sets the global logger
func SetGlobalLogger(l *Logger) {
	globalLogger = l
}

// GetGlobalLogger returns the global logger
func GetGlobalLogger() *Logger {
	return globalLogger
}

// Global logging functions

// Debug uses the global logger
func Debug(msg string) { globalLogger.Debug(msg) }

// Debugf uses the global logger
func Debugf(format string, args ...interface{}) { globalLogger.Debugf(format, args...) }

// Info uses the global logger
func Info(msg string) { globalLogger.Info(msg) }

// Infof uses the global logger
func Infof(format string, args ...interface{}) { globalLogger.Infof(format, args...) }

// Warn uses the global logger
func Warn(msg string) { globalLogger.Warn(msg) }

// Warnf uses the global logger
func Warnf(format string, args ...interface{}) { globalLogger.Warnf(format, args...) }

// Error uses the global logger
func Error(msg string) { globalLogger.Error(msg) }

// Errorf uses the global logger
func Errorf(format string, args ...interface{}) { globalLogger.Errorf(format, args...) }

// Fatal uses the global logger
func Fatal(msg string) { globalLogger.Fatal(msg) }

// Fatalf uses the global logger
func Fatalf(format string, args ...interface{}) { globalLogger.Fatalf(format, args...) }
