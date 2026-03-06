package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{DebugLevel, "DEBUG"},
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{FatalLevel, "FATAL"},
		{Level(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.level.String(); got != tt.expected {
			t.Errorf("Level.String() = %v, want %v", got, tt.expected)
		}
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"DEBUG", DebugLevel},
		{"INFO", InfoLevel},
		{"WARN", WarnLevel},
		{"ERROR", ErrorLevel},
		{"FATAL", FatalLevel},
		{"UNKNOWN", InfoLevel},
	}

	for _, tt := range tests {
		if got := ParseLevel(tt.input); got != tt.expected {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestLoggerLevels(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New(DebugLevel, buf)

	l.Debug("debug message")
	l.Info("info message")
	l.Warn("warn message")
	l.Error("error message")

	output := buf.String()

	if !strings.Contains(output, "debug message") {
		t.Error("Expected debug message in output")
	}
	if !strings.Contains(output, "info message") {
		t.Error("Expected info message in output")
	}
	if !strings.Contains(output, "warn message") {
		t.Error("Expected warn message in output")
	}
	if !strings.Contains(output, "error message") {
		t.Error("Expected error message in output")
	}
}

func TestLoggerLevelFiltering(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New(WarnLevel, buf)

	l.Debug("debug message")
	l.Info("info message")
	l.Warn("warn message")
	l.Error("error message")

	output := buf.String()

	if strings.Contains(output, "debug message") {
		t.Error("Did not expect debug message in output")
	}
	if strings.Contains(output, "info message") {
		t.Error("Did not expect info message in output")
	}
	if !strings.Contains(output, "warn message") {
		t.Error("Expected warn message in output")
	}
	if !strings.Contains(output, "error message") {
		t.Error("Expected error message in output")
	}
}

func TestLoggerWithField(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New(DebugLevel, buf)
	l = l.WithField("key", "value")
	l.Info("test message")

	output := buf.String()
	if !strings.Contains(output, "key=value") {
		t.Errorf("Expected field in output, got: %s", output)
	}
}

func TestLoggerWithComponent(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New(DebugLevel, buf)
	l = l.WithComponent("test-component")
	l.Info("test message")

	output := buf.String()
	if !strings.Contains(output, "[test-component]") {
		t.Errorf("Expected component in output, got: %s", output)
	}
}

func TestLoggerFormatted(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New(DebugLevel, buf)
	l.Infof("test %s %d", "message", 42)

	output := buf.String()
	if !strings.Contains(output, "test message 42") {
		t.Errorf("Expected formatted message in output, got: %s", output)
	}
}

func TestLoggerWithError(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New(DebugLevel, buf)
	l.Log(ErrorLevel, "error occurred", nil)

	output := buf.String()
	if !strings.Contains(output, "error occurred") {
		t.Errorf("Expected error message in output, got: %s", output)
	}
}

func TestIsEnabled(t *testing.T) {
	l := New(WarnLevel, nil)

	if l.IsEnabled(DebugLevel) {
		t.Error("Debug should not be enabled when level is Warn")
	}
	if l.IsEnabled(InfoLevel) {
		t.Error("Info should not be enabled when level is Warn")
	}
	if !l.IsEnabled(WarnLevel) {
		t.Error("Warn should be enabled when level is Warn")
	}
	if !l.IsEnabled(ErrorLevel) {
		t.Error("Error should be enabled when level is Warn")
	}
}

func TestSetLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New(DebugLevel, buf)

	l.Info("first")
	l.SetLevel(ErrorLevel)
	l.Info("second")

	output := buf.String()
	if !strings.Contains(output, "first") {
		t.Error("Expected first message")
	}
	if strings.Contains(output, "second") {
		t.Error("Did not expect second message after level change")
	}
}

func TestGlobalLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	oldLogger := GetGlobalLogger()
	SetGlobalLogger(New(InfoLevel, buf))
	defer SetGlobalLogger(oldLogger)

	Info("global test")

	output := buf.String()
	if !strings.Contains(output, "global test") {
		t.Errorf("Expected global test in output, got: %s", output)
	}
}
