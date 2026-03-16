package logger

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestLoggerAllLevels tests all logging levels
func TestLoggerAllLevels(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.Debug("debug message")
	log.Info("info message")
	log.Warn("warn message")
	log.Error("error message")

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

// TestLoggerLevelString tests level string representation
func TestLoggerLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{DebugLevel, "DEBUG"},
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{FatalLevel, "FATAL"},
		{Level(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.expected {
			t.Errorf("Level(%d).String() = %v, want %v", tt.level, got, tt.expected)
		}
	}
}

// TestLoggerParseLevel tests level parsing
func TestLoggerParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"DEBUG", DebugLevel},
		{"INFO", InfoLevel},
		{"WARN", WarnLevel},
		{"ERROR", ErrorLevel},
		{"FATAL", FatalLevel},
		{"unknown", InfoLevel},
		{"", InfoLevel},
	}

	for _, tt := range tests {
		got := ParseLevel(tt.input)
		if got != tt.expected {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

// TestLoggerWithErrorParam tests logging with error parameter
func TestLoggerWithErrorParam(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)
	testErr := errors.New("test error")

	log.Log(ErrorLevel, "error occurred", testErr)

	output := buf.String()
	if !strings.Contains(output, "error occurred") {
		t.Errorf("Expected 'error occurred' in output, got: %s", output)
	}
	if !strings.Contains(output, "error=test error") {
		t.Errorf("Expected error in output, got: %s", output)
	}
}

// TestLoggerWithNilError tests logging with nil error
func TestLoggerWithNilError(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.Log(InfoLevel, "no error", nil)

	output := buf.String()
	if !strings.Contains(output, "no error") {
		t.Errorf("Expected 'no error' in output, got: %s", output)
	}
	if strings.Contains(output, "error=") {
		t.Error("Should not contain error= when error is nil")
	}
}

// TestLoggerLogAtDifferentLevels tests Log method at all levels
func TestLoggerLogAtDifferentLevels(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.Log(DebugLevel, "debug log", nil)
	log.Log(InfoLevel, "info log", nil)
	log.Log(WarnLevel, "warn log", nil)
	log.Log(ErrorLevel, "error log", nil)
	log.Log(FatalLevel, "fatal log", nil)

	output := buf.String()
	for _, level := range []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"} {
		if !strings.Contains(output, level) {
			t.Errorf("Expected %s in output", level)
		}
	}
}

// TestLoggerLevelFilteringAllCombinations tests filtering at each level
func TestLoggerLevelFilteringAllCombinations(t *testing.T) {
	tests := []struct {
		setLevel  Level
		logLevel  Level
		shouldLog bool
	}{
		{DebugLevel, DebugLevel, true},
		{DebugLevel, InfoLevel, true},
		{DebugLevel, ErrorLevel, true},
		{InfoLevel, DebugLevel, false},
		{InfoLevel, InfoLevel, true},
		{InfoLevel, WarnLevel, true},
		{WarnLevel, InfoLevel, false},
		{WarnLevel, WarnLevel, true},
		{WarnLevel, ErrorLevel, true},
		{ErrorLevel, WarnLevel, false},
		{ErrorLevel, ErrorLevel, true},
		{FatalLevel, ErrorLevel, false},
		{FatalLevel, FatalLevel, true},
	}

	for _, tt := range tests {
		var buf bytes.Buffer
		log := New(tt.setLevel, &buf)
		log.Log(tt.logLevel, "test message", nil)

		hasMessage := strings.Contains(buf.String(), "test message")
		if hasMessage != tt.shouldLog {
			t.Errorf("Level %v logging at %v: expected log=%v, got log=%v",
				tt.setLevel, tt.logLevel, tt.shouldLog, hasMessage)
		}
	}
}

// TestLoggerIsEnabledAllLevels tests IsEnabled for all level combinations
func TestLoggerIsEnabledAllLevels(t *testing.T) {
	tests := []struct {
		loggerLevel Level
		checkLevel  Level
		expected    bool
	}{
		{DebugLevel, DebugLevel, true},
		{DebugLevel, FatalLevel, true},
		{InfoLevel, DebugLevel, false},
		{InfoLevel, InfoLevel, true},
		{WarnLevel, InfoLevel, false},
		{WarnLevel, WarnLevel, true},
		{ErrorLevel, WarnLevel, false},
		{ErrorLevel, ErrorLevel, true},
		{FatalLevel, ErrorLevel, false},
		{FatalLevel, FatalLevel, true},
	}

	for _, tt := range tests {
		log := New(tt.loggerLevel, nil)
		got := log.IsEnabled(tt.checkLevel)
		if got != tt.expected {
			t.Errorf("IsEnabled: logger=%v, check=%v, expected=%v, got=%v",
				tt.loggerLevel, tt.checkLevel, tt.expected, got)
		}
	}
}

// TestLoggerConcurrentLogging tests concurrent access to logger
func TestLoggerConcurrentLogging(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	var wg sync.WaitGroup
	numGoroutines := 10
	numLogs := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numLogs; j++ {
				log.Infof("goroutine %d log %d", id, j)
			}
		}(i)
	}

	wg.Wait()

	output := buf.String()
	lines := strings.Split(output, "\n")
	// Count non-empty lines
	count := 0
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}

	expected := numGoroutines * numLogs
	if count != expected {
		t.Errorf("Expected %d log lines, got %d", expected, count)
	}
}

// TestLoggerConcurrentSetLevel tests concurrent level changes
func TestLoggerConcurrentSetLevel(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrent logging
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				log.Info("log message")
			}
		}(i)
	}

	// Concurrent level changes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if j%2 == 0 {
					log.SetLevel(DebugLevel)
				} else {
					log.SetLevel(ErrorLevel)
				}
			}
		}(i)
	}

	wg.Wait()
	// Should complete without race conditions
}

// TestLoggerConcurrentSetOutput tests concurrent output changes
func TestLoggerConcurrentSetOutput(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	log := New(DebugLevel, &buf1)

	var wg sync.WaitGroup

	// Concurrent logging
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				log.Info("log message")
			}
		}(i)
	}

	// Concurrent output changes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if j%2 == 0 {
					log.SetOutput(&buf1)
				} else {
					log.SetOutput(&buf2)
				}
			}
		}(i)
	}

	wg.Wait()
	// Should complete without race conditions
}

// TestLoggerWithFieldsChaining tests field chaining
func TestLoggerWithFieldsChaining(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log1 := log.WithField("key1", "value1")
	log2 := log1.WithField("key2", "value2")
	log3 := log2.WithFields(map[string]interface{}{
		"key3": "value3",
		"key4": "value4",
	})

	log3.Info("test message")

	output := buf.String()
	if !strings.Contains(output, "key1=value1") {
		t.Error("Expected key1 in output")
	}
	if !strings.Contains(output, "key2=value2") {
		t.Error("Expected key2 in output")
	}
	if !strings.Contains(output, "key3=value3") {
		t.Error("Expected key3 in output")
	}
	if !strings.Contains(output, "key4=value4") {
		t.Error("Expected key4 in output")
	}

	// Original logger should not have these fields
	buf.Reset()
	log.Info("original")
	if strings.Contains(buf.String(), "key1=") {
		t.Error("Original logger should not have key1")
	}
}

// TestLoggerWithComponentChaining tests component chaining
func TestLoggerWithComponentChaining(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log1 := log.WithComponent("component1")
	log2 := log1.WithComponent("component2")

	log2.Info("test")

	output := buf.String()
	if !strings.Contains(output, "[component2]") {
		t.Errorf("Expected component2 in output, got: %s", output)
	}
	if strings.Contains(output, "[component1]") {
		t.Error("Should not contain component1")
	}
}

// TestLoggerDefaultOutput tests default output is stdout
func TestLoggerDefaultOutput(t *testing.T) {
	log := New(InfoLevel, nil)
	if log.output == nil {
		t.Error("Expected non-nil output")
	}
}

// TestLoggerDefaultFunction tests Default() function
func TestLoggerDefaultFunction(t *testing.T) {
	log := Default()
	if log == nil {
		t.Fatal("Expected non-nil logger")
	}
	if log.level != InfoLevel {
		t.Errorf("Expected InfoLevel, got %v", log.level)
	}
}

// TestLoggerCopyFieldsIndependence tests that copied fields are independent
func TestLoggerCopyFieldsIndependence(t *testing.T) {
	src := map[string]interface{}{
		"a": 1,
		"b": "two",
	}
	dst := copyFields(src)

	// Modify dst
	dst["c"] = 3
	dst["a"] = 999

	// src should be unchanged
	if src["a"] != 1 {
		t.Error("src['a'] should be unchanged")
	}
	if _, ok := src["c"]; ok {
		t.Error("src should not have key 'c'")
	}
}

// TestLoggerCopyFieldsEmpty tests copying empty fields
func TestLoggerCopyFieldsEmpty(t *testing.T) {
	src := map[string]interface{}{}
	dst := copyFields(src)

	if len(dst) != 0 {
		t.Errorf("Expected empty dst, got %d items", len(dst))
	}

	// Modifying dst should not affect src
	dst["new"] = "value"
	if len(src) != 0 {
		t.Error("src should still be empty")
	}
}

// TestLoggerCopyFieldsNil tests copying nil fields
func TestLoggerCopyFieldsNil(t *testing.T) {
	dst := copyFields(nil)
	if len(dst) != 0 {
		t.Errorf("Expected empty dst for nil input, got %d items", len(dst))
	}
}

// TestLoggerFieldTypes tests various field value types
func TestLoggerFieldTypes(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.WithField("string", "text").
		WithField("int", 42).
		WithField("float", 3.14).
		WithField("bool", true).
		WithField("nil", nil).
		Info("test")

	output := buf.String()
	if !strings.Contains(output, "string=text") {
		t.Error("Expected string field")
	}
	if !strings.Contains(output, "int=42") {
		t.Error("Expected int field")
	}
	if !strings.Contains(output, "float=3.14") {
		t.Error("Expected float field")
	}
	if !strings.Contains(output, "bool=true") {
		t.Error("Expected bool field")
	}
	if !strings.Contains(output, "nil=") {
		t.Error("Expected nil field")
	}
}

// TestLoggerTimestampFormat tests timestamp is present
func TestLoggerTimestampFormat(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.Info("test")

	output := buf.String()
	// Check for ISO 8601 format: YYYY-MM-DDTHH:MM:SS
	if !strings.Contains(output, "T") {
		t.Error("Expected timestamp with T separator")
	}
	if !strings.Contains(output, "Z") {
		t.Error("Expected timestamp with Z suffix")
	}
}

// TestLoggerNewWithNilOutput tests New with nil output
func TestLoggerNewWithNilOutput(t *testing.T) {
	log := New(InfoLevel, nil)
	if log.output == nil {
		t.Error("Expected non-nil output (should default to stdout)")
	}
}

// TestLoggerSetLevelConcurrentWithLog tests setting level while logging
func TestLoggerSetLevelConcurrentWithLog(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	done := make(chan bool)

	// Logger goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			log.Debug("message")
		}
		done <- true
	}()

	// Level setter goroutine
	go func() {
		for i := 0; i < 100; i++ {
			log.SetLevel(InfoLevel)
			log.SetLevel(DebugLevel)
		}
		done <- true
	}()

	<-done
	<-done
}

// TestGlobalLoggerGetNilFallback tests GetGlobalLogger nil fallback
func TestGlobalLoggerGetNilFallback(t *testing.T) {
	// Save current
	original := globalLogger.Load()
	defer globalLogger.Store(original)

	// Set to nil
	globalLogger.Store(nil)

	// Get should return fallback
	log := GetGlobalLogger()
	if log == nil {
		t.Fatal("Expected non-nil logger from fallback")
	}

	// Verify it works
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.Info("test")
	if !strings.Contains(buf.String(), "test") {
		t.Error("Expected fallback logger to work")
	}
}

// TestLoggerWriteError tests behavior when write fails
func TestLoggerWriteError(t *testing.T) {
	failingWriter := &failingWriter{failAfter: 0}
	log := New(DebugLevel, failingWriter)

	// Should not panic even if write fails
	log.Info("test message")
}

// TestLoggerWriteErrorAfterN tests write failing after N writes
func TestLoggerWriteErrorAfterN(t *testing.T) {
	failingWriter := &failingWriter{failAfter: 2}
	log := New(DebugLevel, failingWriter)

	// First two should succeed
	log.Info("message 1")
	log.Info("message 2")

	// This one will fail but should not panic
	log.Info("message 3")
}

type failingWriter struct {
	failAfter int
	writes    int
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	w.writes++
	if w.writes > w.failAfter {
		return 0, errors.New("write failed")
	}
	return len(p), nil
}

// TestLoggerLargeMessage tests logging large messages
func TestLoggerLargeMessage(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	largeMsg := strings.Repeat("x", 10000)
	log.Info(largeMsg)

	output := buf.String()
	if !strings.Contains(output, largeMsg) {
		t.Error("Expected large message in output")
	}
}

// TestLoggerManyFields tests logging with many fields
func TestLoggerManyFields(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	fields := make(map[string]interface{})
	for i := 0; i < 100; i++ {
		fields[string(rune('a'+i%26))+string(rune('0'+i/26))] = i
	}

	log.WithFields(fields).Info("test")

	output := buf.String()
	for i := 0; i < 100; i++ {
		key := string(rune('a'+i%26)) + string(rune('0'+i/26))
		if !strings.Contains(output, key+"=") {
			t.Errorf("Expected field %s in output", key)
		}
	}
}

// TestLoggerEmptyMessage tests logging empty message
func TestLoggerEmptyMessage(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.Info("")

	output := buf.String()
	if !strings.Contains(output, "INFO") {
		t.Error("Expected level in output")
	}
}

// TestLoggerSpecialCharacters tests logging special characters
func TestLoggerSpecialCharacters(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.Info("message with special chars: \n\t\r\"'\\")
	log.WithField("key", "value with | = chars").Info("test")

	output := buf.String()
	if !strings.Contains(output, "message with special chars") {
		t.Error("Expected message with special chars")
	}
}

// TestLoggerUnicode tests logging unicode characters
func TestLoggerUnicode(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.Info("unicode: 你好世界 🌍 émojis")
	log.WithField("emoji", "🚀").Info("test")

	output := buf.String()
	if !strings.Contains(output, "你好世界") {
		t.Error("Expected unicode characters")
	}
}

// TestLoggerComponentWithFields tests component combined with fields
func TestLoggerComponentWithFields(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.WithComponent("test-comp").
		WithField("key", "value").
		Info("message")

	output := buf.String()
	if !strings.Contains(output, "[test-comp]") {
		t.Error("Expected component")
	}
	if !strings.Contains(output, "key=value") {
		t.Error("Expected field")
	}
}

// TestLoggerFormatStrings tests various format strings
func TestLoggerFormatStrings(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.Debugf("int: %d", 42)
	log.Infof("string: %s", "test")
	log.Warnf("float: %.2f", 3.14159)
	log.Errorf("bool: %t", true)
	log.Infof("mixed: %s %d %v", "a", 1, true)

	output := buf.String()
	if !strings.Contains(output, "int: 42") {
		t.Error("Expected formatted int")
	}
	if !strings.Contains(output, "string: test") {
		t.Error("Expected formatted string")
	}
	if !strings.Contains(output, "float: 3.14") {
		t.Error("Expected formatted float")
	}
	if !strings.Contains(output, "bool: true") {
		t.Error("Expected formatted bool")
	}
}

// TestLoggerOutputInterface tests that output interface is used correctly
func TestLoggerOutputInterface(t *testing.T) {
	custom := &customWriter{}
	log := New(DebugLevel, custom)

	log.Info("test message")

	if !custom.written {
		t.Error("Expected custom writer to be called")
	}
	if !strings.Contains(custom.data, "test message") {
		t.Error("Expected message in custom writer")
	}
}

type customWriter struct {
	data    string
	written bool
}

func (w *customWriter) Write(p []byte) (n int, err error) {
	w.data = string(p)
	w.written = true
	return len(p), nil
}

// TestLoggerBufferedWriter tests with buffered writer
func TestLoggerBufferedWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	log := New(DebugLevel, buf)

	for i := 0; i < 100; i++ {
		log.Infof("message %d", i)
	}

	output := buf.String()
	for i := 0; i < 100; i++ {
		expected := fmt.Sprintf("message %d", i)
		if !strings.Contains(output, expected) {
			t.Errorf("Expected %s in output", expected)
		}
	}
}

// TestLoggerWithFieldsMultipleCalls tests multiple WithFields calls
func TestLoggerWithFieldsMultipleCalls(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log1 := log.WithFields(map[string]interface{}{
		"a": 1,
		"b": 2,
	})
	log2 := log1.WithFields(map[string]interface{}{
		"c": 3,
		"d": 4,
	})

	log2.Info("test")

	output := buf.String()
	for _, key := range []string{"a=1", "b=2", "c=3", "d=4"} {
		if !strings.Contains(output, key) {
			t.Errorf("Expected %s in output", key)
		}
	}
}

// TestLoggerFieldOverwrite tests overwriting fields
func TestLoggerFieldOverwrite(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.WithField("key", "old").
		WithField("key", "new").
		Info("test")

	output := buf.String()
	// Should have the new value
	if !strings.Contains(output, "key=new") {
		t.Error("Expected new value")
	}
}

// TestLoggerChainedCalls tests chained method calls
func TestLoggerChainedCalls(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.WithComponent("comp").
		WithField("k1", "v1").
		WithFields(map[string]interface{}{"k2": "v2"}).
		WithField("k3", "v3").
		Info("chained")

	output := buf.String()
	if !strings.Contains(output, "[comp]") {
		t.Error("Expected component")
	}
	if !strings.Contains(output, "k1=v1") {
		t.Error("Expected k1")
	}
	if !strings.Contains(output, "k2=v2") {
		t.Error("Expected k2")
	}
	if !strings.Contains(output, "k3=v3") {
		t.Error("Expected k3")
	}
}

// TestLoggerRaceCondition tests for race conditions
func TestLoggerRaceCondition(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	var wg sync.WaitGroup

	// Multiple goroutines doing different operations
	for i := 0; i < 20; i++ {
		wg.Add(4)

		// Log
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				log.Info("message")
			}
		}()

		// SetLevel
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if id%2 == 0 {
					log.SetLevel(DebugLevel)
				} else {
					log.SetLevel(ErrorLevel)
				}
			}
		}(i)

		// SetOutput
		go func() {
			defer wg.Done()
			var b bytes.Buffer
			for j := 0; j < 50; j++ {
				log.SetOutput(&b)
			}
		}()

		// WithField (creates new loggers)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = log.WithField("key", j)
			}
		}()
	}

	wg.Wait()
}

// TestLoggerLogFunctionAtBoundary tests logging at level boundary
func TestLoggerLogFunctionAtBoundary(t *testing.T) {
	var buf bytes.Buffer
	log := New(InfoLevel, &buf)

	// Debug should not log
	log.Log(DebugLevel, "debug", nil)
	if strings.Contains(buf.String(), "debug") {
		t.Error("Debug should not be logged at InfoLevel")
	}

	// Info should log
	log.Log(InfoLevel, "info", nil)
	if !strings.Contains(buf.String(), "info") {
		t.Error("Info should be logged at InfoLevel")
	}
}

// TestLoggerErrorFieldFormatting tests error formatting in fields
func TestLoggerErrorFieldFormatting(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.WithField("err", errors.New("field error")).Info("test")

	output := buf.String()
	if !strings.Contains(output, "err=field error") {
		t.Errorf("Expected error in field, got: %s", output)
	}
}

// TestLoggerSetGlobalLoggerNil tests SetGlobalLogger with nil
func TestLoggerSetGlobalLoggerNil(t *testing.T) {
	original := GetGlobalLogger()
	defer SetGlobalLogger(original)

	SetGlobalLogger(nil)

	log := GetGlobalLogger()
	if log == nil {
		t.Error("Expected non-nil logger after SetGlobalLogger(nil)")
	}
}

// TestLoggerGetGlobalLoggerMultipleCalls tests multiple GetGlobalLogger calls
func TestLoggerGetGlobalLoggerMultipleCalls(t *testing.T) {
	original := GetGlobalLogger()
	defer SetGlobalLogger(original)

	// Multiple calls should return the same logger
	log1 := GetGlobalLogger()
	log2 := GetGlobalLogger()

	if log1 != log2 {
		t.Error("Multiple GetGlobalLogger calls should return same logger")
	}
}

// TestLoggerNewWithStdout tests New with default stdout
func TestLoggerNewWithStdout(t *testing.T) {
	log := New(DebugLevel, nil)
	if log.output == nil {
		t.Error("Expected non-nil output")
	}

	// Should be able to log without panic
	log.Info("test to stdout")
}

// TestLoggerLogWithAllLevelTypes tests Log method with all level types
func TestLoggerLogWithAllLevelTypes(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	levels := []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel, FatalLevel}
	for _, level := range levels {
		buf.Reset()
		log.Log(level, "test message", nil)
		output := buf.String()
		if !strings.Contains(output, level.String()) {
			t.Errorf("Expected %s in output for level %v", level.String(), level)
		}
	}
}

// TestLoggerComplexFieldTypes tests complex field types
func TestLoggerComplexFieldTypes(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	log.WithField("slice", []int{1, 2, 3}).
		WithField("map", map[string]int{"a": 1}).
		WithField("struct", struct{ Name string }{Name: "test"}).
		Info("complex types")

	output := buf.String()
	if !strings.Contains(output, "slice=") {
		t.Error("Expected slice field")
	}
	if !strings.Contains(output, "map=") {
		t.Error("Expected map field")
	}
	if !strings.Contains(output, "struct=") {
		t.Error("Expected struct field")
	}
}
