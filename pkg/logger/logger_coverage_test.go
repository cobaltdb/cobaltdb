package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestLoggerWithFields(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)

	child := log.WithFields(map[string]interface{}{
		"key1": "val1",
		"key2": 42,
	})
	child.Info("test msg")

	output := buf.String()
	if !strings.Contains(output, "test msg") {
		t.Errorf("expected 'test msg' in output, got: %s", output)
	}
}

func TestLoggerSetOutput(t *testing.T) {
	var buf bytes.Buffer
	log := New(InfoLevel, &buf)
	log.Info("hello")

	if !strings.Contains(buf.String(), "hello") {
		t.Errorf("expected 'hello' in buffer, got: %s", buf.String())
	}

	// Change output
	var buf2 bytes.Buffer
	log.SetOutput(&buf2)
	log.Info("second")
	if !strings.Contains(buf2.String(), "second") {
		t.Error("expected 'second' in new buffer")
	}
}

func TestLoggerDebugf(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)
	log.Debugf("value=%d", 42)

	if !strings.Contains(buf.String(), "value=42") {
		t.Errorf("expected 'value=42' in output")
	}
}

func TestLoggerWarnAndWarnf(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)
	log.Warn("warning msg")
	log.Warnf("warning %s", "formatted")

	output := buf.String()
	if !strings.Contains(output, "warning msg") {
		t.Error("expected 'warning msg'")
	}
	if !strings.Contains(output, "warning formatted") {
		t.Error("expected 'warning formatted'")
	}
}

func TestLoggerErrorAndErrorf(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)
	log.Error("error msg")
	log.Errorf("error %d", 500)

	output := buf.String()
	if !strings.Contains(output, "error msg") {
		t.Error("expected 'error msg'")
	}
	if !strings.Contains(output, "error 500") {
		t.Error("expected 'error 500'")
	}
}

func TestGlobalLoggerSetAndGet(t *testing.T) {
	original := GetGlobalLogger()
	if original == nil {
		t.Fatal("expected non-nil global logger")
	}

	var buf bytes.Buffer
	custom := New(WarnLevel, &buf)
	SetGlobalLogger(custom)

	got := GetGlobalLogger()
	if got != custom {
		t.Error("expected custom logger")
	}

	// Nil resets to default
	SetGlobalLogger(nil)
	got = GetGlobalLogger()
	if got == nil {
		t.Error("expected non-nil default after nil set")
	}

	// Restore original
	SetGlobalLogger(original)
}

func TestGlobalDebugAndDebugf(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)
	SetGlobalLogger(log)
	defer SetGlobalLogger(nil)

	Debug("global debug")
	Debugf("global debugf %d", 1)

	output := buf.String()
	if !strings.Contains(output, "global debug") {
		t.Error("expected 'global debug'")
	}
	if !strings.Contains(output, "global debugf 1") {
		t.Error("expected 'global debugf 1'")
	}
}

func TestGlobalInfof(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)
	SetGlobalLogger(log)
	defer SetGlobalLogger(nil)

	Infof("info %s", "test")
	if !strings.Contains(buf.String(), "info test") {
		t.Error("expected 'info test'")
	}
}

func TestGlobalWarnAndWarnf(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)
	SetGlobalLogger(log)
	defer SetGlobalLogger(nil)

	Warn("gwarn")
	Warnf("gwarnf %d", 2)

	output := buf.String()
	if !strings.Contains(output, "gwarn") {
		t.Error("expected 'gwarn'")
	}
	if !strings.Contains(output, "gwarnf 2") {
		t.Error("expected 'gwarnf 2'")
	}
}

func TestGlobalErrorAndErrorf(t *testing.T) {
	var buf bytes.Buffer
	log := New(DebugLevel, &buf)
	SetGlobalLogger(log)
	defer SetGlobalLogger(nil)

	Error("gerr")
	Errorf("gerrf %d", 3)

	output := buf.String()
	if !strings.Contains(output, "gerr") {
		t.Error("expected 'gerr'")
	}
	if !strings.Contains(output, "gerrf 3") {
		t.Error("expected 'gerrf 3'")
	}
}

func TestCopyFieldsWithData(t *testing.T) {
	src := map[string]interface{}{
		"a": 1,
		"b": "two",
	}
	dst := copyFields(src)
	if len(dst) != 2 {
		t.Errorf("expected 2 fields, got %d", len(dst))
	}
	// Modifying dst shouldn't affect src
	dst["c"] = 3
	if _, ok := src["c"]; ok {
		t.Error("copy should be independent")
	}
}
