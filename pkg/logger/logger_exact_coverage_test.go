package logger

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestZeroValueLoggerUsesFallbackOutputMutex(t *testing.T) {
	var buf bytes.Buffer
	log := &Logger{level: DebugLevel, output: &buf, fields: map[string]interface{}{}}
	if got := log.sharedOutputMu(); got != &fallbackOutputMu {
		t.Fatalf("zero-value output mutex = %p, want fallback %p", got, &fallbackOutputMu)
	}

	log.Log(ErrorLevel, "failed", errors.New("disk full"))
	if got := buf.String(); !strings.Contains(got, "failed | error=disk full") {
		t.Fatalf("log output = %q, want rendered error", got)
	}
}

func TestGetGlobalLoggerLazilyRestoresNilPointer(t *testing.T) {
	original := globalLogger.Load()
	t.Cleanup(func() { globalLogger.Store(original) })

	globalLogger.Store(nil)
	got := GetGlobalLogger()
	if got == nil {
		t.Fatal("GetGlobalLogger returned nil")
	}
	if globalLogger.Load() != got {
		t.Fatal("GetGlobalLogger did not persist its fallback")
	}
}
