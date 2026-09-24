package logger

import (
	"strings"
	"testing"
)

// TestZeroValueLoggerUsable pins the contract that a zero-value Logger — one
// never created via New/NewWithFormat — is usable for derivation and writing.
//
// Regression: fallbackOutputMu and sharedOutputMu explicitly support
// zero-value Loggers (the exact-coverage test pins the mutex derivation), but
// log() wrote to the nil output interface and panicked with a nil pointer
// dereference. The fix routes a nil output to os.Stdout, matching
// NewWithFormat's documented nil-output behavior.
func TestZeroValueLoggerUsable(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("zero-value logger panicked on write: %v", r)
		}
	}()

	var zero Logger
	derived := zero.WithField("component", "test")
	if got := derived.sharedOutputMu(); got != &fallbackOutputMu {
		t.Fatalf("derived zero-value logger output mutex = %p, want fallback %p", got, &fallbackOutputMu)
	}
	derived.Info("zero-value logger write") // pre-fix: panic (nil output)

	// SetOutput on a zero-value logger must keep working and take effect.
	var buf strings.Builder
	zero.SetOutput(&buf)
	zero.Info("zero-value setoutput write")
	if !strings.Contains(buf.String(), "zero-value setoutput write") {
		t.Fatalf("SetOutput on zero-value logger produced %q", buf.String())
	}
}
