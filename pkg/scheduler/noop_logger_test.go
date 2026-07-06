package scheduler

import (
	"testing"
)

func TestNoopLogger(t *testing.T) {
	// Verify that the noopLogger methods exist and don't panic
	var nl noopLogger

	// These should be safe no-ops
	nl.Infof("test format %d", 42)
	nl.Warnf("test warn %s", "message")
	nl.Errorf("test error %v", nil)
}
