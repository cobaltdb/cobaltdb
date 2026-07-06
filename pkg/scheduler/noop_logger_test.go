package scheduler

import "testing"

// noopLogger tests ensure the no-op logger methods do not panic
// when called with various argument patterns.

func TestNoopLogger_Infof(t *testing.T) {
	l := &noopLogger{}
	// These should not panic
	l.Infof("")
	l.Infof("simple message")
	l.Infof("format %d %s", 42, "test")
	l.Infof("multiple: %v %v %v", 1, 2, 3)
}

func TestNoopLogger_Warnf(t *testing.T) {
	l := &noopLogger{}
	l.Warnf("")
	l.Warnf("warning: something happened")
	l.Warnf("format %d %s", 42, "test")
}

func TestNoopLogger_Errorf(t *testing.T) {
	l := &noopLogger{}
	l.Errorf("")
	l.Errorf("error: something failed")
	l.Errorf("format %d %s", 42, "test")
	l.Errorf("error with args: %v %v %v", "a", 1, true)
}
