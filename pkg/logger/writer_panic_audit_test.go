package logger

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type panicAuditWriter struct {
	entered, release chan struct{}
	mode             string
	calls            int
	output           bytes.Buffer
}

func (w *panicAuditWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		if strings.Contains(w.mode, "panic") {
			close(w.entered)
			<-w.release
			panic("injected writer panic")
		}
		if w.mode == "returned-error" {
			return 0, errors.New("injected writer error")
		}
	}
	return w.output.Write(p)
}

func TestLoggerReleasesSharedOutputMutexAfterWriterPanic(t *testing.T) {
	for _, mode := range []string{"normal-control", "returned-error", "panic", "derived-panic", "zero-value-panic"} {
		t.Run(mode, func(t *testing.T) {
			writer := &panicAuditWriter{entered: make(chan struct{}), release: make(chan struct{}), mode: mode}
			l := NewWithFormat(InfoLevel, writer, JSONFormat)
			if mode == "zero-value-panic" {
				l = &Logger{level: InfoLevel, format: JSONFormat, output: writer}
			}
			child := l.WithField("owner", "child")
			first := l
			if mode == "derived-panic" {
				first = child
			}
			panicDone := make(chan interface{}, 1)
			go func() {
				defer func() { panicDone <- recover() }()
				first.Info("first")
			}()
			if strings.Contains(mode, "panic") {
				<-writer.entered
				close(writer.release)
			}
			recovered := <-panicDone
			if strings.Contains(mode, "panic") && recovered != "injected writer panic" {
				t.Fatalf("panic propagation changed: %v", recovered)
			}
			if !strings.Contains(mode, "panic") && recovered != nil {
				t.Fatalf("unexpected panic: %v", recovered)
			}
			lock := child.sharedOutputMu()
			if lock != l.sharedOutputMu() {
				t.Fatal("child does not share writer lock")
			}
			released := lock.TryLock()
			if released {
				lock.Unlock()
			}
			if !released {
				t.Fatal("writer lock stranded after callback")
			}
			l.Info("parent-after")
			child.Info("child-after")
			lines := strings.Split(strings.TrimSpace(writer.output.String()), "\n")
			want := 2
			if mode == "normal-control" {
				want = 3
			}
			if writer.calls != 3 || len(lines) != want {
				t.Fatalf("logging did not recover: calls=%d records=%d", writer.calls, len(lines))
			}
			for _, line := range lines {
				if !json.Valid([]byte(line)) {
					t.Fatalf("invalid output: %s", line)
				}
			}
		})
	}
}
