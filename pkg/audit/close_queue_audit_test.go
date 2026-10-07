package audit

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestLogCloseRejectsGatedProducer(t *testing.T) {
	control, err := New(&Config{Enabled: true, LogFile: filepath.Join(t.TempDir(), "control.log")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	control.Log(EventQuery, "user", "CONTROL")
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := VerifyLogFile(control.config.LogFile, nil)
	if err != nil || result.Entries != 1 {
		t.Fatalf("control: %v %v", result, err)
	}
	fmt.Println("CONTROL EXPECTED: 1 persisted event; ACTUAL:", result.Entries)
	al, err := New(&Config{Enabled: true, LogFile: filepath.Join(t.TempDir(), "case.log")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		al.Log(EventQuery, "user", "GATED", func(e *Event) { close(entered); <-release })
	}()
	<-entered
	if err := al.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	pending := len(al.eventChan)
	fmt.Printf("EXPECTED: 0 events queued after Close; ACTUAL: %d\n", pending)
	if pending != 0 {
		fmt.Println("PROBLEM CONFIRMED")
		t.FailNow()
	}
	// Two nearby edges: repeated Close and calls initiated after Close.
	if err := al.Close(); err != nil {
		t.Fatal(err)
	}
	called := false
	al.Log(EventQuery, "user", "AFTER", func(e *Event) { called = true })
	if called || len(al.eventChan) != 0 {
		t.Fatal("closed logger accepted a new call")
	}
	empty, err := VerifyLogFile(al.config.LogFile, nil)
	if err != nil || empty.Entries != 0 {
		t.Fatalf("closed file: %v %v", empty, err)
	}
	fmt.Println("FIX VERIFIED")
}
