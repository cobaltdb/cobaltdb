package engine

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

type panicDeadlineContext struct {
	context.Context
}

func (panicDeadlineContext) Deadline() (time.Time, bool) {
	panic("deadline panic")
}

func TestExecPanicRecoveryRecordsWithoutStdout(t *testing.T) {
	oldStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = writePipe
	defer func() {
		os.Stdout = oldStdout
		_ = readPipe.Close()
	}()

	db := &DB{options: &Options{ConnectionPool: ConnectionPool{QueryTimeout: time.Second}}}
	_, err = db.Exec(panicDeadlineContext{Context: context.Background()}, "SELECT 1")

	if closeErr := writePipe.Close(); closeErr != nil {
		t.Fatalf("close pipe writer: %v", closeErr)
	}
	os.Stdout = oldStdout
	output, readErr := io.ReadAll(readPipe)
	if readErr != nil {
		t.Fatalf("read stdout capture: %v", readErr)
	}

	if err == nil || !strings.Contains(err.Error(), "internal error in Exec") {
		t.Fatalf("expected recovered Exec panic error, got %v", err)
	}
	if len(output) != 0 {
		t.Fatalf("panic recovery should not write to stdout, got %q", string(output))
	}
	info := db.LastPanicRecovery()
	if info == nil {
		t.Fatal("expected panic recovery info")
	}
	if info.Operation != "Exec" {
		t.Fatalf("expected Exec operation, got %q", info.Operation)
	}
	if info.Value == "" || info.Stack == "" || info.At.IsZero() {
		t.Fatalf("panic recovery info incomplete: %+v", info)
	}
}
