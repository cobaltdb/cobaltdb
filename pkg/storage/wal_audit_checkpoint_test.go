package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

var errCheckpointFinalFlush = errors.New("injected final checkpoint flush failure")

// Sync gates the first checkpoint sync after its initial buffer flush. The
// real sync completes before the gate, allowing Close to finish deterministically.
type checkpointGateFile struct {
	walFile
	armed                  atomic.Bool
	failWrite              atomic.Bool
	entered, release       chan struct{}
	mu                     sync.Mutex
	writes, beforeTruncate []byte
	truncated              bool
}

func (f *checkpointGateFile) Sync() error {
	err := f.walFile.Sync()
	if f.armed.CompareAndSwap(true, false) {
		close(f.entered)
		<-f.release
	}
	return err
}

func (f *checkpointGateFile) Write(p []byte) (int, error) {
	if f.failWrite.Load() {
		return 0, errCheckpointFinalFlush
	}
	n, err := f.walFile.Write(p)
	f.mu.Lock()
	f.writes = append(f.writes, p[:n]...)
	f.mu.Unlock()
	return n, err
}

func (f *checkpointGateFile) Truncate(size int64) error {
	f.mu.Lock()
	f.truncated = true
	f.beforeTruncate = append([]byte(nil), f.writes...)
	f.mu.Unlock()
	return f.walFile.Truncate(size)
}

func TestWALCheckpointRechecksStateAfterInitialSync(t *testing.T) {
	for _, action := range []string{"append", "append_batch", "empty", "close", "flush_failure"} {
		t.Run(action, func(t *testing.T) {
			oldOpen := walOpenFile
			var f *checkpointGateFile
			walOpenFile = func(path string, flags int, mode os.FileMode) (walFile, error) {
				file, err := oldOpen(path, flags, mode)
				if err != nil {
					return nil, err
				}
				f = &checkpointGateFile{walFile: file, entered: make(chan struct{}), release: make(chan struct{})}
				return f, nil
			}
			defer func() { walOpenFile = oldOpen }()
			path := filepath.Join(t.TempDir(), "checkpoint.wal")
			w, err := OpenWAL(path)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			bp := NewBufferPool(4, NewMemory())
			defer bp.Close()
			f.armed.Store(true)
			type result struct {
				err        error
				panicValue any
			}
			done := make(chan result, 1)
			go func() {
				var r result
				defer func() { r.panicValue = recover(); done <- r }()
				r.err = w.Checkpoint(bp)
			}()
			<-f.entered
			payload := []byte("gated-checkpoint-payload")
			record := &WALRecord{TxnID: 1, Type: WALUpdate, Data: payload}
			var actionErr error
			switch action {
			case "append", "flush_failure":
				actionErr = w.AppendWithoutSync(record)
			case "append_batch":
				actionErr = w.AppendBatchWithoutSync([]*WALRecord{record, {TxnID: 1, Type: WALCommit}})
			case "close":
				actionErr = w.Close()
			}
			if action == "flush_failure" {
				f.failWrite.Store(true)
			}
			close(f.release)
			r := <-done
			if actionErr != nil {
				t.Fatal(actionErr)
			}
			if r.panicValue != nil {
				t.Fatalf("Checkpoint panicked after %s: %v", action, r.panicValue)
			}
			f.mu.Lock()
			truncated := f.truncated
			written := bytes.Contains(f.beforeTruncate, payload)
			f.mu.Unlock()
			switch action {
			case "close":
				if !errors.Is(r.err, ErrWALClosed) {
					t.Fatalf("Checkpoint after close = %v, want ErrWALClosed", r.err)
				}
				if truncated {
					t.Fatal("closed WAL was truncated")
				}
			case "flush_failure":
				if !errors.Is(r.err, errCheckpointFinalFlush) {
					t.Fatalf("Checkpoint = %v, want injected flush error", r.err)
				}
				if truncated {
					t.Fatal("WAL truncated despite failed final flush")
				}
			default:
				if r.err != nil {
					t.Fatal(r.err)
				}
				if !truncated {
					t.Fatal("checkpoint never truncated WAL")
				}
				if action != "empty" && !written {
					t.Fatal("concurrent append did not reach file before truncate")
				}
				if w.CheckpointLSN() != w.LSN() {
					t.Fatal("checkpoint LSN differs from WAL LSN")
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenWAL(path)
				if err != nil {
					t.Fatalf("reopen after checkpoint: %v", err)
				}
				defer reopened.Close()
				if err := reopened.Recover(bp); err != nil {
					t.Fatalf("recover after checkpoint: %v", err)
				}
			}
		})
	}
}
