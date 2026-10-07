package storage

import (
	"errors"
	"testing"
)

func TestWALCheckpointSeekFailureKeepsAppendPosition(t *testing.T) {
	for _, mode := range []string{"success", "seek_error", "empty_seek_error", "truncate_error", "retry", "restore_error"} {
		t.Run(mode, func(t *testing.T) {
			w, f, path := openCursorFaultWAL(t)
			if mode != "empty_seek_error" {
				if err := w.Append(&WALRecord{TxnID: 1, Type: WALUpdateCommit, Data: []byte("before-checkpoint")}); err != nil {
					t.Fatal(err)
				}
			}
			bp := NewBufferPool(4, NewMemory())
			defer bp.Close()
			if mode == "seek_error" || mode == "empty_seek_error" || mode == "retry" || mode == "restore_error" {
				f.startErr = errCursorStart
			}
			if mode == "truncate_error" {
				f.truncateErr = errCursorTruncate
			}
			if mode == "restore_error" {
				f.endErr = errCursorEnd
			}
			err := w.Checkpoint(bp)
			switch mode {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
			case "truncate_error":
				if !errors.Is(err, errCursorTruncate) {
					t.Fatalf("truncate error lost: %v", err)
				}
			case "restore_error":
				if !errors.Is(err, errCursorStart) || !errors.Is(err, errCursorEnd) {
					t.Fatalf("primary/restore errors lost: %v", err)
				}
				return
			default:
				if !errors.Is(err, errCursorStart) {
					t.Fatalf("seek error lost: %v", err)
				}
			}
			if mode == "retry" {
				if err := w.Checkpoint(bp); err != nil {
					t.Fatalf("checkpoint retry: %v", err)
				}
			}
			lsn := w.LSN()
			if err := w.Append(&WALRecord{TxnID: 2, Type: WALUpdateCommit, Data: []byte("after-checkpoint")}); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenWAL(path)
			if err != nil {
				t.Fatalf("acknowledged append cannot reopen: %v", err)
			}
			defer reopened.Close()
			if reopened.LSN() != lsn+1 {
				t.Fatalf("reopened LSN=%d, want %d", reopened.LSN(), lsn+1)
			}
			if err := reopened.Recover(bp); err != nil {
				t.Fatal(err)
			}
			ops := reopened.GetReplayOps()
			if len(ops) == 0 || string(ops[len(ops)-1].Data) != "after-checkpoint" {
				t.Fatalf("acknowledged append missing from recovery: %+v", ops)
			}
		})
	}
}
