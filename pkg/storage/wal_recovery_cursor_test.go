package storage

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

var (
	errCursorRead     = errors.New("injected WAL read failure")
	errCursorStart    = errors.New("injected WAL seek-start failure")
	errCursorEnd      = errors.New("injected WAL seek-end failure")
	errCursorTruncate = errors.New("injected WAL truncate failure")
)

type cursorFaultFile struct {
	walFile
	readErr, startErr, endErr, truncateErr error
}

func (f *cursorFaultFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.walFile.Read(p)
}

func (f *cursorFaultFile) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && f.startErr != nil {
		err := f.startErr
		f.startErr = nil
		return 0, err
	}
	if whence == io.SeekEnd && f.endErr != nil {
		err := f.endErr
		f.endErr = nil
		return 0, err
	}
	return f.walFile.Seek(offset, whence)
}

func (f *cursorFaultFile) Truncate(size int64) error {
	if f.truncateErr != nil {
		err := f.truncateErr
		f.truncateErr = nil
		return err
	}
	return f.walFile.Truncate(size)
}

func openCursorFaultWAL(t *testing.T) (*WAL, *cursorFaultFile, string) {
	t.Helper()
	oldOpen := walOpenFile
	var f *cursorFaultFile
	walOpenFile = func(path string, flags int, mode os.FileMode) (walFile, error) {
		file, err := oldOpen(path, flags, mode)
		if err != nil {
			return nil, err
		}
		f = &cursorFaultFile{walFile: file}
		return f, nil
	}
	t.Cleanup(func() { walOpenFile = oldOpen })
	path := filepath.Join(t.TempDir(), "cursor.wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, f, path
}

func TestWALRecoveryRestoresAppendPosition(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, mode := range []string{"success", "page_error", "read_error", "empty", "restore_error"} {
			t.Run(mode+map[bool]string{false: "/plain", true: "/encrypted"}[encrypted], func(t *testing.T) {
				w, f, path := openCursorFaultWAL(t)
				c := makeTestCipher(t)
				if encrypted {
					w.SetEncryptionCipher(c)
				}
				if mode != "empty" {
					first := &WALRecord{TxnID: 1, Type: WALUpdateCommit, Data: []byte("first")}
					if mode == "page_error" {
						first.PageID = 99
					}
					records := []*WALRecord{first}
					for i := 0; i < 20; i++ {
						records = append(records, &WALRecord{TxnID: uint64(i + 2), Type: WALUpdateCommit, Data: bytes.Repeat([]byte("x"), 300)})
					}
					if err := w.AppendBatch(records); err != nil {
						t.Fatal(err)
					}
				}
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				lsn := w.LSN()
				bp := NewBufferPool(4, NewMemory())
				defer bp.Close()
				if mode == "read_error" || mode == "restore_error" {
					f.readErr = errCursorRead
				}
				if mode == "restore_error" {
					f.endErr = errCursorEnd
					err := w.Recover(bp)
					if !errors.Is(err, errCursorRead) || !errors.Is(err, errCursorEnd) {
						t.Fatalf("lost primary/restore errors: %v", err)
					}
					return
				}
				for pass := 0; pass < 2; pass++ {
					err := w.Recover(bp)
					if mode == "page_error" || mode == "read_error" {
						if err == nil {
							t.Fatal("expected recovery failure")
						}
						if mode == "read_error" && !errors.Is(err, errCursorRead) {
							t.Fatalf("read error lost: %v", err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					pos, err := f.walFile.Seek(0, io.SeekCurrent)
					if err != nil {
						t.Fatal(err)
					}
					if pos != int64(len(before)) {
						t.Fatalf("cursor=%d, want EOF=%d after pass %d", pos, len(before), pass)
					}
				}
				f.readErr = nil
				if err := w.Append(&WALRecord{TxnID: 99, Type: WALUpdateCommit, Data: []byte("after-recovery")}); err != nil {
					t.Fatal(err)
				}
				after, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if len(after) <= len(before) || !bytes.Equal(before, after[:len(before)]) {
					t.Fatal("append overwrote existing WAL records")
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenWAL(path)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				if reopened.LSN() != lsn+1 {
					t.Fatalf("reopened LSN=%d, want %d", reopened.LSN(), lsn+1)
				}
			})
		}
	}
}
