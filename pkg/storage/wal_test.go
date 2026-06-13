package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenWAL(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	if wal == nil {
		t.Fatal("WAL is nil")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Failed to stat WAL: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("WAL permissions = %v, want 0600", info.Mode().Perm())
	}
}

func TestOpenWALRestrictsExistingFilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "loose.wal")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("Failed to create loose WAL: %v", err)
	}

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Failed to stat WAL: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("WAL permissions = %v, want 0600", info.Mode().Perm())
	}
}

func TestOpenWALRejectsSymlink(t *testing.T) {
	tmpDir := t.TempDir()
	target := filepath.Join(tmpDir, "target.wal")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatalf("WriteFile target: %v", err)
	}

	link := filepath.Join(tmpDir, "link.wal")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	_, err := OpenWAL(link)
	if err == nil {
		t.Fatal("expected symlink WAL path to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestOpenWALRejectsSymlinkParentComponent(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "target")
	if err := os.Mkdir(targetDir, 0750); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	linkDir := filepath.Join(tmpDir, "link")
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	walPath := filepath.Join(linkDir, "nested", "test.wal")
	wal, err := OpenWAL(walPath)
	if wal != nil {
		_ = wal.Close()
	}
	if err == nil {
		t.Fatal("expected symlink parent component to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(targetDir, "nested", "test.wal")); !os.IsNotExist(statErr) {
		t.Fatalf("WAL file should not be created through symlink parent, stat err=%v", statErr)
	}
}

func TestOpenWALDoesNotChmodSymlinkRaceTarget(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "race.wal")
	targetPath := filepath.Join(tmpDir, "target.wal")
	if err := os.WriteFile(walPath, nil, 0600); err != nil {
		t.Fatalf("WriteFile WAL: %v", err)
	}
	if err := os.WriteFile(targetPath, nil, 0644); err != nil {
		t.Fatalf("WriteFile target: %v", err)
	}

	originalOpenFile := walOpenFile
	defer func() { walOpenFile = originalOpenFile }()

	swapped := false
	walOpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if name == filepath.Clean(walPath) && !swapped {
			swapped = true
			if err := os.Remove(walPath); err != nil {
				t.Fatalf("remove WAL: %v", err)
			}
			if err := os.Symlink(targetPath, walPath); err != nil {
				t.Skipf("symlink not supported: %v", err)
			}
		}
		return originalOpenFile(name, flag, perm)
	}

	wal, err := OpenWAL(walPath)
	if wal != nil {
		_ = wal.Close()
	}
	if err == nil {
		t.Fatal("expected raced WAL path to be rejected")
	}
	if !strings.Contains(err.Error(), "changed while opening") {
		t.Fatalf("expected changed-while-opening error, got %v", err)
	}
	info, statErr := os.Stat(targetPath)
	if statErr != nil {
		t.Fatalf("stat target WAL: %v", statErr)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("race target permissions = %v, want 0644", info.Mode().Perm())
	}
}

func TestOpenWALCreateRejectsSymlinkRace(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "new-race.wal")
	targetPath := filepath.Join(tmpDir, "target.wal")
	if err := os.WriteFile(targetPath, nil, 0644); err != nil {
		t.Fatalf("WriteFile target: %v", err)
	}

	originalOpenFile := walOpenFile
	defer func() { walOpenFile = originalOpenFile }()

	swapped := false
	walOpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if name == filepath.Clean(walPath) && !swapped {
			swapped = true
			if err := os.Symlink(targetPath, walPath); err != nil {
				t.Skipf("symlink not supported: %v", err)
			}
		}
		return originalOpenFile(name, flag, perm)
	}

	wal, err := OpenWAL(walPath)
	if wal != nil {
		_ = wal.Close()
	}
	if err == nil {
		t.Fatal("expected raced WAL create path to be rejected")
	}
	info, statErr := os.Stat(targetPath)
	if statErr != nil {
		t.Fatalf("stat target WAL: %v", statErr)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("race target permissions = %v, want 0644", info.Mode().Perm())
	}
}

func TestOpenWALRejectsNonRegularFile(t *testing.T) {
	tmpDir := t.TempDir()
	_, err := OpenWAL(tmpDir)
	if err == nil {
		t.Fatal("expected directory WAL path to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestWALAppend(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	record := &WALRecord{
		TxnID:  1,
		Type:   WALInsert,
		PageID: 1,
		Offset: 0,
		Data:   []byte("test data"),
	}

	err = wal.Append(record)
	if err != nil {
		t.Fatalf("Failed to append record: %v", err)
	}
}

func TestWALAppendRejectsNilRecords(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nil-record.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Append", func() error { return wal.Append(nil) }},
		{"AppendWithoutSync", func() error { return wal.AppendWithoutSync(nil) }},
		{"AppendBatch", func() error { return wal.AppendBatch([]*WALRecord{nil}) }},
		{"AppendBatchWithoutSync", func() error { return wal.AppendBatchWithoutSync([]*WALRecord{nil}) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fn(); !errors.Is(err, ErrInvalidWALRecord) {
				t.Fatalf("expected ErrInvalidWALRecord, got %v", err)
			}
		})
	}
}

type shortNilWALWriter struct {
	limit int
}

func (w shortNilWALWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit {
		return w.limit, nil
	}
	return len(p), nil
}

var errWALFlushTest = errors.New("wal flush failed")

type checkpointFailingWALWriter struct{}

func (f checkpointFailingWALWriter) Write(_ []byte) (int, error) {
	return 0, errWALFlushTest
}

func TestWriteWALFullRejectsShortWrite(t *testing.T) {
	err := writeWALFull(shortNilWALWriter{limit: 3}, []byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeWALFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestWALAppendRejectsUnknownRecordType(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "unknown-record.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	err = wal.Append(&WALRecord{TxnID: 1, Type: WALRecordType(0xff)})
	if !errors.Is(err, ErrInvalidWALRecord) {
		t.Fatalf("expected ErrInvalidWALRecord, got %v", err)
	}
}

func TestWALAppendBatchDurableBeforeClose(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "batch.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	records := []*WALRecord{
		{TxnID: 1, Type: WALInsert, PageID: 1, Offset: 0, Data: []byte("small")},
		{TxnID: 1, Type: WALCommit},
	}
	if err := wal.AppendBatch(records); err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Reopen after AppendBatch failed: %v", err)
	}
	defer reopened.Close()
	if got := reopened.LSN(); got != 2 {
		t.Fatalf("reopened LSN = %d, want 2", got)
	}
}

func TestWALAppendBatchLargeRecordCRC(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "large-batch.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	largePayload := make([]byte, walBatchBufferSize+512)
	for i := range largePayload {
		largePayload[i] = byte(i % 251)
	}
	if err := wal.AppendBatch([]*WALRecord{
		{TxnID: 7, Type: WALInsert, PageID: 1, Offset: 0, Data: largePayload},
	}); err != nil {
		t.Fatalf("AppendBatch large record failed: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Reopen after large AppendBatch failed: %v", err)
	}
	defer reopened.Close()
	if got := reopened.LSN(); got != 1 {
		t.Fatalf("reopened LSN = %d, want 1", got)
	}
}

func TestOpenWALTruncatesPartialTrailingRecord(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "partial-tail.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if err := wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("valid")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	validSize := info.Size()

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.Write([]byte{0xde, 0xad, 0xbe, 0xef}); err != nil {
		_ = file.Close()
		t.Fatalf("Write partial tail: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close partial tail: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL with partial tail: %v", err)
	}
	if got := reopened.LSN(); got != 1 {
		t.Fatalf("LSN after truncating partial tail = %d, want 1", got)
	}
	if err := reopened.Append(&WALRecord{TxnID: 2, Type: WALCommit}); err != nil {
		t.Fatalf("Append after tail truncation: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened WAL: %v", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("Stat after reopen: %v", err)
	}
	if info.Size() <= validSize {
		t.Fatalf("expected appended WAL size > %d, got %d", validSize, info.Size())
	}

	reopenedAgain, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL after append: %v", err)
	}
	defer reopenedAgain.Close()
	if got := reopenedAgain.LSN(); got != 2 {
		t.Fatalf("LSN after append = %d, want 2", got)
	}
}

func TestWALMultipleAppends(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	for i := 0; i < 10; i++ {
		record := &WALRecord{
			TxnID:  uint64(i),
			Type:   WALInsert,
			PageID: uint32(i),
			Data:   []byte("data"),
		}

		err := wal.Append(record)
		if err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
	}
}

func TestWALCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()
	pool.SetWAL(wal)

	// Append some records
	for i := 0; i < 5; i++ {
		record := &WALRecord{
			TxnID:  uint64(i),
			Type:   WALInsert,
			PageID: uint32(i),
			Data:   []byte("data"),
		}
		if err := wal.Append(record); err != nil {
			t.Fatalf("Failed to append WAL record: %v", err)
		}
	}

	// Create checkpoint
	err = wal.Checkpoint(pool)
	if err != nil {
		t.Fatalf("Failed to checkpoint: %v", err)
	}
}

func TestWALRecover(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	// Write some data
	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	for i := 0; i < 5; i++ {
		record := &WALRecord{
			TxnID:  uint64(i),
			Type:   WALInsert,
			PageID: uint32(i),
			Data:   []byte("data"),
		}
		if err := wal.Append(record); err != nil {
			t.Fatalf("Failed to append WAL record: %v", err)
		}
	}
	wal.Close()

	// Reopen and recover
	backend := NewMemory()
	pool := NewBufferPool(10, backend)
	defer pool.Close()

	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	err = wal2.Recover(pool)
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}
}

func TestWALRecoverRejectsUnknownRecordType(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	record := &WALRecord{LSN: 1, TxnID: 1, Type: WALRecordType(0xff)}
	buf := make([]byte, walHeaderSize+4)
	if err := writeRecordHeader(buf[:walHeaderSize], record, 0); err != nil {
		t.Fatalf("writeRecordHeader: %v", err)
	}
	crcHash := crc32.ChecksumIEEE(buf[:walHeaderSize])
	binary.LittleEndian.PutUint32(buf[walHeaderSize:], crcHash)
	if err := os.WriteFile(path, buf, 0600); err != nil {
		t.Fatalf("write WAL: %v", err)
	}

	_, err := OpenWAL(path)
	if !errors.Is(err, ErrInvalidWALRecord) {
		t.Fatalf("expected ErrInvalidWALRecord, got %v", err)
	}
}

func TestOpenWALRejectsNonMonotonicLSN(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "non-monotonic.wal")

	var walData []byte
	for _, record := range []*WALRecord{
		{LSN: 1, TxnID: 1, Type: WALInsert, Data: []byte("first")},
		{LSN: 1, TxnID: 2, Type: WALInsert, Data: []byte("duplicate-lsn")},
	} {
		buf := make([]byte, walHeaderSize+len(record.Data)+4)
		if err := writeRecordHeader(buf[:walHeaderSize], record, len(record.Data)); err != nil {
			t.Fatalf("writeRecordHeader: %v", err)
		}
		copy(buf[walHeaderSize:], record.Data)
		crcHash := crc32.ChecksumIEEE(buf[:walHeaderSize])
		crcHash = crc32.Update(crcHash, crc32.IEEETable, record.Data)
		binary.LittleEndian.PutUint32(buf[walHeaderSize+len(record.Data):], crcHash)
		walData = append(walData, buf...)
	}
	if err := os.WriteFile(path, walData, 0600); err != nil {
		t.Fatalf("write WAL: %v", err)
	}

	_, err := OpenWAL(path)
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("expected ErrWALCorrupted, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "non-monotonic WAL LSN") {
		t.Fatalf("expected non-monotonic LSN error, got %v", err)
	}
}

func TestWALLSN(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	lsn := wal.LSN()
	if lsn != 0 {
		t.Errorf("Expected initial LSN 0, got %d", lsn)
	}

	record := &WALRecord{
		Type: WALInsert,
		Data: []byte("data"),
	}
	if err := wal.Append(record); err != nil {
		t.Fatalf("Failed to append WAL record: %v", err)
	}

	lsn = wal.LSN()
	if lsn == 0 {
		t.Error("Expected non-zero LSN after append")
	}
}

func TestWALCheckpointLSN(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Before checkpoint
	cpLSN := wal.CheckpointLSN()
	if cpLSN != 0 {
		t.Errorf("Expected checkpoint LSN 0, got %d", cpLSN)
	}

	// Append
	record := &WALRecord{Type: WALInsert, Data: []byte("data")}
	if err := wal.Append(record); err != nil {
		t.Fatalf("Failed to append WAL record: %v", err)
	}
}

func TestWALRecordTypes(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	types := []WALRecordType{WALInsert, WALUpdate, WALDelete, WALCommit, WALRollback, WALCheckpoint}

	for i, rt := range types {
		record := &WALRecord{
			TxnID: uint64(i),
			Type:  rt,
			Data:  []byte("data"),
		}
		err := wal.Append(record)
		if err != nil {
			t.Errorf("Failed to append record type %d: %v", rt, err)
		}
	}
}

func TestWALCloseTwice(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Close again should not error
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL twice: %v", err)
	}
}

func TestWALAppendBatchAfterCloseReturnsErrWALClosed(t *testing.T) {
	tmpDir := t.TempDir()

	t.Run("fast path", func(t *testing.T) {
		path := filepath.Join(tmpDir, "fast.wal")
		wal, err := OpenWAL(path)
		if err != nil {
			t.Fatalf("Failed to open WAL: %v", err)
		}
		if err := wal.Close(); err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}

		err = wal.AppendBatch([]*WALRecord{{Type: WALInsert, Data: []byte("small")}})
		if !errors.Is(err, ErrWALClosed) {
			t.Fatalf("expected ErrWALClosed, got %v", err)
		}
	})

	t.Run("formatted path", func(t *testing.T) {
		path := filepath.Join(tmpDir, "formatted.wal")
		wal, err := OpenWAL(path)
		if err != nil {
			t.Fatalf("Failed to open WAL: %v", err)
		}
		if err := wal.Close(); err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}

		err = wal.AppendBatch([]*WALRecord{{Type: WALInsert, Data: make([]byte, walBatchBufferSize)}})
		if !errors.Is(err, ErrWALClosed) {
			t.Fatalf("expected ErrWALClosed, got %v", err)
		}
	})
}

func TestWALApplyRecordBoundsCheck(t *testing.T) {
	backend := NewMemory()
	pool := NewBufferPool(4, backend)
	defer pool.Close()
	writeTestPage(t, backend, 1, PageTypeLeaf)

	wal := &WAL{}
	record := &WALRecord{
		Type:   WALUpdate,
		PageID: 1,
		Offset: uint16(PageSize - 2),
		Data:   []byte("toolong"),
	}

	err := wal.applyRecord(pool, record)
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("expected ErrWALCorrupted, got %v", err)
	}
}

func TestWALRecoverRejectsNilBufferPool(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nil-recovery-target.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	if err := wal.Recover(nil); !errors.Is(err, ErrInvalidWALRecoveryTarget) {
		t.Fatalf("expected ErrInvalidWALRecoveryTarget, got %v", err)
	}
}

func TestWALApplyRecordRejectsNilInputs(t *testing.T) {
	wal := &WAL{}
	record := &WALRecord{Type: WALUpdate, PageID: 1, Data: []byte("data")}

	if err := wal.applyRecord(nil, record); !errors.Is(err, ErrInvalidWALRecoveryTarget) {
		t.Fatalf("expected ErrInvalidWALRecoveryTarget, got %v", err)
	}

	backend := NewMemory()
	pool := NewBufferPool(4, backend)
	defer pool.Close()

	if err := wal.applyRecord(pool, nil); !errors.Is(err, ErrInvalidWALRecord) {
		t.Fatalf("expected ErrInvalidWALRecord, got %v", err)
	}
}

func TestWALRecoveryBufferTrackerLimitsPendingRecords(t *testing.T) {
	tracker := walRecoveryBufferTracker{records: walMaxRecoveryPendingRecords - 1}

	if err := tracker.add(&WALRecord{Type: WALInsert, Data: []byte("ok")}); err != nil {
		t.Fatalf("add at record limit: %v", err)
	}

	err := tracker.add(&WALRecord{Type: WALInsert})
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("expected WAL corruption at pending record limit, got %v", err)
	}
}

func TestWALRecoveryBufferTrackerLimitsPendingBytes(t *testing.T) {
	tracker := walRecoveryBufferTracker{bytes: walMaxRecoveryPendingBytes - 1}

	if err := tracker.add(&WALRecord{Type: WALInsert, Data: []byte("x")}); err != nil {
		t.Fatalf("add at byte limit: %v", err)
	}

	err := tracker.add(&WALRecord{Type: WALInsert, Data: []byte("x")})
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("expected WAL corruption at pending byte limit, got %v", err)
	}
}

func TestWALRecoveryBufferTrackerRemoveReleasesPendingBudget(t *testing.T) {
	tracker := walRecoveryBufferTracker{}
	records := []*WALRecord{
		{Type: WALInsert, Data: []byte("abc")},
		{Type: WALUpdate, Data: []byte("de")},
	}

	for _, record := range records {
		if err := tracker.add(record); err != nil {
			t.Fatalf("add pending record: %v", err)
		}
	}
	tracker.remove(records)

	if tracker.records != 0 || tracker.bytes != 0 {
		t.Fatalf("pending budget after remove = records:%d bytes:%d, want zero", tracker.records, tracker.bytes)
	}
	if err := tracker.add(&WALRecord{Type: WALDelete, Data: []byte("z")}); err != nil {
		t.Fatalf("add after remove: %v", err)
	}
}

func TestWALGroupCommit(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Enable group commit with a small interval so the test doesn't hang
	wal.EnableGroupCommit(0, 5*time.Millisecond)

	done := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func(idx int) {
			rec := &WALRecord{
				TxnID: uint64(idx),
				Type:  WALInsert,
				Data:  []byte(fmt.Sprintf("row%d", idx)),
			}
			done <- wal.Append(rec)
		}(i)
	}

	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// All three appends should have completed after the next ticker sync
	wal.DisableGroupCommit()

	if wal.LSN() != 3 {
		t.Fatalf("expected LSN=3, got %d", wal.LSN())
	}
}

func TestWALGroupCommitBatchSize(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit_batch.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Enable group commit with batch size of 2 (no ticker)
	wal.EnableGroupCommit(2, 0)

	// First append should block until second append triggers the batch
	done1 := make(chan error, 1)
	go func() {
		done1 <- wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("a")})
	}()

	// Give goroutine time to start and block
	time.Sleep(20 * time.Millisecond)

	// Second append should trigger immediate sync and unblock both
	if err := wal.Append(&WALRecord{TxnID: 2, Type: WALInsert, Data: []byte("b")}); err != nil {
		t.Fatalf("Second append failed: %v", err)
	}

	if err := <-done1; err != nil {
		t.Fatalf("First append failed: %v", err)
	}

	wal.DisableGroupCommit()

	if wal.LSN() != 2 {
		t.Fatalf("expected LSN=2, got %d", wal.LSN())
	}
}

func TestWALGroupCommitSyncOff(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit_off.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// SyncOff: no background sync, no batch size
	wal.EnableGroupCommit(0, 0)

	// Append should return immediately without blocking
	if err := wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("x")}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Data is in buffer but not synced
	wal.DisableGroupCommit() // this flushes pending

	if wal.LSN() != 1 {
		t.Fatalf("expected LSN=1, got %d", wal.LSN())
	}
}

func TestWALGroupCommitCanBeReconfiguredAndReenabled(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit_reenable.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	wal.EnableGroupCommit(0, time.Hour)
	wal.EnableGroupCommit(0, 5*time.Millisecond)
	if err := wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("first")}); err != nil {
		t.Fatalf("append after reconfigure: %v", err)
	}
	wal.DisableGroupCommit()

	wal.EnableGroupCommit(0, 5*time.Millisecond)
	done := make(chan error, 1)
	go func() {
		done <- wal.Append(&WALRecord{TxnID: 2, Type: WALInsert, Data: []byte("second")})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("append after re-enable: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("append after re-enable did not complete")
	}
	wal.DisableGroupCommit()

	if wal.LSN() != 2 {
		t.Fatalf("expected LSN=2, got %d", wal.LSN())
	}
}

func TestWALCheckpointFlushesPendingGroupCommit(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit_checkpoint.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	pool := NewBufferPool(4, NewMemory())
	defer pool.Close()
	wal.EnableGroupCommit(2, 0)

	done := make(chan error, 1)
	go func() {
		done <- wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("pending")})
	}()
	time.Sleep(20 * time.Millisecond)

	if err := wal.Checkpoint(pool); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pending append returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("checkpoint did not flush pending group commit append")
	}
	wal.DisableGroupCommit()
}

func TestWALCheckpointStopsBeforeTruncateOnPendingFlushError(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "checkpoint_flush_error.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	if err := wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("durable-before-checkpoint")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat WAL before checkpoint: %v", err)
	}
	sizeBefore := info.Size()

	wal.mu.Lock()
	wal.bufWriter = bufio.NewWriter(checkpointFailingWALWriter{})
	if _, err := wal.bufWriter.Write([]byte("pending")); err != nil {
		wal.mu.Unlock()
		t.Fatalf("buffer pending WAL bytes: %v", err)
	}
	wal.mu.Unlock()

	pool := NewBufferPool(4, NewMemory())
	defer pool.Close()

	err = wal.Checkpoint(pool)
	if !errors.Is(err, errWALFlushTest) {
		t.Fatalf("expected checkpoint flush error, got %v", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("stat WAL after checkpoint: %v", err)
	}
	if info.Size() != sizeBefore {
		t.Fatalf("checkpoint changed WAL size after flush failure: got %d, want %d", info.Size(), sizeBefore)
	}
}

func TestWALSyncFlushesPendingGroupCommitCallers(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "groupcommit_sync.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	wal.EnableGroupCommit(2, 0)
	done := make(chan error, 1)
	go func() {
		done <- wal.Append(&WALRecord{TxnID: 1, Type: WALInsert, Data: []byte("pending")})
	}()
	time.Sleep(20 * time.Millisecond)

	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pending append returned error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Sync did not release pending group commit append")
	}
	wal.DisableGroupCommit()
}

func TestWALPendingSyncSnapshotDoesNotSignalLaterWaiters(t *testing.T) {
	wal := &WAL{}
	first := make(chan error, 1)
	second := make(chan error, 1)

	wal.groupCommitMu.Lock()
	wal.pendingSyncs = append(wal.pendingSyncs, first)
	wal.groupCommitMu.Unlock()

	pending := wal.popPendingSyncs()

	wal.groupCommitMu.Lock()
	wal.pendingSyncs = append(wal.pendingSyncs, second)
	wal.groupCommitMu.Unlock()

	signalPendingSyncs(pending, nil)

	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first pending sync got error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("first pending sync was not signaled")
	}

	select {
	case err := <-second:
		t.Fatalf("later pending sync was signaled by stale flush result: %v", err)
	default:
	}
}

// TestWALRecoverLogicalRecords verifies that logical WAL records (PageID==0,
// Offset==0) are buffered in replayOps rather than corrupting page 0.
func TestWALRecoverLogicalRecords(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "logical.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Write a committed logical transaction.
	txnID := uint64(42)
	key := []byte("users:row1")
	val := []byte("value1")
	data := make([]byte, 4+len(key)+len(val))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(key)))
	copy(data[4:4+len(key)], key)
	copy(data[4+len(key):], val)

	if err := wal.Append(&WALRecord{TxnID: txnID, Type: WALUpdate, Data: data}); err != nil {
		t.Fatalf("append data: %v", err)
	}
	if err := wal.Append(&WALRecord{TxnID: txnID, Type: WALCommit}); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	wal.Close()

	// Reopen and recover
	backend := NewMemory()
	pool := NewBufferPool(4, backend)
	defer pool.Close()

	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	if err := wal2.Recover(pool); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	ops := wal2.GetReplayOps()
	if len(ops) != 1 {
		t.Fatalf("expected 1 replay op, got %d", len(ops))
	}
	if ops[0].Type != WALUpdate {
		t.Errorf("expected WALUpdate, got %d", ops[0].Type)
	}
	if string(ops[0].Data) != string(data) {
		t.Errorf("data mismatch: got %q, want %q", ops[0].Data, data)
	}

	if err := wal2.Recover(pool); err != nil {
		t.Fatalf("second Recover failed: %v", err)
	}
	ops = wal2.GetReplayOps()
	if len(ops) != 1 {
		t.Fatalf("expected second recovery to replace replay ops, got %d", len(ops))
	}
}

func TestWALGetReplayOpsReturnsIsolatedData(t *testing.T) {
	wal := &WAL{
		replayOps: []WALReplayOp{
			{TxnID: 1, Type: WALUpdate, Data: []byte("logical-row")},
		},
	}

	ops := wal.GetReplayOps()
	ops[0].TxnID = 99
	ops[0].Data[0] = 'X'
	ops = append(ops, WALReplayOp{TxnID: 2, Type: WALDelete, Data: []byte("extra")})
	if len(ops) != 2 {
		t.Fatalf("expected local replay op append to succeed, got %d ops", len(ops))
	}

	again := wal.GetReplayOps()
	if len(again) != 1 {
		t.Fatalf("expected 1 replay op, got %d", len(again))
	}
	if again[0].TxnID != 1 {
		t.Fatalf("TxnID was mutated through returned slice: got %d", again[0].TxnID)
	}
	if string(again[0].Data) != "logical-row" {
		t.Fatalf("Data was mutated through returned slice: got %q", again[0].Data)
	}
}

func TestWALAppendBatchWithoutSync(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "batch.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	records := []*WALRecord{
		{TxnID: 1, Type: WALInsert, PageID: 1, Data: []byte("row1")},
		{TxnID: 1, Type: WALInsert, PageID: 2, Data: []byte("row2")},
		{TxnID: 1, Type: WALUpdate, PageID: 1, Data: []byte("row1_updated")},
	}

	if err := wal.AppendBatchWithoutSync(records); err != nil {
		t.Fatalf("AppendBatchWithoutSync failed: %v", err)
	}

	if wal.LSN() != 3 {
		t.Fatalf("expected LSN=3, got %d", wal.LSN())
	}

	// Sync so records are durable for reopen
	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	wal.Close()

	// Reopen and recover
	backend := NewMemory()
	pool := NewBufferPool(4, backend)
	defer pool.Close()

	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	if err := wal2.Recover(pool); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if wal2.LSN() != 3 {
		t.Errorf("expected LSN=3 after recover, got %d", wal2.LSN())
	}
}
