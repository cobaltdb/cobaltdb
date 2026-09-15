package storage

import (
	"bytes"
	"os"
	"testing"
)

// recordingFile wraps the real WAL file and records every Write issued before
// the first Truncate, so a test can prove that Checkpoint made buffered
// appended records durable (wrote them to the file) BEFORE truncating the log.
type recordingFile struct {
	walFile
	writes    [][]byte
	truncated bool
}

func (r *recordingFile) Write(p []byte) (int, error) {
	if !r.truncated {
		r.writes = append(r.writes, append([]byte(nil), p...))
	}
	return r.walFile.Write(p)
}

func (r *recordingFile) Truncate(size int64) error {
	r.truncated = true
	return r.walFile.Truncate(size)
}

// TestCheckpointFlushesBufferedRecordsBeforeTruncate proves Checkpoint's
// step-1 contract: "Flush + fsync the old bufWriter at its CURRENT position,
// making every appended record durable BEFORE anything is truncated."
//
// AppendWithoutSync legally leaves record bytes buffered in w.bufWriter. If
// Checkpoint only fsyncs the file (no bufWriter flush) and then truncates,
// those bytes are destroyed without ever touching disk while their LSN is
// consumed and group-commit waiters are signaled success — a silent loss of
// an appended record on the very crash the checkpoint is supposed to survive.
func TestCheckpointFlushesBufferedRecordsBeforeTruncate(t *testing.T) {
	var rec *recordingFile
	origOpen := walOpenFile
	walOpenFile = func(path string, flag int, perm os.FileMode) (walFile, error) {
		f, err := origOpen(path, flag, perm)
		if err != nil {
			return nil, err
		}
		rec = &recordingFile{walFile: f}
		return rec, nil
	}
	defer func() { walOpenFile = origOpen }()

	w, err := OpenWAL(t.TempDir() + "/test.wal")
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	if rec == nil {
		t.Fatal("seam not used: recording wrapper missing")
	}

	bp, err := NewBufferPoolWithError(4, NewMemory())
	if err != nil {
		t.Fatalf("buffer pool: %v", err)
	}

	// Legally buffered record: AppendWithoutSync leaves the bytes in
	// w.bufWriter without flushing — exactly the state Checkpoint must make
	// durable before truncating.
	if err := w.AppendWithoutSync(&WALRecord{
		TxnID:  1,
		Type:   WALInsert,
		PageID: 1,
		Offset: 0,
		Data:   []byte("payload"),
	}); err != nil {
		t.Fatalf("AppendWithoutSync: %v", err)
	}

	if err := w.Checkpoint(bp); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	if !rec.truncated {
		t.Fatal("precondition failed: checkpoint did not truncate the WAL")
	}
	for i, wbytes := range rec.writes {
		if bytes.Contains(wbytes, []byte("payload")) {
			return // the buffered record reached the file before truncation
		}
		if i >= 8 && len(rec.writes) > 8 {
			break
		}
	}
	t.Fatalf("FAIL: Checkpoint truncated the WAL without ever writing the buffered record's bytes to the file (%d writes before truncate)", len(rec.writes))
}
