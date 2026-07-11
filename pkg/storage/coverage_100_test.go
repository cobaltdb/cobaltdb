package storage

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errCoverage = errors.New("coverage backend failure")

type coverageBackend struct {
	data        []byte
	size        int64
	readErr     error
	writeErr    error
	syncErr     error
	truncateErr error
	closeErr    error
	shortRead   bool
	shortWrite  bool
	closed      bool
}

func (b *coverageBackend) ReadAt(dst []byte, off int64) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	if off < 0 || off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(dst, b.data[off:])
	if b.shortRead && n > 0 {
		n--
	}
	return n, nil
}
func (b *coverageBackend) WriteAt(src []byte, off int64) (int, error) {
	if b.writeErr != nil {
		return 0, b.writeErr
	}
	n := len(src)
	if b.shortWrite && n > 0 {
		n--
	}
	end := int(off) + n
	if end > len(b.data) {
		b.data = append(b.data, make([]byte, end-len(b.data))...)
	}
	copy(b.data[int(off):end], src[:n])
	if int64(end) > b.size {
		b.size = int64(end)
	}
	return n, nil
}
func (b *coverageBackend) Sync() error { return b.syncErr }
func (b *coverageBackend) Size() int64 {
	if b.size != 0 {
		return b.size
	}
	return int64(len(b.data))
}
func (b *coverageBackend) Truncate(n int64) error {
	if b.truncateErr != nil {
		return b.truncateErr
	}
	b.size = n
	return nil
}
func (b *coverageBackend) Close() error { b.closed = true; return b.closeErr }

func coveragePage(id uint32, typ PageType) []byte {
	p := NewPage(id, typ)
	out := append([]byte(nil), p.Data...)
	putPageData(p.Data)
	return out
}

func TestCoverage100BufferPoolEdges(t *testing.T) {
	t.Run("pin saturates", func(t *testing.T) {
		p := &CachedPage{pinned: maxCachedPagePinCount}
		p.Pin()
		if p.pinned != maxCachedPagePinCount {
			t.Fatalf("pin overflowed: %d", p.pinned)
		}
	})
	t.Run("constructor validation and zero page count normalization", func(t *testing.T) {
		if _, err := NewBufferPoolWithError(0, NewMemory()); err == nil {
			t.Fatal("zero capacity accepted")
		}
		if _, err := NewBufferPoolWithError(1, nil); err == nil {
			t.Fatal("nil backend accepted")
		}
		bp, err := NewBufferPoolWithError(1, &coverageBackend{size: 1})
		if err != nil || bp.nextPageID != 1 {
			t.Fatalf("normalization: id=%d err=%v", bp.nextPageID, err)
		}
		compat := NewBufferPool(0, NewMemory())
		if compat.capacity != 1 || compat.initErr == nil {
			t.Fatalf("compat wrapper state: %+v", compat)
		}
		if _, err := compat.GetPage(0); err == nil {
			t.Fatal("deferred init error missing")
		}
	})
	t.Run("closed slow path", func(t *testing.T) {
		bp := NewBufferPool(1, NewMemory())
		bp.closed = true
		if _, err := bp.GetPage(0); !errors.Is(err, ErrBufferPoolClosed) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("read and validation failures", func(t *testing.T) {
		for name, backend := range map[string]Backend{
			"read":       &coverageBackend{readErr: errCoverage},
			"short":      &coverageBackend{data: make([]byte, PageSize), shortRead: true},
			"bad header": &coverageBackend{data: make([]byte, PageSize*2)},
		} {
			t.Run(name, func(t *testing.T) {
				bp := NewBufferPool(1, backend)
				if _, err := bp.GetPage(1); err == nil {
					t.Fatal("expected load failure")
				}
			})
		}
	})
	t.Run("full pool and dirty eviction failures", func(t *testing.T) {
		bp := NewBufferPool(1, NewMemory())
		p, _ := bp.NewPage(PageTypeLeaf)
		if _, err := bp.NewPage(PageTypeLeaf); !errors.Is(err, ErrBufferFull) {
			t.Fatalf("pinned eviction: %v", err)
		}
		p.Unpin()
		bp.PauseBackgroundFlusher()
		if _, err := bp.NewPage(PageTypeLeaf); !errors.Is(err, ErrBufferFull) {
			t.Fatalf("suspended eviction: %v", err)
		}
		bp.ResumeBackgroundFlusher()
		bp.backend = &coverageBackend{writeErr: errCoverage}
		if _, err := bp.NewPage(PageTypeLeaf); !errors.Is(err, errCoverage) {
			t.Fatalf("dirty eviction write: %v", err)
		}
	})
	t.Run("flush propagation", func(t *testing.T) {
		b := &coverageBackend{writeErr: errCoverage}
		bp := NewBufferPool(2, b)
		p, _ := bp.NewPage(PageTypeLeaf)
		p.Unpin()
		if err := bp.FlushAll(); !errors.Is(err, errCoverage) {
			t.Fatalf("FlushAll: %v", err)
		}
		if err := bp.FlushDirty(); !errors.Is(err, errCoverage) {
			t.Fatalf("FlushDirty: %v", err)
		}
		if !p.IsDirty() {
			t.Fatal("failed flush cleared dirty bit")
		}
		b.writeErr = nil
		b.syncErr = errCoverage
		if err := bp.Close(); !errors.Is(err, errCoverage) {
			t.Fatalf("Close sync: %v", err)
		}
		b.syncErr = nil
		if err := bp.Close(); err != nil {
			t.Fatal(err)
		}
		if err := bp.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("flusher suspension and error limit", func(t *testing.T) {
		b := &coverageBackend{writeErr: errCoverage}
		bp := NewBufferPool(1, b)
		p, _ := bp.NewPage(PageTypeLeaf)
		p.Unpin()
		bp.PauseBackgroundFlusher()
		bp.flushDirtyPages()
		if !p.IsDirty() {
			t.Fatal("suspended flush wrote page")
		}
		bp.ResumeBackgroundFlusher()
		bp.flushRunning, bp.flushDone, bp.flushErrLimit = true, make(chan struct{}), 1
		bp.flushDirtyPages()
		if bp.flushRunning || bp.flushDone != nil || bp.flushErrCount != 1 {
			t.Fatalf("flusher not halted: running=%v count=%d", bp.flushRunning, bp.flushErrCount)
		}
		b.writeErr = nil
		p.SetDirty(false)
		bp.flushDirtyPages()
		if bp.flushErrCount != 0 {
			t.Fatalf("success did not reset errors: %d", bp.flushErrCount)
		}
	})
}

func TestCoverage100CompressionEdges(t *testing.T) {
	t.Run("read backend error and malformed framing", func(t *testing.T) {
		cb, _ := NewCompressedBackend(&coverageBackend{readErr: errCoverage}, DefaultCompressionConfig())
		if _, err := cb.ReadAt(make([]byte, PageSize), 0); !errors.Is(err, errCoverage) {
			t.Fatalf("read error: %v", err)
		}
		cases := []struct {
			name string
			data []byte
			dst  int
		}{
			{"oversized original", append(append([]byte{}, compressionMagicZlib...), 1, 1, 0, 0), 1},
			{"truncated payload", append(append([]byte{}, compressionMagicZlib...), 1, 0, 2, 0, 0), PageSize},
			{"bad payload", append(append([]byte{}, compressionMagicZlib...), 1, 0, 1, 0, 0), PageSize},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cb, _ := NewCompressedBackend(&coverageBackend{data: tc.data}, DefaultCompressionConfig())
				if _, err := cb.ReadAt(make([]byte, tc.dst), 0); err == nil {
					t.Fatal("malformed record accepted")
				}
			})
		}
	})
	t.Run("write validation and failures", func(t *testing.T) {
		cb, _ := NewCompressedBackend(NewMemory(), DefaultCompressionConfig())
		if _, err := cb.WriteAt([]byte{1}, -1); !errors.Is(err, ErrInvalidOffset) {
			t.Fatalf("negative: %v", err)
		}
		if _, err := cb.WriteAt([]byte{1, 2}, math.MaxInt64); !errors.Is(err, ErrInvalidSize) {
			t.Fatalf("overflow: %v", err)
		}
		cb.updateLogicalSize(0, 1, errCoverage)
		cb.updateLogicalSize(math.MaxInt64, 2, nil)
		if cb.logicalSize != 0 {
			t.Fatalf("invalid write changed logical size: %d", cb.logicalSize)
		}
		bad := &coverageBackend{writeErr: errCoverage}
		cb, _ = NewCompressedBackend(bad, DefaultCompressionConfig())
		if _, err := cb.WriteAt(make([]byte, PageSize), 0); !errors.Is(err, errCoverage) {
			t.Fatalf("write: %v", err)
		}
		bad.writeErr = nil
		bad.truncateErr = errCoverage
		if err := cb.Truncate(1); !errors.Is(err, errCoverage) {
			t.Fatalf("truncate: %v", err)
		}
	})
	t.Run("conversion bounds", func(t *testing.T) {
		if _, err := checkedUint16(-1, "negative"); err == nil {
			t.Fatal("uint16 negative accepted")
		}
		if _, err := checkedUint64Offset(-1); err == nil {
			t.Fatal("negative offset accepted")
		}
	})
	t.Run("decoder errors and pools", func(t *testing.T) {
		cb, _ := NewCompressedBackend(NewMemory(), DefaultCompressionConfig())
		cb.lz4Readers = syncPoolWith("wrong")
		if _, err := cb.decompressLZ4([]byte("bad"), 1); err == nil {
			t.Fatal("bad lz4 accepted")
		}
		if _, err := cb.decompressZstd([]byte("bad"), 1); err == nil {
			t.Fatal("bad zstd accepted")
		}
		if err := requireFullDecompressed(0, 1, nil); err == nil {
			t.Fatal("short nil-error decode accepted")
		}
		cb.writeBufPool = syncPoolWith("wrong")
		wb := cb.getWriteBuf()
		if len(*wb) != PageSize {
			t.Fatal("write pool fallback")
		}
		cb.readBufPool = syncPoolWith("wrong")
		rb := cb.getReadBuf()
		if len(*rb) != PageSize {
			t.Fatal("read pool fallback")
		}
	})
}

// syncPoolWith creates a pool whose next value has a deliberately wrong type.
func syncPoolWith(v any) (p sync.Pool) { p.Put(v); return p }

func TestCoverage100MemoryPageAndDiskEdges(t *testing.T) {
	t.Run("memory default limits and capacity branches", func(t *testing.T) {
		m := NewMemoryWithLimit(0)
		if _, err := m.WriteAt([]byte{1}, 0); err != nil {
			t.Fatal(err)
		}
		m.data = make([]byte, 1, 4)
		if _, err := m.WriteAt([]byte{2}, 2); err != nil || len(m.data) != 3 {
			t.Fatalf("extend: len=%d err=%v", len(m.data), err)
		}
		m.maxSize = 0
		if err := m.Truncate(4); err != nil {
			t.Fatal(err)
		}
		m.data = make([]byte, 1, 4)
		if err := m.Truncate(3); err != nil || len(m.data) != 3 {
			t.Fatalf("truncate extend: len=%d err=%v", len(m.data), err)
		}
	})
	t.Run("short page header and checksum", func(t *testing.T) {
		if err := validatePageHeader(make([]byte, PageHeaderSize-1), 1); !errors.Is(err, ErrPageCorrupted) {
			t.Fatalf("header: %v", err)
		}
		if got := metaPageChecksum(make([]byte, 35)); got != 0 {
			t.Fatalf("short checksum=%d", got)
		}
	})
	t.Run("disk open and I/O failures", func(t *testing.T) {
		if _, err := OpenDisk(""); err == nil {
			t.Fatal("empty path accepted")
		}
		d := t.TempDir()
		dirPath := filepath.Join(d, "dir")
		if err := os.Mkdir(dirPath, 0700); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenDisk(dirPath); err == nil {
			t.Fatal("directory accepted as database")
		}
		f, err := os.Create(filepath.Join(d, "closed"))
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		db := &DiskBackend{file: f}
		if _, err := db.WriteAt([]byte{1}, 0); err == nil {
			t.Fatal("write on closed descriptor succeeded")
		}
		if err := db.Truncate(0); err == nil {
			t.Fatal("truncate on closed descriptor succeeded")
		}
	})
	t.Run("disk writer error", func(t *testing.T) {
		w := coverageAtWriter{err: errCoverage}
		if _, err := writeDiskFullAt(w, []byte{1}, 0); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
}

type coverageAtWriter struct{ err error }

func (w coverageAtWriter) WriteAt([]byte, int64) (int, error) { return 0, w.err }

func TestCoverage100HelpersCompileUses(t *testing.T) {
	// Keep imports and helper behavior asserted while subsequent WAL/filesystem tests build on them.
	if !bytes.Equal([]byte("a"), []byte("a")) || binary.LittleEndian.Uint16([]byte{1, 0}) != 1 || !strings.Contains("coverage", "cover") {
		t.Fatal("standard helpers")
	}
	var x atomic.Bool
	x.Store(true)
	if !x.Load() {
		t.Fatal("atomic bool")
	}
	_ = time.Millisecond
}

type coverageWriter struct {
	n   int
	err error
}

func (w coverageWriter) Write(p []byte) (int, error) {
	if w.n >= 0 && w.n < len(p) {
		return w.n, w.err
	}
	return len(p), w.err
}

func coverageCipher(t *testing.T) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	c, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func coverageEncodedRecord(t *testing.T, record *WALRecord) []byte {
	t.Helper()
	buf := make([]byte, walHeaderSize+len(record.Data)+4)
	if err := writeRecordHeader(buf, record, len(record.Data)); err != nil {
		t.Fatal(err)
	}
	copy(buf[walHeaderSize:], record.Data)
	binary.LittleEndian.PutUint32(buf[walHeaderSize+len(record.Data):], crc32.ChecksumIEEE(buf[:walHeaderSize+len(record.Data)]))
	return buf
}

func TestCoverage100WALHelpersAndBatch(t *testing.T) {
	t.Run("recovery tracker bounds", func(t *testing.T) {
		var tracker walRecoveryBufferTracker
		if err := tracker.add(nil); !errors.Is(err, ErrInvalidWALRecord) {
			t.Fatalf("nil: %v", err)
		}
		tracker.records = walMaxRecoveryPendingRecords
		if err := tracker.add(&WALRecord{}); !errors.Is(err, ErrWALCorrupted) {
			t.Fatalf("record bound: %v", err)
		}
		tracker.records = 1
		tracker.bytes = walMaxRecoveryPendingBytes
		if err := tracker.add(&WALRecord{Data: []byte{1}}); !errors.Is(err, ErrWALCorrupted) {
			t.Fatalf("byte bound: %v", err)
		}
		tracker.records, tracker.bytes = 1, 0
		tracker.remove([]*WALRecord{nil, {Data: []byte{1}}})
		if tracker.records != 0 || tracker.bytes != 0 {
			t.Fatalf("remove clamp: %+v", tracker)
		}
	})
	t.Run("write full failures", func(t *testing.T) {
		if err := writeWALFull(coverageWriter{n: 0, err: errCoverage}, []byte{1}); !errors.Is(err, errCoverage) {
			t.Fatalf("writer error: %v", err)
		}
		if err := writeWALFull(coverageWriter{n: 0}, []byte{1}); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short write: %v", err)
		}
	})
	t.Run("encrypted length and headers", func(t *testing.T) {
		c := coverageCipher(t)
		if _, err := encryptedRecordDataLen(-1, c); err == nil {
			t.Fatal("negative length accepted")
		}
		if _, err := encryptedRecordDataLen(walMaxRecordDataSize+1, c); err == nil {
			t.Fatal("oversized length accepted")
		}
		if _, err := encryptedRecordDataLen(walMaxRecordDataSize, c); err == nil {
			t.Fatal("encrypted oversized length accepted")
		}
		if err := writeRecordHeader(make([]byte, walHeaderSize), &WALRecord{}, walMaxRecordDataSize+1); err == nil {
			t.Fatal("header oversized length accepted")
		}
		short := []byte{1}
		zeroWALHeaderLSN(short)
		if short[0] != 1 {
			t.Fatal("short header modified")
		}
	})
	t.Run("batch modes and closed WAL", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "batch.wal")
		w, err := OpenWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.AppendBatchWithoutSync([]*WALRecord{{Type: 0xff}}); err == nil {
			t.Fatal("invalid encrypted batch accepted")
		}
		w.EnableGroupCommit(0, 0)
		if err := w.AppendBatch([]*WALRecord{{Type: WALInsert, Data: []byte("x")}}); err != nil {
			t.Fatalf("sync-off batch: %v", err)
		}
		w.DisableGroupCommit()
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := w.AppendBatch([]*WALRecord{{Type: WALInsert}}); !errors.Is(err, ErrWALClosed) {
			t.Fatalf("closed fast batch: %v", err)
		}
		w.SetEncryptionCipher(coverageCipher(t))
		if err := w.AppendBatch([]*WALRecord{{Type: WALInsert, Data: []byte("encrypted")}}); !errors.Is(err, ErrWALClosed) {
			t.Fatalf("closed formatted batch: %v", err)
		}
	})
	t.Run("group commit fallback and failures", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "group.wal")
		w, err := OpenWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.groupCommitAppend(&WALRecord{Type: WALInsert}); err != nil {
			t.Fatalf("disabled fallback: %v", err)
		}
		w.EnableGroupCommit(1, time.Hour)
		if err := w.groupCommitAppend(&WALRecord{Type: WALInsert}); err != nil {
			t.Fatalf("batch trigger: %v", err)
		}
		w.EnableGroupCommit(0, time.Millisecond)
		if w.stopGC == nil {
			t.Fatal("group commit restart did not replace loop")
		}
		w.DisableGroupCommit()
		w.DisableGroupCommit()
		_ = w.Close()
		closed := &WAL{groupCommitEnabled: true}
		if err := closed.groupCommitAppend(&WALRecord{Type: WALInsert}); !errors.Is(err, ErrWALClosed) {
			t.Fatalf("closed group append: %v", err)
		}
		closed.pendingSyncs = []chan error{make(chan error, 1)}
		if err := closed.flushPendingLocked(); !errors.Is(err, ErrWALClosed) {
			t.Fatalf("closed flush: %v", err)
		}
	})
}

func TestCoverage100WALReadOpenAndRecovery(t *testing.T) {
	t.Run("read record malformed and encrypted", func(t *testing.T) {
		w := &WAL{}
		header := make([]byte, walHeaderSize)
		badCRC := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALInsert, Data: []byte("x")})
		badCRC[len(badCRC)-1] ^= 1
		if _, _, err := w.readRecord(bufio.NewReader(bytes.NewReader(badCRC)), header); !errors.Is(err, ErrWALCorrupted) {
			t.Fatalf("CRC: %v", err)
		}
		c := coverageCipher(t)
		w.cipher = c
		shortCipher := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALInsert, Data: []byte{1}})
		if _, _, err := w.readRecord(bufio.NewReader(bytes.NewReader(shortCipher)), header); err == nil {
			t.Fatal("short ciphertext accepted")
		}
	})
	t.Run("open rejects malformed WAL metadata", func(t *testing.T) {
		cases := map[string][]byte{
			"bad crc": func() []byte {
				b := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALInsert})
				b[len(b)-1] ^= 1
				return b
			}(),
			"unknown type": coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: 0xff}),
			"zero lsn":     coverageEncodedRecord(t, &WALRecord{Type: WALInsert}),
			"gap lsn":      append(coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALInsert}), coverageEncodedRecord(t, &WALRecord{LSN: 3, Type: WALInsert})...),
		}
		for name, data := range cases {
			t.Run(name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "bad.wal")
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := OpenWAL(path); err == nil {
					t.Fatal("malformed WAL opened")
				}
			})
		}
	})
	t.Run("partial tail is truncated", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "partial.wal")
		valid := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALCheckpoint})
		if err := os.WriteFile(path, append(valid, 1, 2, 3), 0600); err != nil {
			t.Fatal(err)
		}
		w, err := OpenWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		if w.LSN() != 1 || w.CheckpointLSN() != 1 {
			t.Fatalf("lsn=%d checkpoint=%d", w.LSN(), w.CheckpointLSN())
		}
		if info, _ := os.Stat(path); info.Size() != int64(len(valid)) {
			t.Fatalf("tail not truncated: %d", info.Size())
		}
	})
	t.Run("recover transaction states", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "recover.wal")
		w, err := OpenWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		records := []*WALRecord{
			{TxnID: 1, Type: WALInsert, Data: []byte("logical")},
			{TxnID: 1, Type: WALCommit},
			{TxnID: 2, Type: WALUpdate, Data: []byte("discard")},
			{TxnID: 2, Type: WALRollback},
			{TxnID: 3, Type: WALCommit},
			{TxnID: 3, Type: WALDelete, Data: []byte("postcommit")},
			{TxnID: 4, Type: WALUpdate, Data: []byte("pending")},
			{TxnID: 4, Type: WALUpdateCommit, Data: []byte("combined")},
		}
		for _, r := range records {
			if err := w.AppendWithoutSync(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
		bp := NewBufferPool(4, NewMemory())
		if err := w.Recover(bp); err != nil {
			t.Fatal(err)
		}
		ops := w.GetReplayOps()
		if len(ops) != 4 {
			t.Fatalf("replay ops=%d, want 4: %+v", len(ops), ops)
		}
		ops[0].Data[0] ^= 0xff
		if bytes.Equal(ops[0].Data, w.GetReplayOps()[0].Data) {
			t.Fatal("replay ops alias internal data")
		}
		_ = bp.Close()
		_ = w.Close()
	})
	t.Run("recover validation", func(t *testing.T) {
		w := &WAL{}
		if err := w.Recover(NewBufferPool(1, NewMemory())); !errors.Is(err, ErrWALClosed) {
			t.Fatalf("closed: %v", err)
		}
		path := filepath.Join(t.TempDir(), "nil.wal")
		open, err := OpenWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := open.Recover(nil); !errors.Is(err, ErrInvalidWALRecoveryTarget) {
			t.Fatalf("nil pool: %v", err)
		}
		if err := open.applyRecord(nil, &WALRecord{}); !errors.Is(err, ErrInvalidWALRecoveryTarget) {
			t.Fatalf("apply nil pool: %v", err)
		}
		bp := NewBufferPool(1, NewMemory())
		if err := open.applyRecord(bp, nil); !errors.Is(err, ErrInvalidWALRecord) {
			t.Fatalf("apply nil record: %v", err)
		}
		if err := open.applyRecord(bp, &WALRecord{}); err != nil {
			t.Fatalf("logical apply: %v", err)
		}
		_ = bp.Close()
		_ = open.Close()
	})
}

type coverageErrorReader struct{}

func (coverageErrorReader) Read([]byte) (int, error) { return 0, errCoverage }

func TestCoverage100EncryptionAndSaltEdges(t *testing.T) {
	t.Run("empty salt and invalid sidecars", func(t *testing.T) {
		if err := PersistSalt("", nil); err != nil {
			t.Fatalf("empty salt: %v", err)
		}
		if err := PersistSalt("", []byte{1}); err == nil {
			t.Fatal("empty path accepted")
		}
		if err := PersistSalt("db", make([]byte, maxEncryptionSaltBytes+1)); !errors.Is(err, ErrInvalidSalt) {
			t.Fatalf("large salt: %v", err)
		}
		if _, err := LoadSalt(""); err == nil {
			t.Fatal("empty load path accepted")
		}
		dir := t.TempDir()
		db := filepath.Join(dir, "db")
		if salt, err := LoadSalt(db); err != nil || salt != nil {
			t.Fatalf("missing salt=%v err=%v", salt, err)
		}
		for name, data := range map[string][]byte{
			"marker": []byte("bad"),
			"empty":  []byte(saltFileMarker + "\n"),
			"large":  append([]byte(saltFileMarker+"\n"), make([]byte, maxEncryptionSaltBytes+1)...),
		} {
			t.Run(name, func(t *testing.T) {
				if err := os.WriteFile(db+".salt", data, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := LoadSalt(db); !errors.Is(err, ErrInvalidSalt) {
					t.Fatalf("got %v", err)
				}
			})
		}
	})
	t.Run("salt file type checks", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target")
		if err := os.WriteFile(target, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(dir, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := readSaltFile(link); err == nil {
			t.Fatal("salt symlink accepted")
		}
		if _, err := readSaltFile(dir); err == nil {
			t.Fatal("salt directory accepted")
		}
		large := filepath.Join(dir, "large")
		if err := os.WriteFile(large, make([]byte, len(saltFileMarker)+2+maxEncryptionSaltBytes), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readSaltFile(large); !errors.Is(err, ErrInvalidSalt) {
			t.Fatalf("large file: %v", err)
		}
	})
	t.Run("atomic directory validation", func(t *testing.T) {
		if err := prepareAtomicFileDir("."); err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		nested := filepath.Join(root, "a", "b")
		if err := prepareAtomicFileDir(nested); err != nil {
			t.Fatal(err)
		}
		if info, err := os.Stat(nested); err != nil || !info.IsDir() {
			t.Fatalf("nested info=%v err=%v", info, err)
		}
		file := filepath.Join(root, "file")
		if err := os.WriteFile(file, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := prepareAtomicFileDir(file); err == nil {
			t.Fatal("regular file accepted as directory")
		}
		link := filepath.Join(root, "link")
		if err := os.Symlink(nested, link); err != nil {
			t.Fatal(err)
		}
		if err := prepareAtomicFileDir(link); err == nil {
			t.Fatal("symlink directory accepted")
		}
		if err := syncDir(file); err == nil {
			t.Fatal("synced regular file as directory")
		}
		if err := syncDir(link); err == nil {
			t.Fatal("synced symlink directory")
		}
		if err := rejectStoragePathSymlinkComponents(filepath.Join(link, "child"), "test"); err == nil {
			t.Fatal("symlink component accepted")
		}
		if err := rejectStoragePathSymlinkComponents(filepath.Join(root, "missing", "child"), "test"); err != nil {
			t.Fatalf("missing component: %v", err)
		}
	})
	t.Run("atomic write failures and short writer", func(t *testing.T) {
		root := t.TempDir()
		dirTarget := filepath.Join(root, "target")
		if err := os.Mkdir(dirTarget, 0700); err != nil {
			t.Fatal(err)
		}
		if err := writeFileAtomic(dirTarget, []byte("x"), 0600); err == nil {
			t.Fatal("renamed over directory")
		}
		if _, err := writeFileFull(coverageWriter{n: 0, err: errCoverage}, []byte{1}); !errors.Is(err, errCoverage) {
			t.Fatalf("writer: %v", err)
		}
		if _, err := writeFileFull(coverageWriter{n: 0}, []byte{1}); !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short: %v", err)
		}
	})
	t.Run("encryption random failures and pool forms", func(t *testing.T) {
		old := rand.Reader
		rand.Reader = coverageErrorReader{}
		defer func() { rand.Reader = old }()
		if _, err := NewEncryptedBackend(NewMemory(), &EncryptionConfig{Enabled: true, Key: []byte("key")}); !errors.Is(err, ErrKeyDerivation) {
			t.Fatalf("salt random: %v", err)
		}
		cfg := &EncryptionConfig{Enabled: true, Key: []byte("key"), Salt: []byte("1234567890123456"), PBKDF2Iters: 1}
		eb, err := NewEncryptedBackend(NewMemory(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := eb.WriteAt(make([]byte, PageSize), 0); !errors.Is(err, ErrEncryptionFailed) {
			t.Fatalf("nonce random: %v", err)
		}
	})
	t.Run("read pools, short ciphertext, empty salt", func(t *testing.T) {
		cfg := &EncryptionConfig{Enabled: true, Key: []byte("key"), Salt: []byte("1234567890123456"), PBKDF2Iters: 1}
		backend := &coverageBackend{data: []byte{1}}
		eb, err := NewEncryptedBackend(backend, cfg)
		if err != nil {
			t.Fatal(err)
		}
		eb.readPool.Put([]byte{})
		if _, err := eb.ReadAt(make([]byte, PageSize), 0); !errors.Is(err, io.EOF) {
			t.Fatalf("short ciphertext: %v", err)
		}
		eb.config.Salt = nil
		if eb.GetSalt() != nil {
			t.Fatal("empty salt returned nonnil")
		}
	})
}
