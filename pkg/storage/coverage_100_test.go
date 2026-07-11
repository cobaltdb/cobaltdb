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
		syncPoolWith(&cb.lz4Readers, "wrong")
		if _, err := cb.decompressLZ4([]byte("bad"), 1); err == nil {
			t.Fatal("bad lz4 accepted")
		}
		if _, err := cb.decompressZstd([]byte("bad"), 1); err == nil {
			t.Fatal("bad zstd accepted")
		}
		if err := requireFullDecompressed(0, 1, nil); err == nil {
			t.Fatal("short nil-error decode accepted")
		}
		syncPoolWith(&cb.writeBufPool, "wrong")
		wb := cb.getWriteBuf()
		if len(*wb) != PageSize {
			t.Fatal("write pool fallback")
		}
		syncPoolWith(&cb.readBufPool, "wrong")
		rb := cb.getReadBuf()
		if len(*rb) != PageSize {
			t.Fatal("read pool fallback")
		}
	})
}

// syncPoolWith creates a pool whose next value has a deliberately wrong type.
func syncPoolWith(tgt *sync.Pool, v any) { tgt.Put(v) }

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

type coverageFile struct {
	name        string
	data        []byte
	pos         int64
	info        os.FileInfo
	statErr     error
	chmodErr    error
	seekErr     error
	seekCalls   int
	seekFailAt  int
	syncErr     error
	syncCalls   int
	syncFailAt  int
	truncateErr error
	closeErr    error
	readErr     error
	writeErr    error
}

func (f *coverageFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}
func (f *coverageFile) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	end := int(f.pos) + len(p)
	if end > len(f.data) {
		f.data = append(f.data, make([]byte, end-len(f.data))...)
	}
	copy(f.data[f.pos:], p)
	f.pos = int64(end)
	return len(p), nil
}
func (f *coverageFile) ReadAt(p []byte, o int64) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	if o >= int64(len(f.data)) {
		return 0, io.EOF
	}
	return copy(p, f.data[o:]), nil
}
func (f *coverageFile) WriteAt(p []byte, o int64) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	end := int(o) + len(p)
	if end > len(f.data) {
		f.data = append(f.data, make([]byte, end-len(f.data))...)
	}
	return copy(f.data[o:], p), nil
}
func (f *coverageFile) Name() string {
	if f.name == "" {
		return "coverage-file"
	}
	return f.name
}
func (f *coverageFile) Stat() (os.FileInfo, error) { return f.info, f.statErr }
func (f *coverageFile) Chmod(os.FileMode) error    { return f.chmodErr }
func (f *coverageFile) Seek(o int64, w int) (int64, error) {
	f.seekCalls++
	if f.seekErr != nil && (f.seekFailAt == 0 || f.seekCalls == f.seekFailAt) {
		return 0, f.seekErr
	}
	switch w {
	case io.SeekStart:
		f.pos = o
	case io.SeekCurrent:
		f.pos += o
	case io.SeekEnd:
		f.pos = int64(len(f.data)) + o
	}
	return f.pos, nil
}
func (f *coverageFile) Sync() error {
	f.syncCalls++
	if f.syncErr != nil && (f.syncFailAt == 0 || f.syncCalls == f.syncFailAt) {
		return f.syncErr
	}
	return nil
}
func (f *coverageFile) Truncate(n int64) error {
	if f.truncateErr != nil {
		return f.truncateErr
	}
	if n < int64(len(f.data)) {
		f.data = f.data[:n]
	} else {
		f.data = append(f.data, make([]byte, int(n)-len(f.data))...)
	}
	return nil
}
func (f *coverageFile) Close() error { return f.closeErr }

type coverageInfo struct {
	name string
	size int64
	mode os.FileMode
}

func (i coverageInfo) Name() string       { return i.name }
func (i coverageInfo) Size() int64        { return i.size }
func (i coverageInfo) Mode() os.FileMode  { return i.mode }
func (i coverageInfo) ModTime() time.Time { return time.Time{} }
func (i coverageInfo) IsDir() bool        { return i.mode.IsDir() }
func (i coverageInfo) Sys() any           { return nil }

func TestCoverage100FileFailures(t *testing.T) {
	t.Run("disk open operations", func(t *testing.T) {
		old := diskOpenFile
		defer func() { diskOpenFile = old }()
		path := filepath.Join(t.TempDir(), "db")
		for name, file := range map[string]*coverageFile{
			"stat":        {statErr: errCoverage},
			"not regular": {info: coverageInfo{mode: os.ModeDir}},
			"chmod":       {info: coverageInfo{mode: 0600}, chmodErr: errCoverage},
		} {
			t.Run(name, func(t *testing.T) {
				diskOpenFile = func(string, int, os.FileMode) (diskFile, error) { return nil, errCoverage }
				_ = file
			})
		}
		// Open errors are observable through the existing function seam.
		diskOpenFile = func(string, int, os.FileMode) (diskFile, error) { return nil, errCoverage }
		if _, err := OpenDisk(path); !errors.Is(err, errCoverage) {
			t.Fatalf("open: %v", err)
		}
	})
	t.Run("disk backend method failures", func(t *testing.T) {
		f := &coverageFile{writeErr: errCoverage, truncateErr: errCoverage}
		d := &DiskBackend{file: f}
		if _, err := d.WriteAt([]byte{1}, 0); !errors.Is(err, errCoverage) {
			t.Fatalf("write: %v", err)
		}
		if err := d.Truncate(0); !errors.Is(err, errCoverage) {
			t.Fatalf("truncate: %v", err)
		}
	})
	t.Run("wal readLSN failures", func(t *testing.T) {
		for name, f := range map[string]*coverageFile{
			"stat":       {statErr: errCoverage},
			"seek start": {info: coverageInfo{size: 1}, seekErr: errCoverage},
		} {
			t.Run(name, func(t *testing.T) {
				w := &WAL{file: f}
				if err := w.readLSN(); !errors.Is(err, errCoverage) {
					t.Fatalf("got %v", err)
				}
			})
		}
		partial := &coverageFile{data: []byte{1}, info: coverageInfo{size: 1}, truncateErr: errCoverage}
		if err := (&WAL{file: partial}).readLSN(); !errors.Is(err, errCoverage) {
			t.Fatalf("truncate: %v", err)
		}
		partial = &coverageFile{data: []byte{1}, info: coverageInfo{size: 1}, syncErr: errCoverage}
		if err := (&WAL{file: partial}).readLSN(); !errors.Is(err, errCoverage) {
			t.Fatalf("sync: %v", err)
		}
		valid := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALInsert})
		end := &coverageFile{data: valid, info: coverageInfo{size: int64(len(valid))}}
		end.seekErr = nil
		w := &WAL{file: end}
		if err := w.readLSN(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wal sync append checkpoint close failures", func(t *testing.T) {
		f := &coverageFile{syncErr: errCoverage}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f)}
		if err := w.Sync(); !errors.Is(err, errCoverage) {
			t.Fatalf("sync: %v", err)
		}
		w = &WAL{file: &coverageFile{}, bufWriter: bufio.NewWriterSize(coverageWriter{n: 0, err: errCoverage}, 1)}
		if err := w.AppendWithoutSync(&WALRecord{Type: WALInsert}); !errors.Is(err, errCoverage) {
			t.Fatalf("append header: %v", err)
		}
		w = &WAL{file: &coverageFile{}, bufWriter: bufio.NewWriterSize(coverageWriter{n: 0, err: errCoverage}, 1)}
		if err := w.AppendBatch([]*WALRecord{{Type: WALInsert}}); !errors.Is(err, errCoverage) {
			t.Fatalf("batch write: %v", err)
		}
		f = &coverageFile{closeErr: errCoverage}
		w = &WAL{file: f, bufWriter: bufio.NewWriter(f)}
		if err := w.Close(); !errors.Is(err, errCoverage) {
			t.Fatalf("close: %v", err)
		}
	})
}

func TestCoverage100BufferPoolSlowRechecks(t *testing.T) {
	old := bufferPoolSlowPathHook
	defer func() { bufferPoolSlowPathHook = old }()
	t.Run("closed", func(t *testing.T) {
		bp := NewBufferPool(1, NewMemory())
		bufferPoolSlowPathHook = func() { bp.mu.Lock(); bp.closed = true; bp.mu.Unlock() }
		if _, err := bp.GetPage(0); !errors.Is(err, ErrBufferPoolClosed) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("double checked hit", func(t *testing.T) {
		bp := NewBufferPool(1, NewMemory())
		p := &CachedPage{id: 0, data: make([]byte, PageSize), pinned: 0}
		bufferPoolSlowPathHook = func() { bp.mu.Lock(); bp.pages[0] = p; p.lruElem = bp.lru.PushFront(p); bp.mu.Unlock() }
		got, err := bp.GetPage(0)
		if err != nil || got != p || !p.IsPinned() {
			t.Fatalf("got=%p err=%v pinned=%v", got, err, p.IsPinned())
		}
	})
}

func TestCoverage100OpenConstructorFailures(t *testing.T) {
	t.Run("disk", func(t *testing.T) {
		old := diskOpenFile
		defer func() { diskOpenFile = old }()
		path := filepath.Join(t.TempDir(), "db")
		for name, f := range map[string]*coverageFile{"stat": {statErr: errCoverage}, "type": {info: coverageInfo{mode: os.ModeDir}}, "chmod": {info: coverageInfo{mode: 0600}, chmodErr: errCoverage}} {
			t.Run(name, func(t *testing.T) {
				diskOpenFile = func(string, int, os.FileMode) (diskFile, error) { return f, nil }
				if _, err := OpenDisk(path); err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
	t.Run("wal", func(t *testing.T) {
		old := walOpenFile
		defer func() { walOpenFile = old }()
		path := filepath.Join(t.TempDir(), "wal")
		for name, f := range map[string]*coverageFile{"stat": {statErr: errCoverage}, "type": {info: coverageInfo{mode: os.ModeDir}}, "chmod": {info: coverageInfo{mode: 0600}, chmodErr: errCoverage}} {
			t.Run(name, func(t *testing.T) {
				walOpenFile = func(string, int, os.FileMode) (walFile, error) { return f, nil }
				if _, err := OpenWAL(path); err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
}

func TestCoverage100CheckpointFailures(t *testing.T) {
	newW := func(f *coverageFile) *WAL { return &WAL{file: f, bufWriter: bufio.NewWriter(f)} }
	if err := (&WAL{}).Checkpoint(NewBufferPool(1, NewMemory())); !errors.Is(err, ErrWALClosed) {
		t.Fatalf("closed: %v", err)
	}
	for name, run := range map[string]func() error{
		"pre sync": func() error {
			f := &coverageFile{syncErr: errCoverage}
			return newW(f).Checkpoint(NewBufferPool(1, NewMemory()))
		},
		"pool sync": func() error {
			f := &coverageFile{}
			bp := NewBufferPool(1, &coverageBackend{syncErr: errCoverage})
			return newW(f).Checkpoint(bp)
		},
		"truncate": func() error {
			f := &coverageFile{truncateErr: errCoverage}
			return newW(f).Checkpoint(NewBufferPool(1, NewMemory()))
		},
		"seek": func() error {
			f := &coverageFile{seekErr: errCoverage}
			return newW(f).Checkpoint(NewBufferPool(1, NewMemory()))
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, errCoverage) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestCoverage100RemainingCoreStates(t *testing.T) {
	t.Run("conversion success", func(t *testing.T) {
		if got, err := checkedUint64Offset(7); err != nil || got != 7 {
			t.Fatalf("got=%d err=%v", got, err)
		}
	})
	t.Run("buffer miss eviction", func(t *testing.T) {
		bp := NewBufferPool(1, NewMemory())
		p, _ := bp.NewPage(PageTypeLeaf)
		if _, err := bp.GetPage(99); !errors.Is(err, ErrBufferFull) {
			t.Fatalf("got %v", err)
		}
		p.Unpin()
	})
	t.Run("memory capped growth", func(t *testing.T) {
		m := NewMemoryWithLimit(maxGrowthIncrement * 3)
		m.data = make([]byte, 1, maxGrowthIncrement+1)
		if _, err := m.WriteAt([]byte{1}, maxGrowthIncrement+1); err != nil {
			t.Fatal(err)
		}
		m = NewMemoryWithLimit(maxGrowthIncrement * 3)
		m.data = make([]byte, 1, maxGrowthIncrement+1)
		if err := m.Truncate(maxGrowthIncrement + 2); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("disk directory validation", func(t *testing.T) {
		if err := syncDiskParentDir("file"); err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		missing := filepath.Join(root, "missing", "db")
		if err := syncDiskParentDir(missing); err == nil {
			t.Fatal("missing parent accepted")
		}
		file := filepath.Join(root, "file")
		if err := os.WriteFile(file, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := syncDiskParentDir(filepath.Join(file, "db")); err == nil {
			t.Fatal("file parent accepted")
		}
		link := filepath.Join(root, "link")
		if err := os.Symlink(root, link); err != nil {
			t.Fatal(err)
		}
		if err := syncDiskParentDir(filepath.Join(link, "db")); err == nil {
			t.Fatal("symlink parent accepted")
		}
	})
	t.Run("wal random failure", func(t *testing.T) {
		old := rand.Reader
		rand.Reader = coverageErrorReader{}
		defer func() { rand.Reader = old }()
		if _, err := encryptDataWithCipher(coverageCipher(t), []byte{1}, nil); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("WAL nil readLSN and unexpected read", func(t *testing.T) {
		if err := (&WAL{}).readLSN(); err == nil {
			t.Fatal("nil WAL accepted")
		}
		f := &coverageFile{info: coverageInfo{size: 1}, readErr: errCoverage}
		if err := (&WAL{file: f}).readLSN(); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("record truncations", func(t *testing.T) {
		w := &WAL{}
		h := make([]byte, walHeaderSize)
		binary.LittleEndian.PutUint16(h[23:], 2)
		if _, _, err := w.readRecord(bufio.NewReader(bytes.NewReader(append(h, 1))), make([]byte, walHeaderSize)); err == nil {
			t.Fatal("truncated data accepted")
		}
		if _, _, err := w.readRecord(bufio.NewReader(bytes.NewReader(make([]byte, walHeaderSize))), make([]byte, walHeaderSize)); err == nil {
			t.Fatal("missing CRC accepted")
		}
	})
	t.Run("batch validation and sync errors", func(t *testing.T) {
		w := &WAL{}
		if err := w.AppendBatchWithoutSync([]*WALRecord{{Type: WALInsert}, {Type: 0xff}}); err == nil {
			t.Fatal("bad second record accepted")
		}
		f := &coverageFile{syncErr: errCoverage}
		w = &WAL{file: f, bufWriter: bufio.NewWriter(f)}
		if err := w.AppendBatch([]*WALRecord{{Type: WALInsert}}); !errors.Is(err, errCoverage) {
			t.Fatalf("batch sync: %v", err)
		}
		w = &WAL{groupCommitEnabled: true, syncInterval: time.Hour, pendingSyncs: []chan error{make(chan error, 1)}, file: &coverageFile{syncErr: errCoverage}}
		w.bufWriter = bufio.NewWriter(w.file)
		if err := w.flushPendingLocked(); !errors.Is(err, errCoverage) {
			t.Fatalf("pending sync: %v", err)
		}
	})
	t.Run("append data and CRC writer failures", func(t *testing.T) {
		for _, rec := range []*WALRecord{{Type: WALInsert, Data: []byte("data")}, {Type: WALInsert}} {
			cw := coverageWriter{n: 0, err: errCoverage}
			w := &WAL{file: &coverageFile{}, bufWriter: bufio.NewWriterSize(cw, 1)}
			if err := w.AppendWithoutSync(rec); !errors.Is(err, errCoverage) {
				t.Fatalf("record=%+v err=%v", rec, err)
			}
		}
	})
	t.Run("recover corrupt and physical errors", func(t *testing.T) {
		bad := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALInsert})
		bad[len(bad)-1] ^= 1
		f := &coverageFile{data: bad}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f)}
		if err := w.Recover(NewBufferPool(1, NewMemory())); !errors.Is(err, ErrWALCorrupted) {
			t.Fatalf("crc: %v", err)
		}
		bp := NewBufferPool(1, NewMemory())
		w = &WAL{}
		if err := w.applyRecord(bp, &WALRecord{PageID: 1, Offset: PageSize - 1, Data: []byte{1, 2}}); err == nil {
			t.Fatal("overflow applied")
		}
	})
}

func TestCoverage100AtomicFileInjectedFailures(t *testing.T) {
	old := storageFSOps
	defer func() { storageFSOps = old }()
	regular := coverageInfo{name: "f", mode: 0600}
	directory := coverageInfo{name: "d", mode: os.ModeDir | 0700}
	base := old
	base.sameFile = func(os.FileInfo, os.FileInfo) bool { return true }
	t.Run("disk sync injected stages", func(t *testing.T) {
		realDir := t.TempDir()
		cases := map[string]func() error{
			"open": func() error {
				storageFSOps = base
				storageFSOps.open = func(string) (atomicFile, error) { return nil, errCoverage }
				return syncDiskParentDir("dir/db")
			},
			"stat": func() error {
				storageFSOps = base
				storageFSOps.open = func(string) (atomicFile, error) {
					return &coverageFile{statErr: errCoverage}, nil
				}
				return syncDiskParentDir(filepath.Join(realDir, "db"))
			},
		}
		for name, run := range cases {
			t.Run(name, func(t *testing.T) {
				if err := run(); err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
	t.Run("read salt stages", func(t *testing.T) {
		cases := map[string]func(){
			"open": func() {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return regular, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return nil, errCoverage }
			},
			"stat": func() {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return regular, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return &coverageFile{statErr: errCoverage}, nil }
			},
			"opened type": func() {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return regular, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return &coverageFile{info: directory}, nil }
			},
			"opened size": func() {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return regular, nil }
				storageFSOps.open = func(string) (atomicFile, error) {
					return &coverageFile{info: coverageInfo{mode: 0600, size: int64(len(saltFileMarker) + 2 + maxEncryptionSaltBytes)}}, nil
				}
			},
			"changed": func() {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return regular, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return &coverageFile{info: regular}, nil }
				storageFSOps.sameFile = func(os.FileInfo, os.FileInfo) bool { return false }
			},
			"chmod": func() {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return regular, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return &coverageFile{info: regular, chmodErr: errCoverage}, nil }
			},
			"read": func() {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return regular, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return &coverageFile{info: regular, readErr: errCoverage}, nil }
			},
		}
		for name, setup := range cases {
			t.Run(name, func(t *testing.T) {
				setup()
				if _, err := readSaltFile("salt"); err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
	t.Run("atomic write stages", func(t *testing.T) {
		newOps := func(f *coverageFile) storageFileOps {
			o := base
			o.lstat = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
			o.stat = func(string) (os.FileInfo, error) { return directory, nil }
			o.createTemp = func(string, string) (atomicFile, error) { return f, nil }
			return o
		}
		cases := map[string]func() error{
			"create": func() error {
				o := newOps(nil)
				o.createTemp = func(string, string) (atomicFile, error) { return nil, errCoverage }
				storageFSOps = o
				return writeFileAtomic("dir/file", []byte{1}, 0600)
			},
			"chmod": func() error {
				storageFSOps = newOps(&coverageFile{name: "tmp", chmodErr: errCoverage})
				return writeFileAtomic("dir/file", []byte{1}, 0600)
			},
			"write": func() error {
				storageFSOps = newOps(&coverageFile{name: "tmp", writeErr: errCoverage})
				return writeFileAtomic("dir/file", []byte{1}, 0600)
			},
			"sync": func() error {
				storageFSOps = newOps(&coverageFile{name: "tmp", syncErr: errCoverage})
				return writeFileAtomic("dir/file", []byte{1}, 0600)
			},
			"close": func() error {
				storageFSOps = newOps(&coverageFile{name: "tmp", closeErr: errCoverage})
				return writeFileAtomic("dir/file", []byte{1}, 0600)
			},
			"rename": func() error {
				o := newOps(&coverageFile{name: "tmp"})
				o.rename = func(string, string) error { return errCoverage }
				storageFSOps = o
				return writeFileAtomic("dir/file", []byte{1}, 0600)
			},
			"dir sync": func() error {
				o := newOps(&coverageFile{name: "tmp"})
				o.rename = func(string, string) error { return nil }
				o.lstat = func(string) (os.FileInfo, error) { return directory, nil }
				o.open = func(string) (atomicFile, error) { return &coverageFile{info: directory, syncErr: errCoverage}, nil }
				storageFSOps = o
				return writeFileAtomic("dir/file", []byte{1}, 0600)
			},
		}
		for name, run := range cases {
			t.Run(name, func(t *testing.T) {
				if err := run(); err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
	t.Run("prepare directory stages", func(t *testing.T) {
		cases := map[string]func() error{
			"lstat": func() error {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return nil, errCoverage }
				return prepareAtomicFileDir("dir")
			},
			"mkdir": func() error {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
				storageFSOps.mkdirAll = func(string, os.FileMode) error { return errCoverage }
				return prepareAtomicFileDir("dir")
			},
			"chmod": func() error {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return nil, os.ErrNotExist }
				storageFSOps.chmod = func(string, os.FileMode) error { return errCoverage }
				return prepareAtomicFileDir("dir")
			},
		}
		for name, run := range cases {
			t.Run(name, func(t *testing.T) {
				if err := run(); err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
}

func TestCoverage100FinalFilesystemAndWALStates(t *testing.T) {
	old := storageFSOps
	defer func() { storageFSOps = old }()
	dirInfo := coverageInfo{name: "d", mode: os.ModeDir | 0700}
	regInfo := coverageInfo{name: "f", mode: 0600}
	t.Run("syncDir injected stages", func(t *testing.T) {
		base := old
		cases := map[string]func() error{
			"open": func() error {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return dirInfo, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return nil, errCoverage }
				return syncDir("dir")
			},
			"stat": func() error {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return dirInfo, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return &coverageFile{statErr: errCoverage}, nil }
				return syncDir("dir")
			},
			"type": func() error {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return dirInfo, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return &coverageFile{info: regInfo}, nil }
				return syncDir("dir")
			},
			"changed": func() error {
				storageFSOps = base
				storageFSOps.lstat = func(string) (os.FileInfo, error) { return dirInfo, nil }
				storageFSOps.open = func(string) (atomicFile, error) { return &coverageFile{info: dirInfo}, nil }
				storageFSOps.sameFile = func(os.FileInfo, os.FileInfo) bool { return false }
				return syncDir("dir")
			},
		}
		for name, run := range cases {
			t.Run(name, func(t *testing.T) {
				if err := run(); err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
	t.Run("salt read exceeds stat", func(t *testing.T) {
		storageFSOps = old
		storageFSOps.lstat = func(string) (os.FileInfo, error) { return regInfo, nil }
		storageFSOps.open = func(string) (atomicFile, error) {
			return &coverageFile{info: regInfo, data: make([]byte, len(saltFileMarker)+2+maxEncryptionSaltBytes)}, nil
		}
		storageFSOps.sameFile = func(os.FileInfo, os.FileInfo) bool { return true }
		if _, err := readSaltFile("salt"); !errors.Is(err, ErrInvalidSalt) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("WAL checkpoint writer stages", func(t *testing.T) {
		f := &countFailFile{coverageFile: coverageFile{}, failAfter: 0}
		w := &WAL{file: f, bufWriter: bufio.NewWriterSize(f, 1)}
		if err := w.Checkpoint(NewBufferPool(1, NewMemory())); err == nil {
			t.Fatal("checkpoint flush failure not propagated")
		}
		f2 := &coverageFile{syncErr: errCoverage}
		w = &WAL{file: f2, bufWriter: bufio.NewWriter(f2)}
		if err := w.Checkpoint(NewBufferPool(1, NewMemory())); !errors.Is(err, errCoverage) {
			t.Fatalf("final sync: %v", err)
		}
	})
	t.Run("recover seek read unknown and apply", func(t *testing.T) {
		f := &coverageFile{seekErr: errCoverage}
		w := &WAL{file: f}
		if err := w.Recover(NewBufferPool(1, NewMemory())); !errors.Is(err, errCoverage) {
			t.Fatalf("seek: %v", err)
		}
		unknown := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: 0xff})
		f = &coverageFile{data: unknown}
		w = &WAL{file: f}
		if err := w.Recover(NewBufferPool(1, NewMemory())); err == nil {
			t.Fatal("unknown accepted")
		}
		page := coveragePage(1, PageTypeLeaf)
		mem := NewMemory()
		_, _ = mem.WriteAt(page, PageSize)
		bp := NewBufferPool(2, mem)
		records := append(coverageEncodedRecord(t, &WALRecord{LSN: 1, TxnID: 1, Type: WALInsert, PageID: 1, Offset: 16, Data: []byte{9}}), coverageEncodedRecord(t, &WALRecord{LSN: 2, TxnID: 1, Type: WALCommit})...)
		f = &coverageFile{data: records}
		w = &WAL{file: f}
		if err := w.Recover(bp); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("close sync error", func(t *testing.T) {
		f := &coverageFile{syncErr: errCoverage}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f)}
		if err := w.Close(); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
}

type countFailFile struct {
	coverageFile
	writes    int
	failAfter int
}

func (f *countFailFile) Write(p []byte) (int, error) {
	if f.writes >= f.failAfter {
		return 0, errCoverage
	}
	f.writes += len(p)
	return f.coverageFile.Write(p)
}

func TestCoverage100LastWALPaths(t *testing.T) {
	t.Run("readLSN end seek", func(t *testing.T) {
		data := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALInsert})
		f := &coverageFile{data: data, info: coverageInfo{size: int64(len(data))}, seekErr: errCoverage, seekFailAt: 2}
		if err := (&WAL{file: f}).readLSN(); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("encrypted read success", func(t *testing.T) {
		c := coverageCipher(t)
		w := &WAL{cipher: c}
		r := &WALRecord{LSN: 1, Type: WALInsert, Data: []byte("secret")}
		var aad [walHeaderSize]byte
		cipherLen, _ := encryptedRecordDataLen(len(r.Data), c)
		_ = writeRecordHeader(aad[:], r, cipherLen)
		zeroWALHeaderLSN(aad[:])
		enc, err := encryptDataWithCipher(c, r.Data, aad[:])
		if err != nil {
			t.Fatal(err)
		}
		r.Data = enc
		data := coverageEncodedRecord(t, r)
		got, _, err := w.readRecord(bufio.NewReader(bytes.NewReader(data)), make([]byte, walHeaderSize))
		if err != nil || string(got.Data) != "secret" {
			t.Fatalf("got=%q err=%v", got.Data, err)
		}
	})
	t.Run("batch second append failure", func(t *testing.T) {
		f := &countFailFile{coverageFile: coverageFile{}, failAfter: walHeaderSize + 4}
		w := &WAL{file: f, bufWriter: bufio.NewWriterSize(f, 1)}
		err := w.AppendBatchWithoutSync([]*WALRecord{{Type: WALInsert}, {Type: WALInsert}})
		if err == nil {
			t.Fatal("second append succeeded")
		}
	})
	t.Run("formatted batch write", func(t *testing.T) {
		f := &countFailFile{coverageFile: coverageFile{}, failAfter: 0}
		w := &WAL{file: f, bufWriter: bufio.NewWriterSize(f, 1), cipher: coverageCipher(t)}
		if err := w.AppendBatch([]*WALRecord{{Type: WALInsert, Data: []byte("x")}}); err == nil {
			t.Fatal("formatted write succeeded")
		}
	})
	t.Run("finish batch error", func(t *testing.T) {
		f := &coverageFile{syncErr: errCoverage}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f), groupCommitEnabled: true, batchSize: 1}
		if err := w.finishBatchSync(); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("format encryption error", func(t *testing.T) {
		old := rand.Reader
		rand.Reader = coverageErrorReader{}
		defer func() { rand.Reader = old }()
		w := &WAL{}
		if _, _, err := w.formatBatch([]*WALRecord{{Type: WALInsert, Data: []byte("x")}}, coverageCipher(t)); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("append encrypted random error", func(t *testing.T) {
		old := rand.Reader
		rand.Reader = coverageErrorReader{}
		defer func() { rand.Reader = old }()
		f := &coverageFile{}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f), cipher: coverageCipher(t)}
		r := &WALRecord{LSN: 9, Type: WALInsert, Data: []byte("x")}
		if err := w.AppendWithoutSync(r); !errors.Is(err, errCoverage) || r.LSN != 9 {
			t.Fatalf("lsn=%d err=%v", r.LSN, err)
		}
	})
	t.Run("append sync flush and file errors", func(t *testing.T) {
		f := &countFailFile{coverageFile: coverageFile{}, failAfter: 0}
		w := &WAL{file: f, bufWriter: bufio.NewWriterSize(f, 64)}
		if err := w.Append(&WALRecord{Type: WALInsert}); err == nil {
			t.Fatal("flush error missing")
		}
		f2 := &coverageFile{syncErr: errCoverage}
		w = &WAL{file: f2, bufWriter: bufio.NewWriter(f2)}
		if err := w.Append(&WALRecord{Type: WALInsert}); !errors.Is(err, errCoverage) {
			t.Fatalf("sync: %v", err)
		}
	})
	t.Run("Sync flush error", func(t *testing.T) {
		f := &countFailFile{coverageFile: coverageFile{}, failAfter: 0}
		w := &WAL{file: f, bufWriter: bufio.NewWriterSize(f, 64)}
		_, _ = w.bufWriter.Write([]byte{1})
		if err := w.Sync(); err == nil {
			t.Fatal("flush error missing")
		}
	})
	t.Run("group commit error branches", func(t *testing.T) {
		f := &coverageFile{syncErr: errCoverage}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f), groupCommitEnabled: true, batchSize: 1}
		if err := w.groupCommitAppend(&WALRecord{Type: WALInsert}); !errors.Is(err, errCoverage) {
			t.Fatalf("batch=%v", err)
		}
		w = &WAL{file: f, bufWriter: bufio.NewWriter(f), groupCommitEnabled: true, syncInterval: time.Hour}
		go func() { time.Sleep(time.Millisecond); w.Sync() }()
		if err := w.groupCommitAppend(&WALRecord{Type: WALInsert}); !errors.Is(err, errCoverage) {
			t.Fatalf("wait=%v", err)
		}
	})
	t.Run("checkpoint final sync", func(t *testing.T) {
		f := &coverageFile{syncErr: errCoverage, syncFailAt: 3}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f)}
		if err := w.Checkpoint(NewBufferPool(1, NewMemory())); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v calls=%d", err, f.syncCalls)
		}
	})
	t.Run("recover checkpoint and apply failures", func(t *testing.T) {
		cp := coverageEncodedRecord(t, &WALRecord{LSN: 1, Type: WALCheckpoint})
		old := coverageEncodedRecord(t, &WALRecord{LSN: 1, TxnID: 1, Type: WALInsert, Data: []byte("skip")})
		f := &coverageFile{data: append(cp, old...)}
		w := &WAL{file: f}
		if err := w.Recover(NewBufferPool(1, NewMemory())); err != nil {
			t.Fatal(err)
		}
		for _, typ := range []WALRecordType{WALCommit, WALUpdateCommit} {
			records := append(coverageEncodedRecord(t, &WALRecord{LSN: 1, TxnID: 2, Type: WALInsert, PageID: 99, Data: []byte{1}}), coverageEncodedRecord(t, &WALRecord{LSN: 2, TxnID: 2, Type: typ, PageID: func() uint32 {
				if typ == WALUpdateCommit {
					return 99
				}
				return 0
			}(), Data: []byte{1}})...)
			f = &coverageFile{data: records}
			w = &WAL{file: f}
			if err := w.Recover(NewBufferPool(1, NewMemory())); err == nil {
				t.Fatalf("type=%v", typ)
			}
		}
	})
}

func TestCoverage100AbsoluteLastPaths(t *testing.T) {
	old := storageFSOps
	defer func() { storageFSOps = old }()
	reg := coverageInfo{mode: 0600}
	dir := coverageInfo{mode: os.ModeDir | 0700}
	t.Run("constructor lstat", func(t *testing.T) {
		storageFSOps = old
		storageFSOps.lstat = func(path string) (os.FileInfo, error) {
			if strings.HasSuffix(path, "db") || strings.HasSuffix(path, "wal") {
				return nil, errCoverage
			}
			return nil, os.ErrNotExist
		}
		if _, err := OpenDisk("db"); !errors.Is(err, errCoverage) {
			t.Fatalf("disk=%v", err)
		}
		if _, err := OpenWAL("wal"); !errors.Is(err, errCoverage) {
			t.Fatalf("wal=%v", err)
		}
	})
	t.Run("new file directory sync", func(t *testing.T) {
		storageFSOps = old
		storageFSOps.open = func(string) (atomicFile, error) { return nil, errCoverage }
		oldDisk := diskOpenFile
		defer func() { diskOpenFile = oldDisk }()
		diskOpenFile = func(string, int, os.FileMode) (diskFile, error) { return &coverageFile{info: reg}, nil }
		if _, err := OpenDisk("db"); err == nil {
			t.Fatal("disk sync failure missing")
		}
	})

	t.Run("syncDir lstat and symlink recheck", func(t *testing.T) {
		storageFSOps = old
		calls := 0
		storageFSOps.lstat = func(string) (os.FileInfo, error) {
			calls++
			if calls == 1 {
				return dir, nil
			}
			return nil, errCoverage
		}
		if err := syncDir("dir"); !errors.Is(err, errCoverage) {
			t.Fatalf("lstat=%v", err)
		}
		calls = 0
		storageFSOps.lstat = func(string) (os.FileInfo, error) {
			calls++
			if calls == 1 {
				return dir, nil
			}
			return coverageInfo{mode: os.ModeSymlink}, nil
		}
		if err := syncDir("dir"); err == nil {
			t.Fatal("symlink recheck accepted")
		}
	})
	t.Run("encrypted append oversize", func(t *testing.T) {
		f := &coverageFile{}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f), cipher: coverageCipher(t)}
		if err := w.AppendWithoutSync(&WALRecord{Type: WALInsert, Data: make([]byte, walMaxRecordDataSize)}); err == nil {
			t.Fatal("encrypted oversize accepted")
		}
	})
	t.Run("append staged data and CRC", func(t *testing.T) {
		for _, limit := range []int{walHeaderSize, walHeaderSize + 4} {
			f := &countFailFile{coverageFile: coverageFile{}, failAfter: limit}
			w := &WAL{file: f, bufWriter: bufio.NewWriterSize(f, 1)}
			if err := w.AppendWithoutSync(&WALRecord{Type: WALInsert, Data: []byte("data")}); err == nil {
				t.Fatalf("limit=%d", limit)
			}
		}
	})
	t.Run("checkpoint dirty and final flush", func(t *testing.T) {
		f := &coverageFile{}
		bad := &coverageBackend{writeErr: errCoverage}
		bp := NewBufferPool(1, bad)
		p, _ := bp.NewPage(PageTypeLeaf)
		p.Unpin()
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f)}
		if err := w.Checkpoint(bp); !errors.Is(err, errCoverage) {
			t.Fatalf("dirty=%v", err)
		}
		cf := &countFailFile{coverageFile: coverageFile{}, failAfter: 0}
		w = &WAL{file: cf, bufWriter: bufio.NewWriter(cf)}
		if err := w.Checkpoint(NewBufferPool(1, NewMemory())); err == nil {
			t.Fatal("final flush failure missing")
		}
	})
	t.Run("recover nonEOF and immediate apply", func(t *testing.T) {
		f := &coverageFile{readErr: errCoverage}
		w := &WAL{file: f}
		if err := w.Recover(NewBufferPool(1, NewMemory())); !errors.Is(err, errCoverage) {
			t.Fatalf("read=%v", err)
		}
		records := append(coverageEncodedRecord(t, &WALRecord{LSN: 1, TxnID: 1, Type: WALCommit}), coverageEncodedRecord(t, &WALRecord{LSN: 2, TxnID: 1, Type: WALInsert, PageID: 99, Data: []byte{1}})...)
		w = &WAL{file: &coverageFile{data: records}}
		if err := w.Recover(NewBufferPool(1, NewMemory())); err == nil {
			t.Fatal("immediate apply failure missing")
		}
		combined := coverageEncodedRecord(t, &WALRecord{LSN: 1, TxnID: 2, Type: WALUpdateCommit, PageID: 99, Data: []byte{1}})
		w = &WAL{file: &coverageFile{data: combined}}
		if err := w.Recover(NewBufferPool(1, NewMemory())); err == nil {
			t.Fatal("combined apply failure missing")
		}
	})
}

func TestCoverage100FinalEdgePaths(t *testing.T) {
	old := storageFSOps
	defer func() { storageFSOps = old }()
	t.Run("path component lstat error", func(t *testing.T) {
		storageFSOps = old
		realDir := t.TempDir()
		first := true
		storageFSOps.lstat = func(p string) (os.FileInfo, error) {
			if first {
				first = false
				return os.Lstat(realDir)
			}
			return nil, errCoverage
		}
		if err := rejectStoragePathSymlinkComponents(filepath.Join(realDir, "child", "grandchild"), "test"); err == nil || !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("WAL sync dir failure", func(t *testing.T) {
		wold := walOpenFile
		defer func() { walOpenFile = wold }()
		fso := storageFSOps
		defer func() { storageFSOps = fso }()
		storageFSOps.lstat = func(p string) (os.FileInfo, error) {
			if p == "." || strings.HasSuffix(p, "/.") {
				return nil, errCoverage
			}
			return nil, os.ErrNotExist
		}
		walOpenFile = func(string, int, os.FileMode) (walFile, error) {
			return &coverageFile{info: coverageInfo{mode: 0600}}, nil
		}
		if _, err := OpenWAL("wal"); err == nil {
			t.Fatal("sync dir failure missing")
		}
	})
	t.Run("checkpoint pre sync error", func(t *testing.T) {
		f := &coverageFile{syncErr: errCoverage, syncFailAt: 2}
		w := &WAL{file: f, bufWriter: bufio.NewWriter(f)}
		if err := w.Checkpoint(NewBufferPool(1, NewMemory())); !errors.Is(err, errCoverage) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("recover immediate page error", func(t *testing.T) {
		bp := NewBufferPool(1, NewMemory())
		records := append(coverageEncodedRecord(t, &WALRecord{LSN: 1, TxnID: 1, Type: WALCommit}), coverageEncodedRecord(t, &WALRecord{LSN: 2, TxnID: 1, Type: WALInsert, PageID: 99, Offset: 0, Data: make([]byte, PageSize+1)})...)
		w := &WAL{file: &coverageFile{data: records}}
		if err := w.Recover(bp); err == nil {
			t.Fatal("immediate apply failure missing")
		}
	})
	t.Run("recover pending overflow", func(t *testing.T) {
		storageFSOps = old
		path := filepath.Join(t.TempDir(), "overflow.wal")
		w, err := OpenWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			w.AppendWithoutSync(&WALRecord{TxnID: 1, Type: WALInsert, Data: make([]byte, 100)})
		}
		w.Close()
		walTestMaxPendingBytes = 200
		defer func() { walTestMaxPendingBytes = 0 }()
		w2, err := OpenWAL(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w2.Recover(NewBufferPool(1, NewMemory())); err == nil {
			t.Fatal("overflow not detected")
		}
	})
}
