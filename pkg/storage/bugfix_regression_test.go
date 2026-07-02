package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Fix 1: buffer pool pin-after-unlock race (GetPage fast path must pin while
// still holding the read lock so evict() can never recycle the buffer of a
// page that is being handed out).
// ---------------------------------------------------------------------------

// TestGetPageCacheHitReturnsPinnedPage verifies the basic contract that a
// cache-hit GetPage returns an already-pinned page.
func TestGetPageCacheHitReturnsPinnedPage(t *testing.T) {
	bp, err := NewBufferPoolWithError(4, NewMemory())
	if err != nil {
		t.Fatalf("NewBufferPoolWithError: %v", err)
	}
	defer bp.Close()

	p, err := bp.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage: %v", err)
	}
	id := p.ID()
	bp.Unpin(p)

	got, err := bp.GetPage(id) // cache hit path
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	if !got.IsPinned() {
		t.Fatal("cache-hit GetPage returned an unpinned page")
	}
	bp.Unpin(got)
}

// TestBufferPoolNoBufferAliasingUnderEvictionChurn stresses concurrent cache
// hits against eviction pressure. With the pin-after-unlock bug, a page could
// be evicted between the fast path's RUnlock and Pin, its buffer recycled to
// back a different page, and both pages would alias one buffer — the content
// check below would then observe another page's bytes.
func TestBufferPoolNoBufferAliasingUnderEvictionChurn(t *testing.T) {
	backend := NewMemory()
	bp, err := NewBufferPoolWithError(8, backend)
	if err != nil {
		t.Fatalf("NewBufferPoolWithError: %v", err)
	}
	defer bp.Close()

	const pageCount = 32
	ids := make([]uint32, 0, pageCount)
	for i := 0; i < pageCount; i++ {
		p, err := bp.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("NewPage: %v", err)
		}
		marker := byte(p.ID())
		p.WithDataWrite(func(data []byte) {
			for j := PageHeaderSize; j < PageSize; j++ {
				data[j] = marker
			}
		})
		p.SetDirty(true)
		ids = append(ids, p.ID())
		bp.Unpin(p)
	}
	if err := bp.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed)) // #nosec G404 - test only
			for i := 0; i < 3000; i++ {
				id := ids[r.Intn(len(ids))]
				p, err := bp.GetPage(id)
				if err != nil {
					t.Errorf("GetPage(%d): %v", id, err)
					return
				}
				d := p.Data()
				if d[PageHeaderSize] != byte(id) || d[PageSize-1] != byte(id) {
					t.Errorf("page %d aliased another page's buffer: marker=%d/%d",
						id, d[PageHeaderSize], d[PageSize-1])
					bp.Unpin(p)
					return
				}
				bp.Unpin(p)
			}
		}(int64(w))
	}
	wg.Wait()
}

// TestFlushDirtyPagesBalancesPins verifies that the background flusher's
// eviction guard (pinning collected pages) always unpins them again, and that
// pages end up clean.
func TestFlushDirtyPagesBalancesPins(t *testing.T) {
	bp, err := NewBufferPoolWithError(8, NewMemory())
	if err != nil {
		t.Fatalf("NewBufferPoolWithError: %v", err)
	}
	defer bp.Close()

	pages := make([]*CachedPage, 0, 4)
	for i := 0; i < 4; i++ {
		p, err := bp.NewPage(PageTypeLeaf)
		if err != nil {
			t.Fatalf("NewPage: %v", err)
		}
		p.SetDirty(true)
		bp.Unpin(p)
		pages = append(pages, p)
	}

	bp.flushDirtyPages()

	for _, p := range pages {
		if p.IsDirty() {
			t.Errorf("page %d still dirty after flushDirtyPages", p.ID())
		}
		if p.IsPinned() {
			t.Errorf("page %d left pinned by flushDirtyPages", p.ID())
		}
	}
}

// TestFlushDirtyBalancesPins does the same for the checkpoint-path FlushDirty.
func TestFlushDirtyBalancesPins(t *testing.T) {
	bp, err := NewBufferPoolWithError(8, NewMemory())
	if err != nil {
		t.Fatalf("NewBufferPoolWithError: %v", err)
	}
	defer bp.Close()

	p, err := bp.NewPage(PageTypeLeaf)
	if err != nil {
		t.Fatalf("NewPage: %v", err)
	}
	p.SetDirty(true)
	bp.Unpin(p)

	if err := bp.FlushDirty(); err != nil {
		t.Fatalf("FlushDirty: %v", err)
	}
	if p.IsDirty() {
		t.Error("page still dirty after FlushDirty")
	}
	if p.IsPinned() {
		t.Error("page left pinned by FlushDirty")
	}
}

// ---------------------------------------------------------------------------
// Fix 6: a compressed record (header + payload) must never exceed PageSize —
// otherwise it spills into the next page's slot.
// ---------------------------------------------------------------------------

func TestCompressedWriteNeverOverflowsPageSlot(t *testing.T) {
	mem := NewMemory()
	// LZ4 produces byte-linear compressed sizes for mostly-random data with a
	// small compressible run, which lets us land in the exact overflow window.
	cb, err := NewCompressedBackend(mem, &CompressionConfig{
		Enabled:   true,
		Algorithm: CompressionAlgorithmLZ4,
		Level:     CompressionLevelFast,
		MinRatio:  1.0, // admit anything that shrinks at all
	})
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}
	defer cb.Close()

	// Find a page whose compressed size c satisfies c < PageSize (so the old
	// code chose the compressed representation) but header+c > PageSize (so
	// it would overflow the slot).
	r := rand.New(rand.NewSource(42)) // #nosec G404 - test only
	base := make([]byte, PageSize)
	r.Read(base) // incompressible base
	var page0 []byte
	for k := 0; k <= 600; k++ {
		candidate := make([]byte, PageSize)
		copy(candidate, base)
		for i := 0; i < k; i++ {
			candidate[i] = 'A' // growing compressible run
		}
		compressed, _, err := cb.compress(candidate)
		if err != nil {
			t.Fatalf("compress: %v", err)
		}
		if len(compressed) < PageSize && compressionHeaderSize+len(compressed) > PageSize {
			page0 = candidate
			break
		}
	}
	if page0 == nil {
		t.Skip("could not construct a page in the overflow window for this codec")
	}

	// Neighbouring slot content that must survive intact.
	page1 := bytes.Repeat([]byte{0xAB}, PageSize)
	if _, err := cb.WriteAt(page1, PageSize); err != nil {
		t.Fatalf("WriteAt page1: %v", err)
	}
	if _, err := cb.WriteAt(page0, 0); err != nil {
		t.Fatalf("WriteAt page0: %v", err)
	}

	got0 := make([]byte, PageSize)
	if _, err := cb.ReadAt(got0, 0); err != nil {
		t.Fatalf("ReadAt page0: %v", err)
	}
	if !bytes.Equal(got0, page0) {
		t.Fatal("page 0 did not round-trip")
	}
	got1 := make([]byte, PageSize)
	if _, err := cb.ReadAt(got1, PageSize); err != nil {
		t.Fatalf("ReadAt page1: %v", err)
	}
	if !bytes.Equal(got1, page1) {
		t.Fatal("compressed write for page 0 spilled into page 1's slot")
	}
}

// ---------------------------------------------------------------------------
// Fix 7: header + payload of a compressed page must be issued as ONE WriteAt
// on the underlying backend (crash atomicity).
// ---------------------------------------------------------------------------

// countingBackend wraps a Backend and counts WriteAt calls.
type countingBackend struct {
	Backend
	mu     sync.Mutex
	writes int
}

func (c *countingBackend) WriteAt(buf []byte, offset int64) (int, error) {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return c.Backend.WriteAt(buf, offset)
}

func TestCompressedWriteIsSingleBackendWrite(t *testing.T) {
	counting := &countingBackend{Backend: NewMemory()}
	cb, err := NewCompressedBackend(counting, DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}
	defer cb.Close()

	// Highly compressible page — guaranteed to take the compressed path.
	page := bytes.Repeat([]byte{0x00}, PageSize)
	if _, err := cb.WriteAt(page, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	counting.mu.Lock()
	writes := counting.writes
	counting.mu.Unlock()
	if writes != 1 {
		t.Fatalf("compressed page write issued %d backend WriteAt calls, want 1 (header+payload must be atomic)", writes)
	}

	got := make([]byte, PageSize)
	if _, err := cb.ReadAt(got, 0); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(got, page) {
		t.Fatal("compressed page did not round-trip")
	}
}

// ---------------------------------------------------------------------------
// Fix 8: CompressedBackend.Truncate mutates logicalSize and must hold the
// write lock. Exercised concurrently so `go test -race` flags a regression.
// ---------------------------------------------------------------------------

func TestCompressedTruncateConcurrentWithSize(t *testing.T) {
	cb, err := NewCompressedBackend(NewMemory(), DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("NewCompressedBackend: %v", err)
	}
	defer cb.Close()

	page := bytes.Repeat([]byte{0x01}, PageSize)
	if _, err := cb.WriteAt(page, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if i%2 == 0 {
					_ = cb.Truncate(int64(PageSize))
				} else {
					_ = cb.Size()
				}
			}
		}(i)
	}
	wg.Wait()

	if got := cb.Size(); got != int64(PageSize) {
		t.Fatalf("Size after Truncate = %d, want %d", got, PageSize)
	}
}

// ---------------------------------------------------------------------------
// Fix 9: WAL Checkpoint must flush+sync the old bufWriter at its CURRENT
// position BEFORE truncating. Previously, a record stream that straddled the
// bufio buffer had its auto-flushed head destroyed by Truncate(0) while the
// buffered tail was rewritten at position 0 — a torn WAL.
// ---------------------------------------------------------------------------

func TestWALCheckpointNoTornRecordsWhenBufferStraddles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "straddle.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	pool := NewBufferPool(4, NewMemory())

	// Append enough buffered (unsynced) records to exceed bufio's 4KB buffer,
	// forcing the head of the stream to auto-flush to the file while the tail
	// stays buffered — the exact straddling situation.
	payload := bytes.Repeat([]byte{'x'}, 200)
	records := make([]*WALRecord, 0, 40)
	for i := 0; i < 40; i++ {
		records = append(records, &WALRecord{
			TxnID: uint64(i + 1),
			Type:  WALInsert,
			Data:  payload,
		})
	}
	if err := wal.AppendBatchWithoutSync(records); err != nil {
		t.Fatalf("AppendBatchWithoutSync: %v", err)
	}

	if err := wal.Checkpoint(pool); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("pool.Close: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("wal.Close: %v", err)
	}

	// The WAL must now begin with a well-formed checkpoint record — not the
	// tail half of a torn record stream.
	raw, err := os.ReadFile(path) // #nosec G304 - test temp file
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(raw) < walHeaderSize+4 {
		t.Fatalf("WAL too short after checkpoint: %d bytes", len(raw))
	}
	if got := WALRecordType(raw[16]); got != WALCheckpoint {
		t.Fatalf("first record after checkpoint has type 0x%02x, want checkpoint 0x%02x (torn WAL)",
			uint8(got), uint8(WALCheckpoint))
	}
	dataLen := binary.LittleEndian.Uint16(raw[23:25])
	if dataLen != 0 {
		t.Fatalf("checkpoint record dataLen = %d, want 0", dataLen)
	}

	// Recovery over the reopened WAL must succeed.
	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer wal2.Close()
	pool2 := NewBufferPool(4, NewMemory())
	defer pool2.Close()
	if err := wal2.Recover(pool2); err != nil {
		t.Fatalf("Recover after checkpoint: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Fix 10: meta page checksum must ALWAYS be verified — a stored zero checksum
// is itself corruption, not a legacy format.
// ---------------------------------------------------------------------------

func TestMetaPageDeserializeRejectsZeroedChecksum(t *testing.T) {
	m := NewMetaPage()
	m.PageCount = 7
	m.RootPageID = 3
	data := make([]byte, PageSize)
	m.Serialize(data)

	// Sanity: intact page deserializes fine.
	var ok MetaPage
	if err := ok.Deserialize(data); err != nil {
		t.Fatalf("Deserialize(intact): %v", err)
	}

	// Zero out the stored checksum — previously this DISABLED verification.
	binary.LittleEndian.PutUint32(data[32:36], 0)
	var bad MetaPage
	if err := bad.Deserialize(data); err == nil {
		t.Fatal("Deserialize accepted a meta page with a zeroed checksum")
	}

	// And a flipped payload byte with the zeroed checksum must also fail.
	data[12] ^= 0xFF
	var bad2 MetaPage
	if err := bad2.Deserialize(data); err == nil {
		t.Fatal("Deserialize accepted a corrupted meta page with a zeroed checksum")
	}
}

// TestMetaPageRoundTripStillValid guards against over-tightening: a normally
// serialized meta page must still verify.
func TestMetaPageRoundTripStillValid(t *testing.T) {
	for i := 0; i < 4; i++ {
		m := NewMetaPage()
		m.PageCount = uint32(i + 1)
		m.TxnCounter = uint64(i * 17)
		data := make([]byte, PageSize)
		m.Serialize(data)
		var out MetaPage
		if err := out.Deserialize(data); err != nil {
			t.Fatalf("round-trip %d: %v", i, err)
		}
		if out.PageCount != m.PageCount || out.TxnCounter != m.TxnCounter {
			t.Fatalf("round-trip %d: fields mismatch", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Fix 11 (perf, correctness guard): AppendBatch still produces valid CRCs now
// that the pre-lock CRC computation was removed (CRC computed once, after the
// LSN patch).
// ---------------------------------------------------------------------------

func TestWALAppendBatchCRCsValidAfterSingleComputation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batchcrc.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	// Fast path (≤2 records) and slow path (>2 records).
	small := []*WALRecord{
		{TxnID: 1, Type: WALInsert, Data: []byte("a")},
		{TxnID: 1, Type: WALCommit},
	}
	if err := wal.AppendBatch(small); err != nil {
		t.Fatalf("AppendBatch(fast): %v", err)
	}
	big := make([]*WALRecord, 0, 5)
	for i := 0; i < 5; i++ {
		big = append(big, &WALRecord{TxnID: 2, Type: WALInsert, Data: []byte(fmt.Sprintf("row-%d", i))})
	}
	if err := wal.AppendBatch(big); err != nil {
		t.Fatalf("AppendBatch(slow): %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recover must accept every record (a bad CRC would fail or stop replay).
	wal2, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer wal2.Close()
	pool := NewBufferPool(4, NewMemory())
	defer pool.Close()
	if err := wal2.Recover(pool); err != nil {
		t.Fatalf("Recover: %v", err)
	}
}
