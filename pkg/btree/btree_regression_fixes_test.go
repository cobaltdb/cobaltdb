package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newFixTestTree(t *testing.T, poolCap int, memLimit int64) (*BTree, *storage.BufferPool) {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(poolCap, backend)
	tree, err := NewBTreeWithLimit(pool, memLimit)
	if err != nil {
		t.Fatalf("NewBTreeWithLimit: %v", err)
	}
	return tree, pool
}

// ---------------------------------------------------------------------------
// Fix 2: flushInternal must refuse to persist a root whose overflow page ID
// list does not fit, instead of silently truncating the list (which made the
// tree unloadable — total data loss — on reopen).
// ---------------------------------------------------------------------------

func TestFlushErrorsWhenOverflowListExceedsRootPage(t *testing.T) {
	tree, pool := newFixTestTree(t, 256, 0 /* unlimited memory */)
	defer pool.Close()

	// Establish a small, valid on-disk state first.
	if err := tree.Put([]byte("k0"), []byte("v0")); err != nil {
		t.Fatalf("Put small: %v", err)
	}
	if err := tree.Flush(); err != nil {
		t.Fatalf("Flush small: %v", err)
	}

	// Now grow the tree past the point where the overflow page ID list fits
	// in the root page (maxOverflowPages pages ≈ 4.15MB serialized).
	bigVal := bytes.Repeat([]byte{'z'}, 100_000)
	for i := 0; i < 45; i++ { // ~4.5MB serialized
		if err := tree.Put([]byte(fmt.Sprintf("big%02d", i)), bigVal); err != nil {
			t.Fatalf("Put big %d: %v", i, err)
		}
	}

	err := tree.Flush()
	if err == nil {
		t.Fatal("Flush succeeded for a tree whose overflow list cannot fit in the root page")
	}
	if !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("Flush error should mention the overflow list, got: %v", err)
	}

	// Fix 4 interplay: the failed flush must leave the tree marked dirty so
	// the data is not considered persisted.
	if atomic.LoadInt32(&tree.dirty) != 1 {
		t.Fatal("failed flush cleared the dirty flag")
	}

	// The previously flushed (small) root must be untouched: a strict reopen
	// still loads it and serves the old key.
	reopened, err := OpenBTreeStrict(pool, tree.RootPageID())
	if err != nil {
		t.Fatalf("failed flush corrupted the existing root: %v", err)
	}
	got, err := reopened.Get([]byte("k0"))
	if err != nil || string(got) != "v0" {
		t.Fatalf("Get(k0) after failed big flush = %q, %v; want v0", got, err)
	}
}

// ---------------------------------------------------------------------------
// Fix 2b: a NON-strict open of a corrupt/inconsistent root must not behave
// like an empty tree — reads, writes, scans, and flushes must surface the
// load error instead of silently losing/overwriting the data.
// ---------------------------------------------------------------------------

func TestCorruptRootNonStrictOpenPropagatesLoadError(t *testing.T) {
	tree, pool := newFixTestTree(t, 64, 0)
	defer pool.Close()

	if err := tree.Put([]byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tree.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	rootID := tree.RootPageID()

	// Corrupt the root header: impossible entry count.
	root, err := pool.GetPage(rootID)
	if err != nil {
		t.Fatalf("GetPage(root): %v", err)
	}
	root.WithDataWrite(func(data []byte) {
		binary.LittleEndian.PutUint32(data[storage.PageHeaderSize:], 0xFFFFFFF0)
	})
	root.SetDirty(true)
	pool.Unpin(root)

	corrupt := OpenBTreeWithLimit(pool, rootID, DefaultMemoryLimit)
	loadErr := corrupt.LoadError()
	if loadErr == nil {
		t.Fatal("expected LoadError for corrupt root")
	}

	if _, err := corrupt.Get([]byte("alpha")); err == nil || err == ErrKeyNotFound {
		t.Fatalf("Get on corrupt tree = %v; want the load error, not empty-tree behavior", err)
	}
	if err := corrupt.Put([]byte("new"), []byte("val")); err == nil {
		t.Fatal("Put on corrupt tree succeeded; would later overwrite the root")
	}
	if err := corrupt.PutBatch([][]byte{[]byte("a")}, [][]byte{[]byte("b")}); err == nil {
		t.Fatal("PutBatch on corrupt tree succeeded")
	}
	if err := corrupt.Delete([]byte("alpha")); err == nil || err == ErrKeyNotFound {
		t.Fatalf("Delete on corrupt tree = %v; want the load error", err)
	}
	if err := corrupt.DeleteBatch([][]byte{[]byte("alpha")}); err == nil {
		t.Fatal("DeleteBatch on corrupt tree succeeded")
	}
	if _, err := corrupt.Scan(nil, nil); err == nil {
		t.Fatal("Scan on corrupt tree succeeded; would report an empty tree")
	}
	if err := corrupt.Flush(); err == nil {
		t.Fatal("Flush on corrupt tree succeeded; would overwrite the root with an empty tree")
	}
}

// ---------------------------------------------------------------------------
// Fix 3: Get must not corrupt the shard LRU list. The old code called
// Remove(entry) and then MoveToFront(entry); MoveToFront removes again, and
// removing an already-detached node resets head/tail, orphaning every other
// entry in the shard's list.
// ---------------------------------------------------------------------------

func TestGetPreservesShardLRUIntegrity(t *testing.T) {
	tree, pool := newFixTestTree(t, 64, 0)
	defer pool.Close()

	const n = 300 // spread over 256 shards -> many shards get >= 2 entries
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if err := tree.PutString(key, []byte("value")); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}

	multiEntryShards := 0
	for i := range tree.shards {
		tree.shards[i].lruMu.Lock()
		if len(tree.shards[i].lruMap) >= 2 {
			multiEntryShards++
		}
		tree.shards[i].lruMu.Unlock()
	}
	if multiEntryShards == 0 {
		t.Fatal("test setup: no shard received two or more entries")
	}

	// Hit every key once — the buggy code destroyed a shard's list on the
	// first Get for any shard holding >= 2 entries.
	for i := 0; i < n; i++ {
		if _, err := tree.GetString(fmt.Sprintf("key-%03d", i)); err != nil {
			t.Fatalf("Get key-%03d: %v", i, err)
		}
	}

	totalList, totalMap := 0, 0
	for i := range tree.shards {
		sh := &tree.shards[i]
		sh.lruMu.Lock()
		listLen, mapLen := sh.lruList.Len(), len(sh.lruMap)
		sh.lruMu.Unlock()
		if listLen != mapLen {
			t.Errorf("shard %d: lruList has %d nodes but lruMap has %d entries (list corrupted)",
				i, listLen, mapLen)
		}
		totalList += listLen
		totalMap += mapLen
	}
	if totalList != n || totalMap != n {
		t.Fatalf("LRU bookkeeping lost entries: list=%d map=%d want %d", totalList, totalMap, n)
	}
}

// ---------------------------------------------------------------------------
// Fixes 4 + 5: a Put that lands while a flush snapshot is being written must
// not have its dirty bit clobbered, and eviction must never discard a value
// written after the flush it relies on. Exercised as a concurrent stress:
// with either bug, some keys come back stale or ErrKeyNotFound.
// ---------------------------------------------------------------------------

func TestEvictionNeverLosesConcurrentWrites(t *testing.T) {
	tree, pool := newFixTestTree(t, 512, 16*1024) // small limit -> constant evict+flush churn
	defer pool.Close()

	const workers = 8
	const perWorker = 250
	const passes = 3

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			pad := strings.Repeat("p", 48)
			// Two passes: the second overwrites, so a lost update shows up
			// as a stale first-pass value, not just a missing key.
			for pass := 1; pass <= passes; pass++ {
				for i := 0; i < perWorker; i++ {
					key := fmt.Sprintf("w%d-k%03d", w, i)
					val := fmt.Sprintf("v%d-%s-%s", pass, key, pad)
					// ErrMemoryLimit can transiently fire when several
					// workers race the evictor (pre-existing behavior of the
					// single memory budget) — retry; the write must
					// eventually be accepted and then never be lost.
					var err error
					for attempt := 0; attempt < 1000; attempt++ {
						if err = tree.PutString(key, []byte(val)); err == nil || err != ErrMemoryLimit {
							break
						}
						runtime.Gosched()
					}
					if err != nil {
						errCh <- fmt.Errorf("Put %s: %w", key, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	pad := strings.Repeat("p", 48)
	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			key := fmt.Sprintf("w%d-k%03d", w, i)
			want := fmt.Sprintf("v%d-%s-%s", passes, key, pad)
			got, err := tree.GetString(key)
			if err != nil {
				t.Fatalf("Get %s: %v (write lost by eviction)", key, err)
			}
			if string(got) != want {
				t.Fatalf("Get %s = %q, want %q (stale value resurrected by eviction)", key, got, want)
			}
		}
	}
}

// TestFlushDirtyLifecycle is a direct sanity check of the clear-before-
// snapshot protocol: flush clears dirty, a new Put re-sets it, a failed
// flush restores it (covered in TestFlushErrorsWhenOverflowListExceedsRootPage).
func TestFlushDirtyLifecycle(t *testing.T) {
	tree, pool := newFixTestTree(t, 64, 0)
	defer pool.Close()

	if err := tree.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if atomic.LoadInt32(&tree.dirty) != 1 {
		t.Fatal("Put did not mark tree dirty")
	}
	if err := tree.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if atomic.LoadInt32(&tree.dirty) != 0 {
		t.Fatal("Flush did not clear dirty")
	}
	if err := tree.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if atomic.LoadInt32(&tree.dirty) != 1 {
		t.Fatal("Put after flush did not re-mark tree dirty")
	}
}

// ---------------------------------------------------------------------------
// Fix 12: PutBatch draws LRU entries from lruEntryPool like the single-key
// path; verify batch inserts and batch updates keep list/map consistent.
// ---------------------------------------------------------------------------

func TestPutBatchKeepsLRUConsistent(t *testing.T) {
	tree, pool := newFixTestTree(t, 64, 0)
	defer pool.Close()

	const n = 128
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("batch-%03d", i))
		vals[i] = []byte(fmt.Sprintf("val-%03d", i))
	}
	if err := tree.PutBatch(keys, vals); err != nil {
		t.Fatalf("PutBatch insert: %v", err)
	}
	// Update pass reuses the removed nodes.
	for i := 0; i < n; i++ {
		vals[i] = []byte(fmt.Sprintf("upd-%03d", i))
	}
	if err := tree.PutBatch(keys, vals); err != nil {
		t.Fatalf("PutBatch update: %v", err)
	}

	totalList, totalMap := 0, 0
	for i := range tree.shards {
		sh := &tree.shards[i]
		sh.lruMu.Lock()
		listLen, mapLen := sh.lruList.Len(), len(sh.lruMap)
		sh.lruMu.Unlock()
		if listLen != mapLen {
			t.Errorf("shard %d: list %d != map %d after PutBatch", i, listLen, mapLen)
		}
		totalList += listLen
		totalMap += mapLen
	}
	if totalList != n || totalMap != n {
		t.Fatalf("LRU totals after PutBatch: list=%d map=%d want %d", totalList, totalMap, n)
	}
	for i := 0; i < n; i++ {
		got, err := tree.Get(keys[i])
		if err != nil || string(got) != string(vals[i]) {
			t.Fatalf("Get %s = %q, %v; want %q", keys[i], got, err, vals[i])
		}
	}
}
