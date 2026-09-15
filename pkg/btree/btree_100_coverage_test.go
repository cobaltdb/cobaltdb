package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newCoverageTree(t *testing.T, limit int64) (*BTree, *storage.BufferPool) {
	t.Helper()
	pool := storage.NewBufferPool(128, storage.NewMemory())
	t.Cleanup(func() { _ = pool.Close() })
	tree, err := NewBTreeWithLimit(pool, limit)
	if err != nil {
		t.Fatalf("NewBTreeWithLimit: %v", err)
	}
	return tree, pool
}

func writeRootData(t *testing.T, pool *storage.BufferPool, rootID uint32, payload []byte) {
	t.Helper()
	page, err := pool.GetPage(rootID)
	if err != nil {
		t.Fatalf("GetPage(%d): %v", rootID, err)
	}
	page.WithDataWrite(func(data []byte) {
		clear(data[storage.PageHeaderSize:])
		copy(data[storage.PageHeaderSize:], payload)
	})
	page.SetDirty(true)
	pool.Unpin(page)
}

func serializedRoot(count, overflow uint32, body []byte) []byte {
	data := make([]byte, 8+len(body))
	binary.LittleEndian.PutUint32(data[:4], count)
	binary.LittleEndian.PutUint32(data[4:8], overflow)
	copy(data[8:], body)
	return data
}

func TestCoverageCheckedLengthsAndNilLoadError(t *testing.T) {
	if _, err := checkedUint16Len(-1, "x"); err == nil {
		t.Fatal("negative uint16 length accepted")
	}
	var tree *BTree
	if tree.LoadError() != nil {
		t.Fatal("nil tree reported a load error")
	}
	if cloneBytes(nil) != nil {
		t.Fatal("cloneBytes(nil) must preserve nil")
	}
}

func TestCoverageStringAndBatchValidation(t *testing.T) {
	tree, _ := newCoverageTree(t, 0)
	if _, err := tree.GetString(""); err != ErrInvalidKey {
		t.Fatalf("GetString empty = %v", err)
	}
	if err := tree.PutString(strings.Repeat("k", MaxKeyLength+1), []byte("v")); err != ErrKeyTooLong {
		t.Fatalf("PutString long key = %v", err)
	}
	if err := tree.PutBatch([][]byte{{'a'}}, nil); err == nil {
		t.Fatal("mismatched batch accepted")
	}
	if err := tree.PutBatch(nil, nil); err != nil {
		t.Fatalf("empty batch = %v", err)
	}
	cases := []struct {
		keys [][]byte
		vals [][]byte
		want error
	}{
		{[][]byte{{}}, [][]byte{[]byte("v")}, ErrInvalidKey},
		{[][]byte{[]byte(strings.Repeat("k", MaxKeyLength+1))}, [][]byte{[]byte("v")}, ErrKeyTooLong},
		{[][]byte{[]byte("k")}, [][]byte{{}}, ErrInvalidValue},
	}
	for _, tc := range cases {
		if err := tree.PutBatch(tc.keys, tc.vals); err != tc.want {
			t.Fatalf("PutBatch validation = %v, want %v", err, tc.want)
		}
	}
}

func TestCoveragePutBatchEvictionAndDeleteBatch(t *testing.T) {
	tree, _ := newCoverageTree(t, 12)
	if err := tree.PutBatch([][]byte{[]byte("a"), []byte("b")}, [][]byte{[]byte("111"), []byte("222")}); err != nil {
		t.Fatalf("initial PutBatch: %v", err)
	}
	// Updating one key and adding another forces the batch retry/eviction path.
	if err := tree.PutBatch([][]byte{[]byte("a"), []byte("c")}, [][]byte{[]byte("333"), []byte("444")}); err != nil {
		t.Fatalf("evicting PutBatch: %v", err)
	}
	if tree.Size() != 3 {
		t.Fatalf("size after evicting batch = %d, want 3", tree.Size())
	}
	if err := tree.DeleteBatch(nil); err != nil {
		t.Fatalf("empty DeleteBatch: %v", err)
	}
	if err := tree.DeleteBatch([][]byte{[]byte("a"), []byte("b"), []byte("missing")}); err != nil {
		t.Fatalf("DeleteBatch: %v", err)
	}
	if tree.Size() != 1 {
		t.Fatalf("size after DeleteBatch = %d, want 1", tree.Size())
	}
}

func TestCoverageDeleteBatchEvictedKey(t *testing.T) {
	tree, _ := newCoverageTree(t, 8)
	if err := tree.Put([]byte("a"), []byte("111")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Put([]byte("b"), []byte("222")); err != nil {
		t.Fatal(err)
	}
	if err := tree.DeleteBatch([][]byte{[]byte("a")}); err != nil {
		t.Fatal(err)
	}
	if tree.Size() != 1 {
		t.Fatalf("size = %d, want 1", tree.Size())
	}
}

func TestCoverageCorruptPageVariants(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{"header", serializedRoot(1, ^uint32(0), nil), "header size"},
		{"empty key", serializedRoot(1, 0, []byte{0, 0, 0, 0, 0, 0}), "empty key"},
		{"truncated value length", serializedRoot(1, 0, append([]byte{0xe4, 0x0f}, make([]byte, 4068)...)), "truncated value length"},
		{"value too long", serializedRoot(1, 0, []byte{1, 0, 'k', 0xff, 0xff, 0xff, 0x7f}), "value length"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tree, pool := newCoverageTree(t, 0)
			writeRootData(t, pool, tree.rootPageID, tc.data)
			_, err := OpenBTreeWithLimitStrict(pool, tree.rootPageID, 0)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestCoverageReadKVCorruptionVariants(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{"empty key", serializedRoot(1, 0, []byte{0, 0, 0, 0, 0, 0}), "empty key"},
		{"key too long", serializedRoot(1, 0, []byte{0xff, 0x0f}), "key length"},
		{"truncated value length", serializedRoot(1, 0, append([]byte{0xe4, 0x0f}, make([]byte, 4068)...)), "truncated value length"},
		{"value too long", serializedRoot(1, 0, []byte{1, 0, 'k', 0xff, 0xff, 0xff, 0x7f}), "value length"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tree, pool := newCoverageTree(t, 0)
			writeRootData(t, pool, tree.rootPageID, tc.data)
			_, err := tree.readKVFromPages()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("readKV error = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestCoverageIteratorStringAndBounds(t *testing.T) {
	tree, _ := newCoverageTree(t, 0)
	if err := tree.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	it, err := tree.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	key, value, err := it.(*Iterator).NextString()
	if err != nil || key != "b" || string(value) != "2" {
		t.Fatalf("NextString = %q, %q, %v", key, value, err)
	}
	key, value, err = it.(*Iterator).NextString()
	if err != nil || key != "" || value != nil {
		t.Fatalf("exhausted NextString = %q, %q, %v", key, value, err)
	}

	bounded := &Iterator{pairs: []kvPair{{key: "z", value: []byte("9")}}, endKey: "a", hasEndKey: true}
	if key, value, err := bounded.NextString(); err != nil || key != "" || value != nil {
		t.Fatalf("bounded NextString = %q, %q, %v", key, value, err)
	}
	bounded = &Iterator{pairs: []kvPair{{key: "z", value: []byte("9")}}, endKey: "a", hasEndKey: true}
	if key, value, err := bounded.Next(); err != nil || key != nil || value != nil {
		t.Fatalf("bounded Next = %q, %q, %v", key, value, err)
	}
}

// errorBackend wraps a real backend and can deterministically fail reads.
type errorBackend struct {
	inner    storage.Backend
	readErr  error
	writeErr error
}

func (b *errorBackend) ReadAt(p []byte, off int64) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	return b.inner.ReadAt(p, off)
}
func (b *errorBackend) WriteAt(p []byte, off int64) (int, error) {
	if b.writeErr != nil {
		return 0, b.writeErr
	}
	return b.inner.WriteAt(p, off)
}
func (b *errorBackend) Sync() error               { return b.inner.Sync() }
func (b *errorBackend) Size() int64               { return b.inner.Size() }
func (b *errorBackend) Truncate(size int64) error { return b.inner.Truncate(size) }
func (b *errorBackend) Close() error              { return b.inner.Close() }

func TestCoverageNewTreeStorageFailure(t *testing.T) {
	backend := &errorBackend{inner: storage.NewMemory(), writeErr: io.ErrClosedPipe}
	pool := storage.NewBufferPool(1, backend)
	defer pool.Close()
	// Exhaust the only frame with a pinned page; NewBTree then cannot allocate.
	page, err := pool.NewPage(storage.PageTypeLeaf)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Unpin(page)
	if _, err := NewBTree(pool); err == nil {
		t.Fatal("NewBTree succeeded with no evictable buffer frame")
	}
}

func TestCoverageReadKVInvalidRoot(t *testing.T) {
	pool := storage.NewBufferPool(8, storage.NewMemory())
	defer pool.Close()
	tree := OpenBTree(pool, 9999)
	if _, err := tree.readKVFromPages(); err == nil {
		t.Fatal("readKVFromPages accepted a missing root")
	}
}

func TestCoverageOverflowReadFailures(t *testing.T) {
	tree, pool := newCoverageTree(t, 0)
	data := make([]byte, 12)
	binary.LittleEndian.PutUint32(data[:4], 1)
	binary.LittleEndian.PutUint32(data[4:8], 1)
	binary.LittleEndian.PutUint32(data[8:12], 9999)
	writeRootData(t, pool, tree.rootPageID, data)
	if _, err := OpenBTreeWithLimitStrict(pool, tree.rootPageID, 0); err == nil || !strings.Contains(err.Error(), "overflow page") {
		t.Fatalf("open missing overflow = %v", err)
	}
	if _, err := tree.readKVFromPages(); err == nil || !strings.Contains(err.Error(), "overflow page") {
		t.Fatalf("read missing overflow = %v", err)
	}
}

func TestCoverageParserRunsOutBetweenEntries(t *testing.T) {
	body := make([]byte, usablePageSize-8)
	binary.LittleEndian.PutUint16(body[:2], uint16(len(body)-6))
	tree, pool := newCoverageTree(t, 0)
	writeRootData(t, pool, tree.rootPageID, serializedRoot(2, 0, body))
	if _, err := OpenBTreeWithLimitStrict(pool, tree.rootPageID, 0); err == nil || !strings.Contains(err.Error(), "truncated key length") {
		t.Fatalf("open exhausted entries = %v", err)
	}
	if _, err := tree.readKVFromPages(); err == nil || !strings.Contains(err.Error(), "truncated key length") {
		t.Fatalf("read exhausted entries = %v", err)
	}
}

func TestCoverageEvictedGetOutcomes(t *testing.T) {
	tree, pool := newCoverageTree(t, 0)
	if err := tree.Put([]byte("persisted"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Flush(); err != nil {
		t.Fatal(err)
	}
	tree.shards[shardIndex("missing")].evicted["missing"] = true
	if _, err := tree.GetString("missing"); err != ErrKeyNotFound {
		t.Fatalf("missing evicted Get = %v", err)
	}
	writeRootData(t, pool, tree.rootPageID, serializedRoot(^uint32(0), 0, nil))
	tree.shards[shardIndex("broken")].evicted["broken"] = true
	if _, err := tree.GetString("broken"); err == nil || err == ErrKeyNotFound {
		t.Fatalf("corrupt evicted Get = %v", err)
	}
}

func TestCoverageBatchEvictionRetryAndEvictedDelete(t *testing.T) {
	tree, _ := newCoverageTree(t, 0)
	if err := tree.Put([]byte("old"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	tree.SetMemoryLimit(8)
	if err := tree.PutBatch([][]byte{[]byte("new")}, [][]byte{[]byte("data")}); err != nil {
		t.Fatalf("PutBatch eviction retry: %v", err)
	}
	sh := &tree.shards[shardIndex("disk-only")]
	sh.evicted["disk-only"] = true
	atomic.AddInt64(&tree.keyCount, 1)
	if err := tree.DeleteBatch([][]byte{[]byte("disk-only")}); err != nil {
		t.Fatal(err)
	}
	if sh.evicted["disk-only"] {
		t.Fatal("DeleteBatch retained evicted key marker")
	}
}

func TestCoverageEvictionWithoutCandidates(t *testing.T) {
	tree, _ := newCoverageTree(t, 10)
	atomic.StoreInt64(&tree.memoryUsed, 10)
	if err := tree.evictToMakeSpace(1); err != ErrMemoryLimit {
		t.Fatalf("eviction without candidates = %v, want ErrMemoryLimit", err)
	}
}

func TestCoverageFlushRejectsMalformedInMemoryKey(t *testing.T) {
	tree, _ := newCoverageTree(t, 0)
	key := strings.Repeat("k", MaxKeyLength+1)
	tree.shards[shardIndex(key)].data[key] = []byte("v")
	atomic.StoreInt32(&tree.dirty, 1)
	if err := tree.Flush(); err == nil || !strings.Contains(err.Error(), "key length") {
		t.Fatalf("flush malformed key = %v", err)
	}
}

func TestCoverageScanEvictedMergeAndCorruptDisk(t *testing.T) {
	tree, pool := newCoverageTree(t, 0)
	if err := tree.Put([]byte("disk"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Flush(); err != nil {
		t.Fatal(err)
	}
	sh := &tree.shards[shardIndex("disk")]
	delete(sh.data, "disk")
	sh.evicted["disk"] = true
	it, err := tree.Scan([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatal(err)
	}
	key, value, err := it.Next()
	if err != nil || string(key) != "disk" || string(value) != "value" {
		t.Fatalf("evicted scan = %q %q %v", key, value, err)
	}
	writeRootData(t, pool, tree.rootPageID, serializedRoot(^uint32(0), 0, nil))
	if _, err := tree.Scan(nil, nil); err == nil {
		t.Fatal("scan did not propagate corrupt disk state")
	}
}

func TestCoverageReadKVHeaderAndScanEndFilter(t *testing.T) {
	tree, pool := newCoverageTree(t, 0)
	writeRootData(t, pool, tree.rootPageID, serializedRoot(1, ^uint32(0), nil))
	if _, err := tree.readKVFromPages(); err == nil || !strings.Contains(err.Error(), "header size") {
		t.Fatalf("read oversized header = %v", err)
	}

	// Persist a disk-only key that lies after the requested scan end; the
	// merge must filter it just like in-memory keys.
	tree, _ = newCoverageTree(t, 0)
	if err := tree.Put([]byte("z"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Flush(); err != nil {
		t.Fatal(err)
	}
	sh := &tree.shards[shardIndex("z")]
	delete(sh.data, "z")
	sh.evicted["z"] = true
	it, err := tree.Scan(nil, []byte("m"))
	if err != nil {
		t.Fatal(err)
	}
	if it.Valid() {
		t.Fatal("scan included evicted key after end bound")
	}
}

func TestCoveragePutBatchEntryExceedsLimit(t *testing.T) {
	tree, _ := newCoverageTree(t, 3)
	if err := tree.PutBatch([][]byte{[]byte("key")}, [][]byte{[]byte("value")}); err != ErrMemoryLimit {
		t.Fatalf("oversized PutBatch = %v, want ErrMemoryLimit", err)
	}
}

func TestCoverageEvictionFlushFailures(t *testing.T) {
	t.Run("initial dirty flush", func(t *testing.T) {
		tree, pool := newCoverageTree(t, 10)
		if err := tree.Put([]byte("a"), []byte("1")); err != nil {
			t.Fatal(err)
		}
		_ = pool.Close()
		if err := tree.evictToMakeSpace(9); err == nil || !strings.Contains(err.Error(), "failed to flush during eviction") {
			t.Fatalf("eviction flush error = %v", err)
		}
	})

	t.Run("newer than flush horizon", func(t *testing.T) {
		tree, pool := newCoverageTree(t, 2)
		key := ""
		for i := 0; ; i++ {
			candidate := fmt.Sprintf("candidate-%d", i)
			if shardIndex(candidate) == 0 {
				key = candidate
				break
			}
		}
		tree.SetMemoryLimit(int64(len(key) + 1))
		if err := tree.PutString(key, []byte("1")); err != nil {
			t.Fatal(err)
		}
		sh := &tree.shards[0]
		atomic.StoreInt32(&tree.dirty, 0)
		entry := sh.lruMap[key]
		atomic.StoreInt64(&tree.lastFlushTS, entry.timestamp-1)
		sh.lruMu.Lock()
		done := make(chan error, 1)
		go func() { done <- tree.evictToMakeSpace(1) }()
		time.Sleep(time.Millisecond)
		atomic.StoreInt32(&tree.dirty, 1)
		_ = pool.Close()
		sh.lruMu.Unlock()
		if err := <-done; err == nil || !strings.Contains(err.Error(), "failed to flush during eviction") {
			t.Fatalf("horizon reflush error = %v", err)
		}
	})
}

func TestCoverageFlushAllocationAndPageLookupFailures(t *testing.T) {
	t.Run("overflow allocation", func(t *testing.T) {
		pool := storage.NewBufferPool(1, storage.NewMemory())
		defer pool.Close()
		tree, err := NewBTree(pool)
		if err != nil {
			t.Fatal(err)
		}
		root, err := pool.GetPage(tree.rootPageID)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Unpin(root)
		if err := tree.Put([]byte("k"), make([]byte, usablePageSize)); err != nil {
			t.Fatal(err)
		}
		if err := tree.Flush(); err == nil || !strings.Contains(err.Error(), "allocate overflow") {
			t.Fatalf("overflow allocation error = %v", err)
		}
	})

	t.Run("root lookup", func(t *testing.T) {
		tree, pool := newCoverageTree(t, 0)
		if err := tree.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
		_ = pool.Close()
		if err := tree.Flush(); err == nil {
			t.Fatal("flush succeeded after pool close")
		}
	})

	t.Run("overflow lookup", func(t *testing.T) {
		tree, _ := newCoverageTree(t, 0)
		tree.overflowPages = []uint32{9999}
		if err := tree.Put([]byte("k"), make([]byte, usablePageSize)); err != nil {
			t.Fatal(err)
		}
		if err := tree.Flush(); err == nil || !strings.Contains(err.Error(), "failed to get overflow page") {
			t.Fatalf("overflow lookup error = %v", err)
		}
	})
}

func TestCoverageFlushEvictedSerializationFailures(t *testing.T) {
	tree, pool := newCoverageTree(t, 0)
	if err := tree.Put([]byte("old"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Flush(); err != nil {
		t.Fatal(err)
	}
	sh := &tree.shards[shardIndex("old")]
	delete(sh.data, "old")
	sh.evicted["old"] = true
	key := strings.Repeat("k", MaxKeyLength+1)
	tree.shards[shardIndex(key)].data[key] = []byte("v")
	atomic.StoreInt32(&tree.dirty, 1)
	if err := tree.Flush(); err == nil || !strings.Contains(err.Error(), "key length") {
		t.Fatalf("evicted malformed key flush = %v", err)
	}

	delete(tree.shards[shardIndex(key)].data, key)
	writeRootData(t, pool, tree.rootPageID, serializedRoot(^uint32(0), 0, nil))
	atomic.StoreInt32(&tree.dirty, 1)
	if err := tree.Flush(); err == nil {
		t.Fatal("evicted flush did not propagate corrupt disk state")
	}
}

func TestCoverageScanBackfillsSeenBeforeEvictedShard(t *testing.T) {
	tree, _ := newCoverageTree(t, 0)
	early := ""
	late := ""
	for i := 0; early == "" || late == ""; i++ {
		key := fmt.Sprintf("key-%d", i)
		idx := shardIndex(key)
		if idx < 64 && early == "" {
			early = key
		}
		if idx > 192 && late == "" {
			late = key
		}
	}
	if err := tree.PutString(early, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := tree.Flush(); err != nil {
		t.Fatal(err)
	}
	tree.shards[shardIndex(late)].evicted[late] = true
	if _, err := tree.Scan(nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCoverageErrorsComparable(t *testing.T) {
	if !errors.Is(ErrMemoryLimit, ErrMemoryLimit) {
		t.Fatal("sentinel error identity changed")
	}
}
