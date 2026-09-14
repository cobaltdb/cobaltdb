package btree

import (
	"sync"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// gatedBackend is a storage.Backend wrapper whose ReadAt can be armed to block
// once, signaling readStarted and waiting on gate. flushLocked calls
// readKVFromPages only AFTER every shard snapshot has been taken, so the first
// backend read of a flush is a deterministic barrier: everything before it is
// snapshotted, everything after it happens in the merge.
type gatedBackend struct {
	*storage.MemoryBackend
	mu          sync.Mutex
	armed       bool
	readStarted chan struct{}
	gate        chan struct{}
}

func newGatedBackend() *gatedBackend {
	return &gatedBackend{
		MemoryBackend: storage.NewMemory(),
		readStarted:   make(chan struct{}),
		gate:          make(chan struct{}),
	}
}

func (g *gatedBackend) arm() {
	g.mu.Lock()
	g.armed = true
	g.mu.Unlock()
}

func (g *gatedBackend) ReadAt(buf []byte, offset int64) (int, error) {
	g.mu.Lock()
	armed := g.armed
	if armed {
		g.armed = false
	}
	g.mu.Unlock()
	if armed {
		close(g.readStarted)
		<-g.gate
	}
	return g.MemoryBackend.ReadAt(buf, offset)
}

// TestDeleteEvictedKeyFlushRace proves that deleting an evicted key while a
// flush is in flight must not resurrect the deleted key on disk.
//
// Sequence: "k" is flushed to disk, evicted from memory (evicted[k]=true, data
// only on disk), and then deleted — the delete removes the evicted marker and
// succeeds. If that delete lands after the flush has snapshotted the shard
// (which still shows evicted[k]=true) but before the flush merges old disk
// state, a merge that trusts the stale snapshot writes "k" back to disk. The
// in-memory view stays correct (Get returns ErrKeyNotFound), but after a
// restart loadFromPages resurrects the deleted key: a successful Delete is
// silently undone.
func TestDeleteEvictedKeyFlushRace(t *testing.T) {
	backend := newGatedBackend()
	pool, err := storage.NewBufferPoolWithError(4, backend)
	if err != nil {
		t.Fatalf("failed to create buffer pool: %v", err)
	}
	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("failed to create tree: %v", err)
	}

	// 1. Persist "k" so it exists on disk.
	if err := tree.PutString("k", []byte("v")); err != nil {
		t.Fatalf("put k: %v", err)
	}
	if err := tree.flushInternal(); err != nil {
		t.Fatalf("flush k: %v", err)
	}

	// 2. Force eviction of "k": with limit 9 the insert of "trigger" (delta 8,
	// memoryUsed 2) exceeds the limit and the eviction loop drops the oldest
	// entry, which is "k".
	tree.SetMemoryLimit(9)
	if err := tree.PutString("trigger", []byte("x")); err != nil {
		t.Fatalf("put trigger: %v", err)
	}
	tree.SetMemoryLimit(DefaultMemoryLimit)

	sh := &tree.shards[shardIndex("k")]
	sh.mu.RLock()
	wasEvicted := sh.evicted["k"]
	sh.mu.RUnlock()
	if !wasEvicted {
		t.Fatalf("precondition failed: k was not evicted (test setup broken)")
	}
	if val, err := tree.GetString("k"); err != nil || string(val) != "v" {
		t.Fatalf("precondition failed: evicted read returned %q, %v", val, err)
	}

	// 3. Queue one dirty write so the next flush actually serializes, then
	// push the root page out of the buffer pool cache so the flush's
	// readKVFromPages hits the gated backend.
	if err := tree.PutString("dummy", []byte("d")); err != nil {
		t.Fatalf("put dummy: %v", err)
	}
	for i := 0; i < 16; i++ {
		p, perr := pool.NewPage(storage.PageTypeLeaf)
		if perr != nil {
			t.Fatalf("allocating junk page: %v", perr)
		}
		pool.Unpin(p)
	}

	// 4. Start the flush; it blocks at its first backend read, which is
	// strictly after all shard snapshots (including evicted[k]=true).
	backend.arm()
	flushDone := make(chan error, 1)
	go func() { flushDone <- tree.flushInternal() }()
	<-backend.readStarted

	// 5. Delete the evicted key while the flush is paused between snapshot
	// and merge. The delete must succeed.
	if err := tree.DeleteString("k"); err != nil {
		t.Fatalf("delete k: %v", err)
	}

	// 6. Release the flush and wait for it.
	close(backend.gate)
	if ferr := <-flushDone; ferr != nil {
		t.Fatalf("flush failed: %v", ferr)
	}

	// 7. The delete must hold: the key must be gone from disk, not just from
	// memory. A flush that trusts the stale evicted snapshot resurrects it.
	diskNow, err := tree.readKVFromPages()
	if err != nil {
		t.Fatalf("readKVFromPages: %v", err)
	}
	if val, ok := diskNow["k"]; ok {
		t.Fatalf("FAIL: deleted evicted key %q resurrected on disk (value %q) by the racing flush", "k", val)
	}

	// 8. Durability framing: reopening the tree from the flushed pages must
	// not bring the deleted key back.
	reopened, rerr := OpenBTreeWithLimitStrict(pool, tree.RootPageID(), DefaultMemoryLimit)
	if rerr != nil {
		t.Fatalf("reopen: %v", rerr)
	}
	if val, gerr := reopened.Get([]byte("k")); gerr == nil {
		t.Fatalf("FAIL: deleted evicted key resurrected after reopen (value %q)", val)
	}

	// 9. In-memory view: the delete was always visible immediately.
	if _, gerr := tree.Get([]byte("k")); gerr != ErrKeyNotFound {
		t.Fatalf("in-memory view of deleted key: got %v, want ErrKeyNotFound", gerr)
	}
}
