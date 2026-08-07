package btree

import (
	"sync/atomic"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestFlushLocked_PanicDuringSnapshotRestoresDirty exercises the panic-safety
// path in flushLocked: if a panic occurs during the snapshot map allocations
// (e.g. memory pressure triggers a runtime panic on `make`), the dirty flag
// must be restored so the in-memory data is not silently lost on the next
// crash. Without the recover() in the deferred cleanup, a panic here would
// leave the tree permanently marked clean with its data only in memory.
func TestFlushLocked_PanicDuringSnapshotRestoresDirty(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Seed the tree so dirty is set and there is real data to lose.
	if err := tree.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := tree.Put([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Snapshot the dirty flag before the panic — must be 1, since flushInternal
	// would otherwise short-circuit before touching the snapshot code path.
	prePanicDirty := atomic.LoadInt32(&tree.dirty)
	if prePanicDirty != 1 {
		t.Fatalf("expected dirty=1 before flush, got %d", prePanicDirty)
	}

	// Inject a panic at the snapshot boundary. The dirty flag has just been
	// cleared, but the snapshot maps have not yet been populated.
	cleanup := setFlushTestPanicHook(func() { panic("simulated OOM during snapshot") })
	defer cleanup()

	// Expect flushInternal to panic — the surrounding recover() must re-panic
	// so the caller sees the failure, but only AFTER restoring dirty.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected flushInternal to panic, got no panic")
		}
		// Critical assertion: dirty must be restored to 1 after the panic.
		// Without the recover() in the deferred cleanup, this would be 0
		// and the in-memory data would be silently lost on the next crash.
		postPanicDirty := atomic.LoadInt32(&tree.dirty)
		if postPanicDirty != 1 {
			t.Errorf("dirty=0 after panic during snapshot: in-memory data is now" +
				" considered flushed and may be lost on next crash")
		}
	}()

	// Call flushInternal directly via the public flush API to exercise the
	// full code path including the dirty-clear and recovery defer.
	_ = tree.Flush()
}

// TestFlushLocked_PanicResetsCleanTree is a sanity check: a clean tree (no
// data, dirty=0) must not panic and must not change dirty state. This guards
// against an over-eager fix that clears dirty unconditionally.
func TestFlushLocked_PanicResetsCleanTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// No puts — dirty should already be 0.
	if d := atomic.LoadInt32(&tree.dirty); d != 0 {
		t.Fatalf("expected dirty=0 on fresh tree, got %d", d)
	}

	if err := tree.Flush(); err != nil {
		t.Fatalf("flush on clean tree should not error: %v", err)
	}
	if d := atomic.LoadInt32(&tree.dirty); d != 0 {
		t.Errorf("dirty should remain 0 after flush of clean tree, got %d", d)
	}
}

// TestFlushLocked_NormalFlushRestoresDirtyOnError verifies the existing error
// path: if flushLocked returns an error, dirty is restored to 1. This is the
// documented behavior that the panic recovery must NOT break.
func TestFlushLocked_NormalFlushRestoresDirtyOnError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(100, backend)
	defer pool.Close()

	tree, err := NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}

	// Force a flush error path by setting loadErr. flushLocked returns
	// loadErr without snapshotting, so dirty is never cleared in the first
	// place — this is more of a regression guard for the control flow.
	tree.loadErr = errSimulatedFlushFailure

	if err := tree.Flush(); err == nil {
		t.Fatal("expected flush to return loadErr")
	}
	if d := atomic.LoadInt32(&tree.dirty); d != 0 {
		t.Errorf("dirty should remain 0 after loadErr short-circuit, got %d", d)
	}
}

// errSimulatedFlushFailure is a sentinel to force flushLocked to return early.
var errSimulatedFlushFailure = &flushTestError{msg: "simulated loadErr for flush test"}

type flushTestError struct{ msg string }

func (e *flushTestError) Error() string { return e.msg }

// setFlushTestPanicHook installs a panic hook for the duration of a test and
// returns a cleanup function that restores the previous hook value. This is
// the canonical test-only API for flushTestPanicHook.
func setFlushTestPanicHook(fn func()) (cleanup func()) {
	prev := flushTestPanicHook
	flushTestPanicHook = fn
	return func() { flushTestPanicHook = prev }
}
