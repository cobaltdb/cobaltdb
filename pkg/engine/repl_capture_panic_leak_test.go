package engine

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestReplicateStatementPanicDoesNotLeakCaptureLock proves that a panic inside
// the replication capture window does not leak replCaptureMu's read lock.
//
// The capture defer in execute releases the read lock with straight-line
// statements after fallible calls. A panic anywhere in the defer body skips
// that unlock; the panic is then recovered at the public API boundary
// (runStatement), so the server keeps running while holding
// replCaptureMu.RLock forever. The next replication snapshot takes
// replCaptureMu.Lock() and blocks — and every subsequent replicated write
// queues behind it: one panic deadlocks all replicated writes database-wide.
func TestReplicateStatementPanicDoesNotLeakCaptureLock(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true, CacheSize: 256},
		Replication: ReplicationConfig{
			Role:       "master",
			ListenAddr: "127.0.0.1:0",
			AuthToken:  "test-token",
		},
	})
	if err != nil {
		t.Fatalf("open master: %v", err)
	}
	defer db.Close()
	if db.replicationMasterManager() == nil {
		t.Fatal("precondition failed: replication master manager is nil")
	}

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Arm the hook so the next replicated write panics inside the capture
	// defer body — skipping its replCaptureMu.RUnlock.
	replCapturePanicHook = func() { panic("injected: capture window panic") }
	defer func() { replCapturePanicHook = nil }()

	_, execErr := db.Exec(ctx, "INSERT INTO t VALUES (1)")
	if execErr == nil || !strings.Contains(execErr.Error(), "internal error in Exec") {
		t.Fatalf("precondition failed: expected recovered Exec panic, got %v", execErr)
	}

	// The panic was recovered and the server keeps running — the capture lock
	// must be fully released. A leaked read lock deadlocks this Lock() forever
	// (the leaked holder is this same live goroutine).
	acquired := make(chan struct{})
	go func() {
		db.replCaptureMu.Lock()
		close(acquired)
		db.replCaptureMu.Unlock()
	}()
	select {
	case <-acquired:
		// fixed: the capture lock was released despite the panic
	case <-time.After(3 * time.Second):
		t.Fatal("FAIL: replCaptureMu deadlocked after a recovered capture-window panic — the read lock leaked and every replicated write now hangs")
	}
}
