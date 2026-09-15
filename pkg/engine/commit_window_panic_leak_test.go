package engine

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestCommitWindowPanicDoesNotLeakLocks proves that a panic inside the SQL
// COMMIT window does not leak flushMu or replCaptureMu.
//
// CommitStmt read-locks both flushMu and replCaptureMu and releases them with
// straight-line statements after the fallible commit/flush calls. A panic
// anywhere in the window (CommitTransaction, replTxnFlush) skips both
// unlocks; the panic is recovered at the public API boundary (runStatement),
// so the server keeps running while holding both locks forever: every later
// SQL COMMIT and every checkpoint (flushMu) and every replicated write or
// snapshot (replCaptureMu) deadlocks database-wide.
func TestCommitWindowPanicDoesNotLeakLocks(t *testing.T) {
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

	// Explicit transaction so COMMIT takes the window under test.
	if _, err := db.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Arm the hook so COMMIT panics inside the window — after both read
	// locks are held, before CommitTransaction runs.
	commitWindowPanicHook = func() { panic("injected: commit window panic") }
	defer func() { commitWindowPanicHook = nil }()

	_, commitExecErr := db.Exec(ctx, "COMMIT")
	if commitExecErr == nil || !strings.Contains(commitExecErr.Error(), "internal error in Exec") {
		t.Fatalf("precondition failed: expected recovered Exec panic from COMMIT, got %v", commitExecErr)
	}

	// The panic was recovered and the server keeps running — both locks must
	// be fully released. Either leaked lock deadlocks its probe forever (the
	// leaked holder is this same live goroutine).
	probe := func(name string, lock func(), unlock func()) {
		t.Helper()
		acquired := make(chan struct{})
		go func() {
			lock()
			close(acquired)
			unlock()
		}()
		select {
		case <-acquired:
			// fixed: the lock was released despite the panic
		case <-time.After(3 * time.Second):
			t.Fatalf("FAIL: %s deadlocked after a recovered commit-window panic — the lock leaked and later commits/checkpoints now hang", name)
		}
	}
	probe("replCaptureMu", db.replCaptureMu.Lock, db.replCaptureMu.Unlock)
	probe("flushMu", db.flushMu.Lock, db.flushMu.Unlock)
}
