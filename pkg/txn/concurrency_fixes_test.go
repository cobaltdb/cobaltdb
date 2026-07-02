package txn

// Regression tests for the 2026-07 concurrency-hardening pass on Manager:
//   1. rollback lock release must synchronize on lockMu (releaseLockEntries)
//   2. pruneVersions must not clear version state visible to a racing Begin
//   3. versionShards must be pruned (watermark) even while txns are active
//   4. sync.Pool transaction recycling ABA guards (ID re-validation)
//   5. Options.LockWaitTimeout honored; timeout<=0 defaults instead of failing
//   6. commit-path lock release uses the txn's own key list, entries removed
//   7. wait-for edges are refreshed while polling for a lock
//   8. checkedTxnUint32 range check
//   9. Commit distinguishes user cancellation from deadline expiry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Fix 1: rollback lock release races -------------------------------------

// TestRollbackLockReleaseConcurrentWithLockTraffic exercises the previously
// unsynchronized m.lockEntries mutation in the rollback path against
// concurrent AcquireLockMode/ReleaseLock traffic. Run with -race: the old
// releaseAllLocksUnderLock mutated the lock table without holding lockMu.
func TestRollbackLockReleaseConcurrentWithLockTraffic(t *testing.T) {
	mgr := NewManager(nil)
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}

	for round := 0; round < 25; round++ {
		victim := mgr.Begin(nil)
		for _, k := range keys {
			if err := mgr.AcquireLock(victim.ID, k, time.Second); err != nil {
				t.Fatalf("victim acquire %s: %v", k, err)
			}
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = victim.Rollback()
		}()

		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn := mgr.Begin(&Options{Isolation: SnapshotIsolation, LockWaitTimeout: 100 * time.Millisecond})
				for _, k := range keys {
					if err := mgr.AcquireLock(txn.ID, k, 50*time.Millisecond); err == nil {
						mgr.ReleaseLock(txn.ID, k)
					}
				}
				_ = txn.Rollback()
			}()
		}
		wg.Wait()
	}

	// After all traffic, every key must be acquirable again (no orphaned entries).
	final := mgr.Begin(nil)
	for _, k := range keys {
		if err := mgr.AcquireLock(final.ID, k, time.Second); err != nil {
			t.Fatalf("lock %s orphaned after concurrent rollbacks: %v", k, err)
		}
	}
	_ = final.Rollback()
}

// --- Fix 2: pruneVersions vs Begin race (lost update) ------------------------

// TestPruneVersionsDoesNotDisableConflictDetection hammers pruneVersions from
// background goroutines while transactions perform read-then-conflicting-write
// cycles. With the old zero-active clear() race, a Begin landing in an
// already-scanned shard was invisible and ALL version state could be wiped
// while it held readVersions, so its commit silently missed the conflict.
func TestPruneVersionsDoesNotDisableConflictDetection(t *testing.T) {
	mgr := NewManager(nil)

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					mgr.pruneVersions()
				}
			}
		}()
	}

	// Seed a version for the key.
	seed := mgr.Begin(nil)
	seed.SetWrite("t", "k", []byte("seed"))
	if err := seed.Commit(); err != nil {
		t.Fatalf("seed commit: %v", err)
	}

	for i := 0; i < 1500; i++ {
		a := mgr.Begin(nil)
		a.SetReadVersion("t", "k", mgr.GetCurrentVersion("t", "k"))

		b := mgr.Begin(nil)
		b.SetWrite("t", "k", []byte("b"))
		if err := b.Commit(); err != nil {
			t.Fatalf("iter %d: b commit: %v", i, err)
		}

		a.SetWrite("t", "k", []byte("a"))
		if err := a.Commit(); !errors.Is(err, ErrConflict) {
			t.Fatalf("iter %d: lost update — expected ErrConflict after concurrent commit, got %v", i, err)
		}
	}

	close(done)
	wg.Wait()
}

// --- Fix 3: watermark pruning of versionShards while txns are active ---------

func countVersionEntries(mgr *Manager) int {
	total := 0
	for i := range mgr.versionShards {
		mgr.versionShards[i].mu.Lock()
		total += len(mgr.versionShards[i].versions)
		mgr.versionShards[i].mu.Unlock()
	}
	return total
}

// TestPruneVersionsWatermarkBoundsMemory verifies that version entries no
// active transaction can conflict on (version <= every active txn's beginSeq)
// are pruned even while a transaction stays active. The old code never pruned
// versionShards in that case, so the maps grew one entry per distinct key ever
// written.
func TestPruneVersionsWatermarkBoundsMemory(t *testing.T) {
	mgr := NewManager(nil)

	commitKeys := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			txn := mgr.Begin(nil)
			txn.SetWrite("t", fmt.Sprintf("%s-%d", prefix, i), []byte("v"))
			if err := txn.Commit(); err != nil {
				t.Fatalf("commit %s-%d: %v", prefix, i, err)
			}
		}
	}

	// Keep total commits below 1000 so Commit's automatic every-1000-commits
	// prune does not fire mid-test.
	// Phase 1: 400 commits, all before the long-running txn begins.
	commitKeys("old", 400)

	holder := mgr.Begin(nil) // beginSeq = 400

	// Phase 2: 400 commits while holder is active (versions 401..800).
	commitKeys("new", 400)

	if got := countVersionEntries(mgr); got != 800 {
		t.Fatalf("expected 800 version entries before prune, got %d", got)
	}

	// Prune with holder active: everything holder began after (phase 1) is
	// prunable; phase-2 versions could still conflict with holder and must stay.
	mgr.pruneVersions()

	if got := countVersionEntries(mgr); got != 400 {
		t.Fatalf("watermark prune with active txn: expected 400 entries, got %d", got)
	}

	// A phase-2 key must survive so holder's conflicts are still detected.
	if v := mgr.GetCurrentVersion("t", "new-0"); v == 0 {
		t.Fatal("phase-2 version pruned while an older active txn could still conflict on it")
	}
	// A phase-1 key must be gone.
	if v := mgr.GetCurrentVersion("t", "old-0"); v != 0 {
		t.Fatalf("phase-1 version %d not pruned despite being below the watermark", v)
	}

	// Holder can still detect conflicts on a phase-2 key it read before a
	// subsequent write (readVersion recorded before another commit bumps it).
	holder.SetReadVersion("t", "new-1", mgr.GetCurrentVersion("t", "new-1"))
	bump := mgr.Begin(nil)
	bump.SetWrite("t", "new-1", []byte("bump"))
	if err := bump.Commit(); err != nil {
		t.Fatalf("bump commit: %v", err)
	}
	holder.SetWrite("t", "new-1", []byte("h"))
	if err := holder.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for holder after prune, got %v", err)
	}

	// With no active transactions left, a prune removes everything.
	mgr.pruneVersions()
	if got := countVersionEntries(mgr); got != 0 {
		t.Fatalf("expected all version entries pruned with no active txns, got %d", got)
	}
}

// --- Fix 4: sync.Pool recycling ABA guards -----------------------------------

func TestIDValidatedHelpersRejectRecycledTransaction(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)
	staleID := txn.ID + 1000 // simulates an ID captured before recycling

	if err := txn.addLockHeldIfActiveID(staleID, "k"); !errors.Is(err, ErrTxnNotFound) {
		t.Fatalf("addLockHeldIfActiveID with stale ID: want ErrTxnNotFound, got %v", err)
	}
	if err := txn.activeStateErrorForID(staleID); !errors.Is(err, ErrTxnNotFound) {
		t.Fatalf("activeStateErrorForID with stale ID: want ErrTxnNotFound, got %v", err)
	}
	txn.setWaitingForID(staleID, 42)
	if got := txn.GetWaitingFor(); got != 0 {
		t.Fatalf("setWaitingForID with stale ID mutated waitingFor: %d", got)
	}
	txn.AddLockHeld("k")
	txn.removeLockHeldIfID(staleID, "k")
	if got := txn.GetLocksHeld(); len(got) != 1 {
		t.Fatalf("removeLockHeldIfID with stale ID removed the lock record: %v", got)
	}
	_ = txn.Rollback()
}

// TestResolveDeadlockIgnoresRecycledPointer simulates the sync.Pool ABA: the
// deadlock detector's snapshot maps a victim ID to a *Transaction that has
// since been recycled into a different (innocent) transaction. The resolver
// must not abort it.
func TestResolveDeadlockIgnoresRecycledPointer(t *testing.T) {
	mgr := NewManager(nil)
	innocent := mgr.Begin(nil)

	staleID := innocent.ID + 5000
	activeTxns := map[uint64]*Transaction{
		// Snapshot entry for a dead txn whose pointer was recycled into innocent.
		staleID: innocent,
	}
	mgr.resolveDeadlock([]uint64{staleID}, activeTxns)

	if err := innocent.activeStateErrorForID(innocent.ID); err != nil {
		t.Fatalf("resolveDeadlock aborted an unrelated recycled transaction: %v", err)
	}
	innocent.SetWrite("t", "k", []byte("v"))
	if err := innocent.Commit(); err != nil {
		t.Fatalf("innocent transaction failed to commit after deadlock resolution: %v", err)
	}
}

// --- Fix 5: LockWaitTimeout handling -----------------------------------------

// TestAcquireLockZeroTimeoutUsesDefaultWait verifies timeout<=0 waits (the
// documented default) instead of failing immediately as the old code did.
func TestAcquireLockZeroTimeoutUsesDefaultWait(t *testing.T) {
	mgr := NewManager(nil)
	holder := mgr.Begin(nil)
	waiter := mgr.Begin(nil)

	if waiter.lockWaitTimeout != 5*time.Second {
		t.Fatalf("default LockWaitTimeout not copied to txn: %v", waiter.lockWaitTimeout)
	}

	if err := mgr.AcquireLock(holder.ID, "kd", time.Second); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- mgr.AcquireLock(waiter.ID, "kd", 0) // 0 → default 5s wait
	}()

	select {
	case err := <-errCh:
		t.Fatalf("timeout=0 returned immediately (%v); should wait for the default LockWaitTimeout", err)
	case <-time.After(150 * time.Millisecond):
		// Still waiting — correct. Release and expect the waiter to succeed.
	}
	mgr.ReleaseLock(holder.ID, "kd")

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("waiter should acquire after release, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not acquire after release")
	}
	_ = holder.Rollback()
	_ = waiter.Rollback()
}

// TestAcquireLockZeroTimeoutUsesCustomLockWaitTimeout verifies a custom
// Options.LockWaitTimeout is stored on the transaction and applied when the
// caller passes timeout<=0.
func TestAcquireLockZeroTimeoutUsesCustomLockWaitTimeout(t *testing.T) {
	mgr := NewManager(nil)
	holder := mgr.Begin(nil)
	waiter := mgr.Begin(&Options{Isolation: SnapshotIsolation, LockWaitTimeout: 80 * time.Millisecond})

	if waiter.lockWaitTimeout != 80*time.Millisecond {
		t.Fatalf("custom LockWaitTimeout not copied to txn: %v", waiter.lockWaitTimeout)
	}

	if err := mgr.AcquireLock(holder.ID, "kc", time.Second); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}

	start := time.Now()
	err := mgr.AcquireLock(waiter.ID, "kc", 0)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected lock wait to time out")
	}
	if elapsed < 60*time.Millisecond {
		t.Fatalf("lock wait returned after %v; custom 80ms LockWaitTimeout not applied", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("lock wait took %v; fell back to the 5s default instead of the custom value", elapsed)
	}
	_ = holder.Rollback()
	_ = waiter.Rollback()
}

// --- Fix 6: commit-path lock release -----------------------------------------

func TestCommitReleasesLockEntries(t *testing.T) {
	mgr := NewManager(nil)
	txn := mgr.Begin(nil)
	if err := mgr.AcquireLock(txn.ID, "commit-key", time.Second); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	txn.SetWrite("t", "commit-key", []byte("v"))
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	mgr.lockMu.RLock()
	_, exists := mgr.lockEntries["commit-key"]
	entries := len(mgr.lockEntries)
	mgr.lockMu.RUnlock()
	if exists || entries != 0 {
		t.Fatalf("commit left lock-table entries behind (exists=%v, total=%d)", exists, entries)
	}
	if got := txn.GetLocksHeld(); len(got) != 0 {
		t.Fatalf("commit left locksHeld records: %v", got)
	}

	// Another transaction must acquire the key promptly.
	next := mgr.Begin(nil)
	if err := mgr.AcquireLock(next.ID, "commit-key", 100*time.Millisecond); err != nil {
		t.Fatalf("lock not released by commit: %v", err)
	}
	_ = next.Rollback()
}

// --- Fix 7: wait-for edges refreshed while polling ---------------------------

func TestWaitForEdgeRefreshedWhenBlockerChanges(t *testing.T) {
	mgr := NewManager(nil)
	a := mgr.Begin(nil)
	b := mgr.Begin(nil)
	c := mgr.Begin(nil)

	// a holds a shared lock; b requests exclusive and blocks on a.
	if err := mgr.AcquireLockMode(a.ID, "edge-key", LockShared, time.Second); err != nil {
		t.Fatalf("a shared acquire: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- mgr.AcquireLockMode(b.ID, "edge-key", LockExclusive, 5*time.Second)
	}()

	waitFor := func(want uint64, what string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if b.GetWaitingFor() == want {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("b.waitingFor=%d, want %d (%s)", b.GetWaitingFor(), want, what)
	}
	waitFor(a.ID, "initial edge to a")

	// c also takes a shared lock (granted alongside a), then a releases:
	// b is now blocked by c and its wait-for edge must be re-pointed.
	if err := mgr.AcquireLockMode(c.ID, "edge-key", LockShared, time.Second); err != nil {
		t.Fatalf("c shared acquire: %v", err)
	}
	mgr.ReleaseLock(a.ID, "edge-key")

	waitFor(c.ID, "edge refreshed to current blocker c")

	mgr.ReleaseLock(c.ID, "edge-key")
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("b failed to acquire after blockers released: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("b never acquired the lock")
	}
	if got := b.GetWaitingFor(); got != 0 {
		t.Fatalf("b still marked waiting after acquisition: %d", got)
	}
	_ = a.Rollback()
	_ = b.Rollback()
	_ = c.Rollback()
}

// --- Fix 8: checkedTxnUint32 range check --------------------------------------

func TestCheckedTxnUint32Range(t *testing.T) {
	if _, err := checkedTxnUint32(-1, "n"); err == nil {
		t.Fatal("expected error for negative value")
	}
	if v, err := checkedTxnUint32(0, "n"); err != nil || v != 0 {
		t.Fatalf("checkedTxnUint32(0) = %d, %v", v, err)
	}
	if v, err := checkedTxnUint32(math.MaxUint32, "n"); err != nil || v != math.MaxUint32 {
		t.Fatalf("checkedTxnUint32(MaxUint32) = %d, %v", v, err)
	}
	// Only exercisable on 64-bit platforms where int can exceed MaxUint32.
	if uint64(^uint(0)>>1) > math.MaxUint32 {
		if _, err := checkedTxnUint32(math.MaxUint32+1, "n"); err == nil {
			t.Fatal("expected error for value above MaxUint32")
		}
	}
}

// --- Fix 9: cancellation vs timeout in Commit ---------------------------------

func TestCommitDistinguishesCancellationFromTimeout(t *testing.T) {
	mgr := NewManager(nil)

	// User cancellation → ErrTxnCancelled (wraps context.Canceled).
	ctx, cancel := context.WithCancel(context.Background())
	txn := mgr.BeginWithContext(ctx, nil)
	txn.SetWrite("t", "k1", []byte("v"))
	cancel()
	err := txn.Commit()
	if !errors.Is(err, ErrTxnCancelled) {
		t.Fatalf("cancelled txn commit: want ErrTxnCancelled, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ErrTxnCancelled must wrap context.Canceled, got %v", err)
	}
	if errors.Is(err, ErrTxnTimeout) {
		t.Fatal("user cancellation misreported as timeout")
	}

	// Deadline expiry → ErrTxnTimeout.
	txn2 := mgr.Begin(&Options{Isolation: SnapshotIsolation, Timeout: 10 * time.Millisecond})
	txn2.SetWrite("t", "k2", []byte("v"))
	time.Sleep(30 * time.Millisecond)
	if err := txn2.Commit(); !errors.Is(err, ErrTxnTimeout) {
		t.Fatalf("timed-out txn commit: want ErrTxnTimeout, got %v", err)
	}
}

// --- General: concurrent begin/lock/commit/rollback stress under -race --------

func TestManagerConcurrentLifecycleStress(t *testing.T) {
	mgr := NewManager(nil)
	mgr.Start()
	defer mgr.Stop()

	var wg sync.WaitGroup
	var commits atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				txn := mgr.Begin(&Options{Isolation: SnapshotIsolation, LockWaitTimeout: 50 * time.Millisecond})
				key := fmt.Sprintf("s%d", i%4)
				if err := mgr.AcquireLock(txn.ID, key, 50*time.Millisecond); err != nil {
					_ = txn.Rollback()
					continue
				}
				txn.SetWrite("t", key, []byte{byte(w)})
				if i%3 == 0 {
					_ = txn.Rollback()
				} else if err := txn.Commit(); err == nil {
					commits.Add(1)
				}
				if i%10 == 0 {
					mgr.pruneVersions()
				}
			}
		}(w)
	}
	wg.Wait()

	if commits.Load() == 0 {
		t.Fatal("no transaction ever committed under contention")
	}
	// Lock table must be fully drained.
	mgr.lockMu.RLock()
	remaining := len(mgr.lockEntries)
	mgr.lockMu.RUnlock()
	if remaining != 0 {
		t.Fatalf("lock table not drained after stress: %d entries", remaining)
	}
}
