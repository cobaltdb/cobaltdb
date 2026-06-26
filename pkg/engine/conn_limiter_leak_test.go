package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestConnLimiterNoSlotLeakUnderTimeoutRace exercises the connection limiter's
// release-vs-acquire-cancel race. When releaseConnection hands a slot to a
// parked waiter at the same instant the waiter's context is cancelled, the
// waiter's select may take the cancellation branch while a slot token sits
// buffered in its channel. That slot must be reclaimed, not leaked: before the
// fix, connCount was permanently incremented on each such race and the limiter
// eventually wedged.
//
// Releasing the held slot and cancelling the waiter near-simultaneously makes
// both the handoff and the cancellation ready in the waiter's select, so the
// leak window is hit frequently across many rounds.
func TestConnLimiterNoSlotLeakUnderTimeoutRace(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage:    CoreStorage{InMemory: true, CacheSize: 256},
		ConnectionPool: ConnectionPool{MaxConnections: 1},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if db.connLimit != 1 {
		t.Fatalf("expected connLimit=1, got %d", db.connLimit)
	}

	for round := 0; round < 500; round++ {
		// Occupy the single slot so the next acquirer must queue as a waiter.
		if err := db.acquireConnection(context.Background()); err != nil {
			t.Fatalf("round %d: initial acquire failed: %v", round, err)
		}

		wctx, wcancel := context.WithCancel(context.Background())
		acquired := make(chan error, 1)
		go func() {
			acquired <- db.acquireConnection(wctx)
		}()

		// Let the waiter reach its select and enqueue.
		time.Sleep(100 * time.Microsecond)

		// Race: hand off the held slot AND cancel the waiter at the same time.
		var releaseWg sync.WaitGroup
		releaseWg.Add(1)
		go func() {
			defer releaseWg.Done()
			db.releaseConnection()
		}()
		wcancel()

		if err := <-acquired; err == nil {
			// The waiter received the slot; return it.
			db.releaseConnection()
		}
		// Wait for the held-slot release to finish so each round is balanced
		// before asserting at the end (the race above is still exercised).
		releaseWg.Wait()
	}

	// Every round is balanced (one acquire, one release, one waiter that either
	// acquired+released or timed out+reclaimed), so the limiter must drain.
	if got := db.connCount.Load(); got != 0 {
		t.Fatalf("connCount leaked: got %d, want 0", got)
	}

	// And the limiter must still grant the single slot (not wedged).
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := db.acquireConnection(ctx); err != nil {
		t.Fatalf("limiter wedged after races: %v", err)
	}
	db.releaseConnection()
}
