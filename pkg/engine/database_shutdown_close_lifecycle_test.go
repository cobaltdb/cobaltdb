package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Regression tests for the shutdown-signal lifecycle: shutdownCh must be
// closed exactly once, no matter in which order or from how many goroutines
// Close and Shutdown are called. Close previously closed the channel via its
// own select/default probe while Shutdown used shutdownOnce, so a
// Close→Shutdown sequence panicked with "close of closed channel".

// TestCloseThenShutdownIsIdempotent verifies that calling Shutdown after a
// completed Close is a safe no-op rather than a panic (regression for the
// double close of shutdownCh).
func TestCloseThenShutdownIsIdempotent(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open(:memory:) failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown after Close returned error: %v", err)
	}
}

// TestShutdownThenCloseIsIdempotent verifies the reverse order: Shutdown
// performs the close, and a subsequent explicit Close is a no-op.
func TestShutdownThenCloseIsIdempotent(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("Open(:memory:) failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close after Shutdown returned error: %v", err)
	}
}

// TestConcurrentCloseAndShutdownDoNotRace exercises the race face of the same
// root cause: concurrent Close and Shutdown callers previously raced a
// select/default close against shutdownOnce's close. Panics inside the
// goroutines are recovered and reported as test failures; run with -race for
// full effect.
func TestConcurrentCloseAndShutdownDoNotRace(t *testing.T) {
	for i := 0; i < 8; i++ {
		db, err := Open(":memory:", nil)
		if err != nil {
			t.Fatalf("Open(:memory:) failed: %v", err)
		}

		panicCh := make(chan interface{}, 2)
		var wg sync.WaitGroup
		wg.Add(2)

		run := func(fn func() error) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
			}()
			_ = fn()
		}

		go run(func() error { return db.Close() })

		go run(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return db.Shutdown(ctx)
		})

		wg.Wait()
		close(panicCh)

		if r, ok := <-panicCh; ok {
			t.Fatalf("iteration %d: concurrent Close/Shutdown panicked: %v", i, r)
		}

		// A final Close must remain a safe no-op after any interleaving.
		if err := db.Close(); err != nil {
			t.Fatalf("iteration %d: final Close returned error: %v", i, err)
		}
	}
}
