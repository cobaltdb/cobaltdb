package server

import (
	"context"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestLoadShedderDoesNotBlockBreakerRecovery pins the recovery path: a
// half-open breaker must still receive the sequential successful probes it
// needs to reach MinSuccesses.
//
// Regression: LoadShedder.Admit shed ALL non-critical traffic (every MySQL
// command, ProductionServer.Exec/Query) while any breaker was HalfOpen —
// before the breaker was ever consulted. With the defaults MinSuccesses=3 and
// HalfOpenMaxRequests=1, probe 1 succeeded and every later probe was shed, so
// successes froze at 1 and the breaker stayed half-open forever: after any
// transient outage the server shed 100% of SQL traffic permanently.
func TestLoadShedderDoesNotBlockBreakerRecovery(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := DefaultProductionConfig()
	cfg.CircuitBreaker.ResetTimeout = 20 * time.Millisecond // fast deterministic cooldown
	ps := NewProductionServer(db, cfg)

	// The SQL path routes SELECTs through this breaker (circuitBreakerKey).
	cb := ps.CircuitBreakers.GetOrCreate("SELECT", ps.Config.CircuitBreaker)
	for i := 0; i < cfg.CircuitBreaker.MaxFailures; i++ {
		cb.ReportFailure()
	}
	time.Sleep(2 * cfg.CircuitBreaker.ResetTimeout) // past the reset backoff

	ctx := context.Background()

	// Probe 1: the shedder sees state Open (the Open->HalfOpen transition is
	// lazy inside Allow), so it passes and drives the breaker half-open.
	rows, err := ps.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("probe 1: %v", err)
	}
	rows.Close()
	if got := cb.State(); got != engine.CircuitHalfOpen {
		t.Fatalf("probe 1: breaker state = %v, want half-open", got)
	}

	// The remaining MinSuccesses-1 probes must be admitted; shedding them
	// would deadlock the breaker in half-open forever.
	for i := 2; i <= cfg.CircuitBreaker.MinSuccesses; i++ {
		rows, err := ps.Query(ctx, "SELECT 1")
		if err != nil {
			t.Fatalf("probe %d shed while breaker half-open: %v", i, err)
		}
		rows.Close()
	}
	if got := cb.State(); got != engine.CircuitClosed {
		t.Fatalf("breaker state = %v, want closed after %d probes", got, cfg.CircuitBreaker.MinSuccesses)
	}

	// Traffic flows normally again after recovery.
	rows, err = ps.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("post-recovery query: %v", err)
	}
	rows.Close()
}
