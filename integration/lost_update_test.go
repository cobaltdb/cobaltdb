package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// counterMode selects how each increment of the shared counter is issued.
type counterMode int

const (
	// modeAutocommit issues a bare single-statement read-modify-write.
	modeAutocommit counterMode = iota
	// modeExplicit wraps the same statement in BEGIN/COMMIT.
	modeExplicit
	// modeForUpdate reads with SELECT ... FOR UPDATE, then writes back.
	modeForUpdate
)

type counterResult struct {
	succeeded int64
	conflicts int64
	// exhausted counts increments that burned the entire retry budget without
	// ever succeeding. Without it an exhausted increment is indistinguishable
	// from a lost update: both leave succeeded short and the counter low, so a
	// failure could not say which one happened.
	exhausted int64
	final     int
}

// runConcurrentCounter increments a single row from several goroutines and
// reports the final value. Conflicts are retried, so a correct engine must end
// at exactly workers*incs regardless of mode.
func runConcurrentCounter(t *testing.T, mode counterMode, workers, incs int) counterResult {
	t.Helper()

	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, ctx, `CREATE TABLE ctr (id INT PRIMARY KEY, n INT)`)
	mustExec(t, db, ctx, `INSERT INTO ctr VALUES (1,0)`)

	var succeeded, conflicts, exhausted int64
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < incs; i++ {
				// Retry is part of the contract for the transactional modes:
				// a detected write-write conflict aborts one writer.
				const maxAttempts = 500
				ok := false
				for attempt := 0; attempt < maxAttempts; attempt++ {
					if err := incrementOnce(ctx, db, mode); err == nil {
						atomic.AddInt64(&succeeded, 1)
						ok = true
						break
					}
					atomic.AddInt64(&conflicts, 1)
				}
				if !ok {
					// Every attempt conflicted. Record it explicitly instead of
					// letting the increment vanish, so the assertion below can
					// separate "never applied" from "lost update".
					atomic.AddInt64(&exhausted, 1)
				}
			}
		}()
	}
	wg.Wait()

	var final int
	if err := db.QueryRow(ctx, `SELECT n FROM ctr WHERE id = 1`).Scan(&final); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return counterResult{succeeded: succeeded, conflicts: conflicts, exhausted: exhausted, final: final}
}

func incrementOnce(ctx context.Context, db *engine.DB, mode counterMode) error {
	switch mode {
	case modeAutocommit:
		_, err := db.Exec(ctx, `UPDATE ctr SET n = n + 1 WHERE id = 1`)
		return err

	case modeExplicit:
		tx, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE ctr SET n = n + 1 WHERE id = 1`); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()

	default: // modeForUpdate
		tx, err := db.Begin(ctx)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT n FROM ctr WHERE id = 1 FOR UPDATE`)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		var cur int
		if rows.Next() {
			if err := rows.Scan(&cur); err != nil {
				_ = rows.Close()
				_ = tx.Rollback()
				return err
			}
		}
		_ = rows.Close()
		if _, err := tx.Exec(ctx, `UPDATE ctr SET n = ? WHERE id = 1`, cur+1); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
}

// TestNoLostUpdatesAcrossConcurrencyModes pins the engine's core write-safety
// guarantee: a concurrent read-modify-write must never silently lose an
// increment, in any of the three ways a client can issue it.
//
// Regression: autocommit UPDATE previously lost ~90% of increments while
// reporting success for every statement. Catalog.Update holds only a read
// lock for the whole statement, so concurrent updaters overlapped; only
// explicit transactions had commit-time conflict detection. Autocommit
// UPDATE/DELETE are now serialized on Catalog.autocommitWriteMu.
func TestNoLostUpdatesAcrossConcurrencyModes(t *testing.T) {
	const workers, incs = 8, 50
	want := workers * incs

	modes := []struct {
		name string
		mode counterMode
	}{
		{"autocommit", modeAutocommit},
		{"explicit_transaction", modeExplicit},
		{"select_for_update", modeForUpdate},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			got := runConcurrentCounter(t, m.mode, workers, incs)
			t.Logf("%s: succeeded=%d conflicts=%d exhausted=%d final=%d",
				m.name, got.succeeded, got.conflicts, got.exhausted, got.final)

			// Every increment must be accounted for exactly once: either it
			// committed, or it burned the retry budget and is reported as such.
			// An increment that is neither would silently disappear.
			if total := got.succeeded + got.exhausted; total != int64(want) {
				t.Errorf("%s: succeeded(%d) + retry-exhausted(%d) = %d, want %d: %d increments unaccounted for",
					m.name, got.succeeded, got.exhausted, total, want, int64(want)-total)
			}
			if got.exhausted != 0 {
				t.Errorf("%s: %d increments exhausted the 500-attempt retry budget", m.name, got.exhausted)
			}
			if got.succeeded != int64(want) {
				t.Errorf("%s: %d/%d increments succeeded within the retry budget", m.name, got.succeeded, want)
			}
			if got.final != want {
				t.Errorf("%s: counter = %d, want %d (%d updates lost)", m.name, got.final, want, want-got.final)
			}
		})
	}
}

// TestAutocommitUpdateReportsSuccessOnlyWhenApplied guards the specific
// failure signature of the regression above: every statement returned success
// and a non-zero RowsAffected, yet most increments never landed.
func TestAutocommitUpdateReportsSuccessOnlyWhenApplied(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, ctx, `CREATE TABLE ctr (id INT PRIMARY KEY, n INT)`)
	mustExec(t, db, ctx, `INSERT INTO ctr VALUES (1,0)`)

	const workers, incs = 8, 50
	var applied int64
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < incs; i++ {
				res, err := db.Exec(ctx, `UPDATE ctr SET n = n + 1 WHERE id = 1`)
				if err != nil {
					continue
				}
				atomic.AddInt64(&applied, res.RowsAffected)
			}
		}()
	}
	wg.Wait()

	var final int
	if err := db.QueryRow(ctx, `SELECT n FROM ctr WHERE id = 1`).Scan(&final); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	// Every row the engine claimed to have updated must be reflected in the value.
	if int64(final) != applied {
		t.Errorf("engine reported %d rows updated but counter reached %d", applied, final)
	}
}
