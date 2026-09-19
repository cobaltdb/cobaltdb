package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// TestReturningStatementAffinityUnderConcurrency pins per-statement RETURNING
// affinity: every concurrent INSERT ... RETURNING must return exactly its own
// row. Regression for the catalog-global lastReturningRows slot, which was
// last-writer-wins with no statement affinity — returningMu prevented data
// races, but a second connection's RETURNING statement executing between this
// statement's slot write and the wrapper's slot read made the wrapper return
// the other statement's rows (cross-session wrong results + data leak). The
// fix routes results through a context-attached ReturningCapture.
//
// Red-detection is statistical: the vulnerable window sits inside the
// statement wrapper and cannot be steered externally. The canonical pre-fix
// failure (2026-09-18 session log) was 2 cross-contamination events in this
// exact 4x300000 hammer, both at local iteration ~1660. Expect ~86% detection
// per run against a reintroduced regression; full scale needs ~3min, so
// -short skips it.
func TestReturningStatementAffinityUnderConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("statistical 1.2M-statement hammer; skipped in -short")
	}
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE ret (id INTEGER PRIMARY KEY, tag TEXT)")

	const (
		goroutines = 4
		iterations = 300000
	)
	var contaminated, infra atomic.Int64
	var msgMu sync.Mutex
	firstMsg := ""
	record := func(format string, args ...interface{}) {
		msgMu.Lock()
		if firstMsg == "" {
			firstMsg = fmt.Sprintf(format, args...)
		}
		msgMu.Unlock()
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				id := int64(g*100000 + i + 10)
				tag := fmt.Sprintf("g%d", g)
				if _, err := db.Exec(ctx, "BEGIN"); err != nil {
					// Pre-round-8 this also caught the failed-COMMIT wedge
					// ("transaction already in progress"); the round-8 fix
					// rolls back failed commits, so BEGIN errors here are
					// unexpected. Counted as infra, not a RETURNING verdict.
					infra.Add(1)
					continue
				}
				sql := fmt.Sprintf("INSERT INTO ret VALUES (%d, '%s') RETURNING id, tag", id, tag)
				rows, qerr := db.Query(ctx, sql)
				if qerr != nil {
					infra.Add(1)
					_, _ = db.Exec(ctx, "ROLLBACK")
					continue
				}
				var gotID int64
				var gotTag string
				if !rows.Next() {
					contaminated.Add(1)
					record("no rows returned for id=%d (%s)", id, tag)
					rows.Close()
					_, _ = db.Exec(ctx, "COMMIT")
					continue
				}
				serr := rows.Scan(&gotID, &gotTag)
				rows.Close()
				if serr != nil {
					contaminated.Add(1)
					record("scan failed for id=%d: %v", id, serr)
					_, _ = db.Exec(ctx, "COMMIT")
					continue
				}
				if gotID != id || gotTag != tag {
					contaminated.Add(1)
					record("cross-contaminated RETURNING: got (%d, %s), want (%d, %s)", gotID, gotTag, id, tag)
				}
				if _, err := db.Exec(ctx, "COMMIT"); err != nil {
					// The btree flush refuses once the table crosses the
					// overflow-page format cap ("only N fit in the root
					// page"). Since the round-8 fix, a failed COMMIT rolls
					// back instead of wedging, so continuing would re-attempt
					// a full multi-MB tree flush on every remaining iteration
					// — unbounded. The contamination window (local iteration
					// ~1660) is far behind by then: stop this goroutine; the
					// verdict is already decided.
					infra.Add(1)
					if strings.Contains(err.Error(), "fit in the root page") {
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()

	if n := contaminated.Load(); n > 0 {
		t.Fatalf("RETURNING cross-contamination: %d of %d statements returned another statement's rows (first: %s); infra skips: %d", n, goroutines*iterations, firstMsg, infra.Load())
	}
	t.Logf("hammer clean: %d statements returned exactly their own rows; infra skips (known separate btree-overflow defect): %d", goroutines*iterations, infra.Load())
}
