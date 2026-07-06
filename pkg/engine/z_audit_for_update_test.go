package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestAuditSelectForUpdatePreventsLostUpdate verifies that SELECT ... FOR UPDATE
// provides optimistic lost-update prevention for the read-modify-write pattern:
// a row read under FOR UPDATE is added to the transaction's read set, so a
// concurrent modification makes this transaction's COMMIT fail with a conflict
// (which the application retries), instead of silently overwriting the concurrent
// change. Without it, `SELECT v; UPDATE SET v=<computed>` loses updates under the
// engine's Read Committed isolation.
func TestAuditSelectForUpdatePreventsLostUpdate(t *testing.T) {
	ctx := context.Background()
	db := mustOpenMem(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE a (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO a VALUES (1, 100)")

	// Deterministic conflict: T1 reads under FOR UPDATE, a concurrent writer
	// commits a change, then T1's stale write must be rejected at COMMIT.
	done := make(chan error, 1)
	barrier := make(chan struct{})
	proceed := make(chan struct{})
	go func() {
		_, _ = db.Exec(ctx, "BEGIN")
		rows, _ := db.Query(ctx, "SELECT v FROM a WHERE id = 1 FOR UPDATE")
		var v int64
		if rows.Next() {
			_ = rows.Scan(&v)
		}
		rows.Close()
		close(barrier) // FOR UPDATE read done
		<-proceed      // wait for the concurrent writer to commit
		_, _ = db.Exec(ctx, "UPDATE a SET v = 101 WHERE id = 1")
		_, cerr := db.Exec(ctx, "COMMIT")
		if cerr != nil {
			_, _ = db.Exec(ctx, "ROLLBACK")
		}
		done <- cerr
	}()

	<-barrier
	// Concurrent modification (autocommit on another goroutine).
	go func() {
		_, _ = db.Exec(ctx, "UPDATE a SET v = 200 WHERE id = 1")
		close(proceed)
	}()

	commitErr := <-done
	if commitErr == nil {
		t.Fatal("expected COMMIT to conflict after a concurrent modification of a FOR UPDATE'd row (lost update not prevented)")
	}
	// The concurrent writer's value must survive; the stale update must not.
	rows, err := db.Query(ctx, "SELECT v FROM a WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var final int64
	if rows.Next() {
		_ = rows.Scan(&final)
	}
	if final != 200 {
		t.Fatalf("final v = %d, want 200 (concurrent write should win, stale FOR UPDATE txn should have aborted)", final)
	}
}

// TestAuditForUpdateBankTransferConserves runs concurrent read-modify-write
// transfers using SELECT ... FOR UPDATE and asserts the total balance is
// conserved (no lost updates). With a bare SELECT-then-UPDATE the total drifts.
func TestAuditForUpdateBankTransferConserves(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent bank-transfer stress in -short mode")
	}
	ctx := context.Background()
	db := mustOpenMem(t)
	defer db.Close()
	const N, start = 8, 1000
	mustExec(t, db, "CREATE TABLE acct (id INTEGER PRIMARY KEY, bal INTEGER)")
	for i := 0; i < N; i++ {
		mustExec(t, db, fmt.Sprintf("INSERT INTO acct VALUES (%d, %d)", i, start))
	}
	want := int64(N * start)

	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			x := uint64(seed*2654435761 + 1)
			for k := 0; k < 200; k++ {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				a := int(x % N)
				b := int((x / N) % N)
				if a == b {
					continue
				}
				for retry := 0; retry < 100; retry++ {
					if _, e := db.Exec(ctx, "BEGIN"); e != nil {
						break
					}
					ba := selForUpd(ctx, db, a)
					bb := selForUpd(ctx, db, b)
					_, _ = db.Exec(ctx, fmt.Sprintf("UPDATE acct SET bal = %d WHERE id = %d", ba-10, a))
					_, _ = db.Exec(ctx, fmt.Sprintf("UPDATE acct SET bal = %d WHERE id = %d", bb+10, b))
					if _, e := db.Exec(ctx, "COMMIT"); e != nil {
						_, _ = db.Exec(ctx, "ROLLBACK")
						continue
					}
					break
				}
			}
		}(w)
	}
	wg.Wait()

	rows, err := db.Query(ctx, "SELECT SUM(bal) FROM acct")
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	defer rows.Close()
	var total int64
	if rows.Next() {
		_ = rows.Scan(&total)
	}
	if total != want {
		t.Fatalf("total balance = %d, want %d (lost/created money under FOR UPDATE)", total, want)
	}
}

func selForUpd(ctx context.Context, db *DB, id int) int64 {
	rows, err := db.Query(ctx, fmt.Sprintf("SELECT bal FROM acct WHERE id = %d FOR UPDATE", id))
	if err != nil {
		return 0
	}
	defer rows.Close()
	var v int64
	if rows.Next() {
		_ = rows.Scan(&v)
	}
	return v
}
