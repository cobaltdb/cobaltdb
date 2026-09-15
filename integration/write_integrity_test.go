package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestUniqueConstraintUnderConcurrency checks a non-PK UNIQUE constraint cannot
// be violated by concurrent inserts racing on the same value.
//
// Regression: UNIQUE is enforced by a check-then-insert that ran under only
// c.mu.RLock(), so concurrent autocommit inserts of the same value all observed
// "no duplicate" and all landed — 16 racers routinely stored 3-5 duplicate rows.
// Autocommit INSERT is now serialized on Catalog.autocommitWriteMu.
func TestUniqueConstraintUnderConcurrency(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE)`)

	const workers = 16
	var accepted int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Every worker tries to claim the SAME email with a different id.
			if _, err := db.Exec(ctx, `INSERT INTO u VALUES (?, 'dup@example.com')`, w); err == nil {
				atomic.AddInt64(&accepted, 1)
			}
		}(w)
	}
	wg.Wait()

	var stored int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM u WHERE email = 'dup@example.com'`).Scan(&stored); err != nil {
		t.Fatalf("count: %v", err)
	}
	if accepted != 1 || stored != 1 {
		t.Errorf("UNIQUE violated under concurrency: %d inserts accepted, %d rows stored (want 1/1)", accepted, stored)
	}
}

// TestProbePrimaryKeyUnderContention has every worker race for the SAME
// primary key value.
func TestPrimaryKeyUnderContention(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE pk (id INT PRIMARY KEY, w INT)`)

	const workers = 16
	var accepted int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			if _, err := db.Exec(ctx, `INSERT INTO pk VALUES (7, ?)`, w); err == nil {
				atomic.AddInt64(&accepted, 1)
			}
		}(w)
	}
	wg.Wait()

	var stored int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM pk WHERE id = 7`).Scan(&stored); err != nil {
		t.Fatalf("count: %v", err)
	}
	if accepted != 1 || stored != 1 {
		t.Errorf("PRIMARY KEY violated under contention: %d accepted, %d stored (want 1/1)", accepted, stored)
	}
}

// TestProbeSecondaryIndexConsistency verifies that after concurrent updates an
// indexed lookup returns the same rows as a full scan. A stale secondary index
// shows up as a mismatch between the two.
func TestSecondaryIndexConsistencyAfterConcurrentUpdates(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE idx (id INT PRIMARY KEY, cat TEXT, n INT)`)
	for i := 0; i < 200; i++ {
		if _, err := db.Exec(ctx, `INSERT INTO idx VALUES (?,?,?)`, i, fmt.Sprintf("c%d", i%5), i); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	mustExec(t, db, ctx, `CREATE INDEX ix_cat ON idx (cat)`)

	const workers, rounds = 8, 60
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				id := (w*rounds + r) % 200
				cat := fmt.Sprintf("c%d", (id+r)%5)
				_, _ = db.Exec(ctx, `UPDATE idx SET cat = ? WHERE id = ?`, cat, id)
			}
		}(w)
	}
	wg.Wait()

	// For each category, an index-eligible predicate must agree with a scan
	// that cannot use the index.
	for c := 0; c < 5; c++ {
		cat := fmt.Sprintf("c%d", c)
		var viaIndex, viaScan int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM idx WHERE cat = ?`, cat).Scan(&viaIndex); err != nil {
			t.Fatalf("indexed count: %v", err)
		}
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM idx WHERE cat || '' = ?`, cat).Scan(&viaScan); err != nil {
			t.Fatalf("scan count: %v", err)
		}
		if viaIndex != viaScan {
			t.Errorf("index/scan disagree for %s: indexed=%d scan=%d", cat, viaIndex, viaScan)
		}
	}

	var total int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM idx`).Scan(&total); err != nil {
		t.Fatalf("total: %v", err)
	}
	if total != 200 {
		t.Errorf("row count drifted to %d, want 200", total)
	}
}

// TestProbeConcurrentUpdateDeleteMix runs updates and deletes against the same
// key space and verifies the surviving rows are internally consistent.
func TestConcurrentUpdateDeleteMix(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE m (id INT PRIMARY KEY, n INT, tag TEXT)`)
	for i := 0; i < 100; i++ {
		if _, err := db.Exec(ctx, `INSERT INTO m VALUES (?,?,'live')`, i, 0); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	var wg sync.WaitGroup
	// Updaters mark rows and bump a counter together; the two columns must stay
	// in agreement for every surviving row.
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, _ = db.Exec(ctx, `UPDATE m SET n = n + 1, tag = 'touched' WHERE id = ?`, i)
			}
		}(w)
	}
	// Deleters remove the upper half.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 50; i < 100; i++ {
				_, _ = db.Exec(ctx, `DELETE FROM m WHERE id = ?`, i)
			}
		}()
	}
	wg.Wait()

	// Any row that was touched must carry the matching tag: n>0 implies
	// tag='touched', since both are set by the same statement.
	var inconsistent int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM m WHERE n > 0 AND tag <> 'touched'`).Scan(&inconsistent); err != nil {
		t.Fatalf("consistency check: %v", err)
	}
	if inconsistent != 0 {
		t.Errorf("%d rows have n>0 but tag<>'touched' (torn multi-column update)", inconsistent)
	}

	var remaining int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM m WHERE id >= 50`).Scan(&remaining); err != nil {
		t.Fatalf("count deleted range: %v", err)
	}
	if remaining != 0 {
		t.Errorf("%d rows survived deletion in the 50..99 range", remaining)
	}
}

// TestProbeSumInvariantUnderConcurrentTransfers moves value between two rows
// concurrently; the total must be conserved.
func TestSumInvariantUnderConcurrentTransfers(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE acct (id INT PRIMARY KEY, bal INT)`)
	mustExec(t, db, ctx, `INSERT INTO acct VALUES (1,1000),(2,1000)`)

	const workers, moves = 8, 60
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			from, to := 1, 2
			if w%2 == 1 {
				from, to = 2, 1
			}
			for i := 0; i < moves; i++ {
				tx, err := db.Begin(ctx)
				if err != nil {
					continue
				}
				if _, err := tx.Exec(ctx, `UPDATE acct SET bal = bal - 1 WHERE id = ?`, from); err != nil {
					_ = tx.Rollback()
					continue
				}
				if _, err := tx.Exec(ctx, `UPDATE acct SET bal = bal + 1 WHERE id = ?`, to); err != nil {
					_ = tx.Rollback()
					continue
				}
				_ = tx.Commit()
			}
		}(w)
	}
	wg.Wait()

	var total int
	if err := db.QueryRow(ctx, `SELECT SUM(bal) FROM acct`).Scan(&total); err != nil {
		t.Fatalf("sum: %v", err)
	}
	if total != 2000 {
		t.Errorf("money not conserved: SUM(bal) = %d, want 2000", total)
	}
}

// TestProbeAutoIncrementUnderConcurrency verifies generated keys stay unique.
func TestAutoIncrementUniqueUnderConcurrency(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(ctx, `CREATE TABLE ai (id INTEGER PRIMARY KEY AUTOINCREMENT, v INT)`); err != nil {
		t.Skipf("AUTOINCREMENT not supported in this form: %v", err)
	}

	const workers, per = 8, 40
	var wg sync.WaitGroup
	var ok int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := db.Exec(ctx, `INSERT INTO ai (v) VALUES (?)`, w); err == nil {
					atomic.AddInt64(&ok, 1)
				}
			}
		}(w)
	}
	wg.Wait()

	var rows, distinct int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM ai`).Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM (SELECT DISTINCT id FROM ai) q`).Scan(&distinct); err != nil {
		t.Fatalf("distinct: %v", err)
	}
	if rows != distinct {
		t.Errorf("AUTOINCREMENT produced duplicate ids: %d rows but %d distinct", rows, distinct)
	}
	if int64(rows) != ok {
		t.Errorf("%d inserts acknowledged but %d rows stored", ok, rows)
	}
}
