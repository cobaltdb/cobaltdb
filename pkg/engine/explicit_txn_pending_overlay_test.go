package engine

import (
	"context"
	"testing"
)

// TestExplicitTxnAggregateSeesPendingWrite pins read-your-writes for the
// FIRST read following a single buffered write inside an explicit transaction.
//
// Regression: getEffectiveTableData (pkg/catalog/catalog_core.go) read
// ts.pendingWriteMap directly instead of via getPendingWriteMap.
// appendPendingWriteTs deliberately leaves the map nil until the SECOND
// buffered write (a lazy-allocation fast path), so after exactly one INSERT
// the overlay silently applied nothing: the first in-txn GROUP BY aggregate
// or JOIN returned pre-insert results (0 groups / 0 joined rows), while a
// plain SELECT — which uses the accessor — would have rebuilt the map and
// masked the defect. The fix routes the overlay through the accessor like
// every other consumer.
func TestExplicitTxnAggregateSeesPendingWrite(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	mustExec := func(sql string) {
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	countOf := func(sql string) int {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("query %q returned no rows", sql)
		}
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		return n
	}

	// t has NO primary key: keyInPendingWrites (the dedup check) never runs,
	// so the lazy nil-map state survives the INSERT.
	mustExec("CREATE TABLE t (id INTEGER, v TEXT)")
	mustExec("CREATE TABLE t2 (id INTEGER)")
	mustExec("INSERT INTO t2 (id) VALUES (1)")

	mustExec("BEGIN")
	mustExec("INSERT INTO t (id, v) VALUES (1, 'pending-row')")

	// Discriminator 1: the FIRST in-txn read is the aggregate. The accessor
	// must rebuild the nil map so the overlay applies the pending write.
	if n := countOf("SELECT COUNT(*) FROM (SELECT v, COUNT(*) AS c FROM t GROUP BY v) g"); n != 1 {
		t.Fatalf("in-txn GROUP BY aggregate saw %d group(s), want 1 — the single pending write is invisible to getEffectiveTableData's overlay", n)
	}

	// Discriminator 2: the JOIN caller (catalog_select.go) shares the overlay.
	if n := countOf("SELECT COUNT(*) FROM t JOIN t2 ON t.id = t2.id"); n != 1 {
		t.Fatalf("in-txn JOIN saw %d row(s), want 1 — the single pending write is invisible to the join overlay", n)
	}

	// Control: the plain SELECT (accessor path) sees the pending row too.
	if n := countOf("SELECT COUNT(*) FROM t"); n != 1 {
		t.Fatalf("in-txn plain COUNT saw %d row(s), want 1", n)
	}

	mustExec("ROLLBACK")

	// The buffered write must be discarded by the rollback.
	if n := countOf("SELECT COUNT(*) FROM t"); n != 0 {
		t.Fatalf("post-rollback COUNT = %d, want 0 — the buffered write leaked", n)
	}
}
