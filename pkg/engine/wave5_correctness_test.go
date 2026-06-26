package engine

import (
	"context"
	"testing"
)

// TestSingleStatementTriggerFires verifies a trigger whose body is a single
// statement (no BEGIN...END) actually fires. Before the fix the body was never
// parsed and the trigger was a silent no-op.
func TestSingleStatementTriggerFires(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, `CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)`)
	mustExec(t, db, `CREATE TABLE log (id INTEGER)`)
	mustExec(t, db, `CREATE TRIGGER trg AFTER INSERT ON src FOR EACH ROW INSERT INTO log VALUES (NEW.id)`)
	mustExec(t, db, `INSERT INTO src VALUES (1, 10)`)
	mustExec(t, db, `INSERT INTO src VALUES (2, 20)`)

	var n int64
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM log`).Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 2 {
		t.Fatalf("single-statement trigger did not fire: log has %d rows, want 2", n)
	}
}

// TestPartitionMaxValueAcceptsHighValues verifies VALUES LESS THAN (MAXVALUE)
// is a working catch-all partition. Before the fix it was a dead [0,0) range
// and every value past the previous bound was rejected.
func TestPartitionMaxValueAcceptsHighValues(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, `CREATE TABLE pm (id INTEGER, region INTEGER) PARTITION BY RANGE (region) (PARTITION p0 VALUES LESS THAN (10), PARTITION pmax VALUES LESS THAN (MAXVALUE))`)
	mustExec(t, db, `INSERT INTO pm VALUES (1, 5)`)
	mustExec(t, db, `INSERT INTO pm VALUES (2, 50)`) // belongs in pmax
	mustExec(t, db, `INSERT INTO pm VALUES (3, 9999)`)

	var n int64
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM pm`).Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 3 {
		t.Fatalf("MAXVALUE partition rejected rows: count=%d, want 3", n)
	}
}

// TestBigIntOrderingPrecision verifies that BIGINT values above 2^53 order
// correctly (no float64 precision collapse).
func TestBigIntOrderingPrecision(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, `CREATE TABLE big (b BIGINT)`)
	mustExec(t, db, `INSERT INTO big VALUES (9007199254740992)`)
	mustExec(t, db, `INSERT INTO big VALUES (9007199254740993)`)

	rows, err := db.Query(ctx, `SELECT b FROM big ORDER BY b DESC`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var got []int64
	for rows.Next() {
		var b int64
		if err := rows.Scan(&b); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, b)
	}
	want := []int64{9007199254740993, 9007199254740992}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("BIGINT DESC ordering wrong: got %v, want %v", got, want)
	}
}
