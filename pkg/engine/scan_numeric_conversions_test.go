package engine

import (
	"context"
	"testing"
)

// TestScanNumericConversions pins the database/sql-compatible numeric scan
// conversions: Rows.Scan must accept integer sources into *float64 (and int
// into *int64) exactly as database/sql's convertAssign does. SUM of an
// integral column returns int64 (the sumAccumulator design); consumers
// scanning it into a float64 must not be rejected.
func TestScanNumericConversions(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE sn (v INTEGER)`)
	mustExec(t, db, `INSERT INTO sn VALUES (6)`)

	scanInto := func(sql string, dest interface{}) error {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("query %q: no rows", sql)
		}
		return rows.Scan(dest)
	}

	// The discriminator: SUM returns int64; scanning into *float64 must convert
	// (database/sql convertAssign's explicit int64 case), not error.
	var f float64
	if err := scanInto("SELECT SUM(v) FROM sn", &f); err != nil {
		t.Fatalf("FAIL: SUM int64 into float64 rejected: %v (database/sql converts int64 into *float64)", err)
	}
	if f != 6 {
		t.Fatalf("FAIL: SUM into float64 = %v, want 6", f)
	}

	// Sibling numeric conversions database/sql also performs.
	var f2 float64
	if err := scanInto("SELECT v FROM sn", &f2); err != nil {
		t.Fatalf("FAIL: column int64 into float64 rejected: %v", err)
	}
	if f2 != 6 {
		t.Fatalf("FAIL: column into float64 = %v, want 6", f2)
	}

	var i64 int64
	if err := scanInto("SELECT 6.0", &i64); err != nil {
		t.Fatalf("FAIL: float literal into int64 rejected: %v", err)
	}
	if i64 != 6 {
		t.Fatalf("FAIL: float literal into int64 = %v, want 6", i64)
	}

	var i int
	if err := scanInto("SELECT v FROM sn", &i); err != nil {
		t.Fatalf("FAIL: column int64 into int rejected: %v", err)
	}
	if i != 6 {
		t.Fatalf("FAIL: column into int = %v, want 6", i)
	}

	// Negative control: non-numeric sources must still be rejected.
	var f3 float64
	if err := scanInto("SELECT 'abc'", &f3); err == nil {
		t.Fatal("FAIL: scanning 'abc' into float64 unexpectedly succeeded")
	}
}

// TestScanBytesTextConversions pins the database/sql-compatible []byte scan
// conversions: Rows.Scan must scan TEXT/string/numeric sources into *[]byte
// exactly as database/sql's convertAssign (asBytes) does — []byte is the raw
// rendering of any scalar value.
func TestScanBytesTextConversions(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE sbt (s TEXT, n INTEGER)`)
	mustExec(t, db, `INSERT INTO sbt VALUES ('hello', 42)`)

	scanInto := func(sql string, dest interface{}) error {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("query %q: no rows", sql)
		}
		return rows.Scan(dest)
	}

	// The discriminator: the TEXT column into *[]byte.
	var b []byte
	if err := scanInto("SELECT s FROM sbt", &b); err != nil {
		t.Fatalf("FAIL: TEXT column into []byte rejected: %v (database/sql converts string into []byte)", err)
	}
	if string(b) != "hello" {
		t.Fatalf("FAIL: TEXT into []byte = %q, want hello", b)
	}

	// The string literal.
	var b2 []byte
	if err := scanInto("SELECT 'world'", &b2); err != nil {
		t.Fatalf("FAIL: string literal into []byte rejected: %v", err)
	}
	if string(b2) != "world" {
		t.Fatalf("FAIL: string literal into []byte = %q, want world", b2)
	}

	// The numeric as its string bytes.
	var b3 []byte
	if err := scanInto("SELECT n FROM sbt", &b3); err != nil {
		t.Fatalf("FAIL: integer into []byte rejected: %v", err)
	}
	if string(b3) != "42" {
		t.Fatalf("FAIL: integer into []byte = %q, want 42", b3)
	}
	var b5 []byte
	if err := scanInto("SELECT 'raw'", &b5); err != nil {
		t.Fatalf("FAIL: literal into []byte rejected: %v", err)
	}
	if string(b5) != "raw" {
		t.Fatalf("FAIL: literal into []byte = %q, want raw", b5)
	}
}

// TestScanNullConversions pins the database/sql NULL-scan contract: a NULL
// source scans into any pointer destination as its zero value (and nil for
// *interface{} / *[]byte) — never an error, and never a silent no-op that
// leaves the destination holding the previous row's value.
func TestScanNullConversions(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE nlt (name TEXT, age INTEGER, score REAL, flag BOOLEAN, raw BLOB)`)
	mustExec(t, db, `INSERT INTO nlt VALUES ('Alice', 30, 1.5, TRUE, 'x')`)
	mustExec(t, db, `INSERT INTO nlt VALUES (NULL, NULL, NULL, NULL, NULL)`)

	rows, err := db.Query(ctx, "SELECT name, age, score, flag, raw FROM nlt")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("no first row")
	}
	var name string
	var age int64
	var score float64
	var flag bool
	var raw []byte
	if err := rows.Scan(&name, &age, &score, &flag, &raw); err != nil {
		t.Fatalf("FAIL: first-row scan rejected: %v", err)
	}
	if name != "Alice" || age != 30 || score != 1.5 || flag != true {
		t.Fatalf("FAIL: first-row values wrong: %q %d %v %v", name, age, score, flag)
	}

	if !rows.Next() {
		t.Fatalf("no second row")
	}
	if err := rows.Scan(&name, &age, &score, &flag, &raw); err != nil {
		t.Fatalf("FAIL: NULL row scan rejected: %v (database/sql scans NULL into pointer destinations as zero values)", err)
	}
	if name != "" {
		t.Fatalf("FAIL: NULL name scanned as %q, want \"\" (stale value from previous row — silent no-op)", name)
	}
	if age != 0 {
		t.Fatalf("FAIL: NULL age scanned as %d, want 0", age)
	}
	if score != 0 {
		t.Fatalf("FAIL: NULL score scanned as %v, want 0", score)
	}
	if flag != false {
		t.Fatalf("FAIL: NULL flag scanned as %v, want false", flag)
	}
	if raw != nil {
		t.Fatalf("FAIL: NULL raw scanned as %q, want nil", raw)
	}
}

// TestScanFloatOutOfRangeErrors pins the database/sql-compatible range
// contract for float64 → int/int64 scans: convertAssign converts numeric
// sources into int destinations only when the value is representable (its
// asString → ParseInt round-trip errors otherwise), so an out-of-range or NaN
// float64 must ERROR, never return Go's implementation-defined conversion
// garbage (amd64: the indefinite value -9223372036854775808) with a nil
// error. In-range fractional truncation stays pinned by
// TestScanValueIntEdgeCases / TestScanValueErrors.
func TestScanFloatOutOfRangeErrors(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(ctx, q, args...); err != nil {
			t.Fatalf("%q %v: %v", q, args, err)
		}
	}
	mustExec(`CREATE TABLE sfo_real (r REAL)`)
	mustExec(`INSERT INTO sfo_real VALUES (?)`, 1e300)
	// Two MaxInt64 rows: SUM overflows int64 and the sumAccumulator promotes
	// the result to float64 above MaxInt64 — the natural *int64 scan of a
	// BIGINT sum then hits the same range boundary.
	mustExec(`CREATE TABLE sfo_big (v BIGINT)`)
	mustExec(`INSERT INTO sfo_big VALUES (?)`, int64(9223372036854775807))
	mustExec(`INSERT INTO sfo_big VALUES (?)`, int64(9223372036854775807))

	scanOne := func(sql string, dest ...interface{}) error {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("query %q: no rows", sql)
		}
		return rows.Scan(dest...)
	}

	// Construction guards: the sources really are float64 on the scan path,
	// so the probes below fail for the right reason.
	var fsrc interface{}
	if err := scanOne("SELECT r FROM sfo_real", &fsrc); err != nil {
		t.Fatalf("real source scan: %v", err)
	}
	if _, ok := fsrc.(float64); !ok {
		t.Fatalf("construction changed: REAL column decoded as %T, want float64", fsrc)
	}
	var ssum interface{}
	if err := scanOne("SELECT SUM(v) FROM sfo_big", &ssum); err != nil {
		t.Fatalf("sum scan: %v", err)
	}
	sf, ok := ssum.(float64)
	if !ok || sf <= 9.223372036854775807e18 {
		t.Fatalf("construction changed: SUM = %T(%v), want float64 above MaxInt64", ssum, ssum)
	}

	// Out-of-range float64 into int destinations must error.
	var i64 int64
	if err := scanOne("SELECT r FROM sfo_real", &i64); err == nil {
		t.Fatalf("FAIL: REAL 1e300 into *int64 scanned as %d with no error — out-of-range float64 must error, not return conversion garbage", i64)
	}
	var i int
	if err := scanOne("SELECT r FROM sfo_real", &i); err == nil {
		t.Fatalf("FAIL: REAL 1e300 into *int scanned as %d with no error — out-of-range float64 must error", i)
	}
	if err := scanOne("SELECT SUM(v) FROM sfo_big", &i64); err == nil {
		t.Fatalf("FAIL: overflowed SUM (float64 %v) into *int64 scanned as %d with no error — out-of-range float64 must error", sf, i64)
	}

	// In-range controls on the same paths.
	var frac int64
	if err := scanOne("SELECT 42.5", &frac); err != nil || frac != 42 {
		t.Fatalf("in-range control 42.5→int64: err=%v val=%d (truncation is pinned behavior)", err, frac)
	}
	if err := scanOne("SELECT 'abc'", &frac); err == nil {
		t.Fatalf("in-range control: 'abc'→int64 must still error")
	}
}
