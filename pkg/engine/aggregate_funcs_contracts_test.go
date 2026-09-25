package engine

import (
	"context"
	"sort"
	"strings"
	"testing"
)

// TestAggregateFuncsContracts pins the MySQL aggregate semantics of
// SUM/AVG/MIN/MAX/COUNT/GROUP_CONCAT through the real dispatch:
//
//   - all aggregates skip NULLs; all-NULL input yields NULL (COUNT yields 0),
//   - SUM preserves int64 precision while every input is integral
//     (values above 2^53 must not lose precision to float64),
//   - MIN/MAX order by the catalog comparator and skip NULLs.
//
// Value comparisons use wantEq-style numeric flexibility where the engine's
// result type may vary.
func TestAggregateFuncsContracts(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE agg (v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Rows: 1, 2, 3, NULL — the mixed corpus with one NULL.
	for _, v := range []interface{}{int64(1), int64(2), int64(3), nil} {
		if _, err := db.Exec(ctx, "INSERT INTO agg (v) VALUES (?)", v); err != nil {
			t.Fatalf("insert %v: %v", v, err)
		}
	}

	eq := func(got, want interface{}) bool {
		if got == want {
			return true
		}
		gf, gok := asFloat(got)
		wf, wok := asFloat(want)
		return gok && wok && gf == wf
	}
	check := func(sql string, want interface{}) {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: query: %v", sql, err)
		}
		if !rows.Next() {
			t.Fatalf("%s: no rows", sql)
		}
		var got interface{}
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("%s: scan: %v", sql, err)
		}
		_ = rows.Close()
		if !eq(got, want) {
			t.Fatalf("%s = %#v, want %#v", sql, got, want)
		}
	}

	check("SELECT SUM(v) FROM agg", float64(6)) // NULL skipped
	check("SELECT AVG(v) FROM agg", float64(2)) // (1+2+3)/3
	check("SELECT MIN(v) FROM agg", float64(1)) // NULL skipped
	check("SELECT MAX(v) FROM agg", float64(3)) // NULL skipped
	check("SELECT COUNT(*) FROM agg", int64(4)) // all rows
	check("SELECT COUNT(v) FROM agg", int64(3)) // non-NULL only

	// All-NULL input: aggregates yield NULL, COUNT yields 0.
	if _, err := db.Exec(ctx, "CREATE TABLE agg_null (v INTEGER)"); err != nil {
		t.Fatalf("create null table: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := db.Exec(ctx, "INSERT INTO agg_null (v) VALUES (NULL)"); err != nil {
			t.Fatalf("insert NULL: %v", err)
		}
	}
	for _, sql := range []string{
		"SELECT SUM(v) FROM agg_null",
		"SELECT AVG(v) FROM agg_null",
		"SELECT MIN(v) FROM agg_null",
		"SELECT MAX(v) FROM agg_null",
		"SELECT GROUP_CONCAT(v) FROM agg_null",
	} {
		check(sql, nil)
	}
	check("SELECT COUNT(*) FROM agg_null", int64(3))
	check("SELECT COUNT(v) FROM agg_null", int64(0))

	// Empty set: aggregates yield NULL, COUNT yields 0.
	if _, err := db.Exec(ctx, "CREATE TABLE agg_empty (v INTEGER)"); err != nil {
		t.Fatalf("create empty table: %v", err)
	}
	for _, sql := range []string{
		"SELECT SUM(v) FROM agg_empty",
		"SELECT AVG(v) FROM agg_empty",
		"SELECT MIN(v) FROM agg_empty",
		"SELECT MAX(v) FROM agg_empty",
	} {
		check(sql, nil)
	}
	check("SELECT COUNT(*) FROM agg_empty", int64(0))

	// int64 precision preservation: SUM over values above 2^53 must not
	// lose precision to float64 accumulation (sumAccumulator's contract).
	if _, err := db.Exec(ctx, "CREATE TABLE agg_big (v BIGINT)"); err != nil {
		t.Fatalf("create big table: %v", err)
	}
	big := []int64{9007199254740993, 1, 2} // 2^53+1, +1, +2
	for _, v := range big {
		if _, err := db.Exec(ctx, "INSERT INTO agg_big (v) VALUES (?)", v); err != nil {
			t.Fatalf("insert big %d: %v", v, err)
		}
	}
	rows, err := db.Query(ctx, "SELECT SUM(v) FROM agg_big")
	if err != nil {
		t.Fatalf("big query: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("big: no rows")
	}
	var sumBig interface{}
	if err := rows.Scan(&sumBig); err != nil {
		t.Fatalf("big: scan: %v", err)
	}
	_ = rows.Close()
	sumI, ok := sumBig.(int64)
	if !ok {
		t.Fatalf("SUM over int64 inputs returned %T(%v), want int64 precision", sumBig, sumBig)
	}
	if sumI != 9007199254740996 {
		t.Fatalf("SUM = %d, want %d (precision lost to float64?)", sumI, 9007199254740996)
	}

	// MIN/MAX over strings order lexicographically (NULL skipped).
	if _, err := db.Exec(ctx, "CREATE TABLE agg_s (v TEXT)"); err != nil {
		t.Fatalf("create text table: %v", err)
	}
	for _, s := range []interface{}{"pear", "apple", nil, "banana"} {
		if _, err := db.Exec(ctx, "INSERT INTO agg_s (v) VALUES (?)", s); err != nil {
			t.Fatalf("insert %v: %v", s, err)
		}
	}
	check("SELECT MIN(v) FROM agg_s", "apple")
	check("SELECT MAX(v) FROM agg_s", "pear")

	// GROUP_CONCAT without an internal ORDER BY has an undefined element order
	// (the MySQL contract — the concatenation follows the row-scan order), so
	// assert the value SET rather than a specific concatenation.
	gcRows, err := db.Query(ctx, "SELECT GROUP_CONCAT(v) FROM agg_s")
	if err != nil {
		t.Fatalf("GROUP_CONCAT query: %v", err)
	}
	if !gcRows.Next() {
		t.Fatal("GROUP_CONCAT returned no row")
	}
	var joined string
	if err := gcRows.Scan(&joined); err != nil {
		t.Fatalf("GROUP_CONCAT scan: %v", err)
	}
	gcRows.Close()
	parts := strings.Split(joined, ",")
	sort.Strings(parts)
	if got := strings.Join(parts, ","); got != "apple,banana,pear" {
		t.Errorf("GROUP_CONCAT set: got %q, want the set {apple, banana, pear}", got)
	}
}

// NULLString exists so the insert loop can carry a typed NULL cleanly.
func NULLString() interface{} { return nil }
