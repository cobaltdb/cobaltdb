package engine

import (
	"context"
	"testing"
)

// TestSumTypeConsistentAcrossQueryShapes pins that SUM returns the same Go
// type regardless of query shape. SUM is computed by several routes (the
// aggregate fast path trySimpleAggregateFastPath, its byte-level and
// full-decode branches, and reduceBasicAggregate on grouped/HAVING queries);
// a future change to any route that flips the result type (e.g. introducing
// int64 on one path) would be a Scan-visible inconsistency for consumers.
// Current engine-wide semantics: SUM of an integral column returns float64
// on every route.
func TestSumTypeConsistentAcrossQueryShapes(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE st (id INTEGER PRIMARY KEY, v INTEGER)`)
	mustExec(t, db, `INSERT INTO st VALUES (1, 1), (2, 2), (3, 3)`)

	scanSingle := func(sql string) interface{} {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("query %q: no rows", sql)
		}
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan %q: %v", sql, err)
		}
		return v
	}

	// Shape 1: bare SUM (byte-level fast path branch).
	plain := scanSingle(`SELECT SUM(v) FROM st`)
	// Shape 2: SUM with WHERE (full-decode fast path branch).
	withWhere := scanSingle(`SELECT SUM(v) FROM st WHERE v > 0`)
	// Shape 3: SUM with ORDER BY (fast path declines; standard aggregate route).
	withOrder := scanSingle(`SELECT SUM(v) FROM st ORDER BY 1`)
	// Shape 4: grouped SUM (reduceBasicAggregate route).
	groupedRows, err := db.Query(ctx, `SELECT id, SUM(v) FROM st GROUP BY id ORDER BY id`)
	if err != nil {
		t.Fatalf("grouped query: %v", err)
	}
	defer groupedRows.Close()
	var gsum interface{}
	for groupedRows.Next() {
		var id interface{}
		if err := groupedRows.Scan(&id, &gsum); err != nil {
			t.Fatalf("grouped scan: %v", err)
		}
		break
	}

	shape := func(name string, v interface{}) {
		t.Helper()
		if _, ok := v.(float64); !ok {
			t.Fatalf("FAIL: %s SUM type = %T (%v), want float64", name, v, v)
		}
	}
	shape("plain", plain)
	shape("withWhere", withWhere)
	shape("withOrder", withOrder)
	shape("grouped", gsum)

	if plain != 6.0 {
		t.Fatalf("FAIL: plain SUM = %v, want 6", plain)
	}
	if withWhere != 6.0 || withOrder != 6.0 {
		t.Fatalf("FAIL: filtered/ordered SUM values = %v / %v, want 6", withWhere, withOrder)
	}
	if gsum != 1.0 {
		t.Fatalf("FAIL: grouped SUM = %v, want 1", gsum)
	}
}
