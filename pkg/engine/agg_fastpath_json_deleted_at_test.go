package engine

import (
	"context"
	"fmt"
	"testing"
)

// TestAggFastPathIgnoresUserJSONDeletedAtKeys pins that the aggregate fast
// path's byte-level soft-delete check (catalog_fastpath.go
// bytesContainDeletedAt) reads only the row's own version metadata and never
// a "deleted_at" key that lives inside a column's stored JSON document.
//
// A JSON column holding a native nested object (e.g. JSON_EXTRACT('{"deleted_at":42}','$'))
// serializes it unescaped inside the row's "data" array, ahead of the row's
// "version" metadata. When the check matched the FIRST "deleted_at": in the
// row instead of the version one, user data decided the verdict in both
// directions:
//
//   - a live row whose JSON column held a non-zero "deleted_at" was treated as
//     soft-deleted and silently dropped from COUNT(*)/AVG/SUM (AVG(v) over
//     rows 10 and 20 returned 20 instead of 15);
//   - a real tombstone whose JSON column held a zero "deleted_at" was treated
//     as live and resurrected (COUNT(*) returned 2 after deleting one of two
//     rows).
//
// Both control queries (a WHERE clause forcing the full-decode branch, and a
// plain projection route) are asserted so a harness failure cannot masquerade
// as the defect.
func TestAggFastPathIgnoresUserJSONDeletedAtKeys(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE live_rows (id INTEGER PRIMARY KEY, v REAL, payload JSON)`)
	mustExec(t, db, `CREATE TABLE doomed_rows (id INTEGER PRIMARY KEY, v REAL, payload JSON)`)

	// Live rows whose JSON column embeds a "deleted_at" key.
	mustExec(t, db, `INSERT INTO live_rows VALUES (1, 10.0, JSON_EXTRACT('{"deleted_at":42}', '$'))`)
	mustExec(t, db, `INSERT INTO live_rows VALUES (2, 20.0, NULL)`)
	// Rows where one is soft-deleted and the survivor embeds a zero key.
	mustExec(t, db, `INSERT INTO doomed_rows VALUES (1, 10.0, JSON_EXTRACT('{"deleted_at":0}', '$'))`)
	mustExec(t, db, `INSERT INTO doomed_rows VALUES (2, 20.0, NULL)`)
	mustExec(t, db, `DELETE FROM doomed_rows WHERE id = 1`)

	scalar := func(sql string) interface{} {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("query %q returned no rows", sql)
		}
		var out interface{}
		if err := rows.Scan(&out); err != nil {
			t.Fatalf("scan %q: %v", sql, err)
		}
		for rows.Next() {
			t.Fatalf("query %q returned more than one row", sql)
		}
		return out
	}
	want := func(v interface{}, exact string) {
		t.Helper()
		if fmt.Sprint(v) != exact {
			t.Fatalf("FAIL: got %v (%T), want %s", v, v, exact)
		}
	}

	// Direction 1: live rows must never be dropped by the byte fast path.
	want(scalar(`SELECT COUNT(*) FROM live_rows`), "2")
	want(scalar(`SELECT AVG(v) FROM live_rows`), "15")
	want(scalar(`SELECT SUM(v) FROM live_rows`), "30")
	want(scalar(`SELECT MIN(v) FROM live_rows`), "10")
	want(scalar(`SELECT MAX(v) FROM live_rows`), "20")
	want(scalar(`SELECT COUNT(v) FROM live_rows`), "2")

	// Direction 2: a real tombstone must never be resurrected, and the row is
	// gone from the plain projection route too.
	want(scalar(`SELECT COUNT(*) FROM doomed_rows`), "1")
	want(scalar(`SELECT COUNT(*) FROM (SELECT id FROM doomed_rows) AS d`), "1")
	want(scalar(`SELECT AVG(v) FROM doomed_rows`), "20")
	want(scalar(`SELECT SUM(v) FROM doomed_rows`), "20")

	// Secondary branch: the same JSON document reached through a WHERE clause
	// (full-decode branch) must agree with the fast path.
	want(scalar(`SELECT AVG(v) FROM live_rows WHERE id > 0`), "15")
	want(scalar(`SELECT AVG(v) FROM doomed_rows WHERE id > 0`), "20")

	// Text-column control: JSON-escaped inner quotes cannot fake a match.
	mustExec(t, db, `CREATE TABLE text_rows (id INTEGER PRIMARY KEY, payload TEXT)`)
	mustExec(t, db, `INSERT INTO text_rows VALUES (1, '{"deleted_at":42}'), (2, NULL)`)
	want(scalar(`SELECT COUNT(*) FROM text_rows`), "2")
}
