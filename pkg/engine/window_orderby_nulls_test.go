package engine

import (
	"context"
	"testing"
)

// The window ORDER BY loop (parseWindowExpr) lacked NULLS FIRST/LAST parsing
// while the general ORDER BY tail supported it, so `OVER (ORDER BY v NULLS
// LAST)` failed to parse. Placement is observable through a running COUNT(v):
// with rows (1,NULL),(2,10),(3,20), NULLS FIRST puts the NULL row first
// (COUNT(v)=0 there) and NULLS LAST puts it last (COUNT(v)=2 there).
func TestWindowOrderByNullsPlacement(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExecWindow(t, db, "CREATE TABLE w (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecWindow(t, db, "INSERT INTO w VALUES (1, NULL), (2, 10), (3, 20)")

	cases := []struct {
		name     string
		sql      string
		wantByID map[int]int
	}{
		{"NullsFirst", "SELECT id, COUNT(v) OVER (ORDER BY v NULLS FIRST) AS c FROM w", map[int]int{1: 0, 2: 1, 3: 2}},
		{"NullsLast", "SELECT id, COUNT(v) OVER (ORDER BY v NULLS LAST) AS c FROM w", map[int]int{1: 2, 2: 1, 3: 2}},
		{"DescNullsFirst", "SELECT id, COUNT(v) OVER (ORDER BY v DESC NULLS FIRST) AS c FROM w", map[int]int{1: 0, 2: 2, 3: 1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tc.sql+" ORDER BY id")
			if err != nil {
				t.Fatalf("window NULLS placement query failed to parse/execute: %v", err)
			}
			cols := rows.Columns()
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			for rows.Next() {
				if err := rows.Scan(ptrs...); err != nil {
					t.Fatalf("scan: %v", err)
				}
				id := toIntWindow(vals[0])
				want, ok := tc.wantByID[id]
				if !ok {
					t.Fatalf("unexpected id %v", vals[0])
				}
				got := toIntWindow(vals[1])
				if got != want {
					t.Fatalf("%s: COUNT(v) for id=%d = %v, want %d (NULL placement not honored)", tc.name, id, vals[1], want)
				}
			}
			_ = rows.Close()
		})
	}
}

// The general ORDER BY path already honors NULLS FIRST/LAST and the
// ASC/DESC defaults (ASC: NULLs last, DESC: NULLs first) — pinned here so
// the window support matches the same contract.
func TestOrderByNullsPlacementGeneral(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExecWindow(t, db, "CREATE TABLE w (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExecWindow(t, db, "INSERT INTO w VALUES (1, NULL), (2, 10), (3, 20)")

	orders := []struct {
		name string
		sql  string
		want []interface{} // v values in returned order (nil == NULL)
	}{
		{"NullsFirst", "SELECT v FROM w ORDER BY v NULLS FIRST", []interface{}{nil, int64(10), int64(20)}},
		{"NullsLast", "SELECT v FROM w ORDER BY v NULLS LAST", []interface{}{int64(10), int64(20), nil}},
		{"DescNullsFirst", "SELECT v FROM w ORDER BY v DESC NULLS FIRST", []interface{}{nil, int64(20), int64(10)}},
		{"AscDefault", "SELECT v FROM w ORDER BY v", []interface{}{int64(10), int64(20), nil}},
		{"DescDefault", "SELECT v FROM w ORDER BY v DESC", []interface{}{nil, int64(20), int64(10)}},
	}
	for _, tc := range orders {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			cols := rows.Columns()
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			var got []interface{}
			for rows.Next() {
				if err := rows.Scan(ptrs...); err != nil {
					t.Fatalf("scan: %v", err)
				}
				got = append(got, vals[0])
			}
			_ = rows.Close()
			if len(got) != len(tc.want) {
				t.Fatalf("got %d rows, want %d", len(got), len(tc.want))
			}
			for i := range tc.want {
				wantIsNull := tc.want[i] == nil
				gotIsNull := got[i] == nil
				if wantIsNull != gotIsNull || (!wantIsNull && got[i] != tc.want[i]) {
					t.Fatalf("%s: row %d = %v, want %v", tc.name, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func mustExecWindow(t *testing.T, db interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (Result, error)
}, sql string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func toIntWindow(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	default:
		return -1
	}
}
