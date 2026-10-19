package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// TestWindowAggregateOverJoinHiddenValueColumn pins that an AGGREGATE window
// function (SUM/AVG/MIN/MAX) resolves its VALUE expression column over a JOIN
// even when that column is not in the SELECT list.
//
// Pre-fix, resolveHiddenWindowCols collected hidden columns from PARTITION BY
// and ORDER BY only, never from the window's Args. evalWindowExprOnRow
// resolves an identifier by scanning selectCols and falls back to
// table-column lookups guarded by `if table != nil`; the JOIN path calls
// evaluateWindowFunctions with table=nil and fullRows=nil, so every fallback
// is dead and the value column resolved to nil. The aggregate then returned
// NULL with no error, and the answer silently depended on whether the column
// happened to also be listed in the SELECT list:
//
//	SELECT a.id, SUM(a.v) OVER () FROM a JOIN b  -> [1 NULL] [2 NULL] [3 NULL]
//	SELECT a.id, a.v, SUM(a.v) OVER () FROM a JOIN b -> [1 10 60] ...
//
// A silent wrong answer is worse than an error here: a running total or a
// per-group average over joined rows quietly read as "no data".
func TestWindowAggregateOverJoinHiddenValueColumn(t *testing.T) {
	db := setupWindowJoinFixture(t)
	defer db.Close()

	// The value column a.v is deliberately ABSENT from every SELECT list below.
	cases := []struct {
		sel  string
		want string
	}{
		{"SELECT a.id, SUM(a.v) OVER () FROM a JOIN b ON a.id=b.id", "60"},
		{"SELECT a.id, AVG(a.v) OVER () FROM a JOIN b ON a.id=b.id", "20"},
		{"SELECT a.id, MIN(a.v) OVER () FROM a JOIN b ON a.id=b.id", "10"},
		{"SELECT a.id, MAX(a.v) OVER () FROM a JOIN b ON a.id=b.id", "30"},
		{"SELECT a.id, SUM(a.v) OVER (PARTITION BY a.id) FROM a JOIN b ON a.id=b.id", "10"},
		// Value column from the JOINED table, not the driving one.
		{"SELECT a.id, SUM(b.w) OVER () FROM a JOIN b ON a.id=b.id", "600"},
		// Window ORDER BY plus a hidden value column together.
		{"SELECT a.id, SUM(a.v) OVER (ORDER BY a.id) FROM a JOIN b ON a.id=b.id", "10"},
	}
	for _, tt := range cases {
		rows, err := db.Query(context.Background(), tt.sel)
		if err != nil {
			t.Errorf("query %q errored: %v", tt.sel, err)
			continue
		}
		if !rows.Next() {
			t.Errorf("query %q returned no rows", tt.sel)
			rows.Close()
			continue
		}
		vals := make([]interface{}, len(rows.Columns()))
		ptrs := make([]interface{}, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if serr := rows.Scan(ptrs...); serr != nil {
			t.Errorf("scan %q: %v", tt.sel, serr)
			rows.Close()
			continue
		}
		rows.Close()
		// The first row of an OVER () / ORDER BY frame is the running value,
		// which for these fixtures is the smallest value.
		if got := fmt.Sprintf("%v", vals[1]); got != tt.want {
			t.Errorf("query %q: window value = %s, want %s "+
				"(a column absent from the SELECT list resolved to NULL)", tt.sel, got, tt.want)
		}
	}
}

// TestWindowAggregateOverJoinUnaffectedShapes is the control set: shapes that
// worked before the fix must keep working, and the answer must no longer depend
// on whether the value column is also listed in the SELECT list.
func TestWindowAggregateOverJoinUnaffectedShapes(t *testing.T) {
	db := setupWindowJoinFixture(t)
	defer db.Close()

	// Adding the value column to the SELECT list must NOT change the aggregate.
	// Note the window column sits at a different index in each query: the
	// 3-column form has a.v at index 1 and the window at index 2.
	withCol, err := queryCol(t, db,
		"SELECT a.id, a.v, SUM(a.v) OVER () FROM a JOIN b ON a.id=b.id", 2)
	if err != nil {
		t.Fatalf("listed-column variant: %v", err)
	}
	hidden, err := queryCol(t, db,
		"SELECT a.id, SUM(a.v) OVER () FROM a JOIN b ON a.id=b.id", 1)
	if err != nil {
		t.Fatalf("hidden-column variant: %v", err)
	}
	if withCol != hidden {
		t.Errorf("window aggregate depends on whether the value column is in the SELECT "+
			"list: with column = %s, hidden column = %s", withCol, hidden)
	}

	// Ranking window functions are position-based and never read a value
	// column; they must be unchanged.
	for _, tt := range []struct {
		sel  string
		want string
	}{
		{"SELECT a.id, ROW_NUMBER() OVER (ORDER BY a.id) FROM a JOIN b ON a.id=b.id", "1"},
		{"SELECT a.id, RANK() OVER (ORDER BY a.id) FROM a JOIN b ON a.id=b.id", "1"},
		{"SELECT a.id, DENSE_RANK() OVER (ORDER BY a.id) FROM a JOIN b ON a.id=b.id", "1"},
		{"SELECT a.id, COUNT(*) OVER () FROM a JOIN b ON a.id=b.id", "3"},
	} {
		got, err := queryCol(t, db, tt.sel, 1)
		if err != nil {
			t.Errorf("CONTROL FAILED %q: %v", tt.sel, err)
			continue
		}
		if got != tt.want {
			t.Errorf("CONTROL FAILED %q: got %s, want %s", tt.sel, got, tt.want)
		}
	}

	// The no-join path must be untouched: it already resolved correctly.
	plain, err := queryCol(t, db, "SELECT id, SUM(v) OVER () FROM a", 1)
	if err != nil {
		t.Fatalf("CONTROL FAILED unjoined window aggregate: %v", err)
	}
	if plain != "60" {
		t.Errorf("CONTROL FAILED: unjoined SUM(v) OVER () = %s, want 60", plain)
	}
}

// queryCol returns the given column of the first row, as text.
func queryCol(t *testing.T, db *DB, sel string, col int) (string, error) {
	t.Helper()
	rows, err := db.Query(context.Background(), sel)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", fmt.Errorf("no rows")
	}
	vals := make([]interface{}, len(rows.Columns()))
	ptrs := make([]interface{}, len(vals))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if serr := rows.Scan(ptrs...); serr != nil {
		return "", serr
	}
	if col >= len(vals) {
		return "", fmt.Errorf("column %d out of range (%d columns)", col, len(vals))
	}
	return fmt.Sprintf("%v", vals[col]), nil
}

func setupWindowJoinFixture(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "winjoin"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id INTEGER, v INTEGER)")
	mustExec(t, db, "CREATE TABLE b (id INTEGER, w INTEGER)")
	mustExec(t, db, "INSERT INTO a VALUES (1,10),(2,20),(3,30)")
	mustExec(t, db, "INSERT INTO b VALUES (1,100),(2,200),(3,300)")
	return db
}
