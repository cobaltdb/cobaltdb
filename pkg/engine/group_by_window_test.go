package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// TestGroupByWindowEvaluated pins that window functions are computed over the
// GROUP BY result rows.
//
// Pre-fix, every window column in a GROUP BY query silently evaluated to NULL:
//
//	SELECT g, ROW_NUMBER() OVER (ORDER BY g) FROM t GROUP BY g  -> [x NULL] [y NULL]
//
// Root cause: in selectLockedInternal the GROUP BY / aggregate branch calls
// computeAggregatesWithGroupBy and RETURNS at catalog_core.go — before the
// window-detection block below it and before applySelectPostProcess, the only
// other single-table caller of evaluateWindowFunctions. catalog_aggregate.go
// contains zero references to isWindow or embeddedWindows, so nothing on that
// path ever evaluated a window column.
//
// DISTINCT is the neighbouring shape that worked: it is not a GROUP BY branch,
// so it reached applySelectPostProcess and the windows were computed. That
// asymmetry is what the control below pins.
func TestGroupByWindowEvaluated(t *testing.T) {
	db := setupGroupByWindowFixture(t)
	defer db.Close()

	cases := []struct {
		sel  string
		col  int
		want string
	}{
		{"SELECT g, ROW_NUMBER() OVER (ORDER BY g) FROM t GROUP BY g", 1, "1"},
		{"SELECT g, SUM(v) AS s, ROW_NUMBER() OVER (ORDER BY g) FROM t GROUP BY g", 2, "1"},
		{"SELECT g, SUM(v) AS s, ROW_NUMBER() OVER (ORDER BY SUM(v) DESC) FROM t GROUP BY g", 2, "1"},
		{"SELECT g, SUM(v) AS s, RANK() OVER (ORDER BY SUM(v)) FROM t GROUP BY g", 2, "1"},
		{"SELECT g, COUNT(*) AS c, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) FROM t GROUP BY g", 2, "1"},
		{"SELECT g, SUM(v) AS s, COUNT(*) OVER (PARTITION BY g) FROM t GROUP BY g", 2, "1"},
		// Group key not in the SELECT list.
		{"SELECT SUM(v) AS s, ROW_NUMBER() OVER (ORDER BY g) FROM t GROUP BY g", 1, "1"},
	}
	for _, tt := range cases {
		got := groupByWindowCol(t, db, tt.sel, tt.col)
		if got == "<nil>" {
			t.Errorf("query %q left the window column NULL; a GROUP BY query never "+
				"evaluated its window functions", tt.sel)
			continue
		}
		if got != tt.want {
			t.Errorf("query %q: got %s, want %s", tt.sel, got, tt.want)
		}
	}
}

// TestGroupByWindowSecondRowAndOrdering checks more than the first row: the
// second group must also get a real (distinct, ordered) window value, so a fix
// that merely filled column 0 cannot pass.
func TestGroupByWindowSecondRowAndOrdering(t *testing.T) {
	db := setupGroupByWindowFixture(t)
	defer db.Close()

	// Groups x,y must rank 1,2 in that order.
	rows, err := db.Query(context.Background(),
		"SELECT g, ROW_NUMBER() OVER (ORDER BY g) FROM t GROUP BY g")
	if err != nil {
		t.Fatalf("query errored: %v", err)
	}
	defer rows.Close()
	want := []struct{ grp, rn string }{{"x", "1"}, {"y", "2"}}
	i := 0
	for rows.Next() {
		var grp, rn interface{}
		if serr := rows.Scan(&grp, &rn); serr != nil {
			t.Fatalf("scan: %v", serr)
		}
		if i >= len(want) {
			break
		}
		if got := fmt.Sprintf("%v", grp); got != want[i].grp {
			t.Errorf("row %d group = %s, want %s", i, got, want[i].grp)
		}
		if got := fmt.Sprintf("%v", rn); got != want[i].rn {
			t.Errorf("row %d ROW_NUMBER = %s, want %s", i, got, want[i].rn)
		}
		i++
	}
	if i != len(want) {
		t.Errorf("got %d rows, want %d", i, len(want))
	}
}

// TestGroupByWindowControls is the control set: the plain (non-GROUP BY)
// window path, the DISTINCT path, and plain GROUP BY aggregation must all be
// unchanged by the fix.
func TestGroupByWindowControls(t *testing.T) {
	db := setupGroupByWindowFixture(t)
	defer db.Close()

	// Plain window path (no GROUP BY) — the shape that always worked.
	if got := groupByWindowCol(t, db, "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t", 1); got != "1" {
		t.Errorf("CONTROL FAILED: plain ROW_NUMBER() OVER (ORDER BY id) = %s, want 1", got)
	}
	if got := groupByWindowCol(t, db, "SELECT id, SUM(v) OVER () FROM t", 1); got != "60" {
		t.Errorf("CONTROL FAILED: plain SUM(v) OVER () = %s, want 60", got)
	}

	// DISTINCT window path — not a GROUP BY branch.
	if got := groupByWindowCol(t, db, "SELECT DISTINCT g, ROW_NUMBER() OVER (ORDER BY g) FROM t", 1); got != "1" {
		t.Errorf("CONTROL FAILED: DISTINCT window = %s, want 1", got)
	}

	// Plain GROUP BY aggregation with no window column must be unchanged.
	rows, err := db.Query(context.Background(), "SELECT g, SUM(v) FROM t GROUP BY g")
	if err != nil {
		t.Fatalf("CONTROL FAILED: plain GROUP BY errored: %v", err)
	}
	defer rows.Close()
	got := ""
	for rows.Next() {
		var grp, s interface{}
		rows.Scan(&grp, &s)
		got += fmt.Sprintf("[%v=%v] ", grp, s)
	}
	if got != "[x=30] [y=30] " {
		t.Errorf("CONTROL FAILED: plain GROUP BY aggregation = %q, want \"[x=30] [y=30] \"", got)
	}
}

func groupByWindowCol(t *testing.T, db *DB, sel string, col int) string {
	t.Helper()
	rows, err := db.Query(context.Background(), sel)
	if err != nil {
		return "ERR:" + err.Error()
	}
	defer rows.Close()
	if !rows.Next() {
		return "NOROWS"
	}
	vals := make([]interface{}, len(rows.Columns()))
	ptrs := make([]interface{}, len(vals))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if serr := rows.Scan(ptrs...); serr != nil {
		return "SCANERR:" + serr.Error()
	}
	if col >= len(vals) {
		return "COLOOR"
	}
	return fmt.Sprintf("%v", vals[col])
}

func setupGroupByWindowFixture(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "gbwin"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE t (id INTEGER, g TEXT, v INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1,'x',10),(2,'x',20),(3,'y',30)")
	return db
}
