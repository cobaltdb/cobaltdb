package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// TestEmbeddedWindowOverJoinEvaluates pins that a window function EMBEDDED
// inside a larger expression (SUM(a.v) OVER () + 1) is evaluated over a JOIN.
//
// Pre-fix, an embedded window silently produced NULL over a JOIN. Three
// defects on the JOIN path conspired, all absent from the single-table path:
//
//  1. executeSelectWithJoin gated window evaluation on `ci.isWindow` alone,
//     while the single-table path (selectLockedInternal) gates on
//     `ci.isWindow || len(ci.embeddedWindows) > 0`. An embedded window sets
//     embeddedWindows but NOT isWindow, so the gate was false and
//     evaluateWindowFunctions was never called at all.
//  2. evaluateWindowFunctions' embedded-window pass read `table.Columns`
//     unconditionally, but the JOIN path passes table == nil — a latent nil
//     dereference that panicked as soon as the gate was corrected.
//  3. resolveHiddenWindowCols skipped any column with `!ci.isWindow`, so an
//     embedded window's value/PARTITION BY/ORDER BY columns were never
//     collected; with table == nil in evalWindowExprOnRow every table-column
//     fallback is dead, so those columns resolved to nil.
//
// This is distinct from the top-level window fix: that one failed only when
// the value column was missing from the SELECT list, whereas an EMBEDDED
// window returned NULL even with the column listed.
func TestEmbeddedWindowOverJoinEvaluates(t *testing.T) {
	db := setupEmbeddedWindowFixture(t)
	defer db.Close()

	cases := []struct {
		sel  string
		col  int
		want string
	}{
		{"SELECT a.id, SUM(a.v) OVER () + 1 FROM a JOIN b ON a.id=b.id", 1, "61"},
		{"SELECT a.id, SUM(a.v) OVER () * 2 FROM a JOIN b ON a.id=b.id", 1, "120"},
		{"SELECT a.id, SUM(b.w) OVER () - 1 FROM a JOIN b ON a.id=b.id", 1, "599"},
		{"SELECT a.id, SUM(a.v) OVER (PARTITION BY a.id) + 1 FROM a JOIN b ON a.id=b.id", 1, "11"},
		{"SELECT a.id, ROW_NUMBER() OVER (ORDER BY a.id) + 100 FROM a JOIN b ON a.id=b.id", 1, "101"},
		// Value column listed explicitly must give the SAME answer — the
		// result must never depend on whether the column is projected. Note
		// the window column sits at index 2 here (index 1 is a.v).
		{"SELECT a.id, a.v, SUM(a.v) OVER () + 1 FROM a JOIN b ON a.id=b.id", 2, "61"},
	}
	for _, tt := range cases {
		got := embeddedWindowCol(t, db, tt.sel, tt.col)
		if got == "<nil>" {
			t.Errorf("query %q evaluated the embedded window to NULL; an embedded "+
				"window over a JOIN was never evaluated", tt.sel)
			continue
		}
		if got != tt.want {
			t.Errorf("query %q: got %s, want %s", tt.sel, got, tt.want)
		}
	}
}

// TestEmbeddedWindowJoinRunningFrameAndNoJoinControls is the control set: the
// single-table path, top-level windows over a JOIN, and running/frame
// semantics must all be unchanged by the gate and collector fixes.
func TestEmbeddedWindowJoinRunningFrameAndNoJoinControls(t *testing.T) {
	db := setupEmbeddedWindowFixture(t)
	defer db.Close()

	// Single-table path is the reference and was already correct.
	for _, tt := range []struct {
		sel  string
		want string
	}{
		{"SELECT id, SUM(v) OVER () + 1 FROM a", "61"},
		{"SELECT id, SUM(v) OVER (ORDER BY id) + 1 FROM a", "11"},
		{"SELECT id, ROW_NUMBER() OVER (ORDER BY id) + 100 FROM a", "101"},
	} {
		if got := embeddedWindowCol(t, db, tt.sel, 1); got != tt.want {
			t.Errorf("CONTROL FAILED %q: got %s, want %s", tt.sel, got, tt.want)
		}
	}

	// Top-level (non-embedded) windows over a JOIN — round 4's fix — unchanged.
	if got := embeddedWindowCol(t, db,
		"SELECT a.id, SUM(a.v) OVER () FROM a JOIN b ON a.id=b.id", 1); got != "60" {
		t.Errorf("CONTROL FAILED: top-level SUM(a.v) OVER () over a JOIN = %s, want 60", got)
	}

	// Running totals over a JOIN: each row must carry its own running value.
	rows, err := db.Query(context.Background(),
		"SELECT a.id, SUM(a.v) OVER (ORDER BY a.id) + 1 FROM a JOIN b ON a.id=b.id")
	if err != nil {
		t.Fatalf("CONTROL FAILED: running embedded window over a JOIN errored: %v", err)
	}
	defer rows.Close()
	want := []string{"11", "31", "61"}
	i := 0
	for rows.Next() {
		var id, val interface{}
		if serr := rows.Scan(&id, &val); serr != nil {
			t.Fatalf("scan: %v", serr)
		}
		if i >= len(want) {
			break
		}
		if got := fmt.Sprintf("%v", val); got != want[i] {
			t.Errorf("CONTROL FAILED: running embedded window row %d = %s, want %s", i, got, want[i])
		}
		i++
	}
	if i != len(want) {
		t.Errorf("CONTROL FAILED: running embedded window returned %d rows, want %d", i, len(want))
	}
}

func embeddedWindowCol(t *testing.T, db *DB, sel string, col int) string {
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

func setupEmbeddedWindowFixture(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "embedwin"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id INTEGER, v INTEGER)")
	mustExec(t, db, "CREATE TABLE b (id INTEGER, w INTEGER)")
	mustExec(t, db, "INSERT INTO a VALUES (1,10),(2,20),(3,30)")
	mustExec(t, db, "INSERT INTO b VALUES (1,100),(2,200),(3,300)")
	return db
}
