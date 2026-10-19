package engine

import (
	"context"
	"path/filepath"
	"testing"
)

// TestJoinProjectionErrorPropagates pins error propagation for an EXPRESSION
// select column in a JOIN query.
//
// Pre-fix, projectJoinSelectCols (pkg/catalog/catalog_select.go) projected with
//
//	val, err := evaluateExpression(...)
//	if err == nil { projected = append(projected, val); continue }
//	projected = append(projected, nil)
//
// so an evaluation FAILURE was downgraded to a NULL and the row was returned:
//
//	SELECT a.id, a.v/0 FROM a JOIN b ON a.id=b.id
//	  -> [1, NULL] [2, NULL]      (no error)
//
// while the identical expression raised "division by zero" ungrouped, in
// WHERE, in a derived table and in LIMIT/OFFSET. That contradicts the
// documented contract in CLAUDE.md ("Division by zero raises an error") and the
// contract the WHERE, group-key, value-collection, ORDER-BY and GROUP BY
// projection sites already honor.
//
// This is the sibling of the GROUP BY projection swallow in
// catalog_aggregate.go computeGroupResultRows — same defect class, different
// file and different site.
func TestJoinProjectionErrorPropagates(t *testing.T) {
	db := setupJoinProjectionFixture(t)
	defer db.Close()

	if rows, err := db.Query(context.Background(),
		"SELECT a.id, a.v/0 FROM a JOIN b ON a.id=b.id"); err == nil {
		got := ""
		for rows.Next() {
			var id, val interface{}
			if serr := rows.Scan(&id, &val); serr != nil {
				t.Fatalf("scan: %v", serr)
			}
			got += "[id=" + toStrEng(id) + " v=" + toStrEng(val) + "] "
		}
		rows.Close()
		t.Fatalf("JOIN projection swallowed the error and returned %s; the same "+
			"expression errors ungrouped (documented contract: division by zero raises)", got)
	}

	// Boundary: the same expression on the other side of the join.
	if rows, err := db.Query(context.Background(),
		"SELECT a.id, b.w/0 FROM a JOIN b ON a.id=b.id"); err == nil {
		rows.Close()
		t.Errorf("JOIN projection of the joined table's expression still swallowed the error")
	}
}

// TestJoinProjectionValidShapesAndWindows is the control for the fix: valid
// JOIN projections must keep their real values, and window functions over a
// JOIN must keep working. Window columns carry ci.index == -1 and are computed
// AFTER projectJoinSelectCols by evaluateWindowFunctions, so the pre-pass
// legitimately errors on them — the fix exempts exactly those, and this test
// fails if the exemption is dropped or too broad.
func TestJoinProjectionValidShapesAndWindows(t *testing.T) {
	db := setupJoinProjectionFixture(t)
	defer db.Close()

	cases := []struct {
		sel       string
		wantFirst string
	}{
		{"SELECT a.id, a.v FROM a JOIN b ON a.id=b.id", "1"},
		{"SELECT a.id, a.v * 2 FROM a JOIN b ON a.id=b.id", "1"},
		{"SELECT a.id, b.w FROM a JOIN b ON a.id=b.id", "1"},
		// Window functions over a JOIN — the shape the exemption protects.
		{"SELECT a.id, ROW_NUMBER() OVER (ORDER BY a.id) FROM a JOIN b ON a.id=b.id", "1"},
		{"SELECT a.id, SUM(a.v) OVER () FROM a JOIN b ON a.id=b.id", "1"},
		{"SELECT a.id, RANK() OVER (ORDER BY a.v DESC) FROM a JOIN b ON a.id=b.id", "1"},
	}
	for _, tt := range cases {
		rows, err := db.Query(context.Background(), tt.sel)
		if err != nil {
			t.Errorf("CONTROL FAILED %q: errored: %v", tt.sel, err)
			continue
		}
		if !rows.Next() {
			t.Errorf("CONTROL FAILED %q: returned no rows", tt.sel)
			rows.Close()
			continue
		}
		vals := make([]interface{}, len(rows.Columns()))
		ptrs := make([]interface{}, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if serr := rows.Scan(ptrs...); serr != nil {
			t.Errorf("CONTROL FAILED %q: scan: %v", tt.sel, serr)
			rows.Close()
			continue
		}
		rows.Close()
		if got := toStrEng(vals[0]); got != tt.wantFirst {
			t.Errorf("CONTROL FAILED %q: first column = %q, want %q", tt.sel, got, tt.wantFirst)
		}
	}
}

// TestUngroupedProjectionErrorUnchanged is the harness control: the ungrouped
// projection of the same expression must keep raising, proving the JOIN result
// above reflects the defect rather than a changed global contract.
func TestUngroupedProjectionErrorUnchanged(t *testing.T) {
	db := setupJoinProjectionFixture(t)
	defer db.Close()
	if rows, err := db.Query(context.Background(), "SELECT a.v/0 FROM a"); err == nil {
		rows.Close()
		t.Fatal("CONTROL FAILED: ungrouped SELECT a.v/0 no longer errors; the documented " +
			"division-by-zero contract this fix relies on has changed")
	}
}

func setupJoinProjectionFixture(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "joinproj"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id INTEGER, v INTEGER)")
	mustExec(t, db, "CREATE TABLE b (id INTEGER, w INTEGER)")
	mustExec(t, db, "INSERT INTO a VALUES (1,10),(2,20)")
	mustExec(t, db, "INSERT INTO b VALUES (1,100),(2,200)")
	return db
}
