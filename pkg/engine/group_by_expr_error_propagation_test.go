package engine

import (
	"context"
	"path/filepath"
	"testing"
)

// TestGroupByExpressionErrorPropagates pins error propagation for a
// NON-AGGREGATE EXPRESSION select column in a GROUP BY query.
//
// Pre-fix, computeGroupResultRows' `ci.index == -1` branch projected the
// value with `if err == nil { resultRow[i] = val }`, so an evaluation FAILURE
// was downgraded to a missing value and the row silently came back with NULL
// in that column:
//
//	SELECT g, x/0 FROM t GROUP BY g   -> [g=a, v=NULL]   (no error)
//
// while the identical expression raised "division by zero" in the ungrouped
// projection path and in WHERE. That contradicts the documented contract in
// CLAUDE.md ("Division by zero raises an error") and the contract the WHERE,
// group-key, value-collection and ORDER-BY sites already honor — an error
// swallow at a projection layer those rounds did not reach.
//
// This is NOT the round-24 applyGroupByOrderBy comparator defect (same file,
// different site and different observable: that one is an intransitive sort
// comparator; this one is a dropped evaluation error).
func TestGroupByExpressionErrorPropagates(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "grpexpr"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (g TEXT, x INTEGER)")
	mustExec(t, db, "INSERT INTO t (g,x) VALUES ('a',1),('a',2)")

	// The candidate: an erroring expression as a grouped non-aggregate column.
	if rows, err := db.Query(context.Background(), "SELECT g, x/0 FROM t GROUP BY g"); err == nil {
		got := ""
		for rows.Next() {
			var a, b interface{}
			if serr := rows.Scan(&a, &b); serr != nil {
				t.Fatalf("scan: %v", serr)
			}
			got += "[g=" + toStrEng(a) + " v=" + toStrEng(b) + "]"
		}
		rows.Close()
		t.Fatalf("grouped non-aggregate expression swallowed the error and returned %s; "+
			"the same expression errors ungrouped (documented contract: division by zero raises)", got)
	}

	// Boundary: the same expression behind HAVING (still a grouped projection).
	if rows, err := db.Query(context.Background(), "SELECT g, x/0 FROM t GROUP BY g HAVING 1=1"); err == nil {
		rows.Close()
		t.Errorf("grouped expression with HAVING still swallowed the error")
	}
}

// TestGroupByExpressionValidProjects is the control for the fix: a VALID
// grouped non-aggregate expression must keep projecting its real value, so the
// error propagation does not turn every such column into NULL.
func TestGroupByExpressionValidProjects(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "okexpr"), nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (g TEXT, x INTEGER)")
	mustExec(t, db, "INSERT INTO t (g,x) VALUES ('a',1),('a',2)")

	rows, err := db.Query(context.Background(), "SELECT g, x+1 FROM t GROUP BY g")
	if err != nil {
		t.Fatalf("valid grouped expression errored: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("valid grouped expression returned no rows")
	}
	var g, v interface{}
	if err := rows.Scan(&g, &v); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if v != int64(2) && v != 2 {
		t.Fatalf("SELECT g, x+1 GROUP BY g projected v=%v (%T), want 2", v, v)
	}

	// Neighbouring shapes that must be unaffected: a plain column, an aliased
	// expression, and an aggregate column in the same grouped query.
	for _, sel := range []string{
		"SELECT g, x FROM t GROUP BY g",
		"SELECT g, x * 2 AS doubled FROM t GROUP BY g",
		"SELECT g, COUNT(*) AS n FROM t GROUP BY g",
		"SELECT g, SUM(x) AS s FROM t GROUP BY g",
	} {
		r, err := db.Query(context.Background(), sel)
		if err != nil {
			t.Errorf("neighbouring grouped shape %q errored: %v", sel, err)
			continue
		}
		if !r.Next() {
			t.Errorf("neighbouring grouped shape %q returned no rows", sel)
		}
		r.Close()
	}
}
