package engine

import (
	"context"
	"strings"
	"testing"
)

// The engine's established contract (round-19-of-25 + the DML scan-path fix)
// is that WHERE evaluation errors FAIL the statement: the non-grouped scan
// path and the aggregate fast paths propagate evaluateWhere errors. The
// grouped-aggregate row filter (buildGroupByGroups /
// buildGroupByGroupsFromRows) instead swallowed them — treating an error as
// "row doesn't match" — so the same erroring WHERE failed
// `SELECT COUNT(*) FROM g WHERE 1/0=1` but silently returned an empty result
// for `SELECT dept, COUNT(*) FROM g WHERE 1/0=1 GROUP BY dept`.

func TestGroupedWhereErrorFailsQuerySerial(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE g (id INTEGER PRIMARY KEY, dept TEXT)`)
	mustExec(t, db, `INSERT INTO g VALUES (1, 'eng'), (2, 'ops')`)

	// Control: a valid WHERE groups normally.
	rows, err := db.Query(ctx, `SELECT dept, COUNT(*) FROM g WHERE id > 0 GROUP BY dept`)
	if err != nil {
		t.Fatalf("control grouped query: %v", err)
	}
	defer rows.Close()
	groups := 0
	for rows.Next() {
		groups++
	}
	if groups != 2 {
		t.Fatalf("sanity: expected 2 groups, got %d", groups)
	}

	// Non-grouped control (the established contract): must fail.
	if _, err = db.Query(ctx, `SELECT COUNT(*) FROM g WHERE 1/0=1`); err == nil {
		t.Fatalf("FAIL: non-grouped WHERE error unexpectedly swallowed")
	}

	// The probe: the SAME erroring WHERE on the grouped path must also fail,
	// not silently return an empty result.
	if _, err = db.Query(ctx, `SELECT dept, COUNT(*) FROM g WHERE 1/0=1 GROUP BY dept`); err == nil {
		t.Fatalf("FAIL: grouped query silently swallowed the WHERE evaluation error")
	}
}

func TestGroupedWhereErrorFailsQueryParallel(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE gp (id INTEGER PRIMARY KEY, dept TEXT)`)
	var sb strings.Builder
	sb.WriteString(`INSERT INTO gp VALUES `)
	for i := 1; i <= 1100; i++ {
		if i > 1 {
			sb.WriteString(",")
		}
		sb.WriteString(`(`)
		sb.WriteString(itoa(i))
		sb.WriteString(", 'd')")
	}
	mustExec(t, db, sb.String())

	// 1100 rows >= the parallel threshold (1000): the parallel grouping
	// branch runs.
	rows, err := db.Query(ctx, `SELECT dept, COUNT(*) FROM gp WHERE id > 0 GROUP BY dept`)
	if err != nil {
		t.Fatalf("control grouped query: %v", err)
	}
	defer rows.Close()

	// The probe: the erroring WHERE must propagate through the parallel
	// branch too.
	if _, err = db.Query(ctx, `SELECT dept, COUNT(*) FROM gp WHERE 1/0=1 GROUP BY dept`); err == nil {
		t.Fatalf("FAIL: parallel grouped query silently swallowed the WHERE evaluation error")
	}
}
