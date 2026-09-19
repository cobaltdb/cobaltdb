package engine

import (
	"context"
	"strings"
	"testing"
)

// The non-grouped projection path propagates SELECT-expression evaluation
// errors (projectSelectedRow returns err; filterAndProjectRow propagates it),
// so `SELECT dept/0 FROM g` fails. The GROUP BY clause's own expression must
// follow the same contract: buildGroupByGroups' group-key construction
// instead swallowed evaluateExpression errors (`if err == nil { write key }`),
// silently collapsing every erroring row into one group.

func TestGroupByExpressionErrorFailsQuery(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE g (id INTEGER PRIMARY KEY, dept TEXT)`)
	mustExec(t, db, `INSERT INTO g VALUES (1, 'eng'), (2, 'ops')`)

	// Consistency gate: the non-grouped projection of the SAME erroring
	// expression must fail (the established projection contract).
	if _, err = db.Query(ctx, `SELECT dept/0 FROM g`); err == nil {
		t.Fatalf("gate: non-grouped projection of an erroring expression did not fail — behavior would be consistent with the GROUP BY swallow")
	}

	// Control: a valid GROUP BY expression groups normally.
	rows, err := db.Query(ctx, `SELECT dept, COUNT(*) FROM g GROUP BY dept`)
	if err != nil {
		t.Fatalf("control grouped query: %v", err)
	}
	defer rows.Close()

	// The probe: an erroring GROUP BY expression must FAIL the query, not
	// silently collapse every row into one group.
	if _, err = db.Query(ctx, `SELECT COUNT(*) FROM g GROUP BY dept/0`); err == nil {
		t.Fatalf("FAIL: grouped query silently swallowed the GROUP BY expression error")
	}
}

func TestGroupByExpressionErrorFailsQueryParallel(t *testing.T) {
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
	rows, err := db.Query(ctx, `SELECT dept, COUNT(*) FROM gp GROUP BY dept`)
	if err != nil {
		t.Fatalf("control grouped query: %v", err)
	}
	defer rows.Close()

	// The probe: the erroring GROUP BY expression must fail through the
	// parallel branch too.
	if _, err = db.Query(ctx, `SELECT COUNT(*) FROM gp GROUP BY dept/0`); err == nil {
		t.Fatalf("FAIL: parallel grouped query silently swallowed the GROUP BY expression error")
	}
}
