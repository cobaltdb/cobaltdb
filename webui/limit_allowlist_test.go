package main

import (
	"reflect"
	"sort"
	"testing"
)

// Regression tests for table allow-list extraction of LIMIT/OFFSET subqueries.
//
// walkSelect and walkUnion previously did not walk SelectStmt.Limit/.Offset or
// UnionStmt.Limit/.Offset, so "SELECT * FROM t LIMIT (SELECT COUNT(*) FROM
// secret)" extracted only {"t"} while the engine actually reads "secret" during
// evaluation (verified by engine probe) — a fail-open bypass of the per-token
// table allow-list boundary, whose contract is fail-closed.

func TestExtractTableRefsLimitOffsetSubqueries(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"SELECT * FROM t LIMIT (SELECT COUNT(*) FROM secret)", []string{"secret", "t"}},
		{"SELECT * FROM t LIMIT 1 OFFSET (SELECT COUNT(*) FROM secret)", []string{"secret", "t"}},
		// Union variant: the LIMIT subquery references a table touched nowhere
		// else in the statement, so the miss is attributable to Limit alone.
		{"SELECT * FROM t UNION ALL SELECT * FROM secret LIMIT (SELECT COUNT(*) FROM third)", []string{"secret", "t", "third"}},
	}
	for _, tc := range cases {
		got, err := extractTableRefs(tc.sql)
		if err != nil {
			t.Errorf("extractTableRefs(%q) error: %v — the boundary must stay fail-closed, but known shapes must extract", tc.sql, err)
			continue
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("extractTableRefs(%q) = %v; want %v — allow-list bypass: LIMIT/OFFSET subquery tables missing", tc.sql, got, tc.want)
		}
	}
}

func TestExtractTableRefsLimitControls(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"SELECT * FROM t LIMIT 1", []string{"t"}},
		{"SELECT * FROM t LIMIT ?", []string{"t"}},
		{"SELECT * FROM t", []string{"t"}},
	}
	for _, tc := range cases {
		got, err := extractTableRefs(tc.sql)
		if err != nil {
			t.Errorf("extractTableRefs(%q) error: %v", tc.sql, err)
			continue
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("extractTableRefs(%q) = %v; want %v", tc.sql, got, tc.want)
		}
	}
}
