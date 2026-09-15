package query

import "testing"

// TestCollectPlaceholdersWalksAllChildBearingExpressions verifies the live
// placeholder walker descends into every child-bearing expression type.
// collectPlaceholders/reindexPlaceholders are how the parser assigns `?`
// indices (parsePrimary creates placeholders with Index=0), so a placeholder
// missed by this walker keeps Index 0 and silently binds the first query
// argument instead of its own — wrong results with no error.
func TestCollectPlaceholdersWalksAllChildBearingExpressions(t *testing.T) {
	mustWhere := func(sql string) Expression {
		st, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		sel, ok := st.(*SelectStmt)
		if !ok {
			t.Fatalf("%q did not parse to *SelectStmt (%T)", sql, st)
		}
		if sel.Where == nil {
			t.Fatalf("%q parsed without a WHERE clause", sql)
		}
		return sel.Where
	}

	cases := []struct {
		name        string
		sql         string
		wantIndices []int
	}{
		{
			name:        "plain comparisons (existing coverage guard)",
			sql:         "SELECT * FROM t WHERE a = ? AND b = ?",
			wantIndices: []int{0, 1},
		},
		{
			name:        "MATCH ... AGAINST pattern",
			sql:         "SELECT * FROM docs WHERE MATCH(body) AGAINST(?) AND id = ?",
			wantIndices: []int{0, 1},
		},
		{
			name:        "LIKE ... ESCAPE",
			sql:         "SELECT * FROM t WHERE name LIKE ? ESCAPE ? AND id = ?",
			wantIndices: []int{0, 1, 2},
		},
		{
			name:        "INTERVAL value in date arithmetic",
			sql:         "SELECT * FROM t WHERE created_at > DATE_SUB(NOW(), INTERVAL ? DAY) AND id = ?",
			wantIndices: []int{0, 1},
		},
		{
			name:        "IN subquery JOIN condition",
			sql:         "SELECT * FROM t WHERE x IN (SELECT y FROM s JOIN u ON u.tag = ?) AND id = ?",
			wantIndices: []int{0, 1},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			phs := collectPlaceholders(mustWhere(tc.sql))
			if len(phs) != len(tc.wantIndices) {
				t.Fatalf("collectPlaceholders found %d placeholders, want %d (sql: %s)",
					len(phs), len(tc.wantIndices), tc.sql)
			}
			for i, ph := range phs {
				if ph.Index != tc.wantIndices[i] {
					t.Errorf("placeholder %d has Index=%d, want %d (appearance order)",
						i, ph.Index, tc.wantIndices[i])
				}
			}
		})
	}
}
