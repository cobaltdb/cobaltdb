package catalog

import (
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestViewSQLRoundTripsNaturalJoins pins that a view using a NATURAL [INNER|
// LEFT|RIGHT|FULL] [OUTER] JOIN keeps its NATURAL semantics when its
// definition is serialized for persistence.
//
// joinClauseToSQL used to prepend "NATURAL" to the join keyword and then let
// the join-type switch overwrite the whole string, silently dropping NATURAL.
// The persisted SQL is what Save() writes into the catalog tree and what
// Load() re-parses, so a dropped NATURAL changed (or broke) the view after a
// save/load round-trip.
//
// The join type is resolved first and NATURAL prepended afterwards, so
// "NATURAL JOIN" serializes as "NATURAL INNER JOIN" (the parser sets
// Type=TokenInner for a bare NATURAL JOIN) and every typed variant keeps both
// keywords.
func TestViewSQLRoundTripsNaturalJoins(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"natural join", "CREATE VIEW v AS SELECT a.x FROM a NATURAL JOIN b"},
		{"natural inner join", "CREATE VIEW v AS SELECT a.x FROM a NATURAL INNER JOIN b"},
		{"natural left join", "CREATE VIEW v AS SELECT a.x FROM a NATURAL LEFT JOIN b"},
		{"natural left outer join", "CREATE VIEW v AS SELECT a.x FROM a NATURAL LEFT OUTER JOIN b"},
		{"natural right join", "CREATE VIEW v AS SELECT a.x FROM a NATURAL RIGHT JOIN b"},
		{"natural right outer join", "CREATE VIEW v AS SELECT a.x FROM a NATURAL RIGHT OUTER JOIN b"},
		{"natural full join", "CREATE VIEW v AS SELECT a.x FROM a NATURAL FULL JOIN b"},
		{"natural full outer join", "CREATE VIEW v AS SELECT a.x FROM a NATURAL FULL OUTER JOIN b"},
		// Controls: ordinary joins must be unaffected by the fix.
		{"inner join on", "CREATE VIEW v AS SELECT a.x FROM a INNER JOIN b ON a.id = b.id"},
		{"left join on", "CREATE VIEW v AS SELECT a.x FROM a LEFT JOIN b ON a.id = b.id"},
		{"cross join", "CREATE VIEW v AS SELECT a.x FROM a CROSS JOIN b"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := query.Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.sql, err)
			}
			cv, ok := stmt.(*query.CreateViewStmt)
			if !ok {
				t.Fatalf("expected *query.CreateViewStmt, got %T", stmt)
			}
			if cv.Query == nil || len(cv.Query.Joins) == 0 {
				t.Fatalf("parsed CREATE VIEW lost its join: %q", tc.sql)
			}
			wantNatural := cv.Query.Joins[0].Natural

			persisted := createViewSQL("v", cv.Query)

			// The NATURAL keyword must survive serialization exactly when the
			// parsed join is NATURAL, and must not appear otherwise.
			if got := strings.Contains(persisted, "NATURAL"); got != wantNatural {
				t.Fatalf("persisted %q: NATURAL keyword present=%v, want %v (input %q)",
					persisted, got, wantNatural, tc.sql)
			}

			// The persisted SQL is re-parsed on Load; it must yield the same
			// NATURAL flag, otherwise the view changes on reload.
			reStmt, err := query.Parse(persisted)
			if err != nil {
				t.Fatalf("persisted view SQL does not re-parse (Load would fail): %q: %v", persisted, err)
			}
			reCV, ok := reStmt.(*query.CreateViewStmt)
			if !ok {
				t.Fatalf("re-parsed %q is not a CREATE VIEW: %T", persisted, reStmt)
			}
			if reCV.Query == nil || len(reCV.Query.Joins) == 0 {
				t.Fatalf("re-parsed view lost its join: %q", persisted)
			}
			if reCV.Query.Joins[0].Natural != wantNatural {
				t.Fatalf("round-trip changed semantics: %q re-parses with Natural=%v, want %v",
					persisted, reCV.Query.Joins[0].Natural, wantNatural)
			}
			if !wantNatural && reCV.Query.Joins[0].Type != cv.Query.Joins[0].Type {
				t.Fatalf("round-trip changed join type: %q re-parses with %v, want %v",
					persisted, reCV.Query.Joins[0].Type, cv.Query.Joins[0].Type)
			}
		})
	}
}
