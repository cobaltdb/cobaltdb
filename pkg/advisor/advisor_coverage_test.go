package advisor

import (
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestAdvisorRecordsGroupByWithoutPrimaryTable(t *testing.T) {
	a := NewIndexAdvisor()
	a.Analyze(&query.SelectStmt{
		GroupBy: []query.Expression{&query.QualifiedIdentifier{Table: "events", Column: "kind"}},
	})

	recs := a.Recommendations(nil)
	if len(recs) != 1 {
		t.Fatalf("recommendations = %v, want one", recs)
	}
	if recs[0].TableName != "events" || recs[0].Columns[0] != "kind" {
		t.Fatalf("recommendation = %+v, want events.kind", recs[0])
	}
	if !strings.Contains(recs[0].Reason, "GROUP BY (x1)") {
		t.Fatalf("reason = %q, want GROUP BY count", recs[0].Reason)
	}
}

func TestExtractColumnsHandlesNilListsAndDuplicates(t *testing.T) {
	out := extractColumns(nil)
	if len(out) != 0 {
		t.Fatalf("nil expression columns = %v, want empty", out)
	}

	out = extractColumns(&query.InExpr{
		Expr: &query.Identifier{Name: "ID"},
		List: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "other"},
		},
	})
	got := out[""]
	if len(got) != 2 || got[0] != "ID" || got[1] != "other" {
		t.Fatalf("deduplicated columns = %v, want [ID other]", got)
	}
}
