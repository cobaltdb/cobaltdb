package engine

import (
	"context"
	"testing"
)

// TestAuditFTSMatchesLiveContent is a regression test for a full-text-search
// correctness bug: the FTS inverted index is populated only at CREATE FULLTEXT
// INDEX time and never maintained on write, yet MATCH ... AGAINST gated every
// term on that frozen index. As a result, terms inserted or updated after index
// creation could never match — and creating an index silently broke MATCH
// queries that the live-substring fallback answered correctly. MATCH now always
// evaluates against the row's live column text.
func TestAuditFTSMatchesLiveContent(t *testing.T) {
	ctx := context.Background()
	db := mustOpenMem(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)")
	mustExec(t, db, "INSERT INTO docs VALUES (1, 'hello world')")
	mustExec(t, db, "CREATE FULLTEXT INDEX ft ON docs(body)")

	ids := func(sql string) string {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		out := ""
		for rows.Next() {
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out += toStrEng(v)
		}
		return out
	}

	// A term present at index-creation time still matches.
	if got := ids("SELECT id FROM docs WHERE MATCH(body) AGAINST('world')"); got != "1" {
		t.Errorf("AGAINST('world') = %q, want \"1\"", got)
	}
	// A row inserted AFTER index creation must match.
	mustExec(t, db, "INSERT INTO docs VALUES (2, 'lazy dog')")
	if got := ids("SELECT id FROM docs WHERE MATCH(body) AGAINST('dog')"); got != "2" {
		t.Errorf("AGAINST('dog') after post-index insert = %q, want \"2\"", got)
	}
	// Content added by UPDATE must match; removed content must not.
	mustExec(t, db, "UPDATE docs SET body = 'cherry grape' WHERE id = 1")
	if got := ids("SELECT id FROM docs WHERE MATCH(body) AGAINST('cherry')"); got != "1" {
		t.Errorf("AGAINST('cherry') after update = %q, want \"1\"", got)
	}
	if got := ids("SELECT id FROM docs WHERE MATCH(body) AGAINST('world')"); got != "" {
		t.Errorf("AGAINST('world') after update removed it = %q, want empty", got)
	}
	// Deleted rows must not match.
	mustExec(t, db, "DELETE FROM docs WHERE id = 2")
	if got := ids("SELECT id FROM docs WHERE MATCH(body) AGAINST('dog')"); got != "" {
		t.Errorf("AGAINST('dog') after delete = %q, want empty", got)
	}
}

func toStrEng(v interface{}) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	case int64:
		return itoa(int(s))
	case int:
		return itoa(s)
	default:
		return ""
	}
}
