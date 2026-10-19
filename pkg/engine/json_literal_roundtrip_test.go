package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

// TestJSONLiteralRoundTripMaterialization pins that valueToLiteralExpr
// preserves a JSON column's composite decoded values (objects scan back as
// map[string]interface{}, arrays as []interface{}) as their JSON document,
// rather than rendering Go syntax. Neither type had a case in the switch, so
// both fell to the default `fmt.Sprintf("%v", val)`, which silently corrupted
// the document on both of valueToLiteralExpr's call sites:
//
//  1. CTAS  (database_ddl.go: CREATE TABLE dst AS SELECT ...)
//  2. INSERT..SELECT with a conflict clause (database_dml.go executeUpsert's
//     materialized-source branch)
//
// Pre-fix, a {"deleted_at":42,"name":"x"} payload materialized as the string
// "map[deleted_at:42 name:x]" and [1,2,3] as "[1 2 3]" — not JSON, with no
// error at DDL or query time. The fix re-encodes composites via json.Marshal
// (mirroring the earlier []byte fix, which likewise materializes as a string
// literal), so the round-trip preserves the document.
//
// Comparison is by document identity: a materialized value legitimately comes
// back as JSON TEXT (the literal re-injection path stores a string), so the
// scanned value is parsed as JSON when it parses — Go-syntax renderings do not
// parse and fail the comparison, which is exactly the corruption to detect.
func TestJSONLiteralRoundTripMaterialization(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	jExec := func(sql string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	jScan := func(sql string) interface{} {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("query %q returned no rows", sql)
		}
		vals := make([]interface{}, 1)
		if err := rows.Scan(&vals[0]); err != nil {
			t.Fatalf("scan %q: %v", sql, err)
		}
		return vals[0]
	}
	// docEqual reports whether two scanned values hold the same JSON document.
	docEqual := func(a, b interface{}) bool {
		norm := func(v interface{}) string {
			if s, ok := v.(string); ok {
				var parsed interface{}
				if err := json.Unmarshal([]byte(s), &parsed); err == nil {
					v = parsed
				}
			}
			enc, err := json.Marshal(v)
			if err != nil {
				return fmt.Sprintf("!!marshal-error:%v", err)
			}
			return string(enc)
		}
		return norm(a) == norm(b)
	}

	jExec(`CREATE TABLE src (id INTEGER PRIMARY KEY, payload JSON)`)
	jExec(`INSERT INTO src VALUES (1, JSON_EXTRACT('{"deleted_at":42,"name":"x"}', '$'))`)
	jExec(`INSERT INTO src VALUES (2, JSON_EXTRACT('[1,2,3]', '$'))`)
	jExec(`INSERT INTO src VALUES (3, JSON_EXTRACT('{"a":[1,{"b":2}]}', '$'))`)
	// Boundaries: empty documents.
	jExec(`INSERT INTO src VALUES (4, JSON_EXTRACT('{}', '$'))`)
	jExec(`INSERT INTO src VALUES (5, JSON_EXTRACT('[]', '$'))`)

	// Control: the same JSON text in a TEXT column must round-trip untouched.
	jExec(`CREATE TABLE ctl (id INTEGER PRIMARY KEY, t TEXT)`)
	jExec(`INSERT INTO ctl VALUES (1, '{"deleted_at":42,"name":"x"}')`)
	if got, want := jScan(`SELECT t FROM ctl WHERE id = 1`), `{"deleted_at":42,"name":"x"}`; fmt.Sprint(got) != want {
		t.Fatalf("control: source text = %q, want %q", fmt.Sprint(got), want)
	}

	// Control: the source documents themselves must scan back intact.
	srcCases := []struct {
		id   string
		want string
	}{
		{"1", `{"deleted_at":42,"name":"x"}`},
		{"2", `[1,2,3]`},
		{"3", `{"a":[1,{"b":2}]}`},
		{"4", `{}`},
		{"5", `[]`},
	}
	for _, tc := range srcCases {
		got := jScan(`SELECT payload FROM src WHERE id = ` + tc.id)
		if !docEqual(got, tc.want) {
			t.Fatalf("control: src id=%s payload = %v (%T), want document %s", tc.id, got, got, tc.want)
		}
	}

	// Call site 1 — CTAS materialization must preserve every document.
	jExec(`CREATE TABLE dst_ctas AS SELECT id, payload FROM src`)
	for _, tc := range srcCases {
		src := jScan(`SELECT payload FROM src WHERE id = ` + tc.id)
		dst := jScan(`SELECT payload FROM dst_ctas WHERE id = ` + tc.id)
		if !docEqual(src, dst) {
			t.Fatalf("CTAS id=%s: src=%v dst=%v (want equal documents)", tc.id, src, dst)
		}
	}

	// Call site 2 — INSERT..SELECT with a conflict clause.
	jExec(`CREATE TABLE dst_conf (id INTEGER PRIMARY KEY, payload JSON)`)
	jExec(`INSERT INTO dst_conf VALUES (1, 'starter')`)
	jExec(`INSERT INTO dst_conf SELECT id, payload FROM src WHERE id = 1 ON CONFLICT (id) DO UPDATE SET payload = VALUES(payload)`)
	src := jScan(`SELECT payload FROM src WHERE id = 1`)
	dst := jScan(`SELECT payload FROM dst_conf WHERE id = 1`)
	if !docEqual(src, dst) {
		t.Fatalf("conflict materialization: src=%v dst=%v (want equal documents)", src, dst)
	}

	// Control: plain TEXT round-trip through CTAS stays untouched.
	jExec(`CREATE TABLE ctl_ctas AS SELECT id, t FROM ctl`)
	if got, want := jScan(`SELECT t FROM ctl_ctas WHERE id = 1`), `{"deleted_at":42,"name":"x"}`; fmt.Sprint(got) != want {
		t.Fatalf("control: TEXT CTAS = %q, want %q", fmt.Sprint(got), want)
	}
}
