package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

// TestBLOBRowInt64Fidelity pins that a row containing a BLOB (which takes the
// binary row-encoder path, rowHasBinaryValue -> encodeBinaryVersionedRow)
// preserves int64 values of ANY magnitude in its other columns.
//
// decodeBinaryVersionedRow (pkg/catalog/temporal.go) used to unmarshal Data
// through []interface{}, where encoding/json turns every number into float64
// BEFORE the int64-restore heuristic runs — and float64 cannot represent
// integers above 2^53. A stored 9007199254740993 (2^53+1, odd) silently came
// back as 9007199254740992 (even): the VALUE changed, with no error at write
// or read time, while the identical INSERT into a row without a BLOB
// round-tripped exactly through the JSON fast path (which parses integers as
// int64 directly) — the internal-inconsistency tell.
//
// The decoder now uses json.Decoder with UseNumber, restoring integers
// exactly from their digit text (restoreBinaryRowNumber), keeping the
// historical integral heuristic for exponent-form values and normalizing
// numbers nested inside JSON column values back to float64
// (normalizeBinaryRowJSONValue) so no caller observes a json.Number.
func TestBLOBRowInt64Fidelity(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{{"memory", ":memory:"}, {"disk", filepath.Join(t.TempDir(), "blob-int64.db")}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Open(tc.path, nil)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()

			mustExec(t, db, `CREATE TABLE b (id INTEGER PRIMARY KEY, big INTEGER, small INTEGER, frac REAL, exp REAL, meta JSON, blob BLOB)`)
			// Local parameterized exec (the package-level mustExec is
			// sql-only).
			bexec := func(sql string, args ...interface{}) {
				t.Helper()
				if _, err := db.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("exec %q: %v", sql, err)
				}
			}
			// BLOB row (binary path): the int64 edge 2^53+1 plus boundary values.
			bexec(`INSERT INTO b VALUES (?, ?, ?, ?, ?, JSON_EXTRACT('{"n":42}','$'), ?)`,
				int64(1), int64(9007199254740993), int64(42), 1.5, float64(1e15), []byte{0x00, 0xff, 'x'})
			// Control row (JSON fast path): same values, NULL blob.
			bexec(`INSERT INTO b VALUES (?, ?, ?, ?, ?, JSON_EXTRACT('{"n":42}','$'), ?)`,
				int64(2), int64(9007199254740993), int64(42), 1.5, float64(1e15), nil)

			scan := func(sql string) interface{} {
				t.Helper()
				rows, err := db.Query(ctx, sql)
				if err != nil {
					t.Fatalf("query %q: %v", sql, err)
				}
				defer rows.Close()
				if !rows.Next() {
					t.Fatalf("query %q: no rows", sql)
				}
				var v interface{}
				if err := rows.Scan(&v); err != nil {
					t.Fatalf("scan %q: %v", sql, err)
				}
				return v
			}

			// Discriminator: the BLOB row's 2^53+1 must be an exact int64.
			v := scan(`SELECT big FROM b WHERE id = 1`)
			if n, ok := v.(int64); !ok || n != 9007199254740993 {
				t.Fatalf("FAIL: BLOB row big = %T(%v), want int64(9007199254740993) — the binary row path corrupted the value through float64", v, v)
			}
			// Parity: the value is odd; float64 rounding makes it even.
			if p := fmt.Sprint(scan(`SELECT big % 2 FROM b WHERE id = 1`)); p != "1" {
				t.Fatalf("FAIL: BLOB row big parity = %v, want 1 (9007199254740993 is odd)", p)
			}
			// Control: the NULL-blob row round-trips exactly.
			cv := scan(`SELECT big FROM b WHERE id = 2`)
			if n, ok := cv.(int64); !ok || n != 9007199254740993 {
				t.Fatalf("control: NULL-blob row big = %T(%v), want int64(9007199254740993)", cv, cv)
			}
			// Boundary: small int stays int64 in a BLOB row.
			sv := scan(`SELECT small FROM b WHERE id = 1`)
			if n, ok := sv.(int64); !ok || n != 42 {
				t.Fatalf("BLOB row small = %T(%v), want int64(42)", sv, sv)
			}
			// Boundary: non-integral stays float64.
			fv := scan(`SELECT frac FROM b WHERE id = 1`)
			if f, ok := fv.(float64); !ok || f != 1.5 {
				t.Fatalf("BLOB row frac = %T(%v), want float64(1.5)", fv, fv)
			}
			// Historical behavior: exponent-form 1e+15 keeps its int64
			// restoration (the pre-UseNumber heuristic is preserved).
			ev := scan(`SELECT exp FROM b WHERE id = 1`)
			if n, ok := ev.(int64); !ok || n != 1000000000000000 {
				t.Fatalf("BLOB row exp = %T(%v), want int64(1000000000000000)", ev, ev)
			}
			// Nested JSON numbers normalize to float64 (never json.Number).
			mv := scan(`SELECT meta FROM b WHERE id = 1`)
			m, ok := mv.(map[string]interface{})
			if !ok {
				t.Fatalf("BLOB row meta = %T, want map[string]interface{}", mv)
			}
			if n, ok := m["n"].(float64); !ok || n != 42 {
				t.Fatalf("BLOB row meta[\"n\"] = %T(%v), want float64(42) — a json.Number leaked from the decoder", m["n"], m["n"])
			}
			// The BLOB itself stays intact.
			bv := scan(`SELECT blob FROM b WHERE id = 1`)
			bs, ok := bv.([]byte)
			if !ok || len(bs) != 3 || bs[0] != 0x00 || bs[1] != 0xff || bs[2] != 'x' {
				t.Fatalf("BLOB row blob = %T(%v), want the original 3 bytes", bv, bv)
			}
		})
	}
}
