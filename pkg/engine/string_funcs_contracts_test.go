package engine

import (
	"context"
	"testing"
)

// TestStringFuncsContracts pins the MySQL value contracts of the string
// builtins (SUBSTR/SUBSTRING, INSTR, LOCATE, REPLACE, CONCAT, CONCAT_WS,
// LENGTH, TRIM/LTRIM/RTRIM) through the real dispatch:
//
//   - positions are 1-based CHARACTERS for multibyte strings
//     (SUBSTR('héllo',1,2)='hé', INSTR('héllo','l')=3, LOCATE likewise),
//   - SUBSTR position 0 yields the empty string (MySQL: "If pos is 0, the
//     function returns an empty string"),
//   - negative SUBSTR positions count from the end ('héllo',-2 → 'lo'),
//   - CONCAT returns NULL if any argument is NULL; CONCAT_WS skips NULLs,
//   - LENGTH counts bytes (LENGTH('héllo') = 6).
func TestStringFuncsContracts(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	cases := []struct {
		sql  string
		want interface{}
	}{
		// SUBSTR: rune-based positions, negative starts, position 0.
		{"SELECT SUBSTR('héllo', 1, 2)", "hé"},
		{"SELECT SUBSTR('héllo', 3)", "llo"},
		{"SELECT SUBSTR('héllo', -2)", "lo"},
		{"SELECT SUBSTR('héllo', -2, 5)", "lo"},
		{"SELECT SUBSTR('héllo', 2, 10)", "éllo"},
		{"SELECT SUBSTR('héllo', 6)", ""},
		{"SELECT SUBSTR('héllo', 0, 2)", ""},
		{"SELECT SUBSTR('héllo', 0)", ""},
		{"SELECT SUBSTRING('héllo', 0, 2)", ""},
		// INSTR / LOCATE: 1-based character positions, 0 when absent.
		{"SELECT INSTR('héllo', 'l')", float64(3)},
		{"SELECT INSTR('héllo', 'x')", float64(0)},
		{"SELECT LOCATE('l', 'héllo')", float64(3)},
		{"SELECT LOCATE('l', 'héllo', 4)", float64(4)},
		{"SELECT LOCATE('l', 'héllo', 5)", float64(0)},
		{"SELECT POSITION('l' IN 'héllo')", float64(3)},
		// REPLACE / CONCAT / CONCAT_WS.
		{"SELECT REPLACE('aaa', 'aa', 'b')", "ba"},
		{"SELECT REPLACE('héllo', 'é', 'e')", "hello"},
		{"SELECT REPLACE('abc', '', 'x')", "abc"},
		{"SELECT CONCAT('a', 'b', 'c')", "abc"},
		{"SELECT CONCAT('a', NULL, 'c')", nil},
		{"SELECT CONCAT_WS('-', 'a', NULL, 'c')", "a-c"},
		// LENGTH counts bytes; TRIM family trims spaces and tabs/newlines.
		{"SELECT LENGTH('héllo')", int64(6)},
		{"SELECT LENGTH('')", int64(0)},
		{"SELECT TRIM('  x  ')", "x"},
		{"SELECT LTRIM('\t x')", "x"},
		{"SELECT RTRIM('x \n')", "x"},
	}
	for _, tc := range cases {
		rows, err := db.Query(ctx, tc.sql)
		if err != nil {
			t.Fatalf("%s: query: %v", tc.sql, err)
		}
		if !rows.Next() {
			t.Fatalf("%s: no rows", tc.sql)
		}
		var got interface{}
		if err := rows.Scan(&got); err != nil {
			t.Fatalf("%s: scan: %v", tc.sql, err)
		}
		_ = rows.Close()
		if !wantEq(got, tc.want) {
			t.Fatalf("%s = %#v, want %#v", tc.sql, got, tc.want)
		}
	}
}

// wantEq compares results across the engine's numeric result types (int,
// int64, float64), falling back to interface equality for strings and nil.
func wantEq(got, want interface{}) bool {
	if got == want {
		return true
	}
	gf, gok := asFloat(got)
	wf, wok := asFloat(want)
	return gok && wok && gf == wf
}
