package integration

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// scalarString evaluates a single-value expression and renders it for
// comparison. NULL is reported as the literal "NULL".
func scalarString(t *testing.T, db *engine.DB, ctx context.Context, sql string) string {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatalf("query %q returned no rows", sql)
	}
	var v interface{}
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan %q: %v", sql, err)
	}
	if v == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", v)
}

// TestMySQLCompatScalarFunctions covers the MySQL builtins added for client
// and ORM compatibility. Expected values follow MySQL 8 semantics.
func TestMySQLCompatScalarFunctions(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	cases := []struct{ name, sql, want string }{
		// CHAR_LENGTH counts characters; LENGTH counts bytes.
		{"char_length_ascii", `SELECT CHAR_LENGTH('abc')`, "3"},
		{"char_length_utf8", `SELECT CHAR_LENGTH('héllo')`, "5"},
		{"character_length", `SELECT CHARACTER_LENGTH('héllo')`, "5"},
		{"length_is_bytes", `SELECT LENGTH('héllo')`, "6"},
		{"char_length_null", `SELECT CHAR_LENGTH(NULL)`, "NULL"},
		{"bit_length", `SELECT BIT_LENGTH('abc')`, "24"},

		{"ord_ascii", `SELECT ORD('A')`, "65"},
		{"ord_multibyte", `SELECT ORD('é')`, "50089"}, // 0xC3A9
		{"ord_empty", `SELECT ORD('')`, "0"},

		{"space", `SELECT CONCAT('[', SPACE(3), ']')`, "[   ]"},
		{"elt_hit", `SELECT ELT(2,'a','b','c')`, "b"},
		{"elt_out_of_range", `SELECT ELT(9,'a','b')`, "NULL"},
		{"elt_zero", `SELECT ELT(0,'a','b')`, "NULL"},
		{"field_hit", `SELECT FIELD('b','a','b','c')`, "2"},
		{"field_miss", `SELECT FIELD('z','a','b')`, "0"},

		{"unhex", `SELECT UNHEX('414243')`, "ABC"},
		{"unhex_invalid", `SELECT UNHEX('zz')`, "NULL"},
		{"bin", `SELECT BIN(5)`, "101"},
		{"oct", `SELECT OCT(8)`, "10"},
		{"to_base64", `SELECT TO_BASE64('abc')`, "YWJj"},
		{"from_base64", `SELECT FROM_BASE64('YWJj')`, "abc"},
		{"base64_roundtrip", `SELECT FROM_BASE64(TO_BASE64('hello world'))`, "hello world"},

		// Hash digests verified against known vectors.
		{"md5", `SELECT MD5('abc')`, "900150983cd24fb0d6963f7d28e17f72"},
		{"sha1", `SELECT SHA1('abc')`, "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{"sha2_256", `SELECT SHA2('abc',256)`, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"sha2_default", `SELECT SHA2('abc',0)`, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"sha2_bad_width", `SELECT SHA2('abc',999)`, "NULL"},
		{"crc32", `SELECT CRC32('abc')`, "891568578"},
		{"md5_null", `SELECT MD5(NULL)`, "NULL"},

		{"isnull_true", `SELECT ISNULL(NULL)`, "1"},
		{"isnull_false", `SELECT ISNULL(1)`, "0"},

		// DATE_FORMAT uses MySQL specifiers: %i is minutes, %M the month name.
		{"date_format_ymd", `SELECT DATE_FORMAT('2024-03-05 14:07:09','%Y-%m-%d')`, "2024-03-05"},
		{"date_format_time", `SELECT DATE_FORMAT('2024-03-05 14:07:09','%H:%i:%s')`, "14:07:09"},
		{"date_format_month_name", `SELECT DATE_FORMAT('2024-03-05 14:07:09','%M')`, "March"},
		{"date_format_12h", `SELECT DATE_FORMAT('2024-03-05 14:07:09','%h:%i %p')`, "02:07 PM"},
		{"date_format_midnight", `SELECT DATE_FORMAT('2024-03-05 00:30:00','%h %p')`, "12 AM"},
		{"date_format_literal_pct", `SELECT DATE_FORMAT('2024-03-05 00:00:00','%%')`, "%"},
		{"date_format_null", `SELECT DATE_FORMAT(NULL,'%Y')`, "NULL"},

		{"last_day", `SELECT LAST_DAY('2024-02-10')`, "2024-02-29"}, // leap year
		{"last_day_dec", `SELECT LAST_DAY('2023-12-01')`, "2023-12-31"},

		{"unix_timestamp_fixed", `SELECT UNIX_TIMESTAMP('1970-01-02 00:00:00')`, "86400"},
		{"from_unixtime", `SELECT FROM_UNIXTIME(0,'%Y-%m-%d')`, "1970-01-01"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scalarString(t, db, ctx, tc.sql); got != tc.want {
				t.Errorf("%s\n  sql:  %s\n  got:  %s\n  want: %s", tc.name, tc.sql, got, tc.want)
			}
		})
	}
}

// TestMySQLCompatIfFunction covers MySQL's IF(cond, then, else). IF shares a
// keyword with the DDL `IF [NOT] EXISTS` guards, so this also asserts those
// still parse.
func TestMySQLCompatIfFunction(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	if _, err := db.Exec(ctx, `CREATE TABLE s (id INT PRIMARY KEY, v INT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO s VALUES (1,10),(2,0)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	cases := []struct{ name, sql, want string }{
		{"if_true", `SELECT IF(1,'a','b')`, "a"},
		{"if_false", `SELECT IF(0,'a','b')`, "b"},
		{"if_null_cond", `SELECT IF(NULL,'a','b')`, "b"},
		{"if_expr_cond", `SELECT IF(2>1,'yes','no')`, "yes"},
		{"if_nested", `SELECT IF(0,'a',IF(1,'b','c'))`, "b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scalarString(t, db, ctx, tc.sql); got != tc.want {
				t.Errorf("%s: got %s, want %s", tc.sql, got, tc.want)
			}
		})
	}

	// IF must not evaluate the untaken branch: the guarded division by zero
	// would error if both branches were evaluated eagerly.
	t.Run("if_is_lazy", func(t *testing.T) {
		rows, err := db.Query(ctx, `SELECT IF(v = 0, 0, 100/v) FROM s ORDER BY id`)
		if err != nil {
			t.Fatalf("lazy IF query: %v", err)
		}
		defer func() { _ = rows.Close() }()
		var got []string
		for rows.Next() {
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got = append(got, fmt.Sprintf("%v", v))
		}
		want := []string{"10", "0"}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("row %d: got %s, want %s", i, got[i], want[i])
			}
		}
	})

	t.Run("ddl_if_guards_still_parse", func(t *testing.T) {
		if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS s (id INT)`); err != nil {
			t.Errorf("CREATE TABLE IF NOT EXISTS: %v", err)
		}
		if _, err := db.Exec(ctx, `DROP TABLE IF EXISTS definitely_absent`); err != nil {
			t.Errorf("DROP TABLE IF EXISTS: %v", err)
		}
	})
}

// TestMySQLCompatRandAndUUID checks the non-deterministic builtins by shape
// and range rather than exact value.
func TestMySQLCompatRandAndUUID(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	seen := make(map[string]bool)
	for i := 0; i < 20; i++ {
		got := scalarString(t, db, ctx, `SELECT RAND()`)
		f, err := strconv.ParseFloat(got, 64)
		if err != nil {
			t.Fatalf("RAND() returned non-numeric %q: %v", got, err)
		}
		if f < 0 || f >= 1 {
			t.Errorf("RAND() = %v, want value in [0,1)", f)
		}
		seen[got] = true
	}
	if len(seen) < 2 {
		t.Errorf("RAND() produced %d distinct values over 20 calls, want varying output", len(seen))
	}

	u := scalarString(t, db, ctx, `SELECT UUID()`)
	if len(u) != 36 || strings.Count(u, "-") != 4 {
		t.Errorf("UUID() = %q, want 36-char hyphenated form", u)
	}
	if u[14] != '4' {
		t.Errorf("UUID() = %q, want version 4 (position 14 == '4')", u)
	}
	if u2 := scalarString(t, db, ctx, `SELECT UUID()`); u2 == u {
		t.Errorf("UUID() returned the same value twice: %q", u)
	}
}

// TestMySQLCompatFunctionsOverTableData exercises the new builtins against
// stored rows, covering the row-evaluation path rather than constant folding.
func TestMySQLCompatFunctionsOverTableData(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	if _, err := db.Exec(ctx, `CREATE TABLE u (id INT PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO u VALUES (1,'ali'),(2,'béa'),(3,NULL)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rows, err := db.Query(ctx, `SELECT id, CHAR_LENGTH(name), LENGTH(name), MD5(name) FROM u ORDER BY id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer func() { _ = rows.Close() }()

	type rec struct {
		id               int
		charLen, byteLen interface{}
		md5              interface{}
	}
	var got []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.charLen, &r.byteLen, &r.md5); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	if fmt.Sprintf("%v", got[1].charLen) != "3" {
		t.Errorf("CHAR_LENGTH('béa') = %v, want 3", got[1].charLen)
	}
	if fmt.Sprintf("%v", got[1].byteLen) != "4" {
		t.Errorf("LENGTH('béa') = %v, want 4 bytes", got[1].byteLen)
	}
	if got[2].charLen != nil || got[2].md5 != nil {
		t.Errorf("NULL name should yield NULL results, got charLen=%v md5=%v", got[2].charLen, got[2].md5)
	}
}
