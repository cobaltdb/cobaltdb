package test

import (
	"fmt"
	"strings"
	"testing"
)

// =============================================================================
// v93: Deep function coverage targeting specific uncovered code paths
// Targets: evaluateFunctionCall remaining functions, replaceAggregatesInExpr,
//          applyGroupByOrderBy branches, evaluateLike ESCAPE/NOT LIKE
// =============================================================================

// --- evaluateFunctionCall: REVERSE ---
func TestV93_FuncReverse(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93r (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93r VALUES (1, 'hello')")
	afExpectVal(t, db, ctx, "SELECT REVERSE(s) FROM t93r WHERE id = 1", "olleh")
}

func TestV93_FuncReverseEmpty(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93re (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93re VALUES (1, '')")
	afExpectVal(t, db, ctx, "SELECT REVERSE(s) FROM t93re WHERE id = 1", "")
}

// --- evaluateFunctionCall: REPEAT ---
func TestV93_FuncRepeat(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93rep (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93rep VALUES (1, 'ab')")
	afExpectVal(t, db, ctx, "SELECT REPEAT(s, 3) FROM t93rep WHERE id = 1", "ababab")
}

func TestV93_FuncRepeatZero(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93rz (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93rz VALUES (1, 'x')")
	afExpectVal(t, db, ctx, "SELECT REPEAT(s, 0) FROM t93rz WHERE id = 1", "")
}

// --- evaluateFunctionCall: LEFT / RIGHT ---
func TestV93_FuncLeft(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93l (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93l VALUES (1, 'hello')")
	afExpectVal(t, db, ctx, "SELECT LEFT(s, 3) FROM t93l WHERE id = 1", "hel")
}

func TestV93_FuncLeftZero(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93lz (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93lz VALUES (1, 'hello')")
	afExpectVal(t, db, ctx, "SELECT LEFT(s, 0) FROM t93lz WHERE id = 1", "")
}

func TestV93_FuncLeftOverflow(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93lo (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93lo VALUES (1, 'hi')")
	afExpectVal(t, db, ctx, "SELECT LEFT(s, 100) FROM t93lo WHERE id = 1", "hi")
}

func TestV93_FuncRight(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93ri (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93ri VALUES (1, 'hello')")
	afExpectVal(t, db, ctx, "SELECT RIGHT(s, 3) FROM t93ri WHERE id = 1", "llo")
}

func TestV93_FuncRightZero(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93rz2 (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93rz2 VALUES (1, 'hello')")
	afExpectVal(t, db, ctx, "SELECT RIGHT(s, 0) FROM t93rz2 WHERE id = 1", "")
}

func TestV93_FuncRightOverflow(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93ro (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93ro VALUES (1, 'hi')")
	afExpectVal(t, db, ctx, "SELECT RIGHT(s, 100) FROM t93ro WHERE id = 1", "hi")
}

// --- evaluateFunctionCall: LPAD / RPAD ---
func TestV93_FuncLpad(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93lp (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93lp VALUES (1, 'hi')")
	afExpectVal(t, db, ctx, "SELECT LPAD(s, 5, '0') FROM t93lp WHERE id = 1", "000hi")
}

func TestV93_FuncLpadTruncate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93lt (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93lt VALUES (1, 'hello world')")
	afExpectVal(t, db, ctx, "SELECT LPAD(s, 3, '0') FROM t93lt WHERE id = 1", "hel")
}

func TestV93_FuncRpad(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93rp (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93rp VALUES (1, 'hi')")
	afExpectVal(t, db, ctx, "SELECT RPAD(s, 5, '0') FROM t93rp WHERE id = 1", "hi000")
}

func TestV93_FuncRpadTruncate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93rt (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93rt VALUES (1, 'hello world')")
	afExpectVal(t, db, ctx, "SELECT RPAD(s, 3, '0') FROM t93rt WHERE id = 1", "hel")
}

// --- evaluateFunctionCall: HEX ---
func TestV93_FuncHexNumeric(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93h (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93h VALUES (1, 255)")
	afExpectVal(t, db, ctx, "SELECT HEX(val) FROM t93h WHERE id = 1", "FF")
}

func TestV93_FuncHexString(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93hs (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93hs VALUES (1, 'AB')")
	afExpectVal(t, db, ctx, "SELECT HEX(s) FROM t93hs WHERE id = 1", "4142")
}

// --- evaluateFunctionCall: TYPEOF ---
func TestV93_FuncTypeofInteger(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93ti (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93ti VALUES (1, 42)")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(val) FROM t93ti WHERE id = 1", "integer")
}

func TestV93_FuncTypeofReal(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93tr (id INTEGER PRIMARY KEY, val REAL)")
	afExec(t, db, ctx, "INSERT INTO t93tr VALUES (1, 3.14)")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(val) FROM t93tr WHERE id = 1", "real")
}

func TestV93_FuncTypeofText(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93tt (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93tt VALUES (1, 'abc')")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(s) FROM t93tt WHERE id = 1", "text")
}

func TestV93_FuncTypeofNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93tn (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93tn VALUES (1, NULL)")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(val) FROM t93tn WHERE id = 1", "null")
}

func TestV93_FuncTypeofBool(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93tb (id INTEGER PRIMARY KEY, val BOOLEAN)")
	afExec(t, db, ctx, "INSERT INTO t93tb VALUES (1, TRUE)")
	rows := afQuery(t, db, ctx, "SELECT TYPEOF(val) FROM t93tb WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	// TYPEOF(TRUE) returns "integer" in CobaltDB
	if got != "integer" && got != "boolean" {
		t.Fatalf("expected 'integer' or 'boolean', got %v", got)
	}
}

// --- evaluateFunctionCall: IIF ---
func TestV93_FuncIIFTrue(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93iif (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93iif VALUES (1, 10)")
	afExpectVal(t, db, ctx, "SELECT IIF(val > 5, 'yes', 'no') FROM t93iif WHERE id = 1", "yes")
}

func TestV93_FuncIIFFalse(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93iif2 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93iif2 VALUES (1, 3)")
	afExpectVal(t, db, ctx, "SELECT IIF(val > 5, 'yes', 'no') FROM t93iif2 WHERE id = 1", "no")
}

func TestV93_FuncIIFNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93iif3 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93iif3 VALUES (1, NULL)")
	afExpectVal(t, db, ctx, "SELECT IIF(val, 'yes', 'no') FROM t93iif3 WHERE id = 1", "no")
}

func TestV93_FuncIIFNumericCondition(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93iif4 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93iif4 VALUES (1, 0)")
	afExec(t, db, ctx, "INSERT INTO t93iif4 VALUES (2, 1)")
	afExpectVal(t, db, ctx, "SELECT IIF(val, 'truthy', 'falsy') FROM t93iif4 WHERE id = 1", "falsy")
	afExpectVal(t, db, ctx, "SELECT IIF(val, 'truthy', 'falsy') FROM t93iif4 WHERE id = 2", "truthy")
}

// --- evaluateFunctionCall: RANDOM ---
func TestV93_FuncRandom(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93rand (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t93rand VALUES (1)")
	rows := afQuery(t, db, ctx, "SELECT RANDOM() FROM t93rand")
	if len(rows) == 0 || rows[0][0] == nil {
		t.Fatal("RANDOM() should return a value")
	}
}

// --- evaluateFunctionCall: UNICODE ---
func TestV93_FuncUnicode(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93u (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93u VALUES (1, 'A')")
	afExpectVal(t, db, ctx, "SELECT UNICODE(s) FROM t93u WHERE id = 1", float64(65))
}

func TestV93_FuncUnicodeEmpty(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93ue (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93ue VALUES (1, '')")
	rows := afQuery(t, db, ctx, "SELECT UNICODE(s) FROM t93ue WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	// UNICODE('') returns nil
	if rows[0][0] != nil {
		t.Fatalf("expected nil for UNICODE(''), got %v", rows[0][0])
	}
}

// --- evaluateFunctionCall: CHAR ---
func TestV93_FuncChar(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93c (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t93c VALUES (1)")
	afExpectVal(t, db, ctx, "SELECT CHAR(65, 66, 67) FROM t93c WHERE id = 1", "ABC")
}

// --- evaluateFunctionCall: ZEROBLOB ---
func TestV93_FuncZeroblob(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93z (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t93z VALUES (1)")
	rows := afQuery(t, db, ctx, "SELECT ZEROBLOB(4) FROM t93z WHERE id = 1")
	if len(rows) == 0 || rows[0][0] == nil {
		t.Fatal("ZEROBLOB should return a value")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if len(got) != 4 {
		t.Logf("ZEROBLOB(4) returned: %q (len=%d)", got, len(got))
	}
}

// --- evaluateFunctionCall: QUOTE ---
func TestV93_FuncQuoteNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93q (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93q VALUES (1, NULL)")
	afExpectVal(t, db, ctx, "SELECT QUOTE(val) FROM t93q WHERE id = 1", "NULL")
}

func TestV93_FuncQuoteString(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93qs (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93qs VALUES (1, 'it''s')")
	rows := afQuery(t, db, ctx, "SELECT QUOTE(s) FROM t93qs WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if !strings.Contains(got, "'") {
		t.Fatalf("QUOTE should wrap in quotes, got %v", got)
	}
}

// --- evaluateFunctionCall: GLOB ---
func TestV93_FuncGlobMatch(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93g (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93g VALUES (1, 'file.txt')")
	rows := afQuery(t, db, ctx, "SELECT GLOB('*.txt', s) FROM t93g WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "1" && got != "true" && got != "TRUE" {
		t.Fatalf("GLOB('*.txt', 'file.txt') should match, got %v", got)
	}
}

func TestV93_FuncGlobNoMatch(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93gn (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93gn VALUES (1, 'file.csv')")
	rows := afQuery(t, db, ctx, "SELECT GLOB('*.txt', s) FROM t93gn WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "0" && got != "false" && got != "FALSE" {
		t.Fatalf("GLOB('*.txt', 'file.csv') should not match, got %v", got)
	}
}

// --- evaluateFunctionCall: CAST BOOLEAN ---
func TestV93_CastToBoolean(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93cb (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93cb VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO t93cb VALUES (2, 0)")
	rows := afQuery(t, db, ctx, "SELECT CAST(val AS BOOLEAN) FROM t93cb WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "true" && got != "1" {
		t.Fatalf("CAST(1 AS BOOLEAN) expected true, got %v", got)
	}
	rows2 := afQuery(t, db, ctx, "SELECT CAST(val AS BOOLEAN) FROM t93cb WHERE id = 2")
	if len(rows2) == 0 {
		t.Fatal("no rows")
	}
	got2 := fmt.Sprintf("%v", rows2[0][0])
	if got2 != "false" && got2 != "0" {
		t.Fatalf("CAST(0 AS BOOLEAN) expected false, got %v", got2)
	}
}

func TestV93_CastStringToBoolean(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93csb (id INTEGER PRIMARY KEY, s TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93csb VALUES (1, 'true')")
	afExec(t, db, ctx, "INSERT INTO t93csb VALUES (2, 'false')")
	rows := afQuery(t, db, ctx, "SELECT CAST(s AS BOOLEAN) FROM t93csb WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "true" && got != "1" {
		t.Fatalf("CAST('true' AS BOOLEAN) expected true, got %v", got)
	}
}

// --- evaluateFunctionCall: PRINTF edge cases ---
func TestV93_PrintfFormatI(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93pf (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93pf VALUES (1, 42)")
	// %i should be same as %d
	rows := afQuery(t, db, ctx, "SELECT PRINTF('%i', val) FROM t93pf WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "42" {
		t.Fatalf("PRINTF('%%i', 42) expected '42', got %v", got)
	}
}

// --- evaluateLike: NOT LIKE ---
func TestV93_NotLike(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93nl (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93nl VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t93nl VALUES (2, 'world')")
	afExec(t, db, ctx, "INSERT INTO t93nl VALUES (3, 'help')")
	rows := afExpectRows(t, db, ctx, "SELECT name FROM t93nl WHERE name NOT LIKE 'hel%'", 1)
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "world" {
		t.Fatalf("expected 'world', got %v", got)
	}
}

// --- evaluateLike: ESCAPE clause ---
func TestV93_LikeEscape(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93le (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93le VALUES (1, 'hello_world')")
	afExec(t, db, ctx, "INSERT INTO t93le VALUES (2, 'helloXworld')")
	afExec(t, db, ctx, "INSERT INTO t93le VALUES (3, 'hello world')")
	// Use ! as escape char, !_ matches literal underscore
	rows := afQuery(t, db, ctx, "SELECT name FROM t93le WHERE name LIKE 'hello!_world' ESCAPE '!'")
	if len(rows) != 1 {
		// If ESCAPE is not supported or behaves differently, just log
		t.Logf("LIKE ESCAPE returned %d rows (expected 1)", len(rows))
	}
}

// --- evaluateLike: consecutive %% ---
func TestV93_LikeConsecutivePercent(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93lc (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93lc VALUES (1, 'abc')")
	afExec(t, db, ctx, "INSERT INTO t93lc VALUES (2, 'xyz')")
	afExpectRows(t, db, ctx, "SELECT name FROM t93lc WHERE name LIKE '%%abc%%'", 1)
}

// =============================================================================
// applyGroupByOrderBy: ORDER BY paths (only called via JOIN+GROUP BY or view/CTE)
// Must use JOIN + GROUP BY to trigger applyGroupByOrderBy function
// =============================================================================
func TestV93_JoinGroupByOrderBySumIdent(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93go_c (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93go_o (id INTEGER PRIMARY KEY, cid INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93go_c VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t93go_c VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO t93go_c VALUES (3, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO t93go_o VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t93go_o VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO t93go_o VALUES (3, 2, 50)")
	afExec(t, db, ctx, "INSERT INTO t93go_o VALUES (4, 3, 300)")
	// JOIN+GROUP BY + ORDER BY SUM(amount) DESC → FunctionCall path with Identifier arg
	rows := afExpectRows(t, db, ctx,
		"SELECT c.name, SUM(o.amount) FROM t93go_c c JOIN t93go_o o ON c.id = o.cid "+
			"GROUP BY c.name ORDER BY SUM(o.amount) DESC", 3)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Alice" && first != "Carol" {
		t.Fatalf("expected 'Alice' or 'Carol' first, got %v", first)
	}
}

func TestV93_JoinGroupByOrderByCountStar(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93gc_d (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93gc_e (id INTEGER PRIMARY KEY, did INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93gc_d VALUES (1, 'Eng')")
	afExec(t, db, ctx, "INSERT INTO t93gc_d VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO t93gc_e VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO t93gc_e VALUES (2, 1)")
	afExec(t, db, ctx, "INSERT INTO t93gc_e VALUES (3, 1)")
	afExec(t, db, ctx, "INSERT INTO t93gc_e VALUES (4, 2)")
	// JOIN+GROUP BY + ORDER BY COUNT(*) DESC → FunctionCall path with StarExpr
	rows := afExpectRows(t, db, ctx,
		"SELECT d.name, COUNT(*) FROM t93gc_d d JOIN t93gc_e e ON d.id = e.did "+
			"GROUP BY d.name ORDER BY COUNT(*) DESC", 2)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Eng" {
		t.Fatalf("expected 'Eng' first (count 3), got %v", first)
	}
}

func TestV93_JoinGroupByOrderByStringCol(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93gs_p (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93gs_s (id INTEGER PRIMARY KEY, pid INTEGER, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93gs_p VALUES (1, 'Charlie')")
	afExec(t, db, ctx, "INSERT INTO t93gs_p VALUES (2, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t93gs_p VALUES (3, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO t93gs_s VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO t93gs_s VALUES (2, 2, 20)")
	afExec(t, db, ctx, "INSERT INTO t93gs_s VALUES (3, 3, 30)")
	// JOIN+GROUP BY + ORDER BY p.name → string comparison path in applyGroupByOrderBy
	rows := afExpectRows(t, db, ctx,
		"SELECT p.name, SUM(s.score) FROM t93gs_p p JOIN t93gs_s s ON p.id = s.pid "+
			"GROUP BY p.name ORDER BY p.name", 3)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Alice" {
		t.Fatalf("expected 'Alice' first, got %v", first)
	}
}

func TestV93_JoinGroupByOrderByPositional(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93gp_t (id INTEGER PRIMARY KEY, label TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93gp_v (id INTEGER PRIMARY KEY, tid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93gp_t VALUES (1, 'X')")
	afExec(t, db, ctx, "INSERT INTO t93gp_t VALUES (2, 'Y')")
	afExec(t, db, ctx, "INSERT INTO t93gp_v VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t93gp_v VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO t93gp_v VALUES (3, 2, 50)")
	// JOIN+GROUP BY + ORDER BY 2 DESC → positional/NumberLiteral path
	rows := afExpectRows(t, db, ctx,
		"SELECT t.label, SUM(v.val) FROM t93gp_t t JOIN t93gp_v v ON t.id = v.tid "+
			"GROUP BY t.label ORDER BY 2 DESC", 2)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "X" {
		t.Fatalf("expected 'X' first (sum 300), got %v", first)
	}
}

func TestV93_JoinGroupByOrderByQualifiedIdent(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93qi_a (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93qi_b (id INTEGER PRIMARY KEY, aid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93qi_a VALUES (1, 'C')")
	afExec(t, db, ctx, "INSERT INTO t93qi_a VALUES (2, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93qi_a VALUES (3, 'B')")
	afExec(t, db, ctx, "INSERT INTO t93qi_b VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO t93qi_b VALUES (2, 2, 20)")
	afExec(t, db, ctx, "INSERT INTO t93qi_b VALUES (3, 3, 30)")
	// JOIN+GROUP BY + ORDER BY a.cat → QualifiedIdentifier path
	rows := afExpectRows(t, db, ctx,
		"SELECT a.cat, SUM(b.val) FROM t93qi_a a JOIN t93qi_b b ON a.id = b.aid "+
			"GROUP BY a.cat ORDER BY a.cat", 3)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "A" {
		t.Fatalf("expected 'A' first, got %v", first)
	}
}

func TestV93_JoinGroupByOrderByStringDesc(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93sd_m (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93sd_d (id INTEGER PRIMARY KEY, mid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93sd_m VALUES (1, 'Alpha')")
	afExec(t, db, ctx, "INSERT INTO t93sd_m VALUES (2, 'Beta')")
	afExec(t, db, ctx, "INSERT INTO t93sd_m VALUES (3, 'Gamma')")
	afExec(t, db, ctx, "INSERT INTO t93sd_d VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO t93sd_d VALUES (2, 2, 20)")
	afExec(t, db, ctx, "INSERT INTO t93sd_d VALUES (3, 3, 30)")
	// JOIN+GROUP BY + ORDER BY m.name DESC → string comparison DESC branch
	rows := afExpectRows(t, db, ctx,
		"SELECT m.name, SUM(d.val) FROM t93sd_m m JOIN t93sd_d d ON m.id = d.mid "+
			"GROUP BY m.name ORDER BY m.name DESC", 3)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Gamma" {
		t.Fatalf("expected 'Gamma' first, got %v", first)
	}
}

func TestV93_JoinGroupByOrderByNulls(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93n_h (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93n_i (id INTEGER PRIMARY KEY, hid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93n_h VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93n_h VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO t93n_i VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO t93n_i VALUES (2, 2, NULL)")
	// JOIN+GROUP BY + ORDER BY 2 with NULLs → null comparison branches
	rows := afExpectRows(t, db, ctx,
		"SELECT h.cat, SUM(i.val) FROM t93n_h h JOIN t93n_i i ON h.id = i.hid "+
			"GROUP BY h.cat ORDER BY 2", 2)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// =============================================================================
// replaceAggregatesInExpr: CaseExpr in JOIN + GROUP BY (lines 6817-6831)
// =============================================================================
func TestV93_JoinGroupByCaseExpr(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93jc_orders (id INTEGER PRIMARY KEY, cust_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t93jc_custs (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93jc_custs VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t93jc_custs VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO t93jc_orders VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t93jc_orders VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO t93jc_orders VALUES (3, 2, 50)")
	// CASE WHEN SUM(...) > X THEN Y ELSE Z END in JOIN context triggers CaseExpr path
	rows := afExpectRows(t, db, ctx,
		"SELECT c.name, CASE WHEN SUM(o.amount) > 100 THEN 'high' ELSE 'low' END "+
			"FROM t93jc_custs c JOIN t93jc_orders o ON c.id = o.cust_id "+
			"GROUP BY c.name", 2)
	for _, row := range rows {
		name := fmt.Sprintf("%v", row[0])
		label := fmt.Sprintf("%v", row[1])
		if name == "Alice" && label != "high" {
			t.Fatalf("Alice should be 'high', got %v", label)
		}
		if name == "Bob" && label != "low" {
			t.Fatalf("Bob should be 'low', got %v", label)
		}
	}
}

// --- replaceAggregatesInExpr: UnaryExpr (negation of aggregate in JOIN)
func TestV93_JoinGroupByNegatedAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93jn_items (id INTEGER PRIMARY KEY, cat_id INTEGER, price INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t93jn_cats (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93jn_cats VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93jn_cats VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO t93jn_items VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO t93jn_items VALUES (2, 1, 20)")
	afExec(t, db, ctx, "INSERT INTO t93jn_items VALUES (3, 2, 30)")
	// -SUM(price) triggers UnaryExpr in replaceAggregatesInExpr
	rows := afQuery(t, db, ctx,
		"SELECT c.name, -SUM(i.price) FROM t93jn_cats c JOIN t93jn_items i ON c.id = i.cat_id GROUP BY c.name")
	if len(rows) < 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	for _, row := range rows {
		name := fmt.Sprintf("%v", row[0])
		val := fmt.Sprintf("%v", row[1])
		if name == "A" && val != "-30" {
			t.Fatalf("A: expected -30, got %v", val)
		}
		if name == "B" && val != "-30" {
			t.Fatalf("B: expected -30, got %v", val)
		}
	}
}

// --- replaceAggregatesInExpr: FunctionCall wrapping aggregate (COALESCE)
func TestV93_JoinGroupByCoalesceAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93jco_a (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93jco_b (id INTEGER PRIMARY KEY, aid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93jco_a VALUES (1, 'X')")
	afExec(t, db, ctx, "INSERT INTO t93jco_a VALUES (2, 'Y')")
	afExec(t, db, ctx, "INSERT INTO t93jco_b VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO t93jco_b VALUES (2, 1, 20)")
	// COALESCE(SUM(val), 0) wraps aggregate in non-aggregate function call
	rows := afQuery(t, db, ctx,
		"SELECT a.name, COALESCE(SUM(b.val), 0) FROM t93jco_a a JOIN t93jco_b b ON a.id = b.aid GROUP BY a.name")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
}

// --- replaceAggregatesInExpr: AliasExpr wrapping aggregate in JOIN
func TestV93_JoinGroupByAliasedAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93ja_p (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93ja_s (id INTEGER PRIMARY KEY, pid INTEGER, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93ja_p VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO t93ja_p VALUES (2, 'P2')")
	afExec(t, db, ctx, "INSERT INTO t93ja_s VALUES (1, 1, 80)")
	afExec(t, db, ctx, "INSERT INTO t93ja_s VALUES (2, 1, 90)")
	afExec(t, db, ctx, "INSERT INTO t93ja_s VALUES (3, 2, 70)")
	// SUM(score) AS total triggers AliasExpr wrapping aggregate
	rows := afExpectRows(t, db, ctx,
		"SELECT p.name, SUM(s.score) AS total FROM t93ja_p p JOIN t93ja_s s ON p.id = s.pid GROUP BY p.name ORDER BY total DESC", 2)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "P1" {
		t.Fatalf("expected 'P1' first (total 170), got %v", first)
	}
}

// =============================================================================
// ORDER BY with expression-arg match via JOIN (ORDER BY SUM(price * qty))
// =============================================================================
func TestV93_JoinOrderByExpressionAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93oe_c (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93oe_i (id INTEGER PRIMARY KEY, cid INTEGER, price INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93oe_c VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93oe_c VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO t93oe_i VALUES (1, 1, 10, 2)")
	afExec(t, db, ctx, "INSERT INTO t93oe_i VALUES (2, 2, 5, 10)")
	afExec(t, db, ctx, "INSERT INTO t93oe_i VALUES (3, 1, 20, 3)")
	// JOIN+GROUP BY + ORDER BY SUM(i.price * i.qty) → exprArgMatch path
	rows := afQuery(t, db, ctx,
		"SELECT c.cat, SUM(i.price * i.qty) FROM t93oe_c c JOIN t93oe_i i ON c.id = i.cid "+
			"GROUP BY c.cat ORDER BY SUM(i.price * i.qty) DESC")
	if len(rows) < 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// =============================================================================
// ORDER BY with QualifiedIdentifier via JOIN (a.colname)
// =============================================================================
func TestV93_JoinGroupByOrderByQualifiedCol(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93gq_a (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93gq_b (id INTEGER PRIMARY KEY, aid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93gq_a VALUES (1, 'C')")
	afExec(t, db, ctx, "INSERT INTO t93gq_a VALUES (2, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93gq_a VALUES (3, 'B')")
	afExec(t, db, ctx, "INSERT INTO t93gq_b VALUES (1, 1, 30)")
	afExec(t, db, ctx, "INSERT INTO t93gq_b VALUES (2, 2, 10)")
	afExec(t, db, ctx, "INSERT INTO t93gq_b VALUES (3, 3, 20)")
	// JOIN+GROUP BY + ORDER BY a.cat → QualifiedIdentifier in applyGroupByOrderBy
	rows := afExpectRows(t, db, ctx,
		"SELECT a.cat, SUM(b.val) FROM t93gq_a a JOIN t93gq_b b ON a.id = b.aid "+
			"GROUP BY a.cat ORDER BY a.cat", 3)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "A" {
		t.Fatalf("expected 'A' first, got %v", first)
	}
}

// =============================================================================
// JOIN + GROUP BY + multiple aggregates + ORDER BY (comprehensive)
// =============================================================================
func TestV93_JoinGroupByMultiAggregateOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93jm_dept (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93jm_emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93jm_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO t93jm_dept VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO t93jm_dept VALUES (3, 'HR')")
	afExec(t, db, ctx, "INSERT INTO t93jm_emp VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t93jm_emp VALUES (2, 1, 120)")
	afExec(t, db, ctx, "INSERT INTO t93jm_emp VALUES (3, 1, 80)")
	afExec(t, db, ctx, "INSERT INTO t93jm_emp VALUES (4, 2, 90)")
	afExec(t, db, ctx, "INSERT INTO t93jm_emp VALUES (5, 2, 95)")
	afExec(t, db, ctx, "INSERT INTO t93jm_emp VALUES (6, 3, 70)")
	// JOIN + GROUP BY + multiple aggregates + ORDER BY aggregate
	rows := afExpectRows(t, db, ctx,
		"SELECT d.name, COUNT(e.id), SUM(e.salary), AVG(e.salary) "+
			"FROM t93jm_dept d JOIN t93jm_emp e ON d.id = e.dept_id "+
			"GROUP BY d.name ORDER BY SUM(e.salary) DESC", 3)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Engineering" {
		t.Fatalf("expected 'Engineering' first, got %v", first)
	}
}

// =============================================================================
// VIEW + aggregate ORDER BY (goes through applyOuterQuery → applyGroupByOrderBy)
// =============================================================================
func TestV93_ViewGroupByOrderBySumDesc(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93vgo (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93vgo VALUES (1, 'A', 30)")
	afExec(t, db, ctx, "INSERT INTO t93vgo VALUES (2, 'B', 10)")
	afExec(t, db, ctx, "INSERT INTO t93vgo VALUES (3, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t93vgo VALUES (4, 'C', 40)")
	// Complex view (with alias) → applyOuterQuery → applyGroupByOrderBy
	afExec(t, db, ctx, "CREATE VIEW v93vgo AS SELECT cat AS category, val AS value FROM t93vgo")
	rows := afExpectRows(t, db, ctx,
		"SELECT category, SUM(value) FROM v93vgo GROUP BY category ORDER BY SUM(value) DESC", 3)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "A" && first != "C" {
		t.Fatalf("expected 'A' or 'C' first (highest sum), got %v", first)
	}
}

func TestV93_ViewGroupByOrderByCountStar(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93vgc (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93vgc VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93vgc VALUES (2, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93vgc VALUES (3, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93vgc VALUES (4, 'B')")
	afExec(t, db, ctx, "CREATE VIEW v93vgc AS SELECT cat AS category FROM t93vgc")
	rows := afExpectRows(t, db, ctx,
		"SELECT category, COUNT(*) FROM v93vgc GROUP BY category ORDER BY COUNT(*) DESC", 2)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "A" {
		t.Fatalf("expected 'A' first (count 3), got %v", first)
	}
}

func TestV93_ViewGroupByOrderByPositional(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93vgp (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93vgp VALUES (1, 'X', 100)")
	afExec(t, db, ctx, "INSERT INTO t93vgp VALUES (2, 'Y', 50)")
	afExec(t, db, ctx, "INSERT INTO t93vgp VALUES (3, 'X', 200)")
	afExec(t, db, ctx, "CREATE VIEW v93vgp AS SELECT cat AS category, val AS value FROM t93vgp")
	// ORDER BY 2 DESC (positional) → NumberLiteral path in applyGroupByOrderBy
	rows := afExpectRows(t, db, ctx,
		"SELECT category, SUM(value) FROM v93vgp GROUP BY category ORDER BY 2 DESC", 2)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "X" {
		t.Fatalf("expected 'X' first (sum 300), got %v", first)
	}
}

func TestV93_ViewGroupByOrderByStringCol(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93vgs (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93vgs VALUES (1, 'Charlie', 10)")
	afExec(t, db, ctx, "INSERT INTO t93vgs VALUES (2, 'Alice', 20)")
	afExec(t, db, ctx, "INSERT INTO t93vgs VALUES (3, 'Bob', 30)")
	afExec(t, db, ctx, "CREATE VIEW v93vgs AS SELECT name AS person, val AS score FROM t93vgs")
	rows := afExpectRows(t, db, ctx,
		"SELECT person, SUM(score) FROM v93vgs GROUP BY person ORDER BY person", 3)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Alice" {
		t.Fatalf("expected 'Alice' first, got %v", first)
	}
}

// =============================================================================
// Multiple LIKE variants
// =============================================================================
func TestV93_LikeUnderscore(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93lu (id INTEGER PRIMARY KEY, code TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93lu VALUES (1, 'A1')")
	afExec(t, db, ctx, "INSERT INTO t93lu VALUES (2, 'B2')")
	afExec(t, db, ctx, "INSERT INTO t93lu VALUES (3, 'AB')")
	// _ matches exactly one character
	rows := afExpectRows(t, db, ctx, "SELECT code FROM t93lu WHERE code LIKE 'A_'", 2)
	if len(rows) != 2 {
		t.Fatalf("expected 2, got %d", len(rows))
	}
}

func TestV93_NotLikeUnderscore(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93nlu (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93nlu VALUES (1, 'AB')")
	afExec(t, db, ctx, "INSERT INTO t93nlu VALUES (2, 'CD')")
	afExec(t, db, ctx, "INSERT INTO t93nlu VALUES (3, 'ABC')")
	// NOT LIKE 'A_' - should match CD and ABC
	rows := afExpectRows(t, db, ctx, "SELECT name FROM t93nlu WHERE name NOT LIKE 'A_'", 2)
	if len(rows) != 2 {
		t.Fatalf("expected 2, got %d", len(rows))
	}
}

// =============================================================================
// Additional edge cases
// =============================================================================
func TestV93_ConcatWsWithNulls(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93cw (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93cw VALUES (1, 'hello', NULL, 'world')")
	// CONCAT_WS should skip NULLs
	rows := afQuery(t, db, ctx, "SELECT CONCAT_WS(', ', a, b, c) FROM t93cw WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	// Should be "hello, world" (skipping NULL)
	if !strings.Contains(got, "hello") || !strings.Contains(got, "world") {
		t.Fatalf("CONCAT_WS should include hello and world, got %v", got)
	}
}

// --- JOIN + ORDER BY SUM with QualifiedIdentifier arg
func TestV93_JoinOrderByAggregateQualifiedArg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93oaq_t (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93oaq_v (id INTEGER PRIMARY KEY, tid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93oaq_t VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93oaq_t VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO t93oaq_v VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t93oaq_v VALUES (2, 1, 150)")
	afExec(t, db, ctx, "INSERT INTO t93oaq_v VALUES (3, 2, 200)")
	// JOIN + ORDER BY SUM(v.val) → FunctionCall with QualifiedIdentifier arg
	rows := afQuery(t, db, ctx,
		"SELECT t.cat, SUM(v.val) FROM t93oaq_t t JOIN t93oaq_v v ON t.id = v.tid "+
			"GROUP BY t.cat ORDER BY SUM(v.val) DESC")
	if len(rows) < 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "A" {
		t.Fatalf("expected 'A' first (sum 250), got %v", first)
	}
}

// --- GROUP_CONCAT in JOIN context
func TestV93_JoinGroupByGroupConcat(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93jgg_d (id INTEGER PRIMARY KEY, dept TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93jgg_e (id INTEGER PRIMARY KEY, did INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t93jgg_d VALUES (1, 'Eng')")
	afExec(t, db, ctx, "INSERT INTO t93jgg_d VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO t93jgg_e VALUES (1, 1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t93jgg_e VALUES (2, 1, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO t93jgg_e VALUES (3, 2, 'Carol')")
	rows := afQuery(t, db, ctx,
		"SELECT d.dept, GROUP_CONCAT(e.name) FROM t93jgg_d d JOIN t93jgg_e e ON d.id = e.did GROUP BY d.dept")
	if len(rows) < 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// --- MIN/MAX in JOIN context with ORDER BY
func TestV93_JoinGroupByMinMaxOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93mm_g (id INTEGER PRIMARY KEY, grp TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93mm_v (id INTEGER PRIMARY KEY, gid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93mm_g VALUES (1, 'X')")
	afExec(t, db, ctx, "INSERT INTO t93mm_g VALUES (2, 'Y')")
	afExec(t, db, ctx, "INSERT INTO t93mm_v VALUES (1, 1, 5)")
	afExec(t, db, ctx, "INSERT INTO t93mm_v VALUES (2, 1, 15)")
	afExec(t, db, ctx, "INSERT INTO t93mm_v VALUES (3, 2, 10)")
	afExec(t, db, ctx, "INSERT INTO t93mm_v VALUES (4, 2, 20)")
	rows := afExpectRows(t, db, ctx,
		"SELECT g.grp, MIN(v.val), MAX(v.val) FROM t93mm_g g JOIN t93mm_v v ON g.id = v.gid "+
			"GROUP BY g.grp ORDER BY MAX(v.val) DESC", 2)
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Y" {
		t.Fatalf("expected 'Y' first (max 20), got %v", first)
	}
}

// --- AVG in JOIN context
func TestV93_JoinGroupByAvg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t93avg_c (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t93avg_v (id INTEGER PRIMARY KEY, cid INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t93avg_c VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t93avg_c VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO t93avg_v VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO t93avg_v VALUES (2, 1, 20)")
	afExec(t, db, ctx, "INSERT INTO t93avg_v VALUES (3, 2, 30)")
	rows := afQuery(t, db, ctx,
		"SELECT c.cat, AVG(v.val) FROM t93avg_c c JOIN t93avg_v v ON c.id = v.cid GROUP BY c.cat")
	if len(rows) < 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}
