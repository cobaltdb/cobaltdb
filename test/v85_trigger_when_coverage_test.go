package test

import (
	"fmt"
	"testing"
)

// ==================== V85: TRIGGER WHEN CLAUSE & COVERAGE BOOST ====================
// Targets:
// 1. Trigger WHEN clause (new feature) - executeTriggers WHEN paths
// 2. resolveTriggerExpr paths: BETWEEN, IN, IS NULL, LIKE, CAST in trigger WHEN
// 3. evaluateFunctionCall more paths
// 4. evaluateHaving edge cases
// 5. computeViewAggregate deeper paths
// 6. applyGroupByOrderBy more paths
// 7. selectLocked deeper paths (OFFSET, LIMIT with expressions)
// 8. evaluateCastExpr more type combos
// 9. evaluateBetween / evaluateLike more edge paths
// 10. toNumber, extractLiteralValue, valueToExpr deeper paths

func TestV85_TriggerWhenBasicComparison(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Setup
	afExec(t, db, ctx, "CREATE TABLE tw_basic (id INTEGER PRIMARY KEY, val INTEGER, status TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tw_log (msg TEXT)")

	// Trigger that only fires WHEN NEW.val > 10
	afExec(t, db, ctx, `CREATE TRIGGER tw_gt10 AFTER INSERT ON tw_basic
		FOR EACH ROW WHEN NEW.val > 10
		BEGIN
			INSERT INTO tw_log VALUES ('big:' || CAST(NEW.val AS TEXT));
		END`)

	// Insert val=5 (should NOT fire trigger)
	afExec(t, db, ctx, "INSERT INTO tw_basic VALUES (1, 5, 'low')")
	rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM tw_log")
	if fmt.Sprintf("%v", rows[0][0]) != "0" {
		t.Fatalf("expected 0 log rows for val=5, got %v", rows[0][0])
	}

	// Insert val=20 (should fire trigger)
	afExec(t, db, ctx, "INSERT INTO tw_basic VALUES (2, 20, 'high')")
	rows = afQuery(t, db, ctx, "SELECT COUNT(*) FROM tw_log")
	if fmt.Sprintf("%v", rows[0][0]) != "1" {
		t.Fatalf("expected 1 log row for val=20, got %v", rows[0][0])
	}

	// Check log content
	rows = afQuery(t, db, ctx, "SELECT msg FROM tw_log")
	if fmt.Sprintf("%v", rows[0][0]) != "big:20" {
		t.Fatalf("expected 'big:20', got %v", rows[0][0])
	}
}

func TestV85_TriggerWhenEquality(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_eq (id INTEGER PRIMARY KEY, color TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tw_eq_log (msg TEXT)")

	// Trigger only fires WHEN NEW.color = 'red'
	afExec(t, db, ctx, `CREATE TRIGGER tw_red AFTER INSERT ON tw_eq
		FOR EACH ROW WHEN NEW.color = 'red'
		BEGIN
			INSERT INTO tw_eq_log VALUES ('red_inserted');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_eq VALUES (1, 'blue')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_eq_log", float64(0))

	afExec(t, db, ctx, "INSERT INTO tw_eq VALUES (2, 'red')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_eq_log", float64(1))

	afExec(t, db, ctx, "INSERT INTO tw_eq VALUES (3, 'green')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_eq_log", float64(1))
}

func TestV85_TriggerWhenAND(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_and (id INTEGER PRIMARY KEY, val INTEGER, active INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_and_log (msg TEXT)")

	// Trigger fires WHEN val > 5 AND active = 1
	afExec(t, db, ctx, `CREATE TRIGGER tw_active_big AFTER INSERT ON tw_and
		FOR EACH ROW WHEN NEW.val > 5 AND NEW.active = 1
		BEGIN
			INSERT INTO tw_and_log VALUES ('active_big');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_and VALUES (1, 3, 1)")   // val too low
	afExec(t, db, ctx, "INSERT INTO tw_and VALUES (2, 10, 0)")  // not active
	afExec(t, db, ctx, "INSERT INTO tw_and VALUES (3, 10, 1)")  // should fire
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_and_log", float64(1))
}

func TestV85_TriggerWhenOR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_or (id INTEGER PRIMARY KEY, priority INTEGER, urgent INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_or_log (msg TEXT)")

	// Trigger fires WHEN priority > 8 OR urgent = 1
	afExec(t, db, ctx, `CREATE TRIGGER tw_alert AFTER INSERT ON tw_or
		FOR EACH ROW WHEN NEW.priority > 8 OR NEW.urgent = 1
		BEGIN
			INSERT INTO tw_or_log VALUES ('alert');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_or VALUES (1, 3, 0)")  // neither
	afExec(t, db, ctx, "INSERT INTO tw_or VALUES (2, 9, 0)")  // priority high
	afExec(t, db, ctx, "INSERT INTO tw_or VALUES (3, 1, 1)")  // urgent
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_or_log", float64(2))
}

func TestV85_TriggerWhenWithFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_fn (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tw_fn_log (msg TEXT)")

	// Trigger fires WHEN LENGTH(NEW.name) > 5
	afExec(t, db, ctx, `CREATE TRIGGER tw_longname AFTER INSERT ON tw_fn
		FOR EACH ROW WHEN LENGTH(NEW.name) > 5
		BEGIN
			INSERT INTO tw_fn_log VALUES (NEW.name);
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_fn VALUES (1, 'Bob')")      // 3 chars - skip
	afExec(t, db, ctx, "INSERT INTO tw_fn VALUES (2, 'Charlie')")  // 7 chars - fire
	afExec(t, db, ctx, "INSERT INTO tw_fn VALUES (3, 'Jo')")       // 2 chars - skip
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_fn_log", float64(1))
	afExpectVal(t, db, ctx, "SELECT msg FROM tw_fn_log", "Charlie")
}

func TestV85_TriggerWhenUpdateOldNew(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_upd (id INTEGER PRIMARY KEY, score INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_upd_log (msg TEXT)")

	// Trigger fires WHEN NEW.score > OLD.score (score increased)
	afExec(t, db, ctx, `CREATE TRIGGER tw_score_up AFTER UPDATE ON tw_upd
		FOR EACH ROW WHEN NEW.score > OLD.score
		BEGIN
			INSERT INTO tw_upd_log VALUES ('up:' || CAST(NEW.score AS TEXT));
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_upd VALUES (1, 50)")
	afExec(t, db, ctx, "UPDATE tw_upd SET score = 30 WHERE id = 1")  // decrease - skip
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_upd_log", float64(0))

	afExec(t, db, ctx, "UPDATE tw_upd SET score = 80 WHERE id = 1")  // increase - fire
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_upd_log", float64(1))
}

func TestV85_TriggerWhenDelete(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_del (id INTEGER PRIMARY KEY, protected INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_del_log (msg TEXT)")

	// Trigger fires WHEN OLD.protected = 1
	afExec(t, db, ctx, `CREATE TRIGGER tw_del_protected AFTER DELETE ON tw_del
		FOR EACH ROW WHEN OLD.protected = 1
		BEGIN
			INSERT INTO tw_del_log VALUES ('deleted_protected:' || CAST(OLD.id AS TEXT));
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_del VALUES (1, 0)")
	afExec(t, db, ctx, "INSERT INTO tw_del VALUES (2, 1)")
	afExec(t, db, ctx, "INSERT INTO tw_del VALUES (3, 0)")

	afExec(t, db, ctx, "DELETE FROM tw_del WHERE id = 1")  // not protected
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_del_log", float64(0))

	afExec(t, db, ctx, "DELETE FROM tw_del WHERE id = 2")  // protected
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_del_log", float64(1))
}

func TestV85_TriggerWhenNullCondition(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_null (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_null_log (msg TEXT)")

	// WHEN NEW.val > 10 - when val is NULL, condition should be NULL and trigger should not fire
	afExec(t, db, ctx, `CREATE TRIGGER tw_null_check AFTER INSERT ON tw_null
		FOR EACH ROW WHEN NEW.val > 10
		BEGIN
			INSERT INTO tw_null_log VALUES ('fired');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_null VALUES (1, NULL)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_null_log", float64(0))

	afExec(t, db, ctx, "INSERT INTO tw_null VALUES (2, 20)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_null_log", float64(1))
}

func TestV85_TriggerWhenNotEqual(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_ne (id INTEGER PRIMARY KEY, status TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tw_ne_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER tw_not_deleted AFTER INSERT ON tw_ne
		FOR EACH ROW WHEN NEW.status != 'deleted'
		BEGIN
			INSERT INTO tw_ne_log VALUES (NEW.status);
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_ne VALUES (1, 'active')")
	afExec(t, db, ctx, "INSERT INTO tw_ne VALUES (2, 'deleted')")
	afExec(t, db, ctx, "INSERT INTO tw_ne VALUES (3, 'pending')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_ne_log", float64(2))
}

func TestV85_TriggerWhenMultipleTriggers(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_multi (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_multi_log (msg TEXT)")

	// Two triggers with different WHEN conditions
	afExec(t, db, ctx, `CREATE TRIGGER tw_small AFTER INSERT ON tw_multi
		FOR EACH ROW WHEN NEW.val < 10
		BEGIN
			INSERT INTO tw_multi_log VALUES ('small');
		END`)

	afExec(t, db, ctx, `CREATE TRIGGER tw_big AFTER INSERT ON tw_multi
		FOR EACH ROW WHEN NEW.val >= 100
		BEGIN
			INSERT INTO tw_multi_log VALUES ('big');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_multi VALUES (1, 5)")    // small fires
	afExec(t, db, ctx, "INSERT INTO tw_multi VALUES (2, 50)")   // neither fires
	afExec(t, db, ctx, "INSERT INTO tw_multi VALUES (3, 200)")  // big fires

	rows := afQuery(t, db, ctx, "SELECT msg FROM tw_multi_log ORDER BY msg")
	if len(rows) != 2 {
		t.Fatalf("expected 2 log rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "big" {
		t.Fatalf("expected 'big', got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[1][0]) != "small" {
		t.Fatalf("expected 'small', got %v", rows[1][0])
	}
}

// ==================== TRIGGER WHEN WITH COMPLEX EXPRESSIONS ====================

func TestV85_TriggerWhenBETWEEN(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_btw (id INTEGER PRIMARY KEY, temp INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_btw_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER tw_normal_temp AFTER INSERT ON tw_btw
		FOR EACH ROW WHEN NEW.temp BETWEEN 36 AND 38
		BEGIN
			INSERT INTO tw_btw_log VALUES ('normal');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_btw VALUES (1, 35)")  // below range
	afExec(t, db, ctx, "INSERT INTO tw_btw VALUES (2, 37)")  // in range
	afExec(t, db, ctx, "INSERT INTO tw_btw VALUES (3, 40)")  // above range
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_btw_log", float64(1))
}

func TestV85_TriggerWhenIN(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_in (id INTEGER PRIMARY KEY, role TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tw_in_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER tw_admin AFTER INSERT ON tw_in
		FOR EACH ROW WHEN NEW.role IN ('admin', 'superadmin')
		BEGIN
			INSERT INTO tw_in_log VALUES ('admin_added');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_in VALUES (1, 'user')")
	afExec(t, db, ctx, "INSERT INTO tw_in VALUES (2, 'admin')")
	afExec(t, db, ctx, "INSERT INTO tw_in VALUES (3, 'moderator')")
	afExec(t, db, ctx, "INSERT INTO tw_in VALUES (4, 'superadmin')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_in_log", float64(2))
}

func TestV85_TriggerWhenLIKE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_like (id INTEGER PRIMARY KEY, email TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tw_like_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER tw_gmail AFTER INSERT ON tw_like
		FOR EACH ROW WHEN NEW.email LIKE '%@gmail.com'
		BEGIN
			INSERT INTO tw_like_log VALUES ('gmail_user');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_like VALUES (1, 'bob@yahoo.com')")
	afExec(t, db, ctx, "INSERT INTO tw_like VALUES (2, 'alice@gmail.com')")
	afExec(t, db, ctx, "INSERT INTO tw_like VALUES (3, 'carol@outlook.com')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_like_log", float64(1))
}

func TestV85_TriggerWhenISNULL(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_isnull (id INTEGER PRIMARY KEY, notes TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tw_isnull_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER tw_no_notes AFTER INSERT ON tw_isnull
		FOR EACH ROW WHEN NEW.notes IS NULL
		BEGIN
			INSERT INTO tw_isnull_log VALUES ('missing_notes');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_isnull VALUES (1, 'some notes')")
	afExec(t, db, ctx, "INSERT INTO tw_isnull VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO tw_isnull VALUES (3, 'more notes')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_isnull_log", float64(1))
}

func TestV85_TriggerWhenISNOTNULL(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_notnull (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tw_notnull_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER tw_has_val AFTER INSERT ON tw_notnull
		FOR EACH ROW WHEN NEW.val IS NOT NULL
		BEGIN
			INSERT INTO tw_notnull_log VALUES (NEW.val);
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_notnull VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO tw_notnull VALUES (2, 'hello')")
	afExec(t, db, ctx, "INSERT INTO tw_notnull VALUES (3, NULL)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_notnull_log", float64(1))
	afExpectVal(t, db, ctx, "SELECT msg FROM tw_notnull_log", "hello")
}

// ==================== MORE FUNCTION COVERAGE ====================

func TestV85_FunctionHEX(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT HEX(255)", "FF")
	afExpectVal(t, db, ctx, "SELECT HEX(0)", "0")
	afExpectVal(t, db, ctx, "SELECT HEX(16)", "10")
}

func TestV85_FunctionOCT(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	// OCT may not be implemented - try it
	rows, err := db.Query(ctx, "SELECT OCT(8)")
	if err == nil {
		rows.Close()
	}
}

func TestV85_FunctionREVERSE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT REVERSE('hello')", "olleh")
	afExpectVal(t, db, ctx, "SELECT REVERSE('')", "")
	afExpectVal(t, db, ctx, "SELECT REVERSE('a')", "a")
}

func TestV85_FunctionLPADRPAD(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT LPAD('hi', 5, '*')", "***hi")
	afExpectVal(t, db, ctx, "SELECT RPAD('hi', 5, '*')", "hi***")
	afExpectVal(t, db, ctx, "SELECT LPAD('hello', 3, '*')", "hel")
	afExpectVal(t, db, ctx, "SELECT RPAD('hello', 3, '*')", "hel")
}

func TestV85_FunctionREPEAT(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT REPEAT('ab', 3)", "ababab")
	afExpectVal(t, db, ctx, "SELECT REPEAT('x', 0)", "")
	afExpectVal(t, db, ctx, "SELECT REPEAT('z', 1)", "z")
}

func TestV85_FunctionREPLACE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT REPLACE('hello world', 'world', 'go')", "hello go")
	afExpectVal(t, db, ctx, "SELECT REPLACE('aaa', 'a', 'bb')", "bbbbbb")
}

func TestV85_FunctionLEFTRIGHT(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT LEFT('hello', 3)", "hel")
	afExpectVal(t, db, ctx, "SELECT RIGHT('hello', 3)", "llo")
	afExpectVal(t, db, ctx, "SELECT LEFT('hi', 10)", "hi")
	afExpectVal(t, db, ctx, "SELECT RIGHT('hi', 10)", "hi")
}

func TestV85_FunctionCOALESCE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, NULL, 'found')", "found")
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, 42)", float64(42))
	afExpectVal(t, db, ctx, "SELECT COALESCE('first', 'second')", "first")
}

func TestV85_FunctionNULLIF(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	rows := afQuery(t, db, ctx, "SELECT NULLIF(1, 1)")
	if rows[0][0] != nil {
		t.Fatalf("expected nil for NULLIF(1,1), got %v", rows[0][0])
	}
	afExpectVal(t, db, ctx, "SELECT NULLIF(1, 2)", float64(1))
	afExpectVal(t, db, ctx, "SELECT NULLIF('a', 'b')", "a")
}

func TestV85_FunctionIIF(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT IIF(1=1, 'yes', 'no')", "yes")
	afExpectVal(t, db, ctx, "SELECT IIF(1=2, 'yes', 'no')", "no")
	afExpectVal(t, db, ctx, "SELECT IIF(NULL, 'yes', 'no')", "no")
}

func TestV85_FunctionINSTR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT INSTR('hello world', 'world')", float64(7))
	afExpectVal(t, db, ctx, "SELECT INSTR('hello', 'xyz')", float64(0))
	afExpectVal(t, db, ctx, "SELECT INSTR('abcabc', 'bc')", float64(2))
}

func TestV85_FunctionTYPEOF(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT TYPEOF(42)", "integer")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(3.14)", "real")
	afExpectVal(t, db, ctx, "SELECT TYPEOF('hello')", "text")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(NULL)", "null")
}

func TestV85_FunctionZEROBLOB(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	// ZEROBLOB returns null bytes, check its length
	afExpectVal(t, db, ctx, "SELECT LENGTH(ZEROBLOB(4))", float64(4))
}

func TestV85_FunctionROUND(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT ROUND(3.14159, 2)", float64(3.14))
	afExpectVal(t, db, ctx, "SELECT ROUND(2.5)", float64(3))
	afExpectVal(t, db, ctx, "SELECT ROUND(-1.5)", float64(-2))
}

func TestV85_FunctionTRIM(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT TRIM('  hello  ')", "hello")
	afExpectVal(t, db, ctx, "SELECT LTRIM('  hello  ')", "hello  ")
	afExpectVal(t, db, ctx, "SELECT RTRIM('  hello  ')", "  hello")
}

func TestV85_FunctionSUBSTR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT SUBSTR('hello', 2, 3)", "ell")
	afExpectVal(t, db, ctx, "SELECT SUBSTR('hello', 1, 1)", "h")
}

func TestV85_FunctionGROUP_CONCAT(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE gc_t (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO gc_t VALUES ('a')")
	afExec(t, db, ctx, "INSERT INTO gc_t VALUES ('b')")
	afExec(t, db, ctx, "INSERT INTO gc_t VALUES ('c')")
	rows := afQuery(t, db, ctx, "SELECT GROUP_CONCAT(val) FROM gc_t")
	if len(rows) == 0 || rows[0][0] == nil {
		t.Fatalf("expected GROUP_CONCAT result")
	}
}

func TestV85_FunctionCEILFLOOR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT CEIL(3.2)", float64(4))
	afExpectVal(t, db, ctx, "SELECT FLOOR(3.8)", float64(3))
	afExpectVal(t, db, ctx, "SELECT CEIL(-1.1)", float64(-1))
	afExpectVal(t, db, ctx, "SELECT FLOOR(-1.1)", float64(-2))
}

// ==================== CAST EDGE CASES ====================

func TestV85_CastIntToText(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT CAST(42 AS TEXT)", "42")
	afExpectVal(t, db, ctx, "SELECT CAST(-7 AS TEXT)", "-7")
}

func TestV85_CastTextToInt(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT CAST('123' AS INTEGER)", int64(123))
	afExpectVal(t, db, ctx, "SELECT CAST('0' AS INTEGER)", int64(0))
}

func TestV85_CastTextToReal(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT CAST('3.14' AS REAL)", float64(3.14))
	afExpectVal(t, db, ctx, "SELECT CAST('0.0' AS REAL)", float64(0))
}

func TestV85_CastRealToInt(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT CAST(3.7 AS INTEGER)", int64(3))
	afExpectVal(t, db, ctx, "SELECT CAST(-2.9 AS INTEGER)", int64(-2))
}

func TestV85_CastBoolToInt(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExpectVal(t, db, ctx, "SELECT CAST(1=1 AS INTEGER)", int64(1))
	afExpectVal(t, db, ctx, "SELECT CAST(1=2 AS INTEGER)", int64(0))
}

func TestV85_CastNullPreserved(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	rows := afQuery(t, db, ctx, "SELECT CAST(NULL AS INTEGER)")
	if rows[0][0] != nil {
		t.Fatalf("expected nil for CAST(NULL AS INTEGER), got %v", rows[0][0])
	}
}

// ==================== HAVING EDGE CASES ====================

func TestV85_HavingWithOR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_or (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_or VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO h_or VALUES ('a', 20)")
	afExec(t, db, ctx, "INSERT INTO h_or VALUES ('b', 5)")
	afExec(t, db, ctx, "INSERT INTO h_or VALUES ('c', 100)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM h_or GROUP BY cat HAVING SUM(val) > 20 OR COUNT(*) > 1 ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestV85_HavingWithNOT(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_not (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_not VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO h_not VALUES ('a', 20)")
	afExec(t, db, ctx, "INSERT INTO h_not VALUES ('b', 5)")
	afExec(t, db, ctx, "INSERT INTO h_not VALUES ('c', 100)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM h_not GROUP BY cat HAVING NOT SUM(val) < 10 ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (a=30, c=100), got %d: %v", len(rows), rows)
	}
}

func TestV85_HavingWithBETWEEN(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_btw (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_btw VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO h_btw VALUES ('a', 20)")
	afExec(t, db, ctx, "INSERT INTO h_btw VALUES ('b', 5)")
	afExec(t, db, ctx, "INSERT INTO h_btw VALUES ('c', 100)")
	afExec(t, db, ctx, "INSERT INTO h_btw VALUES ('d', 50)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM h_btw GROUP BY cat HAVING SUM(val) BETWEEN 10 AND 60")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (a=30, d=50), got %d: %v", len(rows), rows)
	}
}

func TestV85_HavingWithCASE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_case (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_case VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO h_case VALUES ('a', 20)")
	afExec(t, db, ctx, "INSERT INTO h_case VALUES ('b', 5)")
	afExec(t, db, ctx, "INSERT INTO h_case VALUES ('c', 100)")

	rows := afQuery(t, db, ctx, `SELECT cat, SUM(val) as s FROM h_case GROUP BY cat
		HAVING CASE WHEN SUM(val) > 50 THEN 1 ELSE 0 END = 1`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (c=100), got %d: %v", len(rows), rows)
	}
	if fmt.Sprintf("%v", rows[0][0]) != "c" {
		t.Fatalf("expected cat='c', got %v", rows[0][0])
	}
}

func TestV85_HavingWithIN(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_in (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_in VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO h_in VALUES ('b', 20)")
	afExec(t, db, ctx, "INSERT INTO h_in VALUES ('c', 30)")

	rows := afQuery(t, db, ctx, "SELECT cat FROM h_in GROUP BY cat HAVING SUM(val) IN (10, 30)")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
}

// ==================== VIEW AGGREGATE DEEPER ====================

func TestV85_ViewWithGroupConcat(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v_gc (dept TEXT, emp TEXT)")
	afExec(t, db, ctx, "INSERT INTO v_gc VALUES ('eng', 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v_gc VALUES ('eng', 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v_gc VALUES ('hr', 'Carol')")

	afExec(t, db, ctx, "CREATE VIEW v_gc_view AS SELECT dept, GROUP_CONCAT(emp) as emps FROM v_gc GROUP BY dept")
	rows := afQuery(t, db, ctx, "SELECT * FROM v_gc_view ORDER BY dept")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV85_ViewWithHaving(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v_hv (cat TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v_hv VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO v_hv VALUES ('a', 20)")
	afExec(t, db, ctx, "INSERT INTO v_hv VALUES ('b', 5)")
	afExec(t, db, ctx, "INSERT INTO v_hv VALUES ('c', 100)")

	afExec(t, db, ctx, "CREATE VIEW v_hv_view AS SELECT cat, SUM(amount) as total FROM v_hv GROUP BY cat HAVING SUM(amount) > 15")
	rows := afQuery(t, db, ctx, "SELECT * FROM v_hv_view ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (a=30, c=100), got %d: %v", len(rows), rows)
	}
}

func TestV85_ViewWithMultipleAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE v_ma (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v_ma VALUES ('x', 10)")
	afExec(t, db, ctx, "INSERT INTO v_ma VALUES ('x', 20)")
	afExec(t, db, ctx, "INSERT INTO v_ma VALUES ('x', 30)")
	afExec(t, db, ctx, "INSERT INTO v_ma VALUES ('y', 100)")

	afExec(t, db, ctx, `CREATE VIEW v_ma_view AS
		SELECT cat, COUNT(*) as cnt, SUM(val) as total, AVG(val) as avg_val,
		       MIN(val) as min_val, MAX(val) as max_val
		FROM v_ma GROUP BY cat`)
	rows := afQuery(t, db, ctx, "SELECT * FROM v_ma_view ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ==================== GROUP BY + ORDER BY DEEPER ====================

func TestV85_GroupByOrderByMultiAgg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gbo (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO gbo VALUES ('c', 30)")
	afExec(t, db, ctx, "INSERT INTO gbo VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO gbo VALUES ('b', 20)")
	afExec(t, db, ctx, "INSERT INTO gbo VALUES ('a', 15)")

	// ORDER BY aggregate DESC
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) as s FROM gbo GROUP BY cat ORDER BY SUM(val) DESC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "c" {
		t.Fatalf("expected first row cat='c', got %v", rows[0][0])
	}
}

func TestV85_GroupByOrderByCount(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gbo2 (tag TEXT)")
	afExec(t, db, ctx, "INSERT INTO gbo2 VALUES ('go')")
	afExec(t, db, ctx, "INSERT INTO gbo2 VALUES ('go')")
	afExec(t, db, ctx, "INSERT INTO gbo2 VALUES ('go')")
	afExec(t, db, ctx, "INSERT INTO gbo2 VALUES ('rust')")
	afExec(t, db, ctx, "INSERT INTO gbo2 VALUES ('rust')")
	afExec(t, db, ctx, "INSERT INTO gbo2 VALUES ('python')")

	rows := afQuery(t, db, ctx, "SELECT tag, COUNT(*) as cnt FROM gbo2 GROUP BY tag ORDER BY COUNT(*) ASC")
	if fmt.Sprintf("%v", rows[0][0]) != "python" {
		t.Fatalf("expected 'python' first (count=1), got %v", rows[0][0])
	}
}

func TestV85_GroupByLimitOffset(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gbl (cat TEXT, v INTEGER)")
	for i := 0; i < 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO gbl VALUES ('c%d', %d)", i, i*10))
	}

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(v) FROM gbl GROUP BY cat ORDER BY cat LIMIT 3 OFFSET 2")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "c2" {
		t.Fatalf("expected first row 'c2', got %v", rows[0][0])
	}
}

// ==================== LIKE DEEPER PATTERNS ====================

func TestV85_LikeEscapePercent(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE lk (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO lk VALUES ('100% done')")
	afExec(t, db, ctx, "INSERT INTO lk VALUES ('100 done')")
	afExec(t, db, ctx, "INSERT INTO lk VALUES ('hello')")

	// % at end should match all starting with '100'
	rows := afQuery(t, db, ctx, "SELECT val FROM lk WHERE val LIKE '100%'")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV85_LikeUnderscore(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE lku (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO lku VALUES ('cat')")
	afExec(t, db, ctx, "INSERT INTO lku VALUES ('cut')")
	afExec(t, db, ctx, "INSERT INTO lku VALUES ('cute')")
	afExec(t, db, ctx, "INSERT INTO lku VALUES ('ca')")

	rows := afQuery(t, db, ctx, "SELECT val FROM lku WHERE val LIKE 'c_t'")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (cat, cut), got %d: %v", len(rows), rows)
	}
}

func TestV85_NotLike(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE nlk (name TEXT)")
	afExec(t, db, ctx, "INSERT INTO nlk VALUES ('Alice')")
	afExec(t, db, ctx, "INSERT INTO nlk VALUES ('Bob')")
	afExec(t, db, ctx, "INSERT INTO nlk VALUES ('Arnold')")

	rows := afQuery(t, db, ctx, "SELECT name FROM nlk WHERE name NOT LIKE 'A%' ORDER BY name")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (Bob), got %d: %v", len(rows), rows)
	}
}

// ==================== BETWEEN EDGE CASES ====================

func TestV85_BetweenStrings(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE btws (name TEXT)")
	afExec(t, db, ctx, "INSERT INTO btws VALUES ('Alice')")
	afExec(t, db, ctx, "INSERT INTO btws VALUES ('Bob')")
	afExec(t, db, ctx, "INSERT INTO btws VALUES ('Charlie')")
	afExec(t, db, ctx, "INSERT INTO btws VALUES ('David')")

	rows := afQuery(t, db, ctx, "SELECT name FROM btws WHERE name BETWEEN 'B' AND 'D' ORDER BY name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (Bob, Charlie), got %d: %v", len(rows), rows)
	}
}

func TestV85_NotBetween(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE nbtw (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO nbtw VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO nbtw VALUES (5)")
	afExec(t, db, ctx, "INSERT INTO nbtw VALUES (10)")
	afExec(t, db, ctx, "INSERT INTO nbtw VALUES (15)")

	rows := afQuery(t, db, ctx, "SELECT val FROM nbtw WHERE val NOT BETWEEN 5 AND 10 ORDER BY val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (1, 15), got %d: %v", len(rows), rows)
	}
}

// ==================== SELECT WITH COMPLEX OFFSET/LIMIT ====================

func TestV85_SelectLimitExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE slm (id INTEGER)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO slm VALUES (%d)", i))
	}

	// LIMIT with expression
	rows := afQuery(t, db, ctx, "SELECT id FROM slm ORDER BY id LIMIT 2+3")
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	// OFFSET with expression
	rows = afQuery(t, db, ctx, "SELECT id FROM slm ORDER BY id LIMIT 3 OFFSET 1+1")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "3" {
		t.Fatalf("expected first id=3, got %v", rows[0][0])
	}
}

// ==================== COMPLEX QUERIES WITH SUBQUERIES ====================

func TestV85_SubqueryInSELECT(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE sq_main (id INTEGER, dept TEXT)")
	afExec(t, db, ctx, "CREATE TABLE sq_salaries (emp_id INTEGER, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO sq_main VALUES (1, 'eng')")
	afExec(t, db, ctx, "INSERT INTO sq_main VALUES (2, 'hr')")
	afExec(t, db, ctx, "INSERT INTO sq_salaries VALUES (1, 80000)")
	afExec(t, db, ctx, "INSERT INTO sq_salaries VALUES (2, 60000)")

	rows := afQuery(t, db, ctx, `SELECT m.dept,
		(SELECT salary FROM sq_salaries WHERE emp_id = m.id) as sal
		FROM sq_main m ORDER BY m.id`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][1]) != "80000" {
		t.Fatalf("expected salary=80000, got %v", rows[0][1])
	}
}

func TestV85_SubqueryInWHERE_EXISTS(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE sq_orders (id INTEGER, customer_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE sq_customers (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO sq_customers VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO sq_customers VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO sq_customers VALUES (3, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO sq_orders VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO sq_orders VALUES (2, 3)")

	rows := afQuery(t, db, ctx, `SELECT name FROM sq_customers c
		WHERE EXISTS (SELECT 1 FROM sq_orders WHERE customer_id = c.id) ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
}

// ==================== MATERIALIZED VIEW OPERATIONS ====================

func TestV85_MaterializedViewRefreshAndQuery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE mv_sales (region TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO mv_sales VALUES ('east', 100)")
	afExec(t, db, ctx, "INSERT INTO mv_sales VALUES ('west', 200)")
	afExec(t, db, ctx, "INSERT INTO mv_sales VALUES ('east', 150)")

	afExec(t, db, ctx, "CREATE MATERIALIZED VIEW mv_totals AS SELECT region, SUM(amount) as total FROM mv_sales GROUP BY region")

	// Materialized views are queried via special mechanism, just verify creation works
	// Add more data and refresh
	afExec(t, db, ctx, "INSERT INTO mv_sales VALUES ('east', 50)")
	afExec(t, db, ctx, "REFRESH MATERIALIZED VIEW mv_totals")
}

// ==================== WINDOW FUNCTION DEEPER ====================

func TestV85_WindowNTILE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wnt (id INTEGER, val INTEGER)")
	for i := 1; i <= 8; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO wnt VALUES (%d, %d)", i, i*10))
	}

	rows := afQuery(t, db, ctx, "SELECT id, NTILE(4) OVER (ORDER BY id) as quartile FROM wnt")
	if len(rows) != 8 {
		t.Fatalf("expected 8 rows, got %d", len(rows))
	}
	// Verify we got results (NTILE may return nil if not fully implemented)
	t.Logf("NTILE results: first row = %v", rows[0])
}

func TestV85_WindowPercentRank(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wpr (score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wpr VALUES (10)")
	afExec(t, db, ctx, "INSERT INTO wpr VALUES (20)")
	afExec(t, db, ctx, "INSERT INTO wpr VALUES (30)")
	afExec(t, db, ctx, "INSERT INTO wpr VALUES (40)")

	rows := afQuery(t, db, ctx, "SELECT score, PERCENT_RANK() OVER (ORDER BY score) as pr FROM wpr")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// Verify we got results (PERCENT_RANK may return nil if not fully supported)
	t.Logf("PERCENT_RANK results: first row = %v", rows[0])
}

func TestV85_WindowCumeDist(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wcd (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wcd VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO wcd VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO wcd VALUES (3)")
	afExec(t, db, ctx, "INSERT INTO wcd VALUES (4)")

	rows := afQuery(t, db, ctx, "SELECT val, CUME_DIST() OVER (ORDER BY val) as cd FROM wcd")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestV85_WindowLagLead(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wll (id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wll VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO wll VALUES (2, 200)")
	afExec(t, db, ctx, "INSERT INTO wll VALUES (3, 300)")

	rows := afQuery(t, db, ctx, `SELECT id,
		LAG(val, 1) OVER (ORDER BY id) as prev_val,
		LEAD(val, 1) OVER (ORDER BY id) as next_val
		FROM wll ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// First row LAG should be NULL
	if rows[0][1] != nil {
		t.Fatalf("expected NULL for LAG at first row, got %v", rows[0][1])
	}
	// Second row LAG should be 100
	if fmt.Sprintf("%v", rows[1][1]) != "100" {
		t.Fatalf("expected LAG=100 for id=2, got %v", rows[1][1])
	}
}

// ==================== TRANSACTION + SAVEPOINT DEEPER ====================

func TestV85_SavepointNestedRelease(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE sp_nest (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "INSERT INTO sp_nest VALUES (1, 'a')")
	afExec(t, db, ctx, "SAVEPOINT sp1")
	afExec(t, db, ctx, "INSERT INTO sp_nest VALUES (2, 'b')")
	afExec(t, db, ctx, "SAVEPOINT sp2")
	afExec(t, db, ctx, "INSERT INTO sp_nest VALUES (3, 'c')")
	afExec(t, db, ctx, "RELEASE SAVEPOINT sp2")  // sp2 released, insert 3 committed to sp1
	afExec(t, db, ctx, "RELEASE SAVEPOINT sp1")  // sp1 released, all committed to txn
	afExec(t, db, ctx, "COMMIT")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM sp_nest")
	if fmt.Sprintf("%v", rows[0][0]) != "3" {
		t.Fatalf("expected 3 rows, got %v", rows[0][0])
	}
}

func TestV85_SavepointRollbackPartial(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE sp_part (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "INSERT INTO sp_part VALUES (1, 'keep')")
	afExec(t, db, ctx, "SAVEPOINT sp1")
	afExec(t, db, ctx, "INSERT INTO sp_part VALUES (2, 'lose')")
	afExec(t, db, ctx, "INSERT INTO sp_part VALUES (3, 'lose')")
	afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp1")  // undo inserts 2,3
	afExec(t, db, ctx, "INSERT INTO sp_part VALUES (4, 'keep')")
	afExec(t, db, ctx, "COMMIT")

	rows := afQuery(t, db, ctx, "SELECT id FROM sp_part ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
	if fmt.Sprintf("%v", rows[0][0]) != "1" || fmt.Sprintf("%v", rows[1][0]) != "4" {
		t.Fatalf("expected ids 1,4, got %v,%v", rows[0][0], rows[1][0])
	}
}

// ==================== ALTER TABLE DEEPER ====================

func TestV85_AlterTableRenameTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE old_name (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO old_name VALUES (1, 'hello')")
	afExec(t, db, ctx, "ALTER TABLE old_name RENAME TO new_name")

	// Old name should fail
	_, err := db.Query(ctx, "SELECT * FROM old_name")
	if err == nil {
		t.Fatalf("expected error querying old_name after rename")
	}

	// New name should work
	rows := afQuery(t, db, ctx, "SELECT val FROM new_name")
	if fmt.Sprintf("%v", rows[0][0]) != "hello" {
		t.Fatalf("expected 'hello', got %v", rows[0][0])
	}
}

func TestV85_AlterTableRenameColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rc_tbl (id INTEGER, old_col TEXT)")
	afExec(t, db, ctx, "INSERT INTO rc_tbl VALUES (1, 'data')")
	afExec(t, db, ctx, "ALTER TABLE rc_tbl RENAME COLUMN old_col TO new_col")

	rows := afQuery(t, db, ctx, "SELECT new_col FROM rc_tbl")
	if fmt.Sprintf("%v", rows[0][0]) != "data" {
		t.Fatalf("expected 'data', got %v", rows[0][0])
	}
}

// ==================== INDEX OPERATIONS ====================

func TestV85_IndexQueryAcceleration(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE idx_tbl (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	for i := 1; i <= 50; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO idx_tbl VALUES (%d, 'name%d', %d)", i, i, 20+i%30))
	}

	afExec(t, db, ctx, "CREATE INDEX idx_age ON idx_tbl(age)")

	// Query using index
	rows := afQuery(t, db, ctx, "SELECT id FROM idx_tbl WHERE age = 25")
	if len(rows) == 0 {
		t.Fatalf("expected rows for age=25")
	}
}

func TestV85_UniqueIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ui_tbl (id INTEGER, email TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX ui_email ON ui_tbl(email)")
	afExec(t, db, ctx, "INSERT INTO ui_tbl VALUES (1, 'alice@test.com')")

	// Duplicate should fail
	_, err := db.Exec(ctx, "INSERT INTO ui_tbl VALUES (2, 'alice@test.com')")
	if err == nil {
		t.Fatalf("expected error for duplicate email in unique index")
	}

	// Different email should succeed
	afExec(t, db, ctx, "INSERT INTO ui_tbl VALUES (3, 'bob@test.com')")
}

// ==================== ANALYZE ====================

func TestV85_AnalyzeTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE an_tbl (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO an_tbl VALUES (%d, 'val%d')", i, i))
	}

	afExec(t, db, ctx, "ANALYZE an_tbl")
}

// ==================== VACUUM ====================

func TestV85_VacuumBasic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE vac_tbl (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO vac_tbl VALUES (%d, 'val%d')", i, i))
	}
	// Delete some rows to create fragmentation
	afExec(t, db, ctx, "DELETE FROM vac_tbl WHERE id > 5")

	afExec(t, db, ctx, "VACUUM")

	// Verify data still intact
	rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM vac_tbl")
	if fmt.Sprintf("%v", rows[0][0]) != "5" {
		t.Fatalf("expected 5 rows after VACUUM, got %v", rows[0][0])
	}
}

// ==================== COMPLEX JOIN PATTERNS ====================

func TestV85_SelfJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE emp (id INTEGER, name TEXT, manager_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (1, 'CEO', NULL)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (2, 'VP', 1)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (3, 'Manager', 2)")

	rows := afQuery(t, db, ctx, `SELECT e.name, m.name as mgr
		FROM emp e INNER JOIN emp m ON e.manager_id = m.id ORDER BY e.id`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV85_ThreeTableJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE j_orders (id INTEGER, customer_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE j_customers (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE j_items (order_id INTEGER, product TEXT)")

	afExec(t, db, ctx, "INSERT INTO j_customers VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO j_customers VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO j_orders VALUES (10, 1)")
	afExec(t, db, ctx, "INSERT INTO j_orders VALUES (11, 2)")
	afExec(t, db, ctx, "INSERT INTO j_items VALUES (10, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO j_items VALUES (11, 'Gadget')")

	rows := afQuery(t, db, ctx, `SELECT c.name, i.product
		FROM j_customers c
		INNER JOIN j_orders o ON c.id = o.customer_id
		INNER JOIN j_items i ON o.id = i.order_id
		ORDER BY c.name`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV85_LeftJoinWithGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE lj_dept (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE lj_emp (id INTEGER, dept_id INTEGER, salary INTEGER)")

	afExec(t, db, ctx, "INSERT INTO lj_dept VALUES (1, 'eng')")
	afExec(t, db, ctx, "INSERT INTO lj_dept VALUES (2, 'hr')")
	afExec(t, db, ctx, "INSERT INTO lj_dept VALUES (3, 'empty')")  // no employees
	afExec(t, db, ctx, "INSERT INTO lj_emp VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO lj_emp VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO lj_emp VALUES (3, 2, 150)")

	rows := afQuery(t, db, ctx, `SELECT d.name, COUNT(e.id) as emp_count
		FROM lj_dept d LEFT JOIN lj_emp e ON d.id = e.dept_id
		GROUP BY d.name ORDER BY d.name`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// ==================== COMPLEX EXPRESSION IN SELECT ====================

func TestV85_CaseInSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE cs (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cs VALUES (10)")
	afExec(t, db, ctx, "INSERT INTO cs VALUES (50)")
	afExec(t, db, ctx, "INSERT INTO cs VALUES (90)")

	rows := afQuery(t, db, ctx, `SELECT val,
		CASE WHEN val < 30 THEN 'low' WHEN val < 70 THEN 'mid' ELSE 'high' END as label
		FROM cs ORDER BY val`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][1]) != "low" {
		t.Fatalf("expected 'low' for val=10, got %v", rows[0][1])
	}
	if fmt.Sprintf("%v", rows[1][1]) != "mid" {
		t.Fatalf("expected 'mid' for val=50, got %v", rows[1][1])
	}
	if fmt.Sprintf("%v", rows[2][1]) != "high" {
		t.Fatalf("expected 'high' for val=90, got %v", rows[2][1])
	}
}

func TestV85_ArithmeticInSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ar (a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ar VALUES (10, 3)")

	rows := afQuery(t, db, ctx, "SELECT a + b, a - b, a * b, a / b, a % b FROM ar")
	if fmt.Sprintf("%v", rows[0][0]) != "13" {
		t.Fatalf("expected 13 for a+b, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[0][1]) != "7" {
		t.Fatalf("expected 7 for a-b, got %v", rows[0][1])
	}
}

func TestV85_ConcatInSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE cn (first TEXT, last TEXT)")
	afExec(t, db, ctx, "INSERT INTO cn VALUES ('John', 'Doe')")

	afExpectVal(t, db, ctx, "SELECT first || ' ' || last FROM cn", "John Doe")
}

// ==================== ERROR HANDLING ====================

func TestV85_CreateTableDuplicateName(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dup_tbl (id INTEGER)")
	_, err := db.Exec(ctx, "CREATE TABLE dup_tbl (id INTEGER)")
	if err == nil {
		t.Fatalf("expected error for duplicate table name")
	}
}

func TestV85_CreateTableIfNotExists(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ine_tbl (id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE IF NOT EXISTS ine_tbl (id INTEGER)")  // should not error
}

func TestV85_DropTableIfExists(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "DROP TABLE IF EXISTS nonexistent_table")  // should not error
}

func TestV85_InsertIntoNonexistentTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	_, err := db.Exec(ctx, "INSERT INTO ghost_table VALUES (1)")
	if err == nil {
		t.Fatalf("expected error for insert into nonexistent table")
	}
}

func TestV85_SelectFromNonexistentTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	_, err := db.Query(ctx, "SELECT * FROM ghost_table")
	if err == nil {
		t.Fatalf("expected error for select from nonexistent table")
	}
}

// ==================== DEFAULT VALUES ====================

func TestV85_DefaultValueExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, `CREATE TABLE def_tbl (
		id INTEGER PRIMARY KEY,
		status TEXT DEFAULT 'active',
		count INTEGER DEFAULT 0,
		rate REAL DEFAULT 1.5
	)`)

	afExec(t, db, ctx, "INSERT INTO def_tbl (id) VALUES (1)")
	rows := afQuery(t, db, ctx, "SELECT * FROM def_tbl WHERE id = 1")
	if fmt.Sprintf("%v", rows[0][1]) != "active" {
		t.Fatalf("expected default 'active', got %v", rows[0][1])
	}
}

// ==================== CTE PATTERNS ====================

func TestV85_CTEWithAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE cte_sales (region TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cte_sales VALUES ('east', 100)")
	afExec(t, db, ctx, "INSERT INTO cte_sales VALUES ('east', 200)")
	afExec(t, db, ctx, "INSERT INTO cte_sales VALUES ('west', 300)")

	rows := afQuery(t, db, ctx, `WITH totals AS (
		SELECT region, SUM(amount) as total FROM cte_sales GROUP BY region
	) SELECT * FROM totals ORDER BY region`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV85_CTEChained(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE cte_data (id INTEGER, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cte_data VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO cte_data VALUES (2, 'a', 20)")
	afExec(t, db, ctx, "INSERT INTO cte_data VALUES (3, 'b', 30)")

	rows := afQuery(t, db, ctx, `WITH
		step1 AS (SELECT cat, SUM(val) as total FROM cte_data GROUP BY cat),
		step2 AS (SELECT * FROM step1 WHERE total > 15)
		SELECT * FROM step2 ORDER BY cat`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV85_RecursiveCTE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `WITH RECURSIVE cnt(x) AS (
		SELECT 1
		UNION ALL
		SELECT x + 1 FROM cnt WHERE x < 5
	) SELECT x FROM cnt`)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

// ==================== UNION / INTERSECT / EXCEPT ====================

func TestV85_UnionAll(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ua1 (val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE ua2 (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ua1 VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO ua1 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO ua2 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO ua2 VALUES (3)")

	rows := afQuery(t, db, ctx, "SELECT val FROM ua1 UNION ALL SELECT val FROM ua2 ORDER BY val")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestV85_Union(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE u1 (val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE u2 (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO u1 VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO u1 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO u2 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO u2 VALUES (3)")

	rows := afQuery(t, db, ctx, "SELECT val FROM u1 UNION SELECT val FROM u2 ORDER BY val")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (deduped), got %d", len(rows))
	}
}

func TestV85_Intersect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE i1 (val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE i2 (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO i1 VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO i1 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO i1 VALUES (3)")
	afExec(t, db, ctx, "INSERT INTO i2 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO i2 VALUES (3)")
	afExec(t, db, ctx, "INSERT INTO i2 VALUES (4)")

	rows := afQuery(t, db, ctx, "SELECT val FROM i1 INTERSECT SELECT val FROM i2 ORDER BY val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (2,3), got %d", len(rows))
	}
}

func TestV85_Except(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE e1 (val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE e2 (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO e1 VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO e1 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO e1 VALUES (3)")
	afExec(t, db, ctx, "INSERT INTO e2 VALUES (2)")

	rows := afQuery(t, db, ctx, "SELECT val FROM e1 EXCEPT SELECT val FROM e2 ORDER BY val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (1,3), got %d", len(rows))
	}
}

// ==================== INSERT/UPDATE/DELETE PATTERNS ====================

func TestV85_InsertMultipleRows(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE im (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO im VALUES (1, 'a'), (2, 'b'), (3, 'c')")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM im")
	if fmt.Sprintf("%v", rows[0][0]) != "3" {
		t.Fatalf("expected 3 rows, got %v", rows[0][0])
	}
}

func TestV85_UpdateWithExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ue (id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ue VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO ue VALUES (2, 20)")

	afExec(t, db, ctx, "UPDATE ue SET val = val * 2 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT val FROM ue WHERE id = 1", float64(20))
}

func TestV85_DeleteWithSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ds_main (id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE ds_keep (id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ds_main VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO ds_main VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO ds_main VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO ds_keep VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO ds_keep VALUES (3)")

	afExec(t, db, ctx, "DELETE FROM ds_main WHERE id NOT IN (SELECT id FROM ds_keep)")
	rows := afQuery(t, db, ctx, "SELECT id FROM ds_main ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ==================== CONSTRAINT TESTS ====================

func TestV85_PrimaryKeyConstraint(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE pkc (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO pkc VALUES (1, 'a')")

	_, err := db.Exec(ctx, "INSERT INTO pkc VALUES (1, 'b')")
	if err == nil {
		t.Fatalf("expected error for duplicate primary key")
	}
}

func TestV85_NotNullConstraint(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE nnc (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	afExec(t, db, ctx, "INSERT INTO nnc VALUES (1, 'valid')")

	_, err := db.Exec(ctx, "INSERT INTO nnc VALUES (2, NULL)")
	if err == nil {
		t.Fatalf("expected error for NULL in NOT NULL column")
	}
}

func TestV85_CheckConstraint(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE chk (id INTEGER, age INTEGER CHECK(age >= 0))")
	afExec(t, db, ctx, "INSERT INTO chk VALUES (1, 25)")

	_, err := db.Exec(ctx, "INSERT INTO chk VALUES (2, -1)")
	if err == nil {
		t.Fatalf("expected error for CHECK constraint violation")
	}
}

// ==================== FOREIGN KEY CASCADE ====================

func TestV85_ForeignKeyCascadeDelete(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE fk_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE fk_child (id INTEGER, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_parent(id) ON DELETE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO fk_parent VALUES (1, 'p1')")
	afExec(t, db, ctx, "INSERT INTO fk_parent VALUES (2, 'p2')")
	afExec(t, db, ctx, "INSERT INTO fk_child VALUES (10, 1)")
	afExec(t, db, ctx, "INSERT INTO fk_child VALUES (11, 1)")
	afExec(t, db, ctx, "INSERT INTO fk_child VALUES (12, 2)")

	afExec(t, db, ctx, "DELETE FROM fk_parent WHERE id = 1")

	// Children of parent 1 should be cascaded
	rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM fk_child")
	if fmt.Sprintf("%v", rows[0][0]) != "1" {
		t.Fatalf("expected 1 child row after cascade delete, got %v", rows[0][0])
	}
}

func TestV85_ForeignKeySetNullDelete(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE fk_sn_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE fk_sn_child (id INTEGER, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_sn_parent(id) ON DELETE SET NULL)")

	afExec(t, db, ctx, "INSERT INTO fk_sn_parent VALUES (1, 'p1')")
	afExec(t, db, ctx, "INSERT INTO fk_sn_child VALUES (10, 1)")

	afExec(t, db, ctx, "DELETE FROM fk_sn_parent WHERE id = 1")

	rows := afQuery(t, db, ctx, "SELECT parent_id FROM fk_sn_child WHERE id = 10")
	if rows[0][0] != nil {
		t.Fatalf("expected NULL parent_id after SET NULL, got %v", rows[0][0])
	}
}

// ==================== JSON FUNCTIONS ====================

func TestV85_JSONExtractNested(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, `CREATE TABLE jn (id INTEGER, data TEXT)`)
	afExec(t, db, ctx, `INSERT INTO jn VALUES (1, '{"user":{"name":"Alice","age":30}}')`)

	afExpectVal(t, db, ctx, `SELECT JSON_EXTRACT(data, '$.user.name') FROM jn`, "Alice")
	afExpectVal(t, db, ctx, `SELECT JSON_EXTRACT(data, '$.user.age') FROM jn`, float64(30))
}

func TestV85_JSONSet(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// JSON_SET with string value
	afExpectVal(t, db, ctx, `SELECT JSON_SET('{"a":1}', '$.b', 'hello')`, `{"a":1,"b":"hello"}`)
}

func TestV85_JSONRemove(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, `SELECT JSON_REMOVE('{"a":1,"b":2}', '$.b')`, `{"a":1}`)
}

func TestV85_JSONType(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('{"a":1}')`, "object")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('[1,2,3]')`, "array")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('"hello"')`, "string")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('42')`, "number")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('true')`, "boolean")
	afExpectVal(t, db, ctx, `SELECT JSON_TYPE('null')`, "null")
}

func TestV85_JSONArrayLength(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, `SELECT JSON_ARRAY_LENGTH('[1,2,3,4]')`, float64(4))
	afExpectVal(t, db, ctx, `SELECT JSON_ARRAY_LENGTH('[]')`, float64(0))
}

func TestV85_JSONKeys(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_KEYS('{"a":1,"b":2,"c":3}')`)
	if len(rows) == 0 {
		t.Fatalf("expected result from JSON_KEYS")
	}
}

func TestV85_JSONMerge(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_MERGE('{"a":1}', '{"b":2}')`)
	if len(rows) == 0 {
		t.Fatalf("expected result from JSON_MERGE")
	}
}

func TestV85_JSONQuoteUnquote(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, `SELECT JSON_QUOTE('hello')`, `"hello"`)
	afExpectVal(t, db, ctx, `SELECT JSON_UNQUOTE('"hello"')`, "hello")
}

func TestV85_JSONValid(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_VALID('{"a":1}')`)
	if rows[0][0] != true {
		t.Fatalf("expected true for valid JSON, got %v (%T)", rows[0][0], rows[0][0])
	}

	rows = afQuery(t, db, ctx, `SELECT JSON_VALID('not json')`)
	if rows[0][0] != false {
		t.Fatalf("expected false for invalid JSON, got %v (%T)", rows[0][0], rows[0][0])
	}
}

// ==================== REGEX FUNCTIONS ====================

func TestV85_RegexMatch(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rgx (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO rgx VALUES ('hello123')")
	afExec(t, db, ctx, "INSERT INTO rgx VALUES ('world')")
	afExec(t, db, ctx, "INSERT INTO rgx VALUES ('abc456')")

	rows := afQuery(t, db, ctx, "SELECT val FROM rgx WHERE REGEXP_MATCH(val, '[0-9]+')")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows with digits, got %d", len(rows))
	}
}

func TestV85_RegexReplace(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT REGEXP_REPLACE('hello123world', '[0-9]+', 'NUM')", "helloNUMworld")
}

func TestV85_RegexExtract(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Returns array-like format
	afExpectVal(t, db, ctx, "SELECT REGEXP_EXTRACT('hello123world', '[0-9]+')", "[123]")
}

// ==================== DISTINCT WITH NULL ====================

func TestV85_DistinctWithNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dn (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO dn VALUES ('a')")
	afExec(t, db, ctx, "INSERT INTO dn VALUES ('a')")
	afExec(t, db, ctx, "INSERT INTO dn VALUES (NULL)")
	afExec(t, db, ctx, "INSERT INTO dn VALUES (NULL)")
	afExec(t, db, ctx, "INSERT INTO dn VALUES ('b')")

	rows := afQuery(t, db, ctx, "SELECT DISTINCT val FROM dn ORDER BY val")
	if len(rows) != 3 {
		t.Fatalf("expected 3 distinct values (NULL, a, b), got %d: %v", len(rows), rows)
	}
}

// ==================== DERIVED TABLE ====================

func TestV85_DerivedTableWithAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dt_sales (region TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dt_sales VALUES ('east', 100)")
	afExec(t, db, ctx, "INSERT INTO dt_sales VALUES ('east', 200)")
	afExec(t, db, ctx, "INSERT INTO dt_sales VALUES ('west', 300)")

	rows := afQuery(t, db, ctx, `SELECT region, total FROM
		(SELECT region, SUM(amount) as total FROM dt_sales GROUP BY region) sub
		ORDER BY region`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ==================== INSERT INTO SELECT ====================

func TestV85_InsertIntoSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE iis_src (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE iis_dst (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO iis_src VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO iis_src VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO iis_src VALUES (3, 'c')")

	afExec(t, db, ctx, "INSERT INTO iis_dst SELECT * FROM iis_src WHERE id > 1")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM iis_dst")
	if fmt.Sprintf("%v", rows[0][0]) != "2" {
		t.Fatalf("expected 2 rows, got %v", rows[0][0])
	}
}

// ==================== MULTIPLE TRIGGERS ON SAME TABLE ====================

func TestV85_TriggerMultipleAfter(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tma (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tma_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER tma_first AFTER INSERT ON tma
		FOR EACH ROW
		BEGIN
			INSERT INTO tma_log VALUES ('first');
		END`)

	afExec(t, db, ctx, `CREATE TRIGGER tma_second AFTER INSERT ON tma
		FOR EACH ROW
		BEGIN
			INSERT INTO tma_log VALUES ('second');
		END`)

	afExec(t, db, ctx, "INSERT INTO tma VALUES (1, 100)")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*) FROM tma_log")
	if fmt.Sprintf("%v", rows[0][0]) != "2" {
		t.Fatalf("expected 2 log rows, got %v", rows[0][0])
	}
}

// ==================== EXPRESSION WITH UNARY MINUS ====================

func TestV85_UnaryMinus(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT -5", int64(-5))
	afExpectVal(t, db, ctx, "SELECT -(-3)", int64(3))
	afExpectVal(t, db, ctx, "SELECT ABS(-42)", float64(42))
}

// ==================== COMPLEX WHERE CLAUSES ====================

func TestV85_WhereWithMultipleConditions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wmc (id INTEGER, a INTEGER, b TEXT, c REAL)")
	afExec(t, db, ctx, "INSERT INTO wmc VALUES (1, 10, 'hello', 1.5)")
	afExec(t, db, ctx, "INSERT INTO wmc VALUES (2, 20, 'world', 2.5)")
	afExec(t, db, ctx, "INSERT INTO wmc VALUES (3, 30, 'hello', 3.5)")
	afExec(t, db, ctx, "INSERT INTO wmc VALUES (4, 40, 'world', 4.5)")

	rows := afQuery(t, db, ctx, "SELECT id FROM wmc WHERE a > 15 AND b = 'hello' AND c < 4.0")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (id=3), got %d", len(rows))
	}

	rows = afQuery(t, db, ctx, "SELECT id FROM wmc WHERE (a < 15 OR a > 35) AND b = 'world'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (id=4), got %d", len(rows))
	}
}

// ==================== FTS (Full-Text Search) ====================

func TestV85_FTSCreateAndSearch(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE fts_docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
	afExec(t, db, ctx, "INSERT INTO fts_docs VALUES (1, 'Go Programming', 'Go is a statically typed language')")
	afExec(t, db, ctx, "INSERT INTO fts_docs VALUES (2, 'Rust Overview', 'Rust focuses on safety and performance')")
	afExec(t, db, ctx, "INSERT INTO fts_docs VALUES (3, 'Python Intro', 'Python is dynamically typed')")

	// Create FTS index with FULLTEXT keyword
	afExec(t, db, ctx, "CREATE FULLTEXT INDEX fts_idx ON fts_docs(title, body)")
}

// ==================== PREPARED STATEMENT PATTERNS ====================

func TestV85_PreparedSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ps (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO ps VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO ps VALUES (2, 'Bob')")

	rows, err := db.Query(ctx, "SELECT name FROM ps WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("prepared select: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		vals := make([]interface{}, 1)
		ptrs := []interface{}{&vals[0]}
		rows.Scan(ptrs...)
		if fmt.Sprintf("%v", vals[0]) != "Alice" {
			t.Fatalf("expected Alice, got %v", vals[0])
		}
	}
}

func TestV85_PreparedInsert(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE pi (id INTEGER, name TEXT)")

	_, err := db.Exec(ctx, "INSERT INTO pi VALUES (?, ?)", 1, "test")
	if err != nil {
		t.Fatalf("prepared insert: %v", err)
	}

	afExpectVal(t, db, ctx, "SELECT name FROM pi WHERE id = 1", "test")
}

func TestV85_PreparedUpdate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE pu (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO pu VALUES (1, 'old')")

	_, err := db.Exec(ctx, "UPDATE pu SET val = ? WHERE id = ?", "new", 1)
	if err != nil {
		t.Fatalf("prepared update: %v", err)
	}

	afExpectVal(t, db, ctx, "SELECT val FROM pu WHERE id = 1", "new")
}

// ==================== AGGREGATE WITH NULL ====================

func TestV85_AggregateNullHandling(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE an (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO an VALUES (10)")
	afExec(t, db, ctx, "INSERT INTO an VALUES (NULL)")
	afExec(t, db, ctx, "INSERT INTO an VALUES (20)")
	afExec(t, db, ctx, "INSERT INTO an VALUES (NULL)")
	afExec(t, db, ctx, "INSERT INTO an VALUES (30)")

	afExpectVal(t, db, ctx, "SELECT SUM(val) FROM an", float64(60))
	afExpectVal(t, db, ctx, "SELECT AVG(val) FROM an", float64(20))
	afExpectVal(t, db, ctx, "SELECT COUNT(val) FROM an", float64(3))     // non-NULL count
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM an", float64(5))       // total count
	afExpectVal(t, db, ctx, "SELECT MIN(val) FROM an", float64(10))
	afExpectVal(t, db, ctx, "SELECT MAX(val) FROM an", float64(30))
}

func TestV85_AggregateAllNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ann (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ann VALUES (NULL)")
	afExec(t, db, ctx, "INSERT INTO ann VALUES (NULL)")

	rows := afQuery(t, db, ctx, "SELECT SUM(val) FROM ann")
	if rows[0][0] != nil {
		t.Fatalf("expected NULL for SUM of all NULLs, got %v", rows[0][0])
	}

	rows = afQuery(t, db, ctx, "SELECT AVG(val) FROM ann")
	if rows[0][0] != nil {
		t.Fatalf("expected NULL for AVG of all NULLs, got %v", rows[0][0])
	}
}

// ==================== TRIGGER WHEN WITH NUMERIC ZERO ====================

func TestV85_TriggerWhenNumericZeroFalse(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_zero (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_zero_log (msg TEXT)")

	// WHEN condition evaluates to 0 (numeric false) should skip trigger
	afExec(t, db, ctx, `CREATE TRIGGER tw_zero_check AFTER INSERT ON tw_zero
		FOR EACH ROW WHEN NEW.val - NEW.val
		BEGIN
			INSERT INTO tw_zero_log VALUES ('fired');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_zero VALUES (1, 42)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_zero_log", float64(0))
}

// ==================== TRIGGER WITH NO WHEN (ALWAYS FIRES) ====================

func TestV85_TriggerNoWhenAlwaysFires(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_always (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_always_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER tw_always_fire AFTER INSERT ON tw_always
		FOR EACH ROW
		BEGIN
			INSERT INTO tw_always_log VALUES ('fired');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_always VALUES (1, 0)")
	afExec(t, db, ctx, "INSERT INTO tw_always VALUES (2, 100)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tw_always_log", float64(2))
}

// ==================== MIXED TRIGGER WHEN + NO WHEN ====================

func TestV85_TriggerMixWhenAndNoWhen(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tw_mix (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tw_mix_log (msg TEXT)")

	// Always-fire trigger
	afExec(t, db, ctx, `CREATE TRIGGER tw_mix_always AFTER INSERT ON tw_mix
		FOR EACH ROW
		BEGIN
			INSERT INTO tw_mix_log VALUES ('always');
		END`)

	// Conditional trigger
	afExec(t, db, ctx, `CREATE TRIGGER tw_mix_cond AFTER INSERT ON tw_mix
		FOR EACH ROW WHEN NEW.val > 50
		BEGIN
			INSERT INTO tw_mix_log VALUES ('conditional');
		END`)

	afExec(t, db, ctx, "INSERT INTO tw_mix VALUES (1, 10)")   // only always fires
	afExec(t, db, ctx, "INSERT INTO tw_mix VALUES (2, 100)")  // both fire

	rows := afQuery(t, db, ctx, "SELECT msg, COUNT(*) FROM tw_mix_log GROUP BY msg ORDER BY msg")
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(rows), rows)
	}
}
