package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// TestRegression_CrashRecoveryBrandNewDB verifies that a brand-new disk database
// which performs DDL+DML and then "crashes" (no clean Close) before any
// checkpoint can be reopened with its committed writes replayed from the WAL.
// The crash is simulated by snapshotting the on-disk data file and WAL after the
// writes and opening the snapshot, so no clean shutdown flush is involved.
func TestRegression_CrashRecoveryBrandNewDB(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")

	db, err := Open(src, &Options{CoreStorage: CoreStorage{SyncMode: SyncFull}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE acct (id INTEGER PRIMARY KEY, bal INTEGER)")
	mustExec(t, db, "INSERT INTO acct VALUES (1,100),(2,200)")
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE acct SET bal=999 WHERE id=1"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	mustExec(t, db, "INSERT INTO acct VALUES (3,300)")

	// Snapshot the on-disk state (simulated crash: no clean Close on this copy).
	snap := filepath.Join(dir, "snap.db")
	copyFile(t, src, snap)
	copyFile(t, src+".wal", snap+".wal")
	db.Close()

	rdb, err := Open(snap, &Options{CoreStorage: CoreStorage{SyncMode: SyncFull}})
	if err != nil {
		t.Fatalf("reopen after crash failed: %v", err)
	}
	defer rdb.Close()

	got := map[int]int{}
	rows, err := rdb.Query(ctx, "SELECT id, bal FROM acct ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id, bal int
		if err := rows.Scan(&id, &bal); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = bal
		n++
	}
	if n != 3 {
		t.Fatalf("recovered %d rows, want 3 (no double-apply): %v", n, got)
	}
	if got[1] != 999 || got[2] != 200 || got[3] != 300 {
		t.Errorf("recovered values = %v, want map[1:999 2:200 3:300]", got)
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

// openRegressionDB opens an in-memory database for the regression suite.
func openRegressionDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func mustExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// queryRows returns all result rows for sql.
func queryRows(t *testing.T, db *DB, sql string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer rows.Close()
	cols := rows.Columns()
	var out [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan %q: %v", sql, err)
		}
		out = append(out, vals)
	}
	return out
}

// scalar returns the first column of the first row, stringified.
func scalar(t *testing.T, db *DB, sql string) string {
	t.Helper()
	rows := queryRows(t, db, sql)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatalf("query %q returned no scalar", sql)
	}
	return fmt.Sprintf("%v", rows[0][0])
}

// TestRegression_ImplicitColumnAlias covers `SELECT expr alias` (no AS).
func TestRegression_ImplicitColumnAlias(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1,'a',30),(2,'b',25)")

	rows := queryRows(t, db, "SELECT COUNT(*) c, AVG(age) a, MAX(age) m FROM t")
	if len(rows) != 1 || len(rows[0]) != 3 {
		t.Fatalf("implicit alias produced %d cols (want 3): %v", len(rows[0]), rows)
	}
	if got := fmt.Sprintf("%v", rows[0][0]); got != "2" {
		t.Fatalf("COUNT(*) c = %s, want 2", got)
	}
}

// TestRegression_DistinctAggregates covers SUM/AVG/COUNT/GROUP_CONCAT DISTINCT.
func TestRegression_DistinctAggregates(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1,'a',1),(2,'a',1),(3,'a',2),(4,'b',3),(5,'b',3)")

	cases := map[string]string{
		"SELECT SUM(DISTINCT v) FROM t":          "6",
		"SELECT COUNT(DISTINCT v) FROM t":        "3",
		"SELECT AVG(DISTINCT v) FROM t":          "2",
		"SELECT GROUP_CONCAT(DISTINCT v) FROM t": "1,2,3",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}

	// Distinct over a derived table.
	if got := scalar(t, db, "SELECT COUNT(DISTINCT v) FROM (SELECT 1 v UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) x"); got != "3" {
		t.Errorf("derived COUNT(DISTINCT) = %s, want 3", got)
	}
}

// TestRegression_MathFunctions covers MOD/POWER/SQRT.
func TestRegression_MathFunctions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT MOD(17,5)":         "2",
		"SELECT POWER(2,10)":       "1024",
		"SELECT SQRT(16)":          "4",
		"SELECT SIGN(-5)":          "-1",
		"SELECT GREATEST(3,7,2)":   "7",
		"SELECT LEAST(3,7,2)":      "2",
		"SELECT LOG(2,8)":          "3",
		"SELECT TRUNCATE(3.567,1)": "3.5",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

// TestRegression_StringFunctions covers ASCII/LOCATE/SUBSTRING_INDEX.
func TestRegression_StringFunctions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT ASCII('A')":                        "65",
		"SELECT LOCATE('lo','hello')":              "4",
		"SELECT SUBSTRING_INDEX('a,b,c,d',',',2)":  "a,b",
		"SELECT SUBSTRING_INDEX('a,b,c,d',',',-2)": "c,d",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

// TestRegression_DateFunctions covers STRFTIME format specifiers.
func TestRegression_DateFunctions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	if got := scalar(t, db, "SELECT STRFTIME('%Y-%m-%d', '2024-03-15 13:45:30')"); got != "2024-03-15" {
		t.Errorf("STRFTIME = %s, want 2024-03-15", got)
	}
	if got := scalar(t, db, "SELECT STRFTIME('%Y', '2024-03-15')"); got != "2024" {
		t.Errorf("STRFTIME year = %s, want 2024", got)
	}
}

// TestRegression_JSONConstructors covers JSON_OBJECT/JSON_ARRAY.
func TestRegression_JSONConstructors(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	if got := scalar(t, db, "SELECT JSON_ARRAY(1,2,3)"); got != "[1,2,3]" {
		t.Errorf("JSON_ARRAY = %s, want [1,2,3]", got)
	}
	if got := scalar(t, db, "SELECT JSON_EXTRACT(JSON_OBJECT('name','alice'),'$.name')"); got != "alice" {
		t.Errorf("JSON_OBJECT/EXTRACT = %s, want alice", got)
	}
}

// TestRegression_NullsOrdering covers ORDER BY ... NULLS FIRST/LAST.
func TestRegression_NullsOrdering(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1,5),(2,NULL),(3,1)")

	first := queryRows(t, db, "SELECT x FROM t ORDER BY x ASC NULLS FIRST")
	if first[0][0] != nil {
		t.Errorf("NULLS FIRST: first row = %v, want NULL", first[0][0])
	}
	last := queryRows(t, db, "SELECT x FROM t ORDER BY x ASC NULLS LAST")
	if last[len(last)-1][0] != nil {
		t.Errorf("NULLS LAST: last row = %v, want NULL", last[len(last)-1][0])
	}
}

// TestRegression_WindowFrame covers ROWS BETWEEN frame bounds.
func TestRegression_WindowFrame(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE w (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO w VALUES (1,10),(2,20),(3,30),(4,40)")

	rows := queryRows(t, db, "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) s FROM w ORDER BY id")
	want := []string{"10", "30", "50", "70"}
	if len(rows) != len(want) {
		t.Fatalf("window frame returned %d rows, want %d", len(rows), len(want))
	}
	for i, w := range want {
		if got := fmt.Sprintf("%v", rows[i][0]); got != w {
			t.Errorf("frame row %d = %s, want %s", i, got, w)
		}
	}
}

// TestRegression_WindowInExpression covers window functions nested inside an
// expression (e.g. SUM(x) OVER () + 1), which previously evaluated to NULL.
func TestRegression_WindowInExpression(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO s VALUES (1,10),(2,20),(3,30)")

	// SUM(v) OVER () = 60, so each + 1 = 61.
	rows := queryRows(t, db, "SELECT SUM(v) OVER () + 1 sp FROM s")
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	for i, r := range rows {
		if got := fmt.Sprintf("%v", r[0]); got != "61" {
			t.Errorf("row %d SUM OVER ()+1 = %s, want 61", i, got)
		}
	}

	// ROW_NUMBER() * 10 nested in an expression.
	rn := queryRows(t, db, "SELECT ROW_NUMBER() OVER (ORDER BY v) * 10 r FROM s ORDER BY v")
	want := []string{"10", "20", "30"}
	for i, w := range want {
		if got := fmt.Sprintf("%v", rn[i][0]); got != w {
			t.Errorf("ROW_NUMBER*10 row %d = %s, want %s", i, got, w)
		}
	}

	// Window nested inside a scalar function call: COALESCE(LAG(v) OVER (), 0).
	lag := queryRows(t, db, "SELECT COALESCE(LAG(v) OVER (ORDER BY v), 0) p FROM s ORDER BY v")
	lagWant := []string{"0", "10", "20"}
	for i, w := range lagWant {
		if got := fmt.Sprintf("%v", lag[i][0]); got != w {
			t.Errorf("COALESCE(LAG) row %d = %s, want %s", i, got, w)
		}
	}
}

// TestRegression_DerivedToDerivedJoin covers joining two derived tables, which
// previously dropped the second derived table's columns.
func TestRegression_DerivedToDerivedJoin(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE s (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)")
	mustExec(t, db, "INSERT INTO s VALUES (1,'a',10),(2,'a',20),(3,'b',30)")

	rows := queryRows(t, db, "SELECT d1.g, d1.s1, d2.cnt FROM (SELECT g, SUM(v) s1 FROM s GROUP BY g) d1 JOIN (SELECT g, COUNT(*) cnt FROM s GROUP BY g) d2 ON d1.g=d2.g ORDER BY d1.g")
	if len(rows) != 2 || len(rows[0]) != 3 {
		t.Fatalf("derived-to-derived join produced %v (want two rows with g,s1,cnt)", rows)
	}
	// group a: s1=30, cnt=2.
	if got := fmt.Sprintf("%v", rows[0][2]); got != "2" {
		t.Errorf("d2.cnt = %s, want 2 (column was dropped)", got)
	}
}

// TestRegression_InsertSelectOnConflict covers INSERT ... SELECT with ON CONFLICT.
func TestRegression_InsertSelectOnConflict(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO s VALUES (1,10),(2,20)")
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1,100)")

	mustExec(t, db, "INSERT INTO t SELECT id, v FROM s ON CONFLICT(id) DO UPDATE SET v=999")
	if got := scalar(t, db, "SELECT v FROM t WHERE id=1"); got != "999" {
		t.Errorf("conflicting row v = %s, want 999", got)
	}
	if got := scalar(t, db, "SELECT v FROM t WHERE id=2"); got != "20" {
		t.Errorf("new row v = %s, want 20", got)
	}
}

// TestRegression_WindowOverDerivedTable covers window functions over a derived
// table (subquery in FROM), which previously projected NULL.
func TestRegression_WindowOverDerivedTable(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE s (id INTEGER PRIMARY KEY, cust TEXT, qty INTEGER)")
	mustExec(t, db, "INSERT INTO s VALUES (1,'a',3),(2,'a',5),(3,'b',4)")

	rows := queryRows(t, db, "SELECT cust, tq, RANK() OVER (ORDER BY tq DESC) r FROM (SELECT cust, SUM(qty) tq FROM s GROUP BY cust) g ORDER BY r")
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// cust a has tq=8 (rank 1), cust b has tq=4 (rank 2).
	if got := fmt.Sprintf("%v", rows[0][2]); got != "1" {
		t.Errorf("RANK first row = %s, want 1", got)
	}
	if got := fmt.Sprintf("%v", rows[1][2]); got != "2" {
		t.Errorf("RANK second row = %s, want 2", got)
	}
}

// TestRegression_StddevVariance covers STDDEV/VARIANCE aggregates (previously
// returned one NULL per row instead of a single aggregated value).
func TestRegression_StddevVariance(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE n (v INTEGER)")
	mustExec(t, db, "INSERT INTO n VALUES (2),(4),(4),(4),(5),(5),(7),(9)")

	rows := queryRows(t, db, "SELECT ROUND(STDDEV(v),4) sd, ROUND(VARIANCE(v),4) var, COUNT(*) c FROM n")
	if len(rows) != 1 {
		t.Fatalf("STDDEV query returned %d rows, want 1 (aggregated)", len(rows))
	}
	if got := fmt.Sprintf("%v", rows[0][0]); got != "2" {
		t.Errorf("STDDEV = %s, want 2", got)
	}
	if got := fmt.Sprintf("%v", rows[0][1]); got != "4" {
		t.Errorf("VARIANCE = %s, want 4", got)
	}
}

// TestRegression_TrigAndDateFunctions covers trig and date-extraction functions.
func TestRegression_TrigAndDateFunctions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT ROUND(SIN(0),4)":                     "0",
		"SELECT ROUND(COS(0),4)":                     "1",
		"SELECT YEAR('2024-03-15')":                  "2024",
		"SELECT MONTH('2024-03-15')":                 "3",
		"SELECT DAY('2024-03-15')":                   "15",
		"SELECT DATEDIFF('2024-03-20','2024-03-15')": "5",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

// TestRegression_WindowRankFunctions covers PERCENT_RANK and CUME_DIST.
func TestRegression_WindowRankFunctions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE w (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO w VALUES (1,10),(2,20),(3,20),(4,40)")

	rows := queryRows(t, db, "SELECT PERCENT_RANK() OVER (ORDER BY v) pr, CUME_DIST() OVER (ORDER BY v) cd FROM w ORDER BY id")
	if len(rows) != 4 {
		t.Fatalf("got %d rows, want 4", len(rows))
	}
	if got := fmt.Sprintf("%v", rows[0][0]); got != "0" {
		t.Errorf("PERCENT_RANK first = %s, want 0", got)
	}
	if got := fmt.Sprintf("%v", rows[3][0]); got != "1" {
		t.Errorf("PERCENT_RANK last = %s, want 1", got)
	}
	if got := fmt.Sprintf("%v", rows[0][1]); got != "0.25" {
		t.Errorf("CUME_DIST first = %s, want 0.25", got)
	}
}

// TestRegression_InlineForeignKey covers `col TYPE REFERENCES other(col)`.
func TestRegression_InlineForeignKey(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))")
	mustExec(t, db, "INSERT INTO parent VALUES (1)")
	mustExec(t, db, "INSERT INTO child VALUES (1,1)")
	if _, err := db.Exec(context.Background(), "INSERT INTO child VALUES (2,99)"); err == nil {
		t.Fatal("inline FK violation was not rejected")
	}
}

// TestRegression_OnConflict covers ON CONFLICT DO NOTHING and DO UPDATE.
func TestRegression_OnConflict(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE u (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO u VALUES (1,10)")

	mustExec(t, db, "INSERT INTO u VALUES (1,999) ON CONFLICT(id) DO NOTHING")
	if got := scalar(t, db, "SELECT v FROM u WHERE id=1"); got != "10" {
		t.Errorf("DO NOTHING changed value to %s, want 10", got)
	}

	mustExec(t, db, "INSERT INTO u VALUES (1,50) ON CONFLICT(id) DO UPDATE SET v=50")
	if got := scalar(t, db, "SELECT v FROM u WHERE id=1"); got != "50" {
		t.Errorf("DO UPDATE value = %s, want 50", got)
	}

	mustExec(t, db, "INSERT INTO u VALUES (2,20) ON CONFLICT(id) DO UPDATE SET v=99")
	if got := scalar(t, db, "SELECT v FROM u WHERE id=2"); got != "20" {
		t.Errorf("ON CONFLICT non-conflicting insert v = %s, want 20", got)
	}
}

// TestRegression_DefaultKeyword covers the DEFAULT keyword in INSERT ... VALUES.
func TestRegression_DefaultKeyword(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE d (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'new', cnt INTEGER DEFAULT 0)")
	mustExec(t, db, "INSERT INTO d (id, status) VALUES (1, DEFAULT)")
	mustExec(t, db, "INSERT INTO d VALUES (2, 'custom', DEFAULT)")

	if got := scalar(t, db, "SELECT status FROM d WHERE id=1"); got != "new" {
		t.Errorf("DEFAULT status = %s, want new", got)
	}
	if got := scalar(t, db, "SELECT cnt FROM d WHERE id=2"); got != "0" {
		t.Errorf("DEFAULT cnt = %s, want 0", got)
	}
}

// TestRegression_CteToCteJoin covers joining two CTEs together (previously the
// second CTE's columns were silently dropped).
func TestRegression_CteToCteJoin(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	rows := queryRows(t, db, "WITH a AS (SELECT 1 x), b AS (SELECT 2 y) SELECT a.x, b.y FROM a, b")
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("CTE-to-CTE join produced %v (want one row with x,y)", rows)
	}
	if got := fmt.Sprintf("%v", rows[0][0]); got != "1" {
		t.Errorf("a.x = %s, want 1", got)
	}
	if got := fmt.Sprintf("%v", rows[0][1]); got != "2" {
		t.Errorf("b.y = %s, want 2", got)
	}

	// Explicit JOIN ... ON between two CTEs.
	r2 := queryRows(t, db, "WITH e AS (SELECT 1 id, 'eng' d), m AS (SELECT 1 eid, 'alice' nm) SELECT e.d, m.nm FROM e JOIN m ON e.id=m.eid")
	if len(r2) != 1 || fmt.Sprintf("%v", r2[0][1]) != "alice" {
		t.Errorf("CTE JOIN ON produced %v, want [eng alice]", r2)
	}
}

// TestRegression_DateAddSub covers DATE_ADD / DATE_SUB.
func TestRegression_DateAddSub(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	if got := scalar(t, db, "SELECT DATE_ADD('2024-01-15', 10)"); got != "2024-01-25" {
		t.Errorf("DATE_ADD = %s, want 2024-01-25", got)
	}
	if got := scalar(t, db, "SELECT DATE_SUB('2024-01-15', 5)"); got != "2024-01-10" {
		t.Errorf("DATE_SUB = %s, want 2024-01-10", got)
	}
}

// TestRegression_CastAndExtract covers CAST with type parameters, EXTRACT(...FROM)
// and POSITION(...IN) syntax.
func TestRegression_CastAndExtract(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT CAST('7' AS INT) + 1":             "8",
		"SELECT CAST(42 AS VARCHAR(10))":          "42",
		"SELECT EXTRACT(YEAR FROM '2024-03-15')":  "2024",
		"SELECT EXTRACT(MONTH FROM '2024-03-15')": "3",
		"SELECT POSITION('lo' IN 'hello')":        "4",
		"SELECT POSITION('x' IN 'hello')":         "0",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

// TestRegression_NullSafeEquality covers the <=> operator.
func TestRegression_NullSafeEquality(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT NULL <=> NULL": "true",
		"SELECT 1 <=> NULL":    "false",
		"SELECT 1 <=> 1":       "true",
		"SELECT 1 <=> 2":       "false",
		"SELECT 5 <= 5":        "true", // ensure <= is unaffected
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

// TestRegression_BitwiseOperators covers &, |, ^, <<, >> and their precedence.
func TestRegression_BitwiseOperators(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT 6 & 3":      "2",
		"SELECT 6 | 1":      "7",
		"SELECT 6 ^ 2":      "4",
		"SELECT 1 << 4":     "16",
		"SELECT 16 >> 2":    "4",
		"SELECT 1 | 2 & 3":  "3", // & binds tighter than |
		"SELECT 1 << 2 + 1": "8", // + binds tighter than <<
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

// TestRegression_LargeIntegerPrecision covers integers above 2^53, which were
// previously corrupted by a float64 round-trip.
func TestRegression_LargeIntegerPrecision(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE li (v INTEGER)")
	mustExec(t, db, "INSERT INTO li VALUES (9007199254740993),(9223372036854775807)")

	if got := scalar(t, db, "SELECT v FROM li WHERE v = 9007199254740993"); got != "9007199254740993" {
		t.Errorf("2^53+1 stored as %s, want 9007199254740993", got)
	}
	if got := scalar(t, db, "SELECT v FROM li WHERE v = 9223372036854775807"); got != "9223372036854775807" {
		t.Errorf("max int64 stored as %s, want 9223372036854775807", got)
	}
	// Negation must also preserve int64 precision (unary minus).
	if got := scalar(t, db, "SELECT -9007199254740993"); got != "-9007199254740993" {
		t.Errorf("negated large int = %s, want -9007199254740993", got)
	}
	// Ordinary integer arithmetic is unaffected.
	if got := scalar(t, db, "SELECT 2 + 3"); got != "5" {
		t.Errorf("2+3 = %s, want 5", got)
	}
}

// TestRegression_CompositeUnique covers table-level UNIQUE (col, ...) constraints.
func TestRegression_CompositeUnique(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE cu (a INTEGER, b INTEGER, UNIQUE(a,b))")
	mustExec(t, db, "INSERT INTO cu VALUES (1,1),(1,2),(2,1)")

	// Same (a,b) pair must be rejected.
	if _, err := db.Exec(context.Background(), "INSERT INTO cu VALUES (1,1)"); err == nil {
		t.Fatal("composite UNIQUE violation was not rejected")
	}
	// A distinct pair sharing one column is allowed.
	if _, err := db.Exec(context.Background(), "INSERT INTO cu VALUES (1,3)"); err != nil {
		t.Fatalf("non-duplicate insert rejected: %v", err)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM cu"); got != "4" {
		t.Errorf("row count = %s, want 4", got)
	}
}

// TestRegression_CtasAndTruncate covers CREATE TABLE AS SELECT and TRUNCATE.
func TestRegression_CtasAndTruncate(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE s (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)")
	mustExec(t, db, "INSERT INTO s VALUES (1,'a',10),(2,'a',20),(3,'b',30)")

	mustExec(t, db, "CREATE TABLE summary AS SELECT g, SUM(v) total FROM s GROUP BY g")
	if got := scalar(t, db, "SELECT COUNT(*) FROM summary"); got != "2" {
		t.Errorf("CTAS row count = %s, want 2", got)
	}
	if got := scalar(t, db, "SELECT total FROM summary WHERE g='b'"); got != "30" {
		t.Errorf("CTAS value = %s, want 30", got)
	}

	mustExec(t, db, "TRUNCATE TABLE summary")
	if got := scalar(t, db, "SELECT COUNT(*) FROM summary"); got != "0" {
		t.Errorf("after TRUNCATE count = %s, want 0", got)
	}
}

// TestRegression_UnqualifiedJoinColumn covers selecting an unqualified column
// that belongs to a joined table (previously dropped from the projection).
func TestRegression_UnqualifiedJoinColumn(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE a (id INTEGER, x TEXT)")
	mustExec(t, db, "CREATE TABLE b (id INTEGER, y TEXT)")
	mustExec(t, db, "INSERT INTO a VALUES (1,'a1'),(2,'a2')")
	mustExec(t, db, "INSERT INTO b VALUES (1,'b1'),(2,'b2')")

	// y is a column of b, referenced without a table prefix.
	rows := queryRows(t, db, "SELECT id, x, y FROM a JOIN b ON a.id=b.id ORDER BY id")
	if len(rows) != 2 || len(rows[0]) != 3 {
		t.Fatalf("unqualified join column produced %v (want 2 rows, 3 cols)", rows)
	}
	if got := fmt.Sprintf("%v", rows[0][2]); got != "b1" {
		t.Errorf("y = %s, want b1 (column was dropped)", got)
	}

	// Also via JOIN USING.
	u := queryRows(t, db, "SELECT id, x, y FROM a JOIN b USING(id) ORDER BY id")
	if len(u) != 2 || len(u[0]) != 3 {
		t.Fatalf("JOIN USING unqualified column produced %v", u)
	}
}

// TestRegression_QualifiedStar covers table.* expansion.
func TestRegression_QualifiedStar(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1,10),(2,20)")
	mustExec(t, db, "CREATE TABLE m (tid INTEGER, label TEXT)")
	mustExec(t, db, "INSERT INTO m VALUES (1,'a'),(2,'b')")

	// t.* in a JOIN yields only t's columns, plus the qualified m column.
	rows := queryRows(t, db, "SELECT t.*, m.label FROM t JOIN m ON t.id=m.tid ORDER BY t.id")
	if len(rows) != 2 || len(rows[0]) != 3 {
		t.Fatalf("t.* JOIN produced %v (want 2 rows, 3 cols)", rows)
	}
	// Derived-table qualified star.
	d := queryRows(t, db, "SELECT s.* FROM (SELECT id, v*2 dbl FROM t) s ORDER BY id")
	if len(d) != 2 || len(d[0]) != 2 {
		t.Fatalf("derived s.* produced %v (want 2 rows, 2 cols)", d)
	}
}

// TestRegression_SystemVariables covers @@variable references.
func TestRegression_SystemVariables(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	if got := scalar(t, db, "SELECT @@version"); got != "5.7.0-CobaltDB" {
		t.Errorf("@@version = %s", got)
	}
	if got := scalar(t, db, "SELECT @@autocommit"); got != "1" {
		t.Errorf("@@autocommit = %s", got)
	}
	// Unknown system variable resolves to NULL rather than erroring.
	if rows := queryRows(t, db, "SELECT @@nonexistent_var x"); rows[0][0] != nil {
		t.Errorf("@@nonexistent_var = %v, want NULL", rows[0][0])
	}
}

// TestRegression_MySQLSessionFunctions covers VERSION/DATABASE/USER and SHOW INDEX.
func TestRegression_MySQLSessionFunctions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	if got := scalar(t, db, "SELECT VERSION()"); got != "5.7.0-CobaltDB" {
		t.Errorf("VERSION() = %s", got)
	}
	if got := scalar(t, db, "SELECT DATABASE()"); got != "database" {
		t.Errorf("DATABASE() = %s", got)
	}
	if got := scalar(t, db, "SELECT USER()"); got != "root@localhost" {
		t.Errorf("USER() = %s", got)
	}

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "CREATE INDEX idx_name ON t(name)")
	rows := queryRows(t, db, "SHOW INDEX FROM t")
	// PRIMARY + idx_name = 2 rows.
	if len(rows) != 2 {
		t.Fatalf("SHOW INDEX returned %d rows, want 2", len(rows))
	}
}

// TestRegression_RLSFiltersPerUser verifies that row-level security policies
// actually filter rows by the per-query user. Previously the engine never
// propagated the query context to the catalog, so policies never applied.
func TestRegression_RLSFiltersPerUser(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	base := context.Background()
	mustExec(t, db, "CREATE TABLE docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "INSERT INTO docs VALUES (1,'alice'),(2,'bob'),(3,'alice')")
	mustExec(t, db, "CREATE POLICY p1 ON docs FOR ALL USING (owner = current_user())")

	owners := func(user string) string {
		ctx := context.WithValue(base, security.RLSUserKey, user)
		// The policy references `owner`, so it must be in the projection: RLS is
		// evaluated post-projection (see Known Limitations).
		rows, err := db.Query(ctx, "SELECT id, owner FROM docs ORDER BY id")
		if err != nil {
			t.Fatalf("query as %s: %v", user, err)
		}
		defer rows.Close()
		var out string
		for rows.Next() {
			var id int
			var owner string
			if err := rows.Scan(&id, &owner); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out += fmt.Sprintf("%d", id)
		}
		return out
	}

	if got := owners("alice"); got != "13" {
		t.Errorf("alice sees %q, want \"13\"", got)
	}
	if got := owners("bob"); got != "2" {
		t.Errorf("bob sees %q, want \"2\"", got)
	}
}

// TestRegression_DumpSchemaConstraints covers TableSchema/TableIndexDDL emitting
// foreign keys, composite primary keys, and secondary indexes so a SQL dump can
// restore them (previously the dumped schema dropped FKs and indexes).
func TestRegression_DumpSchemaConstraints(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT)")
	mustExec(t, db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, data TEXT)")
	mustExec(t, db, "CREATE INDEX idx_data ON child(data)")

	schema, err := db.TableSchema("child")
	if err != nil {
		t.Fatalf("TableSchema: %v", err)
	}
	if !strings.Contains(schema, "FOREIGN KEY") || !strings.Contains(schema, "REFERENCES parent") {
		t.Errorf("schema missing foreign key clause:\n%s", schema)
	}
	if !strings.Contains(schema, "ON DELETE CASCADE") {
		t.Errorf("schema missing referential action:\n%s", schema)
	}

	idx := db.TableIndexDDL("child")
	var found bool
	for _, d := range idx {
		if strings.Contains(d, "idx_data") && strings.Contains(d, "child") {
			found = true
		}
	}
	if !found {
		t.Errorf("TableIndexDDL missing idx_data: %v", idx)
	}

	// Composite primary key emits a table-level clause.
	mustExec(t, db, "CREATE TABLE ck (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a, b))")
	cs, err := db.TableSchema("ck")
	if err != nil {
		t.Fatalf("TableSchema ck: %v", err)
	}
	if !strings.Contains(cs, "PRIMARY KEY (a, b)") {
		t.Errorf("composite PK not emitted as table-level clause:\n%s", cs)
	}
}

// TestRegression_StartTransaction covers START TRANSACTION (the form drivers
// and ORMs emit for db.Begin()), previously unrecognized by the parser.
func TestRegression_StartTransaction(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1,10)")

	mustExec(t, db, "START TRANSACTION")
	mustExec(t, db, "UPDATE t SET v=99 WHERE id=1")
	mustExec(t, db, "COMMIT")
	if got := scalar(t, db, "SELECT v FROM t WHERE id=1"); got != "99" {
		t.Errorf("after START TRANSACTION/COMMIT v = %s, want 99", got)
	}

	// Rollback path.
	mustExec(t, db, "START TRANSACTION")
	mustExec(t, db, "UPDATE t SET v=0 WHERE id=1")
	mustExec(t, db, "ROLLBACK")
	if got := scalar(t, db, "SELECT v FROM t WHERE id=1"); got != "99" {
		t.Errorf("after ROLLBACK v = %s, want 99", got)
	}
}

// TestRegression_Explain covers EXPLAIN returning a plan.
func TestRegression_Explain(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1,'a')")
	rows := queryRows(t, db, "EXPLAIN SELECT * FROM t WHERE name='a'")
	if len(rows) == 0 {
		t.Fatal("EXPLAIN returned no plan rows")
	}
}
