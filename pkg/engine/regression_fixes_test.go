package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestRegression_EncryptionRoundTrip verifies that an encrypted disk database
// can be reopened with the same key and reads back its data. Previously the
// encrypted backend wrote each page's (larger) ciphertext at the raw logical
// offset, so encrypted pages overlapped on disk and corrupted each other,
// making reopen fail with "message authentication failed".
func TestRegression_EncryptionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "enc.db")
	key := make([]byte, 32)
	for i := range key {
		key[i] = 7
	}
	encOpts := func() *Options {
		return &Options{
			Security: Security{EncryptionConfig: &storage.EncryptionConfig{
				Enabled: true, Key: append([]byte(nil), key...), Algorithm: "aes-256-gcm",
			}},
		}
	}

	db, err := Open(path, encOpts())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE secret (id INTEGER PRIMARY KEY, data TEXT)")
	for i := 1; i <= 30; i++ {
		mustExec(t, db, fmt.Sprintf("INSERT INTO secret VALUES (%d, 'row-%d')", i, i))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rdb, err := Open(path, encOpts())
	if err != nil {
		t.Fatalf("reopen encrypted db failed: %v", err)
	}
	defer rdb.Close()
	if got := scalar(t, rdb, "SELECT COUNT(*) FROM secret"); got != "30" {
		t.Errorf("count after reopen = %s, want 30", got)
	}
	if got := scalar(t, rdb, "SELECT data FROM secret WHERE id=25"); got != "row-25" {
		t.Errorf("row 25 = %q, want row-25", got)
	}
}

// TestRegression_VectorDistanceFunctions verifies the scalar vector distance
// functions compute correct values and order nearest-neighbour queries.
func TestRegression_VectorDistanceFunctions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, embedding VECTOR(3))")
	mustExec(t, db, "INSERT INTO items VALUES (1,'a','[1.0,0.0,0.0]'),(2,'b','[0.0,1.0,0.0]'),(3,'c','[0.9,0.1,0.0]'),(4,'d','[0.0,0.0,1.0]')")

	// L2 distance: exact match is 0, 'c' is the closest non-exact neighbour.
	rows := queryRows(t, db, "SELECT name FROM items ORDER BY L2_DISTANCE(embedding,'[1.0,0.0,0.0]') LIMIT 2")
	if len(rows) != 2 || fmt.Sprintf("%v", rows[0][0]) != "a" || fmt.Sprintf("%v", rows[1][0]) != "c" {
		t.Errorf("L2 nearest order = %v, want [a c]", rows)
	}
	// Exact-match L2 distance is 0.
	if got := scalar(t, db, "SELECT L2_DISTANCE(embedding,'[1.0,0.0,0.0]') FROM items WHERE id=1"); got != "0" {
		t.Errorf("L2 self-distance = %s, want 0", got)
	}
	// Cosine similarity: identical direction is 1, orthogonal is 0.
	if got := scalar(t, db, "SELECT COSINE_SIMILARITY(embedding,'[1.0,0.0,0.0]') FROM items WHERE id=1"); got != "1" {
		t.Errorf("cosine self-similarity = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT COSINE_SIMILARITY(embedding,'[1.0,0.0,0.0]') FROM items WHERE id=2"); got != "0" {
		t.Errorf("cosine orthogonal = %s, want 0", got)
	}
	// Dot product.
	if got := scalar(t, db, "SELECT DOT_PRODUCT(embedding,'[1.0,0.0,0.0]') FROM items WHERE id=3"); got != "0.9" {
		t.Errorf("dot product = %s, want 0.9", got)
	}
}

// TestRegression_EncryptedCrashRecovery verifies that an encrypted database
// recovers committed writes from its (encrypted) WAL after an unclean shutdown.
// Previously the WAL record's AAD authenticated the LSN, which is patched on
// disk after encryption, so recovery failed with "message authentication
// failed".
func TestRegression_EncryptedCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	k := make([]byte, 32)
	for i := range k {
		k[i] = 7
	}
	opts := func() *Options {
		return &Options{
			Security:    Security{EncryptionConfig: &storage.EncryptionConfig{Enabled: true, Key: append([]byte(nil), k...), Algorithm: "aes-256-gcm"}},
			CoreStorage: CoreStorage{SyncMode: SyncFull},
		}
	}
	db, err := Open(src, opts())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE s (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "INSERT INTO s VALUES (1,'a'),(2,'b')")
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE s SET v='committed' WHERE id=1"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Snapshot data file, WAL, and salt sidecar (simulated crash).
	snap := filepath.Join(dir, "snap.db")
	copyFile(t, src, snap)
	copyFile(t, src+".wal", snap+".wal")
	copyFile(t, src+".salt", snap+".salt")
	db.Close()

	rdb, err := Open(snap, opts())
	if err != nil {
		t.Fatalf("reopen encrypted db after crash failed: %v", err)
	}
	defer rdb.Close()
	if got := scalar(t, rdb, "SELECT v FROM s WHERE id=1"); got != "committed" {
		t.Errorf("id=1 recovered as %q, want committed", got)
	}
	if got := scalar(t, rdb, "SELECT COUNT(*) FROM s"); got != "2" {
		t.Errorf("recovered %s rows, want 2", got)
	}
}

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

// TestRegression_AllAggregateQuantifier treats ALL as the default aggregate mode.
func TestRegression_AllAggregateQuantifier(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE agg_all_t (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO agg_all_t VALUES (1, 1), (2, 1), (3, 2), (4, NULL)")

	cases := map[string]string{
		"SELECT COUNT(ALL v) FROM agg_all_t": "3",
		"SELECT SUM(ALL v) FROM agg_all_t":   "4",
		"SELECT AVG(ALL v) FROM agg_all_t":   "1.3333333333333333",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

func TestRegression_AggregateFilterClause(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE agg_filter_t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER, keep INTEGER)")
	mustExec(t, db, "INSERT INTO agg_filter_t VALUES (1,'a',10,1),(2,'a',-5,0),(3,'a',NULL,1),(4,'b',7,1),(5,'b',3,0)")

	cases := map[string]string{
		"SELECT COUNT(*) FILTER (WHERE v > 0) FROM agg_filter_t":                                    "3",
		"SELECT SUM(v) FILTER (WHERE keep = 1) FROM agg_filter_t":                                   "17",
		"SELECT AVG(v) FILTER (WHERE g = 'b') FROM agg_filter_t":                                    "5",
		"SELECT GROUP_CONCAT(v ORDER BY id SEPARATOR '|') FILTER (WHERE g = 'b') FROM agg_filter_t": "7|3",
		"SELECT JSON_ARRAYAGG(v) FILTER (WHERE keep = 1) FROM agg_filter_t":                         "[10,null,7]",
		"SELECT COUNT(*) FILTER (WHERE v > 0) FROM (SELECT v FROM agg_filter_t) x":                  "3",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}

	rows := queryRows(t, db, "SELECT g, COUNT(*) FILTER (WHERE v > 0) FROM agg_filter_t GROUP BY g ORDER BY g")
	if len(rows) != 2 {
		t.Fatalf("grouped filter rows = %d, want 2: %v", len(rows), rows)
	}
	if got := fmt.Sprintf("%v", rows[0][1]); got != "1" {
		t.Errorf("group a count = %s, want 1", got)
	}
	if got := fmt.Sprintf("%v", rows[1][1]); got != "2" {
		t.Errorf("group b count = %s, want 2", got)
	}
}

func TestRegression_WindowAggregateFilterClause(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE win_filter_t (id INTEGER PRIMARY KEY, g TEXT, v INTEGER, keep INTEGER)")
	mustExec(t, db, "INSERT INTO win_filter_t VALUES (1,'a',10,1),(2,'a',-5,0),(3,'a',NULL,1),(4,'b',7,1),(5,'b',3,0)")

	rows := queryRows(t, db, `SELECT id,
		COUNT(*) FILTER (WHERE keep = 1) OVER (PARTITION BY g ORDER BY id),
		SUM(v) FILTER (WHERE keep = 1) OVER (PARTITION BY g),
		AVG(v) FILTER (WHERE v > 0) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
		FROM win_filter_t ORDER BY id`)
	want := [][]string{
		{"1", "1", "10", "10"},
		{"2", "1", "10", "10"},
		{"3", "2", "10", "<nil>"},
		{"4", "1", "7", "7"},
		{"5", "1", "7", "5"},
	}
	if len(rows) != len(want) {
		t.Fatalf("rows = %d, want %d: %v", len(rows), len(want), rows)
	}
	for i := range want {
		for j := range want[i] {
			if got := fmt.Sprintf("%v", rows[i][j]); got != want[i][j] {
				t.Fatalf("row %d col %d = %s, want %s; rows=%v", i, j, got, want[i][j], rows)
			}
		}
	}
}

func TestRegression_CreateOrReplaceView(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE corv_base (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO corv_base VALUES (1,'a'),(2,'b'),(3,'c')")
	mustExec(t, db, "CREATE VIEW corv_view AS SELECT id, name FROM corv_base WHERE id <= 2")
	if got := scalar(t, db, "SELECT COUNT(*) FROM corv_view"); got != "2" {
		t.Fatalf("initial view count = %s, want 2", got)
	}

	mustExec(t, db, "CREATE OR REPLACE VIEW corv_view AS SELECT id FROM corv_base WHERE id >= 2")
	if got := scalar(t, db, "SELECT COUNT(*) FROM corv_view"); got != "2" {
		t.Fatalf("replaced view count = %s, want 2", got)
	}
	if got := scalar(t, db, "SELECT MIN(id) FROM corv_view"); got != "2" {
		t.Fatalf("replaced view min id = %s, want 2", got)
	}

	if _, err := db.Exec(context.Background(), "CREATE OR REPLACE VIEW corv_base AS SELECT 1"); err == nil {
		t.Fatal("CREATE OR REPLACE VIEW over existing table succeeded, want error")
	}

	mustExec(t, db, "BEGIN")
	mustExec(t, db, "CREATE OR REPLACE VIEW corv_view AS SELECT id FROM corv_base WHERE id = 3")
	if got := scalar(t, db, "SELECT COUNT(*) FROM corv_view"); got != "1" {
		t.Fatalf("transaction replaced view count = %s, want 1", got)
	}
	mustExec(t, db, "ROLLBACK")
	if got := scalar(t, db, "SELECT MIN(id) FROM corv_view"); got != "2" {
		t.Fatalf("rollback restored view min id = %s, want 2", got)
	}
}

func TestRegression_CreateViewColumnList(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "view_columns.db")
	db, err := Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE view_col_base (id INTEGER PRIMARY KEY, raw_name TEXT)")
	mustExec(t, db, "INSERT INTO view_col_base VALUES (1, 'Ada'), (2, 'Linus')")
	mustExec(t, db, "CREATE VIEW view_col_aliases (person_id, display_name) AS SELECT id, raw_name AS ignored_alias FROM view_col_base")

	rows, err := db.Query(ctx, "SELECT * FROM view_col_aliases ORDER BY person_id")
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	if got := rows.Columns(); len(got) != 2 || got[0] != "person_id" || got[1] != "display_name" {
		t.Fatalf("view columns = %#v, want [person_id display_name]", got)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close rows: %v", err)
	}
	if got := scalar(t, db, "SELECT display_name FROM view_col_aliases WHERE person_id = 2"); got != "Linus" {
		t.Fatalf("view alias lookup = %s, want Linus", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	db, err = Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if got := scalar(t, db, "SELECT display_name FROM view_col_aliases WHERE person_id = 1"); got != "Ada" {
		t.Fatalf("reopened view alias lookup = %s, want Ada", got)
	}

	if _, err := db.Exec(ctx, "CREATE VIEW bad_view_cols (a, b) AS SELECT id FROM view_col_base"); err == nil {
		t.Fatal("expected mismatched view column list to fail")
	}
}

func TestRegression_RefreshMaterializedViewConcurrently(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE refresh_mv_src (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO refresh_mv_src VALUES (1, 'old')")
	mustExec(t, db, "CREATE MATERIALIZED VIEW refresh_mv AS SELECT id, name FROM refresh_mv_src")
	if got := scalar(t, db, "SELECT COUNT(*) FROM refresh_mv"); got != "1" {
		t.Fatalf("initial materialized view count = %s, want 1", got)
	}

	mustExec(t, db, "INSERT INTO refresh_mv_src VALUES (2, 'new')")
	mustExec(t, db, "REFRESH MATERIALIZED VIEW CONCURRENTLY refresh_mv")
	if got := scalar(t, db, "SELECT COUNT(*) FROM refresh_mv"); got != "2" {
		t.Fatalf("refreshed materialized view count = %s, want 2", got)
	}
	if got := scalar(t, db, "SELECT name FROM refresh_mv WHERE id = 2"); got != "new" {
		t.Fatalf("refreshed materialized view row = %s, want new", got)
	}
}

func TestRegression_ShowFromInSynonyms(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE show_syn_t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
	mustExec(t, db, "CREATE INDEX show_syn_idx ON show_syn_t(email)")

	if rows := queryRows(t, db, "SHOW COLUMNS IN show_syn_t"); len(rows) != 2 {
		t.Fatalf("SHOW COLUMNS IN returned %d rows, want 2: %v", len(rows), rows)
	}
	if rows := queryRows(t, db, "SHOW INDEXES IN show_syn_t"); len(rows) < 2 {
		t.Fatalf("SHOW INDEXES IN returned %d rows, want at least 2: %v", len(rows), rows)
	}
	if rows := queryRows(t, db, "SHOW KEYS IN show_syn_t"); len(rows) < 2 {
		t.Fatalf("SHOW KEYS IN returned %d rows, want at least 2: %v", len(rows), rows)
	}
}

func TestRegression_CreateIndexColumnModifiers(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE idx_mod_t (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	mustExec(t, db, "CREATE INDEX idx_mod_name ON idx_mod_t(name DESC)")
	mustExec(t, db, "CREATE INDEX idx_mod_mix ON idx_mod_t(name COLLATE NOCASE, email ASC COLLATE BINARY)")

	rows := queryRows(t, db, "SHOW INDEXES FROM idx_mod_t")
	seen := map[string]int{}
	for _, row := range rows {
		if len(row) < 5 {
			t.Fatalf("SHOW INDEXES row too short: %v", row)
		}
		keyName := fmt.Sprintf("%v", row[2])
		colName := fmt.Sprintf("%v", row[4])
		seen[keyName+":"+colName]++
	}
	for _, want := range []string{"idx_mod_name:name", "idx_mod_mix:name", "idx_mod_mix:email"} {
		if seen[want] == 0 {
			t.Fatalf("missing index column %s in SHOW INDEXES rows: %v", want, rows)
		}
	}
}

func TestRegression_GroupConcatSeparator(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE gc_sep_t (id INTEGER PRIMARY KEY, g TEXT, v TEXT)")
	mustExec(t, db, "INSERT INTO gc_sep_t VALUES (1,'a','x'),(2,'a','y'),(3,'a','x'),(4,'b','z')")

	cases := map[string]string{
		"SELECT GROUP_CONCAT(v SEPARATOR '|') FROM gc_sep_t WHERE g = 'a'":                          "x|y|x",
		"SELECT GROUP_CONCAT(v, ':') FROM gc_sep_t WHERE g = 'a'":                                   "x:y:x",
		"SELECT GROUP_CONCAT(DISTINCT v SEPARATOR '') FROM gc_sep_t WHERE g = 'a'":                  "xy",
		"SELECT GROUP_CONCAT(v SEPARATOR g) FROM gc_sep_t WHERE g = 'a'":                            "xayax",
		"SELECT GROUP_CONCAT(id ORDER BY v DESC, id ASC SEPARATOR '|') FROM gc_sep_t WHERE g = 'a'": "2|1|3",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}

	rows := queryRows(t, db, "SELECT g, GROUP_CONCAT(id ORDER BY v DESC, id ASC SEPARATOR '|') FROM gc_sep_t GROUP BY g ORDER BY g")
	if len(rows) != 2 {
		t.Fatalf("grouped rows = %d, want 2: %v", len(rows), rows)
	}
	if got := fmt.Sprintf("%v", rows[0][1]); got != "2|1|3" {
		t.Errorf("group a concat = %s, want 2|1|3", got)
	}
	if got := fmt.Sprintf("%v", rows[1][1]); got != "4" {
		t.Errorf("group b concat = %s, want 4", got)
	}
}

// TestRegression_MathFunctions covers MOD/POWER/SQRT.
func TestRegression_MathFunctions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT MOD(17,5)":         "2",
		"SELECT 17 MOD 5":          "2",
		"SELECT 20 MOD 6 + 1":      "3",
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

func TestRegression_ScalarMinMax(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	cases := map[string]string{
		"SELECT MAX(10,20)":       "20",
		"SELECT MIN(10,20)":       "10",
		"SELECT MAX(10,NULL,20)":  "20",
		"SELECT MIN(NULL,10,20)":  "10",
		"SELECT MAX('a','c','b')": "c",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}

	mustExec(t, db, "CREATE TABLE scalar_minmax_t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	mustExec(t, db, "INSERT INTO scalar_minmax_t VALUES (1, 5, 9), (2, 7, 3)")
	rows := queryRows(t, db, "SELECT MAX(a,b), MIN(a,b) FROM scalar_minmax_t ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2: %v", len(rows), rows)
	}
	if got := fmt.Sprintf("%v,%v", rows[0][0], rows[0][1]); got != "9,5" {
		t.Errorf("row 1 scalar MIN/MAX = %s, want 9,5", got)
	}
	if got := fmt.Sprintf("%v,%v", rows[1][0], rows[1][1]); got != "7,3" {
		t.Errorf("row 2 scalar MIN/MAX = %s, want 7,3", got)
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

func TestRegression_JSONAggregates(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE json_agg_t (id INTEGER PRIMARY KEY, k TEXT, v INTEGER)")
	mustExec(t, db, "INSERT INTO json_agg_t VALUES (1,'a',2),(2,'b',3),(3,'c',NULL)")

	if got := scalar(t, db, "SELECT JSON_ARRAYAGG(v) FROM json_agg_t"); got != "[2,3,null]" {
		t.Errorf("JSON_ARRAYAGG = %s, want [2,3,null]", got)
	}
	if got := scalar(t, db, "SELECT JSON_OBJECTAGG(k, v) FROM json_agg_t WHERE v IS NOT NULL"); got != `{"a":2,"b":3}` {
		t.Errorf("JSON_OBJECTAGG = %s, want {\"a\":2,\"b\":3}", got)
	}
	rows := queryRows(t, db, "SELECT k, JSON_ARRAYAGG(v) FROM json_agg_t GROUP BY k ORDER BY k")
	if len(rows) != 3 {
		t.Fatalf("grouped JSON_ARRAYAGG rows = %d, want 3: %v", len(rows), rows)
	}
	if got := fmt.Sprintf("%v", rows[2][1]); got != "[null]" {
		t.Errorf("grouped JSON_ARRAYAGG null = %s, want [null]", got)
	}
}

func TestRegression_CreateTableColumnCollate(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "column_collate.db")
	db, err := Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mustExec(t, db, "CREATE TABLE collated_people (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE, tag TEXT COLLATE BINARY)")
	mustExec(t, db, "INSERT INTO collated_people VALUES (1, 'Ada', 'A')")
	schema, err := db.TableSchema("collated_people")
	if err != nil {
		t.Fatalf("TableSchema: %v", err)
	}
	for _, want := range []string{"name TEXT COLLATE NOCASE", "tag TEXT COLLATE BINARY"} {
		if !strings.Contains(schema, want) {
			t.Fatalf("schema %q missing %q", schema, want)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if got := scalar(t, db, "SELECT name FROM collated_people WHERE id = 1"); got != "Ada" {
		t.Fatalf("reopened row = %s, want Ada", got)
	}
	schema, err = db.TableSchema("collated_people")
	if err != nil {
		t.Fatalf("reopened TableSchema: %v", err)
	}
	if !strings.Contains(schema, "name TEXT COLLATE NOCASE") || !strings.Contains(schema, "tag TEXT COLLATE BINARY") {
		t.Fatalf("reopened schema lost collations: %q", schema)
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

func TestRegression_DerivedTableWithoutAlias(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE derived_no_alias (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO derived_no_alias VALUES (1,10),(2,20),(3,30)")

	if got := scalar(t, db, "SELECT SUM(v) FROM (SELECT v FROM derived_no_alias WHERE id >= 2)"); got != "50" {
		t.Fatalf("SUM from aliasless derived table = %s, want 50", got)
	}
	rows := queryRows(t, db, "SELECT v FROM (SELECT v FROM derived_no_alias WHERE v > 10) ORDER BY v")
	if len(rows) != 2 {
		t.Fatalf("aliasless derived rows = %v, want 2 rows", rows)
	}
	if got := fmt.Sprintf("%v", rows[0][0]); got != "20" {
		t.Fatalf("first aliasless derived row = %s, want 20", got)
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

// TestRegression_IsDistinctFrom covers standard SQL null-aware distinctness.
func TestRegression_IsDistinctFrom(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT NULL IS DISTINCT FROM NULL":     "false",
		"SELECT NULL IS DISTINCT FROM 1":        "true",
		"SELECT 1 IS DISTINCT FROM NULL":        "true",
		"SELECT 1 IS DISTINCT FROM 1":           "false",
		"SELECT 1 IS DISTINCT FROM 2":           "true",
		"SELECT NULL IS NOT DISTINCT FROM NULL": "true",
		"SELECT NULL IS NOT DISTINCT FROM 1":    "false",
		"SELECT 1 IS NOT DISTINCT FROM 1":       "true",
		"SELECT 1 IS NOT DISTINCT FROM 2":       "false",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

// TestRegression_RegexpOperator covers MySQL-style REGEXP/RLIKE predicates.
func TestRegression_RegexpOperator(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT 'abc123' REGEXP '[0-9]+'":     "true",
		"SELECT 'abcdef' REGEXP '[0-9]+'":     "false",
		"SELECT 'abcdef' NOT REGEXP '[0-9]+'": "true",
		"SELECT 'abc123' NOT REGEXP '[0-9]+'": "false",
		"SELECT 'abc123' RLIKE '[a-z]+'":      "true",
		"SELECT NULL REGEXP '[0-9]+'":         "<nil>",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

func TestRegression_GlobOperator(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT 'file.txt' GLOB '*.txt'":     "true",
		"SELECT 'file.csv' GLOB '*.txt'":     "false",
		"SELECT 'file.csv' NOT GLOB '*.txt'": "true",
		"SELECT 'file.txt' NOT GLOB '*.txt'": "false",
		"SELECT NULL GLOB '*.txt'":           "<nil>",
		"SELECT 'file.txt' GLOB NULL":        "<nil>",
		"SELECT GLOB('*.txt', 'file.txt')":   "true",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

func TestRegression_SelectLockingClauses(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE lock_src (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO lock_src VALUES (1, 'a'), (2, 'b')")

	cases := []string{
		"SELECT id FROM lock_src WHERE id = 1 FOR UPDATE",
		"SELECT id FROM lock_src WHERE id = 1 FOR SHARE",
		"SELECT id FROM lock_src WHERE id = 1 FOR UPDATE OF lock_src NOWAIT",
		"SELECT id FROM lock_src WHERE id = 1 FOR UPDATE SKIP LOCKED",
		"SELECT id FROM lock_src WHERE id = 1 FOR UPDATE WAIT 1",
	}
	for _, sql := range cases {
		if got := scalar(t, db, sql); got != "1" {
			t.Errorf("%q = %s, want 1", sql, got)
		}
	}
}

func TestRegression_CreateTemporaryTable(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "temp_table.db")
	db, err := Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mustExec(t, db, "CREATE TEMPORARY TABLE session_items (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	mustExec(t, db, "CREATE INDEX session_items_name_idx ON session_items(name)")
	mustExec(t, db, "INSERT INTO session_items VALUES (1, 'one')")
	if got := scalar(t, db, "SELECT name FROM session_items WHERE id = 1"); got != "one" {
		t.Fatalf("temporary table scalar = %s, want one", got)
	}
	if rows := queryRows(t, db, "SHOW TABLES"); len(rows) != 1 || fmt.Sprintf("%v", rows[0][0]) != "session_items" {
		t.Fatalf("SHOW TABLES while open = %v, want session_items", rows)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES after reopen: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		vals := make([]interface{}, len(rows.Columns()))
		ptrs := make([]interface{}, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan SHOW TABLES after reopen: %v", err)
		}
		t.Fatalf("temporary table persisted after reopen: %v", vals)
	}
	if _, err := db.Query(ctx, "SELECT * FROM session_items"); err == nil {
		t.Fatal("temporary table was queryable after reopen")
	}
}

func TestRegression_CreateTemporaryView(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "temp_view.db")
	db, err := Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mustExec(t, db, "CREATE TABLE temp_view_base (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO temp_view_base VALUES (1, 'one'), (2, 'two')")
	mustExec(t, db, "CREATE TEMP VIEW session_view AS SELECT name FROM temp_view_base WHERE id = 2")
	if got := scalar(t, db, "SELECT name FROM session_view"); got != "two" {
		t.Fatalf("temporary view scalar = %s, want two", got)
	}
	mustExec(t, db, "CREATE VIEW replace_view AS SELECT name FROM temp_view_base WHERE id = 1")
	mustExec(t, db, "CREATE OR REPLACE TEMP VIEW replace_view AS SELECT name FROM temp_view_base WHERE id = 2")
	if got := scalar(t, db, "SELECT name FROM replace_view"); got != "two" {
		t.Fatalf("temporary replaced view scalar = %s, want two", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if got := scalar(t, db, "SELECT name FROM temp_view_base WHERE id = 1"); got != "one" {
		t.Fatalf("base table scalar after reopen = %s, want one", got)
	}
	if _, err := db.Query(ctx, "SELECT * FROM session_view"); err == nil {
		t.Fatal("temporary view was queryable after reopen")
	}
	if _, err := db.Query(ctx, "SELECT * FROM replace_view"); err == nil {
		t.Fatal("temporary replacement view was queryable after reopen")
	}
}

func TestRegression_CreateDropCollection(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "collection.db")
	db, err := Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	mustExec(t, db, "CREATE COLLECTION docs")
	mustExec(t, db, "CREATE COLLECTION IF NOT EXISTS docs")
	if rows := queryRows(t, db, "SHOW TABLES"); len(rows) != 1 || fmt.Sprintf("%v", rows[0][0]) != "docs" {
		t.Fatalf("SHOW TABLES after create collection = %v, want docs", rows)
	}
	if _, err := db.Exec(ctx, "CREATE COLLECTION docs"); err == nil {
		t.Fatal("duplicate CREATE COLLECTION succeeded")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(dbPath, &Options{CoreStorage: CoreStorage{CacheSize: 1024}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if rows := queryRows(t, db, "SHOW TABLES"); len(rows) != 1 || fmt.Sprintf("%v", rows[0][0]) != "docs" {
		t.Fatalf("SHOW TABLES after reopen = %v, want docs", rows)
	}
	mustExec(t, db, "DROP COLLECTION docs")
	if rows := queryRows(t, db, "SHOW TABLES"); len(rows) != 0 {
		t.Fatalf("SHOW TABLES after drop collection = %v, want empty", rows)
	}
	mustExec(t, db, "DROP COLLECTION IF EXISTS docs")
	if _, err := db.Exec(ctx, "DROP COLLECTION docs"); err == nil {
		t.Fatal("DROP COLLECTION of missing collection succeeded")
	}
}

func TestRegression_CallProcedureNamedArgs(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE call_named_args (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	mustExec(t, db, "CREATE PROCEDURE insert_named(p_id INTEGER, p_name TEXT, p_score INTEGER) BEGIN INSERT INTO call_named_args VALUES (p_id, p_name, p_score); END")
	mustExec(t, db, "CALL insert_named(p_score => 42, p_id => 1, p_name => 'Ada')")
	if got := scalar(t, db, "SELECT name FROM call_named_args WHERE id = 1"); got != "Ada" {
		t.Fatalf("named arg name = %s, want Ada", got)
	}
	if got := scalar(t, db, "SELECT score FROM call_named_args WHERE id = 1"); got != "42" {
		t.Fatalf("named arg score = %s, want 42", got)
	}

	if _, err := db.Exec(ctx, "CALL insert_named(p_id => 2, p_id => 3, p_name => 'dup')"); err == nil || !strings.Contains(err.Error(), "assigned more than once") {
		t.Fatalf("duplicate named arg error = %v, want assigned more than once", err)
	}
	if _, err := db.Exec(ctx, "CALL insert_named(nope => 2, p_name => 'bad', p_score => 1)"); err == nil || !strings.Contains(err.Error(), "no parameter named nope") {
		t.Fatalf("unknown named arg error = %v, want no parameter named nope", err)
	}
}

// TestRegression_IsBooleanPredicates covers SQL boolean IS predicates.
func TestRegression_IsBooleanPredicates(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	cases := map[string]string{
		"SELECT TRUE IS TRUE":        "true",
		"SELECT FALSE IS TRUE":       "false",
		"SELECT NULL IS TRUE":        "false",
		"SELECT TRUE IS NOT TRUE":    "false",
		"SELECT FALSE IS NOT TRUE":   "true",
		"SELECT NULL IS NOT TRUE":    "true",
		"SELECT TRUE IS FALSE":       "false",
		"SELECT FALSE IS FALSE":      "true",
		"SELECT NULL IS FALSE":       "false",
		"SELECT TRUE IS NOT FALSE":   "true",
		"SELECT FALSE IS NOT FALSE":  "false",
		"SELECT NULL IS NOT FALSE":   "true",
		"SELECT TRUE IS UNKNOWN":     "false",
		"SELECT FALSE IS UNKNOWN":    "false",
		"SELECT NULL IS UNKNOWN":     "true",
		"SELECT TRUE IS NOT UNKNOWN": "true",
		"SELECT NULL IS NOT UNKNOWN": "false",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}
}

// TestRegression_LimitAll treats LIMIT ALL as an explicit no-limit clause.
func TestRegression_LimitAll(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE limit_all_t (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "INSERT INTO limit_all_t VALUES (1), (2), (3)")

	rows := queryRows(t, db, "SELECT id FROM limit_all_t ORDER BY id LIMIT ALL")
	if len(rows) != 3 {
		t.Fatalf("LIMIT ALL row count = %d, want 3: %#v", len(rows), rows)
	}

	rows = queryRows(t, db, "SELECT id FROM limit_all_t ORDER BY id LIMIT ALL OFFSET 1")
	if len(rows) != 2 {
		t.Fatalf("LIMIT ALL OFFSET row count = %d, want 2: %#v", len(rows), rows)
	}
}

// TestRegression_OnDuplicateKeyUpdate covers MySQL-style upsert syntax.
func TestRegression_OnDuplicateKeyUpdate(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_upsert (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)")
	mustExec(t, db, "INSERT INTO mysql_upsert VALUES (1, 10, 'old')")

	mustExec(t, db, "INSERT INTO mysql_upsert VALUES (1, 20, 'incoming') ON DUPLICATE KEY UPDATE v = 30, note = 'updated'")
	if got := scalar(t, db, "SELECT v FROM mysql_upsert WHERE id = 1"); got != "30" {
		t.Fatalf("duplicate update v = %s, want 30", got)
	}
	if got := scalar(t, db, "SELECT note FROM mysql_upsert WHERE id = 1"); got != "updated" {
		t.Fatalf("duplicate update note = %s, want updated", got)
	}

	mustExec(t, db, "INSERT INTO mysql_upsert VALUES (2, 40, 'new') ON DUPLICATE KEY UPDATE v = 50")
	if got := scalar(t, db, "SELECT v FROM mysql_upsert WHERE id = 2"); got != "40" {
		t.Fatalf("non-conflicting insert v = %s, want 40", got)
	}
}

func TestRegression_OnDuplicateKeyUpdateUniqueColumn(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_upsert_unique (id INTEGER PRIMARY KEY, email TEXT UNIQUE, hits INTEGER)")
	mustExec(t, db, "INSERT INTO mysql_upsert_unique VALUES (1, 'a@example.com', 1)")

	mustExec(t, db, "INSERT INTO mysql_upsert_unique VALUES (2, 'a@example.com', 10) ON DUPLICATE KEY UPDATE hits = 2")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_upsert_unique"); got != "1" {
		t.Fatalf("unique conflict row count = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT id FROM mysql_upsert_unique WHERE email = 'a@example.com'"); got != "1" {
		t.Fatalf("unique conflict id = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT hits FROM mysql_upsert_unique WHERE email = 'a@example.com'"); got != "2" {
		t.Fatalf("unique conflict hits = %s, want 2", got)
	}
}

func TestRegression_OnDuplicateKeyUpdateUniqueIndex(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_upsert_unique_idx (id INTEGER PRIMARY KEY, email TEXT, hits INTEGER)")
	mustExec(t, db, "CREATE UNIQUE INDEX mysql_upsert_email_idx ON mysql_upsert_unique_idx(email)")
	mustExec(t, db, "INSERT INTO mysql_upsert_unique_idx VALUES (1, 'a@example.com', 1)")

	mustExec(t, db, "INSERT INTO mysql_upsert_unique_idx VALUES (2, 'a@example.com', 10) ON DUPLICATE KEY UPDATE hits = 3")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_upsert_unique_idx"); got != "1" {
		t.Fatalf("unique index conflict row count = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT id FROM mysql_upsert_unique_idx WHERE email = 'a@example.com'"); got != "1" {
		t.Fatalf("unique index conflict id = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT hits FROM mysql_upsert_unique_idx WHERE email = 'a@example.com'"); got != "3" {
		t.Fatalf("unique index conflict hits = %s, want 3", got)
	}
}

func TestRegression_OnDuplicateKeyUpdateValuesFunction(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_upsert_values (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)")
	mustExec(t, db, "INSERT INTO mysql_upsert_values VALUES (1, 10, 'old')")

	mustExec(t, db, "INSERT INTO mysql_upsert_values VALUES (1, 20, 'incoming') ON DUPLICATE KEY UPDATE v = VALUES(v), note = VALUES(note)")
	if got := scalar(t, db, "SELECT v FROM mysql_upsert_values WHERE id = 1"); got != "20" {
		t.Fatalf("VALUES(v) update = %s, want 20", got)
	}
	if got := scalar(t, db, "SELECT note FROM mysql_upsert_values WHERE id = 1"); got != "incoming" {
		t.Fatalf("VALUES(note) update = %s, want incoming", got)
	}
}

func TestRegression_OnDuplicateKeyUpdateValuesFunctionUniqueColumn(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_upsert_values_unique (id INTEGER PRIMARY KEY, email TEXT UNIQUE, hits INTEGER)")
	mustExec(t, db, "INSERT INTO mysql_upsert_values_unique VALUES (1, 'a@example.com', 1)")

	mustExec(t, db, "INSERT INTO mysql_upsert_values_unique VALUES (2, 'a@example.com', 10) ON DUPLICATE KEY UPDATE hits = VALUES(hits) + 1")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_upsert_values_unique"); got != "1" {
		t.Fatalf("unique VALUES() conflict row count = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT hits FROM mysql_upsert_values_unique WHERE email = 'a@example.com'"); got != "11" {
		t.Fatalf("unique VALUES(hits) update = %s, want 11", got)
	}
}

func TestRegression_InsertSetSyntax(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_insert_set (id INTEGER PRIMARY KEY, name TEXT, score INTEGER DEFAULT 7)")

	mustExec(t, db, "INSERT INTO mysql_insert_set SET id = 1, name = 'Ada'")
	if got := scalar(t, db, "SELECT name FROM mysql_insert_set WHERE id = 1"); got != "Ada" {
		t.Fatalf("INSERT SET name = %s, want Ada", got)
	}
	if got := scalar(t, db, "SELECT score FROM mysql_insert_set WHERE id = 1"); got != "7" {
		t.Fatalf("INSERT SET default score = %s, want 7", got)
	}
}

func TestRegression_InsertSetOnDuplicateKeyUpdate(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_insert_set_upsert (id INTEGER PRIMARY KEY, hits INTEGER, note TEXT)")
	mustExec(t, db, "INSERT INTO mysql_insert_set_upsert SET id = 1, hits = 1, note = 'old'")

	mustExec(t, db, "INSERT INTO mysql_insert_set_upsert SET id = 1, hits = 4, note = 'new' ON DUPLICATE KEY UPDATE hits = VALUES(hits), note = VALUES(note)")
	if got := scalar(t, db, "SELECT hits FROM mysql_insert_set_upsert WHERE id = 1"); got != "4" {
		t.Fatalf("INSERT SET upsert hits = %s, want 4", got)
	}
	if got := scalar(t, db, "SELECT note FROM mysql_insert_set_upsert WHERE id = 1"); got != "new" {
		t.Fatalf("INSERT SET upsert note = %s, want new", got)
	}
}

func TestRegression_ReplaceInto(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_replace (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
	mustExec(t, db, "INSERT INTO mysql_replace VALUES (1, 'a@example.com', 'Alice')")
	mustExec(t, db, "INSERT INTO mysql_replace VALUES (2, 'b@example.com', 'Bob')")

	mustExec(t, db, "REPLACE INTO mysql_replace VALUES (1, 'a2@example.com', 'Alice Updated')")
	if got := scalar(t, db, "SELECT email FROM mysql_replace WHERE id = 1"); got != "a2@example.com" {
		t.Fatalf("REPLACE INTO primary-key email = %s, want a2@example.com", got)
	}
	if got := scalar(t, db, "SELECT name FROM mysql_replace WHERE id = 1"); got != "Alice Updated" {
		t.Fatalf("REPLACE INTO primary-key name = %s, want Alice Updated", got)
	}

	mustExec(t, db, "REPLACE INTO mysql_replace VALUES (3, 'b@example.com', 'Bob Replaced')")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_replace WHERE email = 'b@example.com'"); got != "1" {
		t.Fatalf("REPLACE INTO unique conflict count = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT id FROM mysql_replace WHERE email = 'b@example.com'"); got != "3" {
		t.Fatalf("REPLACE INTO unique conflict id = %s, want 3", got)
	}
}

func TestRegression_ReplaceSetSyntax(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_replace_set (id INTEGER PRIMARY KEY, name TEXT, score INTEGER DEFAULT 7)")
	mustExec(t, db, "INSERT INTO mysql_replace_set VALUES (1, 'old', 1)")

	mustExec(t, db, "REPLACE mysql_replace_set SET id = 1, name = 'new'")
	if got := scalar(t, db, "SELECT name FROM mysql_replace_set WHERE id = 1"); got != "new" {
		t.Fatalf("REPLACE SET name = %s, want new", got)
	}
	if got := scalar(t, db, "SELECT score FROM mysql_replace_set WHERE id = 1"); got != "7" {
		t.Fatalf("REPLACE SET default score = %s, want 7", got)
	}
}

func TestRegression_InsertIgnoreMySQLSyntax(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_insert_ignore (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
	mustExec(t, db, "INSERT INTO mysql_insert_ignore VALUES (1, 'a@example.com', 'Alice')")
	mustExec(t, db, "INSERT INTO mysql_insert_ignore VALUES (2, 'b@example.com', 'Bob')")

	mustExec(t, db, "INSERT IGNORE INTO mysql_insert_ignore VALUES (1, 'new-a@example.com', 'Alice New')")
	if got := scalar(t, db, "SELECT email FROM mysql_insert_ignore WHERE id = 1"); got != "a@example.com" {
		t.Fatalf("INSERT IGNORE primary-key email = %s, want a@example.com", got)
	}
	if got := scalar(t, db, "SELECT name FROM mysql_insert_ignore WHERE id = 1"); got != "Alice" {
		t.Fatalf("INSERT IGNORE primary-key name = %s, want Alice", got)
	}

	mustExec(t, db, "INSERT IGNORE INTO mysql_insert_ignore VALUES (3, 'b@example.com', 'Bob New')")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_insert_ignore"); got != "2" {
		t.Fatalf("INSERT IGNORE unique conflict count = %s, want 2", got)
	}

	mustExec(t, db, "INSERT IGNORE INTO mysql_insert_ignore VALUES (4, 'd@example.com', 'Dana')")
	if got := scalar(t, db, "SELECT name FROM mysql_insert_ignore WHERE id = 4"); got != "Dana" {
		t.Fatalf("INSERT IGNORE new row name = %s, want Dana", got)
	}
}

func TestRegression_InsertIgnoreSetSyntax(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_insert_ignore_set (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO mysql_insert_ignore_set VALUES (1, 'old')")

	mustExec(t, db, "INSERT IGNORE INTO mysql_insert_ignore_set SET id = 1, name = 'ignored'")
	if got := scalar(t, db, "SELECT name FROM mysql_insert_ignore_set WHERE id = 1"); got != "old" {
		t.Fatalf("INSERT IGNORE SET existing name = %s, want old", got)
	}
}

func TestRegression_InsertOrRollbackAbortFail(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE insert_or_conflict (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO insert_or_conflict VALUES (1, 'base')")

	mustExec(t, db, "BEGIN")
	mustExec(t, db, "INSERT INTO insert_or_conflict VALUES (2, 'pending')")
	if _, err := db.Exec(context.Background(), "INSERT OR ROLLBACK INTO insert_or_conflict VALUES (1, 'dup')"); err == nil {
		t.Fatal("INSERT OR ROLLBACK duplicate should fail")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM insert_or_conflict"); got != "1" {
		t.Fatalf("INSERT OR ROLLBACK count = %s, want 1", got)
	}
	if _, err := db.Exec(context.Background(), "COMMIT"); err == nil {
		t.Fatal("COMMIT after INSERT OR ROLLBACK conflict should fail because transaction was rolled back")
	}

	for _, action := range []string{"ABORT", "FAIL"} {
		mustExec(t, db, "BEGIN")
		mustExec(t, db, "INSERT INTO insert_or_conflict VALUES (2, '"+strings.ToLower(action)+"')")
		if _, err := db.Exec(context.Background(), "INSERT OR "+action+" INTO insert_or_conflict VALUES (1, 'dup')"); err == nil {
			t.Fatalf("INSERT OR %s duplicate should fail", action)
		}
		mustExec(t, db, "COMMIT")
		if got := scalar(t, db, "SELECT name FROM insert_or_conflict WHERE id = 2"); got != strings.ToLower(action) {
			t.Fatalf("INSERT OR %s left transaction state = %s, want %s", action, got, strings.ToLower(action))
		}
		mustExec(t, db, "DELETE FROM insert_or_conflict WHERE id = 2")
	}
}

func TestRegression_InsertDefaultValues(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE default_values_t (id INTEGER PRIMARY KEY AUTO_INCREMENT, status TEXT DEFAULT 'active', score INTEGER DEFAULT 7)")

	mustExec(t, db, "INSERT INTO default_values_t DEFAULT VALUES")
	if got := scalar(t, db, "SELECT id FROM default_values_t"); got != "1" {
		t.Fatalf("DEFAULT VALUES auto id = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT status FROM default_values_t WHERE id = 1"); got != "active" {
		t.Fatalf("DEFAULT VALUES status = %s, want active", got)
	}
	if got := scalar(t, db, "SELECT score FROM default_values_t WHERE id = 1"); got != "7" {
		t.Fatalf("DEFAULT VALUES score = %s, want 7", got)
	}
}

func TestRegression_MySQLInsertPriorityModifiers(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_insert_mod (id INTEGER PRIMARY KEY, name TEXT)")

	mustExec(t, db, "INSERT LOW_PRIORITY INTO mysql_insert_mod VALUES (1, 'low')")
	if got := scalar(t, db, "SELECT name FROM mysql_insert_mod WHERE id = 1"); got != "low" {
		t.Fatalf("INSERT LOW_PRIORITY name = %s, want low", got)
	}

	mustExec(t, db, "INSERT HIGH_PRIORITY IGNORE INTO mysql_insert_mod VALUES (1, 'ignored')")
	if got := scalar(t, db, "SELECT name FROM mysql_insert_mod WHERE id = 1"); got != "low" {
		t.Fatalf("INSERT HIGH_PRIORITY IGNORE name = %s, want low", got)
	}

	mustExec(t, db, "INSERT DELAYED INTO mysql_insert_mod VALUES (2, 'delayed')")
	if got := scalar(t, db, "SELECT name FROM mysql_insert_mod WHERE id = 2"); got != "delayed" {
		t.Fatalf("INSERT DELAYED name = %s, want delayed", got)
	}
}

func TestRegression_MySQLReplacePriorityModifiers(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_replace_mod (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO mysql_replace_mod VALUES (1, 'old')")

	mustExec(t, db, "REPLACE LOW_PRIORITY INTO mysql_replace_mod VALUES (1, 'low')")
	if got := scalar(t, db, "SELECT name FROM mysql_replace_mod WHERE id = 1"); got != "low" {
		t.Fatalf("REPLACE LOW_PRIORITY name = %s, want low", got)
	}

	mustExec(t, db, "REPLACE DELAYED mysql_replace_mod SET id = 1, name = 'delayed'")
	if got := scalar(t, db, "SELECT name FROM mysql_replace_mod WHERE id = 1"); got != "delayed" {
		t.Fatalf("REPLACE DELAYED name = %s, want delayed", got)
	}
}

func TestRegression_MySQLUpdateLowPriorityModifier(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_update_mod (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO mysql_update_mod VALUES (1, 'old')")

	mustExec(t, db, "UPDATE LOW_PRIORITY mysql_update_mod SET name = 'new' WHERE id = 1")
	if got := scalar(t, db, "SELECT name FROM mysql_update_mod WHERE id = 1"); got != "new" {
		t.Fatalf("UPDATE LOW_PRIORITY name = %s, want new", got)
	}
}

func TestRegression_UpdateTargetAlias(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE update_alias (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)")
	mustExec(t, db, "INSERT INTO update_alias VALUES (1, 10, 'old'), (2, 20, 'keep')")

	mustExec(t, db, "UPDATE update_alias AS a SET a.v = 100, note = 'alias' WHERE a.id = 1")

	if got := scalar(t, db, "SELECT v FROM update_alias WHERE id = 1"); got != "100" {
		t.Fatalf("updated row v = %s, want 100", got)
	}
	if got := scalar(t, db, "SELECT note FROM update_alias WHERE id = 1"); got != "alias" {
		t.Fatalf("updated row note = %s, want alias", got)
	}
	if got := scalar(t, db, "SELECT v FROM update_alias WHERE id = 2"); got != "20" {
		t.Fatalf("unchanged row v = %s, want 20", got)
	}
}

func TestRegression_UpdateSetList(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE update_set_list (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER, note TEXT)")
	mustExec(t, db, "INSERT INTO update_set_list VALUES (1, 10, 20, 'old'), (2, 30, 40, 'keep')")

	mustExec(t, db, "UPDATE update_set_list SET (x, y) = (100, x + y) WHERE id = 1")
	if got := scalar(t, db, "SELECT x FROM update_set_list WHERE id = 1"); got != "100" {
		t.Fatalf("updated x = %s, want 100", got)
	}
	if got := scalar(t, db, "SELECT y FROM update_set_list WHERE id = 1"); got != "30" {
		t.Fatalf("updated y = %s, want 30", got)
	}
	if got := scalar(t, db, "SELECT x FROM update_set_list WHERE id = 2"); got != "30" {
		t.Fatalf("unchanged x = %s, want 30", got)
	}

	if _, err := db.Exec(context.Background(), "UPDATE update_set_list SET (x, y) = (1) WHERE id = 1"); err == nil || !strings.Contains(err.Error(), "SET column list has 2 columns") {
		t.Fatalf("mismatched SET list error = %v, want column/value count error", err)
	}
}

func TestRegression_MySQLUpdateJoin(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_update_join (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)")
	mustExec(t, db, "CREATE TABLE mysql_update_join_src (id INTEGER PRIMARY KEY, new_v INTEGER, flag INTEGER)")
	mustExec(t, db, "INSERT INTO mysql_update_join VALUES (1, 10, 'old'), (2, 20, 'keep')")
	mustExec(t, db, "INSERT INTO mysql_update_join_src VALUES (1, 100, 1), (2, 200, 0)")

	mustExec(t, db, "UPDATE mysql_update_join JOIN mysql_update_join_src ON mysql_update_join.id = mysql_update_join_src.id SET mysql_update_join.v = mysql_update_join_src.new_v, note = 'updated' WHERE mysql_update_join_src.flag = 1")

	if got := scalar(t, db, "SELECT v FROM mysql_update_join WHERE id = 1"); got != "100" {
		t.Fatalf("updated row v = %s, want 100", got)
	}
	if got := scalar(t, db, "SELECT note FROM mysql_update_join WHERE id = 1"); got != "updated" {
		t.Fatalf("updated row note = %s, want updated", got)
	}
	if got := scalar(t, db, "SELECT v FROM mysql_update_join WHERE id = 2"); got != "20" {
		t.Fatalf("unchanged row v = %s, want 20", got)
	}
}

func TestRegression_MySQLUpdateJoinTargetAlias(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_update_join_alias (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)")
	mustExec(t, db, "CREATE TABLE mysql_update_join_alias_src (id INTEGER PRIMARY KEY, new_v INTEGER, flag INTEGER)")
	mustExec(t, db, "INSERT INTO mysql_update_join_alias VALUES (1, 10, 'old'), (2, 20, 'keep')")
	mustExec(t, db, "INSERT INTO mysql_update_join_alias_src VALUES (1, 100, 1), (2, 200, 0)")

	mustExec(t, db, "UPDATE mysql_update_join_alias AS t JOIN mysql_update_join_alias_src AS s ON t.id = s.id SET t.v = s.new_v, note = 'alias-join' WHERE s.flag = 1")

	if got := scalar(t, db, "SELECT v FROM mysql_update_join_alias WHERE id = 1"); got != "100" {
		t.Fatalf("updated row v = %s, want 100", got)
	}
	if got := scalar(t, db, "SELECT note FROM mysql_update_join_alias WHERE id = 1"); got != "alias-join" {
		t.Fatalf("updated row note = %s, want alias-join", got)
	}
	if got := scalar(t, db, "SELECT v FROM mysql_update_join_alias WHERE id = 2"); got != "20" {
		t.Fatalf("unchanged row v = %s, want 20", got)
	}
}

func TestRegression_MySQLUpdateCommaJoin(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_update_comma (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)")
	mustExec(t, db, "CREATE TABLE mysql_update_comma_src (id INTEGER PRIMARY KEY, new_v INTEGER, flag INTEGER)")
	mustExec(t, db, "INSERT INTO mysql_update_comma VALUES (1, 10, 'old'), (2, 20, 'keep')")
	mustExec(t, db, "INSERT INTO mysql_update_comma_src VALUES (1, 100, 1), (2, 200, 0)")

	mustExec(t, db, "UPDATE mysql_update_comma, mysql_update_comma_src SET mysql_update_comma.v = mysql_update_comma_src.new_v, note = 'comma' WHERE mysql_update_comma.id = mysql_update_comma_src.id AND mysql_update_comma_src.flag = 1")

	if got := scalar(t, db, "SELECT v FROM mysql_update_comma WHERE id = 1"); got != "100" {
		t.Fatalf("updated row v = %s, want 100", got)
	}
	if got := scalar(t, db, "SELECT note FROM mysql_update_comma WHERE id = 1"); got != "comma" {
		t.Fatalf("updated row note = %s, want comma", got)
	}
	if got := scalar(t, db, "SELECT v FROM mysql_update_comma WHERE id = 2"); got != "20" {
		t.Fatalf("unchanged row v = %s, want 20", got)
	}
}

func TestRegression_MySQLDeleteModifiers(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_delete_mod (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO mysql_delete_mod VALUES (1, 'low'), (2, 'quick'), (3, 'both')")

	mustExec(t, db, "DELETE LOW_PRIORITY FROM mysql_delete_mod WHERE id = 1")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_mod WHERE id = 1"); got != "0" {
		t.Fatalf("DELETE LOW_PRIORITY count = %s, want 0", got)
	}

	mustExec(t, db, "DELETE QUICK FROM mysql_delete_mod WHERE id = 2")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_mod WHERE id = 2"); got != "0" {
		t.Fatalf("DELETE QUICK count = %s, want 0", got)
	}

	mustExec(t, db, "DELETE LOW_PRIORITY QUICK FROM mysql_delete_mod WHERE id = 3")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_mod"); got != "0" {
		t.Fatalf("DELETE LOW_PRIORITY QUICK total count = %s, want 0", got)
	}
}

func TestRegression_DeleteTargetAlias(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE delete_alias (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO delete_alias VALUES (1, 'keep'), (2, 'remove')")

	mustExec(t, db, "DELETE FROM delete_alias AS d WHERE d.id = 2")
	if got := scalar(t, db, "SELECT COUNT(*) FROM delete_alias"); got != "1" {
		t.Fatalf("DELETE alias row count = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM delete_alias WHERE id = 2"); got != "0" {
		t.Fatalf("DELETE alias removed id=2 count = %s, want 0", got)
	}
}

func TestRegression_MySQLTargetedDeleteFromJoin(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_delete_target (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "CREATE TABLE mysql_delete_ref (id INTEGER PRIMARY KEY, flag INTEGER)")
	mustExec(t, db, "INSERT INTO mysql_delete_target VALUES (1, 'keep'), (2, 'remove'), (3, 'keep2')")
	mustExec(t, db, "INSERT INTO mysql_delete_ref VALUES (2, 1), (3, 0)")

	mustExec(t, db, "DELETE mysql_delete_target FROM mysql_delete_target JOIN mysql_delete_ref ON mysql_delete_target.id = mysql_delete_ref.id WHERE mysql_delete_ref.flag = 1")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_target"); got != "2" {
		t.Fatalf("targeted DELETE row count = %s, want 2", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_target WHERE id = 2"); got != "0" {
		t.Fatalf("targeted DELETE removed id=2 count = %s, want 0", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_target WHERE id = 3"); got != "1" {
		t.Fatalf("targeted DELETE preserved id=3 count = %s, want 1", got)
	}
}

func TestRegression_MySQLTargetedDeleteFromJoinAlias(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE mysql_delete_target_alias (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "CREATE TABLE mysql_delete_ref_alias (id INTEGER PRIMARY KEY, flag INTEGER)")
	mustExec(t, db, "INSERT INTO mysql_delete_target_alias VALUES (1, 'keep'), (2, 'remove'), (3, 'keep2')")
	mustExec(t, db, "INSERT INTO mysql_delete_ref_alias VALUES (2, 1), (3, 0)")

	mustExec(t, db, "DELETE t FROM mysql_delete_target_alias AS t JOIN mysql_delete_ref_alias AS r ON t.id = r.id WHERE r.flag = 1")
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_target_alias"); got != "2" {
		t.Fatalf("targeted DELETE alias row count = %s, want 2", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_target_alias WHERE id = 2"); got != "0" {
		t.Fatalf("targeted DELETE alias removed id=2 count = %s, want 0", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM mysql_delete_target_alias WHERE id = 3"); got != "1" {
		t.Fatalf("targeted DELETE alias preserved id=3 count = %s, want 1", got)
	}
}

func TestRegression_DeleteUsingJoinOnCondition(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE delete_using_join_target (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "CREATE TABLE delete_using_join_ref (id INTEGER PRIMARY KEY, target_id INTEGER)")
	mustExec(t, db, "CREATE TABLE delete_using_join_guard (id INTEGER PRIMARY KEY, ref_id INTEGER)")
	mustExec(t, db, "INSERT INTO delete_using_join_target VALUES (1, 'delete'), (2, 'keep')")
	mustExec(t, db, "INSERT INTO delete_using_join_ref VALUES (10, 1), (20, 2)")
	mustExec(t, db, "INSERT INTO delete_using_join_guard VALUES (100, 10)")

	mustExec(t, db, "DELETE FROM delete_using_join_target USING delete_using_join_ref JOIN delete_using_join_guard ON delete_using_join_ref.id = delete_using_join_guard.ref_id WHERE delete_using_join_target.id = delete_using_join_ref.target_id")

	if got := scalar(t, db, "SELECT COUNT(*) FROM delete_using_join_target"); got != "1" {
		t.Fatalf("DELETE USING JOIN ON row count = %s, want 1", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM delete_using_join_target WHERE id = 1"); got != "0" {
		t.Fatalf("DELETE USING JOIN ON removed id=1 count = %s, want 0", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM delete_using_join_target WHERE id = 2"); got != "1" {
		t.Fatalf("DELETE USING JOIN ON preserved id=2 count = %s, want 1", got)
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
		"SELECT ~5":         "-6",
		"SELECT ~5 & 15":    "10",
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

func TestRegression_AlterTableAddDropUniqueConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_unique_users (id INTEGER PRIMARY KEY, email TEXT)")
	mustExec(t, db, "INSERT INTO alter_unique_users VALUES (1, 'a@example.com')")
	mustExec(t, db, "INSERT INTO alter_unique_users VALUES (2, 'b@example.com')")
	mustExec(t, db, "ALTER TABLE alter_unique_users ADD CONSTRAINT alter_unique_users_email_uq UNIQUE (email)")

	if _, err := db.Exec(context.Background(), "INSERT INTO alter_unique_users VALUES (3, 'a@example.com')"); err == nil {
		t.Fatal("ALTER TABLE ADD CONSTRAINT UNIQUE did not reject a duplicate insert")
	}

	mustExec(t, db, "ALTER TABLE alter_unique_users DROP CONSTRAINT alter_unique_users_email_uq")
	mustExec(t, db, "INSERT INTO alter_unique_users VALUES (3, 'a@example.com')")
	if got := scalar(t, db, "SELECT COUNT(*) FROM alter_unique_users WHERE email = 'a@example.com'"); got != "2" {
		t.Errorf("duplicate count after DROP CONSTRAINT = %s, want 2", got)
	}
}

func TestRegression_CreateTableNamedUniqueConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE create_named_uq_users (id INTEGER PRIMARY KEY, email TEXT, CONSTRAINT create_named_uq_users_email_uq UNIQUE (email))")
	mustExec(t, db, "INSERT INTO create_named_uq_users VALUES (1, 'a@example.com')")
	if _, err := db.Exec(context.Background(), "INSERT INTO create_named_uq_users VALUES (2, 'a@example.com')"); err == nil {
		t.Fatal("named UNIQUE constraint did not reject duplicate insert")
	}

	mustExec(t, db, "ALTER TABLE create_named_uq_users DROP CONSTRAINT create_named_uq_users_email_uq")
	mustExec(t, db, "INSERT INTO create_named_uq_users VALUES (2, 'a@example.com')")
	if got := scalar(t, db, "SELECT COUNT(*) FROM create_named_uq_users WHERE email = 'a@example.com'"); got != "2" {
		t.Errorf("duplicate count after dropping named UNIQUE = %s, want 2", got)
	}
}

func TestRegression_CreateTableColumnNamedUniqueConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE create_column_named_uq_users (id INTEGER PRIMARY KEY, email TEXT CONSTRAINT create_column_named_uq_users_email_uq UNIQUE)")
	mustExec(t, db, "INSERT INTO create_column_named_uq_users VALUES (1, 'a@example.com')")
	if _, err := db.Exec(context.Background(), "INSERT INTO create_column_named_uq_users VALUES (2, 'a@example.com')"); err == nil {
		t.Fatal("column named UNIQUE constraint did not reject duplicate insert")
	}

	idx := db.TableIndexDDL("create_column_named_uq_users")
	var found bool
	for _, ddl := range idx {
		if strings.Contains(ddl, `CREATE UNIQUE INDEX "create_column_named_uq_users_email_uq"`) &&
			strings.Contains(ddl, `ON "create_column_named_uq_users" ("email")`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("TableIndexDDL missing column named UNIQUE index: %v", idx)
	}

	mustExec(t, db, "ALTER TABLE create_column_named_uq_users DROP CONSTRAINT create_column_named_uq_users_email_uq")
	for _, ddl := range db.TableIndexDDL("create_column_named_uq_users") {
		if strings.Contains(ddl, "create_column_named_uq_users_email_uq") {
			t.Fatalf("TableIndexDDL retained dropped column named UNIQUE constraint: %v", ddl)
		}
	}
	mustExec(t, db, "INSERT INTO create_column_named_uq_users VALUES (2, 'a@example.com')")
	if got := scalar(t, db, "SELECT COUNT(*) FROM create_column_named_uq_users WHERE email = 'a@example.com'"); got != "2" {
		t.Errorf("duplicate count after dropping column named UNIQUE = %s, want 2", got)
	}
}

func TestRegression_AlterTableAddColumnNamedUniqueConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_add_column_named_uq_users (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "ALTER TABLE alter_add_column_named_uq_users ADD COLUMN email TEXT CONSTRAINT alter_add_column_named_uq_users_email_uq UNIQUE")
	mustExec(t, db, "INSERT INTO alter_add_column_named_uq_users (id, email) VALUES (1, 'a@example.com')")
	if _, err := db.Exec(context.Background(), "INSERT INTO alter_add_column_named_uq_users (id, email) VALUES (2, 'a@example.com')"); err == nil {
		t.Fatal("ALTER TABLE ADD COLUMN named UNIQUE did not reject duplicate insert")
	}

	mustExec(t, db, "ALTER TABLE alter_add_column_named_uq_users DROP CONSTRAINT alter_add_column_named_uq_users_email_uq")
	mustExec(t, db, "INSERT INTO alter_add_column_named_uq_users (id, email) VALUES (2, 'a@example.com')")
	if got := scalar(t, db, "SELECT COUNT(*) FROM alter_add_column_named_uq_users WHERE email = 'a@example.com'"); got != "2" {
		t.Errorf("duplicate count after dropping ADD COLUMN named UNIQUE = %s, want 2", got)
	}
}

func TestRegression_DropUniqueConstraintRollbackRestoresEnforcement(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE drop_unique_rb_users (id INTEGER PRIMARY KEY, email TEXT, CONSTRAINT drop_unique_rb_users_email_uq UNIQUE (email))")
	mustExec(t, db, "INSERT INTO drop_unique_rb_users VALUES (1, 'a@example.com')")
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "ALTER TABLE drop_unique_rb_users DROP CONSTRAINT drop_unique_rb_users_email_uq")
	mustExec(t, db, "ROLLBACK")

	if _, err := db.Exec(context.Background(), "INSERT INTO drop_unique_rb_users VALUES (2, 'a@example.com')"); err == nil {
		t.Fatal("ROLLBACK after DROP CONSTRAINT UNIQUE did not restore enforcement")
	}
	var found bool
	for _, ddl := range db.TableIndexDDL("drop_unique_rb_users") {
		if strings.Contains(ddl, `CREATE UNIQUE INDEX "drop_unique_rb_users_email_uq"`) &&
			strings.Contains(ddl, `ON "drop_unique_rb_users" ("email")`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("ROLLBACK after DROP CONSTRAINT UNIQUE did not restore index DDL: %v", db.TableIndexDDL("drop_unique_rb_users"))
	}
}

func TestRegression_CreateIndexInTransactionIncludesPendingRows(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, `CREATE TABLE txn_create_idx_pending ("full name" INTEGER PRIMARY KEY, label TEXT)`)
	mustExec(t, db, "BEGIN")
	mustExec(t, db, `INSERT INTO txn_create_idx_pending ("full name", label) VALUES (1, 'value')`)
	mustExec(t, db, `CREATE INDEX txn_create_idx_pending_name_idx ON txn_create_idx_pending ("full name")`)
	mustExec(t, db, "COMMIT")

	if got := scalar(t, db, `SELECT label FROM txn_create_idx_pending WHERE "full name" = 1`); got != "value" {
		t.Fatalf("indexed lookup after transactional CREATE INDEX = %s, want value", got)
	}
}

func TestRegression_CreateUniqueIndexInTransactionRejectsPendingDuplicates(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE txn_unique_idx_pending (id INTEGER PRIMARY KEY, email TEXT)")
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "INSERT INTO txn_unique_idx_pending VALUES (1, 'same@example.com')")
	mustExec(t, db, "INSERT INTO txn_unique_idx_pending VALUES (2, 'same@example.com')")
	if _, err := db.Exec(context.Background(), "CREATE UNIQUE INDEX txn_unique_idx_pending_email_idx ON txn_unique_idx_pending(email)"); err == nil {
		t.Fatal("CREATE UNIQUE INDEX accepted duplicate pending rows")
	}
	mustExec(t, db, "ROLLBACK")
}

func TestRegression_CreateTableCleansUpWhenConstraintIndexCreationFails(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE create_fail_existing (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE UNIQUE INDEX create_fail_existing_idx ON create_fail_existing(id)")

	if _, err := db.Exec(context.Background(), "CREATE TABLE create_fail_partial (id INTEGER PRIMARY KEY, email TEXT CONSTRAINT create_fail_existing_idx UNIQUE)"); err == nil {
		t.Fatal("CREATE TABLE with conflicting named column UNIQUE index unexpectedly succeeded")
	}
	if _, err := db.TableSchema("create_fail_partial"); err == nil {
		t.Fatal("failed CREATE TABLE left partial table metadata behind")
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE create_fail_partial (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("partial table name was not reusable after failed CREATE TABLE cleanup: %v", err)
	}
}

func TestRegression_CreateTableIfNotExistsSkipsConstraintIndexesForExistingTable(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE create_if_not_exists_noop (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE IF NOT EXISTS create_if_not_exists_noop (email TEXT CONSTRAINT create_if_not_exists_noop_email_uq UNIQUE)")

	schema, err := db.TableSchema("create_if_not_exists_noop")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if strings.Contains(schema, "email") {
		t.Fatalf("CREATE TABLE IF NOT EXISTS changed existing table schema: %s", schema)
	}
	for _, ddl := range db.TableIndexDDL("create_if_not_exists_noop") {
		if strings.Contains(ddl, "create_if_not_exists_noop_email_uq") {
			t.Fatalf("CREATE TABLE IF NOT EXISTS created constraint index for existing table: %v", db.TableIndexDDL("create_if_not_exists_noop"))
		}
	}
}

func TestRegression_CreateTableConstraintIndexFailureRollback(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE create_fail_tx_existing (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE UNIQUE INDEX create_fail_tx_existing_idx ON create_fail_tx_existing(id)")
	mustExec(t, db, "BEGIN")
	if _, err := db.Exec(context.Background(), "CREATE TABLE create_fail_tx_partial (id INTEGER PRIMARY KEY, email TEXT CONSTRAINT create_fail_tx_existing_idx UNIQUE)"); err == nil {
		t.Fatal("CREATE TABLE with conflicting named UNIQUE in transaction unexpectedly succeeded")
	}
	if _, err := db.Exec(context.Background(), "ROLLBACK"); err != nil {
		t.Fatalf("rollback after failed CREATE TABLE constraint-index cleanup failed: %v", err)
	}
	if _, err := db.TableSchema("create_fail_tx_partial"); err == nil {
		t.Fatal("failed transactional CREATE TABLE left partial table metadata behind")
	}
}

func TestRegression_CreateTableNamedPrimaryKeyConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE create_named_pk_orders (tenant_id INTEGER, id INTEGER, item TEXT, CONSTRAINT create_named_pk_orders_pk PRIMARY KEY (tenant_id, id))")
	mustExec(t, db, "INSERT INTO create_named_pk_orders VALUES (10, 1, 'a')")
	if _, err := db.Exec(context.Background(), "INSERT INTO create_named_pk_orders VALUES (10, 1, 'dup')"); err == nil {
		t.Fatal("named PRIMARY KEY constraint did not reject duplicate composite key")
	}
	schema, err := db.TableSchema("create_named_pk_orders")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "PRIMARY KEY (tenant_id, id)") {
		t.Fatalf("schema missing composite primary key: %s", schema)
	}
}

func TestRegression_CreateTableNamedCheckConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE create_named_check_products (id INTEGER PRIMARY KEY, price INTEGER, discount INTEGER, CONSTRAINT create_named_check_discount_ck CHECK (discount <= price))")
	mustExec(t, db, "INSERT INTO create_named_check_products VALUES (1, 100, 25)")
	if _, err := db.Exec(context.Background(), "INSERT INTO create_named_check_products VALUES (2, 100, 125)"); err == nil {
		t.Fatal("named CHECK constraint did not reject invalid insert")
	}
	if _, err := db.Exec(context.Background(), "UPDATE create_named_check_products SET discount = 125 WHERE id = 1"); err == nil {
		t.Fatal("named CHECK constraint did not reject invalid update")
	}
	schema, err := db.TableSchema("create_named_check_products")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "CONSTRAINT create_named_check_discount_ck CHECK (discount <= price)") {
		t.Fatalf("schema missing named CHECK constraint: %s", schema)
	}
}

func TestRegression_CreateTableNamedCheckConstraintPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "named-check.db")
	db, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE persisted_named_check (id INTEGER PRIMARY KEY, qty INTEGER, CONSTRAINT qty_positive_ck CHECK (qty > 0))")
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Exec(context.Background(), "INSERT INTO persisted_named_check VALUES (1, -1)"); err == nil {
		t.Fatal("persisted named CHECK constraint did not reject invalid insert after reopen")
	}
}

func TestRegression_CreateTableColumnNamedCheckConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE create_column_named_check (id INTEGER PRIMARY KEY, age INTEGER CONSTRAINT create_column_named_check_age_ck CHECK (age >= 0))")
	mustExec(t, db, "INSERT INTO create_column_named_check VALUES (1, 42)")
	if _, err := db.Exec(context.Background(), "INSERT INTO create_column_named_check VALUES (2, -1)"); err == nil {
		t.Fatal("column named CHECK constraint did not reject invalid insert")
	}
	schema, err := db.TableSchema("create_column_named_check")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "age INTEGER CONSTRAINT create_column_named_check_age_ck CHECK (age >= 0)") {
		t.Fatalf("schema missing column named CHECK constraint: %s", schema)
	}
}

func TestRegression_CreateTableColumnNamedCheckConstraintPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "column-named-check.db")
	db, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE persisted_column_named_check (id INTEGER PRIMARY KEY, qty INTEGER CONSTRAINT persisted_qty_ck CHECK (qty > 0))")
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Exec(context.Background(), "INSERT INTO persisted_column_named_check VALUES (1, -1)"); err == nil {
		t.Fatal("persisted column named CHECK did not reject invalid insert after reopen")
	}
	schema, err := reopened.TableSchema("persisted_column_named_check")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "CONSTRAINT persisted_qty_ck CHECK (qty > 0)") {
		t.Fatalf("schema missing persisted column named CHECK: %s", schema)
	}
}

func TestRegression_AlterTableAddDropCheckConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_check_products (id INTEGER PRIMARY KEY, price INTEGER, discount INTEGER)")
	mustExec(t, db, "INSERT INTO alter_check_products VALUES (1, 100, 25)")
	mustExec(t, db, "ALTER TABLE alter_check_products ADD CONSTRAINT alter_check_discount_ck CHECK (discount <= price)")
	if _, err := db.Exec(context.Background(), "INSERT INTO alter_check_products VALUES (2, 100, 125)"); err == nil {
		t.Fatal("ALTER TABLE ADD CONSTRAINT CHECK did not reject invalid insert")
	}
	if _, err := db.Exec(context.Background(), "UPDATE alter_check_products SET discount = 125 WHERE id = 1"); err == nil {
		t.Fatal("ALTER TABLE ADD CONSTRAINT CHECK did not reject invalid update")
	}

	mustExec(t, db, "ALTER TABLE alter_check_products DROP CONSTRAINT alter_check_discount_ck")
	mustExec(t, db, "INSERT INTO alter_check_products VALUES (2, 100, 125)")
}

func TestRegression_AlterTableAddCheckConstraintRejectsExistingViolations(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_check_existing (id INTEGER PRIMARY KEY, price INTEGER, discount INTEGER)")
	mustExec(t, db, "INSERT INTO alter_check_existing VALUES (1, 100, 125)")
	if _, err := db.Exec(context.Background(), "ALTER TABLE alter_check_existing ADD CONSTRAINT alter_check_existing_ck CHECK (discount <= price)"); err == nil {
		t.Fatal("ALTER TABLE ADD CONSTRAINT CHECK accepted existing invalid rows")
	}
	mustExec(t, db, "INSERT INTO alter_check_existing VALUES (2, 100, 125)")
}

func TestRegression_AlterTableCheckConstraintRollback(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_check_rb (id INTEGER PRIMARY KEY, amount INTEGER)")
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "ALTER TABLE alter_check_rb ADD CONSTRAINT alter_check_rb_positive CHECK (amount > 0)")
	if _, err := db.Exec(context.Background(), "INSERT INTO alter_check_rb VALUES (1, -1)"); err == nil {
		t.Fatal("CHECK constraint was not enforced inside transaction")
	}
	mustExec(t, db, "ROLLBACK")
	mustExec(t, db, "INSERT INTO alter_check_rb VALUES (1, -1)")
}

func TestRegression_AlterTableAddUniqueConstraintRejectsExistingDuplicates(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_unique_dups (id INTEGER PRIMARY KEY, email TEXT)")
	mustExec(t, db, "INSERT INTO alter_unique_dups VALUES (1, 'dup@example.com')")
	mustExec(t, db, "INSERT INTO alter_unique_dups VALUES (2, 'dup@example.com')")

	if _, err := db.Exec(context.Background(), "ALTER TABLE alter_unique_dups ADD CONSTRAINT alter_unique_dups_email_uq UNIQUE (email)"); err == nil {
		t.Fatal("ALTER TABLE ADD CONSTRAINT UNIQUE accepted duplicate existing data")
	}
}

func TestRegression_AlterTableAddDropForeignKeyConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_fk_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE alter_fk_child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	mustExec(t, db, "INSERT INTO alter_fk_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO alter_fk_child VALUES (1, 1)")
	mustExec(t, db, "ALTER TABLE alter_fk_child ADD CONSTRAINT alter_fk_child_parent_fk FOREIGN KEY (parent_id) REFERENCES alter_fk_parent(id) ON DELETE RESTRICT")

	schema, err := db.TableSchema("alter_fk_child")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "CONSTRAINT alter_fk_child_parent_fk FOREIGN KEY") {
		t.Fatalf("schema missing named FK constraint: %s", schema)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO alter_fk_child VALUES (2, 99)"); err == nil {
		t.Fatal("ALTER TABLE ADD CONSTRAINT FOREIGN KEY did not reject a missing parent")
	}

	mustExec(t, db, "ALTER TABLE alter_fk_child DROP CONSTRAINT alter_fk_child_parent_fk")
	mustExec(t, db, "INSERT INTO alter_fk_child VALUES (2, 99)")
}

func TestRegression_AlterTableAddForeignKeyRejectsExistingViolations(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_fk_bad_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE alter_fk_bad_child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	mustExec(t, db, "INSERT INTO alter_fk_bad_child VALUES (1, 99)")

	if _, err := db.Exec(context.Background(), "ALTER TABLE alter_fk_bad_child ADD CONSTRAINT alter_fk_bad_child_parent_fk FOREIGN KEY (parent_id) REFERENCES alter_fk_bad_parent(id)"); err == nil {
		t.Fatal("ALTER TABLE ADD CONSTRAINT FOREIGN KEY accepted existing orphan rows")
	}
	mustExec(t, db, "INSERT INTO alter_fk_bad_child VALUES (2, 99)")
}

func TestRegression_AlterTableForeignKeyConstraintRollback(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_fk_rb_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE alter_fk_rb_child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	mustExec(t, db, "INSERT INTO alter_fk_rb_parent VALUES (1)")

	mustExec(t, db, "BEGIN")
	mustExec(t, db, "ALTER TABLE alter_fk_rb_child ADD CONSTRAINT alter_fk_rb_child_parent_fk FOREIGN KEY (parent_id) REFERENCES alter_fk_rb_parent(id)")
	if _, err := db.Exec(context.Background(), "INSERT INTO alter_fk_rb_child VALUES (1, 99)"); err == nil {
		t.Fatal("foreign key was not enforced inside transaction")
	}
	mustExec(t, db, "ROLLBACK")

	mustExec(t, db, "INSERT INTO alter_fk_rb_child VALUES (1, 99)")
}

func TestRegression_DropForeignKeyConstraintRollbackRestoresEnforcement(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE drop_fk_rb_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE drop_fk_rb_child (id INTEGER PRIMARY KEY, parent_id INTEGER, CONSTRAINT drop_fk_rb_child_parent_fk FOREIGN KEY (parent_id) REFERENCES drop_fk_rb_parent(id))")
	mustExec(t, db, "INSERT INTO drop_fk_rb_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO drop_fk_rb_child VALUES (1, 1)")

	mustExec(t, db, "BEGIN")
	mustExec(t, db, "ALTER TABLE drop_fk_rb_child DROP CONSTRAINT drop_fk_rb_child_parent_fk")
	mustExec(t, db, "ROLLBACK")

	if _, err := db.Exec(context.Background(), "INSERT INTO drop_fk_rb_child VALUES (2, 99)"); err == nil {
		t.Fatal("ROLLBACK after DROP CONSTRAINT FOREIGN KEY did not restore enforcement")
	}
	refs := db.TableForeignKeyRefs("drop_fk_rb_child")
	if len(refs) != 1 || refs[0] != "drop_fk_rb_parent" {
		t.Fatalf("ROLLBACK after DROP CONSTRAINT FOREIGN KEY did not restore FK refs: %v", refs)
	}
	schema, err := db.TableSchema("drop_fk_rb_child")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "CONSTRAINT drop_fk_rb_child_parent_fk FOREIGN KEY") {
		t.Fatalf("ROLLBACK after DROP CONSTRAINT FOREIGN KEY did not restore schema metadata: %s", schema)
	}
}

func TestRegression_ForeignKeyDefinitionValidation(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE fk_def_parent_sql (id INTEGER PRIMARY KEY, tenant_id INTEGER)")
	if _, err := db.Exec(context.Background(), "CREATE TABLE fk_def_bad_sql (id INTEGER PRIMARY KEY, parent_id INTEGER, tenant_id INTEGER, FOREIGN KEY (parent_id, tenant_id) REFERENCES fk_def_parent_sql(id))"); err == nil {
		t.Fatal("CREATE TABLE accepted mismatched FOREIGN KEY column counts")
	}
	if _, err := db.TableSchema("fk_def_bad_sql"); err == nil {
		t.Fatal("invalid FK table remained visible after failed CREATE TABLE")
	}

	mustExec(t, db, "CREATE TABLE fk_def_child_sql (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_def_parent_sql)")
	schema, err := db.TableSchema("fk_def_child_sql")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "REFERENCES fk_def_parent_sql (id)") {
		t.Fatalf("FK without explicit referenced columns was not normalized to parent PK: %s", schema)
	}

	mustExec(t, db, "CREATE TABLE alter_fk_def_child_sql (id INTEGER PRIMARY KEY, parent_id INTEGER, tenant_id INTEGER)")
	if _, err := db.Exec(context.Background(), "ALTER TABLE alter_fk_def_child_sql ADD CONSTRAINT alter_fk_def_bad_fk FOREIGN KEY (parent_id, tenant_id) REFERENCES fk_def_parent_sql(id)"); err == nil {
		t.Fatal("ALTER TABLE accepted mismatched FOREIGN KEY column counts")
	}
}

func TestRegression_ForeignKeyRejectsMalformedActions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE fk_bad_action_parent (id INTEGER PRIMARY KEY)")
	cases := []string{
		"CREATE TABLE fk_bad_action_set (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_bad_action_parent(id) ON DELETE SET)",
		"CREATE TABLE fk_bad_action_no (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_bad_action_parent(id) ON UPDATE NO)",
		"CREATE TABLE fk_bad_action_unknown (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_bad_action_parent(id) ON DELETE DEFAULT)",
		"CREATE TABLE fk_bad_action_dup_delete (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_bad_action_parent(id) ON DELETE CASCADE ON DELETE RESTRICT)",
		"CREATE TABLE fk_bad_action_dup_update (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_bad_action_parent(id) ON UPDATE CASCADE ON UPDATE RESTRICT)",
	}
	for _, sql := range cases {
		if _, err := db.Exec(context.Background(), sql); err == nil {
			t.Fatalf("CREATE TABLE accepted malformed FOREIGN KEY action: %s", sql)
		}
	}
}

func TestRegression_RenameReferencedTableUpdatesForeignKeys(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE rename_fk_parent_old (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE rename_fk_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES rename_fk_parent_old(id))")
	mustExec(t, db, "INSERT INTO rename_fk_parent_old VALUES (1)")
	mustExec(t, db, "ALTER TABLE rename_fk_parent_old RENAME TO rename_fk_parent_new")

	if _, err := db.Exec(context.Background(), "INSERT INTO rename_fk_child VALUES (1, 1)"); err != nil {
		t.Fatalf("foreign key did not follow renamed parent table: %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO rename_fk_child VALUES (2, 99)"); err == nil {
		t.Fatal("foreign key accepted missing parent after referenced table rename")
	}
	schema, err := db.TableSchema("rename_fk_child")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "REFERENCES rename_fk_parent_new") {
		t.Fatalf("child schema did not persist renamed FK target: %s", schema)
	}
}

func TestRegression_RenameForeignKeyColumnUpdatesConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE rename_fk_col_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE rename_fk_col_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES rename_fk_col_parent(id))")
	mustExec(t, db, "INSERT INTO rename_fk_col_parent VALUES (1)")
	mustExec(t, db, "ALTER TABLE rename_fk_col_child RENAME COLUMN parent_id TO account_id")

	if _, err := db.Exec(context.Background(), "INSERT INTO rename_fk_col_child VALUES (1, 1)"); err != nil {
		t.Fatalf("foreign key did not follow renamed child column: %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO rename_fk_col_child VALUES (2, 99)"); err == nil {
		t.Fatal("foreign key accepted missing parent after child column rename")
	}
	schema, err := db.TableSchema("rename_fk_col_child")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "FOREIGN KEY (account_id)") {
		t.Fatalf("child schema did not persist renamed FK column: %s", schema)
	}
}

func TestRegression_RenameReferencedColumnUpdatesForeignKeys(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE rename_ref_col_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE rename_ref_col_child (id INTEGER PRIMARY KEY, parent_code TEXT, FOREIGN KEY (parent_code) REFERENCES rename_ref_col_parent(code))")
	mustExec(t, db, "INSERT INTO rename_ref_col_parent VALUES (1, 'A')")
	mustExec(t, db, "ALTER TABLE rename_ref_col_parent RENAME COLUMN code TO account_code")

	if _, err := db.Exec(context.Background(), "INSERT INTO rename_ref_col_child VALUES (1, 'A')"); err != nil {
		t.Fatalf("foreign key did not follow renamed referenced column: %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO rename_ref_col_child VALUES (2, 'Z')"); err == nil {
		t.Fatal("foreign key accepted missing parent after referenced column rename")
	}
	schema, err := db.TableSchema("rename_ref_col_child")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "REFERENCES rename_ref_col_parent (account_code)") {
		t.Fatalf("child schema did not persist renamed referenced FK column: %s", schema)
	}
}

func TestRegression_NonPrimaryReferencedForeignKeyActions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE nonpk_fk_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE nonpk_fk_child (id INTEGER PRIMARY KEY, parent_code TEXT, FOREIGN KEY (parent_code) REFERENCES nonpk_fk_parent(code) ON DELETE RESTRICT ON UPDATE CASCADE)")
	mustExec(t, db, "INSERT INTO nonpk_fk_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO nonpk_fk_child VALUES (1, 'A')")

	if _, err := db.Exec(context.Background(), "DELETE FROM nonpk_fk_parent WHERE id = 1"); err == nil {
		t.Fatal("DELETE removed a row referenced through a non-primary-key FK column")
	}
	mustExec(t, db, "UPDATE nonpk_fk_parent SET code = 'B' WHERE id = 1")
	if got := scalar(t, db, "SELECT parent_code FROM nonpk_fk_child WHERE id = 1"); got != "B" {
		t.Fatalf("ON UPDATE CASCADE for non-primary referenced column set child to %q, want B", got)
	}
}

func TestRegression_AlterTableAddForeignKeyValidatesNonPrimaryReferencedColumn(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE alter_nonpk_fk_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE alter_nonpk_fk_child (id INTEGER PRIMARY KEY, parent_code TEXT)")
	mustExec(t, db, "INSERT INTO alter_nonpk_fk_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO alter_nonpk_fk_child VALUES (1, 'A')")
	mustExec(t, db, "ALTER TABLE alter_nonpk_fk_child ADD CONSTRAINT alter_nonpk_fk_child_parent_fk FOREIGN KEY (parent_code) REFERENCES alter_nonpk_fk_parent(code)")

	if _, err := db.Exec(context.Background(), "INSERT INTO alter_nonpk_fk_child VALUES (2, 'Z')"); err == nil {
		t.Fatal("ALTER-added non-primary referenced FK accepted a missing parent value")
	}
}

func TestRegression_CompositeForeignKeyRequiresSameReferencedRow(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE composite_fk_parent (id INTEGER PRIMARY KEY, tenant_id INTEGER, user_id INTEGER)")
	mustExec(t, db, "CREATE TABLE composite_fk_child (id INTEGER PRIMARY KEY, tenant_id INTEGER, user_id INTEGER, FOREIGN KEY (tenant_id, user_id) REFERENCES composite_fk_parent(tenant_id, user_id))")
	mustExec(t, db, "INSERT INTO composite_fk_parent VALUES (1, 1, 2)")
	mustExec(t, db, "INSERT INTO composite_fk_parent VALUES (2, 2, 1)")

	if _, err := db.Exec(context.Background(), "INSERT INTO composite_fk_child VALUES (1, 1, 1)"); err == nil {
		t.Fatal("composite FK accepted values that exist only across different parent rows")
	}
	mustExec(t, db, "INSERT INTO composite_fk_child VALUES (2, 1, 2)")

	if _, err := db.Exec(context.Background(), "UPDATE composite_fk_child SET user_id = 1 WHERE id = 2"); err == nil {
		t.Fatal("composite FK update accepted values that exist only across different parent rows")
	}
}

func TestRegression_SetNullForeignKeyActionHonorsNotNull(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE setnull_nn_del_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setnull_nn_del_child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, FOREIGN KEY (parent_id) REFERENCES setnull_nn_del_parent(id) ON DELETE SET NULL)")
	mustExec(t, db, "INSERT INTO setnull_nn_del_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setnull_nn_del_child VALUES (1, 1)")
	if _, err := db.Exec(context.Background(), "DELETE FROM setnull_nn_del_parent WHERE id = 1"); err == nil {
		t.Fatal("ON DELETE SET NULL bypassed child NOT NULL constraint")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM setnull_nn_del_parent WHERE id = 1"); got != "1" {
		t.Fatalf("parent delete should have been rejected, remaining count = %s", got)
	}
	if got := scalar(t, db, "SELECT parent_id FROM setnull_nn_del_child WHERE id = 1"); got != "1" {
		t.Fatalf("child FK should remain unchanged after rejected SET NULL, got %s", got)
	}

	mustExec(t, db, "CREATE TABLE setnull_nn_upd_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setnull_nn_upd_child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, FOREIGN KEY (parent_id) REFERENCES setnull_nn_upd_parent(id) ON UPDATE SET NULL)")
	mustExec(t, db, "INSERT INTO setnull_nn_upd_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setnull_nn_upd_child VALUES (1, 1)")
	if _, err := db.Exec(context.Background(), "UPDATE setnull_nn_upd_parent SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("ON UPDATE SET NULL bypassed child NOT NULL constraint")
	}
	if got := scalar(t, db, "SELECT id FROM setnull_nn_upd_parent WHERE id = 1"); got != "1" {
		t.Fatalf("parent update should have been rejected, original id query got %s", got)
	}
	if got := scalar(t, db, "SELECT parent_id FROM setnull_nn_upd_child WHERE id = 1"); got != "1" {
		t.Fatalf("child FK should remain unchanged after rejected SET NULL update, got %s", got)
	}
}

func TestRegression_SetNullForeignKeyActionHonorsCheck(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE setnull_ck_del_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setnull_ck_del_child (id INTEGER PRIMARY KEY, parent_id INTEGER, CONSTRAINT setnull_ck_del_parent_id_ck CHECK (parent_id IS NOT NULL), FOREIGN KEY (parent_id) REFERENCES setnull_ck_del_parent(id) ON DELETE SET NULL)")
	mustExec(t, db, "INSERT INTO setnull_ck_del_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setnull_ck_del_child VALUES (1, 1)")
	if _, err := db.Exec(context.Background(), "DELETE FROM setnull_ck_del_parent WHERE id = 1"); err == nil {
		t.Fatal("ON DELETE SET NULL bypassed child CHECK constraint")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM setnull_ck_del_parent WHERE id = 1"); got != "1" {
		t.Fatalf("parent delete should have been rejected, remaining count = %s", got)
	}
	if got := scalar(t, db, "SELECT parent_id FROM setnull_ck_del_child WHERE id = 1"); got != "1" {
		t.Fatalf("child FK should remain unchanged after rejected CHECK failure, got %s", got)
	}

	mustExec(t, db, "CREATE TABLE setnull_ck_upd_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setnull_ck_upd_child (id INTEGER PRIMARY KEY, parent_id INTEGER, CONSTRAINT setnull_ck_upd_parent_id_ck CHECK (parent_id IS NOT NULL), FOREIGN KEY (parent_id) REFERENCES setnull_ck_upd_parent(id) ON UPDATE SET NULL)")
	mustExec(t, db, "INSERT INTO setnull_ck_upd_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setnull_ck_upd_child VALUES (1, 1)")
	if _, err := db.Exec(context.Background(), "UPDATE setnull_ck_upd_parent SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("ON UPDATE SET NULL bypassed child CHECK constraint")
	}
	if got := scalar(t, db, "SELECT id FROM setnull_ck_upd_parent WHERE id = 1"); got != "1" {
		t.Fatalf("parent update should have been rejected, original id query got %s", got)
	}
	if got := scalar(t, db, "SELECT parent_id FROM setnull_ck_upd_child WHERE id = 1"); got != "1" {
		t.Fatalf("child FK should remain unchanged after rejected CHECK failure, got %s", got)
	}
}

func TestRegression_ForeignKeyCascadeActionHonorsChildUnique(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE cascade_unique_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE cascade_unique_child (id INTEGER PRIMARY KEY, parent_code TEXT UNIQUE, FOREIGN KEY (parent_code) REFERENCES cascade_unique_parent(code) ON UPDATE CASCADE)")
	mustExec(t, db, "INSERT INTO cascade_unique_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO cascade_unique_parent VALUES (2, 'B')")
	mustExec(t, db, "INSERT INTO cascade_unique_child VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO cascade_unique_child VALUES (2, 'B')")

	if _, err := db.Exec(context.Background(), "UPDATE cascade_unique_parent SET code = 'B' WHERE id = 1"); err == nil {
		t.Fatal("ON UPDATE CASCADE bypassed child UNIQUE constraint")
	}
	if got := scalar(t, db, "SELECT code FROM cascade_unique_parent WHERE id = 1"); got != "A" {
		t.Fatalf("parent update should have been rejected, got code %s", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM cascade_unique_child WHERE parent_code = 'B'"); got != "1" {
		t.Fatalf("rejected cascade should leave exactly one child with parent_code B, got %s", got)
	}
}

func TestRegression_ForeignKeyCascadeUpdatePropagatesThroughChain(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE cascade_chain_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE cascade_chain_child (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES cascade_chain_parent(code) ON UPDATE CASCADE)")
	mustExec(t, db, "CREATE TABLE cascade_chain_grandchild (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES cascade_chain_child(pcode) ON UPDATE CASCADE)")
	mustExec(t, db, "INSERT INTO cascade_chain_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO cascade_chain_child VALUES (10, 'A')")
	mustExec(t, db, "INSERT INTO cascade_chain_grandchild VALUES (100, 'A')")

	mustExec(t, db, "UPDATE cascade_chain_parent SET code = 'B' WHERE id = 1")
	if got := scalar(t, db, "SELECT pcode FROM cascade_chain_child WHERE id = 10"); got != "B" {
		t.Fatalf("child FK should cascade to B, got %s", got)
	}
	if got := scalar(t, db, "SELECT ccode FROM cascade_chain_grandchild WHERE id = 100"); got != "B" {
		t.Fatalf("grandchild FK should cascade through child to B, got %s", got)
	}
}

func TestRegression_ForeignKeySetNullUpdatePropagatesThroughChain(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE setnull_chain_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE setnull_chain_child (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES setnull_chain_parent(code) ON UPDATE SET NULL)")
	mustExec(t, db, "CREATE TABLE setnull_chain_grandchild (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES setnull_chain_child(pcode) ON UPDATE SET NULL)")
	mustExec(t, db, "INSERT INTO setnull_chain_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO setnull_chain_child VALUES (10, 'A')")
	mustExec(t, db, "INSERT INTO setnull_chain_grandchild VALUES (100, 'A')")

	mustExec(t, db, "UPDATE setnull_chain_parent SET code = 'B' WHERE id = 1")
	if got := scalar(t, db, "SELECT pcode FROM setnull_chain_child WHERE id = 10"); got != "<nil>" {
		t.Fatalf("child FK should be NULL after SET NULL, got %s", got)
	}
	if got := scalar(t, db, "SELECT ccode FROM setnull_chain_grandchild WHERE id = 100"); got != "<nil>" {
		t.Fatalf("grandchild FK should become NULL through child SET NULL, got %s", got)
	}
}

func TestRegression_ForeignKeySetNullDeletePropagatesThroughChain(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE del_setnull_chain_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE del_setnull_chain_child (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES del_setnull_chain_parent(code) ON DELETE SET NULL)")
	mustExec(t, db, "CREATE TABLE del_setnull_chain_grandchild (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES del_setnull_chain_child(pcode) ON UPDATE SET NULL)")
	mustExec(t, db, "INSERT INTO del_setnull_chain_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO del_setnull_chain_child VALUES (10, 'A')")
	mustExec(t, db, "INSERT INTO del_setnull_chain_grandchild VALUES (100, 'A')")

	mustExec(t, db, "DELETE FROM del_setnull_chain_parent WHERE id = 1")
	if got := scalar(t, db, "SELECT pcode FROM del_setnull_chain_child WHERE id = 10"); got != "<nil>" {
		t.Fatalf("child FK should become NULL after parent delete, got %s", got)
	}
	if got := scalar(t, db, "SELECT ccode FROM del_setnull_chain_grandchild WHERE id = 100"); got != "<nil>" {
		t.Fatalf("grandchild FK should become NULL through child SET NULL delete, got %s", got)
	}
}

func TestRegression_ForeignKeySetNullDeleteChainRestrictRollsBack(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE del_restrict_chain_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE del_restrict_chain_child (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES del_restrict_chain_parent(code) ON DELETE SET NULL)")
	mustExec(t, db, "CREATE TABLE del_restrict_chain_grandchild (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES del_restrict_chain_child(pcode) ON UPDATE RESTRICT)")
	mustExec(t, db, "INSERT INTO del_restrict_chain_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO del_restrict_chain_child VALUES (10, 'A')")
	mustExec(t, db, "INSERT INTO del_restrict_chain_grandchild VALUES (100, 'A')")

	if _, err := db.Exec(context.Background(), "DELETE FROM del_restrict_chain_parent WHERE id = 1"); err == nil {
		t.Fatal("ON DELETE SET NULL bypassed downstream ON UPDATE RESTRICT")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM del_restrict_chain_parent WHERE id = 1"); got != "1" {
		t.Fatalf("parent delete should roll back after downstream RESTRICT, remaining count %s", got)
	}
	if got := scalar(t, db, "SELECT pcode FROM del_restrict_chain_child WHERE id = 10"); got != "A" {
		t.Fatalf("child SET NULL should roll back after downstream RESTRICT, got %s", got)
	}
	if got := scalar(t, db, "SELECT ccode FROM del_restrict_chain_grandchild WHERE id = 100"); got != "A" {
		t.Fatalf("grandchild should remain unchanged after downstream RESTRICT, got %s", got)
	}
}

func TestRegression_ForeignKeySetDefaultActions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE setdefault_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setdefault_child (id INTEGER PRIMARY KEY, parent_id INTEGER DEFAULT 0, FOREIGN KEY (parent_id) REFERENCES setdefault_parent(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO setdefault_parent VALUES (0)")
	mustExec(t, db, "INSERT INTO setdefault_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setdefault_parent VALUES (2)")
	mustExec(t, db, "INSERT INTO setdefault_child VALUES (10, 1)")
	mustExec(t, db, "DELETE FROM setdefault_parent WHERE id = 1")
	if got := scalar(t, db, "SELECT parent_id FROM setdefault_child WHERE id = 10"); got != "0" {
		t.Fatalf("ON DELETE SET DEFAULT set child FK to %s, want 0", got)
	}

	mustExec(t, db, "INSERT INTO setdefault_child VALUES (20, 2)")
	mustExec(t, db, "UPDATE setdefault_parent SET id = 3 WHERE id = 2")
	if got := scalar(t, db, "SELECT parent_id FROM setdefault_child WHERE id = 20"); got != "0" {
		t.Fatalf("ON UPDATE SET DEFAULT set child FK to %s, want 0", got)
	}
}

func TestRegression_ForeignKeySetDefaultPersistsAndDumps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "set-default-fk.db")
	db, err := Open(path, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE persist_setdefault_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE persist_setdefault_child (id INTEGER PRIMARY KEY, parent_id INTEGER DEFAULT 0, CONSTRAINT persist_setdefault_child_parent_fk FOREIGN KEY (parent_id) REFERENCES persist_setdefault_parent(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO persist_setdefault_parent VALUES (0)")
	mustExec(t, db, "INSERT INTO persist_setdefault_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO persist_setdefault_child VALUES (10, 1)")

	parentSchema, err := db.TableSchema("persist_setdefault_parent")
	if err != nil {
		t.Fatalf("parent schema: %v", err)
	}
	childSchema, err := db.TableSchema("persist_setdefault_child")
	if err != nil {
		t.Fatalf("child schema: %v", err)
	}
	if !strings.Contains(childSchema, "ON DELETE SET DEFAULT") || !strings.Contains(childSchema, "ON UPDATE SET DEFAULT") {
		t.Fatalf("schema missing SET DEFAULT FK actions: %s", childSchema)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	mustExec(t, reopened, "DELETE FROM persist_setdefault_parent WHERE id = 1")
	if got := scalar(t, reopened, "SELECT parent_id FROM persist_setdefault_child WHERE id = 10"); got != "0" {
		t.Fatalf("persisted ON DELETE SET DEFAULT set child FK to %s, want 0", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened: %v", err)
	}

	restored := openRegressionDB(t)
	defer restored.Close()
	mustExec(t, restored, parentSchema)
	mustExec(t, restored, childSchema)
	mustExec(t, restored, "INSERT INTO persist_setdefault_parent VALUES (0)")
	mustExec(t, restored, "INSERT INTO persist_setdefault_parent VALUES (1)")
	mustExec(t, restored, "INSERT INTO persist_setdefault_child VALUES (10, 1)")
	mustExec(t, restored, "UPDATE persist_setdefault_parent SET id = 2 WHERE id = 1")
	if got := scalar(t, restored, "SELECT parent_id FROM persist_setdefault_child WHERE id = 10"); got != "0" {
		t.Fatalf("restored schema ON UPDATE SET DEFAULT set child FK to %s, want 0", got)
	}
}

func TestRegression_CompositeForeignKeySetDefaultActions(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE composite_setdefault_parent (id INTEGER PRIMARY KEY, tenant_id INTEGER, code INTEGER)")
	mustExec(t, db, "CREATE TABLE composite_setdefault_child (id INTEGER PRIMARY KEY, tenant_id INTEGER DEFAULT 0, code INTEGER DEFAULT 0, FOREIGN KEY (tenant_id, code) REFERENCES composite_setdefault_parent(tenant_id, code) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO composite_setdefault_parent VALUES (1, 0, 0)")
	mustExec(t, db, "INSERT INTO composite_setdefault_parent VALUES (2, 1, 10)")
	mustExec(t, db, "INSERT INTO composite_setdefault_parent VALUES (3, 2, 20)")
	mustExec(t, db, "INSERT INTO composite_setdefault_child VALUES (10, 1, 10)")
	mustExec(t, db, "DELETE FROM composite_setdefault_parent WHERE id = 2")
	if got := scalar(t, db, "SELECT tenant_id || ':' || code FROM composite_setdefault_child WHERE id = 10"); got != "0:0" {
		t.Fatalf("composite ON DELETE SET DEFAULT set child FK to %s, want 0:0", got)
	}

	mustExec(t, db, "INSERT INTO composite_setdefault_child VALUES (20, 2, 20)")
	mustExec(t, db, "UPDATE composite_setdefault_parent SET code = 21 WHERE id = 3")
	if got := scalar(t, db, "SELECT tenant_id || ':' || code FROM composite_setdefault_child WHERE id = 20"); got != "0:0" {
		t.Fatalf("composite ON UPDATE SET DEFAULT set child FK to %s, want 0:0", got)
	}
}

func TestRegression_CompositeForeignKeySetDefaultRejectsDanglingDefaults(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE composite_setdefault_bad_parent (id INTEGER PRIMARY KEY, tenant_id INTEGER, code INTEGER)")
	mustExec(t, db, "CREATE TABLE composite_setdefault_bad_child (id INTEGER PRIMARY KEY, tenant_id INTEGER DEFAULT 9, code INTEGER DEFAULT 9, FOREIGN KEY (tenant_id, code) REFERENCES composite_setdefault_bad_parent(tenant_id, code) ON DELETE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO composite_setdefault_bad_parent VALUES (1, 1, 10)")
	mustExec(t, db, "INSERT INTO composite_setdefault_bad_child VALUES (10, 1, 10)")
	if _, err := db.Exec(context.Background(), "DELETE FROM composite_setdefault_bad_parent WHERE id = 1"); err == nil {
		t.Fatal("composite ON DELETE SET DEFAULT accepted default tuple with no referenced parent")
	}
	if got := scalar(t, db, "SELECT tenant_id || ':' || code FROM composite_setdefault_bad_child WHERE id = 10"); got != "1:10" {
		t.Fatalf("composite child FK should remain unchanged after invalid SET DEFAULT, got %s", got)
	}

	mustExec(t, db, "CREATE TABLE composite_setdefault_multi_parent (id INTEGER PRIMARY KEY, tenant_id INTEGER, code INTEGER)")
	mustExec(t, db, "CREATE TABLE composite_setdefault_multi_child (id INTEGER PRIMARY KEY, tenant_id INTEGER DEFAULT 0, code INTEGER DEFAULT 0, FOREIGN KEY (tenant_id, code) REFERENCES composite_setdefault_multi_parent(tenant_id, code) ON DELETE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO composite_setdefault_multi_parent VALUES (1, 0, 0)")
	mustExec(t, db, "INSERT INTO composite_setdefault_multi_parent VALUES (2, 1, 10)")
	mustExec(t, db, "INSERT INTO composite_setdefault_multi_child VALUES (10, 1, 10)")
	if _, err := db.Exec(context.Background(), "DELETE FROM composite_setdefault_multi_parent WHERE id IN (1, 2)"); err == nil {
		t.Fatal("composite ON DELETE SET DEFAULT allowed default parent tuple to be deleted in the same statement")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM composite_setdefault_multi_parent"); got != "2" {
		t.Fatalf("composite multi-row parent delete should roll back, remaining count %s", got)
	}
	if got := scalar(t, db, "SELECT tenant_id || ':' || code FROM composite_setdefault_multi_child WHERE id = 10"); got != "1:10" {
		t.Fatalf("composite child FK should remain unchanged after rejected multi-row delete, got %s", got)
	}
}

func TestRegression_ForeignKeySetDefaultInvalidDefaultRollsBack(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE setdefault_bad_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setdefault_bad_child (id INTEGER PRIMARY KEY, parent_id INTEGER DEFAULT 99, FOREIGN KEY (parent_id) REFERENCES setdefault_bad_parent(id) ON DELETE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO setdefault_bad_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setdefault_bad_child VALUES (10, 1)")

	if _, err := db.Exec(context.Background(), "DELETE FROM setdefault_bad_parent WHERE id = 1"); err == nil {
		t.Fatal("ON DELETE SET DEFAULT accepted default value with no referenced parent")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM setdefault_bad_parent WHERE id = 1"); got != "1" {
		t.Fatalf("parent delete should roll back after invalid SET DEFAULT, remaining count %s", got)
	}
	if got := scalar(t, db, "SELECT parent_id FROM setdefault_bad_child WHERE id = 10"); got != "1" {
		t.Fatalf("child FK should remain unchanged after invalid SET DEFAULT, got %s", got)
	}
}

func TestRegression_ForeignKeySetDefaultRejectsChangingDefaultParent(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE setdefault_old_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setdefault_old_child (id INTEGER PRIMARY KEY, parent_id INTEGER DEFAULT 1, FOREIGN KEY (parent_id) REFERENCES setdefault_old_parent(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO setdefault_old_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setdefault_old_parent VALUES (2)")
	mustExec(t, db, "INSERT INTO setdefault_old_child VALUES (10, 1)")
	if _, err := db.Exec(context.Background(), "DELETE FROM setdefault_old_parent WHERE id = 1"); err == nil {
		t.Fatal("ON DELETE SET DEFAULT allowed child default to reference the deleted parent row")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM setdefault_old_parent WHERE id = 1"); got != "1" {
		t.Fatalf("parent delete should roll back when default references deleted parent, remaining count %s", got)
	}
	if got := scalar(t, db, "SELECT parent_id FROM setdefault_old_child WHERE id = 10"); got != "1" {
		t.Fatalf("child FK should remain unchanged after rejected delete, got %s", got)
	}

	mustExec(t, db, "CREATE TABLE setdefault_upd_old_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setdefault_upd_old_child (id INTEGER PRIMARY KEY, parent_id INTEGER DEFAULT 1, FOREIGN KEY (parent_id) REFERENCES setdefault_upd_old_parent(id) ON UPDATE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO setdefault_upd_old_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setdefault_upd_old_child VALUES (10, 1)")
	if _, err := db.Exec(context.Background(), "UPDATE setdefault_upd_old_parent SET id = 2 WHERE id = 1"); err == nil {
		t.Fatal("ON UPDATE SET DEFAULT allowed child default to reference the old parent value")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM setdefault_upd_old_parent WHERE id = 1"); got != "1" {
		t.Fatalf("parent update should roll back when default references old parent value, remaining count %s", got)
	}
	if got := scalar(t, db, "SELECT parent_id FROM setdefault_upd_old_child WHERE id = 10"); got != "1" {
		t.Fatalf("child FK should remain unchanged after rejected update, got %s", got)
	}
}

func TestRegression_ForeignKeySetDefaultRejectsDefaultParentDeletedInSameStatement(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE setdefault_multi_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE setdefault_multi_child (id INTEGER PRIMARY KEY, parent_id INTEGER DEFAULT 0, FOREIGN KEY (parent_id) REFERENCES setdefault_multi_parent(id) ON DELETE SET DEFAULT)")
	mustExec(t, db, "INSERT INTO setdefault_multi_parent VALUES (0)")
	mustExec(t, db, "INSERT INTO setdefault_multi_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO setdefault_multi_child VALUES (10, 1)")

	if _, err := db.Exec(context.Background(), "DELETE FROM setdefault_multi_parent WHERE id IN (0, 1)"); err == nil {
		t.Fatal("ON DELETE SET DEFAULT allowed default parent to be deleted in the same statement")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM setdefault_multi_parent"); got != "2" {
		t.Fatalf("multi-row parent delete should roll back, remaining count %s", got)
	}
	if got := scalar(t, db, "SELECT parent_id FROM setdefault_multi_child WHERE id = 10"); got != "1" {
		t.Fatalf("child FK should remain unchanged after rejected multi-row delete, got %s", got)
	}
}

func TestRegression_ForeignKeySetDefaultPropagatesThroughChain(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE setdefault_chain_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE setdefault_chain_child (id INTEGER PRIMARY KEY, pcode TEXT DEFAULT 'DEFAULT', FOREIGN KEY (pcode) REFERENCES setdefault_chain_parent(code) ON DELETE SET DEFAULT)")
	mustExec(t, db, "CREATE TABLE setdefault_chain_grandchild (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES setdefault_chain_child(pcode) ON UPDATE CASCADE)")
	mustExec(t, db, "INSERT INTO setdefault_chain_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO setdefault_chain_parent VALUES (2, 'DEFAULT')")
	mustExec(t, db, "INSERT INTO setdefault_chain_child VALUES (10, 'A')")
	mustExec(t, db, "INSERT INTO setdefault_chain_grandchild VALUES (100, 'A')")

	mustExec(t, db, "DELETE FROM setdefault_chain_parent WHERE id = 1")
	if got := scalar(t, db, "SELECT pcode FROM setdefault_chain_child WHERE id = 10"); got != "DEFAULT" {
		t.Fatalf("child FK should become DEFAULT after parent delete, got %s", got)
	}
	if got := scalar(t, db, "SELECT ccode FROM setdefault_chain_grandchild WHERE id = 100"); got != "DEFAULT" {
		t.Fatalf("grandchild FK should cascade after child SET DEFAULT, got %s", got)
	}
}

func TestRegression_ForeignKeyCascadeUpdateChainRestrictRollsBack(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE cascade_restrict_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE cascade_restrict_child (id INTEGER PRIMARY KEY, pcode TEXT, FOREIGN KEY (pcode) REFERENCES cascade_restrict_parent(code) ON UPDATE CASCADE)")
	mustExec(t, db, "CREATE TABLE cascade_restrict_grandchild (id INTEGER PRIMARY KEY, ccode TEXT, FOREIGN KEY (ccode) REFERENCES cascade_restrict_child(pcode) ON UPDATE RESTRICT)")
	mustExec(t, db, "INSERT INTO cascade_restrict_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO cascade_restrict_child VALUES (10, 'A')")
	mustExec(t, db, "INSERT INTO cascade_restrict_grandchild VALUES (100, 'A')")

	if _, err := db.Exec(context.Background(), "UPDATE cascade_restrict_parent SET code = 'B' WHERE id = 1"); err == nil {
		t.Fatal("chained ON UPDATE CASCADE bypassed downstream RESTRICT")
	}
	if got := scalar(t, db, "SELECT code FROM cascade_restrict_parent WHERE id = 1"); got != "A" {
		t.Fatalf("parent update should roll back after downstream RESTRICT, got %s", got)
	}
	if got := scalar(t, db, "SELECT pcode FROM cascade_restrict_child WHERE id = 10"); got != "A" {
		t.Fatalf("child cascade should roll back after downstream RESTRICT, got %s", got)
	}
	if got := scalar(t, db, "SELECT ccode FROM cascade_restrict_grandchild WHERE id = 100"); got != "A" {
		t.Fatalf("grandchild should remain unchanged after downstream RESTRICT, got %s", got)
	}
}

func TestRegression_ForeignKeyCascadeUniqueFailureRollsBackStatement(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE cascade_unique_stmt_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE cascade_unique_stmt_child (id INTEGER PRIMARY KEY, parent_code TEXT UNIQUE, FOREIGN KEY (parent_code) REFERENCES cascade_unique_stmt_parent(code) ON UPDATE CASCADE)")
	mustExec(t, db, "INSERT INTO cascade_unique_stmt_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO cascade_unique_stmt_parent VALUES (2, 'B')")
	mustExec(t, db, "INSERT INTO cascade_unique_stmt_child VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO cascade_unique_stmt_child VALUES (2, 'B')")

	if _, err := db.Exec(context.Background(), "UPDATE cascade_unique_stmt_parent SET code = 'Z'"); err == nil {
		t.Fatal("multi-row ON UPDATE CASCADE bypassed child UNIQUE constraint")
	}
	if got := scalar(t, db, "SELECT code FROM cascade_unique_stmt_parent WHERE id = 1"); got != "A" {
		t.Fatalf("failed cascade should roll back first parent row, got %s", got)
	}
	if got := scalar(t, db, "SELECT parent_code FROM cascade_unique_stmt_child WHERE id = 1"); got != "A" {
		t.Fatalf("failed cascade should roll back first child action, got %s", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM cascade_unique_stmt_child WHERE parent_code = 'Z'"); got != "0" {
		t.Fatalf("failed cascade should leave no child rows with parent_code Z, got %s", got)
	}
}

func TestRegression_ForeignKeyCascadeDeleteFailureRollsBackStatement(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE cascade_delete_stmt_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE cascade_delete_stmt_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES cascade_delete_stmt_parent(id) ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE cascade_delete_stmt_grandchild (id INTEGER PRIMARY KEY, child_id INTEGER, FOREIGN KEY (child_id) REFERENCES cascade_delete_stmt_child(id) ON DELETE RESTRICT)")
	mustExec(t, db, "INSERT INTO cascade_delete_stmt_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO cascade_delete_stmt_parent VALUES (2)")
	mustExec(t, db, "INSERT INTO cascade_delete_stmt_child VALUES (1, 1)")
	mustExec(t, db, "INSERT INTO cascade_delete_stmt_child VALUES (2, 2)")
	mustExec(t, db, "INSERT INTO cascade_delete_stmt_grandchild VALUES (1, 2)")

	if _, err := db.Exec(context.Background(), "DELETE FROM cascade_delete_stmt_parent"); err == nil {
		t.Fatal("multi-row ON DELETE CASCADE bypassed downstream RESTRICT constraint")
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM cascade_delete_stmt_parent"); got != "2" {
		t.Fatalf("failed cascade delete should restore parent rows, got %s", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM cascade_delete_stmt_child"); got != "2" {
		t.Fatalf("failed cascade delete should restore child rows, got %s", got)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM cascade_delete_stmt_grandchild"); got != "1" {
		t.Fatalf("failed cascade delete should leave grandchild rows unchanged, got %s", got)
	}
}

func TestRegression_ForeignKeyCascadeActionsRollbackWithTransaction(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE cascade_txn_upd_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE cascade_txn_upd_child (id INTEGER PRIMARY KEY, parent_code TEXT UNIQUE, FOREIGN KEY (parent_code) REFERENCES cascade_txn_upd_parent(code) ON UPDATE CASCADE)")
	mustExec(t, db, "INSERT INTO cascade_txn_upd_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO cascade_txn_upd_child VALUES (1, 'A')")
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "UPDATE cascade_txn_upd_parent SET code = 'B' WHERE id = 1")
	mustExec(t, db, "ROLLBACK")
	if got := scalar(t, db, "SELECT parent_code FROM cascade_txn_upd_child WHERE id = 1"); got != "A" {
		t.Fatalf("ROLLBACK should restore cascaded child update, got %s", got)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO cascade_txn_upd_child VALUES (2, 'A')"); err == nil {
		t.Fatal("ROLLBACK should restore child UNIQUE index entry for A")
	}

	mustExec(t, db, "CREATE TABLE cascade_txn_del_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE cascade_txn_del_child (id INTEGER PRIMARY KEY, parent_id INTEGER UNIQUE, FOREIGN KEY (parent_id) REFERENCES cascade_txn_del_parent(id) ON DELETE CASCADE)")
	mustExec(t, db, "INSERT INTO cascade_txn_del_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO cascade_txn_del_child VALUES (1, 1)")
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "DELETE FROM cascade_txn_del_parent WHERE id = 1")
	mustExec(t, db, "ROLLBACK")
	if got := scalar(t, db, "SELECT COUNT(*) FROM cascade_txn_del_child WHERE id = 1"); got != "1" {
		t.Fatalf("ROLLBACK should restore cascaded child delete, got %s", got)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO cascade_txn_del_child VALUES (2, 1)"); err == nil {
		t.Fatal("ROLLBACK should restore child UNIQUE index entry for parent_id 1")
	}
}

func TestRegression_ForeignKeyCascadeActionsRollbackToSavepoint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE cascade_sp_upd_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE cascade_sp_upd_child (id INTEGER PRIMARY KEY, parent_code TEXT UNIQUE, FOREIGN KEY (parent_code) REFERENCES cascade_sp_upd_parent(code) ON UPDATE CASCADE)")
	mustExec(t, db, "INSERT INTO cascade_sp_upd_parent VALUES (1, 'A')")
	mustExec(t, db, "INSERT INTO cascade_sp_upd_child VALUES (1, 'A')")
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "SAVEPOINT fk_upd")
	mustExec(t, db, "UPDATE cascade_sp_upd_parent SET code = 'B' WHERE id = 1")
	mustExec(t, db, "ROLLBACK TO SAVEPOINT fk_upd")
	mustExec(t, db, "COMMIT")
	if got := scalar(t, db, "SELECT parent_code FROM cascade_sp_upd_child WHERE id = 1"); got != "A" {
		t.Fatalf("ROLLBACK TO SAVEPOINT should restore cascaded child update, got %s", got)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO cascade_sp_upd_child VALUES (2, 'A')"); err == nil {
		t.Fatal("ROLLBACK TO SAVEPOINT should restore child UNIQUE index entry for A")
	}

	mustExec(t, db, "CREATE TABLE cascade_sp_del_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE cascade_sp_del_child (id INTEGER PRIMARY KEY, parent_id INTEGER UNIQUE, FOREIGN KEY (parent_id) REFERENCES cascade_sp_del_parent(id) ON DELETE CASCADE)")
	mustExec(t, db, "INSERT INTO cascade_sp_del_parent VALUES (1)")
	mustExec(t, db, "INSERT INTO cascade_sp_del_child VALUES (1, 1)")
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "SAVEPOINT fk_del")
	mustExec(t, db, "DELETE FROM cascade_sp_del_parent WHERE id = 1")
	mustExec(t, db, "ROLLBACK TO SAVEPOINT fk_del")
	mustExec(t, db, "COMMIT")
	if got := scalar(t, db, "SELECT COUNT(*) FROM cascade_sp_del_child WHERE id = 1"); got != "1" {
		t.Fatalf("ROLLBACK TO SAVEPOINT should restore cascaded child delete, got %s", got)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO cascade_sp_del_child VALUES (2, 1)"); err == nil {
		t.Fatal("ROLLBACK TO SAVEPOINT should restore child UNIQUE index entry for parent_id 1")
	}
}

func TestRegression_SelfReferentialCascadeDeleteTerminates(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE self_cycle_fk (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	mustExec(t, db, "INSERT INTO self_cycle_fk VALUES (1, 1)")
	mustExec(t, db, "ALTER TABLE self_cycle_fk ADD CONSTRAINT self_cycle_fk_parent FOREIGN KEY (parent_id) REFERENCES self_cycle_fk(id) ON DELETE CASCADE")

	if _, err := db.Exec(context.Background(), "DELETE FROM self_cycle_fk WHERE id = 1"); err != nil {
		t.Fatalf("self-referential cascade delete should terminate: %v", err)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM self_cycle_fk"); got != "0" {
		t.Fatalf("self-referential cascade should delete the row, got count %s", got)
	}
}

func TestRegression_CyclicCascadeDeleteTerminates(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE cycle_fk (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	mustExec(t, db, "INSERT INTO cycle_fk VALUES (1, 2)")
	mustExec(t, db, "INSERT INTO cycle_fk VALUES (2, 1)")
	mustExec(t, db, "ALTER TABLE cycle_fk ADD CONSTRAINT cycle_fk_parent FOREIGN KEY (parent_id) REFERENCES cycle_fk(id) ON DELETE CASCADE")

	if _, err := db.Exec(context.Background(), "DELETE FROM cycle_fk WHERE id = 1"); err != nil {
		t.Fatalf("cyclic cascade delete should terminate: %v", err)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM cycle_fk"); got != "0" {
		t.Fatalf("cyclic cascade should delete both rows, got count %s", got)
	}
}

func TestRegression_SelfReferentialCascadeUpdateUpdatesLocalFK(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE self_update_fk (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	mustExec(t, db, "INSERT INTO self_update_fk VALUES (1, 1)")
	mustExec(t, db, "ALTER TABLE self_update_fk ADD CONSTRAINT self_update_fk_parent FOREIGN KEY (parent_id) REFERENCES self_update_fk(id) ON UPDATE CASCADE")

	if _, err := db.Exec(context.Background(), "UPDATE self_update_fk SET id = 2 WHERE id = 1"); err != nil {
		t.Fatalf("self-referential cascade update should succeed: %v", err)
	}
	if got := scalar(t, db, "SELECT parent_id FROM self_update_fk WHERE id = 2"); got != "2" {
		t.Fatalf("self-referential cascade should update local FK to 2, got %s", got)
	}
}

func TestRegression_SelfReferentialCascadeUpdateSameColumnTerminates(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE self_update_same_col (id INTEGER PRIMARY KEY, code TEXT UNIQUE, FOREIGN KEY (code) REFERENCES self_update_same_col(code) ON UPDATE CASCADE)")
	mustExec(t, db, "INSERT INTO self_update_same_col VALUES (1, NULL)")
	mustExec(t, db, "UPDATE self_update_same_col SET code = 'A' WHERE id = 1")

	if _, err := db.Exec(context.Background(), "UPDATE self_update_same_col SET code = 'B' WHERE id = 1"); err != nil {
		t.Fatalf("same-column self-referential cascade update should terminate: %v", err)
	}
	if got := scalar(t, db, "SELECT code FROM self_update_same_col WHERE id = 1"); got != "B" {
		t.Fatalf("same-column self-referential cascade should update code to B, got %s", got)
	}
}

func TestRegression_DropReferencedTableRejected(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE drop_ref_parent (id INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE drop_ref_child (id INTEGER PRIMARY KEY, parent_id INTEGER, CONSTRAINT drop_ref_child_parent_fk FOREIGN KEY (parent_id) REFERENCES drop_ref_parent(id))")

	if _, err := db.Exec(context.Background(), "DROP TABLE drop_ref_parent"); err == nil {
		t.Fatal("DROP TABLE removed a table referenced by a foreign key")
	}
	if _, err := db.TableSchema("drop_ref_parent"); err != nil {
		t.Fatalf("referenced parent table should remain after failed DROP TABLE: %v", err)
	}

	mustExec(t, db, "DROP TABLE drop_ref_child")
	mustExec(t, db, "DROP TABLE drop_ref_parent")
}

func TestRegression_DropForeignKeyColumnRejected(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE drop_fk_col_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE drop_fk_col_child (id INTEGER PRIMARY KEY, parent_id INTEGER, CONSTRAINT drop_fk_col_child_parent_fk FOREIGN KEY (parent_id) REFERENCES drop_fk_col_parent(id))")
	if _, err := db.Exec(context.Background(), "ALTER TABLE drop_fk_col_child DROP COLUMN parent_id"); err == nil {
		t.Fatal("DROP COLUMN removed a local foreign key column")
	}

	mustExec(t, db, "CREATE TABLE drop_ref_col_parent (id INTEGER PRIMARY KEY, code TEXT)")
	mustExec(t, db, "CREATE TABLE drop_ref_col_child (id INTEGER PRIMARY KEY, parent_code TEXT, CONSTRAINT drop_ref_col_child_parent_fk FOREIGN KEY (parent_code) REFERENCES drop_ref_col_parent(code))")
	if _, err := db.Exec(context.Background(), "ALTER TABLE drop_ref_col_parent DROP COLUMN code"); err == nil {
		t.Fatal("DROP COLUMN removed a referenced foreign key column")
	}
}

func TestRegression_RenameCheckColumnUpdatesConstraint(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE rename_check_col (id INTEGER PRIMARY KEY, balance INTEGER, minimum INTEGER, CONSTRAINT rename_check_col_balance_ck CHECK (balance >= minimum))")
	mustExec(t, db, "ALTER TABLE rename_check_col RENAME COLUMN balance TO current_balance")

	if _, err := db.Exec(context.Background(), "INSERT INTO rename_check_col VALUES (1, 10, 5)"); err != nil {
		t.Fatalf("CHECK constraint did not follow renamed column: %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO rename_check_col VALUES (2, 1, 5)"); err == nil {
		t.Fatal("CHECK constraint accepted invalid row after column rename")
	}
	schema, err := db.TableSchema("rename_check_col")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !strings.Contains(schema, "current_balance") || strings.Contains(schema, "CHECK ((balance") {
		t.Fatalf("schema did not persist renamed CHECK column: %s", schema)
	}
}

func TestRegression_DropCheckColumnRejected(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE drop_check_col (id INTEGER PRIMARY KEY, balance INTEGER, minimum INTEGER, CONSTRAINT drop_check_col_balance_ck CHECK (balance >= minimum))")
	if _, err := db.Exec(context.Background(), "ALTER TABLE drop_check_col DROP COLUMN minimum"); err == nil {
		t.Fatal("DROP COLUMN removed a column referenced by a CHECK constraint")
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO drop_check_col VALUES (1, 10, 5)"); err != nil {
		t.Fatalf("table should remain usable after failed CHECK-column drop: %v", err)
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

func TestRegression_RLSSelectPolicyUsesBaseRowsBeforeProjectionAndLimit(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	base := context.Background()
	mustExec(t, db, "CREATE TABLE rls_projection_docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "INSERT INTO rls_projection_docs VALUES (1,'bob'),(2,'alice'),(3,'alice')")
	mustExec(t, db, "CREATE POLICY rls_projection_owner ON rls_projection_docs FOR SELECT USING (owner = current_user())")

	alice := context.WithValue(base, security.RLSUserKey, "alice")
	rows, err := db.Query(alice, "SELECT id FROM rls_projection_docs ORDER BY id LIMIT 1")
	if err != nil {
		t.Fatalf("query projected RLS column omitted: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	if got := strings.Join(ids, ""); got != "2" {
		t.Fatalf("alice projected ids = %q, want 2", got)
	}

	countRows, err := db.Query(alice, "SELECT COUNT(*) FROM rls_projection_docs")
	if err != nil {
		t.Fatalf("count as alice: %v", err)
	}
	defer countRows.Close()
	if !countRows.Next() {
		t.Fatal("COUNT(*) returned no row")
	}
	var count int64
	if err := countRows.Scan(&count); err != nil {
		t.Fatalf("scan count: %v", err)
	}
	if count != 2 {
		t.Fatalf("alice COUNT(*) = %d, want 2", count)
	}
}

func TestRegression_RLSFiltersJoinInputs(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	base := context.Background()
	mustExec(t, db, "CREATE TABLE rls_join_docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "CREATE TABLE rls_join_tags (id INTEGER PRIMARY KEY, doc_id INTEGER, owner TEXT)")
	mustExec(t, db, "INSERT INTO rls_join_docs VALUES (1,'bob'),(2,'alice')")
	mustExec(t, db, "INSERT INTO rls_join_tags VALUES (1,1,'bob'),(2,2,'alice'),(3,2,'bob')")
	mustExec(t, db, "CREATE POLICY rls_join_docs_owner ON rls_join_docs FOR SELECT USING (owner = current_user())")
	mustExec(t, db, "CREATE POLICY rls_join_tags_owner ON rls_join_tags FOR SELECT USING (owner = current_user())")

	alice := context.WithValue(base, security.RLSUserKey, "alice")
	rows, err := db.Query(alice, "SELECT rls_join_tags.id FROM rls_join_docs JOIN rls_join_tags ON rls_join_docs.id = rls_join_tags.doc_id ORDER BY rls_join_tags.id")
	if err != nil {
		t.Fatalf("query RLS join: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan join id: %v", err)
		}
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	if got := strings.Join(ids, ""); got != "2" {
		t.Fatalf("alice join tag ids = %q, want 2", got)
	}

	countRows, err := db.Query(alice, "SELECT COUNT(*) FROM rls_join_docs JOIN rls_join_tags ON rls_join_docs.id = rls_join_tags.doc_id")
	if err != nil {
		t.Fatalf("query RLS join count: %v", err)
	}
	defer countRows.Close()
	if !countRows.Next() {
		t.Fatal("join COUNT(*) returned no row")
	}
	var count int64
	if err := countRows.Scan(&count); err != nil {
		t.Fatalf("scan join count: %v", err)
	}
	if count != 1 {
		t.Fatalf("alice join COUNT(*) = %d, want 1", count)
	}
}

func TestRegression_RLSToPublicAppliesWithoutExplicitRole(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	base := context.Background()
	mustExec(t, db, "CREATE TABLE public_docs (id INTEGER PRIMARY KEY, visible BOOLEAN)")
	mustExec(t, db, "INSERT INTO public_docs VALUES (1,true),(2,false)")
	mustExec(t, db, "CREATE POLICY public_docs_visible ON public_docs FOR SELECT TO PUBLIC USING (visible = true)")

	ctx := context.WithValue(base, security.RLSUserKey, "alice")
	rows, err := db.Query(ctx, "SELECT id, visible FROM public_docs ORDER BY id")
	if err != nil {
		t.Fatalf("query as public user: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id int
		var visible bool
		if err := rows.Scan(&id, &visible); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	if got := strings.Join(ids, ""); got != "1" {
		t.Fatalf("TO PUBLIC policy returned ids %q, want 1", got)
	}
}

func TestRegression_RLSRestrictivePolicyNarrowsPermissivePolicy(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	base := context.Background()
	mustExec(t, db, "CREATE TABLE restrictive_docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "INSERT INTO restrictive_docs VALUES (1,'alice'),(2,'bob'),(3,'alice')")
	mustExec(t, db, "CREATE POLICY allow_all_docs ON restrictive_docs FOR SELECT USING (true)")
	mustExec(t, db, "CREATE POLICY owner_guard_docs ON restrictive_docs AS RESTRICTIVE FOR SELECT USING (owner = current_user())")

	alice := context.WithValue(base, security.RLSUserKey, "alice")
	rows, err := db.Query(alice, "SELECT id, owner FROM restrictive_docs ORDER BY id")
	if err != nil {
		t.Fatalf("query as alice: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id int
		var owner string
		if err := rows.Scan(&id, &owner); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	if got := strings.Join(ids, ""); got != "13" {
		t.Fatalf("alice sees ids %q, want 13", got)
	}

	ddl := db.RLSPolicyDDL()
	if len(ddl) != 3 {
		t.Fatalf("RLS DDL count = %d, want 3: %v", len(ddl), ddl)
	}
	if !strings.Contains(strings.Join(ddl, "\n"), "owner_guard_docs") ||
		!strings.Contains(strings.Join(ddl, "\n"), "AS RESTRICTIVE FOR SELECT") {
		t.Fatalf("restrictive policy DDL missing AS RESTRICTIVE: %v", ddl)
	}
}

func TestRegression_RLSPolicyPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rls-persist.db")
	ctx := context.Background()

	db, err := Open(path, &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE persisted_docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "INSERT INTO persisted_docs VALUES (1,'alice'),(2,'bob')")
	mustExec(t, db, "ALTER TABLE persisted_docs ENABLE ROW LEVEL SECURITY")
	mustExec(t, db, "CREATE POLICY persisted_docs_owner ON persisted_docs FOR SELECT USING (owner = current_user())")
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, &Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	bobCtx := context.WithValue(ctx, security.RLSUserKey, "bob")
	rows, err := reopened.Query(bobCtx, "SELECT id, owner FROM persisted_docs ORDER BY id")
	if err != nil {
		t.Fatalf("query as bob after reopen: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id int
		var owner string
		if err := rows.Scan(&id, &owner); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, fmt.Sprintf("%d", id))
	}
	if got := strings.Join(ids, ""); got != "2" {
		t.Fatalf("bob sees %q after reopen, want 2", got)
	}
	if ddl := reopened.RLSPolicyDDL(); len(ddl) != 2 {
		t.Fatalf("reopened RLS DDL count = %d, want 2: %v", len(ddl), ddl)
	}
}

func TestRegression_RLSEnabledTableWithoutPoliciesPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rls-no-policies.db")
	ctx := context.Background()

	db, err := Open(path, &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE locked_docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "INSERT INTO locked_docs VALUES (1,'alice')")
	mustExec(t, db, "ALTER TABLE locked_docs ENABLE ROW LEVEL SECURITY")
	if ddl := db.RLSPolicyDDL(); len(ddl) != 1 {
		t.Fatalf("RLS enabled table DDL count = %d, want 1: %v", len(ddl), ddl)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, &Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	bobCtx := context.WithValue(ctx, security.RLSUserKey, "bob")
	rows, err := reopened.Query(bobCtx, "SELECT id, owner FROM locked_docs ORDER BY id")
	if err != nil {
		t.Fatalf("query after reopen: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("policy-less RLS table returned rows after reopen, want deny-all")
	}
	if ddl := reopened.RLSPolicyDDL(); len(ddl) != 1 {
		t.Fatalf("reopened policy-less RLS DDL count = %d, want 1: %v", len(ddl), ddl)
	}
}

func TestRegression_DropTableRemovesPersistedRLSPolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rls-drop-table.db")
	ctx := context.Background()

	db, err := Open(path, &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE rls_drop_docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "ALTER TABLE rls_drop_docs ENABLE ROW LEVEL SECURITY")
	mustExec(t, db, "CREATE POLICY rls_drop_docs_owner ON rls_drop_docs FOR SELECT USING (owner = current_user())")
	mustExec(t, db, "DROP TABLE rls_drop_docs")
	if ddl := db.RLSPolicyDDL(); len(ddl) != 0 {
		t.Fatalf("drop table left in-memory RLS DDL: %v", ddl)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(path, &Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if ddl := reopened.RLSPolicyDDL(); len(ddl) != 0 {
		t.Fatalf("drop table left persisted RLS DDL after reopen: %v", ddl)
	}
	if _, err := reopened.Query(ctx, "SELECT id, owner FROM rls_drop_docs"); err == nil {
		t.Fatal("dropped table unexpectedly queryable after reopen")
	}
}

// TestRegression_RLSWriteEnforcement verifies write-side row-level security:
// INSERT is denied by WITH CHECK and DELETE/UPDATE only affect rows visible
// under the USING policy.
func TestRegression_RLSWriteEnforcement(t *testing.T) {
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
	mustExec(t, db, "INSERT INTO docs VALUES (1,'alice')")
	mustExec(t, db, "CREATE POLICY p1 ON docs FOR ALL USING (owner = current_user()) WITH CHECK (owner = current_user())")

	bob := context.WithValue(base, security.RLSUserKey, "bob")
	alice := context.WithValue(base, security.RLSUserKey, "alice")

	// bob cannot insert a row owned by alice (WITH CHECK).
	if _, err := db.Exec(bob, "INSERT INTO docs VALUES (2,'alice')"); err == nil {
		t.Error("expected WITH CHECK to deny bob inserting an alice-owned row")
	}
	// bob can insert his own row.
	if _, err := db.Exec(bob, "INSERT INTO docs VALUES (3,'bob')"); err != nil {
		t.Errorf("bob inserting his own row should succeed: %v", err)
	}
	// alice cannot delete bob's rows (not visible under USING).
	res, err := db.Exec(alice, "DELETE FROM docs WHERE owner='bob'")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Errorf("alice deleted %d of bob's rows, want 0", res.RowsAffected)
	}
}

func TestRegression_RLSInsertWithCheckWithoutUsing(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	base := context.Background()
	mustExec(t, db, "CREATE TABLE insert_check_docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "ALTER TABLE insert_check_docs ENABLE ROW LEVEL SECURITY")
	mustExec(t, db, "CREATE POLICY insert_check_owner ON insert_check_docs FOR INSERT WITH CHECK (owner = current_user())")

	bob := context.WithValue(base, security.RLSUserKey, "bob")
	if _, err := db.Exec(bob, "INSERT INTO insert_check_docs VALUES (1,'alice')"); err == nil {
		t.Fatal("WITH CHECK policy allowed bob to insert an alice-owned row")
	}
	if _, err := db.Exec(bob, "INSERT INTO insert_check_docs VALUES (2,'bob')"); err != nil {
		t.Fatalf("WITH CHECK policy rejected bob's own row: %v", err)
	}
	if got := scalar(t, db, "SELECT COUNT(*) FROM insert_check_docs"); got != "1" {
		t.Fatalf("insert_check_docs count = %s, want 1", got)
	}
}

func TestRegression_RLSUpdateWithCheckValidatesNewRow(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	base := context.Background()
	mustExec(t, db, "CREATE TABLE update_check_docs (id INTEGER PRIMARY KEY, owner TEXT)")
	mustExec(t, db, "INSERT INTO update_check_docs VALUES (1,'alice')")
	mustExec(t, db, "ALTER TABLE update_check_docs ENABLE ROW LEVEL SECURITY")
	mustExec(t, db, "CREATE POLICY update_check_owner ON update_check_docs FOR UPDATE USING (owner = current_user()) WITH CHECK (owner = current_user())")

	alice := context.WithValue(base, security.RLSUserKey, "alice")
	res, err := db.Exec(alice, "UPDATE update_check_docs SET owner = 'bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("update violating WITH CHECK returned error: %v", err)
	}
	if res.RowsAffected != 0 {
		t.Fatalf("WITH CHECK violating update affected %d rows, want 0", res.RowsAffected)
	}
	if got := scalar(t, db, "SELECT owner FROM update_check_docs WHERE id = 1"); got != "alice" {
		t.Fatalf("owner after rejected update = %s, want alice", got)
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

func TestRegression_TableIndexHints(t *testing.T) {
	db := openRegressionDB(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE hint_t (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "CREATE INDEX hint_t_name_idx ON hint_t(name)")
	mustExec(t, db, "INSERT INTO hint_t VALUES (1, 'Ada'), (2, 'Linus')")

	cases := map[string]string{
		"SELECT id FROM hint_t INDEXED BY hint_t_name_idx WHERE name = 'Ada'": "1",
		"SELECT id FROM hint_t NOT INDEXED WHERE name = 'Linus'":              "2",
	}
	for sql, want := range cases {
		if got := scalar(t, db, sql); got != want {
			t.Errorf("%q = %s, want %s", sql, got, want)
		}
	}

	rows := queryRows(t, db, "EXPLAIN SELECT * FROM hint_t INDEXED BY hint_t_name_idx WHERE name = 'Ada'")
	var sawHint bool
	for _, row := range rows {
		for _, val := range row {
			if strings.Contains(fmt.Sprintf("%v", val), "hint_t_name_idx") {
				sawHint = true
			}
		}
	}
	if !sawHint {
		t.Fatalf("EXPLAIN did not include INDEXED BY hint_t_name_idx: %v", rows)
	}
}
