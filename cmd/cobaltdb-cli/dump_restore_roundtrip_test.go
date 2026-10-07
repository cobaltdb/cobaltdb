package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// canon20 renders a scanned cell with an explicit TYPE tag so that a value
// whose type changed in the round trip is flagged, not just its text.
func canonDumpCell(v interface{}) string {
	switch val := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if val {
			return "bool:true"
		}
		return "bool:false"
	case int:
		return "int:" + strconv.Itoa(val)
	case int64:
		return "int:" + strconv.FormatInt(val, 10)
	case float64:
		return "float:" + strconv.FormatFloat(val, 'g', -1, 64)
	case float32:
		return "float:" + strconv.FormatFloat(float64(val), 'g', -1, 32)
	case string:
		return "str:" + val
	case []byte:
		return "bytes:" + string(val)
	default:
		return fmt.Sprintf("other:%T:%v", val, val)
	}
}

func readRowsCanonical(t *testing.T, db *engine.DB, quotedTable, orderCol string) [][]string {
	t.Helper()
	rows, err := db.Query(context.Background(), "SELECT * FROM "+quotedTable+" ORDER BY "+orderCol)
	if err != nil {
		t.Fatalf("query %s: %v", quotedTable, err)
	}
	defer rows.Close()
	cols := rows.Columns()
	var out [][]string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range vals {
			dest[i] = &vals[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("scan %s: %v", quotedTable, err)
		}
		rec := make([]string, len(vals))
		for i, v := range vals {
			rec[i] = canonDumpCell(v)
		}
		out = append(out, rec)
	}
	return out
}

// TestDumpRestoreRoundTripPreservesTablesAndRows drives the REAL dumpDatabase and
// restoreDatabase over tables covering NULLs, booleans, floats, negative
// numbers, quoted/reserved identifiers and special characters, then requires
// every table and row to match with type fidelity.
func TestDumpRestoreRoundTripPreservesTablesAndRows(t *testing.T) {
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer src.Close()
	ctx := context.Background()

	if _, err := src.Exec(ctx, "CREATE TABLE vals (id INTEGER PRIMARY KEY, i INTEGER, r REAL, b BOOLEAN, t TEXT, n TEXT)"); err != nil {
		t.Fatalf("create vals: %v", err)
	}
	valRows := []struct {
		id int
		i  int64
		r  float64
		b  bool
		t  string
		n  interface{}
	}{
		{1, -42, 0.1, true, "plain", nil},
		{2, 0, -2.5, false, "it's", ""},
		{3, 9223372036854775807, 1e100, true, "semi;colon", "NULL"},
		{4, -9223372036854775808, 3.141592653589793, false, "back\\slash", "line1\nline2"},
		{5, 7, 123456789.123456789, true, "tab\there", "  padded  "},
		{6, -1, -12345.6789, false, "€ euro \U0001F600 emoji", " quote\"inside"},
	}
	for _, r := range valRows {
		if _, err := src.Exec(ctx,
			"INSERT INTO vals (id, i, r, b, t, n) VALUES (?,?,?,?,?,?)",
			r.id, r.i, r.r, r.b, r.t, r.n); err != nil {
			t.Fatalf("insert vals id=%d: %v", r.id, err)
		}
	}
	// Quoted identifiers: mixed case and an embedded space exercise the
	// identifier-quoting path of dump and restore. (A reserved-word table
	// name such as "order" is rejected by the engine at DDL time by policy,
	// so it is out of scope for the dump/restore path.)
	if _, err := src.Exec(ctx, `CREATE TABLE "CaseTable" ("Col One" INTEGER PRIMARY KEY, "data" TEXT)`); err != nil {
		t.Fatalf(`create "CaseTable": %v`, err)
	}
	for _, rr := range [][2]interface{}{{int64(1), "first"}, {int64(2), "second"}} {
		if _, err := src.Exec(ctx, `INSERT INTO "CaseTable" ("Col One", "data") VALUES (?,?)`, rr[0], rr[1]); err != nil {
			t.Fatalf("insert CaseTable: %v", err)
		}
	}
	// A table with no rows must still be recreated by restore.
	if _, err := src.Exec(ctx, "CREATE TABLE empty_t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create empty_t: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "dump.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dumpDatabase: %v", err)
	}
	raw, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	t.Logf("dumped SQL (%d bytes):\n%s", len(raw), string(raw))

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restoreDatabase: %v", err)
	}

	type tc struct{ label, quoted, order string }
	tables := []tc{
		{"vals", "vals", `"id"`},
		{`"CaseTable"`, `"CaseTable"`, `"Col One"`},
		{"empty_t", "empty_t", `"id"`},
	}
	var fails []string
	for _, tbl := range tables {
		want := readRowsCanonical(t, src, tbl.quoted, tbl.order)
		got := readRowsCanonical(t, dst, tbl.quoted, tbl.order)
		if len(want) != len(got) {
			fails = append(fails, fmt.Sprintf("%s: restored %d rows, want %d", tbl.label, len(got), len(want)))
			continue
		}
		for ri := range want {
			for ci := range want[ri] {
				if want[ri][ci] != got[ri][ci] {
					fails = append(fails, fmt.Sprintf("%s row %d col %d: want %q got %q",
						tbl.label, ri+1, ci+1, want[ri][ci], got[ri][ci]))
				}
			}
		}
	}
	if len(fails) > 0 {
		t.Fatalf("FAIL: %d round-trip mismatch(es):\n  %s", len(fails), strings.Join(fails, "\n  "))
	}
	t.Log("PASS: dump/restore preserved every table and row with type fidelity")
}

// ddlListContains reports whether any DDL string names the given object.
func ddlListContains(ddl []string, name string) bool {
	for _, stmt := range ddl {
		if strings.Contains(stmt, name) {
			return true
		}
	}
	return false
}

// sortFKRefs returns a deterministically ordered copy for comparison.
func sortFKRefs(refs []engine.TableForeignKeyRef) []engine.TableForeignKeyRef {
	out := append([]engine.TableForeignKeyRef(nil), refs...)
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i]) < fmt.Sprint(out[j]) })
	return out
}

// TestDumpRestoreRecreatesIndexesAndForeignKeys extends the round-trip guard
// beyond table data: a dumped database must be restored with its secondary
// indexes and foreign keys intact, and those objects must still enforce their
// constraints in the restored database.
func TestDumpRestoreRecreatesIndexesAndForeignKeys(t *testing.T) {
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer src.Close()
	ctx := context.Background()

	exec := func(db *engine.DB, sql string) error {
		t.Helper()
		_, err := db.Exec(ctx, sql)
		return err
	}
	mustExec := func(db *engine.DB, sql string) {
		t.Helper()
		if err := exec(db, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	// A NAMED self-referencing FK (added via ALTER TABLE, the same form the
	// dump emits) so name fidelity can be asserted strictly, a plain
	// secondary index, and a UNIQUE index whose constraint can be probed
	// after restore.
	mustExec(src, `CREATE TABLE emp (
		id INTEGER PRIMARY KEY,
		mgr INTEGER,
		code TEXT,
		email TEXT
	)`)
	mustExec(src, `ALTER TABLE emp ADD CONSTRAINT emp_mgr_fk FOREIGN KEY (mgr) REFERENCES emp(id)`)
	mustExec(src, `CREATE INDEX idx_emp_code ON emp(code)`)
	mustExec(src, `CREATE UNIQUE INDEX idx_emp_email ON emp(email)`)
	mustExec(src, `INSERT INTO emp (id, mgr, code, email) VALUES (1, NULL, 'a', 'a@x')`)
	mustExec(src, `INSERT INTO emp (id, mgr, code, email) VALUES (2, 1, 'b', 'b@x')`)
	mustExec(src, `INSERT INTO emp (id, mgr, code, email) VALUES (3, 1, 'c', 'c@x')`)
	// An UNNAMED inline FK on a second table: the dump must synthesize a
	// constraint name for it (ADD CONSTRAINT requires one), so this pair is
	// compared on shape, not name.
	mustExec(src, `CREATE TABLE timesheet (
		id INTEGER PRIMARY KEY,
		emp_id INTEGER REFERENCES emp(id)
	)`)
	mustExec(src, `INSERT INTO timesheet (id, emp_id) VALUES (100, 1)`)

	dumpPath := filepath.Join(t.TempDir(), "emp.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dumpDatabase: %v", err)
	}
	raw, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	t.Logf("dumped SQL:\n%s", string(raw))

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restoreDatabase: %v", err)
	}

	// 1. Data rows round-tripped (control for the object checks below).
	for _, tbl := range []struct{ name, order string }{
		{"emp", `"id"`},
		{"timesheet", `"id"`},
	} {
		wantRows := readRowsCanonical(t, src, tbl.name, tbl.order)
		gotRows := readRowsCanonical(t, dst, tbl.name, tbl.order)
		if len(wantRows) != len(gotRows) {
			t.Fatalf("%s: restored %d rows, want %d", tbl.name, len(gotRows), len(wantRows))
		}
		for i := range wantRows {
			if strings.Join(wantRows[i], "|") != strings.Join(gotRows[i], "|") {
				t.Fatalf("%s row %d: want %v got %v", tbl.name, i+1, wantRows[i], gotRows[i])
			}
		}
	}

	// 2. Secondary indexes recreated: presence first (named failure), then
	//    exact DDL equality.
	srcIdx := src.TableIndexDDL("emp")
	dstIdx := dst.TableIndexDDL("emp")
	for _, want := range []string{"idx_emp_code", "idx_emp_email"} {
		if !ddlListContains(dstIdx, want) {
			t.Errorf("FAIL: index %q missing from restored database (dst index DDL: %v)", want, dstIdx)
		}
		if !ddlListContains(srcIdx, want) {
			t.Errorf("control: index %q missing from source database (src index DDL: %v)", want, srcIdx)
		}
	}
	sort.Strings(srcIdx)
	sort.Strings(dstIdx)
	if strings.Join(srcIdx, "\n") != strings.Join(dstIdx, "\n") {
		t.Errorf("FAIL: index DDL differs after restore.\nsrc: %q\ndst: %q", srcIdx, dstIdx)
	}

	// 3. Foreign keys recreated. The NAMED self-reference must keep its name
	//    exactly; the UNNAMED inline FK is compared on shape only, because
	//    the dump synthesizes a name (ADD CONSTRAINT requires one) — the
	//    same thing MySQL (ibfk_N) and Postgres (<table>_<col>_fkey) do.
	empSrcFK := src.TableForeignKeys("emp")
	empDstFK := dst.TableForeignKeys("emp")
	if len(empDstFK) != 1 || empDstFK[0].Name != "emp_mgr_fk" {
		t.Errorf("FAIL: named foreign key emp_mgr_fk not preserved.\nsrc: %v\ndst: %v", empSrcFK, empDstFK)
	}
	if !fkShapeEqual(empSrcFK, empDstFK) {
		t.Errorf("FAIL: emp foreign keys differ after restore.\nsrc: %v\ndst: %v", empSrcFK, empDstFK)
	}
	tsSrcFK := src.TableForeignKeys("timesheet")
	tsDstFK := dst.TableForeignKeys("timesheet")
	if len(tsDstFK) == 0 {
		t.Errorf("FAIL: foreign key on restored timesheet missing (source has %d): %v", len(tsSrcFK), tsDstFK)
	}
	if !fkShapeEqual(tsSrcFK, tsDstFK) {
		t.Errorf("FAIL: timesheet foreign keys differ after restore (shape).\nsrc: %v\ndst: %v", tsSrcFK, tsDstFK)
	}
	if len(dst.TableSelfForeignKeyRefs("emp")) == 0 {
		t.Errorf("FAIL: self-referencing foreign key (mgr -> emp.id) missing from restored database")
	}

	// 4. Constraints still enforced in the restored database. Each probe is
	//    mirrored on the source so the assertion measures restoration, not an
	//    engine quirk: restored behavior must match source behavior.
	probe := func(label, sql string) {
		t.Helper()
		srcRejected := exec(src, sql) != nil
		dstRejected := exec(dst, sql) != nil
		if srcRejected && !dstRejected {
			t.Errorf("FAIL: restored database accepted what the source rejects (%s): %s", label, sql)
		}
		if !srcRejected && dstRejected {
			t.Errorf("FAIL: restored database rejects what the source accepts (%s): %s", label, sql)
		}
	}
	probe("UNIQUE email", `INSERT INTO emp (id, mgr, code, email) VALUES (10, NULL, 'd', 'a@x')`)
	probe("FK missing parent (emp)", `INSERT INTO emp (id, mgr, code, email) VALUES (11, 999, 'e', 'e@x')`)
	probe("FK missing parent (timesheet)", `INSERT INTO timesheet (id, emp_id) VALUES (101, 999)`)
	// A valid self-reference must still be accepted after restore.
	if err := exec(dst, `INSERT INTO emp (id, mgr, code, email) VALUES (12, 1, 'f', 'f@x')`); err != nil {
		t.Errorf("FAIL: restored database rejected a valid self-reference: %v", err)
	}
}

// fkShapeEqual compares foreign keys, ignoring the constraint name where the
// source constraint is unnamed: ALTER TABLE ADD CONSTRAINT requires a name, so
// the dump legitimately synthesizes one for an unnamed inline FK.
func fkShapeEqual(srcFK, dstFK []engine.TableForeignKeyRef) bool {
	if len(srcFK) != len(dstFK) {
		return false
	}
	a := sortFKRefs(srcFK)
	b := append([]engine.TableForeignKeyRef(nil), sortFKRefs(dstFK)...)
	for i := range a {
		if a[i].Name == "" {
			b[i].Name = ""
		}
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// TestDumpRestoreRecreatesFullTextIndex extends the round-trip guard to
// full-text indexes.
//
// Discrimination note: MATCH ... AGAINST evaluates against each row's live
// column text on every query — the inverted index is NOT used to filter
// (CLAUDE.md, "Full-text search") — so the FTS DDL equality check below is
// the ONLY assertion here that can detect a missing FTS index. The MATCH
// probe is kept as a "search still works after restore" control and would
// stay green even if the index were dropped.
func TestDumpRestoreRecreatesFullTextIndex(t *testing.T) {
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer src.Close()
	ctx := context.Background()

	mustExec := func(db *engine.DB, sql string) {
		t.Helper()
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	mustExec(src, `CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)`)
	mustExec(src, `CREATE FULLTEXT INDEX fts_docs_body ON docs(body)`)
	mustExec(src, `INSERT INTO docs (id, body) VALUES (1, 'hello world')`)
	mustExec(src, `INSERT INTO docs (id, body) VALUES (2, 'cobalt database')`)
	mustExec(src, `INSERT INTO docs (id, body) VALUES (3, 'hello again')`)

	dumpPath := filepath.Join(t.TempDir(), "fts.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dumpDatabase: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restoreDatabase: %v", err)
	}

	// The discriminating assertion: the FTS index object must be recreated.
	srcFTS := src.FTSIndexDDL()
	dstFTS := dst.FTSIndexDDL()
	if len(dstFTS) == 0 {
		t.Errorf("FAIL: restored database has no full-text index (source has %d): %v", len(srcFTS), dstFTS)
	}
	sort.Strings(srcFTS)
	sort.Strings(dstFTS)
	if strings.Join(srcFTS, "\n") != strings.Join(dstFTS, "\n") {
		t.Errorf("FAIL: full-text index DDL differs after restore.\nsrc: %q\ndst: %q", srcFTS, dstFTS)
	}

	// Control: data rows round-tripped.
	wantRows := readRowsCanonical(t, src, "docs", `"id"`)
	gotRows := readRowsCanonical(t, dst, "docs", `"id"`)
	if len(wantRows) != len(gotRows) {
		t.Fatalf("docs: restored %d rows, want %d", len(gotRows), len(wantRows))
	}
	for i := range wantRows {
		if strings.Join(wantRows[i], "|") != strings.Join(gotRows[i], "|") {
			t.Fatalf("docs row %d: want %v got %v", i+1, wantRows[i], gotRows[i])
		}
	}

	// Control (non-discriminating, see doc comment): MATCH returns the same
	// rows on source and restored databases.
	ids := func(db *engine.DB) []int {
		t.Helper()
		rows, err := db.Query(ctx, `SELECT id FROM docs WHERE MATCH(body) AGAINST('hello')`)
		if err != nil {
			t.Fatalf("MATCH query: %v", err)
		}
		defer rows.Close()
		var got []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got = append(got, id)
		}
		sort.Ints(got)
		return got
	}
	srcIDs, dstIDs := ids(src), ids(dst)
	if !reflect.DeepEqual(srcIDs, dstIDs) {
		t.Errorf("FAIL: MATCH results differ after restore: src=%v dst=%v", srcIDs, dstIDs)
	}
	if len(srcIDs) != 2 || srcIDs[0] != 1 || srcIDs[1] != 3 {
		t.Errorf("control: unexpected MATCH result %v, want [1 3]", srcIDs)
	}
}
