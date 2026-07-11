package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestFDWCSVSelect(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a temp CSV file
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	content := "id,name,score\n1,alice,95\n2,bob,87\n3,charlie,92\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Create foreign table
	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext_users (id INTEGER, name TEXT, score INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`,
		csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	// SELECT from foreign table
	rows, err := db.Query(ctx, `SELECT id, name, score FROM ext_users WHERE score > 90`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var results []struct {
		id    int
		name  string
		score int
	}
	for rows.Next() {
		var id, score int
		var name string
		if err := rows.Scan(&id, &name, &score); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		results = append(results, struct {
			id    int
			name  string
			score int
		}{id, name, score})
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(results))
	}
	if results[0].name != "alice" || results[0].score != 95 {
		t.Fatalf("Unexpected first row: %+v", results[0])
	}
	if results[1].name != "charlie" || results[1].score != 92 {
		t.Fatalf("Unexpected second row: %+v", results[1])
	}
}

func TestFDWDrop(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(csvPath, []byte("a,b\n1,2\n"), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext (a INTEGER, b INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`, csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	_, err = db.Exec(ctx, `DROP TABLE ext`)
	if err != nil {
		t.Fatalf("Failed to drop foreign table: %v", err)
	}

	_, err = db.Query(ctx, `SELECT * FROM ext`)
	if err == nil {
		t.Fatal("Expected query on dropped foreign table to fail")
	}
}

func TestFDWJoinWithLocal(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create local table
	_, err = db.Exec(ctx, `CREATE TABLE local_depts (id INTEGER PRIMARY KEY, dept TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create local table: %v", err)
	}
	_, err = db.Exec(ctx, `INSERT INTO local_depts VALUES (1, 'engineering'), (2, 'sales')`)
	if err != nil {
		t.Fatalf("Failed to insert into local table: %v", err)
	}

	// Create foreign table
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "users.csv")
	content := "user_id,user_name,dept_id\n1,alice,1\n2,bob,2\n3,charlie,1\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext_users (user_id INTEGER, user_name TEXT, dept_id INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`, csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	// JOIN foreign and local tables
	rows, err := db.Query(ctx, `
		SELECT u.user_name, d.dept
		FROM ext_users u
		JOIN local_depts d ON u.dept_id = d.id
		WHERE d.dept = 'engineering'
	`)
	if err != nil {
		t.Fatalf("JOIN query failed: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name, dept string
		if err := rows.Scan(&name, &dept); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		names = append(names, name)
	}

	if len(names) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(names))
	}
	if names[0] != "alice" || names[1] != "charlie" {
		t.Fatalf("Unexpected names: %v", names)
	}
}

func TestFDWInsertRejection(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(csvPath, []byte("a\n1\n"), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext (a INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`, csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO ext VALUES (2)`)
	if err == nil {
		t.Fatal("Expected INSERT into foreign table to fail")
	}
}

func TestFDWCSVMaxRowsLimitViaSQL(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "large.csv")
	content := "id,name\n1,alice\n2,bob\n3,charlie\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext_limited (id INTEGER, name TEXT) WRAPPER 'csv' OPTIONS (file '%s', max_rows '2')`,
		csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	_, err = db.Query(ctx, `SELECT * FROM ext_limited`)
	if err == nil {
		t.Fatal("Expected max_rows error from foreign table query")
	}
	if !strings.Contains(err.Error(), "row limit exceeded") {
		t.Fatalf("Expected row limit error, got %v", err)
	}
}

func TestFDWCSVProjectionPushdownViaSQL(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "project.csv")
	content := "id,name,score\n1,alice,95\n2,bob,87\n3,charlie,92\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	_, err = db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext_project (id INTEGER, name TEXT, score INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`,
		csvPath,
	))
	if err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	rows, err := db.Query(ctx, `SELECT name FROM ext_project WHERE score > 90 ORDER BY id`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		names = append(names, name)
	}
	want := []string{"alice", "charlie"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
}

// TestFDWCSVNumericEqualityPushdown is a regression test: the CSV `=`/`!=`
// predicate pushdown must compare numerically (not by raw string) so a cell
// like "30.50" matches `WHERE price = 30.5`. String comparison silently dropped
// rows the engine would otherwise match.
func TestFDWCSVNumericEqualityPushdown(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "prices.csv")
	// Non-canonical numeric strings: trailing zeros, integer-formatted float.
	content := "id,price\n1,30.50\n2,5.0\n3,007\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf(
		`CREATE FOREIGN TABLE ext_prices (id INTEGER, price REAL) WRAPPER 'csv' OPTIONS (file '%s')`, csvPath)); err != nil {
		t.Fatalf("create foreign table: %v", err)
	}

	idsFor := func(where string) []int64 {
		rows, err := db.Query(ctx, "SELECT id FROM ext_prices WHERE "+where+" ORDER BY id")
		if err != nil {
			t.Fatalf("query %q: %v", where, err)
		}
		defer rows.Close()
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			ids = append(ids, id)
		}
		return ids
	}

	if got := idsFor("price = 30.5"); len(got) != 1 || got[0] != 1 {
		t.Errorf("price = 30.5 returned %v, want [1]", got)
	}
	if got := idsFor("price = 5"); len(got) != 1 || got[0] != 2 {
		t.Errorf("price = 5 returned %v, want [2]", got)
	}
	if got := idsFor("price = 7"); len(got) != 1 || got[0] != 3 {
		t.Errorf("price = 7 returned %v, want [3]", got)
	}
	if got := idsFor("price != 30.5"); len(got) != 2 {
		t.Errorf("price != 30.5 returned %v, want 2 rows", got)
	}
}

// TestFDWAggregatesOverForeignTable is a regression test: aggregate queries over
// a foreign table were computed against c.tableTrees directly, which foreign
// tables are absent from, so COUNT/SUM/AVG/GROUP BY returned an empty result.
func TestFDWAggregatesOverForeignTable(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	dir := t.TempDir()
	p := filepath.Join(dir, "a.csv")
	if err := os.WriteFile(p, []byte("id,dept,score\n1,eng,95\n2,eng,80\n3,sales,92\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE FOREIGN TABLE ext (id INTEGER, dept TEXT, score INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`, p)); err != nil {
		t.Fatalf("create: %v", err)
	}
	scalar := func(sql string) string {
		r, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer r.Close()
		if !r.Next() {
			t.Fatalf("query %q returned no row", sql)
		}
		var v interface{}
		if err := r.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		return fmt.Sprintf("%v", v)
	}
	if got := scalar("SELECT COUNT(*) FROM ext"); got != "3" {
		t.Errorf("COUNT(*) = %s, want 3", got)
	}
	if got := scalar("SELECT SUM(score) FROM ext"); got != "267" {
		t.Errorf("SUM(score) = %s, want 267", got)
	}
	if got := scalar("SELECT COUNT(*) FROM ext WHERE score > 90"); got != "2" {
		t.Errorf("COUNT(*) WHERE score>90 = %s, want 2", got)
	}
}

// TestFDWFreshMaterializationAlwaysVisible is a statistical regression test for
// the intermittent empty-result bug: foreign-table rows were stamped with
// time.Now() at materialization, so when a scan straddled a Unix-second boundary
// the fresh rows' CreatedAt exceeded the query snapshot time and the temporal
// visibility check dropped them (~1 in 2000). The fix stamps a fixed early time
// so rows are always visible. Loop enough to catch a regression.
func TestFDWFreshMaterializationAlwaysVisible(t *testing.T) {
	iterations := 3000
	if testing.Short() {
		iterations = 30
	}
	ctx := context.Background()
	dir := t.TempDir()
	p := filepath.Join(dir, "v.csv")
	if err := os.WriteFile(p, []byte("id,name,score\n1,alice,95\n2,bob,87\n3,charlie,92\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	for i := 0; i < iterations; i++ {
		db, err := engine.Open(":memory:", engine.DefaultOptions())
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if _, err := db.Exec(ctx, fmt.Sprintf(`CREATE FOREIGN TABLE ext (id INTEGER, name TEXT, score INTEGER) WRAPPER 'csv' OPTIONS (file '%s')`, p)); err != nil {
			db.Close()
			t.Fatalf("create: %v", err)
		}
		r, err := db.Query(ctx, `SELECT id FROM ext WHERE score > 90 ORDER BY id`)
		n := 0
		if err == nil {
			for r.Next() {
				n++
			}
			r.Close()
		}
		db.Close()
		if n != 2 {
			t.Fatalf("iter %d: foreign scan returned %d rows, want 2 (temporal-visibility regression)", i, n)
		}
	}
}
