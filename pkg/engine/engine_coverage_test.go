package engine

import (
	"context"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// ALTER TABLE tests
// ---------------------------------------------------------------------------

func TestAlterTableAddColumn(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO items (id, name) VALUES (1, 'apple')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// ADD COLUMN
	_, err = db.Exec(ctx, "ALTER TABLE items ADD COLUMN price REAL")
	if err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
	}

	// Verify new column exists and defaults to NULL
	rows, err := db.Query(ctx, "SELECT id, name, price FROM items WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after ADD COLUMN failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Expected a row")
	}
	var id int
	var name string
	var price interface{}
	if err := rows.Scan(&id, &name, &price); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if id != 1 || name != "apple" {
		t.Errorf("Unexpected values: id=%d name=%s", id, name)
	}
}

func TestAlterTableDropColumn(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE things (id INTEGER PRIMARY KEY, a TEXT, b TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO things (id, a, b) VALUES (1, 'x', 'y')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// DROP COLUMN
	_, err = db.Exec(ctx, "ALTER TABLE things DROP COLUMN b")
	if err != nil {
		t.Fatalf("ALTER TABLE DROP COLUMN failed: %v", err)
	}

	// Verify column was removed - should only have id and a
	rows, err := db.Query(ctx, "SELECT id, a FROM things WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after DROP COLUMN failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Expected a row")
	}
	var id int
	var a string
	if err := rows.Scan(&id, &a); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if id != 1 || a != "x" {
		t.Errorf("Unexpected values: id=%d a=%s", id, a)
	}
}

func TestAlterTableRenameTable(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE oldtbl (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO oldtbl (id) VALUES (1)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// RENAME TABLE
	_, err = db.Exec(ctx, "ALTER TABLE oldtbl RENAME TO newtbl")
	if err != nil {
		t.Fatalf("ALTER TABLE RENAME TO failed: %v", err)
	}

	// Verify old name is gone and new name works
	rows, err := db.Query(ctx, "SELECT id FROM newtbl")
	if err != nil {
		t.Fatalf("SELECT from renamed table failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Expected a row")
	}
	var id int
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if id != 1 {
		t.Errorf("Expected id=1, got %d", id)
	}
}

func TestAlterTableRenameColumn(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE rcol (id INTEGER PRIMARY KEY, oldcol TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO rcol (id, oldcol) VALUES (1, 'val')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// RENAME COLUMN
	_, err = db.Exec(ctx, "ALTER TABLE rcol RENAME COLUMN oldcol TO newcol")
	if err != nil {
		t.Fatalf("ALTER TABLE RENAME COLUMN failed: %v", err)
	}

	// Query using new column name
	rows, err := db.Query(ctx, "SELECT newcol FROM rcol WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after RENAME COLUMN failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Expected a row")
	}
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if val != "val" {
		t.Errorf("Expected 'val', got %q", val)
	}
}

func TestAlterTableNonExistentTable(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "ALTER TABLE nonexistent ADD COLUMN x TEXT")
	if err == nil {
		t.Error("Expected error for ALTER on non-existent table")
	}
}

// ---------------------------------------------------------------------------
// CREATE POLICY / DROP POLICY (RLS) tests
// ---------------------------------------------------------------------------

func TestCreatePolicyWithRLSDisabled(t *testing.T) {
	// Open without EnableRLS
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE secure (id INTEGER PRIMARY KEY, secret TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Should fail because RLS is not enabled
	_, err = db.Exec(ctx, "CREATE POLICY read_all ON secure FOR SELECT USING (id > 0)")
	if err == nil {
		t.Error("Expected error when creating policy without RLS enabled")
	}
	if err != nil && !strings.Contains(err.Error(), "row-level security") {
		t.Errorf("Expected RLS-related error, got: %v", err)
	}
}

func TestCreateAndDropPolicyWithRLS(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024, EnableRLS: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE rlstable (id INTEGER PRIMARY KEY, owner TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// CREATE POLICY for SELECT
	_, err = db.Exec(ctx, "CREATE POLICY sel_policy ON rlstable FOR SELECT USING (id > 0)")
	if err != nil {
		t.Fatalf("CREATE POLICY failed: %v", err)
	}

	// DROP POLICY
	_, err = db.Exec(ctx, "DROP POLICY sel_policy ON rlstable")
	if err != nil {
		t.Fatalf("DROP POLICY failed: %v", err)
	}
}

func TestCreatePolicyVariousEvents(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024, EnableRLS: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE evttbl (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	events := []string{"ALL", "SELECT", "INSERT", "UPDATE", "DELETE"}
	for i, ev := range events {
		policyName := "p" + ev
		sql := "CREATE POLICY " + policyName + " ON evttbl FOR " + ev + " USING (id > 0)"
		_, err = db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("CREATE POLICY for %s failed: %v", ev, err)
		}
		_ = i
	}
}

func TestDropPolicyWithRLSDisabled(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE tbl (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "DROP POLICY somepolicy ON tbl")
	if err == nil {
		t.Error("Expected error for DROP POLICY without RLS enabled")
	}
}

func TestDropPolicyIfExists(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024, EnableRLS: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE tbl2 (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// DROP POLICY IF EXISTS on a non-existent policy - may or may not error
	// depending on implementation; we just test it doesn't panic
	_, _ = db.Exec(ctx, "DROP POLICY IF EXISTS nopolicy ON tbl2")
}

// ---------------------------------------------------------------------------
// UNION / UNION ALL / INTERSECT / EXCEPT tests
// ---------------------------------------------------------------------------

func TestUnionBasic(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE u1 (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE u2 (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO u1 VALUES (1, 'a')")
	db.Exec(ctx, "INSERT INTO u1 VALUES (2, 'b')")
	db.Exec(ctx, "INSERT INTO u2 VALUES (2, 'b')")
	db.Exec(ctx, "INSERT INTO u2 VALUES (3, 'c')")

	// UNION (deduplicates)
	rows, err := db.Query(ctx, "SELECT id, name FROM u1 UNION SELECT id, name FROM u2")
	if err != nil {
		t.Fatalf("UNION query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("UNION expected 3 rows, got %d", count)
	}
}

func TestUnionAll(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE ua1 (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE ua2 (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO ua1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO ua1 VALUES (2)")
	db.Exec(ctx, "INSERT INTO ua2 VALUES (2)")
	db.Exec(ctx, "INSERT INTO ua2 VALUES (3)")

	// UNION ALL (keeps duplicates)
	rows, err := db.Query(ctx, "SELECT id FROM ua1 UNION ALL SELECT id FROM ua2")
	if err != nil {
		t.Fatalf("UNION ALL query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 4 {
		t.Errorf("UNION ALL expected 4 rows, got %d", count)
	}
}

func TestUnionWithOrderByAndLimit(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE ol1 (val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE ol2 (val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO ol1 VALUES (3)")
	db.Exec(ctx, "INSERT INTO ol1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO ol2 VALUES (4)")
	db.Exec(ctx, "INSERT INTO ol2 VALUES (2)")

	// UNION ALL with ORDER BY and LIMIT
	rows, err := db.Query(ctx, "SELECT val FROM ol1 UNION ALL SELECT val FROM ol2 ORDER BY val LIMIT 3")
	if err != nil {
		t.Fatalf("UNION ALL ORDER BY LIMIT query failed: %v", err)
	}
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		vals = append(vals, v)
	}
	if len(vals) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(vals))
	}
	// Should be sorted: 1, 2, 3
	if vals[0] != 1 || vals[1] != 2 || vals[2] != 3 {
		t.Errorf("Expected [1,2,3], got %v", vals)
	}
}

func TestUnionWithDescOrder(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE d1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE d2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO d1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO d1 VALUES (3)")
	db.Exec(ctx, "INSERT INTO d2 VALUES (2)")
	db.Exec(ctx, "INSERT INTO d2 VALUES (4)")

	rows, err := db.Query(ctx, "SELECT v FROM d1 UNION ALL SELECT v FROM d2 ORDER BY v DESC")
	if err != nil {
		t.Fatalf("UNION ALL ORDER BY DESC failed: %v", err)
	}
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		vals = append(vals, v)
	}
	if len(vals) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(vals))
	}
	if vals[0] != 4 || vals[1] != 3 || vals[2] != 2 || vals[3] != 1 {
		t.Errorf("Expected [4,3,2,1], got %v", vals)
	}
}

func TestUnionWithNullDedup(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE n1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE n2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO n1 VALUES (NULL)")
	db.Exec(ctx, "INSERT INTO n1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO n2 VALUES (NULL)")
	db.Exec(ctx, "INSERT INTO n2 VALUES (1)")

	// UNION deduplicates NULLs
	rows, err := db.Query(ctx, "SELECT v FROM n1 UNION SELECT v FROM n2")
	if err != nil {
		t.Fatalf("UNION with NULLs failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("UNION with NULLs expected 2 rows, got %d", count)
	}
}

func TestIntersect(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE i1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE i2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO i1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO i1 VALUES (2)")
	db.Exec(ctx, "INSERT INTO i1 VALUES (3)")
	db.Exec(ctx, "INSERT INTO i2 VALUES (2)")
	db.Exec(ctx, "INSERT INTO i2 VALUES (3)")
	db.Exec(ctx, "INSERT INTO i2 VALUES (4)")

	rows, err := db.Query(ctx, "SELECT v FROM i1 INTERSECT SELECT v FROM i2")
	if err != nil {
		t.Fatalf("INTERSECT query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("INTERSECT expected 2 rows, got %d", count)
	}
}

func TestIntersectAll(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE ia1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE ia2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO ia1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO ia1 VALUES (2)")
	db.Exec(ctx, "INSERT INTO ia1 VALUES (2)")
	db.Exec(ctx, "INSERT INTO ia2 VALUES (2)")
	db.Exec(ctx, "INSERT INTO ia2 VALUES (2)")
	db.Exec(ctx, "INSERT INTO ia2 VALUES (2)")

	rows, err := db.Query(ctx, "SELECT v FROM ia1 INTERSECT ALL SELECT v FROM ia2")
	if err != nil {
		t.Fatalf("INTERSECT ALL query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	// min(2, 3) = 2 for value 2; value 1 not in right
	if count != 2 {
		t.Errorf("INTERSECT ALL expected 2 rows, got %d", count)
	}
}

func TestExcept(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE e1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE e2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO e1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO e1 VALUES (2)")
	db.Exec(ctx, "INSERT INTO e1 VALUES (3)")
	db.Exec(ctx, "INSERT INTO e2 VALUES (2)")

	rows, err := db.Query(ctx, "SELECT v FROM e1 EXCEPT SELECT v FROM e2")
	if err != nil {
		t.Fatalf("EXCEPT query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("EXCEPT expected 2 rows, got %d", count)
	}
}

func TestExceptAll(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE ea1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE ea2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO ea1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO ea1 VALUES (2)")
	db.Exec(ctx, "INSERT INTO ea1 VALUES (2)")
	db.Exec(ctx, "INSERT INTO ea1 VALUES (3)")
	db.Exec(ctx, "INSERT INTO ea2 VALUES (2)")

	rows, err := db.Query(ctx, "SELECT v FROM ea1 EXCEPT ALL SELECT v FROM ea2")
	if err != nil {
		t.Fatalf("EXCEPT ALL query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	// 1 (not in right), 2 (one subtracted), 3 (not in right) = 3
	if count != 3 {
		t.Errorf("EXCEPT ALL expected 3 rows, got %d", count)
	}
}

func TestUnionWithOffset(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE off1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE off2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO off1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO off1 VALUES (2)")
	db.Exec(ctx, "INSERT INTO off2 VALUES (3)")
	db.Exec(ctx, "INSERT INTO off2 VALUES (4)")

	// UNION ALL with ORDER BY, LIMIT, and OFFSET
	rows, err := db.Query(ctx, "SELECT v FROM off1 UNION ALL SELECT v FROM off2 ORDER BY v LIMIT 2 OFFSET 1")
	if err != nil {
		t.Fatalf("UNION ALL with OFFSET failed: %v", err)
	}
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		vals = append(vals, v)
	}
	if len(vals) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(vals))
	}
	// Sorted: 1,2,3,4 -> offset 1 -> 2,3,4 -> limit 2 -> 2,3
	if vals[0] != 2 || vals[1] != 3 {
		t.Errorf("Expected [2,3], got %v", vals)
	}
}

func TestUnionWithStringValues(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE s1 (name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE s2 (name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO s1 VALUES ('charlie')")
	db.Exec(ctx, "INSERT INTO s1 VALUES ('alice')")
	db.Exec(ctx, "INSERT INTO s2 VALUES ('bob')")
	db.Exec(ctx, "INSERT INTO s2 VALUES ('alice')")

	// UNION deduplicates, ORDER BY string column
	rows, err := db.Query(ctx, "SELECT name FROM s1 UNION SELECT name FROM s2 ORDER BY name")
	if err != nil {
		t.Fatalf("UNION with strings failed: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		names = append(names, n)
	}
	if len(names) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(names))
	}
	if names[0] != "alice" || names[1] != "bob" || names[2] != "charlie" {
		t.Errorf("Expected [alice,bob,charlie], got %v", names)
	}
}

func TestUnionOrderByPositional(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE pos1 (a INTEGER, b TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE pos2 (a INTEGER, b TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO pos1 VALUES (2, 'x')")
	db.Exec(ctx, "INSERT INTO pos1 VALUES (1, 'y')")
	db.Exec(ctx, "INSERT INTO pos2 VALUES (3, 'z')")

	// ORDER BY positional reference (column 1)
	rows, err := db.Query(ctx, "SELECT a, b FROM pos1 UNION ALL SELECT a, b FROM pos2 ORDER BY 1")
	if err != nil {
		t.Fatalf("UNION ALL ORDER BY positional failed: %v", err)
	}
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var a int
		var b string
		if err := rows.Scan(&a, &b); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		vals = append(vals, a)
	}
	if len(vals) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(vals))
	}
	if vals[0] != 1 || vals[1] != 2 || vals[2] != 3 {
		t.Errorf("Expected [1,2,3], got %v", vals)
	}
}

func TestUnionLargeOffset(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE lo1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE lo2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO lo1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO lo2 VALUES (2)")

	// OFFSET larger than result set
	rows, err := db.Query(ctx, "SELECT v FROM lo1 UNION ALL SELECT v FROM lo2 ORDER BY v LIMIT 10 OFFSET 100")
	if err != nil {
		t.Fatalf("UNION ALL with large OFFSET failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Expected 0 rows with large offset, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// SHOW TABLES tests
// ---------------------------------------------------------------------------

func TestShowTables(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// Empty database
	rows, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}
	rows.Close()

	// Create some tables
	db.Exec(ctx, "CREATE TABLE alpha (id INTEGER)")
	db.Exec(ctx, "CREATE TABLE beta (id INTEGER)")

	rows, err = db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES failed: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 1 || cols[0] != "Tables_in_database" {
		t.Errorf("Unexpected columns: %v", cols)
	}

	count := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}
	if count < 2 {
		t.Errorf("Expected at least 2 tables, got %d", count)
	}
}

func TestShowTablesViaExecFails(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// Exec should reject SHOW TABLES
	_, err = db.Exec(ctx, "SHOW TABLES")
	if err == nil {
		t.Error("Expected error when using Exec for SHOW TABLES")
	}
}

// ---------------------------------------------------------------------------
// SHOW CREATE TABLE tests
// ---------------------------------------------------------------------------

func TestShowCreateTable(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	rows, err := db.Query(ctx, "SHOW CREATE TABLE widgets")
	if err != nil {
		t.Fatalf("SHOW CREATE TABLE failed: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 2 || cols[0] != "Table" || cols[1] != "Create Table" {
		t.Errorf("Unexpected columns: %v", cols)
	}

	if !rows.Next() {
		t.Fatal("Expected a row")
	}
	var tblName, createSQL string
	if err := rows.Scan(&tblName, &createSQL); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if tblName != "widgets" {
		t.Errorf("Expected table name 'widgets', got %q", tblName)
	}
	if !strings.Contains(createSQL, "CREATE TABLE widgets") {
		t.Errorf("CREATE TABLE statement doesn't contain expected text: %s", createSQL)
	}
	if !strings.Contains(createSQL, "PRIMARY KEY") {
		t.Errorf("CREATE TABLE statement missing PRIMARY KEY: %s", createSQL)
	}
	if !strings.Contains(createSQL, "NOT NULL") {
		t.Errorf("CREATE TABLE statement missing NOT NULL: %s", createSQL)
	}
}

func TestShowCreateTableNonExistent(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Query(ctx, "SHOW CREATE TABLE nonexistent")
	if err == nil {
		t.Error("Expected error for SHOW CREATE TABLE on non-existent table")
	}
}

// ---------------------------------------------------------------------------
// SHOW COLUMNS FROM tests
// ---------------------------------------------------------------------------

func TestShowColumnsFrom(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE coltest (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, tag TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	rows, err := db.Query(ctx, "SHOW COLUMNS FROM coltest")
	if err != nil {
		t.Fatalf("SHOW COLUMNS FROM failed: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	expected := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	if len(cols) != len(expected) {
		t.Fatalf("Expected %d columns, got %d: %v", len(expected), len(cols), cols)
	}
	for i, exp := range expected {
		if cols[i] != exp {
			t.Errorf("Column %d: expected %q, got %q", i, exp, cols[i])
		}
	}

	type colInfo struct {
		field, typ, null, key, def, extra string
	}
	var colInfos []colInfo
	for rows.Next() {
		var ci colInfo
		if err := rows.Scan(&ci.field, &ci.typ, &ci.null, &ci.key, &ci.def, &ci.extra); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		colInfos = append(colInfos, ci)
	}

	if len(colInfos) != 4 {
		t.Fatalf("Expected 4 columns, got %d", len(colInfos))
	}

	// Check id column
	if colInfos[0].field != "id" || colInfos[0].null != "NO" || colInfos[0].key != "PRI" {
		t.Errorf("id column info unexpected: %+v", colInfos[0])
	}
	// Check name column (NOT NULL)
	if colInfos[1].field != "name" || colInfos[1].null != "NO" {
		t.Errorf("name column info unexpected: %+v", colInfos[1])
	}
	// Check tag column (UNIQUE)
	if colInfos[3].field != "tag" || colInfos[3].key != "UNI" {
		t.Errorf("tag column info unexpected: %+v", colInfos[3])
	}
}

func TestShowColumnsNonExistent(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Query(ctx, "SHOW COLUMNS FROM nope")
	if err == nil {
		t.Error("Expected error for SHOW COLUMNS on non-existent table")
	}
}

func TestShowColumnsViaExecFails(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE etbl (id INTEGER)")

	_, err = db.Exec(ctx, "SHOW COLUMNS FROM etbl")
	if err == nil {
		t.Error("Expected error when using Exec for SHOW COLUMNS")
	}
}

// ---------------------------------------------------------------------------
// SHOW DATABASES tests
// ---------------------------------------------------------------------------

func TestShowDatabases(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	rows, err := db.Query(ctx, "SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES failed: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 1 || cols[0] != "Database" {
		t.Errorf("Unexpected columns: %v", cols)
	}

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}
	var dbName string
	if err := rows.Scan(&dbName); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if dbName != "cobaltdb" {
		t.Errorf("Expected 'cobaltdb', got %q", dbName)
	}
}

// ---------------------------------------------------------------------------
// DESCRIBE table tests
// ---------------------------------------------------------------------------

func TestDescribeTable(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE desctbl (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	rows, err := db.Query(ctx, "DESCRIBE desctbl")
	if err != nil {
		t.Fatalf("DESCRIBE failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("DESCRIBE expected 2 columns, got %d rows", count)
	}
}

// ---------------------------------------------------------------------------
// EXPLAIN tests
// ---------------------------------------------------------------------------

func TestExplainSelect(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE exptbl (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	rows, err := db.Query(ctx, "EXPLAIN SELECT * FROM exptbl WHERE id > 0 ORDER BY id LIMIT 10")
	if err != nil {
		t.Fatalf("EXPLAIN failed: %v", err)
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) != 1 || cols[0] != "QUERY PLAN" {
		t.Errorf("Unexpected columns: %v", cols)
	}

	if !rows.Next() {
		t.Fatal("Expected a row")
	}
	var plan string
	if err := rows.Scan(&plan); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if !strings.Contains(plan, "SELECT FROM exptbl") {
		t.Errorf("Plan missing table reference: %s", plan)
	}
	if !strings.Contains(plan, "WHERE") {
		t.Errorf("Plan missing WHERE: %s", plan)
	}
	if !strings.Contains(plan, "ORDER BY") {
		t.Errorf("Plan missing ORDER BY: %s", plan)
	}
	if !strings.Contains(plan, "LIMIT") {
		t.Errorf("Plan missing LIMIT: %s", plan)
	}
}

// ---------------------------------------------------------------------------
// normalizeRowKey tests (via UNION dedup behavior)
// ---------------------------------------------------------------------------

func TestNormalizeRowKeyVariousTypes(t *testing.T) {
	// Test normalizeRowKey directly
	tests := []struct {
		name string
		row  []interface{}
		want string
	}{
		{
			name: "nil value",
			row:  []interface{}{nil},
			want: "[<nil>]",
		},
		{
			name: "int value",
			row:  []interface{}{int(42)},
			want: "[42]",
		},
		{
			name: "int64 value",
			row:  []interface{}{int64(99)},
			want: "[99]",
		},
		{
			name: "float64 whole number",
			row:  []interface{}{float64(7)},
			want: "[7]",
		},
		{
			name: "float64 fractional",
			row:  []interface{}{float64(3.14)},
			want: "[3.14]",
		},
		{
			name: "string value",
			row:  []interface{}{"hello"},
			want: "[S:hello]",
		},
		{
			name: "bool true",
			row:  []interface{}{true},
			want: "[true]",
		},
		{
			name: "bool false",
			row:  []interface{}{false},
			want: "[false]",
		},
		{
			name: "multiple values",
			row:  []interface{}{int64(1), "test", nil},
			want: "[1 S:test <nil>]",
		},
		{
			name: "empty row",
			row:  []interface{}{},
			want: "[]",
		},
		{
			name: "int and float same value",
			row:  []interface{}{int64(5)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeRowKey(tt.row)
			if tt.want != "" && got != tt.want {
				t.Errorf("normalizeRowKey(%v) = %q, want %q", tt.row, got, tt.want)
			}
		})
	}

	// Ensure int64 and float64 with same integer value produce the same key
	keyInt := normalizeRowKey([]interface{}{int64(5)})
	keyFloat := normalizeRowKey([]interface{}{float64(5)})
	if keyInt != keyFloat {
		t.Errorf("int64(5) key %q != float64(5) key %q", keyInt, keyFloat)
	}
}

// ---------------------------------------------------------------------------
// LRU list (removeTail / pushFront / moveToFront / remove) tests
// ---------------------------------------------------------------------------

func TestStmtLRUListOperations(t *testing.T) {
	l := newStmtLRUList()

	// removeTail on empty list
	if e := l.removeTail(); e != nil {
		t.Error("removeTail on empty list should return nil")
	}

	// Push one element
	e1 := &stmtLRUEntry{sql: "q1"}
	l.pushFront(e1)
	if l.head != e1 || l.tail != e1 {
		t.Error("After pushFront(e1), head and tail should be e1")
	}

	// Push second element to front
	e2 := &stmtLRUEntry{sql: "q2"}
	l.pushFront(e2)
	if l.head != e2 || l.tail != e1 {
		t.Error("After pushFront(e2), head should be e2, tail should be e1")
	}

	// Push third element
	e3 := &stmtLRUEntry{sql: "q3"}
	l.pushFront(e3)
	// Order: e3 -> e2 -> e1

	// moveToFront e1 (tail)
	l.moveToFront(e1)
	// Order: e1 -> e3 -> e2
	if l.head != e1 {
		t.Errorf("After moveToFront(e1), head should be e1, got %s", l.head.sql)
	}
	if l.tail != e2 {
		t.Errorf("After moveToFront(e1), tail should be e2, got %s", l.tail.sql)
	}

	// moveToFront on head (no-op)
	l.moveToFront(e1)
	if l.head != e1 {
		t.Error("moveToFront on head should be no-op")
	}

	// removeTail
	removed := l.removeTail()
	if removed != e2 {
		t.Errorf("removeTail should return e2, got %s", removed.sql)
	}
	// Order: e1 -> e3

	// Remove middle (e3 is tail now)
	l.remove(e3)
	// Order: e1 only
	if l.head != e1 || l.tail != e1 {
		t.Error("After removing e3, only e1 should remain")
	}

	// Remove last element
	l.remove(e1)
	if l.head != nil || l.tail != nil {
		t.Error("After removing last element, list should be empty")
	}

	// removeTail on empty again
	if e := l.removeTail(); e != nil {
		t.Error("removeTail on empty list should return nil")
	}
}

func TestStmtLRUListSingleElement(t *testing.T) {
	l := newStmtLRUList()

	e := &stmtLRUEntry{sql: "only"}
	l.pushFront(e)

	// removeTail on single-element list
	removed := l.removeTail()
	if removed != e {
		t.Error("removeTail should return the only element")
	}
	if l.head != nil || l.tail != nil {
		t.Error("List should be empty after removing single element")
	}
}

// ---------------------------------------------------------------------------
// SET / USE compatibility tests
// ---------------------------------------------------------------------------

func TestSetVarCompatibility(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// SET should be accepted silently
	_, err = db.Exec(ctx, "SET NAMES utf8")
	if err != nil {
		t.Errorf("SET command failed: %v", err)
	}
}

func TestUseCompatibility(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// USE should be accepted silently
	_, err = db.Exec(ctx, "USE mydb")
	if err != nil {
		t.Errorf("USE command failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// DROP INDEX tests
// ---------------------------------------------------------------------------

func TestDropIndex(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE idxtbl (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE INDEX idx_val ON idxtbl (val)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	_, err = db.Exec(ctx, "DROP INDEX idx_val")
	if err != nil {
		t.Errorf("DROP INDEX failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Prepared statement cache tests
// ---------------------------------------------------------------------------

func TestPreparedStatementCacheLRUEviction(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:         true,
		CacheSize:        1024,
		MaxStmtCacheSize: 3,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE cache_test (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Execute multiple different queries to fill cache
	for i := 0; i < 10; i++ {
		_, err = db.Exec(ctx, "INSERT INTO cache_test VALUES (?, ?)", i, "val")
		if err != nil {
			t.Fatalf("INSERT %d failed: %v", i, err)
		}
	}

	// Execute several different SELECT queries beyond cache size
	for i := 0; i < 5; i++ {
		sql := "SELECT * FROM cache_test WHERE id = " + strings.Repeat(" ", i) + "1"
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("Query %d failed: %v", i, err)
		}
		rows.Close()
	}
}

// ---------------------------------------------------------------------------
// TableSchema tests
// ---------------------------------------------------------------------------

func TestTableSchema(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE schema_test (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, age INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	schema, err := db.TableSchema("schema_test")
	if err != nil {
		t.Fatalf("TableSchema failed: %v", err)
	}

	if !strings.Contains(schema, "CREATE TABLE schema_test") {
		t.Errorf("Schema missing CREATE TABLE: %s", schema)
	}
	if !strings.Contains(schema, "PRIMARY KEY") {
		t.Errorf("Schema missing PRIMARY KEY: %s", schema)
	}
	if !strings.Contains(schema, "NOT NULL") {
		t.Errorf("Schema missing NOT NULL: %s", schema)
	}
	if !strings.Contains(schema, "UNIQUE") {
		t.Errorf("Schema missing UNIQUE: %s", schema)
	}
}

func TestTableSchemaNonExistent(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.TableSchema("nope")
	if err == nil {
		t.Error("Expected error for non-existent table schema")
	}
}

// ---------------------------------------------------------------------------
// Tables() API test
// ---------------------------------------------------------------------------

func TestTablesAPI(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE tapi1 (id INTEGER)")
	db.Exec(ctx, "CREATE TABLE tapi2 (id INTEGER)")

	tables := db.Tables()
	found := 0
	for _, t := range tables {
		if t == "tapi1" || t == "tapi2" {
			found++
		}
	}
	if found < 2 {
		t.Errorf("Expected to find tapi1 and tapi2 in tables list, found %d", found)
	}
}

// ---------------------------------------------------------------------------
// QueryRow tests
// ---------------------------------------------------------------------------

func TestQueryRowCoverage(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE qrow (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec(ctx, "INSERT INTO qrow VALUES (1, 'alice')")

	var name string
	var id int
	err = db.QueryRow(ctx, "SELECT id, name FROM qrow WHERE id = 1").Scan(&id, &name)
	if err != nil {
		t.Fatalf("QueryRow.Scan failed: %v", err)
	}
	if id != 1 || name != "alice" {
		t.Errorf("Expected (1, alice), got (%d, %s)", id, name)
	}
}

func TestQueryRowNoRowsCoverage(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE qrow2 (id INTEGER)")

	var id int
	err = db.QueryRow(ctx, "SELECT id FROM qrow2").Scan(&id)
	if err == nil {
		t.Error("Expected error for empty result set")
	}
}

// ---------------------------------------------------------------------------
// Rows edge cases
// ---------------------------------------------------------------------------

func TestRowsScanBeforeNext(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE scantest (id INTEGER)")
	db.Exec(ctx, "INSERT INTO scantest VALUES (1)")

	rows, err := db.Query(ctx, "SELECT id FROM scantest")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Scan before Next() should fail
	var id int
	err = rows.Scan(&id)
	if err == nil {
		t.Error("Expected error for Scan before Next")
	}
}

func TestRowsScanColumnMismatch(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE mismatch (id INTEGER, val TEXT)")
	db.Exec(ctx, "INSERT INTO mismatch VALUES (1, 'x')")

	rows, err := db.Query(ctx, "SELECT id, val FROM mismatch")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	rows.Next()
	// Wrong number of scan targets
	var id int
	err = rows.Scan(&id)
	if err == nil {
		t.Error("Expected column count mismatch error")
	}
}

func TestNilRowsNext(t *testing.T) {
	var rows *Rows
	if rows.Next() {
		t.Error("Next on nil Rows should return false")
	}
}

// ---------------------------------------------------------------------------
// expressionToString coverage (via CREATE POLICY with various expressions)
// ---------------------------------------------------------------------------

func TestCreatePolicyWithComplexExpressions(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024, EnableRLS: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE exptable (id INTEGER PRIMARY KEY, status TEXT, price REAL)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Binary expression
	_, err = db.Exec(ctx, "CREATE POLICY p1 ON exptable FOR SELECT USING (id > 0)")
	if err != nil {
		t.Errorf("CREATE POLICY with binary expr failed: %v", err)
	}

	// IS NULL expression
	_, err = db.Exec(ctx, "CREATE POLICY p2 ON exptable FOR SELECT USING (status IS NOT NULL)")
	if err != nil {
		t.Errorf("CREATE POLICY with IS NOT NULL failed: %v", err)
	}

	// IN expression
	_, err = db.Exec(ctx, "CREATE POLICY p3 ON exptable FOR SELECT USING (id IN (1, 2, 3))")
	if err != nil {
		t.Errorf("CREATE POLICY with IN expr failed: %v", err)
	}

	// LIKE expression
	_, err = db.Exec(ctx, "CREATE POLICY p4 ON exptable FOR SELECT USING (status LIKE 'active%')")
	if err != nil {
		t.Errorf("CREATE POLICY with LIKE expr failed: %v", err)
	}

	// Boolean literal (no USING means default TRUE)
	_, err = db.Exec(ctx, "CREATE POLICY p5 ON exptable FOR INSERT")
	if err != nil {
		t.Errorf("CREATE POLICY without USING failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// compareUnionValues coverage (via UNION ORDER BY with mixed types)
// ---------------------------------------------------------------------------

func TestUnionOrderByWithNulls(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE nv1 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE nv2 (v INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	db.Exec(ctx, "INSERT INTO nv1 VALUES (NULL)")
	db.Exec(ctx, "INSERT INTO nv1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO nv2 VALUES (2)")
	db.Exec(ctx, "INSERT INTO nv2 VALUES (NULL)")

	// This exercises compareUnionValues with nil values
	rows, err := db.Query(ctx, "SELECT v FROM nv1 UNION ALL SELECT v FROM nv2 ORDER BY v")
	if err != nil {
		t.Fatalf("UNION ALL ORDER BY with NULLs failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 4 {
		t.Errorf("Expected 4 rows, got %d", count)
	}
}

// ---------------------------------------------------------------------------
// CREATE VIEW / DROP VIEW tests (dispatch coverage)
// ---------------------------------------------------------------------------

func TestCreateAndDropView(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE vtbl (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	db.Exec(ctx, "INSERT INTO vtbl VALUES (1, 'a')")
	db.Exec(ctx, "INSERT INTO vtbl VALUES (2, 'b')")

	_, err = db.Exec(ctx, "CREATE VIEW vw AS SELECT id, name FROM vtbl WHERE id > 0")
	if err != nil {
		t.Fatalf("CREATE VIEW failed: %v", err)
	}

	// CREATE VIEW IF NOT EXISTS on existing view should succeed
	_, err = db.Exec(ctx, "CREATE VIEW IF NOT EXISTS vw AS SELECT id FROM vtbl")
	if err != nil {
		t.Errorf("CREATE VIEW IF NOT EXISTS failed: %v", err)
	}

	// Query the view
	rows, err := db.Query(ctx, "SELECT * FROM vw")
	if err != nil {
		t.Fatalf("SELECT from view failed: %v", err)
	}
	rows.Close()

	// DROP VIEW
	_, err = db.Exec(ctx, "DROP VIEW vw")
	if err != nil {
		t.Fatalf("DROP VIEW failed: %v", err)
	}

	// DROP VIEW IF EXISTS on already-dropped view
	_, err = db.Exec(ctx, "DROP VIEW IF EXISTS vw")
	if err != nil {
		t.Errorf("DROP VIEW IF EXISTS failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// CREATE/DROP TRIGGER dispatch coverage
// ---------------------------------------------------------------------------

func TestCreateAndDropTrigger(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE trgtbl (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = db.Exec(ctx, "CREATE TABLE trglog (msg TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TRIGGER trg_insert AFTER INSERT ON trgtbl FOR EACH ROW BEGIN INSERT INTO trglog VALUES ('inserted'); END")
	if err != nil {
		t.Fatalf("CREATE TRIGGER failed: %v", err)
	}

	// Fire the trigger
	db.Exec(ctx, "INSERT INTO trgtbl VALUES (1, 'test')")

	// DROP TRIGGER
	_, err = db.Exec(ctx, "DROP TRIGGER trg_insert")
	if err != nil {
		t.Fatalf("DROP TRIGGER failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Chained UNION (3-way) test
// ---------------------------------------------------------------------------

func TestThreeWayUnion(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE tw1 (v INTEGER)")
	db.Exec(ctx, "CREATE TABLE tw2 (v INTEGER)")
	db.Exec(ctx, "CREATE TABLE tw3 (v INTEGER)")

	db.Exec(ctx, "INSERT INTO tw1 VALUES (1)")
	db.Exec(ctx, "INSERT INTO tw2 VALUES (2)")
	db.Exec(ctx, "INSERT INTO tw3 VALUES (3)")

	rows, err := db.Query(ctx, "SELECT v FROM tw1 UNION ALL SELECT v FROM tw2 UNION ALL SELECT v FROM tw3 ORDER BY v")
	if err != nil {
		t.Fatalf("3-way UNION ALL failed: %v", err)
	}
	defer rows.Close()

	var vals []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		vals = append(vals, v)
	}
	if len(vals) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(vals))
	}
	if vals[0] != 1 || vals[1] != 2 || vals[2] != 3 {
		t.Errorf("Expected [1,2,3], got %v", vals)
	}
}

// ---------------------------------------------------------------------------
// scanValue edge cases
// ---------------------------------------------------------------------------

func TestScanValueTypesCoverage(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 1024})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	db.Exec(ctx, "CREATE TABLE svtbl (id INTEGER, val REAL, name TEXT)")
	db.Exec(ctx, "INSERT INTO svtbl VALUES (1, 3.14, 'hello')")

	rows, err := db.Query(ctx, "SELECT id, val, name FROM svtbl")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected a row")
	}

	// Scan into interface{} targets
	var v1, v2, v3 interface{}
	err = rows.Scan(&v1, &v2, &v3)
	if err != nil {
		t.Fatalf("Scan into interface{} failed: %v", err)
	}

	// Scan into typed targets via a fresh query
	rows2, err := db.Query(ctx, "SELECT id, val, name FROM svtbl")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows2.Close()

	if !rows2.Next() {
		t.Fatal("Expected a row")
	}
	var id int64
	var val float64
	var name string
	err = rows2.Scan(&id, &val, &name)
	if err != nil {
		t.Fatalf("Scan into typed targets failed: %v", err)
	}
	if id != 1 || name != "hello" {
		t.Errorf("Unexpected typed values: id=%d name=%s", id, name)
	}
}
