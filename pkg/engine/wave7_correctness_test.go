package engine

import (
	"context"
	"testing"
)

// TestDropColumnPreservesSoftDeletes verifies ALTER TABLE DROP COLUMN does not
// resurrect soft-deleted rows (it must preserve the versioned-row metadata).
func TestDropColumnPreservesSoftDeletes(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, extra TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1,'alice','x')")
	mustExec(t, db, "INSERT INTO t VALUES (2,'bob','y')")
	mustExec(t, db, "DELETE FROM t WHERE id = 2")
	mustExec(t, db, "ALTER TABLE t DROP COLUMN extra")

	var n int64
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("after DROP COLUMN, COUNT = %d, want 1 (soft-deleted row resurrected)", n)
	}
}

// TestViewWithOrderByLimit verifies a view's own ORDER BY / LIMIT is honored.
func TestViewWithOrderByLimit(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE products (id INTEGER, price INTEGER)")
	for _, v := range [][2]int{{1, 10}, {2, 50}, {3, 30}, {4, 90}, {5, 20}} {
		mustExec(t, db, "INSERT INTO products VALUES ("+itoa(v[0])+", "+itoa(v[1])+")")
	}
	mustExec(t, db, "CREATE VIEW top3 AS SELECT id FROM products ORDER BY price DESC LIMIT 3")

	rows, err := db.Query(ctx, "SELECT id FROM top3")
	if err != nil {
		t.Fatalf("query view: %v", err)
	}
	defer rows.Close()
	var got []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, id)
	}
	// Top 3 by price DESC: 90(id4), 50(id2), 30(id3).
	want := []int64{4, 2, 3}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("view ORDER BY/LIMIT ignored: got %v, want %v", got, want)
	}
}

// TestReplaceEvictionRollback verifies that rolling back a transaction whose
// INSERT OR REPLACE evicted a different row (via a UNIQUE secondary index)
// restores the evicted row.
func TestReplaceEvictionRollback(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_email ON t(email)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'a@x')")
	mustExec(t, db, "INSERT INTO t VALUES (2, 'b@x')")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Collides with row 1 on idx_email; row 1 is evicted.
	if _, err := tx.Exec(ctx, "INSERT OR REPLACE INTO t (id, email) VALUES (3, 'a@x')"); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	rows, err := db.Query(ctx, "SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var got []int64
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		got = append(got, id)
	}
	// After rollback both original rows must remain; the new row must be gone.
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("after rollback, ids = %v, want [1 2] (evicted row lost)", got)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
