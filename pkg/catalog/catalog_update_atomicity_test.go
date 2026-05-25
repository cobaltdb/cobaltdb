package catalog

import (
	"fmt"
	"strings"
	"testing"
)

func TestUpdateUniqueIndexFailureRollsBackAppliedRows(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_atomic (id INTEGER PRIMARY KEY, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX idx_upd_atomic_email ON upd_atomic(email)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_atomic (id, email) VALUES (1, 'a@example.com'), (2, 'b@example.com')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err := c.ExecuteQuery("UPDATE upd_atomic SET email = 'same@example.com'")
	if err == nil || !strings.Contains(err.Error(), "UNIQUE constraint failed") {
		t.Fatalf("expected unique constraint error, got %v", err)
	}

	result, err := c.ExecuteQuery("SELECT id, email FROM upd_atomic ORDER BY id")
	if err != nil {
		t.Fatalf("select after failed update: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if fmt.Sprint(result.Rows[0][1]) != "a@example.com" || fmt.Sprint(result.Rows[1][1]) != "b@example.com" {
		t.Fatalf("failed update left partial row changes: %+v", result.Rows)
	}

	indexed, err := c.ExecuteQuery("SELECT id FROM upd_atomic WHERE email = 'same@example.com'")
	if err != nil {
		t.Fatalf("indexed select after failed update: %v", err)
	}
	if len(indexed.Rows) != 0 {
		t.Fatalf("failed update left stale index entries: %+v", indexed.Rows)
	}
}
