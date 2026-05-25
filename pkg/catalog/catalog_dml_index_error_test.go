package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestUpdateIndexDeleteFailureRollsBackRow(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_idx_delete_err (id INTEGER PRIMARY KEY, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_upd_delete_email ON upd_idx_delete_err(email)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_idx_delete_err (id, email) VALUES (1, 'a@example.com')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	c.indexTrees["idx_upd_delete_email"] = &deleteFailTree{
		TreeStore: c.indexTrees["idx_upd_delete_email"],
		err:       errors.New("index delete failed"),
	}

	_, err := c.ExecuteQuery("UPDATE upd_idx_delete_err SET email = 'b@example.com' WHERE id = 1")
	if err == nil || !strings.Contains(err.Error(), "index delete failed") {
		t.Fatalf("expected index delete error, got %v", err)
	}

	result, err := c.ExecuteQuery("SELECT email FROM upd_idx_delete_err WHERE id = 1")
	if err != nil {
		t.Fatalf("select after failed update: %v", err)
	}
	if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "a@example.com" {
		t.Fatalf("failed update should leave original row, got %+v", result.Rows)
	}
}

func TestDeleteRowIndexDeleteFailureKeepsRow(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE del_idx_delete_err (id INTEGER PRIMARY KEY, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_del_delete_email ON del_idx_delete_err(email)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO del_idx_delete_err (id, email) VALUES (1, 'a@example.com')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	c.indexTrees["idx_del_delete_email"] = &deleteFailTree{
		TreeStore: c.indexTrees["idx_del_delete_email"],
		err:       errors.New("index delete failed"),
	}

	err := c.DeleteRow(context.Background(), "del_idx_delete_err", int64(1))
	if err == nil || !strings.Contains(err.Error(), "index delete failed") {
		t.Fatalf("expected index delete error, got %v", err)
	}

	result, err := c.ExecuteQuery("SELECT email FROM del_idx_delete_err WHERE id = 1")
	if err != nil {
		t.Fatalf("select after failed delete: %v", err)
	}
	if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "a@example.com" {
		t.Fatalf("failed delete should leave original row, got %+v", result.Rows)
	}

	indexed, err := c.ExecuteQuery("SELECT id FROM del_idx_delete_err WHERE email = 'a@example.com'")
	if err != nil {
		t.Fatalf("indexed select after failed delete: %v", err)
	}
	if len(indexed.Rows) != 1 {
		t.Fatalf("index should remain usable after failed delete, got %+v", indexed.Rows)
	}
}
