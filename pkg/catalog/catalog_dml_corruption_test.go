package catalog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestUpdateReturnsCorruptRowError(t *testing.T) {
	ctx := context.Background()
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_corrupt_row (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_corrupt_row (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["upd_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	affected, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_corrupt_row",
		Set:   []*query.SetClause{{Column: "name", Value: strReal("bob")}},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "upd_corrupt_row") {
		t.Fatalf("expected corrupt row update error, affected=%d err=%v", affected, err)
	}
	if affected != 0 {
		t.Fatalf("expected no updates after corrupt row error, got %d", affected)
	}
}

func TestDeleteReturnsCorruptRowError(t *testing.T) {
	ctx := context.Background()
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE del_corrupt_row (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO del_corrupt_row (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["del_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	affected, _, err := c.Delete(ctx, &query.DeleteStmt{Table: "del_corrupt_row"}, nil)
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "del_corrupt_row") {
		t.Fatalf("expected corrupt row delete error, affected=%d err=%v", affected, err)
	}
	if affected != 0 {
		t.Fatalf("expected no deletes after corrupt row error, got %d", affected)
	}
}
