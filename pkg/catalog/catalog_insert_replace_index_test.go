package catalog

import (
	"errors"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestInsertReplaceUniqueConflictRemovesNonUniqueIndexEntry(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE replace_idx (id INTEGER PRIMARY KEY, email TEXT, category TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX idx_replace_email ON replace_idx(email)"); err != nil {
		t.Fatalf("create unique index: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_replace_category ON replace_idx(category)"); err != nil {
		t.Fatalf("create category index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO replace_idx (id, email, category) VALUES (1, 'a@example.com', 'old')"); err != nil {
		t.Fatalf("insert original row: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT OR REPLACE INTO replace_idx (id, email, category) VALUES (2, 'a@example.com', 'new')"); err != nil {
		t.Fatalf("insert replace: %v", err)
	}

	categoryIndex := c.indexTrees["idx_replace_category"]
	oldIndexKey := []byte(typeTaggedKey("old") + "\x00" + formatKey(1))
	if _, err := categoryIndex.Get(oldIndexKey); err == nil {
		t.Fatal("old non-unique index entry remained after REPLACE")
	}
	newIndexKey := []byte(typeTaggedKey("new") + "\x00" + formatKey(2))
	if pk, err := categoryIndex.Get(newIndexKey); err != nil || string(pk) != formatKey(2) {
		t.Fatalf("new non-unique index entry missing after REPLACE: pk=%q err=%v", string(pk), err)
	}

	result, err := c.ExecuteQuery("SELECT id, email, category FROM replace_idx")
	if err != nil {
		t.Fatalf("select after replace: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one row after REPLACE, got %+v", result.Rows)
	}
}

func TestResolvePKConflictSurfacesIndexDeleteFailure(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE replace_delete_err (id INTEGER PRIMARY KEY, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_replace_delete_email ON replace_delete_err(email)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO replace_delete_err (id, email) VALUES (1, 'a@example.com')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	deleteErr := errors.New("index delete failed")
	c.indexTrees["idx_replace_delete_email"] = &deleteFailTree{
		TreeStore: c.indexTrees["idx_replace_delete_email"],
		err:       deleteErr,
	}

	skip, err := c.resolvePKConflict(
		c.tableTrees["replace_delete_err"],
		c.tables["replace_delete_err"],
		&query.InsertStmt{Table: "replace_delete_err", ConflictAction: query.ConflictReplace},
		formatKey(1),
	)
	if err == nil || !strings.Contains(err.Error(), "index delete failed") {
		t.Fatalf("expected index delete error, got skip=%v err=%v", skip, err)
	}
	if skip {
		t.Fatal("REPLACE conflict should not be skipped on delete failure")
	}
}
