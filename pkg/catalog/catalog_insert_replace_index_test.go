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

func TestInsertReplacePrimaryKeyDeleteFailureRestoresIndexEntries(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE replace_pk_delete_restore (id INTEGER PRIMARY KEY, email TEXT, category TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX idx_replace_pk_delete_email ON replace_pk_delete_restore(email)"); err != nil {
		t.Fatalf("create email index: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_replace_pk_delete_category ON replace_pk_delete_restore(category)"); err != nil {
		t.Fatalf("create category index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO replace_pk_delete_restore (id, email, category) VALUES (1, 'a@example.com', 'old')"); err != nil {
		t.Fatalf("insert original row: %v", err)
	}

	deleteErr := errors.New("row delete failed")
	c.tableTrees["replace_pk_delete_restore"] = &deleteFailTree{
		TreeStore: c.tableTrees["replace_pk_delete_restore"],
		err:       deleteErr,
	}

	_, err := c.ExecuteQuery("INSERT OR REPLACE INTO replace_pk_delete_restore (id, email, category) VALUES (1, 'b@example.com', 'new')")
	if err == nil || !strings.Contains(err.Error(), "row delete failed") {
		t.Fatalf("expected row delete error, got %v", err)
	}

	assertIndexedRows := func(query string, wantRows int) {
		t.Helper()
		result, err := c.ExecuteQuery(query)
		if err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		if len(result.Rows) != wantRows {
			t.Fatalf("query %q returned %d rows, want %d: %+v", query, len(result.Rows), wantRows, result.Rows)
		}
	}

	assertIndexedRows("SELECT id FROM replace_pk_delete_restore WHERE email = 'a@example.com'", 1)
	assertIndexedRows("SELECT id FROM replace_pk_delete_restore WHERE category = 'old'", 1)
	assertIndexedRows("SELECT id FROM replace_pk_delete_restore WHERE email = 'b@example.com'", 0)
	assertIndexedRows("SELECT id FROM replace_pk_delete_restore WHERE category = 'new'", 0)

	emailIndex := c.indexTrees["idx_replace_pk_delete_email"]
	if pk, err := emailIndex.Get([]byte(typeTaggedKey("a@example.com"))); err != nil || string(pk) != formatKey(1) {
		t.Fatalf("old unique index entry was not restored: pk=%q err=%v", string(pk), err)
	}
	if _, err := emailIndex.Get([]byte(typeTaggedKey("b@example.com"))); err == nil {
		t.Fatal("new unique index entry exists after failed REPLACE")
	}

	categoryIndex := c.indexTrees["idx_replace_pk_delete_category"]
	oldCategoryKey := []byte(typeTaggedKey("old") + "\x00" + formatKey(1))
	if pk, err := categoryIndex.Get(oldCategoryKey); err != nil || string(pk) != formatKey(1) {
		t.Fatalf("old non-unique index entry was not restored: pk=%q err=%v", string(pk), err)
	}
	newCategoryKey := []byte(typeTaggedKey("new") + "\x00" + formatKey(1))
	if _, err := categoryIndex.Get(newCategoryKey); err == nil {
		t.Fatal("new non-unique index entry exists after failed REPLACE")
	}
}

func TestInsertReplaceUniqueConflictDeleteFailureRestoresIndexEntries(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE replace_unique_delete_restore (id INTEGER PRIMARY KEY, email TEXT UNIQUE, category TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX idx_replace_unique_delete_email ON replace_unique_delete_restore(email)"); err != nil {
		t.Fatalf("create email index: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_replace_unique_delete_category ON replace_unique_delete_restore(category)"); err != nil {
		t.Fatalf("create category index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO replace_unique_delete_restore (id, email, category) VALUES (1, 'a@example.com', 'old')"); err != nil {
		t.Fatalf("insert original row: %v", err)
	}

	deleteErr := errors.New("row delete failed")
	c.tableTrees["replace_unique_delete_restore"] = &deleteFailTree{
		TreeStore: c.tableTrees["replace_unique_delete_restore"],
		err:       deleteErr,
	}

	_, err := c.ExecuteQuery("INSERT OR REPLACE INTO replace_unique_delete_restore (id, email, category) VALUES (2, 'a@example.com', 'new')")
	if err == nil || !strings.Contains(err.Error(), "row delete failed") {
		t.Fatalf("expected row delete error, got %v", err)
	}

	result, err := c.ExecuteQuery("SELECT id FROM replace_unique_delete_restore WHERE email = 'a@example.com'")
	if err != nil {
		t.Fatalf("select old email: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(1) {
		t.Fatalf("old row not visible after failed REPLACE: %+v", result.Rows)
	}
	result, err = c.ExecuteQuery("SELECT id FROM replace_unique_delete_restore WHERE category = 'new'")
	if err != nil {
		t.Fatalf("select new category: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("new row visible after failed REPLACE: %+v", result.Rows)
	}

	emailIndex := c.indexTrees["idx_replace_unique_delete_email"]
	if pk, err := emailIndex.Get([]byte(typeTaggedKey("a@example.com"))); err != nil || string(pk) != formatKey(1) {
		t.Fatalf("old unique index entry was not restored: pk=%q err=%v", string(pk), err)
	}

	categoryIndex := c.indexTrees["idx_replace_unique_delete_category"]
	oldCategoryKey := []byte(typeTaggedKey("old") + "\x00" + formatKey(1))
	if pk, err := categoryIndex.Get(oldCategoryKey); err != nil || string(pk) != formatKey(1) {
		t.Fatalf("old non-unique index entry was not restored: pk=%q err=%v", string(pk), err)
	}
	newCategoryKey := []byte(typeTaggedKey("new") + "\x00" + formatKey(2))
	if _, err := categoryIndex.Get(newCategoryKey); err == nil {
		t.Fatal("new non-unique index entry exists after failed REPLACE")
	}
}
