package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
)

type putFailOnceTree struct {
	btree.TreeStore
	err    error
	failed bool
}

func (t *putFailOnceTree) Put(key, value []byte) error {
	if !t.failed {
		t.failed = true
		return t.err
	}
	return t.TreeStore.Put(key, value)
}

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

func TestDeleteRowSoftDeleteFailureRestoresIndexEntries(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE del_put_restore (id INTEGER PRIMARY KEY, email TEXT, category TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX idx_del_put_email ON del_put_restore(email)"); err != nil {
		t.Fatalf("create email index: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_del_put_category ON del_put_restore(category)"); err != nil {
		t.Fatalf("create category index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO del_put_restore (id, email, category) VALUES (1, 'a@example.com', 'old')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	c.tableTrees["del_put_restore"] = &putFailTree{
		TreeStore: c.tableTrees["del_put_restore"],
		err:       errors.New("soft delete write failed"),
	}

	err := c.DeleteRow(context.Background(), "del_put_restore", int64(1))
	if err == nil || !strings.Contains(err.Error(), "soft delete write failed") {
		t.Fatalf("expected soft delete write error, got %v", err)
	}

	result, err := c.ExecuteQuery("SELECT email FROM del_put_restore WHERE id = 1")
	if err != nil {
		t.Fatalf("select after failed delete: %v", err)
	}
	if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "a@example.com" {
		t.Fatalf("failed delete should leave original row, got %+v", result.Rows)
	}

	indexedEmail, err := c.ExecuteQuery("SELECT id FROM del_put_restore WHERE email = 'a@example.com'")
	if err != nil {
		t.Fatalf("indexed email select after failed delete: %v", err)
	}
	if len(indexedEmail.Rows) != 1 {
		t.Fatalf("email index should remain usable after failed delete, got %+v", indexedEmail.Rows)
	}
	indexedCategory, err := c.ExecuteQuery("SELECT id FROM del_put_restore WHERE category = 'old'")
	if err != nil {
		t.Fatalf("indexed category select after failed delete: %v", err)
	}
	if len(indexedCategory.Rows) != 1 {
		t.Fatalf("category index should remain usable after failed delete, got %+v", indexedCategory.Rows)
	}

	emailIndex := c.indexTrees["idx_del_put_email"]
	if pk, err := emailIndex.Get([]byte(typeTaggedKey("a@example.com"))); err != nil || string(pk) != formatKey(1) {
		t.Fatalf("unique index entry was not restored: pk=%q err=%v", string(pk), err)
	}
	categoryIndex := c.indexTrees["idx_del_put_category"]
	categoryKey := []byte(typeTaggedKey("old") + "\x00" + formatKey(1))
	if pk, err := categoryIndex.Get(categoryKey); err != nil || string(pk) != formatKey(1) {
		t.Fatalf("non-unique index entry was not restored: pk=%q err=%v", string(pk), err)
	}
}

func TestUpdatePrimaryKeyDeleteFailureKeepsOldRow(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_pk_delete_err (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_pk_delete_err (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	c.tableTrees["upd_pk_delete_err"] = &deleteFailTree{
		TreeStore: c.tableTrees["upd_pk_delete_err"],
		err:       errors.New("delete failed"),
	}

	_, err := c.ExecuteQuery("UPDATE upd_pk_delete_err SET id = 2 WHERE id = 1")
	if err == nil || !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("expected old key delete failure, got %v", err)
	}

	oldRow, err := c.ExecuteQuery("SELECT name FROM upd_pk_delete_err WHERE id = 1")
	if err != nil {
		t.Fatalf("select old row: %v", err)
	}
	if len(oldRow.Rows) != 1 || fmt.Sprint(oldRow.Rows[0][0]) != "alice" {
		t.Fatalf("old row should remain after failed PK update, got %+v", oldRow.Rows)
	}

	newRow, err := c.ExecuteQuery("SELECT name FROM upd_pk_delete_err WHERE id = 2")
	if err != nil {
		t.Fatalf("select new row: %v", err)
	}
	if len(newRow.Rows) != 0 {
		t.Fatalf("new row should not be written after failed PK update, got %+v", newRow.Rows)
	}
}

func TestUpdateFromIndexPutFailureRollsBackRowAndIndex(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_join_put_restore (id INTEGER PRIMARY KEY, category TEXT)"); err != nil {
		t.Fatalf("create target: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE upd_join_src (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_upd_join_category ON upd_join_put_restore(category)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_join_put_restore (id, category) VALUES (1, 'old')"); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_join_src (id) VALUES (1)"); err != nil {
		t.Fatalf("insert source: %v", err)
	}

	c.indexTrees["idx_upd_join_category"] = &putFailOnceTree{
		TreeStore: c.indexTrees["idx_upd_join_category"],
		err:       errors.New("index put failed"),
	}

	_, err := c.ExecuteQuery("UPDATE upd_join_put_restore SET category = 'new' FROM upd_join_src WHERE upd_join_put_restore.id = upd_join_src.id")
	if err == nil || !strings.Contains(err.Error(), "index put failed") {
		t.Fatalf("expected index put error, got %v", err)
	}

	result, err := c.ExecuteQuery("SELECT category FROM upd_join_put_restore WHERE id = 1")
	if err != nil {
		t.Fatalf("select after failed update: %v", err)
	}
	if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "old" {
		t.Fatalf("failed UPDATE FROM should leave original row, got %+v", result.Rows)
	}

	oldIndexed, err := c.ExecuteQuery("SELECT id FROM upd_join_put_restore WHERE category = 'old'")
	if err != nil {
		t.Fatalf("indexed old select after failed update: %v", err)
	}
	if len(oldIndexed.Rows) != 1 {
		t.Fatalf("old index entry should remain usable after failed update, got %+v", oldIndexed.Rows)
	}
	newIndexed, err := c.ExecuteQuery("SELECT id FROM upd_join_put_restore WHERE category = 'new'")
	if err != nil {
		t.Fatalf("indexed new select after failed update: %v", err)
	}
	if len(newIndexed.Rows) != 0 {
		t.Fatalf("new index entry should not remain after failed update, got %+v", newIndexed.Rows)
	}

	categoryIndex := c.indexTrees["idx_upd_join_category"]
	oldKey := []byte(typeTaggedKey("old") + "\x00" + formatKey(1))
	if pk, err := categoryIndex.Get(oldKey); err != nil || string(pk) != formatKey(1) {
		t.Fatalf("old non-unique index entry was not restored: pk=%q err=%v", string(pk), err)
	}
	newKey := []byte(typeTaggedKey("new") + "\x00" + formatKey(1))
	if _, err := categoryIndex.Get(newKey); err == nil {
		t.Fatal("new non-unique index entry exists after failed UPDATE FROM")
	}
}

func TestDeleteUsingSoftDeleteFailureRollsBackRowAndIndex(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE del_using_put_restore (id INTEGER PRIMARY KEY, category TEXT)"); err != nil {
		t.Fatalf("create target: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE del_using_src (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_del_using_category ON del_using_put_restore(category)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO del_using_put_restore (id, category) VALUES (1, 'old')"); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO del_using_src (id) VALUES (1)"); err != nil {
		t.Fatalf("insert source: %v", err)
	}

	c.tableTrees["del_using_put_restore"] = &putFailTree{
		TreeStore: c.tableTrees["del_using_put_restore"],
		err:       errors.New("soft delete write failed"),
	}

	_, err := c.ExecuteQuery("DELETE FROM del_using_put_restore USING del_using_src WHERE del_using_put_restore.id = del_using_src.id")
	if err == nil || !strings.Contains(err.Error(), "soft delete write failed") {
		t.Fatalf("expected soft delete write error, got %v", err)
	}

	result, err := c.ExecuteQuery("SELECT category FROM del_using_put_restore WHERE id = 1")
	if err != nil {
		t.Fatalf("select after failed delete: %v", err)
	}
	if len(result.Rows) != 1 || fmt.Sprint(result.Rows[0][0]) != "old" {
		t.Fatalf("failed DELETE USING should leave original row, got %+v", result.Rows)
	}

	indexed, err := c.ExecuteQuery("SELECT id FROM del_using_put_restore WHERE category = 'old'")
	if err != nil {
		t.Fatalf("indexed select after failed delete: %v", err)
	}
	if len(indexed.Rows) != 1 {
		t.Fatalf("old index entry should remain usable after failed delete, got %+v", indexed.Rows)
	}

	categoryIndex := c.indexTrees["idx_del_using_category"]
	oldKey := []byte(typeTaggedKey("old") + "\x00" + formatKey(1))
	if pk, err := categoryIndex.Get(oldKey); err != nil || string(pk) != formatKey(1) {
		t.Fatalf("old non-unique index entry was not restored: pk=%q err=%v", string(pk), err)
	}
}
