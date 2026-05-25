package catalog

import (
	"fmt"
	"strings"
	"testing"
)

func TestSelectReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE sel_corrupt_row (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO sel_corrupt_row (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["sel_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	_, err := c.ExecuteQuery("SELECT id, name FROM sel_corrupt_row")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "sel_corrupt_row") {
		t.Fatalf("expected corrupt row select error, got %v", err)
	}
}

func TestJoinSelectReturnsCorruptMainRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE join_corrupt_main (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create main table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE join_corrupt_side (id INTEGER PRIMARY KEY, main_id INTEGER)"); err != nil {
		t.Fatalf("create side table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO join_corrupt_main (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert main: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO join_corrupt_side (id, main_id) VALUES (1, 1)"); err != nil {
		t.Fatalf("insert side: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["join_corrupt_main"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt main row: %v", err)
	}

	_, err := c.ExecuteQuery("SELECT join_corrupt_main.id FROM join_corrupt_main JOIN join_corrupt_side ON join_corrupt_main.id = join_corrupt_side.main_id")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "join_corrupt_main") {
		t.Fatalf("expected corrupt main row join error, got %v", err)
	}
}
