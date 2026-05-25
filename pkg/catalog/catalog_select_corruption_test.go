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

func TestJoinSelectReturnsCorruptRightRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE join_clean_main (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create main table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE join_corrupt_right (id INTEGER PRIMARY KEY, main_id INTEGER)"); err != nil {
		t.Fatalf("create right table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO join_clean_main (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert main: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO join_corrupt_right (id, main_id) VALUES (1, 1)"); err != nil {
		t.Fatalf("insert right: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["join_corrupt_right"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt right row: %v", err)
	}

	_, err := c.ExecuteQuery("SELECT join_clean_main.id FROM join_clean_main JOIN join_corrupt_right ON join_clean_main.id = join_corrupt_right.main_id")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "join_corrupt_right") {
		t.Fatalf("expected corrupt right row join error, got %v", err)
	}
}

func TestGroupBySelectReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE group_corrupt_row (id INTEGER PRIMARY KEY, region TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO group_corrupt_row (id, region) VALUES (1, 'north')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["group_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	_, err := c.ExecuteQuery("SELECT region, COUNT(*) FROM group_corrupt_row GROUP BY region")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "group_corrupt_row") {
		t.Fatalf("expected corrupt group by row error, got %v", err)
	}
}

func TestJoinGroupBySelectReturnsCorruptRightRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE join_group_clean_main (id INTEGER PRIMARY KEY, region TEXT)"); err != nil {
		t.Fatalf("create main table: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE join_group_corrupt_right (id INTEGER PRIMARY KEY, main_id INTEGER, amount INTEGER)"); err != nil {
		t.Fatalf("create right table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO join_group_clean_main (id, region) VALUES (1, 'north')"); err != nil {
		t.Fatalf("insert main: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO join_group_corrupt_right (id, main_id, amount) VALUES (1, 1, 10)"); err != nil {
		t.Fatalf("insert right: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["join_group_corrupt_right"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt right row: %v", err)
	}

	_, err := c.ExecuteQuery("SELECT join_group_clean_main.region, COUNT(*) FROM join_group_clean_main JOIN join_group_corrupt_right ON join_group_clean_main.id = join_group_corrupt_right.main_id GROUP BY join_group_clean_main.region")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "join_group_corrupt_right") {
		t.Fatalf("expected corrupt right row join group by error, got %v", err)
	}
}

func TestCountFastPathReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE count_corrupt_row (id INTEGER PRIMARY KEY, value INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO count_corrupt_row (id, value) VALUES (1, 10)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["count_corrupt_row"].Put([]byte(pkKey), []byte("{not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	_, err := c.ExecuteQuery("SELECT COUNT(*) FROM count_corrupt_row")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "count_corrupt_row") {
		t.Fatalf("expected corrupt count fast path error, got %v", err)
	}
}

func TestAggregateFastPathReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE agg_corrupt_row (id INTEGER PRIMARY KEY, amount INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO agg_corrupt_row (id, amount) VALUES (1, 10)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["agg_corrupt_row"].Put([]byte(pkKey), []byte("{not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	_, err := c.ExecuteQuery("SELECT SUM(amount) FROM agg_corrupt_row")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "agg_corrupt_row") {
		t.Fatalf("expected corrupt aggregate fast path error, got %v", err)
	}
}
