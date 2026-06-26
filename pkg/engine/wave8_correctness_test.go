package engine

import (
	"context"
	"strings"
	"testing"
)

// TestQueryCacheKeyDistinguishesWhere verifies the query result cache does not
// return one query's rows for a different query (the key must include WHERE).
func TestQueryCacheKeyDistinguishesWhere(t *testing.T) {
	db, _ := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true, CacheSize: 256},
		QueryCache:  QueryCacheConfig{EnableQueryCache: true, QueryCacheSize: 100},
	})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO users VALUES (1, 'alice')")
	mustExec(t, db, "INSERT INTO users VALUES (2, 'bob')")

	q := func(sql string) string {
		var name string
		if err := db.QueryRow(ctx, sql).Scan(new(int64), &name); err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		return name
	}
	if got := q("SELECT id, name FROM users WHERE id = 1"); got != "alice" {
		t.Fatalf("id=1 -> %q, want alice", got)
	}
	if got := q("SELECT id, name FROM users WHERE id = 2"); got != "bob" {
		t.Fatalf("id=2 -> %q, want bob (cache key collision returned the wrong row)", got)
	}
}

// TestQueryCacheInvalidatedByDDL verifies a cached SELECT does not survive a
// DROP TABLE (serving rows from a table that no longer exists).
func TestQueryCacheInvalidatedByDDL(t *testing.T) {
	db, _ := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true, CacheSize: 256},
		QueryCache:  QueryCacheConfig{EnableQueryCache: true, QueryCacheSize: 100},
	})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO users VALUES (1, 'alice')")

	rows, err := db.Query(ctx, "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("first select: %v", err)
	}
	rows.Close()
	mustExec(t, db, "DROP TABLE users")

	if _, err := db.Query(ctx, "SELECT id, name FROM users"); err == nil {
		t.Fatal("SELECT after DROP TABLE succeeded (served stale cached rows)")
	}
}

// TestTriggerRecursionGuard verifies a self-referential trigger errors instead
// of crashing the process with a stack overflow.
func TestTriggerRecursionGuard(t *testing.T) {
	db, _ := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id INTEGER, v INTEGER)")
	mustExec(t, db, "CREATE TRIGGER loop AFTER INSERT ON t FOR EACH ROW INSERT INTO t VALUES (NEW.id + 1, 0)")

	_, err := db.Exec(ctx, "INSERT INTO t VALUES (1, 0)")
	if err == nil {
		t.Fatal("expected recursion-depth error from self-referential trigger")
	}
	if !strings.Contains(err.Error(), "recursion") {
		t.Fatalf("expected recursion error, got: %v", err)
	}
}
