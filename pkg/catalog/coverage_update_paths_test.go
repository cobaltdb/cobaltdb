package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestUpdateLockedPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Update PK
	c.ExecuteQuery("CREATE TABLE pk_upd (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO pk_upd (id, name) VALUES (1, 'alice')")
	c.ExecuteQuery("INSERT INTO pk_upd (id, name) VALUES (2, 'bob')")
	_, _, err := c.Update(ctx, mustParseUpdate("UPDATE pk_upd SET id = 3 WHERE id = 1"), nil)
	if err != nil {
		t.Logf("Update PK: %v", err)
	}

	// Update PK to duplicate value
	_, _, err = c.Update(ctx, mustParseUpdate("UPDATE pk_upd SET id = 2 WHERE id = 3"), nil)
	if err == nil {
		t.Error("Expected duplicate PK error")
	}

	// Update with unique index conflict
	c.ExecuteQuery("CREATE TABLE uniq_upd (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	c.ExecuteQuery("INSERT INTO uniq_upd (id, code) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO uniq_upd (id, code) VALUES (2, 'B')")
	_, _, err = c.Update(ctx, mustParseUpdate("UPDATE uniq_upd SET code = 'B' WHERE id = 1"), nil)
	if err == nil {
		t.Error("Expected unique constraint error")
	}

	// Update with AFTER trigger
	c.ExecuteQuery("CREATE TABLE upd_trig (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE upd_audit (id INTEGER PRIMARY KEY, old_name TEXT)")
	c.ExecuteQuery("INSERT INTO upd_trig (id, name) VALUES (1, 'alice')")
	c.ExecuteQuery("INSERT INTO upd_audit (id, old_name) VALUES (1, '')")
	c.CreateTrigger(mustParseTrigger("CREATE TRIGGER trg_upd AFTER UPDATE ON upd_trig BEGIN UPDATE upd_audit SET old_name = OLD.name WHERE id = OLD.id; END"))
	_, _, err = c.Update(ctx, mustParseUpdate("UPDATE upd_trig SET name = 'ALICE' WHERE id = 1"), nil)
	if err != nil {
		t.Logf("Update with AFTER trigger: %v", err)
	}

	// Update RETURNING
	_, _, err = c.Update(ctx, mustParseUpdate("UPDATE pk_upd SET name = 'charlie' WHERE id = 2 RETURNING id, name"), nil)
	if err != nil {
		t.Logf("Update RETURNING: %v", err)
	}

	_ = ctx
}

func mustParseTrigger(sql string) *query.CreateTriggerStmt {
	parsed, err := query.Parse(sql)
	if err != nil {
		panic(err)
	}
	if trig, ok := parsed.(*query.CreateTriggerStmt); ok {
		return trig
	}
	panic("parsed statement is not a CREATE TRIGGER")
}
