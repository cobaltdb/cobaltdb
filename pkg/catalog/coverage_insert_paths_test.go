package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestInsertLockedPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Auto-increment with missing value
	c.ExecuteQuery("CREATE TABLE auto_t (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)")
	_, err := c.ExecuteQuery("INSERT INTO auto_t (name) VALUES ('alice')")
	if err != nil {
		t.Logf("Auto-increment insert: %v", err)
	}
	// Insert with one fewer value (auto-increment fills in)
	_, err = c.ExecuteQuery("INSERT INTO auto_t VALUES ('bob')")
	if err != nil {
		t.Logf("Auto-increment default: %v", err)
	}

	// PK ConflictReplace
	c.ExecuteQuery("CREATE TABLE pk_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO pk_t (id, name) VALUES (1, 'alice')")
	_, err = c.ExecuteQuery("INSERT OR REPLACE INTO pk_t (id, name) VALUES (1, 'bob')")
	if err != nil {
		t.Logf("PK ConflictReplace: %v", err)
	}

	// PK ConflictIgnore
	_, err = c.ExecuteQuery("INSERT OR IGNORE INTO pk_t (id, name) VALUES (1, 'charlie')")
	if err != nil {
		t.Logf("PK ConflictIgnore: %v", err)
	}

	// Unique index ConflictReplace
	c.ExecuteQuery("CREATE TABLE uniq_idx_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE UNIQUE INDEX uniq_idx_name ON uniq_idx_t(name)")
	c.ExecuteQuery("INSERT INTO uniq_idx_t (id, name) VALUES (1, 'alice')")
	_, err = c.ExecuteQuery("INSERT OR REPLACE INTO uniq_idx_t (id, name) VALUES (2, 'alice')")
	if err != nil {
		t.Logf("Unique index ConflictReplace: %v", err)
	}

	// Unique index ConflictIgnore
	_, err = c.ExecuteQuery("INSERT OR IGNORE INTO uniq_idx_t (id, name) VALUES (3, 'alice')")
	if err != nil {
		t.Logf("Unique index ConflictIgnore: %v", err)
	}

	// INSERT...SELECT with ZEROBLOB (returns []byte, triggers default case in type switch)
	c.ExecuteQuery("CREATE TABLE blob_src (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO blob_src (id) VALUES (1)")
	c.ExecuteQuery("CREATE TABLE blob_dst (id INTEGER PRIMARY KEY, data BLOB)")
	_, err = c.ExecuteQuery("INSERT INTO blob_dst (id, data) SELECT id, ZEROBLOB(10) FROM blob_src")
	if err != nil {
		t.Logf("INSERT...SELECT ZEROBLOB: %v", err)
	}

	// INSERT with AFTER trigger
	c.ExecuteQuery("CREATE TABLE trig_t (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE trig_audit (id INTEGER PRIMARY KEY, old_name TEXT)")
	c.ExecuteQuery("INSERT INTO trig_audit (id, old_name) VALUES (1, '')")
	// Create an AFTER INSERT trigger directly
	c.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "trig_after_insert",
		Table: "trig_t",
		Time:  "AFTER",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "trig_audit",
				Columns: []string{"id", "old_name"},
				Values: [][]query.Expression{
					{&query.ColumnRef{Table: "NEW", Column: "id"},
						&query.ColumnRef{Table: "NEW", Column: "name"},
					},
				},
			},
		},
	})
	_, err = c.ExecuteQuery("INSERT INTO trig_t (id, name) VALUES (1, 'alice')")
	if err != nil {
		t.Logf("INSERT with AFTER trigger: %v", err)
	}

	_ = ctx
}
