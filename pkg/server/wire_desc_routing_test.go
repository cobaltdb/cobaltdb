package server

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

// Regression: the wire server's handleQuery routed reads with a hand-rolled
// prefix list (SELECT / WITH / SHOW / EXPLAIN / DESCRIBE) that omitted DESC —
// MySQL's DESCRIBE shorthand, which the engine accepts as a read. `DESC t`
// was therefore routed to ProductionServer.Exec, which rejects reads with an
// internal error, so a valid read statement failed over the wire while the
// same statement spelled DESCRIBE succeeded.
func TestWireHandleQueryRoutesDescAsRead(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(context.Background(), `CREATE TABLE t1 (id INTEGER PRIMARY KEY, d VARCHAR(10))`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(context.Background(), `INSERT INTO t1 VALUES (1, 'x')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, err := New(ps, &Config{RequireAuth: false, AuthEnabled: false})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	client := &ClientConn{ID: 1, Server: srv}

	assertResult := func(t *testing.T, resp interface{}, what string) {
		t.Helper()
		if errMsg, isErr := resp.(*wire.ErrorMessage); isErr {
			t.Fatalf("%s routed to Exec and rejected: code=%d %s", what, errMsg.Code, errMsg.Message)
		}
		result, ok := resp.(*wire.ResultMessage)
		if !ok {
			t.Fatalf("%s returned unexpected response type %T", what, resp)
		}
		if len(result.Columns) == 0 {
			t.Fatalf("%s returned an empty result set", what)
		}
	}

	// The defect: DESC must be routed as a read exactly like DESCRIBE.
	assertResult(t, client.handleQuery(context.Background(), wire.NewQueryMessage("DESC t1")), "DESC t1")
	assertResult(t, client.handleQuery(context.Background(), wire.NewQueryMessage("DESCRIBE t1")), "DESCRIBE t1")
	assertResult(t, client.handleQuery(context.Background(), wire.NewQueryMessage("SELECT id FROM t1")), "SELECT id FROM t1")

	// Non-read control: INSERT must still route to Exec and return OK.
	resp := client.handleQuery(context.Background(), wire.NewQueryMessage(`INSERT INTO t1 VALUES (2, 'y')`))
	if _, isOK := resp.(*wire.OKMessage); !isOK {
		t.Fatalf("INSERT did not return an OK message: %T", resp)
	}
}
