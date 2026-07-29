package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

// TestAuditWireRLSEnforced verifies that the msgpack wire server injects the
// authenticated user identity into the query context so row-level-security
// policies are enforced. Before the fix, handleQuery passed a bare context and
// RLS failed OPEN — every authenticated user saw every row regardless of any
// CREATE POLICY. This drives the real ClientConn.handleMessage path.
func TestAuditWireRLSEnforced(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024},
		Security:    engine.Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	setup := []string{
		"CREATE TABLE docs (id INTEGER PRIMARY KEY, owner TEXT)",
		"INSERT INTO docs VALUES (1,'alice'),(2,'bob'),(3,'carol')",
		"CREATE POLICY p_docs ON docs FOR SELECT USING (owner = current_user())",
	}
	for _, s := range setup {
		if _, err := db.Exec(context.Background(), s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}

	ps := NewProductionServer(db, DefaultProductionConfig())
	srv, _ := New(ps, &Config{AuthEnabled: false, RequireAuth: false})

	runAs := func(user string) [][]interface{} {
		client := &ClientConn{ID: 1, Server: srv, authed: true, username: user}
		payload, _ := wire.Encode(&wire.QueryMessage{SQL: "SELECT id, owner FROM docs"})
		resp := client.handleMessage(wire.MsgQuery, payload)
		res, ok := resp.(*wire.ResultMessage)
		if !ok {
			t.Fatalf("user %s: expected ResultMessage, got %T (%v)", user, resp, resp)
		}
		return res.Rows
	}

	// Each user must see ONLY their own row.
	for _, user := range []string{"alice", "bob", "carol"} {
		rows := runAs(user)
		if len(rows) != 1 {
			t.Errorf("user %s saw %d rows, want 1 (RLS bypass): %v", user, len(rows), rows)
			continue
		}
		if got := toStr(rows[0][1]); got != user {
			t.Errorf("user %s saw owner %q, want %q", user, got, user)
		}
	}
}

func toStr(v interface{}) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	case *string:
		if s != nil {
			return *s
		}
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}
