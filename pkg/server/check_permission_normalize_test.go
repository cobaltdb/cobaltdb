package server

import (
	"testing"
)

// TestCheckPermissionNormalizesStatementFamilies pins the keyword→action
// normalization in checkPermission: WITH (CTE), EXPLAIN, SHOW, DESC/DESCRIBE
// are reads (SELECT permission class) and REPLACE/UPSERT are writes (INSERT
// class), mirroring the engine's statement-class authority (circuitBreakerKey
// and the query router). Pre-fix a non-admin with the SELECT permission
// granted was denied `WITH ... SELECT ...` — a core read construct — with
// wire error 8 "permission denied", because the first-keyword switch treated
// WITH as unknown. Deny-by-default for genuinely unknown keywords is
// intentional and preserved (TRUNCATE has no unambiguous permission action).
func TestCheckPermissionNormalizesStatementFamilies(t *testing.T) {
	srv, err := New(nil, &Config{
		AuthEnabled:      true,
		RequireAuth:      true,
		DefaultAdminUser: "admin",
		DefaultAdminPass: "admin-pass",
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	authn := srv.GetAuthenticator()
	if err := authn.CreateUser("alice", "alice-pass", false); err != nil {
		t.Fatalf("create alice: %v", err)
	}
	if err := authn.GrantPermission("alice", "", "", []string{"SELECT", "INSERT"}); err != nil {
		t.Fatalf("grant alice: %v", err)
	}

	alice := &ClientConn{Server: srv, authed: true, username: "alice"}

	// Read family normalized to the SELECT permission class.
	for _, sql := range []string{
		"WITH c AS (SELECT 1) SELECT * FROM c",
		"EXPLAIN SELECT * FROM t",
		"SHOW TABLES",
		"DESC t",
		"DESCRIBE t",
		"SELECT * FROM t",
	} {
		if !alice.checkPermission(sql) {
			t.Errorf("checkPermission(%q) = false, want true (read family)", sql)
		}
	}

	// Write aliases normalized to the INSERT permission class.
	for _, sql := range []string{
		"REPLACE INTO t VALUES (1)",
		"UPSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (1)",
	} {
		if !alice.checkPermission(sql) {
			t.Errorf("checkPermission(%q) = false, want true (write alias)", sql)
		}
	}

	// Deny-by-default preserved for keywords with no unambiguous action.
	for _, sql := range []string{
		"TRUNCATE TABLE t",
		"GRANT SELECT ON t TO u",
	} {
		if alice.checkPermission(sql) {
			t.Errorf("checkPermission(%q) = true, want false (deny-by-default)", sql)
		}
	}

	// Unauthenticated connections are still denied regardless of SQL.
	anon := &ClientConn{Server: srv}
	if anon.checkPermission("SELECT 1") {
		t.Error("unauthenticated checkPermission = true, want false")
	}
}
