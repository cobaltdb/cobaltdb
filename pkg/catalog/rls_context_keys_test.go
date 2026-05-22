package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// TestRLSContext_TypedKeysHonored guards against regressing the RLS context-key
// bypass: callers using the typed security.RLS*Key constants (the documented
// public API) must be visible to catalog-level row filtering, not just to
// SQL-level evaluation. See review finding CRIT-1.
func TestRLSContext_TypedKeysHonored(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, security.RLSUserKey, "alice")
	ctx = context.WithValue(ctx, security.RLSRolesKey, []string{"admin", "auditor"})

	user, roles := rlsContext(ctx)
	if user != "alice" {
		t.Fatalf("typed RLSUserKey not read: got %q want %q", user, "alice")
	}
	if len(roles) != 2 || roles[0] != "admin" || roles[1] != "auditor" {
		t.Fatalf("typed RLSRolesKey not read: got %v", roles)
	}
}

// TestRLSContext_LegacyKeysHonored covers the backward-compat path so existing
// tests and embedded callers using the legacy string keys keep working.
func TestRLSContext_LegacyKeysHonored(t *testing.T) {
	//nolint:staticcheck // intentionally using string keys to verify legacy support
	ctx := context.WithValue(context.Background(), "cobaltdb_user", "bob")
	//nolint:staticcheck
	ctx = context.WithValue(ctx, "cobaltdb_roles", []string{"user"})

	user, roles := rlsContext(ctx)
	if user != "bob" {
		t.Fatalf("legacy string key not read: got %q", user)
	}
	if len(roles) != 1 || roles[0] != "user" {
		t.Fatalf("legacy roles key not read: got %v", roles)
	}
}

// TestRLSContext_TypedKeysWin verifies precedence when both are set.
func TestRLSContext_TypedKeysWin(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, security.RLSUserKey, "typed")
	//nolint:staticcheck
	ctx = context.WithValue(ctx, "cobaltdb_user", "legacy")

	user, _ := rlsContext(ctx)
	if user != "typed" {
		t.Fatalf("typed key should take precedence over legacy: got %q", user)
	}
}

// TestRLSContext_NilSafe ensures a nil context doesn't panic.
func TestRLSContext_NilSafe(t *testing.T) {
	user, roles := rlsContext(nil) //nolint:staticcheck // exercising nil ctx defense
	if user != "" || roles != nil {
		t.Fatalf("nil ctx should yield empty user/roles, got (%q, %v)", user, roles)
	}
}
