package security

import (
	"context"
	"testing"
)

func TestListEnabledTables(t *testing.T) {
	mgr := NewManager()

	// No tables enabled yet
	tables := mgr.ListEnabledTables()
	if len(tables) != 0 {
		t.Fatalf("expected 0 enabled tables, got %d", len(tables))
	}

	// Create a policy which auto-enables RLS for the table
	policy := &Policy{
		Name:       "policy1",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "user_id = current_user",
	}
	if err := mgr.CreatePolicy(policy); err != nil {
		t.Fatalf("CreatePolicy failed: %v", err)
	}

	tables = mgr.ListEnabledTables()
	if len(tables) != 1 {
		t.Fatalf("expected 1 enabled table, got %d: %v", len(tables), tables)
	}
	if tables[0] != "users" {
		t.Errorf("expected 'users', got '%s'", tables[0])
	}
}

func TestListEnabledTablesMultiple(t *testing.T) {
	mgr := NewManager()

	// Create policies for multiple tables
	for _, table := range []string{"users", "orders", "products"} {
		policy := &Policy{
			Name:       "policy_" + table,
			TableName:  table,
			Type:       PolicySelect,
			Expression: "true",
		}
		if err := mgr.CreatePolicy(policy); err != nil {
			t.Fatalf("CreatePolicy(%s) failed: %v", table, err)
		}
	}

	tables := mgr.ListEnabledTables()
	if len(tables) != 3 {
		t.Fatalf("expected 3 enabled tables, got %d: %v", len(tables), tables)
	}

	// Verify sorted order
	expected := []string{"orders", "products", "users"}
	for i, e := range expected {
		if tables[i] != e {
			t.Errorf("expected tables[%d]=%s, got %s", i, e, tables[i])
		}
	}
}

func TestCheckAccessWithCheckNoRLS(t *testing.T) {
	mgr := NewManager()
	// RLS not enabled for this table — should return true
	allowed, err := mgr.CheckAccessWithCheck(
		context.Background(),
		"users",
		PolicySelect,
		map[string]interface{}{"user_id": "alice"},
		"alice",
		[]string{},
	)
	if err != nil {
		t.Fatalf("CheckAccessWithCheck failed: %v", err)
	}
	if !allowed {
		t.Error("expected access allowed when RLS not enabled")
	}
}

func TestCheckAccessWithCheckNoPolicies(t *testing.T) {
	mgr := NewManager()
	// Enable RLS but create no policies — should deny all
	mgr.EnableTable("users")

	allowed, err := mgr.CheckAccessWithCheck(
		context.Background(),
		"users",
		PolicySelect,
		map[string]interface{}{"user_id": "alice"},
		"alice",
		[]string{},
	)
	if err != nil {
		t.Fatalf("CheckAccessWithCheck failed: %v", err)
	}
	if allowed {
		t.Error("expected access denied when RLS enabled but no policies")
	}
}

func TestCheckAccessWithCheckWithPolicy(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	// Create a policy with a USING expression that matches the row
	policy := &Policy{
		Name:       "user_sel",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "user_id = 'alice'",
	}
	if err := mgr.CreatePolicy(policy); err != nil {
		t.Fatalf("CreatePolicy failed: %v", err)
	}

	// Check with matching user — should be allowed
	allowed, err := mgr.CheckAccessWithCheck(
		ctx,
		"users",
		PolicySelect,
		map[string]interface{}{"user_id": "alice"},
		"alice",
		[]string{},
	)
	if err != nil {
		t.Fatalf("CheckAccessWithCheck failed: %v", err)
	}
	if !allowed {
		t.Error("expected access allowed for matching user")
	}
}
