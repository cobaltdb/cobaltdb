package security

import (
	"context"
	"testing"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("RLS manager is nil")
	}
	if mgr.policies == nil {
		t.Fatal("policies map not initialized")
	}
	if mgr.tablePolicies == nil {
		t.Fatal("tablePolicies map not initialized")
	}
	if mgr.enabledTables == nil {
		t.Fatal("enabledTables map not initialized")
	}
}

func TestCreatePolicy(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "user_id = current_user",
	}

	err := mgr.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Verify policy was created
	policies := mgr.GetTablePolicies("users")
	if len(policies) != 1 {
		t.Fatalf("Expected 1 policy, got %d", len(policies))
	}

	if policies[0].Name != "test_policy" {
		t.Errorf("Expected policy name 'test_policy', got '%s'", policies[0].Name)
	}

	// RLS should be auto-enabled
	if !mgr.IsEnabled("users") {
		t.Error("RLS should be auto-enabled when creating a policy")
	}
}

func TestCreateDuplicatePolicy(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "user_id = current_user",
	}

	err := mgr.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Try to create the same policy again
	err = mgr.CreatePolicy(policy)
	if err == nil {
		t.Error("Expected error when creating duplicate policy")
	}
}

func TestDropPolicy(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "user_id = current_user",
	}

	err := mgr.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Drop the policy
	err = mgr.DropPolicy("users", "test_policy")
	if err != nil {
		t.Fatalf("Failed to drop policy: %v", err)
	}

	// Verify policy was dropped
	policies := mgr.GetTablePolicies("users")
	if len(policies) != 0 {
		t.Fatalf("Expected 0 policies, got %d", len(policies))
	}
}

func TestDropNonExistentPolicy(t *testing.T) {
	mgr := NewManager()

	err := mgr.DropPolicy("users", "non_existent")
	if err == nil {
		t.Error("Expected error when dropping non-existent policy")
	}
}

func TestEnableDisableRLS(t *testing.T) {
	mgr := NewManager()

	// Initially disabled
	if mgr.IsEnabled("users") {
		t.Error("RLS should be disabled by default")
	}

	// Enable RLS
	mgr.EnableTable("users")
	if !mgr.IsEnabled("users") {
		t.Error("RLS should be enabled after EnableTable")
	}

	// Disable RLS
	mgr.DisableTable("users")
	if mgr.IsEnabled("users") {
		t.Error("RLS should be disabled after DisableTable")
	}
}

func TestGetPolicy(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "user_id = current_user",
	}

	err := mgr.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Get the policy
	retrieved, err := mgr.GetPolicy("users", "test_policy")
	if err != nil {
		t.Fatalf("Failed to get policy: %v", err)
	}

	if retrieved.Name != "test_policy" {
		t.Errorf("Expected policy name 'test_policy', got '%s'", retrieved.Name)
	}

	// Try to get non-existent policy
	_, err = mgr.GetPolicy("users", "non_existent")
	if err == nil {
		t.Error("Expected error when getting non-existent policy")
	}
}

func TestCheckAccessWithRLSDisabled(t *testing.T) {
	mgr := NewManager()
	// RLS disabled by default

	row := map[string]interface{}{
		"id":       1,
		"username": "alice",
	}

	ctx := context.Background()
	// Should allow access when RLS is disabled
	canAccess, err := mgr.CheckAccess(ctx, "users", PolicySelect, row, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess failed: %v", err)
	}

	if !canAccess {
		t.Error("Expected CheckAccess to return true when RLS disabled")
	}
}

func TestCheckAccessWithNoPolicies(t *testing.T) {
	mgr := NewManager()

	// Enable RLS but no policies
	mgr.EnableTable("users")

	row := map[string]interface{}{
		"id":       1,
		"username": "alice",
	}

	ctx := context.Background()
	// Should deny access when RLS enabled but no policies
	canAccess, err := mgr.CheckAccess(ctx, "users", PolicySelect, row, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess failed: %v", err)
	}

	if canAccess {
		t.Error("Expected CheckAccess to return false when RLS enabled but no policies")
	}
}

func TestFilterRowsWithRLSDisabled(t *testing.T) {
	mgr := NewManager()
	// RLS disabled by default

	rows := []map[string]interface{}{
		{"id": 1, "username": "alice"},
		{"id": 2, "username": "bob"},
	}

	ctx := context.Background()
	// Filter rows - should return all rows when RLS is disabled
	filtered, err := mgr.FilterRows(ctx, "users", PolicySelect, rows, "alice", nil)
	if err != nil {
		t.Fatalf("FilterRows failed: %v", err)
	}

	if len(filtered) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(filtered))
	}
}

func TestFilterRowsWithNoPolicies(t *testing.T) {
	mgr := NewManager()

	// Enable RLS but no policies
	mgr.EnableTable("users")

	rows := []map[string]interface{}{
		{"id": 1, "username": "alice"},
		{"id": 2, "username": "bob"},
	}

	ctx := context.Background()
	// Filter rows - should return no rows when RLS enabled but no policies
	filtered, err := mgr.FilterRows(ctx, "users", PolicySelect, rows, "alice", nil)
	if err != nil {
		t.Fatalf("FilterRows failed: %v", err)
	}

	if len(filtered) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(filtered))
	}
}

func TestPolicyWithUsers(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "true",
		Users:      []string{"alice", "bob"},
	}

	err := mgr.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	retrieved, _ := mgr.GetPolicy("users", "test_policy")
	if len(retrieved.Users) != 2 {
		t.Errorf("Expected 2 users, got %d", len(retrieved.Users))
	}
}

func TestPolicyWithRoles(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "true",
		Roles:      []string{"admin", "manager"},
	}

	err := mgr.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	retrieved, _ := mgr.GetPolicy("users", "test_policy")
	if len(retrieved.Roles) != 2 {
		t.Errorf("Expected 2 roles, got %d", len(retrieved.Roles))
	}
}

func TestPolicyTypes(t *testing.T) {
	mgr := NewManager()

	tests := []struct {
		name       string
		policyType PolicyType
	}{
		{"SELECT", PolicySelect},
		{"INSERT", PolicyInsert},
		{"UPDATE", PolicyUpdate},
		{"DELETE", PolicyDelete},
		{"ALL", PolicyAll},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &Policy{
				Name:       "policy_" + tt.name,
				TableName:  "test_table",
				Type:       tt.policyType,
				Expression: "true",
			}

			err := mgr.CreatePolicy(policy)
			if err != nil {
				t.Errorf("Failed to create policy: %v", err)
			}

			retrieved, _ := mgr.GetPolicy("test_table", "policy_"+tt.name)
			if retrieved.Type != tt.policyType {
				t.Errorf("Expected policy type %v, got %v", tt.policyType, retrieved.Type)
			}

			// Test String() method
			expectedStr := tt.name
			if retrieved.Type.String() != expectedStr {
				t.Errorf("Expected policy type string '%s', got '%s'", expectedStr, retrieved.Type.String())
			}

			// Clean up
			mgr.DropPolicy("test_table", "policy_"+tt.name)
		})
	}
}

func TestListPolicies(t *testing.T) {
	mgr := NewManager()

	// Create multiple policies
	policy1 := &Policy{
		Name:       "policy1",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "true",
	}
	policy2 := &Policy{
		Name:       "policy2",
		TableName:  "orders",
		Type:       PolicySelect,
		Expression: "true",
	}

	mgr.CreatePolicy(policy1)
	mgr.CreatePolicy(policy2)

	policies := mgr.ListPolicies()
	if len(policies) != 2 {
		t.Errorf("Expected 2 policies, got %d", len(policies))
	}
}

func TestEnableDisablePolicy(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "true",
	}

	mgr.CreatePolicy(policy)

	// Disable the policy
	err := mgr.DisablePolicy("users", "test_policy")
	if err != nil {
		t.Fatalf("Failed to disable policy: %v", err)
	}

	retrieved, _ := mgr.GetPolicy("users", "test_policy")
	if retrieved.Enabled {
		t.Error("Policy should be disabled")
	}

	// Enable the policy
	err = mgr.EnablePolicy("users", "test_policy")
	if err != nil {
		t.Fatalf("Failed to enable policy: %v", err)
	}

	retrieved, _ = mgr.GetPolicy("users", "test_policy")
	if !retrieved.Enabled {
		t.Error("Policy should be enabled")
	}
}

func TestSerializeDeserializePolicies(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "user_id = current_user",
		Users:      []string{"alice"},
		Roles:      []string{"admin"},
		Enabled:    true,
		Metadata:   map[string]interface{}{"created_by": "admin"},
	}

	mgr.CreatePolicy(policy)

	// Serialize
	data, err := mgr.SerializePolicies()
	if err != nil {
		t.Fatalf("Failed to serialize policies: %v", err)
	}
	if len(data) == 0 {
		t.Error("Serialized data should not be empty")
	}

	// Create new manager and deserialize
	mgr2 := NewManager()
	err = mgr2.DeserializePolicies(data)
	if err != nil {
		t.Fatalf("Failed to deserialize policies: %v", err)
	}

	// Verify policy was restored
	retrieved, err := mgr2.GetPolicy("users", "test_policy")
	if err != nil {
		t.Fatalf("Failed to get deserialized policy: %v", err)
	}

	if retrieved.Name != "test_policy" {
		t.Errorf("Expected name 'test_policy', got '%s'", retrieved.Name)
	}

	if retrieved.TableName != "users" {
		t.Errorf("Expected table 'users', got '%s'", retrieved.TableName)
	}

	if len(retrieved.Users) != 1 || retrieved.Users[0] != "alice" {
		t.Error("Users not deserialized correctly")
	}

	if len(retrieved.Roles) != 1 || retrieved.Roles[0] != "admin" {
		t.Error("Roles not deserialized correctly")
	}
}

func TestForceRow(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "test_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "true",
		Metadata:   map[string]interface{}{"force_row_security": true},
	}

	mgr.CreatePolicy(policy)

	if !mgr.ForceRow("users") {
		t.Error("ForceRow should return true when policy has force_row_security")
	}

	// Create policy without force_row_security
	mgr2 := NewManager()
	policy2 := &Policy{
		Name:       "test_policy2",
		TableName:  "orders",
		Type:       PolicySelect,
		Expression: "true",
	}
	mgr2.CreatePolicy(policy2)

	if mgr2.ForceRow("orders") {
		t.Error("ForceRow should return false when policy doesn't have force_row_security")
	}
}

func TestCheckAccessWithCurrentUserExpression(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "user_isolation",
		TableName:  "documents",
		Type:       PolicySelect,
		Expression: "user_id = current_user",
	}

	mgr.CreatePolicy(policy)

	rows := []map[string]interface{}{
		{"id": 1, "user_id": "alice", "content": "Alice's doc"},
		{"id": 2, "user_id": "bob", "content": "Bob's doc"},
		{"id": 3, "user_id": "alice", "content": "Alice's other doc"},
	}

	// Test as alice
	ctx := context.WithValue(context.Background(), RLSUserKey, "alice")
	filtered, err := mgr.FilterRows(ctx, "documents", PolicySelect, rows, "alice", nil)
	if err != nil {
		t.Fatalf("FilterRows failed: %v", err)
	}

	// Should see 2 rows (alice's documents)
	if len(filtered) != 2 {
		t.Errorf("Expected 2 rows for alice, got %d", len(filtered))
	}

	// Test as bob
	ctx = context.WithValue(context.Background(), RLSUserKey, "bob")
	filtered, err = mgr.FilterRows(ctx, "documents", PolicySelect, rows, "bob", nil)
	if err != nil {
		t.Fatalf("FilterRows failed: %v", err)
	}

	// Should see 1 row (bob's document)
	if len(filtered) != 1 {
		t.Errorf("Expected 1 row for bob, got %d", len(filtered))
	}
}

func TestCheckAccessWithRoles(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "admin_access",
		TableName:  "sensitive_data",
		Type:       PolicySelect,
		Expression: "true",
		Roles:      []string{"admin"},
	}

	mgr.CreatePolicy(policy)

	row := map[string]interface{}{"id": 1, "secret": "top secret"}
	ctx := context.Background()

	// Admin should have access
	canAccess, err := mgr.CheckAccess(ctx, "sensitive_data", PolicySelect, row, "alice", []string{"admin"})
	if err != nil {
		t.Fatalf("CheckAccess failed: %v", err)
	}
	if !canAccess {
		t.Error("Admin should have access")
	}

	// Regular user should not have access
	canAccess, err = mgr.CheckAccess(ctx, "sensitive_data", PolicySelect, row, "bob", []string{"user"})
	if err != nil {
		t.Fatalf("CheckAccess failed: %v", err)
	}
	if canAccess {
		t.Error("Regular user should not have access")
	}
}

func TestPolicyKeyNormalization(t *testing.T) {
	mgr := NewManager()

	// Create with uppercase
	policy := &Policy{
		Name:       "MyPolicy",
		TableName:  "MyTable",
		Type:       PolicySelect,
		Expression: "true",
	}

	err := mgr.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Should be able to retrieve with lowercase
	retrieved, err := mgr.GetPolicy("mytable", "mypolicy")
	if err != nil {
		t.Fatalf("Failed to get policy with lowercase: %v", err)
	}

	if retrieved.Name != "mypolicy" {
		t.Errorf("Expected normalized name 'mypolicy', got '%s'", retrieved.Name)
	}

	if retrieved.TableName != "mytable" {
		t.Errorf("Expected normalized table 'mytable', got '%s'", retrieved.TableName)
	}
}
