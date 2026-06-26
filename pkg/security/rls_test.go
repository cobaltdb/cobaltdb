package security

import (
	"context"
	"strings"
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

func TestPolicyStateIsolation(t *testing.T) {
	mgr := NewManager()
	policy := &Policy{
		Name:       "copy_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "true",
		Users:      []string{"alice"},
		Roles:      []string{"reader"},
		Metadata: map[string]interface{}{
			"owner": "security",
			"nested": map[string]interface{}{
				"tier": "gold",
			},
			"bytes": []byte("secret"),
			"labels": map[string]string{
				"env": "prod",
			},
			"history": []map[string]interface{}{
				{"version": "v1"},
			},
		},
	}

	if err := mgr.CreatePolicy(policy); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	policy.Users[0] = "mallory"
	policy.Roles[0] = "admin"
	policy.Metadata["owner"] = "mutated"
	policy.Metadata["nested"].(map[string]interface{})["tier"] = "bronze"
	policy.Metadata["bytes"].([]byte)[0] = 'x'
	policy.Metadata["labels"].(map[string]string)["env"] = "dev"
	policy.Metadata["history"].([]map[string]interface{})[0]["version"] = "v2"

	retrieved, err := mgr.GetPolicy("users", "copy_policy")
	if err != nil {
		t.Fatalf("GetPolicy: %v", err)
	}
	if retrieved.Users[0] != "alice" || retrieved.Roles[0] != "reader" {
		t.Fatalf("CreatePolicy retained caller-owned slices: users=%v roles=%v", retrieved.Users, retrieved.Roles)
	}
	if retrieved.Metadata["owner"] != "security" {
		t.Fatalf("CreatePolicy retained caller-owned metadata: %v", retrieved.Metadata["owner"])
	}
	if retrieved.Metadata["nested"].(map[string]interface{})["tier"] != "gold" {
		t.Fatalf("CreatePolicy retained caller-owned nested metadata: %v", retrieved.Metadata["nested"])
	}
	if string(retrieved.Metadata["bytes"].([]byte)) != "secret" {
		t.Fatalf("CreatePolicy retained caller-owned byte metadata: %q", retrieved.Metadata["bytes"])
	}
	if retrieved.Metadata["labels"].(map[string]string)["env"] != "prod" {
		t.Fatalf("CreatePolicy retained caller-owned string map metadata: %v", retrieved.Metadata["labels"])
	}
	if retrieved.Metadata["history"].([]map[string]interface{})[0]["version"] != "v1" {
		t.Fatalf("CreatePolicy retained caller-owned map slice metadata: %v", retrieved.Metadata["history"])
	}

	retrieved.Users[0] = "eve"
	retrieved.Roles[0] = "writer"
	retrieved.Metadata["owner"] = "external"
	retrieved.Metadata["nested"].(map[string]interface{})["tier"] = "silver"
	retrieved.Metadata["bytes"].([]byte)[0] = 'y'
	retrieved.Metadata["labels"].(map[string]string)["env"] = "stage"
	retrieved.Metadata["history"].([]map[string]interface{})[0]["version"] = "v3"

	retrievedAgain, err := mgr.GetPolicy("users", "copy_policy")
	if err != nil {
		t.Fatalf("GetPolicy second read: %v", err)
	}
	if retrievedAgain.Users[0] != "alice" || retrievedAgain.Roles[0] != "reader" {
		t.Fatalf("GetPolicy returned mutable slices: users=%v roles=%v", retrievedAgain.Users, retrievedAgain.Roles)
	}
	if retrievedAgain.Metadata["owner"] != "security" {
		t.Fatalf("GetPolicy returned mutable metadata: %v", retrievedAgain.Metadata["owner"])
	}
	if retrievedAgain.Metadata["nested"].(map[string]interface{})["tier"] != "gold" {
		t.Fatalf("GetPolicy returned mutable nested metadata: %v", retrievedAgain.Metadata["nested"])
	}
	if string(retrievedAgain.Metadata["bytes"].([]byte)) != "secret" {
		t.Fatalf("GetPolicy returned mutable byte metadata: %q", retrievedAgain.Metadata["bytes"])
	}
	if retrievedAgain.Metadata["labels"].(map[string]string)["env"] != "prod" {
		t.Fatalf("GetPolicy returned mutable string map metadata: %v", retrievedAgain.Metadata["labels"])
	}
	if retrievedAgain.Metadata["history"].([]map[string]interface{})[0]["version"] != "v1" {
		t.Fatalf("GetPolicy returned mutable map slice metadata: %v", retrievedAgain.Metadata["history"])
	}

	tablePolicies := mgr.GetTablePolicies("users")
	if len(tablePolicies) != 1 {
		t.Fatalf("expected 1 table policy, got %d", len(tablePolicies))
	}
	tablePolicies[0].Users[0] = "trent"
	tablePolicyAgain, err := mgr.GetPolicy("users", "copy_policy")
	if err != nil {
		t.Fatalf("GetPolicy after GetTablePolicies mutation: %v", err)
	}
	if tablePolicyAgain.Users[0] != "alice" {
		t.Fatalf("GetTablePolicies returned mutable policy: users=%v", tablePolicyAgain.Users)
	}

	listed := mgr.ListPolicies()
	if len(listed) != 1 {
		t.Fatalf("expected 1 listed policy, got %d", len(listed))
	}
	listed[0].Roles[0] = "operator"
	listedAgain, err := mgr.GetPolicy("users", "copy_policy")
	if err != nil {
		t.Fatalf("GetPolicy after ListPolicies mutation: %v", err)
	}
	if listedAgain.Roles[0] != "reader" {
		t.Fatalf("ListPolicies returned mutable policy: roles=%v", listedAgain.Roles)
	}
}

func TestCreatePolicyInvalidExpressionIsAtomic(t *testing.T) {
	mgr := NewManager()
	err := mgr.CreatePolicy(&Policy{
		Name:       "bad_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "status ~~ active",
	})
	if err == nil {
		t.Fatal("expected invalid expression error")
	}

	if mgr.IsEnabled("users") {
		t.Fatal("CreatePolicy enabled RLS for a rejected policy")
	}
	if _, err := mgr.GetPolicy("users", "bad_policy"); err != ErrPolicyNotFound {
		t.Fatalf("CreatePolicy stored rejected policy, got err=%v", err)
	}
	if policies := mgr.GetTablePolicies("users"); len(policies) != 0 {
		t.Fatalf("CreatePolicy indexed rejected policy: %d policies", len(policies))
	}
}

func TestCreatePolicyResourceLimitsAreAtomic(t *testing.T) {
	tests := []struct {
		name   string
		policy *Policy
		want   string
	}{
		{
			name: "policy name too large",
			policy: &Policy{
				Name:      strings.Repeat("p", maxPolicyIdentifierBytes+1),
				TableName: "limited",
				Type:      PolicySelect,
			},
			want: "policy identifier too large",
		},
		{
			name: "table name too large",
			policy: &Policy{
				Name:      "too_large_table",
				TableName: strings.Repeat("t", maxPolicyIdentifierBytes+1),
				Type:      PolicySelect,
			},
			want: "policy identifier too large",
		},
		{
			name: "too many users",
			policy: &Policy{
				Name:      "too_many_users",
				TableName: "limited",
				Type:      PolicySelect,
				Users:     make([]string, maxPolicyPrincipals+1),
			},
			want: "too many policy principals",
		},
		{
			name: "role name too large",
			policy: &Policy{
				Name:      "role_too_large",
				TableName: "limited",
				Type:      PolicySelect,
				Roles:     []string{strings.Repeat("r", maxPolicyPrincipalBytes+1)},
			},
			want: "invalid security policy",
		},
		{
			name: "metadata too large",
			policy: &Policy{
				Name:      "metadata_too_large",
				TableName: "limited",
				Type:      PolicySelect,
				Metadata: map[string]interface{}{
					"payload": strings.Repeat("m", maxPolicyMetadataBytes+1),
				},
			},
			want: "policy metadata too large",
		},
		{
			name: "metadata not serializable",
			policy: &Policy{
				Name:      "metadata_invalid",
				TableName: "limited",
				Type:      PolicySelect,
				Metadata: map[string]interface{}{
					"bad": func() {},
				},
			},
			want: "invalid policy metadata",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := NewManager()
			if tt.policy.Users != nil {
				for i := range tt.policy.Users {
					tt.policy.Users[i] = "user"
				}
			}

			err := mgr.CreatePolicy(tt.policy)
			if err == nil {
				t.Fatal("expected oversized policy to be rejected")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
			if mgr.IsEnabled("limited") {
				t.Fatal("CreatePolicy enabled RLS for a rejected policy")
			}
			if policies := mgr.GetTablePolicies("limited"); len(policies) != 0 {
				t.Fatalf("rejected policy was indexed: %d policies", len(policies))
			}
		})
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

func TestRestrictivePolicyNarrowsPermissiveAccess(t *testing.T) {
	mgr := NewManager()
	if err := mgr.CreatePolicy(&Policy{
		Name:       "allow_all",
		TableName:  "documents",
		Type:       PolicySelect,
		Expression: "true",
	}); err != nil {
		t.Fatalf("CreatePolicy permissive: %v", err)
	}
	if err := mgr.CreatePolicy(&Policy{
		Name:        "owner_guard",
		TableName:   "documents",
		Type:        PolicySelect,
		Expression:  "owner = current_user",
		Restrictive: true,
	}); err != nil {
		t.Fatalf("CreatePolicy restrictive: %v", err)
	}

	ctx := context.WithValue(context.Background(), RLSUserKey, "alice")
	allowed, err := mgr.CheckAccess(ctx, "documents", PolicySelect, map[string]interface{}{"owner": "alice"}, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess owner row: %v", err)
	}
	if !allowed {
		t.Fatal("restrictive policy denied a row that passed both policies")
	}

	allowed, err = mgr.CheckAccess(ctx, "documents", PolicySelect, map[string]interface{}{"owner": "bob"}, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess other row: %v", err)
	}
	if allowed {
		t.Fatal("restrictive policy failed to narrow a permissive allow-all policy")
	}
}

func TestRestrictivePolicyAloneDoesNotGrantAccess(t *testing.T) {
	mgr := NewManager()
	if err := mgr.CreatePolicy(&Policy{
		Name:        "owner_guard",
		TableName:   "documents",
		Type:        PolicySelect,
		Expression:  "owner = current_user",
		Restrictive: true,
	}); err != nil {
		t.Fatalf("CreatePolicy restrictive: %v", err)
	}

	ctx := context.WithValue(context.Background(), RLSUserKey, "alice")
	allowed, err := mgr.CheckAccess(ctx, "documents", PolicySelect, map[string]interface{}{"owner": "alice"}, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}
	if allowed {
		t.Fatal("restrictive-only policy granted access without a permissive policy")
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

func TestDeserializePoliciesRejectsInvalidStructureWithoutMutatingState(t *testing.T) {
	mgr := NewManager()
	if err := mgr.CreatePolicy(&Policy{
		Name:       "existing_policy",
		TableName:  "users",
		Type:       PolicySelect,
		Expression: "TRUE",
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	err := mgr.DeserializePolicies([]byte(`{"users:nil_policy":null}`))
	if err == nil {
		t.Fatal("expected nil policy to be rejected")
	}
	if !strings.Contains(err.Error(), "nil policy") {
		t.Fatalf("expected nil policy error, got %v", err)
	}

	if _, getErr := mgr.GetPolicy("users", "existing_policy"); getErr != nil {
		t.Fatalf("DeserializePolicies mutated existing state after invalid input: %v", getErr)
	}
}

func TestDeserializePoliciesRejectsOversizedPayload(t *testing.T) {
	mgr := NewManager()

	payload := []byte(`{"users:p":{"name":"p","table_name":"users","expression":"TRUE","enabled":true,"metadata":"`)
	payload = append(payload, strings.Repeat("x", maxSerializedPoliciesBytes)...)
	payload = append(payload, []byte(`"}}`)...)

	err := mgr.DeserializePolicies(payload)
	if err == nil {
		t.Fatal("expected oversized serialized policies to be rejected")
	}
	if !strings.Contains(err.Error(), "serialized policies too large") {
		t.Fatalf("expected oversized policies error, got %v", err)
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

func TestCheckAccessWithPublicRole(t *testing.T) {
	mgr := NewManager()
	if err := mgr.CreatePolicy(&Policy{
		Name:       "public_policy",
		TableName:  "documents",
		Type:       PolicySelect,
		Expression: "visible = true",
		Roles:      []string{"PUBLIC"},
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	allowed, err := mgr.CheckAccess(context.Background(), "documents", PolicySelect, map[string]interface{}{"visible": true}, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}
	if !allowed {
		t.Fatal("PUBLIC policy did not apply to a user without explicit roles")
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

// TestRLSRestrictivePolicyFailsClosedWhenExprUncompilable verifies that a
// restrictive policy whose expression fails to recompile on load (via
// DeserializePolicies) denies access (fail-closed) instead of being silently
// skipped, which would leak rows it was meant to hide (fail-open).
func TestRLSRestrictivePolicyFailsClosedWhenExprUncompilable(t *testing.T) {
	// Build a valid permissive(grant-all) + restrictive policy set and serialize.
	src := NewManager()
	if err := src.CreatePolicy(&Policy{
		Name: "allow_all", TableName: "docs", Type: PolicySelect,
		Expression: "true", Enabled: true,
	}); err != nil {
		t.Fatalf("create permissive: %v", err)
	}
	if err := src.CreatePolicy(&Policy{
		Name: "hide_secret", TableName: "docs", Type: PolicySelect,
		Expression: "owner_id = 12345", Restrictive: true, Enabled: true,
	}); err != nil {
		t.Fatalf("create restrictive: %v", err)
	}
	blob, err := src.SerializePolicies()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	// Corrupt only the restrictive policy's expression into one that cannot be
	// compiled (simulating version/format skew or a hand-edited blob).
	corrupted := strings.Replace(string(blob), "owner_id = 12345", "status ~~ active", 1)
	if corrupted == string(blob) {
		t.Fatal("test setup: restrictive expression not found in serialized blob")
	}

	// Reload. DeserializePolicies tolerates the compile failure (logs+continues),
	// leaving the restrictive policy enabled but without a compiled expression.
	dst := NewManager()
	if err := dst.DeserializePolicies([]byte(corrupted)); err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	// The permissive policy would grant this row; the restrictive policy must
	// still hide it. With the uncompiled restrictive policy, access must be
	// DENIED (fail-closed), not allowed.
	row := map[string]interface{}{"owner_id": float64(1), "status": "secret"}
	allowed, err := dst.CheckAccess(context.Background(), "docs", PolicySelect, row, "someuser", nil)
	if err != nil {
		t.Fatalf("CheckAccess: %v", err)
	}
	if allowed {
		t.Fatal("restrictive policy with an uncompilable expression leaked the row (fail-open)")
	}
}
