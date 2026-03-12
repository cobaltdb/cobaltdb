package security

import (
	"context"
	"testing"
)

func TestGetValueFromRow(t *testing.T) {
	mgr := NewManager()

	row := map[string]interface{}{
		"name":    "alice",
		"age":     30,
		"country": "US",
	}

	// Exact match
	if v := mgr.getValueFromRow("name", row); v != "alice" {
		t.Errorf("expected alice, got %v", v)
	}

	// Lowercase fallback
	if v := mgr.getValueFromRow("NAME", row); v != "alice" {
		t.Errorf("expected alice via lowercase fallback, got %v", v)
	}

	// Missing key
	if v := mgr.getValueFromRow("missing", row); v != nil {
		t.Errorf("expected nil for missing key, got %v", v)
	}

	// Empty row
	if v := mgr.getValueFromRow("name", map[string]interface{}{}); v != nil {
		t.Errorf("expected nil for empty row, got %v", v)
	}
}

func TestGetContextValue(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	// No context values set
	if v := mgr.getContextValue("CURRENT_USER", ctx); v != nil {
		t.Errorf("expected nil, got %v", v)
	}
	if v := mgr.getContextValue("CURRENT_USER()", ctx); v != nil {
		t.Errorf("expected nil, got %v", v)
	}
	if v := mgr.getContextValue("CURRENT_TENANT", ctx); v != nil {
		t.Errorf("expected nil, got %v", v)
	}
	if v := mgr.getContextValue("CURRENT_ROLE", ctx); v != nil {
		t.Errorf("expected nil, got %v", v)
	}
	if v := mgr.getContextValue("SESSION_USER", ctx); v != nil {
		t.Errorf("expected nil for SESSION_USER, got %v", v)
	}
	if v := mgr.getContextValue("unknown_func", ctx); v != nil {
		t.Errorf("expected nil for unknown, got %v", v)
	}

	// With context values set
	ctx = context.WithValue(ctx, RLSUserKey, "alice")
	ctx = context.WithValue(ctx, RLSTenantKey, "tenant1")
	ctx = context.WithValue(ctx, RLSRoleKey, "admin")

	if v := mgr.getContextValue("CURRENT_USER", ctx); v != "alice" {
		t.Errorf("expected alice, got %v", v)
	}
	if v := mgr.getContextValue("current_tenant()", ctx); v != "tenant1" {
		t.Errorf("expected tenant1, got %v", v)
	}
	if v := mgr.getContextValue("current_role()", ctx); v != "admin" {
		t.Errorf("expected admin, got %v", v)
	}

	// SESSION_USER falls back to RLSUserKey when RLSSessionUserKey not set
	if v := mgr.getContextValue("SESSION_USER", ctx); v != "alice" {
		t.Errorf("expected alice for SESSION_USER fallback, got %v", v)
	}

	// SESSION_USER with explicit session user
	ctx = context.WithValue(ctx, RLSSessionUserKey, "bob")
	if v := mgr.getContextValue("SESSION_USER", ctx); v != "bob" {
		t.Errorf("expected bob for SESSION_USER, got %v", v)
	}
}

func TestToFloat64EdgeCases(t *testing.T) {
	// int32
	if f, ok := ToFloat64(int32(42)); !ok || f != 42 {
		t.Errorf("int32: expected 42, got %v", f)
	}

	// float32
	if f, ok := ToFloat64(float32(3.14)); !ok || f < 3.13 || f > 3.15 {
		t.Errorf("float32: expected ~3.14, got %v", f)
	}

	// bool true
	if f, ok := ToFloat64(true); !ok || f != 1 {
		t.Errorf("bool true: expected 1, got %v", f)
	}

	// bool false
	if f, ok := ToFloat64(false); !ok || f != 0 {
		t.Errorf("bool false: expected 0, got %v", f)
	}

	// string number
	if f, ok := ToFloat64("42.5"); !ok || f != 42.5 {
		t.Errorf("string number: expected 42.5, got %v", f)
	}

	// string non-number
	if _, ok := ToFloat64("abc"); ok {
		t.Error("expected false for non-numeric string")
	}

	// nil
	if _, ok := ToFloat64(nil); ok {
		t.Error("expected false for nil")
	}

	// []byte (unsupported)
	if _, ok := ToFloat64([]byte("test")); ok {
		t.Error("expected false for []byte")
	}
}

func TestIsBareColumn(t *testing.T) {
	// Valid column names
	if !isBareColumn("name") {
		t.Error("expected true for 'name'")
	}
	if !isBareColumn("user_id") {
		t.Error("expected true for 'user_id'")
	}
	if !isBareColumn("col123") {
		t.Error("expected true for 'col123'")
	}

	// Invalid: contains operators
	if isBareColumn("a = b") {
		t.Error("expected false for 'a = b'")
	}
	if isBareColumn("a < b") {
		t.Error("expected false for 'a < b'")
	}
	if isBareColumn("a > b") {
		t.Error("expected false for 'a > b'")
	}

	// Invalid: reserved words
	if isBareColumn("TRUE") {
		t.Error("expected false for 'TRUE'")
	}
	if isBareColumn("NULL") {
		t.Error("expected false for 'NULL'")
	}
	if isBareColumn("AND") {
		t.Error("expected false for 'AND'")
	}
	if isBareColumn("OR") {
		t.Error("expected false for 'OR'")
	}
	if isBareColumn("NOT") {
		t.Error("expected false for 'NOT'")
	}
	if isBareColumn("IN") {
		t.Error("expected false for 'IN'")
	}
	if isBareColumn("LIKE") {
		t.Error("expected false for 'LIKE'")
	}
	if isBareColumn("BETWEEN") {
		t.Error("expected false for 'BETWEEN'")
	}
	if isBareColumn("IS") {
		t.Error("expected false for 'IS'")
	}

	// Invalid: contains non-identifier chars
	if isBareColumn("a.b") {
		t.Error("expected false for 'a.b'")
	}
	if isBareColumn("a b") {
		t.Error("expected false for 'a b'")
	}

	// Invalid: empty
	if isBareColumn("") {
		t.Error("expected false for empty string")
	}
}

func TestParseSimpleExpressionEdgeCases(t *testing.T) {
	mgr := NewManager()
	ctx := context.WithValue(context.Background(), RLSUserKey, "alice")

	// Bare column as boolean
	expr, err := mgr.parseSimpleExpression("is_active")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Row with is_active = true
	result, err := expr(ctx, map[string]interface{}{"is_active": true})
	if err != nil || !result {
		t.Error("expected true for is_active=true")
	}
	// Row with is_active = false
	result, err = expr(ctx, map[string]interface{}{"is_active": false})
	if err != nil || result {
		t.Error("expected false for is_active=false")
	}
	// Row with is_active = nil
	result, err = expr(ctx, map[string]interface{}{"is_active": nil})
	if err != nil || result {
		t.Error("expected false for is_active=nil")
	}
	// Row without is_active (non-nil value is truthy)
	result, err = expr(ctx, map[string]interface{}{"is_active": "yes"})
	if err != nil || !result {
		t.Error("expected true for is_active='yes'")
	}

	// CURRENT_USER as bare expression
	expr, err = mgr.parseSimpleExpression("CURRENT_USER")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err = expr(ctx, nil)
	if err != nil || !result {
		t.Error("expected true for CURRENT_USER when user is set")
	}
	// Without user in context
	result, err = expr(context.Background(), nil)
	if err != nil || result {
		t.Error("expected false for CURRENT_USER when no user set")
	}

	// CURRENT_TENANT as bare expression
	expr, err = mgr.parseSimpleExpression("CURRENT_TENANT()")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result, err = expr(context.Background(), nil)
	if err != nil || result {
		t.Error("expected false for CURRENT_TENANT when not set")
	}

	// Unsupported expression
	_, err = mgr.parseSimpleExpression("1 + 2")
	if err == nil {
		t.Error("expected error for unsupported expression")
	}
}

func TestCreateComparisonEvaluatorContextExpr(t *testing.T) {
	mgr := NewManager()
	ctx := context.WithValue(context.Background(), RLSUserKey, "alice")

	// user_id = current_user
	expr := mgr.createComparisonEvaluator("user_id", "=", "CURRENT_USER")
	row := map[string]interface{}{"user_id": "alice"}
	result, err := expr(ctx, row)
	if err != nil || !result {
		t.Error("expected true for user_id=alice vs current_user=alice")
	}

	// Mismatch
	row = map[string]interface{}{"user_id": "bob"}
	result, err = expr(ctx, row)
	if err != nil || result {
		t.Error("expected false for user_id=bob vs current_user=alice")
	}

	// Numeric comparisons
	expr = mgr.createComparisonEvaluator("age", ">=", "18")
	row = map[string]interface{}{"age": float64(21)}
	result, err = expr(ctx, row)
	if err != nil || !result {
		t.Error("expected true for age=21 >= 18")
	}
	row = map[string]interface{}{"age": float64(16)}
	result, err = expr(ctx, row)
	if err != nil || result {
		t.Error("expected false for age=16 >= 18")
	}

	// String less-than
	expr = mgr.createComparisonEvaluator("name", "<", "'z'")
	row = map[string]interface{}{"name": "alice"}
	result, err = expr(ctx, row)
	if err != nil || !result {
		t.Error("expected true for name=alice < z")
	}

	// String greater-than
	expr = mgr.createComparisonEvaluator("name", ">", "'a'")
	row = map[string]interface{}{"name": "bob"}
	result, err = expr(ctx, row)
	if err != nil || !result {
		t.Error("expected true for name=bob > a")
	}

	// String <=
	expr = mgr.createComparisonEvaluator("name", "<=", "'alice'")
	row = map[string]interface{}{"name": "alice"}
	result, err = expr(ctx, row)
	if err != nil || !result {
		t.Error("expected true for name=alice <= alice")
	}

	// NULL comparisons
	expr = mgr.createComparisonEvaluator("col", "=", "'val'")
	row = map[string]interface{}{"col": nil}
	result, err = expr(ctx, row)
	if err != nil || result {
		t.Error("expected false for nil = 'val'")
	}

	// NULL != comparison
	expr = mgr.createComparisonEvaluator("col", "!=", "'val'")
	row = map[string]interface{}{"col": nil}
	result, err = expr(ctx, row)
	if err != nil || !result {
		t.Error("expected true for nil != 'val'")
	}

	// NULL < comparison (should return false)
	expr = mgr.createComparisonEvaluator("col", "<", "'val'")
	row = map[string]interface{}{"col": nil}
	result, err = expr(ctx, row)
	if err != nil || result {
		t.Error("expected false for nil < 'val'")
	}

	// Both NULL =
	expr = mgr.createComparisonEvaluator("a", "=", "b")
	row = map[string]interface{}{"a": nil, "b": nil}
	result, err = expr(ctx, row)
	// Both nil columns - getValue for "b" will try row["b"] which is nil
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCheckAccessWithRolesDeep(t *testing.T) {
	mgr := NewManager()
	ctx := context.WithValue(context.Background(), RLSUserKey, "alice")

	// Create policy with role restriction
	err := mgr.CreatePolicy(&Policy{
		Name:       "role_policy",
		TableName:  "secrets",
		Type:       PolicySelect,
		Expression: "TRUE",
		Roles:      []string{"admin", "superuser"},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}

	row := map[string]interface{}{"data": "secret"}

	// User with matching role
	allowed, err := mgr.CheckAccess(ctx, "secrets", PolicySelect, row, "alice", []string{"admin"})
	if err != nil || !allowed {
		t.Error("expected allowed for user with admin role")
	}

	// User with non-matching role
	allowed, err = mgr.CheckAccess(ctx, "secrets", PolicySelect, row, "alice", []string{"viewer"})
	if err != nil || allowed {
		t.Error("expected denied for user with viewer role")
	}

	// User with one matching role among many
	allowed, err = mgr.CheckAccess(ctx, "secrets", PolicySelect, row, "alice", []string{"viewer", "superuser"})
	if err != nil || !allowed {
		t.Error("expected allowed for user with superuser role among others")
	}
}

func TestCheckAccessWithUsers(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	// Create policy with user restriction
	err := mgr.CreatePolicy(&Policy{
		Name:       "user_policy",
		TableName:  "data",
		Type:       PolicyAll,
		Expression: "TRUE",
		Users:      []string{"alice", "bob"},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}

	row := map[string]interface{}{"val": 1}

	// Matching user
	allowed, err := mgr.CheckAccess(ctx, "data", PolicySelect, row, "alice", nil)
	if err != nil || !allowed {
		t.Error("expected allowed for alice")
	}

	// Non-matching user
	allowed, err = mgr.CheckAccess(ctx, "data", PolicySelect, row, "charlie", nil)
	if err != nil || allowed {
		t.Error("expected denied for charlie")
	}
}

func TestCheckAccessPolicyTypeFilter(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	// Create SELECT-only policy
	err := mgr.CreatePolicy(&Policy{
		Name:       "select_only",
		TableName:  "items",
		Type:       PolicySelect,
		Expression: "TRUE",
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}

	row := map[string]interface{}{"id": 1}

	// SELECT should pass
	allowed, err := mgr.CheckAccess(ctx, "items", PolicySelect, row, "anyone", nil)
	if err != nil || !allowed {
		t.Error("expected allowed for SELECT")
	}

	// INSERT should fail (no matching policy)
	allowed, err = mgr.CheckAccess(ctx, "items", PolicyInsert, row, "anyone", nil)
	if err != nil || allowed {
		t.Error("expected denied for INSERT with SELECT-only policy")
	}

	// DELETE should fail
	allowed, err = mgr.CheckAccess(ctx, "items", PolicyDelete, row, "anyone", nil)
	if err != nil || allowed {
		t.Error("expected denied for DELETE with SELECT-only policy")
	}
}

func TestGetValue(t *testing.T) {
	mgr := NewManager()
	ctx := context.WithValue(context.Background(), RLSUserKey, "alice")
	row := map[string]interface{}{"name": "bob", "age": 30}

	// Quoted string
	if v := mgr.getValue("'hello'", ctx, row); v != "hello" {
		t.Errorf("expected hello, got %v", v)
	}
	// Double-quoted string
	if v := mgr.getValue("\"world\"", ctx, row); v != "world" {
		t.Errorf("expected world, got %v", v)
	}

	// Number
	if v := mgr.getValue("42", ctx, row); v != float64(42) {
		t.Errorf("expected 42, got %v", v)
	}

	// Boolean TRUE
	if v := mgr.getValue("TRUE", ctx, row); v != true {
		t.Errorf("expected true, got %v", v)
	}
	// Boolean FALSE
	if v := mgr.getValue("FALSE", ctx, row); v != false {
		t.Errorf("expected false, got %v", v)
	}

	// Context value
	if v := mgr.getValue("CURRENT_USER", ctx, row); v != "alice" {
		t.Errorf("expected alice, got %v", v)
	}

	// Row value
	if v := mgr.getValue("name", ctx, row); v != "bob" {
		t.Errorf("expected bob, got %v", v)
	}
}

func TestPolicyTypeString(t *testing.T) {
	tests := []struct {
		pt   PolicyType
		want string
	}{
		{PolicySelect, "SELECT"},
		{PolicyInsert, "INSERT"},
		{PolicyUpdate, "UPDATE"},
		{PolicyDelete, "DELETE"},
		{PolicyAll, "ALL"},
		{PolicyType(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.pt.String(); got != tt.want {
			t.Errorf("PolicyType(%d).String() = %s, want %s", tt.pt, got, tt.want)
		}
	}
}

func TestForceRowDeep(t *testing.T) {
	mgr := NewManager()

	// No policies - not forced
	if mgr.ForceRow("users") {
		t.Error("expected false when no policies")
	}

	// Policy without force_row_security
	mgr.CreatePolicy(&Policy{
		Name:      "basic",
		TableName: "users",
		Type:      PolicyAll,
	})
	if mgr.ForceRow("users") {
		t.Error("expected false without force_row_security metadata")
	}

	// Policy with force_row_security = true
	mgr.CreatePolicy(&Policy{
		Name:      "forced",
		TableName: "secrets",
		Type:      PolicyAll,
		Metadata:  map[string]interface{}{"force_row_security": true},
	})
	if !mgr.ForceRow("secrets") {
		t.Error("expected true with force_row_security=true")
	}

	// Policy with force_row_security = false
	mgr.CreatePolicy(&Policy{
		Name:      "not_forced",
		TableName: "public",
		Type:      PolicyAll,
		Metadata:  map[string]interface{}{"force_row_security": false},
	})
	if mgr.ForceRow("public") {
		t.Error("expected false with force_row_security=false")
	}
}
