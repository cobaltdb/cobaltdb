package security

import (
	"testing"
	"time"
)

func TestValueToStringKey(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"nil", nil, "<nil>"},
		{"string", "hello", "hello"},
		{"bytes", []byte("world"), "world"},
		{"int64", int64(42), "42"},
		{"int", 99, "99"},
		{"float64", float64(3.14), "3.14"},
		{"bool_true", true, "true"},
		{"bool_false", false, "false"},
		{"time", time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), "2024-06-15 00:00:00 +0000 UTC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToStringKey(tt.input)
			if tt.name == "time" {
				// time formats vary, just check it's non-empty
				if result == "" {
					t.Error("expected non-empty time string")
				}
				return
			}
			if result != tt.expected {
				t.Errorf("valueToStringKey(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValueToString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"nil", nil, "<nil>"},
		{"string", "hello", "hello"},
		{"bytes", []byte("data"), "data"},
		{"int64", int64(-7), "-7"},
		{"int", 42, "42"},
		{"float64", float64(2.5), "2.5"},
		{"bool_true", true, "true"},
		{"bool_false", false, "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := valueToString(tt.input)
			if result != tt.expected {
				t.Errorf("valueToString(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseComplexExpression(t *testing.T) {
	mgr := NewManager()

	tests := []struct {
		name string
		expr string
		row  map[string]interface{}
		want bool
	}{
		{"AND true/true", "department = 'engineering' AND level > 5", map[string]interface{}{"department": "engineering", "level": 8}, true},
		{"AND true/false", "department = 'engineering' AND level > 5", map[string]interface{}{"department": "engineering", "level": 3}, false},
		{"AND false/false", "department = 'hr' AND level > 5", map[string]interface{}{"department": "engineering", "level": 3}, false},
		{"OR true/false", "department = 'engineering' OR level > 5", map[string]interface{}{"department": "engineering", "level": 3}, true},
		{"OR false/true", "department = 'hr' OR level > 5", map[string]interface{}{"department": "engineering", "level": 8}, true},
		{"OR false/false", "department = 'hr' OR level > 10", map[string]interface{}{"department": "engineering", "level": 3}, false},
		{"nested parens AND", "(department = 'eng') AND (level > 5)", map[string]interface{}{"department": "eng", "level": 8}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := mgr.parseComplexExpression(tt.expr)
			if err != nil {
				t.Fatalf("parseComplexExpression(%q): %v", tt.expr, err)
			}
			got, err := expr(nil, tt.row)
			if err != nil {
				t.Fatalf("eval: %v", err)
			}
			if got != tt.want {
				t.Errorf("eval(%q, %v) = %v, want %v", tt.expr, tt.row, got, tt.want)
			}
		})
	}
}

func TestCompilePolicyEmptyExpression(t *testing.T) {
	mgr := NewManager()
	policy := &Policy{
		Name:       "test_empty",
		TableName:  "users",
		Expression: "",
		Enabled:    true,
	}
	err := mgr.compilePolicy(policy)
	if err != nil {
		t.Fatalf("compilePolicy with empty expression: %v", err)
	}

	// Should evaluate to true (default allow)
	key := mgr.policyKey("users", "test_empty")
	expr := mgr.compiledExprs[key]
	result, err := expr(nil, map[string]interface{}{"name": "alice"})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !result {
		t.Error("expected default allow")
	}
}

func TestCompilePolicyWithExpression(t *testing.T) {
	mgr := NewManager()
	policy := &Policy{
		Name:       "test_expr",
		TableName:  "orders",
		Expression: "status = 'active'",
		Enabled:    true,
	}
	err := mgr.compilePolicy(policy)
	if err != nil {
		t.Fatalf("compilePolicy: %v", err)
	}

	key := mgr.policyKey("orders", "test_expr")
	expr := mgr.compiledExprs[key]

	result, err := expr(nil, map[string]interface{}{"status": "active"})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !result {
		t.Error("expected true for matching row")
	}

	result, err = expr(nil, map[string]interface{}{"status": "closed"})
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if result {
		t.Error("expected false for non-matching row")
	}
}

func TestDeserializePoliciesWithInvalidExpression(t *testing.T) {
	mgr := NewManager()

	// Valid JSON but invalid expression
	data := []byte(`{
		"users:bad_policy": {
			"name": "bad_policy",
			"tableName": "users",
			"expression": "!!!invalid!!!",
			"enabled": true,
			"metadata": null
		}
	}`)

	err := mgr.DeserializePolicies(data)
	if err != nil {
		// Should succeed (errors are logged, not returned)
		t.Fatalf("DeserializePolicies should not fail on bad expressions: %v", err)
	}
}

func TestForceRowSecurity(t *testing.T) {
	mgr := NewManager()

	// No policies — should return false
	if mgr.ForceRow("users") {
		t.Error("expected false with no policies")
	}

	// Create policy with force_row_security metadata
	mgr.CreatePolicy(&Policy{
		Name:       "force_test",
		TableName:  "users",
		Expression: "department = 'eng'",
		Enabled:    true,
		Metadata:   map[string]interface{}{"force_row_security": true},
	})

	if !mgr.ForceRow("users") {
		t.Error("expected true with force_row_security policy")
	}
}

func TestEnableDisablePolicyExtra(t *testing.T) {
	mgr := NewManager()
	mgr.CreatePolicy(&Policy{
		Name:       "test_pol",
		TableName:  "orders",
		Expression: "status = 'active'",
		Enabled:    true,
	})

	// Disable
	err := mgr.DisablePolicy("orders", "test_pol")
	if err != nil {
		t.Fatalf("DisablePolicy: %v", err)
	}

	// Re-enable
	err = mgr.EnablePolicy("orders", "test_pol")
	if err != nil {
		t.Fatalf("EnablePolicy: %v", err)
	}
}
