// RLS Expression Parser Tests
package security

import (
	"context"
	"testing"
)

func TestExpressionParser(t *testing.T) {
	mgr := NewManager()

	tests := []struct {
		name       string
		expression string
		row        map[string]interface{}
		ctx        context.Context
		want       bool
		wantErr    bool
	}{
		{
			name:       "Boolean TRUE",
			expression: "TRUE",
			row:        map[string]interface{}{},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "Boolean FALSE",
			expression: "FALSE",
			row:        map[string]interface{}{},
			ctx:        context.Background(),
			want:       false,
			wantErr:    false,
		},
		{
			name:       "Equal numbers",
			expression: "age = 25",
			row:        map[string]interface{}{"age": 25},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "Not equal numbers",
			expression: "age != 25",
			row:        map[string]interface{}{"age": 30},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "Greater than",
			expression: "age > 20",
			row:        map[string]interface{}{"age": 25},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "Less than or equal",
			expression: "age <= 25",
			row:        map[string]interface{}{"age": 25},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "String equality",
			expression: "name = 'John'",
			row:        map[string]interface{}{"name": "John"},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "IS NULL true",
			expression: "email IS NULL",
			row:        map[string]interface{}{"email": nil},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "IS NOT NULL true",
			expression: "email IS NOT NULL",
			row:        map[string]interface{}{"email": "test@example.com"},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "IN operator",
			expression: "status IN ('active', 'pending')",
			row:        map[string]interface{}{"status": "active"},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "LIKE operator",
			expression: "name LIKE 'John%'",
			row:        map[string]interface{}{"name": "Johnson"},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "LIKE underscore wildcard",
			expression: "code LIKE 'A_'",
			row:        map[string]interface{}{"code": "A1"},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "AND operator",
			expression: "age > 18 AND status = 'active'",
			row:        map[string]interface{}{"age": 25, "status": "active"},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "AND operator - second false",
			expression: "age > 18 AND status = 'inactive'",
			row:        map[string]interface{}{"age": 25, "status": "active"},
			ctx:        context.Background(),
			want:       false,
			wantErr:    false,
		},
		{
			name:       "OR operator - first true",
			expression: "role = 'admin' OR role = 'moderator'",
			row:        map[string]interface{}{"role": "admin"},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "OR operator - second true",
			expression: "role = 'admin' OR role = 'moderator'",
			row:        map[string]interface{}{"role": "moderator"},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "NOT operator",
			expression: "NOT deleted",
			row:        map[string]interface{}{"deleted": false},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "Parentheses",
			expression: "(age > 18 AND age < 65)",
			row:        map[string]interface{}{"age": 25},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "Complex expression",
			expression: "(role = 'admin' OR role = 'manager') AND active = true",
			row:        map[string]interface{}{"role": "manager", "active": true},
			ctx:        context.Background(),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "Current user expression",
			expression: "user_id = current_user",
			row:        map[string]interface{}{"user_id": "alice"},
			ctx:        context.WithValue(context.Background(), RLSUserKey, "alice"),
			want:       true,
			wantErr:    false,
		},
		{
			name:       "Current tenant expression",
			expression: "tenant_id = current_tenant",
			row:        map[string]interface{}{"tenant_id": "tenant1"},
			ctx:        context.WithValue(context.Background(), RLSTenantKey, "tenant1"),
			want:       true,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := &Policy{
				Name:       "test_policy",
				TableName:  "test_table",
				Type:       PolicySelect,
				Expression: tt.expression,
			}

			err := mgr.CreatePolicy(policy)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreatePolicy() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err != nil {
				return
			}

			// Clean up for next test
			defer mgr.DropPolicy("test_table", "test_policy")

			got, err := mgr.CheckAccess(tt.ctx, "test_table", PolicySelect, tt.row, "", nil)
			if err != nil {
				t.Fatalf("CheckAccess() error = %v", err)
			}

			if got != tt.want {
				t.Errorf("CheckAccess() = %v, want %v for expression: %s", got, tt.want, tt.expression)
			}
		})
	}
}

func TestNullHandling(t *testing.T) {
	mgr := NewManager()

	// Create policy with NULL check
	policy := &Policy{
		Name:       "null_policy",
		TableName:  "test_table",
		Type:       PolicySelect,
		Expression: "deleted_at IS NULL",
	}

	if err := mgr.CreatePolicy(policy); err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Row with NULL deleted_at should be allowed
	row1 := map[string]interface{}{"deleted_at": nil}
	allowed, err := mgr.CheckAccess(context.Background(), "test_table", PolicySelect, row1, "", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if !allowed {
		t.Error("Row with NULL deleted_at should be allowed")
	}

	// Row with non-NULL deleted_at should be denied
	row2 := map[string]interface{}{"deleted_at": "2024-01-01"}
	allowed, err = mgr.CheckAccess(context.Background(), "test_table", PolicySelect, row2, "", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if allowed {
		t.Error("Row with non-NULL deleted_at should be denied")
	}
}

func TestComplexPolicyWithUserAndExpression(t *testing.T) {
	mgr := NewManager()

	// Create policy that checks both user and expression
	policy := &Policy{
		Name:       "owner_policy",
		TableName:  "documents",
		Type:       PolicySelect,
		Expression: "owner_id = current_user OR shared = true",
		Users:      []string{}, // All users
	}

	if err := mgr.CreatePolicy(policy); err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	ctx := context.WithValue(context.Background(), RLSUserKey, "alice")

	// Row owned by alice should be allowed
	row1 := map[string]interface{}{"owner_id": "alice", "shared": false}
	allowed, err := mgr.CheckAccess(ctx, "documents", PolicySelect, row1, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if !allowed {
		t.Error("Row owned by alice should be allowed for alice")
	}

	// Shared row should be allowed even if not owned
	row2 := map[string]interface{}{"owner_id": "bob", "shared": true}
	allowed, err = mgr.CheckAccess(ctx, "documents", PolicySelect, row2, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if !allowed {
		t.Error("Shared row should be allowed")
	}

	// Private row owned by bob should be denied
	row3 := map[string]interface{}{"owner_id": "bob", "shared": false}
	allowed, err = mgr.CheckAccess(ctx, "documents", PolicySelect, row3, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if allowed {
		t.Error("Private row owned by bob should be denied for alice")
	}
}

func TestPolicyWithGreaterThanOrEqual(t *testing.T) {
	mgr := NewManager()

	policy := &Policy{
		Name:       "age_policy",
		TableName:  "content",
		Type:       PolicySelect,
		Expression: "min_age >= 18",
	}

	if err := mgr.CreatePolicy(policy); err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Row with min_age = 18 should be allowed
	row1 := map[string]interface{}{"min_age": 18}
	allowed, err := mgr.CheckAccess(context.Background(), "content", PolicySelect, row1, "", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if !allowed {
		t.Error("Row with min_age >= 18 should be allowed")
	}

	// Row with min_age = 21 should be allowed
	row2 := map[string]interface{}{"min_age": 21}
	allowed, err = mgr.CheckAccess(context.Background(), "content", PolicySelect, row2, "", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if !allowed {
		t.Error("Row with min_age >= 18 should be allowed")
	}

	// Row with min_age = 16 should be denied
	row3 := map[string]interface{}{"min_age": 16}
	allowed, err = mgr.CheckAccess(context.Background(), "content", PolicySelect, row3, "", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if allowed {
		t.Error("Row with min_age < 18 should be denied")
	}
}
