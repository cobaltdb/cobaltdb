// Package security provides row-level security (RLS) for CobaltDB
package security

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrPolicyNotFound      = errors.New("security policy not found")
	ErrPolicyAlreadyExists = errors.New("security policy already exists")
	ErrInvalidPolicy       = errors.New("invalid security policy")
	ErrRLSNotEnabled       = errors.New("row-level security not enabled for table")
)

// PolicyType defines the type of policy
type PolicyType int

const (
	PolicySelect PolicyType = iota
	PolicyInsert
	PolicyUpdate
	PolicyDelete
	PolicyAll
)

func (p PolicyType) String() string {
	switch p {
	case PolicySelect:
		return "SELECT"
	case PolicyInsert:
		return "INSERT"
	case PolicyUpdate:
		return "UPDATE"
	case PolicyDelete:
		return "DELETE"
	case PolicyAll:
		return "ALL"
	default:
		return "UNKNOWN"
	}
}

// PolicyExpr represents a policy expression/condition
type PolicyExpr func(ctx context.Context, row map[string]interface{}) (bool, error)

// Policy defines a row-level security policy
type Policy struct {
	Name       string                 `json:"name"`
	TableName  string                 `json:"table_name"`
	Type       PolicyType             `json:"type"`
	Expression string                 `json:"expression"`     // SQL expression
	Users      []string               `json:"users"`          // Apply to specific users (empty = all)
	Roles      []string               `json:"roles"`          // Apply to specific roles
	Enabled    bool                   `json:"enabled"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// Manager manages row-level security policies
type Manager struct {
	policies      map[string]*Policy // key: "tableName:policyName"
	tablePolicies map[string][]string // key: tableName, value: []policyNames
	enabledTables map[string]bool     // Tables with RLS enabled
	compiledExprs map[string]PolicyExpr // Compiled expressions
	mu            sync.RWMutex
}

// NewManager creates a new RLS manager
func NewManager() *Manager {
	return &Manager{
		policies:      make(map[string]*Policy),
		tablePolicies: make(map[string][]string),
		enabledTables: make(map[string]bool),
		compiledExprs: make(map[string]PolicyExpr),
	}
}

// EnableTable enables row-level security for a table
func (m *Manager) EnableTable(tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabledTables[strings.ToLower(tableName)] = true
}

// DisableTable disables row-level security for a table
func (m *Manager) DisableTable(tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.enabledTables, strings.ToLower(tableName))
}

// IsEnabled checks if RLS is enabled for a table
func (m *Manager) IsEnabled(tableName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enabledTables[strings.ToLower(tableName)]
}

// CreatePolicy creates a new security policy
func (m *Manager) CreatePolicy(policy *Policy) error {
	if policy == nil || policy.Name == "" || policy.TableName == "" {
		return ErrInvalidPolicy
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.policyKey(policy.TableName, policy.Name)
	if _, exists := m.policies[key]; exists {
		return ErrPolicyAlreadyExists
	}

	// Normalize
	policy.TableName = strings.ToLower(policy.TableName)
	policy.Name = strings.ToLower(policy.Name)
	if policy.Metadata == nil {
		policy.Metadata = make(map[string]interface{})
	}
	policy.Enabled = true

	// Store policy
	m.policies[key] = policy
	m.tablePolicies[policy.TableName] = append(m.tablePolicies[policy.TableName], policy.Name)

	// Enable RLS for table automatically
	m.enabledTables[policy.TableName] = true

	// Compile expression (simplified - real implementation would parse SQL)
	if err := m.compilePolicy(policy); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}

	return nil
}

// DropPolicy removes a security policy
func (m *Manager) DropPolicy(tableName, policyName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableName = strings.ToLower(tableName)
	policyName = strings.ToLower(policyName)
	key := m.policyKey(tableName, policyName)

	if _, exists := m.policies[key]; !exists {
		return ErrPolicyNotFound
	}

	delete(m.policies, key)
	delete(m.compiledExprs, key)

	// Remove from table policies
	if policies, ok := m.tablePolicies[tableName]; ok {
		newPolicies := make([]string, 0, len(policies)-1)
		for _, p := range policies {
			if p != policyName {
				newPolicies = append(newPolicies, p)
			}
		}
		m.tablePolicies[tableName] = newPolicies
	}

	return nil
}

// GetPolicy retrieves a policy
func (m *Manager) GetPolicy(tableName, policyName string) (*Policy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.policyKey(tableName, policyName)
	policy, exists := m.policies[key]
	if !exists {
		return nil, ErrPolicyNotFound
	}

	// Return copy
	p := *policy
	return &p, nil
}

// GetTablePolicies returns all policies for a table
func (m *Manager) GetTablePolicies(tableName string) []*Policy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableName = strings.ToLower(tableName)
	policyNames := m.tablePolicies[tableName]

	policies := make([]*Policy, 0, len(policyNames))
	for _, name := range policyNames {
		key := m.policyKey(tableName, name)
		if policy, ok := m.policies[key]; ok && policy.Enabled {
			p := *policy
			policies = append(policies, &p)
		}
	}
	return policies
}

// CheckAccess checks if a user can access a row based on policies
func (m *Manager) CheckAccess(ctx context.Context, tableName string, policyType PolicyType, row map[string]interface{}, user string, roles []string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tableName = strings.ToLower(tableName)

	// If RLS not enabled for table, allow access
	if !m.enabledTables[tableName] {
		return true, nil
	}

	policyNames := m.tablePolicies[tableName]
	if len(policyNames) == 0 {
		// RLS enabled but no policies = deny all
		return false, nil
	}

	// Check applicable policies
	for _, name := range policyNames {
		key := m.policyKey(tableName, name)
		policy, ok := m.policies[key]
		if !ok || !policy.Enabled {
			continue
		}

		// Check if policy applies to this operation type
		if policy.Type != PolicyAll && policy.Type != policyType {
			continue
		}

		// Check if policy applies to this user
		if len(policy.Users) > 0 {
			found := false
			for _, u := range policy.Users {
				if strings.EqualFold(u, user) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Check if policy applies to any of user's roles
		if len(policy.Roles) > 0 {
			found := false
			for _, policyRole := range policy.Roles {
				for _, userRole := range roles {
					if strings.EqualFold(policyRole, userRole) {
						found = true
						break
					}
				}
				if found {
					break
				}
			}
			if !found {
				continue
			}
		}

		// Evaluate policy expression
		expr, ok := m.compiledExprs[key]
		if !ok {
			continue
		}

		allowed, err := expr(ctx, row)
		if err != nil {
			return false, err
		}
		if allowed {
			return true, nil
		}
	}

	// No policy allowed access
	return false, nil
}

// FilterRows filters a slice of rows based on RLS policies
func (m *Manager) FilterRows(ctx context.Context, tableName string, policyType PolicyType, rows []map[string]interface{}, user string, roles []string) ([]map[string]interface{}, error) {
	if !m.IsEnabled(tableName) {
		return rows, nil
	}

	filtered := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		allowed, err := m.CheckAccess(ctx, tableName, policyType, row, user, roles)
		if err != nil {
			return nil, err
		}
		if allowed {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

// EnablePolicy enables a policy
func (m *Manager) EnablePolicy(tableName, policyName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.policyKey(tableName, policyName)
	policy, ok := m.policies[key]
	if !ok {
		return ErrPolicyNotFound
	}
	policy.Enabled = true
	return nil
}

// DisablePolicy disables a policy
func (m *Manager) DisablePolicy(tableName, policyName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := m.policyKey(tableName, policyName)
	policy, ok := m.policies[key]
	if !ok {
		return ErrPolicyNotFound
	}
	policy.Enabled = false
	return nil
}

// ListPolicies returns all policies
func (m *Manager) ListPolicies() []*Policy {
	m.mu.RLock()
	defer m.mu.RUnlock()

	policies := make([]*Policy, 0, len(m.policies))
	for _, p := range m.policies {
		policy := *p
		policies = append(policies, &policy)
	}
	return policies
}

// SerializePolicies serializes all policies to JSON
func (m *Manager) SerializePolicies() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return json.Marshal(m.policies)
}

// DeserializePolicies loads policies from JSON
func (m *Manager) DeserializePolicies(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var policies map[string]*Policy
	if err := json.Unmarshal(data, &policies); err != nil {
		return err
	}

	m.policies = policies

	// Rebuild indexes
	m.tablePolicies = make(map[string][]string)
	m.enabledTables = make(map[string]bool)

	for key, policy := range policies {
		m.tablePolicies[policy.TableName] = append(m.tablePolicies[policy.TableName], policy.Name)
		if policy.Enabled {
			m.enabledTables[policy.TableName] = true
		}

		// Recompile expression
		if err := m.compilePolicy(policy); err != nil {
			// Log error but continue
			delete(m.compiledExprs, key)
		}
	}

	return nil
}

func (m *Manager) policyKey(tableName, policyName string) string {
	return strings.ToLower(tableName) + ":" + strings.ToLower(policyName)
}

// compilePolicy compiles a policy expression
// This is a simplified version - real implementation would parse SQL
func (m *Manager) compilePolicy(policy *Policy) error {
	key := m.policyKey(policy.TableName, policy.Name)

	// Default expression that allows all
	expr := func(ctx context.Context, row map[string]interface{}) (bool, error) {
		return true, nil
	}

	// If expression is provided, try to parse it
	if policy.Expression != "" {
		compiledExpr, err := m.parseExpression(policy.Expression)
		if err != nil {
			return err
		}
		expr = compiledExpr
	}

	m.compiledExprs[key] = expr
	return nil
}

// parseExpression parses a SQL expression and returns an evaluator
// This is a simplified placeholder - real implementation would have full SQL parser
func (m *Manager) parseExpression(expr string) (PolicyExpr, error) {
	// Simple expressions for common patterns
	expr = strings.TrimSpace(strings.ToLower(expr))

	// user_id = current_user
	if strings.Contains(expr, "current_user") {
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			user := ctx.Value("user")
			if user == nil {
				return false, nil
			}
			userStr, ok := user.(string)
			if !ok {
				return false, nil
			}

			// Check if row has user_id column
			if rowUserID, ok := row["user_id"]; ok {
				return fmt.Sprintf("%v", rowUserID) == userStr, nil
			}
			return false, nil
		}, nil
	}

	// tenant_id = current_tenant
	if strings.Contains(expr, "current_tenant") {
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			tenant := ctx.Value("tenant")
			if tenant == nil {
				return false, nil
			}

			if rowTenantID, ok := row["tenant_id"]; ok {
				return fmt.Sprintf("%v", rowTenantID) == fmt.Sprintf("%v", tenant), nil
			}
			return false, nil
		}, nil
	}

	// Default: allow all
	return func(ctx context.Context, row map[string]interface{}) (bool, error) {
		return true, nil
	}, nil
}

// ForceRow indicates that RLS should be applied even for table owners
func (m *Manager) ForceRow(tableName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if any policy has force_row_security
	tableName = strings.ToLower(tableName)
	policyNames := m.tablePolicies[tableName]

	for _, name := range policyNames {
		key := m.policyKey(tableName, name)
		if policy, ok := m.policies[key]; ok {
			if force, ok := policy.Metadata["force_row_security"]; ok {
				if b, ok := force.(bool); ok && b {
					return true
				}
			}
		}
	}
	return false
}
