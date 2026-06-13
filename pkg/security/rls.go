// Package security provides row-level security (RLS) for CobaltDB
package security

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrPolicyNotFound      = errors.New("security policy not found")
	ErrPolicyAlreadyExists = errors.New("security policy already exists")
	ErrInvalidPolicy       = errors.New("invalid security policy")
	ErrRLSNotEnabled       = errors.New("row-level security not enabled for table")
	ErrInvalidExpression   = errors.New("invalid policy expression")

	// Pre-compiled regex patterns for performance
	inRegex      = regexp.MustCompile(`(?i)^(.+?)\s+(NOT\s+)?IN\s*\((.+?)\)$`)
	likeRegex    = regexp.MustCompile(`(?i)^(.+?)\s+(NOT\s+)?LIKE\s+['"](.+?)['"]$`)
	betweenRegex = regexp.MustCompile(`(?i)^(.+?)\s+(NOT\s+)?BETWEEN\s+(.+?)\s+AND\s+(.+?)$`)
)

const (
	maxSerializedPoliciesBytes = 1 << 20
	maxSerializedPolicyCount   = 10000
	maxPolicyExpressionBytes   = 16 << 10
	maxPolicyLikePatternBytes  = 4 << 10
	maxPolicyInListValues      = 1024
	maxPolicyIdentifierBytes   = 256
	maxPolicyPrincipals        = 1024
	maxPolicyPrincipalBytes    = 256
	maxPolicyMetadataBytes     = 64 << 10
)

// toUpperFast returns an uppercased copy of s only if s contains lowercase
// letters. This avoids an allocation when s is already uppercase.
func toUpperFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			return strings.ToUpper(s)
		}
	}
	return s
}

// toLowerFast returns a lowercased copy of s only if s contains uppercase
// letters. This avoids an allocation when s is already lowercase.
func toLowerFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return strings.ToLower(s)
		}
	}
	return s
}

// Typed context keys to avoid collisions with other packages
type rlsUserKey struct{}
type rlsTenantKey struct{}
type rlsRoleKey struct{}
type rlsRolesKey struct{}
type rlsSessionUserKey struct{}

// RLSUserKey is the context key for the current user in RLS checks
var RLSUserKey = rlsUserKey{}

// RLSTenantKey is the context key for the current tenant in RLS checks
var RLSTenantKey = rlsTenantKey{}

// RLSRoleKey is the context key for the current role (single string) in RLS checks
var RLSRoleKey = rlsRoleKey{}

// RLSRolesKey is the context key for the list of roles ([]string) used by
// catalog-level row filtering. Distinct from RLSRoleKey which is consumed by
// SQL-level evaluation (e.g. CURRENT_ROLE()) and carries a single role.
var RLSRolesKey = rlsRolesKey{}

// RLSSessionUserKey is the context key for the session user in RLS checks
var RLSSessionUserKey = rlsSessionUserKey{}

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
	Name            string                 `json:"name"`
	TableName       string                 `json:"table_name"`
	Type            PolicyType             `json:"type"`
	Expression      string                 `json:"expression"`       // USING expression
	CheckExpression string                 `json:"check_expression"` // WITH CHECK expression
	Restrictive     bool                   `json:"restrictive,omitempty"`
	Users           []string               `json:"users"` // Apply to specific users (empty = all)
	Roles           []string               `json:"roles"` // Apply to specific roles
	Enabled         bool                   `json:"enabled"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// Manager manages row-level security policies
type Manager struct {
	policies           map[string]*Policy    // key: "tableName:policyName"
	tablePolicies      map[string][]string   // key: tableName, value: []policyNames
	enabledTables      map[string]bool       // Tables with RLS enabled
	compiledExprs      map[string]PolicyExpr // Compiled USING expressions
	compiledCheckExprs map[string]PolicyExpr // Compiled WITH CHECK expressions
	mu                 sync.RWMutex
}

// NewManager creates a new RLS manager
func NewManager() *Manager {
	return &Manager{
		policies:           make(map[string]*Policy),
		tablePolicies:      make(map[string][]string),
		enabledTables:      make(map[string]bool),
		compiledExprs:      make(map[string]PolicyExpr),
		compiledCheckExprs: make(map[string]PolicyExpr),
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

// ListEnabledTables returns all tables with row-level security enabled.
func (m *Manager) ListEnabledTables() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tables := make([]string, 0, len(m.enabledTables))
	for tableName, enabled := range m.enabledTables {
		if enabled {
			tables = append(tables, tableName)
		}
	}
	sort.Strings(tables)
	return tables
}

// CreatePolicy creates a new security policy
func (m *Manager) CreatePolicy(policy *Policy) error {
	if policy == nil || policy.Name == "" || policy.TableName == "" {
		return ErrInvalidPolicy
	}
	if err := validatePolicyDefinition(policy); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	storedPolicy := clonePolicy(policy)
	storedPolicy.TableName = strings.ToLower(storedPolicy.TableName)
	storedPolicy.Name = strings.ToLower(storedPolicy.Name)
	if storedPolicy.Metadata == nil {
		storedPolicy.Metadata = make(map[string]interface{})
	}
	storedPolicy.Enabled = true

	key := m.policyKey(storedPolicy.TableName, storedPolicy.Name)
	if _, exists := m.policies[key]; exists {
		return ErrPolicyAlreadyExists
	}

	if err := m.compilePolicy(storedPolicy); err != nil {
		delete(m.compiledExprs, key)
		delete(m.compiledCheckExprs, key)
		return fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
	}

	m.policies[key] = storedPolicy
	m.tablePolicies[storedPolicy.TableName] = append(m.tablePolicies[storedPolicy.TableName], storedPolicy.Name)
	m.enabledTables[storedPolicy.TableName] = true

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
	delete(m.compiledCheckExprs, key)

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

	return clonePolicy(policy), nil
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
			policies = append(policies, clonePolicy(policy))
		}
	}
	return policies
}

// CheckAccess checks if a user can access a row based on policies
func (m *Manager) CheckAccess(ctx context.Context, tableName string, policyType PolicyType, row map[string]interface{}, user string, roles []string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkAccessLocked(ctx, tableName, policyType, row, user, roles, false)
}

// CheckAccessWithCheck checks a row against WITH CHECK expressions when a policy
// defines one, falling back to USING expressions for backward compatibility.
func (m *Manager) CheckAccessWithCheck(ctx context.Context, tableName string, policyType PolicyType, row map[string]interface{}, user string, roles []string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.checkAccessLocked(ctx, tableName, policyType, row, user, roles, true)
}

func (m *Manager) checkAccessLocked(ctx context.Context, tableName string, policyType PolicyType, row map[string]interface{}, user string, roles []string, useCheck bool) (bool, error) {
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

	foundPermissive := false
	allowedPermissive := false

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
				if strings.EqualFold(policyRole, "PUBLIC") {
					found = true
					break
				}
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
		if useCheck && policy.CheckExpression != "" {
			expr, ok = m.compiledCheckExprs[key]
		}
		if !ok {
			continue
		}

		allowed, err := expr(ctx, row)
		if err != nil {
			return false, err
		}
		if policy.Restrictive {
			if !allowed {
				return false, nil
			}
			continue
		}

		foundPermissive = true
		if allowed {
			allowedPermissive = true
		}
	}

	// Restrictive policies only narrow access granted by permissive policies.
	if !foundPermissive {
		return false, nil
	}
	return allowedPermissive, nil
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
		policies = append(policies, clonePolicy(p))
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

	if len(data) > maxSerializedPoliciesBytes {
		return fmt.Errorf("%w: serialized policies too large: %d bytes", ErrInvalidPolicy, len(data))
	}

	var policies map[string]*Policy
	if err := json.Unmarshal(data, &policies); err != nil {
		return err
	}
	if len(policies) > maxSerializedPolicyCount {
		return fmt.Errorf("%w: too many policies: %d", ErrInvalidPolicy, len(policies))
	}

	stagedPolicies := make(map[string]*Policy, len(policies))
	stagedTablePolicies := make(map[string][]string)
	stagedEnabledTables := make(map[string]bool)
	stagedCompiledExprs := make(map[string]PolicyExpr)
	stagedCompiledCheckExprs := make(map[string]PolicyExpr)

	for key, policy := range policies {
		normalized, normalizedKey, err := normalizeDeserializedPolicy(key, policy)
		if err != nil {
			return err
		}
		if _, exists := stagedPolicies[normalizedKey]; exists {
			return fmt.Errorf("%w: duplicate policy %q", ErrInvalidPolicy, normalizedKey)
		}

		stagedPolicies[normalizedKey] = normalized
		stagedTablePolicies[normalized.TableName] = append(stagedTablePolicies[normalized.TableName], normalized.Name)
		if policy.Enabled {
			stagedEnabledTables[normalized.TableName] = true
		}

		// Recompile expression
		if err := compilePolicyInto(normalized, stagedCompiledExprs, stagedCompiledCheckExprs, m.parseExpression); err != nil {
			// Log error but continue
			delete(stagedCompiledExprs, normalizedKey)
			delete(stagedCompiledCheckExprs, normalizedKey)
		}
	}

	m.policies = stagedPolicies
	m.tablePolicies = stagedTablePolicies
	m.enabledTables = stagedEnabledTables
	m.compiledExprs = stagedCompiledExprs
	m.compiledCheckExprs = stagedCompiledCheckExprs
	return nil
}

func normalizeDeserializedPolicy(key string, policy *Policy) (*Policy, string, error) {
	if policy == nil {
		return nil, "", fmt.Errorf("%w: nil policy for key %q", ErrInvalidPolicy, key)
	}

	normalized := clonePolicy(policy)
	if normalized.TableName == "" || normalized.Name == "" {
		tableName, policyName, ok := strings.Cut(key, ":")
		if !ok {
			return nil, "", fmt.Errorf("%w: policy key %q must be table:policy", ErrInvalidPolicy, key)
		}
		if normalized.TableName == "" {
			normalized.TableName = tableName
		}
		if normalized.Name == "" {
			normalized.Name = policyName
		}
	}
	normalized.TableName = strings.ToLower(strings.TrimSpace(normalized.TableName))
	normalized.Name = strings.ToLower(strings.TrimSpace(normalized.Name))
	if normalized.TableName == "" || normalized.Name == "" {
		return nil, "", ErrInvalidPolicy
	}
	if err := validatePolicyDefinition(normalized); err != nil {
		return nil, "", err
	}
	if normalized.Metadata == nil {
		normalized.Metadata = make(map[string]interface{})
	}
	return normalized, normalized.TableName + ":" + normalized.Name, nil
}

func validatePolicyDefinition(policy *Policy) error {
	if policy == nil {
		return ErrInvalidPolicy
	}
	if strings.TrimSpace(policy.Name) == "" || strings.TrimSpace(policy.TableName) == "" {
		return ErrInvalidPolicy
	}
	if len(policy.Name) > maxPolicyIdentifierBytes || len(policy.TableName) > maxPolicyIdentifierBytes {
		return fmt.Errorf("%w: policy identifier too large", ErrInvalidPolicy)
	}
	if len(policy.Users) > maxPolicyPrincipals || len(policy.Roles) > maxPolicyPrincipals {
		return fmt.Errorf("%w: too many policy principals", ErrInvalidPolicy)
	}
	if err := validatePolicyPrincipals(policy.Users); err != nil {
		return err
	}
	if err := validatePolicyPrincipals(policy.Roles); err != nil {
		return err
	}
	if policy.Metadata != nil {
		encoded, err := json.Marshal(policy.Metadata)
		if err != nil {
			return fmt.Errorf("%w: invalid policy metadata: %v", ErrInvalidPolicy, err)
		}
		if len(encoded) > maxPolicyMetadataBytes {
			return fmt.Errorf("%w: policy metadata too large: %d bytes", ErrInvalidPolicy, len(encoded))
		}
	}
	return nil
}

func validatePolicyPrincipals(values []string) error {
	for _, value := range values {
		if strings.TrimSpace(value) == "" || len(value) > maxPolicyPrincipalBytes {
			return ErrInvalidPolicy
		}
	}
	return nil
}

func (m *Manager) policyKey(tableName, policyName string) string {
	return strings.ToLower(tableName) + ":" + strings.ToLower(policyName)
}

func clonePolicy(policy *Policy) *Policy {
	if policy == nil {
		return nil
	}
	cloned := *policy
	cloned.Users = cloneStringSlice(policy.Users)
	cloned.Roles = cloneStringSlice(policy.Roles)
	cloned.Metadata = cloneMetadata(policy.Metadata)
	return &cloned
}

func cloneStringSlice(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneMetadata(metadata map[string]interface{}) map[string]interface{} {
	if metadata == nil {
		return nil
	}
	cloned := make(map[string]interface{}, len(metadata))
	for key, value := range metadata {
		cloned[key] = cloneMetadataValue(value)
	}
	return cloned
}

func cloneMetadataValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		return cloneMetadata(typed)
	case []interface{}:
		cloned := make([]interface{}, len(typed))
		for i, item := range typed {
			cloned[i] = cloneMetadataValue(item)
		}
		return cloned
	case []string:
		return cloneStringSlice(typed)
	case []byte:
		return append([]byte(nil), typed...)
	case map[string]string:
		return cloneStringStringMap(typed)
	case []map[string]interface{}:
		cloned := make([]map[string]interface{}, len(typed))
		for i, item := range typed {
			cloned[i] = cloneMetadata(item)
		}
		return cloned
	default:
		return typed
	}
}

func cloneStringStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

// compilePolicy compiles a policy expression
func (m *Manager) compilePolicy(policy *Policy) error {
	return compilePolicyInto(policy, m.compiledExprs, m.compiledCheckExprs, m.parseExpression)
}

func compilePolicyInto(policy *Policy, compiledExprs, compiledCheckExprs map[string]PolicyExpr, parse func(string) (PolicyExpr, error)) error {
	key := strings.ToLower(policy.TableName) + ":" + strings.ToLower(policy.Name)

	// Default expression that allows all
	expr := func(ctx context.Context, row map[string]interface{}) (bool, error) {
		return true, nil
	}
	checkExpr := expr

	// If expression is provided, try to parse it
	if policy.Expression != "" {
		compiledExpr, err := parse(policy.Expression)
		if err != nil {
			return err
		}
		expr = compiledExpr
	}
	if policy.CheckExpression != "" {
		compiledExpr, err := parse(policy.CheckExpression)
		if err != nil {
			return err
		}
		checkExpr = compiledExpr
	}

	compiledExprs[key] = expr
	compiledCheckExprs[key] = checkExpr
	return nil
}

// parseExpression parses a SQL expression and returns an evaluator
// Supports: =, !=, <>, <, >, <=, >=, AND, OR, NOT, IN, NOT IN,
// LIKE, NOT LIKE, BETWEEN, NOT BETWEEN, IS NULL, IS NOT NULL
func (m *Manager) parseExpression(expr string) (PolicyExpr, error) {
	expr = strings.TrimSpace(expr)
	if len(expr) > maxPolicyExpressionBytes {
		return nil, fmt.Errorf("%w: policy expression too large: %d bytes", ErrInvalidExpression, len(expr))
	}
	if expr == "" {
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			return true, nil
		}, nil
	}

	upperExpr := toUpperFast(expr)

	// Handle boolean literals first
	if upperExpr == "TRUE" {
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			return true, nil
		}, nil
	}
	if upperExpr == "FALSE" {
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			return false, nil
		}, nil
	}

	// Handle NOT operator - must check for "NOT " followed by something
	if strings.HasPrefix(upperExpr, "NOT ") {
		innerExpr, err := m.parseExpression(expr[4:])
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			result, err := innerExpr(ctx, row)
			if err != nil {
				return false, err
			}
			return !result, nil
		}, nil
	}

	// Handle parentheses - find matching pair and strip them
	if strings.HasPrefix(expr, "(") {
		// Find the matching closing parenthesis
		depth := 1
		endIdx := -1
		for i := 1; i < len(expr); i++ {
			if expr[i] == '(' {
				depth++
			} else if expr[i] == ')' {
				depth--
				if depth == 0 {
					endIdx = i
					break
				}
			}
		}
		// If the entire expression is wrapped in parentheses
		if endIdx == len(expr)-1 {
			return m.parseExpression(expr[1 : len(expr)-1])
		}
	}

	// Try to parse complex expressions (AND/OR with proper precedence)
	parsedExpr, err := m.parseComplexExpression(expr)
	if err == nil {
		return parsedExpr, nil
	}

	// Fall back to simple expression parsing
	return m.parseSimpleExpression(expr)
}

// parseComplexExpression handles AND/OR combinations with proper precedence and parentheses
func (m *Manager) parseComplexExpression(expr string) (PolicyExpr, error) {
	expr = strings.TrimSpace(expr)

	// Find top-level AND/OR (not inside parentheses)
	andIdx := findTopLevelOperator(expr, " AND ")
	if andIdx >= 0 {
		leftExpr, err := m.parseExpression(expr[:andIdx])
		if err != nil {
			return nil, err
		}
		rightExpr, err := m.parseExpression(expr[andIdx+5:])
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			left, err := leftExpr(ctx, row)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return rightExpr(ctx, row)
		}, nil
	}

	orIdx := findTopLevelOperator(expr, " OR ")
	if orIdx >= 0 {
		leftExpr, err := m.parseExpression(expr[:orIdx])
		if err != nil {
			return nil, err
		}
		rightExpr, err := m.parseExpression(expr[orIdx+4:])
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			left, err := leftExpr(ctx, row)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return rightExpr(ctx, row)
		}, nil
	}

	return nil, fmt.Errorf("could not parse complex expression")
}

// findTopLevelOperator finds an operator at the top level (not inside parentheses)
func findTopLevelOperator(expr, op string) int {
	depth := 0
	upperExpr := toUpperFast(expr)
	upperOp := toUpperFast(op)
	for i := 0; i <= len(upperExpr)-len(upperOp); i++ {
		if expr[i] == '(' {
			depth++
		} else if expr[i] == ')' {
			depth--
		} else if depth == 0 && strings.HasPrefix(upperExpr[i:], upperOp) {
			return i
		}
	}
	return -1
}

// parseSimpleExpression handles simple comparison expressions
func (m *Manager) parseSimpleExpression(expr string) (PolicyExpr, error) {
	expr = strings.TrimSpace(expr)

	// Check for IS NULL / IS NOT NULL
	if nullExpr := parseNullCheck(expr); nullExpr != nil {
		return nullExpr, nil
	}

	// Check for IN operator
	if inExpr, err := parseInOperator(expr); err != nil {
		return nil, err
	} else if inExpr != nil {
		return inExpr, nil
	}

	// Check for LIKE operator
	if likeExpr, err := parseLikeOperator(expr); err != nil {
		return nil, err
	} else if likeExpr != nil {
		return likeExpr, nil
	}

	// Check for BETWEEN operator
	if betweenExpr := m.parseBetweenOperator(expr); betweenExpr != nil {
		return betweenExpr, nil
	}

	// Parse comparison operators - check for >=, <=, <>, != first (before single char ops)
	operators := []string{"<=", ">=", "<>", "!=", "=", "<", ">"}
	for _, op := range operators {
		if idx := findTopLevelOperator(expr, op); idx >= 0 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(op):])
			return m.createComparisonEvaluator(left, op, right), nil
		}
	}

	// Check if this is a bare column name (treat as boolean)
	if isBareColumn(expr) {
		colName := strings.TrimSpace(expr)
		lowerCol := toLowerFast(colName)
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			val, ok := row[colName]
			if !ok {
				val = row[lowerCol]
			}
			// Treat as boolean: non-nil and non-false values are true
			if val == nil {
				return false, nil
			}
			if b, ok := val.(bool); ok {
				return b, nil
			}
			// Non-nil values are truthy
			return true, nil
		}, nil
	}

	// Check for context functions (bare current_user, current_tenant, etc.)
	switch toUpperFast(expr) {
	case "CURRENT_USER", "CURRENT_USER()":
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			user := ctx.Value(RLSUserKey)
			return user != nil && user != "", nil
		}, nil
	case "CURRENT_TENANT", "CURRENT_TENANT()":
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			tenant := ctx.Value(RLSTenantKey)
			return tenant != nil && tenant != "", nil
		}, nil
	}

	return nil, fmt.Errorf("%w: unsupported expression: %s", ErrInvalidExpression, expr)
}

// isBareColumn checks if expr is a bare column name (not a comparison, not quoted, etc.)
func isBareColumn(expr string) bool {
	expr = strings.TrimSpace(expr)
	upperExpr := toUpperFast(expr)

	// Check for operators
	if strings.Contains(expr, "=") || strings.Contains(expr, "<") || strings.Contains(expr, ">") {
		return false
	}

	// Check for SQL keywords
	switch upperExpr {
	case "TRUE", "FALSE", "NULL", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS",
		"CURRENT_USER", "CURRENT_TENANT", "CURRENT_ROLE", "SESSION_USER":
		return false
	}

	// Must be alphanumeric with underscores (identifier pattern)
	for _, ch := range expr {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
			return false
		}
	}

	return len(expr) > 0
}

// createComparisonEvaluator creates an evaluator for comparison operations
func (m *Manager) createComparisonEvaluator(left, op, right string) PolicyExpr {
	// Normalize column name if it looks like a column reference
	leftCol := normalizeColumnName(left)

	// Check if this is a context expression like "user_id = current_user"
	isContextExpr := isContextFunction(right)

	return func(ctx context.Context, row map[string]interface{}) (bool, error) {
		var leftVal, rightVal interface{}

		if isContextExpr {
			// For context expressions, left is column, right is context value
			leftVal = m.getValueFromRow(leftCol, row)
			rightVal = m.getContextValue(right, ctx)
		} else {
			leftVal = m.getValue(left, ctx, row)
			rightVal = m.getValue(right, ctx, row)
		}

		// Handle NULL comparisons
		if leftVal == nil || rightVal == nil {
			switch op {
			case "=":
				return leftVal == rightVal, nil
			case "!=", "<>":
				return leftVal != rightVal, nil
			default:
				return false, nil
			}
		}

		// Try numeric comparison first
		leftNum, leftIsNum := ToFloat64(leftVal)
		rightNum, rightIsNum := ToFloat64(rightVal)

		if leftIsNum && rightIsNum {
			switch op {
			case "=":
				return leftNum == rightNum, nil
			case "!=", "<>":
				return leftNum != rightNum, nil
			case "<":
				return leftNum < rightNum, nil
			case ">":
				return leftNum > rightNum, nil
			case "<=":
				return leftNum <= rightNum, nil
			case ">=":
				return leftNum >= rightNum, nil
			}
		}

		// String comparison
		leftStr := valueToString(leftVal)
		rightStr := valueToString(rightVal)

		switch op {
		case "=":
			return leftStr == rightStr, nil
		case "!=", "<>":
			return leftStr != rightStr, nil
		case "<":
			return leftStr < rightStr, nil
		case ">":
			return leftStr > rightStr, nil
		case "<=":
			return leftStr <= rightStr, nil
		case ">=":
			return leftStr >= rightStr, nil
		}

		return false, nil
	}
}

func (m *Manager) parseBetweenOperator(expr string) PolicyExpr {
	expr = strings.TrimSpace(expr)
	matches := betweenRegex.FindStringSubmatch(expr)
	if len(matches) != 5 {
		return nil
	}

	valueExpr := strings.TrimSpace(matches[1])
	not := strings.TrimSpace(matches[2]) != ""
	lowerExpr := strings.TrimSpace(matches[3])
	upperExpr := strings.TrimSpace(matches[4])

	return func(ctx context.Context, row map[string]interface{}) (bool, error) {
		value := m.getValue(valueExpr, ctx, row)
		lower := m.getValue(lowerExpr, ctx, row)
		upper := m.getValue(upperExpr, ctx, row)
		if value == nil || lower == nil || upper == nil {
			return false, nil
		}

		var result bool
		valueNum, valueIsNum := ToFloat64(value)
		lowerNum, lowerIsNum := ToFloat64(lower)
		upperNum, upperIsNum := ToFloat64(upper)
		if valueIsNum && lowerIsNum && upperIsNum {
			result = valueNum >= lowerNum && valueNum <= upperNum
		} else {
			valueStr := valueToString(value)
			lowerStr := valueToString(lower)
			upperStr := valueToString(upper)
			result = valueStr >= lowerStr && valueStr <= upperStr
		}
		if not {
			return !result, nil
		}
		return result, nil
	}
}

// isContextFunction checks if expr is a context function like current_user
func isContextFunction(expr string) bool {
	upperExpr := strings.ToUpper(strings.TrimSpace(expr))
	return upperExpr == "CURRENT_USER" || upperExpr == "CURRENT_USER()" ||
		upperExpr == "CURRENT_TENANT" || upperExpr == "CURRENT_TENANT()" ||
		upperExpr == "CURRENT_ROLE" || upperExpr == "CURRENT_ROLE()" ||
		upperExpr == "SESSION_USER"
}

// normalizeColumnName normalizes a column name
func normalizeColumnName(name string) string {
	name = strings.TrimSpace(name)
	// Remove any trailing comparison operators or whitespace
	name = strings.TrimRight(name, "=<>")
	return strings.TrimSpace(name)
}

// getValue retrieves a value from context or row
func (m *Manager) getValue(name string, ctx context.Context, row map[string]interface{}) interface{} {
	name = strings.TrimSpace(name)
	upperName := toUpperFast(name)

	// Check for quoted strings
	if (strings.HasPrefix(name, "'") && strings.HasSuffix(name, "'")) ||
		(strings.HasPrefix(name, "\"") && strings.HasSuffix(name, "\"")) {
		return name[1 : len(name)-1]
	}

	// Check for numbers
	if num, err := strconv.ParseFloat(name, 64); err == nil {
		return num
	}

	// Check for boolean
	if upperName == "TRUE" {
		return true
	}
	if upperName == "FALSE" {
		return false
	}

	// Check for context variables
	if val := m.getContextValue(name, ctx); val != nil {
		return val
	}

	// Get from row
	return m.getValueFromRow(name, row)
}

// getContextValue retrieves a value from context
func (m *Manager) getContextValue(name string, ctx context.Context) interface{} {
	upperName := toUpperFast(strings.TrimSpace(name))

	switch upperName {
	case "CURRENT_USER", "CURRENT_USER()":
		return ctx.Value(RLSUserKey)
	case "CURRENT_TENANT", "CURRENT_TENANT()":
		return ctx.Value(RLSTenantKey)
	case "CURRENT_ROLE", "CURRENT_ROLE()":
		return ctx.Value(RLSRoleKey)
	case "SESSION_USER":
		if user := ctx.Value(RLSSessionUserKey); user != nil {
			return user
		}
		return ctx.Value(RLSUserKey)
	}
	return nil
}

// getValueFromRow retrieves a value from row
func (m *Manager) getValueFromRow(name string, row map[string]interface{}) interface{} {
	if val, ok := row[name]; ok {
		return val
	}
	// Try lowercase
	if val, ok := row[toLowerFast(name)]; ok {
		return val
	}
	return nil
}

// parseNullCheck parses IS NULL / IS NOT NULL expressions
func parseNullCheck(expr string) PolicyExpr {
	exprUpper := strings.ToUpper(strings.TrimSpace(expr))

	// IS NOT NULL
	if idx := strings.LastIndex(exprUpper, " IS NOT NULL"); idx > 0 {
		columnName := strings.TrimSpace(expr[:idx])
		lowerCol := toLowerFast(columnName)
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			val, ok := row[columnName]
			if !ok {
				val = row[lowerCol]
			}
			return val != nil, nil
		}
	}

	// IS NULL
	if idx := strings.LastIndex(exprUpper, " IS NULL"); idx > 0 {
		columnName := strings.TrimSpace(expr[:idx])
		lowerCol := toLowerFast(columnName)
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			val, ok := row[columnName]
			if !ok {
				val = row[lowerCol]
			}
			return val == nil, nil
		}
	}

	return nil
}

// parseInOperator parses IN operator expressions
func parseInOperator(expr string) (PolicyExpr, error) {
	expr = strings.TrimSpace(expr)

	// Match pattern: column [NOT] IN (value1, value2, ...)
	// Uses pre-compiled package-level regex
	matches := inRegex.FindStringSubmatch(expr)
	if len(matches) != 4 {
		return nil, nil
	}

	columnName := strings.TrimSpace(matches[1])
	lowerCol := toLowerFast(columnName)
	not := strings.TrimSpace(matches[2]) != ""
	valuesStr := matches[3]

	// Parse values
	values := parseValueList(valuesStr)
	if len(values) > maxPolicyInListValues {
		return nil, fmt.Errorf("%w: policy IN list has too many values: %d", ErrInvalidExpression, len(values))
	}

	return func(ctx context.Context, row map[string]interface{}) (bool, error) {
		rowValue, ok := row[columnName]
		if !ok {
			rowValue = row[lowerCol]
		}

		rowStr := valueToString(rowValue)
		found := false
		for _, v := range values {
			if rowStr == v {
				found = true
				break
			}
		}
		if not {
			return !found, nil
		}
		return found, nil
	}, nil
}

// parseLikeOperator parses LIKE operator expressions
func parseLikeOperator(expr string) (PolicyExpr, error) {
	expr = strings.TrimSpace(expr)

	// Match pattern: column [NOT] LIKE 'pattern'
	// Uses pre-compiled package-level regex
	matches := likeRegex.FindStringSubmatch(expr)
	if len(matches) != 4 {
		return nil, nil
	}

	columnName := strings.TrimSpace(matches[1])
	lowerCol := toLowerFast(columnName)
	not := strings.TrimSpace(matches[2]) != ""
	pattern := matches[3]
	if len(pattern) > maxPolicyLikePatternBytes {
		return nil, fmt.Errorf("%w: policy LIKE pattern too large: %d bytes", ErrInvalidExpression, len(pattern))
	}

	// Convert SQL LIKE pattern to regex
	regexPattern := likeToRegex(pattern)
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, row map[string]interface{}) (bool, error) {
		rowValue, ok := row[columnName]
		if !ok {
			rowValue = row[lowerCol]
		}

		matched := re.MatchString(valueToString(rowValue))
		if not {
			return !matched, nil
		}
		return matched, nil
	}, nil
}

// ToFloat64 converts a value to float64
func ToFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	case bool:
		if val {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func parseValueList(valuesStr string) []string {
	values := []string{}
	parts := strings.Split(valuesStr, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		// Remove quotes
		if (strings.HasPrefix(p, "'") && strings.HasSuffix(p, "'")) ||
			(strings.HasPrefix(p, "\"") && strings.HasSuffix(p, "\"")) {
			p = p[1 : len(p)-1]
		}
		values = append(values, p)
	}
	return values
}

func likeToRegex(pattern string) string {
	// Escape regex special characters except % and _
	result := regexp.QuoteMeta(pattern)
	// Replace SQL wildcards with regex equivalents
	result = strings.ReplaceAll(result, "%", ".*")
	result = strings.ReplaceAll(result, "_", ".")
	return "^" + result + "$"
}

// valueToString converts a value to a string without fmt.Sprintf reflection
// for common types. Used in per-row RLS evaluation hot paths.
func valueToString(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return valueToStringKey(val)
	}
}

// valueToStringKey is a local copy of catalog.ValueToStringKey to avoid import cycles.
func valueToStringKey(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
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
