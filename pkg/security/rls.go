// Package security provides row-level security (RLS) for CobaltDB
package security

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
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
)

// Typed context keys to avoid collisions with other packages
type rlsUserKey struct{}
type rlsTenantKey struct{}
type rlsRoleKey struct{}
type rlsSessionUserKey struct{}

// RLSUserKey is the context key for the current user in RLS checks
var RLSUserKey = rlsUserKey{}

// RLSTenantKey is the context key for the current tenant in RLS checks
var RLSTenantKey = rlsTenantKey{}

// RLSRoleKey is the context key for the current role in RLS checks
var RLSRoleKey = rlsRoleKey{}

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
	Name       string                 `json:"name"`
	TableName  string                 `json:"table_name"`
	Type       PolicyType             `json:"type"`
	Expression string                 `json:"expression"` // SQL expression
	Users      []string               `json:"users"`      // Apply to specific users (empty = all)
	Roles      []string               `json:"roles"`      // Apply to specific roles
	Enabled    bool                   `json:"enabled"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// Manager manages row-level security policies
type Manager struct {
	policies      map[string]*Policy    // key: "tableName:policyName"
	tablePolicies map[string][]string   // key: tableName, value: []policyNames
	enabledTables map[string]bool       // Tables with RLS enabled
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

	// Compile expression
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
// Supports: =, !=, <>, <, >, <=, >=, AND, OR, NOT, IN, LIKE, IS NULL, IS NOT NULL
func (m *Manager) parseExpression(expr string) (PolicyExpr, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			return true, nil
		}, nil
	}

	upperExpr := strings.ToUpper(expr)

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
	upperExpr := strings.ToUpper(expr)
	upperOp := strings.ToUpper(op)
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
	upperExpr := strings.ToUpper(expr)

	// Check for IS NULL / IS NOT NULL
	if nullExpr := parseNullCheck(expr); nullExpr != nil {
		return nullExpr, nil
	}

	// Check for IN operator
	if inExpr := parseInOperator(expr); inExpr != nil {
		return inExpr, nil
	}

	// Check for LIKE operator
	if likeExpr := parseLikeOperator(expr); likeExpr != nil {
		return likeExpr, nil
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
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			val, ok := row[colName]
			if !ok {
				val = row[strings.ToLower(colName)]
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
	upperExpr = strings.ToUpper(expr)
	switch upperExpr {
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
	upperExpr := strings.ToUpper(expr)

	// Check for operators
	if strings.Contains(expr, "=") || strings.Contains(expr, "<") || strings.Contains(expr, ">") {
		return false
	}

	// Check for SQL keywords
	switch upperExpr {
	case "TRUE", "FALSE", "NULL", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS":
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
		leftStr := fmt.Sprintf("%v", leftVal)
		rightStr := fmt.Sprintf("%v", rightVal)

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
	upperName := strings.ToUpper(name)

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
	upperName := strings.ToUpper(strings.TrimSpace(name))

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
	if val, ok := row[strings.ToLower(name)]; ok {
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
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			val, ok := row[columnName]
			if !ok {
				val = row[strings.ToLower(columnName)]
			}
			return val != nil, nil
		}
	}

	// IS NULL
	if idx := strings.LastIndex(exprUpper, " IS NULL"); idx > 0 {
		columnName := strings.TrimSpace(expr[:idx])
		return func(ctx context.Context, row map[string]interface{}) (bool, error) {
			val, ok := row[columnName]
			if !ok {
				val = row[strings.ToLower(columnName)]
			}
			return val == nil, nil
		}
	}

	return nil
}

// parseInOperator parses IN operator expressions
func parseInOperator(expr string) PolicyExpr {
	expr = strings.TrimSpace(expr)

	// Match pattern: column IN (value1, value2, ...)
	inRegex := regexp.MustCompile(`(?i)^(.+?)\s+IN\s*\((.+?)\)$`)
	matches := inRegex.FindStringSubmatch(expr)
	if len(matches) != 3 {
		return nil
	}

	columnName := strings.TrimSpace(matches[1])
	valuesStr := matches[2]

	// Parse values
	values := parseValueList(valuesStr)

	return func(ctx context.Context, row map[string]interface{}) (bool, error) {
		rowValue, ok := row[columnName]
		if !ok {
			rowValue = row[strings.ToLower(columnName)]
		}

		rowStr := fmt.Sprintf("%v", rowValue)
		for _, v := range values {
			if rowStr == v {
				return true, nil
			}
		}
		return false, nil
	}
}

// parseLikeOperator parses LIKE operator expressions
func parseLikeOperator(expr string) PolicyExpr {
	expr = strings.TrimSpace(expr)

	// Match pattern: column LIKE 'pattern'
	likeRegex := regexp.MustCompile(`(?i)^(.+?)\s+LIKE\s+['"](.+?)['"]$`)
	matches := likeRegex.FindStringSubmatch(expr)
	if len(matches) != 3 {
		return nil
	}

	columnName := strings.TrimSpace(matches[1])
	pattern := matches[2]

	// Convert SQL LIKE pattern to regex
	regexPattern := likeToRegex(pattern)
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return nil
	}

	return func(ctx context.Context, row map[string]interface{}) (bool, error) {
		rowValue, ok := row[columnName]
		if !ok {
			rowValue = row[strings.ToLower(columnName)]
		}

		return re.MatchString(fmt.Sprintf("%v", rowValue)), nil
	}
}

// Helper functions

func splitLogical(expr, separator string) []string {
	exprUpper := strings.ToUpper(expr)
	sepUpper := strings.ToUpper(separator)

	idx := strings.Index(exprUpper, sepUpper)
	if idx < 0 {
		return nil
	}

	return []string{
		strings.TrimSpace(expr[:idx]),
		strings.TrimSpace(expr[idx+len(separator):]),
	}
}

func splitByOperator(expr, op string) []string {
	idx := strings.Index(strings.ToUpper(expr), op)
	if idx < 0 {
		return nil
	}

	return []string{
		strings.TrimSpace(expr[:idx]),
		strings.TrimSpace(expr[idx+len(op):]),
	}
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

func extractColumnName(expr, contextFunc string) string {
	expr = strings.ToLower(strings.TrimSpace(expr))
	contextFunc = strings.ToLower(contextFunc)

	// Remove everything after the context function
	if idx := strings.Index(expr, contextFunc); idx > 0 {
		return strings.TrimSpace(expr[:idx])
	}
	return ""
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
