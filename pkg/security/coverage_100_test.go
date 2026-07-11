package security

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestSecurityRemainingPolicyLifecyclePaths(t *testing.T) {
	m := NewManager()
	if err := m.CreatePolicy(nil); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("nil policy: %v", err)
	}
	if err := m.EnablePolicy("missing", "missing"); !errors.Is(err, ErrPolicyNotFound) {
		t.Fatalf("enable missing: %v", err)
	}
	if err := m.DisablePolicy("missing", "missing"); !errors.Is(err, ErrPolicyNotFound) {
		t.Fatalf("disable missing: %v", err)
	}

	p := &Policy{Name: "keep", TableName: "items", Expression: "TRUE"}
	if err := m.CreatePolicy(p); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.tablePolicies["items"] = append(m.tablePolicies["items"], "other")
	m.mu.Unlock()
	if err := m.DropPolicy("items", "keep"); err != nil {
		t.Fatal(err)
	}
	if got := m.tablePolicies["items"]; len(got) != 1 || got[0] != "other" {
		t.Fatalf("drop retained entries = %v", got)
	}
}

func TestSecurityRemainingAccessPaths(t *testing.T) {
	m := NewManager()
	m.EnableTable("docs")
	m.mu.Lock()
	m.tablePolicies["docs"] = []string{"missing", "disabled", "check", "noexpr"}
	m.policies["docs:disabled"] = &Policy{Name: "disabled", TableName: "docs", Enabled: false}
	m.policies["docs:check"] = &Policy{Name: "check", TableName: "docs", Type: PolicyAll, Enabled: true, CheckExpression: "TRUE"}
	m.compiledExprs["docs:check"] = func(context.Context, map[string]interface{}) (bool, error) { return true, nil }
	m.compiledCheckExprs["docs:check"] = func(context.Context, map[string]interface{}) (bool, error) { return true, nil }
	m.policies["docs:noexpr"] = &Policy{Name: "noexpr", TableName: "docs", Type: PolicyAll, Enabled: true}
	m.mu.Unlock()

	allowed, err := m.CheckAccessWithCheck(context.Background(), "docs", PolicySelect, nil, "u", nil)
	if err != nil || !allowed {
		t.Fatalf("check evaluator: allowed=%v err=%v", allowed, err)
	}

	boom := errors.New("boom")
	m.mu.Lock()
	m.tablePolicies["docs"] = []string{"error"}
	m.policies["docs:error"] = &Policy{Name: "error", TableName: "docs", Type: PolicyAll, Enabled: true}
	m.compiledExprs["docs:error"] = func(context.Context, map[string]interface{}) (bool, error) { return false, boom }
	m.mu.Unlock()
	if _, err := m.CheckAccess(context.Background(), "docs", PolicySelect, nil, "u", nil); !errors.Is(err, boom) {
		t.Fatalf("expression error = %v", err)
	}
	if _, err := m.FilterRows(context.Background(), "docs", PolicySelect, []map[string]interface{}{{}}, "u", nil); !errors.Is(err, boom) {
		t.Fatalf("filter error = %v", err)
	}
}

func TestSecurityRemainingDeserializeValidationPaths(t *testing.T) {
	m := NewManager()
	if err := m.DeserializePolicies([]byte("{")); err == nil {
		t.Fatal("expected malformed JSON error")
	}

	many := make(map[string]*Policy, maxSerializedPolicyCount+1)
	for i := 0; i <= maxSerializedPolicyCount; i++ {
		many[string(rune(i+1))] = nil
	}
	data, err := json.Marshal(many)
	if err != nil || len(data) > maxSerializedPoliciesBytes {
		t.Fatalf("count fixture: len=%d err=%v", len(data), err)
	}
	if err := m.DeserializePolicies(data); !errors.Is(err, ErrInvalidPolicy) || !strings.Contains(err.Error(), "too many") {
		t.Fatalf("too many policies: %v", err)
	}

	cases := []string{
		`{"bad": {}}`,
		`{"t:": {}}`,
		`{"T:P":{"name":"P","table_name":"T"},"t:p":{"name":"p","table_name":"t"}}`,
		`{"x":{"name":" ","table_name":" "}}`,
	}
	for _, input := range cases {
		if err := m.DeserializePolicies([]byte(input)); !errors.Is(err, ErrInvalidPolicy) {
			t.Errorf("DeserializePolicies(%s) = %v", input, err)
		}
	}

	normalized, key, err := normalizeDeserializedPolicy("Table:Policy", &Policy{})
	if err != nil || key != "table:policy" || normalized.Metadata == nil {
		t.Fatalf("normalize fallback: %#v %q %v", normalized, key, err)
	}
	if _, _, err := normalizeDeserializedPolicy(":", &Policy{}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("normalize empty key components: %v", err)
	}
	if _, _, err := normalizeDeserializedPolicy("ignored", &Policy{Name: strings.Repeat("x", maxPolicyIdentifierBytes+1), TableName: "t"}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("normalize invalid definition: %v", err)
	}
	if _, _, err := normalizeDeserializedPolicy("x", nil); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("normalize nil: %v", err)
	}
}

func TestSecurityRemainingDefinitionAndClonePaths(t *testing.T) {
	tooLong := strings.Repeat("x", maxPolicyIdentifierBytes+1)
	cases := []*Policy{
		nil,
		{Name: " ", TableName: "t"},
		{Name: tooLong, TableName: "t"},
		{Name: "p", TableName: "t", Users: make([]string, maxPolicyPrincipals+1)},
		{Name: "p", TableName: "t", Users: []string{""}},
		{Name: "p", TableName: "t", Roles: []string{""}},
		{Name: "p", TableName: "t", Metadata: map[string]interface{}{"bad": make(chan int)}},
		{Name: "p", TableName: "t", Metadata: map[string]interface{}{"big": strings.Repeat("x", maxPolicyMetadataBytes)}},
	}
	for i, p := range cases {
		if err := validatePolicyDefinition(p); !errors.Is(err, ErrInvalidPolicy) {
			t.Errorf("case %d: %v", i, err)
		}
	}

	if clonePolicy(nil) != nil || cloneStringStringMap(nil) != nil {
		t.Fatal("nil clones must remain nil")
	}
	metadata := map[string]interface{}{
		"map":       map[string]interface{}{"x": []byte("a")},
		"list":      []interface{}{[]string{"a"}},
		"strings":   []string{"a"},
		"bytes":     []byte("a"),
		"stringMap": map[string]string{"a": "b"},
		"maps":      []map[string]interface{}{{"a": 1}},
		"scalar":    42,
	}
	cloned := cloneMetadata(metadata)
	metadata["bytes"].([]byte)[0] = 'z'
	if string(cloned["bytes"].([]byte)) != "a" {
		t.Fatal("metadata byte slice aliases source")
	}
}

func TestSecurityRemainingCompilerAndEvaluatorPaths(t *testing.T) {
	m := NewManager()
	parseErr := errors.New("parse")
	parse := func(s string) (PolicyExpr, error) {
		if s == "bad" {
			return nil, parseErr
		}
		return func(context.Context, map[string]interface{}) (bool, error) { return s == "yes", nil }, nil
	}
	for _, p := range []*Policy{
		{Name: "p", TableName: "t", Expression: "bad"},
		{Name: "p", TableName: "t", Expression: "yes", CheckExpression: "bad"},
	} {
		if err := compilePolicyInto(p, map[string]PolicyExpr{}, map[string]PolicyExpr{}, parse); !errors.Is(err, parseErr) {
			t.Fatalf("compile error = %v", err)
		}
	}
	compiled, checks := map[string]PolicyExpr{}, map[string]PolicyExpr{}
	if err := compilePolicyInto(&Policy{Name: "p", TableName: "t", CheckExpression: "yes"}, compiled, checks, parse); err != nil {
		t.Fatal(err)
	}
	if ok, _ := checks["t:p"](context.Background(), nil); !ok {
		t.Fatal("compiled check expression returned false")
	}
	if allow, _ := m.parseExpression(""); func() bool { ok, _ := allow(context.Background(), nil); return !ok }() {
		t.Fatal("empty expression should allow")
	}
	if nested, err := m.parseExpression("((TRUE))"); err != nil {
		t.Fatal(err)
	} else if ok, _ := nested(context.Background(), nil); !ok {
		t.Fatal("nested parentheses should be true")
	}

	for _, input := range []string{"NOT !!!", "TRUE AND !!!", "!!! AND TRUE", "TRUE OR !!!", "!!! OR TRUE"} {
		if _, err := m.parseExpression(input); err == nil {
			t.Errorf("parseExpression(%q) succeeded", input)
		}
	}

	boom := errors.New("eval")
	bad := func(context.Context, map[string]interface{}) (bool, error) { return false, boom }
	truth := func(context.Context, map[string]interface{}) (bool, error) { return true, nil }
	falsity := func(context.Context, map[string]interface{}) (bool, error) { return false, nil }
	for name, fn := range map[string]PolicyExpr{
		"not error": notPolicyExpr(bad),
		"and left":  andPolicyExpr(bad, truth),
		"and right": andPolicyExpr(truth, bad),
		"or left":   orPolicyExpr(bad, truth),
		"or right":  orPolicyExpr(falsity, bad),
	} {
		if _, err := fn(context.Background(), nil); !errors.Is(err, boom) {
			t.Errorf("%s did not propagate error: %v", name, err)
		}
	}
	if got, _ := notPolicyExpr(truth)(context.Background(), nil); got {
		t.Fatal("NOT true should be false")
	}
	if got, _ := andPolicyExpr(falsity, bad)(context.Background(), nil); got {
		t.Fatal("false AND error should short-circuit false")
	}
	if got, _ := orPolicyExpr(truth, bad)(context.Background(), nil); !got {
		t.Fatal("true OR error should short-circuit true")
	}

	for _, tc := range []struct {
		op          string
		left, right interface{}
	}{
		{"<>", 1, 2}, {"<", 1, 2}, {">", 2, 1}, {"<=", 1, 1}, {">=", 1, 1},
		{"!=", "a", "b"}, {"<>", "a", "b"}, {"<", "a", "b"}, {">", "b", "a"}, {"<=", "a", "a"}, {">=", "a", "a"}, {"?", "a", "a"},
	} {
		fn := m.createComparisonEvaluator("left", tc.op, "right")
		_, _ = fn(context.Background(), map[string]interface{}{"left": tc.left, "right": tc.right})
	}

	bare, err := m.parseSimpleExpression("COL")
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := bare(context.Background(), map[string]interface{}{"col": true}); !ok {
		t.Fatal("bare column lowercase fallback")
	}

	between := m.parseBetweenOperator("x BETWEEN 1 AND 2")
	ok, _ := between(context.Background(), map[string]interface{}{"x": nil})
	if ok {
		t.Fatal("nil BETWEEN matched")
	}

	for _, expr := range []string{"COL IS NOT NULL", "COL IS NULL"} {
		fn := parseNullCheck(expr)
		_, _ = fn(context.Background(), map[string]interface{}{"col": 1})
	}
	in, _ := parseInOperator("COL IN ('x')")
	_, _ = in(context.Background(), map[string]interface{}{"col": "x"})
	like, _ := parseLikeOperator("COL LIKE 'x%'")
	_, _ = like(context.Background(), map[string]interface{}{"col": "xyz"})

	if f, ok := ToFloat64(int64(7)); !ok || f != 7 {
		t.Fatal("int64 conversion")
	}
	if got := valueToString(struct{ X int }{1}); got == "" {
		t.Fatal("fallback string")
	}
}
