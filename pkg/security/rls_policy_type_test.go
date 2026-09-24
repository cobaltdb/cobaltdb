package security

import (
	"errors"
	"testing"
)

// TestCreatePolicyRejectsUnknownPolicyType pins the contract that a policy
// with an out-of-range PolicyType is rejected at creation time.
//
// Regression: validatePolicyDefinition validated name/table/principals/
// metadata but never range-checked policy.Type; CreatePolicy accepted e.g.
// PolicyType(99) and checkAccessLocked's applicability filter
// (Type != PolicyAll && Type != policyType -> skip) then ignored the policy
// forever — a RESTRICTIVE policy failed open on its restriction.
func TestCreatePolicyRejectsUnknownPolicyType(t *testing.T) {
	m := NewManager()
	m.EnableTable("docs")

	for _, bad := range []PolicyType{PolicyType(99), PolicyType(-1)} {
		err := m.CreatePolicy(&Policy{
			Name: "bad_type", TableName: "docs", Type: bad,
			Restrictive: true, Expression: `tenant_id = 't1'`,
		})
		if !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("CreatePolicy with PolicyType %d: err = %v, want ErrInvalidPolicy", bad, err)
		}
	}

	// Every documented type stays accepted.
	valid := []PolicyType{PolicySelect, PolicyInsert, PolicyUpdate, PolicyDelete, PolicyAll}
	for _, pt := range valid {
		if err := m.CreatePolicy(&Policy{
			Name: "valid_type", TableName: "docs", Type: pt,
		}); err != nil {
			t.Fatalf("CreatePolicy with valid type %v rejected: %v", pt, err)
		}
		// Drop it again so the next iteration starts clean (same name).
		if err := m.DropPolicy("docs", "valid_type"); err != nil {
			t.Fatalf("DropPolicy: %v", err)
		}
	}
}

// TestDeserializeRejectsUnknownPolicyType pins the same validation on the
// persistence path: a serialized policy with an out-of-range type must be
// rejected instead of being loaded as a silently inert policy.
func TestDeserializeRejectsUnknownPolicyType(t *testing.T) {
	m := NewManager()
	blob := []byte(`{"policies":{"docs:bad":{"name":"bad","table_name":"docs","type":99,` +
		`"expression":"tenant_id = 't1'","enabled":true}}}`)
	if err := m.DeserializePolicies(blob); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("DeserializePolicies with type 99: err = %v, want ErrInvalidPolicy", err)
	}
}
