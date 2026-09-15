package security

import (
	"context"
	"testing"
)

// TestRLSEnabledTablesSurvivePersistence is the regression test for the RLS
// persistence defect fixed in this round: SerializePolicies used to write only
// the policies map, and DeserializePolicies rebuilt the enabled-table set
// solely from policy.Enabled flags. Runtime deny-all states — an RLS-enabled
// table with zero policies, or with all of its policies disabled — were
// therefore not representable in the serialized form and silently degraded to
// allow-all across a persistence round-trip (fail-closed became fail-open).
func TestRLSEnabledTablesSurvivePersistence(t *testing.T) {
	ctx := context.Background()

	// Scenario 1: table enabled with NO policies — runtime deny-all.
	mgr := NewManager()
	mgr.EnableTable("tenant_data")

	denied, err := mgr.CheckAccess(ctx, "tenant_data", PolicySelect, map[string]interface{}{"id": 1}, "mallory", nil)
	if err != nil {
		t.Fatalf("pre-roundtrip CheckAccess error: %v", err)
	}
	if denied {
		t.Fatal("pre-roundtrip precondition broken: expected deny-all on an enabled table with no policies")
	}

	data, err := mgr.SerializePolicies()
	if err != nil {
		t.Fatalf("SerializePolicies: %v", err)
	}

	mgr2 := NewManager()
	if err := mgr2.DeserializePolicies(data); err != nil {
		t.Fatalf("DeserializePolicies: %v", err)
	}

	if !mgr2.IsEnabled("tenant_data") {
		t.Fatal("RLS-enabled table with no policies lost its enabled state across the persistence round-trip")
	}
	allowed, err := mgr2.CheckAccess(ctx, "tenant_data", PolicySelect, map[string]interface{}{"id": 1}, "mallory", nil)
	if err != nil {
		t.Fatalf("post-roundtrip CheckAccess error: %v", err)
	}
	if allowed {
		t.Fatal("CheckAccess flipped from deny to allow across the persistence round-trip")
	}

	// Scenario 2: enabled table whose only policy is disabled — runtime deny-all.
	mgr3 := NewManager()
	if err := mgr3.CreatePolicy(&Policy{
		Name:       "p",
		TableName:  "orders",
		Type:       PolicySelect,
		Expression: "TRUE",
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	if err := mgr3.DisablePolicy("orders", "p"); err != nil {
		t.Fatalf("DisablePolicy: %v", err)
	}

	denied3, err := mgr3.CheckAccess(ctx, "orders", PolicySelect, map[string]interface{}{"id": 1}, "mallory", nil)
	if err != nil {
		t.Fatalf("pre-roundtrip CheckAccess(orders) error: %v", err)
	}
	if denied3 {
		t.Fatal("pre-roundtrip precondition broken: expected deny when the only policy on an RLS-enabled table is disabled")
	}

	data3, err := mgr3.SerializePolicies()
	if err != nil {
		t.Fatalf("SerializePolicies(orders): %v", err)
	}

	mgr4 := NewManager()
	if err := mgr4.DeserializePolicies(data3); err != nil {
		t.Fatalf("DeserializePolicies(orders): %v", err)
	}

	if !mgr4.IsEnabled("orders") {
		t.Fatal("enabled-table state for a table whose only policy is disabled was lost across the persistence round-trip")
	}
	allowed4, err := mgr4.CheckAccess(ctx, "orders", PolicySelect, map[string]interface{}{"id": 1}, "mallory", nil)
	if err != nil {
		t.Fatalf("post-roundtrip CheckAccess(orders) error: %v", err)
	}
	if allowed4 {
		t.Fatal("orders flipped from deny to allow across the persistence round-trip")
	}
}

// TestDeserializePoliciesLegacyFormatDerivesEnabledTables pins the backward
// compatibility contract: legacy bare-map blobs (keys "table:policy") must
// still load, deriving the enabled-table set from enabled policies exactly as
// the pre-envelope implementation did.
func TestDeserializePoliciesLegacyFormatDerivesEnabledTables(t *testing.T) {
	mgr := NewManager()
	legacy := []byte(`{"orders:p1":{"name":"p1","tableName":"orders","type":0,"expression":"user_id = current_user","enabled":true}}`)
	if err := mgr.DeserializePolicies(legacy); err != nil {
		t.Fatalf("DeserializePolicies(legacy): %v", err)
	}
	if !mgr.IsEnabled("orders") {
		t.Fatal("legacy blob with an enabled policy should enable the policy's table")
	}
	if _, err := mgr.GetPolicy("orders", "p1"); err != nil {
		t.Fatalf("legacy policy not restored: %v", err)
	}

	// Disabled policy in a legacy blob: no enabled-table entry is derived, so
	// the table stays unprotected (legacy lossy behavior, kept for compat).
	mgr2 := NewManager()
	legacyDisabled := []byte(`{"orders:p1":{"name":"p1","tableName":"orders","type":0,"expression":"TRUE","enabled":false}}`)
	if err := mgr2.DeserializePolicies(legacyDisabled); err != nil {
		t.Fatalf("DeserializePolicies(legacy disabled): %v", err)
	}
	if mgr2.IsEnabled("orders") {
		t.Fatal("legacy blob with only disabled policies must not enable the table (legacy semantics)")
	}
}

// TestDeserializePoliciesEnabledTablesVerbatim pins the new-format semantics:
// the serialized enabled-table set is restored verbatim, so an enabled policy
// on a table that is NOT RLS-enabled (reachable at runtime via DisableTable)
// survives the round-trip with the table still unprotected.
func TestDeserializePoliciesEnabledTablesVerbatim(t *testing.T) {
	ctx := context.Background()

	mgr := NewManager()
	if err := mgr.CreatePolicy(&Policy{
		Name:       "p",
		TableName:  "audit_log",
		Type:       PolicySelect,
		Expression: "TRUE",
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	// DisableTable removes the table from the enabled set but leaves the
	// policy in place: policy exists, table not RLS-enabled, access allowed.
	mgr.DisableTable("audit_log")

	data, err := mgr.SerializePolicies()
	if err != nil {
		t.Fatalf("SerializePolicies: %v", err)
	}

	mgr2 := NewManager()
	if err := mgr2.DeserializePolicies(data); err != nil {
		t.Fatalf("DeserializePolicies: %v", err)
	}

	if mgr2.IsEnabled("audit_log") {
		t.Fatal("enabled-table set must be restored verbatim: audit_log was not RLS-enabled at serialization time")
	}
	if _, err := mgr2.GetPolicy("audit_log", "p"); err != nil {
		t.Fatalf("policy on non-enabled table not restored: %v", err)
	}
	allowed, err := mgr2.CheckAccess(ctx, "audit_log", PolicySelect, map[string]interface{}{"id": 1}, "mallory", nil)
	if err != nil {
		t.Fatalf("CheckAccess error: %v", err)
	}
	if !allowed {
		t.Fatal("RLS-disabled table must allow access even with an existing policy")
	}
}

// TestSerializePoliciesDeterministicOutput guards the envelope format: the
// serialized state must contain the policies and the enabled-table set, with
// deterministic ordering.
func TestSerializePoliciesDeterministicOutput(t *testing.T) {
	mgr := NewManager()
	mgr.EnableTable("b_table")
	mgr.EnableTable("a_table")
	if err := mgr.CreatePolicy(&Policy{
		Name:       "p",
		TableName:  "c_table",
		Type:       PolicySelect,
		Expression: "TRUE",
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	first, err := mgr.SerializePolicies()
	if err != nil {
		t.Fatalf("SerializePolicies: %v", err)
	}
	second, err := mgr.SerializePolicies()
	if err != nil {
		t.Fatalf("SerializePolicies (second): %v", err)
	}
	if string(first) != string(second) {
		t.Fatal("SerializePolicies output must be deterministic for identical state")
	}
}
