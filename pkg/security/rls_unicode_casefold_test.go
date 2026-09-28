package security

import (
	"context"
	"testing"
)

// findTopLevelOperator must locate top-level operators with matching that is
// aligned to the ORIGINAL expression bytes. Regression: it pre-uppercased the
// expression with toUpperFast and prefix-tested that copy while walking
// expr's bytes in parallel — but Unicode simple case mapping can make the
// uppercased copy BYTE-SHORTER than the input (ı→I, ſ→S, each 2 bytes → 1),
// so the two index spaces desynced and operators after such a rune were
// missed or found at a shifted index. A policy like
// `town = 'ıskılı' AND active = 1` lost its top-level AND, fell back to a
// bogus first-'=' comparison, and degraded to deny-all for the rows it
// explicitly grants.

func unicodeCasefoldCheck(t *testing.T, m *Manager, town string, active bool) bool {
	t.Helper()
	allowed, err := m.CheckAccess(context.Background(), "docs", PolicySelect,
		map[string]interface{}{"town": town, "active": active}, "alice", nil)
	if err != nil {
		t.Fatalf("CheckAccess(town=%q, active=%v): %v", town, active, err)
	}
	return allowed
}

// TestUnicodeCasefoldASCIIPolicyParses pins the ASCII shape: the same policy
// without width-changing runes parses and enforces the AND correctly.
func TestUnicodeCasefoldASCIIPolicyParses(t *testing.T) {
	m := NewManager()
	m.EnableTable("docs")
	if err := m.CreatePolicy(&Policy{
		Name:       "ascii_policy",
		TableName:  "docs",
		Type:       PolicySelect,
		Expression: `town = 'iskili' AND active = 1`,
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	if !unicodeCasefoldCheck(t, m, "iskili", true) {
		t.Fatal(`granted row ('iskili', active) DENIED — ASCII AND-policy must parse`)
	}
	if unicodeCasefoldCheck(t, m, "iskili", false) {
		t.Fatal(`('iskili', inactive) unexpectedly ALLOWED`)
	}
	if unicodeCasefoldCheck(t, m, "other", true) {
		t.Fatal(`('other', active) unexpectedly ALLOWED`)
	}
}

// TestUnicodeCasefoldDoesNotDesyncOperatorScan pins the fix: a literal
// containing a rune whose uppercase is narrower (ı→I) must not desync the
// operator scan — the policy must keep granting exactly the rows it says.
func TestUnicodeCasefoldDoesNotDesyncOperatorScan(t *testing.T) {
	m := NewManager()
	m.EnableTable("docs")
	if err := m.CreatePolicy(&Policy{
		Name:       "turkish_policy",
		TableName:  "docs",
		Type:       PolicySelect,
		Expression: `town = 'ıskılı' AND active = 1`,
	}); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	if !unicodeCasefoldCheck(t, m, "ıskılı", true) {
		t.Fatal(`FAIL: row with town = "ıskılı", active=1 DENIED — the ı→I case-fold made toUpperFast's output byte-shorter than the input, desyncing findTopLevelOperator's expr/upperExpr indices so the top-level AND was missed and the policy degraded to deny-all`)
	}
	if unicodeCasefoldCheck(t, m, "ıskılı", false) {
		t.Fatal(`('ıskılı', inactive) unexpectedly ALLOWED`)
	}
	if unicodeCasefoldCheck(t, m, "other", true) {
		t.Fatal(`('other', active) unexpectedly ALLOWED`)
	}
}
