package replication

import (
	"errors"
	"testing"
)

func TestFailoverReadinessReportsTransportIsNotHA(t *testing.T) {
	tests := []struct {
		name string
		role Role
		want string
	}{
		{name: "standalone", role: RoleStandalone, want: "standalone"},
		{name: "master", role: RoleMaster, want: "master"},
		{name: "slave", role: RoleSlave, want: "slave"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := NewManager(&Config{Role: tt.role, Mode: ModeAsync})
			readiness := mgr.GetFailoverReadiness()
			if readiness.Role != tt.want {
				t.Fatalf("expected role %q, got %q", tt.want, readiness.Role)
			}
			if readiness.AutomaticFailover || readiness.Consensus || readiness.Fencing || readiness.SafePromotion {
				t.Fatalf("replication transport must not report HA-ready: %+v", readiness)
			}
			if len(readiness.Blockers) == 0 {
				t.Fatal("expected explicit HA blockers")
			}
		})
	}
}

func TestPromoteToMasterRequiresExternalFencing(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	if err := mgr.PromoteToMaster(); !errors.Is(err, ErrAutomaticFailoverUnsupported) {
		t.Fatalf("expected ErrAutomaticFailoverUnsupported, got %v", err)
	}
	if got := mgr.GetStatus().Role; got != "slave" {
		t.Fatalf("unsafe promotion changed role to %q", got)
	}
}
