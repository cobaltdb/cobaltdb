package replication

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"
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

func TestPromoteToMasterWithFencingRequiresProof(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	tests := []struct {
		name string
		req  PromotionRequest
		want string
	}{
		{name: "missing token", req: PromotionRequest{Epoch: 1, OldPrimaryFenced: true}, want: "fencing token"},
		{name: "missing fence", req: PromotionRequest{Epoch: 1, FencingToken: "tok"}, want: "old primary"},
		{name: "missing epoch", req: PromotionRequest{FencingToken: "tok", OldPrimaryFenced: true}, want: "epoch"},
		{name: "expired token", req: PromotionRequest{Epoch: 1, FencingToken: "tok", OldPrimaryFenced: true, ExpiresAt: time.Now().Add(-time.Second)}, want: "expired"},
		{name: "stale replica", req: PromotionRequest{Epoch: 1, FencingToken: "tok", OldPrimaryFenced: true, RequiredLSN: 10}, want: "behind"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.PromoteToMasterWithFencing(tt.req)
			if !errors.Is(err, ErrPromotionRejected) {
				t.Fatalf("expected ErrPromotionRejected, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
			if got := mgr.GetStatus().Role; got != "slave" {
				t.Fatalf("rejected promotion changed role to %q", got)
			}
		})
	}
}

func TestPromoteToMasterWithFencingPromotesDisconnectedCaughtUpSlave(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	mgr.lastApplied = 42

	err := mgr.PromoteToMasterWithFencing(PromotionRequest{
		FencingToken:     "lease-abc",
		Epoch:            7,
		OldPrimaryFenced: true,
		ExpiresAt:        time.Now().Add(time.Minute),
		RequiredLSN:      42,
	})
	if err != nil {
		t.Fatalf("PromoteToMasterWithFencing: %v", err)
	}

	status := mgr.GetStatus()
	if status.Role != "master" {
		t.Fatalf("role = %q, want master", status.Role)
	}
	if status.CurrentMaster != 42 {
		t.Fatalf("current master LSN = %d, want 42", status.CurrentMaster)
	}
	if status.PromotionEpoch != 7 {
		t.Fatalf("promotion epoch = %d, want 7", status.PromotionEpoch)
	}
	readiness := mgr.GetFailoverReadiness()
	if readiness.AutomaticFailover || readiness.Consensus {
		t.Fatalf("manual promotion must not report automatic HA: %+v", readiness)
	}
	if !readiness.Fencing || !readiness.SafePromotion {
		t.Fatalf("expected fenced manual promotion readiness, got %+v", readiness)
	}
}

func TestPromoteToMasterWithFencingRejectsActiveMasterConnection(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	left, right := net.Pipe()
	defer right.Close()
	mgr.setMasterConn(left)

	err := mgr.PromoteToMasterWithFencing(PromotionRequest{
		FencingToken:     "lease-abc",
		Epoch:            1,
		OldPrimaryFenced: true,
		ExpiresAt:        time.Now().Add(time.Minute),
	})
	if !errors.Is(err, ErrPromotionRejected) {
		t.Fatalf("expected ErrPromotionRejected, got %v", err)
	}
	if !strings.Contains(err.Error(), "master connection") {
		t.Fatalf("expected master connection error, got %v", err)
	}
	if got := mgr.GetStatus().Role; got != "slave" {
		t.Fatalf("rejected promotion changed role to %q", got)
	}
	mgr.closeMasterConn()
}
