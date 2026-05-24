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

func TestFencePrimaryRejectsFutureWrites(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	if err := mgr.ReplicateWALEntry([]byte("before")); err != nil {
		t.Fatalf("ReplicateWALEntry before fence: %v", err)
	}
	if got := mgr.GetStatus().CurrentMaster; got != 1 {
		t.Fatalf("current LSN before fence = %d, want 1", got)
	}

	if err := mgr.FencePrimary(PrimaryFenceRequest{
		FencingToken: "fence-master",
		Epoch:        3,
		ExpiresAt:    time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("FencePrimary: %v", err)
	}

	status := mgr.GetStatus()
	if !status.PrimaryFenced || status.FencedEpoch != 3 {
		t.Fatalf("expected fenced status at epoch 3, got %+v", status)
	}
	if err := mgr.ReplicateWALEntry([]byte("after")); !errors.Is(err, ErrPrimaryFenced) {
		t.Fatalf("expected ErrPrimaryFenced, got %v", err)
	}
	if got := mgr.GetStatus().CurrentMaster; got != 1 {
		t.Fatalf("fenced write advanced LSN to %d, want 1", got)
	}
}

func TestFencePrimaryRequiresProof(t *testing.T) {
	mgr := NewManager(&Config{Role: RoleMaster, Mode: ModeAsync})
	tests := []struct {
		name string
		req  PrimaryFenceRequest
		want string
	}{
		{name: "missing token", req: PrimaryFenceRequest{Epoch: 1}, want: "fencing token"},
		{name: "missing epoch", req: PrimaryFenceRequest{FencingToken: "tok"}, want: "epoch"},
		{name: "expired", req: PrimaryFenceRequest{FencingToken: "tok", Epoch: 1, ExpiresAt: time.Now().Add(-time.Second)}, want: "expired"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.FencePrimary(tt.req)
			if !errors.Is(err, ErrPromotionRejected) {
				t.Fatalf("expected ErrPromotionRejected, got %v", err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
			if mgr.GetStatus().PrimaryFenced {
				t.Fatal("rejected fence request marked primary fenced")
			}
		})
	}
}
