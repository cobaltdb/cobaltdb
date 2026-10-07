package replication

import (
	"sync"
	"testing"
	"time"
)

// TestGetMetricsDoesNotRaceRoleTransition guards Manager.role reads on the
// metrics path.
//
// Manager.role is written under m.mu by the HA transitions
// (PromoteToMasterWithFencing, RejoinAsReplica). Every other reader takes that
// lock — the exported Role() accessor and getMasterConn() both use
// m.mu.RLock(). currentReplicationLagMillis used to read m.role directly with
// no lock, and it is reachable from the exported GetMetrics(), so a metrics
// scrape concurrent with a promotion was an unsynchronized read/write of the
// same field.
//
// The race is only observable under the race detector, so this test guards
// `go test -race ./pkg/replication/`; a plain `go test` run passes either way.
// The Role() subtest is the control: it reads the same field through the
// correct accessor and must stay clean, which proves the harness discriminates
// locked from unlocked reads.
func TestGetMetricsDoesNotRaceRoleTransition(t *testing.T) {
	newSlave := func() *Manager {
		return NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	}
	promote := func(m *Manager, epoch uint64) {
		_ = m.PromoteToMasterWithFencing(PromotionRequest{
			FencingToken:     "test-token",
			Epoch:            epoch,
			OldPrimaryFenced: true,
			ExpiresAt:        time.Now().Add(time.Minute),
		})
	}

	t.Run("control_role_accessor", func(t *testing.T) {
		m := newSlave()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				_ = m.Role()
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				promote(m, uint64(i+1))
			}
		}()
		wg.Wait()
	})

	t.Run("get_metrics", func(t *testing.T) {
		m := newSlave()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				_ = m.GetMetrics()
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				promote(m, uint64(i+1))
			}
		}()
		wg.Wait()
	})
}

// TestHATransitionGuardsDoNotRaceRoleTransition covers the remaining unsynchronized
// reads of the role fields.
//
// PromoteToMasterWithFencing, FencePrimary and RejoinAsReplica each guard on
// `m.role` / `m.config.Role` BEFORE taking m.mu.Lock(), while their sibling
// transitions write both fields under that lock. Two concurrent transitions
// therefore read those fields with no lock while the other writes them. They
// now read through roleSnapshot(), which takes m.mu.RLock() and returns both.
//
// Same -race-only visibility as the test above: a plain `go test` run passes
// either way, so this guards `go test -race ./pkg/replication/`.
func TestHATransitionGuardsDoNotRaceRoleTransition(t *testing.T) {
	newSlave := func() *Manager {
		return NewManager(&Config{Role: RoleSlave, Mode: ModeAsync})
	}
	promote := func(m *Manager, epoch uint64) {
		_ = m.PromoteToMasterWithFencing(PromotionRequest{
			FencingToken:     "test-token",
			Epoch:            epoch,
			OldPrimaryFenced: true,
			ExpiresAt:        time.Now().Add(time.Minute),
		})
	}

	t.Run("control_role_accessor", func(t *testing.T) {
		m := newSlave()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 3000; i++ {
				_ = m.Role()
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 3000; i++ {
				promote(m, uint64(i+1))
			}
		}()
		wg.Wait()
	})

	t.Run("concurrent_promote_guards", func(t *testing.T) {
		m := newSlave()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 3000; i++ {
				promote(m, uint64(2*i+1))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 3000; i++ {
				promote(m, uint64(2*i+2))
			}
		}()
		wg.Wait()
	})
}
