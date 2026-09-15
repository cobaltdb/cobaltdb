package replication

import (
	"bufio"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

// A failed snapshot apply must mark a snapshot refresh as required
// (requireSnapshot=1, like the RESYNC handler) so that the reconnect
// handshake requests RESUME_SNAPSHOT and the slave retries the snapshot.
//
// handleSnapshotMessage previously returned the OnApplySnapshot error without
// setting requireSnapshot, so runSlaveLoop's reconnect sent
// "RESUME <lastApplied>" — the STALE pre-snapshot position. The master
// re-streamed entries the failed snapshot already contained; the engine-side
// apply (applyReplicationSnapshot) had already discarded the buffer pool and
// truncated the backend, so re-applied entries either failed on the closed
// pool (infinite error loop) or poisoned the stream with PK conflicts that
// never advance lastApplied. The replica froze until master-side WAL
// retention eviction happened to force a RESYNC.

// TestSnapshotApplyFailureSetsRequireSnapshot drives the real snapshot frame
// handler with a failing engine callback and asserts the fail-closed error
// surfaces AND the resync-required flag is set for the reconnect handshake.
func TestSnapshotApplyFailureSetsRequireSnapshot(t *testing.T) {
	m := NewManager(&Config{
		Role:      RoleSlave,
		StateFile: "", // keep the test off disk: save/load are no-ops
	})

	var applied int
	m.OnApplySnapshot = func(data []byte, lsn uint64) error {
		applied++
		// Mirrors a real engine-side failure: applyReplicationSnapshot has
		// already discarded the pool and truncated the backend when the
		// reload step fails.
		return fmt.Errorf("simulated engine failure: buffer pool discarded, backend truncated")
	}

	// Drive handleSnapshotMessage exactly as readMasterFrame does: the control
	// line (including its trailing newline) plus the raw payload in the reader.
	msg := "SNAPSHOT 1000 4\n"
	reader := bufio.NewReader(strings.NewReader("\x00\x00\x00\x00"))

	err := m.handleSnapshotMessage(reader, msg)
	if err == nil {
		t.Fatal("expected the snapshot apply error to surface (fail closed)")
	}
	if applied != 1 {
		t.Fatalf("OnApplySnapshot invoked %d times, want 1", applied)
	}
	if atomic.LoadUint32(&m.requireSnapshot) == 0 {
		t.Fatalf("failed snapshot apply left requireSnapshot=0; reconnect would send RESUME %d (stale) instead of RESUME_SNAPSHOT", atomic.LoadUint64(&m.lastApplied))
	}
}

// TestSnapshotApplySuccessClearsRequireSnapshotAndAdvances verifies the happy
// path that must stay intact: a successful snapshot apply advances lastApplied
// to the snapshot LSN, clears the resync-required flag, and ACKs.
func TestSnapshotApplySuccessClearsRequireSnapshotAndAdvances(t *testing.T) {
	m := NewManager(&Config{
		Role:      RoleSlave,
		StateFile: "",
	})
	// Simulate a prior failed apply that set the resync-required flag.
	atomic.StoreUint32(&m.requireSnapshot, 1)

	m.OnApplySnapshot = func(data []byte, lsn uint64) error {
		return nil
	}

	msg := "SNAPSHOT 1000 4\n"
	reader := bufio.NewReader(strings.NewReader("\x00\x00\x00\x00"))

	if err := m.handleSnapshotMessage(reader, msg); err != nil {
		t.Fatalf("successful snapshot apply returned error: %v", err)
	}
	if got := atomic.LoadUint64(&m.lastApplied); got != 1000 {
		t.Fatalf("lastApplied = %d, want 1000 (snapshot LSN)", got)
	}
	if atomic.LoadUint32(&m.requireSnapshot) != 0 {
		t.Fatal("requireSnapshot still set after a successful snapshot apply")
	}
}
