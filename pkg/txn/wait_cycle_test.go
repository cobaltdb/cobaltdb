package txn

import (
	"sort"
	"testing"
)

func sortedCopy(ids []uint64) []uint64 {
	out := append([]uint64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func equalSets(a, b []uint64) bool {
	a, b = sortedCopy(a), sortedCopy(b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFindWaitCycle_NoCycle(t *testing.T) {
	// 1 -> 2 -> 3 -> (none)
	g := map[uint64]uint64{1: 2, 2: 3, 3: 0}
	if c := findWaitCycle(g); c != nil {
		t.Fatalf("expected no cycle, got %v", c)
	}
}

func TestFindWaitCycle_SimpleCycle(t *testing.T) {
	// 1 -> 2 -> 3 -> 1
	g := map[uint64]uint64{1: 2, 2: 3, 3: 1}
	c := findWaitCycle(g)
	if !equalSets(c, []uint64{1, 2, 3}) {
		t.Fatalf("expected cycle {1,2,3}, got %v", c)
	}
}

// The key regression: a transaction on a path leading INTO the cycle must NOT
// appear in the reported cycle, otherwise resolveDeadlock can abort an innocent
// transaction and fail to break the actual deadlock.
func TestFindWaitCycle_ExcludesTailLeadingIntoCycle(t *testing.T) {
	// Cycle: 1 -> 2 -> 3 -> 1.  Tail: 4 -> 1 (4 waits on a cycle member).
	g := map[uint64]uint64{1: 2, 2: 3, 3: 1, 4: 1}
	// Run many times: map iteration order is randomized, so the DFS may start
	// from the tail (4) on some runs. The result must be stable and exclude 4.
	for i := 0; i < 200; i++ {
		c := findWaitCycle(g)
		if !equalSets(c, []uint64{1, 2, 3}) {
			t.Fatalf("iteration %d: expected cycle {1,2,3} excluding tail 4, got %v", i, c)
		}
		for _, id := range c {
			if id == 4 {
				t.Fatalf("tail transaction 4 must not be in the cycle, got %v", c)
			}
		}
	}
}

func TestFindWaitCycle_SelfLoop(t *testing.T) {
	g := map[uint64]uint64{1: 1}
	if c := findWaitCycle(g); !equalSets(c, []uint64{1}) {
		t.Fatalf("expected self-cycle {1}, got %v", c)
	}
}

// End-to-end: with a tail transaction holding the highest StartTS (the would-be
// "youngest victim"), the detector must still abort a transaction that is on the
// real cycle, not the innocent tail.
func TestDeadlockResolutionAbortsCycleMemberNotTail(t *testing.T) {
	m := NewManager(nil, nil)
	m.Start()
	defer m.Stop()

	// Begin order sets increasing StartTS; the last one begun is "youngest".
	a := m.Begin(nil)
	b := m.Begin(nil)
	c := m.Begin(nil)
	tail := m.Begin(nil) // highest StartTS -> would be chosen as victim if included

	// Cycle a -> b -> c -> a, plus tail -> a (innocent, leads into the cycle).
	a.SetWaitingFor(b.ID)
	b.SetWaitingFor(c.ID)
	c.SetWaitingFor(a.ID)
	tail.SetWaitingFor(a.ID)

	m.checkForDeadlocks()

	if !isActive(m, tail.ID) {
		t.Fatal("tail transaction was aborted, but it is not part of the deadlock cycle")
	}
	aborted := !isActive(m, a.ID) || !isActive(m, b.ID) || !isActive(m, c.ID)
	if !aborted {
		t.Fatal("expected one cycle member (a/b/c) to be aborted to break the deadlock")
	}
}

func isActive(m *Manager, id uint64) bool {
	shard := activeShardIdx(id)
	m.activeShards[shard].RLock()
	_, ok := m.activeShards[shard].m[id]
	m.activeShards[shard].RUnlock()
	return ok
}
