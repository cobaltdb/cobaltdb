package txn

import "sort"

// Test-only helpers. detectConflicts mirrors the commit-path conflict check
// (commitWithConflictDetection step 3) as a standalone probe; it has no
// production callers and lives in a _test.go file so the lint gate's
// unused-code check stays clean.

func (m *Manager) detectConflicts(txn *Transaction) error {
	if txn.Isolation < SnapshotIsolation {
		return nil
	}

	var shardArr [8]int
	var shardExtra []int
	shardCount := 0
	addShard := func(s int) {
		for i := 0; i < shardCount; i++ {
			if shardArr[i] == s {
				return
			}
		}
		for i := range shardExtra {
			if shardExtra[i] == s {
				return
			}
		}
		if shardCount < len(shardArr) {
			shardArr[shardCount] = s
			shardCount++
		} else {
			shardExtra = append(shardExtra, s)
		}
	}
	for wk := range txn.ReadSet {
		addShard(versionShardIdx(wk.TreeName, wk.Key))
	}
	var sorted []int
	if len(shardExtra) == 0 {
		sorted = shardArr[:shardCount]
	} else {
		sorted = make([]int, 0, shardCount+len(shardExtra))
		sorted = append(sorted, shardArr[:shardCount]...)
		sorted = append(sorted, shardExtra...)
	}
	sort.Ints(sorted)

	for _, s := range sorted {
		m.versionShards[s].mu.Lock()
	}
	defer func() {
		for i := len(sorted) - 1; i >= 0; i-- {
			m.versionShards[sorted[i]].mu.Unlock()
		}
	}()

	for wk, readVersion := range txn.ReadSet {
		currentVersion, exists := m.versionShards[versionShardIdx(wk.TreeName, wk.Key)].versions[wk]
		if !exists {
			continue
		}
		if currentVersion > readVersion {
			return ErrConflict
		}
	}

	return nil
}
