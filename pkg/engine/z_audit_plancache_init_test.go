package engine

import "testing"

// TestAuditPlanCacheEnabledOnNewDB verifies that Options.PlanCache.EnablePlanCache
// is honored for newly-created databases (:memory: and first-time disk opens go
// through createNew). The init previously lived only in loadExisting, so the
// configured plan cache was silently disabled on a fresh open.
func TestAuditPlanCacheEnabledOnNewDB(t *testing.T) {
	opts := DefaultOptions()
	opts.PlanCache.EnablePlanCache = true

	db, err := Open(":memory:", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if !db.IsPlanCacheEnabled() {
		t.Fatal("PlanCache.EnablePlanCache=true but plan cache is not enabled on a :memory: DB")
	}

	// The deprecated top-level flag must also work.
	opts2 := DefaultOptions()
	opts2.EnablePlanCache = true
	db2, err := Open(":memory:", opts2)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	defer db2.Close()
	if !db2.IsPlanCacheEnabled() {
		t.Fatal("deprecated Options.EnablePlanCache=true did not enable the plan cache")
	}
}
