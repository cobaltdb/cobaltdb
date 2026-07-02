package catalog

// Per-query CTE/derived-table result storage.
//
// Plain SELECT statements execute under cat.mu.RLock, so multiple queries run
// concurrently. Some execution paths (derived table + JOIN, complex view +
// JOIN, materialized-view resolution) need to materialize an intermediate
// result set and make it visible to the JOIN/aggregate helpers that resolve
// table references by name. Historically these wrote into the shared
// cat.cteResults map, which (a) is a data race under RLock (fatal "concurrent
// map writes") and (b) leaks one query's results into another query using the
// same alias.
//
// Instead, per-query results are stored in a goroutine-keyed overlay
// (query execution is synchronous within one goroutine; the parallel scan
// workers never resolve table references). Readers consult the per-query
// overlay first via lookupCTEResult, then fall back to the shared
// cat.cteResults map, which is only written while holding the exclusive lock
// (ExecuteCTE / recursive CTE execution).

import "sync"

// cteOverlayStore holds per-goroutine CTE/derived-table overlays.
// The zero value is ready to use, so directly-constructed Catalogs work.
type cteOverlayStore struct {
	m sync.Map // goroutine ID (int64) -> map[string]*cteResultSet
}

// lookupCTEResult resolves a CTE/derived-table/materialized-view result by
// name, consulting the current goroutine's per-query overlay first and the
// shared cteResults map second.
func (c *Catalog) lookupCTEResult(name string) (*cteResultSet, bool) {
	key := toLowerFast(name)
	if v, ok := c.localCTE.m.Load(goroutineID()); ok {
		if rs, ok2 := v.(map[string]*cteResultSet)[key]; ok2 {
			return rs, true
		}
	}
	if c.cteResults != nil {
		if rs, ok := c.cteResults[key]; ok {
			return rs, true
		}
	}
	return nil, false
}

// setLocalCTEResult registers a per-query result set under the given name for
// the current goroutine and returns a cleanup function that MUST be called
// (typically deferred) when the query finishes. The cleanup restores any
// previously shadowed entry, so nested scopes reusing an alias are safe.
func (c *Catalog) setLocalCTEResult(name string, rs *cteResultSet) func() {
	gid := goroutineID()
	var overlay map[string]*cteResultSet
	if v, ok := c.localCTE.m.Load(gid); ok {
		overlay = v.(map[string]*cteResultSet)
	} else {
		overlay = make(map[string]*cteResultSet, 2)
		c.localCTE.m.Store(gid, overlay)
	}
	key := toLowerFast(name)
	prev, had := overlay[key]
	overlay[key] = rs
	return func() {
		if had {
			overlay[key] = prev
			return
		}
		delete(overlay, key)
		if len(overlay) == 0 {
			c.localCTE.m.Delete(gid)
		}
	}
}
