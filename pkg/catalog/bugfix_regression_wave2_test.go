package catalog

// Regression tests for the concurrency / correctness fix wave:
//  1. concurrent derived-table+JOIN SELECTs (shared cteResults map race and
//     cross-query alias contamination)
//  2. WHERE evaluation errors propagated instead of silently dropping rows
//  3. projection evaluation errors propagated instead of NULL
//  5. index/PK point lookups coerce the search value to the column type
//  6. query result cache: recursive table deps, view deps, rollback
//     invalidation, invalidation epoch

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func mustExec(t *testing.T, c *Catalog, sql string) *QueryResult {
	t.Helper()
	res, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("ExecuteQuery(%q): %v", sql, err)
	}
	return res
}

// --- Fix 1: concurrent derived-table JOIN ---------------------------------

// TestConcurrentDerivedTableJoinRace reproduces the fatal "concurrent map
// writes" crash: 8 goroutines running a derived-table+JOIN SELECT wrote the
// derived result into the shared cteResults map while holding only the read
// lock. Run with -race.
func TestConcurrentDerivedTableJoinRace(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE r (id INTEGER PRIMARY KEY, v INTEGER)")
	for i := 1; i <= 20; i++ {
		mustExec(t, c, fmt.Sprintf("INSERT INTO r VALUES (%d, %d)", i, i*10))
	}

	const goroutines = 8
	const iterations = 30
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				stmt, err := query.Parse("SELECT d.id FROM (SELECT id, v FROM r) AS d JOIN r ON d.id = r.id")
				if err != nil {
					errCh <- err
					return
				}
				_, rows, err := c.Select(stmt.(*query.SelectStmt), nil)
				if err != nil {
					errCh <- err
					return
				}
				if len(rows) != 20 {
					errCh <- fmt.Errorf("expected 20 rows, got %d", len(rows))
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent derived-table JOIN: %v", err)
	}
}

// TestConcurrentDerivedTableAliasIsolation verifies that two concurrent
// queries using the SAME derived-table alias over different tables never see
// each other's rows (the shared map leaked one query's result set into the
// other).
func TestConcurrentDerivedTableAliasIsolation(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE small_t (id INTEGER PRIMARY KEY)")
	mustExec(t, c, "CREATE TABLE big_t (id INTEGER PRIMARY KEY)")
	for i := 1; i <= 3; i++ {
		mustExec(t, c, fmt.Sprintf("INSERT INTO small_t VALUES (%d)", i))
	}
	for i := 101; i <= 110; i++ {
		mustExec(t, c, fmt.Sprintf("INSERT INTO big_t VALUES (%d)", i))
	}

	run := func(table string, wantRows int, errCh chan<- error) {
		sql := fmt.Sprintf("SELECT d.id FROM (SELECT id FROM %s) AS d JOIN %s ON d.id = %s.id", table, table, table)
		for i := 0; i < 40; i++ {
			stmt, err := query.Parse(sql)
			if err != nil {
				errCh <- err
				return
			}
			_, rows, err := c.Select(stmt.(*query.SelectStmt), nil)
			if err != nil {
				errCh <- err
				return
			}
			if len(rows) != wantRows {
				errCh <- fmt.Errorf("table %s: expected %d rows, got %d (alias contamination)", table, wantRows, len(rows))
				return
			}
		}
		errCh <- nil
	}

	errCh := make(chan error, 2)
	go run("small_t", 3, errCh)
	go run("big_t", 10, errCh)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

// --- Fix 2: WHERE evaluation errors propagate ------------------------------

func TestWhereUnknownColumnErrors(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE werr (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, c, "INSERT INTO werr VALUES (1, 10)")
	mustExec(t, c, "INSERT INTO werr VALUES (2, 20)")

	_, err := c.ExecuteQuery("SELECT id FROM werr WHERE nonexistent_col = 5")
	if err == nil {
		t.Fatal("expected error for WHERE on nonexistent column, got empty success")
	}
}

func TestWhereMultiRowScalarSubqueryErrors(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE wsub (id INTEGER PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO wsub VALUES (1)")
	mustExec(t, c, "INSERT INTO wsub VALUES (2)")

	// The scalar subquery yields 2 rows; this must be an error, not an empty
	// result set.
	_, err := c.ExecuteQuery("SELECT id FROM wsub WHERE id = (SELECT id FROM wsub)")
	if err == nil {
		t.Fatal("expected error for multi-row scalar subquery in WHERE")
	}
}

// --- Fix 3: projection evaluation errors propagate --------------------------

func TestProjectionDivisionByZeroErrors(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE perr (id INTEGER PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO perr VALUES (1)")

	if _, err := c.ExecuteQuery("SELECT id / 0 FROM perr"); err == nil {
		t.Fatal("expected division-by-zero error from projection, got NULL result")
	}
	// SELECT without FROM must error as well.
	if _, err := c.ExecuteQuery("SELECT 1 / 0"); err == nil {
		t.Fatal("expected division-by-zero error from scalar SELECT")
	}
}

// --- Fix 5: index/PK lookups coerce the search value -----------------------

func TestPKLookupStringCoercion(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE pkc (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, c, "INSERT INTO pkc VALUES (1, 'a')")
	mustExec(t, c, "INSERT INTO pkc VALUES (2, 'b')")

	// String literal against an INTEGER PK: index point lookup must coerce
	// (full scan matches; the index path returned empty before the fix).
	res := mustExec(t, c, "SELECT id FROM pkc WHERE id = '2'")
	if len(res.Rows) != 1 {
		t.Fatalf("WHERE id = '2' on INTEGER PK: expected 1 row, got %d", len(res.Rows))
	}
}

func TestPKLookupLargeIntegerExactness(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE bigpk (id INTEGER PRIMARY KEY, v TEXT)")
	// 2^53+1 and 2^53+3 are indistinguishable in float64.
	mustExec(t, c, "INSERT INTO bigpk VALUES (9007199254740993, 'x')")
	mustExec(t, c, "INSERT INTO bigpk VALUES (9007199254740995, 'y')")

	res := mustExec(t, c, "SELECT v FROM bigpk WHERE id = 9007199254740995")
	if len(res.Rows) != 1 {
		t.Fatalf("large int PK lookup: expected 1 row, got %d", len(res.Rows))
	}
	if s, _ := res.Rows[0][0].(string); s != "y" {
		t.Fatalf("large int PK lookup returned wrong row: %v", res.Rows[0])
	}
}

func TestSecondaryIndexLookupStringCoercion(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE sic (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, c, "INSERT INTO sic VALUES (1, 5)")
	mustExec(t, c, "INSERT INTO sic VALUES (2, 6)")
	mustExec(t, c, "CREATE INDEX idx_sic_v ON sic (v)")

	res := mustExec(t, c, "SELECT id FROM sic WHERE v = '6'")
	if len(res.Rows) != 1 {
		t.Fatalf("WHERE v = '6' on indexed INTEGER col: expected 1 row, got %d", len(res.Rows))
	}
}

func TestIndexLookupNonIntegralFloatFallsBackToScan(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE fic (id INTEGER PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO fic VALUES (2)")

	// 2.5 cannot be an INTEGER value: previously formatKeyComponent truncated
	// it to key "2" and the index lookup wrongly matched id=2.
	res := mustExec(t, c, "SELECT id FROM fic WHERE id = 2.5")
	if len(res.Rows) != 0 {
		t.Fatalf("WHERE id = 2.5 must not match id=2 via truncated index key, got %d rows", len(res.Rows))
	}
}

// --- Fix 6: query cache correctness ----------------------------------------

func TestQueryCacheInvalidatesSubqueryTables(t *testing.T) {
	c := newTestCatalog(t)
	c.EnableQueryCache(100, time.Minute)
	defer c.DisableQueryCache()

	mustExec(t, c, "CREATE TABLE qc_t (id INTEGER PRIMARY KEY)")
	mustExec(t, c, "CREATE TABLE qc_u (id INTEGER PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO qc_t VALUES (1)")
	mustExec(t, c, "INSERT INTO qc_t VALUES (2)")
	mustExec(t, c, "INSERT INTO qc_u VALUES (1)")

	sql := "SELECT id FROM qc_t WHERE id IN (SELECT id FROM qc_u)"
	res := mustExec(t, c, sql)
	if len(res.Rows) != 1 {
		t.Fatalf("initial query: expected 1 row, got %d", len(res.Rows))
	}

	// A write to the SUBQUERY table must invalidate the cached result.
	mustExec(t, c, "INSERT INTO qc_u VALUES (2)")

	res = mustExec(t, c, sql)
	if len(res.Rows) != 2 {
		t.Fatalf("after insert into subquery table: expected 2 rows, got %d (stale cache)", len(res.Rows))
	}
}

func TestQueryCacheInvalidatesViewBaseTables(t *testing.T) {
	c := newTestCatalog(t)
	c.EnableQueryCache(100, time.Minute)
	defer c.DisableQueryCache()

	mustExec(t, c, "CREATE TABLE qcv_base (id INTEGER PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO qcv_base VALUES (1)")
	if err := c.CreateView("qcv_view", mustParseSelect("SELECT id FROM qcv_base")); err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	sql := "SELECT id FROM qcv_view"
	res := mustExec(t, c, sql)
	if len(res.Rows) != 1 {
		t.Fatalf("initial view query: expected 1 row, got %d", len(res.Rows))
	}

	// A write to the view's BASE table must invalidate the cached view result.
	mustExec(t, c, "INSERT INTO qcv_base VALUES (2)")

	res = mustExec(t, c, sql)
	if len(res.Rows) != 2 {
		t.Fatalf("after insert into base table: expected 2 rows, got %d (stale cached view)", len(res.Rows))
	}
}

func TestQueryCacheInvalidatedOnRollback(t *testing.T) {
	c := newTestCatalog(t)
	c.EnableQueryCache(100, time.Minute)
	defer c.DisableQueryCache()

	mustExec(t, c, "CREATE TABLE qcr (id INTEGER PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO qcr VALUES (1)")

	c.BeginTransaction(1)
	mustExec(t, c, "INSERT INTO qcr VALUES (2)")

	// SELECT inside the open transaction sees (and caches) the uncommitted row.
	res := mustExec(t, c, "SELECT id FROM qcr")
	if len(res.Rows) != 2 {
		t.Fatalf("in-txn query: expected 2 rows, got %d", len(res.Rows))
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// The rolled-back row must not survive in the query cache.
	res = mustExec(t, c, "SELECT id FROM qcr")
	if len(res.Rows) != 1 {
		t.Fatalf("after rollback: expected 1 row, got %d (uncommitted row served from cache)", len(res.Rows))
	}
}

func TestInvalidateQueryCacheBumpsEpoch(t *testing.T) {
	c := newTestCatalog(t)
	before := c.cacheEpoch.Load()
	c.invalidateQueryCache("any_table")
	if c.cacheEpoch.Load() == before {
		t.Fatal("invalidateQueryCache must advance the invalidation epoch")
	}
	before = c.cacheEpoch.Load()
	c.invalidateQueryCacheAll()
	if c.cacheEpoch.Load() == before {
		t.Fatal("invalidateQueryCacheAll must advance the invalidation epoch")
	}
}

func TestResolveCacheTableDepsMaterializedViewUncacheable(t *testing.T) {
	c := newTestCatalog(t)
	mustExec(t, c, "CREATE TABLE mvdep (id INTEGER PRIMARY KEY)")
	c.mu.Lock()
	c.materializedViews["mv1"] = &MaterializedViewDef{Name: "mv1"}
	c.mu.Unlock()

	c.mu.RLock()
	_, cacheable := c.resolveCacheTableDeps([]string{"mv1"})
	c.mu.RUnlock()
	if cacheable {
		t.Fatal("queries over materialized views must be uncacheable")
	}

	c.mu.RLock()
	deps, cacheable := c.resolveCacheTableDeps([]string{"mvdep"})
	c.mu.RUnlock()
	if !cacheable || len(deps) != 1 || !strings.EqualFold(deps[0], "mvdep") {
		t.Fatalf("plain table deps: got %v cacheable=%v", deps, cacheable)
	}
}
