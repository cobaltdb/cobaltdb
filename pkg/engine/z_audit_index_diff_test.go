package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// TestAuditIndexScanMatchesFullScan is a differential audit: the same rows are
// loaded into an indexed table and a non-indexed table, then a battery of
// WHERE/ORDER BY/range queries must return identical result sets from both.
// Any divergence is an index-scan correctness bug.
func TestAuditIndexScanMatchesFullScan(t *testing.T) {
	ctx := context.Background()
	db := mustOpenMem(t)
	defer db.Close()

	mustExec(t, db, "CREATE TABLE noidx (id INTEGER PRIMARY KEY, k INTEGER, s TEXT)")
	mustExec(t, db, "CREATE TABLE idx (id INTEGER PRIMARY KEY, k INTEGER, s TEXT)")

	// Deterministic pseudo-random data with duplicates and NULLs.
	names := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	for i := 1; i <= 200; i++ {
		kv := "NULL"
		if i%7 != 0 { // some NULL k values
			kv = fmt.Sprintf("%d", (i*37)%50)
		}
		sv := "NULL"
		if i%5 != 0 {
			sv = "'" + names[(i*13)%len(names)] + "'"
		}
		for _, tbl := range []string{"noidx", "idx"} {
			mustExec(t, db, fmt.Sprintf("INSERT INTO %s (id, k, s) VALUES (%d, %s, %s)", tbl, i, kv, sv))
		}
	}
	// Add index AFTER load to exercise index build path too.
	mustExec(t, db, "CREATE INDEX idx_k ON idx (k)")
	mustExec(t, db, "CREATE INDEX idx_s ON idx (s)")

	preds := []string{
		"k = 11",
		"k = 0",
		"k <> 11",
		"k > 25",
		"k >= 25",
		"k < 10",
		"k <= 10",
		"k > 10 AND k < 30",
		"k BETWEEN 5 AND 15",
		"k IN (1, 11, 21, 31)",
		"k IS NULL",
		"k IS NOT NULL",
		"s = 'alpha'",
		"s <> 'alpha'",
		"s IS NULL",
		"s IS NOT NULL",
		"s IN ('alpha','beta')",
		"s LIKE 'a%'",
		"k = 11 OR s = 'beta'",
		"k > 40 AND s IS NOT NULL",
	}

	normalize := func(rows [][]interface{}) []string {
		var lines []string
		for _, r := range rows {
			parts := make([]string, len(r))
			for i, v := range r {
				parts[i] = fmt.Sprintf("%v", v)
			}
			lines = append(lines, strings.Join(parts, "|"))
		}
		sort.Strings(lines)
		return lines
	}

	for _, p := range preds {
		a := normalize(queryRows(t, db, "SELECT id, k, s FROM noidx WHERE "+p))
		b := normalize(queryRows(t, db, "SELECT id, k, s FROM idx WHERE "+p))
		if strings.Join(a, "\n") != strings.Join(b, "\n") {
			t.Errorf("PREDICATE %q divergence:\n  noidx(%d): %v\n  idx(%d):   %v", p, len(a), a, len(b), b)
		}
	}

	// ORDER BY via index vs not.
	for _, q := range []string{
		"SELECT id FROM %s WHERE k IS NOT NULL ORDER BY k, id",
		"SELECT id FROM %s WHERE s IS NOT NULL ORDER BY s, id",
		"SELECT id FROM %s ORDER BY k DESC, id LIMIT 10",
	} {
		a := normalizeOrdered(queryRows(t, db, fmt.Sprintf(q, "noidx")))
		b := normalizeOrdered(queryRows(t, db, fmt.Sprintf(q, "idx")))
		if a != b {
			t.Errorf("ORDER query %q divergence:\n  noidx: %v\n  idx:   %v", q, a, b)
		}
	}

	// --- Mutation consistency: apply the same UPDATE/DELETE to both tables and
	// re-run the predicate battery. Catches index-maintenance divergence. ---
	muts := []string{
		"UPDATE %s SET k = 11 WHERE id = 3",
		"UPDATE %s SET k = NULL WHERE id = 11",
		"UPDATE %s SET k = k + 100 WHERE k = 11",
		"UPDATE %s SET s = 'zeta' WHERE id BETWEEN 20 AND 30",
		"UPDATE %s SET s = NULL WHERE s = 'alpha'",
		"DELETE FROM %s WHERE k > 45",
		"DELETE FROM %s WHERE s IS NULL AND id %% 2 = 0",
		"UPDATE %s SET k = 11 WHERE s = 'beta'",
	}
	for _, m := range muts {
		mustExec(t, db, fmt.Sprintf(m, "noidx"))
		mustExec(t, db, fmt.Sprintf(m, "idx"))
	}
	for _, p := range preds {
		a := normalize(queryRows(t, db, "SELECT id, k, s FROM noidx WHERE "+p))
		b := normalize(queryRows(t, db, "SELECT id, k, s FROM idx WHERE "+p))
		if strings.Join(a, "\n") != strings.Join(b, "\n") {
			t.Errorf("POST-MUTATION PREDICATE %q divergence:\n  noidx(%d): %v\n  idx(%d):   %v", p, len(a), a, len(b), b)
		}
	}
	// Full-table sanity: both tables must contain exactly the same rows.
	all := "SELECT id, k, s FROM %s"
	a := normalize(queryRows(t, db, fmt.Sprintf(all, "noidx")))
	b := normalize(queryRows(t, db, fmt.Sprintf(all, "idx")))
	if strings.Join(a, "\n") != strings.Join(b, "\n") {
		t.Errorf("POST-MUTATION full-table divergence:\n  noidx(%d)\n  idx(%d)\n  noidx=%v\n  idx=%v", len(a), len(b), a, b)
	}

	_ = ctx
}

func normalizeOrdered(rows [][]interface{}) string {
	var lines []string
	for _, r := range rows {
		parts := make([]string, len(r))
		for i, v := range r {
			parts[i] = fmt.Sprintf("%v", v)
		}
		lines = append(lines, strings.Join(parts, "|"))
	}
	return strings.Join(lines, "\n")
}

func mustOpenMem(t *testing.T) *DB {
	t.Helper()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}
