package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
)

// TestAuditParallelMatchesSerial verifies the parallel scan/aggregate paths
// (forced on via a tiny threshold and many workers) return byte-identical
// results to the serial path (parallel disabled) over the SAME data, at a size
// above the parallel threshold. Any divergence is a parallel-execution bug.
func TestAuditParallelMatchesSerial(t *testing.T) {
	ctx := context.Background()

	load := func(db *DB) {
		mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, g INTEGER, v INTEGER, s TEXT)")
		names := []string{"a", "b", "c", "d", "e"}
		var sb strings.Builder
		for i := 1; i <= 3000; i++ {
			g := (i * 17) % 12
			v := (i * 31) % 100
			// negatives and NULLs to stress aggregate/decode paths.
			vExpr := fmt.Sprintf("%d", v-50)
			if i%13 == 0 {
				vExpr = "NULL"
			}
			s := "'" + names[(i*7)%len(names)] + "'"
			if i%11 == 0 {
				s = "NULL"
			}
			sb.Reset()
			fmt.Fprintf(&sb, "INSERT INTO t (id, g, v, s) VALUES (%d, %d, %s, %s)", i, g, vExpr, s)
			mustExec(t, db, sb.String())
		}
	}

	par, err := Open(":memory:", &Options{ParallelQuery: ParallelQueryConfig{Workers: 4, Threshold: 8}})
	if err != nil {
		t.Fatalf("open par: %v", err)
	}
	defer par.Close()
	ser, err := Open(":memory:", &Options{ParallelQuery: ParallelQueryConfig{Workers: 0, Threshold: 0}})
	if err != nil {
		t.Fatalf("open ser: %v", err)
	}
	defer ser.Close()

	load(par)
	load(ser)

	queries := []string{
		"SELECT id, g, v, s FROM t WHERE v > 0",
		"SELECT id FROM t WHERE s = 'a'",
		"SELECT id FROM t WHERE s IS NULL",
		"SELECT id FROM t WHERE v IS NULL",
		"SELECT id FROM t WHERE v BETWEEN -10 AND 10",
		"SELECT g, COUNT(*) FROM t GROUP BY g",
		"SELECT g, SUM(v) FROM t GROUP BY g",
		"SELECT g, AVG(v) FROM t GROUP BY g",
		"SELECT g, MIN(v), MAX(v) FROM t GROUP BY g",
		"SELECT g, COUNT(v) FROM t GROUP BY g",
		"SELECT s, COUNT(*) FROM t GROUP BY s",
		"SELECT g, COUNT(*) FROM t WHERE v > 0 GROUP BY g HAVING COUNT(*) > 5",
		"SELECT COUNT(*) FROM t",
		"SELECT SUM(v) FROM t",
		"SELECT AVG(v) FROM t",
		"SELECT COUNT(DISTINCT g) FROM t",
	}

	norm := func(rows [][]interface{}) string {
		var lines []string
		for _, r := range rows {
			parts := make([]string, len(r))
			for i, v := range r {
				parts[i] = fmt.Sprintf("%v", v)
			}
			lines = append(lines, strings.Join(parts, "|"))
		}
		sort.Strings(lines)
		return strings.Join(lines, "\n")
	}

	for _, q := range queries {
		a := norm(queryRows(t, par, q))
		b := norm(queryRows(t, ser, q))
		if a != b {
			t.Errorf("PARALLEL vs SERIAL divergence for %q:\n  parallel:\n%s\n  serial:\n%s", q, a, b)
		}
	}
	_ = ctx
}
