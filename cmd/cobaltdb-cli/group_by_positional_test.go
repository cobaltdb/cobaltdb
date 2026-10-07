package main

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestGroupByPositionalMatchesMySQL pins MySQL positional-GROUP-BY semantics.
//
// Round 24 deferred `SELECT * FROM t GROUP BY 1` (rejected with "invalid
// use of star expression"; MySQL accepts it). Round 25 closed it: the
// ordinal rewrite now skips bare-star select items (mirroring ORDER BY) and
// the grouped path resolves the surviving ordinal against the expanded
// output columns. This test asserts the full semantics unconditionally.
func TestGroupByPositionalMatchesMySQL(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE g2 (dept TEXT, n INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range [][2]interface{}{{"eng", 1}, {"eng", 2}, {"ops", 3}} {
		if _, err := db.Exec(ctx, "INSERT INTO g2 (dept, n) VALUES (?,?)", r[0], r[1]); err != nil {
			t.Fatalf("insert %v: %v", r, err)
		}
	}

	groups := func(sql string) ([]string, error) {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var dept string
			var cnt int64
			if err := rows.Scan(&dept, &cnt); err != nil {
				return nil, err
			}
			got = append(got, dept+"="+itoa64(cnt))
		}
		sort.Strings(got)
		return got, nil
	}

	var fails []string

	// CONTROL: named GROUP BY.
	got, err := groups(`SELECT dept, COUNT(*) FROM g2 GROUP BY dept`)
	if err != nil {
		fails = append(fails, "named control FAILED: "+err.Error())
	} else if strings.Join(got, ",") != "eng=2,ops=1" {
		fails = append(fails, "named control: got "+strings.Join(got, ",")+", want eng=2,ops=1")
	}

	// SIBLING: positional GROUP BY with an explicit select list.
	got, err = groups(`SELECT dept, COUNT(*) FROM g2 GROUP BY 1`)
	if err != nil {
		fails = append(fails, "GROUP BY 1 (explicit list) FAILED: "+err.Error())
	} else if strings.Join(got, ",") != "eng=2,ops=1" {
		fails = append(fails, "GROUP BY 1 (explicit list): got "+strings.Join(got, ",")+", want eng=2,ops=1")
	}

	// THE GAP: bare "*" select item at the ordinal position. MySQL resolves
	// the ordinal against the first output column (dept) and accepts this.
	// Skipped while the documented gap exists; asserts fully once fixed.
	rows, err := db.Query(ctx, `SELECT * FROM g2 GROUP BY 1`)
	if err != nil {
		fails = append(fails, "SELECT * ... GROUP BY 1 FAILED: "+err.Error())
	} else {
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		if n != 2 {
			fails = append(fails, "SELECT * ... GROUP BY 1: got "+itoa64(int64(n))+" groups, want 2 (eng, ops)")
		}
	}

	// Combined GROUP BY / ORDER BY ordinals.
	if _, err := db.Query(ctx, `SELECT dept, COUNT(*) FROM g2 GROUP BY 1 ORDER BY 1`); err != nil {
		fails = append(fails, "GROUP BY 1 ORDER BY 1 FAILED: "+err.Error())
	}

	if len(fails) > 0 {
		t.Fatalf("FAIL: %d positional-GROUP-BY case(s) diverge from MySQL:\n  %s", len(fails), strings.Join(fails, "\n  "))
	}
	t.Log("PASS: positional GROUP BY matches MySQL semantics (gap case active and verified)")
	t.Log("PASS: positional GROUP BY matches MySQL semantics in every case")
}

func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
