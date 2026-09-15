package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func p2db(t *testing.T) (*engine.DB, context.Context) {
	t.Helper()
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db, context.Background()
}

// rowsToStr renders a full result set as "a|b;c|d" for comparison.
func rowsToStr(t *testing.T, db *engine.DB, ctx context.Context, sql string) string {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		return "ERR:" + err.Error()
	}
	defer func() { _ = rows.Close() }()
	cols := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "SCANERR:" + err.Error()
		}
		var cells []string
		for _, v := range vals {
			if v == nil {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, fmt.Sprintf("%v", v))
			}
		}
		out = append(out, strings.Join(cells, "|"))
	}
	return strings.Join(out, ";")
}

func TestProbeJoins(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE l (id INT PRIMARY KEY, v TEXT)`)
	mustExec(t, db, ctx, `CREATE TABLE r (id INT PRIMARY KEY, lid INT, w TEXT)`)
	mustExec(t, db, ctx, `INSERT INTO l VALUES (1,'a'),(2,'b'),(3,'c')`)
	mustExec(t, db, ctx, `INSERT INTO r VALUES (10,1,'x'),(11,1,'y'),(12,2,'z'),(13,NULL,'orphan')`)

	cases := []struct{ name, sql, want string }{
		{"inner", `SELECT l.v, r.w FROM l JOIN r ON l.id = r.lid ORDER BY l.v, r.w`, "a|x;a|y;b|z"},
		{"left", `SELECT l.v, r.w FROM l LEFT JOIN r ON l.id = r.lid ORDER BY l.v, r.w`, "a|x;a|y;b|z;c|NULL"},
		{"right", `SELECT l.v, r.w FROM l RIGHT JOIN r ON l.id = r.lid ORDER BY r.w`, "NULL|orphan;a|x;a|y;b|z"},
		{"left_count", `SELECT COUNT(*) FROM l LEFT JOIN r ON l.id = r.lid`, "4"},
		{"inner_count", `SELECT COUNT(*) FROM l JOIN r ON l.id = r.lid`, "3"},
		// NULL never matches in a join predicate.
		{"null_no_match", `SELECT COUNT(*) FROM l JOIN r ON l.id = r.lid WHERE r.lid IS NULL`, "0"},
		// Aggregate over LEFT JOIN must count NULLs correctly.
		{"left_group", `SELECT l.v, COUNT(r.id) FROM l LEFT JOIN r ON l.id = r.lid GROUP BY l.v ORDER BY l.v`, "a|2;b|1;c|0"},
		{"cross", `SELECT COUNT(*) FROM l CROSS JOIN r`, "12"},
		{"self", `SELECT COUNT(*) FROM l a JOIN l b ON a.id < b.id`, "3"},
	}
	runCases(t, db, ctx, cases)
}

func TestProbeWindowAndCTE(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE s (id INT PRIMARY KEY, g TEXT, n INT)`)
	mustExec(t, db, ctx, `INSERT INTO s VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40),(5,'b',50)`)

	cases := []struct{ name, sql, want string }{
		{"row_number", `SELECT ROW_NUMBER() OVER (ORDER BY id) FROM s`, "1;2;3;4;5"},
		{"partition_rn", `SELECT g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) FROM s ORDER BY id`, "a|1;a|2;b|1;b|2;b|3"},
		{"rank_ties", `SELECT RANK() OVER (ORDER BY g) FROM s ORDER BY id`, "1;1;3;3;3"},
		{"dense_rank", `SELECT DENSE_RANK() OVER (ORDER BY g) FROM s ORDER BY id`, "1;1;2;2;2"},
		{"lag", `SELECT LAG(n) OVER (ORDER BY id) FROM s`, "NULL;10;20;30;40"},
		{"lead", `SELECT LEAD(n) OVER (ORDER BY id) FROM s`, "20;30;40;50;NULL"},
		{"sum_over_partition", `SELECT g, SUM(n) OVER (PARTITION BY g) FROM s ORDER BY id`, "a|30;a|30;b|120;b|120;b|120"},
		{"count_over", `SELECT COUNT(*) OVER () FROM s`, "5;5;5;5;5"},

		{"cte_simple", `WITH t AS (SELECT n FROM s WHERE n > 25) SELECT COUNT(*) FROM t`, "3"},
		{"cte_join", `WITH t AS (SELECT g, SUM(n) AS tot FROM s GROUP BY g) SELECT g, tot FROM t ORDER BY g`, "a|30;b|120"},
		{"cte_recursive", `WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 5) SELECT COUNT(*) FROM c`, "5"},

		{"union", `SELECT n FROM s WHERE n=10 UNION SELECT n FROM s WHERE n=10`, "10"},
		{"union_all", `SELECT n FROM s WHERE n=10 UNION ALL SELECT n FROM s WHERE n=10`, "10;10"},
		{"except", `SELECT g FROM s EXCEPT SELECT 'a'`, "b"},
		{"intersect", `SELECT COUNT(*) FROM (SELECT g FROM s INTERSECT SELECT 'a') q`, "1"},
	}
	runCases(t, db, ctx, cases)
}

func TestProbeSubqueryAndGroupBy(t *testing.T) {
	db, ctx := p2db(t)
	defer func() { _ = db.Close() }()

	mustExec(t, db, ctx, `CREATE TABLE o (id INT PRIMARY KEY, cust TEXT, amt INT)`)
	mustExec(t, db, ctx, `INSERT INTO o VALUES (1,'a',100),(2,'a',200),(3,'b',50),(4,'c',NULL)`)

	cases := []struct{ name, sql, want string }{
		{"group_sum", `SELECT cust, SUM(amt) FROM o GROUP BY cust ORDER BY cust`, "a|300;b|50;c|NULL"},
		{"group_count", `SELECT cust, COUNT(amt) FROM o GROUP BY cust ORDER BY cust`, "a|2;b|1;c|0"},
		{"having_sum", `SELECT cust FROM o GROUP BY cust HAVING SUM(amt) > 100 ORDER BY cust`, "a"},
		{"in_subq", `SELECT COUNT(*) FROM o WHERE cust IN (SELECT cust FROM o WHERE amt > 100)`, "2"},
		{"not_in_subq", `SELECT COUNT(*) FROM o WHERE cust NOT IN (SELECT cust FROM o WHERE amt > 100)`, "2"},
		{"correlated", `SELECT COUNT(*) FROM o x WHERE amt > (SELECT AVG(amt) FROM o y WHERE y.cust = x.cust)`, "1"},
		{"scalar_subq_null", `SELECT (SELECT amt FROM o WHERE id = 999)`, "NULL"},
		{"exists_false", `SELECT COUNT(*) FROM o WHERE EXISTS (SELECT 1 FROM o WHERE id = 999)`, "0"},
		{"group_by_expr", `SELECT COUNT(*) FROM o GROUP BY cust IS NULL`, "4"},
	}
	runCases(t, db, ctx, cases)
}

func mustExec(t *testing.T, db *engine.DB, ctx context.Context, sql string) {
	t.Helper()
	if _, err := db.Exec(ctx, sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func runCases(t *testing.T, db *engine.DB, ctx context.Context, cases []struct{ name, sql, want string }) {
	t.Helper()
	for _, tc := range cases {
		got := rowsToStr(t, db, ctx, tc.sql)
		if got != tc.want {
			t.Errorf("%-22s sql=%s\n   got:  %s\n   want: %s", tc.name, tc.sql, got, tc.want)
		}
	}
}
