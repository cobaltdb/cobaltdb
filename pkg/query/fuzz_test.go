package query

import "testing"

func FuzzParseSQL(f *testing.F) {
	seeds := []string{
		"SELECT 1",
		"SELECT a, b FROM t WHERE x = 1",
		"INSERT INTO t (a) VALUES (1)",
		"UPDATE t SET a = 1 WHERE b = 2",
		"DELETE FROM t WHERE x = 1",
		"CREATE TABLE t (a INT PRIMARY KEY)",
		"DROP TABLE t",
		"SELECT * FROM t JOIN u ON t.a = u.b",
		"SELECT COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1",
		"WITH cte AS (SELECT 1) SELECT * FROM cte",
		"SELECT * FROM t WHERE a IN (SELECT b FROM u)",
		"SELECT * FROM t ORDER BY a LIMIT 10 OFFSET 5",
		"ALTER TABLE t ADD COLUMN b INT",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		// Parser should never panic, even on malformed input
		_, _ = Parse(sql)
	})
}
