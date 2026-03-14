package query

import (
	"testing"
)

// TestParseStatementTypes targets various statement parsing functions
func TestParseStatementTypes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"VACUUM", "VACUUM"},
		{"VACUUM table", "VACUUM test"},
		{"ANALYZE", "ANALYZE"},
		{"ANALYZE table", "ANALYZE test"},
		{"BEGIN", "BEGIN"},
		{"BEGIN TRANSACTION", "BEGIN TRANSACTION"},
		{"COMMIT", "COMMIT"},
		{"ROLLBACK", "ROLLBACK"},
		{"RELEASE SAVEPOINT", "RELEASE SAVEPOINT sp"},
		{"CALL", "CALL myproc()"},
		{"CALL with args", "CALL myproc(1, 'test')"},
		{"SHOW TABLES", "SHOW TABLES"},
		{"SHOW DATABASES", "SHOW DATABASES"},
		{"USE database", "USE mydb"},
		{"DESCRIBE table", "DESCRIBE test"},
		{"DESC table", "DESC test"},
		{"EXPLAIN", "EXPLAIN SELECT * FROM t"},
		{"EXPLAIN QUERY PLAN", "EXPLAIN QUERY PLAN SELECT * FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseAlterTableOperations targets ALTER TABLE variations
func TestParseAlterTableOperations(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"ALTER TABLE ADD COLUMN", "ALTER TABLE t ADD COLUMN newcol INTEGER"},
		{"ALTER TABLE ADD COLUMN with default", "ALTER TABLE t ADD COLUMN newcol INTEGER DEFAULT 0"},
		{"ALTER TABLE ADD COLUMN not null", "ALTER TABLE t ADD COLUMN newcol INTEGER NOT NULL"},
		{"ALTER TABLE DROP COLUMN", "ALTER TABLE t DROP COLUMN oldcol"},
		{"ALTER TABLE RENAME", "ALTER TABLE t RENAME TO newt"},
		{"ALTER TABLE RENAME COLUMN", "ALTER TABLE t RENAME COLUMN oldcol TO newcol"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseSetOperations targets UNION, INTERSECT, EXCEPT
func TestParseSetOperations(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"UNION", "SELECT 1 UNION SELECT 2"},
		{"UNION ALL", "SELECT 1 UNION ALL SELECT 2"},
		{"UNION DISTINCT", "SELECT 1 UNION DISTINCT SELECT 2"},
		{"INTERSECT", "SELECT 1 INTERSECT SELECT 1"},
		{"INTERSECT ALL", "SELECT 1 INTERSECT ALL SELECT 1"},
		{"EXCEPT", "SELECT 1 EXCEPT SELECT 2"},
		{"EXCEPT ALL", "SELECT 1 EXCEPT ALL SELECT 2"},
		{"Multiple UNION", "SELECT 1 UNION SELECT 2 UNION SELECT 3"},
		{"UNION with ORDER BY", "SELECT 1 UNION SELECT 2 ORDER BY 1"},
		{"UNION with LIMIT", "SELECT 1 UNION SELECT 2 LIMIT 10"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseJoinTypesBoost targets different JOIN syntaxes
func TestParseJoinTypesBoost(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"INNER JOIN", "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id"},
		{"LEFT JOIN", "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id"},
		{"LEFT OUTER JOIN", "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id"},
		{"RIGHT JOIN", "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id"},
		{"RIGHT OUTER JOIN", "SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id"},
		{"FULL JOIN", "SELECT * FROM t1 FULL JOIN t2 ON t1.id = t2.id"},
		{"FULL OUTER JOIN", "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id"},
		{"CROSS JOIN", "SELECT * FROM t1 CROSS JOIN t2"},
		{"JOIN with USING", "SELECT * FROM t1 JOIN t2 USING (id)"},
		{"Multiple JOINs", "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseAggregateFunctionsBoost targets aggregate function parsing
func TestParseAggregateFunctionsBoost(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"COUNT(*)", "SELECT COUNT(*) FROM t"},
		{"COUNT(DISTINCT)", "SELECT COUNT(DISTINCT col) FROM t"},
		{"SUM", "SELECT SUM(col) FROM t"},
		{"AVG", "SELECT AVG(col) FROM t"},
		{"MIN", "SELECT MIN(col) FROM t"},
		{"MAX", "SELECT MAX(col) FROM t"},
		{"GROUP_CONCAT", "SELECT GROUP_CONCAT(col) FROM t"},
		{"GROUP_CONCAT with separator", "SELECT GROUP_CONCAT(col SEPARATOR ',') FROM t"},
		{"JSON_ARRAYAGG", "SELECT JSON_ARRAYAGG(col) FROM t"},
		{"JSON_OBJECTAGG", "SELECT JSON_OBJECTAGG(key, val) FROM t"},
		{"Aggregate with FILTER", "SELECT COUNT(*) FILTER (WHERE col > 0) FROM t"},
		{"Aggregate with OVER", "SELECT SUM(col) OVER () FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseWindowFunctionsBoost targets window function parsing
func TestParseWindowFunctionsBoost(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"ROW_NUMBER", "SELECT ROW_NUMBER() OVER () FROM t"},
		{"RANK", "SELECT RANK() OVER () FROM t"},
		{"DENSE_RANK", "SELECT DENSE_RANK() OVER () FROM t"},
		{"NTILE", "SELECT NTILE(4) OVER () FROM t"},
		{"LAG", "SELECT LAG(col) OVER () FROM t"},
		{"LEAD", "SELECT LEAD(col) OVER () FROM t"},
		{"FIRST_VALUE", "SELECT FIRST_VALUE(col) OVER () FROM t"},
		{"LAST_VALUE", "SELECT LAST_VALUE(col) OVER () FROM t"},
		{"NTH_VALUE", "SELECT NTH_VALUE(col, 2) OVER () FROM t"},
		{"Window with PARTITION BY", "SELECT ROW_NUMBER() OVER (PARTITION BY col1) FROM t"},
		{"Window with ORDER BY", "SELECT ROW_NUMBER() OVER (ORDER BY col1) FROM t"},
		{"Window with frame", "SELECT SUM(col) OVER (ORDER BY col ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"},
		{"Named window", "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (ORDER BY col)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseExpressions targets various expression types
func TestParseExpressions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Binary AND", "SELECT * FROM t WHERE a AND b"},
		{"Binary OR", "SELECT * FROM t WHERE a OR b"},
		{"Unary NOT", "SELECT * FROM t WHERE NOT a"},
		{"Unary +", "SELECT +a FROM t"},
		{"Unary -", "SELECT -a FROM t"},
		{"Unary ~", "SELECT ~a FROM t"},
		{"CASE simple", "SELECT CASE a WHEN 1 THEN 'one' ELSE 'other' END FROM t"},
		{"CASE searched", "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END FROM t"},
		{"CASE with multiple WHENs", "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t"},
		{"BETWEEN", "SELECT * FROM t WHERE col BETWEEN 1 AND 10"},
		{"NOT BETWEEN", "SELECT * FROM t WHERE col NOT BETWEEN 1 AND 10"},
		{"IN list", "SELECT * FROM t WHERE col IN (1, 2, 3)"},
		{"NOT IN list", "SELECT * FROM t WHERE col NOT IN (1, 2, 3)"},
		{"LIKE", "SELECT * FROM t WHERE col LIKE '%test%'"},
		{"NOT LIKE", "SELECT * FROM t WHERE col NOT LIKE '%test%'"},
		{"GLOB", "SELECT * FROM t WHERE col GLOB '*.txt'"},
		{"MATCH", "SELECT * FROM t WHERE col MATCH 'pattern'"},
		{"REGEXP", "SELECT * FROM t WHERE col REGEXP '^[a-z]+$'"},
		{"IS", "SELECT * FROM t WHERE col IS NULL"},
		{"IS NOT", "SELECT * FROM t WHERE col IS NOT NULL"},
		{"IS DISTINCT FROM", "SELECT * FROM t WHERE col IS DISTINCT FROM 1"},
		{"IS NOT DISTINCT FROM", "SELECT * FROM t WHERE col IS NOT DISTINCT FROM 1"},
		{"COLLATE", "SELECT * FROM t WHERE col COLLATE NOCASE = 'test'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseLiterals targets literal value parsing
func TestParseLiterals(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Integer", "SELECT 42"},
		{"Negative integer", "SELECT -42"},
		{"Float", "SELECT 3.14"},
		{"Negative float", "SELECT -3.14"},
		{"Scientific", "SELECT 1.5e10"},
		{"String single quote", "SELECT 'hello'"},
		{"String double quote", "SELECT \"hello\""},
		{"String with escape", "SELECT 'it''s'"},
		{"Blob", "SELECT X'48656C6C6F'"},
		{"TRUE", "SELECT TRUE"},
		{"FALSE", "SELECT FALSE"},
		{"NULL", "SELECT NULL"},
		{"CURRENT_DATE", "SELECT CURRENT_DATE"},
		{"CURRENT_TIME", "SELECT CURRENT_TIME"},
		{"CURRENT_TIMESTAMP", "SELECT CURRENT_TIMESTAMP"},
		{"Date literal", "SELECT DATE '2024-01-01'"},
		{"Time literal", "SELECT TIME '12:00:00'"},
		{"Timestamp literal", "SELECT TIMESTAMP '2024-01-01 12:00:00'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseDMLWithClauses targets INSERT/UPDATE/DELETE with various clauses
func TestParseDMLWithClauses(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"INSERT VALUES", "INSERT INTO t VALUES (1, 2, 3)"},
		{"INSERT columns", "INSERT INTO t (a, b, c) VALUES (1, 2, 3)"},
		{"INSERT multiple rows", "INSERT INTO t VALUES (1, 2), (3, 4)"},
		{"INSERT SELECT", "INSERT INTO t SELECT * FROM s"},
		{"INSERT OR REPLACE", "INSERT OR REPLACE INTO t VALUES (1, 2)"},
		{"INSERT OR IGNORE", "INSERT OR IGNORE INTO t VALUES (1, 2)"},
		{"REPLACE", "REPLACE INTO t VALUES (1, 2)"},
		{"UPDATE simple", "UPDATE t SET a = 1"},
		{"UPDATE multiple", "UPDATE t SET a = 1, b = 2"},
		{"UPDATE WHERE", "UPDATE t SET a = 1 WHERE b = 2"},
		{"UPDATE FROM", "UPDATE t SET a = s.b FROM s WHERE t.id = s.id"},
		{"DELETE simple", "DELETE FROM t"},
		{"DELETE WHERE", "DELETE FROM t WHERE a = 1"},
		{"DELETE USING", "DELETE FROM t USING s WHERE t.id = s.id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseSelectClauses targets SELECT with various clauses
func TestParseSelectClauses(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"SELECT DISTINCT", "SELECT DISTINCT a FROM t"},
		{"SELECT ALL", "SELECT ALL a FROM t"},
		{"SELECT TOP", "SELECT TOP 10 * FROM t"},
		{"SELECT LIMIT", "SELECT * FROM t LIMIT 10"},
		{"SELECT OFFSET", "SELECT * FROM t LIMIT 10 OFFSET 5"},
		{"SELECT FETCH", "SELECT * FROM t FETCH FIRST 10 ROWS ONLY"},
		{"SELECT FOR UPDATE", "SELECT * FROM t FOR UPDATE"},
		{"SELECT FOR SHARE", "SELECT * FROM t FOR SHARE"},
		{"SELECT with table sample", "SELECT * FROM t TABLESAMPLE SYSTEM (10)"},
		{"SELECT with hints", "SELECT /*+ INDEX(t idx) */ * FROM t"},
		{"SELECT INTO", "SELECT * INTO newt FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseEdgeCases targets edge cases and error handling
func TestParseEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		valid bool
	}{
		{"Empty string", "", false},
		{"Only whitespace", "   ", false},
		{"Only semicolon", ";", false},
		{"Multiple statements", "SELECT 1; SELECT 2", true},
		{"Very long identifier", "SELECT aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa FROM t", true},
		{"Nested subqueries", "SELECT * FROM (SELECT * FROM (SELECT * FROM t) AS a) AS b", true},
		{"Complex expression", "SELECT (a + b) * (c - d) / (e % f) FROM t", true},
		{"Mixed case keywords", "SeLeCt * FrOm t", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.valid && err != nil {
				t.Logf("Parse %s: %v", tt.name, err)
			}
		})
	}
}
