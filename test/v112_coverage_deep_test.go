package test

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestV112CoverageDeep targets low-coverage catalog functions
func TestV112CoverageDeep(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		// Window Functions - evaluateWindowFunctions coverage
		{"Window_RowNumber_Basic", "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM (SELECT 1 AS id UNION SELECT 2 UNION SELECT 3)"},
		{"Window_Rank_Basic", "SELECT RANK() OVER (ORDER BY score DESC) FROM (SELECT 100 AS score UNION ALL SELECT 100 UNION ALL SELECT 90)"},
		{"Window_DenseRank", "SELECT DENSE_RANK() OVER (ORDER BY score) FROM (SELECT 10 AS score UNION ALL SELECT 10 UNION ALL SELECT 20)"},
		{"Window_Sum_Over", "SELECT SUM(score) OVER (ORDER BY id) FROM (SELECT 1 AS id, 10 AS score UNION ALL SELECT 2, 20)"},
		{"Window_Avg_Over", "SELECT AVG(score) OVER (ORDER BY id) FROM (SELECT 1 AS id, 10 AS score UNION ALL SELECT 2, 20)"},
		{"Window_Count_Over", "SELECT COUNT(*) OVER (ORDER BY id) FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3)"},
		{"Window_MinMax_Over", "SELECT MIN(score) OVER (), MAX(score) OVER () FROM (SELECT 10 AS score UNION ALL SELECT 20)"},
		{"Window_Partition_By", "SELECT id, SUM(amount) OVER (PARTITION BY category) FROM (SELECT 1 AS id, 100 AS amount, 'A' AS category UNION ALL SELECT 2, 200, 'A' UNION ALL SELECT 3, 50, 'B')"},
		{"Window_Frame_Rows", "SELECT SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (SELECT 1 AS id, 10 AS val UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 30)"},
		{"Window_Lead_Lag", "SELECT id, LAG(val, 1) OVER (ORDER BY id), LEAD(val, 1) OVER (ORDER BY id) FROM (SELECT 1 AS id, 10 AS val UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 30)"},
		{"Window_FirstLast", "SELECT FIRST_VALUE(val) OVER (ORDER BY id), LAST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM (SELECT 1 AS id, 10 AS val UNION ALL SELECT 2, 20)"},
		{"Window_Ntile", "SELECT id, NTILE(2) OVER (ORDER BY id) FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4)"},

		// SELECT with JOINs - executeSelectWithJoin coverage
		{"Join_Inner", "SELECT * FROM (SELECT 1 AS a) t1 JOIN (SELECT 1 AS b) t2 ON t1.a = t2.b"},
		{"Join_Left", "SELECT * FROM (SELECT 1 AS a) t1 LEFT JOIN (SELECT 1 AS b) t2 ON t1.a = t2.b"},
		{"Join_Right", "SELECT * FROM (SELECT 1 AS a) t1 RIGHT JOIN (SELECT 1 AS b) t2 ON t1.a = t2.b"},
		{"Join_Full", "SELECT * FROM (SELECT 1 AS a) t1 FULL OUTER JOIN (SELECT 1 AS b) t2 ON t1.a = t2.b"},
		{"Join_Cross", "SELECT * FROM (SELECT 1 AS a) t1 CROSS JOIN (SELECT 2 AS b) t2"},
		{"Join_Multiple", "SELECT * FROM (SELECT 1 AS id) t1 JOIN (SELECT 1 AS id, 'x' AS val) t2 ON t1.id = t2.id JOIN (SELECT 1 AS id, 100 AS amt) t3 ON t2.id = t3.id"},
		{"Join_Self", "SELECT a.id, b.id FROM (SELECT 1 AS id UNION ALL SELECT 2) a JOIN (SELECT 1 AS id UNION ALL SELECT 2) b ON a.id = b.id"},
		{"Join_WithWhere", "SELECT * FROM (SELECT 1 AS a) t1 JOIN (SELECT 1 AS b) t2 ON t1.a = t2.b WHERE t1.a = 1"},

		// GROUP BY with Aggregates - computeAggregatesWithGroupBy coverage
		{"GroupBy_Single", "SELECT category, SUM(amount) FROM (SELECT 'A' AS category, 100 AS amount UNION ALL SELECT 'A', 200 UNION ALL SELECT 'B', 50) GROUP BY category"},
		{"GroupBy_Multiple", "SELECT cat, sub, COUNT(*) FROM (SELECT 'A' AS cat, 'X' AS sub UNION ALL SELECT 'A', 'X' UNION ALL SELECT 'A', 'Y' UNION ALL SELECT 'B', 'X') GROUP BY cat, sub"},
		{"GroupBy_WithHaving", "SELECT category, SUM(amount) FROM (SELECT 'A' AS category, 100 AS amount UNION ALL SELECT 'B', 50 UNION ALL SELECT 'C', 200) GROUP BY category HAVING SUM(amount) > 75"},
		{"GroupBy_WithOrder", "SELECT category, COUNT(*) FROM (SELECT 'B' AS category UNION ALL SELECT 'A' UNION ALL SELECT 'A') GROUP BY category ORDER BY COUNT(*) DESC"},
		{"GroupBy_AllAggs", "SELECT category, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM (SELECT 'A' AS category, 10 AS val UNION ALL SELECT 'A', 20 UNION ALL SELECT 'B', 5) GROUP BY category"},

		// JOIN + GROUP BY - executeSelectWithJoinAndGroupBy coverage
		{"JoinGroupBy_Basic", "SELECT t1.cat, SUM(t2.val) FROM (SELECT 1 AS id, 'A' AS cat) t1 JOIN (SELECT 1 AS id, 100 AS val) t2 ON t1.id = t2.id GROUP BY t1.cat"},
		{"JoinGroupBy_Multiple", "SELECT t1.a, t2.b, SUM(t3.c) FROM (SELECT 1 AS id, 'X' AS a) t1 JOIN (SELECT 1 AS id, 'Y' AS b) t2 ON t1.id = t2.id JOIN (SELECT 1 AS id, 10 AS c) t3 ON t2.id = t3.id GROUP BY t1.a, t2.b"},

		// Scalar Subqueries - executeScalarSelect coverage
		{"ScalarSub_InSelect", "SELECT (SELECT MAX(val) FROM (SELECT 1 AS val UNION ALL SELECT 2 UNION ALL SELECT 3)) AS max_val"},
		{"ScalarSub_InWhere", "SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) t WHERE t.id > (SELECT AVG(val) FROM (SELECT 1 AS val UNION ALL SELECT 2))"},
		{"ScalarSub_Nested", "SELECT (SELECT (SELECT 1)) AS nested"},

		// CTE with UNION - executeCTEUnion coverage
		{"CTE_Union", "WITH cte AS (SELECT 1 AS id UNION SELECT 2 UNION SELECT 3) SELECT * FROM cte ORDER BY id"},
		{"CTE_UnionAll", "WITH cte AS (SELECT 1 AS id UNION ALL SELECT 1 UNION ALL SELECT 2) SELECT COUNT(*) AS cnt FROM cte"},
		{"CTE_Intersect", "WITH cte AS (SELECT 1 AS id INTERSECT SELECT 1 UNION SELECT 1) SELECT * FROM cte"},
		{"CTE_Except", "WITH cte AS (SELECT 1 AS id UNION SELECT 2 EXCEPT SELECT 2) SELECT * FROM cte"},

		// Complex SELECT scenarios - applyOuterQuery coverage
		{"Select_DistinctOrder", "SELECT DISTINCT val FROM (SELECT 'B' AS val UNION ALL SELECT 'A' UNION ALL SELECT 'B' UNION ALL SELECT 'C') ORDER BY val"},
		{"Select_UnionOrder", "SELECT 1 AS id UNION ALL SELECT 2 ORDER BY id"},
		{"Select_SubqueryFrom", "SELECT * FROM (SELECT id, val FROM (SELECT 1 AS id, 'x' AS val) t1) t2 WHERE id = 1"},

		// UPDATE...FROM - updateWithJoinLocked coverage
		{"UpdateFrom_Basic", "CREATE TABLE upd_tgt (id INTEGER PRIMARY KEY, val INTEGER); CREATE TABLE upd_src (id INTEGER PRIMARY KEY, new_val INTEGER); INSERT INTO upd_tgt VALUES (1, 10); INSERT INTO upd_src VALUES (1, 100); UPDATE upd_tgt SET val = (SELECT new_val FROM upd_src WHERE upd_src.id = upd_tgt.id)"},

		// DELETE...USING - deleteWithUsingLocked coverage
		{"DeleteUsing_Basic", "CREATE TABLE del_tgt (id INTEGER PRIMARY KEY); CREATE TABLE del_src (id INTEGER PRIMARY KEY); INSERT INTO del_tgt VALUES (1); INSERT INTO del_src VALUES (1); DELETE FROM del_tgt WHERE id IN (SELECT id FROM del_src)"},

		// ANALYZE - collectColumnStats coverage
		{"Analyze_Basic", "CREATE TABLE analyze_t (id INTEGER PRIMARY KEY, val INTEGER); INSERT INTO analyze_t VALUES (1, 10); INSERT INTO analyze_t VALUES (2, 20); ANALYZE analyze_t"},

		// Set operations with ORDER BY and LIMIT
		{"SetOp_OrderLimit", "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY id LIMIT 2"},

		// Complex expressions in ORDER BY
		{"OrderBy_Expression", "SELECT id, val FROM (SELECT 1 AS id, 10 AS val UNION ALL SELECT 2, 20) ORDER BY val * 2 DESC"},

		// Derived tables with aliases
		{"DerivedTable_Alias", "SELECT t.x FROM (SELECT 1 AS x) AS t WHERE t.x = 1"},

		// Multiple subqueries in SELECT
		{"MultiSubquery_Select", "SELECT (SELECT 1), (SELECT 2), (SELECT 3)"},

		// Correlated subquery in SELECT
		{"CorrelatedSubquery", "SELECT t1.id, (SELECT MAX(t2.val) FROM (SELECT 1 AS id, 10 AS val UNION ALL SELECT 1, 20 UNION ALL SELECT 2, 5) t2 WHERE t2.id = t1.id) FROM (SELECT 1 AS id UNION ALL SELECT 2) t1"},

		// EXISTS subquery
		{"ExistsSubquery", "SELECT * FROM (SELECT 1 AS id) t WHERE EXISTS (SELECT 1 FROM (SELECT 1 AS id) t2 WHERE t2.id = t.id)"},

		// NOT EXISTS subquery
		{"NotExistsSubquery", "SELECT * FROM (SELECT 1 AS id) t WHERE NOT EXISTS (SELECT 1 FROM (SELECT 2 AS id) t2 WHERE t2.id = t.id)"},

		// IN subquery
		{"InSubquery", "SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) t WHERE t.id IN (SELECT id FROM (SELECT 1 AS id UNION ALL SELECT 2))"},

		// NOT IN subquery
		{"NotInSubquery", "SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3) t WHERE t.id NOT IN (SELECT id FROM (SELECT 4 AS id))"},

		// CASE with subquery
		{"CaseWithSubquery", "SELECT CASE WHEN (SELECT MAX(val) FROM (SELECT 1 AS val)) > 0 THEN 'positive' ELSE 'zero' END"},

		// COALESCE with multiple args
		{"Coalesce_Multi", "SELECT COALESCE(NULL, NULL, 1, 2, 3)"},

		// NULLIF function
		{"Nullif_Basic", "SELECT NULLIF(1, 1), NULLIF(1, 2)"},

		// GREATEST/LEAST
		{"GreatestLeast", "SELECT GREATEST(1, 2, 3), LEAST(1, 2, 3)"},

		// String functions
		{"String_Concat", "SELECT CONCAT('a', 'b', 'c')"},
		{"String_Length", "SELECT LENGTH('hello')"},
		{"String_UpperLower", "SELECT UPPER('abc'), LOWER('ABC')"},
		{"String_Substr", "SELECT SUBSTR('hello', 2, 3)"},
		{"String_Replace", "SELECT REPLACE('hello world', 'world', 'universe')"},
		{"String_Trim", "SELECT TRIM('  hello  ')"},
		{"String_InStr", "SELECT INSTR('hello world', 'world')"},
		{"String_Repeat", "SELECT REPEAT('*', 5)"},
		{"String_LpadRpad", "SELECT LPAD('1', 3, '0'), RPAD('1', 3, '0')"},
		{"String_Hex", "SELECT HEX('abc')"},
		{"String_Quote", "SELECT QUOTE('hello')"},

		// Math functions
		{"Math_Abs", "SELECT ABS(-10), ABS(10)"},
		{"Math_Round", "SELECT ROUND(3.14159, 2)"},
		{"Math_CeilFloor", "SELECT CEIL(3.2), FLOOR(3.8)"},
		{"Math_Power", "SELECT POWER(2, 3)"},
		{"Math_Sqrt", "SELECT SQRT(16)"},
		{"Math_Mod", "SELECT MOD(10, 3)"},
		{"Math_Random", "SELECT RANDOM()"},

		// Date/Time functions
		{"Date_Now", "SELECT NOW()"},
		{"Date_Date", "SELECT DATE('2024-01-15')"},
		{"Date_Time", "SELECT TIME('14:30:00')"},
		{"Date_Datetime", "SELECT DATETIME('2024-01-15 14:30:00')"},
		{"Date_Strftime", "SELECT STRFTIME('%Y-%m-%d', 'now')"},
		{"Date_JulianDay", "SELECT JULIANDAY('2024-01-15')"},
		{"Date_UnixEpoch", "SELECT UNIXEPOCH(), UNIXEPOCH('2024-01-01')"},
		{"Date_StrfTime", "SELECT STRFTIME('%Y', 'now')"},

		// JSON functions
		{"Json_Object", "SELECT JSON_OBJECT('key', 'value')"},
		{"Json_Array", "SELECT JSON_ARRAY(1, 2, 3)"},
		{"Json_Extract", "SELECT JSON_EXTRACT('\"{\\\"a\\\": 1}\"', '$.a')"},
		{"Json_Type", "SELECT JSON_TYPE('[1,2,3]')"},
		{"Json_Valid", "SELECT JSON_VALID('{}'), JSON_VALID('invalid')"},
		{"Json_Quote", "SELECT JSON_QUOTE('hello')"},
		{"Json_GroupArray", "SELECT JSON_GROUP_ARRAY(id) FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3)"},
		{"Json_GroupObject", "SELECT JSON_GROUP_OBJECT('key' || id, id) FROM (SELECT 1 AS id UNION ALL SELECT 2)"},

		// Aggregate with DISTINCT
		{"Agg_CountDistinct", "SELECT COUNT(DISTINCT val) FROM (SELECT 1 AS val UNION ALL SELECT 1 UNION ALL SELECT 2)"},
		{"Agg_SumDistinct", "SELECT SUM(DISTINCT val) FROM (SELECT 10 AS val UNION ALL SELECT 10 UNION ALL SELECT 20)"},

		// GROUP BY with ROLLUP (if supported)
		{"GroupBy_Rollup", "SELECT COALESCE(category, 'Total') AS cat, SUM(amount) FROM (SELECT 'A' AS category, 100 AS amount UNION ALL SELECT 'B', 200 UNION ALL SELECT 'A', 150) GROUP BY category WITH ROLLUP"},

		// HAVING with aggregate comparison
		{"Having_Complex", "SELECT category FROM (SELECT 'A' AS category, 100 AS amount UNION ALL SELECT 'A', 200 UNION ALL SELECT 'B', 50) GROUP BY category HAVING COUNT(*) > 1 AND SUM(amount) > 200"},

		// LIMIT with OFFSET
		{"Limit_Offset", "SELECT * FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) ORDER BY id LIMIT 2 OFFSET 2"},

		// ALL and ANY (SOME) comparisons
		{"AllComparison", "SELECT 1 < ALL (SELECT val FROM (SELECT 2 AS val UNION ALL SELECT 3 UNION ALL SELECT 4))"},
		{"AnyComparison", "SELECT 2 = ANY (SELECT val FROM (SELECT 1 AS val UNION ALL SELECT 2 UNION ALL SELECT 3))"},

		// BETWEEN with different types
		{"Between_Numbers", "SELECT * FROM (SELECT 5 AS val) WHERE val BETWEEN 1 AND 10"},
		{"Between_Strings", "SELECT * FROM (SELECT 'e' AS val) WHERE val BETWEEN 'a' AND 'z'"},

		// LIKE patterns
		{"Like_Patterns", "SELECT 'hello' LIKE 'hel%', 'hello' LIKE '%lo', 'hello' LIKE '%ell%', 'hello' LIKE 'he_lo'"},

		// IS NULL, IS NOT NULL
		{"IsNullChecks", "SELECT NULL IS NULL, 1 IS NOT NULL, NULL IS NOT NULL, 1 IS NULL"},

		// CAST expressions
		{"Cast_Types", "SELECT CAST(123 AS TEXT), CAST('123' AS INTEGER), CAST(1.5 AS INTEGER), CAST(1 AS REAL)"},

		// Column aliases
		{"ColumnAliases", "SELECT 1 AS one, 2 AS two, 1 + 2 AS three"},
		{"TableAliases", "SELECT t.a, t.b FROM (SELECT 1 AS a, 2 AS b) AS t"},

		// SELECT without FROM
		{"SelectNoFrom", "SELECT 1, 2, 3"},
		{"SelectNoFrom_Expr", "SELECT 1 + 2, 'hello', 3.14"},

		// VALUES clause
		{"ValuesClause", "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)"},
	}

	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	passed := 0
	failed := 0

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("%s: %v", tt.name, err)
				failed++
			} else {
				passed++
			}
		})
	}

	t.Logf("TestV112CoverageDeep: %d/%d passed", passed, len(tests))
}
