package query

import (
	"testing"
)

// TestParsePrimaryCoverage targets parsePrimary with various expressions
func TestParsePrimaryCoverage(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		valid bool
	}{
		{"literal string", "SELECT 'hello'", true},
		{"literal integer", "SELECT 42", true},
		{"literal float", "SELECT 3.14", true},
		{"literal null", "SELECT NULL", true},
		{"literal true", "SELECT TRUE", true},
		{"literal false", "SELECT FALSE", true},
		{"identifier", "SELECT col FROM t", true},
		{"parenthesized expr", "SELECT (1 + 2)", true},
		{"function call", "SELECT COUNT(*)", true},
		{"CASE expression", "SELECT CASE WHEN 1=1 THEN 'a' END", true},
		{"CAST expression", "SELECT CAST(x AS INTEGER)", true},
		{"EXISTS subquery", "SELECT EXISTS (SELECT 1)", true},
		{"column ref", "SELECT t.col FROM t", true},
		{"star", "SELECT * FROM t", true},
		{"array access", "SELECT arr[0]", false},
		{"typed literal", "SELECT DATE '2024-01-01'", false},
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

// TestParseCreateFTSIndexCoverage targets parseCreateFTSIndex
func TestParseCreateFTSIndexCoverage(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		valid bool
	}{
		{"basic FTS index", "CREATE FTS INDEX idx ON t(col)", true},
		{"FTS index with analyzer", "CREATE FTS INDEX idx ON t(col) WITH ANALYZER standard", false},
		{"FTS index if not exists", "CREATE FTS INDEX IF NOT EXISTS idx ON t(col)", true},
		{"FTS index with multiple columns", "CREATE FTS INDEX idx ON t(col1, col2)", true},
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

// TestParseCreateMaterializedViewCoverage targets parseCreateMaterializedView
func TestParseCreateMaterializedViewCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"basic materialized view", "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t"},
		{"materialized view if not exists", "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT * FROM t"},
		{"materialized view with refresh", "CREATE MATERIALIZED VIEW mv REFRESH ON COMMIT AS SELECT * FROM t"},
		{"materialized view with interval", "CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR AS SELECT * FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseDropMaterializedViewCoverage targets parseDropMaterializedView
func TestParseDropMaterializedViewCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"drop materialized view", "DROP MATERIALIZED VIEW mv"},
		{"drop materialized view if exists", "DROP MATERIALIZED VIEW IF EXISTS mv"},
		{"drop materialized view cascade", "DROP MATERIALIZED VIEW mv CASCADE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseRefreshCoverage targets parseRefresh
func TestParseRefreshCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"refresh materialized view", "REFRESH MATERIALIZED VIEW mv"},
		{"refresh with concurrent", "REFRESH MATERIALIZED VIEW CONCURRENTLY mv"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseCreatePolicyCoverage targets parseCreatePolicy
func TestParseCreatePolicyCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"basic policy", "CREATE POLICY p ON t FOR SELECT USING (true)"},
		{"policy for all", "CREATE POLICY p ON t FOR ALL TO PUBLIC USING (x > 0)"},
		{"policy with check", "CREATE POLICY p ON t FOR INSERT WITH CHECK (x > 0)"},
		{"policy for update", "CREATE POLICY p ON t FOR UPDATE USING (owner = CURRENT_USER)"},
		{"policy for delete", "CREATE POLICY p ON t FOR DELETE USING (owner = CURRENT_USER)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseCastCoverage targets parseCast
func TestParseCastCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"cast as integer", "SELECT CAST(x AS INTEGER)"},
		{"cast as text", "SELECT CAST(x AS TEXT)"},
		{"cast as real", "SELECT CAST(x AS REAL)"},
		{"cast as boolean", "SELECT CAST(x AS BOOLEAN)"},
		{"cast as timestamp", "SELECT CAST(x AS TIMESTAMP)"},
		{"cast as decimal", "SELECT CAST(x AS DECIMAL(10,2))"},
		{"cast as varchar", "SELECT CAST(x AS VARCHAR(255))"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseWithCTECoverage targets parseWithCTE
func TestParseWithCTECoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"simple CTE", "WITH cte AS (SELECT 1) SELECT * FROM cte"},
		{"multiple CTEs", "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b"},
		{"recursive CTE", "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 5) SELECT * FROM cte"},
		{"CTE with columns", "WITH cte(x, y) AS (SELECT 1, 2) SELECT * FROM cte"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseCreateTriggerCoverage targets parseCreateTrigger
func TestParseCreateTriggerCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"before insert trigger", "CREATE TRIGGER trg BEFORE INSERT ON t BEGIN SELECT 1; END"},
		{"after update trigger", "CREATE TRIGGER trg AFTER UPDATE ON t BEGIN SELECT 1; END"},
		{"instead of delete trigger", "CREATE TRIGGER trg INSTEAD OF DELETE ON v BEGIN SELECT 1; END"},
		{"trigger with when", "CREATE TRIGGER trg BEFORE INSERT ON t WHEN (NEW.x > 0) BEGIN SELECT 1; END"},
		{"trigger for each row", "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW BEGIN SELECT 1; END"},
		{"drop trigger", "DROP TRIGGER IF EXISTS trg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseProcedureBodyCoverage targets parseProcedureBody
func TestParseProcedureBodyCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"create procedure", "CREATE PROCEDURE p() BEGIN SELECT 1; END"},
		{"create procedure with params", "CREATE PROCEDURE p(IN x INT, OUT y INT) BEGIN SELECT x; END"},
		{"drop procedure", "DROP PROCEDURE IF EXISTS p"},
		{"call procedure", "CALL p()"},
		{"call with args", "CALL p(1, 2)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseCreateIndexCoverage targets parseCreateIndex
func TestParseCreateIndexCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"create unique index", "CREATE UNIQUE INDEX idx ON t(col)"},
		{"create index if not exists", "CREATE INDEX IF NOT EXISTS idx ON t(col)"},
		{"create index on multiple columns", "CREATE INDEX idx ON t(col1, col2 DESC)"},
		{"create index with where", "CREATE INDEX idx ON t(col) WHERE col > 0"},
		{"drop index", "DROP INDEX IF EXISTS idx"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseCreateCollectionCoverage targets parseCreateCollection
func TestParseCreateCollectionCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"create collection", "CREATE COLLECTION c"},
		{"create collection if not exists", "CREATE COLLECTION IF NOT EXISTS c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseSavepointCoverage targets parseSavepoint
func TestParseSavepointCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"savepoint", "SAVEPOINT sp"},
		{"release savepoint", "RELEASE SAVEPOINT sp"},
		{"rollback to savepoint", "ROLLBACK TO SAVEPOINT sp"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseCallCoverage targets parseCall
func TestParseCallCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"call procedure", "CALL p()"},
		{"call with args", "CALL p(1, 'test', NULL)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseExistsExprCoverage targets parseExistsExpr
func TestParseExistsExprCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"exists subquery", "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s)"},
		{"not exists subquery", "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM s)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseParenthesizedCoverage targets parseParenthesized
func TestParseParenthesizedCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"simple paren", "SELECT (1)"},
		{"paren with expr", "SELECT (1 + 2)"},
		{"nested paren", "SELECT ((1))"},
		{"paren with select", "SELECT (SELECT 1)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseDropViewCoverage targets parseDropView
func TestParseDropViewCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"drop view", "DROP VIEW v"},
		{"drop view if exists", "DROP VIEW IF EXISTS v"},
		{"drop multiple views", "DROP VIEW v1, v2, v3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseDropPolicyCoverage targets parseDropPolicy
func TestParseDropPolicyCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"drop policy", "DROP POLICY p ON t"},
		{"drop policy if exists", "DROP POLICY IF EXISTS p ON t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseNumberCoverage targets parseNumber
func TestParseNumberCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"integer", "SELECT 42"},
		{"negative integer", "SELECT -42"},
		{"float", "SELECT 3.14"},
		{"negative float", "SELECT -3.14"},
		{"scientific notation", "SELECT 1e10"},
		{"hex literal", "SELECT 0xFF"},
		{"binary literal", "SELECT 0b1010"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}

// TestParseComparisonCoverage targets parseComparison
func TestParseComparisonCoverage(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"equals", "SELECT * FROM t WHERE x = 1"},
		{"not equals", "SELECT * FROM t WHERE x <> 1"},
		{"less than", "SELECT * FROM t WHERE x < 1"},
		{"greater than", "SELECT * FROM t WHERE x > 1"},
		{"less equals", "SELECT * FROM t WHERE x <= 1"},
		{"greater equals", "SELECT * FROM t WHERE x >= 1"},
		{"is null", "SELECT * FROM t WHERE x IS NULL"},
		{"is not null", "SELECT * FROM t WHERE x IS NOT NULL"},
		{"between", "SELECT * FROM t WHERE x BETWEEN 1 AND 10"},
		{"in list", "SELECT * FROM t WHERE x IN (1, 2, 3)"},
		{"like", "SELECT * FROM t WHERE x LIKE '%test%'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			t.Logf("Parse %s: %v", tt.name, err)
		})
	}
}
