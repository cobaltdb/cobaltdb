package query

import (
	"testing"
)

// ============================================================
// Parser Error Path Coverage
// ============================================================

func TestCovBoost3_Parser_SelectErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Select_InvalidWhere", "SELECT * FROM t WHERE"},
		{"Select_InvalidGroupBy", "SELECT * FROM t GROUP BY"},
		{"Select_InvalidOrderBy", "SELECT * FROM t ORDER BY"},
		{"Select_InvalidJoin", "SELECT * FROM t JOIN"},
		{"Select_InvalidOn", "SELECT * FROM t1 JOIN t2 ON"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
		})
	}
}

func TestCovBoost3_Parser_InsertErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Insert_MissingInto", "INSERT"},
		{"Insert_MissingTable", "INSERT INTO"},
		{"Insert_MissingValues", "INSERT INTO t (id)"},
		{"Insert_InvalidValues", "INSERT INTO t VALUES"},
		{"Insert_InvalidColumn", "INSERT INTO t (@invalid) VALUES (1)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
		})
	}
}

func TestCovBoost3_Parser_UpdateErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Update_MissingTable", "UPDATE"},
		{"Update_MissingSet", "UPDATE t"},
		{"Update_InvalidSet", "UPDATE t SET"},
		{"Update_InvalidWhere", "UPDATE t SET a=1 WHERE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
		})
	}
}

func TestCovBoost3_Parser_DeleteErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Delete_MissingFrom", "DELETE"},
		{"Delete_MissingTable", "DELETE FROM"},
		{"Delete_InvalidWhere", "DELETE FROM t WHERE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
		})
	}
}

func TestCovBoost3_Parser_CreateTableErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Create_MissingTable", "CREATE TABLE"},
		{"Create_InvalidName", "CREATE TABLE 123abc"},
		{"Create_MissingColumns", "CREATE TABLE t ()"},
		{"Create_InvalidColumn", "CREATE TABLE t (@invalid)"},
		{"Create_MissingDataType", "CREATE TABLE t (col)"},
		{"Create_InvalidConstraint", "CREATE TABLE t (id INTEGER CONSTRAINT)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
		})
	}
}

// ============================================================
// Expression Parsing Edge Cases
// ============================================================

func TestCovBoost3_Parser_ExpressionEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Expr_NestedParens", "SELECT * FROM t WHERE ((a = 1))"},
		{"Expr_ComplexLogic", "SELECT * FROM t WHERE (a = 1 AND b = 2) OR (c = 3 AND d = 4)"},
		{"Expr_Arithmetic", "SELECT a + b * c - d / e FROM t"},
		{"Expr_Modulo", "SELECT a % b FROM t"},
		{"Expr_Shift", "SELECT a << b >> c FROM t"},
		{"Expr_Ternary", "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t"},
		{"Expr_NullCheck", "SELECT * FROM t WHERE a IS NULL"},
		{"Expr_NotNull", "SELECT * FROM t WHERE a IS NOT NULL"},
		{"Expr_Between", "SELECT * FROM t WHERE a BETWEEN 1 AND 10"},
		{"Expr_NotBetween", "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10"},
		{"Expr_InList", "SELECT * FROM t WHERE a IN (1, 2, 3)"},
		{"Expr_NotInList", "SELECT * FROM t WHERE a NOT IN (1, 2, 3)"},
		{"Expr_Like", "SELECT * FROM t WHERE a LIKE '%test%'"},
		{"Expr_NotLike", "SELECT * FROM t WHERE a NOT LIKE '%test%'"},
		{"Expr_Exists", "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2)"},
		{"Expr_Cast", "SELECT CAST(a AS TEXT) FROM t"},
		{"Expr_Coalesce", "SELECT COALESCE(a, b, c) FROM t"},
		{"Expr_NullIf", "SELECT NULLIF(a, b) FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}

// ============================================================
// Window Function Parsing
// ============================================================

func TestCovBoost3_Parser_WindowFunctions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Window_RowNumber", "SELECT ROW_NUMBER() OVER () FROM t"},
		{"Window_Rank", "SELECT RANK() OVER (ORDER BY a) FROM t"},
		{"Window_DenseRank", "SELECT DENSE_RANK() OVER (ORDER BY a) FROM t"},
		{"Window_SumOver", "SELECT SUM(a) OVER (PARTITION BY b) FROM t"},
		{"Window_AvgOver", "SELECT AVG(a) OVER (PARTITION BY b ORDER BY c) FROM t"},
		{"Window_Lead", "SELECT LEAD(a, 1) OVER (ORDER BY b) FROM t"},
		{"Window_Lag", "SELECT LAG(a, 1, 0) OVER (ORDER BY b) FROM t"},
		{"Window_FirstValue", "SELECT FIRST_VALUE(a) OVER (ORDER BY b) FROM t"},
		{"Window_Ntile", "SELECT NTILE(4) OVER (ORDER BY a) FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}

// ============================================================
// CTE Parsing
// ============================================================

func TestCovBoost3_Parser_CTE(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"CTE_Simple", "WITH cte AS (SELECT 1) SELECT * FROM cte"},
		{"CTE_Multiple", "WITH cte1 AS (SELECT 1), cte2 AS (SELECT 2) SELECT * FROM cte1, cte2"},
		{"CTE_Recursive", "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte"},
		{"CTE_WithColumns", "WITH cte(col) AS (SELECT 1) SELECT * FROM cte"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}

// ============================================================
// Subquery Parsing
// ============================================================

func TestCovBoost3_Parser_Subqueries(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Subquery_InSelect", "SELECT (SELECT MAX(a) FROM t2) FROM t1"},
		{"Subquery_InFrom", "SELECT * FROM (SELECT * FROM t2) AS sub"},
		{"Subquery_InWhere", "SELECT * FROM t1 WHERE a IN (SELECT b FROM t2)"},
		{"Subquery_Correlated", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.b = t1.a)"},
		{"Subquery_Derived", "SELECT * FROM (SELECT a, b FROM t2 WHERE c = 1) AS derived(d, e)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}

// ============================================================
// Function Call Parsing
// ============================================================

func TestCovBoost3_Parser_FunctionCalls(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Func_NoArgs", "SELECT NOW() FROM t"},
		{"Func_OneArg", "SELECT UPPER(name) FROM t"},
		{"Func_MultipleArgs", "SELECT SUBSTR(name, 1, 5) FROM t"},
		{"Func_Distinct", "SELECT COUNT(DISTINCT a) FROM t"},
		{"Func_Star", "SELECT COUNT(*) FROM t"},
		{"Func_Nested", "SELECT UPPER(TRIM(name)) FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}

// ============================================================
// JOIN Parsing
// ============================================================

func TestCovBoost3_Parser_Joins(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Join_Inner", "SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.b"},
		{"Join_Left", "SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.b"},
		{"Join_Right", "SELECT * FROM t1 RIGHT JOIN t2 ON t1.a = t2.b"},
		{"Join_Full", "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.a = t2.b"},
		{"Join_Cross", "SELECT * FROM t1 CROSS JOIN t2"},
		{"Join_Multiple", "SELECT * FROM t1 JOIN t2 ON t1.a = t2.b JOIN t3 ON t2.c = t3.d"},
		{"Join_TableAlias", "SELECT * FROM t1 AS a JOIN t2 AS b ON a.id = b.id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}

// ============================================================
// Transaction Parsing
// ============================================================

func TestCovBoost3_Parser_Transactions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Txn_Begin", "BEGIN"},
		{"Txn_BeginTransaction", "BEGIN TRANSACTION"},
		{"Txn_Commit", "COMMIT"},
		{"Txn_Rollback", "ROLLBACK"},
		{"Txn_Savepoint", "SAVEPOINT sp1"},
		{"Txn_ReleaseSavepoint", "RELEASE SAVEPOINT sp1"},
		{"Txn_RollbackTo", "ROLLBACK TO SAVEPOINT sp1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}

// ============================================================
// DDL Parsing
// ============================================================

func TestCovBoost3_Parser_DDL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"DDL_CreateIndex", "CREATE INDEX idx ON t(a)"},
		{"DDL_CreateUniqueIndex", "CREATE UNIQUE INDEX idx ON t(a, b)"},
		{"DDL_CreateView", "CREATE VIEW v AS SELECT * FROM t"},
		{"DDL_CreateTrigger", "CREATE TRIGGER tr AFTER INSERT ON t BEGIN DELETE FROM t WHERE id = 0; END"},
		{"DDL_DropTable", "DROP TABLE t"},
		{"DDL_DropTableIfExists", "DROP TABLE IF EXISTS t"},
		{"DDL_DropIndex", "DROP INDEX idx"},
		{"DDL_DropView", "DROP VIEW v"},
		{"DDL_AlterTableAdd", "ALTER TABLE t ADD COLUMN c INTEGER"},
		{"DDL_AlterTableDrop", "ALTER TABLE t DROP COLUMN c"},
		{"DDL_Analyze", "ANALYZE t"},
		{"DDL_Vacuum", "VACUUM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}

// ============================================================
// Reserved Keywords
// ============================================================

func TestCovBoost3_Parser_ReservedKeywords(t *testing.T) {
	// These should work when quoted or used as identifiers in appropriate contexts
	tests := []struct {
		name string
		sql  string
	}{
		{"Keyword_TableName", "CREATE TABLE `select` (id INTEGER)"},
		{"Keyword_ColumnName", "CREATE TABLE t (`order` INTEGER)"},
		{"Keyword_Alias", "SELECT a AS `from` FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.sql, err)
			}
		})
	}
}
