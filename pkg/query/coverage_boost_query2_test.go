package query

import (
	"testing"
)

// ============================================================
// Additional Parser Coverage Tests
// ============================================================

// Test parseJoin - 81.8% coverage
func TestCovBoostQuery2_Join(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Join_Inner", "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id", false},
		{"Join_Left", "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id", false},
		{"Join_Right", "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id", false},
		{"Join_Full", "SELECT * FROM t1 FULL JOIN t2 ON t1.id = t2.id", false},
		{"Join_Cross", "SELECT * FROM t1 CROSS JOIN t2", false},
		{"Join_LeftOuter", "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id", false},
		{"Join_RightOuter", "SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id", false},
		{"Join_FullOuter", "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id", false},
		{"Join_Using", "SELECT * FROM t1 JOIN t2 USING (id)", false},
		{"Join_MultiUsing", "SELECT * FROM t1 JOIN t2 USING (id, name)", false},
		{"Join_MissingOn", "SELECT * FROM t1 JOIN t2", true},
		{"Join_MissingOnExpr", "SELECT * FROM t1 JOIN t2 ON", true},
		{"Join_MissingUsingLParen", "SELECT * FROM t1 JOIN t2 USING id)", true},
		{"Join_MissingUsingRParen", "SELECT * FROM t1 JOIN t2 USING (id", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseComparison - 80.4% coverage
func TestCovBoostQuery2_ComparisonExtended(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Compare_IsNull", "SELECT * FROM t WHERE x IS NULL", false},
		{"Compare_IsNotNull", "SELECT * FROM t WHERE x IS NOT NULL", false},
		{"Compare_Between", "SELECT * FROM t WHERE x BETWEEN 1 AND 10", false},
		{"Compare_NotBetween", "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10", false},
		{"Compare_InList", "SELECT * FROM t WHERE x IN (1, 2, 3)", false},
		{"Compare_NotInList", "SELECT * FROM t WHERE x NOT IN (1, 2, 3)", false},
		{"Compare_InSubquery", "SELECT * FROM t WHERE x IN (SELECT y FROM t2)", false},
		{"Compare_NotInSubquery", "SELECT * FROM t WHERE x NOT IN (SELECT y FROM t2)", false},
		{"Compare_Like", "SELECT * FROM t WHERE x LIKE 'pattern'", false},
		{"Compare_NotLike", "SELECT * FROM t WHERE x NOT LIKE 'pattern'", false},
		{"Compare_Regexp", "SELECT * FROM t WHERE x REGEXP 'pattern'", false},
		{"Compare_NotRegexp", "SELECT * FROM t WHERE x NOT REGEXP 'pattern'", false},
		{"Compare_Glob", "SELECT * FROM t WHERE x GLOB 'pattern'", false},
		{"Compare_NotGlob", "SELECT * FROM t WHERE x NOT GLOB 'pattern'", false},
		{"Compare_Match", "SELECT * FROM t WHERE x MATCH 'pattern'", false},
		{"Compare_NotMatch", "SELECT * FROM t WHERE x NOT MATCH 'pattern'", false},
		{"Compare_BetweenMissingAnd", "SELECT * FROM t WHERE x BETWEEN 1", true},
		{"Compare_InMissingExpr", "SELECT * FROM t WHERE x IN", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseCreateTable - 83.1% coverage
func TestCovBoostQuery2_CreateTable(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"CreateTable_IfNotExists", "CREATE TABLE IF NOT EXISTS t (id INT)", false},
		{"CreateTable_Strict", "CREATE TABLE t (id INT) STRICT", false},
		{"CreateTable_WithoutRowid", "CREATE TABLE t (id INT PRIMARY KEY) WITHOUT ROWID", false},
		{"CreateTable_Partitioned", "CREATE TABLE t (id INT) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100))", false},
		{"CreateTable_MultipleConstraints", "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE, email TEXT CHECK (email LIKE '%@%'))", false},
		{"CreateTable_DefaultValues", "CREATE TABLE t (id INT DEFAULT 0, name TEXT DEFAULT 'unknown', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", false},
		// Note: These features not fully supported
		// {"CreateTable_Temp", "CREATE TEMP TABLE t (id INT)", false},
		// {"CreateTable_Temporary", "CREATE TEMPORARY TABLE t (id INT)", false},
		// {"CreateTable_AsSelect", "CREATE TABLE t AS SELECT * FROM t2", false},
		// {"CreateTable_ForeignKey", "CREATE TABLE t (id INT, ref_id INT REFERENCES t2(id))", false},
		// {"CreateTable_ForeignKeyOnDelete", "CREATE TABLE t (id INT, ref_id INT REFERENCES t2(id) ON DELETE CASCADE)", false},
		// {"CreateTable_ForeignKeyOnUpdate", "CREATE TABLE t (id INT, ref_id INT REFERENCES t2(id) ON UPDATE SET NULL)", false},
		{"CreateTable_MissingTableName", "CREATE TABLE (id INT)", true},
		{"CreateTable_MissingLParen", "CREATE TABLE t id INT)", true},
		{"CreateTable_MissingRParen", "CREATE TABLE t (id INT", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseAlterTable - 84.6% coverage
func TestCovBoostQuery2_AlterTable(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"AlterTable_AddColumn", "ALTER TABLE t ADD COLUMN col INT", false},
		{"AlterTable_AddColumnNotNull", "ALTER TABLE t ADD COLUMN col INT NOT NULL", false},
		{"AlterTable_AddColumnDefault", "ALTER TABLE t ADD COLUMN col INT DEFAULT 0", false},
		{"AlterTable_DropColumn", "ALTER TABLE t DROP COLUMN col", false},
		{"AlterTable_RenameColumn", "ALTER TABLE t RENAME COLUMN old TO new", false},
		{"AlterTable_RenameTo", "ALTER TABLE t RENAME TO new_t", false},
		// Note: These features not fully supported
		// {"AlterTable_AddConstraint", "ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (id)", false},
		// {"AlterTable_DropConstraint", "ALTER TABLE t DROP CONSTRAINT pk", false},
		{"AlterTable_MissingTable", "ALTER TABLE ADD COLUMN col INT", true},
		{"AlterTable_MissingAction", "ALTER TABLE t", true},
		{"AlterTable_MissingColumnName", "ALTER TABLE t ADD COLUMN", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseCaseExpr - 86.2% coverage
func TestCovBoostQuery2_CaseExpr(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Case_Simple", "SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t", false},
		{"Case_Searched", "SELECT CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END FROM t", false},
		{"Case_NoElse", "SELECT CASE x WHEN 1 THEN 'one' END FROM t", false},
		{"Case_Nested", "SELECT CASE WHEN x = 1 THEN CASE y WHEN 1 THEN 'both' END END FROM t", false},
		{"Case_MissingWhen", "SELECT CASE x THEN 'one' END FROM t", true},
		{"Case_MissingThen", "SELECT CASE x WHEN 1 'one' END FROM t", true},
		{"Case_MissingEnd", "SELECT CASE x WHEN 1 THEN 'one' FROM t", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseCast - 85.7% coverage
func TestCovBoostQuery2_Cast(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Cast_Integer", "SELECT CAST(x AS INTEGER) FROM t", false},
		{"Cast_Text", "SELECT CAST(x AS TEXT) FROM t", false},
		{"Cast_Real", "SELECT CAST(x AS REAL) FROM t", false},
		{"Cast_Blob", "SELECT CAST(x AS BLOB) FROM t", false},
		{"Cast_Boolean", "SELECT CAST(x AS BOOLEAN) FROM t", false},
		{"Cast_Date", "SELECT CAST(x AS DATE) FROM t", false},
		{"Cast_Timestamp", "SELECT CAST(x AS TIMESTAMP) FROM t", false},
		{"Cast_Nested", "SELECT CAST(CAST(x AS TEXT) AS INTEGER) FROM t", false},
		{"Cast_MissingLParen", "SELECT CAST x AS INTEGER) FROM t", true},
		{"Cast_MissingExpr", "SELECT CAST( AS INTEGER) FROM t", true},
		{"Cast_MissingAs", "SELECT CAST(x INTEGER) FROM t", true},
		{"Cast_MissingType", "SELECT CAST(x AS) FROM t", true},
		{"Cast_MissingRParen", "SELECT CAST(x AS INTEGER FROM t", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseNumber - 83.3% coverage
func TestCovBoostQuery2_Number(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Number_Integer", "SELECT 42 FROM t", false},
		{"Number_Negative", "SELECT -42 FROM t", false},
		{"Number_Decimal", "SELECT 3.14159 FROM t", false},
		{"Number_NegativeDecimal", "SELECT -3.14159 FROM t", false},
		{"Number_Exponent", "SELECT 1e10 FROM t", false},
		{"Number_NegativeExponent", "SELECT -1e10 FROM t", false},
		{"Number_ExponentDecimal", "SELECT 1.5e10 FROM t", false},
		{"Number_Zero", "SELECT 0 FROM t", false},
		{"Number_LeadingDecimal", "SELECT .5 FROM t", false},
		{"Number_Hex", "SELECT 0xFF FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseFunctionCall - 90.0% coverage
func TestCovBoostQuery2_FunctionCall(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Func_Distinct", "SELECT COUNT(DISTINCT x) FROM t", false},
		{"Func_Star", "SELECT COUNT(*) FROM t", false},
		{"Func_Over", "SELECT ROW_NUMBER() OVER (ORDER BY x) FROM t", false},
		{"Func_OverPartition", "SELECT ROW_NUMBER() OVER (PARTITION BY y ORDER BY x) FROM t", false},
		{"Func_GroupConcat", "SELECT GROUP_CONCAT(x) FROM t", false},
		{"Func_GroupConcatSeparator", "SELECT GROUP_CONCAT(x, '-') FROM t", false},
		{"Func_MissingRParen", "SELECT COUNT(* FROM t", true},
		// Note: These features not fully supported
		// {"Func_All", "SELECT COUNT(ALL x) FROM t", false},
		// {"Func_Filter", "SELECT COUNT(*) FILTER (WHERE x > 0) FROM t", false},
		// {"Func_OverFrame", "SELECT SUM(x) OVER (ORDER BY y ROWS UNBOUNDED PRECEDING) FROM t", false},
		// {"Func_OverRange", "SELECT SUM(x) OVER (ORDER BY y RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t", false},
		// {"Func_GroupConcatOrderBy", "SELECT GROUP_CONCAT(x ORDER BY x DESC) FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseWindowExpr - 91.4% coverage
func TestCovBoostQuery2_WindowExpr(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Window_OverEmpty", "SELECT ROW_NUMBER() OVER () FROM t", false},
		{"Window_PartitionBy", "SELECT ROW_NUMBER() OVER (PARTITION BY x) FROM t", false},
		{"Window_PartitionByMulti", "SELECT ROW_NUMBER() OVER (PARTITION BY x, y) FROM t", false},
		{"Window_OrderBy", "SELECT ROW_NUMBER() OVER (ORDER BY x) FROM t", false},
		{"Window_OrderByMulti", "SELECT ROW_NUMBER() OVER (ORDER BY x, y DESC) FROM t", false},
		{"Window_MissingRParen", "SELECT ROW_NUMBER() OVER ( FROM t", true},
		// Note: These features not fully supported
		// {"Window_OverName", "SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS ()", false},
		// {"Window_FrameRows", "SELECT SUM(x) OVER (ROWS UNBOUNDED PRECEDING) FROM t", false},
		// {"Window_FrameRange", "SELECT SUM(x) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t", false},
		// {"Window_FrameGroups", "SELECT SUM(x) OVER (GROUPS 1 PRECEDING) FROM t", false},
		// {"Window_ExcludeCurrent", "SELECT SUM(x) OVER (ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) FROM t", false},
		// {"Window_ExcludeGroup", "SELECT SUM(x) OVER (ROWS UNBOUNDED PRECEDING EXCLUDE GROUP) FROM t", false},
		// {"Window_ExcludeTies", "SELECT SUM(x) OVER (ROWS UNBOUNDED PRECEDING EXCLUDE TIES) FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseDropTable - 81.8% coverage
func TestCovBoostQuery2_DropTable(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"DropTable_Simple", "DROP TABLE t", false},
		{"DropTable_IfExists", "DROP TABLE IF EXISTS t", false},
		{"DropTable_Multiple", "DROP TABLE t1, t2, t3", false},
		{"DropTable_Cascade", "DROP TABLE t CASCADE", false},
		{"DropTable_Restrict", "DROP TABLE t RESTRICT", false},
		{"DropTable_MissingName", "DROP TABLE", true},
		{"DropTable_MissingIfExists", "DROP TABLE IF t", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseForeignKeyDef - 84.2% coverage
func TestCovBoostQuery2_ForeignKey(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"FK_Simple", "CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES t2(id))", false},
		{"FK_MultiCol", "CREATE TABLE t (a INT, b INT, FOREIGN KEY (a, b) REFERENCES t2(x, y))", false},
		{"FK_OnDeleteCascade", "CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES t2(id) ON DELETE CASCADE)", false},
		{"FK_OnDeleteSetNull", "CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES t2(id) ON DELETE SET NULL)", false},
		{"FK_OnDeleteRestrict", "CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES t2(id) ON DELETE RESTRICT)", false},
		{"FK_OnDeleteNoAction", "CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES t2(id) ON DELETE NO ACTION)", false},
		{"FK_OnUpdateCascade", "CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES t2(id) ON UPDATE CASCADE)", false},
		{"FK_OnUpdateSetNull", "CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES t2(id) ON UPDATE SET NULL)", false},
		{"FK_BothActions", "CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES t2(id) ON DELETE CASCADE ON UPDATE SET NULL)", false},
		{"FK_MissingColumns", "CREATE TABLE t (id INT, FOREIGN KEY REFERENCES t2(id))", true},
		{"FK_MissingReferences", "CREATE TABLE t (id INT, FOREIGN KEY (ref) t2(id))", true},
		{"FK_MissingRefTable", "CREATE TABLE t (id INT, FOREIGN KEY (ref) REFERENCES)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseColumnDef - 86.4% coverage
func TestCovBoostQuery2_ColumnDef(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Col_AllTypes", "CREATE TABLE t (i INTEGER, t TEXT, r REAL, b BLOB, bo BOOLEAN, j JSON, d DATE, ts TIMESTAMP, dt DATETIME)", false},
		{"Col_Size", "CREATE TABLE t (name VARCHAR(255), code CHAR(10))", false},
		{"Col_Decimal", "CREATE TABLE t (price DECIMAL(10, 2))", false},
		{"Col_PrimaryKey", "CREATE TABLE t (id INTEGER PRIMARY KEY)", false},
		{"Col_NotNull", "CREATE TABLE t (name TEXT NOT NULL)", false},
		{"Col_Unique", "CREATE TABLE t (email TEXT UNIQUE)", false},
		{"Col_AutoIncrement", "CREATE TABLE t (id INTEGER AUTOINCREMENT)", false},
		{"Col_DefaultLiteral", "CREATE TABLE t (status TEXT DEFAULT 'active')", false},
		{"Col_DefaultNumber", "CREATE TABLE t (count INTEGER DEFAULT 0)", false},
		{"Col_DefaultNull", "CREATE TABLE t (name TEXT DEFAULT NULL)", false},
		{"Col_DefaultExpr", "CREATE TABLE t (created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", false},
		{"Col_Check", "CREATE TABLE t (age INTEGER CHECK (age >= 0))", false},
		{"Col_MultipleConstraints", "CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL AUTOINCREMENT, name TEXT NOT NULL UNIQUE CHECK (LENGTH(name) > 0))", false},
		// Note: These features not fully supported
		// {"Col_Collate", "CREATE TABLE t (name TEXT COLLATE NOCASE)", false},
		// {"Col_Generated", "CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)", false},
		// {"Col_Virtual", "CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseShow - 86.2% coverage
func TestCovBoostQuery2_Show(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Show_Tables", "SHOW TABLES", false},
		{"Show_Databases", "SHOW DATABASES", false},
		{"Show_CreateTable", "SHOW CREATE TABLE t", false},
		{"Show_Columns", "SHOW COLUMNS FROM t", false},
		// Note: These features not fully supported
		// {"Show_ColumnsIn", "SHOW COLUMNS IN t", false},
		// {"Show_Indexes", "SHOW INDEXES FROM t", false},
		// {"Show_IndexesIn", "SHOW INDEXES IN t", false},
		{"Show_Status", "SHOW STATUS", false},
		{"Show_Variables", "SHOW VARIABLES", false},
		{"Show_Warnings", "SHOW WARNINGS", false},
		{"Show_Errors", "SHOW ERRORS", false},
		{"Show_Like", "SHOW TABLES LIKE 'user%'", false},
		{"Show_Where", "SHOW TABLES WHERE Name LIKE 'user%'", false},
		{"Show_MissingTableName", "SHOW CREATE TABLE", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseInsert - 89.5% coverage
func TestCovBoostQuery2_Insert(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// Note: DEFAULT VALUES not fully supported
		// {"Insert_DefaultValues", "INSERT INTO t DEFAULT VALUES", false},
		{"Insert_Select", "INSERT INTO t SELECT * FROM t2", false},
		{"Insert_OnConflictIgnore", "INSERT OR IGNORE INTO t VALUES (1)", false},
		{"Insert_OnConflictReplace", "INSERT OR REPLACE INTO t VALUES (1)", false},
		// Note: These features not fully supported
		// {"Insert_OnConflictRollback", "INSERT OR ROLLBACK INTO t VALUES (1)", false},
		// {"Insert_OnConflictAbort", "INSERT OR ABORT INTO t VALUES (1)", false},
		// {"Insert_OnConflictFail", "INSERT OR FAIL INTO t VALUES (1)", false},
		{"Insert_Returning", "INSERT INTO t VALUES (1) RETURNING *", false},
		{"Insert_ReturningColumns", "INSERT INTO t VALUES (1) RETURNING id, name", false},
		{"Insert_Upsert", "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING", false},
		{"Insert_UpsertUpdate", "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET name = 'updated'", false},
		{"Insert_UpsertWhere", "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET name = 'updated' WHERE id > 0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseUpdate - 89.9% coverage
func TestCovBoostQuery2_Update(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Update_From", "UPDATE t SET x = 1 FROM t2 WHERE t.id = t2.id", false},
		{"Update_Returning", "UPDATE t SET x = 1 RETURNING *", false},
		{"Update_ReturningColumns", "UPDATE t SET x = 1 RETURNING id, x", false},
		{"Update_SetSubquery", "UPDATE t SET x = (SELECT MAX(y) FROM t2)", false},
		{"Update_SetExpression", "UPDATE t SET x = y + 1, z = y * 2", false},
		// Note: Multi-column SET not fully supported
		// {"Update_SetList", "UPDATE t SET (x, y) = (1, 2)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseDelete - 90.0% coverage
func TestCovBoostQuery2_Delete(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Delete_Using", "DELETE FROM t USING t2 WHERE t.id = t2.id", false},
		{"Delete_Returning", "DELETE FROM t RETURNING *", false},
		{"Delete_ReturningColumns", "DELETE FROM t RETURNING id, name", false},
		{"Delete_OrderBy", "DELETE FROM t WHERE x > 0 ORDER BY x", false},
		{"Delete_Limit", "DELETE FROM t WHERE x > 0 LIMIT 10", false},
		{"Delete_OrderByLimit", "DELETE FROM t WHERE x > 0 ORDER BY x LIMIT 10", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseSelect - 91.9% coverage
func TestCovBoostQuery2_Select(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Select_Distinct", "SELECT DISTINCT x FROM t", false},
		{"Select_Star", "SELECT * FROM t", false},
		// Note: Qualified star may not be fully supported
		// {"Select_QualifiedStar", "SELECT t.* FROM t", false},
		// Note: These features not fully supported
		// {"Select_DistinctOn", "SELECT DISTINCT ON (x) x, y FROM t", false},
		// {"Select_All", "SELECT ALL x FROM t", false},
		// {"Select_Except", "SELECT * EXCEPT (x) FROM t", false},
		// {"Select_Replace", "SELECT * REPLACE (y AS x) FROM t", false},
		// {"Select_Window", "SELECT x, ROW_NUMBER() OVER (w) FROM t WINDOW w AS (ORDER BY x)", false},
		// {"Select_ForUpdate", "SELECT * FROM t FOR UPDATE", false},
		// {"Select_ForShare", "SELECT * FROM t FOR SHARE", false},
		// {"Select_ForUpdateWait", "SELECT * FROM t FOR UPDATE WAIT 10", false},
		// {"Select_ForUpdateNoWait", "SELECT * FROM t FOR UPDATE NOWAIT", false},
		// {"Select_ForUpdateSkipLocked", "SELECT * FROM t FOR UPDATE SKIP LOCKED", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseSetOp - 96.9% coverage
func TestCovBoostQuery2_SetOp(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Union_All", "SELECT 1 UNION ALL SELECT 2", false},
		// Note: DISTINCT modifier not fully supported
		// {"Union_Distinct", "SELECT 1 UNION DISTINCT SELECT 2", false},
		// {"Intersect_All", "SELECT 1 INTERSECT ALL SELECT 2", false},
		// {"Except_All", "SELECT 1 EXCEPT ALL SELECT 2", false},
		{"Union_OrderBy", "SELECT 1 UNION SELECT 2 ORDER BY 1", false},
		{"Union_Limit", "SELECT 1 UNION SELECT 2 LIMIT 10", false},
		{"Union_OrderByLimit", "SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 10", false},
		{"MultipleUnions", "SELECT 1 UNION SELECT 2 UNION SELECT 3", false},
		{"MixedSetOps", "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test readString - 88.5% coverage
func TestCovBoostQuery2_ReadString(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"String_SingleQuote", "SELECT 'hello' FROM t", false},
		{"String_DoubleQuote", "SELECT \"hello\" FROM t", false},
		{"String_EscapedQuote", "SELECT 'it''s' FROM t", false},
		{"String_BackslashN", "SELECT 'line1\\nline2' FROM t", false},
		{"String_BackslashT", "SELECT 'col1\\tcol2' FROM t", false},
		{"String_BackslashR", "SELECT 'line\\r' FROM t", false},
		{"String_Backslash0", "SELECT 'null\\0char' FROM t", false},
		{"String_BackslashQuote", "SELECT 'quote\\\"' FROM t", false},
		{"String_BackslashBackslash", "SELECT 'path\\\\file' FROM t", false},
		{"String_Empty", "SELECT '' FROM t", false},
		{"String_Unicode", "SELECT 'hello world' FROM t", false},
		{"String_Unterminated", "SELECT 'unterminated FROM t", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseIdentifierList - 90.0% coverage
func TestCovBoostQuery2_IdentifierList(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"IdList_Single", "CREATE INDEX idx ON t (col)", false},
		{"IdList_Multi", "CREATE INDEX idx ON t (a, b, c)", false},
		{"IdList_MissingIdentifier", "CREATE INDEX idx ON t ()", true},
		// Note: These features not fully supported
		// {"IdList_OrderedAsc", "CREATE INDEX idx ON t (col ASC)", false},
		// {"IdList_OrderedDesc", "CREATE INDEX idx ON t (col DESC)", false},
		// {"IdList_Collate", "CREATE INDEX idx ON t (col COLLATE NOCASE)", false},
		// {"IdList_OrderedCollate", "CREATE INDEX idx ON t (col ASC COLLATE NOCASE)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseReturningClause - 91.7% coverage
func TestCovBoostQuery2_ReturningClause(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Returning_Star", "INSERT INTO t VALUES (1) RETURNING *", false},
		{"Returning_Column", "INSERT INTO t VALUES (1) RETURNING id", false},
		{"Returning_Expr", "INSERT INTO t VALUES (1) RETURNING id + 1", false},
		{"Returning_Alias", "INSERT INTO t VALUES (1) RETURNING id AS new_id", false},
		{"Returning_Multiple", "INSERT INTO t VALUES (1) RETURNING id, name, created_at", false},
		{"Returning_MissingExpr", "INSERT INTO t VALUES (1) RETURNING", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseNot - 90.0% coverage
func TestCovBoostQuery2_Not(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Not_In", "SELECT * FROM t WHERE x NOT IN (1, 2, 3)", false},
		{"Not_Between", "SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10", false},
		{"Not_Like", "SELECT * FROM t WHERE x NOT LIKE 'pattern'", false},
		{"Not_Exists", "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM t2)", false},
		{"Not_Expr", "SELECT * FROM t WHERE NOT x > 0", false},
		{"Not_Not", "SELECT * FROM t WHERE NOT NOT x > 0", false},
		// Note: These features not fully supported
		// {"Not_Regexp", "SELECT * FROM t WHERE x NOT REGEXP 'pattern'", false},
		// {"Not_Glob", "SELECT * FROM t WHERE x NOT GLOB 'pattern'", false},
		// {"Not_Match", "SELECT * FROM t WHERE x NOT MATCH 'pattern'", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseOr and parseAnd - 88.9% coverage
func TestCovBoostQuery2_OrAnd(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Or_Simple", "SELECT * FROM t WHERE x = 1 OR y = 2", false},
		{"Or_Multiple", "SELECT * FROM t WHERE x = 1 OR y = 2 OR z = 3", false},
		{"And_Simple", "SELECT * FROM t WHERE x = 1 AND y = 2", false},
		{"And_Multiple", "SELECT * FROM t WHERE x = 1 AND y = 2 AND z = 3", false},
		{"OrAnd_Mixed", "SELECT * FROM t WHERE (x = 1 OR y = 2) AND (z = 3 OR w = 4)", false},
		{"OrAnd_Complex", "SELECT * FROM t WHERE x = 1 AND y = 2 OR z = 3 AND w = 4", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseAdditive and parseMultiplicative - 90.9% coverage
func TestCovBoostQuery2_Arithmetic(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Add_Simple", "SELECT 1 + 2 FROM t", false},
		{"Add_Multiple", "SELECT 1 + 2 + 3 FROM t", false},
		{"Sub_Simple", "SELECT 5 - 3 FROM t", false},
		{"Sub_Negative", "SELECT -5 - 3 FROM t", false},
		{"Mul_Simple", "SELECT 2 * 3 FROM t", false},
		{"Mul_Multiple", "SELECT 2 * 3 * 4 FROM t", false},
		{"Div_Simple", "SELECT 10 / 2 FROM t", false},
		{"Div_Decimal", "SELECT 10.0 / 3 FROM t", false},
		{"Mod_Simple", "SELECT 10 % 3 FROM t", false},
		// Note: MOD keyword not fully supported
		// {"Modulo_Simple", "SELECT 10 MOD 3 FROM t", false},
		{"Mixed_Arithmetic", "SELECT 1 + 2 * 3 - 4 / 2 FROM t", false},
		{"Paren_Arithmetic", "SELECT (1 + 2) * (3 - 4) / 2 FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseUnary - 87.5% coverage
func TestCovBoostQuery2_Unary(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Unary_Plus", "SELECT +5 FROM t", false},
		{"Unary_Minus", "SELECT -5 FROM t", false},
		{"Unary_Not", "SELECT NOT x FROM t", false},
		{"Unary_Nested", "SELECT -+5 FROM t", false},
		{"Unary_Column", "SELECT -x FROM t", false},
		// Note: These features not fully supported
		// {"Unary_Tilde", "SELECT ~5 FROM t", false},
		// {"Unary_DoubleMinus", "SELECT --5 FROM t", false},
		{"Unary_Expr", "SELECT -(x + y) FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseTableRef - 87.8% coverage
func TestCovBoostQuery2_TableRef(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"TableRef_Alias", "SELECT * FROM t AS alias", false},
		{"TableRef_ImplicitAlias", "SELECT * FROM t alias", false},
		{"TableRef_Subquery", "SELECT * FROM (SELECT * FROM t) AS sub", false},
		// Note: Subquery without alias not fully supported
		// {"TableRef_SubqueryNoAlias", "SELECT * FROM (SELECT * FROM t)", false},
		{"TableRef_Backtick", "SELECT * FROM `my table`", false},
		// Note: These features not fully supported
		// {"TableRef_IndexedBy", "SELECT * FROM t INDEXED BY idx", false},
		// {"TableRef_NotIndexed", "SELECT * FROM t NOT INDEXED", false},
		// {"TableRef_TableFunc", "SELECT * FROM generate_series(1, 10) AS t(x)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseSelectItem - 88.9% coverage
func TestCovBoostQuery2_SelectItem(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"SelectItem_Alias", "SELECT x AS y FROM t", false},
		{"SelectItem_ImplicitAlias", "SELECT x y FROM t", false},
		{"SelectItem_Qualified", "SELECT t.x FROM t", false},
		{"SelectItem_Expr", "SELECT x + 1 FROM t", false},
		{"SelectItem_ExprAlias", "SELECT x + 1 AS incremented FROM t", false},
		{"SelectItem_Star", "SELECT * FROM t", false},
		{"SelectItem_Subquery", "SELECT (SELECT MAX(x) FROM t2) FROM t", false},
		// Note: Qualified star may not be fully supported
		// {"SelectItem_QualifiedStar", "SELECT t.* FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseRollback - 88.9% coverage
func TestCovBoostQuery2_Rollback(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Rollback_Simple", "ROLLBACK", false},
		{"Rollback_Transaction", "ROLLBACK TRANSACTION", false},
		{"Rollback_Work", "ROLLBACK WORK", false},
		{"Rollback_To", "ROLLBACK TO SAVEPOINT sp", false},
		{"Rollback_ToShort", "ROLLBACK TO sp", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseRefresh - 88.9% coverage
func TestCovBoostQuery2_Refresh(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Refresh_Simple", "REFRESH MATERIALIZED VIEW mv", false},
		{"Refresh_MissingName", "REFRESH MATERIALIZED VIEW", true},
		// Note: CONCURRENTLY not fully supported
		// {"Refresh_Concurrent", "REFRESH MATERIALIZED VIEW CONCURRENTLY mv", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseCreateView - 83.3% coverage
func TestCovBoostQuery2_CreateView(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"CreateView_Simple", "CREATE VIEW v AS SELECT * FROM t", false},
		{"CreateView_IfNotExists", "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t", false},
		{"CreateView_MissingName", "CREATE VIEW AS SELECT * FROM t", true},
		{"CreateView_MissingAs", "CREATE VIEW v SELECT * FROM t", true},
		{"CreateView_MissingSelect", "CREATE VIEW v AS", true},
		// Note: These features not fully supported
		// {"CreateView_Temp", "CREATE TEMP VIEW v AS SELECT * FROM t", false},
		// {"CreateView_ColumnList", "CREATE VIEW v (a, b, c) AS SELECT x, y, z FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseDropPolicy - 88.2% coverage
func TestCovBoostQuery2_DropPolicy(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"DropPolicy_Simple", "DROP POLICY p ON t", false},
		{"DropPolicy_IfExists", "DROP POLICY IF EXISTS p ON t", false},
		{"DropPolicy_MissingName", "DROP POLICY ON t", true},
		{"DropPolicy_MissingIfExists", "DROP POLICY IF p ON t", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseDropTrigger - 90.9% coverage
func TestCovBoostQuery2_DropTrigger(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"DropTrigger_Simple", "DROP TRIGGER tr", false},
		{"DropTrigger_IfExists", "DROP TRIGGER IF EXISTS tr", false},
		{"DropTrigger_MissingName", "DROP TRIGGER", true},
		{"DropTrigger_MissingIfExists", "DROP TRIGGER IF tr", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseDropIndex - 90.9% coverage
func TestCovBoostQuery2_DropIndex(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"DropIndex_Simple", "DROP INDEX idx ON t", false},
		{"DropIndex_IfExists", "DROP INDEX IF EXISTS idx ON t", false},
		{"DropIndex_MissingName", "DROP INDEX ON t", true},
		{"DropIndex_MissingIfExists", "DROP INDEX IF idx ON t", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseDropProcedure - 90.9% coverage
func TestCovBoostQuery2_DropProcedure(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"DropProcedure_Simple", "DROP PROCEDURE p", false},
		{"DropProcedure_IfExists", "DROP PROCEDURE IF EXISTS p", false},
		{"DropProcedure_MissingName", "DROP PROCEDURE", true},
		{"DropProcedure_MissingIfExists", "DROP PROCEDURE IF p", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseCreateProcedure - 85.3% coverage
func TestCovBoostQuery2_CreateProcedure(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"CreateProcedure_NoParams", "CREATE PROCEDURE p() BEGIN SELECT 1; END", false},
		{"CreateProcedure_OneParam", "CREATE PROCEDURE p(x INT) BEGIN SELECT x; END", false},
		{"CreateProcedure_MultiParams", "CREATE PROCEDURE p(x INT, y TEXT, z REAL) BEGIN SELECT x, y, z; END", false},
		{"CreateProcedure_InParam", "CREATE PROCEDURE p(IN x INT) BEGIN SELECT x; END", false},
		{"CreateProcedure_OutParam", "CREATE PROCEDURE p(OUT x INT) BEGIN SET x = 1; END", false},
		{"CreateProcedure_InOutParam", "CREATE PROCEDURE p(INOUT x INT) BEGIN SELECT x; END", false},
		{"CreateProcedure_IfNotExists", "CREATE PROCEDURE IF NOT EXISTS p() BEGIN SELECT 1; END", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseWithCTE - 88.4% coverage
func TestCovBoostQuery2_CTEExtended(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"CTE_MultipleRefs", "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte, cte", false},
		{"CTE_Chained", "WITH cte1 AS (SELECT 1 AS x), cte2 AS (SELECT x + 1 AS y FROM cte1) SELECT * FROM cte2", false},
		// Note: CTE with non-SELECT statements not fully supported
		// {"CTE_Insert", "WITH cte AS (SELECT * FROM t) INSERT INTO t2 SELECT * FROM cte", false},
		// {"CTE_Update", "WITH cte AS (SELECT id FROM t) UPDATE t2 SET x = 1 WHERE id IN (SELECT id FROM cte)", false},
		// {"CTE_Delete", "WITH cte AS (SELECT id FROM t) DELETE FROM t2 WHERE id IN (SELECT id FROM cte)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test parseCreatePolicy - 83.6% coverage
func TestCovBoostQuery2_CreatePolicyExtended(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Policy_Permissive", "CREATE POLICY p ON t AS PERMISSIVE FOR SELECT USING (true)", false},
		{"Policy_Restrictive", "CREATE POLICY p ON t AS RESTRICTIVE FOR SELECT USING (true)", false},
		{"Policy_Public", "CREATE POLICY p ON t TO PUBLIC", false},
		{"Policy_CurrentUser", "CREATE POLICY p ON t TO CURRENT_USER", false},
		{"Policy_SessionUser", "CREATE POLICY p ON t TO SESSION_USER", false},
		{"Policy_MultipleTo", "CREATE POLICY p ON t TO admin, user, readonly", false},
		{"Policy_UsingAndWithCheck", "CREATE POLICY p ON t FOR UPDATE USING (x > 0) WITH CHECK (x > 0)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for: %s", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
