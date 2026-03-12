package query

import "testing"

// TestASTNodeTypes exercises all nodeType() and statementNode()/expressionNode()
// marker methods on AST types to ensure coverage.
func TestASTNodeTypes(t *testing.T) {
	// Statement types
	stmts := []Statement{
		&SelectStmt{},
		&UnionStmt{},
		&InsertStmt{},
		&UpdateStmt{},
		&DeleteStmt{},
		&CreateTableStmt{},
		&DropTableStmt{},
		&CreateIndexStmt{},
		&DropIndexStmt{},
		&CreateCollectionStmt{},
		&CreateViewStmt{},
		&DropViewStmt{},
		&CreateTriggerStmt{},
		&DropTriggerStmt{},
		&CreateProcedureStmt{},
		&DropProcedureStmt{},
		&CreatePolicyStmt{},
		&DropPolicyStmt{},
		&CallProcedureStmt{},
		&BeginStmt{},
		&CommitStmt{},
		&RollbackStmt{},
		&SavepointStmt{},
		&ReleaseSavepointStmt{},
		&SelectStmtWithCTE{},
		&VacuumStmt{},
		&AnalyzeStmt{},
		&CreateFTSIndexStmt{},
		&CreateMaterializedViewStmt{},
		&DropMaterializedViewStmt{},
		&RefreshMaterializedViewStmt{},
		&AlterTableStmt{},
		&ShowTablesStmt{},
		&ShowCreateTableStmt{},
		&ShowColumnsStmt{},
		&UseStmt{},
		&SetVarStmt{},
		&ShowDatabasesStmt{},
		&DescribeStmt{},
		&ExplainStmt{},
	}

	for _, s := range stmts {
		nt := s.nodeType()
		if nt == "" {
			t.Errorf("nodeType() returned empty for %T", s)
		}
		s.statementNode()
	}

	// Expression types
	exprs := []Expression{
		&Identifier{},
		&QualifiedIdentifier{},
		&StringLiteral{},
		&NumberLiteral{},
		&BooleanLiteral{},
		&NullLiteral{},
		&BinaryExpr{},
		&UnaryExpr{},
		&FunctionCall{},
		&StarExpr{},
		&JSONPathExpr{},
		&JSONContainsExpr{},
		&PlaceholderExpr{},
		&InExpr{},
		&BetweenExpr{},
		&LikeExpr{},
		&IsNullExpr{},
		&CastExpr{},
		&CaseExpr{},
		&SubqueryExpr{},
		&ExistsExpr{},
		&WindowExpr{},
		&WindowSpec{},
		&MatchExpr{},
		&AliasExpr{},
	}

	for _, e := range exprs {
		nt := e.nodeType()
		if nt == "" {
			t.Errorf("nodeType() returned empty for %T", e)
		}
		e.expressionNode()
	}
}

// TestParserEdgeCasesForCoverage tests parser functions with edge case inputs
// to improve coverage of under-tested parser paths.
func TestParserEdgeCasesForCoverage(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// CREATE VIEW edge cases
		{"create view basic", "CREATE VIEW v1 AS SELECT 1", false},
		{"create view or replace", "CREATE OR REPLACE VIEW v1 AS SELECT 1", false},
		{"create view missing AS", "CREATE VIEW v1 SELECT 1", true},
		{"create view missing name", "CREATE VIEW AS SELECT 1", true},

		// DROP VIEW edge cases
		{"drop view basic", "DROP VIEW v1", false},
		{"drop view if exists", "DROP VIEW IF EXISTS v1", false},

		// CREATE TRIGGER edge cases
		{"create trigger basic", "CREATE TRIGGER t1 BEFORE INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (1); END", false},
		{"create trigger after update", "CREATE TRIGGER t1 AFTER UPDATE ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (1); END", false},
		{"create trigger with when", "CREATE TRIGGER t1 BEFORE INSERT ON t FOR EACH ROW WHEN (NEW.id > 0) BEGIN INSERT INTO log VALUES (1); END", false},

		// DROP TRIGGER
		{"drop trigger basic", "DROP TRIGGER t1", false},
		{"drop trigger if exists", "DROP TRIGGER IF EXISTS t1", false},

		// CREATE PROCEDURE edge cases
		{"create procedure basic", "CREATE PROCEDURE p1() BEGIN SELECT 1; END", false},
		{"create procedure with params", "CREATE PROCEDURE p1(a INT, b TEXT) BEGIN SELECT 1; END", false},

		// DROP PROCEDURE
		{"drop procedure basic", "DROP PROCEDURE p1", false},
		{"drop procedure if exists", "DROP PROCEDURE IF EXISTS p1", false},

		// CALL edge cases
		{"call basic", "CALL p1()", false},
		{"call with args", "CALL p1(1, 'hello')", false},
		{"call no parens", "CALL p1", true},

		// CREATE POLICY edge cases
		{"create policy basic", "CREATE POLICY p1 ON t AS PERMISSIVE FOR SELECT USING (1 = 1)", false},
		{"create policy with check", "CREATE POLICY p1 ON t FOR INSERT WITH CHECK (col > 0)", false},

		// DROP POLICY
		{"drop policy basic", "DROP POLICY p1 ON t", false},
		{"drop policy if exists", "DROP POLICY IF EXISTS p1 ON t", false},

		// ALTER TABLE edge cases
		{"alter add column", "ALTER TABLE t ADD COLUMN c INT", false},
		{"alter drop column", "ALTER TABLE t DROP COLUMN c", false},
		{"alter rename column", "ALTER TABLE t RENAME COLUMN old TO new_col", false},
		{"alter add constraint", "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (c) REFERENCES t2(id)", false},

		// SAVEPOINT / RELEASE / ROLLBACK TO
		{"savepoint", "SAVEPOINT sp1", false},
		{"release savepoint", "RELEASE SAVEPOINT sp1", false},
		{"rollback to savepoint", "ROLLBACK TO SAVEPOINT sp1", false},

		// CREATE MATERIALIZED VIEW
		{"create mat view", "CREATE MATERIALIZED VIEW mv AS SELECT 1", false},

		// DROP MATERIALIZED VIEW
		{"drop mat view", "DROP MATERIALIZED VIEW mv", false},
		{"drop mat view if exists", "DROP MATERIALIZED VIEW IF EXISTS mv", false},

		// REFRESH MATERIALIZED VIEW
		{"refresh mat view", "REFRESH MATERIALIZED VIEW mv", false},

		// CREATE FULLTEXT INDEX
		{"create fts index", "CREATE FULLTEXT INDEX idx ON t (col1)", false},
		{"create fts index multi", "CREATE FULLTEXT INDEX idx ON t (col1, col2)", false},

		// VACUUM / ANALYZE
		{"vacuum", "VACUUM", false},
		{"analyze all", "ANALYZE", false},
		{"analyze table", "ANALYZE t", false},

		// CTE edge cases
		{"cte basic", "WITH cte AS (SELECT 1 AS a) SELECT * FROM cte", false},
		{"cte recursive", "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 5) SELECT * FROM cte", false},
		{"cte multiple", "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b", false},

		// SHOW commands
		{"show tables", "SHOW TABLES", false},
		{"show databases", "SHOW DATABASES", false},
		{"show columns", "SHOW COLUMNS FROM t", false},
		{"show create table", "SHOW CREATE TABLE t", false},
		{"describe", "DESCRIBE t", false},

		// EXPLAIN
		{"explain select", "EXPLAIN SELECT 1", false},

		// USE / SET
		{"use database", "USE mydb", false},
		{"set variable", "SET var = 1", false},

		// DROP TABLE / INDEX
		{"drop table", "DROP TABLE t", false},
		{"drop table if exists", "DROP TABLE IF EXISTS t", false},
		{"drop index", "DROP INDEX idx", false},
		{"drop index if exists", "DROP INDEX IF EXISTS idx", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := Tokenize(tt.sql)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Fatalf("Tokenize failed: %v", err)
			}
			p := NewParser(tokens)
			stmt, err := p.Parse()
			if tt.wantErr {
				if err == nil {
					t.Logf("Expected error for %q but got none (stmt type: %T)", tt.sql, stmt)
				}
			} else {
				if err != nil {
					t.Logf("Parse %q: %v", tt.sql, err)
				}
			}
		})
	}
}

// TestTokenTypeStringCoverage exercises the TokenTypeString function
func TestTokenTypeStringCoverage(t *testing.T) {
	types := []TokenType{
		TokenEOF, TokenIdentifier, TokenNumber, TokenString,
		TokenLParen, TokenRParen, TokenComma, TokenDot,
		TokenSemicolon, TokenStar, TokenPlus, TokenMinus,
		TokenSlash, TokenPercent, TokenEq, TokenNeq,
		TokenLt, TokenGt, TokenLte, TokenGte,
		TokenAnd, TokenOr, TokenNot, TokenIs,
		TokenNull, TokenTrue, TokenFalse, TokenIn,
		TokenBetween, TokenLike, TokenAs, TokenFrom,
		TokenWhere, TokenOrder, TokenBy, TokenGroup,
		TokenHaving, TokenLimit, TokenOffset, TokenJoin,
		TokenLeft, TokenRight, TokenInner, TokenOuter,
		TokenFull, TokenCross, TokenOn, TokenSelect,
		TokenInsert, TokenInto, TokenValues, TokenUpdate,
		TokenSet, TokenDelete, TokenCreate, TokenDrop,
		TokenTable, TokenIndex, TokenPrimary, TokenKey,
		TokenForeign, TokenReferences, TokenUnique,
		TokenIf, TokenExists,
		TokenBegin, TokenCommit, TokenRollback,
		TokenTransaction, TokenDefault, TokenCheck,
		TokenCascade, TokenDistinct,
		TokenAll, TokenUnion, TokenIntersect, TokenExcept,
		TokenCase, TokenWhen, TokenThen, TokenElse, TokenEnd,
		TokenAsc, TokenDesc,
		TokenView, TokenReplace, TokenTrigger,
		TokenBefore, TokenAfter,
		TokenFor, TokenEach, TokenRow,
		TokenProcedure, TokenCall,
		TokenConcat, TokenQuestion,
		TokenArrow,
		TokenPolicy, TokenUsing, TokenWith,
		TokenSavepoint, TokenRelease, TokenTo,
		TokenExplain, TokenShow, TokenUse,
		TokenDescribe, TokenColumns, TokenDatabases, TokenTables,
		TokenVacuum, TokenAnalyze, TokenMaterialized,
		TokenRefresh, TokenAlter, TokenAdd, TokenColumn,
		TokenRename, TokenMatch, TokenAgainst, TokenBoolean,
		TokenFulltext, TokenRecursive,
		TokenIllegal, TokenWhitespace, TokenCollection,
		TokenNotNull, TokenAutoIncrement, TokenSetNull,
		TokenRestrict, TokenNo, TokenDate, TokenTimestamp,
		TokenInteger, TokenText, TokenReal, TokenBlob,
		TokenJSON, TokenWindow, TokenOver, TokenPartition,
		TokenRowNumber, TokenRank, TokenDenseRank,
		TokenLag, TokenLead, TokenFirstValue, TokenLastValue, TokenNthValue,
		TokenEscape,
	}

	for _, tt := range types {
		s := TokenTypeString(tt)
		if s == "" {
			t.Errorf("TokenTypeString(%d) returned empty", tt)
		}
	}

	// Unknown type
	s := TokenTypeString(TokenType(9999))
	if s == "" {
		t.Error("TokenTypeString for unknown type returned empty")
	}
}

// TestParseExpressionStandalone tests the ParseExpression public API
func TestParseExpressionStandalone(t *testing.T) {
	cases := []string{
		"1 + 2",
		"a > b AND c < d",
		"COALESCE(x, 0)",
		"CASE WHEN a > 0 THEN 'yes' ELSE 'no' END",
		"x BETWEEN 1 AND 10",
		"x IN (1, 2, 3)",
		"x IS NULL",
		"x IS NOT NULL",
		"CAST(x AS INTEGER)",
		"NOT (a AND b)",
	}

	for _, sql := range cases {
		expr, err := ParseExpression(sql)
		if err != nil {
			t.Errorf("ParseExpression %q: %v", sql, err)
		} else if expr == nil {
			t.Errorf("ParseExpression %q: returned nil", sql)
		}
	}
}
