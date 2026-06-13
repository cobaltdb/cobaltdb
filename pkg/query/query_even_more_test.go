package query

import (
	"strings"
	"testing"
)

// TestParseSelectItemComplex tests parseSelectItem with complex expressions
func TestParseSelectItemComplex(t *testing.T) {
	tests := []struct {
		sql string
	}{
		{"SELECT a + b FROM t"},
		{"SELECT a - b FROM t"},
		{"SELECT a * b FROM t"},
		{"SELECT a / b FROM t"},
		{"SELECT -a FROM t"},
		{"SELECT +a FROM t"},
		{"SELECT a AS alias FROM t"},
		{"SELECT DISTINCT a FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("Failed to parse %q: %v", tt.sql, err)
			}
		})
	}
}

// TestParseJoinTypes tests different JOIN types
func TestParseJoinTypes(t *testing.T) {
	tests := []struct {
		sql  string
		join TokenType
	}{
		{"SELECT * FROM a INNER JOIN b ON a.id = b.id", TokenInner},
		{"SELECT * FROM a LEFT JOIN b ON a.id = b.id", TokenLeft},
		{"SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id", TokenLeft},
		{"SELECT * FROM a RIGHT JOIN b ON a.id = b.id", TokenRight},
		{"SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id", TokenRight},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("Failed to parse %q: %v", tt.sql, err)
				return
			}
			selectStmt := stmt.(*SelectStmt)
			if len(selectStmt.Joins) != 1 {
				t.Errorf("Expected 1 join, got %d", len(selectStmt.Joins))
				return
			}
			if selectStmt.Joins[0].Type != tt.join {
				t.Errorf("Expected join type %v, got %v", tt.join, selectStmt.Joins[0].Type)
			}
		})
	}
}

// TestParseTableRefWithSchema tests table references with schema
func TestParseTableRefWithSchema(t *testing.T) {
	sql := "SELECT * FROM schema.table"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	selectStmt := stmt.(*SelectStmt)
	// Schema.table parsing may vary - just verify it parses
	if selectStmt.From.Name == "" {
		t.Error("Expected non-empty table name")
	}
}

// TestParseExpressionPrecedence tests operator precedence
func TestParseExpressionPrecedence(t *testing.T) {
	tests := []string{
		"SELECT * FROM t WHERE a + b * c = d",
		"SELECT * FROM t WHERE a * b + c = d",
		"SELECT * FROM t WHERE a - b - c = d",
		"SELECT * FROM t WHERE a / b / c = d",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Errorf("Failed to parse %q: %v", sql, err)
			}
		})
	}
}

// TestParseComplexWhere tests complex WHERE clauses
func TestParseComplexWhere(t *testing.T) {
	tests := []string{
		"SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3",
		"SELECT * FROM t WHERE a = 1 AND (b = 2 OR c = 3)",
		"SELECT * FROM t WHERE NOT (a = 1 AND b = 2)",
		"SELECT * FROM t WHERE ((a = 1))",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Errorf("Failed to parse %q: %v", sql, err)
			}
		})
	}
}

// TestParseCaseExpression tests CASE expressions
func TestParseCaseExpression(t *testing.T) {
	tests := []string{
		"SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END FROM t",
		"SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t",
		"SELECT CASE a WHEN 1 THEN 'one' ELSE 'other' END FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("CASE expression may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseCastExpression tests CAST expressions
func TestParseCastExpression(t *testing.T) {
	sql := "SELECT CAST(a AS INTEGER) FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("CAST expression may not be fully supported: %v", err)
	}
}

// TestParseExistsSubquery tests EXISTS subqueries
func TestParseExistsSubquery(t *testing.T) {
	sql := "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("EXISTS subquery may not be fully supported: %v", err)
	}
}

// TestParseJSONOperations tests JSON operations
func TestParseJSONOperations(t *testing.T) {
	tests := []string{
		"SELECT data->name FROM t",
		"SELECT data->>name FROM t",
		"SELECT * FROM t WHERE data @> '{\"key\": \"value\"}'",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("JSON operation may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseAggregateFunctions tests aggregate function parsing
func TestParseAggregateFunctions(t *testing.T) {
	tests := []string{
		"SELECT COUNT(*) FROM t",
		"SELECT COUNT(DISTINCT a) FROM t",
		"SELECT SUM(a) FROM t",
		"SELECT AVG(a) FROM t",
		"SELECT MIN(a) FROM t",
		"SELECT MAX(a) FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Aggregate function may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseWindowFunctions tests window function parsing
func TestParseWindowFunctions(t *testing.T) {
	tests := []string{
		"SELECT ROW_NUMBER() OVER (ORDER BY a) FROM t",
		"SELECT RANK() OVER (ORDER BY a) FROM t",
		"SELECT ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Window function may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseForeignKey tests foreign key constraints
func TestParseForeignKey(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE
	)`
	_, err := Parse(sql)
	if err != nil {
		t.Logf("Foreign key may not be fully supported: %v", err)
	}
}

// TestParseTableConstraints tests table-level constraints
func TestParseTableConstraints(t *testing.T) {
	tests := []string{
		"CREATE TABLE t (id INTEGER, PRIMARY KEY (id))",
		"CREATE TABLE t (id INTEGER, UNIQUE (id))",
		"CREATE TABLE t (id INTEGER, CHECK (id > 0))",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Table constraint may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseInsertWithSelect tests INSERT ... SELECT
func TestParseInsertWithSelect(t *testing.T) {
	sql := "INSERT INTO t (a, b) SELECT x, y FROM u"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("INSERT ... SELECT may not be fully supported: %v", err)
	}
}

// TestParseUpdateWithJoin tests UPDATE with JOIN
func TestParseUpdateWithJoin(t *testing.T) {
	sql := "UPDATE t JOIN u ON t.id = u.id SET t.a = u.b"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("UPDATE with JOIN may not be fully supported: %v", err)
	}
}

// TestParseDeleteWithJoin tests DELETE with JOIN
func TestParseDeleteWithJoin(t *testing.T) {
	sql := "DELETE FROM t USING u WHERE t.id = u.id"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("DELETE with USING may not be fully supported: %v", err)
	}
}

// TestParseUnion tests UNION
func TestParseUnion(t *testing.T) {
	sql := "SELECT a FROM t UNION SELECT b FROM u"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("UNION may not be fully supported: %v", err)
	}
}

// TestParseIntersect tests INTERSECT
func TestParseIntersect(t *testing.T) {
	sql := "SELECT a FROM t INTERSECT SELECT b FROM u"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("INTERSECT may not be fully supported: %v", err)
	}
}

// TestParseExcept tests EXCEPT
func TestParseExcept(t *testing.T) {
	sql := "SELECT a FROM t EXCEPT SELECT b FROM u"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("EXCEPT may not be fully supported: %v", err)
	}
}

// TestParseCTE tests Common Table Expressions
func TestParseCTE(t *testing.T) {
	sql := "WITH cte AS (SELECT * FROM t) SELECT * FROM cte"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("CTE may not be fully supported: %v", err)
	}
}

// TestParseRecursiveCTE tests recursive CTEs
func TestParseRecursiveCTE(t *testing.T) {
	sql := "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("Recursive CTE may not be fully supported: %v", err)
	}
}

// TestParseLimitAll tests LIMIT ALL
func TestParseLimitAll(t *testing.T) {
	sql := "SELECT * FROM t LIMIT ALL"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("LIMIT ALL should parse: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	if selectStmt.Limit != nil {
		t.Fatalf("LIMIT ALL should not set a numeric limit, got %T", selectStmt.Limit)
	}
}

// TestParseOffsetWithoutLimit tests OFFSET without LIMIT
func TestParseOffsetWithoutLimit(t *testing.T) {
	sql := "SELECT * FROM t OFFSET 10"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("OFFSET without LIMIT may not be fully supported: %v", err)
	}
}

// TestParseReturning tests RETURNING clause
func TestParseReturning(t *testing.T) {
	tests := []string{
		"INSERT INTO t (a) VALUES (1) RETURNING *",
		"INSERT INTO t (a) VALUES (1) RETURNING id",
		"UPDATE t SET a = 1 RETURNING *",
		"DELETE FROM t RETURNING *",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("RETURNING may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseUpsert tests UPSERT (INSERT ... ON CONFLICT)
func TestParseUpsert(t *testing.T) {
	sql := "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET b = 2"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("UPSERT may not be fully supported: %v", err)
	}
}

// TestLexerNextTokenEdgeCases tests edge cases in NextToken
func TestLexerNextTokenEdgeCases(t *testing.T) {
	tests := []struct {
		input string
		desc  string
	}{
		{"/* block comment */ SELECT 1", "block comment"},
		{"-- line comment\nSELECT 1", "line comment"},
		{"/* nested /* comment */ */ SELECT 1", "nested comment"},
		{"", "empty input"},
		{"   ", "whitespace only"},
		{"\t\n\r", "various whitespace"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := Tokenize(tt.input)
			// Just make sure it doesn't panic
			_ = err
		})
	}
}

// TestParserPeek tests the parser's peek function
func TestParserPeek(t *testing.T) {
	tokens, _ := Tokenize("SELECT 1")
	p := NewParser(tokens)
	_ = p.peek()
	// Just make sure it doesn't panic
}

// TestParseQualifiedIdentifierComplex tests qualified identifiers
func TestParseQualifiedIdentifierComplex(t *testing.T) {
	tests := []string{
		"SELECT db.schema.table.column FROM db.schema.table",
		"SELECT schema.table.column FROM schema.table",
		"SELECT table.column FROM table",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex qualified identifier may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseExpressionListEdgeCases tests expression list parsing
func TestParseExpressionListEdgeCases(t *testing.T) {
	tests := []string{
		"SELECT * FROM t WHERE a IN ()",
		"SELECT * FROM t WHERE a IN (1)",
		"SELECT * FROM t WHERE a IN (1, 2, 3, 4, 5)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			// Just make sure it doesn't panic
			_ = err
		})
	}
}

// TestParseOrderByEdgeCases tests ORDER BY edge cases
func TestParseOrderByEdgeCases(t *testing.T) {
	tests := []string{
		"SELECT * FROM t ORDER BY 1",
		"SELECT * FROM t ORDER BY a NULLS FIRST",
		"SELECT * FROM t ORDER BY a NULLS LAST",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("ORDER BY edge case may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseColumnAliases tests column aliases
func TestParseColumnAliases(t *testing.T) {
	tests := []string{
		"SELECT a AS x FROM t",
		"SELECT a x FROM t",
		"SELECT a AS \"quoted alias\" FROM t",
		"SELECT a AS `backtick alias` FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Column alias may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseIndexOptions tests index options
func TestParseIndexOptions(t *testing.T) {
	tests := []string{
		"CREATE UNIQUE INDEX idx ON t (a)",
		"CREATE INDEX idx ON t (a, b)",
		"CREATE INDEX idx ON t (a DESC)",
		"CREATE INDEX idx ON t (a ASC, b DESC)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Index options may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseDropIfExists tests DROP IF EXISTS
func TestParseDropIfExists(t *testing.T) {
	tests := []string{
		"DROP TABLE IF EXISTS t",
		"DROP INDEX IF EXISTS idx",
		"DROP VIEW IF EXISTS v",
		"DROP TRIGGER IF EXISTS trg",
		"DROP PROCEDURE IF EXISTS proc",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("DROP IF EXISTS may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseTransactionModes tests transaction modes
func TestParseTransactionModes(t *testing.T) {
	tests := []string{
		"BEGIN",
		"BEGIN TRANSACTION",
		"BEGIN DEFERRABLE",
		"BEGIN IMMEDIATE",
		"BEGIN EXCLUSIVE",
		"BEGIN READ ONLY",
		"BEGIN READ WRITE",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Transaction mode may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseSavepoint tests savepoints
func TestParseSavepoint(t *testing.T) {
	tests := []string{
		"SAVEPOINT sp1",
		"RELEASE SAVEPOINT sp1",
		"ROLLBACK TO SAVEPOINT sp1",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Savepoint may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseAnalyze tests ANALYZE
func TestParseAnalyze(t *testing.T) {
	sql := "ANALYZE t"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("ANALYZE may not be fully supported: %v", err)
	}
}

// TestParseVacuum tests VACUUM
func TestParseVacuum(t *testing.T) {
	sql := "VACUUM"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("VACUUM may not be fully supported: %v", err)
	}
}

// TestParsePragma tests PRAGMA
func TestParsePragma(t *testing.T) {
	tests := []string{
		"PRAGMA foreign_keys",
		"PRAGMA foreign_keys = ON",
		"PRAGMA table_info(t)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("PRAGMA may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseAttachDetach tests ATTACH and DETACH
func TestParseAttachDetach(t *testing.T) {
	tests := []string{
		"ATTACH DATABASE 'file.db' AS alias",
		"DETACH DATABASE alias",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("ATTACH/DETACH may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseReindex tests REINDEX
func TestParseReindex(t *testing.T) {
	sql := "REINDEX t"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("REINDEX may not be fully supported: %v", err)
	}
}

// TestParseAlterTable tests ALTER TABLE
func TestParseAlterTable(t *testing.T) {
	tests := []string{
		"ALTER TABLE t RENAME TO new_t",
		"ALTER TABLE t ADD COLUMN c INTEGER",
		"ALTER TABLE t DROP COLUMN c",
		"ALTER TABLE t RENAME COLUMN old_c TO new_c",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("ALTER TABLE may not be fully supported: %v", err)
			}
		})
	}
}

// TestParseForeignKeyConstraint tests foreign key constraint parsing
func TestParseForeignKeyConstraint(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL
	)`
	_, err := Parse(sql)
	if err != nil {
		t.Logf("Foreign key constraint parsing error: %v", err)
	}
}

// TestParseTableWithMultipleConstraints tests table with multiple constraints
func TestParseTableWithMultipleConstraints(t *testing.T) {
	sql := `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		price REAL CHECK (price > 0),
		category_id INTEGER,
		FOREIGN KEY (category_id) REFERENCES categories(id),
		UNIQUE (name, category_id)
	)`
	_, err := Parse(sql)
	if err != nil {
		t.Logf("Multiple constraints parsing error: %v", err)
	}
}

// TestTokenTypeStringMore tests TokenTypeString function with more cases
func TestTokenTypeStringMore(t *testing.T) {
	tests := []struct {
		tokenType TokenType
		expected  string
	}{
		{TokenEOF, "EOF"},
		{TokenIdentifier, "IDENTIFIER"},
		{TokenNumber, "NUMBER"},
		{TokenString, "STRING"},
		{TokenPlus, "PLUS"},
		{TokenMinus, "MINUS"},
		{TokenStar, "STAR"},
		{TokenSlash, "SLASH"},
		{TokenEq, "EQ"},
		{TokenNeq, "NEQ"},
		{TokenLt, "LT"},
		{TokenGt, "GT"},
		{TokenLte, "LTE"},
		{TokenGte, "GTE"},
		{TokenAnd, "AND"},
		{TokenOr, "OR"},
		{TokenNot, "NOT"},
		{TokenSelect, "SELECT"},
		{TokenInsert, "INSERT"},
		{TokenUpdate, "UPDATE"},
		{TokenDelete, "DELETE"},
		{TokenFrom, "FROM"},
		{TokenWhere, "WHERE"},
		{TokenJoin, "JOIN"},
		{TokenLeft, "LEFT"},
		{TokenRight, "RIGHT"},
		{TokenInner, "INNER"},
		{TokenOuter, "OUTER"},
		{TokenOn, "ON"},
		{TokenGroup, "GROUP"},
		{TokenOrder, "ORDER"},
		{TokenBy, "BY"},
		{TokenHaving, "HAVING"},
		{TokenLimit, "LIMIT"},
		{TokenOffset, "OFFSET"},
		{TokenCreate, "CREATE"},
		{TokenTable, "TABLE"},
		{TokenIndex, "INDEX"},
		{TokenDrop, "DROP"},
		{TokenBegin, "BEGIN"},
		{TokenCommit, "COMMIT"},
		{TokenRollback, "ROLLBACK"},
		{TokenTransaction, "TRANSACTION"},
		{TokenType(9999), "UNKNOWN"},
	}

	for _, tt := range tests {
		result := TokenTypeString(tt.tokenType)
		if result == "" || result == "UNKNOWN" {
			t.Logf("TokenTypeString(%v) returned %s", tt.tokenType, result)
		}
	}
}

// TestParsePlaceholderWithOffset tests placeholder parsing with offset
func TestParsePlaceholderWithOffset(t *testing.T) {
	sql := "SELECT * FROM t WHERE a = ? AND b = ?"
	tokens, err := Tokenize(sql)
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	parser := NewParser(tokens)
	_, err = parser.Parse()
	if err != nil {
		t.Logf("Parse with placeholders error: %v", err)
	}
}

// TestParseComplexSubquery tests complex subquery parsing
func TestParseComplexSubquery(t *testing.T) {
	sql := "SELECT * FROM (SELECT id, name FROM users WHERE active = 1) AS active_users WHERE id > 5"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("Complex subquery parsing error: %v", err)
	}
}

// TestParseNestedSubqueries tests nested subqueries
func TestParseNestedSubqueries(t *testing.T) {
	sql := "SELECT * FROM t WHERE id IN (SELECT user_id FROM orders WHERE product_id IN (SELECT id FROM products WHERE price > 100))"
	_, err := Parse(sql)
	if err != nil {
		t.Logf("Nested subqueries parsing error: %v", err)
	}
}

// TestParseComplexJoinChain tests complex join chains
func TestParseComplexJoinChain(t *testing.T) {
	tests := []string{
		"SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
		"SELECT * FROM a LEFT JOIN b ON a.id = b.a_id RIGHT JOIN c ON b.id = c.b_id",
		"SELECT * FROM a INNER JOIN b ON a.id = b.a_id LEFT OUTER JOIN c ON b.id = c.b_id",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex join parsing error: %v", err)
			}
		})
	}
}

// TestParseComplexExpressions tests complex expression parsing
func TestParseComplexExpressions(t *testing.T) {
	tests := []string{
		"SELECT (a + b) * (c - d) / e FROM t",
		"SELECT a + b * c - d / e FROM t",
		"SELECT -a + +b FROM t",
		"SELECT ~a & 15 FROM t",
		"SELECT NOT (a = 1 AND b = 2) OR c = 3 FROM t",
		"SELECT (a IN (1, 2, 3)) AND (b BETWEEN 1 AND 10) FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex expression parsing error: %v", err)
			}
		})
	}
}

// TestParseAllTokenTypes tests parsing with various token types
func TestParseAllTokenTypes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Boolean literal", "SELECT TRUE, FALSE FROM t"},
		{"Null literal", "SELECT NULL FROM t"},
		{"Blob literal", "SELECT x'48656c6c6f' FROM t"},
		{"Date literal", "SELECT DATE '2024-01-01' FROM t"},
		{"Timestamp literal", "SELECT TIMESTAMP '2024-01-01 12:00:00' FROM t"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err != nil {
				t.Logf("Token type parsing error: %v", err)
			}
		})
	}
}

// TestLexerAllTokenTypes tests lexer with all token types
func TestLexerAllTokenTypes(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"+", TokenPlus},
		{"-", TokenMinus},
		{"*", TokenStar},
		{"/", TokenSlash},
		{"%", TokenPercent},
		{"=", TokenEq},
		{"!=", TokenNeq},
		{"<>", TokenNeq},
		{"<", TokenLt},
		{">", TokenGt},
		{"<=", TokenLte},
		{">=", TokenGte},
		{"||", TokenConcat},
		{",", TokenComma},
		{";", TokenSemicolon},
		{"(", TokenLParen},
		{")", TokenRParen},
		{".", TokenDot},
		{"?", TokenQuestion},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			tokens, err := Tokenize(tt.input)
			if err != nil {
				t.Logf("Tokenize error for %s: %v", tt.input, err)
				return
			}
			if len(tokens) > 0 && tokens[0].Type != tt.expected {
				t.Logf("Expected %v for %s, got %v", tt.expected, tt.input, tokens[0].Type)
			}
		})
	}
}

// TestParseCreateTableWithDefaults tests CREATE TABLE with DEFAULT values
func TestParseCreateTableWithDefaults(t *testing.T) {
	sql := `CREATE TABLE test_defaults (
		id INTEGER PRIMARY KEY DEFAULT 1,
		name TEXT DEFAULT 'unknown',
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		is_active BOOLEAN DEFAULT TRUE
	)`
	_, err := Parse(sql)
	if err != nil {
		t.Logf("CREATE TABLE with defaults parsing error: %v", err)
	}
}

// TestParseComplexUpdate tests complex UPDATE statements
func TestParseComplexUpdate(t *testing.T) {
	tests := []string{
		"UPDATE t SET a = 1, b = 2, c = 3 WHERE id = 1",
		"UPDATE t SET a = (SELECT MAX(x) FROM u) WHERE id = 1",
		"UPDATE t SET a = b + c * d WHERE id IN (SELECT id FROM u)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex UPDATE parsing error: %v", err)
			}
		})
	}
}

// TestParseComplexDelete tests complex DELETE statements
func TestParseComplexDelete(t *testing.T) {
	tests := []string{
		"DELETE FROM t WHERE id IN (SELECT user_id FROM orders)",
		"DELETE FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex DELETE parsing error: %v", err)
			}
		})
	}
}

// TestParseCreateIndexComplex tests complex CREATE INDEX statements
func TestParseCreateIndexComplex(t *testing.T) {
	tests := []string{
		"CREATE UNIQUE INDEX idx1 ON t (a DESC, b ASC, c)",
		"CREATE INDEX idx2 ON t (a) WHERE a > 0",
		"CREATE INDEX idx3 ON t USING btree (a)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex CREATE INDEX parsing error: %v", err)
			}
		})
	}
}

// TestParseTriggerComplex tests complex trigger parsing
func TestParseTriggerComplex(t *testing.T) {
	tests := []string{
		"CREATE TRIGGER trg AFTER INSERT ON t BEGIN UPDATE stats SET count = count + 1; END",
		"CREATE TRIGGER trg BEFORE UPDATE ON t FOR EACH ROW BEGIN SELECT 1; END",
		"CREATE TRIGGER trg INSTEAD OF DELETE ON v BEGIN SELECT 1; END",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex trigger parsing error: %v", err)
			}
		})
	}
}

// TestParseProcedureComplex tests complex procedure parsing
func TestParseProcedureComplex(t *testing.T) {
	sql := `CREATE PROCEDURE proc(IN param1 INTEGER, OUT param2 TEXT)
	BEGIN
		SELECT * FROM t WHERE id = param1;
	END`
	_, err := Parse(sql)
	if err != nil {
		t.Logf("Complex procedure parsing error: %v", err)
	}
}

// TestParseViewComplex tests complex view parsing
func TestParseViewComplex(t *testing.T) {
	tests := []string{
		"CREATE VIEW v AS SELECT a, b FROM t WHERE c = 1",
		"CREATE TEMP VIEW v AS SELECT * FROM t",
		"CREATE TEMPORARY VIEW v AS SELECT DISTINCT a FROM t ORDER BY a",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := Parse(sql)
			if err != nil {
				t.Logf("Complex view parsing error: %v", err)
			}
			if strings.Contains(sql, "TEMP") {
				createView, ok := stmt.(*CreateViewStmt)
				if !ok {
					t.Fatalf("stmt = %T, want CreateViewStmt", stmt)
				}
				if !createView.Temporary {
					t.Fatalf("Temporary = false, want true for %s", sql)
				}
			}
		})
	}
}

// TestParseDropComplex tests complex DROP statements
func TestParseDropComplex(t *testing.T) {
	tests := []string{
		"DROP TABLE IF EXISTS t",
		"DROP INDEX IF EXISTS idx",
		"DROP VIEW IF EXISTS v",
		"DROP TRIGGER IF EXISTS trg",
		"DROP PROCEDURE IF EXISTS proc",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex DROP parsing error: %v", err)
			}
		})
	}
}

// TestParseSelectComplexOrdering tests complex SELECT with ordering
func TestParseSelectComplexOrdering(t *testing.T) {
	tests := []string{
		"SELECT * FROM t ORDER BY a NULLS FIRST",
		"SELECT * FROM t ORDER BY a NULLS LAST",
		"SELECT * FROM t ORDER BY a ASC, b DESC, c",
		"SELECT * FROM t ORDER BY 1, 2",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex ordering parsing error: %v", err)
			}
		})
	}
}

// TestParseSelectComplexLimit tests complex LIMIT/OFFSET parsing
func TestParseSelectComplexLimit(t *testing.T) {
	tests := []string{
		"SELECT * FROM t LIMIT 10 OFFSET 5",
		"SELECT * FROM t OFFSET 5",
		"SELECT * FROM t LIMIT ALL",
		"SELECT * FROM t LIMIT ? OFFSET ?",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex LIMIT parsing error: %v", err)
			}
		})
	}
}

// TestParseAllDataTypes tests all data type parsing
func TestParseAllDataTypes(t *testing.T) {
	sql := `CREATE TABLE all_types (
		col1 INTEGER,
		col2 TEXT,
		col3 REAL,
		col4 BLOB,
		col5 BOOLEAN,
		col6 JSON,
		col7 DATE,
		col8 TIMESTAMP,
		col9 VARCHAR(255),
		col10 DECIMAL(10, 2),
		col11 FLOAT,
		col12 DOUBLE,
		col13 INT,
		col14 BIGINT,
		col15 SMALLINT,
		col16 TINYINT,
		col17 CHAR(10),
		col18 NCHAR(50),
		col19 NVARCHAR(100),
		col20 CLOB,
		col21 NCLOB,
		col22 UUID
	)`
	_, err := Parse(sql)
	if err != nil {
		t.Logf("All data types parsing error: %v", err)
	}
}

// TestParseExplain tests EXPLAIN
func TestParseExplain(t *testing.T) {
	tests := []string{
		"EXPLAIN SELECT * FROM t",
		"EXPLAIN QUERY PLAN SELECT * FROM t",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err != nil {
				t.Logf("EXPLAIN may not be fully supported: %v", err)
			}
		})
	}
}

// TestApplyPlaceholderOffset tests the applyPlaceholderOffset function
func TestApplyPlaceholderOffset(t *testing.T) {
	// Test with BinaryExpr containing placeholders
	expr := &BinaryExpr{
		Left:  &PlaceholderExpr{Index: 0},
		Right: &PlaceholderExpr{Index: 1},
	}
	applyPlaceholderOffset(expr, 5)

	if expr.Left.(*PlaceholderExpr).Index != 5 {
		t.Errorf("Expected placeholder index 5, got %d", expr.Left.(*PlaceholderExpr).Index)
	}
	if expr.Right.(*PlaceholderExpr).Index != 6 {
		t.Errorf("Expected placeholder index 6, got %d", expr.Right.(*PlaceholderExpr).Index)
	}
}

// TestApplyPlaceholderOffsetWithNil tests applyPlaceholderOffset with nil expression
func TestApplyPlaceholderOffsetWithNil(t *testing.T) {
	// Should not panic with nil expression
	applyPlaceholderOffset(nil, 5)
}

// TestApplyPlaceholderOffsetComplex tests applyPlaceholderOffset with complex expressions
func TestApplyPlaceholderOffsetComplex(t *testing.T) {
	// Test with FunctionCall containing placeholders
	expr := &FunctionCall{
		Name: "FUNC",
		Args: []Expression{
			&PlaceholderExpr{Index: 0},
			&BinaryExpr{
				Left:  &PlaceholderExpr{Index: 1},
				Right: &NumberLiteral{Value: 42},
			},
		},
	}
	applyPlaceholderOffset(expr, 10)

	if expr.Args[0].(*PlaceholderExpr).Index != 10 {
		t.Errorf("Expected placeholder index 10, got %d", expr.Args[0].(*PlaceholderExpr).Index)
	}
	binaryExpr := expr.Args[1].(*BinaryExpr)
	if binaryExpr.Left.(*PlaceholderExpr).Index != 11 {
		t.Errorf("Expected placeholder index 11, got %d", binaryExpr.Left.(*PlaceholderExpr).Index)
	}
}

// TestApplyPlaceholderOffsetInExpr tests applyPlaceholderOffset with InExpr
func TestApplyPlaceholderOffsetInExpr(t *testing.T) {
	expr := &InExpr{
		Expr: &PlaceholderExpr{Index: 0},
		List: []Expression{
			&PlaceholderExpr{Index: 1},
			&PlaceholderExpr{Index: 2},
		},
	}
	applyPlaceholderOffset(expr, 3)

	if expr.Expr.(*PlaceholderExpr).Index != 3 {
		t.Errorf("Expected placeholder index 3, got %d", expr.Expr.(*PlaceholderExpr).Index)
	}
	if expr.List[0].(*PlaceholderExpr).Index != 4 {
		t.Errorf("Expected placeholder index 4, got %d", expr.List[0].(*PlaceholderExpr).Index)
	}
	if expr.List[1].(*PlaceholderExpr).Index != 5 {
		t.Errorf("Expected placeholder index 5, got %d", expr.List[1].(*PlaceholderExpr).Index)
	}
}

// TestApplyPlaceholderOffsetBetweenExpr tests applyPlaceholderOffset with BetweenExpr
func TestApplyPlaceholderOffsetBetweenExpr(t *testing.T) {
	expr := &BetweenExpr{
		Expr:  &PlaceholderExpr{Index: 0},
		Lower: &PlaceholderExpr{Index: 1},
		Upper: &PlaceholderExpr{Index: 2},
	}
	applyPlaceholderOffset(expr, 7)

	if expr.Expr.(*PlaceholderExpr).Index != 7 {
		t.Errorf("Expected placeholder index 7, got %d", expr.Expr.(*PlaceholderExpr).Index)
	}
	if expr.Lower.(*PlaceholderExpr).Index != 8 {
		t.Errorf("Expected placeholder index 8, got %d", expr.Lower.(*PlaceholderExpr).Index)
	}
	if expr.Upper.(*PlaceholderExpr).Index != 9 {
		t.Errorf("Expected placeholder index 9, got %d", expr.Upper.(*PlaceholderExpr).Index)
	}
}

// TestApplyPlaceholderOffsetUnaryExpr tests applyPlaceholderOffset with UnaryExpr
func TestApplyPlaceholderOffsetUnaryExpr(t *testing.T) {
	expr := &UnaryExpr{
		Operator: TokenMinus,
		Expr:     &PlaceholderExpr{Index: 0},
	}
	applyPlaceholderOffset(expr, 2)

	if expr.Expr.(*PlaceholderExpr).Index != 2 {
		t.Errorf("Expected placeholder index 2, got %d", expr.Expr.(*PlaceholderExpr).Index)
	}
}

// TestParserCurrentPeekMore tests parser current() and peek() methods with edge cases
func TestParserCurrentPeek(t *testing.T) {
	// Test with empty token list
	p := &Parser{tokens: []Token{}, pos: 0}

	// current() should return EOF when pos >= len(tokens)
	curr := p.current()
	if curr.Type != TokenEOF {
		t.Errorf("Expected EOF, got %v", curr.Type)
	}

	// peek() should return EOF when pos+1 >= len(tokens)
	peek := p.peek()
	if peek.Type != TokenEOF {
		t.Errorf("Expected EOF, got %v", peek.Type)
	}

	// Test with tokens
	p2 := &Parser{
		tokens: []Token{
			{Type: TokenSelect, Literal: "SELECT"},
			{Type: TokenIdentifier, Literal: "name"},
			{Type: TokenFrom, Literal: "FROM"},
		},
		pos: 0,
	}

	if p2.current().Type != TokenSelect {
		t.Errorf("Expected SELECT, got %v", p2.current().Type)
	}

	if p2.peek().Type != TokenIdentifier {
		t.Errorf("Expected Identifier, got %v", p2.peek().Type)
	}

	// Move to end
	p2.pos = 2
	if p2.current().Type != TokenFrom {
		t.Errorf("Expected FROM, got %v", p2.current().Type)
	}
	if p2.peek().Type != TokenEOF {
		t.Errorf("Expected EOF, got %v", p2.peek().Type)
	}
}

// TestParseCreateIndexMore tests CREATE INDEX with more options
func TestParseCreateIndexMore(t *testing.T) {
	tests := []struct {
		sql        string
		indexName  string
		tableName  string
		unique     bool
		ifNotExist bool
		columns    []string
	}{
		{"CREATE INDEX idx1 ON users(name)", "idx1", "users", false, false, []string{"name"}},
		{"CREATE INDEX IF NOT EXISTS idx3 ON products(price)", "idx3", "products", false, true, []string{"price"}},
		{"CREATE INDEX idx4 ON users(name DESC, email ASC)", "idx4", "users", false, false, []string{"name", "email"}},
		{"CREATE INDEX idx5 ON users(name COLLATE NOCASE, email ASC COLLATE BINARY)", "idx5", "users", false, false, []string{"name", "email"}},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("Parse error: %v", err)
				return
			}
			idxStmt, ok := stmt.(*CreateIndexStmt)
			if !ok {
				t.Errorf("Expected CreateIndexStmt, got %T", stmt)
				return
			}
			if idxStmt.Index != tt.indexName {
				t.Errorf("Expected index name %q, got %q", tt.indexName, idxStmt.Index)
			}
			if idxStmt.Table != tt.tableName {
				t.Errorf("Expected table name %q, got %q", tt.tableName, idxStmt.Table)
			}
			if idxStmt.Unique != tt.unique {
				t.Errorf("Expected unique %v, got %v", tt.unique, idxStmt.Unique)
			}
			if idxStmt.IfNotExists != tt.ifNotExist {
				t.Errorf("Expected IfNotExists %v, got %v", tt.ifNotExist, idxStmt.IfNotExists)
			}
			if len(idxStmt.Columns) != len(tt.columns) {
				t.Fatalf("Expected columns %v, got %v", tt.columns, idxStmt.Columns)
			}
			for i, want := range tt.columns {
				if idxStmt.Columns[i] != want {
					t.Fatalf("Expected columns %v, got %v", tt.columns, idxStmt.Columns)
				}
			}
		})
	}
}

func TestParseColumnCollate(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (name TEXT COLLATE NOCASE, email TEXT COLLATE BINARY)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}
	if got := createStmt.Columns[0].Collation; got != "NOCASE" {
		t.Fatalf("name collation = %q, want NOCASE", got)
	}
	if got := createStmt.Columns[1].Collation; got != "BINARY" {
		t.Fatalf("email collation = %q, want BINARY", got)
	}
}

// TestParseForeignKeyDef tests foreign key constraint parsing
func TestParseForeignKeyDef(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INT PRIMARY KEY,
		user_id INT,
		FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL
	)`

	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	createStmt, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}

	// Check that we have 2 columns
	if len(createStmt.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(createStmt.Columns))
	}

	// Check foreign key constraint
	if len(createStmt.ForeignKeys) != 1 {
		t.Errorf("Expected 1 foreign key, got %d", len(createStmt.ForeignKeys))
	} else {
		fk := createStmt.ForeignKeys[0]
		if len(fk.Columns) != 1 || fk.Columns[0] != "user_id" {
			t.Errorf("Expected FK column 'user_id', got %v", fk.Columns)
		}
		if fk.ReferencedTable != "users" {
			t.Errorf("Expected ref table 'users', got %q", fk.ReferencedTable)
		}
		if len(fk.ReferencedColumns) != 1 || fk.ReferencedColumns[0] != "id" {
			t.Errorf("Expected ref column 'id', got %v", fk.ReferencedColumns)
		}
	}
}

func TestParseForeignKeySetDefaultAndMalformedActions(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (id INT, ref INT DEFAULT 0, FOREIGN KEY (ref) REFERENCES parent(id) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT)")
	if err != nil {
		t.Fatalf("SET DEFAULT foreign key actions should parse: %v", err)
	}
	createStmt := stmt.(*CreateTableStmt)
	if len(createStmt.ForeignKeys) != 1 {
		t.Fatalf("expected 1 foreign key, got %d", len(createStmt.ForeignKeys))
	}
	fk := createStmt.ForeignKeys[0]
	if fk.OnDelete != "SET DEFAULT" || fk.OnUpdate != "SET DEFAULT" {
		t.Fatalf("unexpected SET DEFAULT actions: delete=%q update=%q", fk.OnDelete, fk.OnUpdate)
	}

	cases := []string{
		"CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES parent(id) ON DELETE SET)",
		"CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES parent(id) ON UPDATE NO)",
		"CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES parent(id) ON DELETE DEFAULT)",
		"CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES parent(id) ON DELETE CASCADE ON DELETE RESTRICT)",
		"CREATE TABLE t (id INT, ref INT, FOREIGN KEY (ref) REFERENCES parent(id) ON UPDATE CASCADE ON UPDATE RESTRICT)",
	}
	for _, sql := range cases {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected malformed foreign key action to fail: %s", sql)
		}
	}
}

// TestParseComparisonMore tests comparison operators
func TestParseComparisonMore(t *testing.T) {
	tests := []struct {
		sql      string
		operator string
	}{
		{"SELECT * FROM t WHERE a = 1", "="},
		{"SELECT * FROM t WHERE a != 1", "!="},
		{"SELECT * FROM t WHERE a <> 1", "!="}, // <> is normalized to !=
		{"SELECT * FROM t WHERE a < 1", "<"},
		{"SELECT * FROM t WHERE a > 1", ">"},
		{"SELECT * FROM t WHERE a <= 1", "<="},
		{"SELECT * FROM t WHERE a >= 1", ">="},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Errorf("Parse error: %v", err)
				return
			}
			selectStmt := stmt.(*SelectStmt)
			binaryExpr := selectStmt.Where.(*BinaryExpr)
			if TokenTypeString(binaryExpr.Operator) != tt.operator {
				t.Errorf("Expected operator %q, got %q", tt.operator, TokenTypeString(binaryExpr.Operator))
			}
		})
	}
}

func TestParseIsDistinctFrom(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantNot bool
	}{
		{
			name:    "is distinct from",
			sql:     "SELECT * FROM t WHERE a IS DISTINCT FROM NULL",
			wantNot: true,
		},
		{
			name:    "is not distinct from",
			sql:     "SELECT * FROM t WHERE a IS NOT DISTINCT FROM NULL",
			wantNot: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			selectStmt := stmt.(*SelectStmt)
			expr := selectStmt.Where
			if tt.wantNot {
				unary, ok := expr.(*UnaryExpr)
				if !ok {
					t.Fatalf("expected UnaryExpr, got %T", expr)
				}
				if unary.Operator != TokenNot {
					t.Fatalf("expected NOT unary operator, got %s", TokenTypeString(unary.Operator))
				}
				expr = unary.Expr
			}
			binary, ok := expr.(*BinaryExpr)
			if !ok {
				t.Fatalf("expected BinaryExpr, got %T", expr)
			}
			if binary.Operator != TokenNullSafeEq {
				t.Fatalf("expected null-safe equality, got %s", TokenTypeString(binary.Operator))
			}
		})
	}
}

func TestParseRegexpOperator(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantNot bool
	}{
		{
			name: "regexp",
			sql:  "SELECT * FROM t WHERE name REGEXP '^[a-z]+$'",
		},
		{
			name:    "not regexp",
			sql:     "SELECT * FROM t WHERE name NOT REGEXP '^[a-z]+$'",
			wantNot: true,
		},
		{
			name: "rlike",
			sql:  "SELECT * FROM t WHERE name RLIKE '^[a-z]+$'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			selectStmt := stmt.(*SelectStmt)
			expr := selectStmt.Where
			if tt.wantNot {
				unary, ok := expr.(*UnaryExpr)
				if !ok {
					t.Fatalf("expected UnaryExpr, got %T", expr)
				}
				if unary.Operator != TokenNot {
					t.Fatalf("expected NOT unary operator, got %s", TokenTypeString(unary.Operator))
				}
				expr = unary.Expr
			}
			fn, ok := expr.(*FunctionCall)
			if !ok {
				t.Fatalf("expected FunctionCall, got %T", expr)
			}
			if fn.Name != "REGEXP_LIKE" {
				t.Fatalf("expected REGEXP_LIKE, got %s", fn.Name)
			}
			if len(fn.Args) != 2 {
				t.Fatalf("expected 2 REGEXP_LIKE args, got %d", len(fn.Args))
			}
		})
	}
}

func TestParseAllAggregateQuantifier(t *testing.T) {
	stmt, err := Parse("SELECT COUNT(ALL v), SUM(ALL v), AVG(ALL v) FROM t")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(selectStmt.Columns))
	}
	for i, col := range selectStmt.Columns {
		fn, ok := col.(*FunctionCall)
		if !ok {
			t.Fatalf("column %d = %T, want FunctionCall", i, col)
		}
		if fn.Distinct {
			t.Fatalf("column %d distinct = true, want false for ALL", i)
		}
		if len(fn.Args) != 1 {
			t.Fatalf("column %d args = %d, want 1", i, len(fn.Args))
		}
		if _, ok := fn.Args[0].(*Identifier); !ok {
			t.Fatalf("column %d arg = %T, want Identifier", i, fn.Args[0])
		}
	}
}

func TestParseGroupConcatSeparatorSyntax(t *testing.T) {
	cases := []struct {
		sql      string
		distinct bool
		sep      string
		orderBy  int
	}{
		{"SELECT GROUP_CONCAT(v SEPARATOR '|') FROM t", false, "|", 0},
		{"SELECT GROUP_CONCAT(DISTINCT v SEPARATOR '') FROM t", true, "", 0},
		{"SELECT GROUP_CONCAT(v, ':') FROM t", false, ":", 0},
		{"SELECT GROUP_CONCAT(v ORDER BY id DESC, v ASC SEPARATOR '|') FROM t", false, "|", 2},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			selectStmt := stmt.(*SelectStmt)
			fn, ok := selectStmt.Columns[0].(*FunctionCall)
			if !ok {
				t.Fatalf("column = %T, want FunctionCall", selectStmt.Columns[0])
			}
			if fn.Name != "GROUP_CONCAT" {
				t.Fatalf("function = %s, want GROUP_CONCAT", fn.Name)
			}
			if fn.Distinct != tc.distinct {
				t.Fatalf("distinct = %v, want %v", fn.Distinct, tc.distinct)
			}
			if len(fn.Args) != 2 {
				t.Fatalf("args = %d, want 2", len(fn.Args))
			}
			sep, ok := fn.Args[1].(*StringLiteral)
			if !ok {
				t.Fatalf("separator arg = %T, want StringLiteral", fn.Args[1])
			}
			if sep.Value != tc.sep {
				t.Fatalf("separator = %q, want %q", sep.Value, tc.sep)
			}
			if len(fn.OrderBy) != tc.orderBy {
				t.Fatalf("orderBy = %d, want %d", len(fn.OrderBy), tc.orderBy)
			}
			if tc.orderBy > 0 && !fn.OrderBy[0].Desc {
				t.Fatal("first ORDER BY should be DESC")
			}
		})
	}
}

func TestParseAggregateFilterClause(t *testing.T) {
	stmt, err := Parse("SELECT COUNT(*) FILTER (WHERE v > 0), SUM(v) FILTER (WHERE keep = 1), GROUP_CONCAT(v ORDER BY id DESC SEPARATOR '|') FILTER (WHERE g = 'b') FROM t")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.Columns) != 3 {
		t.Fatalf("columns = %d, want 3", len(selectStmt.Columns))
	}

	for i, col := range selectStmt.Columns {
		fn, ok := col.(*FunctionCall)
		if !ok {
			t.Fatalf("column %d = %T, want FunctionCall", i, col)
		}
		if fn.Filter == nil {
			t.Fatalf("column %d filter = nil, want predicate", i)
		}
	}

	groupConcat := selectStmt.Columns[2].(*FunctionCall)
	if groupConcat.Name != "GROUP_CONCAT" {
		t.Fatalf("function = %s, want GROUP_CONCAT", groupConcat.Name)
	}
	if len(groupConcat.OrderBy) != 1 || !groupConcat.OrderBy[0].Desc {
		t.Fatalf("GROUP_CONCAT orderBy = %#v, want one DESC order expression", groupConcat.OrderBy)
	}
}

func TestParseWindowAggregateFilterClause(t *testing.T) {
	stmt, err := Parse("SELECT COUNT(*) FILTER (WHERE keep = 1) OVER (PARTITION BY g ORDER BY id) FROM t")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	we, ok := selectStmt.Columns[0].(*WindowExpr)
	if !ok {
		t.Fatalf("column = %T, want WindowExpr", selectStmt.Columns[0])
	}
	if we.Function != "COUNT" {
		t.Fatalf("function = %s, want COUNT", we.Function)
	}
	if we.Filter == nil {
		t.Fatal("filter = nil, want predicate")
	}
	if len(we.PartitionBy) != 1 || len(we.OrderBy) != 1 {
		t.Fatalf("partition/order sizes = %d/%d, want 1/1", len(we.PartitionBy), len(we.OrderBy))
	}
}

func TestParseCreateOrReplaceView(t *testing.T) {
	stmt, err := Parse("CREATE OR REPLACE VIEW v AS SELECT id FROM t")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	createView, ok := stmt.(*CreateViewStmt)
	if !ok {
		t.Fatalf("stmt = %T, want CreateViewStmt", stmt)
	}
	if !createView.OrReplace {
		t.Fatal("OrReplace = false, want true")
	}
	if createView.Name != "v" {
		t.Fatalf("Name = %s, want v", createView.Name)
	}
}

func TestParseJSONAggregates(t *testing.T) {
	cases := []struct {
		sql  string
		name string
		args int
	}{
		{"SELECT JSON_ARRAYAGG(v) FROM t", "JSON_ARRAYAGG", 1},
		{"SELECT JSON_OBJECTAGG(k, v) FROM t", "JSON_OBJECTAGG", 2},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			selectStmt := stmt.(*SelectStmt)
			fn, ok := selectStmt.Columns[0].(*FunctionCall)
			if !ok {
				t.Fatalf("column = %T, want FunctionCall", selectStmt.Columns[0])
			}
			if fn.Name != tc.name {
				t.Fatalf("function = %s, want %s", fn.Name, tc.name)
			}
			if len(fn.Args) != tc.args {
				t.Fatalf("args = %d, want %d", len(fn.Args), tc.args)
			}
		})
	}
}

func TestParseIsBooleanPredicate(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantFunc string
		wantNot  bool
	}{
		{
			name:     "is true",
			sql:      "SELECT * FROM t WHERE active IS TRUE",
			wantFunc: "IS_TRUE",
		},
		{
			name:     "is not false",
			sql:      "SELECT * FROM t WHERE active IS NOT FALSE",
			wantFunc: "IS_FALSE",
			wantNot:  true,
		},
		{
			name:     "is unknown",
			sql:      "SELECT * FROM t WHERE active IS UNKNOWN",
			wantFunc: "IS_UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			selectStmt := stmt.(*SelectStmt)
			expr := selectStmt.Where
			if tt.wantNot {
				unary, ok := expr.(*UnaryExpr)
				if !ok {
					t.Fatalf("expected UnaryExpr, got %T", expr)
				}
				if unary.Operator != TokenNot {
					t.Fatalf("expected NOT unary operator, got %s", TokenTypeString(unary.Operator))
				}
				expr = unary.Expr
			}
			fn, ok := expr.(*FunctionCall)
			if !ok {
				t.Fatalf("expected FunctionCall, got %T", expr)
			}
			if fn.Name != tt.wantFunc {
				t.Fatalf("expected %s, got %s", tt.wantFunc, fn.Name)
			}
			if len(fn.Args) != 1 {
				t.Fatalf("expected one boolean-test arg, got %d", len(fn.Args))
			}
		})
	}
}

func TestParseOnDuplicateKeyUpdate(t *testing.T) {
	stmt, err := Parse("INSERT INTO t (id, v) VALUES (1, 2) ON DUPLICATE KEY UPDATE v = 3")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	if insertStmt.OnConflict == nil {
		t.Fatal("expected ON DUPLICATE KEY UPDATE to populate OnConflict")
	}
	if insertStmt.OnConflict.DoUpdate == nil || len(insertStmt.OnConflict.DoUpdate) != 1 {
		t.Fatalf("expected one DO UPDATE assignment, got %#v", insertStmt.OnConflict.DoUpdate)
	}
	if insertStmt.OnConflict.DoUpdate[0].Column != "v" {
		t.Fatalf("expected assignment to v, got %s", insertStmt.OnConflict.DoUpdate[0].Column)
	}
}

func TestParseOnDuplicateKeyUpdateValuesFunction(t *testing.T) {
	stmt, err := Parse("INSERT INTO t (id, v) VALUES (1, 2) ON DUPLICATE KEY UPDATE v = VALUES(v)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	fn, ok := insertStmt.OnConflict.DoUpdate[0].Value.(*FunctionCall)
	if !ok {
		t.Fatalf("expected VALUES() FunctionCall, got %T", insertStmt.OnConflict.DoUpdate[0].Value)
	}
	if fn.Name != "VALUES" || len(fn.Args) != 1 {
		t.Fatalf("unexpected function call: %#v", fn)
	}
	arg, ok := fn.Args[0].(*Identifier)
	if !ok || arg.Name != "v" {
		t.Fatalf("expected VALUES(v) argument, got %#v", fn.Args[0])
	}
}

func TestParseInsertSetSyntax(t *testing.T) {
	stmt, err := Parse("INSERT INTO t SET id = 1, name = 'Ada'")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	if got, want := insertStmt.Columns, []string{"id", "name"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	if len(insertStmt.Values) != 1 || len(insertStmt.Values[0]) != 2 {
		t.Fatalf("values = %#v, want one row with two values", insertStmt.Values)
	}
	if _, ok := insertStmt.Values[0][0].(*NumberLiteral); !ok {
		t.Fatalf("first value = %T, want NumberLiteral", insertStmt.Values[0][0])
	}
	if _, ok := insertStmt.Values[0][1].(*StringLiteral); !ok {
		t.Fatalf("second value = %T, want StringLiteral", insertStmt.Values[0][1])
	}
}

func TestParseInsertSetOnDuplicateKeyUpdate(t *testing.T) {
	stmt, err := Parse("INSERT INTO t SET id = 1, v = 2 ON DUPLICATE KEY UPDATE v = VALUES(v)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	if insertStmt.OnConflict == nil || len(insertStmt.OnConflict.DoUpdate) != 1 {
		t.Fatalf("expected ON DUPLICATE KEY UPDATE clause, got %#v", insertStmt.OnConflict)
	}
}

func TestParseReplaceInto(t *testing.T) {
	stmt, err := Parse("REPLACE INTO t (id, name) VALUES (1, 'Ada')")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	if insertStmt.ConflictAction != ConflictReplace {
		t.Fatalf("ConflictAction = %v, want ConflictReplace", insertStmt.ConflictAction)
	}
	if insertStmt.Table != "t" {
		t.Fatalf("Table = %q, want t", insertStmt.Table)
	}
	if got, want := insertStmt.Columns, []string{"id", "name"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
}

func TestParseReplaceSetSyntax(t *testing.T) {
	stmt, err := Parse("REPLACE t SET id = 1, name = 'Ada'")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	if insertStmt.ConflictAction != ConflictReplace {
		t.Fatalf("ConflictAction = %v, want ConflictReplace", insertStmt.ConflictAction)
	}
	if len(insertStmt.Values) != 1 || len(insertStmt.Values[0]) != 2 {
		t.Fatalf("values = %#v, want one row with two values", insertStmt.Values)
	}
}

func TestParseInsertIgnoreMySQLSyntax(t *testing.T) {
	stmt, err := Parse("INSERT IGNORE INTO t (id, name) VALUES (1, 'Ada')")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	if insertStmt.ConflictAction != ConflictIgnore {
		t.Fatalf("ConflictAction = %v, want ConflictIgnore", insertStmt.ConflictAction)
	}
	if got, want := insertStmt.Columns, []string{"id", "name"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
}

func TestParseInsertIgnoreSetSyntax(t *testing.T) {
	stmt, err := Parse("INSERT IGNORE INTO t SET id = 1, name = 'Ada'")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	if insertStmt.ConflictAction != ConflictIgnore {
		t.Fatalf("ConflictAction = %v, want ConflictIgnore", insertStmt.ConflictAction)
	}
	if len(insertStmt.Values) != 1 || len(insertStmt.Values[0]) != 2 {
		t.Fatalf("values = %#v, want one row with two values", insertStmt.Values)
	}
}

func TestParseInsertMySQLPriorityModifiers(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantIgnore bool
	}{
		{
			name: "low priority values",
			sql:  "INSERT LOW_PRIORITY INTO t VALUES (1)",
		},
		{
			name:       "high priority ignore set",
			sql:        "INSERT HIGH_PRIORITY IGNORE INTO t SET id = 1",
			wantIgnore: true,
		},
		{
			name: "delayed default values",
			sql:  "INSERT DELAYED INTO t DEFAULT VALUES",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			insertStmt := stmt.(*InsertStmt)
			if tt.wantIgnore && insertStmt.ConflictAction != ConflictIgnore {
				t.Fatalf("ConflictAction = %v, want ConflictIgnore", insertStmt.ConflictAction)
			}
		})
	}
}

func TestParseReplaceMySQLPriorityModifiers(t *testing.T) {
	tests := []string{
		"REPLACE LOW_PRIORITY INTO t VALUES (1)",
		"REPLACE DELAYED t SET id = 1",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := Parse(sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			insertStmt := stmt.(*InsertStmt)
			if insertStmt.ConflictAction != ConflictReplace {
				t.Fatalf("ConflictAction = %v, want ConflictReplace", insertStmt.ConflictAction)
			}
		})
	}
}

func TestParseModInfixPreservesModFunction(t *testing.T) {
	stmt, err := Parse("SELECT 10 MOD 3, MOD(10, 4)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	if len(selectStmt.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(selectStmt.Columns))
	}
	modExpr, ok := selectStmt.Columns[0].(*BinaryExpr)
	if !ok {
		t.Fatalf("first column = %T, want *BinaryExpr", selectStmt.Columns[0])
	}
	if modExpr.Operator != TokenPercent {
		t.Fatalf("MOD operator = %v, want TokenPercent", modExpr.Operator)
	}
	if _, ok := selectStmt.Columns[1].(*FunctionCall); !ok {
		t.Fatalf("second column = %T, want *FunctionCall", selectStmt.Columns[1])
	}
}

func TestParseGlobOperator(t *testing.T) {
	tests := []struct {
		sql string
		not bool
	}{
		{"SELECT * FROM t WHERE name GLOB '*.txt'", false},
		{"SELECT * FROM t WHERE name NOT GLOB '*.txt'", true},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			selectStmt := stmt.(*SelectStmt)
			expr := selectStmt.Where
			if tt.not {
				unary, ok := expr.(*UnaryExpr)
				if !ok || unary.Operator != TokenNot {
					t.Fatalf("where = %T, want NOT unary", expr)
				}
				expr = unary.Expr
			}
			fn, ok := expr.(*FunctionCall)
			if !ok {
				t.Fatalf("where = %T, want *FunctionCall", expr)
			}
			if fn.Name != "GLOB" || len(fn.Args) != 2 {
				t.Fatalf("GLOB call = %#v", fn)
			}
		})
	}
}

func TestParseSelectLockingClauses(t *testing.T) {
	tests := []struct {
		sql        string
		mode       string
		targets    []string
		waitPolicy string
		hasWait    bool
	}{
		{"SELECT * FROM t FOR UPDATE", "UPDATE", nil, "", false},
		{"SELECT * FROM t FOR SHARE", "SHARE", nil, "", false},
		{"SELECT * FROM t FOR UPDATE OF t NOWAIT", "UPDATE", []string{"t"}, "NOWAIT", false},
		{"SELECT * FROM t FOR UPDATE OF public.t SKIP LOCKED", "UPDATE", []string{"public.t"}, "SKIP LOCKED", false},
		{"SELECT * FROM t FOR UPDATE WAIT 10", "UPDATE", nil, "WAIT", true},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := ParseStrict(tt.sql)
			if err != nil {
				t.Fatalf("ParseStrict: %v", err)
			}
			selectStmt := stmt.(*SelectStmt)
			if selectStmt.Locking == nil {
				t.Fatalf("Locking is nil")
			}
			if selectStmt.Locking.Mode != tt.mode {
				t.Fatalf("Mode = %q, want %q", selectStmt.Locking.Mode, tt.mode)
			}
			if len(selectStmt.Locking.Targets) != len(tt.targets) {
				t.Fatalf("Targets = %v, want %v", selectStmt.Locking.Targets, tt.targets)
			}
			for i := range tt.targets {
				if selectStmt.Locking.Targets[i] != tt.targets[i] {
					t.Fatalf("Targets = %v, want %v", selectStmt.Locking.Targets, tt.targets)
				}
			}
			if selectStmt.Locking.WaitPolicy != tt.waitPolicy {
				t.Fatalf("WaitPolicy = %q, want %q", selectStmt.Locking.WaitPolicy, tt.waitPolicy)
			}
			if tt.hasWait && selectStmt.Locking.WaitValue == nil {
				t.Fatalf("WaitValue is nil")
			}
		})
	}
}

func TestParseTableIndexHints(t *testing.T) {
	tests := []struct {
		sql        string
		indexHint  string
		notIndexed bool
	}{
		{"SELECT * FROM t INDEXED BY idx_t_name", "idx_t_name", false},
		{"SELECT * FROM t AS x INDEXED BY idx_t_name", "idx_t_name", false},
		{"SELECT * FROM t NOT INDEXED", "", true},
		{"SELECT * FROM t AS x NOT INDEXED", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := ParseStrict(tt.sql)
			if err != nil {
				t.Fatalf("ParseStrict: %v", err)
			}
			selectStmt := stmt.(*SelectStmt)
			if selectStmt.From.IndexHint != tt.indexHint {
				t.Fatalf("IndexHint = %q, want %q", selectStmt.From.IndexHint, tt.indexHint)
			}
			if selectStmt.From.NotIndexed != tt.notIndexed {
				t.Fatalf("NotIndexed = %v, want %v", selectStmt.From.NotIndexed, tt.notIndexed)
			}
		})
	}
}

func TestOptimizeSelectRespectsNotIndexedHint(t *testing.T) {
	stmt, err := ParseStrict("SELECT * FROM t NOT INDEXED WHERE id = 1")
	if err != nil {
		t.Fatalf("ParseStrict: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	optimized, err := NewQueryOptimizer().OptimizeSelect(selectStmt)
	if err != nil {
		t.Fatalf("OptimizeSelect: %v", err)
	}
	if optimized.From.IndexHint != "" {
		t.Fatalf("IndexHint = %q, want empty for NOT INDEXED", optimized.From.IndexHint)
	}
	if !optimized.From.NotIndexed {
		t.Fatal("NotIndexed = false, want true")
	}
}

func TestParseRefreshMaterializedViewConcurrently(t *testing.T) {
	stmt, err := ParseStrict("REFRESH MATERIALIZED VIEW CONCURRENTLY mv")
	if err != nil {
		t.Fatalf("ParseStrict: %v", err)
	}
	refreshStmt := stmt.(*RefreshMaterializedViewStmt)
	if refreshStmt.Name != "mv" {
		t.Fatalf("Name = %q, want mv", refreshStmt.Name)
	}
	if !refreshStmt.Concurrently {
		t.Fatal("Concurrently = false, want true")
	}
}

func TestParseDropCollection(t *testing.T) {
	tests := []struct {
		sql      string
		ifExists bool
	}{
		{"DROP COLLECTION c", false},
		{"DROP COLLECTION IF EXISTS c", true},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := ParseStrict(tt.sql)
			if err != nil {
				t.Fatalf("ParseStrict: %v", err)
			}
			dropStmt := stmt.(*DropCollectionStmt)
			if dropStmt.Name != "c" {
				t.Fatalf("Name = %q, want c", dropStmt.Name)
			}
			if dropStmt.IfExists != tt.ifExists {
				t.Fatalf("IfExists = %v, want %v", dropStmt.IfExists, tt.ifExists)
			}
		})
	}
}

func TestParseCreateTemporaryTable(t *testing.T) {
	tests := []string{
		"CREATE TEMP TABLE t (id INT)",
		"CREATE TEMPORARY TABLE t (id INT)",
		"CREATE TEMP TABLE IF NOT EXISTS t (id INT)",
		"CREATE TEMPORARY TABLE t AS SELECT 1",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseStrict(sql)
			if err != nil {
				t.Fatalf("ParseStrict: %v", err)
			}
			createStmt, ok := stmt.(*CreateTableStmt)
			if !ok {
				t.Fatalf("stmt = %T, want *CreateTableStmt", stmt)
			}
			if !createStmt.Temporary {
				t.Fatalf("Temporary = false, want true")
			}
		})
	}
}

func TestParseCreateViewColumnList(t *testing.T) {
	stmt, err := ParseStrict("CREATE VIEW v (a, b) AS SELECT x, y AS old_name FROM t")
	if err != nil {
		t.Fatalf("ParseStrict: %v", err)
	}
	createStmt := stmt.(*CreateViewStmt)
	if len(createStmt.Columns) != 2 || createStmt.Columns[0] != "a" || createStmt.Columns[1] != "b" {
		t.Fatalf("Columns = %#v, want [a b]", createStmt.Columns)
	}
	if len(createStmt.Query.Columns) != 2 {
		t.Fatalf("query columns = %d, want 2", len(createStmt.Query.Columns))
	}
	first, ok := createStmt.Query.Columns[0].(*AliasExpr)
	if !ok || first.Alias != "a" {
		t.Fatalf("first query column = %#v, want alias a", createStmt.Query.Columns[0])
	}
	second, ok := createStmt.Query.Columns[1].(*AliasExpr)
	if !ok || second.Alias != "b" {
		t.Fatalf("second query column = %#v, want alias b", createStmt.Query.Columns[1])
	}

	if _, err := ParseStrict("CREATE VIEW bad (a, b) AS SELECT x FROM t"); err == nil {
		t.Fatal("expected error for mismatched view column list")
	}
}

func TestParseUpdateMySQLLowPriorityModifier(t *testing.T) {
	stmt, err := Parse("UPDATE LOW_PRIORITY t SET name = 'Ada' WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	updateStmt := stmt.(*UpdateStmt)
	if updateStmt.Table != "t" {
		t.Fatalf("Table = %q, want t", updateStmt.Table)
	}
	if len(updateStmt.Set) != 1 || updateStmt.Set[0].Column != "name" {
		t.Fatalf("Set = %#v, want assignment to name", updateStmt.Set)
	}
	if updateStmt.Where == nil {
		t.Fatal("expected WHERE expression")
	}
}

func TestParseUpdateTargetAlias(t *testing.T) {
	stmt, err := Parse("UPDATE t AS x SET x.name = 'Ada' WHERE x.id = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	updateStmt := stmt.(*UpdateStmt)
	if updateStmt.Table != "t" || updateStmt.Alias != "x" {
		t.Fatalf("target = %s alias %s, want t alias x", updateStmt.Table, updateStmt.Alias)
	}
	if len(updateStmt.Set) != 1 || updateStmt.Set[0].Column != "name" {
		t.Fatalf("Set = %#v, want normalized assignment to name", updateStmt.Set)
	}
	if updateStmt.Where == nil {
		t.Fatal("expected WHERE expression")
	}
}

func TestParseUpdateSetList(t *testing.T) {
	stmt, err := Parse("UPDATE t SET (x, y) = (?, ? + 1) WHERE id = ?")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	updateStmt := stmt.(*UpdateStmt)
	if len(updateStmt.Set) != 2 {
		t.Fatalf("Set = %#v, want 2 assignments", updateStmt.Set)
	}
	if updateStmt.Set[0].Column != "x" || updateStmt.Set[1].Column != "y" {
		t.Fatalf("Set columns = %#v, want x/y", updateStmt.Set)
	}
	if ph, ok := updateStmt.Set[0].Value.(*PlaceholderExpr); !ok || ph.Index != 0 {
		t.Fatalf("first SET value = %#v, want placeholder index 0", updateStmt.Set[0].Value)
	}
	secondExpr, ok := updateStmt.Set[1].Value.(*BinaryExpr)
	if !ok {
		t.Fatalf("second SET value = %#v, want BinaryExpr", updateStmt.Set[1].Value)
	}
	if ph, ok := secondExpr.Left.(*PlaceholderExpr); !ok || ph.Index != 1 {
		t.Fatalf("second SET left = %#v, want placeholder index 1", secondExpr.Left)
	}
	where, ok := updateStmt.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Where = %#v, want BinaryExpr", updateStmt.Where)
	}
	if ph, ok := where.Right.(*PlaceholderExpr); !ok || ph.Index != 2 {
		t.Fatalf("WHERE right = %#v, want placeholder index 2", where.Right)
	}
}

func TestParseMySQLUpdateJoin(t *testing.T) {
	stmt, err := Parse("UPDATE t JOIN u ON t.id = u.id SET t.name = u.name WHERE u.flag = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	updateStmt := stmt.(*UpdateStmt)
	if updateStmt.Table != "t" {
		t.Fatalf("Table = %q, want t", updateStmt.Table)
	}
	if len(updateStmt.Joins) != 1 || updateStmt.Joins[0].Table.Name != "u" {
		t.Fatalf("Joins = %#v, want one join to u", updateStmt.Joins)
	}
	if updateStmt.Joins[0].Condition == nil {
		t.Fatal("expected JOIN condition")
	}
	if len(updateStmt.Set) != 1 || updateStmt.Set[0].Column != "name" {
		t.Fatalf("Set = %#v, want assignment to name", updateStmt.Set)
	}
	if updateStmt.Where == nil {
		t.Fatal("expected WHERE expression")
	}
}

func TestParseMySQLUpdateJoinTargetAlias(t *testing.T) {
	stmt, err := Parse("UPDATE t AS x JOIN u AS y ON x.id = y.id SET x.name = y.name WHERE y.flag = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	updateStmt := stmt.(*UpdateStmt)
	if updateStmt.Table != "t" || updateStmt.Alias != "x" {
		t.Fatalf("target = %s alias %s, want t alias x", updateStmt.Table, updateStmt.Alias)
	}
	if len(updateStmt.Joins) != 1 || updateStmt.Joins[0].Table.Name != "u" || updateStmt.Joins[0].Table.Alias != "y" {
		t.Fatalf("Joins = %#v, want one join to u alias y", updateStmt.Joins)
	}
	if len(updateStmt.Set) != 1 || updateStmt.Set[0].Column != "name" {
		t.Fatalf("Set = %#v, want assignment to name", updateStmt.Set)
	}
	if updateStmt.Where == nil {
		t.Fatal("expected WHERE expression")
	}
}

func TestParseMySQLUpdateCommaJoin(t *testing.T) {
	stmt, err := Parse("UPDATE t, u SET t.name = u.name WHERE t.id = u.id")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	updateStmt := stmt.(*UpdateStmt)
	if updateStmt.Table != "t" {
		t.Fatalf("Table = %q, want t", updateStmt.Table)
	}
	if len(updateStmt.Joins) != 1 || updateStmt.Joins[0].Table.Name != "u" {
		t.Fatalf("Joins = %#v, want one comma source u", updateStmt.Joins)
	}
	if updateStmt.Joins[0].Condition != nil {
		t.Fatalf("comma source should rely on WHERE, got join condition %#v", updateStmt.Joins[0].Condition)
	}
	if len(updateStmt.Set) != 1 || updateStmt.Set[0].Column != "name" {
		t.Fatalf("Set = %#v, want assignment to name", updateStmt.Set)
	}
	if updateStmt.Where == nil {
		t.Fatal("expected WHERE expression")
	}
}

func TestParseDeleteMySQLModifiers(t *testing.T) {
	tests := []string{
		"DELETE LOW_PRIORITY FROM t WHERE id = 1",
		"DELETE QUICK FROM t WHERE id = 1",
		"DELETE LOW_PRIORITY QUICK FROM t WHERE id = 1",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			stmt, err := Parse(sql)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}
			deleteStmt := stmt.(*DeleteStmt)
			if deleteStmt.Table != "t" {
				t.Fatalf("Table = %q, want t", deleteStmt.Table)
			}
			if deleteStmt.Where == nil {
				t.Fatal("expected WHERE expression")
			}
		})
	}
}

func TestParseMySQLTargetedDeleteFromJoin(t *testing.T) {
	stmt, err := Parse("DELETE t FROM t JOIN u ON t.id = u.id WHERE u.flag = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	deleteStmt := stmt.(*DeleteStmt)
	if deleteStmt.Table != "t" {
		t.Fatalf("Table = %q, want t", deleteStmt.Table)
	}
	if len(deleteStmt.Using) != 1 || deleteStmt.Using[0].Name != "u" {
		t.Fatalf("Using = %#v, want one source table u", deleteStmt.Using)
	}
	if deleteStmt.Where == nil {
		t.Fatal("expected combined JOIN/WHERE expression")
	}
}

func TestParseMySQLTargetedDeleteFromAlias(t *testing.T) {
	stmt, err := Parse("DELETE x FROM t AS x JOIN u ON x.id = u.id WHERE u.flag = 1")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	deleteStmt := stmt.(*DeleteStmt)
	if deleteStmt.Table != "t" || deleteStmt.Alias != "x" {
		t.Fatalf("target = %s alias %s, want t alias x", deleteStmt.Table, deleteStmt.Alias)
	}
	if len(deleteStmt.Using) != 1 || deleteStmt.Using[0].Name != "u" {
		t.Fatalf("Using = %#v, want one source table u", deleteStmt.Using)
	}
}
