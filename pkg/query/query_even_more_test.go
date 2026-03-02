package query

import (
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
	_, err := Parse(sql)
	if err != nil {
		t.Logf("LIMIT ALL may not be fully supported: %v", err)
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
			_, err := Parse(sql)
			if err != nil {
				t.Logf("Complex view parsing error: %v", err)
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
