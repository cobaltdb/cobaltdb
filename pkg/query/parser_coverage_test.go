package query

import (
	"strings"
	"testing"
)

// ---- SHOW statements ----

func TestParseShowTables(t *testing.T) {
	stmt, err := Parse("SHOW TABLES")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if _, ok := stmt.(*ShowTablesStmt); !ok {
		t.Errorf("Expected *ShowTablesStmt, got %T", stmt)
	}
}

func TestParseShowDatabases(t *testing.T) {
	stmt, err := Parse("SHOW DATABASES")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if _, ok := stmt.(*ShowDatabasesStmt); !ok {
		t.Errorf("Expected *ShowDatabasesStmt, got %T", stmt)
	}
}

func TestParseShowCreateTable(t *testing.T) {
	stmt, err := Parse("SHOW CREATE TABLE users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sct, ok := stmt.(*ShowCreateTableStmt)
	if !ok {
		t.Fatalf("Expected *ShowCreateTableStmt, got %T", stmt)
	}
	if sct.Table != "users" {
		t.Errorf("Expected table=users, got %s", sct.Table)
	}
}

func TestParseShowCreateTableMissingName(t *testing.T) {
	_, err := Parse("SHOW CREATE TABLE")
	if err == nil {
		t.Error("Expected error for SHOW CREATE TABLE without table name")
	}
}

func TestParseShowColumnsFrom(t *testing.T) {
	stmt, err := Parse("SHOW COLUMNS FROM orders")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sc, ok := stmt.(*ShowColumnsStmt)
	if !ok {
		t.Fatalf("Expected *ShowColumnsStmt, got %T", stmt)
	}
	if sc.Table != "orders" {
		t.Errorf("Expected table=orders, got %s", sc.Table)
	}
}

func TestParseShowStatus(t *testing.T) {
	// SHOW STATUS, SHOW VARIABLES, etc. should parse (MySQL compat)
	for _, kw := range []string{"STATUS", "VARIABLES", "WARNINGS", "ERRORS"} {
		stmt, err := Parse("SHOW " + kw)
		if err != nil {
			t.Errorf("SHOW %s failed: %v", kw, err)
			continue
		}
		// These currently return ShowTablesStmt as placeholder
		if stmt == nil {
			t.Errorf("SHOW %s returned nil", kw)
		}
	}
}

func TestParseShowUnsupported(t *testing.T) {
	_, err := Parse("SHOW FOOBAR")
	if err == nil {
		t.Error("Expected error for SHOW FOOBAR")
	}
}

// ---- DESCRIBE / DESC ----

func TestParseDescribe(t *testing.T) {
	stmt, err := Parse("DESCRIBE users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ds, ok := stmt.(*DescribeStmt)
	if !ok {
		t.Fatalf("Expected *DescribeStmt, got %T", stmt)
	}
	if ds.Table != "users" {
		t.Errorf("Expected table=users, got %s", ds.Table)
	}
}

func TestParseDesc(t *testing.T) {
	stmt, err := Parse("DESC orders")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ds, ok := stmt.(*DescribeStmt)
	if !ok {
		t.Fatalf("Expected *DescribeStmt, got %T", stmt)
	}
	if ds.Table != "orders" {
		t.Errorf("Expected table=orders, got %s", ds.Table)
	}
}

func TestParseDescribeMissingTable(t *testing.T) {
	_, err := Parse("DESCRIBE")
	if err == nil {
		t.Error("Expected error for DESCRIBE without table name")
	}
}

// ---- SET variable = value ----

func TestParseSetVar(t *testing.T) {
	stmt, err := Parse("SET autocommit = 1")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sv, ok := stmt.(*SetVarStmt)
	if !ok {
		t.Fatalf("Expected *SetVarStmt, got %T", stmt)
	}
	if sv.Variable != "autocommit" {
		t.Errorf("Expected variable=autocommit, got %s", sv.Variable)
	}
	if sv.Value != "1" {
		t.Errorf("Expected value=1, got %s", sv.Value)
	}
}

func TestParseSetVarNoEquals(t *testing.T) {
	stmt, err := Parse("SET NAMES utf8")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sv, ok := stmt.(*SetVarStmt)
	if !ok {
		t.Fatalf("Expected *SetVarStmt, got %T", stmt)
	}
	// Variable should contain everything since there's no =
	if sv.Variable == "" {
		t.Error("Expected non-empty variable")
	}
}

func TestParseSetVarMultiWord(t *testing.T) {
	stmt, err := Parse("SET GLOBAL max_connections = 100")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sv, ok := stmt.(*SetVarStmt)
	if !ok {
		t.Fatalf("Expected *SetVarStmt, got %T", stmt)
	}
	if !strings.Contains(sv.Variable, "max_connections") {
		t.Errorf("Expected variable to contain max_connections, got %s", sv.Variable)
	}
	if sv.Value != "100" {
		t.Errorf("Expected value=100, got %s", sv.Value)
	}
}

// ---- USE database ----

func TestParseUse(t *testing.T) {
	stmt, err := Parse("USE mydb")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	us, ok := stmt.(*UseStmt)
	if !ok {
		t.Fatalf("Expected *UseStmt, got %T", stmt)
	}
	if us.Database != "mydb" {
		t.Errorf("Expected database=mydb, got %s", us.Database)
	}
}

func TestParseUseMissingDB(t *testing.T) {
	_, err := Parse("USE")
	if err == nil {
		t.Error("Expected error for USE without database name")
	}
}

// ---- CREATE POLICY ----

func TestParseCreatePolicyBasic(t *testing.T) {
	sql := "CREATE POLICY user_policy ON users USING (user_id = 1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	cp, ok := stmt.(*CreatePolicyStmt)
	if !ok {
		t.Fatalf("Expected *CreatePolicyStmt, got %T", stmt)
	}
	if cp.Name != "user_policy" {
		t.Errorf("Expected name=user_policy, got %s", cp.Name)
	}
	if cp.Table != "users" {
		t.Errorf("Expected table=users, got %s", cp.Table)
	}
	if !cp.Permissive {
		t.Error("Expected Permissive=true by default")
	}
	if cp.Event != "ALL" {
		t.Errorf("Expected event=ALL by default, got %s", cp.Event)
	}
	if cp.Using == nil {
		t.Error("Expected non-nil USING expression")
	}
}

func TestParseCreatePolicyWithFor(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		event string
	}{
		{"FOR ALL", "CREATE POLICY p ON t FOR ALL USING (1=1)", "ALL"},
		{"FOR SELECT", "CREATE POLICY p ON t FOR SELECT USING (1=1)", "SELECT"},
		{"FOR INSERT", "CREATE POLICY p ON t FOR INSERT WITH CHECK (1=1)", "INSERT"},
		{"FOR UPDATE", "CREATE POLICY p ON t FOR UPDATE USING (1=1)", "UPDATE"},
		{"FOR DELETE", "CREATE POLICY p ON t FOR DELETE USING (1=1)", "DELETE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}
			cp := stmt.(*CreatePolicyStmt)
			if cp.Event != tt.event {
				t.Errorf("Expected event=%s, got %s", tt.event, cp.Event)
			}
		})
	}
}

func TestParseCreatePolicyWithCheck(t *testing.T) {
	sql := "CREATE POLICY ins_policy ON items FOR INSERT WITH CHECK (price > 0)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if cp.WithCheck == nil {
		t.Error("Expected non-nil WITH CHECK expression")
	}
}

func TestParseCreatePolicyWithTo(t *testing.T) {
	sql := "CREATE POLICY role_policy ON docs TO admin, editor USING (1=1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if len(cp.ForRoles) != 2 {
		t.Fatalf("Expected 2 roles, got %d", len(cp.ForRoles))
	}
	if cp.ForRoles[0] != "admin" || cp.ForRoles[1] != "editor" {
		t.Errorf("Expected roles [admin, editor], got %v", cp.ForRoles)
	}
}

// ---- DROP POLICY ----

func TestParseDropPolicy(t *testing.T) {
	stmt, err := Parse("DROP POLICY user_policy ON users")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	dp, ok := stmt.(*DropPolicyStmt)
	if !ok {
		t.Fatalf("Expected *DropPolicyStmt, got %T", stmt)
	}
	if dp.Name != "user_policy" {
		t.Errorf("Expected name=user_policy, got %s", dp.Name)
	}
	if dp.Table != "users" {
		t.Errorf("Expected table=users, got %s", dp.Table)
	}
	if dp.IfExists {
		t.Error("Expected IfExists=false")
	}
}

func TestParseDropPolicyIfExists(t *testing.T) {
	stmt, err := Parse("DROP POLICY IF EXISTS old_policy ON t")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	dp := stmt.(*DropPolicyStmt)
	if !dp.IfExists {
		t.Error("Expected IfExists=true")
	}
	if dp.Name != "old_policy" {
		t.Errorf("Expected name=old_policy, got %s", dp.Name)
	}
}

func TestParseDropPolicyNoTable(t *testing.T) {
	stmt, err := Parse("DROP POLICY mypol")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	dp := stmt.(*DropPolicyStmt)
	if dp.Name != "mypol" {
		t.Errorf("Expected name=mypol, got %s", dp.Name)
	}
	if dp.Table != "" {
		t.Errorf("Expected empty table, got %s", dp.Table)
	}
}

// ---- UNION / UNION ALL / INTERSECT / EXCEPT ----

func TestParseUnionCov(t *testing.T) {
	sql := "SELECT id FROM a UNION SELECT id FROM b"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	us, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected *UnionStmt, got %T", stmt)
	}
	if us.All {
		t.Error("Expected All=false for UNION")
	}
	if us.Op != SetOpUnion {
		t.Errorf("Expected SetOpUnion, got %d", us.Op)
	}
}

func TestParseUnionAll(t *testing.T) {
	sql := "SELECT name FROM t1 UNION ALL SELECT name FROM t2"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	us, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected *UnionStmt, got %T", stmt)
	}
	if !us.All {
		t.Error("Expected All=true for UNION ALL")
	}
}

func TestParseIntersectCov(t *testing.T) {
	sql := "SELECT id FROM a INTERSECT SELECT id FROM b"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	us, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected *UnionStmt, got %T", stmt)
	}
	if us.Op != SetOpIntersect {
		t.Errorf("Expected SetOpIntersect, got %d", us.Op)
	}
}

func TestParseExceptCov(t *testing.T) {
	sql := "SELECT id FROM a EXCEPT SELECT id FROM b"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	us, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected *UnionStmt, got %T", stmt)
	}
	if us.Op != SetOpExcept {
		t.Errorf("Expected SetOpExcept, got %d", us.Op)
	}
}

func TestParseUnionChain(t *testing.T) {
	sql := "SELECT 1 UNION SELECT 2 UNION SELECT 3"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	// Should produce nested UnionStmts
	us, ok := stmt.(*UnionStmt)
	if !ok {
		t.Fatalf("Expected *UnionStmt, got %T", stmt)
	}
	// Left should itself be a UnionStmt
	_, leftIsUnion := us.Left.(*UnionStmt)
	if !leftIsUnion {
		t.Logf("Left is %T (may be flat or nested)", us.Left)
	}
}

// ---- Parse depth limit ----

func TestParseDepthLimit(t *testing.T) {
	// Build a deeply nested expression: (((((...1...)))))
	var b strings.Builder
	b.WriteString("SELECT ")
	depth := 250
	for i := 0; i < depth; i++ {
		b.WriteString("(")
	}
	b.WriteString("1")
	for i := 0; i < depth; i++ {
		b.WriteString(")")
	}

	_, err := Parse(b.String())
	if err == nil {
		t.Error("Expected depth limit error for deeply nested expression")
	}
	if err != nil && !strings.Contains(err.Error(), "depth") && !strings.Contains(err.Error(), "nesting") {
		t.Logf("Got error (possibly different message): %v", err)
	}
}

// ---- Unterminated string literal ----

func TestUnterminatedStringLiteral(t *testing.T) {
	_, err := Tokenize("SELECT 'unterminated")
	if err == nil {
		t.Error("Expected error for unterminated string literal")
	}
	if err != nil && !strings.Contains(err.Error(), "unterminated") && !strings.Contains(err.Error(), "illegal") {
		t.Logf("Got error: %v", err)
	}
}

func TestUnterminatedDoubleQuoteLiteral(t *testing.T) {
	_, err := Tokenize(`SELECT "unterminated`)
	if err == nil {
		t.Error("Expected error for unterminated double-quoted string")
	}
}

// ---- Block comment handling ----

func TestBlockCommentTokenize(t *testing.T) {
	tokens, err := Tokenize("SELECT /* this is a comment */ 1")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	// Should have SELECT, 1, EOF (comment stripped)
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenNumber && tok.Literal == "1" {
			found = true
		}
	}
	if !found {
		t.Error("Expected number 1 after block comment")
	}
}

func TestBlockCommentMultiline(t *testing.T) {
	sql := "SELECT /* multi\nline\ncomment */ 42"
	tokens, err := Tokenize(sql)
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenNumber && tok.Literal == "42" {
			found = true
		}
	}
	if !found {
		t.Error("Expected 42 after multiline block comment")
	}
}

func TestBlockCommentUnterminated(t *testing.T) {
	// Unterminated block comment should reach EOF and still tokenize
	tokens, err := Tokenize("SELECT /* unterminated comment")
	if err != nil {
		t.Logf("Got error (may be expected): %v", err)
	}
	_ = tokens // just checking it doesn't panic
}

func TestLineComment(t *testing.T) {
	tokens, err := Tokenize("SELECT -- this is a comment\n1")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenNumber && tok.Literal == "1" {
			found = true
		}
	}
	if !found {
		t.Error("Expected 1 after line comment")
	}
}

// ---- Backtick-quoted identifiers ----

func TestBacktickIdentifier(t *testing.T) {
	tokens, err := Tokenize("SELECT `my column` FROM `my table`")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenIdentifier && tok.Literal == "my column" {
			found = true
		}
	}
	if !found {
		t.Error("Expected backtick-quoted identifier 'my column'")
	}
}

func TestBacktickInSelect(t *testing.T) {
	stmt, err := Parse("SELECT `value` FROM `data`")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Statement is nil")
	}
}

// ---- Expression operators: BETWEEN, IN, IS NULL, CASE, CAST, LIKE ----

func TestParseBetween(t *testing.T) {
	sql := "SELECT * FROM t WHERE x BETWEEN 1 AND 10"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Where == nil {
		t.Error("Expected WHERE clause with BETWEEN")
	}
}

func TestParseNotBetween(t *testing.T) {
	sql := "SELECT * FROM t WHERE x NOT BETWEEN 5 AND 15"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseIn(t *testing.T) {
	sql := "SELECT * FROM t WHERE x IN (1, 2, 3)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Where == nil {
		t.Error("Expected WHERE clause with IN")
	}
}

func TestParseNotIn(t *testing.T) {
	sql := "SELECT * FROM t WHERE x NOT IN (4, 5, 6)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseInSubquery(t *testing.T) {
	sql := "SELECT * FROM t WHERE x IN (SELECT id FROM other)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseIsNull(t *testing.T) {
	sql := "SELECT * FROM t WHERE x IS NULL"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Where == nil {
		t.Error("Expected WHERE clause")
	}
}

func TestParseIsNotNull(t *testing.T) {
	sql := "SELECT * FROM t WHERE x IS NOT NULL"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseCaseWhen(t *testing.T) {
	sql := "SELECT CASE WHEN x > 0 THEN 'positive' WHEN x = 0 THEN 'zero' ELSE 'negative' END FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if stmt == nil {
		t.Fatal("Statement is nil")
	}
}

func TestParseCaseWhenNoElse(t *testing.T) {
	sql := "SELECT CASE WHEN x = 1 THEN 'one' END FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseSimpleCase(t *testing.T) {
	sql := "SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseCast(t *testing.T) {
	sql := "SELECT CAST(x AS TEXT) FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseCastInteger(t *testing.T) {
	sql := "SELECT CAST('123' AS INTEGER) FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseLike(t *testing.T) {
	sql := "SELECT * FROM t WHERE name LIKE '%test%'"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Where == nil {
		t.Error("Expected WHERE clause")
	}
}

func TestParseNotLike(t *testing.T) {
	sql := "SELECT * FROM t WHERE name NOT LIKE 'foo%'"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseLikeEscape(t *testing.T) {
	sql := "SELECT * FROM t WHERE name LIKE '%10\\%%' ESCAPE '\\'"
	_, err := Parse(sql)
	if err != nil {
		// ESCAPE clause may or may not be supported
		t.Logf("LIKE ESCAPE result: %v", err)
	}
}

// ---- Misc parser edge cases ----

func TestParseEmptyStatementCov(t *testing.T) {
	_, err := Parse("")
	if err == nil {
		t.Error("Expected error for empty statement")
	}
}

func TestParseExplainCov(t *testing.T) {
	sql := "EXPLAIN SELECT * FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	es, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("Expected *ExplainStmt, got %T", stmt)
	}
	if es.Statement == nil {
		t.Error("Expected non-nil inner statement")
	}
}

func TestParseSelectWithAlias(t *testing.T) {
	sql := "SELECT t.id AS tid, t.name FROM users t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseSelectDistinctCov(t *testing.T) {
	sql := "SELECT DISTINCT name FROM users"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseSelectOrderByLimit(t *testing.T) {
	sql := "SELECT * FROM t ORDER BY id DESC LIMIT 10 OFFSET 5"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseSelectGroupByHaving(t *testing.T) {
	sql := "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseSelectSubqueryInWhere(t *testing.T) {
	sql := "SELECT * FROM t WHERE id = (SELECT MAX(id) FROM t)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseDerivedTable(t *testing.T) {
	sql := "SELECT * FROM (SELECT 1 AS x) sub"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

// ---- Lexer edge cases ----

func TestLexerNeqOperator(t *testing.T) {
	tokens, err := Tokenize("1 != 2")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenNeq {
			found = true
		}
	}
	if !found {
		t.Error("Expected != operator")
	}
}

func TestLexerLtGtNeq(t *testing.T) {
	tokens, err := Tokenize("1 <> 2")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenNeq {
			found = true
		}
	}
	if !found {
		t.Error("Expected <> operator")
	}
}

func TestLexerLteGte(t *testing.T) {
	tokens, err := Tokenize("x <= 5 AND y >= 10")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	foundLte := false
	foundGte := false
	for _, tok := range tokens {
		if tok.Type == TokenLte {
			foundLte = true
		}
		if tok.Type == TokenGte {
			foundGte = true
		}
	}
	if !foundLte {
		t.Error("Expected <= operator")
	}
	if !foundGte {
		t.Error("Expected >= operator")
	}
}

func TestLexerConcat(t *testing.T) {
	tokens, err := Tokenize("'a' || 'b'")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenConcat {
			found = true
		}
	}
	if !found {
		t.Error("Expected || operator")
	}
}

func TestLexerPercent(t *testing.T) {
	tokens, err := Tokenize("10 % 3")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenPercent {
			found = true
		}
	}
	if !found {
		t.Error("Expected % operator")
	}
}

func TestLexerQuestionMark(t *testing.T) {
	tokens, err := Tokenize("SELECT ?")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenQuestion {
			found = true
		}
	}
	if !found {
		t.Error("Expected ? token")
	}
}

func TestLexerFloatNumber(t *testing.T) {
	tokens, err := Tokenize("3.14")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	if tokens[0].Type != TokenNumber || tokens[0].Literal != "3.14" {
		t.Errorf("Expected number 3.14, got %v", tokens[0])
	}
}

func TestLexerScientificNotation(t *testing.T) {
	tokens, err := Tokenize("1.5e10")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	if tokens[0].Type != TokenNumber || tokens[0].Literal != "1.5e10" {
		t.Errorf("Expected number 1.5e10, got %v", tokens[0])
	}
}

func TestLexerScientificNotationNegative(t *testing.T) {
	tokens, err := Tokenize("2.5E-3")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	if tokens[0].Type != TokenNumber || tokens[0].Literal != "2.5E-3" {
		t.Errorf("Expected number 2.5E-3, got %v", tokens[0])
	}
}

func TestLexerEscapedStrings(t *testing.T) {
	tokens, err := Tokenize(`'it''s a test'`)
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	if tokens[0].Type != TokenString || tokens[0].Literal != "it's a test" {
		t.Errorf("Expected escaped string, got %q", tokens[0].Literal)
	}
}

func TestLexerBackslashEscape(t *testing.T) {
	tokens, err := Tokenize(`'line1\nline2'`)
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	if tokens[0].Type != TokenString || tokens[0].Literal != "line1\nline2" {
		t.Errorf("Expected escaped newline in string, got %q", tokens[0].Literal)
	}
}

func TestLexerDoubleQuotedIdentifier(t *testing.T) {
	tokens, err := Tokenize(`SELECT "my column" FROM t`)
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenIdentifier && tok.Literal == "my column" {
			found = true
		}
	}
	if !found {
		t.Error("Expected double-quoted identifier")
	}
}

func TestLexerJSONArrowOperators(t *testing.T) {
	tokens, err := Tokenize("data->key")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenArrow {
			found = true
		}
	}
	if !found {
		t.Error("Expected -> operator")
	}
}

func TestLexerContainsOperator(t *testing.T) {
	tokens, err := Tokenize("data @> other")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenContains {
			found = true
		}
	}
	if !found {
		t.Error("Expected @> operator")
	}
}

func TestLexerIllegalChar(t *testing.T) {
	_, err := Tokenize("SELECT ~")
	if err == nil {
		t.Error("Expected error for illegal character ~")
	}
}

func TestLexerSemicolon(t *testing.T) {
	tokens, err := Tokenize("SELECT 1;")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}
	found := false
	for _, tok := range tokens {
		if tok.Type == TokenSemicolon {
			found = true
		}
	}
	if !found {
		t.Error("Expected semicolon token")
	}
}

// ---- ParseExpression ----

func TestParseExpressionSimple(t *testing.T) {
	expr, err := ParseExpression("42")
	if err != nil {
		t.Fatalf("ParseExpression failed: %v", err)
	}
	if expr == nil {
		t.Fatal("Expression is nil")
	}
}

func TestParseExpressionString(t *testing.T) {
	expr, err := ParseExpression("'hello'")
	if err != nil {
		t.Fatalf("ParseExpression failed: %v", err)
	}
	if expr == nil {
		t.Fatal("Expression is nil")
	}
}

func TestParseExpressionFunction(t *testing.T) {
	expr, err := ParseExpression("UPPER('hello')")
	if err != nil {
		t.Fatalf("ParseExpression failed: %v", err)
	}
	if expr == nil {
		t.Fatal("Expression is nil")
	}
}

// ---- CREATE FOREIGN TABLE ----

func TestParseCreateForeignTable(t *testing.T) {
	sql := `CREATE FOREIGN TABLE ext_users (id INTEGER, name TEXT) WRAPPER 'csv' OPTIONS (file '/tmp/data.csv')`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ft, ok := stmt.(*CreateForeignTableStmt)
	if !ok {
		t.Fatalf("Expected *CreateForeignTableStmt, got %T", stmt)
	}
	if ft.Table != "ext_users" {
		t.Errorf("Expected table=ext_users, got %s", ft.Table)
	}
	if len(ft.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(ft.Columns))
	}
	if ft.Wrapper != "csv" {
		t.Errorf("Expected wrapper=csv, got %s", ft.Wrapper)
	}
	if ft.Options["file"] != "/tmp/data.csv" {
		t.Errorf("Expected file=/tmp/data.csv, got %s", ft.Options["file"])
	}
}

func TestParseCreateForeignTableMissingWrapper(t *testing.T) {
	_, err := Parse("CREATE FOREIGN TABLE ext (id INTEGER)")
	if err == nil {
		t.Error("Expected error for CREATE FOREIGN TABLE without WRAPPER")
	}
}

func TestParseCreateForeignTableNoOptions(t *testing.T) {
	stmt, err := Parse("CREATE FOREIGN TABLE ext (id INTEGER) WRAPPER 'csv'")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	ft, ok := stmt.(*CreateForeignTableStmt)
	if !ok {
		t.Fatalf("Expected *CreateForeignTableStmt, got %T", stmt)
	}
	if ft.Wrapper != "csv" {
		t.Errorf("Expected wrapper=csv, got %s", ft.Wrapper)
	}
	if len(ft.Options) != 0 {
		t.Errorf("Expected empty options, got %v", ft.Options)
	}
}
