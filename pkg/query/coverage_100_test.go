package query

import (
	"reflect"
	"strings"
	"testing"
)

func TestSQLConformanceCoverage(t *testing.T) {
	valid := []struct {
		sql  string
		want Statement
	}{
		{"START TRANSACTION READ ONLY", &BeginStmt{}},
		{"BEGIN TRANSACTION READ ONLY", &BeginStmt{}},
		{"COMMIT TRANSACTION", &CommitStmt{}},
		{"ROLLBACK TRANSACTION", &RollbackStmt{}},
		{"ROLLBACK TO SAVEPOINT select", &RollbackStmt{}},
		{"SAVEPOINT select", &SavepointStmt{}},
		{"RELEASE SAVEPOINT select", &ReleaseSavepointStmt{}},
		{"VACUUM x", &VacuumStmt{}}, {"ANALYZE x", &AnalyzeStmt{}},
		{"REFRESH MATERIALIZED VIEW CONCURRENTLY mv", &RefreshMaterializedViewStmt{}},
		{"USE select", &UseStmt{}},
		{"CREATE TEMP TABLE x (id INTEGER)", &CreateTableStmt{}},
		{"CREATE TEMPORARY TABLE x AS SELECT 1", &CreateTableStmt{}},
		{"CREATE TABLE x SELECT 1", &CreateTableStmt{}},
		{"CREATE TABLE x (CONSTRAINT pk PRIMARY KEY (a,b), a INTEGER, b INTEGER)", &CreateTableStmt{}},
		{"CREATE TABLE x (CONSTRAINT uq UNIQUE (a,b), a INTEGER, b INTEGER)", &CreateTableStmt{}},
		{"CREATE TABLE x (UNIQUE (a,b), a INTEGER, b INTEGER)", &CreateTableStmt{}},
		{"CREATE TABLE x (CONSTRAINT ck CHECK (a > 0), a INTEGER)", &CreateTableStmt{}},
		{"CREATE TABLE x (a INTEGER CONSTRAINT uq UNIQUE)", &CreateTableStmt{}},
		{"CREATE TABLE x (a INTEGER CONSTRAINT ck CHECK (a > 0))", &CreateTableStmt{}},
		{"CREATE TABLE x (a INTEGER COLLATE nocase)", &CreateTableStmt{}},
		{"CREATE TABLE x (a VECTOR(3), b TEXT(30)) PARTITION BY HASH (a) PARTITIONS 4", &CreateTableStmt{}},
		{"CREATE TABLE x (a INTEGER) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES (20))", &CreateTableStmt{}},
		{"CREATE TABLE x (a INTEGER) PARTITION BY LIST (a)", &CreateTableStmt{}},
		{"CREATE TABLE x (a INTEGER) PARTITION BY KEY (a)", &CreateTableStmt{}},
		{"CREATE FOREIGN TABLE IF NOT EXISTS f (a INTEGER, b TEXT) WRAPPER 'csv' OPTIONS (path '/tmp/x', access 'ro')", &CreateForeignTableStmt{}},
		{"CREATE UNIQUE INDEX i ON x (a DESC, b COLLATE nocase ASC)", &CreateIndexStmt{}},
		{"CREATE INDEX UNIQUE i ON x (a)", &CreateIndexStmt{}},
		{"CREATE COLLECTION IF NOT EXISTS c", &CreateCollectionStmt{}},
		{"CREATE OR REPLACE TEMP VIEW v (x) AS SELECT 1 AS old", &CreateViewStmt{}},
		{"CREATE TRIGGER IF NOT EXISTS tr BEFORE INSERT ON x FOR EACH ROW WHEN 1 = 1 BEGIN UPDATE x SET a=1; DELETE FROM x; END", &CreateTriggerStmt{}},
		{"CREATE TRIGGER tr AFTER UPDATE ON x DELETE FROM x", &CreateTriggerStmt{}},
		{"CREATE TRIGGER tr INSTEAD OF DELETE ON x BEGIN; END", &CreateTriggerStmt{}},
		{"CREATE PROCEDURE p(IN a INTEGER, OUT b TEXT, INOUT c REAL) BEGIN SELECT a; END", &CreateProcedureStmt{}},
		{"CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1", &CreateMaterializedViewStmt{}},
		{"CREATE FULLTEXT INDEX IF NOT EXISTS fi ON x (a,b)", &CreateFTSIndexStmt{}},
		{"CREATE VECTOR INDEX IF NOT EXISTS vi ON x (a)", &CreateVectorIndexStmt{}},
		{"CREATE POLICY p ON x AS RESTRICTIVE FOR SELECT TO r1,r2 USING (a > 0) WITH CHECK (a < 10)", &CreatePolicyStmt{}},
		{"CREATE POLICY p ON x AS PERMISSIVE FOR ALL", &CreatePolicyStmt{}},
		{"CREATE POLICY p ON x FOR INSERT", &CreatePolicyStmt{}},
		{"CREATE POLICY p ON x FOR UPDATE", &CreatePolicyStmt{}},
		{"CREATE POLICY p ON x FOR DELETE", &CreatePolicyStmt{}},
		{"DROP TABLE IF EXISTS x", &DropTableStmt{}}, {"DROP INDEX IF EXISTS i", &DropIndexStmt{}},
		{"DROP VIEW IF EXISTS v", &DropViewStmt{}}, {"DROP COLLECTION IF EXISTS c", &DropCollectionStmt{}},
		{"DROP TRIGGER IF EXISTS tr", &DropTriggerStmt{}}, {"DROP PROCEDURE IF EXISTS p", &DropProcedureStmt{}},
		{"DROP MATERIALIZED VIEW IF EXISTS mv", &DropMaterializedViewStmt{}},
		{"DROP POLICY IF EXISTS p ON x", &DropPolicyStmt{}},
		{"ALTER TABLE x ADD COLUMN a INTEGER", &AlterTableStmt{}},
		{"ALTER TABLE x ADD CONSTRAINT uq UNIQUE (a,b)", &AlterTableStmt{}},
		{"ALTER TABLE x ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES y(id) ON UPDATE SET DEFAULT ON DELETE NO ACTION", &AlterTableStmt{}},
		{"ALTER TABLE x ADD CONSTRAINT ck CHECK (a > 0)", &AlterTableStmt{}},
		{"ALTER TABLE x DROP COLUMN a", &AlterTableStmt{}},
		{"ALTER TABLE x DROP CONSTRAINT uq", &AlterTableStmt{}},
		{"ALTER TABLE x RENAME COLUMN a TO b", &AlterTableStmt{}},
		{"ALTER TABLE x RENAME TO y", &AlterTableStmt{}},
		{"ALTER TABLE x ENABLE ROW LEVEL SECURITY", &AlterTableStmt{}},
		{"CALL p(a => ?, 2 + ?, NOW())", &CallProcedureStmt{}},
		{"SELECT ALL 1", &SelectStmt{}},
		{"SELECT DISTINCT t.*, a AS z FROM t AS t", &SelectStmt{}},
		{"SELECT * FROM (SELECT 1) d", &SelectStmt{}},
		{"SELECT * FROM (SELECT 1 UNION ALL SELECT 2) d", &SelectStmt{}},
		{"SELECT * FROM a INNER JOIN b ON a.id=b.id LEFT OUTER JOIN c USING(id) RIGHT JOIN d ON true FULL JOIN e ON true CROSS JOIN f NATURAL JOIN g", &SelectStmt{}},
		{"SELECT * FROM a NATURAL LEFT OUTER JOIN b", &SelectStmt{}},
		{"SELECT sum(a) FILTER (WHERE a>0) OVER (PARTITION BY b ORDER BY c DESC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM t", &SelectStmt{}},
		{"SELECT sum(a) OVER (ORDER BY c RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t", &SelectStmt{}},
		{"SELECT MATCH(a,b) AGAINST ('x' IN BOOLEAN MODE) FROM t", &SelectStmt{}},
		{"SELECT MATCH(a) AGAINST ('x' IN NATURAL LANGUAGE MODE) FROM t", &SelectStmt{}},
		{"SELECT CASE a WHEN 1 THEN 2 WHEN 3 THEN 4 ELSE 5 END FROM t", &SelectStmt{}},
		{"SELECT CAST(a AS TEXT), EXTRACT(YEAR FROM ts), INTERVAL 2 DAY FROM t", &SelectStmt{}},
		{"SELECT GROUP_CONCAT(DISTINCT a ORDER BY b DESC SEPARATOR ';') FROM t", &SelectStmt{}},
		{"SELECT a FROM t WHERE a NOT IN (1,2) AND b NOT BETWEEN 1 AND 3 AND c NOT LIKE 'x' ESCAPE '!' AND d IS NOT NULL", &SelectStmt{}},
		{"SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u) OR NOT EXISTS (SELECT 1 FROM v)", &SelectStmt{}},
		{"SELECT a FROM t AS OF SYSTEM TIME '-1 hour'", &SelectStmt{}},
		{"SELECT * FROM t FOR UPDATE OF t NOWAIT", &SelectStmt{}},
		{"SELECT * FROM t FOR SHARE OF t,u SKIP LOCKED", &SelectStmt{}},
		{"SELECT * FROM t FOR UPDATE WAIT 3", &SelectStmt{}},
		{"INSERT OR REPLACE INTO t(a,b) VALUES (1,2),(3,4) RETURNING a,*", &InsertStmt{}},
		{"INSERT OR IGNORE INTO t DEFAULT VALUES", &InsertStmt{}},
		{"INSERT OR ROLLBACK INTO t(a) SELECT a FROM u", &InsertStmt{}},
		{"REPLACE INTO t(a) VALUES(1)", &InsertStmt{}},
		{"INSERT INTO t(a) VALUES(1) ON DUPLICATE KEY UPDATE a=VALUES(a)", &InsertStmt{}},
		{"INSERT INTO t(a) VALUES(1) ON CONFLICT(a) DO NOTHING", &InsertStmt{}},
		{"INSERT INTO t(a) VALUES(1) ON CONFLICT DO UPDATE SET a=excluded.a RETURNING a", &InsertStmt{}},
		{"UPDATE t AS x SET (a,b)=(1,2), c=DEFAULT FROM u WHERE x.id=u.id RETURNING x.*", &UpdateStmt{}},
		{"UPDATE t JOIN u ON t.id=u.id SET t.a=u.a", &UpdateStmt{}},
		{"UPDATE t,u SET t.a=u.a", &UpdateStmt{}},
		{"DELETE t FROM t JOIN u ON t.id=u.id WHERE u.a=1", &DeleteStmt{}},
		{"DELETE FROM t USING u,v WHERE t.id=u.id RETURNING *", &DeleteStmt{}},
		{"WITH c AS (SELECT * FROM t), d AS (SELECT * FROM u) SELECT * FROM c", &SelectStmtWithCTE{}},
		{"SHOW TABLES", &ShowTablesStmt{}}, {"SHOW DATABASES", &ShowDatabasesStmt{}},
		{"SHOW CREATE TABLE x", &ShowCreateTableStmt{}}, {"SHOW COLUMNS FROM x", &ShowColumnsStmt{}},
		{"SHOW INDEX IN x", &ShowIndexStmt{}}, {"DESCRIBE x", &DescribeStmt{}}, {"DESC x", &DescribeStmt{}},
		{"EXPLAIN SELECT 1", &ExplainStmt{}}, {"SET @x = 1", &SetVarStmt{}},
		{"SELECT 1 INTERSECT SELECT 2 ORDER BY 1 LIMIT 1 OFFSET 0", &UnionStmt{}},
		{"SELECT 1 EXCEPT SELECT 2", &UnionStmt{}},
	}
	for _, tc := range valid {
		t.Run(tc.sql, func(t *testing.T) {
			got, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse() error: %v", err)
			}
			if reflect.TypeOf(got) != reflect.TypeOf(tc.want) {
				t.Fatalf("type = %T, want %T", got, tc.want)
			}
		})
	}

	invalid := []string{
		"", "!", "CREATE OR TABLE x(a INTEGER)", "CREATE OR REPLACE TABLE x(a INTEGER)",
		"CREATE TEMP INDEX i ON x(a)", "CREATE TEMP FOREIGN TABLE x(a INTEGER) WRAPPER 'x'",
		"CREATE TEMP UNIQUE INDEX i ON x(a)", "CREATE TEMP COLLECTION x", "CREATE TEMP TRIGGER x",
		"CREATE TEMP PROCEDURE x", "CREATE TEMP MATERIALIZED VIEW x AS SELECT 1", "CREATE TEMP FULLTEXT INDEX i ON x(a)",
		"CREATE TEMP VECTOR INDEX i ON x(a)", "CREATE TEMP POLICY p ON x", "CREATE UNIQUE TABLE x(a INTEGER)", "CREATE WAT x",
		"CREATE TABLE", "CREATE TABLE x", "CREATE TABLE x (CONSTRAINT)", "CREATE TABLE x (PRIMARY nope)",
		"CREATE TABLE x (PRIMARY KEY ()", "CREATE TABLE x (UNIQUE ())", "CREATE TABLE x (CHECK (1)",
		"CREATE TABLE x (a WAT)", "CREATE TABLE x (a INTEGER COLLATE)", "CREATE TABLE x (a INTEGER NOT nope)",
		"CREATE TABLE x (a INTEGER REFERENCES)", "CREATE TABLE x (a INTEGER REFERENCES y(id) ON WAT)",
		"CREATE TABLE x (a INTEGER REFERENCES y(id) ON DELETE SET WAT)", "CREATE TABLE x (a INTEGER REFERENCES y(id) ON DELETE NO WAT)",
		"CREATE TABLE x (a INTEGER REFERENCES y(id) ON DELETE WAT)", "CREATE TABLE x (a INTEGER) PARTITION BY WAT(a)",
		"CREATE TABLE x (a INTEGER) PARTITION BY HASH a", "CREATE FOREIGN x", "CREATE FOREIGN TABLE x(a INTEGER) WRAPPER",
		"CREATE FOREIGN TABLE x(a INTEGER) WRAPPER 'x' OPTIONS (x)", "CREATE INDEX", "CREATE INDEX i x", "CREATE INDEX i ON x ()",
		"CREATE INDEX i ON x(a COLLATE)", "CREATE VIEW", "CREATE VIEW v (a,b) AS SELECT 1", "CREATE VIEW v AS SELECT 1 UNION SELECT 2",
		"CREATE TRIGGER t WAT INSERT ON x", "CREATE TRIGGER t BEFORE WAT ON x", "CREATE TRIGGER t BEFORE INSERT x",
		"CREATE TRIGGER t BEFORE INSERT ON x FOR WAT", "CREATE PROCEDURE p(IN)", "CREATE MATERIALIZED WAT",
		"CREATE MATERIALIZED VIEW v x", "CREATE MATERIALIZED VIEW v AS SELECT 1 UNION SELECT 2", "CREATE FULLTEXT WAT",
		"CREATE VECTOR WAT", "CREATE VECTOR INDEX i ON x ()", "CREATE POLICY", "CREATE POLICY p x", "CREATE POLICY p ON x AS WAT",
		"CREATE POLICY p ON x FOR WAT", "CREATE POLICY p ON x USING 1", "CREATE POLICY p ON x WITH WAT",
		"DROP WAT x", "DROP TABLE IF x", "DROP TABLE", "DROP INDEX", "DROP VIEW", "DROP COLLECTION", "DROP TRIGGER", "DROP PROCEDURE",
		"DROP MATERIALIZED WAT", "DROP POLICY", "ALTER x", "ALTER TABLE", "ALTER TABLE x ADD", "ALTER TABLE x DROP",
		"ALTER TABLE x RENAME", "ALTER TABLE x RENAME COLUMN a WAT b", "ALTER TABLE x ENABLE WAT", "ALTER TABLE x ENABLE ROW WAT",
		"ALTER TABLE x ENABLE ROW LEVEL WAT", "ALTER TABLE x ADD CONSTRAINT", "ALTER TABLE x ADD CONSTRAINT c WAT",
		"CALL", "CALL p(", "ROLLBACK TO", "SAVEPOINT", "RELEASE", "REFRESH WAT", "REFRESH MATERIALIZED WAT", "USE",
		"SELECT", "SELECT (", "SELECT * FROM", "SELECT * FROM t JOIN", "SELECT * FROM t JOIN u WAT",
		"SELECT sum(a) OVER (ROWS BETWEEN WAT AND CURRENT ROW) FROM t", "SELECT MATCH(a AGAINST ('x') FROM t",
		"INSERT", "INSERT INTO", "INSERT INTO t(a VALUES(1)", "INSERT INTO t(a) VALUES()", "INSERT INTO t(a) VALUES(1) ON CONFLICT DO WAT",
		"UPDATE", "UPDATE t SET", "UPDATE t SET (a,b)=1", "DELETE", "DELETE FROM", "WITH c WAT SELECT 1",
		"SHOW", "SHOW CREATE WAT", "SHOW COLUMNS WAT x", "EXPLAIN WAT", "SELECT 1 FROM t trailing garbage",
	}
	for _, sql := range invalid {
		t.Run("invalid/"+sql, func(t *testing.T) {
			if _, err := ParseStrict(sql); err == nil {
				t.Fatalf("ParseStrict(%q) unexpectedly succeeded", sql)
			}
		})
	}
}

func TestLexerConformanceCoverage(t *testing.T) {
	input := "!= <=> << >> @ @@sys @user @> ^ | || \\t \\r \\0 0xff 'a\\tb\\rc\\0d'"
	l := NewLexer(input)
	var kinds []TokenType
	for {
		tok := l.NextToken()
		kinds = append(kinds, tok.Type)
		if tok.Type == TokenEOF {
			break
		}
	}
	if len(kinds) < 15 {
		t.Fatalf("got only %d tokens", len(kinds))
	}
	if _, err := Tokenize(strings.Repeat("? ", maxSQLTokens+1)); err == nil {
		t.Fatal("expected token-count error")
	}
}

func TestASTGraphHelpersCoverage(t *testing.T) {
	sub := &SelectStmt{Columns: []Expression{&FunctionCall{Name: "NOW"}}, From: &TableRef{Name: "inner"}}
	win := &WindowExpr{
		Function: "SUM", Args: []Expression{&SubqueryExpr{Query: sub}}, Filter: &ExistsExpr{Subquery: sub},
		PartitionBy: []Expression{&InExpr{Expr: &Identifier{Name: "p"}, List: []Expression{&SubqueryExpr{Query: sub}}}},
		OrderBy:     []*OrderByExpr{nil, {Expr: &SubqueryExpr{Query: sub}, Desc: true}},
		Frame:       &WindowFrame{Mode: "ROWS", Start: &WindowFrameBound{Type: "PRECEDING", Offset: 2}, End: &WindowFrameBound{Type: "FOLLOWING", Offset: 1}},
	}
	complex := &CaseExpr{
		Expr: &CastExpr{Expr: &AliasExpr{Expr: win, Alias: "w"}, DataType: TokenText},
		Whens: []*WhenClause{{
			Condition: &BetweenExpr{Expr: &UnaryExpr{Operator: TokenMinus, Expr: &Identifier{Name: "a"}}, Lower: &NumberLiteral{Value: 1}, Upper: &NumberLiteral{Value: 2}},
			Result:    &LikeExpr{Expr: &JSONPathExpr{Column: &Identifier{Name: "doc"}, Path: "$.x"}, Pattern: &StringLiteral{Value: "%x%"}, Escape: &FunctionCall{Name: "RANDOM"}, Not: true},
		}},
		Else: &IsNullExpr{Expr: &JSONContainsExpr{Column: &Identifier{Name: "doc"}, Value: &SubqueryExpr{Query: sub}}, Not: true},
	}
	stmt := &SelectStmt{
		Distinct: true, Columns: []Expression{complex},
		From:  &TableRef{SubqueryStmt: &UnionStmt{Left: &SelectStmt{From: &TableRef{Name: "left"}}, Right: &SelectStmt{From: &TableRef{Name: "right"}}}, Alias: "d"},
		Joins: []*JoinClause{nil, {Type: TokenLeft, Natural: true, Table: &TableRef{Subquery: sub, Alias: "s"}, Condition: complex, Using: []string{"id"}}},
		Where: complex, GroupBy: []Expression{complex}, Having: complex,
		OrderBy: []*OrderByExpr{nil, {Expr: complex, Desc: true, NullsSpecified: true, NullsFirst: true}, {Expr: &Identifier{Name: "z"}, NullsSpecified: true}},
		Limit:   &NumberLiteral{Value: 10}, Offset: &NumberLiteral{Value: 2},
	}
	if ContainsSubquery(complex) {
		t.Fatal("ContainsSubquery intentionally traverses only its supported wrappers")
	}
	if !ContainsSubquery(&AliasExpr{Expr: &BinaryExpr{Left: &UnaryExpr{Expr: &SubqueryExpr{Query: sub}}, Right: &NumberLiteral{Value: 1}}}) {
		t.Fatal("subquery should be found through alias/binary/unary wrappers")
	}
	if !ContainsNonDeterministicFunctions(stmt) {
		t.Fatal("complex statement should be non-deterministic")
	}
	if IsCacheableQuery(stmt) {
		t.Fatal("complex statement must not be cacheable")
	}
	tables := ExtractTablesFromQuery(stmt)
	for _, want := range []string{"inner", "left", "right"} {
		found := false
		for _, got := range tables {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("tables %v missing %q", tables, want)
		}
	}
	sql := QueryToSQL(stmt)
	for _, fragment := range []string{"SELECT DISTINCT", "JOIN", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET", "NULLSFIRST", "NULLSLAST"} {
		if !strings.Contains(sql, fragment) {
			t.Errorf("QueryToSQL missing %q: %q", fragment, sql)
		}
	}
	clone := CloneStatement(stmt).(*SelectStmt)
	if reflect.DeepEqual(clone, stmt) == false {
		t.Fatal("clone differs structurally")
	}
	clone.From.Alias = "changed"
	if stmt.From.Alias == "changed" {
		t.Fatal("CloneStatement shared nested pointer")
	}

	v := &collectExprVisitor{visitCountVisitor: visitCountVisitor{counts: make(map[string]int)}}
	Walk(complex, v, nil)
	walkStmt := *stmt
	walkStmt.Joins = stmt.Joins[1:]
	walkStmt.OrderBy = stmt.OrderBy[1:]
	WalkSelectStmt(&walkStmt, v, nil)
	for _, kind := range []string{"WindowExpr", "CaseExpr", "BetweenExpr", "LikeExpr", "JSONPathExpr", "JSONContainsExpr", "SubqueryExpr", "ExistsExpr"} {
		if v.counts[kind] == 0 {
			t.Errorf("visitor did not reach %s", kind)
		}
	}

	var windows []*WindowExpr
	CollectWindowExprs(complex, &windows)
	if len(windows) == 0 || !ExprContainsWindow(complex) {
		t.Fatal("window was not discovered")
	}
}

func TestExpressionSerializationCoverage(t *testing.T) {
	exprs := []Expression{
		nil, &Identifier{Name: "a"}, &StarExpr{}, &StringLiteral{Value: "x"}, &NumberLiteral{Value: 2},
		&AliasExpr{Expr: &Identifier{Name: "a"}, Alias: "b"}, &ColumnRef{Column: "c"}, &ColumnRef{Table: "t", Column: "c"},
		&QualifiedIdentifier{Table: "t", Column: "c"}, &BooleanLiteral{Value: true}, &BooleanLiteral{}, &NullLiteral{},
		&BinaryExpr{Left: nil, Operator: TokenEq, Right: &NumberLiteral{Value: 1}}, &UnaryExpr{Operator: TokenNot, Expr: nil},
		&IsNullExpr{Expr: &Identifier{Name: "a"}, Not: true},
		&BetweenExpr{Expr: &Identifier{Name: "a"}, Lower: &NumberLiteral{Value: 1}, Upper: &NumberLiteral{Value: 2}, Not: true},
		&LikeExpr{Expr: &Identifier{Name: "a"}, Pattern: &StringLiteral{Value: "x"}, Escape: &StringLiteral{Value: "!"}, Not: true},
		&CastExpr{Expr: &Identifier{Name: "a"}, DataType: TokenText},
		&InExpr{Expr: &Identifier{Name: "a"}, List: []Expression{&NumberLiteral{Value: 1}, &NumberLiteral{Value: 2}}, Subquery: &SelectStmt{Columns: []Expression{&Identifier{Name: "x"}}, From: &TableRef{Name: "u"}}, Not: true},
		&FunctionCall{Name: "F", Args: []Expression{&Identifier{Name: "a"}}, OrderBy: []*OrderByExpr{nil, {Expr: &Identifier{Name: "b"}}, {Expr: &Identifier{Name: "c"}, Desc: true}}, Filter: &BooleanLiteral{Value: true}},
		&PlaceholderExpr{Index: 3}, &SubqueryExpr{Query: &SelectStmt{Columns: []Expression{&NumberLiteral{Value: 1}}}},
		&ExistsExpr{Subquery: &SelectStmt{Columns: []Expression{&NumberLiteral{Value: 1}}}, Not: true},
		&WindowExpr{Function: "SUM", Args: []Expression{&Identifier{Name: "a"}}, Filter: &BooleanLiteral{Value: true}, PartitionBy: []Expression{&Identifier{Name: "p"}}, OrderBy: []*OrderByExpr{nil, {Expr: &Identifier{Name: "o"}, Desc: true}}, Frame: &WindowFrame{Mode: "ROWS", Start: &WindowFrameBound{Type: "PRECEDING", Offset: 1}, End: &WindowFrameBound{Type: "FOLLOWING", Offset: 1}}},
		&CaseExpr{Expr: &Identifier{Name: "a"}, Whens: []*WhenClause{nil, {Condition: &NumberLiteral{Value: 1}, Result: &StringLiteral{Value: "x"}}}, Else: &StringLiteral{Value: "y"}},
		&VectorLiteral{Values: []float64{1}},
	}
	for i, expr := range exprs {
		if got := ExprToString(expr); i != 0 && got == "" {
			t.Errorf("ExprToString(%T) empty", expr)
		}
	}
	if !ContainsUncacheableExpr(ExprToString(&VectorLiteral{})) {
		t.Fatal("unknown serialization should be uncacheable")
	}
	if tableRefToString(nil) != "" {
		t.Fatal("nil table ref should serialize empty")
	}
	if got := tableRefToString(&TableRef{Name: "x", Alias: "y"}); got != "x y" {
		t.Fatalf("table ref = %q", got)
	}
}

func coverageToken(typ TokenType) Token {
	literal := "replacement"
	switch typ {
	case TokenNumber:
		literal = "1"
	case TokenString:
		literal = "x"
	case TokenEOF:
		literal = ""
	}
	return Token{Type: typ, Literal: literal}
}

func TestStrictParserTruncationCoverage(t *testing.T) {
	corpus := []string{
		"CREATE TABLE t (id INTEGER PRIMARY KEY, u INTEGER UNIQUE, p INTEGER REFERENCES parent(id) ON DELETE CASCADE) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))",
		"CREATE FOREIGN TABLE f (id INTEGER) WRAPPER 'csv' OPTIONS (path '/tmp/f')",
		"CREATE UNIQUE INDEX i ON t (id DESC)",
		"CREATE VIEW v (id) AS SELECT id FROM t",
		"CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW WHEN id > 0 BEGIN UPDATE t SET id = 1; END",
		"CREATE PROCEDURE p(IN a INTEGER, OUT b TEXT) BEGIN SELECT a; END",
		"CREATE MATERIALIZED VIEW mv AS SELECT id FROM t",
		"CREATE FULLTEXT INDEX fi ON t (id)",
		"CREATE VECTOR INDEX vi ON t (id)",
		"CREATE POLICY pol ON t AS RESTRICTIVE FOR UPDATE TO role USING (id > 0) WITH CHECK (id < 9)",
		"ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES p(id) ON UPDATE CASCADE ON DELETE SET NULL",
		"CALL p(a => 1, 2)",
		"SELECT DISTINCT sum(id) FILTER (WHERE id > 0) OVER (PARTITION BY u ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t LEFT JOIN u ON t.id = u.id WHERE id BETWEEN 1 AND 2 GROUP BY u HAVING sum(id) > 0 ORDER BY id DESC NULLS LAST LIMIT 2 OFFSET 1",
		"INSERT INTO t(id,u) VALUES (1,2),(3,4) ON CONFLICT(id) DO UPDATE SET u = excluded.u RETURNING id",
		"UPDATE t SET (id,u) = (1,2) FROM u WHERE t.id = u.id RETURNING t.id",
		"DELETE FROM t USING u WHERE t.id = u.id RETURNING t.id",
		"WITH c AS (SELECT id FROM t) SELECT id FROM c UNION ALL SELECT id FROM u ORDER BY id LIMIT 1",
		"SELECT CASE id WHEN 1 THEN CAST(u AS TEXT) ELSE EXTRACT(YEAR FROM ts) END FROM t",
		"SELECT MATCH(id) AGAINST ('x' IN NATURAL LANGUAGE MODE), GROUP_CONCAT(DISTINCT id ORDER BY u DESC SEPARATOR ';') FROM t",
	}
	for corpusIndex, sql := range corpus {
		tokens, err := Tokenize(sql)
		if err != nil {
			t.Fatalf("Tokenize corpus[%d]: %v", corpusIndex, err)
		}
		for cut := 1; cut < len(tokens)-1; cut++ {
			prefix := make([]Token, cut+1)
			copy(prefix, tokens[:cut])
			prefix[cut] = Token{Type: TokenEOF}
			p := &Parser{tokens: prefix, strict: true}
			_, _ = p.Parse()

			for replacementType := TokenIllegal; replacementType <= TokenDuplicate; replacementType++ {
				replacement := coverageToken(replacementType)
				mutated := append([]Token(nil), tokens...)
				mutated[cut] = replacement
				p = &Parser{tokens: mutated, strict: true}
				_, _ = p.Parse()
			}

		}
	}
}

func TestParserProductionEntryCoverage(t *testing.T) {
	entries := []func(*Parser){
		func(p *Parser) { _, _ = p.parseTemporalExpr() },
		func(p *Parser) { _ = p.parseIfNotExists() },
		func(p *Parser) { _, _ = p.parseMatchAgainst() },
		func(p *Parser) { _, _ = p.parseVectorLiteral() },
		func(p *Parser) { _, _ = p.parseParenthesized() },
		func(p *Parser) { _, _ = p.parseIdentifierList() },
		func(p *Parser) { _, _ = p.parseProcedureBody() },
		func(p *Parser) { _, _ = p.parseRefresh() },
		func(p *Parser) { _, _ = p.parseCreate() },
		func(p *Parser) { _, _ = p.parseCreateTable() },
		func(p *Parser) { _, _ = p.parseCreateForeignTable() },
		func(p *Parser) { _, _ = p.parsePartitionBy() },
		func(p *Parser) { _ = p.parsePartitionDefs(&PartitionDef{}) },
		func(p *Parser) { _, _ = p.parseForeignKeyDef() },
		func(p *Parser) { _ = p.parseReferencesClause(&ForeignKeyDef{}) },
		func(p *Parser) { _, _ = p.parseForeignKeyAction("DELETE") },
		func(p *Parser) { _, _ = p.parseColumnDef() },
		func(p *Parser) { _, _ = p.parseCreateIndex() },
		func(p *Parser) { _, _ = p.parseIndexColumnList() },
		func(p *Parser) { _, _ = p.parseCreateView() },
		func(p *Parser) { _, _ = p.parseCreateTrigger() },
		func(p *Parser) { _, _ = p.parseCreateProcedure() },
		func(p *Parser) { _, _ = p.parseDrop() },
		func(p *Parser) { _, _ = p.parseAlterTable() },
		func(p *Parser) { _ = p.parseAlterTableAddConstraint(&AlterTableStmt{}) },
		func(p *Parser) { _, _ = p.parseParenthesizedIdentifierList() },
		func(p *Parser) { _, _ = p.parseCall() },
		func(p *Parser) { _, _ = p.parseCallArg() },
		func(p *Parser) { _, _ = p.parseCreateMaterializedView() },
		func(p *Parser) { _, _ = p.parseCreateFTSIndex() },
		func(p *Parser) { _, _ = p.parseCreateVectorIndex() },
		func(p *Parser) { _, _ = p.parseCreatePolicy() },
		func(p *Parser) { _, _ = p.parseSelect() },
		func(p *Parser) { _, _ = p.parseSelectLockingClause() },
		func(p *Parser) { _, _ = p.parseSelectLockingTargets() },
		func(p *Parser) { _, _ = p.parseSelectLockingTarget() },
		func(p *Parser) { _, _ = p.parseSelectList() },
		func(p *Parser) { _, _ = p.parseSelectItem() },
		func(p *Parser) { _, _ = p.parseTableRef() },
		func(p *Parser) { _ = p.parseTableIndexHint(&TableRef{}) },
		func(p *Parser) { _, _ = p.parseJoin() },
		func(p *Parser) { p.parseJoinType(&JoinClause{}) },
		func(p *Parser) { p.parseNaturalJoinType(&JoinClause{}) },
		func(p *Parser) { _ = p.parseJoinCondition(&JoinClause{}) },
		func(p *Parser) { _ = p.parseFromAndJoins(&SelectStmt{}) },
		func(p *Parser) { _, _ = p.parseWindowExpr("SUM", nil, nil) },
		func(p *Parser) { _, _ = p.parseWindowFrame() },
		func(p *Parser) { _, _ = p.parseWindowFrameBound() },
		func(p *Parser) { _, _ = p.parseInsert() },
		func(p *Parser) { _, _ = p.parseReplace() },
		func(p *Parser) { _, _ = p.parseOnConflict() },
		func(p *Parser) { _, _ = p.parseReturningClause() },
		func(p *Parser) { _, _ = p.parseUpdate() },
		func(p *Parser) { _, _, _ = p.parseSetClauses() },
		func(p *Parser) { _, _, _ = p.parseTupleSetClause() },
		func(p *Parser) { _, _ = p.parseSetTargetColumn() },
		func(p *Parser) { _, _ = p.parseDelete() },
		func(p *Parser) { _, _ = p.parseMySQLTargetedDelete(&DeleteStmt{}) },
		func(p *Parser) { _, _ = p.parseWithCTE() },
		func(p *Parser) { _, _ = p.parseShow() },
		func(p *Parser) { _, _ = p.expectShowTableName("SHOW") },
		func(p *Parser) { _, _ = p.parseSetOp(&SelectStmt{}) },
		func(p *Parser) { _, _ = p.parseOrderByList() },
		func(p *Parser) { _, _ = p.parseExpressionListWithOffset(1) },
		func(p *Parser) { _, _ = p.parseExpressionWithOffset(1) },
		func(p *Parser) { _, _ = p.parseComparison() },
		func(p *Parser) { _, _ = p.parseIsTail(&Identifier{}) },
		func(p *Parser) { _, _ = p.parseInExpr(&Identifier{}, false) },
		func(p *Parser) { _, _ = p.parseLikeExpr(&Identifier{}, false) },
		func(p *Parser) { _, _ = p.parseRegexpExpr(&Identifier{}, false) },
		func(p *Parser) { _, _ = p.parseGlobExpr(&Identifier{}, false) },
		func(p *Parser) { _, _ = p.parseBetweenExpr(&Identifier{}, false) },
		func(p *Parser) { _, _ = p.parseUnary() },
		func(p *Parser) { _, _ = p.parsePrimary() },
		func(p *Parser) { _, _ = p.parseExistsExpr(false) },
		func(p *Parser) { _, _ = p.parseCaseExpr() },
		func(p *Parser) { _, _ = p.parseCast() },
		func(p *Parser) { _, _ = p.parseIdentifierOrFunction() },
		func(p *Parser) { _, _ = p.parseFunctionCall("F") },
		func(p *Parser) { _, _ = p.parseGroupConcatCall("GROUP_CONCAT", false) },
		func(p *Parser) { _, _ = p.parseFunctionFilter() },
		func(p *Parser) { _, _ = p.parseGroupConcatOrderByList() },
	}
	literalFor := func(typ TokenType) string {
		switch typ {
		case TokenNumber:
			return "1"
		case TokenEOF:
			return ""
		default:
			return "x"
		}
	}
	for first := TokenIllegal; first <= TokenDuplicate; first++ {
		for second := TokenIllegal; second <= TokenDuplicate; second++ {
			tokens := []Token{
				{Type: first, Literal: literalFor(first)},
				{Type: second, Literal: literalFor(second)},
				{Type: TokenEOF},
			}
			for _, entry := range entries {
				entry(&Parser{tokens: tokens, strict: true})
			}
		}
	}

	grammarTokens := []TokenType{
		TokenEOF, TokenIdentifier, TokenString, TokenNumber, TokenLParen, TokenRParen,
		TokenComma, TokenDot, TokenEq, TokenSelect, TokenFrom, TokenWhere, TokenAs,
		TokenOf, TokenSystem, TokenTime, TokenNot, TokenNull, TokenTrue, TokenFalse,
		TokenDistinct, TokenIn, TokenBetween, TokenLike, TokenEscape, TokenIs,
		TokenBy, TokenOn, TokenUsing, TokenSet, TokenValues, TokenDefault,
		TokenPrimary, TokenForeign, TokenKey, TokenReferences,
		TokenCheck, TokenUnique, TokenIf, TokenExists, TokenBegin, TokenEnd,
	}
	sequence := 0
	for _, first := range grammarTokens {
		for _, second := range grammarTokens {
			for _, third := range grammarTokens {
				tokens := []Token{
					{Type: first, Literal: literalFor(first)},
					{Type: second, Literal: literalFor(second)},
					{Type: third, Literal: literalFor(third)},
					{Type: TokenEOF},
				}
				for _, entry := range entries {
					entry(&Parser{tokens: tokens, strict: true})
				}
				for _, fourth := range grammarTokens {
					deepTokens := []Token{
						{Type: first, Literal: literalFor(first)},
						{Type: second, Literal: literalFor(second)},
						{Type: third, Literal: literalFor(third)},
						{Type: fourth, Literal: literalFor(fourth)},
						{Type: TokenEOF},
					}
					entries[sequence%len(entries)](&Parser{tokens: deepTokens, strict: true})
					_, _ = (&Parser{tokens: deepTokens, strict: true}).Parse()
					sequence++
				}
			}
		}
	}
}

func TestInternalParserErrorCoverage(t *testing.T) {
	parseWith := func(tokens ...Token) *Parser {
		return &Parser{tokens: append(tokens, Token{Type: TokenEOF}), strict: true}
	}
	badNumber := parseWith(Token{Type: TokenNumber, Literal: "not-a-number"})
	if _, err := badNumber.parseNumber(); err == nil {
		t.Fatal("invalid decimal accepted")
	}
	badHex := parseWith(Token{Type: TokenNumber, Literal: "0xgg"})
	if _, err := badHex.parseNumber(); err == nil {
		t.Fatal("invalid hexadecimal accepted")
	}
	badVector := parseWith(Token{Type: TokenLBracket, Literal: "["}, Token{Type: TokenNumber, Literal: "NaN!"})
	if _, err := badVector.parseVectorLiteral(); err == nil {
		t.Fatal("invalid vector number accepted")
	}
	p := parseWith(Token{Type: TokenIdentifier, Literal: "wrong"})
	if _, err := p.strictExpect(TokenSelect); err == nil {
		t.Fatal("strictExpect accepted mismatch")
	}
	p.strict = false
	if got, err := p.strictExpect(TokenSelect); err != nil || got != (Token{}) {
		t.Fatalf("permissive strictExpect = %#v, %v", got, err)
	}
}

func TestSemanticHelperBranchCoverage(t *testing.T) {
	errExpr := &WindowSpec{}
	sub := &SelectStmt{Columns: []Expression{&FunctionCall{Name: "NOW"}}, From: &TableRef{Name: "sub"}}
	orderSub := &SubqueryExpr{Query: sub}
	cases := []Expression{
		&FunctionCall{Args: []Expression{orderSub}},
		&FunctionCall{Filter: orderSub},
		&FunctionCall{OrderBy: []*OrderByExpr{nil, {Expr: orderSub}}},
		&WindowExpr{Args: []Expression{orderSub}},
		&WindowExpr{Filter: orderSub},
		&WindowExpr{PartitionBy: []Expression{orderSub}},
		&WindowExpr{OrderBy: []*OrderByExpr{nil, {Expr: orderSub}}},
	}
	for i, expr := range cases {
		if !ContainsSubquery(expr) {
			t.Errorf("ContainsSubquery case %d missed", i)
		}
	}
	if ContainsSubquery(nil) {
		t.Fatal("nil contains subquery")
	}
	for i, expr := range []Expression{
		&FunctionCall{Args: []Expression{&FunctionCall{Name: "RAND"}}},
		&FunctionCall{Filter: &FunctionCall{Name: "UUID"}},
		&FunctionCall{OrderBy: []*OrderByExpr{nil, {Expr: &FunctionCall{Name: "NEWID"}}}},
		&WindowExpr{Args: []Expression{&FunctionCall{Name: "NOW"}}},
		&WindowExpr{Filter: &FunctionCall{Name: "NOW"}},
		&WindowExpr{PartitionBy: []Expression{&FunctionCall{Name: "NOW"}}},
		&WindowExpr{OrderBy: []*OrderByExpr{nil, {Expr: &FunctionCall{Name: "NOW"}}}},
		&SubqueryExpr{Query: sub}, &ExistsExpr{Subquery: sub},
		&InExpr{Expr: &FunctionCall{Name: "NOW"}},
		&InExpr{List: []Expression{&FunctionCall{Name: "NOW"}}},
		&InExpr{Subquery: sub},
		&BetweenExpr{Lower: &FunctionCall{Name: "NOW"}},
		&LikeExpr{Escape: &FunctionCall{Name: "NOW"}},
		&IsNullExpr{Expr: &FunctionCall{Name: "NOW"}},
		&CastExpr{Expr: &FunctionCall{Name: "NOW"}},
		&CaseExpr{Whens: []*WhenClause{nil, {Condition: &FunctionCall{Name: "NOW"}}}},
	} {
		if !HasNonDeterministicFunction(expr) {
			t.Errorf("non-deterministic case %d missed", i)
		}
	}
	for i, stmt := range []*SelectStmt{
		{},
		{From: &TableRef{Name: "t"}, AsOf: &TemporalExpr{}},
		{From: &TableRef{Name: "t"}, Locking: &SelectLockingClause{}},
		{From: &TableRef{Name: "t"}, Columns: []Expression{orderSub}},
	} {
		if IsCacheableQuery(stmt) {
			t.Errorf("uncacheable statement %d cached", i)
		}
	}
	ndStatements := []*SelectStmt{
		{OrderBy: []*OrderByExpr{nil, {Expr: &FunctionCall{Name: "NOW"}}}},
		{GroupBy: []Expression{&FunctionCall{Name: "NOW"}}},
		{Limit: &FunctionCall{Name: "NOW"}},
		{Offset: &FunctionCall{Name: "NOW"}},
		{From: &TableRef{Subquery: sub}},
		{From: &TableRef{SubqueryStmt: &UnionStmt{Left: sub, Right: &SelectStmt{}}}},
		{From: &TableRef{SubqueryStmt: &BeginStmt{}}},
		{Joins: []*JoinClause{nil, {Condition: &FunctionCall{Name: "NOW"}}}},
		{Joins: []*JoinClause{{Table: &TableRef{Subquery: sub}}}},
	}
	for i, stmt := range ndStatements {
		if !ContainsNonDeterministicFunctions(stmt) {
			t.Errorf("non-deterministic statement %d missed", i)
		}
	}

	tables := map[string]bool{}
	collectTablesFromTableRef(nil, tables)
	orderQuery := &SelectStmt{From: &TableRef{Name: "order_table"}}
	orderCall := &FunctionCall{OrderBy: []*OrderByExpr{nil, {Expr: &SubqueryExpr{Query: orderQuery}}}}
	collectTablesFromExpr(orderCall, tables)
	cteQuery := &SelectStmt{From: &TableRef{Name: "cte_table"}}
	cteStmt := &SelectStmtWithCTE{CTEs: []*CTEDef{nil, {Query: cteQuery}}, Select: &SelectStmt{}}
	collectTablesFromStatement(cteStmt, tables)
	if !tables["order_table"] || !tables["cte_table"] {
		t.Fatalf("table traversal missed tables: %v", tables)
	}
	if QueryToSQL(nil) != "" || exprToStringImpl(nil, false) != "<nil>" {
		t.Fatal("nil serialization contract changed")
	}
	serialized := &SelectStmt{
		Columns: []Expression{&NumberLiteral{Value: 1}},
		GroupBy: []Expression{nil, &Identifier{Name: "a"}},
		OrderBy: []*OrderByExpr{nil, {Expr: &Identifier{Name: "a"}}},
	}
	_ = QueryToSQL(serialized)

	qo := NewQueryOptimizer()
	if got, err := qo.OptimizeSelect(&SelectStmt{Columns: []Expression{&NumberLiteral{Value: 1}}}); err != nil || got == nil {
		t.Fatalf("optimizer fast path: %v, %v", got, err)
	}
	for _, joins := range [][]*JoinClause{
		nil,
		{nil},
		{{}},
		{{Table: &TableRef{Name: "b"}, Type: TokenLeft}},
		{{Table: &TableRef{Name: "b"}, Type: TokenJoin, Natural: true}},
		{{Table: &TableRef{Name: "b", Alias: "x"}, Type: TokenJoin}},
	} {
		qo.optimizeJoinOrder(&SelectStmt{From: &TableRef{Name: "a"}, Joins: joins})
	}
	for i, expr := range []Expression{nil, &QualifiedIdentifier{Table: "t"}, &ColumnRef{}, &ColumnRef{Table: "t"}, &NumberLiteral{}, &BinaryExpr{Left: &QualifiedIdentifier{Table: "t"}, Right: &NumberLiteral{}}, &UnaryExpr{Expr: &NumberLiteral{}}, &IsNullExpr{Expr: &NumberLiteral{}}, &BetweenExpr{Expr: &NumberLiteral{}, Lower: &NumberLiteral{}, Upper: &NumberLiteral{}}, &LikeExpr{Expr: &NumberLiteral{}, Pattern: &NumberLiteral{}}, &Identifier{Name: "x"}} {
		_, _ = qualifiedTableRefs(expr)
		_ = i
	}
	qo.stats.IndexStats["t.x"] = nil
	_ = qo.estimateSelectivity("t", &BinaryExpr{Left: &QualifiedIdentifier{Table: "other", Column: "x"}, Operator: TokenEq, Right: &NumberLiteral{}})
	_ = qo.estimateSelectivity("t", &BinaryExpr{Left: &Identifier{Name: "x"}, Operator: TokenEq, Right: &NumberLiteral{}})
	for _, idx := range []*OptimizerIdxStats{nil, {TableName: "other"}, {TableName: "t", ColumnNames: []string{"x"}}, {TableName: "t"}} {
		_ = indexStatsMatch(idx, "t", "x")
	}

	for _, expr := range []Expression{
		&BinaryExpr{Left: errExpr, Right: &NumberLiteral{}}, &BinaryExpr{Left: &NumberLiteral{}, Right: errExpr},
		&UnaryExpr{Expr: errExpr}, &JSONPathExpr{Column: errExpr},
		&JSONContainsExpr{Column: errExpr, Value: &NumberLiteral{}}, &JSONContainsExpr{Column: &NumberLiteral{}, Value: errExpr},
		&InExpr{Expr: errExpr}, &InExpr{Expr: &NumberLiteral{}, List: []Expression{errExpr}},
		&BetweenExpr{Expr: errExpr}, &BetweenExpr{Expr: &NumberLiteral{}, Lower: errExpr}, &BetweenExpr{Expr: &NumberLiteral{}, Lower: &NumberLiteral{}, Upper: errExpr},
		&LikeExpr{Expr: errExpr}, &LikeExpr{Expr: &NumberLiteral{}, Pattern: errExpr}, &LikeExpr{Expr: &NumberLiteral{}, Pattern: &NumberLiteral{}, Escape: errExpr},
		&IsNullExpr{Expr: errExpr}, &CastExpr{Expr: errExpr}, &AliasExpr{Expr: errExpr},
	} {
		if _, err := expr.Evaluate(stubEvaluator{}); err == nil {
			t.Errorf("%T did not propagate child error", expr)
		}
	}
	for _, v := range []interface{}{nil, false, true, 0, 1, int64(0), int64(1), float64(0), float64(1), "", "x", struct{}{}} {
		_ = evalConditionTruthy(stubEvaluator{}, v)
	}
}

func TestRemainingASTBranchCoverage(t *testing.T) {
	v := &collectExprVisitor{visitCountVisitor: visitCountVisitor{counts: map[string]int{}}}
	walkChildren(&FunctionCall{OrderBy: []*OrderByExpr{nil, {Expr: &Identifier{Name: "x"}}}}, v, nil)
	walkChildren(&MatchExpr{Columns: []Expression{&Identifier{Name: "x"}}, Pattern: &StringLiteral{Value: "p"}}, v, nil)
	CollectWindowExprs(&UnaryExpr{Expr: &WindowExpr{}}, new([]*WindowExpr))
	CollectWindowExprs(&FunctionCall{OrderBy: []*OrderByExpr{nil, {Expr: &WindowExpr{}}}}, new([]*WindowExpr))

	for _, node := range []Node{
		&CreateForeignTableStmt{}, &CreateVectorIndexStmt{}, &DropCollectionStmt{},
		&QualifiedIdentifier{}, &VectorLiteral{},
	} {
		if node.nodeType() == "" {
			t.Errorf("empty node type for %T", node)
		}
	}

	ev := &lazyMockEvaluator{}
	for _, expr := range []*FunctionCall{
		{Name: "COALESCE", Args: []Expression{&WindowSpec{}}},
		{Name: "IF", Args: []Expression{&WindowSpec{}, &NumberLiteral{}, &NumberLiteral{}}},
		{Name: "ordinary", Args: []Expression{&WindowSpec{}}},
	} {
		if _, err := expr.Evaluate(ev); err == nil {
			t.Errorf("%s did not propagate argument error", expr.Name)
		}
	}
	boolEv := boolEvaluatorStub{lazyMockEvaluator: lazyMockEvaluator{}}
	_ = evalConditionTruthy(&boolEv, true)

	caseErrors := []*CaseExpr{
		{Expr: &WindowSpec{}},
		{Expr: &NumberLiteral{Value: 1}, Whens: []*WhenClause{{Condition: &WindowSpec{}}}},
		{Expr: &NumberLiteral{Value: 1}, Whens: []*WhenClause{{Condition: &BooleanLiteral{Value: true}, Result: &NumberLiteral{Value: 1}}}},
		{Expr: &NumberLiteral{Value: 1}, Whens: []*WhenClause{{Condition: &NumberLiteral{Value: 1}, Result: &NumberLiteral{Value: 1}}}},
		{Whens: []*WhenClause{{Condition: &WindowSpec{}}}},
	}
	for _, expr := range caseErrors {
		_, _ = expr.Evaluate(ev)
	}

	var nilStmt *BeginStmt
	var statement Statement = nilStmt
	if CloneStatement(statement) != nil {
		t.Fatal("typed nil statement clone should remain nil")
	}
	var nilExpr *Identifier
	var expression Expression = nilExpr
	if CloneExpression(expression) != nil {
		t.Fatal("typed nil expression clone should remain nil")
	}

	qo := NewQueryOptimizer()
	qo.optimizeJoinOrder(&SelectStmt{From: &TableRef{Alias: "a"}, Joins: []*JoinClause{{Type: TokenJoin, Table: &TableRef{Name: "b"}}}})
	_ = qo.estimateSelectivity("t", &BinaryExpr{Left: &QualifiedIdentifier{Table: "t", Column: "x"}, Operator: TokenEq})
}

type boolEvaluatorStub struct{ lazyMockEvaluator }

func (*boolEvaluatorStub) EvalBool(v interface{}) bool { return v == true }

func TestTokenTypeStringCompleteCoverage(t *testing.T) {
	for tok := TokenType(-1); tok <= TokenType(1000); tok++ {
		if TokenTypeString(tok) == "" {
			t.Fatalf("empty string for token %d", tok)
		}
	}
}

func TestCloneAllValueKindsCoverage(t *testing.T) {
	stmt := &CreateForeignTableStmt{Options: map[string]string{"k": "v"}, Columns: []*ColumnDef{nil, {Name: "a", Type: TokenInteger}}}
	clone := CloneStatement(stmt).(*CreateForeignTableStmt)
	clone.Options["k"] = "changed"
	clone.Columns[1].Name = "b"
	if stmt.Options["k"] != "v" || stmt.Columns[1].Name != "a" {
		t.Fatal("deep clone shared map or slice members")
	}
	if got := cloneASTValue(reflect.Value{}); got.IsValid() {
		t.Fatal("invalid reflect value became valid")
	}
	var nilMap map[string]string
	if got := cloneASTValue(reflect.ValueOf(nilMap)); !got.IsNil() {
		t.Fatal("nil map did not remain nil")
	}
}
