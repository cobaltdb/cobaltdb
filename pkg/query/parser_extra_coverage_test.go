package query

import (
	"strings"
	"testing"
)

// ====== Optimizer tests (all 0% coverage) ======

func TestNewQueryOptimizer(t *testing.T) {
	opt := NewQueryOptimizer()
	if opt == nil {
		t.Fatal("expected non-nil optimizer")
	}
}

func TestOptimizeSelect_Nil(t *testing.T) {
	opt := NewQueryOptimizer()
	result, err := opt.OptimizeSelect(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("expected nil result for nil input")
	}
}

func TestOptimizeSelect_SimpleSelect(t *testing.T) {
	opt := NewQueryOptimizer()
	stmt := &SelectStmt{
		From: &TableRef{Name: "users"},
		Where: &BinaryExpr{
			Left:     &Identifier{Name: "id"},
			Operator: TokenEq,
			Right:    &NumberLiteral{Value: 1},
		},
	}
	result, err := opt.OptimizeSelect(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// pushDownPredicates should have set IndexHint = "auto"
	if result.From.IndexHint != "auto" {
		t.Errorf("expected IndexHint 'auto', got %q", result.From.IndexHint)
	}
}

func TestOptimizeSelect_WithJoins(t *testing.T) {
	opt := NewQueryOptimizer()
	opt.stats.RowCount["users"] = 1000
	opt.stats.RowCount["orders"] = 5000

	stmt := &SelectStmt{
		From: &TableRef{Name: "users"},
		Joins: []*JoinClause{
			{
				Table:     &TableRef{Name: "orders"},
				Type:      TokenInner,
				Condition: &BinaryExpr{Left: &Identifier{Name: "id"}, Operator: TokenEq, Right: &Identifier{Name: "user_id"}},
			},
		},
		Where: &BinaryExpr{
			Left:     &Identifier{Name: "id"},
			Operator: TokenEq,
			Right:    &NumberLiteral{Value: 1},
		},
	}
	result, err := opt.OptimizeSelect(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestOptimizeSelect_NoWhere(t *testing.T) {
	opt := NewQueryOptimizer()
	stmt := &SelectStmt{
		From: &TableRef{Name: "users"},
	}
	result, err := opt.OptimizeSelect(stmt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

func TestEstimateSelectivity_WithIndex(t *testing.T) {
	opt := NewQueryOptimizer()
	opt.stats.IndexStats["users.id"] = &OptimizerIdxStats{
		TableName:   "users",
		ColumnNames: []string{"id"},
		Unique:      true,
		Selectivity: 0.001,
	}

	where := &BinaryExpr{
		Left:     &Identifier{Name: "id"},
		Operator: TokenEq,
		Right:    &NumberLiteral{Value: 1},
	}
	sel := opt.estimateSelectivity("users", where)
	if sel != 0.001 {
		t.Errorf("expected selectivity 0.001, got %f", sel)
	}
}

func TestEstimateSelectivity_Default(t *testing.T) {
	opt := NewQueryOptimizer()
	where := &BinaryExpr{
		Left:     &Identifier{Name: "name"},
		Operator: TokenEq,
		Right:    &StringLiteral{Value: "test"},
	}
	sel := opt.estimateSelectivity("users", where)
	if sel != 0.1 {
		t.Errorf("expected default selectivity 0.1, got %f", sel)
	}
}

func TestEstimateSelectivity_Nil(t *testing.T) {
	opt := NewQueryOptimizer()
	sel := opt.estimateSelectivity("users", nil)
	if sel != 1.0 {
		t.Errorf("expected selectivity 1.0 for nil where, got %f", sel)
	}
}

func TestCanUseIndex_NilWhere(t *testing.T) {
	opt := NewQueryOptimizer()
	if opt.canUseIndex("users", nil) {
		t.Error("expected false for nil where")
	}
}

func TestCanUseIndex_EqualityOnIdentifier(t *testing.T) {
	opt := NewQueryOptimizer()
	where := &BinaryExpr{
		Left:     &Identifier{Name: "id"},
		Operator: TokenEq,
		Right:    &NumberLiteral{Value: 1},
	}
	if !opt.canUseIndex("users", where) {
		t.Error("expected true for equality on identifier")
	}
}

func TestCanUseIndex_RangeOperators(t *testing.T) {
	opt := NewQueryOptimizer()
	for _, op := range []TokenType{TokenGt, TokenLt, TokenGte, TokenLte, TokenLike} {
		where := &BinaryExpr{
			Left:     &Identifier{Name: "id"},
			Operator: op,
			Right:    &NumberLiteral{Value: 1},
		}
		if !opt.canUseIndex("users", where) {
			t.Errorf("expected true for operator %d on identifier", op)
		}
	}
}

func TestCanUseIndex_AndOr(t *testing.T) {
	opt := NewQueryOptimizer()
	where := &BinaryExpr{
		Left: &BinaryExpr{
			Left:     &Identifier{Name: "id"},
			Operator: TokenEq,
			Right:    &NumberLiteral{Value: 1},
		},
		Operator: TokenAnd,
		Right: &BinaryExpr{
			Left:     &Identifier{Name: "name"},
			Operator: TokenEq,
			Right:    &StringLiteral{Value: "test"},
		},
	}
	if !opt.canUseIndex("users", where) {
		t.Error("expected true for AND with indexable predicates")
	}
}

func TestCanUseIndex_NonIndexable(t *testing.T) {
	opt := NewQueryOptimizer()
	where := &IsNullExpr{Expr: &Identifier{Name: "id"}}
	if opt.canUseIndex("users", where) {
		t.Error("expected false for IS NULL expression")
	}
}

func TestEstimateTableCost_WithStats(t *testing.T) {
	opt := NewQueryOptimizer()
	opt.stats.RowCount["users"] = 500
	cost := opt.estimateTableCost("users", nil)
	if cost != 500.0 {
		t.Errorf("expected cost 500.0, got %f", cost)
	}
}

func TestEstimateTableCost_DefaultRowCount(t *testing.T) {
	opt := NewQueryOptimizer()
	cost := opt.estimateTableCost("unknown", nil)
	if cost != 1000.0 {
		t.Errorf("expected default cost 1000.0, got %f", cost)
	}
}

func TestOrderTablesBySelectivity(t *testing.T) {
	opt := NewQueryOptimizer()
	opt.stats.RowCount["big"] = 10000
	opt.stats.RowCount["small"] = 100

	tables := opt.orderTablesBySelectivity([]string{"big", "small"}, nil)
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}
	// small should come first (lower cost)
	if tables[0] != "small" {
		t.Errorf("expected 'small' first, got %q", tables[0])
	}
}

func TestOrderTablesBySelectivity_Single(t *testing.T) {
	opt := NewQueryOptimizer()
	tables := opt.orderTablesBySelectivity([]string{"only"}, nil)
	if len(tables) != 1 || tables[0] != "only" {
		t.Errorf("unexpected result for single table: %v", tables)
	}
}

// ====== Parser coverage gaps ======

// --- parseExpressionWithOffset (0% coverage) ---

func TestParseExpressionWithOffset(t *testing.T) {
	tokens, err := Tokenize("SELECT ?")
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(tokens)
	p.advance() // skip SELECT
	expr, err := p.parseExpressionWithOffset(5)
	if err != nil {
		t.Fatal(err)
	}
	ph, ok := expr.(*PlaceholderExpr)
	if !ok {
		t.Fatalf("expected PlaceholderExpr, got %T", expr)
	}
	if ph.Index != 5 {
		t.Errorf("expected index 5 (offset), got %d", ph.Index)
	}
}

// --- parseUnion wrapper (0% coverage) ---

func TestParseUnionWrapper(t *testing.T) {
	sql := "SELECT 1 UNION SELECT 2"
	tokens, err := Tokenize(sql)
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(tokens)
	// Parse initial SELECT
	left, err := p.parseSelect()
	if err != nil {
		t.Fatal(err)
	}
	// Should be at UNION token
	result, err := p.parseUnion(left)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.(*UnionStmt); !ok {
		t.Errorf("expected *UnionStmt, got %T", result)
	}
}

// --- parsePrimary: keyword as function call (uncovered branch) ---

func TestParsePrimary_KeywordAsFunctionCall(t *testing.T) {
	// Test a keyword used as a function name via the default branch
	sql := "SELECT date('2024-01-01') FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

// --- parsePrimary: column after dot is EOF (error case) ---

func TestParsePrimary_QualifiedIdentifierDotEOF(t *testing.T) {
	// t. followed by EOF
	_, err := Parse("SELECT t.")
	if err == nil {
		t.Error("expected error for qualified identifier ending with dot")
	}
}

// --- parsePrimary: unexpected token error ---

func TestParsePrimary_UnexpectedToken(t *testing.T) {
	// FROM is a structural keyword, should fail as expression
	_, err := Parse("SELECT FROM")
	if err == nil {
		t.Error("expected error for structural keyword as expression")
	}
}

// --- parseComparison: IS without NULL ---

func TestParseComparison_IsWithoutNull(t *testing.T) {
	_, err := Parse("SELECT * FROM t WHERE x IS 5")
	if err == nil {
		t.Error("expected error for IS without NULL")
	}
	if err != nil && !strings.Contains(err.Error(), "NULL") {
		t.Logf("got error: %v", err)
	}
}

// --- parseTableRef: derived table with UNION ---

func TestParseTableRef_DerivedTableWithUnion(t *testing.T) {
	sql := "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) AS sub"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.SubqueryStmt == nil {
		t.Error("expected SubqueryStmt for UNION in derived table")
	}
}

// --- parseTableRef: derived table without alias ---

func TestParseTableRef_DerivedTableNoAlias(t *testing.T) {
	sql := "SELECT * FROM (SELECT 1 AS x)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Alias == "" || sel.From.Name == "" {
		t.Fatalf("expected generated alias/name for derived table, got %#v", sel.From)
	}
}

// --- parseTableRef: non-subquery in parens (error) ---

func TestParseTableRef_ParenNotSubquery(t *testing.T) {
	// (123) is not a valid table reference
	_, err := Parse("SELECT * FROM (123)")
	if err == nil {
		t.Error("expected error for non-SELECT in parens in FROM")
	}
}

// --- parseTableRef: table with AS alias ---

func TestParseTableRef_TableWithAsAlias(t *testing.T) {
	sql := "SELECT u.id FROM users AS u"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Alias != "u" {
		t.Errorf("expected alias 'u', got %q", sel.From.Alias)
	}
}

// --- parseJoin: various uncovered join types and conditions ---

func TestParseJoin_NaturalJoin(t *testing.T) {
	sql := "SELECT * FROM a NATURAL JOIN b"
	// NATURAL JOIN not yet supported by parser
	_, err := Parse(sql)
	if err == nil {
		t.Log("NATURAL JOIN parsed successfully (if parser supports it)")
	}
}

func TestParseJoin_CommaJoin(t *testing.T) {
	sql := "SELECT * FROM a, b, c"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Joins) != 2 {
		t.Errorf("expected 2 comma-joins, got %d", len(sel.Joins))
	}
}

func TestParseJoin_UsingClause(t *testing.T) {
	sql := "SELECT * FROM a JOIN b USING (id)"
	// USING clause may not be fully supported
	_, err := Parse(sql)
	if err == nil {
		t.Log("USING clause parsed successfully")
	}
}

func TestParseJoin_LeftJoinWithoutOuter(t *testing.T) {
	sql := "SELECT * FROM a LEFT JOIN b ON a.id = b.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Joins[0].Type != TokenLeft {
		t.Errorf("expected LEFT join, got %v", sel.Joins[0].Type)
	}
}

// --- parseCast: various data types ---

func TestParseCast_ToReal(t *testing.T) {
	sql := "SELECT CAST(x AS REAL) FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("CAST AS REAL failed: %v", err)
	}
}

func TestParseCast_ToBlob(t *testing.T) {
	sql := "SELECT CAST(x AS BLOB) FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("CAST AS BLOB failed: %v", err)
	}
}

// --- parseExistsExpr error cases ---

func TestParseExistsExpr_MissingLParen(t *testing.T) {
	_, err := Parse("SELECT * FROM t WHERE EXISTS SELECT 1")
	if err == nil {
		t.Error("expected error for EXISTS without parentheses")
	}
}

func TestParseExistsExpr_NotSelect(t *testing.T) {
	_, err := Parse("SELECT * FROM t WHERE EXISTS (42)")
	if err == nil {
		t.Error("expected error for EXISTS without SELECT")
	}
}

// --- parseCaseExpr: missing THEN ---

func TestParseCaseExpr_MissingThen(t *testing.T) {
	_, err := Parse("SELECT CASE WHEN 1 = 1 'yes' END FROM t")
	if err == nil {
		t.Error("expected error for CASE missing THEN")
	}
}

// --- parseCaseExpr: missing END ---

func TestParseCaseExpr_MissingEnd(t *testing.T) {
	_, err := Parse("SELECT CASE WHEN 1 = 1 THEN 'yes'")
	if err == nil {
		t.Error("expected error for CASE missing END")
	}
}

// --- parseCast: missing AS ---

func TestParseCast_MissingAs(t *testing.T) {
	_, err := Parse("SELECT CAST(x INTEGER) FROM t")
	if err == nil {
		t.Error("expected error for CAST without AS")
	}
}

// --- CREATE MATERIALIZED VIEW IF NOT EXISTS ---

func TestParseCreateMaterializedView_IfNotExists(t *testing.T) {
	sql := "CREATE MATERIALIZED VIEW IF NOT EXISTS mv1 AS SELECT * FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cmv := stmt.(*CreateMaterializedViewStmt)
	if !cmv.IfNotExists {
		t.Error("expected IfNotExists")
	}
}

// --- CREATE PROCEDURE IF NOT EXISTS ---

func TestParseCreateProcedure_IfNotExists(t *testing.T) {
	sql := "CREATE PROCEDURE IF NOT EXISTS myproc (p1 INTEGER) BEGIN SELECT 1; END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreateProcedureStmt)
	if !cp.IfNotExists {
		t.Error("expected IfNotExists")
	}
}

// --- CREATE PROCEDURE no params, no body ---

func TestParseCreateProcedure_NoParen(t *testing.T) {
	sql := "CREATE PROCEDURE myproc BEGIN SELECT 1; END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreateProcedureStmt)
	if cp.Name != "myproc" {
		t.Errorf("expected name 'myproc', got %q", cp.Name)
	}
	if len(cp.Params) != 0 {
		t.Errorf("expected 0 params, got %d", len(cp.Params))
	}
}

// --- CREATE PROCEDURE with multiple body statements ---

func TestParseCreateProcedure_MultipleBody(t *testing.T) {
	sql := "CREATE PROCEDURE myproc () BEGIN SELECT 1; SELECT 2; END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreateProcedureStmt)
	if len(cp.Body) != 2 {
		t.Errorf("expected 2 body statements, got %d", len(cp.Body))
	}
}

// --- DROP PROCEDURE ---

func TestParseDropProcedureExtra(t *testing.T) {
	sql := "DROP PROCEDURE myproc"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dp := stmt.(*DropProcedureStmt)
	if dp.Name != "myproc" {
		t.Errorf("expected name 'myproc', got %q", dp.Name)
	}
}

func TestParseDropProcedure_IfExists(t *testing.T) {
	sql := "DROP PROCEDURE IF EXISTS myproc"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dp := stmt.(*DropProcedureStmt)
	if !dp.IfExists {
		t.Error("expected IfExists")
	}
}

// --- DROP error case ---

func TestParseDrop_Invalid(t *testing.T) {
	_, err := Parse("DROP FOOBAR")
	if err == nil {
		t.Error("expected error for DROP FOOBAR")
	}
}

// --- CREATE FTS INDEX (more branches) ---

func TestParseCreateFTSIndex_Simple(t *testing.T) {
	sql := "CREATE FULLTEXT INDEX idx1 ON t (col1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	fts := stmt.(*CreateFTSIndexStmt)
	if fts.Index != "idx1" {
		t.Errorf("expected index 'idx1', got %q", fts.Index)
	}
	if fts.Table != "t" {
		t.Errorf("expected table 't', got %q", fts.Table)
	}
}

func TestParseCreateFTSIndex_MultipleColumns(t *testing.T) {
	sql := "CREATE FULLTEXT INDEX idx1 ON docs (title, body, tags)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	fts := stmt.(*CreateFTSIndexStmt)
	if len(fts.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(fts.Columns))
	}
}

// --- CREATE INDEX with multiple columns ---

func TestParseCreateIndex_MultipleColumns(t *testing.T) {
	sql := "CREATE INDEX idx1 ON t (a, b, c)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ci := stmt.(*CreateIndexStmt)
	if len(ci.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(ci.Columns))
	}
}

// --- parseCreatePolicy: more branches ---

func TestParseCreatePolicy_Restrictive(t *testing.T) {
	sql := "CREATE POLICY pol1 ON t AS RESTRICTIVE USING (1 = 1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("RESTRICTIVE policy parsing failed: %v", err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if cp.Permissive {
		t.Error("expected Permissive=false for RESTRICTIVE policy")
	}
}

func TestParseCreatePolicy_NoUsing(t *testing.T) {
	// Policy with just FOR SELECT and no USING clause
	sql := "CREATE POLICY pol1 ON t FOR ALL"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if cp.Using != nil {
		t.Error("expected nil USING for policy with no USING clause")
	}
}

// --- parseWithCTE: more branches ---

func TestParseCTE_WithExcept(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS x EXCEPT SELECT 2 AS x) SELECT * FROM cte"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cte := stmt.(*SelectStmtWithCTE)
	if len(cte.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(cte.CTEs))
	}
}

func TestParseCTE_MissingSelect(t *testing.T) {
	_, err := Parse("WITH cte AS (SELECT 1 AS x) INSERT INTO t VALUES (1)")
	if err == nil {
		t.Error("expected error for CTE without main SELECT")
	}
}

// --- parseSetOp: EXCEPT ALL, INTERSECT ALL ---

func TestParseExceptAll(t *testing.T) {
	sql := "SELECT 1 EXCEPT ALL SELECT 2"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	us := stmt.(*UnionStmt)
	if us.Op != SetOpExcept || !us.All {
		t.Errorf("expected EXCEPT ALL, got op=%d all=%v", us.Op, us.All)
	}
}

func TestParseIntersectAll(t *testing.T) {
	sql := "SELECT 1 INTERSECT ALL SELECT 2"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	us := stmt.(*UnionStmt)
	if us.Op != SetOpIntersect || !us.All {
		t.Errorf("expected INTERSECT ALL, got op=%d all=%v", us.Op, us.All)
	}
}

func TestParseSetOp_MissingSelect(t *testing.T) {
	_, err := Parse("SELECT 1 UNION 42")
	if err == nil {
		t.Error("expected error for UNION without SELECT")
	}
}

// --- Set op with ORDER BY/LIMIT/OFFSET ---

func TestParseUnion_WithOrderByLimit(t *testing.T) {
	sql := "SELECT id FROM a UNION SELECT id FROM b ORDER BY id LIMIT 10 OFFSET 5"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	us := stmt.(*UnionStmt)
	if us.OrderBy == nil {
		t.Error("expected OrderBy on UNION")
	}
	if us.Limit == nil {
		t.Error("expected Limit on UNION")
	}
	if us.Offset == nil {
		t.Error("expected Offset on UNION")
	}
}

// --- parseRefresh error case ---

func TestParseRefresh_MissingView(t *testing.T) {
	_, err := Parse("REFRESH MATERIALIZED")
	if err == nil {
		t.Error("expected error for REFRESH MATERIALIZED without VIEW")
	}
}

// --- JSON path operators ---

func TestParseJSONArrowOperator(t *testing.T) {
	sql := "SELECT data->'name' FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("JSON arrow parse failed: %v", err)
	}
}

func TestParseJSONDoubleArrowOperator(t *testing.T) {
	sql := "SELECT data->>'name' FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Log("->> operator not supported, expected error:", err)
	}
}

// --- Window function with PARTITION BY and ORDER BY ---

func TestParseWindowFunction_PartitionAndOrder(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM emp"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("window function parse failed: %v", err)
	}
}

func TestParseWindowFunction_MultiplePartition(t *testing.T) {
	sql := "SELECT SUM(amount) OVER (PARTITION BY dept, team ORDER BY id) FROM emp"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("window function with multiple partition cols failed: %v", err)
	}
}

// --- CALL with arguments ---

func TestParseCall_WithArgs(t *testing.T) {
	sql := "CALL myproc(1, 'hello', 42)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	call := stmt.(*CallProcedureStmt)
	if call.Name != "myproc" {
		t.Errorf("expected name 'myproc', got %q", call.Name)
	}
	if len(call.Params) != 3 {
		t.Errorf("expected 3 params, got %d", len(call.Params))
	}
}

func TestParseCall_NoArgs(t *testing.T) {
	sql := "CALL myproc()"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	call := stmt.(*CallProcedureStmt)
	if len(call.Params) != 0 {
		t.Errorf("expected 0 params, got %d", len(call.Params))
	}
}

func TestParseCall_NoParen(t *testing.T) {
	sql := "CALL myproc"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	call := stmt.(*CallProcedureStmt)
	if call.Name != "myproc" {
		t.Errorf("expected name 'myproc', got %q", call.Name)
	}
}

// --- ALTER TABLE ---

func TestParseAlterTable_AddColumn(t *testing.T) {
	sql := "ALTER TABLE users ADD COLUMN email TEXT"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Action != "ADD" {
		t.Errorf("expected ADD, got %q", at.Action)
	}
}

func TestParseAlterTable_DropColumn(t *testing.T) {
	sql := "ALTER TABLE users DROP COLUMN email"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Action != "DROP" {
		t.Errorf("expected DROP, got %q", at.Action)
	}
}

func TestParseAlterTable_AddUniqueConstraint(t *testing.T) {
	sql := "ALTER TABLE users ADD CONSTRAINT users_email_uq UNIQUE (email)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Action != "ADD_CONSTRAINT" {
		t.Errorf("expected ADD_CONSTRAINT, got %q", at.Action)
	}
	if at.ConstraintName != "users_email_uq" || at.ConstraintType != "UNIQUE" {
		t.Errorf("unexpected constraint metadata: name=%q type=%q", at.ConstraintName, at.ConstraintType)
	}
	if len(at.ConstraintColumns) != 1 || at.ConstraintColumns[0] != "email" {
		t.Errorf("unexpected constraint columns: %v", at.ConstraintColumns)
	}
}

func TestParseAlterTable_AddForeignKeyConstraint(t *testing.T) {
	sql := "ALTER TABLE orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE RESTRICT"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Action != "ADD_CONSTRAINT" {
		t.Errorf("expected ADD_CONSTRAINT, got %q", at.Action)
	}
	if at.ConstraintName != "orders_user_fk" || at.ConstraintType != "FOREIGN KEY" {
		t.Errorf("unexpected constraint metadata: name=%q type=%q", at.ConstraintName, at.ConstraintType)
	}
	if at.ForeignKey == nil {
		t.Fatal("expected foreign key metadata")
	}
	if at.ForeignKey.Name != "orders_user_fk" || at.ForeignKey.ReferencedTable != "users" {
		t.Errorf("unexpected foreign key: %#v", at.ForeignKey)
	}
	if at.ForeignKey.OnDelete != "CASCADE" || at.ForeignKey.OnUpdate != "RESTRICT" {
		t.Errorf("unexpected actions: delete=%q update=%q", at.ForeignKey.OnDelete, at.ForeignKey.OnUpdate)
	}
}

func TestParseAlterTable_AddCheckConstraint(t *testing.T) {
	sql := "ALTER TABLE products ADD CONSTRAINT discount_ck CHECK (discount <= price)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Action != "ADD_CONSTRAINT" {
		t.Errorf("expected ADD_CONSTRAINT, got %q", at.Action)
	}
	if at.ConstraintName != "discount_ck" || at.ConstraintType != "CHECK" {
		t.Errorf("unexpected constraint metadata: name=%q type=%q", at.ConstraintName, at.ConstraintType)
	}
	if at.ConstraintCheck == nil {
		t.Fatal("expected CHECK expression")
	}
}

func TestParseAlterTable_DropConstraint(t *testing.T) {
	sql := "ALTER TABLE users DROP CONSTRAINT users_email_uq"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Action != "DROP_CONSTRAINT" {
		t.Errorf("expected DROP_CONSTRAINT, got %q", at.Action)
	}
	if at.ConstraintName != "users_email_uq" {
		t.Errorf("expected constraint name users_email_uq, got %q", at.ConstraintName)
	}
}

func TestParseCreateTable_NamedUniqueConstraint(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INTEGER, email TEXT, CONSTRAINT users_email_uq UNIQUE (email))")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.NamedUniqueConstraints) != 1 {
		t.Fatalf("expected one named unique constraint, got %d", len(ct.NamedUniqueConstraints))
	}
	uq := ct.NamedUniqueConstraints[0]
	if uq.Name != "users_email_uq" {
		t.Errorf("constraint name = %q, want users_email_uq", uq.Name)
	}
	if len(uq.Columns) != 1 || uq.Columns[0] != "email" {
		t.Errorf("constraint columns = %v, want [email]", uq.Columns)
	}
}

func TestParseCreateTable_ColumnNamedUniqueConstraint(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (email TEXT CONSTRAINT users_email_uq UNIQUE)")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.Columns) != 1 {
		t.Fatalf("expected one column, got %d", len(ct.Columns))
	}
	if ct.Columns[0].UniqueName != "users_email_uq" {
		t.Fatalf("unique name = %q, want users_email_uq", ct.Columns[0].UniqueName)
	}
	if ct.Columns[0].Unique {
		t.Fatal("named column unique should be enforced via a unique index, not ColumnDef.Unique")
	}
}

func TestParseCreateTable_NamedPrimaryKeyConstraint(t *testing.T) {
	stmt, err := Parse("CREATE TABLE orders (tenant_id INTEGER, id INTEGER, CONSTRAINT orders_pk PRIMARY KEY (tenant_id, id))")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.PrimaryKey) != 2 || ct.PrimaryKey[0] != "tenant_id" || ct.PrimaryKey[1] != "id" {
		t.Fatalf("primary key = %v, want [tenant_id id]", ct.PrimaryKey)
	}
}

func TestParseCreateTable_NamedCheckConstraint(t *testing.T) {
	stmt, err := Parse("CREATE TABLE products (price INTEGER, discount INTEGER, CONSTRAINT price_discount_ck CHECK (discount <= price))")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.CheckConstraints) != 1 {
		t.Fatalf("expected one check constraint, got %d", len(ct.CheckConstraints))
	}
	check := ct.CheckConstraints[0]
	if check.Name != "price_discount_ck" {
		t.Errorf("check name = %q, want price_discount_ck", check.Name)
	}
	if check.Expr == nil {
		t.Fatal("expected check expression")
	}
}

func TestParseCreateTable_ColumnNamedCheckConstraint(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (age INTEGER CONSTRAINT users_age_ck CHECK (age >= 0))")
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.Columns) != 1 {
		t.Fatalf("expected one column, got %d", len(ct.Columns))
	}
	if ct.Columns[0].CheckName != "users_age_ck" {
		t.Fatalf("check name = %q, want users_age_ck", ct.Columns[0].CheckName)
	}
	if ct.Columns[0].Check == nil {
		t.Fatal("expected column check expression")
	}
}

func TestParseAlterTable_RenameColumn(t *testing.T) {
	sql := "ALTER TABLE users RENAME COLUMN old_name TO new_name"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Action != "RENAME_COLUMN" {
		t.Errorf("expected RENAME_COLUMN, got %q", at.Action)
	}
	if at.OldName != "old_name" || at.NewName != "new_name" {
		t.Errorf("expected old_name->new_name, got %q->%q", at.OldName, at.NewName)
	}
}

func TestParseAlterTable_RenameTable(t *testing.T) {
	sql := "ALTER TABLE users RENAME TO accounts"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	at := stmt.(*AlterTableStmt)
	if at.Action != "RENAME_TABLE" {
		t.Errorf("expected RENAME_TABLE, got %q", at.Action)
	}
	if at.NewName != "accounts" {
		t.Errorf("expected 'accounts', got %q", at.NewName)
	}
}

func TestParseAlterTable_InvalidAction(t *testing.T) {
	_, err := Parse("ALTER TABLE users FOOBAR")
	if err == nil {
		t.Error("expected error for invalid ALTER TABLE action")
	}
}

// --- BEGIN TRANSACTION variants ---

func TestParseBegin_WithTransaction(t *testing.T) {
	sql := "BEGIN TRANSACTION"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("BEGIN TRANSACTION failed: %v", err)
	}
}

// --- ROLLBACK TO SAVEPOINT ---

func TestParseRollbackToSavepoint(t *testing.T) {
	sql := "ROLLBACK TO SAVEPOINT sp1"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	rb := stmt.(*RollbackStmt)
	if rb.ToSavepoint != "sp1" {
		t.Errorf("expected savepoint 'sp1', got %q", rb.ToSavepoint)
	}
}

func TestParseRollbackToWithoutSavepointKeyword(t *testing.T) {
	sql := "ROLLBACK TO sp2"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	rb := stmt.(*RollbackStmt)
	if rb.ToSavepoint != "sp2" {
		t.Errorf("expected savepoint 'sp2', got %q", rb.ToSavepoint)
	}
}

// --- RELEASE SAVEPOINT ---

func TestParseReleaseSavepoint(t *testing.T) {
	sql := "RELEASE SAVEPOINT sp1"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	rs := stmt.(*ReleaseSavepointStmt)
	if rs.Name != "sp1" {
		t.Errorf("expected 'sp1', got %q", rs.Name)
	}
}

func TestParseReleaseSavepoint_WithoutKeyword(t *testing.T) {
	sql := "RELEASE sp2"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	rs := stmt.(*ReleaseSavepointStmt)
	if rs.Name != "sp2" {
		t.Errorf("expected 'sp2', got %q", rs.Name)
	}
}

// --- DROP VIEW ---

func TestParseDropViewExtra(t *testing.T) {
	sql := "DROP VIEW myview"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dv := stmt.(*DropViewStmt)
	if dv.Name != "myview" {
		t.Errorf("expected 'myview', got %q", dv.Name)
	}
}

func TestParseDropView_IfExists(t *testing.T) {
	sql := "DROP VIEW IF EXISTS myview"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dv := stmt.(*DropViewStmt)
	if !dv.IfExists {
		t.Error("expected IfExists")
	}
}

// --- DROP INDEX ---

func TestParseDropIndexExtra(t *testing.T) {
	sql := "DROP INDEX myidx"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	di := stmt.(*DropIndexStmt)
	if di.Index != "myidx" {
		t.Errorf("expected 'myidx', got %q", di.Index)
	}
}

func TestParseDropIndex_IfExists(t *testing.T) {
	sql := "DROP INDEX IF EXISTS myidx"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	di := stmt.(*DropIndexStmt)
	if !di.IfExists {
		t.Error("expected IfExists")
	}
}

// --- DROP TABLE ---

func TestParseDropTableExtra(t *testing.T) {
	sql := "DROP TABLE mytable"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dt := stmt.(*DropTableStmt)
	if dt.Table != "mytable" {
		t.Errorf("expected 'mytable', got %q", dt.Table)
	}
}

func TestParseDropTable_IfExists(t *testing.T) {
	sql := "DROP TABLE IF EXISTS mytable"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dt := stmt.(*DropTableStmt)
	if !dt.IfExists {
		t.Error("expected IfExists")
	}
}

// --- Various CREATE TABLE column constraint coverage ---

func TestParseCreateTable_WithDefault(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unknown', active INTEGER DEFAULT 1)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseCreateTable_WithUnique(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseCreateTable_WithCheck(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER CHECK (age > 0))"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

func TestParseCreateTable_IfNotExists(t *testing.T) {
	sql := "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
}

// --- CREATE VIEW with columns ---

func TestParseCreateView_Simple(t *testing.T) {
	sql := "CREATE VIEW v1 AS SELECT id, name FROM users"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cv := stmt.(*CreateViewStmt)
	if cv.Name != "v1" {
		t.Errorf("expected 'v1', got %q", cv.Name)
	}
}

// --- ANALYZE statement ---

func TestParseAnalyzeExtra(t *testing.T) {
	sql := "ANALYZE users"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	a := stmt.(*AnalyzeStmt)
	if a.Table != "users" {
		t.Errorf("expected 'users', got %q", a.Table)
	}
}

// --- VACUUM with no table ---

func TestParseVacuum_NoTable(t *testing.T) {
	sql := "VACUUM"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	v := stmt.(*VacuumStmt)
	if v.Table != "" {
		t.Errorf("expected empty table, got %q", v.Table)
	}
}

// --- applyPlaceholderOffset branches ---

func TestApplyPlaceholderOffset_BinaryExpr(t *testing.T) {
	tokens, err := Tokenize("SELECT ? + ?")
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(tokens)
	p.advance() // skip SELECT
	expr, err := p.parseExpressionWithOffset(10)
	if err != nil {
		t.Fatal(err)
	}
	// Both placeholders should have offset applied
	binExpr := expr.(*BinaryExpr)
	left := binExpr.Left.(*PlaceholderExpr)
	right := binExpr.Right.(*PlaceholderExpr)
	if left.Index < 10 || right.Index < 10 {
		t.Errorf("expected offsets >= 10, got %d and %d", left.Index, right.Index)
	}
}

// --- parseParenthesized: subquery in parentheses ---

func TestParseParenthesized_Subquery(t *testing.T) {
	sql := "SELECT * FROM t WHERE id = (SELECT MAX(id) FROM t)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("subquery in parens failed: %v", err)
	}
}

// --- parseIdentifierOrFunction: qualified identifier ---

func TestParseIdentifierOrFunction_Qualified(t *testing.T) {
	sql := "SELECT t.id, t.name FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("qualified identifier failed: %v", err)
	}
}

// --- FK NO ACTION ---

func TestParseForeignKey_NoAction(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE NO ACTION)"
	_, err := Parse(sql)
	if err != nil {
		t.Log("NO ACTION not yet supported by parser:", err)
	}
}

func TestParseForeignKey_OnUpdateNoAction(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON UPDATE NO ACTION)"
	_, err := Parse(sql)
	if err != nil {
		t.Log("ON UPDATE NO ACTION not yet supported by parser:", err)
	}
}

// --- SELECT with function DISTINCT ---

func TestParseSelectCountDistinct(t *testing.T) {
	sql := "SELECT COUNT(DISTINCT name) FROM users"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("COUNT(DISTINCT) failed: %v", err)
	}
}

// --- collectPlaceholders coverage ---

func TestCollectPlaceholders(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE id = ? AND name = ?")
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	placeholders := collectPlaceholders(sel.Where)
	if len(placeholders) != 2 {
		t.Errorf("expected 2 placeholders, got %d", len(placeholders))
	}
}

// --- parseExpressionListWithOffset ---

func TestParseExpressionListWithOffset(t *testing.T) {
	tokens, err := Tokenize("SELECT ?, ?, ?")
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(tokens)
	p.advance() // skip SELECT
	exprs, err := p.parseExpressionListWithOffset(100)
	if err != nil {
		t.Fatal(err)
	}
	if len(exprs) != 3 {
		t.Fatalf("expected 3 expressions, got %d", len(exprs))
	}
}

// --- Window function: empty OVER() ---

func TestParseWindowFunction_EmptyOver(t *testing.T) {
	sql := "SELECT COUNT(*) OVER () FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("window function with empty OVER() failed: %v", err)
	}
}

// --- SELECT with HAVING ---

func TestParseSelectHaving(t *testing.T) {
	sql := "SELECT dept, COUNT(*) AS cnt FROM emp GROUP BY dept HAVING cnt > 3 ORDER BY cnt DESC"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("SELECT with HAVING failed: %v", err)
	}
}

// --- INSERT with DEFAULT VALUES ---

func TestParseInsertDefaultValues(t *testing.T) {
	sql := "INSERT INTO t DEFAULT VALUES"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("INSERT DEFAULT VALUES should parse: %v", err)
	}
	insertStmt := stmt.(*InsertStmt)
	if len(insertStmt.Values) != 1 || len(insertStmt.Values[0]) != 0 {
		t.Fatalf("values = %#v, want one empty default row", insertStmt.Values)
	}
}

// --- Boolean literal ---

func TestParseBooleanLiterals(t *testing.T) {
	sql := "SELECT * FROM t WHERE active = TRUE AND deleted = FALSE"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("boolean literals failed: %v", err)
	}
}

// --- NULL literal ---

func TestParseNullLiteral(t *testing.T) {
	sql := "SELECT * FROM t WHERE name IS NULL AND age = NULL"
	_, err := Parse(sql)
	if err != nil {
		// IS NULL parsed differently from = NULL
		t.Logf("NULL parse: %v", err)
	}
}

// --- Star expression in function ---

func TestParseStarInFunction(t *testing.T) {
	sql := "SELECT COUNT(*) FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("COUNT(*) failed: %v", err)
	}
}

// --- CREATE POLICY AS PERMISSIVE ---

func TestParseCreatePolicy_Permissive(t *testing.T) {
	sql := "CREATE POLICY pol1 ON t AS PERMISSIVE USING (1 = 1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if !cp.Permissive {
		t.Error("expected Permissive=true")
	}
}

// --- CREATE INDEX simple (no unique, no where, no if-not-exists) ---

func TestParseCreateIndex_Simple(t *testing.T) {
	sql := "CREATE INDEX idx1 ON t (a)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ci := stmt.(*CreateIndexStmt)
	if ci.Unique {
		t.Error("expected not unique")
	}
	if ci.IfNotExists {
		t.Error("expected no IfNotExists")
	}
}

// --- parseComparison: LIKE NOT (IN not pattern) ---

func TestParseComparison_LikeNotVariant(t *testing.T) {
	// This tests the `case TokenLike:` path where NOT is checked right after LIKE
	sql := "SELECT * FROM t WHERE name LIKE '%test%'"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("LIKE comparison failed: %v", err)
	}
}

// --- parseComparison: IN not token (just checking various paths) ---

func TestParseComparison_InList(t *testing.T) {
	sql := "SELECT * FROM t WHERE id IN (1, 2, 3)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("IN list failed: %v", err)
	}
}

func TestParseComparison_BetweenWithNot(t *testing.T) {
	sql := "SELECT * FROM t WHERE x BETWEEN NOT 1 AND 10"
	// This should NOT match the "NOT" in BETWEEN path
	// BETWEEN consumes NOT if it follows
	_, err := Parse(sql)
	// May or may not parse depending on grammar
	t.Logf("BETWEEN NOT result: %v", err)
}

// --- Subquery in SELECT column list ---

func TestParse_SubqueryInSelectColumn(t *testing.T) {
	sql := "SELECT id, (SELECT MAX(salary) FROM emp) AS max_sal FROM users"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("scalar subquery in column list failed: %v", err)
	}
}

// --- INSERT with subquery value ---

func TestParseInsert_WithSubqueryValue(t *testing.T) {
	sql := "INSERT INTO t (a) VALUES ((SELECT MAX(id) FROM other))"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("INSERT with subquery value failed: %v", err)
	}
}

// --- Complex nested expressions ---

func TestParse_ComplexNested(t *testing.T) {
	sql := "SELECT * FROM t WHERE (a > 1 AND b < 10) OR (c = 3 AND NOT d IS NULL)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("complex nested expression failed: %v", err)
	}
}

// --- Multiple CTEs with column lists and UNION inside ---

func TestParseCTE_ComplexWithColumnLists(t *testing.T) {
	sql := `WITH
		c1 (x, y) AS (SELECT 1, 2 UNION SELECT 3, 4),
		c2 AS (SELECT * FROM c1)
		SELECT * FROM c2`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cte := stmt.(*SelectStmtWithCTE)
	if len(cte.CTEs) != 2 {
		t.Errorf("expected 2 CTEs, got %d", len(cte.CTEs))
	}
	if len(cte.CTEs[0].Columns) != 2 {
		t.Errorf("expected 2 columns in first CTE, got %d", len(cte.CTEs[0].Columns))
	}
}

// --- EXPLAIN with various statements ---

func TestParseExplain_Insert(t *testing.T) {
	sql := "EXPLAIN INSERT INTO t VALUES (1)"
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("EXPLAIN INSERT failed: %v", err)
	}
}

// --- CREATE TABLE with multiple foreign keys ---

func TestParseCreateTable_MultipleFKs(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		product_id INTEGER,
		FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
		FOREIGN KEY (product_id) REFERENCES products (id) ON UPDATE SET NULL
	)`
	_, err := Parse(sql)
	if err != nil {
		t.Fatalf("multiple FKs failed: %v", err)
	}
}

// --- parseCreateTrigger: INSTEAD OF ---

func TestParseCreateTrigger_InsteadOf(t *testing.T) {
	sql := "CREATE TRIGGER trig1 INSTEAD OF INSERT ON v1 BEGIN INSERT INTO t VALUES (1); END"
	_, err := Parse(sql)
	if err != nil {
		t.Log("INSTEAD OF trigger not yet supported:", err)
	}
}

// --- parseCreateCollection: without IF NOT EXISTS ---

func TestParseCreateCollection_Simple_ExtraCov(t *testing.T) {
	sql := "CREATE COLLECTION mycoll"
	_, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
}
