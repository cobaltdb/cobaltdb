package query

import (
	"strings"
	"testing"
)

// ---- JOIN types ----

func TestParseFullOuterJoinDeep(t *testing.T) {
	sql := "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(sel.Joins))
	}
	if sel.Joins[0].Type != TokenFull {
		t.Errorf("expected FULL join, got %v", sel.Joins[0].Type)
	}
}

func TestParseRightOuterJoinDeep(t *testing.T) {
	sql := "SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Joins[0].Type != TokenRight {
		t.Errorf("expected RIGHT join, got %v", sel.Joins[0].Type)
	}
}

func TestParseBareOuterJoinDeep(t *testing.T) {
	sql := "SELECT * FROM a OUTER JOIN b ON a.id = b.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Joins[0].Type != TokenFull {
		t.Errorf("expected FULL (from bare OUTER), got %v", sel.Joins[0].Type)
	}
}

func TestParseCrossJoinWithOnConditionDeep(t *testing.T) {
	sql := "SELECT * FROM a CROSS JOIN b ON a.id = b.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Joins[0].Type != TokenCross {
		t.Errorf("expected CROSS join")
	}
	if sel.Joins[0].Condition == nil {
		t.Error("expected ON condition for CROSS JOIN")
	}
}

func TestParseCrossJoinWithoutOnDeep(t *testing.T) {
	sql := "SELECT * FROM a CROSS JOIN b"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.Joins[0].Condition != nil {
		t.Error("expected no ON condition for CROSS JOIN")
	}
}

// ---- NOT IN / NOT LIKE / NOT BETWEEN / LIKE ESCAPE ----

func TestParseNotInExprListDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE x NOT IN (1, 2, 3)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	inExpr, ok := sel.Where.(*InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", sel.Where)
	}
	if !inExpr.Not {
		t.Error("expected NOT flag")
	}
	if len(inExpr.List) != 3 {
		t.Errorf("expected 3 items, got %d", len(inExpr.List))
	}
}

func TestParseNotInSubqueryDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE x NOT IN (SELECT id FROM other)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	inExpr := sel.Where.(*InExpr)
	if !inExpr.Not {
		t.Error("expected NOT flag")
	}
	if inExpr.Subquery == nil {
		t.Error("expected subquery")
	}
}

func TestParseInSubqueryDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE x IN (SELECT id FROM other)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	inExpr := sel.Where.(*InExpr)
	if inExpr.Subquery == nil {
		t.Error("expected subquery in IN clause")
	}
}

func TestParseNotLikeDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE name NOT LIKE '%foo%'"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	likeExpr, ok := sel.Where.(*LikeExpr)
	if !ok {
		t.Fatalf("expected LikeExpr, got %T", sel.Where)
	}
	if !likeExpr.Not {
		t.Error("expected NOT flag")
	}
}

func TestParseNotLikeWithEscapeDeep(t *testing.T) {
	sql := `SELECT * FROM t WHERE name NOT LIKE '%foo%' ESCAPE '#'`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	likeExpr := sel.Where.(*LikeExpr)
	if !likeExpr.Not {
		t.Error("expected NOT flag")
	}
	if likeExpr.Escape == nil {
		t.Error("expected ESCAPE expression")
	}
}

func TestParseLikeWithEscapeDeep(t *testing.T) {
	sql := `SELECT * FROM t WHERE name LIKE '%#_test%' ESCAPE '#'`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	likeExpr := sel.Where.(*LikeExpr)
	if likeExpr.Escape == nil {
		t.Error("expected ESCAPE expression")
	}
}

func TestParseNotBetweenDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE x NOT BETWEEN 10 AND 20"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	betExpr, ok := sel.Where.(*BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", sel.Where)
	}
	if !betExpr.Not {
		t.Error("expected NOT flag")
	}
}

// ---- INSERT variants ----

func TestParseInsertOrReplaceDeep(t *testing.T) {
	sql := "INSERT OR REPLACE INTO t (a) VALUES (1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	if ins.ConflictAction != ConflictReplace {
		t.Errorf("expected ConflictReplace, got %v", ins.ConflictAction)
	}
}

func TestParseInsertOrIgnoreDeep(t *testing.T) {
	sql := "INSERT OR IGNORE INTO t (a) VALUES (1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	if ins.ConflictAction != ConflictIgnore {
		t.Errorf("expected ConflictIgnore, got %v", ins.ConflictAction)
	}
}

func TestParseInsertOrInvalidErrorDeep(t *testing.T) {
	sql := "INSERT OR FOOBAR INTO t VALUES (1)"
	_, err := Parse(sql)
	if err == nil {
		t.Fatal("expected error for INSERT OR FOOBAR")
	}
	if !strings.Contains(err.Error(), "REPLACE or IGNORE") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseInsertSelectDeep(t *testing.T) {
	sql := "INSERT INTO t (a, b) SELECT x, y FROM other"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	if ins.Select == nil {
		t.Error("expected SELECT clause")
	}
	if len(ins.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(ins.Columns))
	}
}

func TestParseInsertMultipleRowsDeep(t *testing.T) {
	sql := "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStmt)
	if len(ins.Values) != 3 {
		t.Errorf("expected 3 rows, got %d", len(ins.Values))
	}
}

// ---- UPDATE with FROM/JOIN ----

func TestParseUpdateWithFromDeep(t *testing.T) {
	sql := "UPDATE t SET val = 1 FROM source s WHERE t.id = s.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStmt)
	if upd.From == nil {
		t.Error("expected FROM clause")
	}
}

func TestParseUpdateWithFromAndJoinDeep(t *testing.T) {
	sql := "UPDATE t SET x = 1 FROM a INNER JOIN b ON a.id = b.id WHERE t.id = a.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStmt)
	if upd.From == nil {
		t.Error("expected FROM")
	}
	if len(upd.Joins) != 1 {
		t.Errorf("expected 1 join, got %d", len(upd.Joins))
	}
}

func TestParseUpdateWithFromCommaDeep(t *testing.T) {
	sql := "UPDATE t SET x = 1 FROM a, b WHERE t.id = a.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStmt)
	if len(upd.Joins) != 1 {
		t.Errorf("expected 1 cross join from comma, got %d", len(upd.Joins))
	}
}

func TestParseUpdateKeywordColumnInSetDeep(t *testing.T) {
	sql := "UPDATE t SET status = 'active' WHERE id = 1"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStmt)
	if upd.Set[0].Column != "status" {
		t.Errorf("expected 'status', got %s", upd.Set[0].Column)
	}
}

// ---- DELETE variants ----

func TestParseDeleteWithAliasDeep(t *testing.T) {
	sql := "DELETE FROM users u WHERE u.active = 0"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if del.Alias != "u" {
		t.Errorf("expected alias 'u', got '%s'", del.Alias)
	}
}

func TestParseDeleteWithAsAliasDeep(t *testing.T) {
	sql := "DELETE FROM users AS u WHERE u.active = 0"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if del.Alias != "u" {
		t.Errorf("expected alias 'u', got '%s'", del.Alias)
	}
}

func TestParseDeleteUsingDeep(t *testing.T) {
	sql := "DELETE FROM t USING other WHERE t.id = other.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if len(del.Using) != 1 {
		t.Errorf("expected 1 USING table, got %d", len(del.Using))
	}
}

func TestParseDeleteUsingMultipleTablesDeep(t *testing.T) {
	sql := "DELETE FROM t USING a, b WHERE t.id = a.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if len(del.Using) != 2 {
		t.Errorf("expected 2 USING tables, got %d", len(del.Using))
	}
}

func TestParseDeleteUsingWithJoinDeep(t *testing.T) {
	sql := "DELETE FROM t USING other INNER JOIN third ON other.id = third.id WHERE t.id = other.id"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if len(del.Using) != 2 {
		t.Errorf("expected 2 USING tables (including joined), got %d", len(del.Using))
	}
}

// ---- FOREIGN KEY actions ----

func TestParseForeignKeyOnDeleteCascadeDeep(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.ForeignKeys) != 1 {
		t.Fatalf("expected 1 FK, got %d", len(ct.ForeignKeys))
	}
	if ct.ForeignKeys[0].OnDelete != "CASCADE" {
		t.Errorf("expected ON DELETE CASCADE, got %s", ct.ForeignKeys[0].OnDelete)
	}
}

func TestParseForeignKeyOnUpdateSetNullDeep(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON UPDATE SET NULL)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if ct.ForeignKeys[0].OnUpdate != "SET NULL" {
		t.Errorf("expected ON UPDATE SET NULL, got %s", ct.ForeignKeys[0].OnUpdate)
	}
}

func TestParseForeignKeyOnDeleteRestrictDeep(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE RESTRICT)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if ct.ForeignKeys[0].OnDelete != "RESTRICT" {
		t.Errorf("expected ON DELETE RESTRICT, got %s", ct.ForeignKeys[0].OnDelete)
	}
}

func TestParseForeignKeyOnDeleteDefaultActionDeep(t *testing.T) {
	// When ON DELETE is followed by an unrecognized keyword, parser falls through to "NO ACTION"
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE SET NULL)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if ct.ForeignKeys[0].OnDelete != "SET NULL" {
		t.Errorf("expected SET NULL, got %s", ct.ForeignKeys[0].OnDelete)
	}
}

func TestParseForeignKeyOnUpdateCascadeDeep(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON UPDATE CASCADE)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if ct.ForeignKeys[0].OnUpdate != "CASCADE" {
		t.Errorf("expected ON UPDATE CASCADE, got %s", ct.ForeignKeys[0].OnUpdate)
	}
}

func TestParseForeignKeyOnUpdateRestrictDeep(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON UPDATE RESTRICT)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if ct.ForeignKeys[0].OnUpdate != "RESTRICT" {
		t.Errorf("expected RESTRICT, got %s", ct.ForeignKeys[0].OnUpdate)
	}
}

func TestParseForeignKeyBothActionsDeep(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE SET NULL)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if ct.ForeignKeys[0].OnDelete != "CASCADE" {
		t.Errorf("expected CASCADE, got %s", ct.ForeignKeys[0].OnDelete)
	}
	if ct.ForeignKeys[0].OnUpdate != "SET NULL" {
		t.Errorf("expected SET NULL, got %s", ct.ForeignKeys[0].OnUpdate)
	}
}

func TestParseForeignKeyMultiColumnDeep(t *testing.T) {
	sql := "CREATE TABLE t (a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES parent (x, y))"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	fk := ct.ForeignKeys[0]
	if len(fk.Columns) != 2 || len(fk.ReferencedColumns) != 2 {
		t.Errorf("expected 2 cols, got %d/%d", len(fk.Columns), len(fk.ReferencedColumns))
	}
}

// ---- CREATE COLLECTION ----

func TestParseCreateCollectionIfNotExistsDeep(t *testing.T) {
	sql := "CREATE COLLECTION IF NOT EXISTS mycoll"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cc := stmt.(*CreateCollectionStmt)
	if !cc.IfNotExists {
		t.Error("expected IfNotExists")
	}
	if cc.Name != "mycoll" {
		t.Errorf("expected 'mycoll', got %s", cc.Name)
	}
}

func TestParseCreateCollectionSimpleDeep(t *testing.T) {
	sql := "CREATE COLLECTION docs"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cc := stmt.(*CreateCollectionStmt)
	if cc.IfNotExists {
		t.Error("expected no IfNotExists")
	}
}

// ---- CREATE TRIGGER variants ----

func TestParseCreateTriggerIfNotExistsDeep(t *testing.T) {
	sql := "CREATE TRIGGER IF NOT EXISTS trig1 BEFORE INSERT ON t BEGIN INSERT INTO log VALUES (1); END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	tr := stmt.(*CreateTriggerStmt)
	if !tr.IfNotExists {
		t.Error("expected IfNotExists")
	}
	if tr.Time != "BEFORE" {
		t.Errorf("expected BEFORE, got %s", tr.Time)
	}
}

func TestParseCreateTriggerAfterUpdateDeep(t *testing.T) {
	sql := "CREATE TRIGGER trig1 AFTER UPDATE ON t BEGIN UPDATE log SET cnt = cnt + 1; END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	tr := stmt.(*CreateTriggerStmt)
	if tr.Time != "AFTER" {
		t.Errorf("expected AFTER, got %s", tr.Time)
	}
	if tr.Event != "UPDATE" {
		t.Errorf("expected UPDATE, got %s", tr.Event)
	}
}

func TestParseCreateTriggerBeforeDeleteDeep(t *testing.T) {
	sql := "CREATE TRIGGER trig1 BEFORE DELETE ON t BEGIN DELETE FROM log; END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	tr := stmt.(*CreateTriggerStmt)
	if tr.Event != "DELETE" {
		t.Errorf("expected DELETE, got %s", tr.Event)
	}
}

func TestParseCreateTriggerWithWhenDeep(t *testing.T) {
	sql := "CREATE TRIGGER trig1 BEFORE INSERT ON t WHEN NEW.val > 0 BEGIN INSERT INTO log VALUES (1); END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	tr := stmt.(*CreateTriggerStmt)
	if tr.Condition == nil {
		t.Error("expected WHEN condition")
	}
}

func TestParseCreateTriggerForEachRowDeep(t *testing.T) {
	sql := "CREATE TRIGGER trig1 BEFORE INSERT ON t FOR EACH ROW BEGIN INSERT INTO log VALUES (1); END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	tr := stmt.(*CreateTriggerStmt)
	if len(tr.Body) == 0 {
		t.Error("expected body statements")
	}
}

func TestParseCreateTriggerInvalidTimingDeep(t *testing.T) {
	sql := "CREATE TRIGGER trig1 DURING INSERT ON t BEGIN SELECT 1; END"
	_, err := Parse(sql)
	if err == nil {
		t.Fatal("expected error for invalid timing")
	}
	if !strings.Contains(err.Error(), "BEFORE, AFTER, or INSTEAD OF") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseCreateTriggerInvalidEventDeep(t *testing.T) {
	sql := "CREATE TRIGGER trig1 BEFORE SELECT ON t BEGIN SELECT 1; END"
	_, err := Parse(sql)
	if err == nil {
		t.Fatal("expected error for invalid event")
	}
	if !strings.Contains(err.Error(), "INSERT, UPDATE, or DELETE") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---- WITH CTE variants ----

func TestParseRecursiveCTEDeep(t *testing.T) {
	sql := "WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 5) SELECT * FROM nums"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cte := stmt.(*SelectStmtWithCTE)
	if !cte.IsRecursive {
		t.Error("expected IsRecursive")
	}
}

func TestParseCTEWithColumnListDeep(t *testing.T) {
	sql := "WITH cte1 (a, b) AS (SELECT 1, 2) SELECT * FROM cte1"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cte := stmt.(*SelectStmtWithCTE)
	if len(cte.CTEs[0].Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cte.CTEs[0].Columns))
	}
}

func TestParseCTEMultipleDeep(t *testing.T) {
	sql := "WITH c1 AS (SELECT 1 AS x), c2 AS (SELECT 2 AS y) SELECT * FROM c1, c2"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cte := stmt.(*SelectStmtWithCTE)
	if len(cte.CTEs) != 2 {
		t.Errorf("expected 2 CTEs, got %d", len(cte.CTEs))
	}
}

// ---- CREATE FTS INDEX ----

func TestParseCreateFTSIndexIfNotExistsDeep(t *testing.T) {
	sql := "CREATE FULLTEXT INDEX IF NOT EXISTS idx1 ON docs (title, body)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	fts := stmt.(*CreateFTSIndexStmt)
	if !fts.IfNotExists {
		t.Error("expected IfNotExists")
	}
	if len(fts.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(fts.Columns))
	}
}

// ---- VACUUM / REFRESH / MATERIALIZED VIEW ----

func TestParseVacuumTableDeep(t *testing.T) {
	sql := "VACUUM mytable"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	v := stmt.(*VacuumStmt)
	if v.Table != "mytable" {
		t.Errorf("expected 'mytable', got '%s'", v.Table)
	}
}

func TestParseRefreshMaterializedViewDeep(t *testing.T) {
	sql := "REFRESH MATERIALIZED VIEW myview"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	r := stmt.(*RefreshMaterializedViewStmt)
	if r.Name != "myview" {
		t.Errorf("expected 'myview', got '%s'", r.Name)
	}
}

func TestParseCreateMaterializedViewDeep(t *testing.T) {
	sql := "CREATE MATERIALIZED VIEW mv1 AS SELECT * FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cmv := stmt.(*CreateMaterializedViewStmt)
	if cmv.Name != "mv1" {
		t.Errorf("expected 'mv1', got '%s'", cmv.Name)
	}
	if cmv.Query == nil {
		t.Error("expected query")
	}
}

func TestParseDropMaterializedViewDeep(t *testing.T) {
	sql := "DROP MATERIALIZED VIEW myview"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dmv := stmt.(*DropMaterializedViewStmt)
	if dmv.Name != "myview" {
		t.Errorf("expected 'myview', got '%s'", dmv.Name)
	}
}

func TestParseDropMaterializedViewIfExistsDeep(t *testing.T) {
	sql := "DROP MATERIALIZED VIEW IF EXISTS myview"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dmv := stmt.(*DropMaterializedViewStmt)
	if !dmv.IfExists {
		t.Error("expected IfExists")
	}
}

// ---- parsePrimary edge cases ----

func TestParsePrimaryNotExistsDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM other WHERE other.id = t.id)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	existsExpr, ok := sel.Where.(*ExistsExpr)
	if !ok {
		t.Fatalf("expected ExistsExpr, got %T", sel.Where)
	}
	if !existsExpr.Not {
		t.Error("expected NOT flag")
	}
}

func TestParsePrimaryUnaryNotDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE NOT 0"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	unary, ok := sel.Where.(*UnaryExpr)
	if !ok {
		t.Fatalf("expected UnaryExpr, got %T", sel.Where)
	}
	if unary.Operator != TokenNot {
		t.Error("expected NOT operator")
	}
}

func TestParsePrimaryKeywordAsIdentifierDeep(t *testing.T) {
	sql := "SELECT status FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
}

func TestParsePrimaryKeywordQualifiedIdentifierDeep(t *testing.T) {
	sql := "SELECT t.status FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
}

// ---- CREATE VIEW variants ----

func TestParseCreateViewIfNotExistsDeep(t *testing.T) {
	sql := "CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cv := stmt.(*CreateViewStmt)
	if !cv.IfNotExists {
		t.Error("expected IfNotExists")
	}
}

// ---- DROP TRIGGER ----

func TestParseDropTriggerIfExistsDeep(t *testing.T) {
	sql := "DROP TRIGGER IF EXISTS trig1"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	dt := stmt.(*DropTriggerStmt)
	if !dt.IfExists {
		t.Error("expected IfExists")
	}
	if dt.Name != "trig1" {
		t.Errorf("expected 'trig1', got '%s'", dt.Name)
	}
}

// ---- IS NOT NULL ----

func TestParseIsNotNullDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE x IS NOT NULL"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	isNull, ok := sel.Where.(*IsNullExpr)
	if !ok {
		t.Fatalf("expected IsNullExpr, got %T", sel.Where)
	}
	if !isNull.Not {
		t.Error("expected NOT flag")
	}
}

// ---- EXISTS ----

func TestParseExistsDeep(t *testing.T) {
	sql := "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM other)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	existsExpr, ok := sel.Where.(*ExistsExpr)
	if !ok {
		t.Fatalf("expected ExistsExpr, got %T", sel.Where)
	}
	if existsExpr.Not {
		t.Error("expected no NOT flag")
	}
}

// ---- CREATE PROCEDURE ----

func TestParseCreateProcedureSimpleDeep(t *testing.T) {
	sql := "CREATE PROCEDURE myproc (p1 INTEGER, p2 TEXT) BEGIN SELECT 1; END"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreateProcedureStmt)
	if cp.Name != "myproc" {
		t.Errorf("expected 'myproc', got '%s'", cp.Name)
	}
	if len(cp.Params) != 2 {
		t.Errorf("expected 2 params, got %d", len(cp.Params))
	}
}

// ---- CAST ----

func TestParseCastExpressionDeep(t *testing.T) {
	sql := "SELECT CAST(x AS INTEGER) FROM t"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
}

// ---- Unary plus ----

func TestParseUnaryPlusDeep(t *testing.T) {
	sql := "SELECT +5 FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
}

// ---- CREATE INDEX variants ----

func TestParseCreateUniqueIndexDeep(t *testing.T) {
	sql := "CREATE UNIQUE INDEX idx1 ON t (a, b)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ci := stmt.(*CreateIndexStmt)
	if !ci.Unique {
		t.Error("expected Unique")
	}
	if len(ci.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(ci.Columns))
	}
}

func TestParseCreateIndexIfNotExistsDeep(t *testing.T) {
	sql := "CREATE INDEX IF NOT EXISTS idx1 ON t (a)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ci := stmt.(*CreateIndexStmt)
	if !ci.IfNotExists {
		t.Error("expected IfNotExists")
	}
}

// ---- CASE expr variants ----

func TestParseCaseWithElseDeep(t *testing.T) {
	sql := "SELECT CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseSearchedCaseDeep(t *testing.T) {
	sql := "SELECT CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
}

// ---- Placeholder in UPDATE ----

func TestParseUpdateWithPlaceholdersDeep(t *testing.T) {
	sql := "UPDATE t SET a = ?, b = ? WHERE id = ?"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStmt)
	if len(upd.Set) != 2 {
		t.Errorf("expected 2 set clauses, got %d", len(upd.Set))
	}
}

// ---- DELETE with WHERE placeholders ----

func TestParseDeleteWithPlaceholdersDeep(t *testing.T) {
	sql := "DELETE FROM t WHERE id = ?"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStmt)
	if del.Where == nil {
		t.Error("expected WHERE clause")
	}
}

// ---- SET op in CTE (INTERSECT, EXCEPT) ----

func TestParseCTEWithIntersectDeep(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS x INTERSECT SELECT 2 AS x) SELECT * FROM cte"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cte := stmt.(*SelectStmtWithCTE)
	if len(cte.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(cte.CTEs))
	}
}

// ---- Concat operator || ----

func TestParseConcatOperatorDeep(t *testing.T) {
	sql := "SELECT 'hello' || ' ' || 'world' FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
}

// ---- Modulo operator ----

func TestParseModuloOperatorDeep(t *testing.T) {
	sql := "SELECT 10 % 3 FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
}

// ---- parseTableRef with subquery ----

func TestParseTableRefSubqueryDeep(t *testing.T) {
	sql := "SELECT * FROM (SELECT 1 AS x) AS sub"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStmt)
	if sel.From.Subquery == nil {
		t.Error("expected subquery in FROM")
	}
	if sel.From.Alias != "sub" {
		t.Errorf("expected alias 'sub', got '%s'", sel.From.Alias)
	}
}

// ---- Parenthesized subquery in SELECT column ----

func TestParseScalarSubqueryDeep(t *testing.T) {
	sql := "SELECT (SELECT COUNT(*) FROM other) AS cnt FROM t"
	_, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
}

// ---- Error paths ----

func TestParseUpdateMissingSetDeep(t *testing.T) {
	_, err := Parse("UPDATE t a = 1")
	if err == nil {
		t.Fatal("expected error for missing SET")
	}
}

func TestParseInsertMissingIntoDeep(t *testing.T) {
	_, err := Parse("INSERT t VALUES (1)")
	if err == nil {
		t.Fatal("expected error for missing INTO")
	}
}

// ---- CREATE POLICY ----

func TestParseCreatePolicyForSelectDeep(t *testing.T) {
	sql := "CREATE POLICY pol1 ON t FOR SELECT TO admin USING (user_id = current_user())"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if cp.Event != "SELECT" {
		t.Errorf("expected SELECT, got %s", cp.Event)
	}
}

func TestParseCreatePolicyForInsertWithCheckDeep(t *testing.T) {
	sql := "CREATE POLICY pol1 ON t FOR INSERT WITH CHECK (status = 'active')"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if cp.Event != "INSERT" {
		t.Errorf("expected INSERT, got %s", cp.Event)
	}
	if cp.WithCheck == nil {
		t.Error("expected WITH CHECK expression")
	}
}

// ---- ON UPDATE SET NULL in FK ----

func TestParseForeignKeyOnUpdateSetNullAgainDeep(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent (id) ON UPDATE SET NULL)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if ct.ForeignKeys[0].OnUpdate != "SET NULL" {
		t.Errorf("expected SET NULL, got %s", ct.ForeignKeys[0].OnUpdate)
	}
}

// ---- FK without referenced columns ----

func TestParseForeignKeyWithoutRefColumnsDeep(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES parent)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStmt)
	if len(ct.ForeignKeys[0].ReferencedColumns) != 0 {
		t.Errorf("expected no referenced columns, got %d", len(ct.ForeignKeys[0].ReferencedColumns))
	}
}

// ---- CREATE POLICY with multiple roles ----

func TestParseCreatePolicyMultipleRolesDeep(t *testing.T) {
	sql := "CREATE POLICY pol1 ON t FOR UPDATE TO admin, editor USING (1 = 1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if len(cp.ForRoles) != 2 {
		t.Errorf("expected 2 roles, got %d", len(cp.ForRoles))
	}
}

// ---- CREATE POLICY for DELETE ----

func TestParseCreatePolicyForDeleteDeep(t *testing.T) {
	sql := "CREATE POLICY pol1 ON t FOR DELETE USING (user_id = 1)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	cp := stmt.(*CreatePolicyStmt)
	if cp.Event != "DELETE" {
		t.Errorf("expected DELETE, got %s", cp.Event)
	}
}
