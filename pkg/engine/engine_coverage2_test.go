package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/audit"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ---------------------------------------------------------------------------
// Helper: open an in-memory DB for quick tests
// ---------------------------------------------------------------------------

func openCoverageDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 256})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

// ===========================================================================
// 1. tokenTypeToString — all branches
// ===========================================================================

func TestCoverage_TokenTypeToString_AllCases(t *testing.T) {
	tests := []struct {
		tok  query.TokenType
		want string
	}{
		{query.TokenEq, "="},
		{query.TokenNeq, "!="},
		{query.TokenLt, "<"},
		{query.TokenGt, ">"},
		{query.TokenLte, "<="},
		{query.TokenGte, ">="},
		{query.TokenAnd, "AND"},
		{query.TokenOr, "OR"},
		{query.TokenNot, "NOT"},
		{query.TokenPlus, "+"},
		{query.TokenMinus, "-"},
		{query.TokenStar, "*"},
		{query.TokenSlash, "/"},
		{query.TokenType(9999), ""}, // default case
	}
	for _, tc := range tests {
		got := tokenTypeToString(tc.tok)
		if got != tc.want {
			t.Errorf("tokenTypeToString(%d) = %q, want %q", tc.tok, got, tc.want)
		}
	}
}

// ===========================================================================
// 2. expressionToString — every type branch
// ===========================================================================

func TestCoverage_ExpressionToString_Nil(t *testing.T) {
	if got := expressionToString(nil); got != "" {
		t.Errorf("expected empty for nil, got %q", got)
	}
}

func TestCoverage_ExpressionToString_Identifier(t *testing.T) {
	e := &query.Identifier{Name: "col1"}
	if got := expressionToString(e); got != "col1" {
		t.Errorf("got %q", got)
	}
}

func TestCoverage_ExpressionToString_QualifiedIdentifier(t *testing.T) {
	// With table prefix
	e := &query.QualifiedIdentifier{Table: "t", Column: "c"}
	if got := expressionToString(e); got != "t.c" {
		t.Errorf("got %q", got)
	}
	// Without table prefix
	e2 := &query.QualifiedIdentifier{Column: "c"}
	if got := expressionToString(e2); got != "c" {
		t.Errorf("got %q", got)
	}
}

func TestCoverage_ExpressionToString_StringLiteral(t *testing.T) {
	e := &query.StringLiteral{Value: "hello"}
	if got := expressionToString(e); got != "'hello'" {
		t.Errorf("got %q", got)
	}
}

func TestCoverage_ExpressionToString_NumberLiteral(t *testing.T) {
	e := &query.NumberLiteral{Value: 42, Raw: "42"}
	if got := expressionToString(e); got != "42" {
		t.Errorf("got %q", got)
	}
}

func TestCoverage_ExpressionToString_BooleanLiteral(t *testing.T) {
	if got := expressionToString(&query.BooleanLiteral{Value: true}); got != "TRUE" {
		t.Errorf("got %q", got)
	}
	if got := expressionToString(&query.BooleanLiteral{Value: false}); got != "FALSE" {
		t.Errorf("got %q", got)
	}
}

func TestCoverage_ExpressionToString_NullLiteral(t *testing.T) {
	if got := expressionToString(&query.NullLiteral{}); got != "NULL" {
		t.Errorf("got %q", got)
	}
}

func TestCoverage_ExpressionToString_BinaryExpr(t *testing.T) {
	e := &query.BinaryExpr{
		Left:     &query.Identifier{Name: "a"},
		Operator: query.TokenPlus,
		Right:    &query.NumberLiteral{Value: 1, Raw: "1"},
	}
	want := "a + 1"
	if got := expressionToString(e); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCoverage_ExpressionToString_UnaryExpr(t *testing.T) {
	e := &query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.BooleanLiteral{Value: true},
	}
	want := "NOT TRUE"
	if got := expressionToString(e); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCoverage_ExpressionToString_FunctionCall(t *testing.T) {
	e := &query.FunctionCall{
		Name: "MAX",
		Args: []query.Expression{
			&query.Identifier{Name: "x"},
			&query.Identifier{Name: "y"},
		},
	}
	want := "MAX(x, y)"
	if got := expressionToString(e); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// No-arg function
	e2 := &query.FunctionCall{Name: "NOW", Args: nil}
	want2 := "NOW()"
	if got := expressionToString(e2); got != want2 {
		t.Errorf("got %q, want %q", got, want2)
	}
}

func TestCoverage_ExpressionToString_InExpr(t *testing.T) {
	e := &query.InExpr{
		Expr: &query.Identifier{Name: "id"},
		List: []query.Expression{
			&query.NumberLiteral{Value: 1, Raw: "1"},
			&query.NumberLiteral{Value: 2, Raw: "2"},
		},
	}
	want := "id IN (1, 2)"
	if got := expressionToString(e); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCoverage_ExpressionToString_LikeExpr(t *testing.T) {
	e := &query.LikeExpr{
		Expr:    &query.Identifier{Name: "name"},
		Pattern: &query.StringLiteral{Value: "%foo%"},
		Not:     false,
	}
	want := "name LIKE '%foo%'"
	if got := expressionToString(e); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// NOT LIKE
	e2 := &query.LikeExpr{
		Expr:    &query.Identifier{Name: "name"},
		Pattern: &query.StringLiteral{Value: "%bar%"},
		Not:     true,
	}
	want2 := "name NOT LIKE '%bar%'"
	if got := expressionToString(e2); got != want2 {
		t.Errorf("got %q, want %q", got, want2)
	}
}

func TestCoverage_ExpressionToString_IsNullExpr(t *testing.T) {
	e := &query.IsNullExpr{
		Expr: &query.Identifier{Name: "val"},
		Not:  false,
	}
	want := "val IS NULL"
	if got := expressionToString(e); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// IS NOT NULL
	e2 := &query.IsNullExpr{
		Expr: &query.Identifier{Name: "val"},
		Not:  true,
	}
	want2 := "val IS NOT NULL"
	if got := expressionToString(e2); got != want2 {
		t.Errorf("got %q, want %q", got, want2)
	}
}

func TestCoverage_ExpressionToString_UnknownType(t *testing.T) {
	// StarExpr is not handled by expressionToString -> default case
	e := &query.StarExpr{}
	if got := expressionToString(e); got != "" {
		t.Errorf("expected empty for unknown expression type, got %q", got)
	}
}

// ===========================================================================
// 3. Open — various configurations
// ===========================================================================

func TestCoverage_Open_NilOptions(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open with nil options: %v", err)
	}
	db.Close()
}

func TestCoverage_Open_DiskBased(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, &Options{CacheSize: 256})
	if err != nil {
		t.Fatalf("open disk db: %v", err)
	}
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO t1 VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	db.Close()

	// Reopen and verify data persisted (covers loadExisting + saveMetaPage)
	db2, err := Open(path, &Options{CacheSize: 256})
	if err != nil {
		t.Fatalf("reopen disk db: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(ctx, "SELECT val FROM t1 WHERE id = 1")
	if err != nil {
		t.Fatalf("query after reopen: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a row after reopen")
	}
	var val string
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != "hello" {
		t.Errorf("got %q, want %q", val, "hello")
	}
}

func TestCoverage_Open_WithEncryptionKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "enc.db")
	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes for AES-256
	db, err := Open(path, &Options{CacheSize: 256, EncryptionKey: key})
	if err != nil {
		t.Fatalf("open with encryption key: %v", err)
	}
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE sec (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	db.Close()
}

func TestCoverage_Open_WithEncryptionConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "enc2.db")
	db, err := Open(path, &Options{
		CacheSize: 256,
		EncryptionConfig: &storage.EncryptionConfig{
			Enabled:   true,
			Key:       []byte("0123456789abcdef0123456789abcdef"),
			Algorithm: "aes-256-gcm",
			UseArgon2: true,
		},
	})
	if err != nil {
		t.Fatalf("open with encryption config: %v", err)
	}
	db.Close()
}

func TestCoverage_Open_WithAuditConfig(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit.log")
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 256,
		AuditConfig: &audit.Config{
			Enabled: true,
			LogFile: logFile,
		},
	})
	if err != nil {
		t.Fatalf("open with audit config: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	// Exec some statements to trigger audit logging code paths
	_, _ = db.Exec(ctx, "CREATE TABLE aud (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO aud VALUES (1, 'test')")
	_, _ = db.Exec(ctx, "UPDATE aud SET name = 'changed' WHERE id = 1")
	_, _ = db.Exec(ctx, "DELETE FROM aud WHERE id = 1")
	_, _ = db.Query(ctx, "SELECT * FROM aud")
}

func TestCoverage_Open_WithRLS(t *testing.T) {
	db, err := Open(":memory:", &Options{InMemory: true, CacheSize: 256, EnableRLS: true})
	if err != nil {
		t.Fatalf("open with RLS: %v", err)
	}
	defer db.Close()
}

func TestCoverage_Open_WithMaxConnections(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:       true,
		CacheSize:      256,
		MaxConnections: 5,
	})
	if err != nil {
		t.Fatalf("open with max connections: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE connt (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

// ===========================================================================
// 4. Commit — transaction commit paths
// ===========================================================================

func TestCoverage_Commit_BasicTransaction(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE txtest (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err = tx.Exec(ctx, "INSERT INTO txtest VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	rows, err := db.Query(ctx, "SELECT val FROM txtest WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected row after commit")
	}
}

func TestCoverage_Commit_DoubleCommit(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE dc (id INTEGER PRIMARY KEY)")

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, _ = tx.Exec(ctx, "INSERT INTO dc VALUES (1)")
	if err := tx.Commit(); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	// Second commit should fail
	err = tx.Commit()
	if err == nil {
		t.Error("expected error on double commit")
	}
}

func TestCoverage_Commit_ViaExec(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE ec (id INTEGER PRIMARY KEY)")
	_, err := db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	_, err = db.Exec(ctx, "INSERT INTO ec VALUES (1)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = db.Exec(ctx, "COMMIT")
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestCoverage_Commit_NoTransactionInProgress(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "COMMIT")
	if err == nil {
		t.Error("expected error when committing with no transaction")
	}
}

func TestCoverage_Rollback_ViaExec(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE rb (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "BEGIN")
	_, _ = db.Exec(ctx, "INSERT INTO rb VALUES (1)")
	_, err := db.Exec(ctx, "ROLLBACK")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	rows, err := db.Query(ctx, "SELECT * FROM rb")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Error("expected no rows after rollback")
	}
}

func TestCoverage_Rollback_NoTransaction(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "ROLLBACK")
	if err == nil {
		t.Error("expected error when rolling back with no transaction")
	}
}

func TestCoverage_Rollback_DoubleRollback(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE dr (id INTEGER PRIMARY KEY)")
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("first rollback: %v", err)
	}
	err = tx.Rollback()
	if err == nil {
		t.Error("expected error on double rollback")
	}
}

func TestCoverage_BeginDoubleTransaction(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Fatalf("first begin: %v", err)
	}
	_, err = db.Exec(ctx, "BEGIN")
	if err == nil {
		t.Error("expected error on double BEGIN")
	}
	// cleanup
	_, _ = db.Exec(ctx, "ROLLBACK")
}

// ===========================================================================
// 5. query function — internal routing
// ===========================================================================

func TestCoverage_Query_SelectBasic(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE q1 (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO q1 VALUES (1, 'alice')")

	rows, err := db.Query(ctx, "SELECT name FROM q1 WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected row")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if name != "alice" {
		t.Errorf("got %q, want alice", name)
	}
}

func TestCoverage_Query_UnionViaQuery(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE ua (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "CREATE TABLE ub (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "INSERT INTO ua VALUES (1)")
	_, _ = db.Exec(ctx, "INSERT INTO ub VALUES (2)")

	rows, err := db.Query(ctx, "SELECT id FROM ua UNION SELECT id FROM ub ORDER BY id")
	if err != nil {
		t.Fatalf("union query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestCoverage_Query_CTEViaQuery(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE ctet (id INTEGER PRIMARY KEY, val INTEGER)")
	_, _ = db.Exec(ctx, "INSERT INTO ctet VALUES (1, 10)")
	_, _ = db.Exec(ctx, "INSERT INTO ctet VALUES (2, 20)")

	rows, err := db.Query(ctx, "WITH cte AS (SELECT id, val FROM ctet) SELECT val FROM cte WHERE id = 2")
	if err != nil {
		t.Fatalf("cte query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected a row")
	}
	var val int
	if err := rows.Scan(&val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != 20 {
		t.Errorf("got %d, want 20", val)
	}
}

func TestCoverage_Query_ShowDatabases(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	rows, err := db.Query(ctx, "SHOW DATABASES")
	if err != nil {
		t.Fatalf("show databases: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected at least one database")
	}
}

func TestCoverage_Query_ExecOnQueryStatement(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE qex (id INTEGER PRIMARY KEY)")
	// SHOW TABLES via Exec should return error
	_, err := db.Exec(ctx, "SHOW TABLES")
	if err == nil {
		t.Error("expected error using Exec for SHOW TABLES")
	}
}

func TestCoverage_Query_DescribeViaQuery(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE desct (id INTEGER PRIMARY KEY, name TEXT)")
	rows, err := db.Query(ctx, "DESCRIBE desct")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count < 2 {
		t.Errorf("expected at least 2 columns described, got %d", count)
	}
}

func TestCoverage_Query_ExplainViaQuery(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE expl (id INTEGER PRIMARY KEY, val TEXT)")
	rows, err := db.Query(ctx, "EXPLAIN SELECT * FROM expl")
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected explain output")
	}
}

// ===========================================================================
// 6. executeAlterTable — all action branches
// ===========================================================================

func TestCoverage_AlterTable_UnsupportedAction(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE alt_unsup (id INTEGER PRIMARY KEY, name TEXT)")

	// Directly call executeAlterTable with unsupported action
	_, err := db.executeAlterTable(ctx, &query.AlterTableStmt{
		Table:  "alt_unsup",
		Action: "MODIFY",
	})
	if err == nil {
		t.Error("expected error for unsupported action")
	}
	if err != nil && !contains(err.Error(), "unsupported ALTER TABLE action") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCoverage_AlterTable_AllActions(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE alt_all (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// ADD COLUMN
	_, err = db.Exec(ctx, "ALTER TABLE alt_all ADD COLUMN email TEXT")
	if err != nil {
		t.Fatalf("add column: %v", err)
	}

	// RENAME COLUMN
	_, err = db.Exec(ctx, "ALTER TABLE alt_all RENAME COLUMN email TO mail")
	if err != nil {
		t.Fatalf("rename column: %v", err)
	}

	// DROP COLUMN
	_, err = db.Exec(ctx, "ALTER TABLE alt_all DROP COLUMN mail")
	if err != nil {
		t.Fatalf("drop column: %v", err)
	}

	// RENAME TABLE
	_, err = db.Exec(ctx, "ALTER TABLE alt_all RENAME TO alt_renamed")
	if err != nil {
		t.Fatalf("rename table: %v", err)
	}

	// Verify rename
	rows, err := db.Query(ctx, "SELECT id FROM alt_renamed")
	if err != nil {
		t.Fatalf("query after rename: %v", err)
	}
	rows.Close()
}

// ===========================================================================
// 7. executeDropTable — including IF EXISTS
// ===========================================================================

func TestCoverage_DropTable_Basic(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE dt1 (id INTEGER PRIMARY KEY)")
	_, err := db.Exec(ctx, "DROP TABLE dt1")
	if err != nil {
		t.Fatalf("drop table: %v", err)
	}
	// Verify gone
	_, err = db.Query(ctx, "SELECT * FROM dt1")
	if err == nil {
		t.Error("expected error after drop")
	}
}

func TestCoverage_DropTable_IfExists(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	// Drop non-existent table with IF EXISTS should not error
	_, err := db.Exec(ctx, "DROP TABLE IF EXISTS nonexistent")
	if err != nil {
		t.Fatalf("drop table if exists: %v", err)
	}
}

func TestCoverage_DropTable_NonExistent(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "DROP TABLE ghost_table")
	if err == nil {
		t.Error("expected error dropping non-existent table")
	}
}

// ===========================================================================
// 8. executeCreateProcedure
// ===========================================================================

func TestCoverage_CreateProcedure_Basic(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "CREATE TABLE proc_tbl (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Create a procedure via direct call since parser only handles header
	proc := &query.CreateProcedureStmt{
		Name:   "test_proc",
		Params: nil,
		Body: []query.Statement{
			&query.InsertStmt{},
		},
	}
	_, err = db.executeCreateProcedure(ctx, proc)
	if err != nil {
		t.Fatalf("create procedure: %v", err)
	}

	// Drop procedure
	_, err = db.Exec(ctx, "DROP PROCEDURE test_proc")
	if err != nil {
		t.Fatalf("drop procedure: %v", err)
	}
}

func TestCoverage_DropProcedure_NonExistent(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "DROP PROCEDURE nonexistent_proc")
	if err == nil {
		t.Error("expected error for non-existent procedure")
	}
}

// ===========================================================================
// 9. Close — various scenarios
// ===========================================================================

func TestCoverage_Close_InMemory(t *testing.T) {
	db := openCoverageDB(t)
	err := db.Close()
	if err != nil {
		t.Errorf("close: %v", err)
	}
	// Double close should be no-op
	err = db.Close()
	if err != nil {
		t.Errorf("double close: %v", err)
	}
}

func TestCoverage_Close_DiskBased(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close_test.db")
	db, err := Open(path, &Options{CacheSize: 256})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE ct (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "INSERT INTO ct VALUES (1)")

	err = db.Close()
	if err != nil {
		t.Errorf("close disk db: %v", err)
	}
}

func TestCoverage_Close_AfterClosedExec(t *testing.T) {
	db := openCoverageDB(t)
	db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "SELECT 1")
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("expected ErrDatabaseClosed, got: %v", err)
	}
	_, err = db.Query(ctx, "SELECT 1")
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("expected ErrDatabaseClosed for Query, got: %v", err)
	}
}

func TestCoverage_Close_WithAuditLogger(t *testing.T) {
	dir := t.TempDir()
	logFile := filepath.Join(dir, "audit_close.log")
	db, err := Open(":memory:", &Options{
		InMemory:  true,
		CacheSize: 256,
		AuditConfig: &audit.Config{
			Enabled: true,
			LogFile: logFile,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	err = db.Close()
	if err != nil {
		t.Errorf("close with audit: %v", err)
	}
}

// ===========================================================================
// 10. saveMetaPage / loadExisting — disk persistence
// ===========================================================================

func TestCoverage_SaveAndLoadMetaPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.db")

	// Create a new database and write data
	db, err := Open(path, &Options{CacheSize: 256})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE meta_tbl (id INTEGER PRIMARY KEY, data TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO meta_tbl VALUES (1, 'test1')")
	_, _ = db.Exec(ctx, "INSERT INTO meta_tbl VALUES (2, 'test2')")
	db.Close()

	// Reopen — this covers loadExisting
	db2, err := Open(path, &Options{CacheSize: 256})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(ctx, "SELECT data FROM meta_tbl ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestCoverage_LoadExisting_WithWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal_test.db")

	// Open with WAL enabled (default)
	db, err := Open(path, &Options{CacheSize: 256, WALEnabled: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE wal_tbl (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO wal_tbl VALUES (1, 'wal_data')")
	db.Close()

	// Reopen with WAL — loadExisting will try WAL recovery path
	db2, err := Open(path, &Options{CacheSize: 256, WALEnabled: true})
	if err != nil {
		t.Fatalf("reopen with WAL: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(ctx, "SELECT name FROM wal_tbl WHERE id = 1")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected row")
	}
	var name string
	rows.Scan(&name)
	if name != "wal_data" {
		t.Errorf("got %q, want wal_data", name)
	}
}

func TestCoverage_LoadExisting_CorruptMetaPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.db")

	// Write garbage to the file to simulate a corrupted meta page
	if err := os.WriteFile(path, []byte("this is not a valid database"), 0644); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	_, err := Open(path, &Options{CacheSize: 256})
	if err == nil {
		t.Error("expected error opening corrupt database")
	}
}

// ===========================================================================
// 11. executeVacuum
// ===========================================================================

func TestCoverage_Vacuum_Basic(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE vac (id INTEGER PRIMARY KEY, name TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO vac VALUES (1, 'a')")
	_, _ = db.Exec(ctx, "INSERT INTO vac VALUES (2, 'b')")
	_, _ = db.Exec(ctx, "DELETE FROM vac WHERE id = 1")

	_, err := db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}

	// Verify data still accessible
	rows, err := db.Query(ctx, "SELECT name FROM vac WHERE id = 2")
	if err != nil {
		t.Fatalf("query after vacuum: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected row after vacuum")
	}
}

func TestCoverage_Vacuum_EmptyTable(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE vac_empty (id INTEGER PRIMARY KEY)")
	_, err := db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Fatalf("vacuum on empty table: %v", err)
	}
}

// ===========================================================================
// 12. executeAnalyze
// ===========================================================================

func TestCoverage_Analyze_SpecificTable(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE an1 (id INTEGER PRIMARY KEY, val TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO an1 VALUES (1, 'x')")
	_, _ = db.Exec(ctx, "INSERT INTO an1 VALUES (2, 'y')")

	_, err := db.Exec(ctx, "ANALYZE an1")
	if err != nil {
		t.Fatalf("analyze specific table: %v", err)
	}
}

func TestCoverage_Analyze_AllTables(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE an2 (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "CREATE TABLE an3 (id INTEGER PRIMARY KEY)")
	_, _ = db.Exec(ctx, "INSERT INTO an2 VALUES (1)")
	_, _ = db.Exec(ctx, "INSERT INTO an3 VALUES (1)")

	_, err := db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Fatalf("analyze all: %v", err)
	}
}

func TestCoverage_Analyze_NonExistentTable(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "ANALYZE ghost_table")
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

// ===========================================================================
// 13. CircuitBreaker.Execute
// ===========================================================================

func TestCoverage_CircuitBreaker_ExecuteSuccess(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	defer cb.Stop()

	err := cb.Execute(context.Background(), func() error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cb.State() != CircuitClosed {
		t.Errorf("expected closed state, got: %s", cb.State())
	}
}

func TestCoverage_CircuitBreaker_ExecuteFailure(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	defer cb.Stop()

	sentinel := errors.New("boom")
	err := cb.Execute(context.Background(), func() error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got: %v", err)
	}
	stats := cb.Stats()
	if stats.Failures < 1 {
		t.Error("expected at least 1 failure")
	}
}

func TestCoverage_CircuitBreaker_ExecuteContextCancel(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	defer cb.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := cb.Execute(ctx, func() error {
		time.Sleep(10 * time.Second) // Will not complete
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

func TestCoverage_CircuitBreaker_ExecutePanic(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	defer cb.Stop()

	err := cb.Execute(context.Background(), func() error {
		panic("test panic")
	})
	if err == nil {
		t.Error("expected error from panic")
	}
	if !contains(err.Error(), "panic") {
		t.Errorf("expected panic error, got: %v", err)
	}
}

func TestCoverage_CircuitBreaker_OpensAfterMaxFailures(t *testing.T) {
	cfg := &CircuitBreakerConfig{
		MaxFailures:         3,
		MinSuccesses:        1,
		ResetTimeout:        1 * time.Second,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(cfg)
	defer cb.Stop()

	sentinel := errors.New("fail")
	// Trigger enough failures to open the circuit
	for i := 0; i < 3; i++ {
		_ = cb.Execute(context.Background(), func() error { return sentinel })
	}

	if cb.State() != CircuitOpen {
		t.Errorf("expected open, got: %s", cb.State())
	}

	// Next call should be rejected
	err := cb.Execute(context.Background(), func() error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got: %v", err)
	}
}

func TestCoverage_CircuitBreaker_HalfOpenRecovery(t *testing.T) {
	cfg := &CircuitBreakerConfig{
		MaxFailures:         2,
		MinSuccesses:        1,
		ResetTimeout:        50 * time.Millisecond, // Short for testing
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(cfg)
	defer cb.Stop()

	// Open the circuit
	sentinel := errors.New("fail")
	for i := 0; i < 2; i++ {
		_ = cb.Execute(context.Background(), func() error { return sentinel })
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("expected open, got: %s", cb.State())
	}

	// Wait for reset timeout
	time.Sleep(100 * time.Millisecond)

	// A success should close it again
	err := cb.Execute(context.Background(), func() error { return nil })
	if err != nil {
		t.Fatalf("expected success in half-open: %v", err)
	}
	if cb.State() != CircuitClosed {
		t.Errorf("expected closed after recovery, got: %s", cb.State())
	}
}

func TestCoverage_CircuitBreaker_ConcurrencyLimit(t *testing.T) {
	cfg := &CircuitBreakerConfig{
		MaxFailures:         5,
		MinSuccesses:        3,
		ResetTimeout:        30 * time.Second,
		MaxConcurrency:      1, // Allow only 1 concurrent request
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(cfg)
	defer cb.Stop()

	// Acquire the single slot
	if err := cb.Allow(); err != nil {
		t.Fatalf("allow: %v", err)
	}

	// Second call should be rejected
	err := cb.Allow()
	if !errors.Is(err, ErrCircuitTooMany) {
		t.Errorf("expected ErrCircuitTooMany, got: %v", err)
	}

	cb.Release()
}

func TestCoverage_CircuitBreaker_StoppedNoOps(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	cb.Stop()

	// After stop, ReportSuccess and ReportFailure should be no-ops
	cb.ReportSuccess()
	cb.ReportFailure()
	// Should not panic or change state
	if cb.State() != CircuitClosed {
		t.Errorf("expected closed state after stop, got: %s", cb.State())
	}
}

// ===========================================================================
// 14. shouldAttemptReset
// ===========================================================================

func TestCoverage_ShouldAttemptReset_NoFailure(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	defer cb.Stop()

	// lastFailure = 0 means no failure recorded
	if !cb.shouldAttemptReset() {
		t.Error("expected true when no failures recorded")
	}
}

func TestCoverage_ShouldAttemptReset_RecentFailure(t *testing.T) {
	cfg := &CircuitBreakerConfig{
		MaxFailures:         5,
		MinSuccesses:        3,
		ResetTimeout:        1 * time.Hour, // Very long timeout
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(cfg)
	defer cb.Stop()

	cb.lastFailure.Store(time.Now().Unix())
	if cb.shouldAttemptReset() {
		t.Error("expected false when failure was recent")
	}
}

func TestCoverage_ShouldAttemptReset_OldFailure(t *testing.T) {
	cfg := &CircuitBreakerConfig{
		MaxFailures:         5,
		MinSuccesses:        3,
		ResetTimeout:        1 * time.Second,
		MaxConcurrency:      10,
		HalfOpenMaxRequests: 1,
	}
	cb := NewCircuitBreaker(cfg)
	defer cb.Stop()

	cb.lastFailure.Store(time.Now().Add(-2 * time.Second).Unix())
	if !cb.shouldAttemptReset() {
		t.Error("expected true when failure is old enough")
	}
}

// ===========================================================================
// Additional coverage: Savepoints, Context cancellation, Tx.Query
// ===========================================================================

func TestCoverage_Savepoint_ViaExec(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE sp (id INTEGER PRIMARY KEY, val TEXT)")
	_, _ = db.Exec(ctx, "BEGIN")
	_, _ = db.Exec(ctx, "INSERT INTO sp VALUES (1, 'a')")
	_, err := db.Exec(ctx, "SAVEPOINT s1")
	if err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	_, _ = db.Exec(ctx, "INSERT INTO sp VALUES (2, 'b')")
	_, err = db.Exec(ctx, "ROLLBACK TO SAVEPOINT s1")
	if err != nil {
		t.Fatalf("rollback to savepoint: %v", err)
	}
	_, _ = db.Exec(ctx, "COMMIT")

	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM sp")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	rows.Next()
	var cnt int
	rows.Scan(&cnt)
	if cnt != 1 {
		t.Errorf("expected 1 row, got %d", cnt)
	}
}

func TestCoverage_Savepoint_NoTransaction(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "SAVEPOINT s1")
	if err == nil {
		t.Error("expected error for savepoint outside transaction")
	}
}

func TestCoverage_ReleaseSavepoint_NoTransaction(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "RELEASE SAVEPOINT s1")
	if err == nil {
		t.Error("expected error for release savepoint outside transaction")
	}
}

func TestCoverage_Execute_ContextCancelled(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Pre-cancel

	_, err := db.Exec(ctx, "CREATE TABLE cc (id INTEGER PRIMARY KEY)")
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestCoverage_TxExec_AfterComplete(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE txdone (id INTEGER PRIMARY KEY)")
	tx, _ := db.Begin(ctx)
	tx.Commit()

	_, err := tx.Exec(ctx, "INSERT INTO txdone VALUES (1)")
	if err == nil {
		t.Error("expected error for exec after commit")
	}
}

func TestCoverage_TxQuery_AfterComplete(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE txqdone (id INTEGER PRIMARY KEY)")
	tx, _ := db.Begin(ctx)
	tx.Rollback()

	_, err := tx.Query(ctx, "SELECT * FROM txqdone")
	if err == nil {
		t.Error("expected error for query after rollback")
	}
}

func TestCoverage_TxQuery_Basic(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE txq (id INTEGER PRIMARY KEY, val TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO txq VALUES (1, 'before')")

	tx, _ := db.Begin(ctx)
	_, _ = tx.Exec(ctx, "UPDATE txq SET val = 'during' WHERE id = 1")

	rows, err := tx.Query(ctx, "SELECT val FROM txq WHERE id = 1")
	if err != nil {
		t.Fatalf("tx query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected row")
	}
	var val string
	rows.Scan(&val)
	if val != "during" {
		t.Errorf("got %q, want during", val)
	}
	tx.Commit()
}

func TestCoverage_Exec_ParseError(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "NOT VALID SQL !!!")
	if err == nil {
		t.Error("expected parse error")
	}
}

func TestCoverage_Query_ParseError(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Query(ctx, "NOT VALID SQL !!!")
	if err == nil {
		t.Error("expected parse error")
	}
}

func TestCoverage_Exec_UnsupportedStatementType(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	// SELECT via Exec should be routed to query path but may fail differently
	// Let's try EXPLAIN via Exec which should say "use Query()"
	_, err := db.Exec(ctx, "DESCRIBE nonexistent")
	if err == nil {
		t.Error("expected error for DESCRIBE via Exec")
	}
}

// ===========================================================================
// CircuitBreakerManager coverage
// ===========================================================================

func TestCoverage_CircuitBreakerManager_GetOrCreate(t *testing.T) {
	mgr := NewCircuitBreakerManager()

	cb1 := mgr.GetOrCreate("op1", DefaultCircuitBreakerConfig())
	cb2 := mgr.GetOrCreate("op1", DefaultCircuitBreakerConfig())
	if cb1 != cb2 {
		t.Error("expected same breaker for same key")
	}

	cb3 := mgr.GetOrCreate("op2", DefaultCircuitBreakerConfig())
	if cb1 == cb3 {
		t.Error("expected different breaker for different key")
	}
}

func TestCoverage_CircuitBreakerManager_Get(t *testing.T) {
	mgr := NewCircuitBreakerManager()

	_, ok := mgr.Get("missing")
	if ok {
		t.Error("expected not found")
	}

	mgr.GetOrCreate("existing", DefaultCircuitBreakerConfig())
	cb, ok := mgr.Get("existing")
	if !ok || cb == nil {
		t.Error("expected to find existing breaker")
	}
}

func TestCoverage_CircuitBreakerManager_Remove(t *testing.T) {
	mgr := NewCircuitBreakerManager()
	mgr.GetOrCreate("rem", DefaultCircuitBreakerConfig())
	mgr.Remove("rem")
	_, ok := mgr.Get("rem")
	if ok {
		t.Error("expected not found after remove")
	}
}

func TestCoverage_CircuitBreakerManager_AllStats(t *testing.T) {
	mgr := NewCircuitBreakerManager()
	mgr.GetOrCreate("a", DefaultCircuitBreakerConfig())
	mgr.GetOrCreate("b", DefaultCircuitBreakerConfig())

	stats := mgr.AllStats()
	if len(stats) != 2 {
		t.Errorf("expected 2 stats, got %d", len(stats))
	}
}

// ===========================================================================
// Materialized views, FTS, and other DDL via engine
// ===========================================================================

func TestCoverage_CreateDropMaterializedView(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE mv_src (id INTEGER PRIMARY KEY, val INTEGER)")
	_, _ = db.Exec(ctx, "INSERT INTO mv_src VALUES (1, 10)")
	_, _ = db.Exec(ctx, "INSERT INTO mv_src VALUES (2, 20)")

	_, err := db.Exec(ctx, "CREATE MATERIALIZED VIEW mv_test AS SELECT val FROM mv_src WHERE val > 5")
	if err != nil {
		t.Fatalf("create mat view: %v", err)
	}

	_, err = db.Exec(ctx, "REFRESH MATERIALIZED VIEW mv_test")
	if err != nil {
		t.Fatalf("refresh mat view: %v", err)
	}

	_, err = db.Exec(ctx, "DROP MATERIALIZED VIEW mv_test")
	if err != nil {
		t.Fatalf("drop mat view: %v", err)
	}
}

func TestCoverage_CreateDropView(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE vw_src (id INTEGER PRIMARY KEY, name TEXT)")
	_, err := db.Exec(ctx, "CREATE VIEW vw_test AS SELECT name FROM vw_src")
	if err != nil {
		t.Fatalf("create view: %v", err)
	}

	// Create IF NOT EXISTS
	_, err = db.Exec(ctx, "CREATE VIEW IF NOT EXISTS vw_test AS SELECT name FROM vw_src")
	if err != nil {
		t.Fatalf("create view if not exists: %v", err)
	}

	// Drop view IF EXISTS on non-existent
	_, err = db.Exec(ctx, "DROP VIEW IF EXISTS nonexistent_view")
	if err != nil {
		t.Fatalf("drop view if exists: %v", err)
	}

	_, err = db.Exec(ctx, "DROP VIEW vw_test")
	if err != nil {
		t.Fatalf("drop view: %v", err)
	}
}

// ===========================================================================
// Shutdown
// ===========================================================================

func TestCoverage_Shutdown(t *testing.T) {
	db := openCoverageDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := db.Shutdown(ctx)
	if err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

// ===========================================================================
// Metrics
// ===========================================================================

func TestCoverage_GetMetrics(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	data, err := db.GetMetrics()
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty metrics")
	}
}

func TestCoverage_GetMetricsCollector(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()

	mc := db.GetMetricsCollector()
	if mc == nil {
		t.Error("expected non-nil collector")
	}
}

// ===========================================================================
// CircuitState.String
// ===========================================================================

func TestCoverage_CircuitStateString(t *testing.T) {
	tests := []struct {
		state CircuitState
		want  string
	}{
		{CircuitClosed, "closed"},
		{CircuitOpen, "open"},
		{CircuitHalfOpen, "half-open"},
		{CircuitState(99), "unknown"},
	}
	for _, tc := range tests {
		if got := tc.state.String(); got != tc.want {
			t.Errorf("CircuitState(%d).String() = %q, want %q", tc.state, got, tc.want)
		}
	}
}

// ===========================================================================
// QueryRow coverage
// ===========================================================================

func TestCoverage_QueryRow_Basic(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE qr (id INTEGER PRIMARY KEY, val TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO qr VALUES (1, 'row1')")

	row := db.QueryRow(ctx, "SELECT val FROM qr WHERE id = 1")
	var val string
	if err := row.Scan(&val); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if val != "row1" {
		t.Errorf("got %q, want row1", val)
	}
}

func TestCoverage_QueryRow_NoRows(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, _ = db.Exec(ctx, "CREATE TABLE qrnr (id INTEGER PRIMARY KEY)")
	row := db.QueryRow(ctx, "SELECT id FROM qrnr WHERE id = 999")
	var id int
	err := row.Scan(&id)
	if err == nil {
		t.Error("expected error for no rows")
	}
}

func TestCoverage_QueryRow_ParseError(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	row := db.QueryRow(ctx, "INVALID SQL!!!")
	var x int
	err := row.Scan(&x)
	if err == nil {
		t.Error("expected error for invalid SQL")
	}
}

// ===========================================================================
// Disk DB open/close cycle with RLS
// ===========================================================================

func TestCoverage_DiskDB_WithRLS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rls.db")

	db, err := Open(path, &Options{CacheSize: 256, EnableRLS: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE rls_tbl (id INTEGER PRIMARY KEY, owner TEXT)")
	_, _ = db.Exec(ctx, "INSERT INTO rls_tbl VALUES (1, 'alice')")
	db.Close()

	// Reopen with RLS
	db2, err := Open(path, &Options{CacheSize: 256, EnableRLS: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
}

// ===========================================================================
// SET and USE compatibility (MySQL compat)
// ===========================================================================

func TestCoverage_SetVar(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "SET NAMES utf8")
	if err != nil {
		t.Fatalf("SET: %v", err)
	}
}

func TestCoverage_Use(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	_, err := db.Exec(ctx, "USE mydb")
	if err != nil {
		t.Fatalf("USE: %v", err)
	}
}

// ===========================================================================
// Query timeout
// ===========================================================================

func TestCoverage_QueryTimeout(t *testing.T) {
	db, err := Open(":memory:", &Options{
		InMemory:     true,
		CacheSize:    256,
		QueryTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	_, err = db.Exec(ctx, "CREATE TABLE qt (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	rows, err := db.Query(ctx, "SELECT * FROM qt")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	rows.Close()
}

// ===========================================================================
// Tx.Query on closed DB
// ===========================================================================

func TestCoverage_TxExec_ClosedDB(t *testing.T) {
	db := openCoverageDB(t)
	ctx := context.Background()
	_, _ = db.Exec(ctx, "CREATE TABLE txcl (id INTEGER PRIMARY KEY)")

	tx, _ := db.Begin(ctx)

	// Close DB while tx is active
	db.mu.Lock()
	db.closed = true
	db.mu.Unlock()

	_, err := tx.Exec(ctx, "INSERT INTO txcl VALUES (1)")
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("expected ErrDatabaseClosed, got: %v", err)
	}

	_, err = tx.Query(ctx, "SELECT * FROM txcl")
	if !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("expected ErrDatabaseClosed for query, got: %v", err)
	}

	// Reset for cleanup
	db.mu.Lock()
	db.closed = false
	db.mu.Unlock()
	tx.done.Store(false)
	tx.Rollback()
	db.Close()
}

// ===========================================================================
// Tx parse error
// ===========================================================================

func TestCoverage_TxExec_ParseError(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	tx, _ := db.Begin(ctx)
	_, err := tx.Exec(ctx, "TOTALLY INVALID!!!")
	if err == nil {
		t.Error("expected parse error")
	}
	tx.Rollback()
}

func TestCoverage_TxQuery_ParseError(t *testing.T) {
	db := openCoverageDB(t)
	defer db.Close()
	ctx := context.Background()

	tx, _ := db.Begin(ctx)
	_, err := tx.Query(ctx, "TOTALLY INVALID!!!")
	if err == nil {
		t.Error("expected parse error")
	}
	tx.Rollback()
}

// ===========================================================================
// helper
// ===========================================================================

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsHelper(s, sub))
}

func containsHelper(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
