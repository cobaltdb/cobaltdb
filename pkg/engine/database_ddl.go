package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// executeCreateTable executes CREATE TABLE
func (db *DB) executeCreateTable(ctx context.Context, stmt *query.CreateTableStmt) (Result, error) {
	if stmt.AsSelect != nil {
		return db.executeCreateTableAsSelect(ctx, stmt)
	}
	if stmt.IfNotExists {
		if _, err := db.catalog.GetTable(stmt.Table); err == nil {
			return Result{RowsAffected: 0}, nil
		}
	}
	cleanupOnError := func(primary error) error {
		if cleanupErr := db.catalog.CleanupFailedCreateTable(stmt.Table); cleanupErr != nil {
			return fmt.Errorf("%w; cleanup failed: %v", primary, cleanupErr)
		}
		return primary
	}
	if err := db.catalog.CreateTable(stmt); err != nil {
		return Result{}, err
	}
	// Named column-level UNIQUE constraints are represented as unique indexes so
	// ALTER TABLE ... DROP CONSTRAINT removes enforcement by dropping the index.
	for _, col := range stmt.Columns {
		if col.UniqueName == "" {
			continue
		}
		idx := &query.CreateIndexStmt{Index: col.UniqueName, Table: stmt.Table, Columns: []string{col.Name}, Unique: true}
		if err := db.catalog.CreateIndex(idx); err != nil {
			return Result{}, cleanupOnError(fmt.Errorf("creating unique constraint %s: %w", col.UniqueName, err))
		}
	}
	// Table-level UNIQUE (col, ...) constraints are enforced via unique indexes.
	for i, cols := range stmt.UniqueConstraints {
		idxName := fmt.Sprintf("%s_uniq_%d", stmt.Table, i)
		idx := &query.CreateIndexStmt{Index: idxName, Table: stmt.Table, Columns: cols, Unique: true, IfNotExists: true}
		if err := db.catalog.CreateIndex(idx); err != nil {
			return Result{}, cleanupOnError(fmt.Errorf("creating unique constraint index: %w", err))
		}
	}
	for _, constraint := range stmt.NamedUniqueConstraints {
		idx := &query.CreateIndexStmt{Index: constraint.Name, Table: stmt.Table, Columns: constraint.Columns, Unique: true}
		if err := db.catalog.CreateIndex(idx); err != nil {
			return Result{}, cleanupOnError(fmt.Errorf("creating unique constraint %s: %w", constraint.Name, err))
		}
	}
	return Result{RowsAffected: 0}, nil
}

func (db *DB) executeCreateCollection(ctx context.Context, stmt *query.CreateCollectionStmt) (Result, error) {
	if err := db.catalog.CreateCollection(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateTableAsSelect implements CREATE TABLE ... AS SELECT (CTAS):
// materialize the query, infer column types, create the table, insert the rows.
func (db *DB) executeCreateTableAsSelect(ctx context.Context, stmt *query.CreateTableStmt) (Result, error) {
	rows, err := db.query(ctx, "", stmt.AsSelect, nil)
	if err != nil {
		return Result{}, err
	}
	cols := rows.Columns()
	var data [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			_ = rows.Close()
			return Result{}, err
		}
		data = append(data, vals)
	}
	if err := rows.Close(); err != nil {
		return Result{}, err
	}

	colDefs := make([]*query.ColumnDef, len(cols))
	for i, name := range cols {
		colDefs[i] = &query.ColumnDef{Name: name, Type: inferCTASColumnType(data, i)}
	}
	createStmt := &query.CreateTableStmt{Table: stmt.Table, IfNotExists: stmt.IfNotExists, Columns: colDefs}
	if err := db.catalog.CreateTable(createStmt); err != nil {
		return Result{}, err
	}

	inserted := int64(0)
	for _, row := range data {
		valExprs := make([]query.Expression, len(row))
		for j, v := range row {
			valExprs[j] = valueToLiteralExpr(v)
		}
		ins := &query.InsertStmt{Table: stmt.Table, Values: [][]query.Expression{valExprs}}
		if _, n, err := db.catalog.Insert(ctx, ins, nil); err != nil {
			return Result{}, fmt.Errorf("CTAS insert: %w", err)
		} else {
			inserted += n
		}
	}
	return Result{RowsAffected: inserted}, nil
}

// inferCTASColumnType picks a column type for CTAS from the materialized values.
func inferCTASColumnType(data [][]interface{}, col int) query.TokenType {
	allInt, allNum, sawVal := true, true, false
	for _, row := range data {
		if col >= len(row) || row[col] == nil {
			continue
		}
		sawVal = true
		switch row[col].(type) {
		case int, int64:
		case float64:
			allInt = false
		default:
			allInt = false
			allNum = false
		}
	}
	switch {
	case !sawVal:
		return query.TokenText
	case allInt:
		return query.TokenInteger
	case allNum:
		return query.TokenReal
	default:
		return query.TokenText
	}
}

func (db *DB) executeCreateForeignTable(ctx context.Context, stmt *query.CreateForeignTableStmt) (Result, error) {
	if err := db.catalog.CreateForeignTable(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeAlterTable executes ALTER TABLE
func (db *DB) executeAlterTable(ctx context.Context, stmt *query.AlterTableStmt) (Result, error) {
	switch stmt.Action {
	case "ADD":
		if err := db.catalog.AlterTableAddColumn(stmt); err != nil {
			return Result{}, err
		}
		if stmt.Column.UniqueName != "" {
			idx := &query.CreateIndexStmt{
				Index:   stmt.Column.UniqueName,
				Table:   stmt.Table,
				Columns: []string{stmt.Column.Name},
				Unique:  true,
			}
			if err := db.catalog.CreateIndex(idx); err != nil {
				// Mirror executeCreateTable's cleanupOnError: the column was
				// added, but its UNIQUE constraint index could not be created.
				// Leaving the column behind would report failure while having
				// mutated the schema.
				cleanup := &query.AlterTableStmt{Table: stmt.Table, Action: "DROP", Column: stmt.Column}
				if cleanupErr := db.catalog.AlterTableDropColumn(cleanup); cleanupErr != nil {
					return Result{}, fmt.Errorf("creating unique constraint %s: %w; cleanup failed: %v", stmt.Column.UniqueName, err, cleanupErr)
				}
				return Result{}, fmt.Errorf("creating unique constraint %s: %w", stmt.Column.UniqueName, err)
			}
		}
	case "DROP":
		if err := db.catalog.AlterTableDropColumn(stmt); err != nil {
			return Result{}, err
		}
	case "RENAME_TABLE":
		if err := db.catalog.AlterTableRename(stmt); err != nil {
			return Result{}, err
		}
	case "RENAME_COLUMN":
		if err := db.catalog.AlterTableRenameColumn(stmt); err != nil {
			return Result{}, err
		}
	case "ADD_CONSTRAINT":
		if strings.EqualFold(stmt.ConstraintType, "FOREIGN KEY") {
			if err := db.catalog.AlterTableAddForeignKeyConstraint(ctx, stmt); err != nil {
				return Result{}, err
			}
			break
		}
		if strings.EqualFold(stmt.ConstraintType, "CHECK") {
			if err := db.catalog.AlterTableAddCheckConstraint(stmt); err != nil {
				return Result{}, err
			}
			break
		}
		if !strings.EqualFold(stmt.ConstraintType, "UNIQUE") {
			return Result{}, fmt.Errorf("unsupported ALTER TABLE constraint type: %s", stmt.ConstraintType)
		}
		idx := &query.CreateIndexStmt{
			Index:   stmt.ConstraintName,
			Table:   stmt.Table,
			Columns: stmt.ConstraintColumns,
			Unique:  true,
		}
		if err := db.catalog.CreateIndex(idx); err != nil {
			return Result{}, err
		}
	case "DROP_CONSTRAINT":
		if err := db.catalog.DropTableConstraint(stmt.Table, stmt.ConstraintName); err != nil {
			return Result{}, err
		}
	case "ENABLE_RLS":
		if err := db.catalog.EnableRLSTable(stmt.Table); err != nil {
			return Result{}, err
		}
	default:
		return Result{}, fmt.Errorf("unsupported ALTER TABLE action: %s", stmt.Action)
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropTable executes DROP TABLE
func (db *DB) executeDropTable(ctx context.Context, stmt *query.DropTableStmt) (Result, error) {
	if err := db.catalog.DropTable(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

func (db *DB) executeDropCollection(ctx context.Context, stmt *query.DropCollectionStmt) (Result, error) {
	if err := db.catalog.DropCollection(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateIndex executes CREATE INDEX
func (db *DB) executeCreateIndex(ctx context.Context, stmt *query.CreateIndexStmt) (Result, error) {
	if err := db.catalog.CreateIndex(stmt); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateView executes CREATE VIEW
func (db *DB) executeCreateView(ctx context.Context, stmt *query.CreateViewStmt) (Result, error) {
	viewQuery, err := applyCreateViewColumnList(stmt)
	if err != nil {
		return Result{}, err
	}
	if stmt.OrReplace {
		var err error
		if stmt.Temporary {
			err = db.catalog.CreateOrReplaceTemporaryViewSQL(stmt.Name, viewQuery, stmt.RawSQL)
		} else {
			err = db.catalog.CreateOrReplaceViewSQL(stmt.Name, viewQuery, stmt.RawSQL)
		}
		if err != nil {
			return Result{}, err
		}
		return Result{RowsAffected: 0}, nil
	}
	if stmt.Temporary {
		err = db.catalog.CreateTemporaryViewSQL(stmt.Name, viewQuery, stmt.RawSQL)
	} else {
		err = db.catalog.CreateViewSQL(stmt.Name, viewQuery, stmt.RawSQL)
	}
	if err != nil {
		if stmt.IfNotExists {
			return Result{RowsAffected: 0}, nil
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

func applyCreateViewColumnList(stmt *query.CreateViewStmt) (*query.SelectStmt, error) {
	if stmt == nil || len(stmt.Columns) == 0 {
		if stmt == nil {
			return nil, fmt.Errorf("nil CREATE VIEW statement")
		}
		return stmt.Query, nil
	}
	if stmt.Query == nil {
		return nil, fmt.Errorf("CREATE VIEW %s has no query", stmt.Name)
	}
	if len(stmt.Columns) != len(stmt.Query.Columns) {
		return nil, fmt.Errorf("view column list has %d columns but query returns %d columns", len(stmt.Columns), len(stmt.Query.Columns))
	}
	viewQuery := *stmt.Query
	viewQuery.Columns = make([]query.Expression, len(stmt.Query.Columns))
	for i, col := range stmt.Query.Columns {
		if alias, ok := col.(*query.AliasExpr); ok {
			viewQuery.Columns[i] = &query.AliasExpr{Expr: alias.Expr, Alias: stmt.Columns[i]}
			continue
		}
		viewQuery.Columns[i] = &query.AliasExpr{Expr: col, Alias: stmt.Columns[i]}
	}
	return &viewQuery, nil
}

// executeDropView executes DROP VIEW
func (db *DB) executeDropView(ctx context.Context, stmt *query.DropViewStmt) (Result, error) {
	if err := db.catalog.DropView(stmt.Name); err != nil {
		if stmt.IfExists {
			return Result{RowsAffected: 0}, nil
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateTrigger executes CREATE TRIGGER
func (db *DB) executeCreateTrigger(ctx context.Context, stmt *query.CreateTriggerStmt) (Result, error) {
	if err := db.catalog.CreateTriggerSQL(stmt, stmt.RawSQL); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropTrigger executes DROP TRIGGER
func (db *DB) executeDropTrigger(ctx context.Context, stmt *query.DropTriggerStmt) (Result, error) {
	if err := db.catalog.DropTrigger(stmt.Name); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateProcedure executes CREATE PROCEDURE
func (db *DB) executeCreateProcedure(ctx context.Context, stmt *query.CreateProcedureStmt) (Result, error) {
	if err := db.catalog.CreateProcedureSQL(stmt, stmt.RawSQL); err != nil {
		if stmt.IfNotExists {
			return Result{}, nil // Silently succeed
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropProcedure executes DROP PROCEDURE
func (db *DB) executeDropProcedure(ctx context.Context, stmt *query.DropProcedureStmt) (Result, error) {
	if err := db.catalog.DropProcedure(stmt.Name); err != nil {
		if stmt.IfExists {
			return Result{}, nil // Silently succeed
		}
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreatePolicy executes CREATE POLICY for row-level security
func (db *DB) executeCreatePolicy(ctx context.Context, stmt *query.CreatePolicyStmt) (Result, error) {
	// Check if RLS is enabled
	if !db.catalog.IsRLSEnabled() {
		return Result{}, errors.New("row-level security is not enabled for this database")
	}
	if _, err := db.catalog.GetTable(stmt.Table); err != nil {
		return Result{}, err
	}

	// Convert Event string to PolicyType
	var policyType security.PolicyType
	switch toUpperFast(stmt.Event) {
	case "ALL":
		policyType = security.PolicyAll
	case "SELECT":
		policyType = security.PolicySelect
	case "INSERT":
		policyType = security.PolicyInsert
	case "UPDATE":
		policyType = security.PolicyUpdate
	case "DELETE":
		policyType = security.PolicyDelete
	default:
		return Result{}, fmt.Errorf("invalid policy event: %s", stmt.Event)
	}

	// Convert Expression to string for storage
	usingExpr := ""
	if stmt.Using != nil {
		usingExpr = expressionToString(stmt.Using)
		if usingExpr == "" {
			// Fail closed: an expression was given but cannot be rendered.
			// Silently storing "TRUE" would weaken a restrictive policy into
			// allow-all.
			return Result{}, fmt.Errorf("unsupported expression in USING clause: policy would be silently permissive")
		}
	}
	checkExpr := ""
	if stmt.WithCheck != nil {
		checkExpr = expressionToString(stmt.WithCheck)
		if checkExpr == "" {
			return Result{}, fmt.Errorf("unsupported expression in WITH CHECK clause: policy would be silently permissive")
		}
	}
	if usingExpr == "" {
		usingExpr = "TRUE" // Default to allowing all if no expression
	}

	// Create the policy
	policy := &security.Policy{
		Name:            stmt.Name,
		TableName:       stmt.Table,
		Type:            policyType,
		Expression:      usingExpr,
		CheckExpression: checkExpr,
		Restrictive:     !stmt.Permissive,
		Users:           nil, // Could be extracted from ForRoles
		Roles:           stmt.ForRoles,
		Enabled:         true,
	}

	if err := db.catalog.CreateRLSPolicy(policy); err != nil {
		return Result{}, err
	}

	return Result{RowsAffected: 0}, nil
}

// expressionToString converts an expression to its SQL string representation
func expressionToString(expr query.Expression) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *query.Identifier:
		return e.Name
	case *query.QualifiedIdentifier:
		if e.Table != "" {
			var sb strings.Builder
			sb.Grow(len(e.Table) + 1 + len(e.Column))
			sb.WriteString(e.Table)
			sb.WriteByte('.')
			sb.WriteString(e.Column)
			return sb.String()
		}
		return e.Column
	case *query.StringLiteral:
		var sb strings.Builder
		sb.Grow(len(e.Value) + 2)
		sb.WriteByte('\'')
		sb.WriteString(e.Value)
		sb.WriteByte('\'')
		return sb.String()
	case *query.NumberLiteral:
		return e.Raw
	case *query.BooleanLiteral:
		if e.Value {
			return "TRUE"
		}
		return "FALSE"
	case *query.NullLiteral:
		return "NULL"
	case *query.BinaryExpr:
		left := expressionToString(e.Left)
		right := expressionToString(e.Right)
		op := tokenTypeToString(e.Operator)
		var sb strings.Builder
		sb.Grow(len(left) + 1 + len(op) + 1 + len(right))
		sb.WriteString(left)
		sb.WriteByte(' ')
		sb.WriteString(op)
		sb.WriteByte(' ')
		sb.WriteString(right)
		return sb.String()
	case *query.UnaryExpr:
		op := tokenTypeToString(e.Operator)
		operand := expressionToString(e.Expr)
		var sb strings.Builder
		sb.Grow(len(op) + 1 + len(operand))
		sb.WriteString(op)
		sb.WriteByte(' ')
		sb.WriteString(operand)
		return sb.String()
	case *query.FunctionCall:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = expressionToString(arg)
		}
		joined := strings.Join(args, ", ")
		var sb strings.Builder
		sb.Grow(len(e.Name) + 1 + len(joined) + 1)
		sb.WriteString(e.Name)
		sb.WriteByte('(')
		sb.WriteString(joined)
		sb.WriteByte(')')
		return sb.String()
	case *query.InExpr:
		exprStr := expressionToString(e.Expr)
		items := make([]string, len(e.List))
		for i, item := range e.List {
			items[i] = expressionToString(item)
		}
		joined := strings.Join(items, ", ")
		var sb strings.Builder
		sb.Grow(len(exprStr) + 5 + len(joined) + 1)
		sb.WriteString(exprStr)
		sb.WriteString(" IN (")
		sb.WriteString(joined)
		sb.WriteByte(')')
		return sb.String()
	case *query.LikeExpr:
		exprStr := expressionToString(e.Expr)
		patternStr := expressionToString(e.Pattern)
		var sb strings.Builder
		if e.Not {
			sb.Grow(len(exprStr) + 10 + len(patternStr))
			sb.WriteString(exprStr)
			sb.WriteString(" NOT LIKE ")
		} else {
			sb.Grow(len(exprStr) + 6 + len(patternStr))
			sb.WriteString(exprStr)
			sb.WriteString(" LIKE ")
		}
		sb.WriteString(patternStr)
		if e.Escape != nil {
			// The rendered string is re-parsed by the RLS evaluator
			// (security/rls.go parses policy.Expression), so dropping ESCAPE
			// would silently change the policy's matching semantics.
			sb.WriteString(" ESCAPE ")
			sb.WriteString(expressionToString(e.Escape))
		}
		return sb.String()
	case *query.IsNullExpr:
		exprStr := expressionToString(e.Expr)
		var sb strings.Builder
		if e.Not {
			sb.Grow(len(exprStr) + 12)
			sb.WriteString(exprStr)
			sb.WriteString(" IS NOT NULL")
		} else {
			sb.Grow(len(exprStr) + 8)
			sb.WriteString(exprStr)
			sb.WriteString(" IS NULL")
		}
		return sb.String()
	case *query.BetweenExpr:
		exprStr := expressionToString(e.Expr)
		lowerStr := expressionToString(e.Lower)
		upperStr := expressionToString(e.Upper)
		keyword := " BETWEEN "
		if e.Not {
			keyword = " NOT BETWEEN "
		}
		var sb strings.Builder
		sb.Grow(len(exprStr) + len(keyword) + len(lowerStr) + 5 + len(upperStr))
		sb.WriteString(exprStr)
		sb.WriteString(keyword)
		sb.WriteString(lowerStr)
		sb.WriteString(" AND ")
		sb.WriteString(upperStr)
		return sb.String()
	default:
		return ""
	}
}

// tokenTypeToString converts a token type to its string representation
func tokenTypeToString(tok query.TokenType) string {
	switch tok {
	case query.TokenEq:
		return "="
	case query.TokenNeq:
		return "!="
	case query.TokenLt:
		return "<"
	case query.TokenGt:
		return ">"
	case query.TokenLte:
		return "<="
	case query.TokenGte:
		return ">="
	case query.TokenAnd:
		return "AND"
	case query.TokenOr:
		return "OR"
	case query.TokenNot:
		return "NOT"
	case query.TokenPlus:
		return "+"
	case query.TokenMinus:
		return "-"
	case query.TokenStar:
		return "*"
	case query.TokenSlash:
		return "/"
	default:
		return ""
	}
}

// executeDropPolicy executes DROP POLICY
func (db *DB) executeDropPolicy(ctx context.Context, stmt *query.DropPolicyStmt) (Result, error) {
	// Check if RLS is enabled
	if !db.catalog.IsRLSEnabled() {
		return Result{}, errors.New("row-level security is not enabled for this database")
	}

	tableName := stmt.Table
	if tableName == "" {
		return Result{}, errors.New("table name required for DROP POLICY")
	}

	if err := db.catalog.DropRLSPolicy(tableName, stmt.Name); err != nil {
		if stmt.IfExists && err.Error() == "security policy not found" {
			return Result{RowsAffected: 0}, nil
		}
		return Result{}, err
	}

	return Result{RowsAffected: 0}, nil
}

// executeVacuum executes VACUUM
func (db *DB) executeVacuum(ctx context.Context, stmt *query.VacuumStmt) (Result, error) {
	if err := db.catalog.Vacuum(db.options.Maintenance.AutoVacuumRetention); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeAnalyze executes ANALYZE
func (db *DB) executeAnalyze(ctx context.Context, stmt *query.AnalyzeStmt) (Result, error) {
	if stmt.Table == "" {
		tables := db.catalog.ListTables()
		for _, tableName := range tables {
			if err := db.catalog.Analyze(tableName); err != nil {
				return Result{}, err
			}
		}
	} else {
		if err := db.catalog.Analyze(stmt.Table); err != nil {
			return Result{}, err
		}
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateMaterializedView executes CREATE MATERIALIZED VIEW
func (db *DB) executeCreateMaterializedView(ctx context.Context, stmt *query.CreateMaterializedViewStmt) (Result, error) {
	if err := db.catalog.CreateMaterializedViewSQL(stmt.Name, stmt.Query, stmt.IfNotExists, stmt.RawSQL); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeDropMaterializedView executes DROP MATERIALIZED VIEW
func (db *DB) executeDropMaterializedView(ctx context.Context, stmt *query.DropMaterializedViewStmt) (Result, error) {
	if err := db.catalog.DropMaterializedView(stmt.Name, stmt.IfExists); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeRefreshMaterializedView executes REFRESH MATERIALIZED VIEW
func (db *DB) executeRefreshMaterializedView(ctx context.Context, stmt *query.RefreshMaterializedViewStmt) (Result, error) {
	if err := db.catalog.RefreshMaterializedView(stmt.Name); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateFTSIndex executes CREATE FULLTEXT INDEX
func (db *DB) executeCreateFTSIndex(ctx context.Context, stmt *query.CreateFTSIndexStmt) (Result, error) {
	if err := db.catalog.CreateFTSIndex(stmt.Index, stmt.Table, stmt.Columns); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}

// executeCreateVectorIndex executes CREATE VECTOR INDEX
func (db *DB) executeCreateVectorIndex(ctx context.Context, stmt *query.CreateVectorIndexStmt) (Result, error) {
	if err := db.catalog.CreateVectorIndex(stmt.Index, stmt.Table, stmt.Column); err != nil {
		return Result{}, err
	}
	return Result{RowsAffected: 0}, nil
}
