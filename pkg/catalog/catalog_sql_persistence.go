package catalog

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func decodePersistedSQLDef(keyStr string, prefix string, value []byte) persistedSQLDef {
	var def persistedSQLDef
	if err := json.Unmarshal(value, &def); err != nil {
		return persistedSQLDef{}
	}
	if def.Name == "" {
		def.Name = strings.TrimPrefix(keyStr, prefix)
	}
	def.SQL = strings.TrimSpace(def.SQL)
	return def
}

func createViewSQL(name string, stmt *query.SelectStmt) string {
	return "CREATE VIEW " + name + " AS " + selectStmtToSQL(stmt)
}

func createTriggerSQL(stmt *query.CreateTriggerStmt) string {
	if stmt == nil {
		return ""
	}
	var parts []string
	parts = append(parts, "CREATE TRIGGER", stmt.Name, stmt.Time, stmt.Event, "ON", stmt.Table)
	if stmt.Condition != nil {
		parts = append(parts, "WHEN", exprToSQL(stmt.Condition))
	}
	body := make([]string, 0, len(stmt.Body))
	for _, bodyStmt := range stmt.Body {
		sql := statementToSQL(bodyStmt)
		if sql != "" {
			body = append(body, sql)
		}
	}
	return strings.Join(parts, " ") + " BEGIN " + strings.Join(body, "; ") + "; END"
}

func createProcedureSQL(stmt *query.CreateProcedureStmt) string {
	if stmt == nil {
		return ""
	}
	params := make([]string, 0, len(stmt.Params))
	for _, param := range stmt.Params {
		if param == nil {
			continue
		}
		params = append(params, param.Name+" "+procedureParamTypeSQL(param.Type))
	}
	body := make([]string, 0, len(stmt.Body))
	for _, bodyStmt := range stmt.Body {
		sql := statementToSQL(bodyStmt)
		if sql != "" {
			body = append(body, sql)
		}
	}
	return "CREATE PROCEDURE " + stmt.Name + "(" + strings.Join(params, ", ") + ") BEGIN " + strings.Join(body, "; ") + "; END"
}

func procedureParamTypeSQL(tt query.TokenType) string {
	switch tt {
	case query.TokenInteger:
		return "INTEGER"
	case query.TokenText:
		return "TEXT"
	case query.TokenReal:
		return "REAL"
	case query.TokenBlob:
		return "BLOB"
	case query.TokenBoolean:
		return "BOOLEAN"
	case query.TokenJSON:
		return "JSON"
	case query.TokenDate:
		return "DATE"
	case query.TokenTimestamp:
		return "TIMESTAMP"
	case query.TokenDatetime:
		return "DATETIME"
	default:
		return "TEXT"
	}
}

func statementToSQL(stmt query.Statement) string {
	switch s := stmt.(type) {
	case *query.SelectStmt:
		return selectStmtToSQL(s)
	case *query.InsertStmt:
		return insertStmtToSQL(s)
	case *query.UpdateStmt:
		return updateStmtToSQL(s)
	case *query.DeleteStmt:
		return deleteStmtToSQL(s)
	default:
		return ""
	}
}

func selectStmtToSQL(stmt *query.SelectStmt) string {
	if stmt == nil {
		return "SELECT 1"
	}
	var parts []string
	parts = append(parts, "SELECT")
	if stmt.Distinct {
		parts = append(parts, "DISTINCT")
	}
	parts = append(parts, exprListSQL(stmt.Columns))
	if stmt.From != nil {
		parts = append(parts, "FROM", tableRefToSQL(stmt.From))
	}
	for _, join := range stmt.Joins {
		parts = append(parts, joinClauseToSQL(join))
	}
	if stmt.Where != nil {
		parts = append(parts, "WHERE", exprToSQL(stmt.Where))
	}
	if len(stmt.GroupBy) > 0 {
		parts = append(parts, "GROUP BY", exprListSQL(stmt.GroupBy))
	}
	if stmt.Having != nil {
		parts = append(parts, "HAVING", exprToSQL(stmt.Having))
	}
	if len(stmt.OrderBy) > 0 {
		order := make([]string, 0, len(stmt.OrderBy))
		for _, ob := range stmt.OrderBy {
			if ob == nil {
				continue
			}
			item := exprToSQL(ob.Expr)
			if ob.Desc {
				item += " DESC"
			}
			order = append(order, item)
		}
		parts = append(parts, "ORDER BY", strings.Join(order, ", "))
	}
	if stmt.Limit != nil {
		parts = append(parts, "LIMIT", exprToSQL(stmt.Limit))
	}
	if stmt.Offset != nil {
		parts = append(parts, "OFFSET", exprToSQL(stmt.Offset))
	}
	return strings.Join(parts, " ")
}

func insertStmtToSQL(stmt *query.InsertStmt) string {
	if stmt == nil {
		return ""
	}
	var parts []string
	parts = append(parts, "INSERT")
	switch stmt.ConflictAction {
	case query.ConflictReplace:
		parts = append(parts, "OR REPLACE")
	case query.ConflictIgnore:
		parts = append(parts, "OR IGNORE")
	}
	parts = append(parts, "INTO", stmt.Table)
	if len(stmt.Columns) > 0 {
		parts = append(parts, "("+strings.Join(stmt.Columns, ", ")+")")
	}
	if stmt.Select != nil {
		parts = append(parts, selectStmtToSQL(stmt.Select))
	} else {
		rows := make([]string, 0, len(stmt.Values))
		for _, row := range stmt.Values {
			rows = append(rows, "("+exprListSQL(row)+")")
		}
		parts = append(parts, "VALUES", strings.Join(rows, ", "))
	}
	if len(stmt.Returning) > 0 {
		parts = append(parts, "RETURNING", exprListSQL(stmt.Returning))
	}
	return strings.Join(parts, " ")
}

func updateStmtToSQL(stmt *query.UpdateStmt) string {
	if stmt == nil {
		return ""
	}
	sets := make([]string, 0, len(stmt.Set))
	for _, set := range stmt.Set {
		if set == nil {
			continue
		}
		sets = append(sets, set.Column+" = "+exprToSQL(set.Value))
	}
	parts := []string{"UPDATE", stmt.Table, "SET", strings.Join(sets, ", ")}
	if stmt.From != nil {
		parts = append(parts, "FROM", tableRefToSQL(stmt.From))
	}
	for _, join := range stmt.Joins {
		parts = append(parts, joinClauseToSQL(join))
	}
	if stmt.Where != nil {
		parts = append(parts, "WHERE", exprToSQL(stmt.Where))
	}
	if len(stmt.Returning) > 0 {
		parts = append(parts, "RETURNING", exprListSQL(stmt.Returning))
	}
	return strings.Join(parts, " ")
}

func deleteStmtToSQL(stmt *query.DeleteStmt) string {
	if stmt == nil {
		return ""
	}
	parts := []string{"DELETE FROM", stmt.Table}
	if stmt.Alias != "" {
		parts = append(parts, stmt.Alias)
	}
	if len(stmt.Using) > 0 {
		using := make([]string, 0, len(stmt.Using))
		for _, ref := range stmt.Using {
			using = append(using, tableRefToSQL(ref))
		}
		parts = append(parts, "USING", strings.Join(using, ", "))
	}
	if stmt.Where != nil {
		parts = append(parts, "WHERE", exprToSQL(stmt.Where))
	}
	if len(stmt.Returning) > 0 {
		parts = append(parts, "RETURNING", exprListSQL(stmt.Returning))
	}
	return strings.Join(parts, " ")
}

func tableRefToSQL(ref *query.TableRef) string {
	if ref == nil {
		return ""
	}
	var sql string
	switch {
	case ref.Subquery != nil:
		sql = "(" + selectStmtToSQL(ref.Subquery) + ")"
	case ref.SubqueryStmt != nil:
		sql = "(" + statementToSQL(ref.SubqueryStmt) + ")"
	default:
		sql = ref.Name
	}
	if ref.Alias != "" {
		sql += " AS " + ref.Alias
	}
	return sql
}

func joinClauseToSQL(join *query.JoinClause) string {
	if join == nil {
		return ""
	}
	joinType := "JOIN"
	if join.Natural {
		joinType = "NATURAL " + joinType
	}
	switch join.Type {
	case query.TokenInner:
		joinType = "INNER JOIN"
	case query.TokenLeft:
		joinType = "LEFT JOIN"
	case query.TokenRight:
		joinType = "RIGHT JOIN"
	case query.TokenFull:
		joinType = "FULL JOIN"
	case query.TokenCross:
		joinType = "CROSS JOIN"
	}
	parts := []string{joinType, tableRefToSQL(join.Table)}
	if join.Condition != nil {
		parts = append(parts, "ON", exprToSQL(join.Condition))
	}
	if len(join.Using) > 0 {
		parts = append(parts, "USING ("+strings.Join(join.Using, ", ")+")")
	}
	return strings.Join(parts, " ")
}

func exprListSQL(exprs []query.Expression) string {
	if len(exprs) == 0 {
		return "*"
	}
	parts := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		parts = append(parts, exprToSQL(expr))
	}
	return strings.Join(parts, ", ")
}

func numberLiteralSQL(n *query.NumberLiteral) string {
	if n.Raw != "" {
		return n.Raw
	}
	if n.Value == float64(int64(n.Value)) {
		return strconv.FormatInt(int64(n.Value), 10)
	}
	return fmt.Sprintf("%v", n.Value)
}
