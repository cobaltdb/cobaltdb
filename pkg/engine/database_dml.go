package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// executeInsert executes INSERT
func (db *DB) executeInsert(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (Result, error) {
	if stmt.OnConflict != nil && stmt.OnConflict.DoUpdate != nil {
		return db.executeUpsert(ctx, stmt, args)
	}
	lastInsertID, rowsAffected, err := db.catalog.Insert(ctx, stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeUpsert implements INSERT ... ON CONFLICT (...) DO UPDATE SET ... by
// attempting a per-row insert and, on a unique/primary-key conflict, applying
// the UPDATE assignments to the conflicting row. Safe under the catalog's
// single-writer model (the conflict check-then-act holds while we run).
func (db *DB) executeUpsert(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (Result, error) {
	table, err := db.catalog.GetTable(stmt.Table)
	if err != nil {
		return Result{}, err
	}

	// Source rows come from VALUES, or from a materialized INSERT ... SELECT.
	valueRows := stmt.Values
	if stmt.Select != nil {
		rows, qerr := db.query(ctx, "", stmt.Select, args)
		if qerr != nil {
			return Result{}, qerr
		}
		cols := rows.Columns()
		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if scanErr := rows.Scan(ptrs...); scanErr != nil {
				_ = rows.Close()
				return Result{}, scanErr
			}
			exprs := make([]query.Expression, len(vals))
			for j, v := range vals {
				exprs[j] = valueToLiteralExpr(v)
			}
			valueRows = append(valueRows, exprs)
		}
		if cerr := rows.Close(); cerr != nil {
			return Result{}, cerr
		}
	}

	// Resolve conflict target columns. Explicit ON CONFLICT targets use that
	// one target; MySQL-style ON DUPLICATE KEY UPDATE has no target, so try the
	// primary key followed by known UNIQUE constraints/indexes.
	conflictTargets := [][]string{stmt.OnConflict.Columns}
	if len(stmt.OnConflict.Columns) == 0 {
		conflictTargets = db.upsertConflictTargets(stmt.Table, table)
	}
	if len(conflictTargets) == 0 {
		return Result{}, fmt.Errorf("ON CONFLICT DO UPDATE requires a conflict target, primary key, or unique key")
	}

	// Map column name -> position within each inserted value row.
	colPos := make(map[string]int)
	if len(stmt.Columns) > 0 {
		for i, c := range stmt.Columns {
			colPos[strings.ToLower(c)] = i
		}
	} else {
		for i, c := range table.Columns {
			colPos[strings.ToLower(c.Name)] = i
		}
	}

	var result Result
	for _, row := range valueRows {
		single := &query.InsertStmt{Table: stmt.Table, Columns: stmt.Columns, Values: [][]query.Expression{row}}
		lastID, n, insErr := db.catalog.Insert(ctx, single, args)
		if insErr == nil {
			result.RowsAffected += n
			result.LastInsertID = lastID
			continue
		}
		if !isUniqueConflictError(insErr) {
			return Result{}, insErr
		}

		updated := false
		for _, conflictCols := range conflictTargets {
			// Build WHERE matching the conflict target to this row's values,
			// reusing the row's own value expressions directly.
			var where query.Expression
			missingTarget := ""
			for _, cc := range conflictCols {
				pos, ok := colPos[strings.ToLower(cc)]
				if !ok || pos >= len(row) {
					missingTarget = cc
					break
				}
				eq := &query.BinaryExpr{Left: &query.Identifier{Name: cc}, Operator: query.TokenEq, Right: row[pos]}
				if where == nil {
					where = eq
				} else {
					where = &query.BinaryExpr{Left: where, Operator: query.TokenAnd, Right: eq}
				}
			}
			if missingTarget != "" {
				if len(stmt.OnConflict.Columns) > 0 {
					return Result{}, fmt.Errorf("ON CONFLICT target column %q not present in inserted values", missingTarget)
				}
				continue
			}

			updateSet, substErr := substituteUpsertValuesInSetClauses(stmt.OnConflict.DoUpdate, colPos, row)
			if substErr != nil {
				return Result{}, substErr
			}
			upd := &query.UpdateStmt{Table: stmt.Table, Set: updateSet, Where: where}
			_, n, updErr := db.catalog.Update(ctx, upd, args)
			if updErr != nil {
				return Result{}, updErr
			}
			if n > 0 {
				result.RowsAffected += n
				updated = true
				break
			}
		}
		if !updated {
			return Result{}, insErr
		}
	}
	return result, nil
}

func (db *DB) upsertConflictTargets(tableName string, table *catalog.TableDef) [][]string {
	var targets [][]string
	seen := make(map[string]bool)
	add := func(cols []string) {
		if len(cols) == 0 {
			return
		}
		keyParts := make([]string, len(cols))
		for i, col := range cols {
			keyParts[i] = strings.ToLower(col)
		}
		key := strings.Join(keyParts, "\x00")
		if seen[key] {
			return
		}
		seen[key] = true
		targets = append(targets, append([]string(nil), cols...))
	}

	add(table.PrimaryKey)
	for _, col := range table.Columns {
		if col.Unique {
			add([]string{col.Name})
		}
	}
	for _, idx := range db.catalog.GetTableIndexes(tableName) {
		if idx.Unique {
			add(idx.Columns)
		}
	}
	return targets
}

func substituteUpsertValuesInSetClauses(set []*query.SetClause, colPos map[string]int, row []query.Expression) ([]*query.SetClause, error) {
	result := make([]*query.SetClause, len(set))
	for i, clause := range set {
		newClause := *clause
		value, err := substituteUpsertValuesExpr(clause.Value, colPos, row)
		if err != nil {
			return nil, err
		}
		newClause.Value = value
		result[i] = &newClause
	}
	return result, nil
}

func substituteUpsertValuesExpr(expr query.Expression, colPos map[string]int, row []query.Expression) (query.Expression, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *query.FunctionCall:
		if strings.EqualFold(e.Name, "VALUES") {
			if len(e.Args) != 1 {
				return nil, fmt.Errorf("VALUES() in ON DUPLICATE KEY UPDATE requires one column argument")
			}
			col, ok := upsertValuesColumnName(e.Args[0])
			if !ok {
				return nil, fmt.Errorf("VALUES() in ON DUPLICATE KEY UPDATE requires a column argument")
			}
			pos, ok := colPos[strings.ToLower(col)]
			if !ok || pos >= len(row) {
				return nil, fmt.Errorf("VALUES(%s) column not present in inserted values", col)
			}
			return query.CloneExpression(row[pos]), nil
		}
		args := make([]query.Expression, len(e.Args))
		for i, arg := range e.Args {
			v, err := substituteUpsertValuesExpr(arg, colPos, row)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}
		orderBy, err := substituteUpsertValuesOrderBy(e.OrderBy, colPos, row)
		if err != nil {
			return nil, err
		}
		filter, err := substituteUpsertValuesExpr(e.Filter, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.FunctionCall{Name: e.Name, Args: args, Distinct: e.Distinct, OrderBy: orderBy, Filter: filter}, nil
	case *query.BinaryExpr:
		left, err := substituteUpsertValuesExpr(e.Left, colPos, row)
		if err != nil {
			return nil, err
		}
		right, err := substituteUpsertValuesExpr(e.Right, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.BinaryExpr{Left: left, Operator: e.Operator, Right: right}, nil
	case *query.UnaryExpr:
		v, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.UnaryExpr{Operator: e.Operator, Expr: v}, nil
	case *query.CaseExpr:
		newCase := &query.CaseExpr{}
		var err error
		if e.Expr != nil {
			newCase.Expr, err = substituteUpsertValuesExpr(e.Expr, colPos, row)
			if err != nil {
				return nil, err
			}
		}
		newCase.Whens = make([]*query.WhenClause, len(e.Whens))
		for i, when := range e.Whens {
			cond, err := substituteUpsertValuesExpr(when.Condition, colPos, row)
			if err != nil {
				return nil, err
			}
			res, err := substituteUpsertValuesExpr(when.Result, colPos, row)
			if err != nil {
				return nil, err
			}
			newCase.Whens[i] = &query.WhenClause{Condition: cond, Result: res}
		}
		if e.Else != nil {
			newCase.Else, err = substituteUpsertValuesExpr(e.Else, colPos, row)
			if err != nil {
				return nil, err
			}
		}
		return newCase, nil
	case *query.BetweenExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		lower, err := substituteUpsertValuesExpr(e.Lower, colPos, row)
		if err != nil {
			return nil, err
		}
		upper, err := substituteUpsertValuesExpr(e.Upper, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.BetweenExpr{Expr: ex, Lower: lower, Upper: upper, Not: e.Not}, nil
	case *query.InExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		list := make([]query.Expression, len(e.List))
		for i, item := range e.List {
			v, err := substituteUpsertValuesExpr(item, colPos, row)
			if err != nil {
				return nil, err
			}
			list[i] = v
		}
		return &query.InExpr{Expr: ex, List: list, Not: e.Not, Subquery: e.Subquery}, nil
	case *query.LikeExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		pattern, err := substituteUpsertValuesExpr(e.Pattern, colPos, row)
		if err != nil {
			return nil, err
		}
		escape, err := substituteUpsertValuesExpr(e.Escape, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.LikeExpr{Expr: ex, Pattern: pattern, Not: e.Not, Escape: escape}, nil
	case *query.IsNullExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.IsNullExpr{Expr: ex, Not: e.Not}, nil
	case *query.CastExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.CastExpr{Expr: ex, DataType: e.DataType}, nil
	case *query.AliasExpr:
		ex, err := substituteUpsertValuesExpr(e.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.AliasExpr{Expr: ex, Alias: e.Alias}, nil
	case *query.JSONPathExpr:
		col, err := substituteUpsertValuesExpr(e.Column, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.JSONPathExpr{Column: col, Path: e.Path, AsText: e.AsText}, nil
	case *query.JSONContainsExpr:
		col, err := substituteUpsertValuesExpr(e.Column, colPos, row)
		if err != nil {
			return nil, err
		}
		val, err := substituteUpsertValuesExpr(e.Value, colPos, row)
		if err != nil {
			return nil, err
		}
		return &query.JSONContainsExpr{Column: col, Value: val}, nil
	default:
		return query.CloneExpression(expr), nil
	}
}

func substituteUpsertValuesOrderBy(orderBy []*query.OrderByExpr, colPos map[string]int, row []query.Expression) ([]*query.OrderByExpr, error) {
	if len(orderBy) == 0 {
		return nil, nil
	}
	out := make([]*query.OrderByExpr, len(orderBy))
	for i, ob := range orderBy {
		if ob == nil {
			continue
		}
		expr, err := substituteUpsertValuesExpr(ob.Expr, colPos, row)
		if err != nil {
			return nil, err
		}
		copied := *ob
		copied.Expr = expr
		out[i] = &copied
	}
	return out, nil
}

func upsertValuesColumnName(expr query.Expression) (string, bool) {
	switch e := expr.(type) {
	case *query.Identifier:
		return e.Name, true
	case *query.QualifiedIdentifier:
		return e.Column, true
	case *query.ColumnRef:
		return e.Column, e.Column != ""
	default:
		return "", false
	}
}

// isUniqueConflictError reports whether err is a primary-key/unique violation
// from the insert path (used to trigger ON CONFLICT DO UPDATE).
func isUniqueConflictError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "UNIQUE constraint failed") || strings.Contains(s, "duplicate primary key")
}

// executeUpdate executes UPDATE
func (db *DB) executeUpdate(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (Result, error) {
	lastInsertID, rowsAffected, err := db.catalog.Update(ctx, stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeDelete executes DELETE
func (db *DB) executeDelete(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (Result, error) {
	lastInsertID, rowsAffected, err := db.catalog.Delete(ctx, stmt, args)
	if err != nil {
		return Result{}, err
	}
	return Result{LastInsertID: lastInsertID, RowsAffected: rowsAffected}, nil
}

// executeInsertReturning executes INSERT with RETURNING clause
func (db *DB) executeInsertReturning(ctx context.Context, stmt *query.InsertStmt, args []interface{}) (*Rows, error) {
	// Capture this statement's RETURNING results with statement affinity:
	// reading the catalog-global slot after execution returns another
	// concurrent statement's rows whenever one interleaves between this
	// statement's write and its read.
	capture := &catalog.ReturningCapture{}
	ctx = catalog.WithReturningCapture(ctx, capture)
	if _, _, err := db.catalog.Insert(ctx, stmt, args); err != nil {
		return nil, err
	}
	returningRows, returningCols := capture.Results()

	if len(returningRows) == 0 {
		return &Rows{rows: nil, columns: returningCols}, nil
	}

	return &Rows{rows: returningRows, columns: returningCols}, nil
}

// executeUpdateReturning executes UPDATE with RETURNING clause
func (db *DB) executeUpdateReturning(ctx context.Context, stmt *query.UpdateStmt, args []interface{}) (*Rows, error) {
	// Statement-affinity capture — see executeInsertReturning.
	capture := &catalog.ReturningCapture{}
	ctx = catalog.WithReturningCapture(ctx, capture)
	if _, _, err := db.catalog.Update(ctx, stmt, args); err != nil {
		return nil, err
	}
	returningRows, returningCols := capture.Results()

	if len(returningRows) == 0 {
		return &Rows{rows: nil, columns: returningCols}, nil
	}

	return &Rows{rows: returningRows, columns: returningCols}, nil
}

// executeDeleteReturning executes DELETE with RETURNING clause
func (db *DB) executeDeleteReturning(ctx context.Context, stmt *query.DeleteStmt, args []interface{}) (*Rows, error) {
	// Statement-affinity capture — see executeInsertReturning.
	capture := &catalog.ReturningCapture{}
	ctx = catalog.WithReturningCapture(ctx, capture)
	if _, _, err := db.catalog.Delete(ctx, stmt, args); err != nil {
		return nil, err
	}
	returningRows, returningCols := capture.Results()

	if len(returningRows) == 0 {
		return &Rows{rows: nil, columns: returningCols}, nil
	}

	return &Rows{rows: returningRows, columns: returningCols}, nil
}
