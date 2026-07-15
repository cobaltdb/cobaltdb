package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/catalog"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// executeCallProcedure executes CALL procedure_name(params)
func (db *DB) executeCallProcedure(ctx context.Context, stmt *query.CallProcedureStmt, args []interface{}) (Result, error) {
	result, _, _, _, err := db.runCallProcedure(ctx, stmt, args, false)
	return result, err
}

func (db *DB) queryCallProcedure(ctx context.Context, stmt *query.CallProcedureStmt, args []interface{}) (*Rows, error) {
	_, resultRows, columns, values, err := db.runCallProcedure(ctx, stmt, args, true)
	if err != nil {
		return nil, err
	}
	if resultRows != nil {
		return resultRows, nil
	}
	if len(columns) == 0 {
		return &Rows{columns: []string{}, rows: [][]interface{}{}}, nil
	}
	return &Rows{columns: columns, rows: [][]interface{}{values}}, nil
}

func (db *DB) runCallProcedure(ctx context.Context, stmt *query.CallProcedureStmt, args []interface{}, captureResultRows bool) (Result, *Rows, []string, []interface{}, error) {
	// Get the procedure from catalog
	proc, err := db.catalog.GetProcedure(stmt.Name)
	if err != nil {
		return Result{}, nil, nil, nil, err
	}

	// Map procedure parameters to call arguments
	paramMap := make(map[string]interface{})
	paramDefs := make(map[string]*query.ParamDef)
	for _, param := range proc.Params {
		paramDefs[param.Name] = param
	}
	if err := evaluateProcedureCallArgs(stmt, proc, args, paramMap); err != nil {
		return Result{}, nil, nil, nil, err
	}

	var totalRowsAffected int64
	var resultRows *Rows
	for _, bodyStmt := range proc.Body {
		if setStmt, ok := bodyStmt.(*query.SetVarStmt); ok && isProcedureOutputParam(setStmt.Variable, paramDefs) {
			val, err := evalProcedureSetValue(setStmt.Value, paramMap)
			if err != nil {
				return Result{}, nil, nil, nil, fmt.Errorf("setting OUT parameter %s: %w", setStmt.Variable, err)
			}
			paramMap[strings.TrimSpace(setStmt.Variable)] = val
			continue
		}
		if captureResultRows && isProcedureResultStatement(bodyStmt) {
			substitutedStmt := substituteParamsInStatement(bodyStmt, paramMap)
			rows, err := db.query(ctx, "", substitutedStmt, nil)
			if err != nil {
				return Result{}, nil, nil, nil, err
			}
			if resultRows != nil {
				_ = resultRows.Close()
			}
			resultRows = rows
			continue
		}
		result, err := db.executeWithParams(ctx, bodyStmt, paramMap)
		if err != nil {
			if resultRows != nil {
				_ = resultRows.Close()
			}
			return Result{}, nil, nil, nil, err
		}
		totalRowsAffected += result.RowsAffected
	}

	outColumns, outValues := procedureOutputValues(proc.Params, paramMap)
	return Result{RowsAffected: totalRowsAffected}, resultRows, outColumns, outValues, nil
}

func evaluateProcedureCallArgs(stmt *query.CallProcedureStmt, proc *query.CreateProcedureStmt, args []interface{}, paramMap map[string]interface{}) error {
	callArgs := stmt.Args
	if len(callArgs) == 0 && len(stmt.Params) > 0 {
		callArgs = make([]query.CallArg, len(stmt.Params))
		for i, paramExpr := range stmt.Params {
			callArgs[i] = query.CallArg{Expr: paramExpr}
		}
	}
	if len(callArgs) == 0 && len(args) > 0 {
		if len(args) != len(proc.Params) {
			return fmt.Errorf("procedure %s expects %d arguments, got %d", proc.Name, len(proc.Params), len(args))
		}
		for i, param := range proc.Params {
			paramMap[param.Name] = args[i]
		}
		return nil
	}
	if len(callArgs) != len(proc.Params) {
		return fmt.Errorf("procedure %s expects %d arguments, got %d", proc.Name, len(proc.Params), len(callArgs))
	}

	procParamByName := make(map[string]*query.ParamDef, len(proc.Params))
	for _, param := range proc.Params {
		procParamByName[strings.ToLower(param.Name)] = param
	}
	nextPositional := 0
	for _, callArg := range callArgs {
		if callArg.Expr == nil {
			return fmt.Errorf("procedure %s has nil call argument", proc.Name)
		}
		val, err := catalog.EvalExpression(callArg.Expr, args)
		if err != nil {
			return fmt.Errorf("evaluating procedure argument: %w", err)
		}
		if callArg.Name == "" {
			if nextPositional >= len(proc.Params) {
				return fmt.Errorf("procedure %s expects %d arguments, got too many positional arguments", proc.Name, len(proc.Params))
			}
			target := proc.Params[nextPositional]
			if _, exists := paramMap[target.Name]; exists {
				return fmt.Errorf("procedure %s argument %s assigned more than once", proc.Name, target.Name)
			}
			paramMap[target.Name] = val
			nextPositional++
			continue
		}

		target := procParamByName[strings.ToLower(strings.TrimSpace(callArg.Name))]
		if target == nil {
			return fmt.Errorf("procedure %s has no parameter named %s", proc.Name, callArg.Name)
		}
		if _, exists := paramMap[target.Name]; exists {
			return fmt.Errorf("procedure %s argument %s assigned more than once", proc.Name, target.Name)
		}
		paramMap[target.Name] = val
	}
	for _, param := range proc.Params {
		if _, exists := paramMap[param.Name]; !exists {
			return fmt.Errorf("procedure %s missing argument %s", proc.Name, param.Name)
		}
	}
	return nil
}

func isProcedureResultStatement(stmt query.Statement) bool {
	switch stmt.(type) {
	case *query.SelectStmt, *query.UnionStmt, *query.SelectStmtWithCTE,
		*query.ShowTablesStmt, *query.ShowCreateTableStmt, *query.ShowColumnsStmt,
		*query.ShowDatabasesStmt, *query.DescribeStmt, *query.ExplainStmt:
		return true
	default:
		return false
	}
}

func isProcedureOutputParam(name string, params map[string]*query.ParamDef) bool {
	param := params[strings.TrimSpace(name)]
	if param == nil {
		return false
	}
	return param.Mode == query.TokenOut || param.Mode == query.TokenInout
}

func procedureOutputValues(params []*query.ParamDef, paramMap map[string]interface{}) ([]string, []interface{}) {
	var columns []string
	var values []interface{}
	for _, param := range params {
		if param == nil || (param.Mode != query.TokenOut && param.Mode != query.TokenInout) {
			continue
		}
		columns = append(columns, param.Name)
		values = append(values, paramMap[param.Name])
	}
	return columns, values
}

func evalProcedureSetValue(valueSQL string, paramMap map[string]interface{}) (interface{}, error) {
	valueSQL = strings.TrimSpace(valueSQL)
	if valueSQL == "" {
		return nil, errors.New("empty SET value")
	}
	stmt, err := query.Parse("SELECT " + valueSQL)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*query.SelectStmt)
	if !ok || len(selectStmt.Columns) == 0 {
		return nil, fmt.Errorf("SET value did not parse as a scalar expression")
	}
	expr := substituteParamsInExpr(selectStmt.Columns[0], paramMap)
	return catalog.EvalExpression(expr, nil)
}

// executeWithParams executes a statement with parameter substitution
func (db *DB) executeWithParams(ctx context.Context, stmt query.Statement, paramMap map[string]interface{}) (Result, error) {
	// Substitute parameters in the statement
	substitutedStmt := substituteParamsInStatement(stmt, paramMap)

	// Execute the statement (with no additional args since params are substituted)
	return db.execute(ctx, "", substitutedStmt, nil)
}

// substituteParamsInStatement replaces parameter references with literal values
func substituteParamsInStatement(stmt query.Statement, paramMap map[string]interface{}) query.Statement {
	switch s := stmt.(type) {
	case *query.InsertStmt:
		newStmt := *s
		newStmt.Values = substituteParamsInValues(s.Values, paramMap)
		return &newStmt
	case *query.UpdateStmt:
		newStmt := *s
		newStmt.Set = substituteParamsInSetClauses(s.Set, paramMap)
		if s.Where != nil {
			newStmt.Where = substituteParamsInExpr(s.Where, paramMap)
		}
		return &newStmt
	case *query.DeleteStmt:
		newStmt := *s
		if s.Where != nil {
			newStmt.Where = substituteParamsInExpr(s.Where, paramMap)
		}
		return &newStmt
	case *query.SelectStmt:
		return substituteParamsInSelectStmt(s, paramMap)
	default:
		return stmt
	}
}

func substituteParamsInSelectStmt(stmt *query.SelectStmt, paramMap map[string]interface{}) *query.SelectStmt {
	if stmt == nil {
		return nil
	}
	newStmt := *stmt
	newStmt.Columns = substituteParamsInExprs(stmt.Columns, paramMap)
	newStmt.Where = substituteParamsInExpr(stmt.Where, paramMap)
	newStmt.GroupBy = substituteParamsInExprs(stmt.GroupBy, paramMap)
	newStmt.Having = substituteParamsInExpr(stmt.Having, paramMap)
	newStmt.Limit = substituteParamsInExpr(stmt.Limit, paramMap)
	newStmt.Offset = substituteParamsInExpr(stmt.Offset, paramMap)
	if stmt.OrderBy != nil {
		newStmt.OrderBy = make([]*query.OrderByExpr, len(stmt.OrderBy))
		for i, order := range stmt.OrderBy {
			if order == nil {
				continue
			}
			copied := *order
			copied.Expr = substituteParamsInExpr(order.Expr, paramMap)
			newStmt.OrderBy[i] = &copied
		}
	}
	return &newStmt
}

func substituteParamsInExprs(exprs []query.Expression, paramMap map[string]interface{}) []query.Expression {
	if exprs == nil {
		return nil
	}
	result := make([]query.Expression, len(exprs))
	for i, expr := range exprs {
		result[i] = substituteParamsInExpr(expr, paramMap)
	}
	return result
}

func substituteParamsInOrderBy(orderBy []*query.OrderByExpr, paramMap map[string]interface{}) []*query.OrderByExpr {
	if len(orderBy) == 0 {
		return nil
	}
	result := make([]*query.OrderByExpr, len(orderBy))
	for i, ob := range orderBy {
		if ob == nil {
			continue
		}
		copied := *ob
		copied.Expr = substituteParamsInExpr(ob.Expr, paramMap)
		result[i] = &copied
	}
	return result
}

// substituteParamsInValues replaces params in VALUES clause
func substituteParamsInValues(values [][]query.Expression, paramMap map[string]interface{}) [][]query.Expression {
	result := make([][]query.Expression, len(values))
	for i, row := range values {
		result[i] = make([]query.Expression, len(row))
		for j, expr := range row {
			result[i][j] = substituteParamsInExpr(expr, paramMap)
		}
	}
	return result
}

// substituteParamsInSetClauses replaces params in SET clause
func substituteParamsInSetClauses(set []*query.SetClause, paramMap map[string]interface{}) []*query.SetClause {
	result := make([]*query.SetClause, len(set))
	for i, clause := range set {
		newClause := *clause
		newClause.Value = substituteParamsInExpr(clause.Value, paramMap)
		result[i] = &newClause
	}
	return result
}

// substituteParamsInExpr replaces parameter identifiers with literal values
func substituteParamsInExpr(expr query.Expression, paramMap map[string]interface{}) query.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *query.Identifier:
		if val, ok := paramMap[e.Name]; ok {
			switch v := val.(type) {
			case string:
				return &query.StringLiteral{Value: v}
			case int:
				return &query.NumberLiteral{Value: float64(v)}
			case int64:
				return &query.NumberLiteral{Value: float64(v)}
			case float64:
				return &query.NumberLiteral{Value: v}
			case bool:
				return &query.BooleanLiteral{Value: v}
			case nil:
				return &query.NullLiteral{}
			default:
				return &query.StringLiteral{Value: catalog.ValueToStringKey(v)}
			}
		}
		return expr
	case *query.BinaryExpr:
		return &query.BinaryExpr{
			Left:     substituteParamsInExpr(e.Left, paramMap),
			Operator: e.Operator,
			Right:    substituteParamsInExpr(e.Right, paramMap),
		}
	case *query.UnaryExpr:
		return &query.UnaryExpr{
			Operator: e.Operator,
			Expr:     substituteParamsInExpr(e.Expr, paramMap),
		}
	case *query.FunctionCall:
		newArgs := make([]query.Expression, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = substituteParamsInExpr(arg, paramMap)
		}
		return &query.FunctionCall{
			Name:     e.Name,
			Args:     newArgs,
			Distinct: e.Distinct,
			OrderBy:  substituteParamsInOrderBy(e.OrderBy, paramMap),
			Filter:   substituteParamsInExpr(e.Filter, paramMap),
		}
	case *query.WindowExpr:
		newArgs := make([]query.Expression, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = substituteParamsInExpr(arg, paramMap)
		}
		partitionBy := make([]query.Expression, len(e.PartitionBy))
		for i, expr := range e.PartitionBy {
			partitionBy[i] = substituteParamsInExpr(expr, paramMap)
		}
		return &query.WindowExpr{
			Function:    e.Function,
			Args:        newArgs,
			Filter:      substituteParamsInExpr(e.Filter, paramMap),
			PartitionBy: partitionBy,
			OrderBy:     substituteParamsInOrderBy(e.OrderBy, paramMap),
			Frame:       e.Frame,
		}
	case *query.CaseExpr:
		newCase := &query.CaseExpr{}
		if e.Expr != nil {
			newCase.Expr = substituteParamsInExpr(e.Expr, paramMap)
		}
		newCase.Whens = make([]*query.WhenClause, len(e.Whens))
		for i, when := range e.Whens {
			newCase.Whens[i] = &query.WhenClause{
				Condition: substituteParamsInExpr(when.Condition, paramMap),
				Result:    substituteParamsInExpr(when.Result, paramMap),
			}
		}
		if e.Else != nil {
			newCase.Else = substituteParamsInExpr(e.Else, paramMap)
		}
		return newCase
	case *query.BetweenExpr:
		return &query.BetweenExpr{
			Expr:  substituteParamsInExpr(e.Expr, paramMap),
			Lower: substituteParamsInExpr(e.Lower, paramMap),
			Upper: substituteParamsInExpr(e.Upper, paramMap),
			Not:   e.Not,
		}
	case *query.InExpr:
		newList := make([]query.Expression, len(e.List))
		for i, item := range e.List {
			newList[i] = substituteParamsInExpr(item, paramMap)
		}
		return &query.InExpr{
			Expr:     substituteParamsInExpr(e.Expr, paramMap),
			List:     newList,
			Not:      e.Not,
			Subquery: e.Subquery,
		}
	case *query.IsNullExpr:
		return &query.IsNullExpr{
			Expr: substituteParamsInExpr(e.Expr, paramMap),
			Not:  e.Not,
		}
	case *query.CastExpr:
		return &query.CastExpr{
			Expr:     substituteParamsInExpr(e.Expr, paramMap),
			DataType: e.DataType,
		}
	case *query.LikeExpr:
		return &query.LikeExpr{
			Expr:    substituteParamsInExpr(e.Expr, paramMap),
			Pattern: substituteParamsInExpr(e.Pattern, paramMap),
			Not:     e.Not,
			Escape:  substituteParamsInExpr(e.Escape, paramMap),
		}
	case *query.AliasExpr:
		return &query.AliasExpr{
			Expr:  substituteParamsInExpr(e.Expr, paramMap),
			Alias: e.Alias,
		}
	default:
		return expr
	}
}
