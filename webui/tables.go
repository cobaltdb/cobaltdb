package main

import (
	"fmt"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// extractTableRefs returns the set of base-table names a statement reads from or
// writes to, lower-cased. It is a security boundary for per-token table
// allow-listing, so it is deliberately FAIL-CLOSED: any statement shape it does
// not fully understand returns an error, and the caller must then deny the
// request rather than run an unchecked query.
//
// CTE names declared by a WITH clause are NOT base tables — they are tracked and
// excluded so that `WITH x AS (SELECT ... FROM real) SELECT * FROM x` is gated on
// `real`, not on the alias `x`.
func extractTableRefs(sql string) ([]string, error) {
	stmt, err := query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("cannot parse statement for table allow-list check: %w", err)
	}
	acc := &tableAccumulator{tables: map[string]struct{}{}}
	if err := acc.walkStatement(stmt, map[string]struct{}{}); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(acc.tables))
	for t := range acc.tables {
		out = append(out, t)
	}
	return out, nil
}

type tableAccumulator struct {
	tables map[string]struct{}
}

func (a *tableAccumulator) add(name string) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name != "" {
		a.tables[name] = struct{}{}
	}
}

// walkStatement dispatches over every statement type. cteScope holds the set of
// CTE names visible in the current scope (already lower-cased).
func (a *tableAccumulator) walkStatement(stmt query.Statement, cteScope map[string]struct{}) error {
	switch s := stmt.(type) {
	case *query.SelectStmt:
		return a.walkSelect(s, cteScope)
	case *query.UnionStmt:
		return a.walkUnion(s, cteScope)
	case *query.SelectStmtWithCTE:
		return a.walkSelectWithCTE(s, cteScope)
	case *query.InsertStmt:
		a.add(s.Table)
		if s.Select != nil {
			if err := a.walkSelect(s.Select, cteScope); err != nil {
				return err
			}
		}
		for _, row := range s.Values {
			for _, e := range row {
				if err := a.walkExpr(e, cteScope); err != nil {
					return err
				}
			}
		}
		return nil
	case *query.UpdateStmt:
		a.add(s.Table)
		if s.From != nil {
			if err := a.walkTableRef(s.From, cteScope); err != nil {
				return err
			}
		}
		for _, j := range s.Joins {
			if j != nil && j.Table != nil {
				if err := a.walkTableRef(j.Table, cteScope); err != nil {
					return err
				}
			}
		}
		for _, sc := range s.Set {
			if sc != nil {
				if err := a.walkExpr(sc.Value, cteScope); err != nil {
					return err
				}
			}
		}
		return a.walkExpr(s.Where, cteScope)
	case *query.DeleteStmt:
		a.add(s.Table)
		for _, t := range s.Using {
			if err := a.walkTableRef(t, cteScope); err != nil {
				return err
			}
		}
		return a.walkExpr(s.Where, cteScope)
	default:
		// DDL and any other statement type is not allow-list-checkable at the
		// table level here; the RBAC layer already restricts DDL to admins, and
		// admins are exempt from allow-lists. Returning an error keeps this path
		// fail-closed for any non-admin who somehow reaches it.
		return fmt.Errorf("statement type %T is not supported for table allow-listing", stmt)
	}
}

func (a *tableAccumulator) walkSelectWithCTE(s *query.SelectStmtWithCTE, cteScope map[string]struct{}) error {
	if s == nil {
		return fmt.Errorf("nil SELECT-with-CTE")
	}
	// Extend the CTE scope with the names declared here; a recursive CTE may
	// reference itself, so register names before walking their bodies.
	inner := cloneScope(cteScope)
	for _, cte := range s.CTEs {
		if cte == nil {
			continue
		}
		inner[strings.ToLower(strings.TrimSpace(cte.Name))] = struct{}{}
	}
	for _, cte := range s.CTEs {
		if cte == nil || cte.Query == nil {
			continue
		}
		if err := a.walkStatement(cte.Query, inner); err != nil {
			return err
		}
	}
	return a.walkSelect(s.Select, inner)
}

func (a *tableAccumulator) walkUnion(s *query.UnionStmt, cteScope map[string]struct{}) error {
	if s == nil {
		return fmt.Errorf("nil UNION")
	}
	if s.Left != nil {
		if err := a.walkStatement(s.Left, cteScope); err != nil {
			return err
		}
	}
	if s.Right != nil {
		if err := a.walkSelect(s.Right, cteScope); err != nil {
			return err
		}
	}
	return nil
}

func (a *tableAccumulator) walkSelect(s *query.SelectStmt, cteScope map[string]struct{}) error {
	if s == nil {
		return nil
	}
	if s.From != nil {
		if err := a.walkTableRef(s.From, cteScope); err != nil {
			return err
		}
	}
	for _, j := range s.Joins {
		if j == nil || j.Table == nil {
			continue
		}
		if err := a.walkTableRef(j.Table, cteScope); err != nil {
			return err
		}
		if err := a.walkExpr(j.Condition, cteScope); err != nil {
			return err
		}
	}
	for _, c := range s.Columns {
		if err := a.walkExpr(c, cteScope); err != nil {
			return err
		}
	}
	if err := a.walkExpr(s.Where, cteScope); err != nil {
		return err
	}
	for _, g := range s.GroupBy {
		if err := a.walkExpr(g, cteScope); err != nil {
			return err
		}
	}
	if err := a.walkExpr(s.Having, cteScope); err != nil {
		return err
	}
	for _, o := range s.OrderBy {
		if o != nil {
			if err := a.walkExpr(o.Expr, cteScope); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *tableAccumulator) walkTableRef(t *query.TableRef, cteScope map[string]struct{}) error {
	if t == nil {
		return nil
	}
	switch {
	case t.Subquery != nil:
		return a.walkSelect(t.Subquery, cteScope)
	case t.SubqueryStmt != nil:
		return a.walkStatement(t.SubqueryStmt, cteScope)
	default:
		name := strings.ToLower(strings.TrimSpace(t.Name))
		if name == "" {
			return nil
		}
		// A reference resolving to an in-scope CTE name is not a base table.
		if _, isCTE := cteScope[name]; isCTE {
			return nil
		}
		a.add(name)
		return nil
	}
}

// walkExpr recurses through every subquery-bearing expression node. Because this
// is a security check, an unknown shape that *could* carry a hidden table must
// fail closed — but the AST's subquery surface is closed (InExpr, SubqueryExpr,
// ExistsExpr), so we enumerate those and recurse structurally through the rest.
func (a *tableAccumulator) walkExpr(e query.Expression, cteScope map[string]struct{}) error {
	if e == nil {
		return nil
	}
	switch ex := e.(type) {
	case *query.SubqueryExpr:
		return a.walkSelect(ex.Query, cteScope)
	case *query.ExistsExpr:
		return a.walkSelect(ex.Subquery, cteScope)
	case *query.InExpr:
		if err := a.walkExpr(ex.Expr, cteScope); err != nil {
			return err
		}
		if ex.Subquery != nil {
			return a.walkSelect(ex.Subquery, cteScope)
		}
		for _, item := range ex.List {
			if err := a.walkExpr(item, cteScope); err != nil {
				return err
			}
		}
		return nil
	case *query.BinaryExpr:
		if err := a.walkExpr(ex.Left, cteScope); err != nil {
			return err
		}
		return a.walkExpr(ex.Right, cteScope)
	case *query.UnaryExpr:
		return a.walkExpr(ex.Expr, cteScope)
	case *query.FunctionCall:
		for _, arg := range ex.Args {
			if err := a.walkExpr(arg, cteScope); err != nil {
				return err
			}
		}
		return a.walkExpr(ex.Filter, cteScope)
	case *query.BetweenExpr:
		if err := a.walkExpr(ex.Expr, cteScope); err != nil {
			return err
		}
		if err := a.walkExpr(ex.Lower, cteScope); err != nil {
			return err
		}
		return a.walkExpr(ex.Upper, cteScope)
	case *query.LikeExpr:
		if err := a.walkExpr(ex.Expr, cteScope); err != nil {
			return err
		}
		if err := a.walkExpr(ex.Pattern, cteScope); err != nil {
			return err
		}
		return a.walkExpr(ex.Escape, cteScope)
	case *query.IsNullExpr:
		return a.walkExpr(ex.Expr, cteScope)
	case *query.CastExpr:
		return a.walkExpr(ex.Expr, cteScope)
	case *query.CaseExpr:
		if err := a.walkExpr(ex.Expr, cteScope); err != nil {
			return err
		}
		for _, w := range ex.Whens {
			if w == nil {
				continue
			}
			if err := a.walkExpr(w.Condition, cteScope); err != nil {
				return err
			}
			if err := a.walkExpr(w.Result, cteScope); err != nil {
				return err
			}
		}
		return a.walkExpr(ex.Else, cteScope)
	case *query.AliasExpr:
		return a.walkExpr(ex.Expr, cteScope)
	case *query.WindowExpr:
		for _, arg := range ex.Args {
			if err := a.walkExpr(arg, cteScope); err != nil {
				return err
			}
		}
		if err := a.walkExpr(ex.Filter, cteScope); err != nil {
			return err
		}
		for _, p := range ex.PartitionBy {
			if err := a.walkExpr(p, cteScope); err != nil {
				return err
			}
		}
		for _, o := range ex.OrderBy {
			if o != nil {
				if err := a.walkExpr(o.Expr, cteScope); err != nil {
					return err
				}
			}
		}
		return nil
	case *query.JSONPathExpr:
		return a.walkExpr(ex.Column, cteScope)
	case *query.JSONContainsExpr:
		if err := a.walkExpr(ex.Column, cteScope); err != nil {
			return err
		}
		return a.walkExpr(ex.Value, cteScope)
	case *query.MatchExpr:
		for _, col := range ex.Columns {
			if err := a.walkExpr(col, cteScope); err != nil {
				return err
			}
		}
		return a.walkExpr(ex.Pattern, cteScope)
	default:
		// Leaf nodes (identifiers, literals, placeholders, star, etc.) cannot
		// carry a table reference. They are safe to ignore.
		return nil
	}
}

func cloneScope(s map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(s)+2)
	for k := range s {
		out[k] = struct{}{}
	}
	return out
}
