package advisor

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ColumnPattern tracks how a single column is used in queries.
type ColumnPattern struct {
	ColumnName    string
	SelectCount   int64
	WhereCount    int64
	JoinCount     int64
	OrderByCount  int64
	GroupByCount  int64
}

// TablePattern aggregates column usage for one table.
type TablePattern struct {
	TableName string
	Columns   map[string]*ColumnPattern
}

// IndexRecommendation represents a suggested index.
type IndexRecommendation struct {
	TableName   string
	Columns     []string
	Reason      string
	QueryCount  int64
	Priority    float64
	Existing    bool
}

// IndexAdvisor analyzes executed queries and recommends missing indexes.
type IndexAdvisor struct {
	patterns map[string]*TablePattern // tableName -> pattern
	mu       sync.RWMutex
	maxPatterns int
}

// NewIndexAdvisor creates a new advisor.
func NewIndexAdvisor() *IndexAdvisor {
	return &IndexAdvisor{
		patterns:    make(map[string]*TablePattern),
		maxPatterns: 10000,
	}
}

// Analyze inspects a SQL statement and records column usage patterns.
func (a *IndexAdvisor) Analyze(stmt query.Statement) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch s := stmt.(type) {
	case *query.SelectStmt:
		a.analyzeSelect(s)
	case *query.UpdateStmt:
		a.analyzeUpdate(s)
	case *query.DeleteStmt:
		a.analyzeDelete(s)
	}
}

func (a *IndexAdvisor) analyzeSelect(stmt *query.SelectStmt) {
	tables := a.collectTablesFromSelect(stmt)
	primaryTable := ""
	if stmt.From != nil {
		primaryTable = stmt.From.Name
	}

	// Helper to record columns, mapping unqualified identifiers to primary table.
	recordCols := func(cols map[string][]string, counter func(*ColumnPattern)) {
		for tableName, colNames := range cols {
			if tableName == "" && primaryTable != "" {
				tableName = primaryTable
			}
			if _, ok := tables[tableName]; !ok && tableName != "" {
				tables[tableName] = true
			}
			for _, col := range colNames {
				if tableName != "" {
					counter(a.record(tableName, col))
				}
			}
		}
	}

	if stmt.Where != nil {
		recordCols(extractColumns(stmt.Where), func(cp *ColumnPattern) { cp.WhereCount++ })
	}
	for _, join := range stmt.Joins {
		if join.Condition != nil {
			recordCols(extractColumns(join.Condition), func(cp *ColumnPattern) { cp.JoinCount++ })
		}
	}
	for _, ob := range stmt.OrderBy {
		if ob.Expr != nil {
			recordCols(extractColumns(ob.Expr), func(cp *ColumnPattern) { cp.OrderByCount++ })
		}
	}
	for _, gb := range stmt.GroupBy {
		recordCols(extractColumns(gb), func(cp *ColumnPattern) { cp.GroupByCount++ })
	}
}

func (a *IndexAdvisor) analyzeUpdate(stmt *query.UpdateStmt) {
	if stmt.Where != nil {
		cols := extractColumns(stmt.Where)
		for _, colNames := range cols {
			for _, col := range colNames {
				a.record(stmt.Table, col).WhereCount++
			}
		}
	}
	for _, set := range stmt.Set {
		if set != nil {
			a.record(stmt.Table, set.Column).SelectCount++
		}
	}
}

func (a *IndexAdvisor) analyzeDelete(stmt *query.DeleteStmt) {
	if stmt.Where != nil {
		cols := extractColumns(stmt.Where)
		for _, colNames := range cols {
			for _, col := range colNames {
				a.record(stmt.Table, col).WhereCount++
			}
		}
	}
}

func (a *IndexAdvisor) collectTablesFromSelect(stmt *query.SelectStmt) map[string]bool {
	tables := make(map[string]bool)
	if stmt.From != nil && stmt.From.Name != "" {
		tables[stmt.From.Name] = true
	}
	for _, join := range stmt.Joins {
		if join.Table != nil && join.Table.Name != "" {
			tables[join.Table.Name] = true
		}
	}
	return tables
}

func (a *IndexAdvisor) record(tableName, columnName string) *ColumnPattern {
	tp, ok := a.patterns[tableName]
	if !ok {
		tp = &TablePattern{
			TableName: tableName,
			Columns:   make(map[string]*ColumnPattern),
		}
		a.patterns[tableName] = tp
	}
	cp, ok := tp.Columns[columnName]
	if !ok {
		cp = &ColumnPattern{ColumnName: columnName}
		tp.Columns[columnName] = cp
	}
	return cp
}

// Recommendations returns missing index suggestions based on observed query patterns.
// existingIndexes maps tableName -> list of existing index column lists.
func (a *IndexAdvisor) Recommendations(existingIndexes map[string][][]string) []*IndexRecommendation {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var recs []*IndexRecommendation
	for tableName, tp := range a.patterns {
		existingCols := existingIndexes[tableName]

		// Sort columns by importance (WHERE + JOIN first, then ORDER BY, then GROUP BY)
		var columns []*ColumnPattern
		for _, cp := range tp.Columns {
			columns = append(columns, cp)
		}
		sort.Slice(columns, func(i, j int) bool {
			scoreI := columns[i].WhereCount*10 + columns[i].JoinCount*8 + columns[i].OrderByCount*4 + columns[i].GroupByCount*2
			scoreJ := columns[j].WhereCount*10 + columns[j].JoinCount*8 + columns[j].OrderByCount*4 + columns[j].GroupByCount*2
			return scoreI > scoreJ
		})

		// Single-column recommendations
		for _, cp := range columns {
			cols := []string{cp.ColumnName}
			if a.hasExactIndex(existingCols, cols) || a.isPrefixOfExisting(existingCols, cols) || a.columnInExisting(existingCols, cp.ColumnName) {
				continue
			}
			score := float64(cp.WhereCount*10 + cp.JoinCount*8 + cp.OrderByCount*4 + cp.GroupByCount*2)
			if score <= 0 {
				continue
			}
			reason := a.buildReason(cp)
			recs = append(recs, &IndexRecommendation{
				TableName:  tableName,
				Columns:    cols,
				Reason:     reason,
				QueryCount: cp.WhereCount + cp.JoinCount + cp.OrderByCount + cp.GroupByCount,
				Priority:   score,
			})
		}

		// Multi-column composite recommendations (top 2-3 columns)
		if len(columns) >= 2 {
			var topCols []string
			var topScore int64
			for i := 0; i < len(columns) && i < 3; i++ {
				if columns[i].WhereCount+columns[i].JoinCount > 0 {
					topCols = append(topCols, columns[i].ColumnName)
					topScore += columns[i].WhereCount*10 + columns[i].JoinCount*8
				}
			}
			if len(topCols) >= 2 && !a.hasExactIndex(existingCols, topCols) && !a.isPrefixOfExisting(existingCols, topCols) {
				recs = append(recs, &IndexRecommendation{
					TableName:  tableName,
					Columns:    append([]string(nil), topCols...),
					Reason:     fmt.Sprintf("composite index for frequent WHERE/JOIN on (%s)", strings.Join(topCols, ", ")),
					QueryCount: topScore / 10,
					Priority:   float64(topScore) * 1.2, // slightly boost composite
				})
			}
		}
	}

	// Sort by priority descending
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Priority > recs[j].Priority
	})

	return recs
}

// Snapshot returns a read-only copy of all recorded patterns.
func (a *IndexAdvisor) Snapshot() map[string]*TablePattern {
	a.mu.RLock()
	defer a.mu.RUnlock()

	out := make(map[string]*TablePattern, len(a.patterns))
	for tn, tp := range a.patterns {
		copyTp := &TablePattern{
			TableName: tp.TableName,
			Columns:   make(map[string]*ColumnPattern, len(tp.Columns)),
		}
		for cn, cp := range tp.Columns {
			copyCp := *cp
			copyTp.Columns[cn] = &copyCp
		}
		out[tn] = copyTp
	}
	return out
}

// Reset clears all recorded patterns.
func (a *IndexAdvisor) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.patterns = make(map[string]*TablePattern)
}

func (a *IndexAdvisor) hasExactIndex(existing [][]string, cols []string) bool {
	for _, idx := range existing {
		if len(idx) == len(cols) {
			match := true
			for i := range idx {
				if !strings.EqualFold(idx[i], cols[i]) {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func (a *IndexAdvisor) columnInExisting(existing [][]string, col string) bool {
	for _, idx := range existing {
		for _, c := range idx {
			if strings.EqualFold(c, col) {
				return true
			}
		}
	}
	return false
}

func (a *IndexAdvisor) isPrefixOfExisting(existing [][]string, cols []string) bool {
	for _, idx := range existing {
		if len(idx) >= len(cols) {
			match := true
			for i := range cols {
				if !strings.EqualFold(idx[i], cols[i]) {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

func (a *IndexAdvisor) buildReason(cp *ColumnPattern) string {
	var parts []string
	if cp.WhereCount > 0 {
		parts = append(parts, fmt.Sprintf("WHERE (x%d)", cp.WhereCount))
	}
	if cp.JoinCount > 0 {
		parts = append(parts, fmt.Sprintf("JOIN (x%d)", cp.JoinCount))
	}
	if cp.OrderByCount > 0 {
		parts = append(parts, fmt.Sprintf("ORDER BY (x%d)", cp.OrderByCount))
	}
	if cp.GroupByCount > 0 {
		parts = append(parts, fmt.Sprintf("GROUP BY (x%d)", cp.GroupByCount))
	}
	return fmt.Sprintf("frequent %s usage", strings.Join(parts, ", "))
}

// extractColumns walks an expression and returns a map of tableName -> columnNames.
// For unqualified identifiers, tableName is "".
func extractColumns(expr query.Expression) map[string][]string {
	result := make(map[string][]string)
	extractColumnsRecursive(expr, result)
	return result
}

func extractColumnsRecursive(expr query.Expression, out map[string][]string) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *query.Identifier:
		out[""] = appendIfMissing(out[""], e.Name)
	case *query.QualifiedIdentifier:
		out[e.Table] = appendIfMissing(out[e.Table], e.Column)
	case *query.BinaryExpr:
		extractColumnsRecursive(e.Left, out)
		extractColumnsRecursive(e.Right, out)
	case *query.UnaryExpr:
		extractColumnsRecursive(e.Expr, out)
	case *query.InExpr:
		extractColumnsRecursive(e.Expr, out)
		for _, item := range e.List {
			extractColumnsRecursive(item, out)
		}
		if e.Subquery != nil {
			cols := extractColumns(e.Subquery.Where)
			for t, c := range cols {
				for _, col := range c {
					out[t] = appendIfMissing(out[t], col)
				}
			}
		}
	case *query.LikeExpr:
		extractColumnsRecursive(e.Expr, out)
		extractColumnsRecursive(e.Pattern, out)
	case *query.IsNullExpr:
		extractColumnsRecursive(e.Expr, out)
	case *query.FunctionCall:
		for _, arg := range e.Args {
			extractColumnsRecursive(arg, out)
		}
	case *query.BetweenExpr:
		extractColumnsRecursive(e.Expr, out)
		extractColumnsRecursive(e.Lower, out)
		extractColumnsRecursive(e.Upper, out)
	case *query.CastExpr:
		extractColumnsRecursive(e.Expr, out)
	case *query.CaseExpr:
		extractColumnsRecursive(e.Expr, out)
		for _, w := range e.Whens {
			extractColumnsRecursive(w.Condition, out)
			extractColumnsRecursive(w.Result, out)
		}
		extractColumnsRecursive(e.Else, out)
	case *query.ExistsExpr:
		if e.Subquery != nil {
			cols := extractColumns(e.Subquery.Where)
			for t, c := range cols {
				for _, col := range c {
					out[t] = appendIfMissing(out[t], col)
				}
			}
		}
	}
}

func appendIfMissing(slice []string, item string) []string {
	for _, s := range slice {
		if strings.EqualFold(s, item) {
			return slice
		}
	}
	return append(slice, item)
}
