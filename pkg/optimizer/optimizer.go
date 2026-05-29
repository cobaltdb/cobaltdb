// Package optimizer provides query optimization for CobaltDB
package optimizer

import (
	"sort"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// Config holds optimizer configuration
type Config struct {
	Enabled                 bool
	EnableIndexSelection    bool
	EnableJoinReorder       bool
	EnablePredicatePushdown bool
	MaxJoinReorderTables    int
}

// DefaultConfig returns default optimizer configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:                 true,
		EnableIndexSelection:    true,
		EnableJoinReorder:       true,
		EnablePredicatePushdown: true,
		MaxJoinReorderTables:    6,
	}
}

// Statistics holds table statistics for optimization
type Statistics struct {
	TableStats map[string]*TableStatistics
}

// TableStatistics holds statistics for a table
type TableStatistics struct {
	TableName    string
	RowCount     int64
	ColumnStats  map[string]*ColumnStatistics
	IndexStats   map[string]*IndexStatistics
	LastAnalyzed int64
}

// ColumnStatistics holds statistics for a column
type ColumnStatistics struct {
	ColumnName     string
	DistinctValues int64
	NullCount      int64
}

// IndexStatistics holds statistics for an index
type IndexStatistics struct {
	IndexName   string
	TableName   string
	Columns     []string
	IsUnique    bool
	Selectivity float64
}

// Optimizer optimizes SQL queries
type Optimizer struct {
	config *Config
	stats  *Statistics
}

// New creates a new query optimizer
func New(config *Config, stats *Statistics) *Optimizer {
	if config == nil {
		config = DefaultConfig()
	} else {
		config = cloneConfig(config)
	}
	if stats == nil {
		stats = &Statistics{TableStats: make(map[string]*TableStatistics)}
	} else {
		stats = cloneStatistics(stats)
	}
	return &Optimizer{config: config, stats: stats}
}

// Optimize optimizes a SELECT statement
func (o *Optimizer) Optimize(stmt *query.SelectStmt) (*query.SelectStmt, error) {
	if !o.config.Enabled {
		return stmt, nil
	}

	optimized := stmt

	if o.config.EnableJoinReorder && len(optimized.Joins) > 0 {
		optimized = o.reorderJoins(optimized)
	}

	return optimized, nil
}

// reorderJoins reorders JOINs for optimal performance
func (o *Optimizer) reorderJoins(stmt *query.SelectStmt) *query.SelectStmt {
	if len(stmt.Joins) == 0 || len(stmt.Joins) > o.config.MaxJoinReorderTables {
		return stmt
	}

	ordered := make([]*query.JoinClause, len(stmt.Joins))
	copy(ordered, stmt.Joins)

	// Sort by estimated selectivity (most selective first)
	sort.Slice(ordered, func(i, j int) bool {
		selI := o.estimateJoinSelectivity(ordered[i])
		selJ := o.estimateJoinSelectivity(ordered[j])
		return selI < selJ
	})

	stmt.Joins = ordered
	return stmt
}

// estimateJoinSelectivity estimates the selectivity of a join
func (o *Optimizer) estimateJoinSelectivity(join *query.JoinClause) float64 {
	selectivity := 0.3

	if join.Table == nil {
		return selectivity
	}

	stats := o.stats.TableStats[join.Table.Name]
	if stats == nil {
		return selectivity
	}

	switch join.Type {
	case query.TokenInner:
		selectivity = 0.1
	case query.TokenLeft, query.TokenRight:
		selectivity = 0.5
	case query.TokenCross:
		selectivity = 1.0
	}

	if stats.RowCount > 10000 {
		selectivity *= 0.8
	}

	return selectivity
}

// SelectBestIndex selects the best index for a table
func (o *Optimizer) SelectBestIndex(tableName string, where query.Expression) string {
	if !o.config.EnableIndexSelection {
		return ""
	}

	stats := o.stats.TableStats[tableName]
	if stats == nil || len(stats.IndexStats) == 0 {
		return ""
	}

	columns := o.extractColumnReferences(where)
	if len(columns) == 0 {
		return ""
	}

	bestIndex := ""
	bestScore := 0.0

	for indexName, indexStats := range stats.IndexStats {
		score := o.scoreIndex(columns, indexStats)
		if score > bestScore {
			bestScore = score
			bestIndex = indexName
		}
	}

	return bestIndex
}

// extractColumnReferences extracts column names from an expression.
// It mirrors the coverage of extractColumnsRecursive in advisor.go:
// - Identifiers and qualified identifiers
// - Binary/unary operators
// - Function arguments, CASE results, subquery columns
//
// If you add a case here, check whether advisor.go also needs the same case.
func (o *Optimizer) extractColumnReferences(expr query.Expression) []string {
	columns := make([]string, 0)

	var traverse func(query.Expression)
	traverse = func(e query.Expression) {
		if e == nil {
			return
		}
		switch v := e.(type) {
		case *query.Identifier:
			columns = append(columns, v.Name)
		case *query.QualifiedIdentifier:
			columns = append(columns, v.Column)
		case *query.BinaryExpr:
			traverse(v.Left)
			traverse(v.Right)
		case *query.UnaryExpr:
			traverse(v.Expr)
		case *query.InExpr:
			traverse(v.Expr)
			for _, item := range v.List {
				traverse(item)
			}
			if v.Subquery != nil && v.Subquery.Where != nil {
				traverse(v.Subquery.Where)
			}
		case *query.LikeExpr:
			traverse(v.Expr)
			traverse(v.Pattern)
		case *query.IsNullExpr:
			traverse(v.Expr)
		case *query.FunctionCall:
			for _, arg := range v.Args {
				traverse(arg)
			}
		case *query.BetweenExpr:
			traverse(v.Expr)
			traverse(v.Lower)
			traverse(v.Upper)
		case *query.CastExpr:
			traverse(v.Expr)
		case *query.CaseExpr:
			traverse(v.Expr)
			for _, w := range v.Whens {
				traverse(w.Condition)
				traverse(w.Result)
			}
			traverse(v.Else)
		case *query.ExistsExpr:
			if v.Subquery != nil && v.Subquery.Where != nil {
				traverse(v.Subquery.Where)
			}
		}
	}

	traverse(expr)
	return columns
}

// scoreIndex scores how well an index matches the query columns
func (o *Optimizer) scoreIndex(columns []string, indexStats *IndexStatistics) float64 {
	if len(indexStats.Columns) == 0 {
		return 0.0
	}

	score := 0.0
	matchedColumns := 0

	for i, indexCol := range indexStats.Columns {
		found := false
		for _, col := range columns {
			if strings.EqualFold(indexCol, col) {
				found = true
				break
			}
		}
		if found {
			matchedColumns++
			score += float64(len(indexStats.Columns)-i) * 10.0
		} else {
			break
		}
	}

	if indexStats.IsUnique {
		score *= 2.0
	}

	score *= (1.0 + indexStats.Selectivity)
	return score
}

// UpdateStatistics updates statistics for a table
func (o *Optimizer) UpdateStatistics(tableName string, stats *TableStatistics) {
	if o.stats.TableStats == nil {
		o.stats.TableStats = make(map[string]*TableStatistics)
	}
	o.stats.TableStats[tableName] = cloneTableStatistics(stats)
}

// GetTableStatistics returns statistics for a table
func (o *Optimizer) GetTableStatistics(tableName string) *TableStatistics {
	return cloneTableStatistics(o.stats.TableStats[tableName])
}

func cloneConfig(config *Config) *Config {
	if config == nil {
		return nil
	}
	cloned := *config
	return &cloned
}

func cloneStatistics(stats *Statistics) *Statistics {
	if stats == nil {
		return nil
	}
	cloned := &Statistics{TableStats: make(map[string]*TableStatistics, len(stats.TableStats))}
	for tableName, tableStats := range stats.TableStats {
		cloned.TableStats[tableName] = cloneTableStatistics(tableStats)
	}
	return cloned
}

func cloneTableStatistics(stats *TableStatistics) *TableStatistics {
	if stats == nil {
		return nil
	}
	cloned := *stats
	cloned.ColumnStats = make(map[string]*ColumnStatistics, len(stats.ColumnStats))
	for columnName, columnStats := range stats.ColumnStats {
		if columnStats == nil {
			continue
		}
		copied := *columnStats
		cloned.ColumnStats[columnName] = &copied
	}
	cloned.IndexStats = make(map[string]*IndexStatistics, len(stats.IndexStats))
	for indexName, indexStats := range stats.IndexStats {
		if indexStats == nil {
			continue
		}
		copied := *indexStats
		if indexStats.Columns != nil {
			copied.Columns = make([]string, len(indexStats.Columns))
			copy(copied.Columns, indexStats.Columns)
		}
		cloned.IndexStats[indexName] = &copied
	}
	return &cloned
}
