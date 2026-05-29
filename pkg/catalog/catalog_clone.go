package catalog

import "github.com/cobaltdb/cobaltdb/pkg/query"

func cloneTableDef(table *TableDef) *TableDef {
	if table == nil {
		return nil
	}
	cloned := *table
	cloned.Columns = cloneColumnDefs(table.Columns)
	cloned.PrimaryKey = cloneStringSlice(table.PrimaryKey)
	cloned.ForeignKeys = cloneForeignKeyDefs(table.ForeignKeys)
	cloned.Partition = clonePartitionInfo(table.Partition)
	cloned.buildColumnIndexCache()
	return &cloned
}

func cloneColumnDefs(columns []ColumnDef) []ColumnDef {
	if columns == nil {
		return nil
	}
	cloned := make([]ColumnDef, len(columns))
	copy(cloned, columns)
	return cloned
}

func cloneForeignKeyDefs(foreignKeys []ForeignKeyDef) []ForeignKeyDef {
	if foreignKeys == nil {
		return nil
	}
	cloned := make([]ForeignKeyDef, len(foreignKeys))
	for i, fk := range foreignKeys {
		cloned[i] = fk
		cloned[i].Columns = cloneStringSlice(fk.Columns)
		cloned[i].ReferencedColumns = cloneStringSlice(fk.ReferencedColumns)
	}
	return cloned
}

func clonePartitionInfo(partition *PartitionInfo) *PartitionInfo {
	if partition == nil {
		return nil
	}
	cloned := *partition
	if partition.Partitions != nil {
		cloned.Partitions = make([]PartitionDef, len(partition.Partitions))
		copy(cloned.Partitions, partition.Partitions)
	}
	return &cloned
}

func cloneIndexDef(index *IndexDef) *IndexDef {
	if index == nil {
		return nil
	}
	cloned := *index
	cloned.Columns = cloneStringSlice(index.Columns)
	return &cloned
}

func cloneForeignTableDef(foreignTable *ForeignTableDef) *ForeignTableDef {
	if foreignTable == nil {
		return nil
	}
	cloned := *foreignTable
	cloned.Columns = cloneColumnDefs(foreignTable.Columns)
	if foreignTable.Options != nil {
		cloned.Options = make(map[string]string, len(foreignTable.Options))
		for key, value := range foreignTable.Options {
			cloned.Options[key] = value
		}
	}
	return &cloned
}

func cloneMaterializedViewDef(view *MaterializedViewDef) *MaterializedViewDef {
	if view == nil {
		return nil
	}
	cloned := *view
	cloned.Columns = cloneStringSlice(view.Columns)
	cloned.Query = cloneSelectStmt(view.Query)
	if view.Data != nil {
		cloned.Data = make([]map[string]interface{}, len(view.Data))
		for i, row := range view.Data {
			if row == nil {
				continue
			}
			cloned.Data[i] = make(map[string]interface{}, len(row))
			for key, value := range row {
				cloned.Data[i][key] = cloneInterfaceValue(value)
			}
		}
	}
	return &cloned
}

func cloneSelectStmt(stmt *query.SelectStmt) *query.SelectStmt {
	if stmt == nil {
		return nil
	}
	cloned := *stmt
	cloned.Columns = cloneExpressions(stmt.Columns)
	cloned.From = cloneTableRef(stmt.From)
	if stmt.Joins != nil {
		cloned.Joins = make([]*query.JoinClause, len(stmt.Joins))
		for i, join := range stmt.Joins {
			cloned.Joins[i] = cloneJoinClause(join)
		}
	}
	cloned.GroupBy = cloneExpressions(stmt.GroupBy)
	if stmt.OrderBy != nil {
		cloned.OrderBy = make([]*query.OrderByExpr, len(stmt.OrderBy))
		for i, orderBy := range stmt.OrderBy {
			if orderBy == nil {
				continue
			}
			copied := *orderBy
			cloned.OrderBy[i] = &copied
		}
	}
	if stmt.AsOf != nil {
		copied := *stmt.AsOf
		cloned.AsOf = &copied
	}
	return &cloned
}

func cloneExpressions(expressions []query.Expression) []query.Expression {
	if expressions == nil {
		return nil
	}
	cloned := make([]query.Expression, len(expressions))
	copy(cloned, expressions)
	return cloned
}

func cloneTableRef(ref *query.TableRef) *query.TableRef {
	if ref == nil {
		return nil
	}
	cloned := *ref
	cloned.Subquery = cloneSelectStmt(ref.Subquery)
	return &cloned
}

func cloneJoinClause(join *query.JoinClause) *query.JoinClause {
	if join == nil {
		return nil
	}
	cloned := *join
	cloned.Table = cloneTableRef(join.Table)
	cloned.Using = cloneStringSlice(join.Using)
	return &cloned
}

func cloneCreateTriggerStmt(stmt *query.CreateTriggerStmt) *query.CreateTriggerStmt {
	if stmt == nil {
		return nil
	}
	cloned := *stmt
	if stmt.Body != nil {
		cloned.Body = make([]query.Statement, len(stmt.Body))
		copy(cloned.Body, stmt.Body)
	}
	return &cloned
}

func cloneCreateProcedureStmt(stmt *query.CreateProcedureStmt) *query.CreateProcedureStmt {
	if stmt == nil {
		return nil
	}
	cloned := *stmt
	if stmt.Params != nil {
		cloned.Params = make([]*query.ParamDef, len(stmt.Params))
		for i, param := range stmt.Params {
			if param == nil {
				continue
			}
			copied := *param
			cloned.Params[i] = &copied
		}
	}
	if stmt.Body != nil {
		cloned.Body = make([]query.Statement, len(stmt.Body))
		copy(cloned.Body, stmt.Body)
	}
	return &cloned
}

func cloneTableStats(stats *TableStats) *TableStats {
	if stats == nil {
		return nil
	}
	stats.mu.RLock()
	defer stats.mu.RUnlock()

	cloned := &TableStats{
		TableName:    stats.TableName,
		RowCount:    stats.RowCount,
		PageCount:   stats.PageCount,
		LastAnalyzed: stats.LastAnalyzed,
		ColumnStats: make(map[string]*ColumnStats, len(stats.ColumnStats)),
	}
	for name, columnStats := range stats.ColumnStats {
		cloned.ColumnStats[name] = cloneColumnStats(columnStats)
	}
	return cloned
}

func cloneColumnStats(stats *ColumnStats) *ColumnStats {
	if stats == nil {
		return nil
	}
	cloned := *stats
	cloned.MinValue = cloneInterfaceValue(stats.MinValue)
	cloned.MaxValue = cloneInterfaceValue(stats.MaxValue)
	if stats.Histogram != nil {
		cloned.Histogram = make([]Bucket, len(stats.Histogram))
		copy(cloned.Histogram, stats.Histogram)
		for i := range cloned.Histogram {
			cloned.Histogram[i].LowerBound = cloneInterfaceValue(cloned.Histogram[i].LowerBound)
			cloned.Histogram[i].UpperBound = cloneInterfaceValue(cloned.Histogram[i].UpperBound)
		}
	}
	return &cloned
}

func cloneStringSlice(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneInterfaceRows(rows [][]interface{}) [][]interface{} {
	if rows == nil {
		return nil
	}
	cloned := make([][]interface{}, len(rows))
	for i, row := range rows {
		cloned[i] = cloneInterfaceSlice(row)
	}
	return cloned
}

func cloneInterfaceSlice(values []interface{}) []interface{} {
	if values == nil {
		return nil
	}
	cloned := make([]interface{}, len(values))
	for i, value := range values {
		cloned[i] = cloneInterfaceValue(value)
	}
	return cloned
}

func cloneInterfaceValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case []byte:
		if typed == nil {
			return []byte(nil)
		}
		cloned := make([]byte, len(typed))
		copy(cloned, typed)
		return cloned
	case []interface{}:
		return cloneInterfaceSlice(typed)
	case map[string]interface{}:
		cloned := make(map[string]interface{}, len(typed))
		for key, mapValue := range typed {
			cloned[key] = cloneInterfaceValue(mapValue)
		}
		return cloned
	default:
		return typed
	}
}