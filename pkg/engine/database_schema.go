package engine

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/catalog"
)

// Tables returns a list of table names.

func (db *DB) Tables() []string {
	return db.catalog.ListTables()
}

// Path returns the database file path

func (db *DB) Path() string {
	return db.path
}

// TableSchema returns a human-readable schema for a table.
func (db *DB) TableSchema(name string) (string, error) {
	return db.tableSchema(name, true, false)
}

// TableSchemaWithoutForeignKeys returns a table schema with foreign key
// constraints omitted, so dumps can restore data before validating FKs.
func (db *DB) TableSchemaWithoutForeignKeys(name string) (string, error) {
	return db.tableSchema(name, false, true)
}

func (db *DB) tableSchema(name string, includeForeignKeys, quoteIdentifiers bool) (string, error) {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return "", err
	}
	compositePK := len(table.PrimaryKey) > 1

	var clauses []string
	for _, col := range table.Columns {
		line := fmt.Sprintf("  %s %s", schemaIdentifier(col.Name, quoteIdentifiers), schemaColumnType(col))
		if col.Collation != "" {
			line += fmt.Sprintf(" COLLATE %s", col.Collation)
		}
		if col.PrimaryKey && !compositePK {
			line += " PRIMARY KEY"
		}
		if col.AutoIncrement {
			line += " AUTOINCREMENT"
		}
		if col.NotNull {
			line += " NOT NULL"
		}
		if col.Unique {
			line += " UNIQUE"
		}
		if col.Default != "" {
			line += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		if col.CheckStr != "" {
			line += " "
			if col.CheckName != "" {
				line += fmt.Sprintf("CONSTRAINT %s ", schemaIdentifier(col.CheckName, quoteIdentifiers))
			}
			line += fmt.Sprintf("CHECK (%s)", schemaCheckExpr(col.CheckStr))
		}
		clauses = append(clauses, line)
	}
	if compositePK {
		clauses = append(clauses, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(schemaIdentifierList(table.PrimaryKey, quoteIdentifiers), ", ")))
	}
	for _, check := range table.Checks {
		clause := "  "
		if check.Name != "" {
			clause += fmt.Sprintf("CONSTRAINT %s ", schemaIdentifier(check.Name, quoteIdentifiers))
		}
		clause += fmt.Sprintf("CHECK (%s)", schemaCheckExpr(check.CheckStr))
		clauses = append(clauses, clause)
	}
	if includeForeignKeys {
		for _, fk := range table.ForeignKeys {
			clause := "  "
			if fk.Name != "" {
				clause += fmt.Sprintf("CONSTRAINT %s ", schemaIdentifier(fk.Name, quoteIdentifiers))
			}
			clause += fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s",
				strings.Join(schemaIdentifierList(fk.Columns, quoteIdentifiers), ", "),
				schemaIdentifier(fk.ReferencedTable, quoteIdentifiers))
			if len(fk.ReferencedColumns) > 0 {
				clause += fmt.Sprintf(" (%s)", strings.Join(schemaIdentifierList(fk.ReferencedColumns, quoteIdentifiers), ", "))
			}
			if fk.OnDelete != "" {
				clause += " ON DELETE " + fk.OnDelete
			}
			if fk.OnUpdate != "" {
				clause += " ON UPDATE " + fk.OnUpdate
			}
			clauses = append(clauses, clause)
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", schemaIdentifier(table.Name, quoteIdentifiers)))
	sb.WriteString(strings.Join(clauses, ",\n"))
	sb.WriteString("\n);")
	return sb.String(), nil
}

func schemaColumnType(col catalog.ColumnDef) string {
	if strings.EqualFold(col.Type, "VECTOR") && col.Dimensions > 0 {
		return fmt.Sprintf("VECTOR(%d)", col.Dimensions)
	}
	return col.Type
}

func schemaIdentifier(name string, quote bool) string {
	if !quote {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func schemaIdentifierList(names []string, quote bool) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = schemaIdentifier(name, quote)
	}
	return out
}

func schemaCheckExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if len(expr) >= 2 && expr[0] == '(' && expr[len(expr)-1] == ')' {
		return strings.TrimSpace(expr[1 : len(expr)-1])
	}
	return expr
}

// TableForeignKeyRef describes a foreign key from a table to another table.
type TableForeignKeyRef struct {
	Name              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string
	OnUpdate          string
}

// TableForeignKeyRefs returns the distinct names of tables referenced by a
// table's foreign keys, used to order a dump so referenced tables come first.
func (db *DB) TableForeignKeyRefs(name string) []string {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return nil
	}
	seen := make(map[string]bool)
	var refs []string
	for _, fk := range table.ForeignKeys {
		if fk.ReferencedTable != "" && !seen[fk.ReferencedTable] && fk.ReferencedTable != name {
			seen[fk.ReferencedTable] = true
			refs = append(refs, fk.ReferencedTable)
		}
	}
	return refs
}

// TableForeignKeys returns foreign key definitions declared on a table.
func (db *DB) TableForeignKeys(name string) []TableForeignKeyRef {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return nil
	}
	refs := make([]TableForeignKeyRef, 0, len(table.ForeignKeys))
	for _, fk := range table.ForeignKeys {
		refs = append(refs, TableForeignKeyRef{
			Name:              fk.Name,
			Columns:           append([]string(nil), fk.Columns...),
			ReferencedTable:   fk.ReferencedTable,
			ReferencedColumns: append([]string(nil), fk.ReferencedColumns...),
			OnDelete:          fk.OnDelete,
			OnUpdate:          fk.OnUpdate,
		})
	}
	return refs
}

// TableSelfForeignKeyRefs returns self-referential foreign keys for a table.
func (db *DB) TableSelfForeignKeyRefs(name string) []TableForeignKeyRef {
	table, err := db.catalog.GetTable(name)
	if err != nil {
		return nil
	}
	var refs []TableForeignKeyRef
	for _, fk := range table.ForeignKeys {
		if !strings.EqualFold(fk.ReferencedTable, name) {
			continue
		}
		refColumns := fk.ReferencedColumns
		if len(refColumns) == 0 {
			refColumns = table.PrimaryKey
		}
		refs = append(refs, TableForeignKeyRef{
			Columns:           append([]string(nil), fk.Columns...),
			ReferencedTable:   fk.ReferencedTable,
			ReferencedColumns: append([]string(nil), refColumns...),
		})
	}
	return refs
}

// TableIndexDDL returns CREATE INDEX statements for a table's secondary indexes.
func (db *DB) TableIndexDDL(name string) []string {
	var ddl []string
	for _, idx := range db.catalog.GetTableIndexes(name) {
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		ddl = append(ddl, fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);",
			unique,
			schemaIdentifier(idx.Name, true),
			schemaIdentifier(name, true),
			strings.Join(schemaIdentifierList(idx.Columns, true), ", ")))
	}
	return ddl
}

// FTSIndexDDL returns CREATE FULLTEXT INDEX statements for SQL dumps.
func (db *DB) FTSIndexDDL() []string {
	indexes := db.catalog.ListFTSIndexDefs()
	ddl := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		ddl = append(ddl, fmt.Sprintf("CREATE FULLTEXT INDEX %s ON %s (%s);",
			schemaIdentifier(idx.Name, true),
			schemaIdentifier(idx.TableName, true),
			strings.Join(schemaIdentifierList(idx.Columns, true), ", ")))
	}
	return ddl
}

// VectorIndexDDL returns CREATE VECTOR INDEX statements for SQL dumps.
func (db *DB) VectorIndexDDL() []string {
	indexes := db.catalog.ListVectorIndexDefs()
	ddl := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		ddl = append(ddl, fmt.Sprintf("CREATE VECTOR INDEX %s ON %s (%s);",
			schemaIdentifier(idx.Name, true),
			schemaIdentifier(idx.TableName, true),
			schemaIdentifier(idx.ColumnName, true)))
	}
	return ddl
}

// RLSPolicyDDL returns ALTER TABLE and CREATE POLICY statements for SQL dumps.
func (db *DB) RLSPolicyDDL() []string {
	tables := db.catalog.ListRLSEnabledTables()
	policies := db.catalog.ListRLSPolicies()
	if len(tables) == 0 && len(policies) == 0 {
		return nil
	}
	ddl := make([]string, 0, len(tables)+len(policies))
	enabledTables := make(map[string]bool)
	for _, tableName := range tables {
		tableKey := strings.ToLower(tableName)
		if enabledTables[tableKey] {
			continue
		}
		ddl = append(ddl, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY;", schemaIdentifier(tableName, true)))
		enabledTables[tableKey] = true
	}
	for _, policy := range policies {
		tableKey := strings.ToLower(policy.TableName)
		if !enabledTables[tableKey] {
			ddl = append(ddl, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY;", schemaIdentifier(policy.TableName, true)))
			enabledTables[tableKey] = true
		}

		var sb strings.Builder
		fmt.Fprintf(&sb, "CREATE POLICY %s ON %s",
			schemaIdentifier(policy.Name, true),
			schemaIdentifier(policy.TableName, true))
		if policy.Restrictive {
			sb.WriteString(" AS RESTRICTIVE")
		}
		fmt.Fprintf(&sb, " FOR %s", policy.Type.String())
		if len(policy.Roles) > 0 {
			sb.WriteString(" TO ")
			sb.WriteString(strings.Join(schemaIdentifierList(policy.Roles, true), ", "))
		}
		if strings.TrimSpace(policy.Expression) != "" {
			fmt.Fprintf(&sb, " USING (%s)", policy.Expression)
		}
		if strings.TrimSpace(policy.CheckExpression) != "" {
			fmt.Fprintf(&sb, " WITH CHECK (%s)", policy.CheckExpression)
		}
		sb.WriteString(";")
		ddl = append(ddl, sb.String())
	}
	return ddl
}

// ForeignTableDDL returns CREATE FOREIGN TABLE statements for SQL dumps.
func (db *DB) ForeignTableDDL() []string {
	foreignTables := db.catalog.ListForeignTables()
	sort.Slice(foreignTables, func(i, j int) bool {
		return strings.ToLower(foreignTables[i].TableName) < strings.ToLower(foreignTables[j].TableName)
	})
	ddl := make([]string, 0, len(foreignTables))
	for _, ft := range foreignTables {
		ddl = append(ddl, foreignTableDDL(ft))
	}
	return ddl
}

func foreignTableDDL(ft catalog.ForeignTableDef) string {
	columns := make([]string, len(ft.Columns))
	for i, col := range ft.Columns {
		line := fmt.Sprintf("%s %s", schemaIdentifier(col.Name, true), col.Type)
		if col.Collation != "" {
			line += fmt.Sprintf(" COLLATE %s", col.Collation)
		}
		if col.PrimaryKey {
			line += " PRIMARY KEY"
		}
		if col.AutoIncrement {
			line += " AUTOINCREMENT"
		}
		if col.NotNull {
			line += " NOT NULL"
		}
		if col.Unique {
			line += " UNIQUE"
		}
		if col.Default != "" {
			line += fmt.Sprintf(" DEFAULT %s", col.Default)
		}
		if col.CheckStr != "" {
			line += " "
			if col.CheckName != "" {
				line += fmt.Sprintf("CONSTRAINT %s ", schemaIdentifier(col.CheckName, true))
			}
			line += fmt.Sprintf("CHECK (%s)", schemaCheckExpr(col.CheckStr))
		}
		columns[i] = line
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE FOREIGN TABLE %s (\n  %s\n) WRAPPER %s",
		schemaIdentifier(ft.TableName, true),
		strings.Join(columns, ",\n  "),
		schemaStringLiteral(ft.Wrapper))
	if len(ft.Options) > 0 {
		keys := make([]string, 0, len(ft.Options))
		for key := range ft.Options {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		options := make([]string, len(keys))
		for i, key := range keys {
			options[i] = fmt.Sprintf("%s %s", schemaIdentifier(key, true), schemaStringLiteral(ft.Options[key]))
		}
		fmt.Fprintf(&sb, " OPTIONS (%s)", strings.Join(options, ", "))
	}
	sb.WriteString(";")
	return sb.String()
}

func schemaStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// ViewDDL returns persisted CREATE VIEW statements for SQL dumps.
func (db *DB) ViewDDL() []string {
	views := db.catalog.ListViewSQL()
	names := make([]string, 0, len(views))
	for name := range views {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	ddl := make([]string, 0, len(names))
	for _, name := range names {
		sql := strings.TrimSpace(views[name])
		if sql == "" {
			continue
		}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		ddl = append(ddl, sql)
	}
	return ddl
}

// MaterializedViewDDL returns persisted CREATE MATERIALIZED VIEW statements.
func (db *DB) MaterializedViewDDL() []string {
	views := db.catalog.ListMaterializedViewSQL()
	names := make([]string, 0, len(views))
	for name := range views {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	ddl := make([]string, 0, len(names))
	for _, name := range names {
		sql := strings.TrimSpace(views[name])
		if sql == "" {
			continue
		}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		ddl = append(ddl, sql)
	}
	return ddl
}

// TriggerDDL returns persisted CREATE TRIGGER statements for SQL dumps.
func (db *DB) TriggerDDL() []string {
	triggers := db.catalog.ListTriggerSQL()
	names := make([]string, 0, len(triggers))
	for name := range triggers {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	ddl := make([]string, 0, len(names))
	for _, name := range names {
		sql := strings.TrimSpace(triggers[name])
		if sql == "" {
			continue
		}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		ddl = append(ddl, sql)
	}
	return ddl
}

// ProcedureDDL returns persisted CREATE PROCEDURE statements for SQL dumps.
func (db *DB) ProcedureDDL() []string {
	procedures := db.catalog.ListProcedureSQL()
	names := make([]string, 0, len(procedures))
	for name := range procedures {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})
	ddl := make([]string, 0, len(names))
	for _, name := range names {
		sql := strings.TrimSpace(procedures[name])
		if sql == "" {
			continue
		}
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		ddl = append(ddl, sql)
	}
	return ddl
}
