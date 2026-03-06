package query

// Node is the base interface for all AST nodes
type Node interface {
	nodeType() string
}

// Statement is the base interface for all SQL statements
type Statement interface {
	Node
	statementNode()
}

// Expression is the base interface for all expressions
type Expression interface {
	Node
	expressionNode()
}

// SelectStmt represents a SELECT statement
type SelectStmt struct {
	Distinct bool
	Columns  []Expression
	From     *TableRef
	Joins    []*JoinClause
	Where    Expression
	GroupBy  []Expression
	Having   Expression
	OrderBy  []*OrderByExpr
	Limit    Expression
	Offset   Expression
}

func (s *SelectStmt) nodeType() string { return "SelectStmt" }
func (s *SelectStmt) statementNode()   {}

// SetOpType represents the type of set operation
type SetOpType int

const (
	SetOpUnion     SetOpType = iota // UNION
	SetOpIntersect                  // INTERSECT
	SetOpExcept                     // EXCEPT
)

// UnionStmt represents a UNION/INTERSECT/EXCEPT of multiple SELECT statements
type UnionStmt struct {
	Left    Statement // SelectStmt or UnionStmt
	Right   *SelectStmt
	All     bool       // ALL variant (no deduplication)
	Op      SetOpType  // UNION, INTERSECT, or EXCEPT
	OrderBy []*OrderByExpr
	Limit   Expression
	Offset  Expression
}

func (s *UnionStmt) nodeType() string { return "UnionStmt" }
func (s *UnionStmt) statementNode()   {}

// ConflictAction specifies behavior on constraint conflict
type ConflictAction int

const (
	ConflictAbort   ConflictAction = iota // Default: abort on conflict
	ConflictReplace                       // INSERT OR REPLACE: replace existing row
	ConflictIgnore                        // INSERT OR IGNORE: skip conflicting row
)

// InsertStmt represents an INSERT statement
type InsertStmt struct {
	Table          string
	Columns        []string
	Values         [][]Expression
	Select         *SelectStmt    // For INSERT INTO ... SELECT ...
	ConflictAction ConflictAction // OR REPLACE / OR IGNORE
}

func (s *InsertStmt) nodeType() string { return "InsertStmt" }
func (s *InsertStmt) statementNode()   {}

// UpdateStmt represents an UPDATE statement
type UpdateStmt struct {
	Table string
	Set   []*SetClause
	Where Expression
}

func (s *UpdateStmt) nodeType() string { return "UpdateStmt" }
func (s *UpdateStmt) statementNode()   {}

// SetClause represents a column assignment in UPDATE
type SetClause struct {
	Column string
	Value  Expression
}

// DeleteStmt represents a DELETE statement
type DeleteStmt struct {
	Table string
	Where Expression
}

func (s *DeleteStmt) nodeType() string { return "DeleteStmt" }
func (s *DeleteStmt) statementNode()   {}

// CreateTableStmt represents a CREATE TABLE statement
type CreateTableStmt struct {
	IfNotExists bool
	Table       string
	Columns     []*ColumnDef
	ForeignKeys []*ForeignKeyDef
}

func (s *CreateTableStmt) nodeType() string { return "CreateTableStmt" }
func (s *CreateTableStmt) statementNode()   {}

// DropTableStmt represents a DROP TABLE statement
type DropTableStmt struct {
	IfExists bool
	Table    string
}

func (s *DropTableStmt) nodeType() string { return "DropTableStmt" }
func (s *DropTableStmt) statementNode()   {}

// CreateIndexStmt represents a CREATE INDEX statement
type CreateIndexStmt struct {
	IfNotExists bool
	Index       string
	Table       string
	Columns     []string
	Unique      bool
}

func (s *CreateIndexStmt) nodeType() string { return "CreateIndexStmt" }
func (s *CreateIndexStmt) statementNode()   {}

// DropIndexStmt represents a DROP INDEX statement
type DropIndexStmt struct {
	IfExists bool
	Index    string
}

func (s *DropIndexStmt) nodeType() string { return "DropIndexStmt" }
func (s *DropIndexStmt) statementNode()   {}

// CreateCollectionStmt represents a CREATE COLLECTION statement
type CreateCollectionStmt struct {
	IfNotExists bool
	Name        string
}

func (s *CreateCollectionStmt) nodeType() string { return "CreateCollectionStmt" }
func (s *CreateCollectionStmt) statementNode()   {}

// CreateViewStmt represents a CREATE VIEW statement
type CreateViewStmt struct {
	IfNotExists bool
	Name        string
	Query       *SelectStmt
}

func (s *CreateViewStmt) nodeType() string { return "CreateViewStmt" }
func (s *CreateViewStmt) statementNode()   {}

// DropViewStmt represents a DROP VIEW statement
type DropViewStmt struct {
	IfExists bool
	Name     string
}

func (s *DropViewStmt) nodeType() string { return "DropViewStmt" }
func (s *DropViewStmt) statementNode()   {}

// CreateTriggerStmt represents a CREATE TRIGGER statement
type CreateTriggerStmt struct {
	IfNotExists bool
	Name        string
	Table       string
	Time        string // BEFORE, AFTER
	Event       string // INSERT, UPDATE, DELETE
	Condition   Expression // WHEN condition (optional)
	Body        []Statement
}

func (s *CreateTriggerStmt) nodeType() string { return "CreateTriggerStmt" }
func (s *CreateTriggerStmt) statementNode()   {}

// DropTriggerStmt represents a DROP TRIGGER statement
type DropTriggerStmt struct {
	IfExists bool
	Name     string
}

func (s *DropTriggerStmt) nodeType() string { return "DropTriggerStmt" }
func (s *DropTriggerStmt) statementNode()   {}

// CreateProcedureStmt represents a CREATE PROCEDURE statement
type CreateProcedureStmt struct {
	IfNotExists bool
	Name        string
	Params      []*ParamDef
	Body        []Statement
}

func (s *CreateProcedureStmt) nodeType() string { return "CreateProcedureStmt" }
func (s *CreateProcedureStmt) statementNode()   {}

// DropProcedureStmt represents a DROP PROCEDURE statement
type DropProcedureStmt struct {
	IfExists bool
	Name     string
}

func (s *DropProcedureStmt) nodeType() string { return "DropProcedureStmt" }
func (s *DropProcedureStmt) statementNode()   {}

// ParamDef represents a procedure parameter
type ParamDef struct {
	Name string
	Type TokenType
}

// CallProcedureStmt represents a CALL statement
type CallProcedureStmt struct {
	Name   string
	Params []Expression
}

func (s *CallProcedureStmt) nodeType() string { return "CallProcedureStmt" }
func (s *CallProcedureStmt) statementNode()   {}

// BeginStmt represents a BEGIN TRANSACTION statement
type BeginStmt struct {
	ReadOnly bool
}

func (s *BeginStmt) nodeType() string { return "BeginStmt" }
func (s *BeginStmt) statementNode()   {}

// CommitStmt represents a COMMIT statement
type CommitStmt struct{}

func (s *CommitStmt) nodeType() string { return "CommitStmt" }
func (s *CommitStmt) statementNode()   {}

// RollbackStmt represents a ROLLBACK statement
type RollbackStmt struct {
	ToSavepoint string // Non-empty for ROLLBACK TO SAVEPOINT name
}

func (s *RollbackStmt) nodeType() string { return "RollbackStmt" }
func (s *RollbackStmt) statementNode()   {}

// SavepointStmt represents a SAVEPOINT name statement
type SavepointStmt struct {
	Name string
}

func (s *SavepointStmt) nodeType() string { return "SavepointStmt" }
func (s *SavepointStmt) statementNode()   {}

// ReleaseSavepointStmt represents a RELEASE SAVEPOINT name statement
type ReleaseSavepointStmt struct {
	Name string
}

func (s *ReleaseSavepointStmt) nodeType() string { return "ReleaseSavepointStmt" }
func (s *ReleaseSavepointStmt) statementNode()   {}

// ColumnDef represents a column definition in CREATE TABLE
type ColumnDef struct {
	Name          string
	Type          TokenType
	NotNull       bool
	Unique        bool
	PrimaryKey    bool
	AutoIncrement bool
	Default       Expression
	Check         Expression // CHECK (expression)
}

// ForeignKeyDef represents a foreign key constraint
type ForeignKeyDef struct {
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	OnDelete          string
	OnUpdate          string
}

// TableRef represents a table reference
type TableRef struct {
	Name         string
	Alias        string
	Subquery     *SelectStmt // non-nil for derived tables: FROM (SELECT ...) AS alias
	SubqueryStmt Statement   // non-nil for derived tables with UNION: FROM (SELECT ... UNION ...) AS alias
}

// JoinClause represents a JOIN clause
type JoinClause struct {
	Type      TokenType // TokenInner, TokenLeft, TokenRight, TokenOuter
	Table     *TableRef
	Condition Expression
}

// OrderByExpr represents an ORDER BY expression
type OrderByExpr struct {
	Expr Expression
	Desc bool
}

// Identifier represents an identifier expression
type Identifier struct {
	Name string
}

func (e *Identifier) nodeType() string { return "Identifier" }
func (e *Identifier) expressionNode()  {}

// QualifiedIdentifier represents a qualified identifier (table.column)
type QualifiedIdentifier struct {
	Table  string
	Column string
}

func (e *QualifiedIdentifier) nodeType() string { return "QualifiedIdentifier" }
func (e *QualifiedIdentifier) expressionNode()  {}

// StringLiteral represents a string literal
type StringLiteral struct {
	Value string
}

func (e *StringLiteral) nodeType() string { return "StringLiteral" }
func (e *StringLiteral) expressionNode()  {}

// NumberLiteral represents a numeric literal
type NumberLiteral struct {
	Value float64
	Raw   string
}

func (e *NumberLiteral) nodeType() string { return "NumberLiteral" }
func (e *NumberLiteral) expressionNode()  {}

// BooleanLiteral represents a boolean literal
type BooleanLiteral struct {
	Value bool
}

func (e *BooleanLiteral) nodeType() string { return "BooleanLiteral" }
func (e *BooleanLiteral) expressionNode()  {}

// NullLiteral represents NULL
type NullLiteral struct{}

func (e *NullLiteral) nodeType() string { return "NullLiteral" }
func (e *NullLiteral) expressionNode()  {}

// BinaryExpr represents a binary expression
type BinaryExpr struct {
	Left     Expression
	Operator TokenType
	Right    Expression
}

func (e *BinaryExpr) nodeType() string { return "BinaryExpr" }
func (e *BinaryExpr) expressionNode()  {}

// UnaryExpr represents a unary expression
type UnaryExpr struct {
	Operator TokenType
	Expr     Expression
}

func (e *UnaryExpr) nodeType() string { return "UnaryExpr" }
func (e *UnaryExpr) expressionNode()  {}

// FunctionCall represents a function call
type FunctionCall struct {
	Name     string
	Args     []Expression
	Distinct bool // for COUNT(DISTINCT col)
}

func (e *FunctionCall) nodeType() string { return "FunctionCall" }
func (e *FunctionCall) expressionNode()  {}

// StarExpr represents * in SELECT *
type StarExpr struct {
	Table string // optional table prefix
}

func (e *StarExpr) nodeType() string { return "StarExpr" }
func (e *StarExpr) expressionNode()  {}

// JSONPathExpr represents a JSON path expression (column->>'path')
type JSONPathExpr struct {
	Column Expression
	Path   string
	AsText bool // ->> vs ->
}

func (e *JSONPathExpr) nodeType() string { return "JSONPathExpr" }
func (e *JSONPathExpr) expressionNode()  {}

// JSONContainsExpr represents a JSON contains expression (column @> value)
type JSONContainsExpr struct {
	Column Expression
	Value  Expression
}

func (e *JSONContainsExpr) nodeType() string { return "JSONContainsExpr" }
func (e *JSONContainsExpr) expressionNode()  {}

// PlaceholderExpr represents a ? placeholder
type PlaceholderExpr struct {
	Index int
}

func (e *PlaceholderExpr) nodeType() string { return "PlaceholderExpr" }
func (e *PlaceholderExpr) expressionNode()  {}

// InExpr represents an IN expression
type InExpr struct {
	Expr     Expression
	List     []Expression
	Not      bool
	Subquery *SelectStmt // For IN (SELECT ...)
}

func (e *InExpr) nodeType() string { return "InExpr" }
func (e *InExpr) expressionNode()  {}

// BetweenExpr represents a BETWEEN expression
type BetweenExpr struct {
	Expr  Expression
	Lower Expression
	Upper Expression
	Not   bool
}

func (e *BetweenExpr) nodeType() string { return "BetweenExpr" }
func (e *BetweenExpr) expressionNode()  {}

// LikeExpr represents a LIKE expression
type LikeExpr struct {
	Expr    Expression
	Pattern Expression
	Not     bool
	Escape  Expression // Optional ESCAPE character
}

func (e *LikeExpr) nodeType() string { return "LikeExpr" }
func (e *LikeExpr) expressionNode()  {}

// IsNullExpr represents an IS NULL expression
type IsNullExpr struct {
	Expr Expression
	Not  bool
}

func (e *IsNullExpr) nodeType() string { return "IsNullExpr" }
func (e *IsNullExpr) expressionNode()  {}

// CastExpr represents a CAST expression
type CastExpr struct {
	Expr     Expression
	DataType TokenType
}

func (e *CastExpr) nodeType() string { return "CastExpr" }
func (e *CastExpr) expressionNode()  {}

// CaseExpr represents a CASE expression
type CaseExpr struct {
	Expr  Expression
	Whens []*WhenClause
	Else  Expression
}

// WhenClause represents a WHEN clause in CASE
type WhenClause struct {
	Condition Expression
	Result    Expression
}

func (e *CaseExpr) nodeType() string { return "CaseExpr" }
func (e *CaseExpr) expressionNode()  {}

// SubqueryExpr represents a subquery expression
type SubqueryExpr struct {
	Query *SelectStmt
}

func (e *SubqueryExpr) nodeType() string { return "SubqueryExpr" }
func (e *SubqueryExpr) expressionNode()  {}

// ExistsExpr represents an EXISTS expression
type ExistsExpr struct {
	Subquery *SelectStmt
	Not      bool
}

func (e *ExistsExpr) nodeType() string { return "ExistsExpr" }
func (e *ExistsExpr) expressionNode()  {}

// WindowExpr represents a window function expression
type WindowExpr struct {
	Function    string         // ROW_NUMBER, RANK, DENSE_RANK, etc.
	Args        []Expression   // Function arguments
	PartitionBy []Expression   // PARTITION BY clause
	OrderBy     []*OrderByExpr // ORDER BY clause
}

func (e *WindowExpr) nodeType() string { return "WindowExpr" }
func (e *WindowExpr) expressionNode()  {}

// WindowSpec represents a window specification (OVER clause)
type WindowSpec struct {
	PartitionBy []Expression
	OrderBy     []*OrderByExpr
}

func (e *WindowSpec) nodeType() string { return "WindowSpec" }
func (e *WindowSpec) expressionNode()  {}

// CTEDef represents a CTE (Common Table Expression) definition
type CTEDef struct {
	Name        string
	Columns     []string // Optional column list
	Query       Statement // *SelectStmt or *UnionStmt (for recursive CTEs)
	IsRecursive bool
}

// SelectStmtWithCTE represents a SELECT statement with CTEs
type SelectStmtWithCTE struct {
	CTEs        []*CTEDef
	IsRecursive bool
	Select      *SelectStmt
}

func (s *SelectStmtWithCTE) nodeType() string { return "SelectStmtWithCTE" }
func (s *SelectStmtWithCTE) statementNode()   {}

// VacuumStmt represents a VACUUM statement
type VacuumStmt struct {
	Table string // Optional table name
}

func (s *VacuumStmt) nodeType() string { return "VacuumStmt" }
func (s *VacuumStmt) statementNode()   {}

// AnalyzeStmt represents an ANALYZE statement
type AnalyzeStmt struct {
	Table string // Optional table name
}

func (s *AnalyzeStmt) nodeType() string { return "AnalyzeStmt" }
func (s *AnalyzeStmt) statementNode()   {}

// CreateFTSIndexStmt represents a CREATE FULLTEXT INDEX statement
type CreateFTSIndexStmt struct {
	IfNotExists bool
	Index       string
	Table       string
	Columns     []string
}

func (s *CreateFTSIndexStmt) nodeType() string { return "CreateFTSIndexStmt" }
func (s *CreateFTSIndexStmt) statementNode()   {}

// MatchExpr represents a MATCH ... AGAINST expression for FTS
type MatchExpr struct {
	Columns []Expression
	Pattern Expression
	Mode    string // BOOLEAN MODE, NATURAL LANGUAGE MODE
}

func (e *MatchExpr) nodeType() string { return "MatchExpr" }
func (e *MatchExpr) expressionNode()  {}

// CreateMaterializedViewStmt represents a CREATE MATERIALIZED VIEW statement
type CreateMaterializedViewStmt struct {
	IfNotExists bool
	Name        string
	Query       *SelectStmt
}

func (s *CreateMaterializedViewStmt) nodeType() string { return "CreateMaterializedViewStmt" }
func (s *CreateMaterializedViewStmt) statementNode()   {}

// DropMaterializedViewStmt represents a DROP MATERIALIZED VIEW statement
type DropMaterializedViewStmt struct {
	IfExists bool
	Name     string
}

func (s *DropMaterializedViewStmt) nodeType() string { return "DropMaterializedViewStmt" }
func (s *DropMaterializedViewStmt) statementNode()   {}

// AliasExpr represents an aliased expression (e.g., SELECT col AS alias)
type AliasExpr struct {
	Expr  Expression
	Alias string
}

func (e *AliasExpr) nodeType() string { return "AliasExpr" }
func (e *AliasExpr) expressionNode()  {}

// RefreshMaterializedViewStmt represents a REFRESH MATERIALIZED VIEW statement
type RefreshMaterializedViewStmt struct {
	Name string
}

func (s *RefreshMaterializedViewStmt) nodeType() string { return "RefreshMaterializedViewStmt" }
func (s *RefreshMaterializedViewStmt) statementNode()   {}

// AlterTableStmt represents ALTER TABLE ADD/DROP/RENAME
type AlterTableStmt struct {
	Table   string
	Action  string // "ADD", "DROP", "RENAME_TABLE", "RENAME_COLUMN"
	Column  ColumnDef
	OldName string // For RENAME COLUMN: old column name
	NewName string // For RENAME TABLE/COLUMN: new name
}

func (s *AlterTableStmt) nodeType() string { return "AlterTableStmt" }
func (s *AlterTableStmt) statementNode()   {}

// ShowTablesStmt represents SHOW TABLES
type ShowTablesStmt struct{}

func (s *ShowTablesStmt) nodeType() string { return "ShowTablesStmt" }
func (s *ShowTablesStmt) statementNode()   {}

// ShowCreateTableStmt represents SHOW CREATE TABLE <name>
type ShowCreateTableStmt struct {
	Table string
}

func (s *ShowCreateTableStmt) nodeType() string { return "ShowCreateTableStmt" }
func (s *ShowCreateTableStmt) statementNode()   {}

// ShowColumnsStmt represents SHOW COLUMNS FROM <table> / DESCRIBE <table>
type ShowColumnsStmt struct {
	Table string
}

func (s *ShowColumnsStmt) nodeType() string { return "ShowColumnsStmt" }
func (s *ShowColumnsStmt) statementNode()   {}

// UseStmt represents USE <database>
type UseStmt struct {
	Database string
}

func (s *UseStmt) nodeType() string { return "UseStmt" }
func (s *UseStmt) statementNode()   {}

// SetVarStmt represents SET <variable> = <value> (MySQL compatibility)
type SetVarStmt struct {
	Variable string
	Value    string
}

func (s *SetVarStmt) nodeType() string { return "SetVarStmt" }
func (s *SetVarStmt) statementNode()   {}

// ShowDatabasesStmt represents SHOW DATABASES
type ShowDatabasesStmt struct{}

func (s *ShowDatabasesStmt) nodeType() string { return "ShowDatabasesStmt" }
func (s *ShowDatabasesStmt) statementNode()   {}

// DescribeStmt represents DESCRIBE <table>
type DescribeStmt struct {
	Table string
}

func (s *DescribeStmt) nodeType() string { return "DescribeStmt" }
func (s *DescribeStmt) statementNode()   {}
