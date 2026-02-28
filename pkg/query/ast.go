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
	Distinct   bool
	Columns    []Expression
	From       *TableRef
	Joins      []*JoinClause
	Where      Expression
	GroupBy    []Expression
	Having     Expression
	OrderBy    []*OrderByExpr
	Limit      Expression
	Offset     Expression
}

func (s *SelectStmt) nodeType() string     { return "SelectStmt" }
func (s *SelectStmt) statementNode()       {}

// InsertStmt represents an INSERT statement
type InsertStmt struct {
	Table   string
	Columns []string
	Values  [][]Expression
}

func (s *InsertStmt) nodeType() string     { return "InsertStmt" }
func (s *InsertStmt) statementNode()       {}

// UpdateStmt represents an UPDATE statement
type UpdateStmt struct {
	Table string
	Set   []*SetClause
	Where Expression
}

func (s *UpdateStmt) nodeType() string     { return "UpdateStmt" }
func (s *UpdateStmt) statementNode()       {}

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

func (s *DeleteStmt) nodeType() string     { return "DeleteStmt" }
func (s *DeleteStmt) statementNode()       {}

// CreateTableStmt represents a CREATE TABLE statement
type CreateTableStmt struct {
	IfNotExists bool
	Table       string
	Columns     []*ColumnDef
}

func (s *CreateTableStmt) nodeType() string     { return "CreateTableStmt" }
func (s *CreateTableStmt) statementNode()       {}

// DropTableStmt represents a DROP TABLE statement
type DropTableStmt struct {
	IfExists bool
	Table    string
}

func (s *DropTableStmt) nodeType() string     { return "DropTableStmt" }
func (s *DropTableStmt) statementNode()       {}

// CreateIndexStmt represents a CREATE INDEX statement
type CreateIndexStmt struct {
	IfNotExists bool
	Index       string
	Table       string
	Columns     []string
	Unique      bool
}

func (s *CreateIndexStmt) nodeType() string     { return "CreateIndexStmt" }
func (s *CreateIndexStmt) statementNode()       {}

// CreateCollectionStmt represents a CREATE COLLECTION statement
type CreateCollectionStmt struct {
	IfNotExists bool
	Name        string
}

func (s *CreateCollectionStmt) nodeType() string     { return "CreateCollectionStmt" }
func (s *CreateCollectionStmt) statementNode()       {}

// BeginStmt represents a BEGIN TRANSACTION statement
type BeginStmt struct {
	ReadOnly bool
}

func (s *BeginStmt) nodeType() string     { return "BeginStmt" }
func (s *BeginStmt) statementNode()       {}

// CommitStmt represents a COMMIT statement
type CommitStmt struct{}

func (s *CommitStmt) nodeType() string     { return "CommitStmt" }
func (s *CommitStmt) statementNode()       {}

// RollbackStmt represents a ROLLBACK statement
type RollbackStmt struct{}

func (s *RollbackStmt) nodeType() string     { return "RollbackStmt" }
func (s *RollbackStmt) statementNode()       {}

// ColumnDef represents a column definition in CREATE TABLE
type ColumnDef struct {
	Name         string
	Type         TokenType
	NotNull      bool
	Unique       bool
	PrimaryKey   bool
	AutoIncrement bool
	Default      Expression
}

// TableRef represents a table reference
type TableRef struct {
	Name  string
	Alias string
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

func (e *Identifier) nodeType() string     { return "Identifier" }
func (e *Identifier) expressionNode()      {}

// QualifiedIdentifier represents a qualified identifier (table.column)
type QualifiedIdentifier struct {
	Table  string
	Column string
}

func (e *QualifiedIdentifier) nodeType() string     { return "QualifiedIdentifier" }
func (e *QualifiedIdentifier) expressionNode()      {}

// StringLiteral represents a string literal
type StringLiteral struct {
	Value string
}

func (e *StringLiteral) nodeType() string     { return "StringLiteral" }
func (e *StringLiteral) expressionNode()      {}

// NumberLiteral represents a numeric literal
type NumberLiteral struct {
	Value float64
	Raw   string
}

func (e *NumberLiteral) nodeType() string     { return "NumberLiteral" }
func (e *NumberLiteral) expressionNode()      {}

// BooleanLiteral represents a boolean literal
type BooleanLiteral struct {
	Value bool
}

func (e *BooleanLiteral) nodeType() string     { return "BooleanLiteral" }
func (e *BooleanLiteral) expressionNode()      {}

// NullLiteral represents NULL
type NullLiteral struct{}

func (e *NullLiteral) nodeType() string     { return "NullLiteral" }
func (e *NullLiteral) expressionNode()      {}

// BinaryExpr represents a binary expression
type BinaryExpr struct {
	Left     Expression
	Operator TokenType
	Right    Expression
}

func (e *BinaryExpr) nodeType() string     { return "BinaryExpr" }
func (e *BinaryExpr) expressionNode()      {}

// UnaryExpr represents a unary expression
type UnaryExpr struct {
	Operator TokenType
	Expr     Expression
}

func (e *UnaryExpr) nodeType() string     { return "UnaryExpr" }
func (e *UnaryExpr) expressionNode()      {}

// FunctionCall represents a function call
type FunctionCall struct {
	Name string
	Args []Expression
}

func (e *FunctionCall) nodeType() string     { return "FunctionCall" }
func (e *FunctionCall) expressionNode()      {}

// StarExpr represents * in SELECT *
type StarExpr struct {
	Table string // optional table prefix
}

func (e *StarExpr) nodeType() string     { return "StarExpr" }
func (e *StarExpr) expressionNode()      {}

// JSONPathExpr represents a JSON path expression (column->>'path')
type JSONPathExpr struct {
	Column Expression
	Path   string
	AsText bool // ->> vs ->
}

func (e *JSONPathExpr) nodeType() string     { return "JSONPathExpr" }
func (e *JSONPathExpr) expressionNode()      {}

// JSONContainsExpr represents a JSON contains expression (column @> value)
type JSONContainsExpr struct {
	Column Expression
	Value  Expression
}

func (e *JSONContainsExpr) nodeType() string     { return "JSONContainsExpr" }
func (e *JSONContainsExpr) expressionNode()      {}

// PlaceholderExpr represents a ? placeholder
type PlaceholderExpr struct {
	Index int
}

func (e *PlaceholderExpr) nodeType() string     { return "PlaceholderExpr" }
func (e *PlaceholderExpr) expressionNode()      {}

// InExpr represents an IN expression
type InExpr struct {
	Expr  Expression
	List  []Expression
	Not   bool
}

func (e *InExpr) nodeType() string     { return "InExpr" }
func (e *InExpr) expressionNode()      {}

// BetweenExpr represents a BETWEEN expression
type BetweenExpr struct {
	Expr  Expression
	Lower Expression
	Upper Expression
	Not   bool
}

func (e *BetweenExpr) nodeType() string     { return "BetweenExpr" }
func (e *BetweenExpr) expressionNode()      {}

// LikeExpr represents a LIKE expression
type LikeExpr struct {
	Expr    Expression
	Pattern Expression
	Not     bool
}

func (e *LikeExpr) nodeType() string     { return "LikeExpr" }
func (e *LikeExpr) expressionNode()      {}

// IsNullExpr represents an IS NULL expression
type IsNullExpr struct {
	Expr Expression
	Not  bool
}

func (e *IsNullExpr) nodeType() string     { return "IsNullExpr" }
func (e *IsNullExpr) expressionNode()      {}

// CastExpr represents a CAST expression
type CastExpr struct {
	Expr     Expression
	DataType TokenType
}

func (e *CastExpr) nodeType() string     { return "CastExpr" }
func (e *CastExpr) expressionNode()      {}

// CaseExpr represents a CASE expression
type CaseExpr struct {
	Expr     Expression
	Whens    []*WhenClause
	Else     Expression
}

// WhenClause represents a WHEN clause in CASE
type WhenClause struct {
	Condition Expression
	Result    Expression
}

func (e *CaseExpr) nodeType() string     { return "CaseExpr" }
func (e *CaseExpr) expressionNode()      {}

// SubqueryExpr represents a subquery expression
type SubqueryExpr struct {
	Query *SelectStmt
}

func (e *SubqueryExpr) nodeType() string     { return "SubqueryExpr" }
func (e *SubqueryExpr) expressionNode()      {}

// ExistsExpr represents an EXISTS expression
type ExistsExpr struct {
	Subquery *SelectStmt
	Not      bool
}

func (e *ExistsExpr) nodeType() string     { return "ExistsExpr" }
func (e *ExistsExpr) expressionNode()      {}
