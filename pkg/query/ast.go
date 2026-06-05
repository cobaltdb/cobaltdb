package query

import (
	"fmt"
	"strconv"
	"strings"
)

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
	Evaluate(Evaluator) (interface{}, error)
	// AcceptVisitor dispatches to the appropriate visitor method
	AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{}
}

// ExpressionVisitor defines the visitor pattern interface for traversing Expression AST nodes.
// Each method corresponds to an expression type. Return value allows visitors to short-circuit
// or transform the AST during traversal.
type ExpressionVisitor interface {
	// Visit methods return the modified expression (for transforms) or the original (for traversal).
	// To skip children, return nil. To continue traversal, return the expression unchanged.
	VisitBinaryExpr(expr *BinaryExpr, ctx interface{}) interface{}
	VisitUnaryExpr(expr *UnaryExpr, ctx interface{}) interface{}
	VisitFunctionCall(expr *FunctionCall, ctx interface{}) interface{}
	VisitIdentifier(expr *Identifier, ctx interface{}) interface{}
	VisitQualifiedIdentifier(expr *QualifiedIdentifier, ctx interface{}) interface{}
	VisitColumnRef(expr *ColumnRef, ctx interface{}) interface{}
	VisitStringLiteral(expr *StringLiteral, ctx interface{}) interface{}
	VisitNumberLiteral(expr *NumberLiteral, ctx interface{}) interface{}
	VisitBooleanLiteral(expr *BooleanLiteral, ctx interface{}) interface{}
	VisitNullLiteral(expr *NullLiteral, ctx interface{}) interface{}
	VisitVectorLiteral(expr *VectorLiteral, ctx interface{}) interface{}
	VisitPlaceholder(expr *PlaceholderExpr, ctx interface{}) interface{}
	VisitInExpr(expr *InExpr, ctx interface{}) interface{}
	VisitBetweenExpr(expr *BetweenExpr, ctx interface{}) interface{}
	VisitLikeExpr(expr *LikeExpr, ctx interface{}) interface{}
	VisitIsNullExpr(expr *IsNullExpr, ctx interface{}) interface{}
	VisitCastExpr(expr *CastExpr, ctx interface{}) interface{}
	VisitCaseExpr(expr *CaseExpr, ctx interface{}) interface{}
	VisitSubqueryExpr(expr *SubqueryExpr, ctx interface{}) interface{}
	VisitExistsExpr(expr *ExistsExpr, ctx interface{}) interface{}
	VisitStarExpr(expr *StarExpr, ctx interface{}) interface{}
	VisitJSONPathExpr(expr *JSONPathExpr, ctx interface{}) interface{}
	VisitJSONContainsExpr(expr *JSONContainsExpr, ctx interface{}) interface{}
	VisitAliasExpr(expr *AliasExpr, ctx interface{}) interface{}
	VisitMatchExpr(expr *MatchExpr, ctx interface{}) interface{}
	VisitWindowExpr(expr *WindowExpr, ctx interface{}) interface{}
	VisitWindowSpec(expr *WindowSpec, ctx interface{}) interface{}
}

// Walk traverses an Expression AST, calling the appropriate visitor method for each node.
// The ctx parameter is passed through to visitor methods and can carry state.
// If a visitor method returns a non-nil expression, Walk uses it instead of the original
// (enabling in-place transformations). If a visitor method returns nil, children are skipped.
func Walk(expr Expression, v ExpressionVisitor, ctx interface{}) {
	if expr == nil {
		return
	}
	result := expr.AcceptVisitor(v, ctx)
	if result == nil {
		return // visitor chose to skip children
	}
	walkChildren(result, v, ctx)
}

// walkChildren recursively walks the children of a visited expression node.
// This is called after AcceptVisitor; the returned expression may be transformed.
func walkChildren(expr interface{}, v ExpressionVisitor, ctx interface{}) {
	switch e := expr.(type) {
	case *BinaryExpr:
		Walk(e.Left, v, ctx)
		Walk(e.Right, v, ctx)
	case *UnaryExpr:
		Walk(e.Expr, v, ctx)
	case *FunctionCall:
		for _, arg := range e.Args {
			Walk(arg, v, ctx)
		}
	case *InExpr:
		Walk(e.Expr, v, ctx)
		if e.Subquery == nil {
			for _, item := range e.List {
				Walk(item, v, ctx)
			}
		}
	case *BetweenExpr:
		Walk(e.Expr, v, ctx)
		Walk(e.Lower, v, ctx)
		Walk(e.Upper, v, ctx)
	case *LikeExpr:
		Walk(e.Expr, v, ctx)
		Walk(e.Pattern, v, ctx)
		if e.Escape != nil {
			Walk(e.Escape, v, ctx)
		}
	case *IsNullExpr:
		Walk(e.Expr, v, ctx)
	case *CastExpr:
		Walk(e.Expr, v, ctx)
	case *CaseExpr:
		if e.Expr != nil {
			Walk(e.Expr, v, ctx)
		}
		for _, w := range e.Whens {
			Walk(w.Condition, v, ctx)
			Walk(w.Result, v, ctx)
		}
		if e.Else != nil {
			Walk(e.Else, v, ctx)
		}
	case *SubqueryExpr:
		if e.Query != nil {
			WalkSelectStmt(e.Query, v, ctx)
		}
	case *ExistsExpr:
		if e.Subquery != nil {
			WalkSelectStmt(e.Subquery, v, ctx)
		}
	case *AliasExpr:
		Walk(e.Expr, v, ctx)
	case *JSONPathExpr:
		Walk(e.Column, v, ctx)
	case *JSONContainsExpr:
		Walk(e.Column, v, ctx)
		Walk(e.Value, v, ctx)
	case *MatchExpr:
		for _, col := range e.Columns {
			Walk(col, v, ctx)
		}
		Walk(e.Pattern, v, ctx)
	}
}

// WalkSelectStmt walks all expressions in a SELECT statement.
func WalkSelectStmt(stmt *SelectStmt, v ExpressionVisitor, ctx interface{}) {
	if stmt == nil {
		return
	}
	for _, col := range stmt.Columns {
		Walk(col, v, ctx)
	}
	for _, j := range stmt.Joins {
		Walk(j.Condition, v, ctx)
	}
	if stmt.Where != nil {
		Walk(stmt.Where, v, ctx)
	}
	for _, gb := range stmt.GroupBy {
		Walk(gb, v, ctx)
	}
	if stmt.Having != nil {
		Walk(stmt.Having, v, ctx)
	}
	for _, ob := range stmt.OrderBy {
		Walk(ob.Expr, v, ctx)
	}
	if stmt.Limit != nil {
		Walk(stmt.Limit, v, ctx)
	}
	if stmt.Offset != nil {
		Walk(stmt.Offset, v, ctx)
	}
}

// AcceptVisitor implementations for all Expression types
func (e *BinaryExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitBinaryExpr(e, ctx)
}
func (e *UnaryExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitUnaryExpr(e, ctx)
}
func (e *FunctionCall) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitFunctionCall(e, ctx)
}
func (e *Identifier) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitIdentifier(e, ctx)
}
func (e *QualifiedIdentifier) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitQualifiedIdentifier(e, ctx)
}
func (e *ColumnRef) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitColumnRef(e, ctx)
}
func (e *StringLiteral) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitStringLiteral(e, ctx)
}
func (e *NumberLiteral) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitNumberLiteral(e, ctx)
}
func (e *BooleanLiteral) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitBooleanLiteral(e, ctx)
}
func (e *NullLiteral) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitNullLiteral(e, ctx)
}
func (e *VectorLiteral) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitVectorLiteral(e, ctx)
}
func (e *PlaceholderExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitPlaceholder(e, ctx)
}
func (e *InExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitInExpr(e, ctx)
}
func (e *BetweenExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitBetweenExpr(e, ctx)
}
func (e *LikeExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitLikeExpr(e, ctx)
}
func (e *IsNullExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitIsNullExpr(e, ctx)
}
func (e *CastExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitCastExpr(e, ctx)
}
func (e *CaseExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitCaseExpr(e, ctx)
}
func (e *SubqueryExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitSubqueryExpr(e, ctx)
}
func (e *ExistsExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitExistsExpr(e, ctx)
}
func (e *StarExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitStarExpr(e, ctx)
}
func (e *JSONPathExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitJSONPathExpr(e, ctx)
}
func (e *JSONContainsExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitJSONContainsExpr(e, ctx)
}
func (e *AliasExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitAliasExpr(e, ctx)
}
func (e *MatchExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitMatchExpr(e, ctx)
}
func (e *WindowExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitWindowExpr(e, ctx)
}
func (e *WindowSpec) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitWindowSpec(e, ctx)
}

// Evaluator is implemented by the catalog to evaluate expression AST nodes.
// Defined in the query package to avoid circular dependencies.
type Evaluator interface {
	EvalBinaryExpr(left, right interface{}, op TokenType) (interface{}, error)
	EvalUnaryExpr(val interface{}, op TokenType) (interface{}, error)
	EvalIdentifier(name string) (interface{}, error)
	EvalQualifiedIdentifier(table, column string) (interface{}, error)
	EvalPlaceholder(index int) (interface{}, error)
	EvalLike(val, pattern, escape interface{}, not bool) (interface{}, error)
	EvalIn(val interface{}, list []interface{}, not bool) (interface{}, error)
	EvalInSubquery(val interface{}, q *SelectStmt, not bool) (interface{}, error)
	EvalBetween(val, lower, upper interface{}, not bool) (interface{}, error)
	EvalIsNull(val interface{}, not bool) (bool, error)
	EvalFunctionCall(name string, args []interface{}, distinct bool) (interface{}, error)
	EvalAlias(inner interface{}) (interface{}, error)
	EvalCase(expr interface{}, whens [][2]interface{}, elseVal interface{}) (interface{}, error)
	EvalCast(val interface{}, dataType TokenType) (interface{}, error)
	EvalSubquery(q *SelectStmt) (interface{}, error)
	EvalExists(q *SelectStmt, not bool) (bool, error)
	EvalJSONPath(jsonVal interface{}, path string, asText bool) (interface{}, error)
	EvalJSONContains(jsonVal, val interface{}) (bool, error)
	EvalMatch(expr *MatchExpr, row []interface{}) (interface{}, error)
	EvalStar(table string) (interface{}, error)
	EvalColumnRef(table, column string) (interface{}, error)
	EvalWindow(w *WindowExpr) (interface{}, error)
}

// TemporalExpr represents AS OF expression for temporal queries
type TemporalExpr struct {
	Timestamp Expression // Literal timestamp or expression like '2024-01-15'
	IsSystem  bool       // True for AS OF SYSTEM TIME expr
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
	AsOf     *TemporalExpr // AS OF clause for temporal queries
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
	All     bool      // ALL variant (no deduplication)
	Op      SetOpType // UNION, INTERSECT, or EXCEPT
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
	Select         *SelectStmt       // For INSERT INTO ... SELECT ...
	ConflictAction ConflictAction    // OR REPLACE / OR IGNORE
	OnConflict     *OnConflictClause // ON CONFLICT (...) DO NOTHING|UPDATE
	Returning      []Expression      // RETURNING clause expressions
}

// OnConflictClause represents `ON CONFLICT [(cols)] DO NOTHING | DO UPDATE SET ...`.
// DoUpdate == nil means DO NOTHING; otherwise it holds the UPDATE assignments.
type OnConflictClause struct {
	Columns  []string // conflict target columns (optional)
	DoUpdate []*SetClause
}

func (s *InsertStmt) nodeType() string { return "InsertStmt" }
func (s *InsertStmt) statementNode()   {}

// UpdateStmt represents an UPDATE statement
type UpdateStmt struct {
	Table     string // Target table to update
	Set       []*SetClause
	From      *TableRef // Optional FROM clause for UPDATE with JOIN
	Joins     []*JoinClause
	Where     Expression
	Returning []Expression // RETURNING clause expressions
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
	Table     string      // Target table to delete from
	Alias     string      // Optional table alias
	Using     []*TableRef // USING clause for DELETE with JOIN
	Where     Expression
	Returning []Expression // RETURNING clause expressions
}

func (s *DeleteStmt) nodeType() string { return "DeleteStmt" }
func (s *DeleteStmt) statementNode()   {}

// PartitionType represents the type of partitioning
type PartitionType int

const (
	PartitionTypeNone PartitionType = iota
	PartitionTypeRange
	PartitionTypeList
	PartitionTypeHash
	PartitionTypeKey
)

// PartitionDef represents table partitioning definition
type PartitionDef struct {
	Type          PartitionType
	Column        string             // Column to partition by
	Expression    Expression         // Optional expression for partitioning
	Partitions    []*SinglePartition // Individual partition definitions
	NumPartitions int                // For HASH/KEY partitioning
}

// SinglePartition represents a single partition definition
type SinglePartition struct {
	Name   string
	Values []Expression // For RANGE/LIST (e.g., LESS THAN (100), IN (1,2,3))
}

// CreateTableStmt represents a CREATE TABLE statement
type CreateTableStmt struct {
	IfNotExists bool
	Table       string
	Columns     []*ColumnDef
	PrimaryKey  []string // Table-level PRIMARY KEY (col1, col2, ...) for composite PK
	ForeignKeys []*ForeignKeyDef
	Partition   *PartitionDef // Table partitioning definition
	AsSelect    Statement     // CREATE TABLE ... AS SELECT ... (CTAS); nil otherwise
	// UniqueConstraints holds table-level UNIQUE (col, ...) constraint column sets.
	UniqueConstraints [][]string
}

func (s *CreateTableStmt) nodeType() string { return "CreateTableStmt" }
func (s *CreateTableStmt) statementNode()   {}

// CreateForeignTableStmt represents a CREATE FOREIGN TABLE statement
type CreateForeignTableStmt struct {
	IfNotExists bool
	Table       string
	Columns     []*ColumnDef
	Wrapper     string
	Options     map[string]string
}

func (s *CreateForeignTableStmt) nodeType() string { return "CreateForeignTableStmt" }
func (s *CreateForeignTableStmt) statementNode()   {}

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

// CreateVectorIndexStmt represents a CREATE VECTOR INDEX statement for HNSW indexes
type CreateVectorIndexStmt struct {
	IfNotExists bool
	Index       string
	Table       string
	Column      string
}

func (s *CreateVectorIndexStmt) nodeType() string { return "CreateVectorIndexStmt" }
func (s *CreateVectorIndexStmt) statementNode()   {}

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
	RawSQL      string
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
	Time        string     // BEFORE, AFTER
	Event       string     // INSERT, UPDATE, DELETE
	Condition   Expression // WHEN condition (optional)
	Body        []Statement
	RawSQL      string
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
	RawSQL      string
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

// CreatePolicyStmt represents a CREATE POLICY statement for row-level security
type CreatePolicyStmt struct {
	Name       string     // Policy name
	Table      string     // Table name
	Permissive bool       // true = PERMISSIVE (default), false = RESTRICTIVE
	Event      string     // ALL, SELECT, INSERT, UPDATE, DELETE
	Using      Expression // USING expression (for SELECT/UPDATE/DELETE)
	WithCheck  Expression // WITH CHECK expression (for INSERT/UPDATE)
	ForRoles   []string   // Roles this policy applies to (empty = all)
}

func (s *CreatePolicyStmt) nodeType() string { return "CreatePolicyStmt" }
func (s *CreatePolicyStmt) statementNode()   {}

// DropPolicyStmt represents a DROP POLICY statement
type DropPolicyStmt struct {
	IfExists bool
	Name     string
	Table    string
}

func (s *DropPolicyStmt) nodeType() string { return "DropPolicyStmt" }
func (s *DropPolicyStmt) statementNode()   {}

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
	Check         Expression     // CHECK (expression)
	Dimensions    int            // For VECTOR type: number of dimensions
	ForeignKey    *ForeignKeyDef // inline column-level REFERENCES constraint
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
	IndexHint    string      // hint for index usage (e.g., "auto", "primary", "idx_name")
}

// JoinClause represents a JOIN clause
type JoinClause struct {
	Type      TokenType // TokenInner, TokenLeft, TokenRight, TokenOuter, TokenCross
	Table     *TableRef
	Condition Expression
	Using     []string // Column names for USING clause (alternative to ON)
	Natural   bool     // NATURAL JOIN - match columns with same name automatically
}

// OrderByExpr represents an ORDER BY expression
type OrderByExpr struct {
	Expr Expression
	Desc bool
	// NullsFirst is honored only when NullsSpecified is true; otherwise NULL
	// placement defaults to last for ASC and first for DESC.
	NullsFirst     bool
	NullsSpecified bool
}

// Identifier represents an identifier expression
// DefaultExpr is the `DEFAULT` keyword used as a value in INSERT ... VALUES.
// The insert path resolves it to the column's defined default.
type DefaultExpr struct{}

func (e *DefaultExpr) nodeType() string { return "DefaultExpr" }
func (e *DefaultExpr) expressionNode()  {}
func (e *DefaultExpr) Evaluate(Evaluator) (interface{}, error) {
	// Resolved by the insert path; evaluating elsewhere yields NULL.
	return nil, nil
}

// AcceptVisitor treats DEFAULT as NULL for any visitor; the insert path resolves
// it to the column default before evaluation, so visitors never see it in
// practice.
func (e *DefaultExpr) AcceptVisitor(v ExpressionVisitor, ctx interface{}) interface{} {
	return v.VisitNullLiteral(&NullLiteral{}, ctx)
}

type Identifier struct {
	Name string
}

func (e *Identifier) nodeType() string { return "Identifier" }
func (e *Identifier) expressionNode()  {}
func (e *Identifier) Evaluate(ev Evaluator) (interface{}, error) {
	if dotIdx := strings.IndexByte(e.Name, '.'); dotIdx > 0 && dotIdx < len(e.Name)-1 {
		return ev.EvalQualifiedIdentifier(e.Name[:dotIdx], e.Name[dotIdx+1:])
	}
	return ev.EvalIdentifier(e.Name)
}

// QualifiedIdentifier represents a qualified identifier (table.column)
type QualifiedIdentifier struct {
	Table  string
	Column string
}

func (e *QualifiedIdentifier) nodeType() string { return "QualifiedIdentifier" }
func (e *QualifiedIdentifier) expressionNode()  {}
func (e *QualifiedIdentifier) Evaluate(ev Evaluator) (interface{}, error) {
	return ev.EvalQualifiedIdentifier(e.Table, e.Column)
}

// ColumnRef represents a column reference (can be used for RETURNING *)
type ColumnRef struct {
	Table  string // Optional table name
	Column string // Column name or "*"
}

func (e *ColumnRef) nodeType() string { return "ColumnRef" }
func (e *ColumnRef) expressionNode()  {}
func (e *ColumnRef) Evaluate(ev Evaluator) (interface{}, error) {
	return ev.EvalColumnRef(e.Table, e.Column)
}

// StringLiteral represents a string literal
type StringLiteral struct {
	Value string
}

func (e *StringLiteral) nodeType() string                        { return "StringLiteral" }
func (e *StringLiteral) expressionNode()                         {}
func (e *StringLiteral) Evaluate(Evaluator) (interface{}, error) { return e.Value, nil }

// NumberLiteral represents a numeric literal
type NumberLiteral struct {
	Value float64
	Raw   string
}

func (e *NumberLiteral) nodeType() string { return "NumberLiteral" }
func (e *NumberLiteral) expressionNode()  {}
func (e *NumberLiteral) Evaluate(Evaluator) (interface{}, error) {
	// Preserve full int64 precision for integer literals; float64 can only hold
	// integers exactly up to 2^53, so large INTEGER values would otherwise be
	// silently corrupted.
	if e.Raw != "" && !strings.ContainsAny(e.Raw, ".eE") {
		if i, err := strconv.ParseInt(e.Raw, 10, 64); err == nil {
			return i, nil
		}
	}
	return e.Value, nil
}

// BooleanLiteral represents a boolean literal
type BooleanLiteral struct {
	Value bool
}

func (e *BooleanLiteral) nodeType() string                        { return "BooleanLiteral" }
func (e *BooleanLiteral) expressionNode()                         {}
func (e *BooleanLiteral) Evaluate(Evaluator) (interface{}, error) { return e.Value, nil }

// NullLiteral represents NULL
type NullLiteral struct{}

func (e *NullLiteral) nodeType() string                        { return "NullLiteral" }
func (e *NullLiteral) expressionNode()                         {}
func (e *NullLiteral) Evaluate(Evaluator) (interface{}, error) { return nil, nil }

// VectorLiteral represents a vector literal [0.1, 0.2, 0.3, ...]
type VectorLiteral struct {
	Values []float64
}

func (e *VectorLiteral) nodeType() string                        { return "VectorLiteral" }
func (e *VectorLiteral) expressionNode()                         {}
func (e *VectorLiteral) Evaluate(Evaluator) (interface{}, error) { return e.Values, nil }

// BinaryExpr represents a binary expression
type BinaryExpr struct {
	Left     Expression
	Operator TokenType
	Right    Expression
}

func (e *BinaryExpr) nodeType() string { return "BinaryExpr" }
func (e *BinaryExpr) expressionNode()  {}
func (e *BinaryExpr) Evaluate(ev Evaluator) (interface{}, error) {
	left, err := e.Left.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	return ev.EvalBinaryExpr(left, right, e.Operator)
}

// UnaryExpr represents a unary expression
type UnaryExpr struct {
	Operator TokenType
	Expr     Expression
}

func (e *UnaryExpr) nodeType() string { return "UnaryExpr" }
func (e *UnaryExpr) expressionNode()  {}
func (e *UnaryExpr) Evaluate(ev Evaluator) (interface{}, error) {
	val, err := e.Expr.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	return ev.EvalUnaryExpr(val, e.Operator)
}

// FunctionCall represents a function call
type FunctionCall struct {
	Name     string
	Args     []Expression
	Distinct bool // for COUNT(DISTINCT col)
}

func (e *FunctionCall) nodeType() string { return "FunctionCall" }
func (e *FunctionCall) expressionNode()  {}
func (e *FunctionCall) Evaluate(ev Evaluator) (interface{}, error) {
	args := make([]interface{}, len(e.Args))
	for i, arg := range e.Args {
		v, err := arg.Evaluate(ev)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	return ev.EvalFunctionCall(e.Name, args, e.Distinct)
}

// StarExpr represents * in SELECT *
type StarExpr struct {
	Table string // optional table prefix
}

func (e *StarExpr) nodeType() string { return "StarExpr" }
func (e *StarExpr) expressionNode()  {}
func (e *StarExpr) Evaluate(ev Evaluator) (interface{}, error) {
	return ev.EvalStar(e.Table)
}

// JSONPathExpr represents a JSON path expression (column->>'path')
type JSONPathExpr struct {
	Column Expression
	Path   string
	AsText bool // ->> vs ->
}

func (e *JSONPathExpr) nodeType() string { return "JSONPathExpr" }
func (e *JSONPathExpr) expressionNode()  {}
func (e *JSONPathExpr) Evaluate(ev Evaluator) (interface{}, error) {
	val, err := e.Column.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	return ev.EvalJSONPath(val, e.Path, e.AsText)
}

// JSONContainsExpr represents a JSON contains expression (column @> value)
type JSONContainsExpr struct {
	Column Expression
	Value  Expression
}

func (e *JSONContainsExpr) nodeType() string { return "JSONContainsExpr" }
func (e *JSONContainsExpr) expressionNode()  {}
func (e *JSONContainsExpr) Evaluate(ev Evaluator) (interface{}, error) {
	jsonVal, err := e.Column.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	val, err := e.Value.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	ok, err := ev.EvalJSONContains(jsonVal, val)
	return ok, err
}

// PlaceholderExpr represents a ? placeholder
type PlaceholderExpr struct {
	Index int
}

func (e *PlaceholderExpr) nodeType() string { return "PlaceholderExpr" }
func (e *PlaceholderExpr) expressionNode()  {}
func (e *PlaceholderExpr) Evaluate(ev Evaluator) (interface{}, error) {
	return ev.EvalPlaceholder(e.Index)
}

// InExpr represents an IN expression
type InExpr struct {
	Expr     Expression
	List     []Expression
	Not      bool
	Subquery *SelectStmt // For IN (SELECT ...)
}

func (e *InExpr) nodeType() string { return "InExpr" }
func (e *InExpr) expressionNode()  {}
func (e *InExpr) Evaluate(ev Evaluator) (interface{}, error) {
	val, err := e.Expr.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	if e.Subquery != nil {
		return ev.EvalInSubquery(val, e.Subquery, e.Not)
	}
	list := make([]interface{}, len(e.List))
	for i, item := range e.List {
		v, err := item.Evaluate(ev)
		if err != nil {
			return nil, err
		}
		list[i] = v
	}
	return ev.EvalIn(val, list, e.Not)
}

// BetweenExpr represents a BETWEEN expression
type BetweenExpr struct {
	Expr  Expression
	Lower Expression
	Upper Expression
	Not   bool
}

func (e *BetweenExpr) nodeType() string { return "BetweenExpr" }
func (e *BetweenExpr) expressionNode()  {}
func (e *BetweenExpr) Evaluate(ev Evaluator) (interface{}, error) {
	val, err := e.Expr.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	lower, err := e.Lower.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	upper, err := e.Upper.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	return ev.EvalBetween(val, lower, upper, e.Not)
}

// LikeExpr represents a LIKE expression
type LikeExpr struct {
	Expr    Expression
	Pattern Expression
	Not     bool
	Escape  Expression // Optional ESCAPE character
}

func (e *LikeExpr) nodeType() string { return "LikeExpr" }
func (e *LikeExpr) expressionNode()  {}
func (e *LikeExpr) Evaluate(ev Evaluator) (interface{}, error) {
	val, err := e.Expr.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	pattern, err := e.Pattern.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	var escape interface{}
	if e.Escape != nil {
		escape, err = e.Escape.Evaluate(ev)
		if err != nil {
			return nil, err
		}
	}
	return ev.EvalLike(val, pattern, escape, e.Not)
}

// IsNullExpr represents an IS NULL expression
type IsNullExpr struct {
	Expr Expression
	Not  bool
}

func (e *IsNullExpr) nodeType() string { return "IsNullExpr" }
func (e *IsNullExpr) expressionNode()  {}
func (e *IsNullExpr) Evaluate(ev Evaluator) (interface{}, error) {
	val, err := e.Expr.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	ok, err := ev.EvalIsNull(val, e.Not)
	return ok, err
}

// CastExpr represents a CAST expression
type CastExpr struct {
	Expr     Expression
	DataType TokenType
}

func (e *CastExpr) nodeType() string { return "CastExpr" }
func (e *CastExpr) expressionNode()  {}
func (e *CastExpr) Evaluate(ev Evaluator) (interface{}, error) {
	val, err := e.Expr.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	return ev.EvalCast(val, e.DataType)
}

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
func (e *CaseExpr) Evaluate(ev Evaluator) (interface{}, error) {
	var exprVal interface{}
	var err error
	if e.Expr != nil {
		exprVal, err = e.Expr.Evaluate(ev)
		if err != nil {
			return nil, err
		}
	}
	whens := make([][2]interface{}, len(e.Whens))
	for i, w := range e.Whens {
		cond, err := w.Condition.Evaluate(ev)
		if err != nil {
			return nil, err
		}
		result, err := w.Result.Evaluate(ev)
		if err != nil {
			return nil, err
		}
		whens[i] = [2]interface{}{cond, result}
	}
	var elseVal interface{}
	if e.Else != nil {
		elseVal, err = e.Else.Evaluate(ev)
		if err != nil {
			return nil, err
		}
	}
	return ev.EvalCase(exprVal, whens, elseVal)
}

// SubqueryExpr represents a subquery expression
type SubqueryExpr struct {
	Query *SelectStmt
}

func (e *SubqueryExpr) nodeType() string { return "SubqueryExpr" }
func (e *SubqueryExpr) expressionNode()  {}
func (e *SubqueryExpr) Evaluate(ev Evaluator) (interface{}, error) {
	return ev.EvalSubquery(e.Query)
}

// ExistsExpr represents an EXISTS expression
type ExistsExpr struct {
	Subquery *SelectStmt
	Not      bool
}

func (e *ExistsExpr) nodeType() string { return "ExistsExpr" }
func (e *ExistsExpr) expressionNode()  {}
func (e *ExistsExpr) Evaluate(ev Evaluator) (interface{}, error) {
	ok, err := ev.EvalExists(e.Subquery, e.Not)
	return ok, err
}

// WindowExpr represents a window function expression
type WindowExpr struct {
	Function    string         // ROW_NUMBER, RANK, DENSE_RANK, etc.
	Args        []Expression   // Function arguments
	PartitionBy []Expression   // PARTITION BY clause
	OrderBy     []*OrderByExpr // ORDER BY clause
	Frame       *WindowFrame   // optional ROWS/RANGE frame clause
}

// WindowFrame represents a window frame clause: ROWS|RANGE BETWEEN start AND end.
type WindowFrame struct {
	Mode  string // "ROWS" or "RANGE"
	Start *WindowFrameBound
	End   *WindowFrameBound
}

// WindowFrameBound represents one bound of a window frame.
type WindowFrameBound struct {
	// Type is one of: "UNBOUNDED_PRECEDING", "PRECEDING", "CURRENT_ROW",
	// "FOLLOWING", "UNBOUNDED_FOLLOWING".
	Type   string
	Offset int // row offset for PRECEDING/FOLLOWING
}

func (e *WindowExpr) nodeType() string { return "WindowExpr" }
func (e *WindowExpr) expressionNode()  {}
func (e *WindowExpr) Evaluate(ev Evaluator) (interface{}, error) {
	return ev.EvalWindow(e)
}

// CollectWindowExprs appends every WindowExpr nested anywhere within expr to out.
func CollectWindowExprs(expr Expression, out *[]*WindowExpr) {
	switch e := expr.(type) {
	case nil:
		return
	case *WindowExpr:
		*out = append(*out, e)
	case *AliasExpr:
		CollectWindowExprs(e.Expr, out)
	case *BinaryExpr:
		CollectWindowExprs(e.Left, out)
		CollectWindowExprs(e.Right, out)
	case *UnaryExpr:
		CollectWindowExprs(e.Expr, out)
	case *FunctionCall:
		for _, a := range e.Args {
			CollectWindowExprs(a, out)
		}
	case *CastExpr:
		CollectWindowExprs(e.Expr, out)
	case *CaseExpr:
		CollectWindowExprs(e.Expr, out)
		for _, w := range e.Whens {
			CollectWindowExprs(w.Condition, out)
			CollectWindowExprs(w.Result, out)
		}
		CollectWindowExprs(e.Else, out)
	}
}

// ExprContainsWindow reports whether expr contains a window function anywhere.
func ExprContainsWindow(expr Expression) bool {
	var ws []*WindowExpr
	CollectWindowExprs(expr, &ws)
	return len(ws) > 0
}

// WindowSpec represents a window specification (OVER clause)
type WindowSpec struct {
	PartitionBy []Expression
	OrderBy     []*OrderByExpr
	Frame       *WindowFrame
}

func (e *WindowSpec) nodeType() string { return "WindowSpec" }
func (e *WindowSpec) expressionNode()  {}
func (e *WindowSpec) Evaluate(Evaluator) (interface{}, error) {
	return nil, fmt.Errorf("window spec cannot be used as expression")
}

// CTEDef represents a CTE (Common Table Expression) definition
type CTEDef struct {
	Name        string
	Columns     []string  // Optional column list
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
func (e *MatchExpr) Evaluate(ev Evaluator) (interface{}, error) {
	return ev.EvalMatch(e, nil)
}

// CreateMaterializedViewStmt represents a CREATE MATERIALIZED VIEW statement
type CreateMaterializedViewStmt struct {
	IfNotExists bool
	Name        string
	Query       *SelectStmt
	RawSQL      string
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
func (e *AliasExpr) Evaluate(ev Evaluator) (interface{}, error) {
	inner, err := e.Expr.Evaluate(ev)
	if err != nil {
		return nil, err
	}
	return ev.EvalAlias(inner)
}

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

// ShowIndexStmt represents SHOW INDEX|INDEXES|KEYS FROM table.
type ShowIndexStmt struct {
	Table string
}

func (s *ShowIndexStmt) nodeType() string { return "ShowIndexStmt" }
func (s *ShowIndexStmt) statementNode()   {}

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

// ExplainStmt represents EXPLAIN <query>
type ExplainStmt struct {
	Statement Statement // The statement to explain
}

func (s *ExplainStmt) nodeType() string { return "ExplainStmt" }
func (s *ExplainStmt) statementNode()   {}
