package query

import (
	"testing"
)

// TestStatementNodeInterfaces covers all statementNode marker methods
func TestStatementNodeInterfaces(t *testing.T) {
	stmts := []Statement{
		&SelectStmt{},
		&UnionStmt{},
		&InsertStmt{},
		&UpdateStmt{},
		&DeleteStmt{},
		&CreateTableStmt{},
		&DropTableStmt{},
		&CreateIndexStmt{},
		&DropIndexStmt{},
		&CreateCollectionStmt{},
		&CreateViewStmt{},
		&DropViewStmt{},
		&CreateTriggerStmt{},
		&DropTriggerStmt{},
		&CreateProcedureStmt{},
		&DropProcedureStmt{},
		&CreatePolicyStmt{},
		&DropPolicyStmt{},
		&CallProcedureStmt{},
		&BeginStmt{},
		&CommitStmt{},
		&RollbackStmt{},
		&SavepointStmt{},
		&ReleaseSavepointStmt{},
		&VacuumStmt{},
		&AnalyzeStmt{},
		&CreateFTSIndexStmt{},
		&CreateMaterializedViewStmt{},
		&DropMaterializedViewStmt{},
		&RefreshMaterializedViewStmt{},
		&AlterTableStmt{},
		&ShowTablesStmt{},
		&ShowCreateTableStmt{},
		&ShowColumnsStmt{},
		&UseStmt{},
		&SetVarStmt{},
		&ShowDatabasesStmt{},
		&DescribeStmt{},
		&ExplainStmt{},
	}

	for _, stmt := range stmts {
		// This calls the marker method
		stmt.statementNode()
	}
}

// TestExpressionNodeInterfaces covers all expressionNode marker methods
func TestExpressionNodeInterfaces(t *testing.T) {
	exprs := []Expression{
		&Identifier{},
		&QualifiedIdentifier{},
		&StringLiteral{},
		&NumberLiteral{},
		&BooleanLiteral{},
		&NullLiteral{},
		&BinaryExpr{},
		&UnaryExpr{},
		&FunctionCall{},
		&StarExpr{},
		&JSONPathExpr{},
		&JSONContainsExpr{},
		&PlaceholderExpr{},
		&InExpr{},
		&BetweenExpr{},
		&LikeExpr{},
		&IsNullExpr{},
		&CastExpr{},
		&CaseExpr{},
		&SubqueryExpr{},
		&ExistsExpr{},
		&WindowExpr{},
		&WindowSpec{},
		&MatchExpr{},
		&AliasExpr{},
	}

	for _, expr := range exprs {
		// This calls the marker method
		expr.expressionNode()
	}
}
