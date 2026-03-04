package query

import (
	"testing"
)

// TestSelectStmtNodeType tests SelectStmt nodeType method
func TestSelectStmtNodeType(t *testing.T) {
	stmt := &SelectStmt{}
	if stmt.nodeType() != "SelectStmt" {
		t.Errorf("Expected nodeType 'SelectStmt', got %s", stmt.nodeType())
	}
}

// TestSelectStmtStatementNode tests SelectStmt statementNode method
func TestSelectStmtStatementNode(t *testing.T) {
	stmt := &SelectStmt{}
	// statementNode is a marker method, just ensure it doesn't panic
	stmt.statementNode()
}

// TestInsertStmtNodeType tests InsertStmt nodeType method
func TestInsertStmtNodeType(t *testing.T) {
	stmt := &InsertStmt{}
	if stmt.nodeType() != "InsertStmt" {
		t.Errorf("Expected nodeType 'InsertStmt', got %s", stmt.nodeType())
	}
}

// TestInsertStmtStatementNode tests InsertStmt statementNode method
func TestInsertStmtStatementNode(t *testing.T) {
	stmt := &InsertStmt{}
	stmt.statementNode()
}

// TestUpdateStmtNodeType tests UpdateStmt nodeType method
func TestUpdateStmtNodeType(t *testing.T) {
	stmt := &UpdateStmt{}
	if stmt.nodeType() != "UpdateStmt" {
		t.Errorf("Expected nodeType 'UpdateStmt', got %s", stmt.nodeType())
	}
}

// TestUpdateStmtStatementNode tests UpdateStmt statementNode method
func TestUpdateStmtStatementNode(t *testing.T) {
	stmt := &UpdateStmt{}
	stmt.statementNode()
}

// TestDeleteStmtNodeType tests DeleteStmt nodeType method
func TestDeleteStmtNodeType(t *testing.T) {
	stmt := &DeleteStmt{}
	if stmt.nodeType() != "DeleteStmt" {
		t.Errorf("Expected nodeType 'DeleteStmt', got %s", stmt.nodeType())
	}
}

// TestDeleteStmtStatementNode tests DeleteStmt statementNode method
func TestDeleteStmtStatementNode(t *testing.T) {
	stmt := &DeleteStmt{}
	stmt.statementNode()
}

// TestCreateTableStmtNodeType tests CreateTableStmt nodeType method
func TestCreateTableStmtNodeType(t *testing.T) {
	stmt := &CreateTableStmt{}
	if stmt.nodeType() != "CreateTableStmt" {
		t.Errorf("Expected nodeType 'CreateTableStmt', got %s", stmt.nodeType())
	}
}

// TestCreateTableStmtStatementNode tests CreateTableStmt statementNode method
func TestCreateTableStmtStatementNode(t *testing.T) {
	stmt := &CreateTableStmt{}
	stmt.statementNode()
}

// TestDropTableStmtNodeType tests DropTableStmt nodeType method
func TestDropTableStmtNodeType(t *testing.T) {
	stmt := &DropTableStmt{}
	if stmt.nodeType() != "DropTableStmt" {
		t.Errorf("Expected nodeType 'DropTableStmt', got %s", stmt.nodeType())
	}
}

// TestDropTableStmtStatementNode tests DropTableStmt statementNode method
func TestDropTableStmtStatementNode(t *testing.T) {
	stmt := &DropTableStmt{}
	stmt.statementNode()
}

// TestCreateIndexStmtNodeType tests CreateIndexStmt nodeType method
func TestCreateIndexStmtNodeType(t *testing.T) {
	stmt := &CreateIndexStmt{}
	if stmt.nodeType() != "CreateIndexStmt" {
		t.Errorf("Expected nodeType 'CreateIndexStmt', got %s", stmt.nodeType())
	}
}

// TestCreateIndexStmtStatementNode tests CreateIndexStmt statementNode method
func TestCreateIndexStmtStatementNode(t *testing.T) {
	stmt := &CreateIndexStmt{}
	stmt.statementNode()
}

// TestBeginStmtNodeType tests BeginStmt nodeType method
func TestBeginStmtNodeType(t *testing.T) {
	stmt := &BeginStmt{}
	if stmt.nodeType() != "BeginStmt" {
		t.Errorf("Expected nodeType 'BeginStmt', got %s", stmt.nodeType())
	}
}

// TestBeginStmtStatementNode tests BeginStmt statementNode method
func TestBeginStmtStatementNode(t *testing.T) {
	stmt := &BeginStmt{}
	stmt.statementNode()
}

// TestCommitStmtNodeType tests CommitStmt nodeType method
func TestCommitStmtNodeType(t *testing.T) {
	stmt := &CommitStmt{}
	if stmt.nodeType() != "CommitStmt" {
		t.Errorf("Expected nodeType 'CommitStmt', got %s", stmt.nodeType())
	}
}

// TestCommitStmtStatementNode tests CommitStmt statementNode method
func TestCommitStmtStatementNode(t *testing.T) {
	stmt := &CommitStmt{}
	stmt.statementNode()
}

// TestRollbackStmtNodeType tests RollbackStmt nodeType method
func TestRollbackStmtNodeType(t *testing.T) {
	stmt := &RollbackStmt{}
	if stmt.nodeType() != "RollbackStmt" {
		t.Errorf("Expected nodeType 'RollbackStmt', got %s", stmt.nodeType())
	}
}

// TestRollbackStmtStatementNode tests RollbackStmt statementNode method
func TestRollbackStmtStatementNode(t *testing.T) {
	stmt := &RollbackStmt{}
	stmt.statementNode()
}

// TestBinaryExprNodeType tests BinaryExpr nodeType method
func TestBinaryExprNodeType(t *testing.T) {
	expr := &BinaryExpr{}
	if expr.nodeType() != "BinaryExpr" {
		t.Errorf("Expected nodeType 'BinaryExpr', got %s", expr.nodeType())
	}
}

// TestBinaryExprExpressionNode tests BinaryExpr expressionNode method
func TestBinaryExprExpressionNode(t *testing.T) {
	expr := &BinaryExpr{}
	expr.expressionNode()
}

// TestIdentifierNodeType tests Identifier nodeType method
func TestIdentifierNodeType(t *testing.T) {
	expr := &Identifier{}
	if expr.nodeType() != "Identifier" {
		t.Errorf("Expected nodeType 'Identifier', got %s", expr.nodeType())
	}
}

// TestIdentifierExpressionNode tests Identifier expressionNode method
func TestIdentifierExpressionNode(t *testing.T) {
	expr := &Identifier{}
	expr.expressionNode()
}

// TestStringLiteralNodeType tests StringLiteral nodeType method
func TestStringLiteralNodeType(t *testing.T) {
	expr := &StringLiteral{}
	if expr.nodeType() != "StringLiteral" {
		t.Errorf("Expected nodeType 'StringLiteral', got %s", expr.nodeType())
	}
}

// TestStringLiteralExpressionNode tests StringLiteral expressionNode method
func TestStringLiteralExpressionNode(t *testing.T) {
	expr := &StringLiteral{}
	expr.expressionNode()
}

// TestNumberLiteralNodeType tests NumberLiteral nodeType method
func TestNumberLiteralNodeType(t *testing.T) {
	expr := &NumberLiteral{}
	if expr.nodeType() != "NumberLiteral" {
		t.Errorf("Expected nodeType 'NumberLiteral', got %s", expr.nodeType())
	}
}

// TestNumberLiteralExpressionNode tests NumberLiteral expressionNode method
func TestNumberLiteralExpressionNode(t *testing.T) {
	expr := &NumberLiteral{}
	expr.expressionNode()
}

// TestBooleanLiteralNodeType tests BooleanLiteral nodeType method
func TestBooleanLiteralNodeType(t *testing.T) {
	expr := &BooleanLiteral{}
	if expr.nodeType() != "BooleanLiteral" {
		t.Errorf("Expected nodeType 'BooleanLiteral', got %s", expr.nodeType())
	}
}

// TestBooleanLiteralExpressionNode tests BooleanLiteral expressionNode method
func TestBooleanLiteralExpressionNode(t *testing.T) {
	expr := &BooleanLiteral{}
	expr.expressionNode()
}

// TestNullLiteralNodeType tests NullLiteral nodeType method
func TestNullLiteralNodeType(t *testing.T) {
	expr := &NullLiteral{}
	if expr.nodeType() != "NullLiteral" {
		t.Errorf("Expected nodeType 'NullLiteral', got %s", expr.nodeType())
	}
}

// TestNullLiteralExpressionNode tests NullLiteral expressionNode method
func TestNullLiteralExpressionNode(t *testing.T) {
	expr := &NullLiteral{}
	expr.expressionNode()
}

// TestPlaceholderExprNodeType tests PlaceholderExpr nodeType method
func TestPlaceholderExprNodeType(t *testing.T) {
	expr := &PlaceholderExpr{}
	if expr.nodeType() != "PlaceholderExpr" {
		t.Errorf("Expected nodeType 'PlaceholderExpr', got %s", expr.nodeType())
	}
}

// TestPlaceholderExprExpressionNode tests PlaceholderExpr expressionNode method
func TestPlaceholderExprExpressionNode(t *testing.T) {
	expr := &PlaceholderExpr{}
	expr.expressionNode()
}

// TestStarExprNodeType tests StarExpr nodeType method
func TestStarExprNodeType(t *testing.T) {
	expr := &StarExpr{}
	if expr.nodeType() != "StarExpr" {
		t.Errorf("Expected nodeType 'StarExpr', got %s", expr.nodeType())
	}
}

// TestStarExprExpressionNode tests StarExpr expressionNode method
func TestStarExprExpressionNode(t *testing.T) {
	expr := &StarExpr{}
	expr.expressionNode()
}

// TestQualifiedIdentifierNodeType tests QualifiedIdentifier nodeType method
func TestQualifiedIdentifierNodeType(t *testing.T) {
	expr := &QualifiedIdentifier{}
	if expr.nodeType() != "QualifiedIdentifier" {
		t.Errorf("Expected nodeType 'QualifiedIdentifier', got %s", expr.nodeType())
	}
}

// TestQualifiedIdentifierExpressionNode tests QualifiedIdentifier expressionNode method
func TestQualifiedIdentifierExpressionNode(t *testing.T) {
	expr := &QualifiedIdentifier{}
	expr.expressionNode()
}

// TestFunctionCallNodeType tests FunctionCall nodeType method
func TestFunctionCallNodeType(t *testing.T) {
	expr := &FunctionCall{}
	if expr.nodeType() != "FunctionCall" {
		t.Errorf("Expected nodeType 'FunctionCall', got %s", expr.nodeType())
	}
}

// TestFunctionCallExpressionNode tests FunctionCall expressionNode method
func TestFunctionCallExpressionNode(t *testing.T) {
	expr := &FunctionCall{}
	expr.expressionNode()
}

// TestLikeExprNodeType tests LikeExpr nodeType method
func TestLikeExprNodeType(t *testing.T) {
	expr := &LikeExpr{}
	if expr.nodeType() != "LikeExpr" {
		t.Errorf("Expected nodeType 'LikeExpr', got %s", expr.nodeType())
	}
}

// TestLikeExprExpressionNode tests LikeExpr expressionNode method
func TestLikeExprExpressionNode(t *testing.T) {
	expr := &LikeExpr{}
	expr.expressionNode()
}

// TestInExprNodeType tests InExpr nodeType method
func TestInExprNodeType(t *testing.T) {
	expr := &InExpr{}
	if expr.nodeType() != "InExpr" {
		t.Errorf("Expected nodeType 'InExpr', got %s", expr.nodeType())
	}
}

// TestInExprExpressionNode tests InExpr expressionNode method
func TestInExprExpressionNode(t *testing.T) {
	expr := &InExpr{}
	expr.expressionNode()
}

// TestBetweenExprNodeType tests BetweenExpr nodeType method
func TestBetweenExprNodeType(t *testing.T) {
	expr := &BetweenExpr{}
	if expr.nodeType() != "BetweenExpr" {
		t.Errorf("Expected nodeType 'BetweenExpr', got %s", expr.nodeType())
	}
}

// TestBetweenExprExpressionNode tests BetweenExpr expressionNode method
func TestBetweenExprExpressionNode(t *testing.T) {
	expr := &BetweenExpr{}
	expr.expressionNode()
}

// TestIsNullExprNodeType tests IsNullExpr nodeType method
func TestIsNullExprNodeType(t *testing.T) {
	expr := &IsNullExpr{}
	if expr.nodeType() != "IsNullExpr" {
		t.Errorf("Expected nodeType 'IsNullExpr', got %s", expr.nodeType())
	}
}

// TestIsNullExprExpressionNode tests IsNullExpr expressionNode method
func TestIsNullExprExpressionNode(t *testing.T) {
	expr := &IsNullExpr{}
	expr.expressionNode()
}

// TestCreateViewStmtNodeType tests CreateViewStmt nodeType method
func TestCreateViewStmtNodeType(t *testing.T) {
	stmt := &CreateViewStmt{}
	if stmt.nodeType() != "CreateViewStmt" {
		t.Errorf("Expected nodeType 'CreateViewStmt', got %s", stmt.nodeType())
	}
}

// TestCreateViewStmtStatementNode tests CreateViewStmt statementNode method
func TestCreateViewStmtStatementNode(t *testing.T) {
	stmt := &CreateViewStmt{}
	stmt.statementNode()
}

// TestDropViewStmtNodeType tests DropViewStmt nodeType method
func TestDropViewStmtNodeType(t *testing.T) {
	stmt := &DropViewStmt{}
	if stmt.nodeType() != "DropViewStmt" {
		t.Errorf("Expected nodeType 'DropViewStmt', got %s", stmt.nodeType())
	}
}

// TestDropViewStmtStatementNode tests DropViewStmt statementNode method
func TestDropViewStmtStatementNode(t *testing.T) {
	stmt := &DropViewStmt{}
	stmt.statementNode()
}

// TestCreateTriggerStmtNodeType tests CreateTriggerStmt nodeType method
func TestCreateTriggerStmtNodeType(t *testing.T) {
	stmt := &CreateTriggerStmt{}
	if stmt.nodeType() != "CreateTriggerStmt" {
		t.Errorf("Expected nodeType 'CreateTriggerStmt', got %s", stmt.nodeType())
	}
}

// TestCreateTriggerStmtStatementNode tests CreateTriggerStmt statementNode method
func TestCreateTriggerStmtStatementNode(t *testing.T) {
	stmt := &CreateTriggerStmt{}
	stmt.statementNode()
}

// TestDropTriggerStmtNodeType tests DropTriggerStmt nodeType method
func TestDropTriggerStmtNodeType(t *testing.T) {
	stmt := &DropTriggerStmt{}
	if stmt.nodeType() != "DropTriggerStmt" {
		t.Errorf("Expected nodeType 'DropTriggerStmt', got %s", stmt.nodeType())
	}
}

// TestDropTriggerStmtStatementNode tests DropTriggerStmt statementNode method
func TestDropTriggerStmtStatementNode(t *testing.T) {
	stmt := &DropTriggerStmt{}
	stmt.statementNode()
}

// TestCreateProcedureStmtNodeType tests CreateProcedureStmt nodeType method
func TestCreateProcedureStmtNodeType(t *testing.T) {
	stmt := &CreateProcedureStmt{}
	if stmt.nodeType() != "CreateProcedureStmt" {
		t.Errorf("Expected nodeType 'CreateProcedureStmt', got %s", stmt.nodeType())
	}
}

// TestCreateProcedureStmtStatementNode tests CreateProcedureStmt statementNode method
func TestCreateProcedureStmtStatementNode(t *testing.T) {
	stmt := &CreateProcedureStmt{}
	stmt.statementNode()
}

// TestDropProcedureStmtNodeType tests DropProcedureStmt nodeType method
func TestDropProcedureStmtNodeType(t *testing.T) {
	stmt := &DropProcedureStmt{}
	if stmt.nodeType() != "DropProcedureStmt" {
		t.Errorf("Expected nodeType 'DropProcedureStmt', got %s", stmt.nodeType())
	}
}

// TestDropProcedureStmtStatementNode tests DropProcedureStmt statementNode method
func TestDropProcedureStmtStatementNode(t *testing.T) {
	stmt := &DropProcedureStmt{}
	stmt.statementNode()
}
