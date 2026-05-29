package query

import (
	"fmt"
	"strconv"
	"strings"
)

// parseCreate parses a CREATE statement
func (p *Parser) parseCreate() (Statement, error) {
	p.advance() // consume CREATE

	switch p.current().Type {
	case TokenTable:
		return p.parseCreateTable()
	case TokenForeign:
		return p.parseCreateForeignTable()
	case TokenIndex:
		return p.parseCreateIndex()
	case TokenUnique:
		// CREATE UNIQUE INDEX ...
		p.advance() // consume UNIQUE
		if p.current().Type != TokenIndex {
			return nil, fmt.Errorf("expected INDEX after UNIQUE")
		}
		stmt, err := p.parseCreateIndex()
		if err != nil {
			return nil, err
		}
		stmt.Unique = true
		return stmt, nil
	case TokenCollection:
		return p.parseCreateCollection()
	case TokenView:
		return p.parseCreateView()
	case TokenTrigger:
		return p.parseCreateTrigger()
	case TokenProcedure:
		return p.parseCreateProcedure()
	case TokenMaterialized:
		return p.parseCreateMaterializedView()
	case TokenFulltext:
		return p.parseCreateFTSIndex()
	case TokenVector:
		return p.parseCreateVectorIndex()
	case TokenPolicy:
		return p.parseCreatePolicy()
	default:
		return nil, fmt.Errorf("unexpected token after CREATE: %s", p.current().Literal)
	}
}

// parseCreateTable parses CREATE TABLE
func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}
	p.advance() // consume TABLE

	stmt.IfNotExists = p.parseIfNotExists()

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// Column definitions and table constraints
	for {
		// Check for FOREIGN KEY constraint
		if p.current().Type == TokenForeign {
			p.advance() // consume FOREIGN
			fk, err := p.parseForeignKeyDef()
			if err != nil {
				return nil, err
			}
			stmt.ForeignKeys = append(stmt.ForeignKeys, fk)
			if !p.match(TokenComma) {
				break
			}
			continue
		}

		// Check for table-level PRIMARY KEY constraint
		if p.current().Type == TokenPrimary {
			p.advance() // consume PRIMARY
			if _, err := p.expect(TokenKey); err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, err
			}
			// Parse column names
			for {
				col, err := p.expect(TokenIdentifier)
				if err != nil {
					return nil, err
				}
				stmt.PrimaryKey = append(stmt.PrimaryKey, col.Literal)
				if !p.match(TokenComma) {
					break
				}
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
			if !p.match(TokenComma) {
				break
			}
			continue
		}

		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)

		if !p.match(TokenComma) {
			break
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	// Parse optional PARTITION BY clause
	if p.current().Type == TokenPartition {
		p.advance() // consume PARTITION
		if _, err := p.expect(TokenBy); err != nil {
			return nil, err
		}
		partitionDef, err := p.parsePartitionBy()
		if err != nil {
			return nil, err
		}
		stmt.Partition = partitionDef
	}

	return stmt, nil
}

// parseCreateForeignTable parses CREATE FOREIGN TABLE
func (p *Parser) parseCreateForeignTable() (*CreateForeignTableStmt, error) {
	stmt := &CreateForeignTableStmt{Options: make(map[string]string)}
	p.advance() // consume FOREIGN

	if _, err := p.expect(TokenTable); err != nil {
		return nil, err
	}

	stmt.IfNotExists = p.parseIfNotExists()

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, col)
		if !p.match(TokenComma) {
			break
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	// WRAPPER 'name'
	if _, err := p.expect(TokenWrapper); err != nil {
		return nil, err
	}
	wrapper, err := p.expect(TokenString)
	if err != nil {
		return nil, err
	}
	stmt.Wrapper = wrapper.Literal

	// OPTIONS (key 'value', ...)
	if p.current().Type == TokenOptions {
		p.advance() // consume OPTIONS
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}
		for {
			keyTok, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			valTok, err := p.expect(TokenString)
			if err != nil {
				return nil, err
			}
			stmt.Options[keyTok.Literal] = valTok.Literal
			if !p.match(TokenComma) {
				break
			}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	return stmt, nil
}

// parsePartitionBy parses PARTITION BY clause
func (p *Parser) parsePartitionBy() (*PartitionDef, error) {
	def := &PartitionDef{}

	switch p.current().Type {
	case TokenRange:
		p.advance()
		def.Type = PartitionTypeRange
	case TokenList:
		p.advance()
		def.Type = PartitionTypeList
	case TokenHash:
		p.advance()
		def.Type = PartitionTypeHash
	case TokenKey:
		p.advance()
		def.Type = PartitionTypeKey
	default:
		return nil, fmt.Errorf("expected RANGE, LIST, HASH, or KEY after PARTITION BY")
	}

	// Parse column or expression in parentheses
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// For simplicity, expect a single column name
	if p.current().Type == TokenIdentifier {
		def.Column = p.current().Literal
		p.advance()
	} else {
		// Could be an expression
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		def.Expression = expr
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	if err := p.parsePartitionDefs(def); err != nil {
		return nil, err
	}
	return def, nil
}

func (p *Parser) parsePartitionDefs(def *PartitionDef) error {
	if p.current().Type == TokenPartitions {
		p.advance()
		if p.current().Type == TokenNumber {
			num, _ := strconv.Atoi(p.current().Literal)
			def.NumPartitions = num
			p.advance()
		}
		return nil
	}
	if p.current().Type != TokenLParen {
		return nil
	}
	p.advance()

	for p.current().Type == TokenPartition {
		p.advance()
		partName, err := p.expect(TokenIdentifier)
		if err != nil {
			return err
		}
		part := &SinglePartition{Name: partName.Literal}

		if p.current().Type == TokenValues {
			p.advance()
			if p.current().Type == TokenLess {
				p.advance()
				if _, err := p.expect(TokenThan); err != nil {
					return fmt.Errorf("expected THAN after LESS: %w", err)
				}
			}
			if _, err := p.expect(TokenLParen); err != nil {
				return err
			}
			valExpr, err := p.parseExpression()
			if err != nil {
				return err
			}
			part.Values = append(part.Values, valExpr)
			if _, err := p.expect(TokenRParen); err != nil {
				return err
			}
		}

		def.Partitions = append(def.Partitions, part)
		if !p.match(TokenComma) {
			break
		}
	}
	_, err := p.expect(TokenRParen)
	return err
}

// parseForeignKeyDef parses a FOREIGN KEY constraint
func (p *Parser) parseForeignKeyDef() (*ForeignKeyDef, error) {
	fk := &ForeignKeyDef{}

	if _, err := p.expect(TokenKey); err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// Parse column names
	for {
		col, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		fk.Columns = append(fk.Columns, col.Literal)

		if !p.match(TokenComma) {
			break
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenReferences); err != nil {
		return nil, err
	}

	// Referenced table
	refTable, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	fk.ReferencedTable = refTable.Literal

	// Optional referenced columns
	if p.match(TokenLParen) {
		for {
			refCol, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			fk.ReferencedColumns = append(fk.ReferencedColumns, refCol.Literal)

			if !p.match(TokenComma) {
				break
			}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	// ON DELETE and ON UPDATE (in any order, both optional)
	for i := 0; i < 2; i++ {
		if !p.match(TokenOn) {
			break
		}
		if p.match(TokenDelete) {
			if p.match(TokenCascade) {
				fk.OnDelete = "CASCADE"
			} else if p.match(TokenSet) {
				p.match(TokenNull)
				fk.OnDelete = "SET NULL"
			} else if p.match(TokenRestrict) {
				fk.OnDelete = "RESTRICT"
			} else if p.match(TokenNo) {
				p.match(TokenAction)
				fk.OnDelete = "NO ACTION"
			}
		} else if p.match(TokenUpdate) {
			if p.match(TokenCascade) {
				fk.OnUpdate = "CASCADE"
			} else if p.match(TokenSet) {
				p.match(TokenNull)
				fk.OnUpdate = "SET NULL"
			} else if p.match(TokenRestrict) {
				fk.OnUpdate = "RESTRICT"
			} else if p.match(TokenNo) {
				p.match(TokenAction)
				fk.OnUpdate = "NO ACTION"
			}
		} else {
			return nil, fmt.Errorf("expected DELETE or UPDATE after ON")
		}
	}

	return fk, nil
}

// parseColumnDef parses a column definition
func (p *Parser) parseColumnDef() (*ColumnDef, error) {
	// Allow keywords as column names (e.g., "key", "value", "text", "name")
	name := p.current()
	if name.Type == TokenIdentifier || (name.Literal != "" && name.Type != TokenEOF && name.Type != TokenLParen && name.Type != TokenRParen && name.Type != TokenComma) {
		p.advance()
	} else {
		return nil, fmt.Errorf("expected column name, got %s", name.Literal)
	}

	col := &ColumnDef{Name: name.Literal}

	// Data type
	switch p.current().Type {
	case TokenInteger, TokenText, TokenReal, TokenBlob, TokenBoolean, TokenJSON, TokenDate, TokenTimestamp, TokenDatetime, TokenVector:
		col.Type = p.current().Type
		p.advance()
	default:
		return nil, fmt.Errorf("expected data type, got %s", p.current().Literal)
	}

	// Parse optional type parameters like VARCHAR(255), DECIMAL(10,2), CHAR(50), VECTOR(768)
	if p.current().Type == TokenLParen {
		p.advance() // consume '('
		// For VECTOR type, parse dimensions
		if col.Type == TokenVector && p.current().Type == TokenNumber {
			dims, _ := strconv.Atoi(p.current().Literal)
			col.Dimensions = dims
			p.advance()
		} else {
			// For other types, just skip parameters
			for p.current().Type != TokenRParen && p.current().Type != TokenEOF {
				p.advance()
			}
		}
		if p.current().Type == TokenRParen {
			p.advance() // consume ')'
		}
	}

	// Column constraints
	for {
		switch p.current().Type {
		case TokenPrimary:
			p.advance()
			if _, err := p.expect(TokenKey); err != nil {
				return nil, err
			}
			col.PrimaryKey = true
		case TokenNot:
			p.advance()
			if _, err := p.expect(TokenNull); err != nil {
				return nil, err
			}
			col.NotNull = true
		case TokenUnique:
			p.advance()
			col.Unique = true
		case TokenAutoIncrement:
			p.advance()
			col.AutoIncrement = true
		case TokenDefault:
			p.advance()
			val, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			col.Default = val
		case TokenCheck:
			p.advance()
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, err
			}
			checkExpr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
			col.Check = checkExpr
		default:
			return col, nil
		}
	}
}

// parseCreateIndex parses CREATE INDEX
func (p *Parser) parseCreateIndex() (*CreateIndexStmt, error) {
	stmt := &CreateIndexStmt{}
	p.advance() // consume INDEX

	if p.match(TokenUnique) {
		stmt.Unique = true
	}

	stmt.IfNotExists = p.parseIfNotExists()

	index, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Index = index.Literal

	if _, err := p.expect(TokenOn); err != nil {
		return nil, err
	}

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	columns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseCreateCollection parses CREATE COLLECTION
func (p *Parser) parseCreateCollection() (*CreateCollectionStmt, error) {
	stmt := &CreateCollectionStmt{}
	p.advance() // consume COLLECTION

	stmt.IfNotExists = p.parseIfNotExists()

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	return stmt, nil
}

// parseCreateView parses CREATE VIEW
func (p *Parser) parseCreateView() (*CreateViewStmt, error) {
	stmt := &CreateViewStmt{}
	p.advance() // consume VIEW

	stmt.IfNotExists = p.parseIfNotExists()

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	// AS SELECT query
	if _, err := p.expect(TokenAs); err != nil {
		return nil, err
	}
	stmt.Query, err = p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("failed to parse view query: %w", err)
	}

	return stmt, nil
}

// parseDropView parses DROP VIEW
func (p *Parser) parseDropView() (*DropViewStmt, error) {
	stmt := &DropViewStmt{}
	p.advance() // consume VIEW

	if p.match(TokenIf) {
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	return stmt, nil
}

// parseCreateTrigger parses CREATE TRIGGER
func (p *Parser) parseCreateTrigger() (*CreateTriggerStmt, error) {
	stmt := &CreateTriggerStmt{}
	p.advance() // consume TRIGGER

	stmt.IfNotExists = p.parseIfNotExists()

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	// BEFORE, AFTER, or INSTEAD OF
	if p.current().Type == TokenBefore {
		p.advance()
		stmt.Time = "BEFORE"
	} else if p.current().Type == TokenAfter {
		p.advance()
		stmt.Time = "AFTER"
	} else if p.current().Type == TokenInstead {
		p.advance()
		if _, err := p.expect(TokenOf); err != nil {
			return nil, err
		}
		stmt.Time = "INSTEAD OF"
	} else {
		return nil, fmt.Errorf("expected BEFORE, AFTER, or INSTEAD OF")
	}

	// INSERT, UPDATE, DELETE
	switch p.current().Type {
	case TokenInsert:
		stmt.Event = "INSERT"
	case TokenUpdate:
		stmt.Event = "UPDATE"
	case TokenDelete:
		stmt.Event = "DELETE"
	default:
		return nil, fmt.Errorf("expected INSERT, UPDATE, or DELETE")
	}
	p.advance()

	// ON table_name
	if _, err := p.expect(TokenOn); err != nil {
		return nil, err
	}
	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	// FOR EACH ROW (optional)
	if p.match(TokenFor) {
		if _, err := p.expect(TokenEach); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRow); err != nil {
			return nil, err
		}
	}

	// WHEN condition (optional)
	if p.match(TokenWhen) {
		cond, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("trigger WHEN: %w", err)
		}
		stmt.Condition = cond
	}

	// Parse BEGIN ... END block
	if p.current().Type == TokenBegin {
		p.advance() // consume BEGIN

		for p.current().Type != TokenEnd && p.current().Type != TokenEOF {
			// Skip semicolons between statements
			if p.current().Type == TokenSemicolon {
				p.advance()
				continue
			}
			// Check for END
			if p.current().Type == TokenEnd {
				break
			}
			bodyStmt, err := p.Parse()
			if err != nil {
				return nil, fmt.Errorf("trigger body: %w", err)
			}
			stmt.Body = append(stmt.Body, bodyStmt)
			// Skip optional semicolon after statement
			if p.current().Type == TokenSemicolon {
				p.advance()
			}
		}

		if p.current().Type == TokenEnd {
			p.advance() // consume END
		}
	}

	return stmt, nil
}

// parseDropTrigger parses DROP TRIGGER
func (p *Parser) parseDropTrigger() (*DropTriggerStmt, error) {
	stmt := &DropTriggerStmt{}
	p.advance() // consume TRIGGER

	if p.match(TokenIf) {
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	return stmt, nil
}

// parseCreateProcedure parses CREATE PROCEDURE
func (p *Parser) parseCreateProcedure() (*CreateProcedureStmt, error) {
	stmt := &CreateProcedureStmt{}
	p.advance() // consume PROCEDURE

	stmt.IfNotExists = p.parseIfNotExists()

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	// (parameters)
	if p.match(TokenLParen) {
		for !p.match(TokenRParen) {
			param := &ParamDef{}

			// Optional IN/OUT/INOUT keyword
			if p.current().Type == TokenIn {
				p.advance() // consume IN
			} else if p.current().Type == TokenOut {
				p.advance() // consume OUT
			}

			paramName, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			param.Name = paramName.Literal

			// Type
			param.Type = p.current().Type
			p.advance()

			stmt.Params = append(stmt.Params, param)

			if !p.match(TokenComma) && p.current().Type != TokenRParen {
				break
			}
		}
	}

	// Parse procedure body: BEGIN ... END
	if p.current().Type == TokenBegin {
		body, err := p.parseProcedureBody()
		if err != nil {
			return nil, err
		}
		stmt.Body = body
	}

	return stmt, nil
}

// parseDropProcedure parses DROP PROCEDURE
func (p *Parser) parseDropProcedure() (*DropProcedureStmt, error) {
	stmt := &DropProcedureStmt{}
	p.advance() // consume PROCEDURE

	if p.match(TokenIf) {
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	return stmt, nil
}

// parseDropPolicy parses DROP POLICY
func (p *Parser) parseDropPolicy() (*DropPolicyStmt, error) {
	stmt := &DropPolicyStmt{}
	p.advance() // consume POLICY

	if p.match(TokenIf) {
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	// ON table (optional but recommended)
	if p.current().Type == TokenOn {
		p.advance() // consume ON
		table, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		stmt.Table = table.Literal
	}

	return stmt, nil
}

// parseDrop parses a DROP statement
func (p *Parser) parseDrop() (Statement, error) {
	p.advance() // consume DROP

	switch p.current().Type {
	case TokenTable:
		return p.parseDropTable()
	case TokenIndex:
		return p.parseDropIndex()
	case TokenView:
		return p.parseDropView()
	case TokenTrigger:
		return p.parseDropTrigger()
	case TokenProcedure:
		return p.parseDropProcedure()
	case TokenMaterialized:
		return p.parseDropMaterializedView()
	case TokenPolicy:
		return p.parseDropPolicy()
	default:
		return nil, fmt.Errorf("unexpected token after DROP: %s", p.current().Literal)
	}
}

// parseDropTable parses DROP TABLE
func (p *Parser) parseDropTable() (*DropTableStmt, error) {
	stmt := &DropTableStmt{}
	p.advance() // consume TABLE

	if p.match(TokenIf) {
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	return stmt, nil
}

// parseDropIndex parses DROP INDEX
func (p *Parser) parseDropIndex() (*DropIndexStmt, error) {
	stmt := &DropIndexStmt{}
	p.advance() // consume INDEX

	if p.match(TokenIf) {
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	index, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Index = index.Literal

	return stmt, nil
}

// parseAlterTable parses ALTER TABLE ... ADD/DROP/RENAME ...
func (p *Parser) parseAlterTable() (*AlterTableStmt, error) {
	p.advance() // consume ALTER

	if _, err := p.expect(TokenTable); err != nil {
		return nil, err
	}

	// Table name (allow keywords as table names)
	tableName := p.current()
	if tableName.Type == TokenIdentifier || (tableName.Literal != "" && tableName.Type != TokenEOF) {
		p.advance()
	} else {
		return nil, fmt.Errorf("expected table name, got %s", tableName.Literal)
	}

	stmt := &AlterTableStmt{Table: tableName.Literal}

	switch p.current().Type {
	case TokenAdd:
		// ADD COLUMN
		p.advance()
		p.match(TokenColumn) // COLUMN keyword is optional
		stmt.Action = "ADD"
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		stmt.Column = *col

	case TokenDrop:
		// DROP COLUMN
		p.advance()
		p.match(TokenColumn) // COLUMN keyword is optional
		stmt.Action = "DROP"
		colName := p.current()
		if colName.Type == TokenIdentifier || (colName.Literal != "" && colName.Type != TokenEOF) {
			stmt.NewName = colName.Literal // Store column name to drop in NewName
			p.advance()
		} else {
			return nil, fmt.Errorf("expected column name, got %s", colName.Literal)
		}

	case TokenRename:
		// RENAME TO new_name  OR  RENAME COLUMN old TO new
		p.advance()
		if p.current().Type == TokenColumn {
			// RENAME COLUMN old_name TO new_name
			p.advance()
			oldName := p.current()
			if oldName.Type == TokenIdentifier || (oldName.Literal != "" && oldName.Type != TokenEOF) {
				stmt.OldName = oldName.Literal
				p.advance()
			} else {
				return nil, fmt.Errorf("expected old column name, got %s", oldName.Literal)
			}
			if !strings.EqualFold(p.current().Literal, "TO") {
				return nil, fmt.Errorf("expected TO, got %s", p.current().Literal)
			}
			p.advance()
			newName := p.current()
			if newName.Type == TokenIdentifier || (newName.Literal != "" && newName.Type != TokenEOF) {
				stmt.NewName = newName.Literal
				p.advance()
			} else {
				return nil, fmt.Errorf("expected new column name, got %s", newName.Literal)
			}
			stmt.Action = "RENAME_COLUMN"
		} else if strings.EqualFold(p.current().Literal, "TO") {
			// RENAME TO new_table_name
			p.advance()
			newTableName := p.current()
			if newTableName.Type == TokenIdentifier || (newTableName.Literal != "" && newTableName.Type != TokenEOF) {
				stmt.NewName = newTableName.Literal
				p.advance()
			} else {
				return nil, fmt.Errorf("expected new table name, got %s", newTableName.Literal)
			}
			stmt.Action = "RENAME_TABLE"
		} else {
			return nil, fmt.Errorf("expected TO or COLUMN after RENAME, got %s", p.current().Literal)
		}

	default:
		return nil, fmt.Errorf("expected ADD, DROP, or RENAME, got %s", p.current().Literal)
	}

	return stmt, nil
}

// parseCall parses CALL procedure statement
func (p *Parser) parseCall() (*CallProcedureStmt, error) {
	p.advance() // consume CALL

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}

	stmt := &CallProcedureStmt{
		Name: name.Literal,
	}

	// Parse arguments
	if p.match(TokenLParen) {
		for !p.match(TokenRParen) {
			arg, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.Params = append(stmt.Params, arg)
			if !p.match(TokenComma) {
				if _, err := p.expect(TokenRParen); err != nil {
					return nil, err
				}
				break
			}
		}
	}

	placeholderOffset := 0
	for _, param := range stmt.Params {
		placeholders := collectPlaceholders(param)
		for i, ph := range placeholders {
			ph.Index = placeholderOffset + i
		}
		placeholderOffset += len(placeholders)
	}

	return stmt, nil
}

// parseCreateMaterializedView parses CREATE MATERIALIZED VIEW
func (p *Parser) parseCreateMaterializedView() (*CreateMaterializedViewStmt, error) {
	stmt := &CreateMaterializedViewStmt{}
	p.advance() // consume MATERIALIZED
	if _, err := p.expect(TokenView); err != nil {
		return nil, err
	}

	stmt.IfNotExists = p.parseIfNotExists()

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	if _, err := p.expect(TokenAs); err != nil {
		return nil, err
	}
	stmt.Query, err = p.parseSelect()
	if err != nil {
		return nil, fmt.Errorf("failed to parse materialized view query: %w", err)
	}

	return stmt, nil
}

// parseDropMaterializedView parses DROP MATERIALIZED VIEW
func (p *Parser) parseDropMaterializedView() (*DropMaterializedViewStmt, error) {
	stmt := &DropMaterializedViewStmt{}
	p.advance() // consume MATERIALIZED
	if _, err := p.expect(TokenView); err != nil {
		return nil, err
	}

	if p.match(TokenIf) {
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	return stmt, nil
}

// parseCreateFTSIndex parses CREATE FULLTEXT INDEX
func (p *Parser) parseCreateFTSIndex() (*CreateFTSIndexStmt, error) {
	stmt := &CreateFTSIndexStmt{}
	p.advance() // consume FULLTEXT

	if _, err := p.expect(TokenIndex); err != nil {
		return nil, err
	}

	stmt.IfNotExists = p.parseIfNotExists()

	index, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Index = index.Literal

	if _, err := p.expect(TokenOn); err != nil {
		return nil, err
	}

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	columns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseCreateVectorIndex parses CREATE VECTOR INDEX
// CREATE VECTOR INDEX [IF NOT EXISTS] idx_name ON table_name (column_name)
func (p *Parser) parseCreateVectorIndex() (*CreateVectorIndexStmt, error) {
	stmt := &CreateVectorIndexStmt{}
	p.advance() // consume VECTOR

	if _, err := p.expect(TokenIndex); err != nil {
		return nil, err
	}

	stmt.IfNotExists = p.parseIfNotExists()

	index, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Index = index.Literal

	if _, err := p.expect(TokenOn); err != nil {
		return nil, err
	}

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// Vector index only supports a single column
	column, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Column = column.Literal

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseCreatePolicy parses CREATE POLICY for row-level security
// CREATE POLICY name ON table [FOR {ALL | SELECT | INSERT | UPDATE | DELETE}]
//
//	[TO role [, ...]] [USING (expression)] [WITH CHECK (expression)]
func (p *Parser) parseCreatePolicy() (*CreatePolicyStmt, error) {
	stmt := &CreatePolicyStmt{
		Permissive: true,  // default
		Event:      "ALL", // default
	}
	p.advance() // consume POLICY

	// Policy name
	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

	// ON table
	if _, err := p.expect(TokenOn); err != nil {
		return nil, err
	}
	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	// Optional AS PERMISSIVE / AS RESTRICTIVE clause
	if p.current().Type == TokenAs {
		p.advance() // consume AS
		switch toUpperFast(p.current().Literal) {
		case "PERMISSIVE":
			stmt.Permissive = true
			p.advance()
		case "RESTRICTIVE":
			stmt.Permissive = false
			p.advance()
		default:
			return nil, fmt.Errorf("expected PERMISSIVE or RESTRICTIVE after AS, got %s", p.current().Literal)
		}
	}

	// Optional FOR clause
	if p.match(TokenFor) {
		eventTok := p.current()
		switch eventTok.Type {
		case TokenAll:
			stmt.Event = "ALL"
			p.advance()
		case TokenSelect:
			stmt.Event = "SELECT"
			p.advance()
		case TokenInsert:
			stmt.Event = "INSERT"
			p.advance()
		case TokenUpdate:
			stmt.Event = "UPDATE"
			p.advance()
		case TokenDelete:
			stmt.Event = "DELETE"
			p.advance()
		default:
			return nil, fmt.Errorf("expected ALL, SELECT, INSERT, UPDATE, or DELETE after FOR, got %s", eventTok.Literal)
		}
	}

	// Optional TO clause for roles
	if p.current().Type == TokenTo {
		p.advance() // consume TO
		roles, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}
		stmt.ForRoles = roles
	}

	// Optional USING clause
	if p.current().Type == TokenUsing {
		p.advance() // consume USING
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}
		usingExpr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error in USING expression: %w", err)
		}
		stmt.Using = usingExpr
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	// Optional WITH CHECK clause
	if p.current().Type == TokenWith {
		p.advance() // consume WITH
		if _, err := p.expect(TokenCheck); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}
		checkExpr, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("error in WITH CHECK expression: %w", err)
		}
		stmt.WithCheck = checkExpr
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	return stmt, nil
}
