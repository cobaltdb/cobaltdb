package query

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser parses SQL tokens into an AST
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser creates a new parser for the given tokens
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

// Parse parses the tokens and returns a statement
func (p *Parser) Parse() (Statement, error) {
	if p.current().Type == TokenEOF {
		return nil, fmt.Errorf("empty statement")
	}

	switch p.current().Type {
	case TokenSelect:
		return p.parseSelect()
	case TokenInsert:
		return p.parseInsert()
	case TokenUpdate:
		return p.parseUpdate()
	case TokenDelete:
		return p.parseDelete()
	case TokenCreate:
		return p.parseCreate()
	case TokenDrop:
		return p.parseDrop()
	case TokenBegin:
		return p.parseBegin()
	case TokenCommit:
		return p.parseCommit()
	case TokenRollback:
		return p.parseRollback()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.current().Literal)
	}
}

// current returns the current token
func (p *Parser) current() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

// peek returns the next token
func (p *Parser) peek() Token {
	if p.pos+1 >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos+1]
}

// advance moves to the next token
func (p *Parser) advance() {
	if p.pos < len(p.tokens) {
		p.pos++
	}
}

// expect checks if the current token is of the expected type and advances
func (p *Parser) expect(t TokenType) (Token, error) {
	if p.current().Type != t {
		return Token{}, fmt.Errorf("expected %s, got %s", TokenTypeString(t), p.current().Literal)
	}
	tok := p.current()
	p.advance()
	return tok, nil
}

// match checks if the current token matches and advances if so
func (p *Parser) match(t TokenType) bool {
	if p.current().Type == t {
		p.advance()
		return true
	}
	return false
}

// parseSelect parses a SELECT statement
func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}
	p.advance() // consume SELECT

	// DISTINCT?
	if p.match(TokenDistinct) {
		stmt.Distinct = true
	} else if p.match(TokenAll) {
		// ALL is default
	}

	// Column list
	columns, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	// FROM
	if p.match(TokenFrom) {
		table, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		stmt.From = table

		// JOINs
		for p.isJoin() {
			join, err := p.parseJoin()
			if err != nil {
				return nil, err
			}
			stmt.Joins = append(stmt.Joins, join)
		}
	}

	// WHERE
	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// GROUP BY
	if p.match(TokenGroup) {
		if _, err := p.expect(TokenBy); err != nil {
			return nil, err
		}
		groupBy, err := p.parseExpressionList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy

		// HAVING
		if p.match(TokenHaving) {
			having, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.Having = having
		}
	}

	// ORDER BY
	if p.match(TokenOrder) {
		if _, err := p.expect(TokenBy); err != nil {
			return nil, err
		}
		orderBy, err := p.parseOrderByList()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// LIMIT
	if p.match(TokenLimit) {
		limit, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Limit = limit
	}

	// OFFSET
	if p.match(TokenOffset) {
		offset, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Offset = offset
	}

	return stmt, nil
}

// parseSelectList parses the SELECT column list
func (p *Parser) parseSelectList() ([]Expression, error) {
	var columns []Expression

	for {
		expr, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		columns = append(columns, expr)

		if !p.match(TokenComma) {
			break
		}
	}

	return columns, nil
}

// parseSelectItem parses a single SELECT item
func (p *Parser) parseSelectItem() (Expression, error) {
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	// AS alias?
	if p.match(TokenAs) {
		alias, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		// Wrap in a binary expr for now, or create an AliasExpr
		_ = alias
	}

	return expr, nil
}

// parseTableRef parses a table reference
func (p *Parser) parseTableRef() (*TableRef, error) {
	tok, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}

	ref := &TableRef{Name: tok.Literal}

	// Alias?
	if p.current().Type == TokenIdentifier {
		ref.Alias = p.current().Literal
		p.advance()
	} else if p.match(TokenAs) {
		alias, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		ref.Alias = alias.Literal
	}

	return ref, nil
}

// isJoin checks if the current token starts a JOIN clause
func (p *Parser) isJoin() bool {
	switch p.current().Type {
	case TokenJoin, TokenInner, TokenLeft, TokenRight, TokenOuter:
		return true
	}
	return false
}

// parseJoin parses a JOIN clause
func (p *Parser) parseJoin() (*JoinClause, error) {
	join := &JoinClause{}

	// JOIN type
	switch p.current().Type {
	case TokenInner:
		join.Type = TokenInner
		p.advance()
		p.expect(TokenJoin)
	case TokenLeft:
		join.Type = TokenLeft
		p.advance()
		if p.match(TokenOuter) {
			// LEFT OUTER JOIN
		}
		p.expect(TokenJoin)
	case TokenRight:
		join.Type = TokenRight
		p.advance()
		if p.match(TokenOuter) {
			// RIGHT OUTER JOIN
		}
		p.expect(TokenJoin)
	case TokenOuter:
		p.advance()
		join.Type = TokenOuter
		p.expect(TokenJoin)
	case TokenJoin:
		join.Type = TokenJoin
		p.advance()
	}

	// Table
	table, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	join.Table = table

	// ON condition
	if _, err := p.expect(TokenOn); err != nil {
		return nil, err
	}

	condition, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	join.Condition = condition

	return join, nil
}

// parseOrderByList parses ORDER BY expressions
func (p *Parser) parseOrderByList() ([]*OrderByExpr, error) {
	var exprs []*OrderByExpr

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		orderExpr := &OrderByExpr{Expr: expr}

		if p.match(TokenDesc) {
			orderExpr.Desc = true
		} else {
			p.match(TokenAsc) // ASC is default
		}

		exprs = append(exprs, orderExpr)

		if !p.match(TokenComma) {
			break
		}
	}

	return exprs, nil
}

// parseExpressionList parses a comma-separated list of expressions
func (p *Parser) parseExpressionList() ([]Expression, error) {
	return p.parseExpressionListWithOffset(0)
}

// parseExpressionListWithOffset parses a comma-separated list of expressions with placeholder offset
func (p *Parser) parseExpressionListWithOffset(placeholderOffset int) ([]Expression, error) {
	var exprs []Expression

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		// Update placeholder indices with position in the list
		if placeholder, ok := expr.(*PlaceholderExpr); ok {
			// Add both offset and current position in the list
			placeholder.Index += placeholderOffset + len(exprs)
		}

		exprs = append(exprs, expr)

		if !p.match(TokenComma) {
			break
		}
	}

	return exprs, nil
}

// parseExpression parses an expression
func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOr()
}

// parseOr parses OR expressions
func (p *Parser) parseOr() (Expression, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.match(TokenOr) {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Operator: TokenOr, Right: right}
	}

	return left, nil
}

// parseAnd parses AND expressions
func (p *Parser) parseAnd() (Expression, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.match(TokenAnd) {
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Operator: TokenAnd, Right: right}
	}

	return left, nil
}

// parseNot parses NOT expressions
func (p *Parser) parseNot() (Expression, error) {
	if p.match(TokenNot) {
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Operator: TokenNot, Expr: expr}, nil
	}

	return p.parseComparison()
}

// parseComparison parses comparison expressions
func (p *Parser) parseComparison() (Expression, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	// Check for various comparison operators
	switch p.current().Type {
	case TokenEq, TokenNeq, TokenLt, TokenGt, TokenLte, TokenGte:
		op := p.current().Type
		p.advance()
		right, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Operator: op, Right: right}, nil
	case TokenLike:
		p.advance()
		not := false
		if p.match(TokenNot) {
			not = true
		}
		pattern, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &LikeExpr{Expr: left, Pattern: pattern, Not: not}, nil
	case TokenIn:
		p.advance()
		not := false
		if p.match(TokenNot) {
			not = true
		}
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}
		list, err := p.parseExpressionList()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return &InExpr{Expr: left, List: list, Not: not}, nil
	case TokenBetween:
		p.advance()
		not := false
		if p.match(TokenNot) {
			not = true
		}
		lower, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenAnd); err != nil {
			return nil, err
		}
		upper, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: left, Lower: lower, Upper: upper, Not: not}, nil
	case TokenIs:
		p.advance()
		not := false
		if p.match(TokenNot) {
			not = true
		}
		if !p.match(TokenNull) {
			return nil, fmt.Errorf("expected NULL after IS")
		}
		return &IsNullExpr{Expr: left, Not: not}, nil
	}

	return left, nil
}

// parseAdditive parses + and - expressions
func (p *Parser) parseAdditive() (Expression, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenPlus || p.current().Type == TokenMinus {
		op := p.current().Type
		p.advance()
		right, err := p.parseMultiplicative()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Operator: op, Right: right}
	}

	return left, nil
}

// parseMultiplicative parses *, /, % expressions
func (p *Parser) parseMultiplicative() (Expression, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenStar || p.current().Type == TokenSlash || p.current().Type == TokenPercent {
		op := p.current().Type
		p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Operator: op, Right: right}
	}

	return left, nil
}

// parseUnary parses unary expressions
func (p *Parser) parseUnary() (Expression, error) {
	if p.current().Type == TokenMinus || p.current().Type == TokenPlus {
		op := p.current().Type
		p.advance()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Operator: op, Expr: expr}, nil
	}

	return p.parsePrimary()
}

// parsePrimary parses primary expressions
func (p *Parser) parsePrimary() (Expression, error) {
	switch p.current().Type {
	case TokenNumber:
		return p.parseNumber()
	case TokenString:
		return p.parseString()
	case TokenIdentifier:
		return p.parseIdentifierOrFunction()
	case TokenLParen:
		return p.parseParenthesized()
	case TokenStar:
		p.advance()
		return &StarExpr{}, nil
	case TokenNull:
		p.advance()
		return &NullLiteral{}, nil
	case TokenTrue:
		p.advance()
		return &BooleanLiteral{Value: true}, nil
	case TokenFalse:
		p.advance()
		return &BooleanLiteral{Value: false}, nil
	case TokenQuestion:
		p.advance()
		return &PlaceholderExpr{}, nil
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.current().Literal)
	}
}

// parseNumber parses a number literal
func (p *Parser) parseNumber() (Expression, error) {
	tok := p.current()
	p.advance()

	val, err := strconv.ParseFloat(tok.Literal, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number: %s", tok.Literal)
	}

	return &NumberLiteral{Value: val, Raw: tok.Literal}, nil
}

// parseString parses a string literal
func (p *Parser) parseString() (Expression, error) {
	tok := p.current()
	p.advance()
	return &StringLiteral{Value: tok.Literal}, nil
}

// parseIdentifierOrFunction parses an identifier or function call
func (p *Parser) parseIdentifierOrFunction() (Expression, error) {
	tok := p.current()
	p.advance()

	// Check for qualified identifier (table.column)
	if p.match(TokenDot) {
		col, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		return &QualifiedIdentifier{Table: tok.Literal, Column: col.Literal}, nil
	}

	// Check for function call
	if p.current().Type == TokenLParen {
		return p.parseFunctionCall(tok.Literal)
	}

	// Check for JSON operators
	if p.current().Type == TokenArrow || p.current().Type == TokenArrow2 {
		asText := p.current().Type == TokenArrow2
		p.advance()
		path, err := p.expect(TokenString)
		if err != nil {
			return nil, err
		}
		return &JSONPathExpr{
			Column: &Identifier{Name: tok.Literal},
			Path:   strings.Trim(path.Literal, "'\""),
			AsText: asText,
		}, nil
	}

	return &Identifier{Name: tok.Literal}, nil
}

// parseFunctionCall parses a function call
func (p *Parser) parseFunctionCall(name string) (Expression, error) {
	p.expect(TokenLParen)

	var args []Expression
	if p.current().Type != TokenRParen {
		for {
			arg, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)

			if !p.match(TokenComma) {
				break
			}
		}
	}

	p.expect(TokenRParen)

	return &FunctionCall{Name: strings.ToUpper(name), Args: args}, nil
}

// parseParenthesized parses a parenthesized expression or subquery
func (p *Parser) parseParenthesized() (Expression, error) {
	p.expect(TokenLParen)

	// Check for subquery
	if p.current().Type == TokenSelect {
		stmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		p.expect(TokenRParen)
		return &SubqueryExpr{Query: stmt}, nil
	}

	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	p.expect(TokenRParen)
	return expr, nil
}

// parseInsert parses an INSERT statement
func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{}
	p.advance() // consume INSERT

	p.expect(TokenInto)

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	// Optional column list
	if p.current().Type == TokenLParen {
		p.advance()
		columns, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = columns
		p.expect(TokenRParen)
	}

	// VALUES
	p.expect(TokenValues)

	// Value lists
	rowCount := 0
	for {
		p.expect(TokenLParen)

		// Calculate placeholder offset for this row
		// If there are columns specified, use that count; otherwise estimate
		placeholderCount := 0
		if len(stmt.Columns) > 0 {
			placeholderCount = len(stmt.Columns)
		} else {
			// We'll estimate based on the first row
			placeholderCount = 0
		}

		offset := rowCount * placeholderCount
		values, err := p.parseExpressionListWithOffset(offset)
		if err != nil {
			return nil, err
		}

		// Update placeholder count after first row
		if rowCount == 0 && placeholderCount == 0 {
			placeholderCount = len(values)
		}

		stmt.Values = append(stmt.Values, values)
		p.expect(TokenRParen)

		if !p.match(TokenComma) {
			break
		}
		rowCount++
	}

	return stmt, nil
}

// parseIdentifierList parses a comma-separated list of identifiers
func (p *Parser) parseIdentifierList() ([]string, error) {
	var ids []string

	for {
		tok, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		ids = append(ids, tok.Literal)

		if !p.match(TokenComma) {
			break
		}
	}

	return ids, nil
}

// parseUpdate parses an UPDATE statement
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{}
	p.advance() // consume UPDATE

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	p.expect(TokenSet)

	// Set clauses
	for {
		col, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}

		p.expect(TokenEq)

		val, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Set = append(stmt.Set, &SetClause{Column: col.Literal, Value: val})

		if !p.match(TokenComma) {
			break
		}
	}

	// WHERE
	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// parseDelete parses a DELETE statement
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}
	p.advance() // consume DELETE

	p.expect(TokenFrom)

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	// WHERE
	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// parseCreate parses a CREATE statement
func (p *Parser) parseCreate() (Statement, error) {
	p.advance() // consume CREATE

	switch p.current().Type {
	case TokenTable:
		return p.parseCreateTable()
	case TokenIndex:
		return p.parseCreateIndex()
	case TokenCollection:
		return p.parseCreateCollection()
	default:
		return nil, fmt.Errorf("unexpected token after CREATE: %s", p.current().Literal)
	}
}

// parseCreateTable parses CREATE TABLE
func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	stmt := &CreateTableStmt{}
	p.advance() // consume TABLE

	if p.match(TokenIf) {
		p.expect(TokenNot)
		p.expect(TokenExists)
		stmt.IfNotExists = true
	}

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	p.expect(TokenLParen)

	// Column definitions
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

	p.expect(TokenRParen)

	return stmt, nil
}

// parseColumnDef parses a column definition
func (p *Parser) parseColumnDef() (*ColumnDef, error) {
	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}

	col := &ColumnDef{Name: name.Literal}

	// Data type
	switch p.current().Type {
	case TokenInteger, TokenText, TokenReal, TokenBlob, TokenBoolean, TokenJSON:
		col.Type = p.current().Type
		p.advance()
	default:
		return nil, fmt.Errorf("expected data type, got %s", p.current().Literal)
	}

	// Column constraints
	for {
		switch p.current().Type {
		case TokenPrimary:
			p.advance()
			p.expect(TokenKey)
			col.PrimaryKey = true
		case TokenNot:
			p.advance()
			p.expect(TokenNull)
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

	if p.match(TokenIf) {
		p.expect(TokenNot)
		p.expect(TokenExists)
		stmt.IfNotExists = true
	}

	index, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Index = index.Literal

	p.expect(TokenOn)

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	p.expect(TokenLParen)

	columns, err := p.parseIdentifierList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	p.expect(TokenRParen)

	return stmt, nil
}

// parseCreateCollection parses CREATE COLLECTION
func (p *Parser) parseCreateCollection() (*CreateCollectionStmt, error) {
	stmt := &CreateCollectionStmt{}
	p.advance() // consume COLLECTION

	if p.match(TokenIf) {
		p.expect(TokenNot)
		p.expect(TokenExists)
		stmt.IfNotExists = true
	}

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Name = name.Literal

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
	default:
		return nil, fmt.Errorf("unexpected token after DROP: %s", p.current().Literal)
	}
}

// parseDropTable parses DROP TABLE
func (p *Parser) parseDropTable() (*DropTableStmt, error) {
	stmt := &DropTableStmt{}
	p.advance() // consume TABLE

	if p.match(TokenIf) {
		p.expect(TokenExists)
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
func (p *Parser) parseDropIndex() (*DropTableStmt, error) {
	// For simplicity, reuse DropTableStmt
	stmt := &DropTableStmt{}
	p.advance() // consume INDEX

	if p.match(TokenIf) {
		p.expect(TokenExists)
		stmt.IfExists = true
	}

	index, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = index.Literal // Using Table field for index name

	return stmt, nil
}

// parseBegin parses BEGIN TRANSACTION
func (p *Parser) parseBegin() (*BeginStmt, error) {
	stmt := &BeginStmt{}
	p.advance() // consume BEGIN

	if p.match(TokenTransaction) {
		// optional
	}

	if p.current().Type == TokenIdentifier && strings.ToUpper(p.current().Literal) == "READ" {
		p.advance()
		if p.current().Type == TokenIdentifier && strings.ToUpper(p.current().Literal) == "ONLY" {
			p.advance()
			stmt.ReadOnly = true
		}
	}

	return stmt, nil
}

// parseCommit parses COMMIT
func (p *Parser) parseCommit() (*CommitStmt, error) {
	p.advance() // consume COMMIT
	p.match(TokenTransaction)
	return &CommitStmt{}, nil
}

// parseRollback parses ROLLBACK
func (p *Parser) parseRollback() (*RollbackStmt, error) {
	p.advance() // consume ROLLBACK
	p.match(TokenTransaction)
	return &RollbackStmt{}, nil
}

// Parse parses a SQL string and returns the AST
func Parse(sql string) (Statement, error) {
	tokens, err := Tokenize(sql)
	if err != nil {
		return nil, err
	}

	parser := NewParser(tokens)
	return parser.Parse()
}
