package query

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser parses SQL tokens into an AST
type Parser struct {
	tokens           []Token
	pos              int
	placeholderCount int // Counter for auto-assigning placeholder indices
	depth            int
}

// NewParser creates a new parser for the given tokens
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

const maxParserDepth = 200

func (p *Parser) enterDepth() error {
	p.depth++
	if p.depth > maxParserDepth {
		return fmt.Errorf("expression nesting depth exceeds maximum (%d)", maxParserDepth)
	}
	return nil
}

func (p *Parser) leaveDepth() {
	p.depth--
}

// Parse parses the tokens and returns a statement
func (p *Parser) Parse() (Statement, error) {
	// Reset placeholder counter for each parse
	p.placeholderCount = 0

	if p.current().Type == TokenEOF {
		return nil, fmt.Errorf("empty statement")
	}

	switch p.current().Type {
	case TokenWith:
		return p.parseWithCTE()
	case TokenSelect:
		stmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		// Check for set operations (UNION/INTERSECT/EXCEPT) after SELECT
		if p.current().Type == TokenUnion || p.current().Type == TokenIntersect || p.current().Type == TokenExcept {
			return p.parseSetOp(stmt)
		}
		return stmt, nil
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
	case TokenSavepoint:
		return p.parseSavepoint()
	case TokenRelease:
		return p.parseRelease()
	case TokenCall:
		return p.parseCall()
	case TokenVacuum:
		return p.parseVacuum()
	case TokenAnalyze:
		return p.parseAnalyze()
	case TokenRefresh:
		return p.parseRefresh()
	case TokenAlter:
		return p.parseAlterTable()
	case TokenShow:
		return p.parseShow()
	case TokenUse:
		return p.parseUse()
	case TokenDescribe:
		return p.parseDescribe()
	case TokenDesc:
		// DESC as statement-level (DESCRIBE alias), not inside ORDER BY
		return p.parseDescribe()
	case TokenExplain:
		return p.parseExplain()
	case TokenSet:
		return p.parseSetVar()
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

		// Comma-separated tables are implicit CROSS JOINs (FROM a, b, c)
		for p.match(TokenComma) {
			crossTable, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}
			stmt.Joins = append(stmt.Joins, &JoinClause{
				Type:  TokenCross,
				Table: crossTable,
			})
		}

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

		// Fix WHERE clause placeholder indices
		// Collect all placeholders in WHERE clause and assign indices starting from 0
		wherePlaceholders := collectPlaceholders(where)
		for i, ph := range wherePlaceholders {
			ph.Index = i
		}
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
	}

	// HAVING (can appear with or without GROUP BY)
	if p.match(TokenHaving) {
		having, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Having = having
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
		expr = &AliasExpr{Expr: expr, Alias: alias.Literal}
	}

	return expr, nil
}

// parseTableRef parses a table reference (table name or derived table subquery)
func (p *Parser) parseTableRef() (*TableRef, error) {
	// Check for derived table: (SELECT ...) [AS] alias
	if p.current().Type == TokenLParen {
		p.advance() // consume '('
		if p.current().Type == TokenSelect {
			// Parse the subquery
			subSelect, err := p.parseSelect()
			if err != nil {
				return nil, fmt.Errorf("error parsing derived table subquery: %w", err)
			}
			// Check for UNION/INTERSECT/EXCEPT inside derived table
			var ref *TableRef
			if p.current().Type == TokenUnion || p.current().Type == TokenIntersect || p.current().Type == TokenExcept {
				unionStmt, err := p.parseSetOp(subSelect)
				if err != nil {
					return nil, fmt.Errorf("error parsing UNION in derived table: %w", err)
				}
				ref = &TableRef{SubqueryStmt: unionStmt}
			} else {
				ref = &TableRef{Subquery: subSelect}
			}
			// Expect closing paren
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, fmt.Errorf("expected ')' after derived table subquery")
			}
			// Parse alias (required for derived tables)
			if p.match(TokenAs) {
				alias, err := p.expect(TokenIdentifier)
				if err != nil {
					return nil, fmt.Errorf("derived table requires an alias")
				}
				ref.Alias = alias.Literal
				ref.Name = alias.Literal
			} else if p.current().Type == TokenIdentifier {
				ref.Alias = p.current().Literal
				ref.Name = p.current().Literal
				p.advance()
			} else {
				return nil, fmt.Errorf("derived table requires an alias")
			}
			return ref, nil
		}
		// Not a subquery - backtrack (this shouldn't happen in valid SQL)
		return nil, fmt.Errorf("expected SELECT after '(' in FROM clause")
	}

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
	case TokenJoin, TokenInner, TokenLeft, TokenRight, TokenOuter, TokenFull, TokenCross, TokenNatural:
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
		if _, err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenLeft:
		join.Type = TokenLeft
		p.advance()
		if p.match(TokenOuter) {
			// LEFT OUTER JOIN
		}
		if _, err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenRight:
		join.Type = TokenRight
		p.advance()
		if p.match(TokenOuter) {
			// RIGHT OUTER JOIN
		}
		if _, err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenFull:
		join.Type = TokenFull
		p.advance()
		if p.match(TokenOuter) {
			// FULL OUTER JOIN
		}
		if _, err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenOuter:
		p.advance()
		join.Type = TokenFull // treat bare OUTER JOIN as FULL OUTER
		if _, err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenCross:
		join.Type = TokenCross
		p.advance()
		if _, err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
	case TokenNatural:
		// NATURAL JOIN - automatically match columns with same name
		join.Natural = true
		p.advance()
		// Check for optional join type after NATURAL
		switch p.current().Type {
		case TokenInner:
			join.Type = TokenInner
			p.advance()
		case TokenLeft:
			join.Type = TokenLeft
			p.advance()
			if p.match(TokenOuter) {
				// NATURAL LEFT OUTER JOIN
			}
		case TokenRight:
			join.Type = TokenRight
			p.advance()
			if p.match(TokenOuter) {
				// NATURAL RIGHT OUTER JOIN
			}
		case TokenFull:
			join.Type = TokenFull
			p.advance()
			if p.match(TokenOuter) {
				// NATURAL FULL OUTER JOIN
			}
		default:
			// NATURAL JOIN without type defaults to INNER
			join.Type = TokenInner
		}
		if _, err := p.expect(TokenJoin); err != nil {
			return nil, err
		}
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

	// ON or USING condition (optional for CROSS JOIN)
	if join.Type == TokenCross {
		// CROSS JOIN doesn't require ON condition, but allow it
		if p.current().Type == TokenOn {
			p.advance()
			condition, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			join.Condition = condition
		}
	} else if p.current().Type == TokenUsing {
		// USING (col1, col2, ...)
		p.advance() // consume USING
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}
		columns, err := p.parseIdentifierList()
		if err != nil {
			return nil, fmt.Errorf("USING clause: %w", err)
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		join.Using = columns
	} else if join.Natural {
		// NATURAL JOIN doesn't require ON - condition is determined by common columns
	} else {
		if _, err := p.expect(TokenOn); err != nil {
			return nil, err
		}
		condition, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		join.Condition = condition
	}

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
	phCount := 0

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		// Update placeholder indices sequentially (including nested ones in function calls)
		placeholders := collectPlaceholders(expr)
		for _, ph := range placeholders {
			ph.Index = placeholderOffset + phCount
			phCount++
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

// parseExpressionWithOffset parses an expression with placeholder offset
func (p *Parser) parseExpressionWithOffset(offset int) (Expression, error) {
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	// Apply offset to all placeholders in the expression
	applyPlaceholderOffset(expr, offset)
	return expr, nil
}

// applyPlaceholderOffset recursively applies offset to placeholders
func applyPlaceholderOffset(expr Expression, offset int) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *PlaceholderExpr:
		e.Index += offset
	case *BinaryExpr:
		applyPlaceholderOffset(e.Left, offset)
		applyPlaceholderOffset(e.Right, offset)
	case *UnaryExpr:
		applyPlaceholderOffset(e.Expr, offset)
	case *FunctionCall:
		for _, arg := range e.Args {
			applyPlaceholderOffset(arg, offset)
		}
	case *InExpr:
		applyPlaceholderOffset(e.Expr, offset)
		for _, item := range e.List {
			applyPlaceholderOffset(item, offset)
		}
	case *BetweenExpr:
		applyPlaceholderOffset(e.Expr, offset)
		applyPlaceholderOffset(e.Lower, offset)
		applyPlaceholderOffset(e.Upper, offset)
	case *LikeExpr:
		applyPlaceholderOffset(e.Expr, offset)
		applyPlaceholderOffset(e.Pattern, offset)
	case *IsNullExpr:
		applyPlaceholderOffset(e.Expr, offset)
	case *AliasExpr:
		applyPlaceholderOffset(e.Expr, offset)
	case *SubqueryExpr:
		// Subqueries would need their own handling
	}
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
	if p.current().Type == TokenNot {
		// Check for NOT EXISTS - handle directly as ExistsExpr{Not: true}
		if p.peek().Type == TokenExists {
			p.advance() // consume NOT
			return p.parseExistsExpr(true)
		}
		p.advance() // consume NOT
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
		var escape Expression
		if p.current().Type == TokenEscape {
			p.advance() // consume ESCAPE
			escape, err = p.parsePrimary()
			if err != nil {
				return nil, err
			}
		}
		return &LikeExpr{Expr: left, Pattern: pattern, Not: not, Escape: escape}, nil
	case TokenIn:
		p.advance()
		not := false
		if p.match(TokenNot) {
			not = true
		}
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}

		// Check for subquery: IN (SELECT ...)
		var subquery *SelectStmt
		var list []Expression
		if p.current().Type == TokenSelect {
			subquery, err = p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
		} else {
			list, err = p.parseExpressionList()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
		}
		return &InExpr{Expr: left, List: list, Not: not, Subquery: subquery}, nil
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
	case TokenNot:
		// Handle NOT IN, NOT LIKE, NOT BETWEEN
		switch p.peek().Type {
		case TokenIn:
			p.advance() // consume NOT
			p.advance() // consume IN
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, err
			}
			var subquery *SelectStmt
			var list []Expression
			if p.current().Type == TokenSelect {
				subquery, err = p.parseSelect()
				if err != nil {
					return nil, err
				}
				if _, err := p.expect(TokenRParen); err != nil {
					return nil, err
				}
			} else {
				list, err = p.parseExpressionList()
				if err != nil {
					return nil, err
				}
				if _, err := p.expect(TokenRParen); err != nil {
					return nil, err
				}
			}
			return &InExpr{Expr: left, List: list, Not: true, Subquery: subquery}, nil
		case TokenLike:
			p.advance() // consume NOT
			p.advance() // consume LIKE
			pattern, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			var escape Expression
			if p.current().Type == TokenEscape {
				p.advance()
				escape, err = p.parsePrimary()
				if err != nil {
					return nil, err
				}
			}
			return &LikeExpr{Expr: left, Pattern: pattern, Not: true, Escape: escape}, nil
		case TokenBetween:
			p.advance() // consume NOT
			p.advance() // consume BETWEEN
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
			return &BetweenExpr{Expr: left, Lower: lower, Upper: upper, Not: true}, nil
		}
	}

	return left, nil
}

// parseAdditive parses + and - expressions
func (p *Parser) parseAdditive() (Expression, error) {
	left, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}

	for p.current().Type == TokenPlus || p.current().Type == TokenMinus || p.current().Type == TokenConcat {
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
	// JSON functions
	case TokenJsonExtract, TokenJsonSet, TokenJsonRemove, TokenJsonArrayLength,
		TokenJsonValid, TokenJsonType, TokenJsonKeys, TokenJsonPretty, TokenJsonMinify,
		TokenJsonMerge, TokenJsonQuote, TokenJsonUnquote:
		return p.parseIdentifierOrFunction()
	// REGEXP functions
	case TokenRegexMatch, TokenRegexReplace, TokenRegexExtract:
		return p.parseIdentifierOrFunction()
	// Aggregate and other functions
	case TokenCount, TokenSum, TokenAvg, TokenMin, TokenMax,
		TokenLength, TokenUpper, TokenLower, TokenTrim, TokenSubstr, TokenSubstring,
		TokenAbs, TokenRound, TokenFloor, TokenCeil, TokenCoalesce, TokenIfNull,
		TokenNullIf, TokenReplace, TokenInstr, TokenPrintf, TokenTime, TokenDatetime,
		TokenStrftime, TokenConcat, TokenLeft, TokenRight:
		return p.parseIdentifierOrFunction()
	// Window functions
	case TokenRowNumber, TokenRank, TokenDenseRank, TokenLag, TokenLead,
		TokenFirstValue, TokenLastValue, TokenNthValue:
		return p.parseIdentifierOrFunction()
	case TokenTypecast:
		return p.parseCast()
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
	case TokenCase:
		return p.parseCaseExpr()
	case TokenExists:
		return p.parseExistsExpr(false)
	case TokenMinus:
		// Unary minus
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Operator: TokenMinus, Expr: expr}, nil
	case TokenNot:
		// Check for NOT EXISTS
		if p.peek().Type == TokenExists {
			p.advance() // consume NOT
			return p.parseExistsExpr(true)
		}
		// Unary NOT
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Operator: TokenNot, Expr: expr}, nil
	case TokenMatch:
		return p.parseMatchAgainst()
	default:
		// Allow keywords to be used as identifiers (column names)
		// e.g., a column named "text", "date", "key", "status", etc.
		// But exclude SQL structural keywords that should never be identifiers in expression context.
		if p.current().Literal != "" && p.current().Type != TokenEOF &&
			!isStructuralKeyword(p.current().Type) {
			tok := p.current()
			p.advance()
			// Check for qualified identifier (table.column)
			if p.match(TokenDot) {
				// After dot, accept any token as column name (keywords can be column names)
				col := p.current()
				if col.Literal == "" || col.Type == TokenEOF {
					return nil, fmt.Errorf("expected column name after '.'")
				}
				p.advance()
				return &QualifiedIdentifier{Table: tok.Literal, Column: col.Literal}, nil
			}
			// Check for function call
			if p.current().Type == TokenLParen {
				return p.parseFunctionCall(tok.Literal)
			}
			return &Identifier{Name: tok.Literal}, nil
		}
		return nil, fmt.Errorf("unexpected token: %s", p.current().Literal)
	}
}

// parseExistsExpr parses EXISTS (SELECT ...) or NOT EXISTS (SELECT ...)
func (p *Parser) parseExistsExpr(not bool) (Expression, error) {
	p.advance() // consume EXISTS

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	if p.current().Type != TokenSelect {
		return nil, fmt.Errorf("expected SELECT after EXISTS(")
	}

	subquery, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return &ExistsExpr{Subquery: subquery, Not: not}, nil
}

// parseMatchAgainst parses MATCH (col1, col2, ...) AGAINST ('pattern')
func (p *Parser) parseMatchAgainst() (Expression, error) {
	p.advance() // consume MATCH

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// Parse column list
	var columns []Expression
	for {
		col, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)

		if !p.match(TokenComma) {
			break
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenAgainst); err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// Parse search pattern
	pattern, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	// Optional: IN BOOLEAN MODE or IN NATURAL LANGUAGE MODE
	mode := ""
	if p.match(TokenIn) {
		if p.match(TokenBoolean) {
			if _, err := p.expect(TokenMode); err != nil {
				return nil, err
			}
			mode = "BOOLEAN MODE"
		} else if p.match(TokenNatural) {
			if _, err := p.expect(TokenLanguage); err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenMode); err != nil {
				return nil, err
			}
			mode = "NATURAL LANGUAGE MODE"
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return &MatchExpr{Columns: columns, Pattern: pattern, Mode: mode}, nil
}

// parseCaseExpr parses a CASE expression
// CASE [expr] WHEN cond1 THEN result1 [WHEN cond2 THEN result2]... [ELSE default] END
func (p *Parser) parseCaseExpr() (Expression, error) {
	p.advance() // consume CASE

	caseExpr := &CaseExpr{}

	// Check for simple CASE: CASE expr WHEN ...
	if p.current().Type != TokenWhen {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		caseExpr.Expr = expr
	}

	// Parse WHEN clauses
	for p.current().Type == TokenWhen {
		p.advance() // consume WHEN

		cond, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		if p.current().Type != TokenThen {
			return nil, fmt.Errorf("expected THEN, got %s", p.current().Literal)
		}
		p.advance() // consume THEN

		result, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		caseExpr.Whens = append(caseExpr.Whens, &WhenClause{
			Condition: cond,
			Result:    result,
		})
	}

	// Parse optional ELSE
	if p.current().Type == TokenElse {
		p.advance() // consume ELSE
		elseExpr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		caseExpr.Else = elseExpr
	}

	// Expect END
	if p.current().Type != TokenEnd {
		return nil, fmt.Errorf("expected END, got %s", p.current().Literal)
	}
	p.advance() // consume END

	return caseExpr, nil
}

// parseCast parses a CAST(expr AS type) expression
func (p *Parser) parseCast() (Expression, error) {
	p.advance() // consume CAST

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	if p.current().Type != TokenAs {
		return nil, fmt.Errorf("expected AS in CAST, got %s", p.current().Literal)
	}
	p.advance() // consume AS

	// Parse the target data type
	dataType := p.current().Type
	p.advance()

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return &CastExpr{Expr: expr, DataType: dataType}, nil
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
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// Check for DISTINCT keyword (e.g., COUNT(DISTINCT col))
	distinct := false
	if p.current().Type == TokenDistinct {
		distinct = true
		p.advance()
	}

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

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	// Check for OVER clause (window function)
	if p.current().Type == TokenOver {
		return p.parseWindowExpr(strings.ToUpper(name), args)
	}

	return &FunctionCall{Name: strings.ToUpper(name), Args: args, Distinct: distinct}, nil
}

// parseWindowExpr parses the OVER (...) clause for window functions
func (p *Parser) parseWindowExpr(funcName string, args []Expression) (Expression, error) {
	p.advance() // consume OVER

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	windowExpr := &WindowExpr{
		Function: funcName,
		Args:     args,
	}

	// Parse PARTITION BY clause (optional)
	if p.current().Type == TokenPartition {
		p.advance() // consume PARTITION
		if p.current().Type == TokenBy {
			p.advance() // consume BY
		}
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, fmt.Errorf("failed to parse PARTITION BY expression: %w", err)
			}
			windowExpr.PartitionBy = append(windowExpr.PartitionBy, expr)
			if !p.match(TokenComma) {
				break
			}
		}
	}

	// Parse ORDER BY clause (optional)
	if p.current().Type == TokenOrder {
		p.advance() // consume ORDER
		if p.current().Type == TokenBy {
			p.advance() // consume BY
		}
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, fmt.Errorf("failed to parse window ORDER BY expression: %w", err)
			}
			orderBy := &OrderByExpr{Expr: expr, Desc: false}
			if p.current().Type == TokenDesc {
				orderBy.Desc = true
				p.advance()
			} else if p.current().Type == TokenAsc {
				p.advance()
			}
			windowExpr.OrderBy = append(windowExpr.OrderBy, orderBy)
			if !p.match(TokenComma) {
				break
			}
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return windowExpr, nil
}

// parseParenthesized parses a parenthesized expression or subquery
func (p *Parser) parseParenthesized() (Expression, error) {
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// Check for subquery
	if p.current().Type == TokenSelect {
		stmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return &SubqueryExpr{Query: stmt}, nil
	}

	if err := p.enterDepth(); err != nil {
		return nil, err
	}
	defer p.leaveDepth()

	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}
	return expr, nil
}

// parseInsert parses an INSERT statement
func (p *Parser) parseInsert() (*InsertStmt, error) {
	stmt := &InsertStmt{}
	p.advance() // consume INSERT

	// Check for INSERT OR REPLACE / INSERT OR IGNORE
	if p.current().Type == TokenOr {
		p.advance() // consume OR
		if p.current().Type == TokenReplace {
			stmt.ConflictAction = ConflictReplace
			p.advance() // consume REPLACE
		} else if p.current().Type == TokenIgnore {
			stmt.ConflictAction = ConflictIgnore
			p.advance() // consume IGNORE
		} else {
			return nil, fmt.Errorf("expected REPLACE or IGNORE after INSERT OR")
		}
	}

	if _, err := p.expect(TokenInto); err != nil {
		return nil, err
	}

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	// Optional column list
	if p.current().Type == TokenLParen {
		// Peek ahead to see if this is a column list or a subquery
		// Column list: (col1, col2, ...)
		// Subquery after column list: (col1, col2) SELECT ...
		p.advance()
		columns, err := p.parseIdentifierList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = columns
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	// INSERT INTO ... SELECT ...
	if p.current().Type == TokenSelect {
		selectStmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = selectStmt
		return stmt, nil
	}

	// VALUES
	if _, err := p.expect(TokenValues); err != nil {
		return nil, err
	}

	// Value lists
	rowCount := 0
	// Calculate placeholder count - if columns specified, use that; otherwise detect from first row
	placeholderCount := len(stmt.Columns)
	for {
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}

		offset := rowCount * placeholderCount
		values, err := p.parseExpressionListWithOffset(offset)
		if err != nil {
			return nil, err
		}

		// Detect placeholder count from first row when no columns specified
		if rowCount == 0 && placeholderCount == 0 {
			placeholderCount = len(values)
		}

		stmt.Values = append(stmt.Values, values)
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}

		if !p.match(TokenComma) {
			break
		}
		rowCount++
	}

	// Parse optional RETURNING clause
	if p.current().Type == TokenReturning {
		p.advance() // consume RETURNING
		returning, err := p.parseReturningClause()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	return stmt, nil
}

// parseReturningClause parses a RETURNING clause (expression list)
func (p *Parser) parseReturningClause() ([]Expression, error) {
	var expressions []Expression

	// Check for * (all columns)
	if p.current().Type == TokenStar {
		p.advance()
		return []Expression{&ColumnRef{Column: "*"}}, nil
	}

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, expr)

		if !p.match(TokenComma) {
			break
		}
	}

	return expressions, nil
}

// parseIdentifierList parses a comma-separated list of identifiers
func (p *Parser) parseIdentifierList() ([]string, error) {
	var ids []string

	for {
		// Allow keywords as column names (e.g., "key", "value", "text")
		tok := p.current()
		if tok.Type == TokenIdentifier || (tok.Literal != "" && tok.Type != TokenEOF && tok.Type != TokenRParen && tok.Type != TokenComma) {
			ids = append(ids, tok.Literal)
			p.advance()
		} else {
			return nil, fmt.Errorf("expected IDENTIFIER, got %s", tok.Literal)
		}

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

	if _, err := p.expect(TokenSet); err != nil {
		return nil, err
	}

	// Count set clauses first for placeholder offset
	setCount := 0

	// Set clauses - parse and save positions
	var setClauses []*SetClause
	var setPlaceholders []*PlaceholderExpr

	for {
		// Allow keywords as column names in SET clause
		col := p.current()
		if col.Type == TokenIdentifier || (col.Literal != "" && col.Type != TokenEOF && col.Type != TokenEq) {
			p.advance()
		} else {
			return nil, fmt.Errorf("expected column name, got %s", col.Literal)
		}

		if _, err := p.expect(TokenEq); err != nil {
			return nil, err
		}

		val, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		clause := &SetClause{Column: col.Literal, Value: val}
		setClauses = append(setClauses, clause)

		// Track placeholders for later fix (including nested ones in function calls)
		setPlaceholders = append(setPlaceholders, collectPlaceholders(val)...)
		setCount++

		if !p.match(TokenComma) {
			break
		}
	}

	// Fix SET clause placeholder indices (they should be 0, 1, 2, ...)
	for i, ph := range setPlaceholders {
		ph.Index = i
	}
	stmt.Set = setClauses

	// Calculate offset for WHERE clause placeholders (count actual placeholders, not SET columns)
	whereOffset := len(setPlaceholders)

	// FROM - for UPDATE with JOIN
	if p.match(TokenFrom) {
		table, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		stmt.From = table

		// Comma-separated tables are implicit CROSS JOINs
		for p.match(TokenComma) {
			crossTable, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}
			stmt.Joins = append(stmt.Joins, &JoinClause{
				Type:  TokenCross,
				Table: crossTable,
			})
		}

		// JOINs
		for p.isJoin() {
			join, err := p.parseJoin()
			if err != nil {
				return nil, err
			}
			stmt.Joins = append(stmt.Joins, join)
		}

		// Fix placeholder indices in FROM/JOIN conditions
		joinPlaceholderOffset := whereOffset
		for _, join := range stmt.Joins {
			if join.Condition != nil {
				joinPlaceholders := collectPlaceholders(join.Condition)
				for i, ph := range joinPlaceholders {
					ph.Index = joinPlaceholderOffset + i
				}
				joinPlaceholderOffset += len(joinPlaceholders)
			}
		}
		whereOffset = joinPlaceholderOffset
	}

	// WHERE
	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where

		// Fix WHERE clause placeholder indices
		// Collect all placeholders in WHERE clause and assign indices starting from whereOffset
		wherePlaceholders := collectPlaceholders(where)
		for i, ph := range wherePlaceholders {
			ph.Index = whereOffset + i
		}
	}

	// Parse optional RETURNING clause
	if p.current().Type == TokenReturning {
		p.advance() // consume RETURNING
		returning, err := p.parseReturningClause()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	return stmt, nil
}

// collectPlaceholders collects all PlaceholderExpr nodes from an expression
func collectPlaceholders(expr Expression) []*PlaceholderExpr {
	var placeholders []*PlaceholderExpr
	collectPlaceholdersRecursive(expr, &placeholders)
	return placeholders
}

func collectPlaceholdersRecursive(expr Expression, placeholders *[]*PlaceholderExpr) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *PlaceholderExpr:
		*placeholders = append(*placeholders, e)
	case *BinaryExpr:
		collectPlaceholdersRecursive(e.Left, placeholders)
		collectPlaceholdersRecursive(e.Right, placeholders)
	case *UnaryExpr:
		collectPlaceholdersRecursive(e.Expr, placeholders)
	case *FunctionCall:
		for _, arg := range e.Args {
			collectPlaceholdersRecursive(arg, placeholders)
		}
	case *InExpr:
		collectPlaceholdersRecursive(e.Expr, placeholders)
		for _, item := range e.List {
			collectPlaceholdersRecursive(item, placeholders)
		}
		if e.Subquery != nil {
			collectPlaceholdersRecursive(e.Subquery.Where, placeholders)
			collectPlaceholdersRecursive(e.Subquery.Having, placeholders)
			for _, col := range e.Subquery.Columns {
				collectPlaceholdersRecursive(col, placeholders)
			}
		}
	case *BetweenExpr:
		collectPlaceholdersRecursive(e.Expr, placeholders)
		collectPlaceholdersRecursive(e.Lower, placeholders)
		collectPlaceholdersRecursive(e.Upper, placeholders)
	case *LikeExpr:
		collectPlaceholdersRecursive(e.Expr, placeholders)
		collectPlaceholdersRecursive(e.Pattern, placeholders)
	case *IsNullExpr:
		collectPlaceholdersRecursive(e.Expr, placeholders)
	case *SubqueryExpr:
		if e.Query != nil {
			collectPlaceholdersRecursive(e.Query.Where, placeholders)
			collectPlaceholdersRecursive(e.Query.Having, placeholders)
			for _, col := range e.Query.Columns {
				collectPlaceholdersRecursive(col, placeholders)
			}
			for _, join := range e.Query.Joins {
				collectPlaceholdersRecursive(join.Condition, placeholders)
			}
		}
	case *ExistsExpr:
		if e.Subquery != nil {
			collectPlaceholdersRecursive(e.Subquery.Where, placeholders)
			collectPlaceholdersRecursive(e.Subquery.Having, placeholders)
			for _, col := range e.Subquery.Columns {
				collectPlaceholdersRecursive(col, placeholders)
			}
			for _, join := range e.Subquery.Joins {
				collectPlaceholdersRecursive(join.Condition, placeholders)
			}
		}
	case *CaseExpr:
		collectPlaceholdersRecursive(e.Expr, placeholders)
		for _, w := range e.Whens {
			collectPlaceholdersRecursive(w.Condition, placeholders)
			collectPlaceholdersRecursive(w.Result, placeholders)
		}
		collectPlaceholdersRecursive(e.Else, placeholders)
	case *AliasExpr:
		collectPlaceholdersRecursive(e.Expr, placeholders)
	case *CastExpr:
		collectPlaceholdersRecursive(e.Expr, placeholders)
	}
}

// parseDelete parses a DELETE statement
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	stmt := &DeleteStmt{}
	p.advance() // consume DELETE

	if _, err := p.expect(TokenFrom); err != nil {
		return nil, err
	}

	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.Table = table.Literal

	// Optional table alias
	if p.current().Type == TokenIdentifier || p.current().Type == TokenAs {
		if p.match(TokenAs) {
			alias, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			stmt.Alias = alias.Literal
		} else {
			stmt.Alias = p.current().Literal
			p.advance()
		}
	}

	// USING - for DELETE with JOIN
	placeholderOffset := 0
	if p.match(TokenUsing) {
		for {
			usingTable, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}
			stmt.Using = append(stmt.Using, usingTable)

			if !p.match(TokenComma) {
				break
			}
		}

		// JOINs after USING
		for p.isJoin() {
			join, err := p.parseJoin()
			if err != nil {
				return nil, err
			}
			// Add JOIN as additional table with condition in WHERE
			stmt.Using = append(stmt.Using, join.Table)
			// Store join condition in a way it can be used with WHERE
			// We'll need to combine with WHERE clause
			if join.Condition != nil {
				// Collect placeholders from join condition
				joinPlaceholders := collectPlaceholders(join.Condition)
				for i, ph := range joinPlaceholders {
					ph.Index = placeholderOffset + i
				}
				placeholderOffset += len(joinPlaceholders)
			}
		}
	}

	// WHERE - placeholders start at offset 0 (or after USING/JOIN placeholders)
	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where

		// Fix WHERE clause placeholder indices
		wherePlaceholders := collectPlaceholders(where)
		for i, ph := range wherePlaceholders {
			ph.Index = placeholderOffset + i
		}
	}

	// Parse optional RETURNING clause
	if p.current().Type == TokenReturning {
		p.advance() // consume RETURNING
		returning, err := p.parseReturningClause()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
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

	if p.match(TokenIf) {
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

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

	// Parse PARTITIONS num or individual partition definitions
	if p.current().Type == TokenPartitions {
		p.advance() // consume PARTITIONS
		// Should be PARTITIONS num
		if p.current().Type == TokenNumber {
			// PARTITIONS num
			num, _ := strconv.Atoi(p.current().Literal)
			def.NumPartitions = num
			p.advance()
		}
	} else if p.current().Type == TokenLParen {
		// Individual partition definitions: (PARTITION p0 VALUES LESS THAN (2020), ...)
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}

		for {
			if p.current().Type != TokenPartition {
				break
			}
			p.advance() // consume PARTITION

			// Parse partition name
			partName, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}

			part := &SinglePartition{Name: partName.Literal}

			// Parse VALUES LESS THAN (value) or VALUES IN (values)
			if p.current().Type == TokenValues {
				p.advance() // consume VALUES

				if p.current().Type == TokenLess { // LESS
					p.advance() // consume LESS
					if _, err := p.expect(TokenThan); err != nil {
						return nil, fmt.Errorf("expected THAN after LESS: %w", err)
					}
				}

				// For now expect a single value in parentheses
				if _, err := p.expect(TokenLParen); err != nil {
					return nil, err
				}

				// Parse value expression
				valExpr, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				part.Values = append(part.Values, valExpr)

				if _, err := p.expect(TokenRParen); err != nil {
					return nil, err
				}
			}

			def.Partitions = append(def.Partitions, part)

			if !p.match(TokenComma) {
				break
			}
		}

		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	return def, nil
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
	case TokenInteger, TokenText, TokenReal, TokenBlob, TokenBoolean, TokenJSON, TokenDate, TokenTimestamp, TokenDatetime:
		col.Type = p.current().Type
		p.advance()
	default:
		return nil, fmt.Errorf("expected data type, got %s", p.current().Literal)
	}

	// Skip optional type parameters like VARCHAR(255), DECIMAL(10,2), CHAR(50)
	if p.current().Type == TokenLParen {
		p.advance() // consume '('
		for p.current().Type != TokenRParen && p.current().Type != TokenEOF {
			p.advance() // skip parameters
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

	if p.match(TokenIf) {
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

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

	if p.match(TokenIf) {
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

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

	if p.match(TokenIf) {
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

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

	if p.match(TokenIf) {
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

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

	if p.match(TokenIf) {
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

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

// parseProcedureBody parses BEGIN ... END block
func (p *Parser) parseProcedureBody() ([]Statement, error) {
	if _, err := p.expect(TokenBegin); err != nil {
		return nil, err
	}

	var statements []Statement

	for p.current().Type != TokenEnd && p.current().Type != TokenEOF {
		// Skip optional semicolons between statements
		if p.current().Type == TokenSemicolon {
			p.advance()
			continue
		}

		// Parse individual statements in the body
		stmt, err := p.Parse()
		if err != nil {
			return nil, fmt.Errorf("error parsing procedure body: %w", err)
		}
		if stmt != nil {
			statements = append(statements, stmt)
		}

		// After each statement, expect semicolon or END
		if p.current().Type == TokenSemicolon {
			p.advance()
		}
	}

	if p.current().Type != TokenEnd {
		return nil, fmt.Errorf("expected END, got %s", p.current().Literal)
	}
	p.advance() // consume END

	return statements, nil
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
func (p *Parser) parseRollback() (Statement, error) {
	p.advance() // consume ROLLBACK
	// Check for ROLLBACK TO [SAVEPOINT] name
	if p.match(TokenTo) {
		p.match(TokenSavepoint) // optional SAVEPOINT keyword
		name, err := p.parseSavepointName()
		if err != nil {
			return nil, fmt.Errorf("expected savepoint name after ROLLBACK TO")
		}
		return &RollbackStmt{ToSavepoint: name}, nil
	}
	p.match(TokenTransaction)
	return &RollbackStmt{}, nil
}

func (p *Parser) parseSavepointName() (string, error) {
	// Savepoint names can be identifiers or SQL keywords used as names
	tok := p.current()
	if tok.Type == TokenIdentifier || tok.Literal != "" {
		p.advance()
		return tok.Literal, nil
	}
	return "", fmt.Errorf("expected savepoint name")
}

func (p *Parser) parseSavepoint() (*SavepointStmt, error) {
	p.advance() // consume SAVEPOINT
	name, err := p.parseSavepointName()
	if err != nil {
		return nil, err
	}
	return &SavepointStmt{Name: name}, nil
}

func (p *Parser) parseRelease() (*ReleaseSavepointStmt, error) {
	p.advance()             // consume RELEASE
	p.match(TokenSavepoint) // optional SAVEPOINT keyword
	name, err := p.parseSavepointName()
	if err != nil {
		return nil, fmt.Errorf("expected savepoint name after RELEASE")
	}
	return &ReleaseSavepointStmt{Name: name}, nil
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

	return stmt, nil
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

// ParseExpression parses a single SQL expression string (e.g., "42", "'hello'", "NOW()")
func ParseExpression(expr string) (Expression, error) {
	tokens, err := Tokenize("SELECT " + expr)
	if err != nil {
		return nil, err
	}
	parser := NewParser(tokens)
	parser.advance() // consume SELECT
	return parser.parseExpression()
}

// parseWithCTE parses a WITH clause (Common Table Expressions)
func (p *Parser) parseWithCTE() (*SelectStmtWithCTE, error) {
	stmt := &SelectStmtWithCTE{}
	p.advance() // consume WITH

	if p.match(TokenRecursive) {
		stmt.IsRecursive = true
	}

	// Parse CTE definitions
	for {
		cte := &CTEDef{}

		name, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		cte.Name = name.Literal

		// Optional column list
		if p.current().Type == TokenLParen {
			p.advance()
			columns, err := p.parseIdentifierList()
			if err != nil {
				return nil, err
			}
			cte.Columns = columns
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
		}

		if _, err := p.expect(TokenAs); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}

		query, err := p.parseSelect()
		if err != nil {
			return nil, fmt.Errorf("failed to parse CTE query: %w", err)
		}
		// Check for set operations (UNION ALL) inside CTE definition - needed for recursive CTEs
		var cteQuery Statement = query
		if p.current().Type == TokenUnion || p.current().Type == TokenIntersect || p.current().Type == TokenExcept {
			cteQuery, err = p.parseSetOp(query)
			if err != nil {
				return nil, fmt.Errorf("failed to parse CTE set operation: %w", err)
			}
		}
		cte.Query = cteQuery

		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}

		stmt.CTEs = append(stmt.CTEs, cte)

		if !p.match(TokenComma) {
			break
		}
	}

	// Parse the main SELECT
	if p.current().Type != TokenSelect {
		return nil, fmt.Errorf("expected SELECT after CTE definitions")
	}

	selectStmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	stmt.Select = selectStmt

	return stmt, nil
}

// parseVacuum parses a VACUUM statement
func (p *Parser) parseVacuum() (*VacuumStmt, error) {
	stmt := &VacuumStmt{}
	p.advance() // consume VACUUM

	// Optional table name
	if p.current().Type == TokenIdentifier {
		stmt.Table = p.current().Literal
		p.advance()
	}

	return stmt, nil
}

// parseAnalyze parses an ANALYZE statement
func (p *Parser) parseAnalyze() (*AnalyzeStmt, error) {
	stmt := &AnalyzeStmt{}
	p.advance() // consume ANALYZE

	// Optional table name
	if p.current().Type == TokenIdentifier {
		stmt.Table = p.current().Literal
		p.advance()
	}

	return stmt, nil
}

// parseRefresh parses a REFRESH MATERIALIZED VIEW statement
func (p *Parser) parseRefresh() (*RefreshMaterializedViewStmt, error) {
	p.advance() // consume REFRESH

	if _, err := p.expect(TokenMaterialized); err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenView); err != nil {
		return nil, err
	}

	name, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}

	return &RefreshMaterializedViewStmt{Name: name.Literal}, nil
}

// parseCreateMaterializedView parses CREATE MATERIALIZED VIEW
func (p *Parser) parseCreateMaterializedView() (*CreateMaterializedViewStmt, error) {
	stmt := &CreateMaterializedViewStmt{}
	p.advance() // consume MATERIALIZED
	if _, err := p.expect(TokenView); err != nil {
		return nil, err
	}

	if p.match(TokenIf) {
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

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

	if p.match(TokenIf) {
		if _, err := p.expect(TokenNot); err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenExists); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

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

// parseShow parses SHOW TABLES, SHOW CREATE TABLE, SHOW DATABASES, SHOW COLUMNS FROM
func (p *Parser) parseShow() (Statement, error) {
	p.advance() // consume SHOW

	switch p.current().Type {
	case TokenTables:
		p.advance()
		return &ShowTablesStmt{}, nil

	case TokenDatabases:
		p.advance()
		return &ShowDatabasesStmt{}, nil

	case TokenCreate:
		p.advance() // consume CREATE
		if _, err := p.expect(TokenTable); err != nil {
			return nil, fmt.Errorf("expected TABLE after SHOW CREATE")
		}
		tok := p.current()
		if tok.Type != TokenEOF && tok.Type != TokenSemicolon {
			p.advance()
			return &ShowCreateTableStmt{Table: tok.Literal}, nil
		}
		return nil, fmt.Errorf("expected table name after SHOW CREATE TABLE")

	case TokenColumns:
		p.advance() // consume COLUMNS
		if _, err := p.expect(TokenFrom); err != nil {
			return nil, fmt.Errorf("expected FROM after SHOW COLUMNS")
		}
		tok := p.current()
		p.advance()
		return &ShowColumnsStmt{Table: tok.Literal}, nil

	case TokenIdentifier:
		varName := p.current().Literal
		p.advance()
		upperVar := strings.ToUpper(varName)
		if upperVar == "STATUS" || upperVar == "VARIABLES" || upperVar == "WARNINGS" || upperVar == "ERRORS" {
			for p.current().Type != TokenSemicolon && p.current().Type != TokenEOF {
				p.advance()
			}
			return &ShowTablesStmt{}, nil
		}
		return nil, fmt.Errorf("unsupported SHOW %s", varName)

	default:
		return nil, fmt.Errorf("expected TABLES, CREATE, DATABASES, or COLUMNS after SHOW, got %s", p.current().Literal)
	}
}

// parseUse parses USE <database>
func (p *Parser) parseUse() (Statement, error) {
	p.advance() // consume USE

	tok := p.current()
	if tok.Type == TokenEOF {
		return nil, fmt.Errorf("expected database name after USE")
	}
	p.advance()
	return &UseStmt{Database: tok.Literal}, nil
}

// parseDescribe parses DESCRIBE <table>
func (p *Parser) parseDescribe() (Statement, error) {
	p.advance() // consume DESCRIBE

	tok := p.current()
	if tok.Type == TokenEOF {
		return nil, fmt.Errorf("expected table name after DESCRIBE")
	}
	p.advance()
	return &DescribeStmt{Table: tok.Literal}, nil
}

// parseExplain parses EXPLAIN <query>
func (p *Parser) parseExplain() (Statement, error) {
	p.advance() // consume EXPLAIN

	// Parse the inner statement (SELECT, INSERT, UPDATE, DELETE)
	innerStmt, err := p.Parse()
	if err != nil {
		return nil, fmt.Errorf("error parsing EXPLAIN statement: %w", err)
	}

	return &ExplainStmt{Statement: innerStmt}, nil
}

// parseSetVar parses SET <variable> = <value> (for MySQL compatibility)
func (p *Parser) parseSetVar() (Statement, error) {
	p.advance() // consume SET

	varParts := []string{}
	for p.current().Type != TokenEq && p.current().Type != TokenEOF && p.current().Type != TokenSemicolon {
		varParts = append(varParts, p.current().Literal)
		p.advance()
	}

	varName := strings.Join(varParts, " ")

	if p.current().Type != TokenEq {
		return &SetVarStmt{Variable: varName, Value: ""}, nil
	}

	p.advance() // consume =

	valueParts := []string{}
	for p.current().Type != TokenSemicolon && p.current().Type != TokenEOF {
		valueParts = append(valueParts, p.current().Literal)
		p.advance()
	}

	return &SetVarStmt{Variable: varName, Value: strings.Join(valueParts, " ")}, nil
}

// parseUnion parses UNION [ALL] SELECT ... chains (backward compat wrapper)
func (p *Parser) parseUnion(left Statement) (Statement, error) {
	return p.parseSetOp(left)
}

// parseSetOp parses UNION/INTERSECT/EXCEPT [ALL] SELECT ... chains
func (p *Parser) parseSetOp(left Statement) (Statement, error) {
	for p.current().Type == TokenUnion || p.current().Type == TokenIntersect || p.current().Type == TokenExcept {
		var op SetOpType
		var opName string
		switch p.current().Type {
		case TokenUnion:
			op = SetOpUnion
			opName = "UNION"
		case TokenIntersect:
			op = SetOpIntersect
			opName = "INTERSECT"
		case TokenExcept:
			op = SetOpExcept
			opName = "EXCEPT"
		}
		p.advance() // consume UNION/INTERSECT/EXCEPT

		all := false
		if p.current().Type == TokenAll {
			all = true
			p.advance() // consume ALL
		}

		if p.current().Type != TokenSelect {
			return nil, fmt.Errorf("expected SELECT after %s", opName)
		}

		right, err := p.parseSelect()
		if err != nil {
			return nil, err
		}

		stmt := &UnionStmt{
			Left:  left,
			Right: right,
			All:   all,
			Op:    op,
		}

		// The right SELECT may have consumed ORDER BY/LIMIT/OFFSET that actually
		// belong to the set operation. Move them from the right SELECT to the stmt.
		if right.OrderBy != nil {
			stmt.OrderBy = right.OrderBy
			right.OrderBy = nil
		}
		if right.Limit != nil {
			stmt.Limit = right.Limit
			right.Limit = nil
		}
		if right.Offset != nil {
			stmt.Offset = right.Offset
			right.Offset = nil
		}

		left = stmt
	}

	return left, nil
}

// isStructuralKeyword returns true for SQL keywords that should NOT be treated
// as identifiers in expression context. These are keywords that have syntactic
// meaning in SQL clauses and would cause misparses if treated as column names.
func isStructuralKeyword(t TokenType) bool {
	switch t {
	case TokenSelect, TokenFrom, TokenWhere, TokenInsert, TokenUpdate, TokenDelete,
		TokenCreate, TokenDrop, TokenAlter, TokenBegin, TokenCommit, TokenRollback,
		TokenGroup, TokenOrder, TokenHaving, TokenLimit, TokenOffset,
		TokenInto, TokenValues, TokenSet, TokenJoin, TokenInner, TokenLeft, TokenRight,
		TokenOuter, TokenFull, TokenCross, TokenOn, TokenAnd, TokenOr, TokenNot,
		TokenIn, TokenBetween, TokenLike, TokenIs, TokenAs,
		TokenTable, TokenIndex, TokenView, TokenTrigger, TokenProcedure,
		TokenIf, TokenThen, TokenElse, TokenWhen, TokenEnd, TokenCase,
		TokenWith, TokenRecursive, TokenDistinct, TokenAll, TokenUnion, TokenIntersect, TokenExcept,
		TokenExists, TokenBy, TokenAsc, TokenDesc,
		TokenVacuum, TokenAnalyze, TokenRefresh, TokenShow, TokenUse, TokenDescribe,
		TokenCall, TokenMaterialized, TokenFulltext, TokenEscape,
		TokenSavepoint, TokenRelease, TokenTo:
		return true
	}
	return false
}
