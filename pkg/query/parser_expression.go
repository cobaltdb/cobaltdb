package query

import (
	"fmt"
	"strings"
)

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

		// Optional NULLS FIRST / NULLS LAST (NULLS/FIRST/LAST are non-keyword
		// identifiers in this lexer).
		if p.current().Type == TokenIdentifier && strings.EqualFold(p.current().Literal, "NULLS") {
			p.advance() // consume NULLS
			switch {
			case p.current().Type == TokenIdentifier && strings.EqualFold(p.current().Literal, "FIRST"):
				p.advance()
				orderExpr.NullsFirst = true
				orderExpr.NullsSpecified = true
			case p.current().Type == TokenIdentifier && strings.EqualFold(p.current().Literal, "LAST"):
				p.advance()
				orderExpr.NullsFirst = false
				orderExpr.NullsSpecified = true
			default:
				return nil, fmt.Errorf("expected FIRST or LAST after NULLS, got %s", p.current().Literal)
			}
		}

		exprs = append(exprs, orderExpr)
		if len(exprs) > maxParserListItems {
			return nil, fmt.Errorf("ORDER BY expression count exceeds maximum (%d)", maxParserListItems)
		}

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
		if len(exprs) > maxParserListItems {
			return nil, fmt.Errorf("expression list count exceeds maximum (%d)", maxParserListItems)
		}

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

// parseExpressionWithOffset parses an expression with placeholder offset.
//
//nolint:unused // retained for parser compatibility tests and future placeholder rewrites.
func (p *Parser) parseExpressionWithOffset(offset int) (Expression, error) {
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	// Apply offset to all placeholders in the expression
	applyPlaceholderOffset(expr, offset)
	return expr, nil
}

// parseBinaryOpLevel is a generic helper for binary operator precedence levels.
// It parses a left-hand side, then consumes operators matching any of `ops`,
// building left-associative BinaryExpr nodes until no more operators match.
func (p *Parser) parseBinaryOpLevel(next func() (Expression, error), ops ...TokenType) (Expression, error) {
	left, err := next()
	if err != nil {
		return nil, err
	}
	for p.current().Type.isOneOf(ops...) {
		op := p.current().Type
		p.advance()
		right, err := next()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Operator: op, Right: right}
	}
	return left, nil
}

// parseOr parses OR expressions
func (p *Parser) parseOr() (Expression, error) {
	return p.parseBinaryOpLevel(p.parseAnd, TokenOr)
}

// parseAnd parses AND expressions
func (p *Parser) parseAnd() (Expression, error) {
	return p.parseBinaryOpLevel(p.parseNot, TokenAnd)
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
		if err := p.enterDepth(); err != nil {
			return nil, err
		}
		defer p.leaveDepth()
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
	left, err := p.parseBitOr()
	if err != nil {
		return nil, err
	}

	switch p.current().Type {
	case TokenEq, TokenNeq, TokenLt, TokenGt, TokenLte, TokenGte, TokenNullSafeEq:
		op := p.current().Type
		p.advance()
		right, err := p.parseBitOr()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Operator: op, Right: right}, nil
	case TokenLike:
		return p.parseLikeExpr(left, false)
	case TokenNot:
		if p.peek().Type == TokenIn {
			p.advance()
			return p.parseInExpr(left, true)
		} else if p.peek().Type == TokenLike {
			p.advance()
			return p.parseLikeExpr(left, true)
		} else if p.peek().Type == TokenRegexp {
			p.advance()
			return p.parseRegexpExpr(left, true)
		} else if isKeywordIdentifier(p.peek(), "GLOB") {
			p.advance()
			return p.parseGlobExpr(left, true)
		} else if p.peek().Type == TokenBetween {
			p.advance()
			return p.parseBetweenExpr(left, true)
		}
	case TokenIn:
		if p.stopAtMatchAgainstMode && (p.peek().Type == TokenBoolean || p.peek().Type == TokenNatural) {
			return left, nil
		}
		return p.parseInExpr(left, false)
	case TokenRegexp:
		return p.parseRegexpExpr(left, false)
	default:
		if isKeywordIdentifier(p.current(), "GLOB") {
			return p.parseGlobExpr(left, false)
		}
	case TokenBetween:
		return p.parseBetweenExpr(left, false)
	case TokenIs:
		p.advance()
		not := p.match(TokenNot)
		if p.current().Type == TokenDistinct {
			p.advance()
			if p.current().Type != TokenFrom {
				return nil, fmt.Errorf("expected FROM after IS DISTINCT")
			}
			p.advance()
			right, err := p.parseBitOr()
			if err != nil {
				return nil, err
			}
			expr := &BinaryExpr{Left: left, Operator: TokenNullSafeEq, Right: right}
			if not {
				return expr, nil
			}
			return &UnaryExpr{Operator: TokenNot, Expr: expr}, nil
		}
		if p.current().Type == TokenTrue || p.current().Type == TokenFalse || p.current().Type == TokenUnknown {
			test := p.current().Type
			p.advance()
			var name string
			switch test {
			case TokenTrue:
				name = "IS_TRUE"
			case TokenFalse:
				name = "IS_FALSE"
			case TokenUnknown:
				name = "IS_UNKNOWN"
			}
			expr := &FunctionCall{Name: name, Args: []Expression{left}}
			if not {
				return &UnaryExpr{Operator: TokenNot, Expr: expr}, nil
			}
			return expr, nil
		}
		if !p.match(TokenNull) {
			return nil, fmt.Errorf("expected NULL after IS")
		}
		return &IsNullExpr{Expr: left, Not: not}, nil
	}

	return left, nil
}

func (p *Parser) parseInExpr(left Expression, not bool) (Expression, error) {
	p.advance() // consume IN
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}
	var subquery *SelectStmt
	var list []Expression
	if p.current().Type == TokenSelect {
		var err error
		subquery, err = p.parseSelect()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	} else {
		var err error
		list, err = p.parseExpressionList()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}
	return &InExpr{Expr: left, List: list, Not: not, Subquery: subquery}, nil
}

func (p *Parser) parseLikeExpr(left Expression, not bool) (Expression, error) {
	p.advance() // consume LIKE
	if !not {
		not = p.match(TokenNot)
	}
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
	return &LikeExpr{Expr: left, Pattern: pattern, Not: not, Escape: escape}, nil
}

func (p *Parser) parseRegexpExpr(left Expression, not bool) (Expression, error) {
	p.advance() // consume REGEXP/RLIKE
	pattern, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	expr := &FunctionCall{
		Name: "REGEXP_LIKE",
		Args: []Expression{left, pattern},
	}
	if not {
		return &UnaryExpr{Operator: TokenNot, Expr: expr}, nil
	}
	return expr, nil
}

func (p *Parser) parseGlobExpr(left Expression, not bool) (Expression, error) {
	p.advance() // consume GLOB
	pattern, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	expr := &FunctionCall{
		Name: "GLOB",
		Args: []Expression{pattern, left},
	}
	if not {
		return &UnaryExpr{Operator: TokenNot, Expr: expr}, nil
	}
	return expr, nil
}

func isKeywordIdentifier(tok Token, keyword string) bool {
	return tok.Type == TokenIdentifier && strings.EqualFold(tok.Literal, keyword)
}

func (p *Parser) parseBetweenExpr(left Expression, not bool) (Expression, error) {
	p.advance() // consume BETWEEN
	if !not {
		not = p.match(TokenNot)
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
}

// parseAdditive parses + and - expressions
// Bitwise precedence (MySQL): | lower than &, which is lower than << >>,
// which sits above additive. ^ binds tighter than * / % (just above unary).
func (p *Parser) parseBitOr() (Expression, error) {
	return p.parseBinaryOpLevel(p.parseBitAnd, TokenBitOr)
}

func (p *Parser) parseBitAnd() (Expression, error) {
	return p.parseBinaryOpLevel(p.parseBitShift, TokenBitAnd)
}

func (p *Parser) parseBitShift() (Expression, error) {
	return p.parseBinaryOpLevel(p.parseAdditive, TokenShiftLeft, TokenShiftRight)
}

func (p *Parser) parseAdditive() (Expression, error) {
	return p.parseBinaryOpLevel(p.parseMultiplicative, TokenPlus, TokenMinus, TokenConcat)
}

// parseMultiplicative parses *, /, % expressions
func (p *Parser) parseMultiplicative() (Expression, error) {
	left, err := p.parseBitXor()
	if err != nil {
		return nil, err
	}
	for {
		op := p.current().Type
		if op != TokenStar && op != TokenSlash && op != TokenPercent && !isModOperator(p.current()) {
			break
		}
		if isModOperator(p.current()) {
			op = TokenPercent
		}
		p.advance()
		right, err := p.parseBitXor()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Operator: op, Right: right}
	}
	return left, nil
}

func isModOperator(tok Token) bool {
	return tok.Type == TokenIdentifier && strings.EqualFold(tok.Literal, "MOD")
}

func (p *Parser) parseBitXor() (Expression, error) {
	return p.parseBinaryOpLevel(p.parseUnary, TokenBitXor)
}

// parseUnary parses unary expressions
func (p *Parser) parseUnary() (Expression, error) {
	if p.current().Type == TokenMinus || p.current().Type == TokenPlus || p.current().Type == TokenBitNot {
		op := p.current().Type
		p.advance()
		if err := p.enterDepth(); err != nil {
			return nil, err
		}
		defer p.leaveDepth()
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
	case TokenDefault:
		// `DEFAULT` as a value (INSERT ... VALUES (..., DEFAULT)).
		p.advance()
		return &DefaultExpr{}, nil
	case TokenValues:
		if p.peek().Type == TokenLParen {
			return p.parseIdentifierOrFunction()
		}
		return nil, fmt.Errorf("unexpected token: %s", p.current().Literal)
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
	// Vector similarity functions
	case TokenCosineSimilarity, TokenL2Distance, TokenInnerProduct:
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
	case TokenLBracket:
		return p.parseVectorLiteral()
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

// parseCaseExpr parses a CASE expression
// CASE [expr] WHEN cond1 THEN result1 [WHEN cond2 THEN result2]... [ELSE default] END
func (p *Parser) parseCaseExpr() (Expression, error) {
	p.advance() // consume CASE

	caseExpr := &CaseExpr{}
	hasSimpleCase := false

	// Check for simple CASE: CASE expr WHEN ...
	if p.current().Type != TokenWhen {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		caseExpr.Expr = expr
		hasSimpleCase = true
	}

	// Parse WHEN clauses
	for p.current().Type == TokenWhen {
		p.advance() // consume WHEN

		cond, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		// For simple CASE, wrap condition as: expr = cond
		if hasSimpleCase && caseExpr.Expr != nil {
			cond = &BinaryExpr{
				Left:     caseExpr.Expr,
				Operator: TokenEq,
				Right:    cond,
			}
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

	// Skip optional type parameters, e.g. DECIMAL(10,2), VARCHAR(255).
	if p.current().Type == TokenLParen {
		depth := 0
		for {
			switch p.current().Type {
			case TokenLParen:
				depth++
			case TokenRParen:
				depth--
			case TokenEOF:
				return nil, fmt.Errorf("unterminated type parameters in CAST")
			}
			p.advance()
			if depth == 0 {
				break
			}
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return &CastExpr{Expr: expr, DataType: dataType}, nil
}

// parseIdentifierOrFunction parses an identifier or function call
func (p *Parser) parseIdentifierOrFunction() (Expression, error) {
	tok := p.current()
	p.advance()

	// Check for qualified identifier (table.column) or qualified star (table.*)
	if p.match(TokenDot) {
		if p.current().Type == TokenStar {
			p.advance()
			return &StarExpr{Table: tok.Literal}, nil
		}
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

// extractFieldToFunc maps an EXTRACT() field name to the equivalent scalar
// date function name.
func extractFieldToFunc(field string) string {
	switch field {
	case "DOW", "DAYOFWEEK":
		return "DAYOFWEEK"
	case "DOY", "DAYOFYEAR":
		return "DAYOFYEAR"
	default:
		// YEAR, MONTH, DAY, HOUR, MINUTE, SECOND map by name; unknown fields
		// fall through and error at evaluation.
		return field
	}
}

// parseFunctionCall parses a function call
func (p *Parser) parseFunctionCall(name string) (Expression, error) {
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	upperName := toUpperFast(name)

	// EXTRACT(field FROM source) -> the corresponding date-field function.
	if upperName == "EXTRACT" {
		field := strings.ToUpper(p.current().Literal)
		p.advance()
		if p.current().Type != TokenFrom {
			return nil, fmt.Errorf("expected FROM in EXTRACT, got %s", p.current().Literal)
		}
		p.advance()
		source, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return &FunctionCall{Name: extractFieldToFunc(field), Args: []Expression{source}}, nil
	}

	// POSITION(substr IN str) -> LOCATE(substr, str). Parse substr below the
	// comparison level so the IN keyword is not consumed as an IN-list operator.
	if upperName == "POSITION" {
		substr, err := p.parseBitOr()
		if err != nil {
			return nil, err
		}
		if p.current().Type == TokenIn {
			p.advance()
			str, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
			return &FunctionCall{Name: "LOCATE", Args: []Expression{substr, str}}, nil
		}
		// Otherwise treat as a normal comma-separated argument list.
		args := []Expression{substr}
		for p.match(TokenComma) {
			a, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			args = append(args, a)
			if len(args) > maxParserListItems {
				return nil, fmt.Errorf("function argument count exceeds maximum (%d)", maxParserListItems)
			}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return &FunctionCall{Name: "LOCATE", Args: args}, nil
	}

	// Check for DISTINCT keyword (e.g., COUNT(DISTINCT col))
	distinct := false
	if p.current().Type == TokenDistinct {
		distinct = true
		p.advance()
	} else if p.current().Type == TokenAll {
		// ALL is the default aggregate quantifier; consume it so
		// COUNT(ALL col) parses the same as COUNT(col).
		p.advance()
	}

	if upperName == "GROUP_CONCAT" {
		return p.parseGroupConcatCall(name, distinct)
	}

	var args []Expression
	if p.current().Type != TokenRParen {
		for {
			arg, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			if len(args) > maxParserListItems {
				return nil, fmt.Errorf("function argument count exceeds maximum (%d)", maxParserListItems)
			}

			if !p.match(TokenComma) {
				break
			}
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	filter, err := p.parseFunctionFilter()
	if err != nil {
		return nil, err
	}

	// Check for OVER clause (window function)
	if p.current().Type == TokenOver {
		return p.parseWindowExpr(toUpperFast(name), args, filter)
	}

	return &FunctionCall{Name: toUpperFast(name), Args: args, Distinct: distinct, Filter: filter}, nil
}

func (p *Parser) parseGroupConcatCall(name string, distinct bool) (Expression, error) {
	var args []Expression
	var orderBy []*OrderByExpr
	if p.current().Type != TokenRParen {
		arg, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		if len(args) > maxParserListItems {
			return nil, fmt.Errorf("function argument count exceeds maximum (%d)", maxParserListItems)
		}

		if p.current().Type == TokenOrder {
			p.advance()
			if _, err := p.expect(TokenBy); err != nil {
				return nil, err
			}
			parsedOrderBy, err := p.parseGroupConcatOrderByList()
			if err != nil {
				return nil, err
			}
			orderBy = parsedOrderBy
		}

		if p.current().Type == TokenIdentifier && strings.EqualFold(p.current().Literal, "SEPARATOR") {
			p.advance()
			sep, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			args = append(args, sep)
			if len(args) > maxParserListItems {
				return nil, fmt.Errorf("function argument count exceeds maximum (%d)", maxParserListItems)
			}
		} else {
			for p.match(TokenComma) {
				arg, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
				if len(args) > maxParserListItems {
					return nil, fmt.Errorf("function argument count exceeds maximum (%d)", maxParserListItems)
				}
			}
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	filter, err := p.parseFunctionFilter()
	if err != nil {
		return nil, err
	}

	if p.current().Type == TokenOver {
		return p.parseWindowExpr(toUpperFast(name), args, filter)
	}

	return &FunctionCall{Name: toUpperFast(name), Args: args, Distinct: distinct, OrderBy: orderBy, Filter: filter}, nil
}

func (p *Parser) parseFunctionFilter() (Expression, error) {
	if p.current().Type != TokenIdentifier || !strings.EqualFold(p.current().Literal, "FILTER") {
		return nil, nil
	}
	p.advance()
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenWhere); err != nil {
		return nil, err
	}
	filter, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}
	return filter, nil
}

func (p *Parser) parseGroupConcatOrderByList() ([]*OrderByExpr, error) {
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
			p.match(TokenAsc)
		}
		if p.current().Type == TokenIdentifier && strings.EqualFold(p.current().Literal, "NULLS") {
			p.advance()
			switch {
			case p.current().Type == TokenIdentifier && strings.EqualFold(p.current().Literal, "FIRST"):
				p.advance()
				orderExpr.NullsFirst = true
				orderExpr.NullsSpecified = true
			case p.current().Type == TokenIdentifier && strings.EqualFold(p.current().Literal, "LAST"):
				p.advance()
				orderExpr.NullsFirst = false
				orderExpr.NullsSpecified = true
			default:
				return nil, fmt.Errorf("expected FIRST or LAST after NULLS, got %s", p.current().Literal)
			}
		}
		exprs = append(exprs, orderExpr)
		if len(exprs) > maxParserListItems {
			return nil, fmt.Errorf("ORDER BY expression count exceeds maximum (%d)", maxParserListItems)
		}

		if !p.match(TokenComma) {
			break
		}
	}
	return exprs, nil
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
