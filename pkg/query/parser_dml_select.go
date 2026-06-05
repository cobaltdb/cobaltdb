package query

import (
	"fmt"
	"strconv"
	"strings"
)

// parseSelect parses a SELECT statement
func (p *Parser) parseSelect() (*SelectStmt, error) {
	stmt := &SelectStmt{}
	p.advance() // consume SELECT

	// DISTINCT?
	if p.match(TokenDistinct) {
		stmt.Distinct = true
	} else {
		// ALL is the default SELECT mode; consume it when present.
		_ = p.match(TokenAll)
	}

	// Column list
	columns, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	// FROM
	if p.match(TokenFrom) {
		if err := p.parseFromAndJoins(stmt); err != nil {
			return nil, err
		}
	}

	// AS OF (temporal queries)
	if p.match(TokenAs) {
		if p.match(TokenOf) {
			temporal, err := p.parseTemporalExpr()
			if err != nil {
				return nil, err
			}
			stmt.AsOf = temporal
		}
	}

	// WHERE
	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
		reindexPlaceholders(where, 0)
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
	} else if p.current().Type == TokenIdentifier && !isStructuralKeyword(p.current().Type) {
		// Implicit alias (no AS keyword): SELECT expr alias.
		// Identifiers have their own token type, so SQL keywords that legitimately
		// follow a select item (FROM, comma, etc.) are never matched here.
		alias := p.current()
		p.advance()
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
	} else if p.current().Type == TokenAs {
		// Check if this is AS OF (temporal) or AS alias
		// Look ahead: if next token is OF, this is AS OF, not alias
		if p.peek().Type == TokenOf {
			// This is AS OF, don't consume AS here - it will be handled by parseSelect
			return ref, nil
		}
		// This is AS alias
		p.advance() // consume AS
		alias, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		ref.Alias = alias.Literal
	}

	return ref, nil
}

// parseJoin parses a JOIN clause
func (p *Parser) parseJoin() (*JoinClause, error) {
	join := &JoinClause{}
	p.parseJoinType(join)

	table, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	join.Table = table

	if err := p.parseJoinCondition(join); err != nil {
		return nil, err
	}
	return join, nil
}

func (p *Parser) parseJoinType(join *JoinClause) {
	switch p.current().Type {
	case TokenInner:
		join.Type = TokenInner
		p.advance()
		_, _ = p.strictExpect(TokenJoin)
	case TokenLeft:
		join.Type = TokenLeft
		p.advance()
		_ = p.match(TokenOuter)
		_, _ = p.strictExpect(TokenJoin)
	case TokenRight:
		join.Type = TokenRight
		p.advance()
		_ = p.match(TokenOuter)
		_, _ = p.strictExpect(TokenJoin)
	case TokenFull:
		join.Type = TokenFull
		p.advance()
		_ = p.match(TokenOuter)
		_, _ = p.strictExpect(TokenJoin)
	case TokenOuter:
		p.advance()
		join.Type = TokenFull
		_, _ = p.strictExpect(TokenJoin)
	case TokenCross:
		join.Type = TokenCross
		p.advance()
		_, _ = p.strictExpect(TokenJoin)
	case TokenNatural:
		join.Natural = true
		p.advance()
		// strictExpect only after sub-type parse so strict mode rejects
		// "NATURAL INNER b" where b is not the JOIN token.
		p.parseNaturalJoinType(join)
		_, _ = p.strictExpect(TokenJoin)
	case TokenJoin:
		join.Type = TokenJoin
		p.advance()
	}
}

func (p *Parser) parseNaturalJoinType(join *JoinClause) {
	switch p.current().Type {
	case TokenInner:
		join.Type = TokenInner
		p.advance()
	case TokenLeft:
		join.Type = TokenLeft
		p.advance()
		_ = p.match(TokenOuter)
	case TokenRight:
		join.Type = TokenRight
		p.advance()
		_ = p.match(TokenOuter)
	case TokenFull:
		join.Type = TokenFull
		p.advance()
		_ = p.match(TokenOuter)
	default:
		join.Type = TokenInner
	}
}

func (p *Parser) parseJoinCondition(join *JoinClause) error {
	switch {
	case join.Type == TokenCross:
		if p.current().Type == TokenOn {
			p.advance()
			cond, err := p.parseExpression()
			if err != nil {
				return err
			}
			join.Condition = cond
		}
	case p.current().Type == TokenUsing:
		p.advance()
		if _, err := p.expect(TokenLParen); err != nil {
			return err
		}
		columns, err := p.parseIdentifierList()
		if err != nil {
			return fmt.Errorf("USING clause: %w", err)
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return err
		}
		join.Using = columns
	case join.Natural:
		// NATURAL: condition from common columns; optionally accept ON / USING.
		if p.current().Type == TokenOn {
			p.advance()
			cond, err := p.parseExpression()
			if err != nil {
				return err
			}
			join.Condition = cond
		} else if p.current().Type == TokenUsing {
			p.advance()
			if _, err := p.expect(TokenLParen); err != nil {
				return err
			}
			columns, err := p.parseIdentifierList()
			if err != nil {
				return fmt.Errorf("USING clause: %w", err)
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return err
			}
			join.Using = columns
		}
	default:
		if _, err := p.expect(TokenOn); err != nil {
			return err
		}
		cond, err := p.parseExpression()
		if err != nil {
			return err
		}
		join.Condition = cond
	}
	return nil
}

// parseFromAndJoins parses FROM table references, comma-separated cross joins,
// and explicit JOIN clauses into stmt.From and stmt.Joins.
func (p *Parser) parseFromAndJoins(stmt *SelectStmt) error {
	table, err := p.parseTableRef()
	if err != nil {
		return err
	}
	stmt.From = table

	for p.match(TokenComma) {
		crossTable, err := p.parseTableRef()
		if err != nil {
			return err
		}
		stmt.Joins = append(stmt.Joins, &JoinClause{
			Type:  TokenCross,
			Table: crossTable,
		})
	}

	for p.isJoin() {
		join, err := p.parseJoin()
		if err != nil {
			return err
		}
		stmt.Joins = append(stmt.Joins, join)
	}
	return nil
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

	// Parse optional window frame clause (ROWS/RANGE ...)
	if p.current().Type == TokenRows || p.current().Type == TokenRange {
		frame, err := p.parseWindowFrame()
		if err != nil {
			return nil, err
		}
		windowExpr.Frame = frame
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return windowExpr, nil
}

// parseWindowFrame parses ROWS|RANGE [BETWEEN] <bound> [AND <bound>].
func (p *Parser) parseWindowFrame() (*WindowFrame, error) {
	frame := &WindowFrame{}
	if p.current().Type == TokenRange {
		frame.Mode = "RANGE"
	} else {
		frame.Mode = "ROWS"
	}
	p.advance() // consume ROWS/RANGE

	if p.match(TokenBetween) {
		start, err := p.parseWindowFrameBound()
		if err != nil {
			return nil, err
		}
		frame.Start = start
		if _, err := p.expect(TokenAnd); err != nil {
			return nil, err
		}
		end, err := p.parseWindowFrameBound()
		if err != nil {
			return nil, err
		}
		frame.End = end
	} else {
		// Single bound form: "ROWS <bound>" means start..CURRENT ROW.
		start, err := p.parseWindowFrameBound()
		if err != nil {
			return nil, err
		}
		frame.Start = start
		frame.End = &WindowFrameBound{Type: "CURRENT_ROW"}
	}
	return frame, nil
}

// parseWindowFrameBound parses one frame bound: UNBOUNDED PRECEDING|FOLLOWING,
// CURRENT ROW, or <n> PRECEDING|FOLLOWING.
func (p *Parser) parseWindowFrameBound() (*WindowFrameBound, error) {
	switch {
	case p.match(TokenUnbounded):
		if p.match(TokenPreceding) {
			return &WindowFrameBound{Type: "UNBOUNDED_PRECEDING"}, nil
		}
		if p.match(TokenFollowing) {
			return &WindowFrameBound{Type: "UNBOUNDED_FOLLOWING"}, nil
		}
		return nil, fmt.Errorf("expected PRECEDING or FOLLOWING after UNBOUNDED")
	case p.match(TokenCurrent):
		if _, err := p.expect(TokenRow); err != nil {
			return nil, err
		}
		return &WindowFrameBound{Type: "CURRENT_ROW"}, nil
	case p.current().Type == TokenNumber:
		offset, err := strconv.Atoi(p.current().Literal)
		if err != nil {
			return nil, fmt.Errorf("invalid frame offset: %s", p.current().Literal)
		}
		p.advance()
		if p.match(TokenPreceding) {
			return &WindowFrameBound{Type: "PRECEDING", Offset: offset}, nil
		}
		if p.match(TokenFollowing) {
			return &WindowFrameBound{Type: "FOLLOWING", Offset: offset}, nil
		}
		return nil, fmt.Errorf("expected PRECEDING or FOLLOWING after frame offset")
	}
	return nil, fmt.Errorf("invalid window frame bound at %s", p.current().Literal)
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
		if err := p.parseInsertTail(stmt); err != nil {
			return nil, err
		}
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

	if err := p.parseInsertTail(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseInsertTail parses the optional ON CONFLICT and RETURNING clauses that may
// follow either INSERT ... VALUES or INSERT ... SELECT.
func (p *Parser) parseInsertTail(stmt *InsertStmt) error {
	// ON CONFLICT comes before RETURNING per SQL syntax.
	if p.current().Type == TokenOn && p.peek().Type == TokenConflict {
		oc, err := p.parseOnConflict()
		if err != nil {
			return err
		}
		stmt.OnConflict = oc
		// DO NOTHING reuses the existing ConflictIgnore skip path.
		if oc.DoUpdate == nil {
			stmt.ConflictAction = ConflictIgnore
		}
	}

	if p.current().Type == TokenReturning {
		p.advance() // consume RETURNING
		returning, err := p.parseReturningClause()
		if err != nil {
			return err
		}
		stmt.Returning = returning
	}
	return nil
}

// parseOnConflict parses `ON CONFLICT [(col, ...)] DO NOTHING | DO UPDATE SET ...`.
func (p *Parser) parseOnConflict() (*OnConflictClause, error) {
	p.advance() // consume ON
	p.advance() // consume CONFLICT

	oc := &OnConflictClause{}

	// Optional conflict target column list.
	if p.match(TokenLParen) {
		for {
			col := p.current()
			if col.Type != TokenIdentifier && col.Literal == "" {
				return nil, fmt.Errorf("expected column name in ON CONFLICT target")
			}
			oc.Columns = append(oc.Columns, col.Literal)
			p.advance()
			if !p.match(TokenComma) {
				break
			}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(TokenDo); err != nil {
		return nil, err
	}

	if p.match(TokenNothing) {
		return oc, nil // DO NOTHING
	}

	if _, err := p.expect(TokenUpdate); err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenSet); err != nil {
		return nil, err
	}
	clauses, _, err := p.parseSetClauses()
	if err != nil {
		return nil, err
	}
	oc.DoUpdate = clauses
	return oc, nil
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

	setClauses, setPlaceholders, err := p.parseSetClauses()
	if err != nil {
		return nil, err
	}
	stmt.Set = setClauses

	whereOffset := len(setPlaceholders)

	if p.match(TokenFrom) {
		whereOffset, err = p.parseUpdateFromJoin(stmt, whereOffset)
		if err != nil {
			return nil, err
		}
	}

	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
		reindexPlaceholders(where, whereOffset)
	}

	if p.current().Type == TokenReturning {
		p.advance()
		returning, err := p.parseReturningClause()
		if err != nil {
			return nil, err
		}
		stmt.Returning = returning
	}

	return stmt, nil
}

func (p *Parser) parseSetClauses() ([]*SetClause, []*PlaceholderExpr, error) {
	var clauses []*SetClause
	var placeholders []*PlaceholderExpr

	for {
		col := p.current()
		if col.Type == TokenIdentifier || (col.Literal != "" && col.Type != TokenEOF && col.Type != TokenEq) {
			p.advance()
		} else {
			return nil, nil, fmt.Errorf("expected column name, got %s", col.Literal)
		}

		if _, err := p.expect(TokenEq); err != nil {
			return nil, nil, err
		}

		val, err := p.parseExpression()
		if err != nil {
			return nil, nil, err
		}

		clauses = append(clauses, &SetClause{Column: col.Literal, Value: val})
		placeholders = append(placeholders, collectPlaceholders(val)...)

		if !p.match(TokenComma) {
			break
		}
	}

	for i, ph := range placeholders {
		ph.Index = i
	}
	return clauses, placeholders, nil
}

func (p *Parser) parseUpdateFromJoin(stmt *UpdateStmt, whereOffset int) (int, error) {
	table, err := p.parseTableRef()
	if err != nil {
		return whereOffset, err
	}
	stmt.From = table

	for p.match(TokenComma) {
		crossTable, err := p.parseTableRef()
		if err != nil {
			return whereOffset, err
		}
		stmt.Joins = append(stmt.Joins, &JoinClause{
			Type:  TokenCross,
			Table: crossTable,
		})
	}

	for p.isJoin() {
		join, err := p.parseJoin()
		if err != nil {
			return whereOffset, err
		}
		stmt.Joins = append(stmt.Joins, join)
	}

	offset := whereOffset
	for _, join := range stmt.Joins {
		if join.Condition != nil {
			joinPHs := collectPlaceholders(join.Condition)
			reindexPlaceholdersFromExpr(join.Condition, offset)
			offset += len(joinPHs)
		}
	}
	return offset, nil
}

// parseDelete parses a DELETE statement
// parseTruncate parses `TRUNCATE [TABLE] name`, modeled as an unconditional
// DELETE so it reuses the delete execution path.
func (p *Parser) parseTruncate() (Statement, error) {
	p.advance() // consume TRUNCATE
	p.match(TokenTable)
	table, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	return &DeleteStmt{Table: table.Literal}, nil
}

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

	case TokenIndex:
		p.advance() // consume INDEX
		if _, err := p.expect(TokenFrom); err != nil {
			return nil, fmt.Errorf("expected FROM after SHOW INDEX")
		}
		tok := p.current()
		p.advance()
		return &ShowIndexStmt{Table: tok.Literal}, nil

	case TokenIdentifier:
		varName := p.current().Literal
		p.advance()
		upperVar := toUpperFast(varName)
		if upperVar == "INDEXES" || upperVar == "KEYS" {
			if _, err := p.expect(TokenFrom); err != nil {
				return nil, fmt.Errorf("expected FROM after SHOW %s", varName)
			}
			tok := p.current()
			p.advance()
			return &ShowIndexStmt{Table: tok.Literal}, nil
		}
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

// parseUnion parses UNION [ALL] SELECT ... chains (backward compat wrapper).
//
//nolint:unused // retained for parser compatibility tests.
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
