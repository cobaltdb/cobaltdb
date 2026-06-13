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
		if !p.match(TokenAll) {
			limit, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			stmt.Limit = limit
		}
	}

	// OFFSET
	if p.match(TokenOffset) {
		offset, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Offset = offset
	}

	if p.current().Type == TokenFor {
		locking, err := p.parseSelectLockingClause()
		if err != nil {
			return nil, err
		}
		stmt.Locking = locking
	}

	return stmt, nil
}

func (p *Parser) parseSelectLockingClause() (*SelectLockingClause, error) {
	p.advance() // consume FOR

	locking := &SelectLockingClause{}
	switch {
	case p.match(TokenUpdate):
		locking.Mode = "UPDATE"
	case isKeywordIdentifier(p.current(), "SHARE"):
		p.advance()
		locking.Mode = "SHARE"
	default:
		return nil, fmt.Errorf("expected UPDATE or SHARE after FOR, got %s", p.current().Literal)
	}

	if p.match(TokenOf) {
		targets, err := p.parseSelectLockingTargets()
		if err != nil {
			return nil, err
		}
		locking.Targets = targets
	}

	switch {
	case isKeywordIdentifier(p.current(), "NOWAIT"):
		p.advance()
		locking.WaitPolicy = "NOWAIT"
	case isKeywordIdentifier(p.current(), "SKIP"):
		p.advance()
		if !isKeywordIdentifier(p.current(), "LOCKED") {
			return nil, fmt.Errorf("expected LOCKED after SKIP, got %s", p.current().Literal)
		}
		p.advance()
		locking.WaitPolicy = "SKIP LOCKED"
	case isKeywordIdentifier(p.current(), "WAIT"):
		p.advance()
		wait, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		locking.WaitPolicy = "WAIT"
		locking.WaitValue = wait
	}

	return locking, nil
}

func (p *Parser) parseSelectLockingTargets() ([]string, error) {
	var targets []string
	for {
		if isSelectLockingClauseBoundary(p.current()) {
			if len(targets) == 0 {
				return nil, fmt.Errorf("expected lock target after OF")
			}
			break
		}

		target, err := p.parseSelectLockingTarget()
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
		if !p.match(TokenComma) {
			break
		}
	}
	return targets, nil
}

func (p *Parser) parseSelectLockingTarget() (string, error) {
	tok := p.current()
	if tok.Type == TokenEOF || tok.Type == TokenSemicolon || tok.Type == TokenComma {
		return "", fmt.Errorf("expected lock target, got %s", tok.Literal)
	}
	name := tok.Literal
	p.advance()
	for p.match(TokenDot) {
		next := p.current()
		if next.Type == TokenEOF || next.Type == TokenSemicolon || next.Type == TokenComma {
			return "", fmt.Errorf("expected identifier after . in lock target")
		}
		name += "." + next.Literal
		p.advance()
	}
	return name, nil
}

func isSelectLockingClauseBoundary(tok Token) bool {
	if tok.Type == TokenEOF || tok.Type == TokenSemicolon ||
		tok.Type == TokenUnion || tok.Type == TokenIntersect || tok.Type == TokenExcept {
		return true
	}
	return isKeywordIdentifier(tok, "NOWAIT") ||
		isKeywordIdentifier(tok, "SKIP") ||
		isKeywordIdentifier(tok, "WAIT")
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
		if len(columns) > maxParserListItems {
			return nil, fmt.Errorf("SELECT column count exceeds maximum (%d)", maxParserListItems)
		}

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
			// Parse alias. If the query omits one, generate an internal name so
			// execution paths that materialize derived tables can still address it.
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
				ref.Alias = p.nextDerivedTableAlias()
				ref.Name = ref.Alias
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
	if p.current().Type == TokenIdentifier && !isTableIndexHintStart(p.current()) {
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

	if err := p.parseTableIndexHint(ref); err != nil {
		return nil, err
	}

	return ref, nil
}

func (p *Parser) nextDerivedTableAlias() string {
	p.derivedAliasCount++
	return fmt.Sprintf("__derived_%d", p.derivedAliasCount)
}

func isTableIndexHintStart(tok Token) bool {
	return isKeywordIdentifier(tok, "INDEXED") || tok.Type == TokenNot
}

func (p *Parser) parseTableIndexHint(ref *TableRef) error {
	switch {
	case isKeywordIdentifier(p.current(), "INDEXED"):
		p.advance()
		if _, err := p.expect(TokenBy); err != nil {
			return err
		}
		idx := p.current()
		if idx.Type == TokenEOF || idx.Type == TokenSemicolon || idx.Type == TokenComma {
			return fmt.Errorf("expected index name after INDEXED BY")
		}
		ref.IndexHint = idx.Literal
		p.advance()
	case p.current().Type == TokenNot:
		p.advance()
		if !isKeywordIdentifier(p.current(), "INDEXED") {
			return fmt.Errorf("expected INDEXED after NOT in table reference, got %s", p.current().Literal)
		}
		p.advance()
		ref.NotIndexed = true
		ref.IndexHint = ""
	}
	return nil
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
func (p *Parser) parseWindowExpr(funcName string, args []Expression, filter Expression) (Expression, error) {
	p.advance() // consume OVER

	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	windowExpr := &WindowExpr{
		Function: funcName,
		Args:     args,
		Filter:   filter,
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

	p.consumeMySQLInsertModifiers()

	// Check for SQLite-style INSERT OR conflict actions.
	if p.current().Type == TokenOr {
		p.advance() // consume OR
		if p.current().Type == TokenReplace {
			stmt.ConflictAction = ConflictReplace
			p.advance() // consume REPLACE
		} else if p.current().Type == TokenIgnore {
			stmt.ConflictAction = ConflictIgnore
			p.advance() // consume IGNORE
		} else if p.current().Type == TokenRollback {
			stmt.ConflictAction = ConflictRollback
			p.advance() // consume ROLLBACK
		} else if p.current().Type == TokenIdentifier && (strings.EqualFold(p.current().Literal, "ABORT") || strings.EqualFold(p.current().Literal, "FAIL")) {
			stmt.ConflictAction = ConflictAbort
			p.advance() // consume ABORT/FAIL
		} else {
			return nil, fmt.Errorf("expected REPLACE, IGNORE, ROLLBACK, ABORT, or FAIL after INSERT OR")
		}
	}
	if p.current().Type == TokenIgnore {
		stmt.ConflictAction = ConflictIgnore
		p.advance() // consume MySQL INSERT IGNORE
	}
	p.consumeMySQLInsertModifiers()

	return p.parseInsertTargetAndSource(stmt, true)
}

// parseReplace parses MySQL's standalone REPLACE statement as INSERT with
// replace-on-conflict semantics.
func (p *Parser) parseReplace() (*InsertStmt, error) {
	stmt := &InsertStmt{ConflictAction: ConflictReplace}
	p.advance() // consume REPLACE
	p.consumeMySQLReplaceModifiers()
	return p.parseInsertTargetAndSource(stmt, false)
}

func (p *Parser) consumeMySQLInsertModifiers() {
	for isMySQLInsertModifier(p.current()) {
		p.advance()
	}
}

func (p *Parser) consumeMySQLReplaceModifiers() {
	for isMySQLReplaceModifier(p.current()) {
		p.advance()
	}
}

func isMySQLInsertModifier(tok Token) bool {
	switch toUpperFast(tok.Literal) {
	case "LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED":
		return true
	default:
		return false
	}
}

func isMySQLReplaceModifier(tok Token) bool {
	switch toUpperFast(tok.Literal) {
	case "LOW_PRIORITY", "DELAYED":
		return true
	default:
		return false
	}
}

func (p *Parser) consumeMySQLUpdateModifiers() {
	for isMySQLUpdateModifier(p.current()) {
		p.advance()
	}
}

func isMySQLUpdateModifier(tok Token) bool {
	return toUpperFast(tok.Literal) == "LOW_PRIORITY"
}

func (p *Parser) consumeMySQLDeleteModifiers() {
	for isMySQLDeleteModifier(p.current()) {
		p.advance()
	}
}

func isMySQLDeleteModifier(tok Token) bool {
	switch toUpperFast(tok.Literal) {
	case "LOW_PRIORITY", "QUICK":
		return true
	default:
		return false
	}
}

func (p *Parser) parseInsertTargetAndSource(stmt *InsertStmt, requireInto bool) (*InsertStmt, error) {
	if stmt == nil {
		stmt = &InsertStmt{}
	}
	if requireInto {
		if _, err := p.expect(TokenInto); err != nil {
			return nil, err
		}
	} else {
		p.match(TokenInto)
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

	// SQL standard / PostgreSQL: INSERT INTO table DEFAULT VALUES.
	if p.current().Type == TokenDefault && p.peek().Type == TokenValues {
		p.advance() // consume DEFAULT
		p.advance() // consume VALUES
		stmt.Values = append(stmt.Values, []Expression{})
		if err := p.parseInsertTail(stmt); err != nil {
			return nil, err
		}
		return stmt, nil
	}

	// MySQL: INSERT INTO table SET col = expr [, col = expr] ...
	if p.current().Type == TokenSet {
		p.advance()
		clauses, _, err := p.parseSetClauses()
		if err != nil {
			return nil, err
		}
		values := make([]Expression, len(clauses))
		for i, clause := range clauses {
			stmt.Columns = append(stmt.Columns, clause.Column)
			values[i] = clause.Value
		}
		stmt.Values = append(stmt.Values, values)
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

	if p.current().Type == TokenOn && p.peek().Type == TokenDuplicate {
		oc, err := p.parseOnDuplicateKeyUpdate()
		if err != nil {
			return err
		}
		stmt.OnConflict = oc
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

// parseOnDuplicateKeyUpdate parses MySQL's `ON DUPLICATE KEY UPDATE ...` and
// maps it onto the existing ON CONFLICT DO UPDATE representation.
func (p *Parser) parseOnDuplicateKeyUpdate() (*OnConflictClause, error) {
	p.advance() // consume ON
	p.advance() // consume DUPLICATE
	if _, err := p.expect(TokenKey); err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenUpdate); err != nil {
		return nil, err
	}
	clauses, _, err := p.parseSetClauses()
	if err != nil {
		return nil, err
	}
	return &OnConflictClause{DoUpdate: clauses}, nil
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
	p.consumeMySQLUpdateModifiers()

	tableRef, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	if tableRef.Name == "" || tableRef.Subquery != nil || tableRef.SubqueryStmt != nil {
		return nil, fmt.Errorf("expected UPDATE target table")
	}
	stmt.Table = tableRef.Name
	stmt.Alias = tableRef.Alias

	whereOffset := 0
	if p.isJoin() {
		var err error
		whereOffset, err = p.parseUpdateJoins(stmt, whereOffset)
		if err != nil {
			return nil, err
		}
	}
	if p.current().Type == TokenComma {
		if err := p.parseUpdateCommaJoins(stmt); err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(TokenSet); err != nil {
		return nil, err
	}

	setClauses, setPlaceholders, err := p.parseSetClauses()
	if err != nil {
		return nil, err
	}
	stmt.Set = setClauses

	for i, ph := range setPlaceholders {
		ph.Index = whereOffset + i
	}
	whereOffset += len(setPlaceholders)

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
		if p.current().Type == TokenLParen {
			tupleClauses, tuplePlaceholders, err := p.parseTupleSetClause()
			if err != nil {
				return nil, nil, err
			}
			clauses = append(clauses, tupleClauses...)
			placeholders = append(placeholders, tuplePlaceholders...)
			if !p.match(TokenComma) {
				break
			}
			continue
		}

		col := p.current()
		colName := col.Literal
		if col.Type == TokenIdentifier || (col.Literal != "" && col.Type != TokenEOF && col.Type != TokenEq) {
			p.advance()
		} else {
			return nil, nil, fmt.Errorf("expected column name, got %s", col.Literal)
		}
		if p.match(TokenDot) {
			qualifiedCol := p.current()
			if qualifiedCol.Literal == "" || qualifiedCol.Type == TokenEOF {
				return nil, nil, fmt.Errorf("expected column name after '.'")
			}
			colName = qualifiedCol.Literal
			p.advance()
		}

		if _, err := p.expect(TokenEq); err != nil {
			return nil, nil, err
		}

		val, err := p.parseExpression()
		if err != nil {
			return nil, nil, err
		}

		clauses = append(clauses, &SetClause{Column: colName, Value: val})
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

func (p *Parser) parseTupleSetClause() ([]*SetClause, []*PlaceholderExpr, error) {
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, nil, err
	}
	var columns []string
	for {
		colName, err := p.parseSetTargetColumn()
		if err != nil {
			return nil, nil, err
		}
		columns = append(columns, colName)
		if !p.match(TokenComma) {
			break
		}
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, nil, err
	}
	if _, err := p.expect(TokenEq); err != nil {
		return nil, nil, err
	}
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, nil, err
	}
	values, err := p.parseExpressionList()
	if err != nil {
		return nil, nil, err
	}
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, nil, err
	}
	if len(columns) != len(values) {
		return nil, nil, fmt.Errorf("SET column list has %d columns but value list has %d expressions", len(columns), len(values))
	}
	clauses := make([]*SetClause, len(columns))
	var placeholders []*PlaceholderExpr
	for i, col := range columns {
		clauses[i] = &SetClause{Column: col, Value: values[i]}
		placeholders = append(placeholders, collectPlaceholders(values[i])...)
	}
	return clauses, placeholders, nil
}

func (p *Parser) parseSetTargetColumn() (string, error) {
	col := p.current()
	if col.Type != TokenIdentifier && !(col.Literal != "" && col.Type != TokenEOF && col.Type != TokenEq && col.Type != TokenComma && col.Type != TokenRParen) {
		return "", fmt.Errorf("expected column name, got %s", col.Literal)
	}
	colName := col.Literal
	p.advance()
	if p.match(TokenDot) {
		qualifiedCol := p.current()
		if qualifiedCol.Literal == "" || qualifiedCol.Type == TokenEOF || qualifiedCol.Type == TokenRParen || qualifiedCol.Type == TokenComma {
			return "", fmt.Errorf("expected column name after '.'")
		}
		colName = qualifiedCol.Literal
		p.advance()
	}
	return colName, nil
}

func (p *Parser) parseUpdateJoins(stmt *UpdateStmt, whereOffset int) (int, error) {
	offset := whereOffset
	for p.isJoin() {
		join, err := p.parseJoin()
		if err != nil {
			return offset, err
		}
		stmt.Joins = append(stmt.Joins, join)
		if join.Condition != nil {
			joinPHs := collectPlaceholders(join.Condition)
			reindexPlaceholdersFromExpr(join.Condition, offset)
			offset += len(joinPHs)
		}
	}
	return offset, nil
}

func (p *Parser) parseUpdateCommaJoins(stmt *UpdateStmt) error {
	for p.match(TokenComma) {
		table, err := p.parseTableRef()
		if err != nil {
			return err
		}
		stmt.Joins = append(stmt.Joins, &JoinClause{
			Type:  TokenJoin,
			Table: table,
		})
	}
	return nil
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
	p.consumeMySQLDeleteModifiers()

	if p.current().Type != TokenFrom {
		return p.parseMySQLTargetedDelete(stmt)
	}

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
	var joinWhere Expression
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
			if join.Condition != nil {
				// Collect placeholders from join condition
				joinPlaceholders := collectPlaceholders(join.Condition)
				for i, ph := range joinPlaceholders {
					ph.Index = placeholderOffset + i
				}
				placeholderOffset += len(joinPlaceholders)
				joinWhere = combineDeleteWhere(joinWhere, join.Condition)
			}
		}
	}

	// WHERE - placeholders start at offset 0 (or after USING/JOIN placeholders)
	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		// Fix WHERE clause placeholder indices
		wherePlaceholders := collectPlaceholders(where)
		for i, ph := range wherePlaceholders {
			ph.Index = placeholderOffset + i
		}
		stmt.Where = combineDeleteWhere(joinWhere, where)
	} else {
		stmt.Where = joinWhere
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

func (p *Parser) parseMySQLTargetedDelete(stmt *DeleteStmt) (*DeleteStmt, error) {
	target := p.current()
	if target.Type != TokenIdentifier || target.Literal == "" {
		return nil, fmt.Errorf("expected DELETE target table, got %s", target.Literal)
	}
	targetName := target.Literal
	p.advance()
	if p.match(TokenDot) {
		if _, err := p.expect(TokenStar); err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(TokenFrom); err != nil {
		return nil, err
	}

	fromTable, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(fromTable.Name, targetName) && !strings.EqualFold(fromTable.Alias, targetName) {
		return nil, fmt.Errorf("DELETE target %q does not match FROM table %q", targetName, fromTable.Name)
	}
	stmt.Table = fromTable.Name
	stmt.Alias = fromTable.Alias

	var joinWhere Expression
	placeholderOffset := 0
	for p.match(TokenComma) {
		usingTable, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		stmt.Using = append(stmt.Using, usingTable)
	}
	for p.isJoin() {
		join, err := p.parseJoin()
		if err != nil {
			return nil, err
		}
		stmt.Using = append(stmt.Using, join.Table)
		if join.Condition != nil {
			reindexPlaceholdersFromExpr(join.Condition, placeholderOffset)
			placeholderOffset += len(collectPlaceholders(join.Condition))
			joinWhere = combineDeleteWhere(joinWhere, join.Condition)
		}
	}

	if p.match(TokenWhere) {
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		reindexPlaceholdersFromExpr(where, placeholderOffset)
		stmt.Where = combineDeleteWhere(joinWhere, where)
	} else {
		stmt.Where = joinWhere
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

func combineDeleteWhere(left, right Expression) Expression {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	return &BinaryExpr{Left: left, Operator: TokenAnd, Right: right}
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
		if err := p.expectShowFromOrIn("SHOW COLUMNS"); err != nil {
			return nil, err
		}
		table, err := p.expectShowTableName("SHOW COLUMNS")
		if err != nil {
			return nil, err
		}
		return &ShowColumnsStmt{Table: table}, nil

	case TokenIndex:
		p.advance() // consume INDEX
		if err := p.expectShowFromOrIn("SHOW INDEX"); err != nil {
			return nil, err
		}
		table, err := p.expectShowTableName("SHOW INDEX")
		if err != nil {
			return nil, err
		}
		return &ShowIndexStmt{Table: table}, nil

	case TokenIdentifier:
		varName := p.current().Literal
		p.advance()
		upperVar := toUpperFast(varName)
		if upperVar == "INDEXES" || upperVar == "KEYS" {
			if err := p.expectShowFromOrIn("SHOW " + varName); err != nil {
				return nil, err
			}
			table, err := p.expectShowTableName("SHOW " + varName)
			if err != nil {
				return nil, err
			}
			return &ShowIndexStmt{Table: table}, nil
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

func (p *Parser) expectShowFromOrIn(context string) error {
	if p.current().Type != TokenFrom && p.current().Type != TokenIn {
		return fmt.Errorf("expected FROM or IN after %s", context)
	}
	p.advance()
	return nil
}

func (p *Parser) expectShowTableName(context string) (string, error) {
	tok := p.current()
	if tok.Type == TokenEOF || tok.Type == TokenSemicolon {
		return "", fmt.Errorf("expected table name after %s", context)
	}
	p.advance()
	return tok.Literal, nil
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
