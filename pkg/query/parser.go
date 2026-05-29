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
	strict           bool // When true, expect() errors instead of silently advancing on known-token mismatches
}

// NewParser creates a new parser for the given tokens
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

// NewParserStrict creates a parser in strict mode that errors on malformed token sequences
// rather than silently tolerating mismatches.
func NewParserStrict(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
		strict: true,
	}
}

const maxParserDepth = 200

// toUpperFast returns an uppercased copy of s only if s contains lowercase
// letters. This avoids an allocation when s is already uppercase.
func toUpperFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			return strings.ToUpper(s)
		}
	}
	return s
}

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

// strictExpect is like expect but only errors in strict mode; in permissive mode it
// silently ignores the mismatch, allowing the parse to continue from an unexpected token.
func (p *Parser) strictExpect(t TokenType) (Token, error) {
	if p.current().Type != t {
		if p.strict {
			return Token{}, fmt.Errorf("expected %s, got %s", TokenTypeString(t), p.current().Literal)
		}
		return Token{}, nil
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

// isJoin checks if the current token starts a JOIN clause
func (p *Parser) isJoin() bool {
	switch p.current().Type {
	case TokenJoin, TokenInner, TokenLeft, TokenRight, TokenOuter, TokenFull, TokenCross, TokenNatural:
		return true
	}
	return false
}

// parseTemporalExpr parses AS OF temporal expression
// Supports: AS OF '2024-01-15' or AS OF SYSTEM TIME '-1 hour'
func (p *Parser) parseTemporalExpr() (*TemporalExpr, error) {
	temporal := &TemporalExpr{}

	// Check for SYSTEM TIME syntax
	if p.match(TokenSystem) {
		if _, err := p.expect(TokenTime); err != nil {
			return nil, err
		}
		temporal.IsSystem = true
	}

	// Parse the timestamp expression
	tsExpr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	temporal.Timestamp = tsExpr

	return temporal, nil
}

func (p *Parser) parseIfNotExists() bool {
	if p.match(TokenIf) {
		if p.match(TokenNot) {
			_ = p.match(TokenExists)
			return true
		}
	}
	return false
}

// applyPlaceholderOffset recursively applies offset to placeholders.
//
//nolint:unused // retained for parser compatibility tests and future placeholder rewrites.
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

// parseVectorLiteral parses a vector literal [0.1, 0.2, 0.3, ...]
func (p *Parser) parseVectorLiteral() (Expression, error) {
	p.advance() // consume [

	var values []float64

	// Empty vector []
	if p.current().Type == TokenRBracket {
		p.advance() // consume ]
		return &VectorLiteral{Values: values}, nil
	}

	// Parse comma-separated numbers
	for {
		// Expect a number
		if p.current().Type != TokenNumber {
			return nil, fmt.Errorf("expected number in vector literal, got %s", p.current().Literal)
		}

		val, err := strconv.ParseFloat(p.current().Literal, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number in vector literal: %s", p.current().Literal)
		}
		values = append(values, val)
		p.advance()

		// Check for closing bracket or comma
		if p.current().Type == TokenRBracket {
			p.advance() // consume ]
			break
		}

		if p.current().Type == TokenComma {
			p.advance() // consume ,
			continue
		}

		return nil, fmt.Errorf("expected ',' or ']' in vector literal, got %s", p.current().Literal)
	}

	return &VectorLiteral{Values: values}, nil
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

func reindexPlaceholders(expr Expression, offset int) {
	phs := collectPlaceholders(expr)
	for i, ph := range phs {
		ph.Index = offset + i
	}
}

func reindexPlaceholdersFromExpr(expr Expression, offset int) {
	phs := collectPlaceholders(expr)
	for i, ph := range phs {
		ph.Index = offset + i
	}
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

// parseBegin parses BEGIN TRANSACTION
func (p *Parser) parseBegin() (*BeginStmt, error) {
	stmt := &BeginStmt{}
	p.advance() // consume BEGIN

	_ = p.match(TokenTransaction) // optional

	if p.current().Type == TokenIdentifier && toUpperFast(p.current().Literal) == "READ" {
		p.advance()
		if p.current().Type == TokenIdentifier && toUpperFast(p.current().Literal) == "ONLY" {
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

// Parse parses a SQL string and returns the AST
func Parse(sql string) (Statement, error) {
	tokens, err := Tokenize(sql)
	if err != nil {
		return nil, err
	}

	parser := NewParser(tokens)
	return parser.Parse()
}

// ParseStrict parses a SQL string and rejects any non-semicolon tokens left
// after the first statement. This preserves Parse's historical permissive
// behavior while giving production callers a stricter compatibility gate.
func ParseStrict(sql string) (Statement, error) {
	tokens, err := Tokenize(sql)
	if err != nil {
		return nil, err
	}

	parser := NewParserStrict(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	if err := parser.expectStatementEnd(); err != nil {
		return nil, err
	}
	return stmt, nil
}

func (p *Parser) expectStatementEnd() error {
	for p.current().Type == TokenSemicolon {
		p.advance()
	}
	if p.current().Type != TokenEOF {
		return fmt.Errorf("unexpected token after statement: %s", p.current().Literal)
	}
	return nil
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
