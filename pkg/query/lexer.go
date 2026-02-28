package query

import (
	"fmt"
	"strings"
	"unicode"
)

// Lexer tokenizes SQL input
type Lexer struct {
	input   string
	pos     int
	readPos int
	ch      byte
	line    int
	column  int
}

// NewLexer creates a new lexer for the given input
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar()
	return l
}

// readChar advances to the next character
func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
	l.column++
	if l.ch == '\n' {
		l.line++
		l.column = 0
	}
}

// peekChar returns the next character without advancing
func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// skipWhitespace skips spaces, tabs, newlines
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	var tok Token
	l.skipWhitespace()

	tok.Line = l.line
	tok.Column = l.column

	switch l.ch {
	case '=':
		tok = newToken(TokenEq, l.ch, l.line, l.column)
		l.readChar()
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TokenNeq, Literal: literal, Line: l.line, Column: l.column - 1}
			l.readChar()
		} else {
			tok = newToken(TokenIllegal, l.ch, l.line, l.column)
			l.readChar()
		}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TokenLte, Literal: literal, Line: l.line, Column: l.column - 1}
			l.readChar()
		} else if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TokenNeq, Literal: literal, Line: l.line, Column: l.column - 1}
			l.readChar()
		} else {
			tok = newToken(TokenLt, l.ch, l.line, l.column)
			l.readChar()
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TokenGte, Literal: literal, Line: l.line, Column: l.column - 1}
			l.readChar()
		} else if l.peekChar() == '>' {
			// JSON operator ->>
			ch := l.ch
			l.readChar()
			if l.peekChar() == '>' {
				ch2 := l.ch
				l.readChar()
				literal := string(ch) + string(ch2) + string(l.ch)
				tok = Token{Type: TokenArrow2, Literal: literal, Line: l.line, Column: l.column - 2}
				l.readChar()
			} else {
				literal := string(ch) + string(l.ch)
				tok = Token{Type: TokenArrow, Literal: literal, Line: l.line, Column: l.column - 1}
				l.readChar()
			}
		} else {
			tok = newToken(TokenGt, l.ch, l.line, l.column)
			l.readChar()
		}
	case '-':
		if l.peekChar() == '>' {
			// JSON operator ->
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TokenArrow, Literal: literal, Line: l.line, Column: l.column - 1}
			l.readChar()
		} else {
			tok = newToken(TokenMinus, l.ch, l.line, l.column)
			l.readChar()
		}
	case '@':
		if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			literal := string(ch) + string(l.ch)
			tok = Token{Type: TokenContains, Literal: literal, Line: l.line, Column: l.column - 1}
			l.readChar()
		} else {
			tok = newToken(TokenIllegal, l.ch, l.line, l.column)
			l.readChar()
		}
	case '+':
		tok = newToken(TokenPlus, l.ch, l.line, l.column)
		l.readChar()
	case '*':
		tok = newToken(TokenStar, l.ch, l.line, l.column)
		l.readChar()
	case '/':
		tok = newToken(TokenSlash, l.ch, l.line, l.column)
		l.readChar()
	case '%':
		tok = newToken(TokenPercent, l.ch, l.line, l.column)
		l.readChar()
	case '(':
		tok = newToken(TokenLParen, l.ch, l.line, l.column)
		l.readChar()
	case ')':
		tok = newToken(TokenRParen, l.ch, l.line, l.column)
		l.readChar()
	case ',':
		tok = newToken(TokenComma, l.ch, l.line, l.column)
		l.readChar()
	case ';':
		tok = newToken(TokenSemicolon, l.ch, l.line, l.column)
		l.readChar()
	case '.':
		tok = newToken(TokenDot, l.ch, l.line, l.column)
		l.readChar()
	case '?':
		tok = newToken(TokenQuestion, l.ch, l.line, l.column)
		l.readChar()
	case '\'':
		tok.Type = TokenString
		tok.Literal = l.readString('\'')
		tok.Line = l.line
		tok.Column = l.column
	case '"':
		tok.Type = TokenString
		tok.Literal = l.readString('"')
		tok.Line = l.line
		tok.Column = l.column
	case '`':
		tok.Type = TokenIdentifier
		tok.Literal = l.readBacktickString()
		tok.Line = l.line
		tok.Column = l.column
	case 0:
		tok.Literal = ""
		tok.Type = TokenEOF
		tok.Line = l.line
		tok.Column = l.column
	default:
		if isLetter(l.ch) {
			literal := l.readIdentifier()
			tok.Type = LookupKeyword(strings.ToUpper(literal))
			tok.Literal = literal
			tok.Line = l.line
			tok.Column = l.column - len(literal) + 1
			return tok
		} else if isDigit(l.ch) {
			tok.Type = TokenNumber
			tok.Literal = l.readNumber()
			tok.Line = l.line
			tok.Column = l.column - len(tok.Literal) + 1
			return tok
		} else {
			tok = newToken(TokenIllegal, l.ch, l.line, l.column)
			l.readChar()
		}
	}

	return tok
}

// readIdentifier reads an identifier (letters, digits, underscores)
func (l *Lexer) readIdentifier() string {
	pos := l.pos
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

// readNumber reads a number (integer or float)
func (l *Lexer) readNumber() string {
	pos := l.pos
	for isDigit(l.ch) {
		l.readChar()
	}
	if l.ch == '.' && isDigit(l.peekChar()) {
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	// Scientific notation
	if l.ch == 'e' || l.ch == 'E' {
		l.readChar()
		if l.ch == '+' || l.ch == '-' {
			l.readChar()
		}
		for isDigit(l.ch) {
			l.readChar()
		}
	}
	return l.input[pos:l.pos]
}

// readString reads a string literal
func (l *Lexer) readString(quote byte) string {
	l.readChar() // consume opening quote
	pos := l.pos
	for l.ch != quote && l.ch != 0 {
		if l.ch == '\\' {
			l.readChar()
		}
		l.readChar()
	}
	str := l.input[pos:l.pos]
	l.readChar() // consume closing quote
	return str
}

// readBacktickString reads a backtick-quoted identifier
func (l *Lexer) readBacktickString() string {
	l.readChar() // consume opening backtick
	pos := l.pos
	for l.ch != '`' && l.ch != 0 {
		l.readChar()
	}
	str := l.input[pos:l.pos]
	l.readChar() // consume closing backtick
	return str
}

// isLetter checks if a character is a letter
func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}

// isDigit checks if a character is a digit
func isDigit(ch byte) bool {
	return unicode.IsDigit(rune(ch))
}

// newToken creates a new token
func newToken(tokenType TokenType, ch byte, line, column int) Token {
	return Token{
		Type:    tokenType,
		Literal: string(ch),
		Line:    line,
		Column:  column,
	}
}

// Tokenize tokenizes the entire input and returns all tokens
func Tokenize(input string) ([]Token, error) {
	l := NewLexer(input)
	var tokens []Token

	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
		if tok.Type == TokenIllegal {
			return nil, fmt.Errorf("illegal token at line %d, column %d: %s", tok.Line, tok.Column, tok.Literal)
		}
	}

	return tokens, nil
}
