package query

// TokenType represents the type of a token
type TokenType int

const (
	// Special tokens
	TokenEOF TokenType = iota
	TokenIllegal
	TokenWhitespace

	// Literals
	TokenIdentifier
	TokenString
	TokenNumber

	// Keywords
	TokenSelect
	TokenInsert
	TokenUpdate
	TokenDelete
	TokenCreate
	TokenDrop
	TokenTable
	TokenCollection
	TokenIndex
	TokenFrom
	TokenWhere
	TokenAnd
	TokenOr
	TokenNot
	TokenOrder
	TokenBy
	TokenLimit
	TokenOffset
	TokenJoin
	TokenInner
	TokenLeft
	TokenRight
	TokenOuter
	TokenOn
	TokenGroup
	TokenHaving
	TokenAs
	TokenSet
	TokenValues
	TokenInto
	TokenPrimary
	TokenKey
	TokenUnique
	TokenNull
	TokenNotNull
	TokenDefault
	TokenAutoIncrement
	TokenCheck
	TokenForeign
	TokenReferences
	TokenCascade
	TokenSetNull
	TokenRestrict
	TokenNo
	TokenIf
	TokenExists
	TokenDistinct
	TokenAll
	TokenAsc
	TokenDesc
	TokenLike
	TokenIn
	TokenBetween
	TokenIs
	TokenBegin
	TokenCommit
	TokenRollback
	TokenTransaction
	TokenBefore
	TokenAfter
	TokenFor
	TokenEach
	TokenRow
	TokenTrue
	TokenFalse

	// Data types
	TokenInteger
	TokenText
	TokenReal
	TokenBlob
	TokenBoolean
	TokenJSON
	TokenDate
	TokenTimestamp
	TokenView
	TokenTrigger
	TokenProcedure

	// Operators
	TokenPlus
	TokenMinus
	TokenStar
	TokenSlash
	TokenPercent
	TokenEq
	TokenNeq
	TokenLt
	TokenGt
	TokenLte
	TokenGte
	TokenConcat

	// JSON operators
	TokenArrow    // ->
	TokenArrow2   // ->>
	TokenContains // @>

	// Punctuation
	TokenLParen
	TokenRParen
	TokenComma
	TokenSemicolon
	TokenDot
	TokenQuestion // ? placeholder for prepared statements

	// Functions
	TokenCount
	TokenSum
	TokenAvg
	TokenMin
	TokenMax
	TokenJsonExtract
	TokenJsonSet
	TokenJsonRemove
	TokenJsonArrayLength
	TokenJsonValid
	TokenJsonType
	TokenLength
	TokenUpper
	TokenLower
	TokenTrim
	TokenSubstr
	TokenSubstring
	TokenAbs
	TokenRound
	TokenFloor
	TokenCeil
	TokenCoalesce
	TokenIfNull
	TokenNullIf
	TokenReplace
	TokenInstr
	TokenPrintf
	TokenTime
	TokenDatetime
	TokenStrftime
	TokenTypecast
)

// Token represents a lexical token
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

// keywords maps keyword strings to token types
var keywords = map[string]TokenType{
	"SELECT":         TokenSelect,
	"INSERT":         TokenInsert,
	"UPDATE":         TokenUpdate,
	"DELETE":         TokenDelete,
	"CREATE":         TokenCreate,
	"DROP":           TokenDrop,
	"TABLE":          TokenTable,
	"COLLECTION":     TokenCollection,
	"INDEX":          TokenIndex,
	"FROM":           TokenFrom,
	"WHERE":          TokenWhere,
	"AND":            TokenAnd,
	"OR":             TokenOr,
	"NOT":            TokenNot,
	"ORDER":          TokenOrder,
	"BY":             TokenBy,
	"LIMIT":          TokenLimit,
	"OFFSET":         TokenOffset,
	"JOIN":           TokenJoin,
	"INNER":          TokenInner,
	"LEFT":           TokenLeft,
	"RIGHT":          TokenRight,
	"OUTER":          TokenOuter,
	"ON":             TokenOn,
	"GROUP":          TokenGroup,
	"HAVING":         TokenHaving,
	"AS":             TokenAs,
	"SET":            TokenSet,
	"VALUES":         TokenValues,
	"INTO":           TokenInto,
	"PRIMARY":        TokenPrimary,
	"KEY":            TokenKey,
	"UNIQUE":         TokenUnique,
	"NULL":           TokenNull,
	"NOT NULL":       TokenNotNull,
	"DEFAULT":        TokenDefault,
	"CHECK":         TokenCheck,
	"FOREIGN":       TokenForeign,
	"REFERENCES":    TokenReferences,
	"CASCADE":       TokenCascade,
	"RESTRICT":     TokenRestrict,
	"NO":           TokenNo,
	"AUTO_INCREMENT": TokenAutoIncrement,
	"IF":             TokenIf,
	"EXISTS":         TokenExists,
	"DISTINCT":       TokenDistinct,
	"ALL":            TokenAll,
	"ASC":            TokenAsc,
	"DESC":           TokenDesc,
	"LIKE":           TokenLike,
	"IN":             TokenIn,
	"BETWEEN":        TokenBetween,
	"IS":             TokenIs,
	"BEGIN":          TokenBegin,
	"COMMIT":         TokenCommit,
	"ROLLBACK":       TokenRollback,
	"TRANSACTION":    TokenTransaction,
	"BEFORE":         TokenBefore,
	"AFTER":          TokenAfter,
	"FOR":            TokenFor,
	"EACH":           TokenEach,
	"ROW":            TokenRow,
	"TRUE":           TokenTrue,
	"FALSE":          TokenFalse,

	// Data types
	"INTEGER": TokenInteger,
	"INT":     TokenInteger,
	"TEXT":    TokenText,
	"STRING":  TokenText,
	"VARCHAR": TokenText,
	"REAL":    TokenReal,
	"FLOAT":   TokenReal,
	"DOUBLE":  TokenReal,
	"BLOB":    TokenBlob,
	"BOOLEAN": TokenBoolean,
	"BOOL":    TokenBoolean,
	"JSON":    TokenJSON,
	"DATE":      TokenDate,
	"TIMESTAMP": TokenTimestamp,
	"VIEW":      TokenView,
	"TRIGGER":   TokenTrigger,
	"PROCEDURE": TokenProcedure,

	// Functions
	"COUNT":           TokenCount,
	"SUM":             TokenSum,
	"AVG":             TokenAvg,
	"MIN":             TokenMin,
	"MAX":             TokenMax,
	"JSON_EXTRACT":    TokenJsonExtract,
	"JSON_SET":        TokenJsonSet,
	"JSON_REMOVE":     TokenJsonRemove,
	"JSON_ARRAY_LENGTH": TokenJsonArrayLength,
	"JSON_VALID":      TokenJsonValid,
	"JSON_TYPE":       TokenJsonType,
	"LENGTH":          TokenLength,
	"UPPER":           TokenUpper,
	"LOWER":           TokenLower,
	"TRIM":            TokenTrim,
	"SUBSTR":          TokenSubstr,
	"SUBSTRING":       TokenSubstring,
	"CONCAT":          TokenConcat,
	"ABS":             TokenAbs,
	"ROUND":           TokenRound,
	"FLOOR":           TokenFloor,
	"CEIL":            TokenCeil,
	"COALESCE":        TokenCoalesce,
	"IFNULL":          TokenIfNull,
	"NULLIF":          TokenNullIf,
	"REPLACE":         TokenReplace,
	"INSTR":           TokenInstr,
	"PRINTF":          TokenPrintf,
	"TIME":            TokenTime,
	"DATETIME":        TokenDatetime,
	"STRFTIME":        TokenStrftime,
	"CAST":            TokenTypecast,
}

// LookupKeyword checks if an identifier is a keyword
func LookupKeyword(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return TokenIdentifier
}

// TokenTypeString returns a string representation of a token type
func TokenTypeString(t TokenType) string {
	switch t {
	case TokenEOF:
		return "EOF"
	case TokenIllegal:
		return "ILLEGAL"
	case TokenIdentifier:
		return "IDENTIFIER"
	case TokenString:
		return "STRING"
	case TokenNumber:
		return "NUMBER"
	case TokenSelect:
		return "SELECT"
	case TokenInsert:
		return "INSERT"
	case TokenUpdate:
		return "UPDATE"
	case TokenDelete:
		return "DELETE"
	case TokenCreate:
		return "CREATE"
	case TokenTable:
		return "TABLE"
	case TokenFrom:
		return "FROM"
	case TokenWhere:
		return "WHERE"
	case TokenAnd:
		return "AND"
	case TokenOr:
		return "OR"
	case TokenEq:
		return "="
	case TokenNeq:
		return "!="
	case TokenLt:
		return "<"
	case TokenGt:
		return ">"
	case TokenLte:
		return "<="
	case TokenGte:
		return ">="
	case TokenLParen:
		return "("
	case TokenRParen:
		return ")"
	case TokenComma:
		return ","
	case TokenSemicolon:
		return ";"
	case TokenDot:
		return "."
	case TokenStar:
		return "*"
	default:
		return "UNKNOWN"
	}
}
