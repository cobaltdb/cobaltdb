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
	TokenFull
	TokenCross
	TokenNatural
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
	TokenUsing
	TokenForeign
	TokenReferences
	TokenCascade
	TokenSetNull
	TokenRestrict
	TokenNo
	TokenAction
	TokenReturning
	TokenIf
	TokenExists
	TokenDistinct
	TokenAll
	TokenUnion
	TokenIntersect
	TokenExcept
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
	TokenSavepoint
	TokenRelease
	TokenTo
	TokenBefore
	TokenAfter
	TokenInstead
	TokenOf
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
	TokenLanguage
	TokenMode
	TokenJSON
	TokenDate
	TokenTimestamp
	TokenView
	TokenTrigger
	TokenProcedure
	TokenRange
	TokenList
	TokenHash
	TokenPolicy
	TokenCall
	TokenOut
	TokenInout
	TokenPartitions
	TokenVector // Vector type for embeddings/AI

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
	TokenLBracket // [ for vector literals
	TokenRBracket // ] for vector literals
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
	TokenJsonKeys
	TokenJsonPretty
	TokenJsonMinify
	TokenJsonMerge
	TokenJsonQuote
	TokenJsonUnquote
	TokenRegexMatch
	TokenRegexReplace
	TokenRegexExtract
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
	TokenIgnore
	TokenInstr
	TokenPrintf
	TokenTime
	TokenDatetime
	TokenStrftime
	TokenTypecast
	TokenWindow
	TokenOver
	TokenPartition
	TokenRowNumber
	TokenRank
	TokenDenseRank
	TokenLag
	TokenLead
	TokenFirstValue
	TokenLastValue
	TokenNthValue

	// Vector similarity functions
	TokenCosineSimilarity // COSINE_SIMILARITY
	TokenL2Distance       // L2_DISTANCE (Euclidean)
	TokenInnerProduct     // INNER_PRODUCT (dot product)

	// CTE (Common Table Expressions)
	TokenWith
	TokenRecursive

	// Maintenance commands
	TokenVacuum
	TokenAnalyze

	// Full-text search
	TokenMatch
	TokenAgainst
	TokenFulltext

	// ESCAPE
	TokenEscape

	// FDW (Foreign Data Wrappers)
	TokenWrapper
	TokenOptions

	// Temporal queries (AS OF SYSTEM TIME)
	TokenAsOf
	TokenSystem

	// ALTER TABLE
	TokenAlter
	TokenAdd
	TokenColumn
	TokenRename
	TokenLess
	TokenThan

	// Materialized views
	TokenMaterialized
	TokenRefresh

	// CASE expression
	TokenCase
	TokenWhen
	TokenThen
	TokenElse
	TokenEnd

	// SHOW / USE / DESCRIBE
	TokenShow
	TokenUse
	TokenDescribe
	TokenExplain
	TokenDatabases
	TokenTables
	TokenColumns
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
	"FULL":           TokenFull,
	"CROSS":          TokenCross,
	"NATURAL":        TokenNatural,
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
	"CHECK":          TokenCheck,
	"FOREIGN":        TokenForeign,
	"REFERENCES":     TokenReferences,
	"CASCADE":        TokenCascade,
	"RESTRICT":       TokenRestrict,
	"NO":             TokenNo,
	"ACTION":         TokenAction,
	"AUTO_INCREMENT": TokenAutoIncrement,
	"AUTOINCREMENT":  TokenAutoIncrement,
	"IF":             TokenIf,
	"EXISTS":         TokenExists,
	"DISTINCT":       TokenDistinct,
	"ALL":            TokenAll,
	"UNION":          TokenUnion,
	"INTERSECT":      TokenIntersect,
	"EXCEPT":         TokenExcept,
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
	"SAVEPOINT":      TokenSavepoint,
	"RELEASE":        TokenRelease,
	"TO":             TokenTo,
	"BEFORE":         TokenBefore,
	"AFTER":          TokenAfter,
	"INSTEAD":        TokenInstead,
	"OF":             TokenOf,
	"FOR":            TokenFor,
	"EACH":           TokenEach,
	"ROW":            TokenRow,
	"TRUE":           TokenTrue,
	"FALSE":          TokenFalse,

	// Data types
	"INTEGER":   TokenInteger,
	"INT":       TokenInteger,
	"BIGINT":    TokenInteger,
	"SMALLINT":  TokenInteger,
	"TINYINT":   TokenInteger,
	"TEXT":      TokenText,
	"STRING":    TokenText,
	"VARCHAR":   TokenText,
	"CHAR":      TokenText,
	"NVARCHAR":  TokenText,
	"NCHAR":     TokenText,
	"CLOB":      TokenText,
	"REAL":      TokenReal,
	"FLOAT":     TokenReal,
	"DOUBLE":    TokenReal,
	"NUMERIC":   TokenReal,
	"DECIMAL":   TokenReal,
	"BLOB":      TokenBlob,
	"BOOLEAN":   TokenBoolean,
	"BOOL":      TokenBoolean,
	"LANGUAGE":  TokenLanguage,
	"MODE":      TokenMode,
	"JSON":      TokenJSON,
	"DATE":      TokenDate,
	"TIMESTAMP": TokenTimestamp,
	"VIEW":      TokenView,
	"TRIGGER":   TokenTrigger,
	"PROCEDURE": TokenProcedure,
	"POLICY":    TokenPolicy,
	"CALL":      TokenCall,
	"USING":     TokenUsing,
	"VECTOR":    TokenVector,

	// Functions
	"COUNT":             TokenCount,
	"SUM":               TokenSum,
	"AVG":               TokenAvg,
	"MIN":               TokenMin,
	"MAX":               TokenMax,
	"JSON_EXTRACT":      TokenJsonExtract,
	"JSON_SET":          TokenJsonSet,
	"JSON_REMOVE":       TokenJsonRemove,
	"JSON_ARRAY_LENGTH": TokenJsonArrayLength,
	"JSON_VALID":        TokenJsonValid,
	"JSON_TYPE":         TokenJsonType,
	"JSON_KEYS":         TokenJsonKeys,
	"JSON_PRETTY":       TokenJsonPretty,
	"JSON_MINIFY":       TokenJsonMinify,
	"JSON_MERGE":        TokenJsonMerge,
	"JSON_QUOTE":        TokenJsonQuote,
	"JSON_UNQUOTE":      TokenJsonUnquote,
	"REGEXP_MATCH":      TokenRegexMatch,
	"REGEXP_REPLACE":    TokenRegexReplace,
	"REGEXP_EXTRACT":    TokenRegexExtract,
	"LENGTH":            TokenLength,
	"UPPER":             TokenUpper,
	"LOWER":             TokenLower,
	"TRIM":              TokenTrim,
	"SUBSTR":            TokenSubstr,
	"SUBSTRING":         TokenSubstring,
	"CONCAT":            TokenConcat,
	"ABS":               TokenAbs,
	"ROUND":             TokenRound,
	"FLOOR":             TokenFloor,
	"CEIL":              TokenCeil,
	"COALESCE":          TokenCoalesce,
	"IFNULL":            TokenIfNull,
	"NULLIF":            TokenNullIf,
	"REPLACE":           TokenReplace,
	"IGNORE":            TokenIgnore,
	"INSTR":             TokenInstr,
	"PRINTF":            TokenPrintf,
	"TIME":              TokenTime,
	"DATETIME":          TokenDatetime,
	"STRFTIME":          TokenStrftime,
	"CAST":              TokenTypecast,
	"WINDOW":            TokenWindow,
	"OVER":              TokenOver,
	"PARTITION":         TokenPartition,
	"ROW_NUMBER":        TokenRowNumber,
	"RANK":              TokenRank,
	"DENSE_RANK":        TokenDenseRank,
	"LAG":               TokenLag,
	"LEAD":              TokenLead,
	"FIRST_VALUE":       TokenFirstValue,
	"LAST_VALUE":        TokenLastValue,
	"NTH_VALUE":         TokenNthValue,

	// CTE
	"WITH":      TokenWith,
	"RECURSIVE": TokenRecursive,

	// Temporal queries
	"SYSTEM": TokenSystem,

	// FDW
	"WRAPPER": TokenWrapper,
	"OPTIONS": TokenOptions,

	// ALTER TABLE
	"ESCAPE":    TokenEscape,
	"ALTER":     TokenAlter,
	"ADD":       TokenAdd,
	"COLUMN":    TokenColumn,
	"RENAME":    TokenRename,
	"RETURNING": TokenReturning,

	// Maintenance commands
	"VACUUM":  TokenVacuum,
	"ANALYZE": TokenAnalyze,

	// Full-text search
	"MATCH":    TokenMatch,
	"AGAINST":  TokenAgainst,
	"FULLTEXT": TokenFulltext,

	// Materialized views
	"MATERIALIZED": TokenMaterialized,
	"REFRESH":      TokenRefresh,

	// CASE expression
	"CASE": TokenCase,
	"WHEN": TokenWhen,
	"THEN": TokenThen,
	"ELSE": TokenElse,
	"END":  TokenEnd,

	// SHOW / USE / DESCRIBE
	"SHOW":      TokenShow,
	"USE":       TokenUse,
	"DESCRIBE":  TokenDescribe,
	"EXPLAIN":   TokenExplain,
	"DATABASES": TokenDatabases,
	"TABLES":    TokenTables,
	"COLUMNS":   TokenColumns,

	// Partitioning
	"RANGE":      TokenRange,
	"LIST":       TokenList,
	"HASH":       TokenHash,
	"LESS":       TokenLess,
	"THAN":       TokenThan,
	"PARTITIONS": TokenPartitions,

	// Vector functions
	"COSINE_SIMILARITY": TokenCosineSimilarity,
	"L2_DISTANCE":       TokenL2Distance,
	"INNER_PRODUCT":     TokenInnerProduct,
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
