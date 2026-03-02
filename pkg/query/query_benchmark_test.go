package query

import (
	"testing"
)

// BenchmarkParseSelect benchmarks parsing SELECT statements
func BenchmarkParseSelect(b *testing.B) {
	sql := "SELECT id, name, value FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(sql)
	}
}

// BenchmarkParseInsert benchmarks parsing INSERT statements
func BenchmarkParseInsert(b *testing.B) {
	sql := "INSERT INTO users (id, name, value) VALUES (1, 'John', 100.5)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(sql)
	}
}

// BenchmarkParseUpdate benchmarks parsing UPDATE statements
func BenchmarkParseUpdate(b *testing.B) {
	sql := "UPDATE users SET name = 'Jane', value = 200.5 WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(sql)
	}
}

// BenchmarkParseDelete benchmarks parsing DELETE statements
func BenchmarkParseDelete(b *testing.B) {
	sql := "DELETE FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(sql)
	}
}

// BenchmarkParseComplexSelect benchmarks parsing complex SELECT statements
func BenchmarkParseComplexSelect(b *testing.B) {
	sql := `SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE u.active = true
		GROUP BY u.id, u.name
		HAVING COUNT(o.id) > 5
		ORDER BY total_amount DESC
		LIMIT 10 OFFSET 20`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(sql)
	}
}

// BenchmarkLexerTokenize benchmarks lexer tokenization
func BenchmarkLexerTokenize(b *testing.B) {
	sql := "SELECT id, name, value FROM users WHERE id = 1 AND name = 'John'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l := NewLexer(sql)
		for l.NextToken().Type != TokenEOF {
		}
	}
}

// BenchmarkParseCreateTable benchmarks parsing CREATE TABLE statements
func BenchmarkParseCreateTable(b *testing.B) {
	sql := `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		age INTEGER DEFAULT 0,
		active BOOLEAN DEFAULT true,
		created_at TIMESTAMP
	)`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(sql)
	}
}

// BenchmarkParseWithSubquery benchmarks parsing queries with subqueries
func BenchmarkParseWithSubquery(b *testing.B) {
	sql := `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(sql)
	}
}

// BenchmarkParseWithMultipleJoins benchmarks parsing queries with multiple joins
func BenchmarkParseWithMultipleJoins(b *testing.B) {
	sql := `SELECT u.name, p.title, c.name
		FROM users u
		JOIN posts p ON u.id = p.user_id
		JOIN categories c ON p.category_id = c.id`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(sql)
	}
}
