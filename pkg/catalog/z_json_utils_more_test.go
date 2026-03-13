package catalog

import (
	"testing"
)

// TestParseJSONPathExtended tests ParseJSONPath with more edge cases
func TestParseJSONPathExtended(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"empty path", "", true},
		{"just dollar", "$", false},
		{"simple key", "name", false},
		{"dotted key", "$.name", false},
		{"nested key", "$.user.name", false},
		{"array index", "$[0]", false},
		{"mixed path", "$.users[0].name", false},
		{"deeply nested", "$.a.b.c.d.e", false},
		{"multiple array indices", "$[0][1][2]", false},
		{"wildcard", "$.store.*", false},
		{"bracket notation", "$['name']", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseJSONPath(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseJSONPath(%q) expected error, got nil", tt.path)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseJSONPath(%q) unexpected error: %v", tt.path, err)
				return
			}
			_ = got
		})
	}
}

// TestJSONExtractExtended tests JSONExtract with more edge cases
func TestJSONExtractExtended(t *testing.T) {
	tests := []struct {
		name string
		json string
		path string
	}{
		{"extract from object", `{"name":"John","age":30}`, "$.name"},
		{"extract number", `{"age":30}`, "$.age"},
		{"extract boolean", `{"active":true}`, "$.active"},
		{"extract null", `{"data":null}`, "$.data"},
		{"extract nested", `{"user":{"name":"John"}}`, "$.user.name"},
		{"extract from array", `[1,2,3]`, "$[0]"},
		{"extract array element", `{"items":[1,2,3]}`, "$.items[1]"},
		{"invalid json", `invalid json`, "$.name"},
		{"nonexistent path", `{"name":"John"}`, "$.nonexistent"},
		{"empty json", `{}`, "$.name"},
		{"empty array", `[]`, "$[0]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONExtract(tt.json, tt.path)
			_ = got
			_ = err
		})
	}
}

// TestJSONSetExtended tests JSONSet with more edge cases
func TestJSONSetExtended(t *testing.T) {
	tests := []struct {
		name  string
		json  string
		path  string
		value string
	}{
		{"set new key", `{}`, "$.name", `"John"`},
		{"update existing", `{"name":"Old"}`, "$.name", `"New"`},
		{"set nested object", `{"user":{}}`, "$.user.name", `"John"`},
		{"set in array", `[1,2,3]`, "$[0]", `10`},
		{"set array element", `{"items":[1,2,3]}`, "$.items[1]", `20`},
		{"invalid json", `invalid`, "$.name", `"value"`},
		{"set number", `{}`, "$.count", `42`},
		{"set boolean", `{}`, "$.active", `true`},
		{"set null", `{}`, "$.data", `null`},
		{"deeply nested", `{}`, "$.a.b.c", `"deep"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONSet(tt.json, tt.path, tt.value)
			_ = got
			_ = err
		})
	}
}

// TestJSONRemoveExtended tests JSONRemove with more edge cases
func TestJSONRemoveExtended(t *testing.T) {
	tests := []struct {
		name string
		json string
		path string
	}{
		{"remove key", `{"name":"John","age":30}`, "$.age"},
		{"remove nested", `{"user":{"name":"John","age":30}}`, "$.user.age"},
		{"remove from array", `[1,2,3]`, "$[1]"},
		{"remove array element", `{"items":[1,2,3]}`, "$.items[0]"},
		{"invalid json", `invalid`, "$.name"},
		{"nonexistent path", `{"name":"John"}`, "$.nonexistent"},
		{"remove only key", `{"name":"John"}`, "$.name"},
		{"remove from empty object", `{}`, "$.name"},
		{"remove from empty array", `[]`, "$[0]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONRemove(tt.json, tt.path)
			_ = got
			_ = err
		})
	}
}

// TestJSONArrayLengthExtended tests JSONArrayLength with more edge cases
func TestJSONArrayLengthExtended(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"simple array", `[1,2,3]`},
		{"empty array", `[]`},
		{"nested arrays", `[[1,2],[3,4]]`},
		{"array of objects", `[{"a":1},{"b":2}]`},
		{"not an array", `{"name":"John"}`},
		{"invalid json", `invalid`},
		{"null", `null`},
		{"string", `"hello"`},
		{"number", `42`},
		{"boolean", `true`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONArrayLength(tt.json)
			_ = got
			_ = err
		})
	}
}

// TestJSONKeysExtended tests JSONKeys with more edge cases
func TestJSONKeysExtended(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"simple object", `{"name":"John","age":30}`},
		{"empty object", `{}`},
		{"nested object", `{"user":{"name":"John"}}`},
		{"not an object", `[1,2,3]`},
		{"invalid json", `invalid`},
		{"null", `null`},
		{"string", `"hello"`},
		{"number", `42`},
		{"many keys", `{"a":1,"b":2,"c":3,"d":4,"e":5}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONKeys(tt.json)
			_ = got
			_ = err
		})
	}
}

// TestJSONPrettyExtended tests JSONPretty with more edge cases
func TestJSONPrettyExtended(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"simple object", `{"name":"John"}`},
		{"already pretty", `{
  "name": "John"
}`},
		{"nested object", `{"user":{"name":"John","age":30}}`},
		{"array", `[1,2,3]`},
		{"invalid json", `invalid`},
		{"empty object", `{}`},
		{"empty array", `[]`},
		{"complex nested", `{"a":{"b":{"c":[1,2,3]}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONPretty(tt.json)
			_ = got
			_ = err
		})
	}
}

// TestJSONMinifyExtended tests JSONMinify with more edge cases
func TestJSONMinifyExtended(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"with spaces", `{ "name" : "John" }`},
		{"with newlines", `{
  "name": "John"
}`},
		{"with tabs", `{	"name":	"John"	}`},
		{"already minified", `{"name":"John"}`},
		{"invalid json", `invalid`},
		{"empty object", `{ }`},
		{"complex nested", `{
  "a": {
    "b": 1
  }
}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONMinify(tt.json)
			_ = got
			_ = err
		})
	}
}

// TestJSONMergeExtended tests JSONMerge with more edge cases
func TestJSONMergeExtended(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
	}{
		{"merge objects", `{"a":1}`, `{"b":2}`},
		{"overwrite value", `{"a":1}`, `{"a":2}`},
		{"merge nested", `{"x":{"a":1}}`, `{"x":{"b":2}}`},
		{"merge arrays", `[1,2]`, `[3,4]`},
		{"empty objects", `{}`, `{}`},
		{"empty arrays", `[]`, `[]`},
		{"invalid first", `invalid`, `{"a":1}`},
		{"invalid second", `{"a":1}`, `invalid`},
		{"both invalid", `invalid1`, `invalid2`},
		{"deep merge", `{"a":{"b":{"c":1}}}`, `{"a":{"b":{"d":2}}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONMerge(tt.a, tt.b)
			_ = got
			_ = err
		})
	}
}

// TestJSONTypeExtended tests JSONType with more edge cases
func TestJSONTypeExtended(t *testing.T) {
	tests := []struct {
		name string
		json string
		path string
	}{
		{"object type", `{"data":{"name":"John"}}`, "$.data"},
		{"array type", `{"items":[1,2,3]}`, "$.items"},
		{"string type", `{"name":"John"}`, "$.name"},
		{"number type", `{"age":30}`, "$.age"},
		{"boolean type", `{"active":true}`, "$.active"},
		{"null type", `{"data":null}`, "$.data"},
		{"invalid json", `invalid`, "$.name"},
		{"nonexistent path", `{"name":"John"}`, "$.nonexistent"},
		{"root object", `{"a":1}`, "$"},
		{"root array", `[1,2,3]`, "$"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONType(tt.json, tt.path)
			_ = got
			_ = err
		})
	}
}

// TestJSONQuoteExtended tests JSONQuote with more edge cases
func TestJSONQuoteExtended(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", `"hello"`},
		{"", `""`},
		{"with spaces", `"with spaces"`},
		{"with \"quotes\"", `"with \"quotes\""`},
		{"with\nnewline", `"with\nnewline"`},
		{"with\ttab", `"with\ttab"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := JSONQuote(tt.input)
			if got != tt.want {
				t.Errorf("JSONQuote(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestJSONUnquoteExtended tests JSONUnquote with more edge cases
func TestJSONUnquoteExtended(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"simple", `"hello"`, "hello", false},
		{"empty", `""`, "", false},
		{"with spaces", `"hello world"`, "hello world", false},
		{"with escaped quotes", `"say \"hello\""`, `say "hello"`, false},
		{"with newline", `"line1\nline2"`, "line1\nline2", false},
		{"with tab", `"col1\tcol2"`, "col1\tcol2", false},
		{"not quoted", `hello`, "", true},
		{"invalid escape", `"\q"`, "", true},
		{"unterminated", `"hello`, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONUnquote(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("JSONUnquote(%q) expected error", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("JSONUnquote(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got != tt.want {
				t.Errorf("JSONUnquote(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestJSONEachExtended tests JSONEach with more edge cases
func TestJSONEachExtended(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"simple object", `{"name":"John","age":30}`},
		{"empty object", `{}`},
		{"nested object", `{"user":{"name":"John"}}`},
		{"array", `[1,2,3]`},
		{"invalid json", `invalid`},
		{"null", `null`},
		{"string", `"hello"`},
		{"number", `42`},
		{"boolean", `true`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JSONEach(tt.json)
			_ = got
			_ = err
		})
	}
}

// TestRegexMatchExtended tests RegexMatch with more edge cases
func TestRegexMatchExtended(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		pattern string
	}{
		{"simple match", "hello", "hello"},
		{"partial match", "hello world", "world"},
		{"wildcard", "hello", "h.*o"},
		{"digits", "test123", `\d+`},
		{"word boundary", "hello world", `\bworld\b`},
		{"start anchor", "hello", "^hello"},
		{"end anchor", "hello", "hello$"},
		{"no match", "hello", "xyz"},
		{"case insensitive", "Hello", "(?i)hello"},
		{"invalid pattern", "hello", "[invalid"},
		{"empty string", "", "^$"},
		{"empty pattern", "hello", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RegexMatch(tt.str, tt.pattern)
			_ = got
			_ = err
		})
	}
}

// TestRegexReplaceExtended tests RegexReplace with more edge cases
func TestRegexReplaceExtended(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		pattern string
		repl    string
	}{
		{"simple replace", "hello world", "world", "there"},
		{"replace all", "foo bar foo", "foo", "baz"},
		{"replace digits", "test123abc", `\d+`, "NUM"},
		{"no match", "hello", "xyz", "replaced"},
		{"empty replacement", "hello world", "world", ""},
		{"special chars in repl", "hello", "hello", "$&$&"},
		{"invalid pattern", "hello", "[invalid", "replaced"},
		{"empty string", "", ".*", "replaced"},
		{"word boundaries", "foo bar baz", `\bbar\b`, "BAR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RegexReplace(tt.str, tt.pattern, tt.repl)
			_ = got
			_ = err
		})
	}
}

// TestRegexExtractExtended tests RegexExtract with more edge cases
func TestRegexExtractExtended(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		pattern string
	}{
		{"simple extract", "hello world", "world"},
		{"extract digits", "test123abc", `\d+`},
		{"extract word", "foo bar baz", `bar`},
		{"extract group", "hello world", `(\w+) world`},
		{"no match", "hello", "xyz"},
		{"empty string", "", ".*"},
		{"invalid pattern", "hello", "[invalid"},
		{"start of string", "hello world", `^hello`},
		{"end of string", "hello world", `world$`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RegexExtract(tt.str, tt.pattern)
			_ = got
			_ = err
		})
	}
}

// TestIsValidJSONExtended tests IsValidJSON with more edge cases
func TestIsValidJSONExtended(t *testing.T) {
	tests := []struct {
		name  string
		json  string
		valid bool
	}{
		{"valid object", `{"name":"John"}`, true},
		{"valid array", `[1,2,3]`, true},
		{"valid string", `"hello"`, true},
		{"valid number", `42`, true},
		{"valid true", `true`, true},
		{"valid false", `false`, true},
		{"valid null", `null`, true},
		{"empty object", `{}`, true},
		{"empty array", `[]`, true},
		{"empty string", ``, false},
		{"invalid syntax", `{name:"John"}`, false},
		{"unterminated", `{"name":"John"`, false},
		{"trailing comma", `{"name":"John",}`, false},
		{"just whitespace", `   `, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsValidJSON(tt.json)
			if got != tt.valid {
				t.Errorf("IsValidJSON(%q) = %v, want %v", tt.json, got, tt.valid)
			}
		})
	}
}
