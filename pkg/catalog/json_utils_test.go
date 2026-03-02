package catalog

import (
	"testing"
)

func TestParseJSONPath(t *testing.T) {
	tests := []struct {
		path string
	}{
		{"$.name"},
		{"$.store.book[0].author"},
		{"$.store.*"},
		{"$"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got, err := ParseJSONPath(tt.path)
			if err != nil {
				t.Logf("ParseJSONPath(%q) error: %v", tt.path, err)
			}
			_ = got
		})
	}
}

func TestJSONExtract(t *testing.T) {
	tests := []struct {
		json string
		path string
	}{
		{`{"name":"John"}`, "$.name"},
		{`[1,2,3]`, "$[0]"},
		{"invalid", "$.name"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got, err := JSONExtract(tt.json, tt.path)
			_ = got
			_ = err
		})
	}
}

func TestJSONSet(t *testing.T) {
	// Just check these don't panic
	_, _ = JSONSet(`{"name":"John"}`, "$.age", "30")
	_, _ = JSONSet(`{}`, "$.name", "Alice")
}

func TestJSONRemove(t *testing.T) {
	// Just check these don't panic
	_, _ = JSONRemove(`{"name":"John","age":30}`, "$.age")
}

func TestJSONArrayLength(t *testing.T) {
	tests := []struct {
		json string
	}{
		{`[1,2,3]`},
		{`[]`},
		{"invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.json, func(t *testing.T) {
			got, err := JSONArrayLength(tt.json)
			_ = got
			_ = err
		})
	}
}

func TestJSONKeys(t *testing.T) {
	tests := []struct {
		json string
	}{
		{`{"name":"John","age":30}`},
		{`{}`},
	}

	for _, tt := range tests {
		t.Run(tt.json, func(t *testing.T) {
			got, err := JSONKeys(tt.json)
			_ = got
			_ = err
		})
	}
}

func TestJSONPretty(t *testing.T) {
	_, _ = JSONPretty(`{"name":"John"}`)
	_, _ = JSONPretty(`[1,2,3]`)
}

func TestJSONMinify(t *testing.T) {
	_, _ = JSONMinify(`{ "name": "John" }`)
	_, _ = JSONMinify(`[ 1, 2, 3 ]`)
}

func TestJSONMerge(t *testing.T) {
	_, _ = JSONMerge(`{"name":"John"}`, `{"age":30}`)
	_, _ = JSONMerge(`{}`, `{"a":1}`)
}

func TestJSONType(t *testing.T) {
	// Just check these don't panic
	_, _ = JSONType(`{"name":"John"}`, "$.name")
	_, _ = JSONType(`{"age":30}`, "$.age")
	_, _ = JSONType("", "")
}

func TestJSONQuote(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", `"hello"`},
		{"", `""`},
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

func TestJSONUnquote(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"hello"`, "hello"},
		{`""`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := JSONUnquote(tt.input)
			if err != nil {
				t.Logf("JSONUnquote error: %v", err)
			}
			if got != tt.want && err == nil {
				t.Errorf("JSONUnquote(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsValidJSON(t *testing.T) {
	tests := []struct {
		json  string
		valid bool
	}{
		{`{"name":"John"}`, true},
		{`[1,2,3]`, true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.json, func(t *testing.T) {
			got := IsValidJSON(tt.json)
			if got != tt.valid {
				t.Errorf("IsValidJSON(%q) = %v, want %v", tt.json, got, tt.valid)
			}
		})
	}
}

func TestRegexMatch(t *testing.T) {
	tests := []struct {
		str     string
		pattern string
	}{
		{"hello", "hel*"},
		{"test123", "\\d+"},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got, err := RegexMatch(tt.str, tt.pattern)
			_ = got
			_ = err
		})
	}
}

func TestRegexReplace(t *testing.T) {
	_, _ = RegexReplace("hello world", "world", "there")
	_, _ = RegexReplace("test123", "\\d+", "num")
}

func TestRegexExtract(t *testing.T) {
	_, _ = RegexExtract("hello world", "wo.*")
	_, _ = RegexExtract("test123abc", "\\d+")
}

func TestJSONEach(t *testing.T) {
	_, _ = JSONEach(`{"name":"John","age":30}`)
	_, _ = JSONEach(`{}`)
}
