package catalog

import (
	"encoding/json"
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

func TestJSONPathGet(t *testing.T) {
	tests := []struct {
		json string
		path string
		want interface{}
	}{
		{`{"name":"John"}`, "$.name", "John"},
		{`{"store":{"book":[{"author":"John"}]}}`, "$.store.book[0].author", "John"},
		{`[1,2,3]`, "$[0]", float64(1)},
		{`{"items":[1,2,3]}`, "$.items[1]", float64(2)},
		{`{"name":"John"}`, "$.age", nil},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			jp, err := ParseJSONPath(tt.path)
			if err != nil {
				t.Logf("ParseJSONPath error: %v", err)
				return
			}

			var data interface{}
			json.Unmarshal([]byte(tt.json), &data)

			got, err := jp.Get(data)
			if tt.want == nil && err == nil {
				t.Error("Expected error for missing key")
				return
			}
			if tt.want != nil && got != tt.want {
				t.Errorf("Get(%q) = %v, want %v", tt.json, got, tt.want)
			}
		})
	}
}

func TestJSONPathSet(t *testing.T) {
	tests := []struct {
		json  string
		path  string
		value interface{}
		want  string
	}{
		{`{}`, "$.name", "John", `{"name":"John"}`},
		{`{"name":"Old"}`, "$.name", "New", `{"name":"New"}`},
		{`{"a":{"b":1}}`, "$.a.c", 2, `{"a":{"b":1,"c":2}}`},
		// Array tests
		{`[1,2,3]`, "$[0]", 10, `[10,2,3]`},
		{`{"items":[1,2,3]}`, "$.items[1]", 20, `{"items":[1,20,3]}`},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			jp, err := ParseJSONPath(tt.path)
			if err != nil {
				t.Logf("ParseJSONPath error: %v", err)
				return
			}

			var data interface{}
			json.Unmarshal([]byte(tt.json), &data)

			err = jp.Set(data, tt.value)
			if err != nil {
				t.Errorf("Set error: %v", err)
				return
			}

			result, _ := json.Marshal(data)
			if string(result) != tt.want {
				t.Errorf("Set result = %s, want %s", string(result), tt.want)
			}
		})
	}
}

func TestJSONPathSetErrors(t *testing.T) {
	// Path not found - nil parent
	jp, _ := ParseJSONPath("$.a.b")
	var data interface{}
	json.Unmarshal([]byte(`{}`), &data)
	err := jp.Set(data, "value")
	if err == nil {
		t.Error("Expected error for path not found")
	}

	// Not an array
	jp, _ = ParseJSONPath("$.name[0]")
	json.Unmarshal([]byte(`{"name":"John"}`), &data)
	err = jp.Set(data, "value")
	if err == nil {
		t.Error("Expected error for not an array")
	}

	// Array index out of bounds
	jp, _ = ParseJSONPath("$.items[10]")
	json.Unmarshal([]byte(`{"items":[1,2,3]}`), &data)
	err = jp.Set(data, "value")
	if err == nil {
		t.Error("Expected error for array index out of bounds")
	}

	// Not an object
	jp, _ = ParseJSONPath("$.name.key")
	json.Unmarshal([]byte(`{"name":"John"}`), &data)
	err = jp.Set(data, "value")
	if err == nil {
		t.Error("Expected error for not an object")
	}
}

func TestJSONPathRemove(t *testing.T) {
	tests := []struct {
		json string
		path string
		want string
	}{
		{`{"name":"John","age":30}`, "$.age", `{"name":"John"}`},
		{`{"a":1,"b":2}`, "$.a", `{"b":2}`},
		// Array removal - shifts elements and appends null
		{`[1,2,3]`, "$[1]", `[1,3,null]`},
		{`{"items":[1,2,3]}`, "$.items[0]", `{"items":[2,3,null]}`},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			jp, err := ParseJSONPath(tt.path)
			if err != nil {
				t.Logf("ParseJSONPath error: %v", err)
				return
			}

			var data interface{}
			json.Unmarshal([]byte(tt.json), &data)

			err = jp.Remove(&data)
			if err != nil {
				t.Errorf("Remove error: %v", err)
				return
			}

			result, _ := json.Marshal(data)
			if string(result) != tt.want {
				t.Errorf("Remove result = %s, want %s", string(result), tt.want)
			}
		})
	}
}

func TestJSONPathRemoveErrors(t *testing.T) {
	// Path not found - nil parent
	jp, _ := ParseJSONPath("$.a.b")
	var data interface{}
	json.Unmarshal([]byte(`{}`), &data)
	err := jp.Remove(&data)
	if err == nil {
		t.Error("Expected error for path not found")
	}

	// Not an array
	jp, _ = ParseJSONPath("$.name[0]")
	json.Unmarshal([]byte(`{"name":"John"}`), &data)
	err = jp.Remove(&data)
	if err == nil {
		t.Error("Expected error for not an array")
	}

	// Array index out of bounds
	jp, _ = ParseJSONPath("$.items[10]")
	json.Unmarshal([]byte(`{"items":[1,2,3]}`), &data)
	err = jp.Remove(&data)
	if err == nil {
		t.Error("Expected error for array index out of bounds")
	}

	// Not an object
	jp, _ = ParseJSONPath("$.name.key")
	json.Unmarshal([]byte(`{"name":"John"}`), &data)
	err = jp.Remove(&data)
	if err == nil {
		t.Error("Expected error for not an object")
	}
}

func TestJSONPathErrors(t *testing.T) {
	// Invalid path
	_, err := ParseJSONPath("")
	if err == nil {
		t.Error("Expected error for empty path")
	}

	// Get on invalid data
	jp, _ := ParseJSONPath("$.name")
	_, err = jp.Get("invalid")
	if err == nil {
		t.Error("Expected error for Get on non-object")
	}
}

func TestMergeJSON(t *testing.T) {
	tests := []struct {
		a    string
		b    string
		want string
	}{
		{`{"a":1}`, `{"b":2}`, `{"a":1,"b":2}`},
		{`{"a":1}`, `{"a":2}`, `{"a":2}`},
		{`{"a":{"b":1}}`, `{"a":{"c":2}}`, `{"a":{"b":1,"c":2}}`},
		{`[1,2]`, `[3,4]`, `[1,2,3,4]`},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got, err := JSONMerge(tt.a, tt.b)
			if err != nil {
				t.Errorf("JSONMerge error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("JSONMerge(%q, %q) = %q, want %q", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestRegexMatchNoMatch(t *testing.T) {
	// No match
	got, err := RegexMatch("hello", "xyz")
	if err != nil {
		t.Errorf("RegexMatch error: %v", err)
	}
	if got {
		t.Error("Expected no match")
	}
}

func TestRegexReplaceNoMatch(t *testing.T) {
	// No match - should return original string
	got, err := RegexReplace("hello", "xyz", "replaced")
	if err != nil {
		t.Errorf("RegexReplace error: %v", err)
	}
	if got != "hello" {
		t.Errorf("RegexReplace = %q, want %q", got, "hello")
	}
}

func TestRegexExtractNoMatch(t *testing.T) {
	// No match
	got, err := RegexExtract("hello", "xyz")
	if err != nil {
		t.Errorf("RegexExtract error: %v", err)
	}
	if got != nil {
		t.Errorf("RegexExtract = %v, want nil", got)
	}
}
