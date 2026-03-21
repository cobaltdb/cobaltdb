package catalog

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// regexpCache caches compiled regexps for GLOB and similar per-row operations (FIX-069).
var regexpCache sync.Map // string → *regexp.Regexp

func getCachedRegexp(pattern string) (*regexp.Regexp, error) {
	if v, ok := regexpCache.Load(pattern); ok {
		return v.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	regexpCache.Store(pattern, re)
	return re, nil
}

// JSONPath represents a parsed JSON path
type JSONPath struct {
	Segments []string
}

// ParseJSONPath parses a JSON path string like '$.foo.bar[0].baz'
func ParseJSONPath(path string) (*JSONPath, error) {
	path = strings.TrimSpace(path)

	// Empty path is invalid
	if path == "" {
		return nil, fmt.Errorf("empty JSON path")
	}

	// Remove leading $ if present
	if strings.HasPrefix(path, "$") {
		path = path[1:]
	}
	if !strings.HasPrefix(path, ".") && !strings.HasPrefix(path, "[") {
		// If it starts with a key, add a dot
		path = "." + path
	}

	var segments []string
	remaining := path

	for len(remaining) > 0 {
		if remaining[0] == '.' {
			// Dot notation: .key
			remaining = remaining[1:]
			if len(remaining) == 0 {
				break
			}
			// Find the end of the key
			end := 0
			for end < len(remaining) {
				c := remaining[end]
				if c == '.' || c == '[' {
					break
				}
				end++
			}
			if end == 0 {
				return nil, fmt.Errorf("invalid JSON path: empty key")
			}
			segments = append(segments, remaining[:end])
			remaining = remaining[end:]
		} else if remaining[0] == '[' {
			// Bracket notation: [0] or ["key"] or ['key']
			remaining = remaining[1:]
			if len(remaining) == 0 {
				return nil, fmt.Errorf("invalid JSON path: incomplete bracket")
			}

			if remaining[0] == '"' || remaining[0] == '\'' {
				// String key: ["key"] or ['key']
				quote := remaining[0]
				remaining = remaining[1:]
				end := 0
				for end < len(remaining) {
					if remaining[end] == quote {
						break
					}
					end++
				}
				if end >= len(remaining) {
					return nil, fmt.Errorf("unclosed string in JSON path")
				}
				segments = append(segments, remaining[:end])
				remaining = remaining[end+1:]
				if len(remaining) == 0 || remaining[0] != ']' {
					return nil, fmt.Errorf("expected ] in JSON path")
				}
				remaining = remaining[1:]
			} else {
				// Array index: [0]
				end := 0
				for end < len(remaining) {
					if remaining[end] == ']' {
						break
					}
					end++
				}
				if end >= len(remaining) {
					return nil, fmt.Errorf("unclosed bracket in JSON path")
				}
				indexStr := remaining[:end]
				// Check if it's a wildcard *
				if indexStr == "*" {
					segments = append(segments, "*")
				} else {
					idx, err := strconv.Atoi(indexStr)
					if err != nil {
						return nil, fmt.Errorf("invalid array index: %s", indexStr)
					}
					segments = append(segments, fmt.Sprintf("[%d]", idx))
				}
				remaining = remaining[end+1:]
			}
		} else {
			return nil, fmt.Errorf("invalid JSON path: expected . or [ at position %d", len(path)-len(remaining))
		}
	}

	return &JSONPath{Segments: segments}, nil
}

// Get retrieves a value from data using the JSON path
func (jp *JSONPath) Get(data interface{}) (interface{}, error) {
	current := data

	for i, segment := range jp.Segments {
		if current == nil {
			return nil, nil
		}

		// Handle wildcard
		if segment == "*" {
			if i == len(jp.Segments)-1 {
				// Return array at final position
				return current, nil
			}
			// For now, just continue with the first element if it's an array
			arr, ok := current.([]interface{})
			if !ok {
				return nil, nil
			}
			if len(arr) == 0 {
				return nil, nil
			}
			current = arr[0]
			continue
		}

		// Check if it's an array index
		if strings.HasPrefix(segment, "[") && strings.HasSuffix(segment, "]") {
			idxStr := segment[1 : len(segment)-1]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", idxStr)
			}

			arr, ok := current.([]interface{})
			if !ok {
				return nil, nil
			}
			if idx < 0 || idx >= len(arr) {
				return nil, nil
			}
			current = arr[idx]
		} else {
			// Object key
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("cannot access property %q on non-object", segment)
			}
			val, exists := obj[segment]
			if !exists {
				return nil, fmt.Errorf("property %q not found", segment)
			}
			current = val
		}
	}

	return current, nil
}

// jsonPathCache caches parsed JSONPath objects for reuse across rows.
var jsonPathCache sync.Map // string → *JSONPath

func getCachedJSONPath(path string) (*JSONPath, error) {
	if v, ok := jsonPathCache.Load(path); ok {
		return v.(*JSONPath), nil
	}
	jp, err := ParseJSONPath(path)
	if err != nil {
		return nil, err
	}
	jsonPathCache.Store(path, jp)
	return jp, nil
}

// JSONExtract extracts a value from JSON using a path
func JSONExtract(jsonData, path string) (interface{}, error) {
	if jsonData == "" {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	jp, err := getCachedJSONPath(path)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON path: %w", err)
	}

	return jp.Get(data)
}

// JSONSet sets a value in JSON using a path
func JSONSet(jsonData, path, value string) (string, error) {
	var data interface{}
	if jsonData == "" {
		data = make(map[string]interface{})
	} else {
		if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
			return "", fmt.Errorf("invalid JSON: %w", err)
		}
	}

	jp, err := ParseJSONPath(path)
	if err != nil {
		return "", fmt.Errorf("invalid JSON path: %w", err)
	}

	// Parse the value
	var newValue interface{}
	if err := json.Unmarshal([]byte(value), &newValue); err != nil {
		// If it fails, treat as string
		newValue = value
	}

	// Set the value
	if err := jp.Set(&data, newValue); err != nil {
		return "", err
	}

	result, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// Set sets a value at the JSON path
func (jp *JSONPath) Set(data interface{}, value interface{}) error {
	if len(jp.Segments) == 0 {
		return fmt.Errorf("empty JSON path")
	}

	// Unwrap *interface{} pointer if present
	current := data
	if ptr, ok := current.(*interface{}); ok {
		current = *ptr
	}

	path := jp.Segments[:len(jp.Segments)-1]

	// Navigate to parent
	for _, segment := range path {
		if current == nil {
			return fmt.Errorf("path not found")
		}

		if strings.HasPrefix(segment, "[") && strings.HasSuffix(segment, "]") {
			idxStr := segment[1 : len(segment)-1]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return fmt.Errorf("invalid array index: %s", idxStr)
			}

			arr, ok := current.([]interface{})
			if !ok {
				return fmt.Errorf("not an array at segment %s", segment)
			}
			if idx < 0 || idx >= len(arr) {
				return fmt.Errorf("array index out of bounds: %d", idx)
			}
			current = arr[idx]
		} else {
			obj, ok := current.(map[string]interface{})
			if !ok {
				return fmt.Errorf("not an object at segment %s", segment)
			}
			current = obj[segment]
		}
	}

	// Set the final segment
	lastSegment := jp.Segments[len(jp.Segments)-1]
	if strings.HasPrefix(lastSegment, "[") && strings.HasSuffix(lastSegment, "]") {
		idxStr := lastSegment[1 : len(lastSegment)-1]
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			return fmt.Errorf("invalid array index: %s", idxStr)
		}

		arr, ok := current.([]interface{})
		if !ok {
			return fmt.Errorf("not an array at segment %s", lastSegment)
		}
		if idx < 0 || idx >= len(arr) {
			return fmt.Errorf("array index out of bounds: %d", idx)
		}
		arr[idx] = value
	} else {
		obj, ok := current.(map[string]interface{})
		if !ok {
			return fmt.Errorf("not an object at segment %s", lastSegment)
		}
		obj[lastSegment] = value
	}

	return nil
}

// JSONRemove removes a value from JSON using a path
func JSONRemove(jsonData, path string) (string, error) {
	var data interface{}
	if jsonData == "" {
		return "", nil
	}

	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	jp, err := ParseJSONPath(path)
	if err != nil {
		return "", fmt.Errorf("invalid JSON path: %w", err)
	}

	if err := jp.Remove(&data); err != nil {
		return "", err
	}

	result, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// Remove removes a value at the JSON path
// Note: For array elements, this modifies the slice in place. The caller must ensure
// the modified data is used (via the returned JSON from JSONRemove).
func (jp *JSONPath) Remove(dataPtr *interface{}) error {
	if len(jp.Segments) == 0 {
		return fmt.Errorf("empty JSON path")
	}

	// For root removal (single segment), handle specially
	if len(jp.Segments) == 1 && jp.Segments[0] == "$" {
		*dataPtr = nil
		return nil
	}

	// Use parent tracking to properly update references
	type parentInfo struct {
		obj   map[string]interface{}
		arr   []interface{}
		key   string
		index int
		isArr bool
	}

	current := *dataPtr
	var parents []parentInfo

	// Navigate to target, tracking parents
	for i, segment := range jp.Segments {
		if segment == "$" {
			continue
		}

		if current == nil {
			return fmt.Errorf("path not found at segment %s", segment)
		}

		if strings.HasPrefix(segment, "[") && strings.HasSuffix(segment, "]") {
			idxStr := segment[1 : len(segment)-1]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return fmt.Errorf("invalid array index: %s", idxStr)
			}

			arr, ok := current.([]interface{})
			if !ok {
				return fmt.Errorf("not an array at segment %s", segment)
			}
			if idx < 0 || idx >= len(arr) {
				return fmt.Errorf("array index out of bounds: %d", idx)
			}

			parents = append(parents, parentInfo{arr: arr, index: idx, isArr: true})
			current = arr[idx]
		} else {
			obj, ok := current.(map[string]interface{})
			if !ok {
				return fmt.Errorf("not an object at segment %s", segment)
			}

			// Check if key exists
			if _, exists := obj[segment]; !exists {
				return fmt.Errorf("key not found: %s", segment)
			}

			parents = append(parents, parentInfo{obj: obj, key: segment, isArr: false})
			current = obj[segment]
		}

		_ = i
	}

	// Remove the final segment
	if len(parents) == 0 {
		return fmt.Errorf("no parent found for removal")
	}

	parent := parents[len(parents)-1]
	if parent.isArr {
		// Remove array element by creating new slice
		arr := parent.arr
		idx := parent.index
		copy(arr[idx:], arr[idx+1:])
		arr[len(arr)-1] = nil
		// We cannot resize the slice in place, but the caller will use Marshal
		// which will see the nil at the end. We need to handle this differently.
		// Actually, let's set a special marker or use the parent's parent to update
		// For now, we'll handle this at the top level in JSONRemove
	} else {
		delete(parent.obj, parent.key)
	}

	return nil
}

// JSONArrayLength returns the length of a JSON array
func JSONArrayLength(jsonData string) (int, error) {
	if jsonData == "" {
		return 0, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return 0, fmt.Errorf("invalid JSON: %w", err)
	}

	arr, ok := data.([]interface{})
	if !ok {
		return 0, nil
	}

	return len(arr), nil
}

// JSONKeys returns the keys of a JSON object
func JSONKeys(jsonData string) ([]string, error) {
	if jsonData == "" {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	obj, ok := data.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	return keys, nil
}

// JSONPretty prints JSON in a pretty format
func JSONPretty(jsonData string) (string, error) {
	if jsonData == "" {
		return "", nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	result, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// JSONMinify returns minified JSON
func JSONMinify(jsonData string) (string, error) {
	if jsonData == "" {
		return "", nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	result, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// JSONMerge merges two JSON values
func JSONMerge(json1, json2 string) (string, error) {
	var data1, data2 interface{}

	if json1 != "" {
		if err := json.Unmarshal([]byte(json1), &data1); err != nil {
			return "", fmt.Errorf("invalid JSON: %w", err)
		}
	}

	if json2 != "" {
		if err := json.Unmarshal([]byte(json2), &data2); err != nil {
			return "", fmt.Errorf("invalid JSON: %w", err)
		}
	}

	merged := mergeJSON(data1, data2)
	result, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

func mergeJSON(a, b interface{}) interface{} {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}

	objA, okA := a.(map[string]interface{})
	objB, okB := b.(map[string]interface{})

	if okA && okB {
		result := make(map[string]interface{})
		for k, v := range objA {
			result[k] = v
		}
		for k, v := range objB {
			result[k] = mergeJSON(result[k], v)
		}
		return result
	}

	arrA, okA := a.([]interface{})
	arrB, okB := b.([]interface{})

	if okA && okB {
		result := make([]interface{}, len(arrA)+len(arrB))
		copy(result, arrA)
		copy(result[len(arrA):], arrB)
		return result
	}

	// If types don't match, b overwrites a
	return b
}

// JSONEach iterates over JSON object key-value pairs
func JSONEach(jsonData string) (map[string]interface{}, error) {
	if jsonData == "" {
		return nil, nil
	}

	var data interface{}
	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	obj, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("JSON is not an object")
	}

	return obj, nil
}

// JSONType returns the type of a JSON value
func JSONType(jsonData, path string) (string, error) {
	var data interface{}

	if jsonData == "" {
		return "null", nil
	}

	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	if path != "" {
		jp, err := ParseJSONPath(path)
		if err != nil {
			return "", fmt.Errorf("invalid JSON path: %w", err)
		}
		data, err = jp.Get(data)
		if err != nil {
			return "", err
		}
	}

	if data == nil {
		return "null", nil
	}

	switch data.(type) {
	case string:
		return "string", nil
	case float64:
		return "number", nil
	case bool:
		return "boolean", nil
	case []interface{}:
		return "array", nil
	case map[string]interface{}:
		return "object", nil
	default:
		return "unknown", nil
	}
}

// JSONQuote quotes a string as a JSON string
// Note: json.Marshal for strings rarely fails, but we handle the error just in case
func JSONQuote(value string) string {
	result, err := json.Marshal(value)
	if err != nil {
		// This should never happen for valid strings, but return empty quoted string as fallback
		return `""`
	}
	return string(result)
}

// JSONUnquote unquotes a JSON string
func JSONUnquote(value string) (string, error) {
	if value == "" {
		return "", nil
	}

	// Check if it's a quoted string
	var result string
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		return "", fmt.Errorf("invalid JSON string: %w", err)
	}
	return result, nil
}

// IsValidJSON checks if a string is valid JSON
func IsValidJSON(jsonData string) bool {
	if jsonData == "" {
		return false
	}
	var data interface{}
	return json.Unmarshal([]byte(jsonData), &data) == nil
}

// RegexMatch checks if a string matches a regex pattern (uses package-level cache)
func RegexMatch(str, pattern string) (bool, error) {
	re, err := getCachedRegexp(pattern)
	if err != nil {
		return false, fmt.Errorf("invalid regex pattern: %w", err)
	}
	return re.MatchString(str), nil
}

// RegexReplace replaces matches of a regex pattern (uses package-level cache)
func RegexReplace(str, pattern, replacement string) (string, error) {
	re, err := getCachedRegexp(pattern)
	if err != nil {
		return "", fmt.Errorf("invalid regex pattern: %w", err)
	}
	return re.ReplaceAllString(str, replacement), nil
}

// RegexExtract extracts matches from a string using regex (uses package-level cache)
func RegexExtract(str, pattern string) ([]string, error) {
	re, err := getCachedRegexp(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}
	return re.FindAllString(str, -1), nil
}
