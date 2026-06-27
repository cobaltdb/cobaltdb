package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const (
	maxJSONDocumentBytes    = maxStringResultLen
	maxJSONPathBytes        = 16 << 10
	maxJSONPathSegments     = 256
	maxJSONPathSegmentBytes = 1024
	maxCachedRegexps        = 1024
	maxCachedJSONPaths      = 1024
)

var errJSONInputTooLarge = errors.New("JSON input too large")

// regexpCache caches compiled regexps for GLOB and similar per-row operations.
var regexpCache = newBoundedRegexpCache(maxCachedRegexps)

func getCachedRegexp(pattern string) (*regexp.Regexp, error) {
	if re, ok := regexpCache.get(pattern); ok {
		return re, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	regexpCache.set(pattern, re)
	return re, nil
}

type boundedRegexpCache struct {
	mu    sync.Mutex
	limit int
	order []string
	items map[string]*regexp.Regexp
}

func newBoundedRegexpCache(limit int) *boundedRegexpCache {
	return &boundedRegexpCache{
		limit: limit,
		order: make([]string, 0, limit),
		items: make(map[string]*regexp.Regexp),
	}
}

func (c *boundedRegexpCache) get(key string) (*regexp.Regexp, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	re, ok := c.items[key]
	return re, ok
}

func (c *boundedRegexpCache) set(key string, value *regexp.Regexp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.items[key]; exists {
		c.items[key] = value
		return
	}
	c.evictIfFull()
	c.items[key] = value
	c.order = append(c.order, key)
}

func (c *boundedRegexpCache) evictIfFull() {
	if c.limit <= 0 {
		clear(c.items)
		c.order = c.order[:0]
		return
	}
	for len(c.items) >= c.limit && len(c.order) > 0 {
		key := c.order[0]
		copy(c.order, c.order[1:])
		c.order = c.order[:len(c.order)-1]
		delete(c.items, key)
	}
}

func (c *boundedRegexpCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func unmarshalJSONInput(input string, target interface{}) error {
	if len(input) > maxJSONDocumentBytes {
		return fmt.Errorf("%w: maximum allowed size is %d bytes", errJSONInputTooLarge, maxJSONDocumentBytes)
	}
	return json.Unmarshal([]byte(input), target)
}

// JSONPath represents a parsed JSON path
type JSONPath struct {
	Segments []string
}

// ParseJSONPath parses a JSON path string like '$.foo.bar[0].baz'
func ParseJSONPath(path string) (*JSONPath, error) {
	path = strings.TrimSpace(path)
	if len(path) > maxJSONPathBytes {
		return nil, fmt.Errorf("JSON path too large: maximum allowed size is %d bytes", maxJSONPathBytes)
	}

	// Empty path is invalid
	if path == "" {
		return nil, fmt.Errorf("empty JSON path")
	}

	// Remove leading $ if present
	path = strings.TrimPrefix(path, "$")
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
			if err := appendJSONPathSegment(&segments, remaining[:end]); err != nil {
				return nil, err
			}
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
				if err := appendJSONPathSegment(&segments, remaining[:end]); err != nil {
					return nil, err
				}
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
					if err := appendJSONPathSegment(&segments, "*"); err != nil {
						return nil, err
					}
				} else {
					idx, err := strconv.Atoi(indexStr)
					if err != nil {
						return nil, fmt.Errorf("invalid array index: %s", indexStr)
					}
					if err := appendJSONPathSegment(&segments, fmt.Sprintf("[%d]", idx)); err != nil {
						return nil, err
					}
				}
				remaining = remaining[end+1:]
			}
		} else {
			return nil, fmt.Errorf("invalid JSON path: expected . or [ at position %d", len(path)-len(remaining))
		}
	}

	return &JSONPath{Segments: segments}, nil
}

func appendJSONPathSegment(segments *[]string, segment string) error {
	if len(segment) > maxJSONPathSegmentBytes {
		return fmt.Errorf("JSON path segment too large: maximum allowed size is %d bytes", maxJSONPathSegmentBytes)
	}
	if len(*segments) >= maxJSONPathSegments {
		return fmt.Errorf("JSON path segment count exceeds maximum (%d)", maxJSONPathSegments)
	}
	*segments = append(*segments, segment)
	return nil
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
			// Recursively apply remaining path to each array element
			arr, ok := current.([]interface{})
			if !ok {
				return nil, nil
			}
			if len(arr) == 0 {
				return nil, nil
			}
			remaining := jp.Segments[i+1:]
			result := make([]interface{}, 0, len(arr))
			for _, elem := range arr {
				subPath := &JSONPath{Segments: remaining}
				val, err := subPath.Get(elem)
				if err != nil {
					return nil, err
				}
				if val != nil {
					result = append(result, val)
				}
			}
			if len(result) == 0 {
				return nil, nil
			}
			return result, nil
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
var jsonPathCache = newBoundedJSONPathCache(maxCachedJSONPaths)

func getCachedJSONPath(path string) (*JSONPath, error) {
	if jp, ok := jsonPathCache.get(path); ok {
		return jp, nil
	}
	jp, err := ParseJSONPath(path)
	if err != nil {
		return nil, err
	}
	jsonPathCache.set(path, jp)
	return jp, nil
}

type boundedJSONPathCache struct {
	mu    sync.Mutex
	limit int
	order []string
	items map[string]*JSONPath
}

func newBoundedJSONPathCache(limit int) *boundedJSONPathCache {
	return &boundedJSONPathCache{
		limit: limit,
		order: make([]string, 0, limit),
		items: make(map[string]*JSONPath),
	}
}

func (c *boundedJSONPathCache) get(key string) (*JSONPath, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	jp, ok := c.items[key]
	return jp, ok
}

func (c *boundedJSONPathCache) set(key string, value *JSONPath) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.items[key]; exists {
		c.items[key] = value
		return
	}
	c.evictIfFull()
	c.items[key] = value
	c.order = append(c.order, key)
}

func (c *boundedJSONPathCache) evictIfFull() {
	if c.limit <= 0 {
		clear(c.items)
		c.order = c.order[:0]
		return
	}
	for len(c.items) >= c.limit && len(c.order) > 0 {
		key := c.order[0]
		copy(c.order, c.order[1:])
		c.order = c.order[:len(c.order)-1]
		delete(c.items, key)
	}
}

func (c *boundedJSONPathCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// JSONExtract extracts a value from JSON using a path
func JSONExtract(jsonData, path string) (interface{}, error) {
	if jsonData == "" {
		return nil, nil
	}

	jp, err := getCachedJSONPath(path)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON path: %w", err)
	}

	var data interface{}
	if err := unmarshalJSONInput(jsonData, &data); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	return jp.Get(data)
}

// JSONSet sets a value in JSON using a path
func JSONSet(jsonData, path, value string) (string, error) {
	var data interface{}
	if jsonData == "" {
		data = make(map[string]interface{})
	} else {
		if err := unmarshalJSONInput(jsonData, &data); err != nil {
			return "", fmt.Errorf("invalid JSON: %w", err)
		}
	}

	jp, err := ParseJSONPath(path)
	if err != nil {
		return "", fmt.Errorf("invalid JSON path: %w", err)
	}

	// Parse the value
	var newValue interface{}
	if err := unmarshalJSONInput(value, &newValue); err != nil {
		if errors.Is(err, errJSONInputTooLarge) {
			return "", fmt.Errorf("invalid JSON value: %w", err)
		}
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

	if err := unmarshalJSONInput(jsonData, &data); err != nil {
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
		// Splice the element out into a genuinely shorter slice and write that
		// shorter slice back into the array's own container (the grandparent, or
		// the document root). Shifting in place and nil-ing the tail (the old
		// behavior) left a spurious trailing null, e.g.
		// JSON_REMOVE('[1,2,3]','$[0]') -> "[2,3,null]".
		arr := parent.arr
		idx := parent.index
		newArr := make([]interface{}, 0, len(arr)-1)
		newArr = append(newArr, arr[:idx]...)
		newArr = append(newArr, arr[idx+1:]...)
		if len(parents) >= 2 {
			gp := parents[len(parents)-2]
			if gp.isArr {
				gp.arr[gp.index] = newArr
			} else {
				gp.obj[gp.key] = newArr
			}
		} else {
			*dataPtr = newArr
		}
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
	if err := unmarshalJSONInput(jsonData, &data); err != nil {
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
	if err := unmarshalJSONInput(jsonData, &data); err != nil {
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
	if err := unmarshalJSONInput(jsonData, &data); err != nil {
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
	if err := unmarshalJSONInput(jsonData, &data); err != nil {
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
		if err := unmarshalJSONInput(json1, &data1); err != nil {
			return "", fmt.Errorf("invalid JSON: %w", err)
		}
	}

	if json2 != "" {
		if err := unmarshalJSONInput(json2, &data2); err != nil {
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
	if err := unmarshalJSONInput(jsonData, &data); err != nil {
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

	if err := unmarshalJSONInput(jsonData, &data); err != nil {
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
	if err := unmarshalJSONInput(value, &result); err != nil {
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
	return unmarshalJSONInput(jsonData, &data) == nil
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
