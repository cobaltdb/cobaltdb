package json

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

// Value represents a JSON value stored as MessagePack
type Value struct {
	raw []byte
}

// NewValue creates a JSON value from a Go value
func NewValue(v interface{}) (*Value, error) {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &Value{raw: data}, nil
}

// NewValueFromJSON creates a JSON value from JSON bytes
func NewValueFromJSON(data []byte) (*Value, error) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return NewValue(v)
}

// NewValueFromString creates a JSON value from a JSON string
func NewValueFromString(s string) (*Value, error) {
	return NewValueFromJSON([]byte(s))
}

// Raw returns the raw MessagePack bytes
func (v *Value) Raw() []byte {
	return v.raw
}

// ToJSON converts the value to JSON bytes
func (v *Value) ToJSON() ([]byte, error) {
	val, err := v.Interface()
	if err != nil {
		return nil, err
	}
	return json.Marshal(val)
}

// String returns the JSON string representation
func (v *Value) String() (string, error) {
	data, err := v.ToJSON()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Interface converts the value to a Go interface{}
func (v *Value) Interface() (interface{}, error) {
	var val interface{}
	err := msgpack.Unmarshal(v.raw, &val)
	return val, err
}

// Get retrieves a value at the given JSON path
func (v *Value) Get(path string) (*Value, error) {
	val, err := v.Interface()
	if err != nil {
		return nil, err
	}

	parts := parsePath(path)
	current := val

	for _, part := range parts {
		switch c := current.(type) {
		case map[string]interface{}:
			var ok bool
			current, ok = c[part]
			if !ok {
				return nil, fmt.Errorf("path not found: %s", path)
			}
		case []interface{}:
			idx, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", part)
			}
			if idx < 0 || idx >= len(c) {
				return nil, fmt.Errorf("array index out of bounds: %d", idx)
			}
			current = c[idx]
		default:
			return nil, fmt.Errorf("cannot navigate path %s in %T", part, current)
		}
	}

	return NewValue(current)
}

// GetString retrieves a string value at the given path
func (v *Value) GetString(path string) (string, error) {
	val, err := v.Get(path)
	if err != nil {
		return "", err
	}

	iface, err := val.Interface()
	if err != nil {
		return "", err
	}

	switch s := iface.(type) {
	case string:
		return s, nil
	case nil:
		return "null", nil
	default:
		return fmt.Sprintf("%v", s), nil
	}
}

// GetInt retrieves an int64 value at the given path
func (v *Value) GetInt(path string) (int64, error) {
	val, err := v.Get(path)
	if err != nil {
		return 0, err
	}

	iface, err := val.Interface()
	if err != nil {
		return 0, err
	}

	switch n := iface.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", iface)
	}
}

// GetFloat retrieves a float64 value at the given path
func (v *Value) GetFloat(path string) (float64, error) {
	val, err := v.Get(path)
	if err != nil {
		return 0, err
	}

	iface, err := val.Interface()
	if err != nil {
		return 0, err
	}

	switch n := iface.(type) {
	case float64:
		return n, nil
	case int64:
		return float64(n), nil
	case int:
		return float64(n), nil
	case string:
		return strconv.ParseFloat(n, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", iface)
	}
}

// GetBool retrieves a bool value at the given path
func (v *Value) GetBool(path string) (bool, error) {
	val, err := v.Get(path)
	if err != nil {
		return false, err
	}

	iface, err := val.Interface()
	if err != nil {
		return false, err
	}

	switch b := iface.(type) {
	case bool:
		return b, nil
	case string:
		return strconv.ParseBool(b)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", iface)
	}
}

// GetArray retrieves an array value at the given path
func (v *Value) GetArray(path string) ([]*Value, error) {
	val, err := v.Get(path)
	if err != nil {
		return nil, err
	}

	iface, err := val.Interface()
	if err != nil {
		return nil, err
	}

	arr, ok := iface.([]interface{})
	if !ok {
		return nil, fmt.Errorf("value at path %s is not an array", path)
	}

	result := make([]*Value, len(arr))
	for i, item := range arr {
		result[i], err = NewValue(item)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Set sets a value at the given JSON path
func (v *Value) Set(path string, value interface{}) (*Value, error) {
	val, err := v.Interface()
	if err != nil {
		return nil, err
	}

	parts := parsePath(path)
	if len(parts) == 0 {
		return NewValue(value)
	}

	// Navigate to parent of target
	current := val
	for i := 0; i < len(parts)-1; i++ {
		switch c := current.(type) {
		case map[string]interface{}:
			var ok bool
			current, ok = c[parts[i]]
			if !ok {
				// Create intermediate object
				c[parts[i]] = make(map[string]interface{})
				current = c[parts[i]]
			}
		case []interface{}:
			idx, err := strconv.Atoi(parts[i])
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", parts[i])
			}
			if idx < 0 || idx >= len(c) {
				return nil, fmt.Errorf("array index out of bounds: %d", idx)
			}
			current = c[idx]
		default:
			return nil, fmt.Errorf("cannot navigate path %s in %T", parts[i], current)
		}
	}

	// Set the value
	switch c := current.(type) {
	case map[string]interface{}:
		c[parts[len(parts)-1]] = value
	default:
		return nil, fmt.Errorf("cannot set value in %T", current)
	}

	return NewValue(val)
}

// Remove removes a value at the given JSON path
func (v *Value) Remove(path string) (*Value, error) {
	val, err := v.Interface()
	if err != nil {
		return nil, err
	}

	parts := parsePath(path)
	if len(parts) == 0 {
		return v, nil
	}

	// Navigate to parent of target
	current := val
	for i := 0; i < len(parts)-1; i++ {
		switch c := current.(type) {
		case map[string]interface{}:
			var ok bool
			current, ok = c[parts[i]]
			if !ok {
				return v, nil // Path doesn't exist, nothing to remove
			}
		case []interface{}:
			idx, err := strconv.Atoi(parts[i])
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", parts[i])
			}
			if idx < 0 || idx >= len(c) {
				return v, nil // Index out of bounds, nothing to remove
			}
			current = c[idx]
		default:
			return v, nil // Cannot navigate, nothing to remove
		}
	}

	// Remove the value
	switch c := current.(type) {
	case map[string]interface{}:
		delete(c, parts[len(parts)-1])
	}

	return NewValue(val)
}

// Contains checks if the JSON value contains the given value
func (v *Value) Contains(other *Value) (bool, error) {
	val, err := v.Interface()
	if err != nil {
		return false, err
	}

	otherVal, err := other.Interface()
	if err != nil {
		return false, err
	}

	return contains(val, otherVal), nil
}

// contains recursively checks if a contains b
func contains(a, b interface{}) bool {
	switch av := a.(type) {
	case map[string]interface{}:
		bv, ok := b.(map[string]interface{})
		if !ok {
			return false
		}
		for k, v := range bv {
			avv, ok := av[k]
			if !ok || !contains(avv, v) {
				return false
			}
		}
		return true
	case []interface{}:
		bv, ok := b.([]interface{})
		if !ok {
			// Check if b is an element of the array
			for _, elem := range av {
				if contains(elem, b) {
					return true
				}
			}
			return false
		}
		// Check if bv is a subset of av
		for _, bvItem := range bv {
			found := false
			for _, avItem := range av {
				if contains(avItem, bvItem) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}

// Type returns the JSON type of the value
func (v *Value) Type() (string, error) {
	val, err := v.Interface()
	if err != nil {
		return "", err
	}

	switch val.(type) {
	case nil:
		return "null", nil
	case bool:
		return "boolean", nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return "number", nil
	case string:
		return "string", nil
	case []interface{}:
		return "array", nil
	case map[string]interface{}:
		return "object", nil
	default:
		return "unknown", nil
	}
}

// ArrayLength returns the length of an array at the given path
func (v *Value) ArrayLength(path string) (int, error) {
	val, err := v.Get(path)
	if err != nil {
		return 0, err
	}

	iface, err := val.Interface()
	if err != nil {
		return 0, err
	}

	arr, ok := iface.([]interface{})
	if !ok {
		return 0, fmt.Errorf("value at path %s is not an array", path)
	}

	return len(arr), nil
}

// Valid checks if the data is valid JSON/MessagePack
func Valid(data []byte) bool {
	var v interface{}
	return msgpack.Unmarshal(data, &v) == nil
}

// parsePath parses a JSON path like "address.city" or "items[0]"
func parsePath(path string) []string {
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")

	var parts []string
	var current strings.Builder

	for i := 0; i < len(path); i++ {
		ch := path[i]
		switch ch {
		case '.':
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		case '[':
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
			// Find closing bracket
			j := i + 1
			for j < len(path) && path[j] != ']' {
				j++
			}
			if j < len(path) {
				parts = append(parts, path[i+1:j])
				i = j
			}
		default:
			current.WriteByte(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// Extract extracts a value from JSON using a path
func Extract(data []byte, path string) (*Value, error) {
	v, err := NewValueFromJSON(data)
	if err != nil {
		return nil, err
	}
	return v.Get(path)
}

// SetValue sets a value in JSON using a path
func SetValue(data []byte, path string, value interface{}) ([]byte, error) {
	v, err := NewValueFromJSON(data)
	if err != nil {
		return nil, err
	}

	newVal, err := v.Set(path, value)
	if err != nil {
		return nil, err
	}

	return newVal.ToJSON()
}

// RemoveValue removes a value from JSON using a path
func RemoveValue(data []byte, path string) ([]byte, error) {
	v, err := NewValueFromJSON(data)
	if err != nil {
		return nil, err
	}

	newVal, err := v.Remove(path)
	if err != nil {
		return nil, err
	}

	return newVal.ToJSON()
}
