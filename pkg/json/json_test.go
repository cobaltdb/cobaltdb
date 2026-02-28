package json

import (
	"testing"
)

func TestNewValue(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{"string", "hello"},
		{"number", 42},
		{"float", 3.14},
		{"bool", true},
		{"null", nil},
		{"array", []interface{}{1, 2, 3}},
		{"object", map[string]interface{}{"name": "John", "age": 30}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := NewValue(tt.input)
			if err != nil {
				t.Fatalf("Failed to create value: %v", err)
			}

			result, err := val.Interface()
			if err != nil {
				t.Fatalf("Failed to convert back: %v", err)
			}

			_ = result // Just check it doesn't error
		})
	}
}

func TestNewValueFromJSON(t *testing.T) {
	jsonStr := `{"name": "John", "age": 30, "skills": ["Go", "TypeScript"]}`

	val, err := NewValueFromString(jsonStr)
	if err != nil {
		t.Fatalf("Failed to create value from JSON: %v", err)
	}

	// Test Get
	name, err := val.GetString("name")
	if err != nil {
		t.Fatalf("Failed to get name: %v", err)
	}
	if name != "John" {
		t.Errorf("Expected name 'John', got %q", name)
	}

	age, err := val.GetInt("age")
	if err != nil {
		t.Fatalf("Failed to get age: %v", err)
	}
	if age != 30 {
		t.Errorf("Expected age 30, got %d", age)
	}

	skills, err := val.GetArray("skills")
	if err != nil {
		t.Fatalf("Failed to get skills: %v", err)
	}
	if len(skills) != 2 {
		t.Errorf("Expected 2 skills, got %d", len(skills))
	}
}

func TestJSONPath(t *testing.T) {
	jsonStr := `{
		"user": {
			"name": "John",
			"address": {
				"city": "Tallinn",
				"country": "Estonia"
			}
		},
		"items": [
			{"id": 1, "name": "Item 1"},
			{"id": 2, "name": "Item 2"}
		]
	}`

	val, err := NewValueFromString(jsonStr)
	if err != nil {
		t.Fatalf("Failed to create value: %v", err)
	}

	// Test nested path
	city, err := val.GetString("user.address.city")
	if err != nil {
		t.Fatalf("Failed to get city: %v", err)
	}
	if city != "Tallinn" {
		t.Errorf("Expected city 'Tallinn', got %q", city)
	}

	// Test array access
	itemName, err := val.GetString("items.0.name")
	if err != nil {
		t.Fatalf("Failed to get item name: %v", err)
	}
	if itemName != "Item 1" {
		t.Errorf("Expected item name 'Item 1', got %q", itemName)
	}
}

func TestSet(t *testing.T) {
	jsonStr := `{"name": "John", "age": 30}`

	val, err := NewValueFromString(jsonStr)
	if err != nil {
		t.Fatalf("Failed to create value: %v", err)
	}

	// Update existing field
	newVal, err := val.Set("name", "Jane")
	if err != nil {
		t.Fatalf("Failed to set name: %v", err)
	}

	name, err := newVal.GetString("name")
	if err != nil {
		t.Fatalf("Failed to get name: %v", err)
	}
	if name != "Jane" {
		t.Errorf("Expected name 'Jane', got %q", name)
	}

	// Add new field
	newVal, err = newVal.Set("city", "Tallinn")
	if err != nil {
		t.Fatalf("Failed to set city: %v", err)
	}

	city, err := newVal.GetString("city")
	if err != nil {
		t.Fatalf("Failed to get city: %v", err)
	}
	if city != "Tallinn" {
		t.Errorf("Expected city 'Tallinn', got %q", city)
	}
}

func TestRemove(t *testing.T) {
	jsonStr := `{"name": "John", "age": 30, "city": "Tallinn"}`

	val, err := NewValueFromString(jsonStr)
	if err != nil {
		t.Fatalf("Failed to create value: %v", err)
	}

	// Remove field
	newVal, err := val.Remove("city")
	if err != nil {
		t.Fatalf("Failed to remove city: %v", err)
	}

	// Verify it's removed
	_, err = newVal.GetString("city")
	if err == nil {
		t.Error("Expected error when getting removed field")
	}
}

func TestContains(t *testing.T) {
	jsonStr := `{"name": "John", "age": 30}`

	val, err := NewValueFromString(jsonStr)
	if err != nil {
		t.Fatalf("Failed to create value: %v", err)
	}

	// Test object contains
	other, _ := NewValueFromString(`{"name": "John"}`)
	contains, err := val.Contains(other)
	if err != nil {
		t.Fatalf("Failed to check contains: %v", err)
	}
	if !contains {
		t.Error("Expected val to contain other")
	}

	// Test array contains
	arrStr := `{"items": ["Go", "TypeScript", "Python"]}`
	arrVal, _ := NewValueFromString(arrStr)
	elem, _ := NewValue("Go")
	itemsVal, err := arrVal.Get("items")
	if err != nil {
		t.Fatalf("Failed to get items: %v", err)
	}
	contains2, err := itemsVal.Contains(elem)
	if err != nil {
		t.Fatalf("Failed to check array contains: %v", err)
	}
	if !contains2 {
		t.Error("Expected array to contain element")
	}
}

func TestType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"null", "null"},
		{"true", "boolean"},
		{"42", "number"},
		{"\"hello\"", "string"},
		{"[1, 2, 3]", "array"},
		{"{\"key\": \"value\"}", "object"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			val, err := NewValueFromJSON([]byte(tt.input))
			if err != nil {
				t.Fatalf("Failed to create value: %v", err)
			}

			typeStr, err := val.Type()
			if err != nil {
				t.Fatalf("Failed to get type: %v", err)
			}

			if typeStr != tt.expected {
				t.Errorf("Expected type %q, got %q", tt.expected, typeStr)
			}
		})
	}
}

func TestArrayLength(t *testing.T) {
	jsonStr := `{"items": [1, 2, 3, 4, 5]}`

	val, err := NewValueFromString(jsonStr)
	if err != nil {
		t.Fatalf("Failed to create value: %v", err)
	}

	length, err := val.ArrayLength("items")
	if err != nil {
		t.Fatalf("Failed to get array length: %v", err)
	}

	if length != 5 {
		t.Errorf("Expected length 5, got %d", length)
	}
}

func TestToJSON(t *testing.T) {
	obj := map[string]interface{}{
		"name": "John",
		"age":  30,
	}

	val, err := NewValue(obj)
	if err != nil {
		t.Fatalf("Failed to create value: %v", err)
	}

	jsonBytes, err := val.ToJSON()
	if err != nil {
		t.Fatalf("Failed to convert to JSON: %v", err)
	}

	// Verify it's valid JSON
	val2, err := NewValueFromJSON(jsonBytes)
	if err != nil {
		t.Fatalf("Failed to parse JSON back: %v", err)
	}

	name, _ := val2.GetString("name")
	if name != "John" {
		t.Errorf("Expected name 'John', got %q", name)
	}
}
