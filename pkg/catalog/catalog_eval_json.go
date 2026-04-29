package catalog

import (
	"fmt"
)

func evaluateJSONFunction(funcName string, args []interface{}) (interface{}, error) {
	switch funcName {
	case "JSON_EXTRACT":
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_EXTRACT requires 2 arguments")
		}
		// Handle various argument types
		var jsonData string
		var path string

		// First arg - could be string or other
		switch v := args[0].(type) {
		case string:
			jsonData = v
		default:
			if args[0] != nil {
				jsonData = ValueToStringKey(args[0])
			}
		}

		// Second arg - could be string or other
		switch v := args[1].(type) {
		case string:
			path = v
		default:
			if args[1] != nil {
				path = ValueToStringKey(args[1])
			}
		}

		return JSONExtract(jsonData, path)

	case "JSON_SET":
		if len(args) < 3 {
			return nil, fmt.Errorf("JSON_SET requires 3 arguments")
		}
		jsonData, _ := args[0].(string)
		path, _ := args[1].(string)
		value, _ := args[2].(string)
		return JSONSet(jsonData, path, value)

	case "JSON_REMOVE":
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_REMOVE requires 2 arguments")
		}
		jsonData, _ := args[0].(string)
		path, _ := args[1].(string)
		return JSONRemove(jsonData, path)

	case "JSON_VALID":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_VALID requires 1 argument")
		}
		if args[0] == nil {
			return false, nil
		}
		str, ok := args[0].(string)
		if !ok {
			return false, nil
		}
		return IsValidJSON(str), nil

	case "JSON_ARRAY_LENGTH":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_ARRAY_LENGTH requires 1 argument")
		}
		if args[0] == nil {
			return 0, nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return 0, nil
		}
		length, err := JSONArrayLength(jsonData)
		if err != nil {
			return nil, err
		}
		return float64(length), nil

	case "JSON_TYPE":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_TYPE requires at least 1 argument")
		}
		if args[0] == nil {
			return "null", nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return "unknown", nil
		}
		var path string
		if len(args) > 1 {
			path, _ = args[1].(string)
		}
		return JSONType(jsonData, path)

	case "JSON_KEYS":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_KEYS requires at least 1 argument")
		}
		if args[0] == nil {
			return nil, nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONKeys(jsonData)

	case "JSON_PRETTY":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_PRETTY requires 1 argument")
		}
		if args[0] == nil {
			return "", nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONPretty(jsonData)

	case "JSON_MINIFY":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_MINIFY requires 1 argument")
		}
		if args[0] == nil {
			return "", nil
		}
		jsonData, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONMinify(jsonData)

	case "JSON_MERGE":
		if len(args) < 2 {
			return nil, fmt.Errorf("JSON_MERGE requires 2 arguments")
		}
		json1, _ := args[0].(string)
		json2, _ := args[1].(string)
		return JSONMerge(json1, json2)

	case "JSON_QUOTE":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_QUOTE requires 1 argument")
		}
		if args[0] == nil {
			return "null", nil
		}
		str, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONQuote(str), nil

	case "JSON_UNQUOTE":
		if len(args) < 1 {
			return nil, fmt.Errorf("JSON_UNQUOTE requires 1 argument")
		}
		if args[0] == nil {
			return "", nil
		}
		str, ok := args[0].(string)
		if !ok {
			return nil, nil
		}
		return JSONUnquote(str)

	case "REGEXP_MATCH":
		if len(args) < 2 {
			return nil, fmt.Errorf("REGEXP_MATCH requires 2 arguments")
		}
		str, _ := args[0].(string)
		pattern, _ := args[1].(string)
		if str == "" || pattern == "" {
			return false, nil
		}
		return RegexMatch(str, pattern)

	case "REGEXP_REPLACE":
		if len(args) < 3 {
			return nil, fmt.Errorf("REGEXP_REPLACE requires 3 arguments")
		}
		str, _ := args[0].(string)
		pattern, _ := args[1].(string)
		replacement, _ := args[2].(string)
		if str == "" || pattern == "" {
			return str, nil
		}
		return RegexReplace(str, pattern, replacement)

	case "REGEXP_EXTRACT":
		if len(args) < 2 {
			return nil, fmt.Errorf("REGEXP_EXTRACT requires 2 arguments")
		}
		str, _ := args[0].(string)
		pattern, _ := args[1].(string)
		if str == "" || pattern == "" {
			return []string{}, nil
		}
		return RegexExtract(str, pattern)

	default:
		return nil, fmt.Errorf("unknown function: %s", funcName)
	}
}

