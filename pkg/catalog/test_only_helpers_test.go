package catalog

// Test-only helpers. These have no production callers and live in a _test.go
// file so the lint gate's unused-code check stays clean.

func (c *boundedRegexpCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *boundedJSONPathCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// extractJSONValue resolves a JSON path against the first row value holding a
// JSON document that matches.
func (c *Catalog) extractJSONValue(row []interface{}, column, path string) interface{} {
	_ = column
	for _, val := range row {
		doc, err := normalizeJSONDocument(val)
		if err != nil || doc == nil {
			continue
		}
		if current := extractJSONPathValue(doc, path); current != nil {
			return current
		}
	}
	return nil
}

// serializeCompositeKey serializes multiple values into a composite key.
func (fke *ForeignKeyEnforcer) serializeCompositeKey(values []interface{}) []byte {
	var parts [][]byte
	for _, v := range values {
		parts = append(parts, fke.serializeValue(v))
	}

	// Join with a delimiter
	var result []byte
	for i, part := range parts {
		if i > 0 {
			result = append(result, 0x00) // Null byte delimiter
		}
		result = append(result, part...)
	}
	return result
}
