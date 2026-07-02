package audit

// Test-only helpers. These wrappers have no production callers and live in a
// _test.go file so the lint gate's unused-code check stays clean.

// maskMetadataValues masks values in metadata whose keys match sensitive patterns.
func maskMetadataValues(metadata map[string]interface{}) map[string]interface{} {
	return maskMetadataValuesWithKeys(metadata, nil)
}
