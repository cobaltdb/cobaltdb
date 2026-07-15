// Package util provides shared utility functions for CobaltDB.
package util

import (
	"strings"
)

// ToUpperFast returns an uppercased copy of s only if s contains lowercase
// letters. This avoids an allocation when s is already uppercase.
func ToUpperFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			return strings.ToUpper(s)
		}
	}
	return s
}

// ToLowerFast returns a lowercased copy of s only if s contains uppercase
// letters. This avoids an allocation when s is already lowercase.
func ToLowerFast(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return strings.ToLower(s)
		}
	}
	return s
}
