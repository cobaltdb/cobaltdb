// Package util provides shared utility functions for CobaltDB.
package util

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// ToUpperFast returns an uppercased copy of s only if s contains lowercase
// letters. This avoids an allocation when s is already uppercase.
// Case detection is Unicode-aware: a string containing only non-ASCII
// lowercase letters (e.g. "ñ") is still uppercased, matching strings.ToUpper.
func ToUpperFast(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < utf8.RuneSelf {
			if 'a' <= c && c <= 'z' {
				return strings.ToUpper(s)
			}
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if unicode.IsLower(r) || unicode.IsTitle(r) || unicode.ToUpper(r) != r {
			return strings.ToUpper(s)
		}
		i += size - 1
	}
	return s
}

// ToLowerFast returns a lowercased copy of s only if s contains uppercase
// letters. This avoids an allocation when s is already lowercase.
// Case detection is Unicode-aware: a string containing only non-ASCII
// uppercase letters (e.g. "Ñ") is still lowercased, matching strings.ToLower.
func ToLowerFast(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < utf8.RuneSelf {
			if 'A' <= c && c <= 'Z' {
				return strings.ToLower(s)
			}
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if unicode.IsUpper(r) || unicode.IsTitle(r) || unicode.ToLower(r) != r {
			return strings.ToLower(s)
		}
		i += size - 1
	}
	return s
}
