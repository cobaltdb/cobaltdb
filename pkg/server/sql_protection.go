// SQL Injection Detection and Prevention
// Monitors and blocks suspicious SQL patterns

package server

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

// SQLProtectionConfig configures SQL injection protection
type SQLProtectionConfig struct {
	// Enable detection (default: true)
	Enabled bool

	// Block on detection (default: false - only log)
	BlockOnDetection bool

	// Max query length (default: 10000)
	MaxQueryLength int

	// Max allowed OR conditions (default: 10)
	MaxORConditions int

	// Max UNION statements (default: 5)
	MaxUNIONCount int

	// Suspicious pattern threshold (default: 3)
	SuspiciousThreshold int
}

// DefaultSQLProtectionConfig returns default configuration
func DefaultSQLProtectionConfig() *SQLProtectionConfig {
	return &SQLProtectionConfig{
		Enabled:             true,
		BlockOnDetection:    false,
		MaxQueryLength:      10000,
		MaxORConditions:     10,
		MaxUNIONCount:       5,
		SuspiciousThreshold: 3,
	}
}

// SQLProtector provides SQL injection protection
type SQLProtector struct {
	config *SQLProtectionConfig

	// Suspicious patterns
	patterns []*SuspiciousPattern

	// Statistics
	stats ProtectionStats

	// Whitelist (safe patterns)
	whitelist map[string]bool
	wlMu      sync.RWMutex
}

// SuspiciousPattern represents a pattern to detect
type SuspiciousPattern struct {
	Name        string
	Pattern     *regexp.Regexp
	Severity    ProtectionSeverity
	Description string
}

// ProtectionSeverity levels for SQL protection
type ProtectionSeverity int

const (
	ProtectionLow ProtectionSeverity = iota
	ProtectionMedium
	ProtectionHigh
	ProtectionCritical
)

// ProtectionStats holds protection statistics
type ProtectionStats struct {
	QueriesChecked    atomic.Uint64
	QueriesBlocked    atomic.Uint64
	QueriesFlagged    atomic.Uint64
	PatternsDetected  atomic.Uint64
	ViolationsByType  map[string]uint64
	statsMu           sync.RWMutex
}

// NewSQLProtector creates a new SQL protector
func NewSQLProtector(config *SQLProtectionConfig) *SQLProtector {
	if config == nil {
		config = DefaultSQLProtectionConfig()
	}

	sp := &SQLProtector{
		config:    config,
		patterns:  compilePatterns(),
		whitelist: make(map[string]bool),
	}
	sp.stats.ViolationsByType = make(map[string]uint64)
	return sp
}

// CheckSQL checks SQL for injection attempts
func (sp *SQLProtector) CheckSQL(sql string) *CheckResult {
	sp.stats.QueriesChecked.Add(1)

	result := &CheckResult{
		SQL:       sql,
		Allowed:   true,
		Violations: []Violation{},
	}

	if !sp.config.Enabled {
		return result
	}

	// Check whitelist
	if sp.isWhitelisted(sql) {
		return result
	}

	// Check query length
	if len(sql) > sp.config.MaxQueryLength {
		result.Violations = append(result.Violations, Violation{
			Type:        "query_too_long",
			Severity:    ProtectionHigh,
			Description: "Query exceeds maximum length",
		})
	}

	sqlUpper := strings.ToUpper(sql)

	// Check for suspicious patterns
	for _, pattern := range sp.patterns {
		if pattern.Pattern.MatchString(sql) {
			result.Violations = append(result.Violations, Violation{
				Type:        pattern.Name,
				Severity:    pattern.Severity,
				Description: pattern.Description,
				Pattern:     pattern.Pattern.String(),
			})
			sp.stats.PatternsDetected.Add(1)
		}
	}

	// Check OR condition count
	orCount := strings.Count(sqlUpper, " OR ")
	if orCount > sp.config.MaxORConditions {
		result.Violations = append(result.Violations, Violation{
			Type:        "too_many_or_conditions",
			Severity:    ProtectionMedium,
			Description: "Query contains too many OR conditions",
		})
	}

	// Check UNION count
	unionCount := strings.Count(sqlUpper, "UNION")
	if unionCount > sp.config.MaxUNIONCount {
		result.Violations = append(result.Violations, Violation{
			Type:        "too_many_unions",
			Severity:    ProtectionMedium,
			Description: "Query contains too many UNION statements",
		})
	}

	// Check comment injection
	if hasSuspiciousComments(sql) {
		result.Violations = append(result.Violations, Violation{
			Type:        "suspicious_comments",
			Severity:    ProtectionHigh,
			Description: "Query contains suspicious comment patterns",
		})
	}

	// Determine if blocked
	if len(result.Violations) > 0 {
		sp.stats.QueriesFlagged.Add(1)
		sp.recordViolations(result.Violations)

		criticalCount := 0
		highCount := 0
		for _, v := range result.Violations {
			if v.Severity == ProtectionCritical {
				criticalCount++
			}
			if v.Severity == ProtectionHigh {
				highCount++
			}
		}

		// Block if critical, or multiple high, or threshold reached
		if sp.config.BlockOnDetection {
			if criticalCount > 0 || highCount >= 2 || len(result.Violations) >= sp.config.SuspiciousThreshold {
				result.Allowed = false
				result.Blocked = true
				sp.stats.QueriesBlocked.Add(1)
			}
		}
	}

	return result
}

// AddWhitelist adds a pattern to whitelist
func (sp *SQLProtector) AddWhitelist(pattern string) {
	sp.wlMu.Lock()
	defer sp.wlMu.Unlock()
	sp.whitelist[pattern] = true
}

// isWhitelisted checks if SQL is whitelisted
func (sp *SQLProtector) isWhitelisted(sql string) bool {
	sp.wlMu.RLock()
	defer sp.wlMu.RUnlock()
	return sp.whitelist[sql]
}

// recordViolations records violation statistics
func (sp *SQLProtector) recordViolations(violations []Violation) {
	sp.stats.statsMu.Lock()
	defer sp.stats.statsMu.Unlock()

	if sp.stats.ViolationsByType == nil {
		sp.stats.ViolationsByType = make(map[string]uint64)
	}

	for _, v := range violations {
		sp.stats.ViolationsByType[v.Type]++
	}
}

// GetStats returns protection statistics
func (sp *SQLProtector) GetStats() ProtectionStatsInfo {
	sp.stats.statsMu.RLock()
	defer sp.stats.statsMu.RUnlock()

	// Copy violations map
	violations := make(map[string]uint64)
	for k, v := range sp.stats.ViolationsByType {
		violations[k] = v
	}

	return ProtectionStatsInfo{
		QueriesChecked:   sp.stats.QueriesChecked.Load(),
		QueriesBlocked:   sp.stats.QueriesBlocked.Load(),
		QueriesFlagged:   sp.stats.QueriesFlagged.Load(),
		PatternsDetected: sp.stats.PatternsDetected.Load(),
		ViolationsByType: violations,
	}
}

// CheckResult holds the check result
type CheckResult struct {
	SQL        string
	Allowed    bool
	Blocked    bool
	Violations []Violation
}

// Violation represents a security violation
type Violation struct {
	Type        string
	Severity    ProtectionSeverity
	Description string
	Pattern     string
}

// ProtectionStatsInfo holds protection statistics
type ProtectionStatsInfo struct {
	QueriesChecked   uint64            `json:"queries_checked"`
	QueriesBlocked   uint64            `json:"queries_blocked"`
	QueriesFlagged   uint64            `json:"queries_flagged"`
	PatternsDetected uint64            `json:"patterns_detected"`
	ViolationsByType map[string]uint64 `json:"violations_by_type"`
}

// compilePatterns compiles suspicious SQL patterns
func compilePatterns() []*SuspiciousPattern {
	return []*SuspiciousPattern{
		{
			Name:        "union_based_injection",
			Pattern:     regexp.MustCompile(`(?i)UNION\s+SELECT`),
			Severity:    ProtectionHigh,
			Description: "UNION-based SQL injection attempt",
		},
		{
			Name:        "error_based_injection",
			Pattern:     regexp.MustCompile(`(?i)'\s*OR\s*'1'\s*=\s*'1`),
			Severity:    ProtectionCritical,
			Description: "Error-based SQL injection attempt",
		},
		{
			Name:        "time_based_blind",
			Pattern:     regexp.MustCompile(`(?i)SLEEP\s*\(|BENCHMARK\s*\(`),
			Severity:    ProtectionHigh,
			Description: "Time-based blind SQL injection",
		},
		{
			Name:        "stacked_queries",
			Pattern:     regexp.MustCompile(`(?i);\s*(SELECT|INSERT|UPDATE|DELETE|DROP)`),
			Severity:    ProtectionCritical,
			Description: "Stacked query attack",
		},
		{
			Name:        "comment_injection",
			Pattern:     regexp.MustCompile(`(?i)/\*!\d+\s*|` + "`" + `.*?` + "`" + `|/\*.*?\*/`),
			Severity:    ProtectionMedium,
			Description: "Comment-based injection",
		},
		{
			Name:        "hex_encoding",
			Pattern:     regexp.MustCompile(`(?i)0x[0-9a-f]+`),
			Severity:    ProtectionLow,
			Description: "Hex-encoded values",
		},
		{
			Name:        "char_encoding",
			Pattern:     regexp.MustCompile(`(?i)CHAR\s*\(\s*\d+`),
			Severity:    ProtectionMedium,
			Description: "CHAR() encoding",
		},
		{
			Name:        "information_schema",
			Pattern:     regexp.MustCompile(`(?i)INFORMATION_SCHEMA\.(TABLES|COLUMNS)`),
			Severity:    ProtectionMedium,
			Description: "Information schema enumeration",
		},
		{
			Name:        "xp_cmdshell",
			Pattern:     regexp.MustCompile(`(?i)XP_CMDSHELL|EXEC\s*\(\s*\'`),
			Severity:    ProtectionCritical,
			Description: "Command execution attempt",
		},
		{
			Name:        "boolean_blind",
			Pattern:     regexp.MustCompile(`(?i)'\s*AND\s*\d+\s*=\s*\d+`),
			Severity:    ProtectionMedium,
			Description: "Boolean-based blind injection",
		},
		{
			Name:        "conditional_blind",
			Pattern:     regexp.MustCompile(`(?i)IF\s*\(\s*\(SELECT|CASE\s+WHEN\s+\(SELECT`),
			Severity:    ProtectionHigh,
			Description: "Conditional blind SQL injection (IF/CASE with subquery)",
		},
		{
			Name:        "outofband_exfil",
			Pattern:     regexp.MustCompile(`(?i)LOAD_FILE\s*\(|INTO\s+OUTFILE|INTO\s+DUMPFILE`),
			Severity:    ProtectionCritical,
			Description: "Out-of-band data exfiltration attempt",
		},
		{
			Name:        "system_function_abuse",
			Pattern:     regexp.MustCompile(`(?i)LOAD\s+DATA|SYSTEM_USER\s*\(\)|SESSION_USER\s*\(\)`),
			Severity:    ProtectionHigh,
			Description: "System function abuse attempt",
		},
		{
			Name:        "double_encoding",
			Pattern:     regexp.MustCompile(`(?i)%27|%22|%3B|%2D%2D`),
			Severity:    ProtectionMedium,
			Description: "URL-encoded SQL injection characters",
		},
		{
			Name:        "or_always_true",
			Pattern:     regexp.MustCompile(`(?i)OR\s+\d+\s*=\s*\d+\s*--|OR\s+''='`),
			Severity:    ProtectionCritical,
			Description: "OR-based always-true condition",
		},
	}
}

// Pre-compiled regexes for SanitizeSQL (avoid recompiling per call)
var (
	sanitizeSingleQuoteRe = regexp.MustCompile(`'[^']*'`)
	sanitizeDoubleQuoteRe = regexp.MustCompile(`"[^"]*"`)
)

// hasSuspiciousComments checks for suspicious comment patterns
func hasSuspiciousComments(sql string) bool {
	// Check for nested comments
	if strings.Count(sql, "/*") != strings.Count(sql, "*/") {
		return true
	}

	// Check for MySQL conditional comments
	if strings.Contains(sql, "/*!") {
		return true
	}

	return false
}

// SanitizeSQL sanitizes SQL for logging (removes sensitive data)
func SanitizeSQL(sql string) string {
	// Remove quoted strings (may contain sensitive data)
	sanitized := sanitizeSingleQuoteRe.ReplaceAllString(sql, "'?'")
	sanitized = sanitizeDoubleQuoteRe.ReplaceAllString(sanitized, "\"?\"")

	// Limit length
	if len(sanitized) > 500 {
		sanitized = sanitized[:500] + "..."
	}

	return sanitized
}
