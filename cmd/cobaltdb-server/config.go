package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// configFileValues holds values parsed from the optional config file.
// CLI flags and environment variables override these.
type configFileValues struct {
	Address       string
	MySQLAddr     string
	DataDir       string
	CacheSize     int
	AuthEnabled   *bool
	TLSEnabled    *bool
	TLSCertFile   string
	TLSKeyFile    string
	MySQLEnabled  *bool
	MaxConns      int
	ReadTimeout   int // seconds
	WriteTimeout  int // seconds
	HealthAddr    string
}

// loadConfigFile parses a CobaltDB configuration file (INI-style) and returns
// the values found. A missing or empty file is not an error. Unknown keys are
// silently skipped for forward compatibility.
//
// The config format is:
//
//	[section]
//	key = "value"    # or unquoted: true, false, 123, duration
//
// Callers apply the returned values to their flag vars, then CLI flags
// override them via flag.Parse().
func loadConfigFile(path string) (*configFileValues, error) {
	if path == "" {
		return &configFileValues{}, nil
	}
	clean := filepath.Clean(path)
	f, err := os.Open(clean)
	if err != nil {
		if os.IsNotExist(err) {
			return &configFileValues{}, nil // optional
		}
		return nil, fmt.Errorf("failed to open config file %s: %w", clean, err)
	}
	defer f.Close()

	type entry struct{ section, key string }
	vals := make(map[entry]string)

	section := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		k := strings.TrimSpace(line[:eq])
		v := strings.TrimSpace(line[eq+1:])
		if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		}
		if k == "" {
			continue
		}
		vals[entry{section, k}] = v
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("error reading config file %s: %w", clean, err)
	}

	cv := &configFileValues{}

	if v, ok := vals[entry{"server", "max_connections"}]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cv.MaxConns = n
		}
	}
	if v, ok := vals[entry{"server", "connection_timeout"}]; ok {
		if d, err := time.ParseDuration(v); err == nil {
			cv.ReadTimeout = int(d.Seconds())
		}
	}
	if v, ok := vals[entry{"server", "query_timeout"}]; ok {
		if d, err := time.ParseDuration(v); err == nil {
			cv.WriteTimeout = int(d.Seconds())
		}
	}

	// TLS
	if v, ok := vals[entry{"server", "tls_enabled"}]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			cv.TLSEnabled = &b
		}
	}
	if v, ok := vals[entry{"server", "tls_cert_file"}]; ok {
		cv.TLSCertFile = v
	}
	if v, ok := vals[entry{"server", "tls_key_file"}]; ok {
		cv.TLSKeyFile = v
	}

	// MySQL
	if v, ok := vals[entry{"mysql", "mysql_enabled"}]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			cv.MySQLEnabled = &b
		}
	}

	// Storage
	if v, ok := vals[entry{"storage", "data_dir"}]; ok {
		cv.DataDir = v
	}
	if v, ok := vals[entry{"storage", "cache_size"}]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cv.CacheSize = n
		}
	}

	// Security
	if v, ok := vals[entry{"security", "auth_enabled"}]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			cv.AuthEnabled = &b
		}
	}

	// Monitoring
	if v, ok := vals[entry{"monitoring", "metrics_port"}]; ok {
		cv.HealthAddr = "127.0.0.1:" + v
	}

	// Compose addresses from host+port
	if h, ok := vals[entry{"server", "host"}]; ok {
		if p, ok2 := vals[entry{"server", "port"}]; ok2 {
			cv.Address = netJoinHostPort(h, p)
		}
	}
	if h, ok := vals[entry{"mysql", "mysql_host"}]; ok {
		if p, ok2 := vals[entry{"mysql", "mysql_port"}]; ok2 {
			cv.MySQLAddr = netJoinHostPort(h, p)
		}
	}

	return cv, nil
}

// netJoinHostPort joins host and port into "host:port", wrapping IPv6
// addresses in brackets as needed.
func netJoinHostPort(host, port string) string {
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}
	return host + ":" + port
}
