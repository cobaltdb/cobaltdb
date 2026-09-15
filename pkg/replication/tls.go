package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const maxReplicationTLSFileBytes int64 = 1 << 20 // 1 MiB

// ValidateConfig checks role, address, authentication, and TLS invariants
// without opening sockets. TLS files are loaded when replication starts.
func ValidateConfig(config *Config) error {
	if config == nil {
		return errors.New("replication config is required")
	}
	if (config.SSLCert == "") != (config.SSLKey == "") {
		return errors.New("replication TLS certificate and key must be configured together")
	}

	switch config.Role {
	case RoleStandalone:
		if replicationTLSConfigured(config) {
			return errors.New("replication TLS settings require master or slave role")
		}
		return nil
	case RoleMaster:
		return validateMasterConfig(config)
	case RoleSlave:
		return validateSlaveConfig(config)
	default:
		return fmt.Errorf("invalid replication role %d", config.Role)
	}
}

func validateMasterConfig(config *Config) error {
	if strings.TrimSpace(config.ListenAddr) == "" {
		return errors.New("master replication listen address is required")
	}
	if _, _, err := net.SplitHostPort(config.ListenAddr); err != nil {
		return fmt.Errorf("invalid replication listen address %q: %w", config.ListenAddr, err)
	}
	if config.SSLServerName != "" {
		return errors.New("replication TLS server name is only valid for slave role")
	}

	tlsEnabled := replicationTLSConfigured(config)
	if tlsEnabled && (config.SSLCert == "" || config.SSLKey == "") {
		return errors.New("master replication TLS requires a certificate and key")
	}
	if replicationEndpointIsNonLoopback(config.ListenAddr) {
		if !tlsEnabled {
			return fmt.Errorf("non-loopback replication listener %q requires TLS", config.ListenAddr)
		}
		if strings.TrimSpace(config.AuthToken) == "" && strings.TrimSpace(config.SSLCA) == "" {
			return fmt.Errorf("non-loopback replication listener %q requires an auth token or mutual TLS", config.ListenAddr)
		}
	}
	return nil
}

func validateSlaveConfig(config *Config) error {
	if strings.TrimSpace(config.MasterAddr) == "" {
		return errors.New("slave replication master address is required")
	}
	if _, _, err := net.SplitHostPort(config.MasterAddr); err != nil {
		return fmt.Errorf("invalid replication master address %q: %w", config.MasterAddr, err)
	}
	if strings.TrimSpace(config.ListenAddr) != "" {
		return errors.New("slave replication listen address must be empty")
	}

	tlsEnabled := replicationTLSConfigured(config)
	if replicationEndpointIsNonLoopback(config.MasterAddr) {
		if !tlsEnabled {
			return fmt.Errorf("non-loopback replication master %q requires TLS", config.MasterAddr)
		}
		if strings.TrimSpace(config.AuthToken) == "" && config.SSLCert == "" {
			return fmt.Errorf("non-loopback replication master %q requires an auth token or client certificate", config.MasterAddr)
		}
	}
	return nil
}

func replicationTLSConfigured(config *Config) bool {
	if config == nil {
		return false
	}
	return config.SSLCert != "" || config.SSLKey != "" || config.SSLCA != "" || config.SSLServerName != ""
}

func replicationEndpointIsNonLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return true
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsLoopback()
}

func loadReplicationServerTLS(config *Config) (*tls.Config, error) {
	if !replicationTLSConfigured(config) {
		return nil, nil
	}
	certificate, err := loadReplicationKeyPair(config.SSLCert, config.SSLKey)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS13,
	}
	if config.SSLCA != "" {
		clientCAs, err := loadReplicationCertPool(config.SSLCA)
		if err != nil {
			return nil, fmt.Errorf("load replication client CA: %w", err)
		}
		tlsConfig.ClientCAs = clientCAs
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsConfig, nil
}

func loadReplicationClientTLS(config *Config) (*tls.Config, error) {
	if !replicationTLSConfigured(config) {
		return nil, nil
	}
	host, _, err := net.SplitHostPort(config.MasterAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid replication master address %q: %w", config.MasterAddr, err)
	}
	serverName := strings.TrimSpace(config.SSLServerName)
	if serverName == "" {
		serverName = strings.Trim(host, "[]")
	}
	if serverName == "" {
		return nil, errors.New("replication TLS server name cannot be empty")
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS13,
		ServerName: serverName,
	}
	if config.SSLCA != "" {
		roots, err := loadReplicationCertPool(config.SSLCA)
		if err != nil {
			return nil, fmt.Errorf("load replication server CA: %w", err)
		}
		tlsConfig.RootCAs = roots
	}
	if config.SSLCert != "" {
		certificate, err := loadReplicationKeyPair(config.SSLCert, config.SSLKey)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

func loadReplicationKeyPair(certPath, keyPath string) (tls.Certificate, error) {
	cleanCert, err := validateReplicationTLSFile(certPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("invalid replication TLS certificate: %w", err)
	}
	cleanKey, err := validateReplicationTLSFile(keyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("invalid replication TLS key: %w", err)
	}
	certificate, err := tls.LoadX509KeyPair(cleanCert, cleanKey) // #nosec G304 -- paths are explicit trusted replication configuration and validated as regular bounded files.
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load replication TLS key pair: %w", err)
	}
	if len(certificate.Certificate) == 0 {
		return tls.Certificate{}, errors.New("replication TLS certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse replication TLS certificate: %w", err)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) {
		return tls.Certificate{}, fmt.Errorf("replication TLS certificate is not valid before %s", leaf.NotBefore.UTC())
	}
	if now.After(leaf.NotAfter) {
		return tls.Certificate{}, fmt.Errorf("replication TLS certificate expired at %s", leaf.NotAfter.UTC())
	}
	certificate.Leaf = leaf
	return certificate, nil
}

func loadReplicationCertPool(path string) (*x509.CertPool, error) {
	clean, err := validateReplicationTLSFile(path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(clean) // #nosec G304 -- explicit trusted replication configuration path validated above.
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("replication CA file contains no valid certificates")
	}
	return pool, nil
}

func validateReplicationTLSFile(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is required")
	}
	clean := filepath.Clean(path)
	info, err := os.Lstat(clean)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("symlinks are not allowed")
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("path is not a regular file")
	}
	if info.Size() <= 0 || info.Size() > maxReplicationTLSFileBytes {
		return "", fmt.Errorf("file size %d is outside allowed range", info.Size())
	}
	return clean, nil
}

func (m *Manager) secureAcceptedConnection(conn net.Conn) (net.Conn, error) {
	m.mu.RLock()
	serverTLS := m.serverTLS
	m.mu.RUnlock()
	if serverTLS == nil {
		return conn, nil
	}
	tlsConn := tls.Server(conn, serverTLS)
	if err := handshakeReplicationTLS(conn, tlsConn); err != nil {
		return nil, err
	}
	return tlsConn, nil
}

func (m *Manager) secureMasterConnection(conn net.Conn) (net.Conn, error) {
	m.mu.RLock()
	clientTLS := m.clientTLS
	m.mu.RUnlock()
	if clientTLS == nil {
		return conn, nil
	}
	tlsConn := tls.Client(conn, clientTLS)
	if err := handshakeReplicationTLS(conn, tlsConn); err != nil {
		return nil, err
	}
	return tlsConn, nil
}

func handshakeReplicationTLS(conn net.Conn, tlsConn *tls.Conn) error {
	deadline := time.Now().Add(replicationAuthTimeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("replication TLS handshake failed: %w", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return err
	}
	return nil
}
