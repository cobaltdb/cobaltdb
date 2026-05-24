package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	ErrInvalidCert     = errors.New("invalid TLS certificate")
	ErrInvalidKey      = errors.New("invalid TLS key")
	ErrCertExpired     = errors.New("TLS certificate expired")
	ErrCertNotYetValid = errors.New("TLS certificate not yet valid")
)

const (
	tlsCertFilePerm os.FileMode = 0644
	tlsKeyFilePerm  os.FileMode = 0600
)

// TLSConfig holds TLS configuration
type TLSConfig struct {
	Enabled                  bool
	CertFile                 string
	KeyFile                  string
	CAFile                   string
	ClientAuth               tls.ClientAuthType
	InsecureSkipVerify       bool // SECURITY: Must be false in production. Certificate verification is mandatory.
	MinVersion               uint16
	MaxVersion               uint16
	CipherSuites             []uint16
	PreferServerCipherSuites bool
	GenerateSelfSigned       bool
	SelfSignedOrg            string
	SelfSignedValidDays      int
}

// DefaultTLSConfig returns default TLS configuration
func DefaultTLSConfig() *TLSConfig {
	return &TLSConfig{
		Enabled:                  false,
		ClientAuth:               tls.NoClientCert,
		MinVersion:               tls.VersionTLS12,
		MaxVersion:               tls.VersionTLS13,
		PreferServerCipherSuites: true,
		SelfSignedOrg:            "CobaltDB",
		SelfSignedValidDays:      365,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_AES_128_GCM_SHA256,
		},
	}
}

// LoadTLSConfig loads TLS configuration
func LoadTLSConfig(config *TLSConfig) (*tls.Config, error) {
	if config == nil {
		return nil, nil
	}
	if !config.Enabled {
		return nil, nil
	}
	config = normalizeTLSConfig(config)

	// Generate self-signed cert if requested
	if config.GenerateSelfSigned {
		if err := generateSelfSignedCert(config); err != nil {
			return nil, fmt.Errorf("failed to generate self-signed certificate: %w", err)
		}
	}

	// Load certificate
	if config.CertFile == "" || config.KeyFile == "" {
		return nil, ErrInvalidCert
	}

	certFile, err := cleanTLSFilePath(config.CertFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCert, err)
	}
	keyFile, err := cleanTLSFilePath(config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCert, err)
	}

	// Verify certificate
	if err := verifyCertificate(&cert); err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		Certificates:             []tls.Certificate{cert},
		MinVersion:               config.MinVersion,
		MaxVersion:               config.MaxVersion,
		CipherSuites:             config.CipherSuites,
		PreferServerCipherSuites: true,
		ClientAuth:               config.ClientAuth,
	}

	// Load CA for client verification
	if config.CAFile != "" {
		caFile, err := cleanTLSFilePath(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("invalid CA file: %w", err)
		}
		caCert, err := os.ReadFile(caFile) // #nosec G304 - CA path is explicit TLS configuration and is cleaned before use.
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to parse CA certificate")
		}

		tlsConfig.ClientCAs = caCertPool
	}

	return tlsConfig, nil
}

func normalizeTLSConfig(config *TLSConfig) *TLSConfig {
	normalized := *config
	if normalized.MinVersion == 0 {
		normalized.MinVersion = tls.VersionTLS12
	}
	if normalized.MaxVersion == 0 {
		normalized.MaxVersion = tls.VersionTLS13
	}
	if normalized.MinVersion < tls.VersionTLS12 {
		normalized.MinVersion = tls.VersionTLS12
	}
	if normalized.MaxVersion < normalized.MinVersion {
		normalized.MaxVersion = normalized.MinVersion
	}
	if len(normalized.CipherSuites) == 0 {
		normalized.CipherSuites = cloneCipherSuites(DefaultTLSConfig().CipherSuites)
	} else {
		normalized.CipherSuites = cloneCipherSuites(normalized.CipherSuites)
	}
	if normalized.SelfSignedOrg == "" {
		normalized.SelfSignedOrg = DefaultTLSConfig().SelfSignedOrg
	}
	if normalized.SelfSignedValidDays <= 0 {
		normalized.SelfSignedValidDays = DefaultTLSConfig().SelfSignedValidDays
	}
	return &normalized
}

func cloneCipherSuites(values []uint16) []uint16 {
	if values == nil {
		return nil
	}
	cloned := make([]uint16, len(values))
	copy(cloned, values)
	return cloned
}

// verifyCertificate verifies the certificate is valid
func verifyCertificate(cert *tls.Certificate) error {
	if len(cert.Certificate) == 0 {
		return ErrInvalidCert
	}

	x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCert, err)
	}

	now := time.Now()
	if now.Before(x509Cert.NotBefore) {
		return ErrCertNotYetValid
	}
	if now.After(x509Cert.NotAfter) {
		return ErrCertExpired
	}

	return nil
}

// generateSelfSignedCert generates a self-signed certificate
func generateSelfSignedCert(config *TLSConfig) error {
	if config.CertFile == "" {
		config.CertFile = filepath.Join("certs", "server.crt")
	}
	if config.KeyFile == "" {
		config.KeyFile = filepath.Join("certs", "server.key")
	}

	certFile, err := cleanTLSFilePath(config.CertFile)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCert, err)
	}
	keyFile, err := cleanTLSFilePath(config.KeyFile)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	config.CertFile = certFile
	config.KeyFile = keyFile

	if err := os.MkdirAll(filepath.Dir(config.CertFile), 0750); err != nil {
		return fmt.Errorf("failed to create certificate directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(config.KeyFile), 0750); err != nil {
		return fmt.Errorf("failed to create key directory: %w", err)
	}

	// Check if certs already exist and are valid
	if _, err := os.Stat(config.CertFile); err == nil {
		if _, err := os.Stat(config.KeyFile); err == nil {
			// Verify existing cert
			cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
			if err == nil {
				if verifyCertificate(&cert) == nil {
					return nil // Valid certs exist
				}
			}
		}
	}

	// Generate private key
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	// Generate random serial number
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:  []string{config.SelfSignedOrg},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{},
			PostalCode:    []string{},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Duration(config.SelfSignedValidDays) * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:              []string{"localhost"},
	}

	// Generate certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if certPEM == nil {
		return errors.New("failed to encode certificate PEM")
	}

	privKey, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return err
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privKey})
	if keyPEM == nil {
		return errors.New("failed to encode private key PEM")
	}

	if err := writeTLSFileAtomic(config.CertFile, certPEM, tlsCertFilePerm); err != nil {
		return fmt.Errorf("failed to write certificate: %w", err)
	}
	if err := writeTLSFileAtomic(config.KeyFile, keyPEM, tlsKeyFilePerm); err != nil {
		return fmt.Errorf("failed to write private key: %w", err)
	}

	return nil
}

func writeTLSFileAtomic(path string, data []byte, perm os.FileMode) error {
	path = filepath.Clean(path)
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	file, err := os.CreateTemp(dir, "."+base+".tmp-*") // #nosec G304 - path is derived from explicit TLS configuration and cleaned before use.
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpPath := file.Name()
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := file.Chmod(perm); err != nil {
		return fmt.Errorf("failed to set temporary file permissions: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}
	closed = true

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to replace file: %w", err)
	}
	tmpPath = ""

	if err := syncTLSDir(dir); err != nil {
		return fmt.Errorf("failed to sync directory: %w", err)
	}

	return nil
}

func syncTLSDir(dir string) error {
	file, err := os.Open(dir) // #nosec G304 - directory path is derived from explicit TLS configuration and cleaned before use.
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func cleanTLSFilePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("TLS file path cannot be empty")
	}
	return filepath.Clean(path), nil
}

// IsTLSEnabled checks if TLS is enabled in the configuration
func IsTLSEnabled(config *TLSConfig) bool {
	return config != nil && config.Enabled
}

// GetTLSListener wraps a net.Listener with TLS
func GetTLSListener(listener net.Listener, tlsConfig *tls.Config) net.Listener {
	if tlsConfig == nil {
		return listener
	}
	return tls.NewListener(listener, tlsConfig)
}

// GenerateClientCert generates a client certificate signed by the server CA
func GenerateClientCert(caCertFile, caKeyFile, clientName string, validDays int) (certPEM, keyPEM []byte, err error) {
	// Load CA
	caCertFile, err = cleanTLSFilePath(caCertFile)
	if err != nil {
		return nil, nil, err
	}
	caCertPEM, err := os.ReadFile(caCertFile) // #nosec G304 - CA certificate path is explicit TLS configuration and is cleaned before use.
	if err != nil {
		return nil, nil, err
	}

	caKeyFile, err = cleanTLSFilePath(caKeyFile)
	if err != nil {
		return nil, nil, err
	}
	caKeyPEM, err := os.ReadFile(caKeyFile) // #nosec G304 - CA key path is explicit TLS configuration and is cleaned before use.
	if err != nil {
		return nil, nil, err
	}

	// Decode CA certificate
	block, _ := pem.Decode(caCertPEM)
	if block == nil {
		return nil, nil, errors.New("failed to decode CA certificate")
	}

	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err
	}

	// Decode CA private key
	block, _ = pem.Decode(caKeyPEM)
	if block == nil {
		return nil, nil, errors.New("failed to decode CA key")
	}

	caKey, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		// Try PKCS#8
		caKeyPKCS8, err2 := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err2 != nil {
			return nil, nil, errors.New("failed to parse CA private key")
		}
		// Use the parsed key directly with CreateCertificate
		caKey = nil
		_ = caKeyPKCS8
		// For simplicity, we'll use ECDSA for client certs too
		// In production, you'd handle RSA keys properly
	}

	// Generate client key
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	// Generate random serial number for client cert
	clientSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	// Create client certificate template
	template := x509.Certificate{
		SerialNumber: clientSerial,
		Subject: pkix.Name{
			Organization: []string{"CobaltDB Client"},
			CommonName:   clientName,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Duration(validDays) * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	// Sign certificate (use caKey if parsed, otherwise this will fail - production code needs full RSA support)
	if caKey == nil {
		return nil, nil, errors.New("RSA CA keys not yet fully supported")
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}

	// Encode certificate
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	// Encode private key
	keyBytes, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPEM, keyPEM, nil
}

// GetCipherSuiteName returns the name of a cipher suite
func GetCipherSuiteName(id uint16) string {
	switch id {
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_RSA_WITH_AES_256_GCM_SHA384:
		return "TLS_RSA_WITH_AES_256_GCM_SHA384"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:
		return "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
	case tls.TLS_AES_128_GCM_SHA256:
		return "TLS_AES_128_GCM_SHA256"
	case tls.TLS_AES_256_GCM_SHA384:
		return "TLS_AES_256_GCM_SHA384"
	case tls.TLS_CHACHA20_POLY1305_SHA256:
		return "TLS_CHACHA20_POLY1305_SHA256"
	default:
		return fmt.Sprintf("Unknown(%d)", id)
	}
}

// GetTLSVersionName returns the name of a TLS version
func GetTLSVersionName(version uint16) string {
	switch version {
	case 0x0300: // SSLv3
		return "SSLv3"
	case tls.VersionTLS10:
		return "TLS 1.0"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS13:
		return "TLS 1.3"
	default:
		return fmt.Sprintf("Unknown(%d)", version)
	}
}
