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
	"time"
)

var (
	ErrInvalidCert     = errors.New("invalid TLS certificate")
	ErrInvalidKey      = errors.New("invalid TLS key")
	ErrCertExpired     = errors.New("TLS certificate expired")
	ErrCertNotYetValid = errors.New("TLS certificate not yet valid")
)

// TLSConfig holds TLS configuration
type TLSConfig struct {
	Enabled                  bool
	CertFile                 string
	KeyFile                  string
	CAFile                   string
	ClientAuth               tls.ClientAuthType
	InsecureSkipVerify       bool
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
	if !config.Enabled {
		return nil, nil
	}

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

	cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCert, err)
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
		PreferServerCipherSuites: config.PreferServerCipherSuites,
		ClientAuth:               config.ClientAuth,
	}

	// Load CA for client verification
	if config.CAFile != "" {
		caCert, err := os.ReadFile(config.CAFile)
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

// verifyCertificate verifies the certificate is valid
func verifyCertificate(cert *tls.Certificate) error {
	if len(cert.Certificate) == 0 {
		return ErrInvalidCert
	}

	x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCert, err)
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
	// Create certs directory if it doesn't exist
	certDir := "certs"
	if err := os.MkdirAll(certDir, 0750); err != nil {
		return err
	}

	config.CertFile = filepath.Join(certDir, "server.crt")
	config.KeyFile = filepath.Join(certDir, "server.key")

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

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
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

	// Write certificate
	certOut, err := os.Create(config.CertFile)
	if err != nil {
		return err
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return err
	}

	// Write private key
	keyOut, err := os.OpenFile(config.KeyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer keyOut.Close()

	privKey, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return err
	}

	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privKey}); err != nil {
		return err
	}

	return nil
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
	caCertPEM, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, nil, err
	}

	caKeyPEM, err := os.ReadFile(caKeyFile)
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

	// Create client certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(2),
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
	case tls.VersionSSL30:
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
