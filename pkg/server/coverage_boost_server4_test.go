package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/wire"
)

// TestListenOnListenerBasic tests server listening on custom listener
func TestListenOnListenerBasic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		if err := srv.ListenOnListener(listener); err != nil {
			t.Logf("ListenOnListener returned: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	pingMsg := make([]byte, 5)
	binary.LittleEndian.PutUint32(pingMsg, 1)
	pingMsg[4] = byte(wire.MsgPing)
	if _, err := conn.Write(pingMsg); err != nil {
		t.Fatalf("Failed to send ping: %v", err)
	}

	var length uint32
	if err := binary.Read(conn, binary.LittleEndian, &length); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	response := make([]byte, length)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	if response[0] != byte(wire.MsgPong) {
		t.Errorf("Expected pong, got: %d", response[0])
	}

	srv.Close()
}

// TestListenOnListenerWithTLS tests server with TLS listener
func TestListenOnListenerWithTLS(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := New(db, DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "test.crt")
	keyFile := filepath.Join(tmpDir, "test.key")

	priv, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"Test"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		DNSNames:     []string{"localhost"},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	certOut, _ := os.Create(certFile)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certOut.Close()
	keyBytes, _ := x509.MarshalECPrivateKey(priv)
	keyOut, _ := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	keyOut.Close()

	tlsConfig := &TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		MinVersion: 0x0303,
		MaxVersion: 0x0303,
	}
	conf, err := LoadTLSConfig(tlsConfig)
	if err != nil {
		t.Fatalf("Failed to load TLS config: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	tlsListener := GetTLSListener(listener, conf)

	go func() {
		if err := srv.ListenOnListener(tlsListener); err != nil {
			t.Logf("ListenOnListener returned: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	srv.Close()
}
