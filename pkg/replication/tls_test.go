package replication

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type replicationTLSFixture struct {
	caCert          string
	serverCert      string
	serverKey       string
	clientCert      string
	clientKey       string
	wrongCA         string
	wrongClientCert string
	wrongClientKey  string
}

func newReplicationTLSFixture(t *testing.T, dnsNames []string, ipAddresses []net.IP) replicationTLSFixture {
	t.Helper()
	dir := t.TempDir()
	caCert, caKey, caDER := makeReplicationCA(t, dir, "ca")
	wrongCA, wrongCAKey, wrongCADER := makeReplicationCA(t, dir, "wrong-ca")
	serverCert, serverKey := makeReplicationLeaf(t, dir, "server", caDER, caKey, false, dnsNames, ipAddresses)
	clientCert, clientKey := makeReplicationLeaf(t, dir, "client", caDER, caKey, true, nil, nil)
	wrongClientCert, wrongClientKey := makeReplicationLeaf(t, dir, "wrong-client", wrongCADER, wrongCAKey, true, nil, nil)
	return replicationTLSFixture{
		caCert: caCert, serverCert: serverCert, serverKey: serverKey,
		clientCert: clientCert, clientKey: clientKey, wrongCA: wrongCA,
		wrongClientCert: wrongClientCert, wrongClientKey: wrongClientKey,
	}
}

func makeReplicationCA(t *testing.T, dir, name string) (string, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, name+".pem")
	writePEMFile(t, certPath, "CERTIFICATE", der, 0600)
	return certPath, key, der
}

func makeReplicationLeaf(t *testing.T, dir, name string, caDER []byte, caKey *ecdsa.PrivateKey, client bool, dnsNames []string, ipAddresses []net.IP) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	extUsage := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	if client {
		extUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: extUsage,
		DNSNames:    dnsNames, IPAddresses: ipAddresses,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(dir, name+"-cert.pem")
	keyPath := filepath.Join(dir, name+"-key.pem")
	writePEMFile(t, certPath, "CERTIFICATE", der, 0600)
	writePEMFile(t, keyPath, "PRIVATE KEY", keyDER, 0600)
	return certPath, keyPath
}

func writePEMFile(t *testing.T, path, blockType string, data []byte, mode os.FileMode) {
	t.Helper()
	encoded := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: data})
	if err := os.WriteFile(path, encoded, mode); err != nil {
		t.Fatal(err)
	}
}

func startTLSReplicationMaster(t *testing.T, fixture replicationTLSFixture, mtls bool, token string) *Manager {
	t.Helper()
	config := DefaultConfig()
	config.Role = RoleMaster
	config.ListenAddr = "127.0.0.1:0"
	config.AuthToken = token
	config.SSLCert = fixture.serverCert
	config.SSLKey = fixture.serverKey
	if mtls {
		config.SSLCA = fixture.caCert
	}
	master := NewManager(config)
	if err := master.Start(); err != nil {
		t.Fatalf("start TLS master: %v", err)
	}
	t.Cleanup(func() { _ = master.Stop() })
	return master
}

func TestReplicationTLSCertificateAndHostnameVerification(t *testing.T) {
	fixture := newReplicationTLSFixture(t, []string{"replication.test"}, nil)
	master := startTLSReplicationMaster(t, fixture, false, "token")

	slaveConfig := DefaultConfig()
	slaveConfig.Role = RoleSlave
	slaveConfig.MasterAddr = master.listener.Addr().String()
	slaveConfig.AuthToken = "token"
	slaveConfig.SSLCA = fixture.caCert
	slaveConfig.SSLServerName = "replication.test"
	slave := NewManager(slaveConfig)
	if err := slave.Start(); err != nil {
		t.Fatalf("trusted TLS slave failed: %v", err)
	}
	defer slave.Stop()
	waitForActiveSlaves(t, master, 1)

	wrongName := *slaveConfig
	wrongName.SSLServerName = "wrong.test"
	if err := NewManager(&wrongName).Start(); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("hostname mismatch error = %v", err)
	}

	wrongCA := *slaveConfig
	wrongCA.SSLCA = fixture.wrongCA
	if err := NewManager(&wrongCA).Start(); err == nil {
		t.Fatal("untrusted server certificate was accepted")
	}
}

func TestReplicationTLSVerifiesIPAddressSAN(t *testing.T) {
	fixture := newReplicationTLSFixture(t, nil, []net.IP{net.ParseIP("127.0.0.1")})
	master := startTLSReplicationMaster(t, fixture, false, "token")
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = master.listener.Addr().String()
	config.AuthToken = "token"
	config.SSLCA = fixture.caCert
	slave := NewManager(config)
	if err := slave.Start(); err != nil {
		t.Fatalf("IP SAN TLS connection failed: %v", err)
	}
	defer slave.Stop()
	waitForActiveSlaves(t, master, 1)
}

func TestReplicationMutualTLSClientAuthentication(t *testing.T) {
	fixture := newReplicationTLSFixture(t, []string{"replication.test"}, nil)
	master := startTLSReplicationMaster(t, fixture, true, "")

	valid := DefaultConfig()
	valid.Role = RoleSlave
	valid.MasterAddr = master.listener.Addr().String()
	valid.SSLCA = fixture.caCert
	valid.SSLServerName = "replication.test"
	valid.SSLCert = fixture.clientCert
	valid.SSLKey = fixture.clientKey
	slave := NewManager(valid)
	if err := slave.Start(); err != nil {
		t.Fatalf("valid mTLS client failed: %v", err)
	}
	defer slave.Stop()
	waitForActiveSlaves(t, master, 1)

	missing := *valid
	missing.SSLCert, missing.SSLKey = "", ""
	if err := NewManager(&missing).Start(); err == nil {
		t.Fatal("mTLS server accepted missing client certificate")
	}

	wrong := *valid
	wrong.SSLCert, wrong.SSLKey = fixture.wrongClientCert, fixture.wrongClientKey
	if err := NewManager(&wrong).Start(); err == nil {
		t.Fatal("mTLS server accepted client certificate from wrong CA")
	}
}

func TestReplicationTLSAndTokenRequireBoth(t *testing.T) {
	fixture := newReplicationTLSFixture(t, []string{"replication.test"}, nil)
	master := startTLSReplicationMaster(t, fixture, true, "correct")
	config := DefaultConfig()
	config.Role = RoleSlave
	config.MasterAddr = master.listener.Addr().String()
	config.AuthToken = "wrong"
	config.SSLCA = fixture.caCert
	config.SSLServerName = "replication.test"
	config.SSLCert = fixture.clientCert
	config.SSLKey = fixture.clientKey
	if err := NewManager(config).Start(); err == nil || !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("correct certificate with wrong token error = %v", err)
	}
}

func TestReplicationRejectsPlaintextAndNeverDowngrades(t *testing.T) {
	t.Run("non-loopback listener", func(t *testing.T) {
		config := DefaultConfig()
		config.Role = RoleMaster
		config.ListenAddr = "0.0.0.0:0"
		config.AuthToken = "token"
		if err := NewManager(config).Start(); err == nil || !strings.Contains(err.Error(), "requires TLS") {
			t.Fatalf("non-loopback plaintext listener error = %v", err)
		}
	})

	t.Run("non-loopback master", func(t *testing.T) {
		config := DefaultConfig()
		config.Role = RoleSlave
		config.MasterAddr = "192.0.2.10:9000"
		config.AuthToken = "token"
		if err := NewManager(config).Start(); err == nil || !strings.Contains(err.Error(), "requires TLS") {
			t.Fatalf("non-loopback plaintext master error = %v", err)
		}
	})

	t.Run("plaintext client to TLS listener", func(t *testing.T) {
		fixture := newReplicationTLSFixture(t, []string{"replication.test"}, nil)
		master := startTLSReplicationMaster(t, fixture, false, "token")
		conn, err := net.DialTimeout("tcp", master.listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		_, _ = conn.Write([]byte("token\nRESUME 0\n"))
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err == nil {
			t.Fatal("plaintext client received replication response from TLS listener")
		}
		if got := atomic.LoadInt32(&master.metrics.ActiveSlaves); got != 0 {
			t.Fatalf("plaintext client became active slave: %d", got)
		}
	})

	t.Run("TLS client to plaintext listener", func(t *testing.T) {
		fixture := newReplicationTLSFixture(t, []string{"replication.test"}, nil)
		plainConfig := DefaultConfig()
		plainConfig.Role = RoleMaster
		plainConfig.ListenAddr = "127.0.0.1:0"
		plainConfig.AuthToken = "token"
		plain := NewManager(plainConfig)
		if err := plain.Start(); err != nil {
			t.Fatal(err)
		}
		defer plain.Stop()

		slaveConfig := DefaultConfig()
		slaveConfig.Role = RoleSlave
		slaveConfig.MasterAddr = plain.listener.Addr().String()
		slaveConfig.AuthToken = "token"
		slaveConfig.SSLCA = fixture.caCert
		slaveConfig.SSLServerName = "replication.test"
		if err := NewManager(slaveConfig).Start(); err == nil {
			t.Fatal("TLS client downgraded to plaintext master")
		}
	})
}

func TestReplicationTLSConfigValidation(t *testing.T) {
	cases := []struct {
		name   string
		config *Config
		want   string
	}{
		{"partial key pair", &Config{Role: RoleMaster, ListenAddr: "127.0.0.1:0", SSLCert: "cert"}, "together"},
		{"master CA without certificate", &Config{Role: RoleMaster, ListenAddr: "127.0.0.1:0", SSLCA: "ca"}, "requires a certificate"},
		{"slave listen address", &Config{Role: RoleSlave, MasterAddr: "127.0.0.1:1", ListenAddr: "127.0.0.1:2"}, "must be empty"},
		{"invalid master address", &Config{Role: RoleSlave, MasterAddr: "bad"}, "invalid replication master address"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateConfig(tc.config); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateConfig error = %v, want %q", err, tc.want)
			}
		})
	}
}
