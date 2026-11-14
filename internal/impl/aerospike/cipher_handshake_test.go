// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The server speaks TLS 1.2 only. The Go client always offers TLS 1.3, so a
// TLS 1.2 suite mismatch would still connect unless the server refuses 1.3.
func TestCipherSuiteMismatch(t *testing.T) {
	const serverSuite = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
	const clientSuite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	caPEM, serverCert := cipherHandshakeCert(t)

	t.Run("same suite connects", func(t *testing.T) {
		cfg := clientTLSConfig(t, caPEM, serverSuite)
		require.Equal(t, []uint16{suiteNamed(t, tls.CipherSuites(), serverSuite).ID}, cfg.CipherSuites)
		state, clientErr, serverErr := handshake(t, serverCert, serverSuite, cfg)
		require.NoError(t, serverErr)
		require.NoError(t, clientErr)
		assert.Equal(t, uint16(tls.VersionTLS12), state.Version)
		assert.Equal(t, suiteNamed(t, tls.CipherSuites(), serverSuite).ID, state.CipherSuite)
	})

	t.Run("different suite fails", func(t *testing.T) {
		cfg := clientTLSConfig(t, caPEM, clientSuite)
		require.Equal(t, []uint16{suiteNamed(t, tls.CipherSuites(), clientSuite).ID}, cfg.CipherSuites)
		_, clientErr, serverErr := handshake(t, serverCert, serverSuite, cfg)
		require.Error(t, clientErr)
		require.Error(t, serverErr)
		assert.Contains(t, clientErr.Error(), "handshake failure")
	})
}

// TestTLSProtocolCap checks the protocols allow-list against a listener fixed
// at one version. An empty list still completes TLS 1.3 when the cipher list
// names only a TLS 1.2 suite.
func TestTLSProtocolCap(t *testing.T) {
	const tls12Suite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	caPEM, serverCert := cipherHandshakeCert(t)

	t.Run("tls 1.2 connects to a tls 1.2 server", func(t *testing.T) {
		cfg := clientTLSConfig(t, caPEM, "", "TLSv1.2")
		state, clientErr, serverErr := handshakeAt(t, serverCert, tls12Suite, cfg, tls.VersionTLS12, tls.VersionTLS12)
		require.NoError(t, serverErr)
		require.NoError(t, clientErr)
		assert.Equal(t, uint16(tls.VersionTLS12), state.Version)
	})

	t.Run("tls 1.2 refuses a tls 1.3 server", func(t *testing.T) {
		cfg := clientTLSConfig(t, caPEM, "", "TLSv1.2")
		_, clientErr, serverErr := handshakeAt(t, serverCert, "", cfg, tls.VersionTLS13, tls.VersionTLS13)
		require.Error(t, clientErr)
		require.Error(t, serverErr)
		assert.Contains(t, clientErr.Error(), "protocol version")
	})

	t.Run("tls 1.3 connects to a tls 1.3 server", func(t *testing.T) {
		cfg := clientTLSConfig(t, caPEM, "", "TLSv1.3")
		state, clientErr, serverErr := handshakeAt(t, serverCert, "", cfg, tls.VersionTLS13, tls.VersionTLS13)
		require.NoError(t, serverErr)
		require.NoError(t, clientErr)
		assert.Equal(t, uint16(tls.VersionTLS13), state.Version)
	})

	t.Run("tls 1.3 refuses a tls 1.2 server", func(t *testing.T) {
		cfg := clientTLSConfig(t, caPEM, "", "TLSv1.3")
		_, clientErr, serverErr := handshakeAt(t, serverCert, tls12Suite, cfg, tls.VersionTLS12, tls.VersionTLS12)
		require.Error(t, clientErr)
		require.Error(t, serverErr)
		assert.Contains(t, clientErr.Error(), "protocol version")
	})

	t.Run("empty list still uses tls 1.3 when ciphers name a tls 1.2 suite", func(t *testing.T) {
		cfg := clientTLSConfig(t, caPEM, tls12Suite)
		require.Equal(t, []uint16{suiteNamed(t, tls.CipherSuites(), tls12Suite).ID}, cfg.CipherSuites)
		assert.Zero(t, cfg.MaxVersion)
		state, clientErr, serverErr := handshakeAt(t, serverCert, "", cfg, tls.VersionTLS13, tls.VersionTLS13)
		require.NoError(t, serverErr)
		require.NoError(t, clientErr)
		assert.Equal(t, uint16(tls.VersionTLS13), state.Version)
	})
}

// The handshake certificate uses serial 2. The CA that signed it uses serial 1.
func TestRevokeCertificates(t *testing.T) {
	const tls12Suite = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	caPEM, serverCert := cipherHandshakeCert(t)

	t.Run("listed peer serial fails", func(t *testing.T) {
		cfg := inboundTLS(t, caPEM, "", nil, []string{"2"})
		_, clientErr, serverErr := handshake(t, serverCert, tls12Suite, cfg)
		require.Error(t, clientErr)
		require.Error(t, serverErr)
		assert.Contains(t, clientErr.Error(), "serial 2 is revoked")
	})

	t.Run("listed ca serial fails", func(t *testing.T) {
		cfg := inboundTLS(t, caPEM, "", nil, []string{"1"})
		_, clientErr, serverErr := handshake(t, serverCert, tls12Suite, cfg)
		require.Error(t, clientErr)
		require.Error(t, serverErr)
		assert.Contains(t, clientErr.Error(), "serial 1 is revoked")
	})

	t.Run("hex serial matches the peer", func(t *testing.T) {
		cfg := inboundTLS(t, caPEM, "", nil, []string{"0x2"})
		_, clientErr, serverErr := handshake(t, serverCert, tls12Suite, cfg)
		require.Error(t, clientErr)
		require.Error(t, serverErr)
		assert.Contains(t, clientErr.Error(), "serial 2 is revoked")
	})

	t.Run("skip cert verify does not see the ca", func(t *testing.T) {
		cfg := inboundTLS(t, caPEM, "", nil, []string{"1"})
		cfg.InsecureSkipVerify = true
		_, clientErr, serverErr := handshake(t, serverCert, tls12Suite, cfg)
		require.NoError(t, serverErr)
		require.NoError(t, clientErr)
	})

	t.Run("other serial still connects", func(t *testing.T) {
		cfg := inboundTLS(t, caPEM, "", nil, []string{"99"})
		state, clientErr, serverErr := handshake(t, serverCert, tls12Suite, cfg)
		require.NoError(t, serverErr)
		require.NoError(t, clientErr)
		assert.Equal(t, uint16(tls.VersionTLS12), state.Version)
	})
}

// handshake runs the parsed inbound tls.Config against a TLS 1.2 listener that
// allows only serverSuite. Both sides stay open until each handshake returns.
func handshake(t *testing.T, cert tls.Certificate, serverSuite string, clientCfg *tls.Config) (tls.ConnectionState, error, error) {
	t.Helper()
	return handshakeAt(t, cert, serverSuite, clientCfg, tls.VersionTLS12, tls.VersionTLS12)
}

// handshakeAt is handshake with the server protocol range chosen by the caller.
// An empty serverSuite leaves the server cipher list unset, which is how a
// TLS 1.3 listener offers its suites.
func handshakeAt(t *testing.T, cert tls.Certificate, serverSuite string, clientCfg *tls.Config, serverMin, serverMax uint16) (tls.ConnectionState, error, error) {
	t.Helper()
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   serverMin,
		MaxVersion:   serverMax,
	}
	if serverSuite != "" {
		serverCfg.CipherSuites = []uint16{suiteNamed(t, tls.CipherSuites(), serverSuite).ID}
	}
	serverErrc := make(chan error, 1)
	go func() {
		conn, acceptErr := raw.Accept()
		if acceptErr != nil {
			serverErrc <- acceptErr
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		serverErrc <- tls.Server(conn, serverCfg).Handshake()
	}()

	tcp, err := net.DialTimeout("tcp", raw.Addr().String(), 3*time.Second)
	require.NoError(t, err)
	defer tcp.Close()
	_ = tcp.SetDeadline(time.Now().Add(3 * time.Second))
	// tls.Dial fills this from the address. tls.Client does not, and an IP
	// certificate is matched from this name against its IP SAN.
	cloned := clientCfg.Clone()
	cloned.ServerName = "127.0.0.1"
	client := tls.Client(tcp, cloned)
	clientErr := client.Handshake()
	var state tls.ConnectionState
	if clientErr == nil {
		state = client.ConnectionState()
	}
	return state, clientErr, <-serverErrc
}

func clientTLSConfig(t *testing.T, caPEM []byte, suite string, protocols ...string) *tls.Config {
	t.Helper()
	return inboundTLS(t, caPEM, suite, protocols, nil)
}

func inboundTLS(t *testing.T, caPEM []byte, suite string, protocols, revoked []string) *tls.Config {
	t.Helper()
	var yaml strings.Builder
	yaml.WriteString("hosts: [\"127.0.0.1:1\"]\n")
	yaml.WriteString("tls:\n  enabled: true\n  root_cas: |\n")
	for line := range strings.SplitSeq(strings.TrimSpace(string(caPEM)), "\n") {
		yaml.WriteString("    ")
		yaml.WriteString(line)
		yaml.WriteByte('\n')
	}
	if suite != "" {
		yaml.WriteString("ciphers: [\"")
		yaml.WriteString(suite)
		yaml.WriteString("\"]\n")
	}
	if len(protocols) > 0 {
		yaml.WriteString("protocols: [")
		for i, protocol := range protocols {
			if i > 0 {
				yaml.WriteString(", ")
			}
			yaml.WriteByte('"')
			yaml.WriteString(protocol)
			yaml.WriteByte('"')
		}
		yaml.WriteString("]\n")
	}
	if len(revoked) > 0 {
		yaml.WriteString("revoke_certificates: [")
		for i, serial := range revoked {
			if i > 0 {
				yaml.WriteString(", ")
			}
			yaml.WriteByte('"')
			yaml.WriteString(serial)
			yaml.WriteByte('"')
		}
		yaml.WriteString("]\n")
	}

	c, err := parseClientYAML(t, yaml.String())
	require.NoError(t, err)
	require.NotNil(t, c.Policy.TlsConfig)
	return c.Policy.TlsConfig
}

func cipherHandshakeCert(t *testing.T) (caPEM []byte, serverCert tls.Certificate) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cipher-handshake-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caTmpl, &key.PublicKey, caKey)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	serverCert, err = tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	return caPEM, serverCert
}
