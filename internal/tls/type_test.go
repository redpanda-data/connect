package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/youmark/pkcs8"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

func createCertificates() (certPem, keyPem []byte) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	priv := x509.MarshalPKCS1PrivateKey(key)

	tml := x509.Certificate{
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(5, 0, 0),
		SerialNumber: big.NewInt(123123),
		Subject: pkix.Name{
			CommonName:   "Benthos",
			Organization: []string{"Benthos"},
		},
		BasicConstraintsValid: true,
	}

	cert, err := x509.CreateCertificate(rand.Reader, &tml, &tml, &key.PublicKey, key)
	if err != nil {
		log.Fatal("Certificate cannot be created.", err.Error())
	}

	certPem = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	})

	keyPem = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: priv})

	return certPem, keyPem
}

type keyPair struct {
	cert []byte
	key  []byte
}

func createCertificatesWithEncryptedPKCS1Key(t *testing.T, password string) keyPair {
	t.Helper()

	certPem, keyPem := createCertificates()
	decodedKey, _ := pem.Decode(keyPem)

	//nolint:staticcheck // SA1019 Disable linting for deprecated  x509.EncryptPEMBlock call
	block, err := x509.EncryptPEMBlock(rand.Reader, decodedKey.Type, decodedKey.Bytes, []byte(password), x509.PEMCipher3DES)
	require.NoError(t, err)

	keyPem = pem.EncodeToMemory(
		block,
	)
	return keyPair{cert: certPem, key: keyPem}
}

func createCertificatesWithEncryptedPKCS8Key(t *testing.T, password string) keyPair {
	t.Helper()

	certPem, keyPem := createCertificates()
	pemBlock, _ := pem.Decode(keyPem)
	decodedKey, err := x509.ParsePKCS1PrivateKey(pemBlock.Bytes)
	require.NoError(t, err)

	keyBytes, err := pkcs8.ConvertPrivateKeyToPKCS8(decodedKey, []byte(password))
	require.NoError(t, err)

	return keyPair{cert: certPem, key: pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: keyBytes})}
}

func TestCertificateFileWithEncryptedKey(t *testing.T) {
	tests := []struct {
		name string
		kp   keyPair
	}{
		{
			name: "PKCS#1",
			kp:   createCertificatesWithEncryptedPKCS1Key(t, "benthos"),
		},
		{
			name: "PKCS#8",
			kp:   createCertificatesWithEncryptedPKCS8Key(t, "benthos"),
		},
	}

	tmpDir := t.TempDir()
	for _, test := range tests {
		fCert, _ := os.CreateTemp(tmpDir, "cert.pem")
		_, _ = fCert.Write(test.kp.cert)
		fCert.Close()

		fKey, _ := os.CreateTemp(tmpDir, "key.pem")
		_, _ = fKey.Write(test.kp.key)
		fKey.Close()

		c := ClientCertConfig{
			KeyFile:  fKey.Name(),
			CertFile: fCert.Name(),
			Password: "benthos",
		}

		_, err := c.Load(ifs.OS())
		if err != nil {
			t.Errorf("Failed to load %s certificate: %s", test.name, err)
		}
	}
}

func TestCertificateWithEncryptedKey(t *testing.T) {
	tests := []struct {
		name string
		kp   keyPair
	}{
		{
			name: "PKCS#1",
			kp:   createCertificatesWithEncryptedPKCS1Key(t, "benthos"),
		},
		{
			name: "PKCS#8",
			kp:   createCertificatesWithEncryptedPKCS8Key(t, "benthos"),
		},
	}

	for _, test := range tests {
		c := ClientCertConfig{
			Cert:     string(test.kp.cert),
			Key:      string(test.kp.key),
			Password: "benthos",
		}

		_, err := c.Load(ifs.OS())
		if err != nil {
			t.Errorf("Failed to load %s certificate: %s", test.name, err)
		}
	}
}

func TestCertificateFileWithEncryptedKeyAndWrongPassword(t *testing.T) {
	tests := []struct {
		name string
		kp   keyPair
		err  string
	}{
		{
			name: "PKCS#1",
			kp:   createCertificatesWithEncryptedPKCS1Key(t, "benthos"),
			err:  "x509: decryption password incorrect",
		},
		{
			name: "PKCS#8",
			kp:   createCertificatesWithEncryptedPKCS8Key(t, "benthos"),
			err:  "pkcs8: incorrect password",
		},
	}

	tmpDir := t.TempDir()
	for _, test := range tests {
		fCert, _ := os.CreateTemp(tmpDir, "cert.pem")
		_, _ = fCert.Write(test.kp.cert)
		fCert.Close()

		fKey, _ := os.CreateTemp(tmpDir, "key.pem")
		_, _ = fKey.Write(test.kp.key)
		fKey.Close()

		c := ClientCertConfig{
			KeyFile:  fKey.Name(),
			CertFile: fCert.Name(),
			Password: "not_bentho",
		}

		_, err := c.Load(ifs.OS())
		require.ErrorContains(t, err, test.err, test.name)
	}
}

func TestEncryptedKeyWithWrongPassword(t *testing.T) {
	tests := []struct {
		name string
		kp   keyPair
		err  string
	}{
		{
			name: "PKCS#1",
			kp:   createCertificatesWithEncryptedPKCS1Key(t, "benthos"),
			err:  "x509: decryption password incorrect",
		},
		{
			name: "PKCS#8",
			kp:   createCertificatesWithEncryptedPKCS8Key(t, "benthos"),
			err:  "pkcs8: incorrect password",
		},
	}

	for _, test := range tests {
		c := ClientCertConfig{
			Cert:     string(test.kp.cert),
			Key:      string(test.kp.key),
			Password: "not_bentho",
		}

		_, err := c.Load(ifs.OS())
		require.ErrorContains(t, err, test.err, test.name)
	}
}

func TestCertificateFileWithNoEncryption(t *testing.T) {
	cert, key := createCertificates()

	tmpDir := t.TempDir()

	fCert, _ := os.CreateTemp(tmpDir, "cert.pem")
	_, _ = fCert.Write(cert)
	defer fCert.Close()

	fKey, _ := os.CreateTemp(tmpDir, "key.pem")
	_, _ = fKey.Write(key)
	defer fKey.Close()

	c := ClientCertConfig{
		KeyFile:  fKey.Name(),
		CertFile: fCert.Name(),
	}

	_, err := c.Load(ifs.OS())
	if err != nil {
		t.Errorf("Failed to load certificate %s", err)
	}
}

func TestCertificateWithNoEncryption(t *testing.T) {
	cert, key := createCertificates()

	c := ClientCertConfig{
		Key:  string(key),
		Cert: string(cert),
	}

	_, err := c.Load(ifs.OS())
	if err != nil {
		t.Errorf("Failed to load certificate %s", err)
	}
}
