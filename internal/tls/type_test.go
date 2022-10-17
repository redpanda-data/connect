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

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

func CreateCertificates() (certPem, keyPem []byte) {
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

func CreateCertificatesWithEncryptedKey(password string) (certPem, keyPem []byte) {
	certPem, keyPem = CreateCertificates()
	decodedKey, _ := pem.Decode(keyPem)

	//nolint:staticcheck // SA1019 Disable linting for deprecated  x509.EncryptPEMBlock call
	block, err := x509.EncryptPEMBlock(rand.Reader, decodedKey.Type, decodedKey.Bytes, []byte(password), x509.PEMCipher3DES)
	if err != nil {
		panic(err)
	}
	keyPem = pem.EncodeToMemory(
		block,
	)
	return certPem, keyPem
}

func TestCertificateFileWithEncryptedKey(t *testing.T) {
	cert, key := CreateCertificatesWithEncryptedKey("benthos")

	fCert, _ := os.CreateTemp("", "cert.pem")
	_, _ = fCert.Write(cert)
	defer fCert.Close()

	fKey, _ := os.CreateTemp("", "key.pem")
	_, _ = fKey.Write(key)
	defer fKey.Close()

	c := ClientCertConfig{
		KeyFile:  fKey.Name(),
		CertFile: fCert.Name(),
		Password: "benthos",
	}

	_, err := c.Load(ifs.OS())
	if err != nil {
		t.Errorf("Failed to load certificate %s", err)
	}
}

func TestCertificateWithEncryptedKey(t *testing.T) {
	cert, key := CreateCertificatesWithEncryptedKey("benthos")

	c := ClientCertConfig{
		Key:      string(key),
		Cert:     string(cert),
		Password: "benthos",
	}

	_, err := c.Load(ifs.OS())
	if err != nil {
		t.Errorf("Failed to load certificate %s", err)
	}
}

func TestCertificateFileWithEncryptedKeyAndWrongPassword(t *testing.T) {
	cert, key := CreateCertificatesWithEncryptedKey("benthos")

	fCert, _ := os.CreateTemp("", "cert.pem")
	_, _ = fCert.Write(cert)
	defer fCert.Close()

	fKey, _ := os.CreateTemp("", "key.pem")
	_, _ = fKey.Write(key)
	defer fKey.Close()

	c := ClientCertConfig{
		KeyFile:  fKey.Name(),
		CertFile: fCert.Name(),
		Password: "not_bentho",
	}

	_, err := c.Load(ifs.OS())
	if err == nil {
		t.Error("Should have failed with wrong password")
	}
}

func TestEncryptedKeyWithWrongPassword(t *testing.T) {
	cert, key := CreateCertificatesWithEncryptedKey("benthos")

	c := ClientCertConfig{
		Key:      string(key),
		Cert:     string(cert),
		Password: "not_bentho",
	}

	_, err := c.Load(ifs.OS())
	if err == nil {
		t.Error("Should have failed with wrong password")
	}
}

func TestCertificateFileWithNoEncryption(t *testing.T) {
	cert, key := CreateCertificates()

	fCert, _ := os.CreateTemp("", "cert.pem")
	_, _ = fCert.Write(cert)
	defer fCert.Close()

	fKey, _ := os.CreateTemp("", "key.pem")
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
	cert, key := CreateCertificates()

	c := ClientCertConfig{
		Key:  string(key),
		Cert: string(cert),
	}

	_, err := c.Load(ifs.OS())
	if err != nil {
		t.Errorf("Failed to load certificate %s", err)
	}
}
