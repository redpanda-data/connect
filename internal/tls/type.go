package tls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

//------------------------------------------------------------------------------

// Documentation is a markdown description of how and why to use TLS settings.
const Documentation = `### TLS

Custom TLS settings can be used to override system defaults. This includes
providing a collection of root certificate authorities, providing a list of
client certificates to use for client verification and skipping certificate
verification.

Client certificates can either be added by file or by raw contents:

` + "``` yaml" + `
enabled: true
client_certs:
  - cert_file: ./example.pem
    key_file: ./example.key
  - cert: foo
    key: bar
` + "```" + ``

//------------------------------------------------------------------------------

// ClientCertConfig contains config fields for a client certificate.
type ClientCertConfig struct {
	CertFile string `json:"cert_file" yaml:"cert_file"`
	KeyFile  string `json:"key_file" yaml:"key_file"`
	Cert     string `json:"cert" yaml:"cert"`
	Key      string `json:"key" yaml:"key"`
}

// Config contains configuration params for TLS.
type Config struct {
	Enabled             bool               `json:"enabled" yaml:"enabled"`
	RootCAs             string             `json:"root_cas" yaml:"root_cas"`
	RootCAsFile         string             `json:"root_cas_file" yaml:"root_cas_file"`
	InsecureSkipVerify  bool               `json:"skip_cert_verify" yaml:"skip_cert_verify"`
	ClientCertificates  []ClientCertConfig `json:"client_certs" yaml:"client_certs"`
	EnableRenegotiation bool               `json:"enable_renegotiation" yaml:"enable_renegotiation"`
}

// NewConfig creates a new Config with default values.
func NewConfig() Config {
	return Config{
		Enabled:             false,
		RootCAs:             "",
		RootCAsFile:         "",
		InsecureSkipVerify:  false,
		ClientCertificates:  []ClientCertConfig{},
		EnableRenegotiation: false,
	}
}

//------------------------------------------------------------------------------

// Get returns a valid *tls.Config based on the configuration values of Config.
// If none of the config fields are set then a nil config is returned.
func (c *Config) Get() (*tls.Config, error) {
	var tlsConf *tls.Config
	initConf := func() {
		if tlsConf != nil {
			return
		}
		tlsConf = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	if len(c.RootCAs) > 0 && len(c.RootCAsFile) > 0 {
		return nil, errors.New("only one field between root_cas and root_cas_file can be specified")
	}

	if len(c.RootCAsFile) > 0 {
		caCert, err := os.ReadFile(c.RootCAsFile)
		if err != nil {
			return nil, err
		}
		initConf()
		tlsConf.RootCAs = x509.NewCertPool()
		tlsConf.RootCAs.AppendCertsFromPEM(caCert)
	}

	if len(c.RootCAs) > 0 {
		initConf()
		tlsConf.RootCAs = x509.NewCertPool()
		tlsConf.RootCAs.AppendCertsFromPEM([]byte(c.RootCAs))
	}

	for _, conf := range c.ClientCertificates {
		cert, err := conf.Load()
		if err != nil {
			return nil, err
		}
		initConf()
		tlsConf.Certificates = append(tlsConf.Certificates, cert)
	}

	if c.EnableRenegotiation {
		initConf()
		tlsConf.Renegotiation = tls.RenegotiateFreelyAsClient
	}

	if c.InsecureSkipVerify {
		initConf()
		tlsConf.InsecureSkipVerify = true
	}

	return tlsConf, nil
}

// Load returns a TLS certificate, based on either file paths in the
// config or the raw certs as strings.
func (c *ClientCertConfig) Load() (tls.Certificate, error) {
	if c.CertFile != "" || c.KeyFile != "" {
		if c.CertFile == "" {
			return tls.Certificate{}, errors.New("missing cert_file field in client certificate config")
		}
		if c.KeyFile == "" {
			return tls.Certificate{}, errors.New("missing key_file field in client certificate config")
		}
		return tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	}
	if c.Cert == "" {
		return tls.Certificate{}, errors.New("missing cert field in client certificate config")
	}
	if c.Key == "" {
		return tls.Certificate{}, errors.New("missing key field in client certificate config")
	}
	return tls.X509KeyPair([]byte(c.Cert), []byte(c.Key))
}

//------------------------------------------------------------------------------
