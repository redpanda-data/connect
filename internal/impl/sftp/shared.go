package sftp

import (
	"fmt"
	"io/fs"
	"net"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	scFieldCredentialsUsername       = "username"
	scFieldCredentialsPassword       = "password"
	scFieldCredentialsPrivateKeyFile = "private_key_file"
	scFieldCredentialsPrivateKeyPass = "private_key_pass"
)

func credentialsFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(scFieldCredentialsUsername).Description("The username to connect to the SFTP server.").Default(""),
		service.NewStringField(scFieldCredentialsPassword).Description("The password for the username to connect to the SFTP server.").Secret().Default(""),
		service.NewStringField(scFieldCredentialsPrivateKeyFile).Description("The private key for the username to connect to the SFTP server.").Default(""),
		service.NewStringField(scFieldCredentialsPrivateKeyPass).Description("Optional passphrase for private key.").Secret().Default(""),
	}
}

func credentialsFromParsed(pConf *service.ParsedConfig) (creds Credentials, err error) {
	if creds.Username, err = pConf.FieldString(scFieldCredentialsUsername); err != nil {
		return
	}
	if creds.Password, err = pConf.FieldString(scFieldCredentialsPassword); err != nil {
		return
	}
	if creds.PrivateKeyFile, err = pConf.FieldString(scFieldCredentialsPrivateKeyFile); err != nil {
		return
	}
	if creds.PrivateKeyPass, err = pConf.FieldString(scFieldCredentialsPrivateKeyPass); err != nil {
		return
	}
	return
}

type Credentials struct {
	Username       string
	Password       string
	PrivateKeyFile string
	PrivateKeyPass string
}

func (c Credentials) GetClient(fs fs.FS, address string) (*sftp.Client, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse address: %v", err)
	}

	// create sftp client and establish connection
	server := &Server{
		Host: host,
		Port: port,
	}

	certCheck := &ssh.CertChecker{
		IsHostAuthority: HostAuthCallback(),
		IsRevoked:       CertCallback(server),
		HostKeyFallback: HostCallback(server),
	}

	config := &ssh.ClientConfig{
		User:            c.Username,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: certCheck.CheckHostKey,
	}

	// set password auth when provided
	if c.Password != "" {
		// append to config.Auth
		config.Auth = append(config.Auth, ssh.Password(c.Password))
	}

	// set private key auth when provided
	if c.PrivateKeyFile != "" {
		// read private key file
		var privateKey []byte
		privateKey, err = service.ReadFile(fs, c.PrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read private key: %v", err)
		}
		// check if passphrase is provided and parse private key
		var signer ssh.Signer
		if c.PrivateKeyPass == "" {
			signer, err = ssh.ParsePrivateKey(privateKey)
		} else {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(privateKey, []byte(c.PrivateKeyPass))
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %v", err)
		}
		// append to config.Auth
		config.Auth = append(config.Auth, ssh.PublicKeys(signer))
	}

	conn, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return client, nil
}

// Server contains connection data for connecting to an SFTP server.
type Server struct {
	Address   string          // host:port
	Host      string          // IP address
	Port      string          // port
	IsSSH     bool            // true if server is running SSH on address:port
	Banner    string          // banner text, if any
	Cert      ssh.Certificate // server's certificate
	Hostname  string          // hostname
	PublicKey ssh.PublicKey   // server's public key
}

// HostAuthorityCallback used when setting up the connection to the SFTP server.
type HostAuthorityCallback func(ssh.PublicKey, string) bool

// IsRevokedCallback used when setting up the connection to the SFTP server.
type IsRevokedCallback func(cert *ssh.Certificate) bool

// HostAuthCallback is called when setting up the connection to the SFTP server.
func HostAuthCallback() HostAuthorityCallback {
	return func(p ssh.PublicKey, addr string) bool {
		return true
	}
}

// CertCallback is called when setting up the connection to the SFTP server.
func CertCallback(s *Server) IsRevokedCallback {
	return func(cert *ssh.Certificate) bool {
		s.Cert = *cert
		s.IsSSH = true

		return false
	}
}

// HostCallback is called when setting up the connection to the SFTP server.
func HostCallback(s *Server) ssh.HostKeyCallback {
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		s.Hostname = hostname
		s.PublicKey = key
		return nil
	}
}
