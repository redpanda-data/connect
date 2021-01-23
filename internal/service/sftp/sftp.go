package sftp

import (
	"fmt"
	"net"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// CredentialsDocs returns a documentation field spec for SFTP credentials
// fields within a Config.
func CredentialsDocs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("username", "The username to connect to the SFTP server."),
		docs.FieldCommon("password", "The password for the username to connect to the SFTP server."),
	}
}

// Credentials contains the credentials for connecting to the SFTP server
type Credentials struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// GetClient establishes a fresh sftp client from a set of credentials and an
// address.
func (c Credentials) GetClient(address string) (*sftp.Client, error) {
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
		User: c.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(c.Password),
		},
		HostKeyCallback: certCheck.CheckHostKey,
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

// Server contains connection data for connecting to an SFTP server
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

// HostAuthorityCallback used when setting up the connection to the SFTP server
type HostAuthorityCallback func(ssh.PublicKey, string) bool

// IsRevokedCallback used when setting up the connection to the SFTP server
type IsRevokedCallback func(cert *ssh.Certificate) bool

// HostAuthCallback is called when setting up the connection to the SFTP server
func HostAuthCallback() HostAuthorityCallback {
	return func(p ssh.PublicKey, addr string) bool {
		return true
	}
}

// CertCallback is called when setting up the connection to the SFTP server
func CertCallback(s *Server) IsRevokedCallback {
	return func(cert *ssh.Certificate) bool {
		s.Cert = *cert
		s.IsSSH = true

		return false
	}
}

// HostCallback is called when setting up the connection to the SFTP server
func HostCallback(s *Server) ssh.HostKeyCallback {
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		s.Hostname = hostname
		s.PublicKey = key
		return nil
	}
}
