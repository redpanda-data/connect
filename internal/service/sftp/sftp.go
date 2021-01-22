package sftp

import (
	"golang.org/x/crypto/ssh"
	"net"
)

// Credentials contains the credentials for connecting to the SFTP server
type Credentials struct {
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
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
