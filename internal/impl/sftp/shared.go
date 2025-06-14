// Copyright 2024 Redpanda Data, Inc.
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

package sftp

import (
	"fmt"
	"net"

	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	scFieldCredentialsUsername       = "username"
	scFieldCredentialsPassword       = "password"
	scFieldCredentialsPrivateKey     = "private_key"
	scFieldCredentialsPrivateKeyFile = "private_key_file"
	scFieldCredentialsPrivateKeyPass = "private_key_pass"
)

func credentialsFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringField(scFieldCredentialsUsername).Description("The username to connect to the SFTP server.").Default(""),
		service.NewStringField(scFieldCredentialsPassword).Description("The password for the username to connect to the SFTP server.").Secret().Default(""),
		service.NewStringField(scFieldCredentialsPrivateKeyFile).Description("The private key for the username to connect to the SFTP server.").Default(""),
		service.NewStringField(scFieldCredentialsPrivateKey).Description("The private key file for the username to connect to the SFTP server.").
			Default("").
			Secret().
			LintRule(
				`root = match { this.exists("private_key") && this.exists("private_key_file") => [ "both ` + scFieldCredentialsPrivateKey + ` and ` + scFieldCredentialsPrivateKeyFile + ` can't be set simultaneously" ], }`,
			),
		service.NewStringField(scFieldCredentialsPrivateKeyPass).Description("Optional passphrase for private key.").Secret().Default(""),
	}
}

func credentialsFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (creds credentials, err error) {
	if creds.Username, err = pConf.FieldString(scFieldCredentialsUsername); err != nil {
		return
	}
	if creds.Password, err = pConf.FieldString(scFieldCredentialsPassword); err != nil {
		return
	}

	var privateKey []byte
	privateKeyStr, err := pConf.FieldString(scFieldCredentialsPrivateKey)
	if err != nil {
		return credentials{}, err
	}
	privateKeyFile, err := pConf.FieldString(scFieldCredentialsPrivateKeyFile)
	if err != nil {
		return credentials{}, err
	}

	if privateKeyStr != "" {
		privateKey = []byte(privateKeyStr)
	} else if privateKeyFile != "" {
		privateKey, err = service.ReadFile(mgr.FS(), privateKeyFile)
		if err != nil {
			return credentials{}, fmt.Errorf("reading private key file: %w", err)
		}
	}

	if privateKey != nil {
		privateKeyPass, err := pConf.FieldString(scFieldCredentialsPrivateKeyPass)
		if err != nil {
			return credentials{}, err
		}

		// check if passphrase is provided and parse private key
		if privateKeyPass == "" {
			creds.Signer, err = ssh.ParsePrivateKey(privateKey)
		} else {
			creds.Signer, err = ssh.ParsePrivateKeyWithPassphrase(privateKey, []byte(privateKeyPass))
		}
		if err != nil {
			return credentials{}, fmt.Errorf("failed to parse private key: %v", err)
		}
	}

	return
}

type credentials struct {
	Username string
	Password string
	Signer   ssh.Signer
}

// GetConnection establishes a connection to a server using the provided
// credentials.
//
// These connections can be used for multiple SFTP clients, and need closing
// separately after the SFTP clients they enable have been closed.
func (c credentials) GetConnection(address string) (*ssh.Client, error) {
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
	if c.Signer != nil {
		config.Auth = append(config.Auth, ssh.PublicKeys(c.Signer))
	}

	conn, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
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
	return func(ssh.PublicKey, string) bool {
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
	return func(hostname string, _ net.Addr, key ssh.PublicKey) error {
		s.Hostname = hostname
		s.PublicKey = key
		return nil
	}
}
