// +build !wasm

package writer

import (
	"context"
	"errors"
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"net"
	"os"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// SFTP is a benthos writer. Type implementation that writes messages to a
// file via an SFTP connection.
type SFTP struct {
	conf SFTPConfig

	client *sftp.Client

	container   field.Expression
	path        field.Expression
	blobType    field.Expression
	accessLevel field.Expression

	log   log.Modular
	stats metrics.Type
}

// NewSFTP creates a new SFTP writer.Type.
func NewSFTP(
	conf SFTPConfig,
	log log.Modular,
	stats metrics.Type,
) (*SFTP, error) {
	s := &SFTP{
		conf:  conf,
		log:   log,
		stats: stats,
	}

	err := s.initSFTPConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SFTP server: %v", err)
	}

	return s, nil
}

// ConnectWithContext attempts to establish a connection to the target SFTP server.
func (s *SFTP) ConnectWithContext(ctx context.Context) error {
	return s.Connect()
}

// Connect attempts to establish a connection to the target SFTP server.
func (s *SFTP) Connect() error {
	return nil
}

// Write attempts to write message contents to a target SFTP server as files.
func (s *SFTP) Write(msg types.Message) error {
	return s.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target storage account as files.
func (s *SFTP) WriteWithContext(_ context.Context, msg types.Message) error {
	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		var file *sftp.File
		_, err := s.client.Stat(s.conf.Filepath)

		if err != nil {
			file, err = s.client.Create(s.conf.Filepath)
			if err != nil {
				s.log.Errorf("Error creating file: %v", err)
				return err
			}
		} else {
			file, err = s.client.OpenFile(s.conf.Filepath, os.O_APPEND|os.O_RDWR)
			if err != nil {
				s.log.Errorf("Error opening file: %v", err)
				return err
			}
		}

		str := string(p.Get()) + "\n"
		_, err = file.Write([]byte(str))

		if err != nil {
			s.log.Errorf("Error writing to file: %v", err)
			return err
		}

		return nil
	})
}

func (s *SFTP) initSFTPConnection() error {
	// create sftp client and establish connection
	server := &SFTPServer{
		Host: s.conf.Server,
		Port: s.conf.Port,
	}

	certCheck := &ssh.CertChecker{
		IsHostAuthority: hostAuthCallback(),
		IsRevoked:       certCallback(server),
		HostKeyFallback: hostCallback(server),
	}

	addr := fmt.Sprintf("%s:%d", s.conf.Server, s.conf.Port)
	config := &ssh.ClientConfig{
		User: s.conf.Credentials.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(s.conf.Credentials.Secret),
		},
		HostKeyCallback: certCheck.CheckHostKey,
	}

	var conn *ssh.Client
	var err error
	connectionAttempts := 0
	for {
		connectionAttempts++
		conn, err = ssh.Dial("tcp", addr, config)
		if err != nil {
			connectionErrorsCounter := s.stats.GetCounter("connection_errors")
			connectionErrorsCounter.Incr(1)
			s.log.Errorf("Failed to dial: %s", err.Error())
			if connectionAttempts >= 10 {
				s.log.Errorf("Failed to connect after %i attempts, stopping", connectionAttempts)
				return errors.New("failed to connect to SFTP server")
			}
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}

	client, err := sftp.NewClient(conn)

	if err != nil {
		clientErrorsCounter := s.stats.GetCounter("client_errors")
		clientErrorsCounter.Incr(1)
		s.log.Errorf("Failed to create client: %s", err.Error())
	}

	s.client = client

	return err
}

type SFTPServer struct {
	Address   string          // host:port
	Host      string          // IP address
	Port      int             // port
	IsSSH     bool            // true if server is running SSH on address:port
	Banner    string          // banner text, if any
	Cert      ssh.Certificate // server's certificate
	Hostname  string          // hostname
	PublicKey ssh.PublicKey   // server's public key
}

type HostAuthorityCallBack func(ssh.PublicKey, string) bool
type IsRevokedCallback func(cert *ssh.Certificate) bool

func hostAuthCallback() HostAuthorityCallBack {
	return func(p ssh.PublicKey, addr string) bool {
		return true
	}
}

func certCallback(s *SFTPServer) IsRevokedCallback {
	return func(cert *ssh.Certificate) bool {
		s.Cert = *cert
		s.IsSSH = true

		return false
	}
}

func hostCallback(s *SFTPServer) ssh.HostKeyCallback {
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		s.Hostname = hostname
		s.PublicKey = key
		return nil
	}
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (s *SFTP) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (s *SFTP) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
