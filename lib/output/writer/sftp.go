// +build !wasm

package writer

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	sftpSetup "github.com/Jeffail/benthos/v3/internal/service/sftp"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

//------------------------------------------------------------------------------

// SFTP is a benthos writer. Type implementation that writes messages to a
// file via an SFTP connection.
type SFTP struct {
	conf SFTPConfig

	client *sftp.Client

	server field.Expression
	path   field.Expression

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

	var err error
	if s.path, err = bloblang.NewField(conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}

	return s, nil
}

// ConnectWithContext attempts to establish a connection to the target SFTP server.
func (s *SFTP) ConnectWithContext(ctx context.Context) error {
	return s.Connect()
}

// Connect attempts to establish a connection to the target SFTP server.
func (s *SFTP) Connect() error {
	err := s.initSFTPConnection()
	return err
}

// Write attempts to write message contents to a target SFTP server as files.
func (s *SFTP) Write(msg types.Message) error {
	return s.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target file via an SFTP connection.
func (s *SFTP) WriteWithContext(_ context.Context, msg types.Message) error {
	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		var file *sftp.File
		path := s.path.String(i, msg)
		_, err := s.client.Stat(path)

		if err != nil {
			dir := filepath.Dir(path)
			err = s.client.MkdirAll(dir)
			if err != nil {
				s.log.Errorf("Error creating directories: %v", err)
				return err
			}

			file, err = s.client.Create(path)
			if err != nil {
				s.log.Errorf("Error creating file: %v", err)
				return err
			}
		} else {
			file, err = s.client.OpenFile(path, os.O_APPEND|os.O_RDWR)
			if err != nil {
				s.log.Errorf("Error opening file: %v", err)
				return err
			}
		}

		str := string(p.Get())
		_, err = file.Write([]byte(str))

		if err != nil {
			s.log.Errorf("Error writing to file: %v", err)
			return err
		}

		return nil
	})
}

func (s *SFTP) initSFTPConnection() error {
	serverURL, err := url.Parse(s.conf.Address)
	if err != nil {
		return fmt.Errorf("failed to parse address: %v", err)
	}

	// create sftp client and establish connection
	server := &sftpSetup.Server{
		Host: serverURL.Hostname(),
		Port: serverURL.Port(),
	}

	certCheck := &ssh.CertChecker{
		IsHostAuthority: sftpSetup.HostAuthCallback(),
		IsRevoked:       sftpSetup.CertCallback(server),
		HostKeyFallback: sftpSetup.HostCallback(server),
	}

	addr := fmt.Sprintf("%s:%s", server.Host, server.Port)
	config := &ssh.ClientConfig{
		User: s.conf.Credentials.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(s.conf.Credentials.Password),
		},
		HostKeyCallback: certCheck.CheckHostKey,
	}

	var conn *ssh.Client
	connectionAttempts := 0
	for {
		connectionAttempts++
		conn, err = ssh.Dial("tcp", addr, config)
		if err != nil {
			connectionErrorsCounter := s.stats.GetCounter("connection_errors")
			connectionErrorsCounter.Incr(1)
			s.log.Errorf("Failed to dial: %s", err.Error())
			if connectionAttempts >= s.conf.MaxConnectionAttempts {
				s.log.Errorf("Failed to connect after %i attempts, stopping", connectionAttempts)
				return errors.New("failed to connect to SFTP server")
			}

			var sleepDuration time.Duration
			if sleepDuration, err = time.ParseDuration(s.conf.RetrySleepDuration); err != nil {
				return fmt.Errorf("failed to parse retry sleep duration: %v", err)
			}
			time.Sleep(sleepDuration)
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

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (s *SFTP) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (s *SFTP) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
