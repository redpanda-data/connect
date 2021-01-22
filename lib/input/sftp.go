package input

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	sftpSetup "github.com/Jeffail/benthos/v3/internal/service/sftp"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func init() {
	var credentialsFields = docs.FieldSpecs{
		docs.FieldCommon("username", "The username to connect to the SFTP server."),
		docs.FieldCommon("password", "The password for the username to connect to the SFTP server."),
	}

	Constructors[TypeSFTP] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := NewSFTP(conf.SFTP, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeSFTP,
				true,
				reader.NewAsyncPreserver(r),
				log, stats,
			)
		}),
		Status: docs.StatusExperimental,
		Summary: `
Downloads objects via an SFTP connection.`,
		Description: `
Downloads objects via an SFTP connection.
## Metadata
This input adds the following metadata fields to each message:
` + "```" + `
- sftp_file_path
` + "```" + `
You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,

		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"address",
				"The address of the server to connect to that has the target files.",
			),
			docs.FieldCommon(
				"credentials",
				"The credentials to use to log into the server.",
			).WithChildren(credentialsFields...),
			docs.FieldCommon(
				"paths",
				"A list of paths to consume sequentially. Glob patterns are supported.",
			),
			docs.FieldCommon(
				"max_connection_attempts",
				"How many times it will try to connect to the server before exiting with an error.",
			),
			docs.FieldAdvanced(
				"retry_sleep_duration",
				"How long it will sleep after failing to connect to the server before trying again, defaults to 5s if not provided.",
				"10s", "5m",
			),
			codec.ReaderDocs,
			docs.FieldAdvanced("delete_on_finish", "Whether to delete files from the server once they are processed."),
			docs.FieldAdvanced("max_buffer", "The largest token size expected when consuming delimited files."),
			docs.FieldAdvanced("multipart", "Consume multipart messages from the codec by interpretting empty lines as the end of the message. Multipart messages are processed as a batch within Benthos. Not all codecs are appropriate for multipart messages."),
		},
		Categories: []Category{
			CategoryServices,
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// SFTPConfig contains configuration fields for the SFTP input type.
type SFTPConfig struct {
	Address               string                `json:"address" yaml:"address"`
	Credentials           sftpSetup.Credentials `json:"credentials" yaml:"credentials"`
	Paths                 []string              `json:"paths" yaml:"paths"`
	MaxConnectionAttempts int                   `json:"max_connection_attempts" yaml:"max_connection_attempts"`
	RetrySleepDuration    string                `json:"retry_sleep_duration" yaml:"retry_sleep_duration"`
	Codec                 string                `json:"codec" yaml:"codec"`
	DeleteOnFinish        bool                  `json:"delete_on_finish" yaml:"delete_on_finish"`
	MaxBuffer             int                   `json:"max_buffer" yaml:"max_buffer"`
	Multipart             bool                  `json:"multipart" yaml:"multipart"`
}

// NewSFTPConfig creates a new SFTPConfig with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Address:               "",
		Credentials:           sftpSetup.Credentials{},
		Paths:                 []string{},
		MaxConnectionAttempts: 10,
		RetrySleepDuration:    "5s",
		Codec:                 "lines",
		DeleteOnFinish:        false,
		MaxBuffer:             1000000,
		Multipart:             false,
	}
}

//------------------------------------------------------------------------------

// SFTP is a benthos reader.Type implementation that reads messages
// from file(s) on an SFTP server.
type SFTP struct {
	conf SFTPConfig

	log   log.Modular
	stats metrics.Type

	client *sftp.Client

	paths       []string
	scannerCtor codec.ReaderConstructor

	scannerMut  sync.Mutex
	scanner     codec.Reader
	currentPath string
}

// NewSFTP creates a new SFTP input type.
func NewSFTP(conf SFTPConfig, log log.Modular, stats metrics.Type) (*SFTP, error) {
	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	s := &SFTP{
		conf:        conf,
		log:         log,
		stats:       stats,
		scannerCtor: ctor,
	}

	return s, err
}

// ConnectWithContext attempts to establish a connection to the target SFTP server.
func (s *SFTP) ConnectWithContext(ctx context.Context) error {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner != nil {
		return nil
	}

	var err error
	if s.client == nil {
		err = s.initSFTPConnection()
		if err != nil {
			return err
		}
		var filepaths []string
		for _, p := range s.conf.Paths {
			paths, err := s.client.Glob(p)
			if err != nil {
				continue
			}
			filepaths = append(filepaths, paths...)
		}
		s.paths = filepaths
	}

	if len(s.paths) == 0 {
		return types.ErrTypeClosed
	}

	nextPath := s.paths[0]

	file, err := s.client.Open(nextPath)
	if err != nil {
		return err
	}

	if s.scanner, err = s.scannerCtor(nextPath, file, func(ctx context.Context, err error) error {
		if err == nil && s.conf.DeleteOnFinish {
			return s.client.Remove(nextPath)
		}
		return nil
	}); err != nil {
		file.Close()
		return err
	}

	s.currentPath = nextPath
	s.paths = s.paths[1:]

	s.log.Infof("Consuming from file '%v'\n", nextPath)

	return err
}

// ReadWithContext attempts to read a new message from the target file(s) on the server.
func (s *SFTP) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner == nil || s.client == nil {
		return nil, nil, types.ErrNotConnected
	}

	msg := message.New(nil)
	acks := []codec.ReaderAckFn{}

	ackFn := func(ctx context.Context, res types.Response) error {
		for _, fn := range acks {
			fn(ctx, res.Error())
		}
		return nil
	}

scanLoop:
	for {
		part, codecAckFn, err := s.scanner.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				err = types.ErrTimeout
			}
			if err != types.ErrTimeout {
				s.scanner.Close(ctx)
				s.scanner = nil
			}
			if errors.Is(err, io.EOF) {
				if msg.Len() > 0 {
					return msg, ackFn, nil
				}
				return nil, nil, types.ErrTimeout
			}
			return nil, nil, err
		}

		part.Metadata().Set("path", s.currentPath)

		acks = append(acks, codecAckFn)
		if s.conf.Multipart {
			if len(part.Get()) == 0 {
				break scanLoop
			}
			msg.Append(part)
		} else if len(part.Get()) > 0 {
			msg.Append(part)
			break scanLoop
		}

	}

	if msg.Len() == 0 {
		return nil, nil, types.ErrTimeout
	}

	return msg, ackFn, nil
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
	go func() {
		s.scannerMut.Lock()
		if s.scanner != nil {
			s.scanner.Close(context.Background())
			s.scanner = nil
			s.paths = nil
		}
		s.scannerMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (s *SFTP) WaitForClose(timeout time.Duration) error {
	return nil
}
