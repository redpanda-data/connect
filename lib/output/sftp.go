package output

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	sftpSetup "github.com/Jeffail/benthos/v3/internal/impl/sftp"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/pkg/sftp"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSFTP] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			sftp, err := newSFTPWriter(conf.SFTP, mgr, log, stats)
			if err != nil {
				return nil, err
			}
			a, err := NewAsyncWriter(
				TypeSFTP, conf.SFTP.MaxInFlight, sftp, log, stats,
			)
			if err != nil {
				return nil, err
			}
			return OnlySinglePayloads(a), nil
		}),
		Status:  docs.StatusExperimental,
		Version: "3.39.0",
		Summary: `Writes files to a server over SFTP.`,
		Description: `
In order to have a different path for each object you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

` + multipartCodecDoc,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"address",
				"The address of the server to connect to that has the target files.",
			),
			docs.FieldCommon(
				"path",
				"The file to save the messages to on the server.",
			),
			codec.WriterDocs,
			docs.FieldCommon(
				"credentials",
				"The credentials to use to log into the server.",
			).WithChildren(sftpSetup.CredentialsDocs()...),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// SFTPConfig contains configuration fields for the SFTP output type.
type SFTPConfig struct {
	Address     string                `json:"address" yaml:"address"`
	Path        string                `json:"path" yaml:"path"`
	Codec       string                `json:"codec" yaml:"codec"`
	Credentials sftpSetup.Credentials `json:"credentials" yaml:"credentials"`
	MaxInFlight int                   `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewSFTPConfig creates a new Config with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Address: "",
		Path:    "",
		Codec:   "all-bytes",
		Credentials: sftpSetup.Credentials{
			Username: "",
			Password: "",
		},
		MaxInFlight: 1,
	}
}

type sftpWriter struct {
	conf SFTPConfig

	client *sftp.Client

	log   log.Modular
	stats metrics.Type

	path      *field.Expression
	codec     codec.WriterConstructor
	codecConf codec.WriterConfig

	handleMut  sync.Mutex
	handlePath string
	handle     codec.Writer
}

func newSFTPWriter(
	conf SFTPConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*sftpWriter, error) {
	s := &sftpWriter{
		conf:  conf,
		log:   log,
		stats: stats,
	}

	var err error
	if s.codec, s.codecConf, err = codec.GetWriter(conf.Codec); err != nil {
		return nil, err
	}
	if s.path, err = interop.NewBloblangField(mgr, conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %w", err)
	}

	return s, nil
}

// ConnectWithContext attempts to establish a connection to the target SFTP server.
func (s *sftpWriter) ConnectWithContext(ctx context.Context) error {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.client != nil {
		return nil
	}

	var err error
	s.client, err = s.conf.Credentials.GetClient(s.conf.Address)
	return err
}

// WriteWithContext attempts to write message contents to a target file via an SFTP connection.
func (s *sftpWriter) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	s.handleMut.Lock()
	client := s.client
	s.handleMut.Unlock()
	if client == nil {
		return types.ErrNotConnected
	}

	return writer.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		path := s.path.String(i, msg)

		s.handleMut.Lock()
		defer s.handleMut.Unlock()

		if s.handle != nil && path == s.handlePath {
			// TODO: Detect underlying connection failure here and drop client.
			return s.handle.Write(ctx, p)
		}
		if s.handle != nil {
			if err := s.handle.Close(ctx); err != nil {
				return err
			}
		}

		flag := os.O_CREATE | os.O_RDWR
		if s.codecConf.Append {
			flag |= os.O_APPEND
		}
		if s.codecConf.Truncate {
			flag |= os.O_TRUNC
		}

		if err := s.client.MkdirAll(filepath.Dir(path)); err != nil {
			return err
		}

		file, err := s.client.OpenFile(path, flag)
		if err != nil {
			return err
		}

		s.handlePath = path
		handle, err := s.codec(file)
		if err != nil {
			return err
		}

		if err = handle.Write(ctx, p); err != nil {
			handle.Close(ctx)
			return err
		}

		if !s.codecConf.CloseAfter {
			s.handle = handle
		} else {
			handle.Close(ctx)
		}
		return nil
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (s *sftpWriter) CloseAsync() {
	go func() {
		s.handleMut.Lock()
		if s.handle != nil {
			s.handle.Close(context.Background())
			s.handle = nil
		}
		if s.client != nil {
			s.client.Close()
			s.client = nil
		}
		s.handleMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (s *sftpWriter) WaitForClose(time.Duration) error {
	return nil
}
