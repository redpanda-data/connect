package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/sftp"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	soFieldAddress     = "address"
	soFieldCredentials = "credentials"
	soFieldPath        = "path"
)

func sftpOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Network").
		Version("3.39.0").
		Summary(`Writes files to an SFTP server.`).
		Description(`In order to have a different path for each object you should use function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here].`+service.OutputPerformanceDocs(true, false)).
		Fields(
			service.NewStringField(soFieldAddress).
				Description("The address of the server to connect to."),
			service.NewInterpolatedStringField(soFieldPath).
				Description("The file to save the messages to on the server."),
			service.NewStringAnnotatedEnumField("codec", map[string]string{
				"all-bytes": "Only applicable to file based outputs. Writes each message to a file in full, if the file already exists the old content is deleted.",
				"append":    "Append each message to the output stream without any delimiter or special encoding.",
				"lines":     "Append each message to the output stream followed by a line break.",
				"delim:x":   "Append each message to the output stream followed by a custom delimiter.",
			}).
				Description("The way in which the bytes of messages should be written out into the output data stream. It's possible to write lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter.").
				LintRule("").
				Examples("lines", "delim:\t", "delim:foobar").
				Default("all-bytes"),
			service.NewObjectField(soFieldCredentials, credentialsFields()...).
				Description("The credentials to use to log into the target server."),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterOutput(
		"sftp", sftpOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newWriterFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sftpWriter struct {
	log *service.Logger
	mgr *service.Resources

	address    string
	creds      Credentials
	path       *service.InterpolatedString
	suffixFn   codecSuffixFn
	appendMode bool

	handleMut  sync.Mutex
	client     *sftp.Client
	handlePath string
	handle     io.WriteCloser
}

func newWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (s *sftpWriter, err error) {
	s = &sftpWriter{
		log: mgr.Logger(),
		mgr: mgr,
	}

	var codecStr string
	if codecStr, err = conf.FieldString("codec"); err != nil {
		return
	}
	if s.suffixFn, s.appendMode, err = codecGetWriter(codecStr); err != nil {
		return nil, err
	}

	if s.address, err = conf.FieldString(soFieldAddress); err != nil {
		return
	}
	if s.path, err = conf.FieldInterpolatedString(soFieldPath); err != nil {
		return
	}
	if s.creds, err = credentialsFromParsed(conf.Namespace(soFieldCredentials)); err != nil {
		return
	}

	return s, nil
}

func (s *sftpWriter) Connect(ctx context.Context) (err error) {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.client != nil {
		return
	}

	s.client, err = s.creds.GetClient(s.mgr.FS(), s.address)
	return
}

func (s *sftpWriter) writeTo(wtr io.Writer, p *service.Message) error {
	mBytes, err := p.AsBytes()
	if err != nil {
		return err
	}

	suffix, addSuffix := s.suffixFn(mBytes)

	if _, err := wtr.Write(mBytes); err != nil {
		return err
	}
	if addSuffix {
		if _, err := wtr.Write(suffix); err != nil {
			return err
		}
	}
	return nil
}

func (s *sftpWriter) Write(ctx context.Context, msg *service.Message) error {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.client == nil {
		return service.ErrNotConnected
	}

	path, err := s.path.TryString(msg)
	if err != nil {
		return fmt.Errorf("path interpolation error: %w", err)
	}

	if s.handle != nil && path == s.handlePath {
		// TODO: Detect underlying connection failure here and drop client.
		return s.writeTo(s.handle, msg)
	}
	if s.handle != nil {
		if err := s.handle.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close written file")
		}
		s.handle = nil
		s.handlePath = ""
	}

	flag := os.O_CREATE | os.O_WRONLY
	if s.appendMode {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	if err := s.client.MkdirAll(filepath.Dir(path)); err != nil {
		if errors.Is(err, sftp.ErrSshFxConnectionLost) {
			return service.ErrNotConnected
		}
		return err
	}

	handle, err := s.client.OpenFile(path, flag)
	if err != nil {
		if errors.Is(err, sftp.ErrSshFxConnectionLost) {
			return service.ErrNotConnected
		}
		return err
	}

	if err := s.writeTo(handle, msg); err != nil {
		_ = handle.Close()
		return err
	}

	if s.appendMode {
		s.handle = handle
		s.handlePath = path
	} else {
		if err := handle.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close written file")
		}
	}
	return nil
}

func (s *sftpWriter) Close(ctx context.Context) error {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.handle != nil {
		if err := s.handle.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close written file")
		}
		s.handle = nil
	}
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close client")
		}
		s.client = nil
	}
	return nil
}
