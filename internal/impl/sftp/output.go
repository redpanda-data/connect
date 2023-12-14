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

	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
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
		Description(output.Description(true, false, `In order to have a different path for each object you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).`)).
		Fields(
			service.NewStringField(soFieldAddress).
				Description("The address of the server to connect to."),
			service.NewInterpolatedStringField(soFieldPath).
				Description("The file to save the messages to on the server."),
			service.NewInternalField(codec.NewWriterDocs("codec").HasDefault("all-bytes")),
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
	client *sftp.Client

	log *service.Logger
	mgr *service.Resources

	address    string
	creds      Credentials
	path       *service.InterpolatedString
	suffixFn   codec.SuffixFn
	appendMode bool

	handleMut  sync.Mutex
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
	if s.suffixFn, s.appendMode, err = codec.GetWriter(codecStr); err != nil {
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

func (s *sftpWriter) Connect(ctx context.Context) error {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	var err error
	s.client, err = s.creds.GetClient(s.mgr.FS(), s.address)
	return err
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
	if s.client == nil {
		return service.ErrNotConnected
	}

	path, err := s.path.TryString(msg)
	if err != nil {
		return fmt.Errorf("path interpolation error: %w", err)
	}

	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.handle != nil && path == s.handlePath {
		// TODO: Detect underlying connection failure here and drop client.
		return s.writeTo(s.handle, msg)
	}
	if s.handle != nil {
		if err := s.handle.Close(); err != nil {
			return err
		}
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

	s.handlePath = path
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
	} else {
		_ = handle.Close()
	}
	return nil
}

func (s *sftpWriter) Close(ctx context.Context) (err error) {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.handle != nil {
		err = s.handle.Close()
		s.handle = nil
	}
	if err == nil && s.client != nil {
		err = s.client.Close()
		s.client = nil
	}
	return
}
