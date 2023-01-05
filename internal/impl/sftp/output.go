package sftp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/sftp"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sftpSetup "github.com/benthosdev/benthos/v4/internal/impl/sftp/shared"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		sftp, err := newSFTPWriter(conf.SFTP, nm)
		if err != nil {
			return nil, err
		}
		a, err := output.NewAsyncWriter("sftp", conf.SFTP.MaxInFlight, sftp, nm)
		if err != nil {
			return nil, err
		}
		return output.OnlySinglePayloads(a), nil
	}), docs.ComponentSpec{
		Name:        "sftp",
		Status:      docs.StatusBeta,
		Version:     "3.39.0",
		Summary:     `Writes files to a server over SFTP.`,
		Description: output.Description(true, false, `In order to have a different path for each object you should use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"address",
				"The address of the server to connect to that has the target files.",
			),
			docs.FieldString(
				"path",
				"The file to save the messages to on the server.",
			),
			codec.WriterDocs,
			docs.FieldObject(
				"credentials",
				"The credentials to use to log into the server.",
			).WithChildren(sftpSetup.CredentialsDocs()...),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).ChildDefaultAndTypesFromStruct(output.NewSFTPConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sftpWriter struct {
	conf output.SFTPConfig

	client *sftp.Client

	log log.Modular
	mgr bundle.NewManagement

	path      *field.Expression
	codec     codec.WriterConstructor
	codecConf codec.WriterConfig

	handleMut  sync.Mutex
	handlePath string
	handle     codec.Writer
}

func newSFTPWriter(conf output.SFTPConfig, mgr bundle.NewManagement) (*sftpWriter, error) {
	s := &sftpWriter{
		conf: conf,
		log:  mgr.Logger(),
		mgr:  mgr,
	}

	var err error
	if s.codec, s.codecConf, err = codec.GetWriter(conf.Codec); err != nil {
		return nil, err
	}
	if s.path, err = mgr.BloblEnvironment().NewField(conf.Path); err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %w", err)
	}

	return s, nil
}

func (s *sftpWriter) Connect(ctx context.Context) error {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.client != nil {
		return nil
	}

	var err error
	s.client, err = s.conf.Credentials.GetClient(s.mgr.FS(), s.conf.Address)
	return err
}

func (s *sftpWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	s.handleMut.Lock()
	client := s.client
	s.handleMut.Unlock()
	if client == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		path, err := s.path.String(i, msg)
		if err != nil {
			return fmt.Errorf("path interpolation error: %w", err)
		}

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

		flag := os.O_CREATE | os.O_WRONLY
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

func (s *sftpWriter) Close(ctx context.Context) (err error) {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.handle != nil {
		err = s.handle.Close(ctx)
		s.handle = nil
	}
	if err == nil && s.client != nil {
		err = s.client.Close()
		s.client = nil
	}
	return
}
