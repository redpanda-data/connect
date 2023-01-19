package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pkg/sftp"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sftpSetup "github.com/benthosdev/benthos/v4/internal/impl/sftp/shared"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	watcherDocs := docs.FieldSpecs{
		docs.FieldBool(
			"enabled",
			"Whether file watching is enabled.",
		),
		docs.FieldString(
			"minimum_age",
			"The minimum period of time since a file was last updated before attempting to consume it. Increasing this period decreases the likelihood that a file will be consumed whilst it is still being written to.",
			"10s", "1m", "10m",
		),
		docs.FieldString(
			"poll_interval",
			"The interval between each attempt to scan the target paths for new files.",
			"100ms", "1s",
		),
		docs.FieldString(
			"cache",
			"A [cache resource](/docs/components/caches/about) for storing the paths of files already consumed.",
		),
	}

	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		r, err := newSFTPReader(conf.SFTP, nm)
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("sftp", input.NewAsyncPreserver(r), nm)
	}), docs.ComponentSpec{
		Name:    "sftp",
		Status:  docs.StatusBeta,
		Version: "3.39.0",
		Summary: `Consumes files from a server over SFTP.`,
		Description: `
## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- sftp_path
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"address",
				"The address of the server to connect to that has the target files.",
			),
			docs.FieldObject(
				"credentials",
				"The credentials to use to log into the server.",
			).WithChildren(sftpSetup.CredentialsDocs()...),
			docs.FieldString(
				"paths",
				"A list of paths to consume sequentially. Glob patterns are supported.",
			).Array(),
			codec.ReaderDocs,
			docs.FieldBool("delete_on_finish", "Whether to delete files from the server once they are processed.").Advanced(),
			docs.FieldInt("max_buffer", "The largest token size expected when consuming delimited files.").Advanced(),
			docs.FieldObject(
				"watcher",
				"An experimental mode whereby the input will periodically scan the target paths for new files and consume them, when all files are consumed the input will continue polling for new files.",
			).WithChildren(watcherDocs...).AtVersion("3.42.0"),
		).ChildDefaultAndTypesFromStruct(input.NewSFTPConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sftpReader struct {
	conf input.SFTPConfig

	log log.Modular
	mgr bundle.NewManagement

	client *sftp.Client

	paths       []string
	scannerCtor codec.ReaderConstructor

	scannerMut  sync.Mutex
	scanner     codec.Reader
	currentPath string

	watcherPollInterval time.Duration
	watcherMinAge       time.Duration
}

func newSFTPReader(conf input.SFTPConfig, mgr bundle.NewManagement) (*sftpReader, error) {
	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	var watcherPollInterval, watcherMinAge time.Duration
	if conf.Watcher.Enabled {
		if watcherPollInterval, err = time.ParseDuration(conf.Watcher.PollInterval); err != nil {
			return nil, fmt.Errorf("failed to parse watcher poll interval: %w", err)
		}

		if watcherMinAge, err = time.ParseDuration(conf.Watcher.MinimumAge); err != nil {
			return nil, fmt.Errorf("failed to parse watcher minimum age: %w", err)
		}

		if conf.Watcher.Cache == "" {
			return nil, errors.New("a cache must be specified when watcher mode is enabled")
		}

		if !mgr.ProbeCache(conf.Watcher.Cache) {
			return nil, fmt.Errorf("cache resource '%v' was not found", conf.Watcher.Cache)
		}
	}

	s := &sftpReader{
		conf:                conf,
		log:                 mgr.Logger(),
		mgr:                 mgr,
		scannerCtor:         ctor,
		watcherPollInterval: watcherPollInterval,
		watcherMinAge:       watcherMinAge,
	}

	return s, err
}

func (s *sftpReader) Connect(ctx context.Context) error {
	var err error

	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner != nil {
		return nil
	}

	if s.client == nil {
		if s.client, err = s.conf.Credentials.GetClient(s.mgr.FS(), s.conf.Address); err != nil {
			return err
		}
		s.log.Debugln("Finding more paths")
		s.paths, err = s.getFilePaths(ctx)
		if err != nil {
			return err
		}
	}

	if len(s.paths) == 0 {
		if !s.conf.Watcher.Enabled {
			s.client.Close()
			s.client = nil
			s.log.Debugln("Paths exhausted, closing input")
			return component.ErrTypeClosed
		}
		select {
		case <-time.After(s.watcherPollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
		s.paths, err = s.getFilePaths(ctx)
		return err
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

func (s *sftpReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner == nil || s.client == nil {
		return nil, nil, component.ErrNotConnected
	}

	parts, codecAckFn, err := s.scanner.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = component.ErrTimeout
		}
		if err != component.ErrTimeout {
			if s.conf.Watcher.Enabled {
				var setErr error
				if cerr := s.mgr.AccessCache(ctx, s.conf.Watcher.Cache, func(cache cache.V1) {
					setErr = cache.Set(ctx, s.currentPath, []byte("@"), nil)
				}); cerr != nil {
					return nil, nil, fmt.Errorf("failed to get the cache for sftp watcher mode: %v", cerr)
				}
				if setErr != nil {
					return nil, nil, fmt.Errorf("failed to update path in cache %s: %v", s.currentPath, err)
				}
			}
			s.scanner.Close(ctx)
			s.scanner = nil
		}
		if errors.Is(err, io.EOF) {
			err = component.ErrTimeout
		}
		return nil, nil, err
	}

	for _, part := range parts {
		part.MetaSetMut("sftp_path", s.currentPath)
	}

	msg := message.Batch(parts)
	return msg, func(ctx context.Context, res error) error {
		return codecAckFn(ctx, res)
	}, nil
}

func (s *sftpReader) Close(ctx context.Context) (err error) {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner != nil {
		err = s.scanner.Close(ctx)
		s.scanner = nil
		s.paths = nil
	}
	if err == nil && s.client != nil {
		err = s.client.Close()
		s.client = nil
	}
	return
}

func (s *sftpReader) getFilePaths(ctx context.Context) ([]string, error) {
	var filepaths []string
	if !s.conf.Watcher.Enabled {
		for _, p := range s.conf.Paths {
			paths, err := s.client.Glob(p)
			if err != nil {
				s.log.Warnf("Failed to scan files from path %v: %v\n", p, err)
				continue
			}
			filepaths = append(filepaths, paths...)
		}
		return filepaths, nil
	}

	if cerr := s.mgr.AccessCache(ctx, s.conf.Watcher.Cache, func(cache cache.V1) {
		for _, p := range s.conf.Paths {
			paths, err := s.client.Glob(p)
			if err != nil {
				s.log.Warnf("Failed to scan files from path %v: %v\n", p, err)
				continue
			}

			for _, path := range paths {
				info, err := s.client.Stat(path)
				if err != nil {
					s.log.Warnf("Failed to stat path %v: %v\n", path, err)
					continue
				}
				if time.Since(info.ModTime()) < s.watcherMinAge {
					continue
				}
				if _, err := cache.Get(ctx, path); err != nil {
					filepaths = append(filepaths, path)
				} else if err = cache.Set(ctx, path, []byte("@"), nil); err != nil { // Reset the TTL for the path
					s.log.Warnf("Failed to set key in cache for path %v: %v\n", path, err)
				}
			}
		}
	}); cerr != nil {
		return nil, fmt.Errorf("error getting cache in getFilePaths: %v", cerr)
	}
	return filepaths, nil
}
