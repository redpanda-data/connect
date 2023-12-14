package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pkg/sftp"

	"github.com/benthosdev/benthos/v4/internal/codec/interop"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	siFieldAddress             = "address"
	siFieldCredentials         = "credentials"
	siFieldPaths               = "paths"
	siFieldDeleteOnFinish      = "delete_on_finish"
	siFieldWatcher             = "watcher"
	siFieldWatcherEnabled      = "enabled"
	siFieldWatcherMinimumAge   = "minimum_age"
	siFieldWatcherPollInterval = "poll_interval"
	siFieldWatcherCache        = "cache"
)

func sftpInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Network").
		Version("3.39.0").
		Summary(`Consumes files from an SFTP server.`).
		Description(`
## Metadata

This input adds the following metadata fields to each message:

`+"```"+`
- sftp_path
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringField(siFieldAddress).
				Description("The address of the server to connect to."),
			service.NewObjectField(siFieldCredentials, credentialsFields()...).
				Description("The credentials to use to log into the target server."),
			service.NewStringListField(siFieldPaths).
				Description("A list of paths to consume sequentially. Glob patterns are supported."),
		).
		Fields(interop.OldReaderCodecFields("to_the_end")...).
		Fields(
			service.NewBoolField(siFieldDeleteOnFinish).
				Description("Whether to delete files from the server once they are processed.").
				Advanced().
				Default(false),
			service.NewObjectField(siFieldWatcher,
				service.NewBoolField(siFieldWatcherEnabled).
					Description("Whether file watching is enabled.").
					Default(false),
				service.NewDurationField(siFieldWatcherMinimumAge).
					Description("The minimum period of time since a file was last updated before attempting to consume it. Increasing this period decreases the likelihood that a file will be consumed whilst it is still being written to.").
					Default("1s").
					Examples("10s", "1m", "10m"),
				service.NewDurationField(siFieldWatcherPollInterval).
					Description("The interval between each attempt to scan the target paths for new files.").
					Default("1s").
					Examples("100ms", "1s"),
				service.NewStringField(siFieldWatcherCache).
					Description("A [cache resource](/docs/components/caches/about) for storing the paths of files already consumed.").
					Default(""),
			).Description("An experimental mode whereby the input will periodically scan the target paths for new files and consume them, when all files are consumed the input will continue polling for new files.").
				Version("3.42.0"),
		)
}

func init() {
	err := service.RegisterBatchInput("sftp", sftpInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		r, err := newSFTPReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksBatched(r), nil
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sftpReader struct {
	log *service.Logger
	mgr *service.Resources

	client *sftp.Client

	address        string
	paths          []string
	creds          Credentials
	scannerCtor    interop.FallbackReaderCodec
	deleteOnFinish bool

	scannerMut  sync.Mutex
	scanner     interop.FallbackReaderStream
	currentPath string

	watcherEnabled      bool
	watcherCache        string
	watcherPollInterval time.Duration
	watcherMinAge       time.Duration
}

func newSFTPReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (s *sftpReader, err error) {
	s = &sftpReader{
		log: mgr.Logger(),
		mgr: mgr,
	}

	if s.address, err = conf.FieldString(siFieldAddress); err != nil {
		return
	}
	if s.paths, err = conf.FieldStringList(siFieldPaths); err != nil {
		return
	}
	if s.creds, err = credentialsFromParsed(conf.Namespace(siFieldCredentials)); err != nil {
		return
	}
	if s.scannerCtor, err = interop.OldReaderCodecFromParsed(conf); err != nil {
		return
	}
	if s.deleteOnFinish, err = conf.FieldBool(siFieldDeleteOnFinish); err != nil {
		return
	}

	{
		wConf := conf.Namespace(siFieldWatcher)
		if s.watcherEnabled, _ = wConf.FieldBool(siFieldWatcherEnabled); s.watcherEnabled {
			if s.watcherCache, err = wConf.FieldString(siFieldWatcherCache); err != nil {
				return
			}
			if s.watcherPollInterval, err = wConf.FieldDuration(siFieldWatcherPollInterval); err != nil {
				return
			}
			if s.watcherMinAge, err = wConf.FieldDuration(siFieldWatcherMinimumAge); err != nil {
				return
			}
			if !mgr.HasCache(s.watcherCache) {
				return nil, fmt.Errorf("cache resource '%v' was not found", s.watcherCache)
			}
		}
	}

	return
}

func (s *sftpReader) Connect(ctx context.Context) error {
	var err error

	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner != nil {
		return nil
	}

	if s.client == nil {
		if s.client, err = s.creds.GetClient(s.mgr.FS(), s.address); err != nil {
			return err
		}
		s.log.Debug("Finding more paths")
		s.paths, err = s.getFilePaths(ctx)
		if err != nil {
			return err
		}
	}

	if len(s.paths) == 0 {
		if !s.watcherEnabled {
			s.client.Close()
			s.client = nil
			s.log.Debug("Paths exhausted, closing input")
			return service.ErrEndOfInput
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

	if s.scanner, err = s.scannerCtor.Create(file, func(ctx context.Context, err error) error {
		if err == nil && s.deleteOnFinish {
			return s.client.Remove(nextPath)
		}
		return nil
	}, scanner.SourceDetails{Name: nextPath}); err != nil {
		file.Close()
		return err
	}

	s.currentPath = nextPath
	s.paths = s.paths[1:]

	s.log.Infof("Consuming from file '%v'", nextPath)
	return err
}

func (s *sftpReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner == nil || s.client == nil {
		return nil, nil, service.ErrNotConnected
	}

	parts, codecAckFn, err := s.scanner.NextBatch(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = component.ErrTimeout
		}
		if err != component.ErrTimeout {
			if s.watcherEnabled {
				var setErr error
				if cerr := s.mgr.AccessCache(ctx, s.watcherCache, func(cache service.Cache) {
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

	return parts, func(ctx context.Context, res error) error {
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
	if !s.watcherEnabled {
		for _, p := range s.paths {
			paths, err := s.client.Glob(p)
			if err != nil {
				s.log.Warnf("Failed to scan files from path %v: %v", p, err)
				continue
			}
			filepaths = append(filepaths, paths...)
		}
		return filepaths, nil
	}

	if cerr := s.mgr.AccessCache(ctx, s.watcherCache, func(cache service.Cache) {
		for _, p := range s.paths {
			paths, err := s.client.Glob(p)
			if err != nil {
				s.log.Warnf("Failed to scan files from path %v: %v", p, err)
				continue
			}

			for _, path := range paths {
				info, err := s.client.Stat(path)
				if err != nil {
					s.log.Warnf("Failed to stat path %v: %v", path, err)
					continue
				}
				if time.Since(info.ModTime()) < s.watcherMinAge {
					continue
				}
				if _, err := cache.Get(ctx, path); err != nil {
					filepaths = append(filepaths, path)
				} else if err = cache.Set(ctx, path, []byte("@"), nil); err != nil { // Reset the TTL for the path
					s.log.Warnf("Failed to set key in cache for path %v: %v", path, err)
				}
			}
		}
	}); cerr != nil {
		return nil, fmt.Errorf("error getting cache in getFilePaths: %v", cerr)
	}
	return filepaths, nil
}
