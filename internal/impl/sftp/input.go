package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"

	"github.com/benthosdev/benthos/v4/internal/codec/interop"
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
			service.NewAutoRetryNacksToggleField(),
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
		return service.AutoRetryNacksBatchedToggled(conf, r)
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sftpReader struct {
	log *service.Logger
	mgr *service.Resources

	// Config
	address        string
	paths          []string
	creds          Credentials
	scannerCtor    interop.FallbackReaderCodec
	deleteOnFinish bool

	watcherEnabled      bool
	watcherCache        string
	watcherPollInterval time.Duration
	watcherMinAge       time.Duration

	pathProvider pathProvider

	// State
	scannerMut  sync.Mutex
	client      *sftp.Client
	scanner     interop.FallbackReaderStream
	currentPath string
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

func (s *sftpReader) Connect(ctx context.Context) (err error) {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner != nil {
		return nil
	}

	if s.client == nil {
		if s.client, err = s.creds.GetClient(s.mgr.FS(), s.address); err != nil {
			return
		}
	}

	if s.pathProvider == nil {
		s.pathProvider = s.getFilePathProvider(ctx)
	}

	var nextPath string
	var file *sftp.File
	for {
		if nextPath, err = s.pathProvider.Next(ctx, s.client); err != nil {
			if errors.Is(err, sftp.ErrSshFxConnectionLost) {
				_ = s.client.Close()
				s.client = nil
				return
			}
			if errors.Is(err, errEndOfPaths) {
				err = service.ErrEndOfInput
			}
			return
		}

		if file, err = s.client.Open(nextPath); err != nil {
			if errors.Is(err, sftp.ErrSshFxConnectionLost) {
				_ = s.client.Close()
				s.client = nil
			}

			s.log.With("path", nextPath, "err", err.Error()).Warn("Unable to open previously identified file")
			if os.IsNotExist(err) {
				// If we failed to open the file because it no longer exists
				// then we can "ack" the path as we're done with it.
				_ = s.pathProvider.Ack(ctx, nextPath, nil)
			} else {
				// Otherwise we "nack" it with the error as we'll want to
				// reprocess it again later.
				_ = s.pathProvider.Ack(ctx, nextPath, err)
			}
		} else {
			break
		}
	}

	if s.scanner, err = s.scannerCtor.Create(file, func(ctx context.Context, aErr error) (outErr error) {
		_ = s.pathProvider.Ack(ctx, nextPath, aErr)
		if aErr != nil {
			return nil
		}
		if s.deleteOnFinish {
			s.scannerMut.Lock()
			client := s.client
			if client == nil {
				if client, outErr = s.creds.GetClient(s.mgr.FS(), s.address); outErr != nil {
					outErr = fmt.Errorf("obtain private client: %w", outErr)
				}
				defer func() {
					_ = client.Close()
				}()
			}
			if outErr == nil {
				if outErr = client.Remove(nextPath); outErr != nil {
					outErr = fmt.Errorf("remove %v: %w", nextPath, outErr)
				}
			}
			s.scannerMut.Unlock()
		}
		return
	}, scanner.SourceDetails{Name: nextPath}); err != nil {
		_ = file.Close()
		_ = s.pathProvider.Ack(ctx, nextPath, err)
		return err
	}
	s.currentPath = nextPath

	s.log.Debugf("Consuming from file '%v'", nextPath)
	return
}

func (s *sftpReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	s.scannerMut.Lock()
	scanner := s.scanner
	client := s.client
	currentPath := s.currentPath
	s.scannerMut.Unlock()

	if scanner == nil || client == nil {
		return nil, nil, service.ErrNotConnected
	}

	parts, codecAckFn, err := scanner.NextBatch(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		_ = scanner.Close(ctx)
		s.scannerMut.Lock()
		if s.currentPath == currentPath {
			s.scanner = nil
			s.currentPath = ""
		}
		s.scannerMut.Unlock()
		if errors.Is(err, io.EOF) {
			err = service.ErrNotConnected
		}
		return nil, nil, err
	}

	for _, part := range parts {
		part.MetaSetMut("sftp_path", currentPath)
	}

	return parts, func(ctx context.Context, res error) error {
		return codecAckFn(ctx, res)
	}, nil
}

func (s *sftpReader) Close(ctx context.Context) error {
	s.scannerMut.Lock()
	scanner := s.scanner
	s.scanner = nil
	client := s.client
	s.client = nil
	s.paths = nil
	s.scannerMut.Unlock()

	if scanner != nil {
		if err := scanner.Close(ctx); err != nil {
			s.log.With("error", err).Warn("Failed to close consumed file")
		}
	}
	if client != nil {
		if err := client.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close client")
		}
	}
	return nil
}

//------------------------------------------------------------------------------

var errEndOfPaths = errors.New("end of paths")

type pathProvider interface {
	Next(context.Context, *sftp.Client) (string, error)
	Ack(context.Context, string, error) error
}

type staticPathProvider struct {
	expandedPaths []string
}

func (s *staticPathProvider) Next(ctx context.Context, client *sftp.Client) (string, error) {
	if len(s.expandedPaths) == 0 {
		return "", errEndOfPaths
	}
	nextPath := s.expandedPaths[0]
	s.expandedPaths = s.expandedPaths[1:]
	return nextPath, nil
}

func (s *staticPathProvider) Ack(context.Context, string, error) error {
	return nil
}

type watcherPathProvider struct {
	mgr          *service.Resources
	cacheName    string
	pollInterval time.Duration
	minAge       time.Duration
	targetPaths  []string

	expandedPaths []string
	nextPoll      time.Time
	followUpPoll  bool
}

func (w *watcherPathProvider) Next(ctx context.Context, client *sftp.Client) (string, error) {
	if len(w.expandedPaths) > 0 {
		nextPath := w.expandedPaths[0]
		w.expandedPaths = w.expandedPaths[1:]
		return nextPath, nil
	}

	if waitFor := time.Until(w.nextPoll); waitFor > 0 {
		w.nextPoll = time.Now().Add(w.pollInterval)
		select {
		case <-time.After(waitFor):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	if cerr := w.mgr.AccessCache(ctx, w.cacheName, func(cache service.Cache) {
		for _, p := range w.targetPaths {
			paths, err := client.Glob(p)
			if err != nil {
				w.mgr.Logger().With("error", err, "path", p).Warn("Failed to scan files from path")
				continue
			}

			for _, path := range paths {
				info, err := client.Stat(path)
				if err != nil {
					w.mgr.Logger().With("error", err, "path", path).Warn("Failed to stat path")
					continue
				}
				if time.Since(info.ModTime()) < w.minAge {
					continue
				}

				// We process it if the marker is a pending symbol (!) and we're
				// polling for the first time, or if the path isn't found in the
				// cache.
				//
				// If we got an unexpected error obtaining a marker for this
				// path from the cache then we skip that path because the
				// watcher will eventually poll again, and the cache.Get
				// operation will re-run.
				if v, err := cache.Get(ctx, path); errors.Is(err, service.ErrKeyNotFound) || (!w.followUpPoll && string(v) == "!") {
					w.expandedPaths = append(w.expandedPaths, path)
					if err = cache.Set(ctx, path, []byte("!"), nil); err != nil {
						// Mark the file target as pending so that we do not reprocess it
						w.mgr.Logger().With("error", err, "path", path).Warn("Failed to mark path as pending")
					}
				}
			}
		}
	}); cerr != nil {
		return "", fmt.Errorf("error obtaining cache: %v", cerr)
	}
	w.followUpPoll = true
	return w.Next(ctx, client)
}

func (w *watcherPathProvider) Ack(ctx context.Context, name string, err error) (outErr error) {
	if cerr := w.mgr.AccessCache(ctx, w.cacheName, func(cache service.Cache) {
		if err == nil {
			outErr = cache.Set(ctx, name, []byte("@"), nil)
		} else {
			_ = cache.Delete(ctx, name)
		}
	}); cerr != nil {
		outErr = cerr
	}
	return
}

func (s *sftpReader) getFilePathProvider(_ context.Context) pathProvider {
	if !s.watcherEnabled {
		var filepaths []string
		for _, p := range s.paths {
			paths, err := s.client.Glob(p)
			if err != nil {
				s.log.Warnf("Failed to scan files from path %v: %v", p, err)
				continue
			}
			filepaths = append(filepaths, paths...)
		}
		return &staticPathProvider{expandedPaths: filepaths}
	}

	return &watcherPathProvider{
		mgr:          s.mgr,
		cacheName:    s.watcherCache,
		pollInterval: s.watcherPollInterval,
		minAge:       s.watcherMinAge,
		targetPaths:  s.paths,
	}
}
