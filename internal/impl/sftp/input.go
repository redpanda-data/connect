// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/codec"
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
== Metadata

This input adds the following metadata fields to each message:

- sftp_path

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(
			service.NewStringField(siFieldAddress).
				Description("The address of the server to connect to."),
			service.NewObjectField(siFieldCredentials, credentialsFields()...).
				Description("The credentials to use to log into the target server."),
			service.NewStringListField(siFieldPaths).
				Description("A list of paths to consume sequentially. Glob patterns are supported."),
			service.NewAutoRetryNacksToggleField(),
		).
		Fields(codec.DeprecatedCodecFields("to_the_end")...).
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
					Description("A xref:components:caches/about.adoc[cache resource] for storing the paths of files already consumed.").
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
	creds          credentials
	scannerCtor    codec.DeprecatedFallbackCodec
	deleteOnFinish bool

	watcherEnabled      bool
	watcherCache        string
	watcherPollInterval time.Duration
	watcherMinAge       time.Duration

	pathProvider pathProvider

	// State
	scannerMut  sync.Mutex
	client      *sftp.Client
	scanner     codec.DeprecatedFallbackStream
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
	if s.scannerCtor, err = codec.DeprecatedCodecFromParsed(conf); err != nil {
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
	file, nextPath, skip, err := s.seekNextPath(ctx)
	if err != nil {
		return err
	}
	if skip {
		return nil
	}

	details := service.NewScannerSourceDetails()
	details.SetName(nextPath)
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
	}, details); err != nil {
		_ = file.Close()
		_ = s.pathProvider.Ack(ctx, nextPath, err)
		return err
	}

	s.scannerMut.Lock()
	s.currentPath = nextPath
	s.scannerMut.Unlock()

	s.log.Debugf("Consuming from file '%v'", nextPath)
	return
}

func (s *sftpReader) initState(ctx context.Context) (client *sftp.Client, pathProvider pathProvider, skip bool, err error) {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner != nil {
		skip = true
		return
	}

	if s.client == nil {
		if s.client, err = s.creds.GetClient(s.mgr.FS(), s.address); err != nil {
			return
		}
	}

	if s.pathProvider == nil {
		s.pathProvider = s.getFilePathProvider(ctx)
	}

	return s.client, s.pathProvider, false, nil
}

func (s *sftpReader) seekNextPath(ctx context.Context) (file *sftp.File, nextPath string, skip bool, err error) {
	client, pathProvider, skip, err := s.initState(ctx)
	if err != nil || skip {
		return
	}

	for {
		if nextPath, err = pathProvider.Next(ctx, client); err != nil {
			if errors.Is(err, sftp.ErrSshFxConnectionLost) {
				_ = client.Close()
				s.scannerMut.Lock()
				s.client = nil
				s.scannerMut.Unlock()
				return
			}
			if errors.Is(err, errEndOfPaths) {
				err = service.ErrEndOfInput
			}
			return
		}

		if file, err = client.Open(nextPath); err != nil {
			if errors.Is(err, sftp.ErrSshFxConnectionLost) {
				_ = client.Close()
				s.scannerMut.Lock()
				s.client = nil
				s.scannerMut.Unlock()
			}

			s.log.With("path", nextPath, "err", err.Error()).Warn("Unable to open previously identified file")
			if os.IsNotExist(err) {
				// If we failed to open the file because it no longer exists
				// then we can "ack" the path as we're done with it.
				_ = pathProvider.Ack(ctx, nextPath, nil)
			} else {
				// Otherwise we "nack" it with the error as we'll want to
				// reprocess it again later.
				_ = pathProvider.Ack(ctx, nextPath, err)
			}
		} else {
			return
		}
	}
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

	return parts, codecAckFn, nil
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
	for {
		if len(w.expandedPaths) > 0 {
			nextPath := w.expandedPaths[0]
			w.expandedPaths = w.expandedPaths[1:]
			return nextPath, nil
		}

		if waitFor := time.Until(w.nextPoll); w.nextPoll.IsZero() || waitFor > 0 {
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
	}
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
